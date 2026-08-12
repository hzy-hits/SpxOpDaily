"""Refresh allowlisted macro events from live calendars into a runtime overlay."""

from __future__ import annotations

import json
import re
import tomllib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from spx_spark.state_io import atomic_write_json_secure, read_json_object

NY = ZoneInfo("America/New_York")
REFRESH_TTL = timedelta(hours=6)
HTTP_TIMEOUT_SECONDS = 20.0
USER_AGENT = "spx-spark-macro-calendar/1.0 (+local; read-only economic calendar)"
FF_THIS_WEEK_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
FOMC_CALENDAR_URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
DEFAULT_PRE_WINDOW = 30
DEFAULT_POST_WINDOW = 90

# One release timestamp collapses to one desk event even when FF emits m/m + y/y.
_FF_RULES: tuple[tuple[re.Pattern[str], str, str], ...] = (
    (re.compile(r"\bcpi\b", re.I), "US CPI", "cpi"),
    (re.compile(r"\bppi\b", re.I), "US PPI", "ppi"),
    (
        re.compile(
            r"non[- ]?farm|nonfarm payrolls|employment situation|unemployment rate",
            re.I,
        ),
        "US Employment Situation",
        "nfp",
    ),
)
_FF_EXCLUDE = re.compile(
    r"member .+ speaks|speaks$|cleveland fed inflation expectations|federal budget",
    re.I,
)


def runtime_macro_events_path(data_root: str | Path) -> Path:
    return Path(data_root) / "runtime" / "macro_events.auto.json"


def refresh_macro_events_if_due(
    data_root: str | Path,
    *,
    now: datetime,
    force: bool = False,
) -> dict[str, Any]:
    """Fetch allowlisted events when the runtime overlay is stale.

    Failures keep the previous overlay (or leave it absent) so the seed TOML
    calendar remains usable.
    """

    path = runtime_macro_events_path(data_root)
    existing = read_json_object(path)
    if not force and _overlay_fresh(existing, now=now):
        return {
            "status": "skipped_fresh",
            "path": str(path),
            "event_count": len(_events_list(existing)),
            "refreshed_at": existing.get("refreshed_at"),
        }
    try:
        events = fetch_allowlisted_macro_events(now=now)
    except (HTTPError, URLError, TimeoutError, OSError, ValueError, json.JSONDecodeError) as exc:
        return {
            "status": "fetch_failed",
            "path": str(path),
            "error": f"{type(exc).__name__}:{exc}",
            "event_count": len(_events_list(existing)),
            "refreshed_at": existing.get("refreshed_at"),
        }
    payload = {
        "schema_version": 1,
        "refreshed_at": _utc(now).isoformat(),
        "source": "ff_thisweek+fomc_html",
        "defaults": {
            "pre_window_minutes": DEFAULT_PRE_WINDOW,
            "post_window_minutes": DEFAULT_POST_WINDOW,
        },
        "events": events,
    }
    atomic_write_json_secure(path, payload)
    return {
        "status": "refreshed",
        "path": str(path),
        "event_count": len(events),
        "refreshed_at": payload["refreshed_at"],
    }


def fetch_allowlisted_macro_events(*, now: datetime) -> list[dict[str, Any]]:
    """Pull near-term FF US high-impact prints plus upcoming FOMC statements.

    Sources are independent: a temporary FF rate-limit must not erase FOMC
    coverage, and a Fed HTML outage must not erase the weekly prints.
    """

    rows: dict[str, dict[str, Any]] = {}
    errors: list[str] = []
    for loader, label in (
        (_ff_this_week_events, "ff_thisweek"),
        (lambda: _fomc_statement_events(now=now), "fomc_html"),
    ):
        try:
            for event in loader():
                rows[str(event["id"])] = event
        except (HTTPError, URLError, TimeoutError, OSError, ValueError, json.JSONDecodeError) as exc:
            errors.append(f"{label}:{type(exc).__name__}")
    if not rows:
        raise ValueError("macro_calendar_sources_unavailable:" + ",".join(errors) or "unknown")
    return sorted(rows.values(), key=lambda row: str(row["release_at"]))


def load_merged_macro_calendar(
    seed_path: Path,
    data_root: str | Path | None,
) -> dict[str, Any]:
    """Merge seed TOML with runtime overlay; overlay wins on matching ids."""

    seed = tomllib.loads(seed_path.read_text(encoding="utf-8"))
    defaults = seed.get("defaults") if isinstance(seed.get("defaults"), Mapping) else {}
    merged: dict[str, dict[str, Any]] = {}
    for raw in seed.get("events") or []:
        if isinstance(raw, Mapping) and raw.get("id"):
            merged[str(raw["id"])] = dict(raw)
    if data_root is not None:
        overlay = read_json_object(runtime_macro_events_path(data_root))
        overlay_defaults = overlay.get("defaults")
        if isinstance(overlay_defaults, Mapping) and overlay_defaults:
            defaults = {**dict(defaults), **dict(overlay_defaults)}
        for raw in _events_list(overlay):
            event_id = str(raw.get("id") or "")
            if event_id:
                merged[event_id] = dict(raw)
    return {
        "defaults": dict(defaults),
        "events": sorted(merged.values(), key=lambda row: str(row.get("release_at") or "")),
        "overlay_refreshed_at": (
            read_json_object(runtime_macro_events_path(data_root)).get("refreshed_at")
            if data_root is not None
            else None
        ),
    }


def _ff_this_week_events() -> list[dict[str, Any]]:
    payload = json.loads(_http_get(FF_THIS_WEEK_URL))
    if not isinstance(payload, list):
        raise ValueError("ff_calendar_thisweek_not_a_list")
    selected: dict[tuple[str, str], dict[str, Any]] = {}
    for row in payload:
        if not isinstance(row, Mapping):
            continue
        if str(row.get("country") or "").upper() not in {"USD", "US"}:
            continue
        if str(row.get("impact") or "").lower() != "high":
            continue
        title = str(row.get("title") or "")
        if _FF_EXCLUDE.search(title):
            continue
        matched = next((rule for rule in _FF_RULES if rule[0].search(title)), None)
        if matched is None:
            continue
        release = _parse_release(row.get("date"))
        if release is None:
            continue
        _, name, slug = matched
        key = (slug, release.astimezone(NY).date().isoformat())
        event_id = f"us-{slug}-{release.astimezone(NY).strftime('%Y-%m-%d')}"
        selected[key] = {
            "id": event_id,
            "name": name,
            "release_at": release.astimezone(NY).isoformat(),
            "impact": "high",
            "pre_window_minutes": DEFAULT_PRE_WINDOW,
            "post_window_minutes": DEFAULT_POST_WINDOW,
            "description": f"Auto-imported from economic calendar: {title}",
        }
    return list(selected.values())


def _fomc_statement_events(*, now: datetime) -> list[dict[str, Any]]:
    html = _http_get(FOMC_CALENDAR_URL).decode("utf-8", "replace")
    year = _utc(now).astimezone(NY).year
    section = _fomc_year_section(html, year)
    if not section:
        return []
    events: list[dict[str, Any]] = []
    for month_name, day_text in re.findall(
        r'fomc-meeting__month[^>]*>\s*<strong>\s*([A-Za-z]+)\s*</strong>.*?'
        r'fomc-meeting__date[^>]*>\s*([^<]+)<',
        section,
        flags=re.I | re.S,
    ):
        if "notation" in day_text.lower():
            continue
        statement_day = _last_meeting_day(day_text)
        if statement_day is None:
            continue
        month = _month_number(month_name)
        if month is None:
            continue
        try:
            release_local = datetime(year, month, statement_day, 14, 0, tzinfo=NY)
        except ValueError:
            continue
        if release_local < _utc(now).astimezone(NY) - timedelta(days=1):
            continue
        event_id = f"us-fomc-{release_local.strftime('%Y-%m-%d')}"
        events.append(
            {
                "id": event_id,
                "name": "FOMC Statement",
                "release_at": release_local.isoformat(),
                "impact": "high",
                "pre_window_minutes": DEFAULT_PRE_WINDOW,
                "post_window_minutes": DEFAULT_POST_WINDOW,
                "description": "Auto-imported from Federal Reserve FOMC calendar.",
            }
        )
    return events


def _fomc_year_section(html: str, year: int) -> str:
    """Slice one calendar year. Fed pages list newer years first, then archives."""

    start_markers = (
        f"{year} FOMC Meetings",
        f"{year}</strong>",
        f"{year}</h4>",
        f"{year}</h3>",
    )
    start = -1
    for marker in start_markers:
        start = html.find(marker)
        if start >= 0:
            break
    if start < 0:
        return ""
    end = len(html)
    for marker in (
        f"{year - 1} FOMC Meetings",
        f"{year + 1} FOMC Meetings",
        f"{year - 1}</strong>",
        f"{year + 1}</strong>",
        f"{year - 1}</h4>",
        f"{year + 1}</h4>",
    ):
        index = html.find(marker, start + 1)
        if index > start:
            end = min(end, index)
    return html[start:end]


def _last_meeting_day(text: str) -> int | None:
    numbers = [int(part) for part in re.findall(r"\d+", text)]
    return numbers[-1] if numbers else None


def _month_number(name: str) -> int | None:
    months = {
        "january": 1,
        "february": 2,
        "march": 3,
        "april": 4,
        "may": 5,
        "june": 6,
        "july": 7,
        "august": 8,
        "september": 9,
        "october": 10,
        "november": 11,
        "december": 12,
    }
    return months.get(name.strip().lower())


def _parse_release(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=NY)
    return parsed.astimezone(timezone.utc)


def _http_get(url: str) -> bytes:
    request = Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json,text/html,*/*",
        },
    )
    with urlopen(request, timeout=HTTP_TIMEOUT_SECONDS) as response:
        return response.read()


def _overlay_fresh(payload: Mapping[str, Any], *, now: datetime) -> bool:
    raw = payload.get("refreshed_at")
    if not isinstance(raw, str):
        return False
    try:
        refreshed = datetime.fromisoformat(raw)
    except ValueError:
        return False
    if refreshed.tzinfo is None:
        refreshed = refreshed.replace(tzinfo=timezone.utc)
    return _utc(now) - refreshed.astimezone(timezone.utc) < REFRESH_TTL


def _events_list(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("events")
    if not isinstance(rows, list):
        return []
    return [dict(row) for row in rows if isinstance(row, Mapping)]


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
