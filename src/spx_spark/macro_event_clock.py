"""Deterministic macro-event clock loaded from an explicit TOML calendar."""

from __future__ import annotations

import os
import tomllib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping

from spx_spark.macro_event_calendar import (
    calendar_coverage,
    load_merged_macro_calendar,
)

DEFAULT_PATH = Path(__file__).resolve().parents[2] / "config" / "macro_events.toml"


def macro_event_state(
    now: datetime,
    *,
    path: str | Path | None = None,
    data_root: str | Path | None = None,
) -> dict[str, object]:
    now = _utc(now)
    resolved = Path(
        path or os.getenv("SPX_SPARK_MACRO_EVENTS_CONFIG") or DEFAULT_PATH
    )
    try:
        if data_root is not None:
            payload = load_merged_macro_calendar(resolved, data_root)
        else:
            payload = tomllib.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError, ValueError) as exc:
        return {
            "mode": "unavailable",
            "entry_allowed": False,
            "reason": f"macro_calendar_unavailable:{type(exc).__name__}",
            "as_of": now.isoformat(),
        }
    defaults = payload.get("defaults") if isinstance(payload, Mapping) else {}
    defaults = defaults if isinstance(defaults, Mapping) else {}
    coverage = calendar_coverage(payload, now=now)
    covered = coverage["status"] == "covered"
    active: list[dict[str, object]] = []
    upcoming: list[tuple[datetime, dict[str, object]]] = []
    for raw in payload.get("events") or [] if isinstance(payload, Mapping) else []:
        if not isinstance(raw, Mapping):
            continue
        release = _time(raw.get("release_at"))
        if release is None:
            continue
        pre = int(raw.get("pre_window_minutes") or defaults.get("pre_window_minutes") or 30)
        post = int(raw.get("post_window_minutes") or defaults.get("post_window_minutes") or 90)
        row = {
            "id": raw.get("id"),
            "name": raw.get("name"),
            "impact": raw.get("impact"),
            "release_at": release.isoformat(),
            "minutes_to_release": round((release - now).total_seconds() / 60.0, 2),
            "description": raw.get("description"),
        }
        if release - timedelta(minutes=pre) <= now < release:
            active.append({**row, "phase": "pre_event"})
        elif release <= now <= release + timedelta(minutes=post):
            active.append({**row, "phase": "post_event"})
        elif release > now:
            upcoming.append((release, row))
    if active:
        blocking = [row for row in active if row["phase"] == "pre_event"]
        selected = min(blocking or active,
                       key=lambda row: abs(float(row["minutes_to_release"])))
        mode = str(selected["phase"])
        return {
            "mode": mode,
            "entry_allowed": not blocking and covered,
            "active_event": selected,
            "active_events": active,
            "coverage": coverage,
            "as_of": now.isoformat(),
            "calendar_path": str(resolved),
            "overlay_refreshed_at": payload.get("overlay_refreshed_at")
            if isinstance(payload, Mapping)
            else None,
        }
    next_event = min(upcoming, key=lambda pair: pair[0])[1] if upcoming else None
    return {
        "mode": "normal" if covered else "unavailable",
        "entry_allowed": covered,
        "reason": None if covered else "macro_calendar_coverage_unknown",
        "coverage": coverage,
        "active_event": None,
        "next_event": next_event,
        "as_of": now.isoformat(),
        "calendar_path": str(resolved),
        "overlay_refreshed_at": payload.get("overlay_refreshed_at")
        if isinstance(payload, Mapping)
        else None,
    }


def _time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return _utc(datetime.fromisoformat(value))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
