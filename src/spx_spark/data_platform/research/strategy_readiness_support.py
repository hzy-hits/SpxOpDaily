"""Shared parsing and session helpers for strategy readiness."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR


def event_at(payload: Mapping[str, object]) -> datetime | None:
    event = payload.get("event")
    fields = {
        "virtual_closed": ("closed_at",),
        "virtual_opened": ("opened_at",),
        "virtual_horizon_outcome": ("observed_at",),
    }.get(str(event), ())
    for field in (
        *fields,
        "evaluated_at",
        "terminal_at",
        "armed_at",
        "confirmed_at",
        "closed_at",
        "opened_at",
        "observed_at",
        "at",
        "updated_at",
    ):
        parsed = parse_time(payload.get(field))
        if parsed is not None:
            return parsed
    return None


def record_session(
    payload: Mapping[str, object],
    *,
    at: datetime | None,
    fallback: date | None,
) -> date | None:
    for field in ("session_date", "session_id"):
        parsed = parse_date(payload.get(field))
        if parsed is not None:
            return parsed
    return research_session(at) if at is not None else fallback


def research_session(at: datetime | None) -> date | None:
    return DEFAULT_MARKET_CALENDAR.research_expiry(at) if at is not None else None


def partition_date(path: Path) -> date | None:
    for parent in path.parents:
        if parent.name.startswith("date="):
            return parse_date(parent.name.removeprefix("date="))
    return None


def parse_time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def parse_date(value: object) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def nonempty_string(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def unique(values: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(values))


def detector_health_start(rows: Sequence[Mapping[str, object]]) -> date | None:
    starts = [
        parsed
        for row in rows
        if (parsed := parse_date(row.get("gth_detector_health_started_session"))) is not None
    ]
    return min(starts, default=None)


def detector_health_start_at(rows: Sequence[Mapping[str, object]]) -> datetime | None:
    times = [
        parsed
        for row in rows
        if (parsed := parse_time(row.get("gth_detector_health_started_at"))) is not None
    ]
    return min(times, default=None)


def policy_bundle_start_at(
    bundle: Mapping[str, object],
    *,
    roles: Sequence[str],
) -> datetime | None:
    starts = bundle.get("role_started_at")
    if not isinstance(starts, Mapping):
        return None
    parsed = [parse_time(starts.get(role)) for role in roles]
    if any(started_at is None for started_at in parsed):
        return None
    return max(parsed, default=None)


def latest_date(*values: object) -> date | None:
    dates = [value for value in values if isinstance(value, date)]
    return max(dates, default=None)


def next_trading_day(value: object) -> date | None:
    if not isinstance(value, date):
        return None
    candidate = value + timedelta(days=1)
    while not DEFAULT_MARKET_CALENDAR.is_trading_day(candidate):
        candidate += timedelta(days=1)
    return candidate


def utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("readiness timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)
