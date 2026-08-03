"""Causal intraday extreme projections for advisory research context."""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import date, datetime, timezone

from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR


UTC = timezone.utc
FUTURE_TOLERANCE_SECONDS = 2.0
MODEL_VERSION = "intraday-extreme-em-bootstrap:v1"


def build_intraday_extreme_ranges(
    *,
    options: Mapping[str, object],
    spx_minutes: Mapping[str, object],
    session_day: date,
    now: datetime,
    max_input_age_seconds: float,
) -> tuple[dict[str, object], dict[str, object]]:
    session = DEFAULT_MARKET_CALENDAR.session(session_day)
    assert session is not None
    target_at = session.close_at.astimezone(UTC)
    if now >= target_at:
        unavailable = _unavailable("rth_close_target_elapsed", target_at)
        return unavailable, unavailable
    if options.get("front_expiry") != session_day.strftime("%Y%m%d"):
        unavailable = _unavailable("same_day_expected_move_expiry_mismatch", target_at)
        return unavailable, unavailable
    option_as_of = _parse_at(options.get("as_of"))
    option_age_seconds = (now - option_as_of).total_seconds() if option_as_of is not None else None
    if (
        option_age_seconds is None
        or option_age_seconds < -FUTURE_TOLERANCE_SECONDS
        or option_age_seconds > max_input_age_seconds
    ):
        unavailable = _unavailable("same_day_expected_move_stale", target_at)
        return unavailable, unavailable
    expected_move = _expected_move(options)
    if expected_move is None:
        unavailable = _unavailable("same_day_expected_move_unavailable", target_at)
        return unavailable, unavailable
    path = _observed_session_path(
        spx_minutes,
        session_day=session_day,
        now=now,
        max_input_age_seconds=max_input_age_seconds,
    )
    if path is None:
        unavailable = _unavailable("fresh_spx_intraday_path_unavailable", target_at)
        return unavailable, unavailable
    current, observed_high, observed_low, observed_at = path
    session_seconds = (target_at - session.open_at.astimezone(UTC)).total_seconds()
    remaining_fraction = max(
        (target_at - now).total_seconds() / session_seconds,
        1.0 / 390.0,
    )
    remaining_move = expected_move * math.sqrt(min(remaining_fraction, 1.0))
    common = {
        "status": "available",
        "quality": "degraded",
        "source": "spx_standardized_minutes_plus_same_day_expected_move",
        "semantics": "experimental_remaining_expected_move_extreme_buckets_not_physical",
        "target_at": target_at.isoformat(),
        "as_of": observed_at.isoformat(),
        "expected_move_points": expected_move,
        "remaining_move_points": round(remaining_move, 4),
        "current_spx": current,
        "observed_session_high": observed_high,
        "observed_session_low": observed_low,
        "model_version": MODEL_VERSION,
    }
    high = {
        **common,
        "p10": round(observed_high + 0.05 * remaining_move, 4),
        "p50": round(observed_high + 0.25 * remaining_move, 4),
        "p90": round(observed_high + 0.75 * remaining_move, 4),
    }
    low = {
        **common,
        "p10": round(observed_low - 0.75 * remaining_move, 4),
        "p50": round(observed_low - 0.25 * remaining_move, 4),
        "p90": round(observed_low - 0.05 * remaining_move, 4),
    }
    return high, low


def _observed_session_path(
    spx_minutes: Mapping[str, object],
    *,
    session_day: date,
    now: datetime,
    max_input_age_seconds: float,
) -> tuple[float, float, float, datetime] | None:
    session = DEFAULT_MARKET_CALENDAR.session(session_day)
    rows = spx_minutes.get("rows")
    if session is None or not isinstance(rows, list):
        return None
    open_at = session.open_at.astimezone(UTC)
    close_at = session.close_at.astimezone(UTC)
    candidates: list[tuple[datetime, float]] = []
    for value in rows:
        row = _mapping(value)
        if row.get("session_date") != session_day.isoformat():
            continue
        at = _parse_at(row.get("minute"))
        price = _selected_price(row)
        if at is not None and price is not None and open_at <= at <= min(now, close_at):
            candidates.append((at, price))
    if not candidates:
        return None
    candidates.sort()
    last_at, current = candidates[-1]
    age_seconds = (now - last_at).total_seconds()
    if not 0.0 <= age_seconds <= max_input_age_seconds:
        return None
    prices = [price for _at, price in candidates]
    return current, max(prices), min(prices), last_at


def _unavailable(reason: str, target_at: datetime) -> dict[str, object]:
    return {
        "status": "unavailable",
        "quality": "unavailable",
        "reason": reason,
        "p10": None,
        "p50": None,
        "p90": None,
        "target_at": target_at.isoformat(),
    }


def _expected_move(options: Mapping[str, object]) -> float | None:
    value = _number(_mapping(options.get("volatility")).get("expected_move_points_0dte"))
    return value if value is not None and value > 0.0 else None


def _selected_price(row: Mapping[str, object]) -> float | None:
    value = _number(_mapping(row.get("selected")).get("price"))
    return value if value is not None and value > 0.0 else None


def _mapping(value: object) -> dict[str, object]:
    return dict(value) if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


def _parse_at(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(UTC)


__all__ = ["build_intraday_extreme_ranges"]
