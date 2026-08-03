"""Causal intraday extreme projections for advisory research context."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR


UTC = timezone.utc
FUTURE_TOLERANCE_SECONDS = 2.0
MODEL_VERSION = "intraday-extreme-em-bootstrap:v1"


@dataclass(frozen=True, slots=True)
class MarketRegimeFreshnessPolicy:
    live_input_max_age_seconds: float
    standardized_spx_minute_max_age_seconds: float

    def __post_init__(self) -> None:
        for name, value in (
            ("live_input_max_age_seconds", self.live_input_max_age_seconds),
            (
                "standardized_spx_minute_max_age_seconds",
                self.standardized_spx_minute_max_age_seconds,
            ),
        ):
            if not math.isfinite(value) or value <= 0.0:
                raise ValueError(f"{name} must be finite and positive")


@dataclass(frozen=True, slots=True)
class CausalSpxMinute:
    minute: datetime
    observed_at: datetime
    source_at: datetime
    transport_at: datetime
    price: float


def build_intraday_extreme_ranges(
    *,
    options: Mapping[str, object],
    spx_minutes: Mapping[str, object],
    session_day: date,
    now: datetime,
    freshness_policy: MarketRegimeFreshnessPolicy,
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
        or option_age_seconds < 0.0
        or option_age_seconds > freshness_policy.live_input_max_age_seconds
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
        freshness_policy=freshness_policy,
    )
    if path is None:
        unavailable = _unavailable("fresh_spx_intraday_path_unavailable", target_at)
        return unavailable, unavailable
    current, observed_high, observed_low, source_at, available_at = path
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
        "as_of": source_at.isoformat(),
        "available_at": available_at.isoformat(),
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
    freshness_policy: MarketRegimeFreshnessPolicy,
) -> tuple[float, float, float, datetime, datetime] | None:
    candidates = causal_spx_session_minutes(
        spx_minutes,
        session_day=session_day,
        now=now,
    )
    if not candidates:
        return None
    latest = candidates[-1]
    source_age_seconds = (now - latest.source_at).total_seconds()
    transport_age_seconds = (now - latest.transport_at).total_seconds()
    max_age = freshness_policy.standardized_spx_minute_max_age_seconds
    if not all(
        -FUTURE_TOLERANCE_SECONDS <= age <= max_age
        for age in (source_age_seconds, transport_age_seconds)
    ):
        return None
    prices = [sample.price for sample in candidates]
    return (
        latest.price,
        max(prices),
        min(prices),
        latest.source_at,
        latest.observed_at,
    )


def causal_spx_session_minutes(
    spx_minutes: Mapping[str, object],
    *,
    session_day: date,
    now: datetime,
) -> tuple[CausalSpxMinute, ...]:
    session = DEFAULT_MARKET_CALENDAR.session(session_day)
    rows = spx_minutes.get("rows")
    if session is None or not isinstance(rows, list):
        return ()
    open_at = session.open_at.astimezone(UTC)
    close_at = session.close_at.astimezone(UTC)
    provider_clock_through = now + timedelta(seconds=FUTURE_TOLERANCE_SECONDS)
    candidates: list[CausalSpxMinute] = []
    for value in rows:
        row = _mapping(value)
        selected = _mapping(row.get("selected"))
        if row.get("session_date") != session_day.isoformat():
            continue
        minute = _parse_at(row.get("minute"))
        observed_at = _parse_at(row.get("observed_at"))
        source_at = _parse_at(selected.get("source_at"))
        transport_at = _parse_at(selected.get("transport_at"))
        price = _selected_price(row)
        if (
            minute is None
            or observed_at is None
            or source_at is None
            or transport_at is None
            or price is None
        ):
            continue
        if not all(open_at <= at < close_at for at in (minute, source_at, transport_at)):
            continue
        if minute > now or observed_at > now or transport_at > now:
            continue
        if transport_at > observed_at or source_at > provider_clock_through:
            continue
        if source_at > observed_at + timedelta(seconds=FUTURE_TOLERANCE_SECONDS):
            continue
        candidates.append(
            CausalSpxMinute(
                minute=minute,
                observed_at=observed_at,
                source_at=source_at,
                transport_at=transport_at,
                price=price,
            )
        )
    return tuple(sorted(candidates, key=lambda item: (item.minute, item.observed_at)))


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


__all__ = [
    "CausalSpxMinute",
    "MarketRegimeFreshnessPolicy",
    "build_intraday_extreme_ranges",
    "causal_spx_session_minutes",
]
