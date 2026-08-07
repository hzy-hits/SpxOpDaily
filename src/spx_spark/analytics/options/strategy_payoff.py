"""Deterministic payoff and conservative BBO math for strategy candidates."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any


def conservative_vertical_bbo(
    long_leg: Mapping[str, Any], short_leg: Mapping[str, Any], *, now: datetime,
    max_quote_age_seconds: float = 15.0, max_source_skew_seconds: float = 2.0,
) -> dict[str, Any]:
    long_bid, long_ask = _nonnegative(long_leg.get("bid")), _positive(long_leg.get("ask"))
    short_bid, short_ask = _nonnegative(short_leg.get("bid")), _positive(short_leg.get("ask"))
    long_at, short_at = _time(long_leg.get("source_at")), _time(short_leg.get("source_at"))
    provider, reasons = str(long_leg.get("provider") or ""), []
    if not provider or provider != str(short_leg.get("provider") or ""):
        reasons.append("spread_leg_provider_mismatch")
    if None in (long_bid, long_ask, short_bid, short_ask):
        reasons.append("spread_leg_nbbo_invalid")
    if long_at is None or short_at is None:
        reasons.append("spread_leg_source_time_missing")
    else:
        now_utc = _utc(now)
        ages = ((now_utc - long_at).total_seconds(), (now_utc - short_at).total_seconds())
        if any(age < 0 for age in ages):
            reasons.append("spread_leg_quote_from_future")
        if any(age > max_quote_age_seconds for age in ages):
            reasons.append("spread_leg_quote_stale")
        if abs((long_at - short_at).total_seconds()) > max_source_skew_seconds:
            reasons.append("spread_leg_time_skew_exceeded")
    if reasons:
        return {"status": "unavailable", "reasons": list(dict.fromkeys(reasons))}
    assert None not in (long_bid, long_ask, short_bid, short_ask, long_at, short_at)
    net_ask, net_bid = float(long_ask) - float(short_bid), max(float(long_bid) - float(short_ask), 0.0)
    if net_ask <= 0 or net_bid > net_ask:
        return {"status": "unavailable", "reasons": ["synthetic_vertical_bbo_invalid"]}
    assert long_at is not None and short_at is not None
    return {
        "status": "ready", "bid": round(net_bid, 4), "ask": round(net_ask, 4),
        "provider": provider, "long_source_at": long_at.isoformat(),
        "short_source_at": short_at.isoformat(),
        "source_skew_seconds": round(abs((long_at - short_at).total_seconds()), 3),
        "max_quote_age_seconds": round(max((_utc(now) - long_at).total_seconds(),
                                           (_utc(now) - short_at).total_seconds()), 3),
        "reasons": [],
    }


def vertical_economics(
    *, long_strike: float, short_strike: float, net_debit: float, right: str
) -> dict[str, float]:
    direction = 1.0 if right.upper() == "C" else -1.0 if right.upper() == "P" else 0.0
    width = (short_strike - long_strike) * direction
    if direction == 0 or width <= 0:
        raise ValueError("vertical strikes must define a debit spread in right direction")
    if not 0 < net_debit < width:
        raise ValueError("vertical debit must be positive and below width")
    return {
        "width_points": width, "max_loss_points": net_debit,
        "max_gain_points": width - net_debit,
        "breakeven_spx": long_strike + direction * net_debit,
        "debit_fraction_of_width": net_debit / width,
    }


def vertical_payoff(
    settlement: float, *, long_strike: float, short_strike: float, net_debit: float, right: str
) -> float:
    if right.upper() == "C":
        intrinsic = max(settlement - long_strike, 0.0) - max(settlement - short_strike, 0.0)
    elif right.upper() == "P":
        intrinsic = max(long_strike - settlement, 0.0) - max(short_strike - settlement, 0.0)
    else:
        raise ValueError("right must be C or P")
    return intrinsic - net_debit


def vertical_entry_quality(
    *, spot: float, atr: float, target: float, stop: float, trigger: float | None,
    direction: str, setup_kind: str, distance_to_vwap_points: float | None,
    impulse_15m_points: float | None, debit_fraction: float,
    thresholds: Mapping[str, float],
) -> tuple[dict[str, float], list[str]]:
    if atr <= 0:
        return {}, ["entry_quality_atr_or_geometry_unavailable"]
    if (direction == "UP" and not stop < spot < target) or (direction == "DOWN" and not target < spot < stop):
        return {}, ["entry_geometry_not_ordered"]
    target_distance, stop_distance = abs(target - spot), abs(spot - stop)
    ratio = target_distance / stop_distance if stop_distance else 0.0
    distance_atr = abs(distance_to_vwap_points or 0.0) / atr
    impulse_atr, stop_atr = abs(impulse_15m_points or 0.0) / atr, stop_distance / atr
    progress = max(0.0, (spot - trigger) / (target - trigger)) if trigger is not None and target != trigger else 0.0
    failed_break = setup_kind == "FAILED_BREAK_RECLAIM"
    min_ratio = thresholds["failed_break_min_target_room_ratio" if failed_break else "min_target_room_ratio"]
    max_debit = thresholds["failed_break_max_debit_fraction" if failed_break else "max_debit_fraction"]
    result = {
        "distance_to_vwap_atr": round(distance_atr, 4), "impulse_15m_atr": round(impulse_atr, 4),
        "target_room_points": round(target_distance, 4), "stop_distance_points": round(stop_distance, 4),
        "target_room_ratio": round(ratio, 4), "debit_fraction_of_width": round(debit_fraction, 4),
        "stop_distance_atr": round(stop_atr, 4), "trigger_target_progress": round(progress, 4),
    }
    late = (
        distance_atr > thresholds["late_chase_distance_atr"]
        and impulse_atr > thresholds["late_chase_impulse_atr"]
    ) or ratio < min_ratio or debit_fraction > max_debit or progress >= 0.60
    if late:
        return result, ["direction_valid_but_entry_too_late"]
    if not thresholds["min_stop_atr"] <= stop_atr <= thresholds["max_stop_atr"]:
        return result, ["stop_distance_outside_atr_band"]
    return result, []


def _positive(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) and value > 0 else None


def _nonnegative(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) and value >= 0 else None


def _time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("quote evaluation time must be timezone-aware")
    return value.astimezone(timezone.utc)
