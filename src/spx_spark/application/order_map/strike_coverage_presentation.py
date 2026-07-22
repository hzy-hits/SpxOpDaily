"""Deterministic presentation of live SPXW strike-price coverage."""

from __future__ import annotations

from typing import Any


def _dash(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.1f}".removesuffix(".0")
    return str(value)


def _two_decimals(value: Any) -> str:
    return f"{float(value):.2f}" if isinstance(value, (int, float)) else "-"


def strike_price_coverage_line(payload: dict[str, Any]) -> str | None:
    value = payload.get("strike_price_coverage")
    if not isinstance(value, dict):
        return None

    target = value.get("target_pair_count")
    complete = value.get("complete_pair_count")
    point_target = value.get("point_target_pair_count")
    point_complete = value.get("point_complete_pair_count")
    counts = (target, complete, point_target, point_complete)
    if not all(isinstance(item, int) for item in counts):
        return None

    complete_range = "-"
    low = value.get("complete_min_strike")
    high = value.get("complete_max_strike")
    if isinstance(low, (int, float)) and isinstance(high, (int, float)):
        complete_range = f"{_dash(low)}–{_dash(high)}"
    radius = int(value.get("radius_strikes") or 30)
    points = int(value.get("radius_points") or 30)
    core = value.get("core_complete_pair_count")
    rotation = value.get("rotation_assisted_pair_count")
    missing_call = value.get("missing_call_count")
    missing_put = value.get("missing_put_count")
    if all(isinstance(item, int) for item in (core, rotation, missing_call, missing_put)):
        age_p50 = _two_decimals(value.get("pair_quote_age_p50_seconds"))
        age_p90 = _two_decimals(value.get("pair_quote_age_p90_seconds"))
        confidence_low = value.get("coverage_confidence_95_low")
        confidence_high = value.get("coverage_confidence_95_high")
        confidence = "-"
        if isinstance(confidence_low, (int, float)) and isinstance(confidence_high, (int, float)):
            confidence = f"{confidence_low * 100:.2f}–{confidence_high * 100:.2f}%"
        return (
            f"价格覆盖  核心{core}对+轮转{rotation}对={complete}/{target}对　"
            f"缺C {missing_call}/缺P {missing_put}　age P50/P90 {age_p50}/{age_p90}s　"
            f"95%CI {confidence}　±{points}点 {point_complete}/{point_target}对　"
            f"双边区间 {complete_range}　NBBO不插值"
        )
    return (
        f"价格覆盖  ATM上下各{radius}档 {complete}/{target}对　"
        f"±{points}点 {point_complete}/{point_target}对　"
        f"双边C/P区间 {complete_range}"
    )
