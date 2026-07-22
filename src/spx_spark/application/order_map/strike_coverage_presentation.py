"""Deterministic presentation of live SPXW strike-price coverage."""

from __future__ import annotations

from typing import Any


def _dash(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.1f}".removesuffix(".0")
    return str(value)


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
    return (
        f"价格覆盖  ATM上下各{radius}档 {complete}/{target}对　"
        f"±{points}点 {point_complete}/{point_target}对　"
        f"双边C/P区间 {complete_range}"
    )
