"""Strike-pair coverage diagnostics for Spring Gamma option overlays."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence

from spx_spark.analytics.options.pricing import finite_float

STRUCTURAL_STRIKE_WINDOW = 61
MINIMUM_CORE_PAIR_COUNT = 13
DENSE_PAIR_RATIO = 0.80


def strike_coverage(
    expiry: Mapping[str, object],
    exposures: Mapping[str, object],
) -> dict[str, object]:
    available_rows = [
        row
        for item in _items(expiry.get("strikes"))
        if (row := _mapping(item)) and finite_float(row.get("strike")) is not None
    ]
    spot = finite_float(_child(exposures, "underlier").get("price"))
    rows = available_rows
    if spot is not None and len(rows) > STRUCTURAL_STRIKE_WINDOW:
        rows = sorted(
            rows,
            key=lambda row: (
                abs(float(row["strike"]) - spot),
                float(row["strike"]),
            ),
        )[:STRUCTURAL_STRIKE_WINDOW]
    rows = sorted(rows, key=lambda row: float(row["strike"]))
    total, legs = len(rows), len(rows) * 2
    complete = [_complete_pair(row) for row in rows]
    paired_strikes = sum(complete)
    core: list[Mapping[str, object]] = []
    left: list[Mapping[str, object]] = []
    right: list[Mapping[str, object]] = []
    if spot is not None and rows:
        core_size = max(1, math.ceil(total / 3))
        core_strikes = {
            float(row["strike"])
            for row in sorted(rows, key=lambda row: abs(float(row["strike"]) - spot))[:core_size]
        }
        core = [row for row in rows if float(row["strike"]) in core_strikes]
        left = [row for row in rows if float(row["strike"]) < spot]
        right = [row for row in rows if float(row["strike"]) > spot]

    def leg_ratio(metric: str) -> float | None:
        return _ratio(
            sum(_leg_valid(row, side, metric) for row in rows for side in ("call", "put")),
            legs,
        )

    computed_iv, computed_delta = leg_ratio("iv"), leg_ratio("delta")
    reported_iv = finite_float(expiry.get("iv_coverage_ratio"))
    reported_delta = finite_float(expiry.get("delta_coverage_ratio"))
    freshness = _child(expiry, "freshness")
    leg_rejections = _mapping(freshness.get("rejection_counts"))
    return {
        "available_strike_count": len(available_rows),
        "strike_count": total,
        "complete_pair_ratio": _ratio(paired_strikes, total),
        "paired_strikes": paired_strikes,
        "core_strike_count": len(core),
        "core_complete_pair_ratio": _pair_ratio(core),
        "left_wing_strike_count": len(left),
        "left_wing_paired_strikes": sum(_complete_pair(row) for row in left),
        "left_wing_complete_pair_ratio": _pair_ratio(left),
        "right_wing_strike_count": len(right),
        "right_wing_paired_strikes": sum(_complete_pair(row) for row in right),
        "right_wing_complete_pair_ratio": _pair_ratio(right),
        "iv_coverage_ratio": _round(computed_iv),
        "delta_coverage_ratio": _round(computed_delta),
        "greek_coverage_ratio": leg_ratio("greeks"),
        "nonzero_oi_leg_ratio": leg_ratio("nonzero_oi"),
        "reported_iv_coverage_ratio": reported_iv,
        "reported_delta_coverage_ratio": reported_delta,
        "underlier": spot,
        "partition_method": ("nearest_61_strikes_then_one_third_core_with_spot_sided_wings"),
        "structural_strike_window": STRUCTURAL_STRIKE_WINDOW,
        "density_state": _density_state(paired_strikes),
        "density_target_pair_count": STRUCTURAL_STRIKE_WINDOW,
        "density_complete_pair_ratio": _ratio(
            paired_strikes,
            STRUCTURAL_STRIKE_WINDOW,
        ),
        "density_minimum_core_pair_count": MINIMUM_CORE_PAIR_COUNT,
        "leg_rejection_reasons": dict(leg_rejections),
        "freshness": dict(freshness),
        "missing_values_are_zero": False,
        "nbbo_interpolated": False,
    }


def _complete_pair(row: Mapping[str, object]) -> bool:
    return all(
        _leg_valid(row, side, metric)
        for side in ("call", "put")
        for metric in ("iv", "delta", "greeks")
    )


def _leg_valid(row: Mapping[str, object], side: str, metric: str) -> bool:
    if metric != "nonzero_oi" and not _leg_analytical_allowed(row, side):
        return False
    if metric == "iv":
        return (finite_float(row.get(f"{side}_iv")) or 0.0) > 0
    if metric == "delta":
        value = finite_float(row.get(f"{side}_delta"))
        return value is not None and -1.0 <= value <= 1.0
    if metric == "nonzero_oi":
        return (finite_float(row.get(f"{side}_open_interest")) or 0.0) > 0
    gamma = finite_float(row.get(f"{side}_gamma"))
    return (
        metric == "greeks"
        and gamma is not None
        and gamma >= 0
        and finite_float(row.get(f"{side}_vanna_per_vol_point")) is not None
        and finite_float(row.get(f"{side}_charm_per_minute")) is not None
    )


def _leg_analytical_allowed(row: Mapping[str, object], side: str) -> bool:
    if "leg_metadata" not in row:
        # Legacy archives did not persist per-leg analytical decisions.
        return True
    metadata = _mapping(row.get("leg_metadata"))
    leg = _mapping(metadata.get(side))
    return leg.get("analytical_allowed") is True


def _density_state(paired_strikes: int) -> str:
    if paired_strikes <= 0:
        return "missing"
    if paired_strikes >= STRUCTURAL_STRIKE_WINDOW:
        return "full_61"
    if paired_strikes / STRUCTURAL_STRIKE_WINDOW >= DENSE_PAIR_RATIO:
        return "dense"
    if paired_strikes >= MINIMUM_CORE_PAIR_COUNT:
        return "core_covered"
    return "sparse"


def _mapping(value: object) -> Mapping[str, object]:
    if isinstance(value, Mapping):
        return value
    to_dict = getattr(value, "to_dict", None)
    payload = to_dict() if callable(to_dict) else None
    return payload if isinstance(payload, Mapping) else {}


def _child(parent: Mapping[str, object], key: str) -> Mapping[str, object]:
    return _mapping(parent.get(key))


def _items(value: object) -> Sequence[object]:
    return value if isinstance(value, (list, tuple)) else ()


def _round(value: object) -> float | None:
    number = finite_float(value)
    return round(number, 6) if number is not None else None


def _ratio(numerator: int, denominator: int) -> float | None:
    return _round(numerator / denominator) if denominator else None


def _pair_ratio(rows: Sequence[Mapping[str, object]]) -> float | None:
    return _ratio(sum(_complete_pair(row) for row in rows), len(rows))
