"""Deterministic volatility inputs for discretionary convexity analysis."""

from __future__ import annotations

from typing import Any, Mapping

from spx_spark.analytics.options.pricing import finite_float


def build_volatility_context(
    payload: Mapping[str, Any],
    *,
    market_state: Mapping[str, Any],
) -> dict[str, Any]:
    frame = _mapping(payload.get("option_structure_frame"))
    volatility = _mapping(frame.get("volatility"))
    indices = _mapping(payload.get("vol_context"))
    values = {
        key: finite_float(volatility.get(key))
        for key in (
            "atm_iv_0dte",
            "atm_iv_1dte",
            "atm_iv_change_5m",
            "atm_iv_change_15m",
            "atm_iv_change_60m",
            "expected_move_points_0dte",
            "expected_move_points_1dte",
            "put_skew_25d_0dte",
            "call_skew_25d_0dte",
            "put_skew_25d_1dte",
            "call_skew_25d_1dte",
            "term_gap",
        )
    }
    values.update(
        {
            "vix1d": finite_float(indices.get("vix1d")),
            "vix": finite_float(indices.get("vix")),
            "realized_range_points": finite_float(
                market_state.get("current_range_points")
            ),
            "same_time_median_range_points": finite_float(
                market_state.get("same_time_median_range_points")
            ),
            "same_time_range_ratio": finite_float(
                market_state.get("same_time_range_ratio")
            ),
        }
    )
    return {
        "status": (
            "available"
            if any(value is not None for value in values.values())
            else "unavailable"
        ),
        **values,
        "iv_minus_realized_premium": {
            "status": "unavailable",
            "reason": "same_unit_remaining_to_1300_realized_variance_baseline_not_calibrated",
        },
        "remaining_implied_move_to_1300": {
            "status": "unavailable",
            "reason": "requires_causal_intraday_variance_scaling",
        },
        "semantics": (
            "greeks_and_implied_vol_describe_option_sensitivity_and_market_pricing_not_direction"
        ),
        "action_authority": "none",
    }


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
