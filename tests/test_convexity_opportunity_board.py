from __future__ import annotations

from typing import Any

from spx_spark.application.order_map.convexity_opportunity_board import (
    build_dense_opportunity_board,
)


def _board(
    *,
    market_state: dict[str, Any] | None = None,
    active_event: dict[str, Any] | None = None,
    quality_status: str = "ready",
) -> dict[str, Any]:
    return build_dense_opportunity_board(
        mandate={"phase": "rth_active"},
        market_state=market_state
        or {
            "D": 8,
            "efficiency_ratio": 0.70,
            "vwap_cross_count": 0,
            "same_time_range_ratio": 1.30,
            "breadth_above_vwap": 0.70,
            "moving_averages": {},
            "rolling_path_percentiles": {
                "status": "provisional",
                "confidence": "medium",
                "sample_count": 12,
                "signed_path_bias": 0.30,
            },
        },
        boundary_tests={"active_event": active_event or {}},
        option_evidence={
            "call": {"edge_status": "not_observed"},
            "put": {"edge_status": "not_observed"},
        },
        volatility_context={},
        data_quality={
            "status": quality_status,
            "reasons": (
                ["complete_cp_pairs_unavailable"]
                if quality_status == "degraded"
                else []
            ),
        },
        hypotheses=[
            {
                "scenario": "lower_rejection_call",
                "status": "available",
                "direction": "up",
                "option_right": "C",
                "boundary_name": "flip_low",
            },
            {
                "scenario": "upper_acceptance_call",
                "status": "available",
                "direction": "up",
                "option_right": "C",
                "boundary_name": "flip_high",
            },
            {
                "scenario": "lower_acceptance_put",
                "status": "available",
                "direction": "down",
                "option_right": "P",
                "boundary_name": "put_wall",
            },
            {
                "scenario": "upper_rejection_put",
                "status": "available",
                "direction": "down",
                "option_right": "P",
                "boundary_name": "call_wall",
            },
        ],
    )


def test_put_wall_breakdown_is_explicitly_disabled_and_cannot_rank_high() -> None:
    board = _board(
        market_state={
            "D": -10,
            "efficiency_ratio": 0.80,
            "vwap_cross_count": 0,
            "same_time_range_ratio": 1.50,
            "breadth_above_vwap": 0.20,
            "moving_averages": {},
            "rolling_path_percentiles": {
                "confidence": "medium",
                "sample_count": 12,
                "signed_path_bias": -0.50,
            },
        },
        active_event={
            "phase": "CONFIRMED",
            "direction": "down",
            "level_kind": "put_wall",
            "formal_signal": True,
            "quality_ok": True,
        },
    )

    put = board["lanes"]["put"]
    assert put["wall_signal"] == "DISABLED_UNSUPPORTED:put_wall"
    assert put["priority"] == "WATCH"
    assert put["priority_score"] <= 2
    assert put["structure_rank"] == ["put_wall_breakdown_disabled"]
    assert all(
        path["scenario"] != "lower_acceptance_put"
        for path in put["trigger_paths"]
    )
    assert all(
        contribution["feature"] != "wall_lifecycle"
        for contribution in put["score_contributions"]
    )
    assert put["execution"]["eligible"] is False


def test_expired_put_wall_event_does_not_hide_upper_rejection_put_watch() -> None:
    board = _board(
        active_event={
            "phase": "EXPIRED",
            "direction": "down",
            "level_kind": "put_wall",
            "formal_signal": False,
            "quality_ok": True,
        },
    )

    put = board["lanes"]["put"]
    assert put["wall_signal"] == "WATCH:upper_rejection_put"
    assert put["structure_rank"] != ["put_wall_breakdown_disabled"]
    assert [path["scenario"] for path in put["trigger_paths"]] == [
        "upper_rejection_put"
    ]


def test_degraded_data_caps_priority_and_exposes_the_quality_reason() -> None:
    board = _board(quality_status="degraded")

    for lane in board["lanes"].values():
        assert lane["priority_score"] <= 2
        assert lane["data_quality_status"] == "degraded"
        assert lane["execution"]["eligible"] is False
        assert "complete_cp_pairs_unavailable" in lane["execution"]["block_reasons"]


def test_missing_direction_cannot_masquerade_as_compression() -> None:
    board = _board(
        market_state={
            "D": None,
            "efficiency_ratio": 0.10,
            "vwap_cross_count": 4,
            "same_time_range_ratio": 0.60,
            "breadth_above_vwap": None,
            "moving_averages": {},
            "rolling_path_percentiles": {},
        }
    )

    vol = board["lanes"]["vol_range"]
    assert vol["volatility_signal"] == "MIXED_OR_UNCALIBRATED"
    assert vol["priority"] == "WATCH"


def test_directional_structure_order_is_symmetric() -> None:
    board = _board()

    assert board["lanes"]["call"]["structure_rank"] == [
        "long_call_watch",
        "call_debit_spread_watch",
    ]
    assert board["lanes"]["put"]["structure_rank"] == [
        "put_debit_spread_watch",
        "long_put_watch",
    ]


def test_closed_board_clears_stale_wall_skew_and_structure_evidence() -> None:
    board = build_dense_opportunity_board(
        mandate={"phase": "hard_exit_reached"},
        market_state={
            "D": 10,
            "efficiency_ratio": 0.9,
            "same_time_range_ratio": 1.5,
            "rolling_path_percentiles": {
                "confidence": "high",
                "sample_count": 20,
                "signed_path_bias": 0.8,
            },
        },
        boundary_tests={
            "active_event": {
                "phase": "CONFIRMED",
                "direction": "up",
                "level_kind": "flip_high",
                "formal_signal": True,
                "quality_ok": True,
            }
        },
        option_evidence={
            "call": {
                "edge_status": "observed_local_skew_edge",
                "vertical": {"strategy": "call_debit_spread"},
            },
            "put": {"edge_status": "observed_local_skew_edge"},
        },
        volatility_context={"atm_iv_0dte": 0.2, "atm_iv_1dte": 0.22},
        data_quality={"status": "ready", "reasons": []},
        hypotheses=[
            {
                "scenario": "upper_acceptance_call",
                "status": "available",
                "option_right": "C",
            }
        ],
    )

    assert board["status"] == "closed"
    assert set(board["lanes"]) == {"call", "put", "vol_range"}
    assert set(board["rank_order"]) == {"call", "put", "vol_range"}
    for lane in board["lanes"].values():
        assert lane["status"] == "closed"
        assert lane["priority_score"] == 0
        assert lane["edge_status"] == "not_evaluated"
        assert lane["structure_rank"] == []
        assert lane["execution"]["eligible"] is False
        assert "strategy_window_inactive" in lane["execution"]["block_reasons"]
    assert board["lanes"]["call"]["wall_signal"] == "CLOSED"
    assert board["lanes"]["put"]["wall_signal"] == "CLOSED"
    assert board["lanes"]["vol_range"]["volatility_signal"] == "CLOSED"
