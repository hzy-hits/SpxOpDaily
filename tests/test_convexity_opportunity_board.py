from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from spx_spark.application.order_map.convexity_gth_modifier_gate import (
    build_ranked_active_event,
    gth_skew_rank_gate_reasons,
)
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
            "reasons": (["complete_cp_pairs_unavailable"] if quality_status == "degraded" else []),
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


def _gth_path_board(path_ranks: dict[str, Any]) -> dict[str, Any]:
    return build_dense_opportunity_board(
        mandate={"phase": "gth_preparation"},
        market_state={
            "D": None,
            "moving_averages": {},
            "rolling_path_percentiles": {},
            "gth_observation": {
                "status": "ready",
                "trend": {"status": "unavailable"},
                "path_ranks": path_ranks,
                "dip_reclaim_call": {"status": "unavailable"},
                "manual_candidate": {"status": "blocked"},
            },
        },
        boundary_tests={"active_event": {}},
        option_evidence={
            "call": {"edge_status": "unknown"},
            "put": {"edge_status": "unknown"},
        },
        volatility_context={},
        data_quality={"status": "degraded", "reasons": ["option_overlay_unavailable"]},
        hypotheses=[],
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
    assert all(path["scenario"] != "lower_acceptance_put" for path in put["trigger_paths"])
    assert all(
        contribution["feature"] != "wall_lifecycle" for contribution in put["score_contributions"]
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
    assert [path["scenario"] for path in put["trigger_paths"]] == ["upper_rejection_put"]


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


def test_gth_live_es_direction_survives_optional_option_data_gaps() -> None:
    board = build_dense_opportunity_board(
        mandate={"phase": "gth_preparation"},
        market_state={
            "D": None,
            "efficiency_ratio": None,
            "breadth_above_vwap": None,
            "moving_averages": {},
            "rolling_path_percentiles": {},
            "gth_observation": {
                "status": "ready",
                "phase": "gth_preparation",
                "trend": {
                    "status": "ready",
                    "regime": "bullish",
                    "return_15m_points": 6.0,
                    "return_60m_points": 18.0,
                    "return_180m_points": None,
                },
                "dip_reclaim_call": {"status": "unavailable"},
                "manual_candidate": {"status": "blocked"},
            },
        },
        boundary_tests={"active_event": {}},
        option_evidence={
            "call": {"edge_status": "unknown"},
            "put": {"edge_status": "unknown"},
        },
        volatility_context={},
        data_quality={
            "status": "degraded",
            "reasons": [
                "destination:unavailable",
                "option_overlay_unavailable",
            ],
        },
        hypotheses=[],
    )

    call = board["lanes"]["call"]
    put = board["lanes"]["put"]
    assert call["status"] == "observed"
    assert put["status"] == "observed"
    assert call["priority"] == "MEDIUM"
    assert call["priority_score"] == 4
    assert call["gth_signal"] == "TREND_BULLISH"
    assert put["gth_signal"] == "COUNTER_TREND"
    assert any(
        row["feature"] == "optional_option_data_degraded_gth_rank_retained"
        for row in call["score_contributions"]
    )
    for lane in (call, put):
        assert lane["execution"]["eligible"] is False
        assert lane["action_authority"] == "none"
        assert lane["automatic_ordering"] is False


def test_gth_bearish_trend_keeps_call_and_put_but_ranks_put_higher() -> None:
    board = build_dense_opportunity_board(
        mandate={"phase": "gth_preparation"},
        market_state={
            "D": None,
            "moving_averages": {},
            "rolling_path_percentiles": {},
            "gth_observation": {
                "status": "ready",
                "phase": "gth_preparation",
                "trend": {
                    "status": "ready",
                    "regime": "bearish",
                    "return_15m_points": -7.0,
                    "return_60m_points": -21.0,
                    "return_180m_points": 3.0,
                },
                "dip_reclaim_call": {
                    "status": "active",
                    "entry_quality_verdict": "blocked",
                },
                "manual_candidate": {"status": "blocked"},
            },
        },
        boundary_tests={"active_event": {}},
        option_evidence={
            "call": {"edge_status": "not_observed"},
            "put": {"edge_status": "not_observed"},
        },
        volatility_context={},
        data_quality={"status": "ready", "reasons": []},
        hypotheses=[],
    )

    call = board["lanes"]["call"]
    put = board["lanes"]["put"]
    assert set(board["lanes"]) == {"call", "put", "vol_range"}
    assert call["status"] == put["status"] == "observed"
    assert "DIP_RECLAIM_OBSERVATION" in call["gth_signal"]
    assert "COUNTER_TREND" in call["gth_signal"]
    assert put["gth_signal"] == "TREND_BEARISH"
    assert put["priority"] == "MEDIUM"
    assert put["priority_score"] > call["priority_score"]
    assert put["execution"]["eligible"] is False


def test_gth_causal_path_ranks_add_two_sided_modifiers_without_execution() -> None:
    board = build_dense_opportunity_board(
        mandate={"phase": "gth_preparation"},
        market_state={
            "D": None,
            "moving_averages": {},
            "rolling_path_percentiles": {},
            "gth_observation": {
                "status": "ready",
                "trend": {"status": "unavailable"},
                "path_ranks": {
                    "status": "ready",
                    "rank_is_probability": False,
                    "horizons": {
                        "15m": {
                            "ready": True,
                            "decision_usable": True,
                            "position_percentile": 25.0,
                            "drawdown_rank_percentile": 80.0,
                            "recovery_rank_percentile": 60.0,
                            "rally_rank_percentile": 30.0,
                            "pullback_rank_percentile": 20.0,
                            "effective_reference_windows": 5,
                        },
                        "60m": {
                            "ready": True,
                            "decision_usable": True,
                            "position_percentile": 75.0,
                            "drawdown_rank_percentile": 30.0,
                            "recovery_rank_percentile": 20.0,
                            "rally_rank_percentile": 90.0,
                            "pullback_rank_percentile": 70.0,
                            "effective_reference_windows": 5,
                        },
                    },
                },
                "dip_reclaim_call": {"status": "unavailable"},
                "manual_candidate": {"status": "blocked"},
            },
        },
        boundary_tests={"active_event": {}},
        option_evidence={
            "call": {"edge_status": "unknown"},
            "put": {"edge_status": "unknown"},
        },
        volatility_context={},
        data_quality={
            "status": "degraded",
            "reasons": ["option_overlay_unavailable"],
        },
        hypotheses=[],
    )

    call = board["lanes"]["call"]
    put = board["lanes"]["put"]
    assert "PATH_LOW" in call["gth_signal"]
    assert "DIP_RECOVERY_RANK" in call["gth_signal"]
    assert "PATH_HIGH" in put["gth_signal"]
    assert "RALLY_PULLBACK_RANK" in put["gth_signal"]
    assert {
        "gth_low_position_rank",
        "gth_dip_recovery_rank",
    } <= {row["feature"] for row in call["score_contributions"]}
    assert {
        "gth_high_position_rank",
        "gth_rally_pullback_rank",
    } <= {row["feature"] for row in put["score_contributions"]}
    assert call["priority_score"] == put["priority_score"] == 2
    for lane in (call, put):
        assert lane["status"] == "observed"
        assert lane["execution"]["eligible"] is False
        assert lane["action_authority"] == "none"
        assert lane["automatic_ordering"] is False


def test_gth_sparse_path_rank_stays_visible_without_direction_points() -> None:
    board = build_dense_opportunity_board(
        mandate={"phase": "gth_preparation"},
        market_state={
            "D": None,
            "moving_averages": {},
            "rolling_path_percentiles": {},
            "gth_observation": {
                "status": "ready",
                "trend": {"status": "unavailable"},
                "path_ranks": {
                    "status": "ready",
                    "horizons": {
                        "15m": {
                            "ready": True,
                            "decision_usable": False,
                            "sampling_quality": "usable_sparse",
                            "position_percentile": 0.0,
                            "drawdown_rank_percentile": 100.0,
                            "recovery_rank_percentile": 100.0,
                            "rally_rank_percentile": 100.0,
                            "pullback_rank_percentile": 100.0,
                        }
                    },
                },
                "dip_reclaim_call": {"status": "unavailable"},
                "manual_candidate": {"status": "blocked"},
            },
        },
        boundary_tests={"active_event": {}},
        option_evidence={
            "call": {"edge_status": "unknown"},
            "put": {"edge_status": "unknown"},
        },
        volatility_context={},
        data_quality={"status": "degraded", "reasons": ["option_overlay_unavailable"]},
        hypotheses=[],
    )

    for side in ("call", "put"):
        lane = board["lanes"][side]
        assert lane["status"] == "observed"
        assert lane["priority_score"] == 0
        assert lane["gth_signal"] == "WATCH"
        assert "degraded_data_priority_cap" in {
            row["feature"] for row in lane["score_contributions"]
        }
        assert not any(
            str(row["feature"]).startswith("gth_")
            and row["feature"] != "gth_stale_structure_modifiers_blocked"
            for row in lane["score_contributions"]
        )


def test_gth_unavailable_path_context_cannot_leak_ready_row_into_rank() -> None:
    board = _gth_path_board(
        {
            "status": "unavailable",
            "horizons": {
                "15m": {
                    "ready": True,
                    "decision_usable": True,
                    "position_percentile": 0.0,
                    "drawdown_rank_percentile": 100.0,
                    "recovery_rank_percentile": 100.0,
                }
            },
        }
    )

    for side in ("call", "put"):
        lane = board["lanes"][side]
        assert lane["priority_score"] == 0
        assert lane["gth_signal"] == "WATCH"
        assert "degraded_data_priority_cap" in {
            row["feature"] for row in lane["score_contributions"]
        }


def test_gth_small_sample_shape_rank_does_not_add_direction_points() -> None:
    board = _gth_path_board(
        {
            "status": "ready",
            "horizons": {
                "15m": {
                    "ready": True,
                    "decision_usable": True,
                    "position_percentile": 50.0,
                    "drawdown_rank_percentile": 100.0,
                    "recovery_rank_percentile": 100.0,
                    "rally_rank_percentile": 100.0,
                    "pullback_rank_percentile": 100.0,
                    "effective_reference_windows": 1,
                }
            },
        }
    )

    for side in ("call", "put"):
        lane = board["lanes"][side]
        assert lane["priority_score"] == 0
        assert lane["gth_signal"] == "WATCH"
        assert not any(
            row["feature"] in {"gth_dip_recovery_rank", "gth_rally_pullback_rank"}
            for row in lane["score_contributions"]
        )


def test_stale_gth_wall_and_skew_cannot_elevate_fresh_direct_es_rank() -> None:
    now = datetime(2026, 7, 30, 5, 0, tzinfo=timezone.utc)
    mandate = {"phase": "gth_preparation", "trading_date": "2026-07-30"}
    event = build_ranked_active_event(
        {
            "level_decision": {
                "event_id": "old-wall",
                "phase": "CONFIRMED",
                "direction": "up",
                "level_kind": "flip_high",
                "formal_signal": True,
                "quality_ok": True,
                "expiry": "20260729",
                "session_date": "2026-07-29",
                "session_mode": "gth",
                "expires_at": (now - timedelta(seconds=1)).isoformat(),
            }
        },
        mandate=mandate,
        now=now,
    )
    stale_candidate = {
        "long": {"expiry": "20260729"},
        "short": {"expiry": "20260729"},
    }
    skew_reasons = gth_skew_rank_gate_reasons(
        {
            "expiry": "20260729",
            "as_of": (now - timedelta(minutes=10)).isoformat(),
        },
        candidate=stale_candidate,
        mandate=mandate,
        now=now,
    )

    board = build_dense_opportunity_board(
        mandate=mandate,
        market_state={
            "D": None,
            "moving_averages": {},
            "rolling_path_percentiles": {},
            "gth_observation": {
                "status": "ready",
                "trend": {
                    "status": "ready",
                    "regime": "bullish",
                    "return_15m_points": 5.0,
                    "return_60m_points": 12.0,
                    "return_180m_points": None,
                },
                "dip_reclaim_call": {"status": "unavailable"},
                "manual_candidate": {"status": "blocked"},
            },
        },
        boundary_tests={"active_event": event},
        option_evidence={
            "call": {
                "edge_status": "observed_local_skew_edge",
                "rank_eligible": False,
                "rank_gate_reasons": skew_reasons,
                "vertical": {"strategy": "call_debit_spread"},
            },
            "put": {"edge_status": "unknown", "rank_eligible": False},
        },
        volatility_context={},
        data_quality={"status": "degraded", "reasons": ["option_overlay_stale"]},
        hypotheses=[],
    )

    call = board["lanes"]["call"]
    assert event["rank_eligible"] is False
    assert "gth_active_event_expired" in event["rank_gate_reasons"]
    assert "gth_skew_stale_or_future" in skew_reasons
    assert call["priority"] == "MEDIUM"
    assert call["priority_score"] == 4
    assert call["wall_signal"] == "UNAVAILABLE"
    assert call["edge_status"] == "unknown"
    assert not {
        "wall_lifecycle",
        "side_level_skew_context",
    } & {row["feature"] for row in call["score_contributions"]}


def test_current_gth_wall_and_skew_remain_optional_positive_modifiers() -> None:
    now = datetime(2026, 7, 30, 5, 0, tzinfo=timezone.utc)
    mandate = {"phase": "gth_preparation", "trading_date": "2026-07-30"}
    event = build_ranked_active_event(
        {
            "level_decision": {
                "event_id": "current-wall",
                "phase": "CONFIRMED",
                "direction": "up",
                "level_kind": "flip_high",
                "formal_signal": True,
                "quality_ok": True,
                "expiry": "20260730",
                "session_date": "2026-07-30",
                "session_mode": "gth",
                "expires_at": (now + timedelta(minutes=2)).isoformat(),
            }
        },
        mandate=mandate,
        now=now,
    )
    candidate = {
        "long": {"expiry": "20260730"},
        "short": {"expiry": "20260730"},
    }
    skew_reasons = gth_skew_rank_gate_reasons(
        {"expiry": "20260730", "as_of": (now - timedelta(seconds=10)).isoformat()},
        candidate=candidate,
        mandate=mandate,
        now=now,
    )
    board = build_dense_opportunity_board(
        mandate=mandate,
        market_state={
            "D": None,
            "moving_averages": {},
            "rolling_path_percentiles": {},
            "gth_observation": {
                "status": "ready",
                "trend": {
                    "status": "ready",
                    "regime": "bullish",
                    "return_15m_points": 5.0,
                    "return_60m_points": 12.0,
                    "return_180m_points": None,
                },
                "dip_reclaim_call": {"status": "unavailable"},
                "manual_candidate": {"status": "blocked"},
            },
        },
        boundary_tests={"active_event": event},
        option_evidence={
            "call": {
                "edge_status": "observed_local_skew_edge",
                "rank_eligible": not skew_reasons,
                "rank_gate_reasons": skew_reasons,
                "vertical": {"strategy": "call_debit_spread"},
            },
            "put": {"edge_status": "unknown", "rank_eligible": False},
        },
        volatility_context={},
        data_quality={"status": "ready", "reasons": []},
        hypotheses=[],
    )

    call = board["lanes"]["call"]
    assert event["rank_eligible"] is True
    assert skew_reasons == []
    assert call["priority"] == "HIGH"
    assert call["priority_score"] == 8
    assert {"wall_lifecycle", "side_level_skew_context"} <= {
        row["feature"] for row in call["score_contributions"]
    }
