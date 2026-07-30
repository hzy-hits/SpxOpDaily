from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from spx_spark.application.order_map.convexity_gth_context import (
    build_gth_observation_context,
)


ET = ZoneInfo("America/New_York")
NOW = datetime(2026, 7, 29, 2, 30, tzinfo=ET)


def test_gth_observation_keeps_blocked_dip_and_both_execution_flags_off() -> None:
    payload = {
        "globex_trend": {
            "session_id": "2026-07-29:gth",
            "regime": "bearish",
            "updated_at": (NOW - timedelta(seconds=10)).isoformat(),
            "metrics": {
                "return_15m_points": -6.0,
                "return_60m_points": -18.0,
                "return_180m_points": 4.0,
            },
            "samples": [{"provider": "schwab"}],
        },
        "gth_dip_reclaim_signal": {
            "kind": "gth_dip_reclaim_call",
            "event_id": "gth-dip:test",
            "session_date": "2026-07-29",
            "confirmed_at": (NOW - timedelta(minutes=1)).isoformat(),
            "valid_until": (NOW + timedelta(minutes=9)).isoformat(),
            "drawdown_points": 16.0,
            "entry_quality": {
                "mode": "decision_grade",
                "verdict": "blocked",
                "block_reasons": ["trend_not_bullish"],
            },
        },
        "gth_manual_candidate": {
            "status": "blocked",
            "candidate_id": "gth-manual:test",
            "source_signal_id": "gth-dip:test",
            "evaluated_at": (NOW - timedelta(seconds=5)).isoformat(),
            "block_reasons": ["long_leg_quote_unavailable"],
            "manual_action_eligible": False,
            "automatic_ordering": False,
            "broker_submission_allowed": False,
        },
    }

    context = build_gth_observation_context(
        payload,
        mandate={
            "phase": "gth_preparation",
            "trading_date": "2026-07-29",
        },
        now=NOW,
    )

    assert context["status"] == "ready"
    assert context["trend"]["status"] == "ready"
    assert context["trend"]["regime"] == "bearish"
    assert context["dip_reclaim_call"]["status"] == "active"
    assert context["dip_reclaim_call"]["entry_quality_verdict"] == "blocked"
    assert context["manual_candidate"]["status"] == "blocked"
    assert context["manual_candidate"]["manual_ready_block_reasons"] == [
        "long_leg_quote_unavailable"
    ]
    assert context["action_authority"] == "none"
    assert context["actionable"] is False
    assert context["automatic_ordering"] is False
    assert context["dip_reclaim_call"]["execution_eligible"] is False
    assert context["manual_candidate"]["execution_eligible"] is False


def test_gth_stale_sources_are_unavailable_without_becoming_authority() -> None:
    payload = {
        "globex_trend": {
            "session_id": "2026-07-28:gth",
            "regime": "bullish",
            "updated_at": (NOW - timedelta(minutes=10)).isoformat(),
            "metrics": {"return_15m_points": 8.0},
        },
        "gth_dip_reclaim_signal": {
            "kind": "gth_dip_reclaim_call",
            "event_id": "gth-dip:old",
            "session_date": "2026-07-28",
            "confirmed_at": (NOW - timedelta(minutes=20)).isoformat(),
            "valid_until": (NOW - timedelta(minutes=10)).isoformat(),
        },
    }

    context = build_gth_observation_context(
        payload,
        mandate={
            "phase": "gth_preparation",
            "trading_date": "2026-07-29",
        },
        now=NOW,
    )

    assert context["status"] == "unavailable"
    assert "gth_trend_session_mismatch" in context["trend"]["reasons"]
    assert "gth_trend_stale_or_future" in context["trend"]["reasons"]
    assert "gth_dip_session_mismatch" in context["dip_reclaim_call"]["reasons"]
    assert "gth_dip_source_expired" in context["dip_reclaim_call"]["reasons"]
    assert context["action_authority"] == "none"
    assert context["automatic_ordering"] is False


def test_gth_causal_path_rank_is_fresh_two_sided_observation_without_trend() -> None:
    payload = {
        "gth_path_ranks": {
            "schema_version": "gth_path_ranks.v1",
            "session_date": "2026-07-29",
            "updated_at": (NOW - timedelta(seconds=5)).isoformat(),
            "provider": "ibkr",
            "sampling_seconds": 5,
            "rank_semantics": "empirical_cdf_midrank_not_probability",
            "rank_method": "causal_non_overlapping_session_windows.v1",
            "horizons": {
                "15m": {
                    "horizon_seconds": 900,
                    "ready": True,
                    "status": "ready",
                    "sample_count": 175,
                    "expected_sample_count": 181,
                    "coverage_ratio": 0.9669,
                    "max_sample_gap_seconds": 15.0,
                    "sampling_quality": "usable_with_gaps",
                    "minimum_decision_samples": 4,
                    "decision_usable": True,
                    "path_rank": {
                        "position_percentile": 22.5,
                        "drawdown_points": 13.0,
                        "drawdown_rank_percentile": 80.0,
                        "recovery_points": 7.0,
                        "recovery_rank_percentile": 60.0,
                        "rally_points": 9.0,
                        "rally_rank_percentile": 40.0,
                        "pullback_points": 2.0,
                        "pullback_rank_percentile": 20.0,
                        "effective_reference_windows": 5,
                        "rank_status": "descriptive",
                    },
                },
                "60m": {
                    "horizon_seconds": 3600,
                    "ready": False,
                    "status": "collecting_full_window",
                    "seconds_until_ready": 1200.0,
                    "path_rank": {},
                },
            },
            "action_authority": "none",
            "actionable": False,
            "automatic_ordering": False,
        }
    }

    context = build_gth_observation_context(
        payload,
        mandate={
            "phase": "gth_preparation",
            "trading_date": "2026-07-29",
        },
        now=NOW,
    )

    path = context["path_ranks"]
    assert context["status"] == "ready"
    assert path["status"] == "ready"
    assert path["ready_horizon_count"] == 1
    assert path["horizons"]["15m"]["position_percentile"] == 22.5
    assert path["horizons"]["15m"]["rank_is_probability"] is False
    assert path["horizons"]["15m"]["decision_usable"] is True
    assert path["horizons"]["60m"]["ready"] is False
    assert path["action_authority"] == "none"
    assert path["automatic_ordering"] is False


def test_gth_path_rank_rejects_stale_or_wrong_session_projection() -> None:
    context = build_gth_observation_context(
        {
            "gth_path_ranks": {
                "schema_version": "gth_path_ranks.v1",
                "session_date": "2026-07-28",
                "updated_at": (NOW - timedelta(minutes=10)).isoformat(),
                "provider": "ibkr",
                "sampling_seconds": 5,
                "rank_semantics": "empirical_cdf_midrank_not_probability",
                "rank_method": "causal_non_overlapping_session_windows.v1",
                "horizons": {
                    "15m": {
                        "ready": True,
                        "decision_usable": True,
                        "path_rank": {"position_percentile": 10.0},
                    }
                },
            }
        },
        mandate={
            "phase": "gth_preparation",
            "trading_date": "2026-07-29",
        },
        now=NOW,
    )

    path = context["path_ranks"]
    assert path["status"] == "unavailable"
    assert "gth_path_rank_session_mismatch" in path["reasons"]
    assert "gth_path_rank_stale_or_future" in path["reasons"]
    assert path["horizons"]["15m"]["ready"] is False
    assert path["horizons"]["15m"]["decision_usable"] is False
    assert context["action_authority"] == "none"


def test_gth_manual_ready_requires_active_matching_source_and_live_ttl() -> None:
    source_id = "gth-dip:ready"
    payload = {
        "gth_dip_reclaim_signal": {
            "kind": "gth_dip_reclaim_call",
            "event_id": source_id,
            "session_date": "2026-07-29",
            "confirmed_at": (NOW - timedelta(minutes=1)).isoformat(),
            "valid_until": (NOW + timedelta(minutes=9)).isoformat(),
        },
        "gth_manual_candidate": {
            "status": "manual_ready",
            "candidate_id": "gth-manual:ready",
            "source_signal_id": source_id,
            "evaluated_at": (NOW - timedelta(seconds=5)).isoformat(),
            "valid_until": (NOW + timedelta(seconds=15)).isoformat(),
            "block_reasons": [],
            "manual_action_eligible": True,
            "automatic_ordering": False,
            "broker_submission_allowed": False,
        },
    }

    context = build_gth_observation_context(
        payload,
        mandate={
            "phase": "gth_preparation",
            "trading_date": "2026-07-29",
        },
        now=NOW,
    )

    manual = context["manual_candidate"]
    assert manual["status"] == "manual_ready"
    assert manual["projection_status"] == "manual_ready"
    assert manual["manual_action_eligible"] is True
    assert manual["action_authority"] == "manual_only"
    assert manual["projection_reasons"] == []


def test_gth_expired_manual_projection_remains_blocked_observation() -> None:
    source_id = "gth-dip:expired"
    payload = {
        "gth_dip_reclaim_signal": {
            "kind": "gth_dip_reclaim_call",
            "event_id": source_id,
            "session_date": "2026-07-29",
            "confirmed_at": (NOW - timedelta(minutes=1)).isoformat(),
            "valid_until": (NOW + timedelta(minutes=9)).isoformat(),
        },
        "gth_manual_candidate": {
            "status": "manual_ready",
            "candidate_id": "gth-manual:expired",
            "source_signal_id": source_id,
            "evaluated_at": (NOW - timedelta(seconds=10)).isoformat(),
            "valid_until": (NOW - timedelta(milliseconds=1)).isoformat(),
            "block_reasons": [],
            "manual_action_eligible": True,
            "automatic_ordering": False,
            "broker_submission_allowed": False,
        },
    }

    context = build_gth_observation_context(
        payload,
        mandate={
            "phase": "gth_preparation",
            "trading_date": "2026-07-29",
        },
        now=NOW,
    )

    manual = context["manual_candidate"]
    assert manual["status"] == "blocked"
    assert manual["projection_status"] == "manual_ready"
    assert "gth_manual_candidate_expired" in manual["projection_reasons"]
    assert manual["manual_action_eligible"] is False
    assert manual["execution_eligible"] is False
    assert manual["action_authority"] == "none"


def test_gth_manual_projection_rejects_source_race_and_elapsed_ttl() -> None:
    payload = {
        "gth_manual_candidate": {
            "status": "manual_ready",
            "candidate_id": "gth-manual:racy",
            "source_signal_id": "gth-dip:missing",
            "evaluated_at": (NOW - timedelta(seconds=21)).isoformat(),
            "valid_until": (NOW + timedelta(seconds=30)).isoformat(),
            "block_reasons": [],
            "manual_action_eligible": True,
            "automatic_ordering": False,
            "broker_submission_allowed": False,
        },
    }

    context = build_gth_observation_context(
        payload,
        mandate={
            "phase": "gth_preparation",
            "trading_date": "2026-07-29",
        },
        now=NOW,
    )

    manual = context["manual_candidate"]
    assert manual["status"] == "blocked"
    assert "gth_manual_candidate_ttl_elapsed" in manual["projection_reasons"]
    assert "gth_manual_candidate_source_inactive" in manual["projection_reasons"]
    assert "gth_manual_candidate_source_id_missing" in manual["projection_reasons"]
    assert manual["manual_action_eligible"] is False
    assert manual["action_authority"] == "none"


def test_gth_manual_projection_requires_exact_active_source_identity() -> None:
    payload = {
        "gth_dip_reclaim_signal": {
            "kind": "gth_dip_reclaim_call",
            "event_id": "gth-dip:current",
            "session_date": "2026-07-29",
            "confirmed_at": (NOW - timedelta(minutes=1)).isoformat(),
            "valid_until": (NOW + timedelta(minutes=9)).isoformat(),
        },
        "gth_manual_candidate": {
            "status": "manual_ready",
            "candidate_id": "gth-manual:prior-source",
            "source_signal_id": "gth-dip:prior",
            "evaluated_at": (NOW - timedelta(seconds=5)).isoformat(),
            "valid_until": (NOW + timedelta(seconds=15)).isoformat(),
            "block_reasons": [],
            "manual_action_eligible": True,
            "automatic_ordering": False,
            "broker_submission_allowed": False,
        },
    }

    context = build_gth_observation_context(
        payload,
        mandate={
            "phase": "gth_preparation",
            "trading_date": "2026-07-29",
        },
        now=NOW,
    )

    manual = context["manual_candidate"]
    assert manual["status"] == "blocked"
    assert manual["projection_reasons"] == ["gth_manual_candidate_source_mismatch"]
    assert manual["manual_action_eligible"] is False
    assert manual["action_authority"] == "none"
