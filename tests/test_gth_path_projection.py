from __future__ import annotations

from spx_spark.application.shock.gth_path_projection import (
    build_gth_path_rank_projection,
)


def test_gth_path_rank_projection_is_bounded_and_drops_raw_samples() -> None:
    projection = build_gth_path_rank_projection(
        {
            "session_date": "2026-07-30",
            "updated_at": "2026-07-30T01:00:00+00:00",
            "continuous_provider": "ibkr",
            "path_sampling_seconds": 5,
            "path_rank_semantics": "empirical_cdf_midrank_not_probability",
            "path_rank_method": "causal_non_overlapping_session_windows.v1",
            "samples": [{"at": "secret-large-raw-tail", "es": 7500.0}],
            "path_history": {"900": [{"drawdown_points": 10.0}]},
            "horizon_readiness": {
                "900": {
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
                        "position_percentile": 24.0,
                        "drawdown_rank_percentile": 80.0,
                        "recovery_rank_percentile": 60.0,
                        "rally_rank_percentile": 40.0,
                        "pullback_rank_percentile": 20.0,
                        "effective_reference_windows": 5,
                    },
                },
            },
        }
    )

    assert projection["schema_version"] == "gth_path_ranks.v1"
    assert projection["provider"] == "ibkr"
    assert projection["action_authority"] == "none"
    assert projection["actionable"] is False
    assert "samples" not in projection
    assert "path_history" not in projection
    horizon = projection["horizons"]["15m"]
    assert horizon["coverage_ratio"] == 0.9669
    assert horizon["decision_usable"] is True
    assert horizon["path_rank"]["position_percentile"] == 24.0
