"""Bounded reader projection for GTH causal path ranks."""

from __future__ import annotations

from typing import Mapping


def build_gth_path_rank_projection(
    gth_state: Mapping[str, object],
) -> dict[str, object]:
    """Publish path evidence without copying the hot worker's raw samples."""

    raw_readiness = gth_state.get("horizon_readiness")
    readiness = raw_readiness if isinstance(raw_readiness, Mapping) else {}
    horizons: dict[str, object] = {}
    rank_fields = (
        "rank_semantics",
        "reference_method",
        "reference_overlap",
        "position_sampling_seconds",
        "position_sample_count",
        "position_percentile",
        "drawdown_points",
        "drawdown_rank_percentile",
        "recovery_points",
        "recovery_rank_percentile",
        "rally_points",
        "rally_rank_percentile",
        "pullback_points",
        "pullback_rank_percentile",
        "effective_reference_windows",
        "rank_status",
    )
    readiness_fields = (
        "horizon_seconds",
        "ready",
        "status",
        "observed_span_seconds",
        "seconds_until_ready",
        "sample_count",
        "expected_sample_count",
        "coverage_ratio",
        "max_sample_gap_seconds",
        "sampling_quality",
        "minimum_decision_samples",
        "decision_usable",
    )
    for raw_key, raw_value in readiness.items():
        if not isinstance(raw_value, Mapping):
            continue
        try:
            horizon_seconds = int(raw_value.get("horizon_seconds") or raw_key)
        except (TypeError, ValueError):
            continue
        if horizon_seconds <= 0:
            continue
        raw_rank = raw_value.get("path_rank")
        rank = raw_rank if isinstance(raw_rank, Mapping) else {}
        horizon = {field: raw_value.get(field) for field in readiness_fields if field in raw_value}
        horizon["path_rank"] = {field: rank.get(field) for field in rank_fields if field in rank}
        horizons[f"{horizon_seconds // 60}m"] = horizon
    return {
        "schema_version": "gth_path_ranks.v1",
        "session_date": gth_state.get("session_date"),
        "updated_at": gth_state.get("updated_at"),
        "provider": gth_state.get("continuous_provider"),
        "sampling_seconds": gth_state.get("path_sampling_seconds"),
        "rank_semantics": gth_state.get("path_rank_semantics"),
        "rank_method": gth_state.get("path_rank_method"),
        "horizons": horizons,
        "action_authority": "none",
        "actionable": False,
        "automatic_ordering": False,
    }


__all__ = ["build_gth_path_rank_projection"]
