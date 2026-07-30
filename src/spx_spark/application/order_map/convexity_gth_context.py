"""Direct-ES GTH observation context, isolated from option execution gates."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from spx_spark.analytics.options.pricing import finite_float


GTH_TREND_MAX_AGE_SECONDS = 120.0
GTH_PATH_RANK_MAX_AGE_SECONDS = 120.0
GTH_MANUAL_PROJECTION_MAX_AGE_SECONDS = 120.0
GTH_MANUAL_CANDIDATE_TTL_SECONDS = 20.0
GTH_PATH_RANK_SEMANTICS = "empirical_cdf_midrank_not_probability"
GTH_PATH_RANK_METHOD = "causal_non_overlapping_session_windows.v1"
GTH_PATH_SAMPLE_SECONDS = 5.0
GTH_PATH_DECISION_MAX_GAP_SECONDS = 600.0


def build_gth_observation_context(
    payload: Mapping[str, Any],
    *,
    mandate: Mapping[str, Any],
    now: datetime,
) -> dict[str, Any]:
    """Build a two-sided observation input without granting execution authority."""

    evaluated_at = _utc(now)
    phase = str(mandate.get("phase") or "")
    expected_session_date = str(mandate.get("trading_date") or "")
    base: dict[str, Any] = {
        "status": "inactive",
        "phase": phase,
        "expected_session_id": (f"{expected_session_date}:gth" if expected_session_date else None),
        "trend": _empty_trend(),
        "path_ranks": _empty_path_ranks("inactive"),
        "dip_reclaim_call": _empty_source("inactive"),
        "manual_candidate": _empty_manual_candidate("inactive"),
        "action_authority": "none",
        "actionable": False,
        "automatic_ordering": False,
    }
    if phase != "gth_preparation":
        return base

    trend = _trend_context(
        _mapping(payload.get("globex_trend")),
        expected_session_date=expected_session_date,
        now=evaluated_at,
    )
    path_ranks = _path_rank_context(
        _mapping(payload.get("gth_path_ranks")),
        expected_session_date=expected_session_date,
        now=evaluated_at,
    )
    dip = _dip_context(
        _mapping(payload.get("gth_dip_reclaim_signal")),
        expected_session_date=expected_session_date,
        now=evaluated_at,
    )
    manual = _manual_candidate_context(
        _mapping(payload.get("gth_manual_candidate")),
        active_source_id=str(dip.get("source_signal_id") or ""),
        source_active=dip.get("status") == "active",
        now=evaluated_at,
    )
    usable = (
        trend.get("status") == "ready"
        or path_ranks.get("status") == "ready"
        or dip.get("status") == "active"
    )
    return {
        **base,
        "status": "ready" if usable else "unavailable",
        "trend": trend,
        "path_ranks": path_ranks,
        "dip_reclaim_call": dip,
        "manual_candidate": manual,
    }


def _path_rank_context(
    projection: Mapping[str, Any],
    *,
    expected_session_date: str,
    now: datetime,
) -> dict[str, Any]:
    if not projection:
        return _empty_path_ranks("unavailable")
    updated_at = _datetime(projection.get("updated_at"))
    age_seconds = (now - updated_at).total_seconds() if updated_at is not None else None
    reasons: list[str] = []
    if projection.get("schema_version") != "gth_path_ranks.v1":
        reasons.append("gth_path_rank_schema_mismatch")
    if str(projection.get("session_date") or "") != expected_session_date:
        reasons.append("gth_path_rank_session_mismatch")
    if updated_at is None:
        reasons.append("gth_path_rank_updated_at_missing")
    elif age_seconds is None or age_seconds < -2.0 or age_seconds > GTH_PATH_RANK_MAX_AGE_SECONDS:
        reasons.append("gth_path_rank_stale_or_future")
    semantics = str(projection.get("rank_semantics") or "")
    if semantics != GTH_PATH_RANK_SEMANTICS:
        reasons.append("gth_path_rank_semantics_mismatch")
    if str(projection.get("rank_method") or "") != GTH_PATH_RANK_METHOD:
        reasons.append("gth_path_rank_method_mismatch")
    sampling_seconds = _number(projection.get("sampling_seconds"))
    if sampling_seconds != GTH_PATH_SAMPLE_SECONDS:
        reasons.append("gth_path_rank_sampling_contract_mismatch")
    provider = str(projection.get("provider") or "")
    if not provider:
        reasons.append("gth_path_rank_provider_missing")

    horizons: dict[str, Any] = {}
    raw_horizons = _mapping(projection.get("horizons"))
    for name in ("15m", "60m"):
        raw = _mapping(raw_horizons.get(name))
        rank = _mapping(raw.get("path_rank"))
        horizon_reasons: list[str] = []
        expected_horizon = 900 if name == "15m" else 3600
        expected_sample_count = int(expected_horizon / GTH_PATH_SAMPLE_SECONDS) + 1
        expected_minimum_samples = int(expected_horizon / 300) + 1
        if _number(raw.get("horizon_seconds")) != expected_horizon:
            horizon_reasons.append("gth_path_rank_horizon_contract_mismatch")
        sample_count = _number(raw.get("sample_count"))
        serialized_expected_samples = _number(raw.get("expected_sample_count"))
        minimum_samples = _number(raw.get("minimum_decision_samples"))
        coverage_ratio = _number(raw.get("coverage_ratio"))
        max_gap = _number(raw.get("max_sample_gap_seconds"))
        if raw.get("ready") is True and (
            sample_count is None
            or sample_count < 2
            or not sample_count.is_integer()
            or serialized_expected_samples != expected_sample_count
            or coverage_ratio is None
            or not 0.0 <= coverage_ratio <= 1.0
            or max_gap is None
            or max_gap < 0.0
        ):
            horizon_reasons.append("gth_path_rank_sample_contract_invalid")
        ready = (
            not reasons
            and not horizon_reasons
            and raw.get("ready") is True
            and _percentile(rank.get("position_percentile")) is not None
        )
        decision_usable = bool(
            ready
            and raw.get("decision_usable") is True
            and minimum_samples is not None
            and minimum_samples == expected_minimum_samples
            and sample_count is not None
            and sample_count >= minimum_samples
            and max_gap is not None
            and max_gap <= GTH_PATH_DECISION_MAX_GAP_SECONDS
        )
        if raw.get("decision_usable") is True and not decision_usable:
            horizon_reasons.append("gth_path_rank_decision_contract_invalid")
        horizons[name] = {
            "status": "ready" if ready else str(raw.get("status") or "unavailable"),
            "ready": ready,
            "horizon_seconds": raw.get("horizon_seconds"),
            "observed_span_seconds": _number(raw.get("observed_span_seconds")),
            "seconds_until_ready": _number(raw.get("seconds_until_ready")),
            "sample_count": int(sample_count) if sample_count is not None else None,
            "expected_sample_count": (
                int(serialized_expected_samples)
                if serialized_expected_samples is not None
                else None
            ),
            "coverage_ratio": coverage_ratio,
            "max_sample_gap_seconds": max_gap,
            "sampling_quality": raw.get("sampling_quality"),
            "minimum_decision_samples": (
                int(minimum_samples) if minimum_samples is not None else None
            ),
            "decision_usable": decision_usable,
            "position_percentile": _percentile(rank.get("position_percentile")),
            "drawdown_points": _number(rank.get("drawdown_points")),
            "drawdown_rank_percentile": _percentile(rank.get("drawdown_rank_percentile")),
            "recovery_points": _number(rank.get("recovery_points")),
            "recovery_rank_percentile": _percentile(rank.get("recovery_rank_percentile")),
            "rally_points": _number(rank.get("rally_points")),
            "rally_rank_percentile": _percentile(rank.get("rally_rank_percentile")),
            "pullback_points": _number(rank.get("pullback_points")),
            "pullback_rank_percentile": _percentile(rank.get("pullback_rank_percentile")),
            "effective_reference_windows": rank.get("effective_reference_windows"),
            "rank_status": rank.get("rank_status"),
            "rank_semantics": semantics or None,
            "rank_is_probability": False,
            "reasons": horizon_reasons,
            "action_authority": "none",
        }
    ready_count = sum(row["ready"] is True for row in horizons.values())
    return {
        "status": "ready" if ready_count else "collecting" if not reasons else "unavailable",
        "session_date": projection.get("session_date"),
        "updated_at": updated_at.isoformat() if updated_at is not None else None,
        "age_seconds": round(age_seconds, 2) if age_seconds is not None else None,
        "maximum_age_seconds": GTH_PATH_RANK_MAX_AGE_SECONDS,
        "provider": provider or None,
        "sampling_seconds": sampling_seconds,
        "rank_semantics": semantics or None,
        "rank_is_probability": False,
        "horizons": horizons,
        "ready_horizon_count": ready_count,
        "reasons": reasons,
        "source": "direct_live_es_causal_session_path_ranks",
        "action_authority": "none",
        "actionable": False,
        "automatic_ordering": False,
    }


def _trend_context(
    trend: Mapping[str, Any],
    *,
    expected_session_date: str,
    now: datetime,
) -> dict[str, Any]:
    expected_session_id = f"{expected_session_date}:gth" if expected_session_date else ""
    session_id = str(trend.get("session_id") or "")
    updated_at = _datetime(trend.get("updated_at"))
    age_seconds = (now - updated_at).total_seconds() if updated_at is not None else None
    regime = str(trend.get("regime") or "").lower()
    reasons: list[str] = []
    if not expected_session_id or session_id != expected_session_id:
        reasons.append("gth_trend_session_mismatch")
    if updated_at is None:
        reasons.append("gth_trend_updated_at_missing")
    elif age_seconds is None or age_seconds < -2.0 or age_seconds > GTH_TREND_MAX_AGE_SECONDS:
        reasons.append("gth_trend_stale_or_future")
    if regime not in {"bullish", "bearish", "neutral"}:
        reasons.append("gth_trend_regime_unavailable")
    metrics = _mapping(trend.get("metrics"))
    return {
        "status": "ready" if not reasons else "unavailable",
        "session_id": session_id or None,
        "regime": regime if regime in {"bullish", "bearish", "neutral"} else None,
        "updated_at": updated_at.isoformat() if updated_at is not None else None,
        "age_seconds": round(age_seconds, 2) if age_seconds is not None else None,
        "maximum_age_seconds": GTH_TREND_MAX_AGE_SECONDS,
        "return_15m_points": _number(metrics.get("return_15m_points")),
        "return_60m_points": _number(metrics.get("return_60m_points")),
        "return_180m_points": _number(metrics.get("return_180m_points")),
        "drawdown_from_regime_high_points": _number(
            metrics.get("drawdown_from_regime_high_points")
        ),
        "rebound_from_regime_low_points": _number(metrics.get("rebound_from_regime_low_points")),
        "provider": _latest_provider(trend),
        "reasons": reasons,
        "source": "direct_live_es_globex_trend",
        "action_authority": "none",
    }


def _dip_context(
    signal: Mapping[str, Any],
    *,
    expected_session_date: str,
    now: datetime,
) -> dict[str, Any]:
    if not signal:
        return _empty_source("unavailable")
    confirmed_at = _datetime(signal.get("confirmed_at"))
    valid_until = _datetime(signal.get("valid_until"))
    reasons: list[str] = []
    if signal.get("kind") != "gth_dip_reclaim_call":
        reasons.append("gth_dip_source_kind_mismatch")
    if str(signal.get("session_date") or "") != expected_session_date:
        reasons.append("gth_dip_session_mismatch")
    if confirmed_at is None or confirmed_at > now:
        reasons.append("gth_dip_confirmation_invalid")
    if valid_until is None or valid_until <= now:
        reasons.append("gth_dip_source_expired")
    entry_quality = _mapping(signal.get("entry_quality"))
    quality_reasons = [str(reason) for reason in entry_quality.get("block_reasons") or []]
    active = not reasons
    return {
        "status": "active" if active else "unavailable",
        "source_signal_id": signal.get("event_id"),
        "right": "C",
        "direction": "up",
        "confirmed_at": (confirmed_at.isoformat() if confirmed_at is not None else None),
        "valid_until": valid_until.isoformat() if valid_until is not None else None,
        "drawdown_points": _number(signal.get("drawdown_points")),
        "entry_quality_mode": entry_quality.get("mode"),
        "entry_quality_verdict": entry_quality.get("verdict"),
        "entry_quality_reasons": quality_reasons[:6],
        "reasons": reasons,
        "source": "gth_dip_reclaim_call",
        "manual_action_eligible": False,
        "execution_eligible": False,
        "action_authority": "none",
        "automatic_ordering": False,
    }


def _manual_candidate_context(
    candidate: Mapping[str, Any],
    *,
    active_source_id: str,
    source_active: bool,
    now: datetime,
) -> dict[str, Any]:
    if not candidate:
        return _empty_manual_candidate("unavailable")
    evaluated_at = _datetime(candidate.get("evaluated_at"))
    age_seconds = (now - evaluated_at).total_seconds() if evaluated_at is not None else None
    reasons: list[str] = []
    if evaluated_at is None:
        reasons.append("gth_manual_candidate_evaluated_at_missing")
    elif (
        age_seconds is None
        or age_seconds < -2.0
        or age_seconds > GTH_MANUAL_PROJECTION_MAX_AGE_SECONDS
    ):
        reasons.append("gth_manual_candidate_stale_or_future")
    elif evaluated_at + timedelta(seconds=GTH_MANUAL_CANDIDATE_TTL_SECONDS) <= now:
        reasons.append("gth_manual_candidate_ttl_elapsed")
    source_id = str(candidate.get("source_signal_id") or "")
    if not source_active:
        reasons.append("gth_manual_candidate_source_inactive")
    if not source_id or not active_source_id:
        reasons.append("gth_manual_candidate_source_id_missing")
    elif source_id != active_source_id:
        reasons.append("gth_manual_candidate_source_mismatch")
    valid_until = _datetime(candidate.get("valid_until"))
    if valid_until is None:
        reasons.append("gth_manual_candidate_valid_until_missing")
    elif valid_until <= now:
        reasons.append("gth_manual_candidate_expired")
    raw_status = str(candidate.get("status") or "unavailable")
    ready = (
        not reasons
        and raw_status == "manual_ready"
        and candidate.get("manual_action_eligible") is True
        and candidate.get("automatic_ordering") is False
        and candidate.get("broker_submission_allowed") is False
    )
    status = "manual_ready" if ready else "blocked" if raw_status == "manual_ready" else raw_status
    return {
        "status": status,
        "projection_status": raw_status,
        "candidate_id": candidate.get("candidate_id"),
        "source_signal_id": source_id or None,
        "side": "call",
        "evaluated_at": (evaluated_at.isoformat() if evaluated_at is not None else None),
        "valid_until": (valid_until.isoformat() if valid_until is not None else None),
        "age_seconds": round(age_seconds, 2) if age_seconds is not None else None,
        "maximum_ready_age_seconds": GTH_MANUAL_CANDIDATE_TTL_SECONDS,
        "projection_reasons": reasons,
        "manual_ready_block_reasons": [
            str(reason) for reason in candidate.get("block_reasons") or []
        ][:8],
        "manual_action_eligible": ready,
        "execution_eligible": False,
        "action_authority": "manual_only" if ready else "none",
        "automatic_ordering": False,
        "broker_submission_allowed": False,
    }


def _latest_provider(trend: Mapping[str, Any]) -> str | None:
    samples = trend.get("samples")
    if isinstance(samples, list) and samples:
        latest = samples[-1]
        if isinstance(latest, Mapping):
            return str(latest.get("provider") or "") or None
    transition = _mapping(trend.get("last_transition"))
    return str(transition.get("provider") or "") or None


def _empty_trend() -> dict[str, Any]:
    return {
        "status": "inactive",
        "session_id": None,
        "regime": None,
        "return_15m_points": None,
        "return_60m_points": None,
        "return_180m_points": None,
        "reasons": [],
        "source": "direct_live_es_globex_trend",
        "action_authority": "none",
    }


def _empty_source(status: str) -> dict[str, Any]:
    return {
        "status": status,
        "source_signal_id": None,
        "right": "C",
        "direction": "up",
        "entry_quality_verdict": None,
        "entry_quality_reasons": [],
        "reasons": [],
        "source": "gth_dip_reclaim_call",
        "manual_action_eligible": False,
        "execution_eligible": False,
        "action_authority": "none",
        "automatic_ordering": False,
    }


def _empty_path_ranks(status: str) -> dict[str, Any]:
    return {
        "status": status,
        "session_date": None,
        "provider": None,
        "sampling_seconds": None,
        "rank_semantics": None,
        "rank_is_probability": False,
        "horizons": {},
        "ready_horizon_count": 0,
        "reasons": [],
        "source": "direct_live_es_causal_session_path_ranks",
        "action_authority": "none",
        "actionable": False,
        "automatic_ordering": False,
    }


def _empty_manual_candidate(status: str) -> dict[str, Any]:
    return {
        "status": status,
        "candidate_id": None,
        "source_signal_id": None,
        "side": "call",
        "projection_reasons": [],
        "manual_ready_block_reasons": [],
        "manual_action_eligible": False,
        "execution_eligible": False,
        "action_authority": "none",
        "automatic_ordering": False,
        "broker_submission_allowed": False,
    }


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    return finite_float(value)


def _percentile(value: object) -> float | None:
    number = _number(value)
    return number if number is not None and 0.0 <= number <= 100.0 else None


def _datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        try:
            return _utc(value)
        except ValueError:
            return None
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return _utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timezone-aware datetime required")
    return value.astimezone(timezone.utc)


__all__ = ["build_gth_observation_context"]
