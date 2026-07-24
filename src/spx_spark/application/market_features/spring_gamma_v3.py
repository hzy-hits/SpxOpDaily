"""Pure, fail-closed Spring Gamma v3 shadow inference.

ES is the sole direction backbone. Option structure and the public level path
may reduce confidence or validate a setup, but neither may create or reverse a
direction. This module performs no I/O and has no production authority.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

import numpy as np

from spx_spark.analytics.options.pricing import finite_float
from spx_spark.marketdata import as_utc
from spx_spark.settings.spring_gamma_v3 import SpringGammaV3Settings


SCHEMA_VERSION = "spring_gamma_v3_shadow.v1"
MODEL_VERSION = "spring_gamma_v3_es_only_shadow.v1"
CALIBRATION_STATUS = "uncalibrated_shadow"

_HORIZONS = (15, 30, 60)
_RETURN_SCALES = {5: 4.0, 15: 8.0, 30: 12.0, 60: 18.0, 180: 35.0}
_FEATURE_WEIGHTS = {
    "return_5m": 0.10,
    "return_15m": 0.25,
    "return_60m": 0.30,
    "return_180m": 0.15,
    "vwap_distance": 0.10,
    "vwap_slope": 0.05,
    "trend_efficiency": 0.05,
}
_GTH_SEGMENTS = frozenset({"asia", "europe", "us_premarket", "curb", "gth"})
_SPRING_PHASES = frozenset({"rejected", "retest", "confirmed"})
_FADE_SIGN = {"put_wall": 1, "flip_low": 1, "flip_high": -1, "call_wall": -1}
_GOOD_LEVEL_QUALITY = frozenset({"ready", "ok", "live", "confirmed"})


@dataclass(frozen=True)
class _Policy:
    enabled: bool
    report_enabled: bool
    prediction_interval_seconds: int
    horizons_minutes: tuple[int, ...]
    greek_max_age_seconds: float
    iv_max_age_seconds: float
    min_pair_ratio: float
    min_iv_coverage: float
    min_delta_coverage: float
    min_oi_coverage: float
    min_paired_strikes: int
    min_probability: float
    min_margin: float


def build_spring_gamma_v3_shadow(
    *,
    market_frame: Mapping[str, object] | object,
    option_frame: Mapping[str, object] | object,
    greek_reference: Mapping[str, object] | object,
    exposure_map: Mapping[str, object] | object,
    now: datetime,
    expected_expiry: str,
    session: str | None = None,
    settings: Mapping[str, object] | object | None = None,
    level_decision: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Return one deterministic shadow record without reading global state."""

    at = as_utc(now)
    market, options = _mapping(market_frame), _mapping(option_frame)
    greeks, exposures = _mapping(greek_reference), _mapping(exposure_map)
    expiry = str(expected_expiry or "")
    selected_session, session_error = _session(market, session)
    policy = _policy(settings, selected_session)
    policy_payload = {"session": selected_session, **asdict(policy)}
    level = _safe_level(level_decision)
    expiry_row = next(
        (
            row
            for item in _items(exposures.get("expiries"))
            if (row := _mapping(item)) and str(row.get("expiry") or "") == expiry
        ),
        {},
    )

    coverage = _strike_coverage(expiry_row, exposures)
    freshness = _freshness(market, options, greeks, exposures, expiry_row, at)
    structure_risk = _structure_risk(greeks, selected_session)
    gate_reasons = _gate_reasons(
        market,
        options,
        greeks,
        expiry_row,
        expiry=expiry,
        session_error=session_error,
        policy=policy,
        coverage=coverage,
        freshness=freshness,
    )
    gate_passed = not gate_reasons
    es_diagnostic, es_values = _es_projection(market)
    decision, opportunity, direction_reasons = _decide(
        es_diagnostic,
        es_values,
        level,
        policy,
        structure_risk,
        gate_passed=gate_passed,
    )
    reasons = list(dict.fromkeys([*gate_reasons, *direction_reasons]))
    abstain = decision["decision"] == "abstain"
    disabled = not policy.enabled
    if not abstain:
        reasons = []

    fingerprint = _fingerprint(
        {
            "as_of": at,
            "expiry": expiry,
            "session": selected_session,
            "market": market,
            "options": options,
            "greeks": greeks,
            "exposures": exposures,
            "level": level,
            "policy": policy_payload,
        }
    )
    session_id = str(market.get("session_id") or "unknown")
    return {
        "schema_version": SCHEMA_VERSION,
        "model_version": MODEL_VERSION,
        "prediction_id": (f"spring-gamma-v3:{session_id}:{expiry or 'unknown'}:{fingerprint[:16]}"),
        "input_fingerprint": fingerprint,
        "market_frame_id": market.get("frame_id"),
        "option_frame_id": options.get("frame_id"),
        "as_of": at.isoformat(),
        "session_id": session_id,
        "session": selected_session,
        "expiry": expiry or None,
        "status": "disabled" if disabled else "abstain" if abstain else "ready",
        "mode": "shadow",
        "direction_authority": "none",
        "action_authority": "none",
        "actionable": False,
        "automatic_ordering": False,
        "calibration_status": CALIBRATION_STATUS,
        "direction": decision,
        "regime": opportunity,
        "opportunity": opportunity,
        "quality": {
            "gate_status": "pass" if gate_passed else "fail",
            "policy_session": selected_session,
            "policy": policy_payload,
            "freshness": freshness,
            "coverage": coverage,
            "exact_expiry_required": True,
            "frozen_structure_allowed": False,
        },
        "risk": _risk_payload(greeks, expiry_row, coverage, structure_risk),
        "level_gate": level,
        "abstain": abstain,
        "abstain_reasons": reasons,
    }


def _es_projection(
    market: Mapping[str, object],
) -> tuple[dict[str, object], dict[str, float | None]]:
    es = _child(market, "es")
    returns = {
        minutes: finite_float(es.get(f"return_{minutes}m_points"))
        for minutes in (5, 15, 30, 60, 180)
    }
    method_30 = "direct_es_return"
    if returns[30] is None and returns[15] is not None and returns[60] is not None:
        returns[30] = returns[15] + (returns[60] - returns[15]) / 3.0
        method_30 = "linear_interpolation_15m_60m_es_only"
    horizon_scores = {
        minutes: _round(np.tanh(value / _RETURN_SCALES[minutes])) if value is not None else None
        for minutes, value in returns.items()
    }

    efficiency = finite_float(es.get("trend_efficiency_60m"))
    efficiency_score = (
        _sign(returns[60]) * float(np.clip(efficiency, 0.0, 1.0))
        if efficiency is not None and _sign(returns[60])
        else None
    )
    raw_features = {
        "return_5m": horizon_scores[5],
        "return_15m": horizon_scores[15],
        "return_60m": horizon_scores[60],
        "return_180m": horizon_scores[180],
        "vwap_distance": _bounded(es.get("vwap_distance_points"), 10.0),
        "vwap_slope": _bounded(es.get("vwap_slope_15m_points"), 2.0),
        "trend_efficiency": efficiency_score,
    }
    available = [
        (float(value), _FEATURE_WEIGHTS[name])
        for name, value in raw_features.items()
        if value is not None
    ]
    composite = (
        float(np.average([value for value, _ in available], weights=[w for _, w in available]))
        if returns[15] is not None and returns[60] is not None and available
        else None
    )
    values = {
        "return_5m": returns[5],
        "score_15m": horizon_scores[15],
        "score_60m": horizon_scores[60],
        "composite": _round(composite),
    }
    return {
        "backbone": "es_only",
        "return_points": {f"{m}m": _round(returns[m]) for m in _HORIZONS},
        "scores": {f"{m}m": horizon_scores[m] for m in _HORIZONS},
        "composite_score": _round(composite),
        "feature_scores": {name: _round(value) for name, value in raw_features.items()},
        "feature_weights": dict(_FEATURE_WEIGHTS),
        "score_method": {
            "horizons": "tanh_es_points_fixed_scales",
            "30m": method_30,
            "composite": "weighted_available_independent_es_features_no_30m_reuse",
        },
    }, values


def _decide(
    diagnostic: Mapping[str, object],
    values: Mapping[str, float | None],
    level: Mapping[str, object],
    policy: _Policy,
    structure_risk: Mapping[str, object],
    *,
    gate_passed: bool,
) -> tuple[dict[str, object], str, list[str]]:
    score_15 = finite_float(values.get("score_15m"))
    score_60 = finite_float(values.get("score_60m"))
    composite = finite_float(values.get("composite"))
    spring_sign = (
        _sign(score_15)
        if _sign(values.get("return_5m"))
        and _sign(values.get("return_5m")) == _sign(score_15) == -_sign(score_60)
        else 0
    )
    trend_sign = (
        _sign(composite)
        if _sign(score_15) and _sign(score_15) == _sign(score_60) == _sign(composite)
        else 0
    )
    spring_path_sign = _spring_path_sign(level)
    candidate = "transition"
    raw_score = composite
    candidate_sign = 0
    if spring_sign and spring_path_sign == spring_sign:
        candidate, candidate_sign, raw_score = "spring_reversion", spring_sign, score_15
    elif trend_sign:
        candidate, candidate_sign = "trend_continuation", trend_sign

    multiplier = finite_float(structure_risk.get("confidence_multiplier")) or 0.0
    confidence_score = raw_score * multiplier if raw_score is not None else None
    confident = _confident(confidence_score, policy)
    opportunity = (
        candidate if gate_passed and (confident or candidate == "transition") else "abstain"
    )
    direction = _direction_name(candidate_sign) if gate_passed and confident else "abstain"
    reasons: list[str] = []
    if score_15 is None or score_60 is None or composite is None:
        reasons.append("es_direction_inputs_incomplete")
    elif not gate_passed:
        reasons.append("structure_gate_failed")
    elif candidate == "transition":
        reasons.append("transition_no_direction")
        if spring_sign:
            reasons.append("spring_reversion_path_unconfirmed")
    elif not confident:
        reasons.append("direction_confidence_below_threshold")

    raw_p = _probabilities(confidence_score)
    published_p, cap = raw_p, None
    if not gate_passed and raw_p[0] is not None:
        cap = max(0.50, policy.min_probability - 0.01)
        published_p = _cap_probabilities(raw_p, cap)
    diagnostic_sign = spring_sign or _sign(composite)
    return (
        {
            **diagnostic,
            "decision": direction,
            "diagnostic_es_direction": _direction_name(diagnostic_sign, flat="flat"),
            "raw_confidence_score": _round(raw_score),
            "confidence_score": _round(confidence_score),
            "p_up": published_p[0],
            "p_down": published_p[1],
            "raw_p_up": raw_p[0],
            "raw_p_down": raw_p[1],
            "confidence_cap": _round(cap),
            "calibration_status": CALIBRATION_STATUS,
            "direction_score_adjustment_from_structure": 0.0,
            "confidence_multiplier_from_structure_risk": _round(multiplier),
        },
        opportunity,
        reasons,
    )


def _structure_risk(
    greeks: Mapping[str, object],
    session: str,
) -> dict[str, object]:
    aggregate = _child(greeks, "aggregate")
    gamma = finite_float(aggregate.get("gross_gamma_abs"))
    charm = finite_float(aggregate.get("gross_charm_5m_abs"))
    vanna = finite_float(aggregate.get("gross_vanna_1vol_abs"))
    charm_equiv = charm / gamma if gamma and charm is not None else None
    vanna_equiv = vanna / gamma if gamma and vanna is not None else None
    charm_weight, vanna_weight = (0.20, 0.10) if session == "rth" else (0.06, 0.03)
    penalty = (
        charm_weight * float(np.tanh(charm_equiv / 5.0))
        + vanna_weight * float(np.tanh(vanna_equiv / 5.0))
        if charm_equiv is not None and vanna_equiv is not None
        else 1.0
    )
    penalty = float(np.clip(penalty, 0.0, 0.30 if session == "rth" else 0.09))
    return {
        "charm_equiv_5m": _round(charm_equiv),
        "vanna_equiv_1vol": _round(vanna_equiv),
        "bounded_penalty": _round(penalty),
        "confidence_multiplier": _round(1.0 - penalty),
        "prior": "fixed_weak_rth" if session == "rth" else "fixed_weak_gth",
        "direction_sign_effect": "none",
    }


def _gate_reasons(
    market: Mapping[str, object],
    options: Mapping[str, object],
    greeks: Mapping[str, object],
    expiry_row: Mapping[str, object],
    *,
    expiry: str,
    session_error: str | None,
    policy: _Policy,
    coverage: Mapping[str, object],
    freshness: Mapping[str, object],
) -> list[str]:
    reasons = [session_error] if session_error else []
    if not policy.enabled:
        reasons.append("shadow_disabled")
    if len(expiry) != 8 or not expiry.isdigit():
        reasons.append("invalid_expected_expiry")
    if not set(_HORIZONS).issubset(policy.horizons_minutes):
        reasons.append("required_horizons_not_configured")
    es = _child(market, "es")
    if _token(market.get("quality")) == "unavailable" or not es:
        reasons.append("market_frame_unavailable")
    if es and es.get("source_at") is None and es.get("observed_at") is None:
        reasons.append("es_source_timestamp_missing")
    _age_gate(
        reasons,
        freshness.get("market_age_seconds"),
        policy.greek_max_age_seconds,
        "market_timestamp_missing",
        "market_frame_stale",
    )
    if str(options.get("front_expiry") or "") != expiry:
        reasons.append("option_exact_expiry_mismatch")
    if _token(options.get("quality")) == "unavailable":
        reasons.append("option_frame_unavailable")
    if _child(options, "structure").get("frozen") is True:
        reasons.append("option_structure_frozen")
    _age_gate(
        reasons,
        freshness.get("iv_age_seconds"),
        policy.iv_max_age_seconds,
        "iv_timestamp_missing",
        "iv_surface_stale",
    )
    if finite_float(expiry_row.get("snapshot_age_seconds")) is None:
        reasons.append("iv_snapshot_age_missing")

    if str(greeks.get("expiry") or "") != expiry:
        reasons.append("greek_exact_expiry_mismatch")
    if str(_child(greeks, "aggregate").get("expiry") or "") != expiry:
        reasons.append("greek_aggregate_exact_expiry_mismatch")
    if str(greeks.get("status") or "") == "unavailable":
        reasons.append("greek_reference_unavailable")
    _age_gate(
        reasons,
        freshness.get("greek_age_seconds"),
        policy.greek_max_age_seconds,
        "greek_timestamp_missing",
        "greek_reference_stale",
    )
    reasons.extend(_greek_gate(greeks, policy))

    if not expiry_row:
        reasons.append("exposure_exact_expiry_unavailable")
    else:
        if str(expiry_row.get("quality") or "") in {"", "unavailable", "no_open_interest"}:
            reasons.append("exposure_quality_unavailable")
        if str(expiry_row.get("oi_quality") or "") in {"", "missing", "stale_or_zero"}:
            reasons.append("open_interest_quality_unavailable")
        if finite_float(_child(expiry_row, "oi_weighted").get("net_gamma_ratio")) is None:
            reasons.append("net_gamma_ratio_unavailable")
        checks = (
            ("complete_pair_ratio", policy.min_pair_ratio, "complete_pair_ratio_insufficient"),
            (
                "core_complete_pair_ratio",
                policy.min_pair_ratio,
                "core_complete_pair_ratio_insufficient",
            ),
            (
                "left_wing_complete_pair_ratio",
                policy.min_pair_ratio,
                "left_wing_complete_pair_ratio_insufficient",
            ),
            (
                "right_wing_complete_pair_ratio",
                policy.min_pair_ratio,
                "right_wing_complete_pair_ratio_insufficient",
            ),
            ("iv_coverage_ratio", policy.min_iv_coverage, "iv_coverage_insufficient"),
            (
                "delta_coverage_ratio",
                policy.min_delta_coverage,
                "delta_coverage_insufficient",
            ),
            ("greek_coverage_ratio", policy.min_pair_ratio, "greek_coverage_insufficient"),
        )
        for key, minimum, reason in checks:
            if (finite_float(coverage.get(key)) or -1.0) < minimum:
                reasons.append(reason)
        if (finite_float(coverage.get("paired_strikes")) or 0) < policy.min_paired_strikes:
            reasons.append("paired_strikes_insufficient")
        if not (finite_float(coverage.get("left_wing_paired_strikes")) or 0):
            reasons.append("left_wing_unpaired")
        if not (finite_float(coverage.get("right_wing_paired_strikes")) or 0):
            reasons.append("right_wing_unpaired")
    return list(dict.fromkeys(reason for reason in reasons if reason))


def _greek_gate(greeks: Mapping[str, object], policy: _Policy) -> list[str]:
    aggregate, model, coverage = (
        _child(greeks, "aggregate"),
        _child(greeks, "model"),
        _child(greeks, "coverage"),
    )
    reasons: list[str] = []
    if not model or not str(model.get("name") or "") or finite_float(model.get("spot")) is None:
        reasons.append("greek_reference_model_missing")
    if not aggregate:
        return [*reasons, "greek_reference_aggregate_missing"]
    if str(aggregate.get("quality") or "") != "ok":
        reasons.append("greek_reference_quality_insufficient")
    for key, reason, allow_zero in (
        ("gross_gamma_abs", "greek_gamma_missing", False),
        ("gross_charm_5m_abs", "greek_charm_missing", True),
        ("gross_vanna_1vol_abs", "greek_vanna_missing", True),
    ):
        value = finite_float(aggregate.get(key))
        if value is None or value < 0 or (not allow_zero and value == 0):
            reasons.append(reason)
    for value, minimum, reason in (
        (
            aggregate.get("iv_coverage_ratio"),
            policy.min_iv_coverage,
            "greek_iv_coverage_insufficient",
        ),
        (
            coverage.get("usable_ratio"),
            policy.min_iv_coverage,
            "greek_usable_coverage_insufficient",
        ),
        (
            aggregate.get("oi_coverage_ratio"),
            policy.min_oi_coverage,
            "greek_oi_coverage_insufficient",
        ),
    ):
        if (finite_float(value) or -1.0) < minimum:
            reasons.append(reason)
    return reasons


def _strike_coverage(
    expiry: Mapping[str, object],
    exposures: Mapping[str, object],
) -> dict[str, object]:
    rows = [
        row
        for item in _items(expiry.get("strikes"))
        if (row := _mapping(item)) and finite_float(row.get("strike")) is not None
    ]
    total, legs = len(rows), len(rows) * 2
    complete = [_complete_pair(row) for row in rows]
    spot = finite_float(_child(exposures, "underlier").get("price"))
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
    return {
        "strike_count": total,
        "complete_pair_ratio": _ratio(sum(complete), total),
        "paired_strikes": sum(complete),
        "core_strike_count": len(core),
        "core_complete_pair_ratio": _pair_ratio(core),
        "left_wing_strike_count": len(left),
        "left_wing_paired_strikes": sum(_complete_pair(row) for row in left),
        "left_wing_complete_pair_ratio": _pair_ratio(left),
        "right_wing_strike_count": len(right),
        "right_wing_paired_strikes": sum(_complete_pair(row) for row in right),
        "right_wing_complete_pair_ratio": _pair_ratio(right),
        "iv_coverage_ratio": (
            _round(min(computed_iv, reported_iv))
            if computed_iv is not None and reported_iv is not None
            else None
        ),
        "delta_coverage_ratio": (
            _round(min(computed_delta, reported_delta))
            if computed_delta is not None and reported_delta is not None
            else None
        ),
        "greek_coverage_ratio": leg_ratio("greeks"),
        "nonzero_oi_leg_ratio": leg_ratio("nonzero_oi"),
        "reported_iv_coverage_ratio": reported_iv,
        "reported_delta_coverage_ratio": reported_delta,
        "underlier": spot,
        "partition_method": "nearest_one_third_core_with_spot_sided_wings",
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


def _freshness(
    market: Mapping[str, object],
    options: Mapping[str, object],
    greeks: Mapping[str, object],
    exposures: Mapping[str, object],
    expiry: Mapping[str, object],
    now: datetime,
) -> dict[str, object]:
    es, aggregate = _child(market, "es"), _child(greeks, "aggregate")
    market_ages = _ages(now, market.get("as_of"), es.get("source_at"), es.get("observed_at"))
    greek_ages = _ages(now, greeks.get("as_of"), aggregate.get("as_of"))
    option_age = _age(now, options.get("as_of"))
    exposure_age = _age(now, exposures.get("as_of"))
    snapshot_age = finite_float(expiry.get("snapshot_age_seconds"))
    exposure_snapshot_age = (
        exposure_age + snapshot_age
        if exposure_age is not None and snapshot_age is not None and snapshot_age >= 0
        else None
    )
    return {
        "market_age_seconds": _round(_worst_age(market_ages)),
        "greek_age_seconds": _round(_worst_age(greek_ages)),
        "iv_age_seconds": _round(
            _worst_age([age for age in (option_age, exposure_snapshot_age) if age is not None])
        ),
        "option_frame_age_seconds": _round(option_age),
        "exposure_frame_age_seconds": _round(exposure_age),
        "exposure_snapshot_age_seconds": _round(exposure_snapshot_age),
    }


def _risk_payload(
    greeks: Mapping[str, object],
    expiry: Mapping[str, object],
    coverage: Mapping[str, object],
    structure_risk: Mapping[str, object],
) -> dict[str, object]:
    aggregate, weighted = _child(greeks, "aggregate"), _child(expiry, "oi_weighted")
    return {
        "role": "quality_gate_and_bounded_confidence_risk_only",
        "direction_score_adjustment": 0.0,
        **structure_risk,
        "gross_gamma_abs": finite_float(aggregate.get("gross_gamma_abs")),
        "gross_charm_5m_abs": finite_float(aggregate.get("gross_charm_5m_abs")),
        "gross_vanna_1vol_abs": finite_float(aggregate.get("gross_vanna_1vol_abs")),
        "net_gamma_ratio_proxy": finite_float(weighted.get("net_gamma_ratio")),
        "vanna_exposure_proxy": finite_float(weighted.get("vex_proxy")),
        "charm_exposure_proxy": finite_float(weighted.get("cex_proxy")),
        "complete_pair_ratio": coverage.get("complete_pair_ratio"),
        "core_complete_pair_ratio": coverage.get("core_complete_pair_ratio"),
        "left_wing_paired_strikes": coverage.get("left_wing_paired_strikes"),
        "right_wing_paired_strikes": coverage.get("right_wing_paired_strikes"),
        "sign_convention": str(expiry.get("sign_convention") or "calls_positive_puts_negative"),
        "dealer_position_sign": "unknown",
        "direction": "unknown",
    }


def _policy(settings: Mapping[str, object] | object | None, session: str) -> _Policy:
    prefix = "rth" if session == "rth" else "gth"
    default = SpringGammaV3Settings()

    def get(*names: str) -> object:
        for source in (settings, default):
            for name in names:
                if isinstance(source, Mapping) and name in source:
                    return source[name]
                if source is not None and hasattr(source, name):
                    return getattr(source, name)
        raise AttributeError(names[0])

    return _Policy(
        bool(get("enabled")),
        bool(get("report_enabled")),
        int(get("prediction_interval_seconds")),
        tuple(int(value) for value in get("horizons_minutes")),
        float(get(f"{prefix}_greek_max_age_seconds")),
        float(get(f"{prefix}_iv_max_age_seconds")),
        float(get(f"{prefix}_min_pair_ratio", "min_pair_ratio")),
        float(get(f"{prefix}_min_iv_coverage", "min_iv_coverage", "min_iv")),
        float(get(f"{prefix}_min_delta_coverage", "min_delta_coverage", "min_delta")),
        float(get(f"{prefix}_min_oi_coverage", "min_oi_coverage", "min_oi")),
        int(get(f"{prefix}_min_paired_strikes", "min_paired_strikes")),
        float(get("min_probability")),
        float(get("min_margin")),
    )


def _safe_level(level: Mapping[str, object] | None) -> dict[str, object]:
    if not isinstance(level, Mapping):
        return {}
    allowed: dict[str, object] = {}
    for key in ("phase", "thesis", "level_kind", "quality"):
        if key in level and level.get(key) is not None:
            allowed[key] = str(level[key])
    for key in ("level", "spot", "distance"):
        if key in level:
            allowed[key] = finite_float(level.get(key))
    if "quality_ok" in level:
        allowed["quality_ok"] = (
            level.get("quality_ok") if isinstance(level.get("quality_ok"), bool) else None
        )
    if finite_float(allowed.get("distance")) is None:
        spot, price_level = finite_float(allowed.get("spot")), finite_float(allowed.get("level"))
        if spot is not None and price_level is not None:
            allowed["distance"] = spot - price_level
    if "quality" not in allowed and isinstance(allowed.get("quality_ok"), bool):
        allowed["quality"] = "ok" if allowed["quality_ok"] else "unavailable"
    return allowed


def _spring_path_sign(level: Mapping[str, object]) -> int:
    quality = level.get("quality")
    quality_ok = quality is True or str(quality or "").lower() in _GOOD_LEVEL_QUALITY
    if (
        str(level.get("thesis") or "") != "fade"
        or str(level.get("phase") or "") not in _SPRING_PHASES
        or finite_float(level.get("level")) is None
        or finite_float(level.get("distance")) is None
        or not quality_ok
    ):
        return 0
    return _FADE_SIGN.get(str(level.get("level_kind") or ""), 0)


def _session(
    market: Mapping[str, object],
    requested: str | None,
) -> tuple[str, str | None]:
    segment = str(_child(market, "diagnostics").get("segment") or "").lower()
    derived = "rth" if segment == "rth" else "gth" if segment in _GTH_SEGMENTS else "unknown"
    if requested is None:
        return derived, None if derived != "unknown" else "market_session_unknown"
    normalized = str(requested).lower()
    if normalized not in {"rth", "gth"}:
        return "unknown", "requested_session_invalid"
    return (
        (normalized, "market_session_mismatch")
        if derived != "unknown" and normalized != derived
        else (normalized, None)
    )


def _age_gate(
    reasons: list[str],
    value: object,
    maximum: float,
    missing: str,
    stale: str,
) -> None:
    age = finite_float(value)
    if age is None:
        reasons.append(missing)
    elif age < -1.0:
        reasons.append(f"{stale}_future_timestamp")
    elif age > maximum:
        reasons.append(stale)


def _ages(now: datetime, *values: object) -> list[float]:
    return [age for value in values if (age := _age(now, value)) is not None]


def _age(now: datetime, value: object) -> float | None:
    parsed = _datetime(value)
    return (now - parsed).total_seconds() if parsed is not None else None


def _datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    return (
        parsed.replace(tzinfo=timezone.utc)
        if parsed.tzinfo is None
        else parsed.astimezone(timezone.utc)
    )


def _worst_age(values: list[float]) -> float | None:
    future = [value for value in values if value < -1.0]
    return min(future) if future else max(values, default=None)


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


def _token(value: object) -> str:
    return str(getattr(value, "value", value) or "").lower()


def _bounded(value: object, scale: float) -> float | None:
    number = finite_float(value)
    return _round(np.tanh(number / scale)) if number is not None else None


def _ratio(numerator: int, denominator: int) -> float | None:
    return _round(numerator / denominator) if denominator else None


def _pair_ratio(rows: Sequence[Mapping[str, object]]) -> float | None:
    return _ratio(sum(_complete_pair(row) for row in rows), len(rows))


def _round(value: object) -> float | None:
    number = finite_float(value)
    return round(number, 6) if number is not None else None


def _sign(value: object) -> int:
    number = finite_float(value)
    return 0 if number is None or abs(number) < 1e-12 else 1 if number > 0 else -1


def _direction_name(sign: int, *, flat: str = "abstain") -> str:
    return "up" if sign > 0 else "down" if sign < 0 else flat


def _probabilities(score: object) -> tuple[float | None, float | None]:
    value = finite_float(score)
    if value is None:
        return None, None
    p_up = (1.0 + float(np.clip(value, -1.0, 1.0))) / 2.0
    return _round(p_up), _round(1.0 - p_up)


def _confident(score: object, policy: _Policy) -> bool:
    p_up, p_down = _probabilities(score)
    return bool(
        p_up is not None
        and p_down is not None
        and max(p_up, p_down) >= policy.min_probability
        and abs(p_up - p_down) >= policy.min_margin
    )


def _cap_probabilities(
    probabilities: tuple[float | None, float | None],
    cap: float,
) -> tuple[float | None, float | None]:
    p_up, p_down = probabilities
    if p_up is None or p_down is None or max(p_up, p_down) <= cap:
        return probabilities
    return (
        (_round(cap), _round(1.0 - cap))
        if p_up >= p_down
        else (
            _round(1.0 - cap),
            _round(cap),
        )
    )


def _fingerprint(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(
        _canonical(payload),
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _canonical(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _canonical(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical(item) for item in value]
    if isinstance(value, datetime):
        return as_utc(value).isoformat()
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (str, int, bool)) or value is None:
        return value
    to_dict = getattr(value, "to_dict", None)
    return _canonical(to_dict()) if callable(to_dict) else str(getattr(value, "value", value))


__all__ = [
    "CALIBRATION_STATUS",
    "MODEL_VERSION",
    "SCHEMA_VERSION",
    "build_spring_gamma_v3_shadow",
]
