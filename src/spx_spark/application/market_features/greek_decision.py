"""Use 0DTE Greeks for contract confidence and exits, never index direction."""

from __future__ import annotations

from typing import Mapping

from spx_spark.settings.market_features import MarketFeatureSettings


def build_greek_decision(
    reference: Mapping[str, object] | None,
    candidates: list[Mapping[str, object]],
    *,
    macro_event: Mapping[str, object] | None,
    policy: MarketFeatureSettings,
) -> dict[str, object]:
    reference = reference or {}
    coverage = reference.get("coverage") if isinstance(reference.get("coverage"), Mapping) else {}
    aggregate = reference.get("aggregate") if isinstance(reference.get("aggregate"), Mapping) else {}
    usable_ratio = _number(coverage.get("usable_ratio")) or 0.0
    oi_ratio = _number(coverage.get("oi_ratio")) or 0.0
    decision_grade = bool(
        reference.get("status") == "ok"
        and aggregate.get("quality") == "ok"
        and usable_ratio >= policy.greek_decision_min_coverage
        and oi_ratio >= policy.greek_decision_min_coverage
    )
    rows = {
        str(row.get("contract_id") or ""): row
        for row in reference.get("contracts") or []
        if isinstance(row, Mapping) and row.get("contract_id")
    }
    scores: dict[str, dict[str, object]] = {}
    for candidate in candidates:
        contract_id = str(candidate.get("contract_id") or "")
        greek = rows.get(contract_id)
        if greek is None:
            scores[contract_id] = {
                "mode": "explanation_only",
                "confidence_adjustment": 0.0,
                "reasons": ["focused_contract_greeks_unavailable"],
            }
            continue
        scores[contract_id] = _score_contract(
            greek,
            candidate,
            macro_event=macro_event or {},
            decision_grade=decision_grade,
            policy=policy,
        )
    return {
        "schema_version": 1,
        "mode": "decision_grade" if decision_grade else "explanation_only",
        "direction_authority": "none",
        "coverage": {
            "usable_ratio": usable_ratio,
            "oi_ratio": oi_ratio,
            "required_ratio": policy.greek_decision_min_coverage,
        },
        "macro_mode": (macro_event or {}).get("mode"),
        "contract_scores": scores,
        "fallback_reason": None if decision_grade else "coverage_or_quality_below_decision_gate",
    }


def _score_contract(
    greek: Mapping[str, object],
    candidate: Mapping[str, object],
    *,
    macro_event: Mapping[str, object],
    decision_grade: bool,
    policy: MarketFeatureSettings,
) -> dict[str, object]:
    reasons: list[str] = []
    score = 0.0
    delta = abs(_number(greek.get("delta")) or 0.0)
    gamma = _number(greek.get("gamma_per_point"))
    speed = _number(greek.get("speed_gamma_per_point"))
    color = _number(greek.get("color_gamma_per_minute"))
    theta = _number(greek.get("theta_per_minute"))
    vanna = _number(greek.get("vanna_delta_per_vol_point"))
    mid = _number(candidate.get("current_mid")) or _number(candidate.get("decision_mid"))
    scenarios = {
        str(row.get("name")): row
        for row in greek.get("scenarios") or []
        if isinstance(row, Mapping)
    }
    theta_loss = _scenario_loss(mid, scenarios.get("clock_plus_15m"))
    iv_crush_loss = _scenario_loss(mid, scenarios.get("iv_down_3vol"))
    if policy.greek_target_delta_min <= delta <= policy.greek_target_delta_max:
        score += 10.0
        reasons.append("delta_in_configured_0dte_band")
    else:
        score -= 10.0
        reasons.append("delta_outside_configured_0dte_band")
    if gamma is not None and gamma > 0:
        score += 5.0
        reasons.append("gamma_convexity_present")
    if speed is not None and speed >= 0:
        score += 5.0
        reasons.append("speed_supports_upside_convexity")
    if color is not None and color >= 0:
        score += 5.0
        reasons.append("color_not_eroding_gamma")
    elif color is not None:
        score -= 5.0
        reasons.append("color_erodes_gamma")
    if theta_loss is not None and theta_loss > policy.greek_max_theta_15m_loss_fraction:
        score -= 15.0
        reasons.append("theta_15m_wait_cost_high")
    if iv_crush_loss is not None and iv_crush_loss > policy.greek_max_iv_crush_loss_fraction:
        score -= 15.0
        reasons.append("iv_crush_loss_high")
    if macro_event.get("mode") == "post_event" and vanna is not None and vanna > 0:
        score -= 10.0
        reasons.append("post_event_vanna_turns_iv_crush_into_delta_drag")
    quality = greek.get("quality") if isinstance(greek.get("quality"), Mapping) else {}
    effective_mode = "decision_grade" if decision_grade and quality.get("status") == "ok" else "explanation_only"
    return {
        "mode": effective_mode,
        "confidence_adjustment": max(min(score, 25.0), -25.0) if effective_mode == "decision_grade" else 0.0,
        "raw_score": score,
        "delta": delta,
        "gamma_per_point": gamma,
        "speed_gamma_per_point": speed,
        "color_gamma_per_minute": color,
        "theta_per_minute": theta,
        "vanna_delta_per_vol_point": vanna,
        "theta_15m_loss_fraction": theta_loss,
        "iv_down_3vol_loss_fraction": iv_crush_loss,
        "exit_flags": {
            "delta_saturated": delta >= policy.greek_delta_saturation,
            "color_eroding_gamma": color is not None and color < 0,
            "post_event_vanna_drag": macro_event.get("mode") == "post_event" and vanna is not None and vanna > 0,
        },
        "reasons": reasons,
    }


def _scenario_loss(mid: float | None, scenario: Mapping[str, object] | None) -> float | None:
    price = _number((scenario or {}).get("reference_price"))
    if mid is None or mid <= 0 or price is None:
        return None
    return max((mid - price) / mid, 0.0)


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) else None
