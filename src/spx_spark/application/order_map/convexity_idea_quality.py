"""Freshness and completeness gates for the discretionary convexity packet."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping

from spx_spark.analytics.options.pricing import finite_float


_LEVEL_KEYS = ("put_wall", "flip_low", "flip_high", "call_wall")
_SHADOW_MAX_AGE_SECONDS = 120.0


def build_wall_probability_context(
    payload: Mapping[str, Any],
    *,
    mandate: Mapping[str, Any],
    now: datetime,
) -> dict[str, Any]:
    """Keep only current-session horizons that remain usable before 13:00 ET."""

    evaluated_at = _utc(now)
    shadow = _mapping(payload.get("spring_gamma_v3_shadow"))
    source = _mapping(shadow.get("wall_probability"))
    raw_horizons = _mapping(source.get("wall_probabilities"))
    hard_exit_at = _datetime(mandate.get("hard_exit_at"))
    phase = str(mandate.get("phase") or "")
    quote_max_age = 90.0 if phase == "gth_preparation" else 15.0
    observed_at = _datetime(source.get("as_of") or shadow.get("as_of"))
    shadow_age = (
        (evaluated_at - observed_at).total_seconds()
        if observed_at is not None
        else None
    )
    expected_expiry = str(mandate.get("trading_date") or "").replace("-", "")
    source_expiry = str(source.get("expiry") or shadow.get("expiry") or "")
    source_reasons: list[str] = []
    if str(shadow.get("status") or "").lower() not in {"ready", "abstain"}:
        source_reasons.append("spring_shadow_not_ready")
    if str(source.get("probability_status") or "").lower() not in {
        "ready",
        "partial",
    }:
        source_reasons.append("wall_probability_source_not_ready")
    if expected_expiry and source_expiry != expected_expiry:
        source_reasons.append("wall_probability_expiry_mismatch")
    if shadow_age is None:
        source_reasons.append("spring_shadow_as_of_missing")
    elif shadow_age < -2.0 or shadow_age > _SHADOW_MAX_AGE_SECONDS:
        source_reasons.append("spring_shadow_stale_or_future")

    horizons: dict[str, dict[str, Any]] = {}
    usable_count = 0
    for horizon in ("15m", "30m", "60m"):
        levels: dict[str, Any] = {}
        for name in _LEVEL_KEYS:
            row = _mapping(_mapping(raw_horizons.get(horizon)).get(name))
            if not row:
                continue
            planned_exit = _datetime(row.get("planned_exit_at"))
            quote_age = _number(row.get("source_quote_age_seconds"))
            reasons = list(source_reasons)
            if str(row.get("status") or "").lower() != "available":
                reasons.append("probability_row_not_available")
            if row.get("holding_window_valid") is False:
                reasons.append("holding_window_invalid")
            if planned_exit is None:
                reasons.append("planned_exit_missing")
            elif planned_exit <= evaluated_at:
                reasons.append("probability_horizon_elapsed")
            elif hard_exit_at is None or planned_exit > hard_exit_at:
                reasons.append("outside_1300_hard_exit")
            if quote_age is None:
                reasons.append("source_quote_age_missing")
            elif quote_age < 0 or quote_age > quote_max_age:
                reasons.append("source_quote_stale")
            reasons = list(dict.fromkeys(reasons))
            usable = not reasons
            compact = {
                key: row.get(key)
                for key in (
                    "status",
                    "level",
                    "prob_close_beyond",
                    "prob_touch",
                    "source_strike",
                    "source_right",
                    "source_iv",
                    "source_quote_age_seconds",
                    "planned_exit_at",
                    "holding_window_valid",
                    "probability_semantics",
                    "touch_probability_semantics",
                )
            }
            compact["strategy_usable"] = usable
            compact["strategy_gate_reasons"] = reasons
            if not usable:
                compact["status"] = reasons[0]
                compact["prob_close_beyond"] = None
                compact["prob_touch"] = None
            else:
                usable_count += 1
            levels[name] = compact
        if levels:
            horizons[horizon] = levels
    remaining = mandate.get("horizon_to_1300_min")
    return {
        "status": "ready" if usable_count else "unavailable",
        "source_status": source.get("probability_status") or "unavailable",
        "as_of": observed_at.isoformat() if observed_at else None,
        "age_seconds": round(shadow_age, 2) if shadow_age is not None else None,
        "maximum_shadow_age_seconds": _SHADOW_MAX_AGE_SECONDS,
        "maximum_quote_age_seconds": quote_max_age,
        "source_gate_reasons": list(dict.fromkeys(source_reasons)),
        "probability_semantics": source.get("probability_semantics")
        or "risk_neutral_not_physical",
        "touch_probability_semantics": source.get("touch_probability_semantics")
        or "reflection_heuristic_not_calibrated_or_physical",
        "horizons": horizons,
        "to_1300": {
            "status": "not_calculated",
            "horizon_minutes": remaining,
            "reason": "exact_remaining_horizon_not_produced_by_current_probability_engine",
        },
    }


def build_quality_summary(
    payload: Mapping[str, Any],
    *,
    destination: Mapping[str, Any],
    mandate: Mapping[str, Any],
) -> dict[str, Any]:
    """Require complete RTH state, real NBBO pairs, and all pricing dependencies."""

    coverage = _mapping(payload.get("strike_price_coverage"))
    shadow = _mapping(payload.get("spring_gamma_v3_shadow"))
    market_state = _mapping(shadow.get("rth_market_state"))
    availability = _mapping(market_state.get("input_availability"))
    overlay = _mapping(shadow.get("option_overlay"))
    pricing = _mapping(payload.get("pricing_reference"))
    decision = _mapping(payload.get("level_decision"))
    complete_pairs = _number(coverage.get("complete_pair_count"))
    target_pairs = _number(coverage.get("target_pair_count"))
    age_p90 = _number(coverage.get("pair_quote_age_p90_seconds"))
    available_inputs = _number(availability.get("available_count"))
    required_inputs = _number(availability.get("required_count"))
    expiry = str(payload.get("expiry") or "")
    frame_expiry = str(
        _mapping(payload.get("option_structure_frame")).get("front_expiry") or ""
    )
    rth = mandate.get("phase") in {"rth_warmup", "rth_active"}
    reasons: list[str] = []
    if destination.get("status") != "ready":
        gate = destination.get("gate_reasons") or [
            destination.get("quality") or "unavailable"
        ]
        reasons.append("destination:" + ",".join(str(item) for item in gate))
    if (rth and overlay.get("status") != "ready") or (
        not rth and overlay.get("status") not in {"ready", None}
    ):
        overlay_reasons = [
            f"option_overlay:{reason}" for reason in overlay.get("reasons") or []
        ][:4]
        reasons.extend(overlay_reasons or ["option_overlay_unavailable"])
    if rth and str(market_state.get("status") or "").lower() != "ready":
        reasons.append("rth_market_state_not_ready")
    if rth and (
        required_inputs is None
        or required_inputs < 8
        or available_inputs is None
        or available_inputs < required_inputs
    ):
        reasons.append("rth_market_state_inputs_incomplete")
    if (rth and decision.get("quality_ok") is not True) or (
        not rth and decision.get("quality_ok") is False
    ):
        reasons.append("level_decision_quality_failed")
    if (rth and pricing.get("pricing_allowed") is not True) or (
        not rth and pricing.get("pricing_allowed") is False
    ):
        reasons.append("pricing_gate_failed")
    if frame_expiry and expiry and frame_expiry != expiry:
        reasons.append("option_expiry_identity_mismatch")
    if rth and (complete_pairs is None or complete_pairs <= 0):
        reasons.append("complete_cp_pairs_unavailable")
    if rth and (target_pairs is None or target_pairs < 13):
        reasons.append("target_cp_pair_count_invalid")
    if rth and complete_pairs is not None and complete_pairs < 13:
        reasons.append("minimum_complete_cp_pairs_not_met")
    if rth and coverage.get("nbbo_interpolation") is not False:
        reasons.append("nbbo_interpolation_not_explicitly_false")
    if rth and age_p90 is not None and age_p90 > 15.0:
        reasons.append("pair_quote_age_p90_exceeds_15s")
    if rth and age_p90 is None:
        reasons.append("pair_quote_age_p90_missing")
    reasons = list(dict.fromkeys(reasons))
    return {
        "status": "ready" if not reasons else "degraded",
        "pricing_allowed": pricing.get("pricing_allowed"),
        "pricing_gate_state": pricing.get("gate_state"),
        "option_overlay_status": overlay.get("status"),
        "market_state_status": market_state.get("status"),
        "market_state_available_count": available_inputs,
        "market_state_required_count": required_inputs,
        "complete_pair_count": complete_pairs,
        "target_pair_count": target_pairs,
        "pair_quote_age_p90_seconds": coverage.get("pair_quote_age_p90_seconds"),
        "nbbo_interpolation": coverage.get("nbbo_interpolation"),
        "reasons": reasons[:12],
    }


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    return finite_float(value)


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
    if value.tzinfo is None:
        raise ValueError("timezone-aware datetime required")
    return value.astimezone(timezone.utc)
