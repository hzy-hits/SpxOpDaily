"""GTH expansion-to-contraction evidence for the manual iron-condor contract."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from spx_spark.application.order_map.candidate_factory import _map, _number, _time
from spx_spark.application.market_features.session_episode import (
    ATM_STRADDLE_GTH_ACTIVE_EXPANSION_FRACTION,
    ATM_STRADDLE_GTH_ACTIVE_WINDOW_SECONDS,
)

GTH_EVIDENCE_CONTRACT_HASH = (
    "sha256:901d69561a93d3503495210c1f49f5c35791695065c0b298470c4e554dd446ba"
)
GTH_TRANSITION_VERSION = "gth_short_gamma_expansion_to_contraction.v2"
GTH_MAX_EXACT_QUOTE_AGE_SECONDS = 30.0
GTH_MAX_EXACT_QUOTE_SKEW_SECONDS = 10.0
GTH_MIN_STRADDLE_OBSERVATIONS = 30
GTH_MIN_EXPANSION_FRACTION = ATM_STRADDLE_GTH_ACTIVE_EXPANSION_FRACTION
GTH_MIN_CONTRACTION_FROM_HIGH_FRACTION = 0.08
GTH_MIN_STRADDLE_DECAY_15M = 0.03
GTH_MIN_PEAK_AGE_SECONDS = 300.0
GTH_MAX_PEAK_AGE_SECONDS = ATM_STRADDLE_GTH_ACTIVE_WINDOW_SECONDS
GTH_MAX_ABS_15M_MOVE_ATR = 1.25
GTH_MAX_GCR10 = 0.20


def gth_iron_condor_transition(
    facts: Mapping[str, Any], *, now: datetime
) -> dict[str, Any]:
    """Detect a causal GTH vol expansion that has started to contract."""

    now = _utc(now)
    session_mode = str(_map(facts.get("session")).get("mode") or "").lower()
    volatility = _map(facts.get("volatility"))
    extrema = _map(volatility.get("atm_straddle_gth_extrema"))
    path = _map(facts.get("path"))
    current = _number(volatility.get("atm_straddle_mid"))
    session_high = _number(volatility.get("atm_straddle_gth_high"))
    session_low = _number(volatility.get("atm_straddle_gth_low"))
    active_high = _number(extrema.get("active_high"))
    active_high_at = _time(extrema.get("active_high_at"))
    active_base_low = _number(extrema.get("active_base_low"))
    active_base_low_at = _time(extrema.get("active_base_low_at"))
    active_episode_available = None not in {
        active_high,
        active_high_at,
        active_base_low,
        active_base_low_at,
    }
    high = active_high if active_episode_available else session_high
    low = session_low
    peak_base_low = (
        active_base_low
        if active_episode_available
        else _number(
            volatility.get("atm_straddle_gth_high_base_low")
            or extrema.get("high_base_low")
        )
    )
    observations = int(_number(volatility.get("atm_straddle_gth_observations") or extrema.get("observations")) or 0)
    high_at = (
        active_high_at
        if active_episode_available
        else _time(volatility.get("atm_straddle_gth_high_at") or extrema.get("high_at"))
    )
    low_at = _time(volatility.get("atm_straddle_gth_low_at") or extrema.get("low_at"))
    peak_base_low_at = (
        active_base_low_at
        if active_episode_available
        else _time(
            volatility.get("atm_straddle_gth_high_base_low_at")
            or extrema.get("high_base_low_at")
        )
    )
    # Backward-compatible migration for a session state written before the
    # causal peak basis was persisted.  This fallback is valid only when the
    # recorded session low demonstrably preceded the peak.
    if (
        peak_base_low is None
        and peak_base_low_at is None
        and low is not None
        and low_at is not None
        and high_at is not None
        and low_at < high_at
    ):
        peak_base_low = low
        peak_base_low_at = low_at
    decay_15m = _number(volatility.get("atm_straddle_decay_15m"))
    iv_change_5m = _number(volatility.get("atm_iv_change_5m"))
    iv_change_15m = _number(volatility.get("atm_iv_change_15m"))
    impulse_15m = _number(path.get("impulse_15m_points"))
    atr_5m = _number(path.get("atr_5m"))
    expansion = (
        (high - peak_base_low) / peak_base_low
        if high is not None
        and peak_base_low is not None
        and high > peak_base_low > 0.0
        else None
    )
    contraction = (
        (high - current) / high
        if high is not None and current is not None and high > 0.0
        else None
    )
    peak_age = (now - high_at).total_seconds() if high_at is not None else None
    move_atr = (
        abs(impulse_15m) / atr_5m
        if impulse_15m is not None and atr_5m is not None and atr_5m > 0.0
        else None
    )

    reasons: list[str] = []
    if session_mode != "gth":
        reasons.append("gth_transition_session_mismatch")
    if observations < GTH_MIN_STRADDLE_OBSERVATIONS:
        reasons.append("gth_transition_observations_insufficient")
    if None in {current, high, high_at, decay_15m, iv_change_5m, iv_change_15m}:
        reasons.append("gth_transition_volatility_inputs_unavailable")
    if peak_base_low is None or peak_base_low_at is None:
        reasons.append("gth_transition_expansion_basis_unavailable")
    if None in {impulse_15m, atr_5m}:
        reasons.append("gth_transition_path_inputs_unavailable")
    if (
        high_at is not None
        and peak_base_low_at is not None
        and not peak_base_low_at < high_at
    ):
        reasons.append("gth_transition_extrema_order_invalid")
    if peak_age is None or not GTH_MIN_PEAK_AGE_SECONDS <= peak_age <= GTH_MAX_PEAK_AGE_SECONDS:
        reasons.append("gth_transition_peak_age_outside_window")
    if expansion is None or expansion < GTH_MIN_EXPANSION_FRACTION:
        reasons.append("gth_transition_expansion_too_small")
    if contraction is None or contraction < GTH_MIN_CONTRACTION_FROM_HIGH_FRACTION:
        reasons.append("gth_transition_contraction_too_small")
    if decay_15m is None or decay_15m < GTH_MIN_STRADDLE_DECAY_15M:
        reasons.append("gth_transition_straddle_not_decaying")
    if iv_change_5m is None or iv_change_5m > 0.0:
        reasons.append("gth_transition_atm_iv_5m_not_contracting")
    if iv_change_15m is None or iv_change_15m > 0.0:
        reasons.append("gth_transition_atm_iv_15m_not_contracting")
    if move_atr is None or move_atr > GTH_MAX_ABS_15M_MOVE_ATR:
        reasons.append("gth_transition_price_not_balanced")

    return {
        "schema_version": GTH_TRANSITION_VERSION,
        "status": "qualified" if not reasons else "waiting",
        "decision_effect": "gth_iron_condor_gate",
        "proxy": "atm_straddle_and_atm_iv_not_dealer_gamma",
        "extrema_scope": (
            "rolling_local_expansion_episode"
            if active_episode_available
            else "legacy_session_extrema"
        ),
        "observations": observations,
        "straddle_current": current,
        "straddle_high": high,
        "straddle_low": low,
        "straddle_session_high": session_high,
        "straddle_peak_base_low": peak_base_low,
        "straddle_peak_base_low_at": (
            peak_base_low_at.isoformat() if peak_base_low_at is not None else None
        ),
        "straddle_expansion_fraction": round(expansion, 8) if expansion is not None else None,
        "straddle_contraction_from_high_fraction": (
            round(contraction, 8) if contraction is not None else None
        ),
        "straddle_decay_15m": decay_15m,
        "atm_iv_change_5m": iv_change_5m,
        "atm_iv_change_15m": iv_change_15m,
        "peak_at": high_at.isoformat() if high_at is not None else None,
        "peak_age_seconds": round(peak_age, 3) if peak_age is not None else None,
        "move_15m_atr": round(move_atr, 8) if move_atr is not None else None,
        "reasons": list(dict.fromkeys(reasons)),
    }


def gth_iron_condor_gate_failures(
    candidate: Mapping[str, Any], session_mode: str
) -> list[dict[str, Any]]:
    """Return the shared GTH execution gates used by lock and ranker."""

    if session_mode != "gth":
        return []
    gates: list[dict[str, Any]] = []
    transition = _map(candidate.get("gth_transition"))
    if transition.get("status") != "qualified":
        gates.append(
            {
                "gate": "gth_iron_condor_transition_unconfirmed",
                "actual": transition.get("status"),
                "threshold": "qualified",
            }
        )
    legs = candidate.get("legs") or ()
    if len(legs) != 4 or any(_map(leg).get("provider") != "ibkr" for leg in legs):
        gates.append(
            {
                "gate": "gth_iron_condor_ibkr_quote_required",
                "actual": [_map(leg).get("provider") for leg in legs],
                "threshold": "four_fresh_ibkr_legs",
            }
        )
    gamma_risk = _map(candidate.get("gamma_risk"))
    gcr10 = _number(gamma_risk.get("gcr10"))
    if gamma_risk.get("status") != "ready" or gcr10 is None:
        gates.append(
            {
                "gate": "gth_iron_condor_gamma_risk_unavailable",
                "actual": gamma_risk.get("reason") or gamma_risk.get("status"),
                "threshold": (
                    f"four_leg_gamma_age<={GTH_MAX_EXACT_QUOTE_AGE_SECONDS:g}s"
                ),
            }
        )
    elif gcr10 > GTH_MAX_GCR10:
        gates.append(
            {
                "gate": "gth_iron_condor_gamma_risk_hot",
                "actual": gcr10,
                "threshold": f"<={GTH_MAX_GCR10}",
            }
        )
    return gates


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("strategy decision time must be timezone-aware")
    return value.astimezone(timezone.utc)
