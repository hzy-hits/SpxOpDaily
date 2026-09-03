"""GTH expansion-to-contraction evidence for the manual iron-condor contract."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from spx_spark.application.order_map.candidate_factory import _map, _number, _time

GTH_EVIDENCE_CONTRACT_HASH = (
    "sha256:0a7b31adf74ce22862bdb573782f85792bc732aebe74a2cbe081de57b224926f"
)
GTH_TRANSITION_VERSION = "gth_short_gamma_expansion_to_contraction.v1"
GTH_MIN_STRADDLE_OBSERVATIONS = 30
GTH_MIN_EXPANSION_FRACTION = 0.10
GTH_MIN_CONTRACTION_FROM_HIGH_FRACTION = 0.08
GTH_MIN_STRADDLE_DECAY_15M = 0.03
GTH_MIN_PEAK_AGE_SECONDS = 300.0
GTH_MAX_PEAK_AGE_SECONDS = 7_200.0
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
    high = _number(volatility.get("atm_straddle_gth_high"))
    low = _number(volatility.get("atm_straddle_gth_low"))
    observations = int(_number(volatility.get("atm_straddle_gth_observations") or extrema.get("observations")) or 0)
    high_at = _time(volatility.get("atm_straddle_gth_high_at") or extrema.get("high_at"))
    low_at = _time(volatility.get("atm_straddle_gth_low_at") or extrema.get("low_at"))
    decay_15m = _number(volatility.get("atm_straddle_decay_15m"))
    iv_change_5m = _number(volatility.get("atm_iv_change_5m"))
    iv_change_15m = _number(volatility.get("atm_iv_change_15m"))
    impulse_15m = _number(path.get("impulse_15m_points"))
    atr_5m = _number(path.get("atr_5m"))
    expansion = (
        (high - low) / low
        if high is not None and low is not None and high > low > 0.0
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
    if None in {current, high, low, high_at, low_at, decay_15m, iv_change_5m, iv_change_15m}:
        reasons.append("gth_transition_volatility_inputs_unavailable")
    if None in {impulse_15m, atr_5m}:
        reasons.append("gth_transition_path_inputs_unavailable")
    if high_at is not None and low_at is not None and not low_at < high_at:
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
        "observations": observations,
        "straddle_current": current,
        "straddle_high": high,
        "straddle_low": low,
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
    if gamma_risk.get("status") != "ready" or gcr10 is None or gcr10 > GTH_MAX_GCR10:
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
