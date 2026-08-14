"""Deterministic regime dimensions for strategy selection."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping


from spx_spark.analytics.options.strategy_payoff import (
    DEFAULT_MANAGEMENT_POLICY,
    PIN_BUTTERFLY_MANAGEMENT_POLICY,
    ManagementPolicy,
)

# Forward mark horizons for strategy_outcomes (v3). Frozen code constant.
MARK_HORIZONS_MINUTES: tuple[int, ...] = (1, 2, 3, 4, 5, 7, 10, 15, 20)

__all__ = (
    "DEFAULT_MANAGEMENT_POLICY",
    "DEFAULT_STRATEGY_POLICY",
    "MARK_HORIZONS_MINUTES",
    "ManagementPolicy",
    "PIN_BUTTERFLY_MANAGEMENT_POLICY",
    "StrategyPolicy",
    "assess_regime",
)


@dataclass(frozen=True, slots=True)
class StrategyPolicy:
    policy_version: str = "strategy_policy.bootstrap.v18"
    # v18: GTH keeps one human direction at a time and sticks the winner for
    # gth_winner_stick_seconds. Rank may not flip UP/DOWN/NEUTRAL, and delivery
    # may not print the opposite side, until that hysteresis expires.
    # v17: one perception contract, session-selected owners. Cash HMM may own
    # RTH path_direction (SPX). Globex HMM never owns GTH path_direction; it
    # only publishes cross_state (NQ/YM/RTY vs ES). GTH direction is ES path.
    # HMM still cannot skip hard gates or order.
    # v16: session-selected index HMM owns path_state when the cash (RTH) or
    # globex-futures (GTH) basket is ready. ES path remains the fallback and
    # a VWAP direction check. HMM still cannot skip hard gates or order.
    # v15: RTH pin butterflies no longer require OI-GEX as a capability gate.
    # STABLE_PIN management holds to 15:45 ET with trail; debit verticals keep
    # the v1 20-minute time stop and 50% premium stop.
    # v14: RTH pin butterflies must keep spot inside the tent, wait until
    # minutes_to_close <= 12 per width point (5-wide from 15:00 ET), and not
    # pin a body while a wall still sits inside 1.5x remaining EM outside the
    # wings. Card text prints the three legs. PIN_STABLE itself is unchanged
    # so iron-condor 12:30 timing does not move.
    # v13: RTH confirmation stays open for two extra 5m bars so a human card
    # can still print; session-episode reclaim expires at the same 60%
    # progress cap as debit chase; flood caps are per session_mode so GTH
    # scans cannot silence RTH.
    # v12: "20Δ 以下" means at-or-below 20, never the richer nearest strike.
    # GTH debit longs use the same 5–20Δ ladder, not 25Δ.
    # v11: short-leg band is 5–20Δ (naked short delta, not 25). GTH iron
    # condors are path-forwarded to the 12:00–13:00 ET clearing window.
    # v10: sell 5–25Δ short legs with a 10-point defined-risk wing; do not pair
    # 25Δ shorts with 5Δ longs. GTH debit longs must sit inside remaining EM.
    # v9: GTH desk map is a live structure scan, not an empty health heartbeat.
    # Always recompute the iron condor from 1-minute quotes. Widen the
    # Call/Put/butterfly scan around spot±5 and 5–20Δ anchors. Winners still
    # push only on trade_ready; unpassed debit spreads are not 可看.
    # v8: GTH enumerates 5-50pt Call/Put debit verticals and butterflies from
    # quotes no older than 60s, then pushes only rank winners on trade_ready.
    # v7: GTH human cards authorize only NEUTRAL session-advance; dip-reclaim
    # requires an aged bullish regime. Continuation m1 stays observe-only.
    trend_score: float = 6.0
    trend_efficiency: float = 0.45
    trend_max_vwap_crosses: float = 2.0
    trend_min_breadth: float = 0.55
    balanced_max_score: float = 3.0
    balanced_max_efficiency: float = 0.30
    balanced_min_vwap_crosses: float = 2.0
    quote_max_age_seconds: float = 15.0
    quote_max_skew_seconds: float = 2.0
    gth_quote_max_age_seconds: float = 60.0
    gth_quote_max_skew_seconds: float = 60.0
    gth_widths: tuple[float, ...] = (5.0, 10.0, 15.0, 20.0, 30.0, 40.0, 50.0)
    gth_long_offsets: tuple[float, ...] = (-5.0, 0.0, 5.0)
    gth_delta_targets: tuple[float, ...] = (0.20, 0.15, 0.10, 0.05)
    iron_condor_short_deltas: tuple[float, ...] = (0.20, 0.15, 0.10, 0.05)
    iron_condor_wing_width: float = 10.0
    opportunity_ttl_seconds: float = 300.0
    min_target_room_ratio: float = 1.5
    failed_break_min_target_room_ratio: float = 1.8
    max_debit_fraction: float = 0.45
    failed_break_max_debit_fraction: float = 0.40
    min_stop_atr: float = 0.25
    max_stop_atr: float = 1.0
    late_chase_distance_atr: float = 1.0
    late_chase_impulse_atr: float = 1.0
    pin_thresholds: tuple[float, ...] = (0.25, 2.5, 5.0, 5.0, 8.0, 0.35, 0.55)
    pin_body_max_center_distance_points: float = 5.0
    pin_body_max_spot_distance_points: float = 15.0
    hmm_trend_min_probability: float = 0.55
    hmm_balanced_min_probability: float = 0.50
    hmm_max_age_seconds: float = 90.0
    gth_trend_min_abs_return_points: float = 1.0
    butterfly_max_debit_fraction: float = 0.35
    butterfly_max_risk_usd: float = 1000.0
    butterfly_minutes_per_width_point: float = 12.0
    butterfly_unresolved_wall_em_multiple: float = 1.5
    # v5: debit vertical short strike may not pass the target, and width may
    # not exceed remaining 0DTE expected move. Missing EM fails closed.
    # V3-3a flood control (activated with policy_version bump to bootstrap.v2).
    candidate_cooldown_seconds: float = 300.0
    max_cards_per_direction_per_session: int = 6
    # GTH hysteresis from the start of the current direction streak, not a
    # sliding window: reprinting the same winner must not refresh the lock.
    gth_winner_stick_seconds: float = 180.0
    # Confirmation bar plus this many subsequent 5m bars remain ENTRY_WINDOW_OPEN.
    rth_setup_hold_bars: int = 2
    max_trigger_target_progress: float = 0.60

    def entry_quality_kwargs(self) -> dict[str, float]:
        names = (
            "min_target_room_ratio", "failed_break_min_target_room_ratio",
            "max_debit_fraction", "failed_break_max_debit_fraction",
            "min_stop_atr", "max_stop_atr", "late_chase_distance_atr",
            "late_chase_impulse_atr",
        )
        return {name: getattr(self, name) for name in names}


DEFAULT_STRATEGY_POLICY = StrategyPolicy()


HMM_STATE_DIRECTION = {
    "state_00": "DOWN",
    "state_01": None,
    "state_02": "UP",
}


def assess_regime(
    facts: Mapping[str, Any], policy: StrategyPolicy = DEFAULT_STRATEGY_POLICY
) -> dict[str, Any]:
    path, event = _map(facts.get("path")), _map(facts.get("event"))
    score, efficiency = _number(path.get("direction_score")), _number(path.get("efficiency_ratio_30m"))
    crosses, breadth = _number(path.get("vwap_crosses_30m")), _number(path.get("breadth_above_vwap"))
    slope, price_side = _number(path.get("vwap_slope")), str(path.get("price_vs_vwap") or "").lower()
    if not price_side:
        distance = _number(path.get("distance_to_vwap_points"))
        price_side = "above" if distance is not None and distance > 0 else "below" if distance is not None and distance < 0 else ""
    direction = "UP" if score is not None and score > 0 else "DOWN" if score is not None and score < 0 else None
    inputs = (score, efficiency, crosses, breadth, slope)
    contradictions: list[str] = []
    cross = _map(facts.get("cross_index"))
    source = str(cross.get("source") or "")
    session_mode = str(_map(facts.get("session")).get("mode") or "")
    if source == "globex_index":
        distance = _number(path.get("distance_to_vwap_points"))
        if distance is not None:
            price_side = (
                "above" if distance > 0 else "below" if distance < 0 else ""
            )
    hmm_cross = _hmm_cross_map(facts, policy)
    if hmm_cross is None:
        cross_state, cross_direction = None, None
        hmm_payload = {**_hmm_unused_payload(facts), "owns_path": False}
    else:
        cross_state, cross_direction, _cross_reasons, hmm_payload = hmm_cross
        hmm_payload = {**hmm_payload, "owns_path": False}
    hmm_owns_path = hmm_cross is not None and source == "cash_index"
    if hmm_owns_path:
        state, direction, reasons, _ = hmm_cross
        hmm_payload = {**hmm_payload, "owns_path": True}
        if (
            state == "TREND"
            and direction in {"UP", "DOWN"}
            and price_side
            and ("above" if direction == "UP" else "below") not in price_side
        ):
            state = "TRANSITION"
            contradictions.append("price_vwap_direction_conflict")
            reasons = [*reasons, "hmm_price_vwap_contradiction"]
        confidence = round(float(hmm_payload["max_state_probability"]), 2)
    else:
        if hmm_cross is not None:
            hmm_payload = {**hmm_payload, "reason": "hmm_cross_state_only_not_path"}
        state, direction, reasons, contradictions, confidence = _coordinate_path(
            facts,
            policy,
            inputs=inputs,
            price_side=price_side,
            direction=direction,
            use_es_path=source == "globex_index" or session_mode == "gth",
        )
    event_state = {
        "pre_event": "SCHEDULED_EVENT_RISK", "post_event": "POST_EVENT_DISCOVERY",
        "normal": "NORMAL",
    }.get(str(event.get("state") or "unavailable"), "UNCERTAIN")
    pin = _pin_assessment(facts, policy)
    coordinate = {
        "cash_index": "index:SPX",
        "globex_index": "future:ES",
    }.get(source) or cross.get("anchor")
    return {
        "schema_version": "regime_assessment.v1", "policy_version": policy.policy_version,
        "path_state": state, "path_direction": direction, "terminal_state": pin["terminal_state"],
        "event_state": event_state, "entry_state": "INSUFFICIENT_DATA",
        "cross_state": cross_state, "cross_direction": cross_direction,
        "coordinate": coordinate,
        "confidence": confidence,
        "reasons": reasons, "contradictions": contradictions, "pin": pin,
        "hmm": hmm_payload,
    }


def _coordinate_path(
    facts: Mapping[str, Any],
    policy: StrategyPolicy,
    *,
    inputs: tuple[Any, ...],
    price_side: str,
    direction: str | None,
    use_es_path: bool,
) -> tuple[str, str | None, list[str], list[str], float]:
    if use_es_path:
        return _es_coordinate_path(_map(facts.get("path")), policy, price_side=price_side)
    score, efficiency, crosses, breadth, slope = inputs
    contradictions: list[str] = []
    capabilities = _map(facts.get("capabilities"))
    path_capability = _map(capabilities.get("path"))
    path_capability_ready = (
        path_capability.get("ready") is True
        if path_capability
        else _map(facts.get("quality")).get("status") == "ready"
    )
    confidence = round(sum(value is not None for value in inputs) / 5, 2)
    if not path_capability_ready:
        return "UNCERTAIN", direction, ["strategy_facts_degraded"], contradictions, confidence
    if None in inputs:
        return "UNCERTAIN", direction, ["path_inputs_unavailable"], contradictions, confidence
    trend = bool(
        abs(float(score)) >= policy.trend_score
        and float(efficiency) >= policy.trend_efficiency
        and float(crosses) <= policy.trend_max_vwap_crosses
        and ((float(score) > 0 and float(breadth) >= policy.trend_min_breadth and float(slope) > 0)
             or (float(score) < 0 and float(breadth) <= 1 - policy.trend_min_breadth and float(slope) < 0))
    )
    if trend and price_side and ("above" if float(score) > 0 else "below") not in price_side:
        trend = False
        contradictions.append("price_vwap_direction_conflict")
    balanced = bool(
        abs(score) <= policy.balanced_max_score
        and efficiency < policy.balanced_max_efficiency
        and crosses >= policy.balanced_min_vwap_crosses
    )
    if trend:
        return "TREND", direction, ["direction_score_confirmed", "path_efficiency_confirmed"], contradictions, 1.0
    if balanced:
        return "BALANCED", None, ["low_path_efficiency", "multiple_vwap_crosses"], contradictions, confidence
    return "TRANSITION", direction, ["path_inputs_not_aligned"], contradictions, confidence


def _es_coordinate_path(
    path: Mapping[str, Any],
    policy: StrategyPolicy,
    *,
    price_side: str,
) -> tuple[str, str | None, list[str], list[str], float]:
    ret5 = _number(path.get("return_5m_points"))
    ret15 = _first_number(path.get("impulse_15m_points"), path.get("return_15m_points"))
    ret1 = _number(path.get("return_1m_points"))
    returns = [value for value in (ret5, ret15, ret1) if value is not None]
    contradictions: list[str] = []
    if not returns:
        return "UNCERTAIN", None, ["es_path_returns_unavailable"], contradictions, 0.0
    threshold = policy.gth_trend_min_abs_return_points
    signed = next((value for value in returns if abs(value) >= threshold), returns[0])
    direction = "UP" if signed > 0 else "DOWN" if signed < 0 else None
    if direction is None:
        return "TRANSITION", None, ["es_path_flat"], contradictions, 0.4
    vwap_conflict = bool(
        price_side and ("above" if direction == "UP" else "below") not in price_side
    )
    if vwap_conflict:
        contradictions.append("price_vwap_direction_conflict")
    efficiency = _number(path.get("efficiency_ratio_30m"))
    trend = bool(
        efficiency is not None
        and float(efficiency) >= policy.trend_efficiency
        and abs(float(signed)) >= threshold
        and not vwap_conflict
    )
    if trend:
        return "TREND", direction, ["es_path_return_confirmed", "path_efficiency_confirmed"], contradictions, 0.7
    if vwap_conflict:
        return "TRANSITION", direction, ["es_price_vwap_contradiction"], contradictions, 0.5
    return "TRANSITION", direction, ["es_path_not_aligned"], contradictions, 0.5


def _hmm_cross_map(
    facts: Mapping[str, Any], policy: StrategyPolicy
) -> tuple[str, str | None, list[str], dict[str, Any]] | None:
    hmm = _map(facts.get("hmm"))
    cross = _map(facts.get("cross_index"))
    source = str(cross.get("source") or "")
    posterior = _map(hmm.get("posterior"))
    probabilities = {
        state: _number(posterior.get(state)) for state in HMM_STATE_DIRECTION
    }
    if (
        hmm.get("status") != "available"
        or cross.get("status") != "ready"
        or cross.get("session_open") is not True
        or source not in {"cash_index", "globex_index"}
        or any(value is None for value in probabilities.values())
    ):
        return None
    resolved = {state: float(value) for state, value in probabilities.items() if value is not None}
    dominant = max(resolved, key=resolved.__getitem__)
    max_probability = resolved[dominant]
    direction = HMM_STATE_DIRECTION[dominant]
    payload = {
        "used": True,
        "status": "available",
        "source": source,
        "anchor": cross.get("anchor"),
        "dominant_state": dominant,
        "max_state_probability": round(max_probability, 4),
        "posterior": {state: round(value, 4) for state, value in resolved.items()},
        "reason": None,
    }
    if dominant == "state_01" and max_probability >= policy.hmm_balanced_min_probability:
        return "BALANCED", None, ["hmm_index_balanced"], payload
    if direction in {"UP", "DOWN"} and max_probability >= policy.hmm_trend_min_probability:
        return (
            "TREND",
            direction,
            ["hmm_index_trend", f"hmm_dominant:{dominant}"],
            payload,
        )
    return "TRANSITION", direction, ["hmm_index_mixed_posterior"], payload


def _hmm_unused_payload(facts: Mapping[str, Any]) -> dict[str, Any]:
    hmm = _map(facts.get("hmm"))
    cross = _map(facts.get("cross_index"))
    reason = str(hmm.get("reason") or "")
    if hmm.get("status") != "available":
        reason = reason or "hmm_unavailable"
    elif cross.get("status") != "ready" or cross.get("session_open") is not True:
        reason = reason or "hmm_index_basket_not_ready"
    else:
        reason = reason or "hmm_index_not_used"
    return {
        "used": False,
        "status": hmm.get("status") or "unavailable",
        "source": cross.get("source"),
        "reason": reason,
    }


def _pin_assessment(facts: Mapping[str, Any], policy: StrategyPolicy) -> dict[str, Any]:
    path, vc, structure = _map(facts.get("path")), _map(facts.get("value_center")), _map(facts.get("structure"))
    vol, mass = _map(facts.get("volatility")), _map(structure.get("q_local_mass_5pt"))
    er, vc15, vc30, vc60 = (_number(path.get("efficiency_ratio_30m")), *(_number(vc.get(f"spx_{w}")) for w in ("15m", "30m", "60m")))
    q_mode, decay = _number(structure.get("q_mode")), _number(vol.get("atm_straddle_decay_15m"))
    closes = [float(value) for value in path.get("pin_path_spx") or () if isinstance(value, int | float)]
    breadth = _number(path.get("breadth_above_vwap"))
    vix = _number(vol.get("vix_return_15m_pct"))
    required = (er, vc15, vc30, vc60, q_mode, decay, breadth, vix)
    if None in required or len(closes) < 4 or not mass:
        return {"terminal_state": "UNCERTAIN", "reason": "pin_inputs_unavailable", "top_centers": []}
    shock_state = str(_map(facts.get("shock")).get("state") or "NONE")
    if shock_state in {"ACTIVE", "POST_SHOCK_DISCOVERY"}:
        return {
            "terminal_state": "NONE",
            "reason": f"shock_{shock_state.lower()}",
            "depin_risk": 1.0,
            "top_centers": [],
        }
    centers = [float(key) for key in mass if str(key).replace(".", "", 1).isdigit()]
    returns = {center: _excursion_returns(closes, center) for center in centers}
    drift30, drift60 = float(vc15) - float(vc30), float(vc15) - float(vc60)
    extreme = abs(closes[-1] - closes[-4]) >= 5 and closes[-1] in {min(closes[-4:]), max(closes[-4:])}
    depin = min(1.0, 0.25 * max(abs(drift30) / 5, abs(drift60) / 8)
                + 0.20 * min(float(er) / 0.4, 1) + 0.20 * (abs(float(breadth) - 0.5) * 2 if breadth is not None else 0)
                + 0.15 * min(max(float(vix), 0) / 0.01, 1) + 0.10 * extreme
                + 0.10 * min(max(-float(decay), 0) / 0.05, 1))
    refs = [_number(structure.get(key)) for key in ("zero_gamma", "put_wall", "call_wall")]
    flip = structure.get("flip_zone")
    if isinstance(flip, (list, tuple)) and len(flip) >= 2:
        refs.append(sum(map(float, flip[:2])) / 2)
    gamma = min((value for value in refs if value is not None), key=lambda value: abs(value - float(q_mode)), default=None)
    max_mass = max(float(value) for value in mass.values()) or 1.0
    ranked = sorted(({
        "center": center,
        "score": round(0.25 * math.exp(-min((abs(center - value) for value in refs if value is not None), default=30) / 5)
                       + 0.25 * (0.5 * math.exp(-abs(center - float(vc30)) / 5) + 0.5 * math.exp(-abs(center - float(vc60)) / 7.5))
                       + 0.20 * float(mass[f"{center:g}"]) / max_mass
                       + 0.15 * (0, 0.4, 0.7, 1)[min(returns[center], 3)] + 0.10 * (float(decay) > 0)
                       - 0.25 * min(max(abs(drift30) / 5, abs(drift60) / 8), 1) - 0.20 * depin, 4),
        "excursion_returns": returns[center],
    } for center in centers), key=lambda row: row["score"], reverse=True)
    er_max, drift30_max, drift60_max, migrate30, migrate60, stable_risk, block_risk = policy.pin_thresholds
    migrating = abs(drift30) > migrate30 or abs(drift60) > migrate60 or float(er) > 0.40 or extreme
    aligned = gamma is not None and max(float(q_mode), float(vc30), gamma) - min(float(q_mode), float(vc30), gamma) <= 5
    stable = (facts.get("minutes_to_close") is not None and int(facts["minutes_to_close"]) <= 210
              and float(er) < er_max and abs(drift30) <= drift30_max and abs(drift60) <= drift60_max
              and max(returns.values(), default=0) >= 2 and float(vix) <= 0.01 and not extreme and aligned
              and float(decay) > 0 and depin < stable_risk)
    terminal = "PIN_MIGRATING" if migrating or depin >= block_risk else "PIN_STABLE" if stable else "NONE"
    return {"terminal_state": terminal, "depin_risk": round(depin, 4), "drift_30m": round(drift30, 2),
            "drift_60m": round(drift60, 2), "recent_extreme_acceptance": extreme,
            "top_centers": ranked[:3]}


def _excursion_returns(values: list[float], center: float) -> int:
    away, count = False, 0
    for value in values:
        if abs(value - center) >= 5:
            away = True
        elif away and abs(value - center) <= 2.5:
            count += 1
            away = False
    return count


def _map(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) else None


def _first_number(*values: object) -> float | None:
    for value in values:
        number = _number(value)
        if number is not None:
            return number
    return None
