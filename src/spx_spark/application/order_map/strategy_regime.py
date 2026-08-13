"""Deterministic regime dimensions for strategy selection."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping


from spx_spark.analytics.options.strategy_payoff import (
    DEFAULT_MANAGEMENT_POLICY,
    ManagementPolicy,
)

# Forward mark horizons for strategy_outcomes (v3). Frozen code constant.
MARK_HORIZONS_MINUTES: tuple[int, ...] = (1, 2, 3, 4, 5, 7, 10, 15, 20)

__all__ = (
    "DEFAULT_MANAGEMENT_POLICY",
    "DEFAULT_STRATEGY_POLICY",
    "MARK_HORIZONS_MINUTES",
    "ManagementPolicy",
    "StrategyPolicy",
    "assess_regime",
)


@dataclass(frozen=True, slots=True)
class StrategyPolicy:
    policy_version: str = "strategy_policy.bootstrap.v9"
    # v9: GTH desk map is a live structure scan, not an empty health heartbeat.
    # Always recompute the 25Δ/5Δ iron condor from 1-minute quotes. Widen the
    # Call/Put/butterfly scan around spot±5 and 10Δ/25Δ anchors. Winners still
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
    gth_delta_targets: tuple[float, ...] = (0.25, 0.10)
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
    butterfly_max_debit_fraction: float = 0.35
    butterfly_max_risk_usd: float = 1000.0
    # v5: debit vertical short strike may not pass the target, and width may
    # not exceed remaining 0DTE expected move. Missing EM fails closed.
    # V3-3a flood control (activated with policy_version bump to bootstrap.v2).
    candidate_cooldown_seconds: float = 300.0
    max_cards_per_direction_per_session: int = 6

    def entry_quality_kwargs(self) -> dict[str, float]:
        names = (
            "min_target_room_ratio", "failed_break_min_target_room_ratio",
            "max_debit_fraction", "failed_break_max_debit_fraction",
            "min_stop_atr", "max_stop_atr", "late_chase_distance_atr",
            "late_chase_impulse_atr",
        )
        return {name: getattr(self, name) for name in names}


DEFAULT_STRATEGY_POLICY = StrategyPolicy()


def assess_regime(
    facts: Mapping[str, Any], policy: StrategyPolicy = DEFAULT_STRATEGY_POLICY
) -> dict[str, Any]:
    path, event = _map(facts.get("path")), _map(facts.get("event"))
    score, efficiency = _number(path.get("direction_score")), _number(path.get("efficiency_ratio_30m"))
    crosses, breadth = _number(path.get("vwap_crosses_30m")), _number(path.get("breadth_above_vwap"))
    slope, price_side = _number(path.get("vwap_slope")), str(path.get("price_vs_vwap") or "").lower()
    direction = "UP" if score is not None and score > 0 else "DOWN" if score is not None and score < 0 else None
    inputs = (score, efficiency, crosses, breadth, slope)
    trend = bool(
        None not in inputs
        and abs(float(score)) >= policy.trend_score
        and float(efficiency) >= policy.trend_efficiency
        and float(crosses) <= policy.trend_max_vwap_crosses
        and ((float(score) > 0 and float(breadth) >= policy.trend_min_breadth and float(slope) > 0)
             or (float(score) < 0 and float(breadth) <= 1 - policy.trend_min_breadth and float(slope) < 0))
    )
    contradictions = []
    if trend and price_side and ("above" if float(score) > 0 else "below") not in price_side:
        trend = False
        contradictions.append("price_vwap_direction_conflict")
    balanced = bool(
        score is not None and efficiency is not None and crosses is not None
        and abs(score) <= policy.balanced_max_score
        and efficiency < policy.balanced_max_efficiency
        and crosses >= policy.balanced_min_vwap_crosses
    )
    capabilities = _map(facts.get("capabilities"))
    path_capability = _map(capabilities.get("path"))
    path_capability_ready = (
        path_capability.get("ready") is True
        if path_capability
        else _map(facts.get("quality")).get("status") == "ready"
    )
    if not path_capability_ready:
        state, reasons = "UNCERTAIN", ["strategy_facts_degraded"]
    elif any(value is None for value in inputs):
        state, reasons = "UNCERTAIN", ["path_inputs_unavailable"]
    elif trend:
        state, reasons = "TREND", ["direction_score_confirmed", "path_efficiency_confirmed"]
    elif balanced:
        state, direction, reasons = "BALANCED", None, ["low_path_efficiency", "multiple_vwap_crosses"]
    else:
        state, reasons = "TRANSITION", ["path_inputs_not_aligned"]
    event_state = {
        "pre_event": "SCHEDULED_EVENT_RISK", "post_event": "POST_EVENT_DISCOVERY",
        "normal": "NORMAL",
    }.get(str(event.get("state") or "unavailable"), "UNCERTAIN")
    pin = _pin_assessment(facts, policy)
    return {
        "schema_version": "regime_assessment.v1", "policy_version": policy.policy_version,
        "path_state": state, "path_direction": direction, "terminal_state": pin["terminal_state"],
        "event_state": event_state, "entry_state": "INSUFFICIENT_DATA",
        "confidence": round(sum(value is not None for value in inputs) / 5, 2),
        "reasons": reasons, "contradictions": contradictions, "pin": pin,
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
