"""Deterministic regime dimensions for strategy selection."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True, slots=True)
class StrategyPolicy:
    policy_version: str = "strategy_policy.bootstrap.v1"
    trend_score: float = 6.0
    trend_efficiency: float = 0.45
    trend_max_vwap_crosses: float = 2.0
    trend_min_breadth: float = 0.55
    balanced_max_score: float = 3.0
    balanced_max_efficiency: float = 0.30
    balanced_min_vwap_crosses: float = 2.0
    quote_max_age_seconds: float = 15.0
    quote_max_skew_seconds: float = 2.0
    opportunity_ttl_seconds: float = 300.0
    min_target_room_ratio: float = 1.5
    failed_break_min_target_room_ratio: float = 1.8
    max_debit_fraction: float = 0.45
    failed_break_max_debit_fraction: float = 0.40
    min_stop_atr: float = 0.25
    max_stop_atr: float = 1.0
    late_chase_distance_atr: float = 1.0
    late_chase_impulse_atr: float = 1.0

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
    if _map(facts.get("quality")).get("status") != "ready":
        state, reasons = "UNCERTAIN", ["strategy_facts_degraded"]
    elif trend:
        state, reasons = "TREND", ["direction_score_confirmed", "path_efficiency_confirmed"]
    elif balanced:
        state, direction, reasons = "BALANCED", None, ["low_path_efficiency", "multiple_vwap_crosses"]
    elif any(value is not None for value in (score, efficiency, crosses)):
        state, reasons = "TRANSITION", ["path_inputs_not_aligned"]
    else:
        state, reasons = "UNCERTAIN", ["path_inputs_unavailable"]
    event_state = {
        "pre_event": "SCHEDULED_EVENT_RISK", "post_event": "POST_EVENT_DISCOVERY",
        "normal": "NORMAL",
    }.get(str(event.get("state") or "unavailable"), "UNCERTAIN")
    return {
        "schema_version": "regime_assessment.v1", "policy_version": policy.policy_version,
        "path_state": state, "path_direction": direction, "terminal_state": "UNCERTAIN",
        "event_state": event_state, "entry_state": "INSUFFICIENT_DATA",
        "confidence": round(sum(value is not None for value in inputs) / 5, 2),
        "reasons": reasons, "contradictions": contradictions,
    }


def _map(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) else None
