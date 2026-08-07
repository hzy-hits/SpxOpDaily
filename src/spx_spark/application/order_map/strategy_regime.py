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


DEFAULT_STRATEGY_POLICY = StrategyPolicy()


def assess_regime(
    facts: Mapping[str, Any],
    policy: StrategyPolicy = DEFAULT_STRATEGY_POLICY,
) -> dict[str, Any]:
    path = _mapping(facts.get("path"))
    quality = _mapping(facts.get("quality"))
    event = _mapping(facts.get("event"))
    score = _number(path.get("direction_score"))
    efficiency = _number(path.get("efficiency_ratio_30m"))
    crosses = _number(path.get("vwap_crosses_30m"))
    breadth = _number(path.get("breadth_above_vwap"))
    price_vs_vwap = str(path.get("price_vs_vwap") or "").lower()
    slope = _number(path.get("vwap_slope"))
    reasons: list[str] = []
    contradictions: list[str] = []

    direction = "UP" if score is not None and score > 0 else "DOWN" if score and score < 0 else None
    trend = bool(
        score is not None
        and efficiency is not None
        and crosses is not None
        and breadth is not None
        and abs(score) >= policy.trend_score
        and efficiency >= policy.trend_efficiency
        and crosses <= policy.trend_max_vwap_crosses
        and (
            (score > 0 and breadth >= policy.trend_min_breadth and slope is not None and slope > 0)
            or (score < 0 and breadth <= 1 - policy.trend_min_breadth and slope is not None and slope < 0)
        )
    )
    if trend and price_vs_vwap:
        expected_side = "above" if score and score > 0 else "below"
        if expected_side not in price_vs_vwap:
            trend = False
            contradictions.append("price_vwap_direction_conflict")
    balanced = bool(
        score is not None
        and efficiency is not None
        and crosses is not None
        and abs(score) <= policy.balanced_max_score
        and efficiency < policy.balanced_max_efficiency
        and crosses >= policy.balanced_min_vwap_crosses
    )

    if quality.get("status") != "ready":
        path_state = "UNCERTAIN"
        reasons.append("strategy_facts_degraded")
    elif trend:
        path_state = "TREND"
        reasons.extend(("direction_score_confirmed", "path_efficiency_confirmed"))
    elif balanced:
        path_state = "BALANCED"
        direction = None
        reasons.extend(("low_path_efficiency", "multiple_vwap_crosses"))
    elif any(value is not None for value in (score, efficiency, crosses)):
        path_state = "TRANSITION"
        reasons.append("path_inputs_not_aligned")
    else:
        path_state = "UNCERTAIN"
        reasons.append("path_inputs_unavailable")

    event_mode = str(event.get("state") or "unavailable")
    if event_mode == "pre_event":
        event_state = "SCHEDULED_EVENT_RISK"
    elif event_mode == "post_event":
        event_state = "POST_EVENT_DISCOVERY"
    else:
        event_state = "NORMAL" if event_mode == "normal" else "UNCERTAIN"
    available = sum(value is not None for value in (score, efficiency, crosses, breadth, slope))
    return {
        "schema_version": "regime_assessment.v1",
        "policy_version": policy.policy_version,
        "path_state": path_state,
        "path_direction": direction,
        "terminal_state": "UNCERTAIN",
        "event_state": event_state,
        "entry_state": "INSUFFICIENT_DATA",
        "confidence": round(available / 5, 2),
        "reasons": reasons,
        "contradictions": contradictions,
    }


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) else None
