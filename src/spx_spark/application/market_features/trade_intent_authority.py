"""Fail-closed authority checks for manual RTH trade intents."""

from __future__ import annotations

from typing import Mapping

from spx_spark.application.market_features.manual_signal_contract import (
    APPROVED_MANUAL_LANE_CONTRACTS,
    LEGACY_PUT_SHADOW_LANES,
)


def live_trade_intent_authority_issues(
    intent: Mapping[str, object],
) -> tuple[str, ...]:
    """Return reasons that forbid a candidate from reaching a live manual lane."""

    issues: list[str] = []
    if intent.get("status") != "trade_ready":
        issues.append("trade_intent_not_trade_ready")
    if intent.get("execution_eligible") is not True:
        issues.append("trade_intent_execution_authority_missing")
    if intent.get("quote_observation_eligible") is not False:
        issues.append("trade_intent_quote_observation_only")
    if intent.get("shadow_mode") is not False:
        issues.append("trade_intent_shadow_mode")
    if intent.get("automatic_ordering") is not False:
        issues.append("trade_intent_automatic_ordering_contract_invalid")
    strategy_lane = str(intent.get("strategy_lane") or "")
    if strategy_lane in LEGACY_PUT_SHADOW_LANES:
        issues.append("put_lane_live_execution_forbidden")
    approved_contract = APPROVED_MANUAL_LANE_CONTRACTS.get(strategy_lane)
    if approved_contract is None:
        issues.append("trade_intent_live_lane_not_approved")
    else:
        expected_direction, expected_right = approved_contract
        if intent.get("direction") != expected_direction:
            issues.append("trade_intent_live_direction_mismatch")
        if not str(intent.get("contract_id") or "").endswith(f":{expected_right}"):
            issues.append("trade_intent_live_contract_right_mismatch")
    return tuple(dict.fromkeys(issues))
