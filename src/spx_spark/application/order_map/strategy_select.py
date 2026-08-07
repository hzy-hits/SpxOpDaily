"""Single strategy-decision authority for the Order Map payload."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from spx_spark.application.order_map.strategy_facts import build_market_fact_pack
from spx_spark.application.order_map.strategy_regime import (
    DEFAULT_STRATEGY_POLICY,
    assess_regime,
)
from spx_spark.storage import LatestState


def build_strategy_decision(
    payload: Mapping[str, Any],
    latest: LatestState,
    now: datetime,
) -> dict[str, Any]:
    facts = build_market_fact_pack(payload, latest, now)
    regime = assess_regime(facts)
    quality = _mapping(facts.get("quality"))
    legacy = [row for row in facts.get("legacy_candidates") or () if isinstance(row, Mapping)]
    reasons: list[str] = []
    if quality.get("status") != "ready":
        reasons.extend(str(reason) for reason in quality.get("reasons") or ())
    if regime.get("event_state") in {"SCHEDULED_EVENT_RISK", "POST_EVENT_DISCOVERY"}:
        reasons.append(f"event_gate:{str(regime['event_state']).lower()}")
    reasons.append("s1_no_trade_only_vertical_and_butterfly_not_yet_enabled")
    reasons = list(dict.fromkeys(reasons))
    nearest = str(legacy[0].get("play") or "") if legacy else None
    reauthorize = (
        "刷新并同步 SPX、ES、SPXW 双边报价"
        if quality.get("status") != "ready"
        else "S2 入场质量与可执行价格通过后重新授权"
    )
    decision_at = str(facts["decision_at"])
    identity = _identity(
        {
            "decision_at": decision_at,
            "available_at": facts["available_at"],
            "session_date": facts.get("session_date"),
            "policy_version": DEFAULT_STRATEGY_POLICY.policy_version,
            "regime": regime,
            "reasons": reasons,
        }
    )
    return {
        "schema_version": "strategy_decision.v1",
        "decision_id": f"strategy:{identity[:24]}",
        "policy_version": DEFAULT_STRATEGY_POLICY.policy_version,
        "decision_at": decision_at,
        "available_at": facts["available_at"],
        "session_date": facts.get("session_date"),
        "decision_type": "NO_TRADE",
        "candidate": None,
        "market_facts": facts,
        "regime": regime,
        "legacy_reference": {
            "candidate_count": len(legacy),
            "nearest_candidate": nearest,
        },
        "desk_view": {
            "state": regime["path_state"],
            "direction": regime.get("path_direction"),
            "conclusion": "NO TRADE",
            "reason": reasons[0],
        },
        "why_not": {
            "nearest_candidate": nearest,
            "reasons": reasons,
            "reauthorize_on": reauthorize,
        },
        "execution": {
            "action": "WAIT",
            "order_type": None,
            "limit": None,
            "automatic_ordering": False,
            "manual_action_only": True,
        },
        "risk": {
            "max_loss": None,
            "invalidation": "没有候选被授权，不建立风险敞口",
        },
        "targets": [],
        "data_quality": quality,
        "action_authority": "none",
        "automatic_ordering": False,
    }


def _identity(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
