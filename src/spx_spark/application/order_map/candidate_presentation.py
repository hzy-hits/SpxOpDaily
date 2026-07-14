"""Decision-aware presentation of order-map candidates."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from spx_spark.application.order_map.models import LEVEL_DECISION_PLAYS, level_decision_play


def apply_candidate_presentation(payload: dict[str, Any], *, now: datetime) -> None:
    """Expose at most one live plan; keep every other candidate observational."""

    candidates = [dict(item) for item in payload.get("candidates") or [] if isinstance(item, dict)]
    repricing = payload.get("level_trigger_repricing")
    repricing = repricing if isinstance(repricing, dict) else {}
    repriced_candidates = [
        dict(item) for item in repricing.get("candidates") or [] if isinstance(item, dict)
    ]
    play, reason = _supported_plan_play(payload, now=now)
    intent = payload.get("trade_intent")
    intent = intent if isinstance(intent, dict) else {}
    intent_event_id = str(intent.get("event_id") or "")
    intent_contract_id = str(intent.get("contract_id") or "")
    matching = [
        item
        for item in repriced_candidates
        if repricing.get("event_id") == intent_event_id
        and item.get("play") == play
        and item.get("contract_id") == intent_contract_id
        and item.get("execution_quote_status") == "executable"
    ]
    if play and len(matching) == 1:
        plan = {
            **matching[0],
            "intent_id": intent.get("intent_id"),
            "context_id": intent.get("context_id"),
            "contract_id": intent_contract_id,
            "current_mid": intent.get("decision_mid"),
            "decision_bid": intent.get("decision_bid"),
            "decision_ask": intent.get("decision_ask"),
            "limit_aggressive": intent.get("entry_limit"),
            "limit_conservative": None,
            "order_style": "live_nbbo_limit",
            "invalidation_spx": intent.get("invalidation_spx"),
            "target_spx": intent.get("target_spx"),
            "intent_expires_at": intent.get("expires_at"),
            "time_stop_at": intent.get("time_stop_at"),
            "max_loss_per_contract": intent.get("max_loss_per_contract"),
            "provider": intent.get("provider"),
            "quote_source_at": intent.get("quote_source_at"),
            "automatic_ordering": intent.get("automatic_ordering", False),
            "presentation_role": "plan",
            "decision_executable": True,
        }
        payload["plan_candidates"] = [plan]
        payload["observation_candidates"] = [
            _as_observation(item) for item in candidates if not _same_candidate(item, plan)
        ]
        payload["candidate_presentation"] = {
            "mode": "single_plan",
            "reason": reason,
            "play": play,
            "intent_id": intent.get("intent_id"),
        }
        return

    payload["plan_candidates"] = []
    payload["observation_candidates"] = [_as_observation(item) for item in candidates]
    payload["candidate_presentation"] = {
        "mode": "observation_only",
        "reason": "unique_trade_ready_candidate_unavailable" if play else reason,
        "play": None,
        "intent_id": intent.get("intent_id"),
    }


def _supported_plan_play(
    payload: dict[str, Any],
    *,
    now: datetime,
) -> tuple[str | None, str]:
    decision = payload.get("level_decision")
    context = payload.get("decision_context")
    intent = payload.get("trade_intent")
    if not isinstance(decision, dict):
        return None, "no_formal_decision"
    if not isinstance(context, dict) or not isinstance(intent, dict):
        return None, "missing_decision_context"
    if intent.get("status") != "trade_ready":
        return None, f"trade_intent_{intent.get('status') or 'unavailable'}"
    if decision.get("phase") != "confirmed":
        return None, "decision_not_confirmed"
    expires_at = _timestamp(intent.get("expires_at"))
    if expires_at is None:
        return None, "trade_intent_expiry_unavailable"
    if _utc(now) >= expires_at:
        return None, "trade_intent_expired"

    thesis = str(decision.get("thesis") or "")
    direction = str(decision.get("direction") or "")
    play = level_decision_play(thesis, direction)
    if (
        play not in LEVEL_DECISION_PLAYS
        or intent.get("play") != play
        or intent.get("thesis") != thesis
        or intent.get("direction") != direction
    ):
        return None, "decision_play_mismatch"
    event_id = str(decision.get("event_id") or "")
    if not event_id or str(intent.get("event_id") or "") != event_id:
        return None, "decision_event_mismatch"
    if not intent.get("intent_id") or not intent.get("contract_id"):
        return None, "trade_intent_identity_unavailable"
    return play, "trade_intent_ready"


def _as_observation(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        **candidate,
        "presentation_role": "observation_scenario",
        "decision_executable": False,
    }


def _same_candidate(candidate: dict[str, Any], plan: dict[str, Any]) -> bool:
    candidate_contract = str(candidate.get("contract_id") or "")
    plan_contract = str(plan.get("contract_id") or "")
    if candidate_contract and plan_contract:
        return candidate_contract == plan_contract
    return (
        candidate.get("play") == plan.get("play")
        and candidate.get("strike") == plan.get("strike")
        and candidate.get("right") == plan.get("right")
    )


def _timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return _utc(datetime.fromisoformat(value))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
