"""Decision-aware presentation of order-map candidates."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from spx_spark.application.order_map.models import LEVEL_DECISION_PLAYS, level_decision_play
from spx_spark.strategy_contract import (
    STRATEGY_EVENT_SCHEMA_VERSION,
    actionable_strategy_contract_issues,
)


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
            "intent_expires_at": intent.get("valid_until") or intent.get("expires_at"),
            "time_stop_at": intent.get("time_stop_at"),
            "max_loss_per_contract": intent.get("max_loss_per_contract"),
            "provider": intent.get("provider"),
            "quote_source_at": intent.get("quote_source_at"),
            "automatic_ordering": intent.get("automatic_ordering", False),
            "presentation_role": "plan",
            "decision_executable": True,
        }
        payload["plan_candidates"] = [plan]
        opposing = _opposing_invalidation(candidates, primary_direction=str(intent.get("direction") or ""))
        payload["observation_candidates"] = []
        payload["opposing_invalidation"] = opposing
        payload["candidate_presentation"] = {
            "mode": "single_plan",
            "reason": reason,
            "play": play,
            "intent_id": intent.get("intent_id"),
        }
        return

    payload["plan_candidates"] = []
    if not _directional_setup_active(payload):
        payload["observation_candidates"] = []
        payload["opposing_invalidation"] = None
        payload["candidate_presentation"] = {
            "mode": "observation_only",
            "reason": "no_directional_setup",
            "play": None,
            "primary_direction": None,
            "direction_source": "level_decision",
            "intent_id": intent.get("intent_id"),
        }
        return
    direction, direction_source = _primary_direction(payload, now=now)
    primary = _choose_primary(candidates, payload=payload, direction=direction)
    payload["observation_candidates"] = [_as_observation(primary)] if primary else []
    payload["opposing_invalidation"] = _opposing_invalidation(
        candidates,
        primary_direction=_candidate_direction(primary) if primary else direction,
    )
    payload["candidate_presentation"] = {
        "mode": "observation_only",
        "reason": "unique_trade_ready_candidate_unavailable" if play else reason,
        "play": primary.get("play") if primary else None,
        "primary_direction": _candidate_direction(primary) if primary else direction or None,
        "direction_source": direction_source,
        "intent_id": intent.get("intent_id"),
    }


def _directional_setup_active(payload: dict[str, Any]) -> bool:
    decision = payload.get("level_decision")
    if not isinstance(decision, dict):
        return False
    return bool(
        str(decision.get("phase") or "").lower()
        in {"accepted", "retest", "confirmed"}
        and str(decision.get("thesis") or "") in {"breakout", "fade"}
        and str(decision.get("direction") or "") in {"up", "down"}
    )


def _primary_direction(payload: dict[str, Any], *, now: datetime) -> tuple[str, str]:
    for source, value in (
        ("trade_intent", payload.get("trade_intent")),
        ("level_decision", payload.get("level_decision")),
        ("regime_decision", payload.get("regime_decision")),
    ):
        if isinstance(value, dict) and str(value.get("direction") or "") in {"up", "down"}:
            return str(value["direction"]), source
    gth = payload.get("gth_dip_reclaim_signal")
    if (
        isinstance(gth, dict)
        and gth.get("kind") == "gth_dip_reclaim_call"
        and not actionable_strategy_contract_issues(gth, now=now)
    ):
        return "up", "gth_dip_reclaim"
    trend = payload.get("globex_trend")
    regime = str(trend.get("regime") or "") if isinstance(trend, dict) else ""
    if regime in {"bullish", "up"}:
        return "up", "globex_trend"
    if regime in {"bearish", "down"}:
        return "down", "globex_trend"
    return "", "nearest_level_no_direction_guess"


def _choose_primary(
    candidates: list[dict[str, Any]],
    *,
    payload: dict[str, Any],
    direction: str,
) -> dict[str, Any] | None:
    if not candidates:
        return None
    directional = [row for row in candidates if _candidate_direction(row) == direction]
    pool = directional or candidates
    spot = None
    underlier = payload.get("underlier")
    if isinstance(underlier, dict) and isinstance(underlier.get("price"), int | float):
        spot = float(underlier["price"])
    greek = payload.get("greek_decision")
    scores = greek.get("contract_scores") if isinstance(greek, dict) else {}
    scores = scores if isinstance(scores, dict) else {}

    def rank(row: dict[str, Any]) -> tuple[float, float, str]:
        adjustment = scores.get(str(row.get("contract_id") or ""))
        confidence = (
            float(adjustment.get("confidence_adjustment") or 0.0)
            if isinstance(adjustment, dict)
            else 0.0
        )
        level = row.get("level")
        distance = abs(float(level) - spot) if spot is not None and isinstance(level, int | float) else 1e9
        return (-confidence, distance, str(row.get("play") or ""))

    return min(pool, key=rank)


def _opposing_invalidation(
    candidates: list[dict[str, Any]], *, primary_direction: str
) -> dict[str, Any] | None:
    if primary_direction not in {"up", "down"}:
        return None
    opposite = "down" if primary_direction == "up" else "up"
    rows = [row for row in candidates if _candidate_direction(row) == opposite]
    if not rows:
        return None
    row = rows[0]
    return {
        "direction": opposite,
        "play": row.get("play"),
        "level": row.get("level"),
        "level_label": row.get("level_label"),
        "role": "primary_strategy_invalidation_only",
        "decision_executable": False,
    }


def _candidate_direction(candidate: dict[str, Any] | None) -> str:
    if not candidate:
        return ""
    right = str(candidate.get("right") or "").upper()
    return "up" if right == "C" else "down" if right == "P" else ""


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
    if intent.get("schema_version") == STRATEGY_EVENT_SCHEMA_VERSION:
        contract_issues = actionable_strategy_contract_issues(intent, now=now)
        if "strategy_event_expired" in contract_issues:
            return None, "trade_intent_expired"
        if contract_issues:
            return None, f"trade_intent_{contract_issues[0]}"
    elif intent.get("schema_version") == 1:
        expires_at = _timestamp(intent.get("expires_at"))
        if expires_at is None:
            return None, "trade_intent_expiry_unavailable"
        if _utc(now) >= expires_at:
            return None, "trade_intent_expired"
    else:
        return None, "trade_intent_strategy_schema_unsupported"

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
