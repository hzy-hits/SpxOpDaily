"""Audit-only handling for level-path transitions.

Human notifications are owned exclusively by the unified strategy decision.
Level transitions remain durable facts, but never enqueue a second non-trade
card beside the eventual executable two-leg candidate.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from spx_spark.application.order_map.level_decision_machine import (
    LevelObservation,
    LevelTransition,
)
from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock, read_json_object


def prepare_level_transition_delivery(
    transition: LevelTransition,
    observation: LevelObservation,
    *,
    now: datetime,
    notify_transitions: bool,
    formal_signal_enabled: bool,
    notifications_enabled: bool,
) -> tuple[dict[str, object] | None, None]:
    """Return a durable audit result and never create a human notification."""

    del observation, notifications_enabled
    if not transition.changed:
        return None, None
    state = transition.state
    phase = transition.current_phase
    return (
        {
            "record_key": (
                f"{state.get('event_id') or 'far'}:"
                f"{state.get('transition_count') or 0}:{phase.value}"
            ),
            "at": _utc(now).isoformat(),
            "event_id": state.get("event_id"),
            "phase": phase.value,
            "previous_phase": transition.previous_phase.value,
            "formal_signal": formal_signal_enabled and phase.value == "confirmed",
            "actionable": False,
            "notify_transitions_configured": notify_transitions,
            "delivery_gate": "unified_strategy_decision_required",
            "reason": "unified_strategy_decision_owned",
            "sinks": [],
            "accepted": False,
            "queued": False,
            "delivered": False,
        },
        None,
    )


def merge_pending_level_transition(
    persisted: Mapping[str, object],
    intent: Mapping[str, object] | None,
) -> tuple[list[dict[str, object]], list[str]]:
    """Retire legacy pending cards while preserving accepted audit identities."""

    del intent
    accepted = sorted(
        {
            str(item)
            for item in persisted.get("accepted_notification_event_ids") or []
            if item
        }
    )[-200:]
    return [], accepted


def flush_pending_level_transition_notifications(
    state_path: Path,
    *,
    now: datetime,
    only_event_id: str | None = None,
    enqueue=None,
) -> dict[str, object] | None:
    """Discard legacy pending transition cards without contacting a sink."""

    del only_event_id, enqueue
    with exclusive_state_lock(state_path):
        state = read_json_object(state_path)
        pending = [
            dict(item)
            for item in state.get("pending_notifications") or []
            if isinstance(item, Mapping)
        ]
        if not pending:
            return None
        state["pending_notifications"] = []
        atomic_write_json_secure(state_path, state)
    return {
        "record_key": f"level-transition-retired:{_utc(now).isoformat()}",
        "at": _utc(now).isoformat(),
        "reason": "legacy_transition_notifications_retired",
        "accepted": False,
        "queued": False,
        "delivered": False,
        "retired_count": len(pending),
    }


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("level transition timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)
