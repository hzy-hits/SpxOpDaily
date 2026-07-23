"""Persistence records for the level-decision shadow runner."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from typing import Mapping

from spx_spark.application.order_map.level_decision_machine import (
    LevelDecisionSettings,
    LevelObservation,
    LevelPhase,
)


def build_transition_record(
    transition,
    observation: LevelObservation,
    *,
    formal_signal_enabled: bool,
    structure_change_pending: bool,
    new_arm_blocked: bool,
) -> dict[str, object]:
    state = transition.state
    formal_signal = formal_signal_enabled and transition.current_phase is LevelPhase.CONFIRMED
    return {
        "record_key": (
            f"{state.get('event_id') or 'far'}:"
            f"{state.get('transition_count') or 0}:{transition.current_phase.value}"
        ),
        "event_id": state.get("event_id"),
        "at": _utc(observation.at).isoformat(),
        "previous_phase": transition.previous_phase.value,
        "current_phase": transition.current_phase.value,
        "thesis": state.get("thesis"),
        "direction": state.get("direction"),
        "level_kind": state.get("level_kind"),
        "level": state.get("level"),
        "spx_level": state.get("spx_level", state.get("level")),
        "spot": observation.spot,
        "es": observation.es,
        "levels": dict(observation.levels),
        "quality_ok": observation.quality_ok,
        "quality_reason": observation.quality_reason,
        "structure_change_pending": structure_change_pending,
        "new_arm_blocked": new_arm_blocked,
        "spot_source": observation.spot_source,
        "trigger_coordinate_kind": observation.trigger_coordinate_kind,
        "trigger_instrument_id": observation.trigger_instrument_id,
        "trigger_basis_points": observation.trigger_basis_points,
        "level_source": observation.level_source,
        "reason": transition.reason,
        "formal_signal": formal_signal,
        "level_path_confirmed": formal_signal,
        "actionable": False,
        "action_gate": "trade_intent_required",
        "attribution": transition_attribution(
            transition.current_phase,
            transition.reason,
        ),
        "state": dict(state),
    }


def build_health_record(
    observation: LevelObservation,
    transition,
    *,
    rth: bool,
    formal_signal_enabled: bool,
    tick: object | None,
    settings: LevelDecisionSettings,
    stable_structure: Mapping[str, object] | None,
    structure_stability: Mapping[str, object] | None,
    structure_change_pending: bool,
    new_arm_blocked: bool,
) -> dict[str, object]:
    formal_signal = formal_signal_enabled and transition.current_phase is LevelPhase.CONFIRMED
    engine_health = getattr(tick, "health", None)
    structure_candidate = (
        structure_stability.get("candidate")
        if isinstance(structure_stability, Mapping)
        else None
    )
    timestamp = _utc(observation.at).isoformat()
    return {
        "schema_version": 2,
        "record_key": timestamp,
        "at": timestamp,
        "session_date": observation.session_date,
        "session_mode": "rth" if rth else "globex",
        "tick_id": getattr(tick, "tick_id", None),
        "source_snapshot_id": getattr(tick, "source_snapshot_id", None),
        "spot": observation.spot,
        "spx_spot": observation.spx_spot,
        "es": observation.es,
        "levels": dict(observation.levels),
        "spx_levels": dict(observation.spx_levels or {}),
        "quality_ok": observation.quality_ok,
        "quality_reason": observation.quality_reason,
        "spot_source": observation.spot_source,
        "level_source": observation.level_source,
        "trigger_coordinate_kind": observation.trigger_coordinate_kind,
        "trigger_instrument_id": observation.trigger_instrument_id,
        "trigger_basis_points": observation.trigger_basis_points,
        "stable_structure": dict(stable_structure or {}),
        "structure_candidate": (
            dict(structure_candidate) if isinstance(structure_candidate, Mapping) else None
        ),
        "structure_change_pending": structure_change_pending,
        "new_arm_blocked": new_arm_blocked,
        "machine_settings": asdict(settings),
        "engine_health": (
            engine_health.to_dict() if hasattr(engine_health, "to_dict") else None
        ),
        "phase": transition.current_phase.value,
        "formal_signal": formal_signal,
        "level_path_confirmed": formal_signal,
        "actionable": False,
        "action_gate": "trade_intent_required",
    }


def transition_attribution(phase: LevelPhase, reason: str) -> str:
    if phase is LevelPhase.CONFIRMED:
        return "confirmed_pending_outcome"
    if phase is LevelPhase.EXPIRED:
        return "no_confirmation"
    if phase is not LevelPhase.INVALIDATED:
        return "state_progression"
    if reason == "structure_drift":
        return "level_error"
    if "data" in reason or "stale" in reason or "unavailable" in reason:
        return "data_error"
    if reason == "crossed_invalidation":
        return "false_break_or_rejection"
    return "invalidated_other"


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("level-decision record timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)
