"""Pure mutually-exclusive wall/flip decision state machine."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Mapping


class LevelPhase(StrEnum):
    FAR = "far"
    APPROACHING = "approaching"
    TESTING = "testing"
    BREAK_PENDING = "break_pending"
    REJECT_PENDING = "reject_pending"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    RETEST = "retest"
    CONFIRMED = "confirmed"
    INVALIDATED = "invalidated"
    EXPIRED = "expired"


class LevelThesis(StrEnum):
    NONE = "none"
    BREAKOUT = "breakout"
    FADE = "fade"


@dataclass(frozen=True)
class LevelDecisionSettings:
    approach_points: float = 12.0
    test_points: float = 4.0
    break_buffer_points: float = 3.0
    reject_points: float = 6.0
    accept_hold_seconds: float = 20.0
    retest_points: float = 4.0
    confirm_move_points: float = 4.0
    confirm_hold_seconds: float = 10.0
    phase_timeout_seconds: float = 90.0
    event_ttl_seconds: float = 300.0
    data_grace_seconds: float = 30.0
    structure_drift_points: float = 5.0
    es_confirm_ratio: float = 0.25
    terminal_rearm_seconds: float = 30.0


@dataclass(frozen=True)
class LevelObservation:
    at: datetime
    spot: float | None
    es: float | None
    levels: Mapping[str, float]
    quality_ok: bool
    quality_reason: str | None = None
    session_date: str = ""
    spot_source: str = "unknown"
    level_source: str = "unknown"
    spx_levels: Mapping[str, float] | None = None
    trigger_coordinate_kind: str = "unknown"
    trigger_instrument_id: str | None = None
    trigger_basis_points: float | None = None
    spx_spot: float | None = None


@dataclass(frozen=True)
class LevelTransition:
    previous_phase: LevelPhase
    current_phase: LevelPhase
    state: dict[str, object]
    changed: bool
    reason: str


LEVEL_OUTSIDE_DIRECTION = {
    "put_wall": -1,
    "flip_low": -1,
    "flip_high": 1,
    "call_wall": 1,
}
TERMINAL_PHASES = frozenset({LevelPhase.INVALIDATED, LevelPhase.EXPIRED})


def empty_level_state(now: datetime) -> dict[str, object]:
    return {
        "schema_version": 1,
        "phase": LevelPhase.FAR.value,
        "thesis": LevelThesis.NONE.value,
        "updated_at": _utc(now).isoformat(),
    }


def advance_level_decision(
    previous: Mapping[str, object] | None,
    observation: LevelObservation,
    *,
    settings: LevelDecisionSettings | None = None,
) -> LevelTransition:
    settings = settings or LevelDecisionSettings()
    now = _utc(observation.at)
    state = dict(previous) if isinstance(previous, Mapping) else empty_level_state(now)
    phase = _phase(state)

    if not observation.quality_ok or observation.spot is None or observation.es is None:
        return _handle_bad_quality(state, phase, observation, settings=settings)
    state.pop("quality_failed_at", None)

    if phase is LevelPhase.FAR:
        return _arm_nearest_level(observation, settings=settings)
    if phase in TERMINAL_PHASES:
        return _handle_terminal_rearm(
            state,
            phase,
            observation,
            settings=settings,
        )
    if _expired(state, now):
        if phase is LevelPhase.CONFIRMED:
            return _transition(state, phase, LevelPhase.EXPIRED, now, "confirmed_ttl_elapsed")
        level = state.get("level")
        if (
            isinstance(level, int | float)
            and observation.spot is not None
            and abs(float(observation.spot) - float(level)) <= settings.approach_points
        ):
            state["expires_at"] = (now + timedelta(seconds=settings.event_ttl_seconds)).isoformat()
            return _unchanged(state, phase, now, "event_ttl_extended_near_level")
        return _transition(state, phase, LevelPhase.EXPIRED, now, "event_ttl_elapsed")
    if (
        str(state.get("trigger_coordinate_kind") or "unknown")
        != observation.trigger_coordinate_kind
    ):
        return _transition(state, phase, LevelPhase.INVALIDATED, now, "trigger_coordinate_changed")
    if _structure_drifted(state, observation, settings):
        return _transition(state, phase, LevelPhase.INVALIDATED, now, "structure_drift")

    spot = float(observation.spot)
    level = float(state["level"])
    outside = int(state["outside_direction"])
    outside_move = outside * (spot - level)
    inside_move = -outside_move
    thesis = LevelThesis(str(state.get("thesis") or LevelThesis.NONE.value))

    if phase is LevelPhase.APPROACHING:
        if abs(spot - level) <= settings.test_points:
            return _transition(state, phase, LevelPhase.TESTING, now, "entered_test_zone")
        if abs(spot - level) > settings.approach_points:
            return _to_far(state, phase, now, "moved_away_before_test")
        return _update_extreme(state, phase, observation, "approach_continues")

    if phase is LevelPhase.TESTING:
        if outside_move >= settings.break_buffer_points:
            state["thesis"] = LevelThesis.BREAKOUT.value
            return _transition(
                state, phase, LevelPhase.BREAK_PENDING, now, "crossed_outside_buffer"
            )
        if inside_move >= settings.reject_points:
            state["thesis"] = LevelThesis.FADE.value
            return _transition(state, phase, LevelPhase.REJECT_PENDING, now, "rejected_from_level")
        return _update_extreme(state, phase, observation, "testing_continues")

    desired_direction = outside if thesis is LevelThesis.BREAKOUT else -outside
    desired_move = desired_direction * (spot - level)
    if desired_move <= -settings.break_buffer_points:
        return _transition(state, phase, LevelPhase.INVALIDATED, now, "crossed_invalidation")
    if _phase_timed_out(state, now, settings):
        return _transition(state, phase, LevelPhase.EXPIRED, now, "phase_timeout")

    if phase in {LevelPhase.BREAK_PENDING, LevelPhase.REJECT_PENDING}:
        threshold = (
            settings.break_buffer_points
            if phase is LevelPhase.BREAK_PENDING
            else settings.reject_points
        )
        target = LevelPhase.ACCEPTED if phase is LevelPhase.BREAK_PENDING else LevelPhase.REJECTED
        if (
            desired_move >= threshold
            and _phase_age(state, now) >= settings.accept_hold_seconds
            and _es_confirms(state, observation, desired_direction, settings)
        ):
            return _transition(state, phase, target, now, "direction_accepted")
        return _update_extreme(state, phase, observation, "pending_acceptance")

    if phase in {LevelPhase.ACCEPTED, LevelPhase.REJECTED}:
        if abs(spot - level) <= settings.retest_points:
            state.pop("confirm_started_at", None)
            return _transition(state, phase, LevelPhase.RETEST, now, "returned_for_retest")
        return _update_extreme(state, phase, observation, "waiting_for_retest")

    if phase is LevelPhase.RETEST:
        if desired_move < settings.confirm_move_points:
            state.pop("confirm_started_at", None)
            return _update_extreme(state, phase, observation, "retest_not_resolved")
        started = _optional_datetime(state.get("confirm_started_at"))
        if started is None:
            state["confirm_started_at"] = now.isoformat()
            return _update_extreme(state, phase, observation, "confirmation_hold_started")
        if (now - started).total_seconds() >= settings.confirm_hold_seconds and _es_confirms(
            state, observation, desired_direction, settings
        ):
            state["confirmed_at"] = now.isoformat()
            state["direction"] = "up" if desired_direction > 0 else "down"
            return _transition(state, phase, LevelPhase.CONFIRMED, now, "retest_confirmed")
        return _update_extreme(state, phase, observation, "confirmation_hold")

    return _unchanged(state, phase, now, "no_transition")


def _arm_nearest_level(
    observation: LevelObservation,
    *,
    settings: LevelDecisionSettings,
) -> LevelTransition:
    now = _utc(observation.at)
    spot = float(observation.spot or 0.0)
    eligible = [
        (abs(spot - float(level)), kind, float(level))
        for kind, level in observation.levels.items()
        if kind in LEVEL_OUTSIDE_DIRECTION and abs(spot - float(level)) <= settings.approach_points
    ]
    if not eligible:
        state = empty_level_state(now)
        return LevelTransition(LevelPhase.FAR, LevelPhase.FAR, state, False, "no_near_level")
    _distance, kind, level = min(eligible, key=lambda row: (row[0], row[1]))
    event_id = _event_id(observation.session_date, kind, level, now)
    phase = (
        LevelPhase.TESTING if abs(spot - level) <= settings.test_points else LevelPhase.APPROACHING
    )
    state: dict[str, object] = {
        "schema_version": 1,
        "event_id": event_id,
        "phase": phase.value,
        "thesis": LevelThesis.NONE.value,
        "level_kind": kind,
        "level": level,
        "spx_level": float((observation.spx_levels or {}).get(kind, level)),
        "trigger_coordinate_kind": observation.trigger_coordinate_kind,
        "trigger_instrument_id": observation.trigger_instrument_id,
        "trigger_basis_points": observation.trigger_basis_points,
        "outside_direction": LEVEL_OUTSIDE_DIRECTION[kind],
        "started_at": now.isoformat(),
        "phase_at": now.isoformat(),
        "expires_at": (now + timedelta(seconds=settings.event_ttl_seconds)).isoformat(),
        "start_spot": spot,
        "start_es": float(observation.es or 0.0),
        "last_spot": spot,
        "last_es": float(observation.es or 0.0),
        "updated_at": now.isoformat(),
        "transition_count": 1,
        "reason": "nearest_level_armed",
    }
    return LevelTransition(LevelPhase.FAR, phase, state, True, "nearest_level_armed")


def _handle_bad_quality(
    state: dict[str, object],
    phase: LevelPhase,
    observation: LevelObservation,
    *,
    settings: LevelDecisionSettings,
) -> LevelTransition:
    now = _utc(observation.at)
    if phase in TERMINAL_PHASES or phase is LevelPhase.FAR:
        return _unchanged(state, phase, now, observation.quality_reason or "data_blocked")
    failed_at = _optional_datetime(state.get("quality_failed_at"))
    if failed_at is None:
        state["quality_failed_at"] = now.isoformat()
        return _unchanged(state, phase, now, observation.quality_reason or "data_grace_started")
    if (now - failed_at).total_seconds() >= settings.data_grace_seconds:
        return _transition(
            state,
            phase,
            LevelPhase.INVALIDATED,
            now,
            observation.quality_reason or "data_quality_timeout",
        )
    return _unchanged(state, phase, now, observation.quality_reason or "data_grace")


def _structure_drifted(
    state: Mapping[str, object],
    observation: LevelObservation,
    settings: LevelDecisionSettings,
) -> bool:
    kind = str(state.get("level_kind") or "")
    current = observation.levels.get(kind)
    return (
        current is None
        or abs(float(current) - float(state["level"])) > settings.structure_drift_points
    )


def _es_confirms(
    state: Mapping[str, object],
    observation: LevelObservation,
    direction: int,
    settings: LevelDecisionSettings,
) -> bool:
    start_es = float(state.get("start_es") or 0.0)
    start_spot = float(state.get("start_spot") or 0.0)
    if not start_es or observation.es is None or observation.spot is None:
        return False
    es_move = direction * (float(observation.es) - start_es)
    spx_move = abs(float(observation.spot) - start_spot)
    return es_move >= max(spx_move * settings.es_confirm_ratio, 0.25)


def _update_extreme(
    state: dict[str, object],
    phase: LevelPhase,
    observation: LevelObservation,
    reason: str,
) -> LevelTransition:
    state["last_spot"] = observation.spot
    state["last_es"] = observation.es
    state["updated_at"] = _utc(observation.at).isoformat()
    return LevelTransition(phase, phase, state, False, reason)


def _transition(
    state: dict[str, object],
    previous: LevelPhase,
    current: LevelPhase,
    now: datetime,
    reason: str,
) -> LevelTransition:
    state["phase"] = current.value
    state["phase_at"] = now.isoformat()
    state["updated_at"] = now.isoformat()
    state["reason"] = reason
    state["transition_count"] = int(state.get("transition_count") or 0) + 1
    return LevelTransition(previous, current, state, previous is not current, reason)


def _to_far(
    state: dict[str, object], previous: LevelPhase, now: datetime, reason: str
) -> LevelTransition:
    new_state = empty_level_state(now)
    new_state["reason"] = reason
    return LevelTransition(previous, LevelPhase.FAR, new_state, True, reason)


def _unchanged(
    state: dict[str, object], phase: LevelPhase, now: datetime, reason: str
) -> LevelTransition:
    state["updated_at"] = now.isoformat()
    return LevelTransition(phase, phase, state, False, reason)


def _phase(state: Mapping[str, object]) -> LevelPhase:
    try:
        return LevelPhase(str(state.get("phase") or LevelPhase.FAR.value))
    except ValueError:
        return LevelPhase.FAR


def _phase_age(state: Mapping[str, object], now: datetime) -> float:
    phase_at = _optional_datetime(state.get("phase_at")) or now
    return max((now - phase_at).total_seconds(), 0.0)


def _phase_timed_out(
    state: Mapping[str, object], now: datetime, settings: LevelDecisionSettings
) -> bool:
    return _phase_age(state, now) > settings.phase_timeout_seconds


def _expired(state: Mapping[str, object], now: datetime) -> bool:
    expires_at = _optional_datetime(state.get("expires_at"))
    return expires_at is not None and now >= expires_at


def _handle_terminal_rearm(
    state: dict[str, object],
    phase: LevelPhase,
    observation: LevelObservation,
    *,
    settings: LevelDecisionSettings,
) -> LevelTransition:
    now = _utc(observation.at)
    if _phase_age(state, now) < settings.terminal_rearm_seconds:
        return _unchanged(state, phase, now, "terminal_hold")
    if phase is LevelPhase.EXPIRED:
        armed = _arm_nearest_level(observation, settings=settings)
        if armed.current_phase is not LevelPhase.FAR:
            return LevelTransition(
                phase,
                armed.current_phase,
                armed.state,
                True,
                "expired_event_rearmed",
            )
    level = state.get("level")
    if isinstance(level, int | float) and observation.spot is not None:
        if abs(float(observation.spot) - float(level)) <= settings.approach_points:
            return _unchanged(state, phase, now, "terminal_waiting_for_level_exit")
    return _to_far(state, phase, now, "terminal_level_exited")


def _optional_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("level-decision timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def _event_id(session_date: str, kind: str, level: float, at: datetime) -> str:
    token = f"{session_date}|{kind}|{level:.4f}|{at.isoformat()}"
    return "level:" + hashlib.sha256(token.encode()).hexdigest()[:24]
