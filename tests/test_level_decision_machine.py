from __future__ import annotations

from datetime import datetime, timedelta, timezone

from spx_spark.application.order_map.level_decision_machine import (
    LevelDecisionSettings,
    LevelObservation,
    LevelPhase,
    LevelThesis,
    advance_level_decision,
)


NOW = datetime(2026, 7, 13, 14, 30, tzinfo=timezone.utc)
SETTINGS = LevelDecisionSettings()


def observation(
    seconds: int,
    *,
    spot: float,
    es: float,
    levels: dict[str, float] | None = None,
    quality_ok: bool = True,
) -> LevelObservation:
    return LevelObservation(
        at=NOW + timedelta(seconds=seconds),
        spot=spot,
        es=es,
        levels=levels or {"put_wall": 100.0, "call_wall": 120.0},
        quality_ok=quality_ok,
        quality_reason=None if quality_ok else "stale_chain",
        session_date="2026-07-13",
    )


def advance(state, seconds: int, *, spot: float, es: float, **kwargs):
    return advance_level_decision(
        state,
        observation(seconds, spot=spot, es=es, **kwargs),
        settings=SETTINGS,
    )


def test_breakout_requires_acceptance_retest_and_confirmation_hold() -> None:
    armed = advance(None, 0, spot=95.0, es=5000.0)
    assert armed.current_phase is LevelPhase.APPROACHING
    assert armed.state["level_kind"] == "put_wall"

    testing = advance(armed.state, 5, spot=99.0, es=5000.0)
    pending = advance(testing.state, 10, spot=96.0, es=4999.0)
    assert pending.current_phase is LevelPhase.BREAK_PENDING
    assert pending.state["thesis"] == LevelThesis.BREAKOUT.value

    accepted = advance(pending.state, 31, spot=95.0, es=4997.0)
    assert accepted.current_phase is LevelPhase.ACCEPTED
    retest = advance(accepted.state, 40, spot=99.0, es=4998.0)
    assert retest.current_phase is LevelPhase.RETEST

    holding = advance(retest.state, 45, spot=95.0, es=4996.0)
    assert holding.current_phase is LevelPhase.RETEST
    confirmed = advance(holding.state, 56, spot=94.0, es=4994.0)
    assert confirmed.current_phase is LevelPhase.CONFIRMED
    assert confirmed.state["direction"] == "down"


def test_fade_and_breakout_are_mutually_exclusive_for_one_frozen_level() -> None:
    levels = {"put_wall": 90.0, "flip_low": 100.0, "flip_high": 105.0, "call_wall": 110.0}
    armed = advance(None, 0, spot=108.0, es=5000.0, levels=levels)
    assert armed.current_phase is LevelPhase.TESTING
    assert armed.state["level_kind"] == "call_wall"

    pending = advance(armed.state, 5, spot=103.0, es=4999.0, levels=levels)
    assert pending.current_phase is LevelPhase.REJECT_PENDING
    assert pending.state["thesis"] == LevelThesis.FADE.value
    rejected = advance(pending.state, 26, spot=102.0, es=4997.0, levels=levels)
    assert rejected.current_phase is LevelPhase.REJECTED
    retest = advance(rejected.state, 35, spot=109.0, es=4998.0, levels=levels)
    holding = advance(retest.state, 40, spot=104.0, es=4996.0, levels=levels)
    confirmed = advance(holding.state, 51, spot=103.0, es=4994.0, levels=levels)
    assert confirmed.current_phase is LevelPhase.CONFIRMED
    assert confirmed.state["direction"] == "down"
    assert confirmed.state["thesis"] == LevelThesis.FADE.value


def test_nearest_level_is_the_only_active_level() -> None:
    levels = {"put_wall": 90.0, "flip_low": 99.0, "flip_high": 103.0, "call_wall": 110.0}
    result = advance(None, 0, spot=100.0, es=5000.0, levels=levels)
    assert result.state["level_kind"] == "flip_low"
    assert "active_levels" not in result.state


def test_sustained_bad_quality_invalidates_active_decision() -> None:
    armed = advance(None, 0, spot=95.0, es=5000.0)
    grace = advance(
        armed.state,
        5,
        spot=95.0,
        es=5000.0,
        quality_ok=False,
    )
    assert grace.current_phase is LevelPhase.APPROACHING
    invalid = advance(
        grace.state,
        36,
        spot=95.0,
        es=5000.0,
        quality_ok=False,
    )
    assert invalid.current_phase is LevelPhase.INVALIDATED
    assert invalid.reason == "stale_chain"


def test_structure_drift_invalidates_frozen_level() -> None:
    armed = advance(None, 0, spot=95.0, es=5000.0)
    result = advance(
        armed.state,
        5,
        spot=96.0,
        es=5000.0,
        levels={"put_wall": 110.0, "call_wall": 120.0},
    )
    assert result.current_phase is LevelPhase.INVALIDATED
    assert result.reason == "structure_drift"


def test_active_level_nearby_extends_ttl_instead_of_entering_a_dead_zone() -> None:
    armed = advance(None, 0, spot=95.0, es=5000.0)
    extended = advance(armed.state, 301, spot=95.0, es=5000.0)
    assert extended.current_phase is LevelPhase.APPROACHING
    assert extended.changed is False
    assert extended.reason == "event_ttl_extended_near_level"


def test_legacy_expired_event_rearms_near_level_after_terminal_hold() -> None:
    armed = advance(None, 0, spot=95.0, es=5000.0)
    legacy = {
        **armed.state,
        "phase": LevelPhase.EXPIRED.value,
        "phase_at": NOW.isoformat(),
    }
    rearmed = advance(legacy, 31, spot=95.0, es=5000.0)
    assert rearmed.previous_phase is LevelPhase.EXPIRED
    assert rearmed.current_phase is LevelPhase.APPROACHING
    assert rearmed.reason == "expired_event_rearmed"
    assert rearmed.state["event_id"] != armed.state["event_id"]
