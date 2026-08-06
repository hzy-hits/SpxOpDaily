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
    arm_allowed: bool = True,
    arm_block_reason: str | None = None,
    trigger_coordinate_kind: str = "unknown",
    trigger_basis_points: float | None = None,
    spx_spot: float | None = None,
    spx_levels: dict[str, float] | None = None,
    session_mode: str = "rth",
    session_date: str = "2026-07-13",
) -> LevelObservation:
    return LevelObservation(
        at=NOW + timedelta(seconds=seconds),
        spot=spot,
        es=es,
        levels=levels or {"put_wall": 100.0, "call_wall": 120.0},
        quality_ok=quality_ok,
        quality_reason=None if quality_ok else "stale_chain",
        session_date=session_date,
        session_mode=session_mode,
        spx_levels=spx_levels,
        trigger_coordinate_kind=trigger_coordinate_kind,
        trigger_basis_points=trigger_basis_points,
        spx_spot=spx_spot,
        arm_allowed=arm_allowed,
        arm_block_reason=arm_block_reason,
    )


def advance(
    state,
    seconds: int,
    *,
    spot: float,
    es: float,
    settings: LevelDecisionSettings = SETTINGS,
    **kwargs,
):
    return advance_level_decision(
        state,
        observation(seconds, spot=spot, es=es, **kwargs),
        settings=settings,
    )


def test_breakout_requires_acceptance_retest_and_confirmation_hold() -> None:
    armed = advance(None, 0, spot=95.0, es=5000.0)
    assert armed.state["reentry_generation"] == 0
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
    assert (
        confirmed.state["expires_at"]
        == (NOW + timedelta(seconds=56 + SETTINGS.event_ttl_seconds)).isoformat()
    )


def test_es_confirmation_is_latched_when_thesis_starts_after_a_long_approach() -> None:
    armed = advance(None, 0, spot=95.0, es=5000.0)
    testing = advance(armed.state, 250, spot=99.0, es=5050.0)
    pending = advance(testing.state, 255, spot=96.0, es=5049.0)

    accepted = advance(pending.state, 276, spot=95.0, es=5047.0)

    assert pending.state["confirmation_start_es"] == 5049.0
    assert pending.state["confirmation_start_spot"] == 96.0
    assert accepted.current_phase is LevelPhase.ACCEPTED
    assert accepted.reason == "direction_accepted"


def test_accepted_one_way_breakout_confirms_without_mandatory_retest() -> None:
    armed = advance(None, 0, spot=95.0, es=5000.0)
    testing = advance(armed.state, 5, spot=99.0, es=5000.0)
    pending = advance(testing.state, 10, spot=96.0, es=4999.0)
    accepted = advance(pending.state, 31, spot=95.0, es=4997.0)

    confirmed = advance(accepted.state, 42, spot=94.0, es=4995.0)

    assert confirmed.current_phase is LevelPhase.CONFIRMED
    assert confirmed.reason == "accepted_follow_through_confirmed"
    assert confirmed.state["direction"] == "down"


def test_confirmed_path_invalidates_when_price_reclaims_the_level() -> None:
    armed = advance(None, 0, spot=95.0, es=5000.0)
    testing = advance(armed.state, 5, spot=99.0, es=5000.0)
    pending = advance(testing.state, 10, spot=96.0, es=4999.0)
    accepted = advance(pending.state, 31, spot=95.0, es=4997.0)
    retest = advance(accepted.state, 40, spot=99.0, es=4998.0)
    holding = advance(retest.state, 45, spot=95.0, es=4996.0)
    confirmed = advance(holding.state, 56, spot=94.0, es=4994.0)

    invalidated = advance(confirmed.state, 65, spot=104.0, es=5004.0)

    assert invalidated.previous_phase is LevelPhase.CONFIRMED
    assert invalidated.current_phase is LevelPhase.INVALIDATED
    assert invalidated.reason == "crossed_invalidation"


def test_confirmed_path_expires_instead_of_remaining_valid_indefinitely() -> None:
    armed = advance(None, 0, spot=95.0, es=5000.0)
    testing = advance(armed.state, 5, spot=99.0, es=5000.0)
    pending = advance(testing.state, 10, spot=96.0, es=4999.0)
    accepted = advance(pending.state, 31, spot=95.0, es=4997.0)
    retest = advance(accepted.state, 40, spot=99.0, es=4998.0)
    holding = advance(retest.state, 45, spot=95.0, es=4996.0)
    confirmed = advance(holding.state, 56, spot=94.0, es=4994.0)

    still_confirmed = advance(confirmed.state, 147, spot=94.0, es=4994.0)

    assert still_confirmed.current_phase is LevelPhase.CONFIRMED
    assert still_confirmed.reason == "no_transition"

    expired = advance(still_confirmed.state, 357, spot=94.0, es=4994.0)

    assert expired.previous_phase is LevelPhase.CONFIRMED
    assert expired.current_phase is LevelPhase.EXPIRED
    assert expired.reason == "confirmed_ttl_elapsed"


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


def test_sustained_bad_quality_degrades_without_reversing_active_decision() -> None:
    armed = advance(None, 0, spot=95.0, es=5000.0)
    grace = advance(
        armed.state,
        5,
        spot=95.0,
        es=5000.0,
        quality_ok=False,
        arm_allowed=False,
        arm_block_reason="structure_change_pending_new_arm_blocked",
    )
    assert grace.current_phase is LevelPhase.APPROACHING
    degraded = advance(
        grace.state,
        36,
        spot=95.0,
        es=5000.0,
        quality_ok=False,
        arm_allowed=False,
        arm_block_reason="structure_change_pending_new_arm_blocked",
    )
    assert degraded.current_phase is LevelPhase.APPROACHING
    assert degraded.changed is False
    assert degraded.reason == "stale_chain"
    assert degraded.state["quality_status"] == "degraded"
    assert degraded.state["quality_reason"] == "stale_chain"
    assert degraded.state["quality_failed_at"] == grace.state["quality_failed_at"]

    recovered = advance(degraded.state, 37, spot=95.0, es=5000.0)
    assert recovered.current_phase is LevelPhase.APPROACHING
    assert "quality_status" not in recovered.state
    assert "quality_reason" not in recovered.state
    assert "quality_failed_at" not in recovered.state


def test_bad_quality_cannot_keep_an_expired_event_alive_or_mutate_trade_ownership() -> None:
    armed = advance(None, 0, spot=95.0, es=5000.0)
    state = {
        **armed.state,
        # This is an opaque handoff marker for a separately owned trade
        # lifecycle; the level machine must preserve it while expiring itself.
        "active_trade_id": "trade:independent-owner",
    }

    expired = advance(
        state,
        301,
        spot=95.0,
        es=5000.0,
        quality_ok=False,
    )

    assert expired.previous_phase is LevelPhase.APPROACHING
    assert expired.current_phase is LevelPhase.EXPIRED
    assert expired.changed is True
    assert expired.reason == "event_ttl_elapsed_during_data_degradation"
    assert expired.state["quality_status"] == "degraded"
    assert expired.state["active_trade_id"] == "trade:independent-owner"


def test_pending_structure_blocks_new_arm_without_failing_data_quality() -> None:
    blocked = advance(
        None,
        0,
        spot=99.0,
        es=5000.0,
        arm_allowed=False,
        arm_block_reason="structure_change_pending_new_arm_blocked",
    )

    assert blocked.current_phase is LevelPhase.FAR
    assert blocked.changed is False
    assert blocked.reason == "structure_change_pending_new_arm_blocked"


def test_pending_structure_does_not_interrupt_active_lifecycle_or_ttl() -> None:
    armed = advance(None, 0, spot=95.0, es=5000.0)
    testing = advance(
        armed.state,
        5,
        spot=99.0,
        es=5000.0,
        arm_allowed=False,
        arm_block_reason="structure_change_pending_new_arm_blocked",
    )
    assert testing.current_phase is LevelPhase.TESTING
    assert testing.reason == "entered_test_zone"

    expired = advance(
        testing.state,
        301,
        spot=115.0,
        es=5000.0,
        arm_allowed=False,
        arm_block_reason="structure_change_pending_new_arm_blocked",
    )

    assert expired.current_phase is LevelPhase.EXPIRED
    assert expired.reason == "event_ttl_elapsed"


def test_pending_structure_blocks_terminal_rearm() -> None:
    armed = advance(None, 0, spot=95.0, es=5000.0)
    terminal = {
        **armed.state,
        "phase": LevelPhase.EXPIRED.value,
        "phase_at": NOW.isoformat(),
    }
    blocked = advance(
        terminal,
        31,
        spot=95.0,
        es=5000.0,
        arm_allowed=False,
        arm_block_reason="structure_change_pending_new_arm_blocked",
    )

    assert blocked.current_phase is LevelPhase.EXPIRED
    assert blocked.changed is False
    assert blocked.reason == "structure_change_pending_new_arm_blocked"


def test_session_boundary_resets_terminal_event_even_when_new_arm_is_blocked() -> None:
    armed = advance(
        None,
        0,
        spot=95.0,
        es=5000.0,
        session_mode="globex",
        trigger_coordinate_kind="chain_implied_spx",
    )
    terminal = {
        **armed.state,
        "phase": LevelPhase.EXPIRED.value,
        "phase_at": NOW.isoformat(),
    }

    reset = advance(
        terminal,
        31,
        spot=99.0,
        es=5000.0,
        session_mode="rth",
        trigger_coordinate_kind="official_spx",
        arm_allowed=False,
        arm_block_reason="structure_change_pending_new_arm_blocked",
    )

    assert reset.current_phase is LevelPhase.FAR
    assert reset.changed is True
    assert reset.reason == "session_boundary_reset"


def test_session_boundary_resets_before_bad_quality_handling() -> None:
    armed = advance(
        None,
        0,
        spot=95.0,
        es=5000.0,
        session_mode="gth",
    )

    reset = advance(
        armed.state,
        5,
        spot=95.0,
        es=5000.0,
        session_mode="closed",
        quality_ok=False,
        arm_allowed=False,
        arm_block_reason="market_session_closed",
    )

    assert reset.current_phase is LevelPhase.FAR
    assert reset.changed is True
    assert reset.reason == "session_boundary_reset"


def test_closed_session_cannot_arm_a_new_level() -> None:
    blocked = advance(
        None,
        0,
        spot=95.0,
        es=5000.0,
        session_mode="closed",
        arm_allowed=False,
        arm_block_reason="market_session_closed",
    )

    assert blocked.current_phase is LevelPhase.FAR
    assert blocked.changed is False
    assert blocked.reason == "market_session_closed"


def test_legacy_globex_state_resets_once_when_typed_gth_session_starts() -> None:
    legacy = advance(
        None,
        0,
        spot=95.0,
        es=5000.0,
        session_mode="globex",
    )

    reset = advance(
        legacy.state,
        5,
        spot=95.0,
        es=5000.0,
        session_mode="gth",
    )

    assert reset.current_phase is LevelPhase.FAR
    assert reset.changed is True
    assert reset.reason == "session_boundary_reset"


def test_same_mode_next_session_resets_stale_level_before_ttl_extension() -> None:
    prior = advance(
        None,
        0,
        spot=95.0,
        es=5000.0,
        session_mode="gth",
        session_date="2026-07-13",
    )

    reset = advance(
        prior.state,
        86_400,
        spot=95.0,
        es=5000.0,
        levels={"put_wall": 110.0, "call_wall": 130.0},
        session_mode="gth",
        session_date="2026-07-14",
    )

    assert reset.current_phase is LevelPhase.FAR
    assert reset.changed is True
    assert reset.reason == "session_boundary_reset"
    assert reset.state.get("event_id") is None


def test_pending_structure_does_not_pause_active_phase_timeout() -> None:
    armed = advance(None, 0, spot=95.0, es=5000.0)
    testing = advance(
        armed.state,
        5,
        spot=99.0,
        es=5000.0,
        arm_allowed=False,
        arm_block_reason="structure_change_pending_new_arm_blocked",
    )
    pending = advance(
        testing.state,
        10,
        spot=96.0,
        es=4999.0,
        arm_allowed=False,
        arm_block_reason="structure_change_pending_new_arm_blocked",
    )
    timed_out = advance(
        pending.state,
        101,
        spot=96.0,
        es=4999.0,
        arm_allowed=False,
        arm_block_reason="structure_change_pending_new_arm_blocked",
    )

    assert timed_out.current_phase is LevelPhase.EXPIRED
    assert timed_out.reason == "phase_timeout"


def test_rth_pending_phase_expires_after_ninety_seconds() -> None:
    settings = LevelDecisionSettings(event_ttl_seconds=600.0)
    rth = {"session_mode": "rth", "settings": settings}
    armed = advance(None, 0, spot=95.0, es=5000.0, **rth)
    testing = advance(armed.state, 5, spot=99.0, es=5000.0, **rth)
    pending = advance(testing.state, 10, spot=96.0, es=4999.0, **rth)

    at_timeout = advance(pending.state, 100, spot=96.0, es=4999.0, **rth)
    assert at_timeout.current_phase is LevelPhase.BREAK_PENDING

    timed_out = advance(at_timeout.state, 101, spot=96.0, es=4999.0, **rth)
    assert timed_out.current_phase is LevelPhase.EXPIRED
    assert timed_out.reason == "phase_timeout"


def test_gth_pending_phase_uses_five_minute_timeout() -> None:
    settings = LevelDecisionSettings(
        phase_timeout_seconds=300.0,
        event_ttl_seconds=300.0,
    )
    gth = {"session_mode": "gth", "settings": settings}
    armed = advance(None, 0, spot=95.0, es=5000.0, **gth)
    testing = advance(armed.state, 5, spot=99.0, es=5000.0, **gth)
    pending = advance(testing.state, 10, spot=96.0, es=4999.0, **gth)

    after_rth_timeout = advance(pending.state, 101, spot=96.0, es=4999.0, **gth)
    assert after_rth_timeout.current_phase is LevelPhase.BREAK_PENDING
    assert after_rth_timeout.reason == "pending_acceptance"

    at_gth_timeout = advance(
        after_rth_timeout.state,
        310,
        spot=96.0,
        es=4999.0,
        **gth,
    )
    assert at_gth_timeout.current_phase is LevelPhase.BREAK_PENDING

    timed_out = advance(at_gth_timeout.state, 311, spot=96.0, es=4999.0, **gth)
    assert timed_out.current_phase is LevelPhase.EXPIRED
    assert timed_out.reason == "phase_timeout"


def test_rth_pending_phase_times_out_while_data_is_degraded() -> None:
    armed = advance(None, 0, spot=95.0, es=5000.0)
    testing = advance(armed.state, 5, spot=99.0, es=5000.0)
    pending = advance(testing.state, 10, spot=96.0, es=4999.0)

    timed_out = advance(
        pending.state,
        101,
        spot=96.0,
        es=4999.0,
        quality_ok=False,
    )

    assert timed_out.current_phase is LevelPhase.EXPIRED
    assert timed_out.reason == "phase_timeout_during_data_degradation"


def test_late_gth_pending_gets_full_phase_window_during_data_degradation() -> None:
    settings = LevelDecisionSettings(
        phase_timeout_seconds=300.0,
        event_ttl_seconds=300.0,
    )
    gth = {"session_mode": "gth", "settings": settings}
    armed = advance(None, 0, spot=95.0, es=5000.0, **gth)
    testing = advance(armed.state, 285, spot=99.0, es=5000.0, **gth)
    pending = advance(testing.state, 290, spot=96.0, es=4999.0, **gth)

    extended = advance(
        pending.state,
        300,
        spot=96.0,
        es=4999.0,
        quality_ok=False,
        **gth,
    )
    assert extended.current_phase is LevelPhase.BREAK_PENDING
    assert extended.reason == "stale_chain"

    timed_out = advance(
        extended.state,
        591,
        spot=96.0,
        es=4999.0,
        quality_ok=False,
        **gth,
    )
    assert timed_out.current_phase is LevelPhase.EXPIRED
    assert timed_out.reason == "phase_timeout_during_data_degradation"


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


def test_expired_event_must_exit_reset_band_before_rearming_same_level() -> None:
    armed = advance(None, 0, spot=95.0, es=5000.0)
    expired = {
        **armed.state,
        "phase": LevelPhase.EXPIRED.value,
        "phase_at": NOW.isoformat(),
    }
    waiting = advance(expired, 31, spot=95.0, es=5000.0)
    assert waiting.current_phase is LevelPhase.EXPIRED
    assert waiting.reason == "terminal_waiting_for_level_exit"

    exited = advance(waiting.state, 32, spot=87.0, es=4992.0)
    assert exited.current_phase is LevelPhase.FAR
    assert exited.reason == "terminal_level_exited"
    assert exited.state["next_reentry_generation"] == 1

    rearmed = advance(exited.state, 33, spot=95.0, es=5000.0)
    assert rearmed.current_phase is LevelPhase.APPROACHING
    assert rearmed.reason == "nearest_level_armed"
    assert rearmed.state["event_id"] != armed.state["event_id"]
    assert rearmed.state["reentry_generation"] == 1


def test_expired_event_clears_outside_reset_band_even_inside_approach_radius() -> None:
    armed = advance(None, 0, spot=95.0, es=5000.0)
    expired = {
        **armed.state,
        "phase": LevelPhase.EXPIRED.value,
        "phase_at": NOW.isoformat(),
    }

    # put_wall=100: seven points is outside the six-point reset band but still
    # inside the twelve-point approach radius.
    reset = advance(expired, 31, spot=93.0, es=4998.0)

    assert reset.current_phase is LevelPhase.FAR
    assert reset.reason == "terminal_level_exited"
    assert reset.state["next_reentry_generation"] == 1

    rearmed = advance(reset.state, 32, spot=93.0, es=4998.0)
    assert rearmed.current_phase is LevelPhase.APPROACHING
    assert rearmed.state["reentry_generation"] == 1


def test_terminal_structure_promotion_can_rearm_without_old_level_exit() -> None:
    armed = advance(None, 0, spot=95.0, es=5000.0)
    terminal = {
        **armed.state,
        "phase": LevelPhase.INVALIDATED.value,
        "phase_at": NOW.isoformat(),
    }
    promoted = advance(
        terminal,
        31,
        spot=106.0,
        es=5011.0,
        levels={"put_wall": 107.0, "call_wall": 120.0},
    )
    assert promoted.current_phase is LevelPhase.TESTING
    assert promoted.reason == "stable_structure_promoted_rearm"
    assert promoted.state["level"] == 107.0
    assert promoted.state["reentry_generation"] == 1


def test_terminal_removed_level_kind_clears_phantom_reset_band() -> None:
    armed = advance(None, 0, spot=95.0, es=5000.0)
    terminal = {
        **armed.state,
        "phase": LevelPhase.EXPIRED.value,
        "phase_at": NOW.isoformat(),
    }

    promoted = advance(
        terminal,
        31,
        spot=95.0,
        es=5000.0,
        levels={"call_wall": 120.0},
    )

    assert promoted.current_phase is LevelPhase.FAR
    assert promoted.reason == "stable_structure_promoted"
    assert "level" not in promoted.state


def test_terminal_removed_level_kind_can_rearm_near_replacement_kind() -> None:
    armed = advance(None, 0, spot=95.0, es=5000.0)
    terminal = {
        **armed.state,
        "phase": LevelPhase.INVALIDATED.value,
        "phase_at": NOW.isoformat(),
    }

    promoted = advance(
        terminal,
        31,
        spot=95.0,
        es=5000.0,
        levels={"flip_low": 96.0, "call_wall": 120.0},
    )

    assert promoted.current_phase is LevelPhase.TESTING
    assert promoted.reason == "stable_structure_promoted_rearm"
    assert promoted.state["level_kind"] == "flip_low"
    assert promoted.state["level"] == 96.0
    assert promoted.state["event_id"] != armed.state["event_id"]


def test_confirmation_persists_spx_coordinate_decision_spot() -> None:
    kwargs = {
        "trigger_coordinate_kind": "es_equivalent",
        "trigger_basis_points": 45.0,
        "spx_spot": 95.0,
    }
    armed = advance(None, 0, spot=140.0, es=5000.0, levels={"put_wall": 145.0}, **kwargs)
    testing = advance(armed.state, 5, spot=144.0, es=5000.0, levels={"put_wall": 145.0}, **kwargs)
    pending = advance(
        testing.state, 10, spot=141.0, es=4999.0, levels={"put_wall": 145.0}, **kwargs
    )
    accepted = advance(
        pending.state, 31, spot=140.0, es=4997.0, levels={"put_wall": 145.0}, **kwargs
    )
    retest = advance(
        accepted.state, 40, spot=144.0, es=4998.0, levels={"put_wall": 145.0}, **kwargs
    )
    holding = advance(retest.state, 45, spot=140.0, es=4996.0, levels={"put_wall": 145.0}, **kwargs)
    confirmed = advance(
        holding.state, 56, spot=139.0, es=4994.0, levels={"put_wall": 145.0}, **kwargs
    )
    assert confirmed.current_phase is LevelPhase.CONFIRMED
    assert confirmed.state["decision_spot"] == 95.0


def test_active_rth_es_event_upgrades_to_official_spx_without_reset() -> None:
    proxy = {
        "trigger_coordinate_kind": "es_equivalent",
        "trigger_basis_points": 40.0,
        "spx_spot": 7388.0,
        "spx_levels": {"flip_low": 7390.0},
    }
    armed = advance(
        None,
        0,
        spot=7428.0,
        es=7428.0,
        levels={"flip_low": 7430.0},
        **proxy,
    )
    event_id = armed.state["event_id"]

    upgraded = advance(
        armed.state,
        5,
        spot=7387.0,
        es=7427.0,
        levels={"flip_low": 7390.0},
        trigger_coordinate_kind="official_spx",
        trigger_basis_points=40.0,
        spx_spot=7387.0,
        spx_levels={"flip_low": 7390.0},
    )

    assert upgraded.current_phase is LevelPhase.BREAK_PENDING
    assert upgraded.reason == "crossed_outside_buffer"
    assert upgraded.state["event_id"] == event_id
    assert upgraded.state["trigger_coordinate_kind"] == "official_spx"
    assert upgraded.state["trigger_instrument_id"] == "index:SPX"
    assert upgraded.state["level"] == 7390.0
    assert upgraded.state["start_spot"] == 7388.0
    assert upgraded.state["coordinate_upgraded_from"] == "es_equivalent"
    assert upgraded.state["coordinate_upgraded_basis_points"] == 40.0


def test_confirmed_rth_es_event_remains_confirmed_after_official_spx_recovers() -> None:
    proxy = {
        "trigger_coordinate_kind": "es_equivalent",
        "trigger_basis_points": 40.0,
        "spx_levels": {"flip_low": 7390.0},
    }
    armed = advance(
        None,
        0,
        spot=7428.0,
        es=7428.0,
        levels={"flip_low": 7430.0},
        spx_spot=7388.0,
        **proxy,
    )
    confirmed_state = {
        **armed.state,
        "phase": LevelPhase.CONFIRMED.value,
        "thesis": LevelThesis.BREAKOUT.value,
        "direction": "down",
        "decision_spot": 7381.92,
        "phase_at": (NOW + timedelta(seconds=1)).isoformat(),
    }

    upgraded = advance(
        confirmed_state,
        5,
        spot=7381.8,
        es=7421.8,
        levels={"flip_low": 7390.0},
        trigger_coordinate_kind="official_spx",
        trigger_basis_points=40.0,
        spx_spot=7381.8,
        spx_levels={"flip_low": 7390.0},
    )

    assert upgraded.current_phase is LevelPhase.CONFIRMED
    assert upgraded.reason == "no_transition"
    assert upgraded.state["event_id"] == armed.state["event_id"]
    assert upgraded.state["trigger_coordinate_kind"] == "official_spx"
    assert upgraded.state["decision_spot"] == 7381.92


def test_active_rth_chain_event_upgrades_to_official_spx_without_reset_or_rebase() -> None:
    chain = {
        "trigger_coordinate_kind": "chain_implied_spx",
        "trigger_basis_points": 40.0,
        "spx_spot": 7388.0,
        "spx_levels": {"flip_low": 7390.0},
    }
    armed = advance(
        None,
        0,
        spot=7388.0,
        es=7428.0,
        levels={"flip_low": 7390.0},
        **chain,
    )
    event_id = armed.state["event_id"]

    upgraded = advance(
        armed.state,
        5,
        spot=7387.0,
        es=7427.0,
        levels={"flip_low": 7390.0},
        trigger_coordinate_kind="official_spx",
        trigger_basis_points=40.0,
        spx_spot=7387.0,
        spx_levels={"flip_low": 7390.0},
    )

    assert upgraded.current_phase is LevelPhase.BREAK_PENDING
    assert upgraded.reason == "crossed_outside_buffer"
    assert upgraded.state["event_id"] == event_id
    assert upgraded.state["trigger_coordinate_kind"] == "official_spx"
    assert upgraded.state["trigger_instrument_id"] == "index:SPX"
    assert upgraded.state["level"] == 7390.0
    assert upgraded.state["start_spot"] == 7388.0
    assert upgraded.state["coordinate_upgraded_from"] == "chain_implied_spx"
    assert upgraded.state["coordinate_upgraded_basis_points"] == 40.0


def test_confirmed_rth_chain_event_remains_confirmed_after_official_spx_recovers() -> None:
    chain = {
        "trigger_coordinate_kind": "chain_implied_spx",
        "trigger_basis_points": 40.0,
        "spx_levels": {"flip_low": 7390.0},
    }
    armed = advance(
        None,
        0,
        spot=7388.0,
        es=7428.0,
        levels={"flip_low": 7390.0},
        spx_spot=7388.0,
        **chain,
    )
    confirmed_state = {
        **armed.state,
        "phase": LevelPhase.CONFIRMED.value,
        "thesis": LevelThesis.BREAKOUT.value,
        "direction": "down",
        "decision_spot": 7381.92,
        "confirmation_start_spot": 7384.0,
        "last_spot": 7382.0,
        "phase_at": (NOW + timedelta(seconds=1)).isoformat(),
    }

    upgraded = advance(
        confirmed_state,
        5,
        spot=7381.8,
        es=7421.8,
        levels={"flip_low": 7390.0},
        trigger_coordinate_kind="official_spx",
        trigger_basis_points=40.0,
        spx_spot=7381.8,
        spx_levels={"flip_low": 7390.0},
    )

    assert upgraded.current_phase is LevelPhase.CONFIRMED
    assert upgraded.reason == "no_transition"
    assert upgraded.state["event_id"] == armed.state["event_id"]
    assert upgraded.state["trigger_coordinate_kind"] == "official_spx"
    assert upgraded.state["start_spot"] == 7388.0
    assert upgraded.state["confirmation_start_spot"] == 7384.0
    assert upgraded.state["last_spot"] == 7382.0
    assert upgraded.state["decision_spot"] == 7381.92
    assert upgraded.state["coordinate_upgraded_from"] == "chain_implied_spx"
