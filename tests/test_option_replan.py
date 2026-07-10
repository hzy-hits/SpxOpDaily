from __future__ import annotations

from datetime import datetime, timedelta, timezone

from spx_spark.ibkr.option_replan import OptionReplanController


START = datetime(2026, 7, 10, 14, 0, tzinfo=timezone.utc)


def accepted_controller() -> OptionReplanController:
    controller = OptionReplanController()
    initial = controller.observe(
        atm_strike=7500,
        source="SPX",
        observed_at=START,
        expiry="20260710",
    ).proposal
    assert initial is not None
    controller.record_result(initial, success=True)
    return controller


def test_initial_plan_is_immediate_but_acceptance_waits_for_success() -> None:
    controller = OptionReplanController()

    decision = controller.observe(
        atm_strike=7500,
        source="SPX",
        observed_at=START,
        expiry="20260710",
    )

    assert decision.proposal is not None
    assert controller.accepted_atm is None
    controller.record_result(decision.proposal, success=True)
    assert controller.accepted_atm == 7500


def test_failed_plan_backs_off_and_applied_time_drives_cooldown() -> None:
    controller = OptionReplanController()
    stale_observation = START - timedelta(hours=2)
    proposal = controller.observe(
        atm_strike=7500,
        source="SPX_stale_bootstrap",
        observed_at=stale_observation,
        decision_at=START,
        expiry="20260710",
    ).proposal
    assert proposal is not None

    controller.record_result(proposal, success=False, applied_at=START)
    retry = controller.observe(
        atm_strike=7500,
        source="SPX",
        observed_at=START + timedelta(seconds=5),
        decision_at=START + timedelta(seconds=5),
        expiry="20260710",
    )
    assert retry.reason == "failure_backoff"

    accepted = controller.observe(
        atm_strike=7500,
        source="SPX",
        observed_at=START + timedelta(seconds=31),
        decision_at=START + timedelta(seconds=31),
        expiry="20260710",
    ).proposal
    assert accepted is not None
    controller.record_result(
        accepted,
        success=True,
        applied_at=START + timedelta(seconds=31),
    )
    assert controller.last_applied_at == START + timedelta(seconds=31)


def test_ping_pong_replay_does_not_replan() -> None:
    controller = accepted_controller()
    sequence = (7480, 7510, 7480, 7510, 7545, 7480)

    decisions = [
        controller.observe(
            atm_strike=atm,
            source="SPX",
            observed_at=START + timedelta(seconds=130 + index * 5),
            expiry="20260710",
        )
        for index, atm in enumerate(sequence)
    ]

    assert all(decision.proposal is None for decision in decisions)


def test_sustained_move_requires_three_observations_over_fifteen_seconds() -> None:
    controller = accepted_controller()

    first = controller.observe(
        atm_strike=7520,
        source="SPX",
        observed_at=START + timedelta(seconds=130),
        expiry="20260710",
    )
    duplicate = controller.observe(
        atm_strike=7520,
        source="SPX",
        observed_at=START + timedelta(seconds=130),
        expiry="20260710",
    )
    second = controller.observe(
        atm_strike=7520,
        source="SPX",
        observed_at=START + timedelta(seconds=138),
        expiry="20260710",
    )
    third = controller.observe(
        atm_strike=7520,
        source="SPX",
        observed_at=START + timedelta(seconds=145),
        expiry="20260710",
    )

    assert first.confirmation_count == 1
    assert duplicate.confirmation_count == 1
    assert second.proposal is None
    assert third.proposal is not None
    assert third.proposal.reason == "confirmed_move"


def test_deadband_observation_breaks_pending_confirmation_sequence() -> None:
    controller = accepted_controller()

    first = controller.observe(
        atm_strike=7520,
        source="SPX",
        observed_at=START + timedelta(seconds=130),
        expiry="20260710",
    )
    deadband = controller.observe(
        atm_strike=7515,
        source="SPX",
        observed_at=START + timedelta(seconds=138),
        expiry="20260710",
    )
    restarted = controller.observe(
        atm_strike=7520,
        source="SPX",
        observed_at=START + timedelta(seconds=145),
        expiry="20260710",
    )
    still_pending = controller.observe(
        atm_strike=7520,
        source="SPX",
        observed_at=START + timedelta(seconds=150),
        expiry="20260710",
    )

    assert first.confirmation_count == 1
    assert deadband.reason == "inside_trigger_band"
    assert restarted.confirmation_count == 1
    assert still_pending.proposal is None
    assert still_pending.confirmation_count == 2


def test_fallback_source_waits_for_accepted_source_grace() -> None:
    controller = accepted_controller()

    first_fallback = controller.observe(
        atm_strike=7540,
        source="SPY*10",
        observed_at=START + timedelta(seconds=10),
        decision_at=START + timedelta(seconds=10),
        expiry="20260710",
    )
    second_fallback = controller.observe(
        atm_strike=7540,
        source="SPY*10",
        observed_at=START + timedelta(seconds=25),
        decision_at=START + timedelta(seconds=25),
        expiry="20260710",
    )
    after_grace = controller.observe(
        atm_strike=7540,
        source="SPY*10",
        observed_at=START + timedelta(seconds=31),
        decision_at=START + timedelta(seconds=31),
        expiry="20260710",
    )

    assert first_fallback.reason == "source_grace"
    assert second_fallback.reason == "source_grace"
    assert after_grace.reason == "awaiting_confirmations"
    assert after_grace.confirmation_count == 1


def test_emergency_move_can_bypass_cooldown_after_hard_minimum() -> None:
    controller = accepted_controller()

    blocked = controller.observe(
        atm_strike=7540,
        source="SPX",
        observed_at=START + timedelta(seconds=20),
        expiry="20260710",
    )
    first = controller.observe(
        atm_strike=7540,
        source="SPX",
        observed_at=START + timedelta(seconds=31),
        expiry="20260710",
    )
    second = controller.observe(
        atm_strike=7540,
        source="SPX",
        observed_at=START + timedelta(seconds=36),
        expiry="20260710",
    )

    assert blocked.proposal is None
    assert first.proposal is None
    assert second.proposal is not None
    assert second.proposal.reason == "emergency_move"


def test_expiry_rollover_uses_last_stable_atm_once_when_raw_reference_missing() -> None:
    controller = accepted_controller()

    rollover = controller.observe(
        atm_strike=None,
        source=None,
        observed_at=START + timedelta(seconds=10),
        expiry="20260713",
    )

    assert rollover.proposal is not None
    assert rollover.proposal.atm_strike == 7500
    assert rollover.proposal.reason == "expiry_rollover"
    controller.record_result(rollover.proposal, success=True)

    repeated = controller.observe(
        atm_strike=None,
        source=None,
        observed_at=START + timedelta(seconds=20),
        expiry="20260713",
    )
    assert repeated.proposal is None


def test_new_expiry_bypasses_failure_backoff_from_old_expiry() -> None:
    controller = accepted_controller()
    observations = []
    for seconds in (130, 138, 145):
        observations.append(
            controller.observe(
                atm_strike=7520,
                source="SPX",
                observed_at=START + timedelta(seconds=seconds),
                decision_at=START + timedelta(seconds=seconds),
                expiry="20260710",
            )
        )
    failed_proposal = observations[-1].proposal
    assert failed_proposal is not None
    controller.record_result(
        failed_proposal,
        success=False,
        applied_at=START + timedelta(seconds=145),
    )

    rollover = controller.observe(
        atm_strike=None,
        source=None,
        observed_at=START + timedelta(seconds=150),
        decision_at=START + timedelta(seconds=150),
        expiry="20260713",
    )

    assert rollover.proposal is not None
    assert rollover.proposal.reason == "expiry_rollover"


def test_accepted_source_ticks_during_failure_backoff_preserve_source_grace() -> None:
    controller = accepted_controller()
    observations = [
        controller.observe(
            atm_strike=7520,
            source="SPX",
            observed_at=START + timedelta(seconds=seconds),
            decision_at=START + timedelta(seconds=seconds),
            expiry="20260710",
        )
        for seconds in (130, 138, 145)
    ]
    proposal = observations[-1].proposal
    assert proposal is not None
    controller.record_result(
        proposal,
        success=False,
        applied_at=START + timedelta(seconds=145),
    )

    suppressed = controller.observe(
        atm_strike=7500,
        source="SPX",
        observed_at=START + timedelta(seconds=160),
        decision_at=START + timedelta(seconds=160),
        expiry="20260710",
    )
    fallback = controller.observe(
        atm_strike=7540,
        source="SPY*10",
        observed_at=START + timedelta(seconds=176),
        decision_at=START + timedelta(seconds=176),
        expiry="20260710",
    )

    assert suppressed.reason == "failure_backoff"
    assert fallback.reason == "source_grace"
