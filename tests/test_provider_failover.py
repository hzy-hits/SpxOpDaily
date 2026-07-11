from datetime import datetime, timedelta, timezone

from spx_spark.provider_failover import (
    FailoverMode,
    FailoverObservation,
    FailoverState,
    FailoverThresholds,
    advance_failover,
)


UTC = timezone.utc


def thresholds() -> FailoverThresholds:
    return FailoverThresholds(
        schwab_unhealthy_observations=2,
        schwab_recovery_observations=2,
        ibkr_unhealthy_observations=2,
    )


def observe(
    state: FailoverState,
    at: datetime,
    *,
    schwab: bool,
    ibkr: bool,
) -> FailoverState:
    return advance_failover(
        state,
        FailoverObservation(
            observed_at=at,
            schwab_healthy=schwab,
            ibkr_healthy=ibkr,
        ),
        thresholds(),
    )


def test_schwab_failure_requests_then_activates_ibkr_once() -> None:
    now = datetime(2026, 7, 13, 13, 30, tzinfo=UTC)
    state = FailoverState.initial(now=now)

    state = observe(state, now, schwab=False, ibkr=False)
    assert state.mode == FailoverMode.SCHWAB_PRIMARY
    state = observe(state, now + timedelta(seconds=15), schwab=False, ibkr=False)
    assert state.mode == FailoverMode.RECOVERY_PENDING
    assert state.ibkr_market_data_required is True
    request_transition = state.transition

    state = observe(state, now + timedelta(seconds=30), schwab=False, ibkr=True)
    assert state.mode == FailoverMode.RECOVERY_PENDING
    assert state.ibkr_recovery_streak == 1
    state = observe(state, now + timedelta(seconds=45), schwab=False, ibkr=True)
    assert state.mode == FailoverMode.IBKR_FALLBACK
    assert state.sequence == 2
    fallback_transition = state.transition
    state = observe(state, now + timedelta(seconds=60), schwab=False, ibkr=True)
    assert state.transition == fallback_transition
    assert state.sequence == 2
    assert request_transition is not None


def test_failover_requires_consecutive_ibkr_recovery_observations() -> None:
    now = datetime(2026, 7, 13, 13, 30, tzinfo=UTC)
    state = FailoverState(
        mode=FailoverMode.BOTH_UNAVAILABLE,
        updated_at=now,
        sequence=3,
    )

    state = observe(state, now + timedelta(seconds=15), schwab=False, ibkr=True)
    assert state.mode == FailoverMode.BOTH_UNAVAILABLE
    assert state.ibkr_recovery_streak == 1

    state = observe(state, now + timedelta(seconds=30), schwab=False, ibkr=True)
    assert state.mode == FailoverMode.IBKR_FALLBACK
    assert state.ibkr_recovery_streak == 2
    assert state.transition is not None
    assert state.transition.previous_mode == FailoverMode.BOTH_UNAVAILABLE


def test_both_unavailable_and_hysteretic_schwab_recovery() -> None:
    now = datetime(2026, 7, 13, 13, 30, tzinfo=UTC)
    state = FailoverState.initial(now=now)
    state = observe(state, now, schwab=False, ibkr=False)
    state = observe(state, now + timedelta(seconds=15), schwab=False, ibkr=False)
    state = observe(state, now + timedelta(seconds=30), schwab=False, ibkr=False)
    assert state.mode == FailoverMode.RECOVERY_PENDING
    state = observe(state, now + timedelta(seconds=45), schwab=False, ibkr=False)

    assert state.mode == FailoverMode.BOTH_UNAVAILABLE

    state = observe(state, now + timedelta(seconds=60), schwab=True, ibkr=False)
    assert state.mode == FailoverMode.BOTH_UNAVAILABLE
    state = observe(state, now + timedelta(seconds=75), schwab=True, ibkr=False)
    assert state.mode == FailoverMode.SCHWAB_PRIMARY
    assert state.ibkr_market_data_required is False


def test_state_roundtrip_preserves_transition_and_control_flag() -> None:
    now = datetime(2026, 7, 13, 13, 30, tzinfo=UTC)
    state = FailoverState.initial(now=now)
    state = observe(state, now, schwab=False, ibkr=True)
    state = observe(state, now + timedelta(seconds=15), schwab=False, ibkr=True)

    restored = FailoverState.from_dict(state.to_dict())

    assert restored == state
    assert restored.mode == FailoverMode.IBKR_FALLBACK
    assert restored.ibkr_market_data_required is True


def test_cold_standby_history_does_not_consume_pending_startup_budget() -> None:
    now = datetime(2026, 7, 13, 13, 30, tzinfo=UTC)
    state = FailoverState.initial(now=now)
    for offset in range(20):
        state = observe(
            state,
            now + timedelta(seconds=offset * 15),
            schwab=True,
            ibkr=False,
        )
    assert state.ibkr_unhealthy_streak == 0

    first_failure = now + timedelta(minutes=10)
    state = observe(state, first_failure, schwab=False, ibkr=False)
    state = observe(state, first_failure + timedelta(seconds=15), schwab=False, ibkr=False)

    assert state.mode == FailoverMode.RECOVERY_PENDING
    assert state.ibkr_unhealthy_streak == 0
    state = observe(state, first_failure + timedelta(seconds=30), schwab=False, ibkr=False)
    assert state.mode == FailoverMode.RECOVERY_PENDING
    state = observe(state, first_failure + timedelta(seconds=45), schwab=False, ibkr=False)
    assert state.mode == FailoverMode.BOTH_UNAVAILABLE


def test_transition_id_is_unique_after_daily_state_reset() -> None:
    first_day = datetime(2026, 7, 13, 13, 30, tzinfo=UTC)
    second_day = first_day + timedelta(days=1)

    first = observe(
        observe(FailoverState.initial(now=first_day), first_day, schwab=False, ibkr=True),
        first_day + timedelta(seconds=15),
        schwab=False,
        ibkr=True,
    )
    second = observe(
        observe(FailoverState.initial(now=second_day), second_day, schwab=False, ibkr=True),
        second_day + timedelta(seconds=15),
        schwab=False,
        ibkr=True,
    )

    assert first.transition is not None
    assert second.transition is not None
    assert first.transition.sequence == second.transition.sequence == 1
    assert first.transition.transition_id != second.transition.transition_id
