from __future__ import annotations

from spx_spark.schwab.quota_machine import (
    QuotaPolicy,
    QuotaState,
    advance_quota_state,
    lane_allowed,
)
from spx_spark.schwab.request_models import QuotaMode, RequestWindow


def test_quota_state_throttles_on_429_and_recovers_in_stages() -> None:
    policy = QuotaPolicy()
    state = advance_quota_state(
        QuotaState(),
        RequestWindow(attempts=40, throttled=1, failures=1),
        policy=policy,
    )
    assert state.mode is QuotaMode.THROTTLED
    assert not lane_allowed(state.mode, priority=0)

    state = advance_quota_state(
        state,
        RequestWindow(),
        policy=policy,
        retry_after_elapsed=True,
    )
    assert state.mode is QuotaMode.COOLDOWN
    assert lane_allowed(state.mode, priority=1)
    assert not lane_allowed(state.mode, priority=2)

    state = advance_quota_state(
        state,
        RequestWindow(attempts=10),
        policy=policy,
    )
    assert state.mode is QuotaMode.RECOVERING


def test_quota_pressure_uses_seventy_percent_of_nominal_capacity() -> None:
    policy = QuotaPolicy()
    state = advance_quota_state(
        QuotaState(),
        RequestWindow(attempts=84),
        policy=policy,
    )
    assert state.mode is QuotaMode.PRESSURE
