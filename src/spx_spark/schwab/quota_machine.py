"""Pure Schwab quota-state transitions."""

from __future__ import annotations

from dataclasses import dataclass

from spx_spark.schwab.request_models import QuotaMode, RequestWindow


@dataclass(frozen=True)
class QuotaPolicy:
    nominal_requests_per_minute: int = 120
    planned_requests_per_minute: int = 84
    pressure_fraction: float = 0.70
    recovery_fraction: float = 0.50
    recovery_successes: int = 10

    def __post_init__(self) -> None:
        if self.nominal_requests_per_minute <= 0:
            raise ValueError("nominal request capacity must be positive")
        if not 0 < self.planned_requests_per_minute < self.nominal_requests_per_minute:
            raise ValueError("planned request capacity must preserve provider reserve")
        if not 0 < self.recovery_fraction < self.pressure_fraction < 1:
            raise ValueError("quota fractions must satisfy recovery < pressure < 1")


@dataclass(frozen=True)
class QuotaState:
    mode: QuotaMode = QuotaMode.NORMAL
    consecutive_successes: int = 0
    stable_windows: int = 0


def advance_quota_state(
    state: QuotaState,
    window: RequestWindow,
    *,
    policy: QuotaPolicy,
    retry_after_elapsed: bool = False,
) -> QuotaState:
    """Advance capacity mode from one rolling-window observation."""

    if window.throttled:
        return QuotaState(mode=QuotaMode.THROTTLED)

    if state.mode is QuotaMode.THROTTLED:
        if not retry_after_elapsed:
            return state
        return QuotaState(mode=QuotaMode.COOLDOWN)

    if state.mode is QuotaMode.COOLDOWN:
        successes = state.consecutive_successes + max(window.attempts - window.failures, 0)
        if window.failures:
            successes = 0
        if successes >= policy.recovery_successes:
            return QuotaState(mode=QuotaMode.RECOVERING)
        return QuotaState(mode=QuotaMode.COOLDOWN, consecutive_successes=successes)

    if state.mode is QuotaMode.RECOVERING:
        if window.failures:
            return QuotaState(mode=QuotaMode.PRESSURE)
        stable = state.stable_windows + 1
        if stable >= 5:
            return QuotaState(mode=QuotaMode.NORMAL)
        return QuotaState(mode=QuotaMode.RECOVERING, stable_windows=stable)

    pressure_at = policy.nominal_requests_per_minute * policy.pressure_fraction
    recover_at = policy.nominal_requests_per_minute * policy.recovery_fraction
    if window.attempts >= pressure_at:
        return QuotaState(mode=QuotaMode.PRESSURE)
    if state.mode is QuotaMode.PRESSURE and window.attempts < recover_at and not window.failures:
        return QuotaState(mode=QuotaMode.NORMAL)
    return state


def lane_allowed(mode: QuotaMode, *, priority: int) -> bool:
    if mode is QuotaMode.THROTTLED:
        return False
    if mode is QuotaMode.COOLDOWN:
        return priority <= 1
    if mode is QuotaMode.PRESSURE:
        return priority <= 3
    return True
