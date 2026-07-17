"""Service-loop health aggregation and heartbeat payloads."""

from __future__ import annotations

from datetime import datetime

from spx_spark.application.realtime.health import evaluate_engine_health
from spx_spark.application.runtime.tasks import TaskRuntimeState
from spx_spark.domain.health import EngineHealth, EngineMode, TaskCriticality
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR


# Exit code for unrecoverable critical-task failure (systemd Restart=on-failure).
CRITICAL_FAILURE_EXIT_CODE = 75

# Realtime quote/analytics factors must fail closed when absent from the last tick.
_FAIL_CLOSED_FACTORS = frozenset(
    {
        "tradfi_anchor",
        "front_chain_fresh",
        "analytics_ok",
    }
)


def critical_tasks_healthy(states: list[TaskRuntimeState]) -> bool:
    for state in states:
        if state.criticality is TaskCriticality.CRITICAL and not state.healthy:
            return False
    return True


def critical_tasks_warmed(states: list[TaskRuntimeState]) -> bool:
    """True only when every critical task has succeeded at least once."""

    critical = [state for state in states if state.criticality is TaskCriticality.CRITICAL]
    if not critical:
        return True
    return all(state.last_success_at is not None for state in critical)


def any_critical_task_succeeded(states: list[TaskRuntimeState]) -> bool:
    critical = [state for state in states if state.criticality is TaskCriticality.CRITICAL]
    if not critical:
        return True
    return any(state.last_success_at is not None for state in critical)


def any_critical_unrecoverable(states: list[TaskRuntimeState]) -> bool:
    return any(
        state.criticality is TaskCriticality.CRITICAL
        and not state.healthy
        and state.consecutive_failures >= state.max_consecutive_failures
        for state in states
    )


def aggregate_runtime_health(
    states: list[TaskRuntimeState],
    *,
    checked_at: datetime,
    tradfi_anchor_usable: bool | None = None,
    front_chain_fresh: bool | None = None,
    analytics_succeeded: bool | None = None,
    outbox_writable: bool | None = None,
) -> EngineHealth:
    realtime_health = next(
        (
            state.last_engine_health
            for state in states
            if state.name == "realtime_engine" and state.last_engine_health is not None
        ),
        None,
    )
    factors = realtime_health.get("factors", {}) if realtime_health else {}
    if not isinstance(factors, dict):
        factors = {}

    warmed = critical_tasks_warmed(states)
    any_success = any_critical_task_succeeded(states)

    def resolved(explicit: bool | None, factor: str) -> bool:
        if explicit is not None:
            return explicit
        value = factors.get(factor)
        if isinstance(value, bool):
            return value
        # Quotes / analytics factors fail closed; others default True only when
        # we already have a realtime tick (otherwise also fail closed until warm).
        if factor in _FAIL_CLOSED_FACTORS:
            return False
        if realtime_health is None:
            return False
        return True

    return evaluate_engine_health(
        tradfi_anchor_usable=resolved(tradfi_anchor_usable, "tradfi_anchor"),
        front_chain_fresh=resolved(front_chain_fresh, "front_chain_fresh"),
        analytics_succeeded=resolved(analytics_succeeded, "analytics_ok"),
        outbox_writable=resolved(outbox_writable, "outbox_writable"),
        critical_tasks_healthy=critical_tasks_healthy(states),
        checked_at=checked_at,
        engine_failed=False,
        warmed_up=warmed,
        any_critical_success=any_success,
        cash_session_open=DEFAULT_MARKET_CALENDAR.is_rth_open(checked_at),
    )


def build_heartbeat_event(
    states: list[TaskRuntimeState],
    *,
    health: EngineHealth,
    in_flight_tasks: list[str],
    now_monotonic: float | None = None,
    finished_at: datetime,
) -> dict[str, object]:
    """Heartbeat that reflects real readiness — never unconditionally ok=true."""

    return {
        "task": "heartbeat",
        "ok": health.mode is EngineMode.READY,
        "mode": health.mode.value,
        "health": health.to_dict(),
        "finished_at": finished_at.isoformat(),
        "scheduled_tasks": [state.name for state in states],
        "in_flight_tasks": sorted(in_flight_tasks),
        "tasks": [
            state.to_dict(now_monotonic=now_monotonic) for state in states
        ],
    }
