"""Service-loop heartbeat / critical-exit health tests."""

from __future__ import annotations

from datetime import datetime, timezone

from spx_spark.application.runtime.health import (
    CRITICAL_FAILURE_EXIT_CODE,
    any_critical_unrecoverable,
    build_heartbeat_event,
    aggregate_runtime_health,
)
from spx_spark.application.runtime.tasks import TaskRuntimeState
from spx_spark.domain.health import EngineMode, TaskCriticality, TaskMode
from spx_spark.service_loop import ServiceTask, drain_finished_tasks, submit_due_tasks


NOW = datetime(2026, 7, 11, 15, 0, tzinfo=timezone.utc)


def test_heartbeat_ok_false_when_not_ready() -> None:
    states = [
        TaskRuntimeState(
            name="schwab_chains",
            criticality=TaskCriticality.CRITICAL,
            mode=TaskMode.UNHEALTHY,
            consecutive_failures=5,
            max_consecutive_failures=5,
            last_success_at=NOW,
        )
    ]
    health = aggregate_runtime_health(states, checked_at=NOW)
    event = build_heartbeat_event(
        states,
        health=health,
        in_flight_tasks=[],
        finished_at=NOW,
    )
    assert health.mode is EngineMode.BLOCKED
    assert event["ok"] is False
    assert event["mode"] == "blocked"
    assert event["tasks"][0]["name"] == "schwab_chains"


def test_just_started_critical_tasks_must_not_be_ready() -> None:
    states = [
        TaskRuntimeState(
            name="ibkr",
            criticality=TaskCriticality.CRITICAL,
            mode=TaskMode.IDLE,
        ),
        TaskRuntimeState(
            name="schwab_chains",
            criticality=TaskCriticality.CRITICAL,
            mode=TaskMode.IDLE,
        ),
    ]
    health = aggregate_runtime_health(
        states,
        checked_at=NOW,
        tradfi_anchor_usable=True,
        front_chain_fresh=True,
        analytics_succeeded=True,
        outbox_writable=True,
    )
    event = build_heartbeat_event(
        states,
        health=health,
        in_flight_tasks=[],
        finished_at=NOW,
    )
    assert health.mode is EngineMode.STARTING
    assert event["ok"] is False
    assert health.factors["tradfi_anchor"] is True


def test_missing_realtime_factors_fail_closed() -> None:
    states = [
        TaskRuntimeState(
            name="hyperliquid",
            criticality=TaskCriticality.OPTIONAL,
            mode=TaskMode.IDLE,
        )
    ]
    health = aggregate_runtime_health(states, checked_at=NOW)
    assert health.factors["tradfi_anchor"] is False
    assert health.factors["front_chain_fresh"] is False
    assert health.factors["analytics_ok"] is False
    assert health.mode is not EngineMode.READY


def test_heartbeat_ok_true_only_when_ready() -> None:
    states = [
        TaskRuntimeState(
            name="ibkr",
            criticality=TaskCriticality.CRITICAL,
            mode=TaskMode.IDLE,
            last_success_at=NOW,
        ),
        TaskRuntimeState(
            name="schwab_chains",
            criticality=TaskCriticality.CRITICAL,
            mode=TaskMode.IDLE,
            last_success_at=NOW,
        ),
        TaskRuntimeState(
            name="realtime_engine",
            criticality=TaskCriticality.IMPORTANT,
            mode=TaskMode.IDLE,
            last_success_at=NOW,
            last_engine_health={
                "mode": "ready",
                "factors": {
                    "tradfi_anchor": True,
                    "front_chain_fresh": True,
                    "analytics_ok": True,
                    "outbox_writable": True,
                    "critical_tasks_ok": True,
                },
                "reasons": [],
            },
        ),
    ]
    health = aggregate_runtime_health(states, checked_at=NOW)
    event = build_heartbeat_event(
        states,
        health=health,
        in_flight_tasks=["hyperliquid"],
        finished_at=NOW,
        now_monotonic=10.0,
    )
    assert health.mode is EngineMode.READY
    assert event["ok"] is True
    assert event["in_flight_tasks"] == ["hyperliquid"]

def test_heartbeat_uses_latest_realtime_engine_factors() -> None:
    state = TaskRuntimeState(
        name="realtime_engine",
        criticality=TaskCriticality.IMPORTANT,
        mode=TaskMode.IDLE,
        last_engine_health={
            "mode": "blocked",
            "factors": {
                "tradfi_anchor": False,
                "front_chain_fresh": False,
                "analytics_ok": True,
                "outbox_writable": True,
                "critical_tasks_ok": True,
            },
            "reasons": ["tradfi_anchor_failed", "front_chain_fresh_failed"],
        },
    )

    health = aggregate_runtime_health([state], checked_at=NOW)
    event = build_heartbeat_event(
        [state],
        health=health,
        in_flight_tasks=[],
        finished_at=NOW,
    )

    assert health.mode is EngineMode.BLOCKED
    assert event["ok"] is False
    assert event["tasks"][0]["last_engine_health"]["mode"] == "blocked"


def test_critical_unrecoverable_detects_exit_condition() -> None:
    healthy = TaskRuntimeState(name="alert_engine", criticality=TaskCriticality.IMPORTANT)
    broken = TaskRuntimeState(
        name="ibkr",
        criticality=TaskCriticality.CRITICAL,
        mode=TaskMode.UNHEALTHY,
        consecutive_failures=5,
        max_consecutive_failures=5,
    )
    assert any_critical_unrecoverable([healthy, broken]) is True
    assert CRITICAL_FAILURE_EXIT_CODE == 75


def test_drain_finished_tasks_updates_runtime_state() -> None:
    task = ServiceTask("ibkr", 60, lambda: 0)
    assert task.runtime is not None
    assert task.criticality is TaskCriticality.CRITICAL

    class DoneFuture:
        def done(self) -> bool:
            return True

        def result(self) -> dict[str, object]:
            return {
                "task": "ibkr",
                "ok": False,
                "exit_code": 1,
                "finished_at": NOW.isoformat(),
                "error": "boom",
            }

    in_flight = {"ibkr": DoneFuture()}  # type: ignore[dict-item]
    events = drain_finished_tasks([task], in_flight)  # type: ignore[arg-type]
    assert events[0]["ok"] is False
    assert task.runtime.consecutive_failures == 1
    assert task.runtime.mode is TaskMode.BACKOFF


def test_submit_due_marks_running() -> None:
    task = ServiceTask("hyperliquid", 30, lambda: 0)
    submitted: list[ServiceTask] = []

    def submit(item: ServiceTask):
        submitted.append(item)

        class Pending:
            def done(self) -> bool:
                return False

        return Pending()

    in_flight: dict = {}
    submit_due_tasks([task], in_flight, submit, now=100.0)
    assert task.runtime is not None
    assert task.runtime.mode is TaskMode.RUNNING
    assert task.name in in_flight
