"""Concurrent service-loop scheduler and one-shot runner."""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone

from spx_spark.application.runtime.health import (
    CRITICAL_FAILURE_EXIT_CODE,
    aggregate_runtime_health,
    any_critical_unrecoverable,
    build_heartbeat_event,
)
from spx_spark.application.runtime.registry import ServiceTask
from spx_spark.application.runtime.runner import run_task
from spx_spark.application.runtime.settings import DEFAULT_MAX_CONCURRENT_TASKS
from spx_spark.domain.health import EngineMode


def print_event(event: dict[str, object]) -> None:
    print(json.dumps(event, sort_keys=True), flush=True)


def next_delay_seconds(task: ServiceTask, result: dict[str, object]) -> int:
    delay = max(task.interval_seconds, 1)
    if task.name != "ibkr":
        return delay
    provider_status = str(result.get("provider_status") or "")
    provider_reason = str(result.get("provider_reason") or "").lower()
    competing = bool(result.get("competing_session")) or "competing session" in provider_reason
    unavailable = provider_status == "unavailable" or result.get("ok") is False
    if competing and task.conflict_probe_seconds is not None:
        return max(task.conflict_probe_seconds, 1)
    if unavailable and task.failure_interval_seconds is not None:
        return max(task.failure_interval_seconds, 1)
    return delay


def run_once(tasks: list[ServiceTask]) -> int:
    results = [run_task(task) for task in tasks]
    for result in results:
        print_event(result)
    return 0 if all(result["ok"] for result in results) else 1


def submit_due_tasks(
    tasks: list[ServiceTask],
    in_flight: dict[str, Future],
    submit: Callable[[ServiceTask], Future],
    *,
    now: float,
) -> None:
    for task in tasks:
        if task.name in in_flight or now < task.next_run_monotonic:
            continue
        if task.runtime is not None:
            task.runtime.mark_running(now_monotonic=now)
        in_flight[task.name] = submit(task)


def drain_finished_tasks(
    tasks: list[ServiceTask],
    in_flight: dict[str, Future],
) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    for task in tasks:
        future = in_flight.get(task.name)
        if future is None or not future.done():
            continue
        del in_flight[task.name]
        event = future_event(task, future)
        finished_at = datetime.now(tz=timezone.utc)
        raw_finished = event.get("finished_at")
        if isinstance(raw_finished, str):
            try:
                finished_at = datetime.fromisoformat(raw_finished)
            except ValueError:
                pass
        if task.runtime is not None:
            engine_health = event.get("engine_health")
            if isinstance(engine_health, dict):
                task.runtime.last_engine_health = engine_health
            if event.get("ok") is True:
                task.runtime.record_success(finished_at=finished_at)
            else:
                task.runtime.record_failure(
                    finished_at=finished_at,
                    error=str(event.get("error") or event.get("exit_code") or "task_failed"),
                )
            event["task_mode"] = task.runtime.mode.value
            event["consecutive_failures"] = task.runtime.consecutive_failures
        events.append(event)
        task.next_run_monotonic = time.monotonic() + next_delay_seconds(task, event)
    return events


def future_event(task: ServiceTask, future: Future) -> dict[str, object]:
    try:
        return future.result()
    except Exception as exc:  # noqa: BLE001 - run_task already catches; this is a backstop
        return {
            "task": task.name,
            "ok": False,
            "exit_code": 1,
            "duration_ms": None,
            "finished_at": datetime.now(tz=timezone.utc).isoformat(),
            "error": f"task future failed: {exc}",
            "stdout_chars": 0,
            "stderr_chars": 0,
        }


def run_loop(
    tasks: list[ServiceTask],
    *,
    heartbeat_seconds: int,
    max_concurrent_tasks: int = DEFAULT_MAX_CONCURRENT_TASKS,
) -> int:
    """Concurrent scheduler: one slow task no longer delays every other task.

    Each task runs on a worker thread (real tasks are child processes with
    their own timeout) and at most one instance of a given task is in flight,
    so a hung IBKR cycle cannot skew the alert/Hyperliquid cadence and tasks
    can never pile up on themselves.

    Heartbeat reflects aggregated EngineMode readiness (never unconditionally
    ok=true). Unrecoverable critical-task failure exits with
    CRITICAL_FAILURE_EXIT_CODE so systemd can restart the unit.
    """
    executor = ThreadPoolExecutor(
        max_workers=max(max_concurrent_tasks, 1),
        thread_name_prefix="spx-task",
    )
    in_flight: dict[str, Future] = {}
    # Defer the first heartbeat until the interval elapses so STARTING tasks
    # are not reported READY before any critical work has completed.
    last_heartbeat = time.monotonic()
    try:
        while True:
            now = time.monotonic()
            submit_due_tasks(tasks, in_flight, lambda task: executor.submit(run_task, task), now=now)
            for event in drain_finished_tasks(tasks, in_flight):
                print_event(event)

            states = [task.runtime for task in tasks if task.runtime is not None]
            if any_critical_unrecoverable(states):
                health = aggregate_runtime_health(
                    states,
                    checked_at=datetime.now(tz=timezone.utc),
                )
                print_event(
                    {
                        "task": "supervisor",
                        "ok": False,
                        "mode": health.mode.value,
                        "error": "critical_task_unrecoverable",
                        "exit_code": CRITICAL_FAILURE_EXIT_CODE,
                        "finished_at": datetime.now(tz=timezone.utc).isoformat(),
                        "health": health.to_dict(),
                    }
                )
                return CRITICAL_FAILURE_EXIT_CODE

            if now - last_heartbeat >= heartbeat_seconds:
                checked_at = datetime.now(tz=timezone.utc)
                health = aggregate_runtime_health(states, checked_at=checked_at)
                print_event(
                    build_heartbeat_event(
                        states,
                        health=health,
                        in_flight_tasks=list(in_flight),
                        now_monotonic=now,
                        finished_at=checked_at,
                    )
                )
                last_heartbeat = now
                if health.mode is EngineMode.FAILED:
                    return CRITICAL_FAILURE_EXIT_CODE
            time.sleep(0.5)
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
