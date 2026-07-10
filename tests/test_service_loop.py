from __future__ import annotations

import json
import sys
import time

from concurrent.futures import ThreadPoolExecutor

from spx_spark.service_loop import (
    ServiceLoopSettings,
    ServiceTask,
    build_tasks,
    drain_finished_tasks,
    next_delay_seconds,
    run_once,
    run_task,
    submit_due_tasks,
)


def make_settings(**overrides) -> ServiceLoopSettings:
    values = {
        "hyperliquid_enabled": True,
        "polymarket_enabled": False,
        "ibkr_enabled": False,
        "iv_surface_enabled": True,
        "intraday_shock_enabled": True,
        "alert_enabled": True,
        "hyperliquid_interval_seconds": 30,
        "polymarket_interval_seconds": 60,
        "ibkr_interval_seconds": 60,
        "iv_surface_interval_seconds": 300,
        "intraday_shock_interval_seconds": 5,
        "alert_interval_seconds": 30,
        "heartbeat_seconds": 60,
        "ibkr_skip_options": False,
        "ibkr_connect_retry_seconds": 300,
        "ibkr_conflict_probe_seconds": 300,
    }
    values.update(overrides)
    return ServiceLoopSettings(**values)


def test_service_loop_defaults_do_not_enable_ibkr() -> None:
    tasks = build_tasks(make_settings())

    names = [task.name for task in tasks]
    assert names == ["intraday_shock", "hyperliquid", "iv_surface", "alert_engine"]
    assert all(task.command for task in tasks)


def test_service_loop_can_enable_ibkr_explicitly() -> None:
    tasks = build_tasks(make_settings(ibkr_enabled=True, ibkr_skip_options=True))

    assert [task.name for task in tasks] == [
        "intraday_shock",
        "hyperliquid",
        "ibkr",
        "iv_surface",
        "alert_engine",
    ]


def test_service_loop_can_enable_polymarket_explicitly() -> None:
    tasks = build_tasks(make_settings(polymarket_enabled=True))

    assert [task.name for task in tasks] == [
        "intraday_shock",
        "hyperliquid",
        "polymarket",
        "iv_surface",
        "alert_engine",
    ]


def test_run_once_keeps_running_tasks_and_reports_failure() -> None:
    calls: list[str] = []
    tasks = build_tasks(
        make_settings(
            hyperliquid_enabled=False,
            iv_surface_enabled=False,
            intraday_shock_enabled=False,
            alert_enabled=False,
        )
    )
    tasks.append(ServiceTask("noop", 1, lambda: calls.append("ok") or 0))
    tasks.append(ServiceTask("fail", 1, lambda: 1))

    assert run_once(tasks) == 1
    assert calls == ["ok"]


def test_run_task_suppresses_success_stdout() -> None:
    def noisy_task() -> int:
        print("large child payload")
        return 0

    event = run_task(ServiceTask("noisy", 1, noisy_task))

    assert event["ok"] is True
    assert event["stdout_chars"] == len("large child payload\n")
    assert "stdout_tail" not in event


def test_run_task_keeps_failure_stdout_tail() -> None:
    def failing_task() -> int:
        print("diagnostic payload")
        return 1

    event = run_task(ServiceTask("fail", 1, failing_task))

    assert event["ok"] is False
    assert event["stdout_tail"] == "diagnostic payload\n"


def test_run_task_times_out_hanging_task(monkeypatch) -> None:
    monkeypatch.setenv("SPX_SERVICE_TASK_TIMEOUT_SECONDS", "1")

    def hanging_task() -> int:
        time.sleep(5)
        return 0

    event = run_task(ServiceTask("hang", 1, hanging_task))

    assert event["ok"] is False
    assert event["exit_code"] == 1
    assert event["error"] == "service task exceeded 1s timeout"


def test_run_task_times_out_hanging_command(monkeypatch) -> None:
    monkeypatch.setenv("SPX_SERVICE_TASK_TIMEOUT_SECONDS", "1")

    event = run_task(
        ServiceTask(
            "command_hang",
            1,
            lambda: 0,
            command=(sys.executable, "-c", "import time; time.sleep(5)"),
        )
    )

    assert event["ok"] is False
    assert event["exit_code"] == 124
    assert event["error"] == "service task exceeded 1s timeout"


def test_ibkr_task_extracts_provider_state_from_json_stdout() -> None:
    def ibkr_task() -> int:
        print(
            json.dumps(
                {
                    "provider_state": {
                        "status": "unavailable",
                        "reason": "competing session blocks live market data (IBKR 10197)",
                        "connected": True,
                    },
                    "competing_session": True,
                    "error_count": 1,
                    "provider_error_count": 1,
                }
            )
        )
        return 0

    event = run_task(ServiceTask("ibkr", 60, ibkr_task))

    assert event["provider_status"] == "unavailable"
    assert event["provider_connected"] is True
    assert event["competing_session"] is True
    assert event["error_count"] == 1
    assert event["provider_error_count"] == 1


def test_alert_task_extracts_notification_summary_from_json_stdout() -> None:
    def alert_task() -> int:
        print(
            json.dumps(
                {
                    "alert_count": 1,
                    "notification": {
                        "enabled": True,
                        "selected_count": 1,
                        "sent_count": 0,
                        "skipped_reason": None,
                        "sinks": [
                            {
                                "sink": "openclaw_message",
                                "attempted": True,
                                "ok": False,
                                "dry_run": False,
                                "exit_code": 0,
                                "error": "openclaw returned ret=-2",
                            }
                        ],
                    },
                }
            )
        )
        return 0

    event = run_task(ServiceTask("alert_engine", 30, alert_task))

    assert event["alert_count"] == 1
    assert event["notification_enabled"] is True
    assert event["notification_selected_count"] == 1
    assert event["notification_sent_count"] == 0
    assert event["notification_sinks"] == [
        {
            "sink": "openclaw_message",
            "attempted": True,
            "ok": False,
            "dry_run": False,
            "exit_code": 0,
            "error": "openclaw returned ret=-2",
        }
    ]


def test_submit_due_tasks_skips_in_flight_and_not_due_tasks() -> None:
    ready = ServiceTask("ready", 1, lambda: 0)
    busy = ServiceTask("busy", 1, lambda: 0)
    later = ServiceTask("later", 1, lambda: 0, next_run_monotonic=10**12)
    submitted: list[str] = []

    with ThreadPoolExecutor(max_workers=2) as executor:
        in_flight = {"busy": executor.submit(lambda: {"task": "busy", "ok": True})}

        def submit(task: ServiceTask):
            submitted.append(task.name)
            return executor.submit(run_task, task)

        submit_due_tasks([ready, busy, later], in_flight, submit, now=0.0)

        assert submitted == ["ready"]
        assert set(in_flight) == {"ready", "busy"}


def test_drain_finished_tasks_emits_events_and_reschedules() -> None:
    task = ServiceTask("noop", interval_seconds=30, fn=lambda: 0)

    with ThreadPoolExecutor(max_workers=1) as executor:
        in_flight = {"noop": executor.submit(run_task, task)}
        in_flight["noop"].result()

        events = drain_finished_tasks([task], in_flight)

    assert len(events) == 1
    assert events[0]["task"] == "noop"
    assert events[0]["ok"] is True
    assert in_flight == {}
    assert task.next_run_monotonic > 0.0


def test_slow_task_does_not_block_other_tasks() -> None:
    order: list[str] = []

    def slow() -> int:
        time.sleep(0.5)
        order.append("slow")
        return 0

    def fast() -> int:
        order.append("fast")
        return 0

    slow_task = ServiceTask("slow", 1, slow)
    fast_task = ServiceTask("fast", 1, fast)

    with ThreadPoolExecutor(max_workers=2) as executor:
        in_flight: dict = {}
        submit_due_tasks(
            [slow_task, fast_task],
            in_flight,
            lambda task: executor.submit(run_task, task),
            now=0.0,
        )
        in_flight["fast"].result(timeout=5)
        in_flight["slow"].result(timeout=5)

    assert order == ["fast", "slow"]


def test_ibkr_competing_session_uses_probe_delay() -> None:
    task = ServiceTask(
        "ibkr",
        60,
        lambda: 0,
        failure_interval_seconds=300,
        conflict_probe_seconds=300,
    )
    result = {
        "ok": True,
        "provider_status": "unavailable",
        "provider_reason": "competing session blocks live market data (IBKR 10197)",
        "competing_session": True,
    }

    assert next_delay_seconds(task, result) == 300
