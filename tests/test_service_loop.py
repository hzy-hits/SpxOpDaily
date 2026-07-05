from __future__ import annotations

from spx_spark.service_loop import ServiceLoopSettings, ServiceTask, build_tasks, run_once


def make_settings(**overrides) -> ServiceLoopSettings:
    values = {
        "hyperliquid_enabled": True,
        "ibkr_enabled": False,
        "iv_surface_enabled": True,
        "alert_enabled": True,
        "hyperliquid_interval_seconds": 30,
        "ibkr_interval_seconds": 60,
        "iv_surface_interval_seconds": 300,
        "alert_interval_seconds": 30,
        "heartbeat_seconds": 60,
        "ibkr_skip_options": False,
    }
    values.update(overrides)
    return ServiceLoopSettings(**values)


def test_service_loop_defaults_do_not_enable_ibkr() -> None:
    tasks = build_tasks(make_settings())

    names = [task.name for task in tasks]
    assert names == ["hyperliquid", "iv_surface", "alert_engine"]


def test_service_loop_can_enable_ibkr_explicitly() -> None:
    tasks = build_tasks(make_settings(ibkr_enabled=True, ibkr_skip_options=True))

    assert [task.name for task in tasks] == [
        "hyperliquid",
        "ibkr",
        "iv_surface",
        "alert_engine",
    ]


def test_run_once_keeps_running_tasks_and_reports_failure() -> None:
    calls: list[str] = []
    tasks = build_tasks(make_settings(hyperliquid_enabled=False, iv_surface_enabled=False, alert_enabled=False))
    tasks.append(ServiceTask("noop", 1, lambda: calls.append("ok") or 0))
    tasks.append(ServiceTask("fail", 1, lambda: 1))

    assert run_once(tasks) == 1
    assert calls == ["ok"]
