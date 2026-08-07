from __future__ import annotations

import asyncio

import pytest

from spx_spark.core_main import _run_owner, _run_periodic


def test_owned_core_loop_exit_fails_for_systemd_restart() -> None:
    with pytest.raises(RuntimeError, match="surface_dashboard exited unexpectedly"):
        asyncio.run(_run_owner("surface_dashboard", lambda: 0, asyncio.Event()))


def test_periodic_core_task_stops_without_an_extra_cycle() -> None:
    shutdown = asyncio.Event()
    calls = 0

    def run() -> int:
        nonlocal calls
        calls += 1
        shutdown.set()
        return 0

    asyncio.run(_run_periodic("test", run, 1.0, shutdown))

    assert calls == 1


def test_periodic_core_task_failure_exits_for_systemd_restart() -> None:
    with pytest.raises(RuntimeError, match="provider_failover exited with status 7"):
        asyncio.run(
            _run_periodic("provider_failover", lambda: 7, 1.0, asyncio.Event())
        )
