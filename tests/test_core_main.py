from __future__ import annotations

import asyncio

import pytest

from spx_spark.core_main import _run_owner, _run_periodic


def test_owned_core_loop_exit_fails_for_systemd_restart() -> None:
    with pytest.raises(RuntimeError, match="test_owner exited unexpectedly"):
        asyncio.run(_run_owner("test_owner", lambda: 0, asyncio.Event()))


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


def test_optional_task_failure_keeps_sampler_running(caplog) -> None:
    calls = 0
    samples = 0

    async def scenario():
        shutdown = asyncio.Event()
        def optional():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise TimeoutError("calendar unavailable")
            shutdown.set()
            return 0
        async def sampler():
            nonlocal samples
            while not shutdown.is_set():
                samples += 1
                await asyncio.sleep(0)
        async with asyncio.TaskGroup() as group:
            group.create_task(_run_periodic("research", optional, 0.001, shutdown, critical=False))
            group.create_task(sampler())
    asyncio.run(scenario())
    assert calls == 2 and samples > 0
    assert "Optional Core task failed" in caplog.text
