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


def test_slow_strategy_cycle_does_not_stop_shock_sampling(monkeypatch, tmp_path) -> None:
    import threading
    from types import SimpleNamespace
    from spx_spark import core_main

    entered = threading.Event()
    observed = threading.Event()
    monkeypatch.setattr(core_main, 'get_settings', lambda: SimpleNamespace(log_level='INFO', core_lock_root=tmp_path))
    app = SimpleNamespace(runtime=SimpleNamespace(provider_failover_enabled=False, realtime_engine_enabled=False,
                         alerts_enabled=False), globex_trend=SimpleNamespace(enabled=False),
                         alerts=SimpleNamespace(steven_enabled=False))
    monkeypatch.setattr(core_main, 'current_app_settings', lambda: app)
    monkeypatch.setattr(core_main, 'configure_logging', lambda *a: None)
    monkeypatch.setattr(core_main, '_regime_publisher', lambda: None)

    def sampler(*, stop_event, **kwargs):
        stop_event.wait(2)
        return 0

    def slow_strategy(*, stop_event, **kwargs):
        entered.set()
        assert observed.wait(1), 'shock must run before this blocked strategy completes'
        stop_event.wait(2)
        return 0

    async def scenario():
        loop = asyncio.get_running_loop()
        callbacks = []
        monkeypatch.setattr(loop, 'add_signal_handler', lambda sig, cb: callbacks.append(cb))
        def shock(*, stop_event, **kwargs):
            assert entered.wait(1)
            observed.set()
            loop.call_soon_threadsafe(callbacks[0])
            stop_event.wait(2)
            return 0
        monkeypatch.setattr(core_main.es_bar_sampler, 'run_with_stop', sampler)
        monkeypatch.setattr(core_main.spx_minute_sampler, 'run_with_stop', sampler)
        monkeypatch.setattr(core_main.market_features_hot_worker, 'run_with_stop', slow_strategy)
        monkeypatch.setattr(core_main.intraday_shock_hot_worker, 'run_with_stop', shock)
        await asyncio.wait_for(core_main.main(), 3)
    asyncio.run(scenario())
    assert observed.is_set()
