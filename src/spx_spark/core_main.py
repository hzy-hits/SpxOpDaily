"""Structured-concurrency owner for real-time SPX Core tasks."""
from __future__ import annotations
import asyncio
import signal
import threading
from collections.abc import Callable, Mapping
from contextlib import nullcontext
from datetime import datetime, timezone
from functools import partial
import uvicorn
from spx_spark import alert_engine, provider_failover_controller, surface_dashboard
from spx_spark.app_settings import AppSettings, get_settings
from spx_spark.application.globex_trend import service as globex_trend
from spx_spark.application.realtime.composition import run_realtime_engine_cycle
from spx_spark.application.runtime import (
    es_bar_sampler,
    intraday_shock_hot_worker,
    market_features_hot_worker,
    market_regime_signal,
    spx_minute_sampler,
)
from spx_spark.application.runtime.market_regime_range import MarketRegimeFreshnessPolicy
from spx_spark.config import StorageSettings
from spx_spark.logging_setup import configure_logging
from spx_spark.settings import current_app_settings
from spx_spark.strategy import steven
from spx_spark.web.live_api import create_default_app
async def _run_owner(name: str, runner: Callable[[], int],
                     shutdown: asyncio.Event) -> None:
    code = await asyncio.to_thread(runner)
    if code != 0:
        raise RuntimeError(f"{name} exited with status {code}")
    if not shutdown.is_set():
        raise RuntimeError(f"{name} exited unexpectedly")
async def _run_periodic(name: str, runner: Callable[[], int],
                        interval_seconds: float, shutdown: asyncio.Event) -> None:
    while not shutdown.is_set():
        code = await asyncio.to_thread(runner)
        if code != 0:
            raise RuntimeError(f"{name} exited with status {code}")
        try:
            await asyncio.wait_for(shutdown.wait(), timeout=interval_seconds)
        except TimeoutError:
            pass
async def _serve_api(settings: AppSettings, shutdown: asyncio.Event) -> None:
    server = uvicorn.Server(uvicorn.Config(create_default_app(),
                            uds=str(settings.core_socket_path), access_log=False,
                            log_config=None))
    server.capture_signals = lambda: nullcontext()  # type: ignore[method-assign]
    serve_task = asyncio.create_task(server.serve())
    stop_task = asyncio.create_task(shutdown.wait())
    try:
        done, _pending = await asyncio.wait((serve_task, stop_task),
                                            return_when=asyncio.FIRST_COMPLETED)
        if serve_task in done and not shutdown.is_set():
            await serve_task
            raise RuntimeError("spx-core API exited unexpectedly")
        server.should_exit = True
        await serve_task
    finally:
        server.should_exit = True
        stop_task.cancel()

def _regime_publisher() -> Callable[[Mapping[str, object], Mapping[str, object]], None]:
    storage = StorageSettings.from_env()
    policy = current_app_settings().market_data
    paths = market_regime_signal.SignalPaths.from_data_root(storage.data_root)
    freshness = MarketRegimeFreshnessPolicy(
        live_input_max_age_seconds=float(storage.latest_stale_after_seconds),
        standardized_spx_minute_max_age_seconds=float(policy.standardized_minute_max_age_seconds),
    )

    def publish(market: Mapping[str, object], options: Mapping[str, object]) -> None:
        market_regime_signal.produce_once(
            paths=paths, now=datetime.now(tz=timezone.utc),
            freshness_policy=freshness, market=market, options=options,
        )

    return publish

async def main() -> None:
    settings = get_settings()
    configure_logging("spx-core", settings.log_level)
    settings.core_lock_root.mkdir(parents=True, exist_ok=True)
    settings.core_socket_path.parent.mkdir(parents=True, exist_ok=True)
    stop_event = threading.Event()
    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()

    def request_shutdown() -> None:
        stop_event.set()
        shutdown.set()

    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, request_shutdown)
    storage = StorageSettings.from_env()
    app_settings = current_app_settings()
    runtime = app_settings.runtime
    feature_runner = partial(
        market_features_hot_worker.run_with_stop,
        on_frames=_regime_publisher(),
        on_analytical_snapshot=partial(
            intraday_shock_hot_worker.run_embedded_intraday_shock_cycle,
            app_settings=app_settings, storage_settings=storage, emit_json=False),
        additional_lock_path=str(settings.core_lock_root /
                                 intraday_shock_hot_worker.LOCK_FILE_NAME),
        emit_json=False,
    )
    try:
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(_run_owner("es_bar_sampler", partial(
                es_bar_sampler.run_with_stop, stop_event=stop_event, lock_path=str(
                    settings.core_lock_root / es_bar_sampler.LOCK_FILE_NAME)), shutdown))
            tasks.create_task(_run_owner("spx_minute_sampler", partial(
                spx_minute_sampler.run_with_stop, stop_event=stop_event, lock_path=str(
                    settings.core_lock_root / "spx-spark-spx-minute-sampler.lock")), shutdown))
            tasks.create_task(_run_owner("market_features_hot_worker", partial(
                feature_runner, stop_event=stop_event, lock_path=str(
                    settings.core_lock_root / market_features_hot_worker.LOCK_FILE_NAME)), shutdown))
            tasks.create_task(_run_owner("surface_dashboard", partial(
                surface_dashboard.run_loop, storage_settings=storage,
                interval_seconds=5.0, output_path=f"{storage.data_root}/published/spxw-surface/snapshot.json",
                stop_event=stop_event), shutdown))
            if runtime.provider_failover_enabled:
                tasks.create_task(_run_periodic(
                    "provider_failover", partial(provider_failover_controller.run, ["--json"]),
                    runtime.provider_failover_interval_seconds, shutdown))
            if app_settings.globex_trend.enabled:
                tasks.create_task(_run_periodic(
                    "globex_trend", partial(globex_trend.run, ["--json"]),
                    app_settings.globex_trend.interval_seconds, shutdown))
            if runtime.realtime_engine_enabled:
                tasks.create_task(_run_periodic(
                    "realtime_engine", run_realtime_engine_cycle,
                    runtime.realtime_engine_interval_seconds, shutdown))
            if runtime.alerts_enabled:
                tasks.create_task(_run_periodic(
                    "alert_engine", partial(alert_engine.run, ["--json"]),
                    runtime.alert_interval_seconds, shutdown))
            if app_settings.alerts.steven_enabled:
                tasks.create_task(_run_periodic(
                    "steven", partial(steven.run, ["--json"]),
                    runtime.alert_interval_seconds, shutdown))
            tasks.create_task(_serve_api(settings, shutdown))
    finally:
        request_shutdown()
