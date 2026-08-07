"""Structured-concurrency owner for real-time SPX Core tasks."""

from __future__ import annotations

import asyncio
import signal
import threading
from collections.abc import Callable
from contextlib import nullcontext

import uvicorn

from spx_spark.app_settings import AppSettings, get_settings
from spx_spark.application.runtime import es_bar_sampler, spx_minute_sampler
from spx_spark.logging_setup import configure_logging
from spx_spark.web.replay_api import create_default_app


async def _run_sampler(
    name: str,
    runner: Callable[..., int],
    *,
    stop_event: threading.Event,
    shutdown: asyncio.Event,
    lock_path: str,
) -> None:
    code = await asyncio.to_thread(
        runner, stop_event=stop_event, lock_path=lock_path
    )
    if code != 0:
        raise RuntimeError(f"{name} exited with status {code}")
    if not shutdown.is_set():
        raise RuntimeError(f"{name} exited unexpectedly")


async def _serve_api(settings: AppSettings, shutdown: asyncio.Event) -> None:
    server = uvicorn.Server(uvicorn.Config(
        create_default_app(), uds=str(settings.core_socket_path),
        access_log=False, log_config=None))
    server.capture_signals = lambda: nullcontext()  # type: ignore[method-assign]
    serve_task = asyncio.create_task(server.serve())
    stop_task = asyncio.create_task(shutdown.wait())
    try:
        done, _pending = await asyncio.wait(
            (serve_task, stop_task), return_when=asyncio.FIRST_COMPLETED
        )
        if serve_task in done and not shutdown.is_set():
            await serve_task
            raise RuntimeError("spx-core API exited unexpectedly")
        server.should_exit = True
        await serve_task
    finally:
        server.should_exit = True
        stop_task.cancel()


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
    try:
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(_run_sampler(
                "es_bar_sampler", es_bar_sampler.run_with_stop,
                stop_event=stop_event, shutdown=shutdown,
                lock_path=str(settings.core_lock_root / es_bar_sampler.LOCK_FILE_NAME)))
            tasks.create_task(_run_sampler(
                "spx_minute_sampler", spx_minute_sampler.run_with_stop,
                stop_event=stop_event, shutdown=shutdown,
                lock_path=str(settings.core_lock_root / "spx-spark-spx-minute-sampler.lock")))
            tasks.create_task(_serve_api(settings, shutdown))
    finally:
        request_shutdown()
