"""Persistent, single-owner runner for the latency-sensitive intraday-shock cycle."""

from __future__ import annotations

import argparse
import os
import threading
import time
from collections.abc import Callable
from datetime import datetime, timezone
from functools import partial
from itertools import count
from pathlib import Path
from typing import TYPE_CHECKING

from spx_spark.application.runtime.market_features_hot_worker import (
    DEFAULT_MAX_CONSECUTIVE_FAILURES,
    ProcessLock,
    ProcessLockUnavailable,
    StopEvent,
    default_user_runtime_lock_path,
    install_stop_handlers,
    print_event,
    run_locked_once,
    run_worker_loop,
)
from spx_spark.config import StorageSettings
from spx_spark.settings import AppSettings, load_app_settings
from spx_spark.state_io import atomic_write_json_secure

if TYPE_CHECKING:
    from spx_spark.analytics.options.models import OptionsMap
    from spx_spark.storage import LatestState


LOCK_FILE_NAME = "spx-spark-intraday-shock-hot-worker.lock"
_EMBEDDED_CYCLES = count(1)


def default_lock_path() -> Path:
    return default_user_runtime_lock_path(LOCK_FILE_NAME)


def run_intraday_shock_cycle(
    *,
    emit_json: bool = True,
    latest_state: LatestState | None = None,
    options_map: OptionsMap | None = None,
    app_settings: AppSettings | None = None,
    storage_settings: StorageSettings | None = None,
) -> int:
    # The first cycle pays import cost once. Later cycles reuse the interpreter
    # and module graph instead of spawning and importing every few seconds.
    from spx_spark.application.shock import service

    kwargs: dict[str, object] = {}
    if latest_state is not None:
        kwargs["latest_state"] = latest_state
    if options_map is not None:
        kwargs["options_map"] = options_map
    if app_settings is not None:
        kwargs["app_settings"] = app_settings
    if storage_settings is not None:
        kwargs["storage_settings"] = storage_settings
    return service.run(["--json"] if emit_json else [], **kwargs)


def run_embedded_intraday_shock_cycle(
    latest_state: LatestState,
    options_map: OptionsMap,
    *,
    app_settings: AppSettings,
    storage_settings: StorageSettings,
    interval_seconds: float = 5.0,
    emit_json: bool = False,
) -> None:
    """Run shock first inside one feature tick while retaining its own lease."""

    cycle_number = next(_EMBEDDED_CYCLES)
    started_at = datetime.now(tz=timezone.utc)
    started_monotonic = time.monotonic()
    error: str | None = None
    raised: Exception | None = None
    exit_code = 1
    try:
        exit_code = run_intraday_shock_cycle(
            emit_json=emit_json,
            latest_state=latest_state,
            options_map=options_map,
            app_settings=app_settings,
            storage_settings=storage_settings,
        )
    except Exception as exc:  # noqa: BLE001 - preserve failure in the lease
        raised = exc
        error = f"{type(exc).__name__}:{exc}"
    finished_at = datetime.now(tz=timezone.utc)
    duration_seconds = max(time.monotonic() - started_monotonic, 0.0)
    event = {
        "schema_version": 1,
        "task": "intraday_shock_hot_worker",
        "event": "cycle_finished",
        "cycle": cycle_number,
        "ok": exit_code == 0 and error is None,
        "exit_code": exit_code,
        "error": error,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_ms": duration_seconds * 1000.0,
        "interval_seconds": interval_seconds,
        "overrun_ms": max(duration_seconds - interval_seconds, 0.0) * 1000.0,
        "consecutive_failures": 0 if exit_code == 0 and error is None else 1,
        "execution_mode": "embedded_direct_call",
    }
    atomic_write_json_secure(
        Path(storage_settings.data_root) / "latest" / "intraday_shock_hot_worker.lease.json",
        event,
    )
    print_event(event)
    if raised is not None:
        raise RuntimeError(error) from raised
    if exit_code != 0:
        raise RuntimeError(f"intraday shock exited with status {exit_code}")


def run_locked_intraday_shock_once(
    cycle: Callable[[], int],
    *,
    lock_path: str | os.PathLike[str] | None = None,
) -> int:
    """Run a direct shock CLI invocation under the hot worker's owner lock."""

    resolved_lock_path = Path(lock_path) if lock_path is not None else default_lock_path()
    return run_locked_once(cycle, lock_path=resolved_lock_path, task_name="intraday_shock")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the persistent intraday-shock hot worker.")
    parser.add_argument(
        "--interval-seconds",
        type=float,
        help="Start-to-start cadence; defaults to intraday_shock_interval_seconds.",
    )
    parser.add_argument(
        "--lock-path",
        type=Path,
        help="Exclusive owner lock; defaults to the stable XDG user-runtime path.",
    )
    parser.add_argument(
        "--max-consecutive-failures",
        type=int,
        default=DEFAULT_MAX_CONSECUTIVE_FAILURES,
    )
    parser.add_argument("--once", action="store_true", help="Run one locked cycle and exit.")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    app_settings = load_app_settings()
    interval_seconds = (
        float(args.interval_seconds)
        if args.interval_seconds is not None
        else float(app_settings.runtime.intraday_shock_interval_seconds)
    )
    lock_path = args.lock_path or default_lock_path()
    stop_event = threading.Event()
    install_stop_handlers(stop_event)
    exit_code = run_with_stop(
        stop_event=stop_event,
        lock_path=lock_path,
        interval_seconds=interval_seconds,
        max_consecutive_failures=args.max_consecutive_failures,
        max_cycles=1 if args.once else None,
        app_settings=app_settings,
    )
    print_event(
        {
            "task": "intraday_shock_hot_worker",
            "event": "stopped",
            "ok": exit_code == 0,
            "exit_code": exit_code,
        }
    )
    return exit_code


def run_with_stop(
    *,
    stop_event: StopEvent,
    lock_path: str | os.PathLike[str],
    interval_seconds: float | None = None,
    max_consecutive_failures: int = DEFAULT_MAX_CONSECUTIVE_FAILURES,
    max_cycles: int | None = None,
    emit_json: bool = True,
    app_settings: AppSettings | None = None,
) -> int:
    app = app_settings or load_app_settings()
    cadence = float(
        interval_seconds
        if interval_seconds is not None
        else app.runtime.intraday_shock_interval_seconds
    )
    storage = StorageSettings.from_env()
    lease_path = Path(storage.data_root) / "latest" / "intraday_shock_hot_worker.lease.json"
    cycle = partial(
        run_intraday_shock_cycle,
        emit_json=emit_json,
        app_settings=app,
        storage_settings=storage,
    )
    try:
        with ProcessLock(lock_path):
            print_event(
                {
                    "task": "intraday_shock_hot_worker",
                    "event": "started",
                    "ok": True,
                    "pid": os.getpid(),
                    "interval_seconds": cadence,
                    "lock_path": str(lock_path),
                }
            )
            exit_code = run_worker_loop(
                cycle,
                interval_seconds=cadence,
                stop_event=stop_event,
                max_consecutive_failures=max_consecutive_failures,
                max_cycles=max_cycles,
                task_name="intraday_shock_hot_worker",
                lease_path=lease_path,
            )
    except ProcessLockUnavailable as exc:
        print_event(
            {
                "task": "intraday_shock_hot_worker",
                "event": "lock_unavailable",
                "ok": False,
                "error": str(exc),
                "lock_path": str(lock_path),
            }
        )
        return 75
    return exit_code


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
