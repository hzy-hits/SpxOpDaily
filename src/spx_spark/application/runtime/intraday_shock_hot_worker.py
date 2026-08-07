"""Persistent, single-owner runner for the latency-sensitive intraday-shock cycle."""

from __future__ import annotations

import argparse
import os
import threading
from collections.abc import Callable
from pathlib import Path

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
from spx_spark.application.runtime.settings import ServiceLoopSettings
from spx_spark.config import StorageSettings
from spx_spark.settings import load_app_settings


LOCK_FILE_NAME = "spx-spark-intraday-shock-hot-worker.lock"


def default_lock_path() -> Path:
    return default_user_runtime_lock_path(LOCK_FILE_NAME)


def run_intraday_shock_cycle(*, emit_json: bool = True) -> int:
    # The first cycle pays import cost once. Later cycles reuse the interpreter
    # and module graph instead of spawning and importing every few seconds.
    from spx_spark.application.shock import service

    return service.run(["--json"] if emit_json else [])


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
    loop_settings = ServiceLoopSettings.from_app_settings(load_app_settings())
    interval_seconds = (
        float(args.interval_seconds)
        if args.interval_seconds is not None
        else float(loop_settings.intraday_shock_interval_seconds)
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
) -> int:
    loop_settings = ServiceLoopSettings.from_app_settings(load_app_settings())
    cadence = float(
        interval_seconds
        if interval_seconds is not None
        else loop_settings.intraday_shock_interval_seconds
    )
    storage = StorageSettings.from_env()
    lease_path = Path(storage.data_root) / "latest" / "intraday_shock_hot_worker.lease.json"
    cycle = (
        run_intraday_shock_cycle
        if emit_json
        else lambda: run_intraday_shock_cycle(emit_json=False)
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
