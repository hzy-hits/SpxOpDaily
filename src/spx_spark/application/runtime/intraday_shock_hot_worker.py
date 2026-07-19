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
    default_user_runtime_lock_path,
    install_stop_handlers,
    print_event,
    run_locked_once,
    run_worker_loop,
)
from spx_spark.application.runtime.settings import ServiceLoopSettings
from spx_spark.settings import load_app_settings


LOCK_FILE_NAME = "spx-spark-intraday-shock-hot-worker.lock"


def default_lock_path() -> Path:
    return default_user_runtime_lock_path(LOCK_FILE_NAME)


def run_intraday_shock_cycle() -> int:
    # The first cycle pays import cost once. Later cycles reuse the interpreter
    # and module graph instead of spawning and importing every few seconds.
    from spx_spark.application.shock import service

    return service.run(["--json"])


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
    app = load_app_settings()
    loop_settings = ServiceLoopSettings.from_app_settings(app)
    interval_seconds = (
        float(args.interval_seconds)
        if args.interval_seconds is not None
        else float(loop_settings.intraday_shock_interval_seconds)
    )
    lock_path = args.lock_path or default_lock_path()
    stop_event = threading.Event()
    install_stop_handlers(stop_event)

    try:
        with ProcessLock(lock_path):
            print_event(
                {
                    "task": "intraday_shock_hot_worker",
                    "event": "started",
                    "ok": True,
                    "pid": os.getpid(),
                    "interval_seconds": interval_seconds,
                    "lock_path": str(lock_path),
                }
            )
            exit_code = run_worker_loop(
                run_intraday_shock_cycle,
                interval_seconds=interval_seconds,
                stop_event=stop_event,
                max_consecutive_failures=args.max_consecutive_failures,
                max_cycles=1 if args.once else None,
                task_name="intraday_shock_hot_worker",
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

    print_event(
        {
            "task": "intraday_shock_hot_worker",
            "event": "stopped",
            "ok": exit_code == 0,
            "exit_code": exit_code,
        }
    )
    return exit_code


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
