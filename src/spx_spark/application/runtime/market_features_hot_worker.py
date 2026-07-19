"""Persistent, single-owner runner for the latency-sensitive market-feature cycle."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import signal
import tempfile
import threading
import time
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from types import FrameType
from typing import Protocol

from spx_spark.settings import load_app_settings


DEFAULT_MAX_CONSECUTIVE_FAILURES = 5
LOCK_FILE_NAME = "spx-spark-market-features-hot-worker.lock"


class StopEvent(Protocol):
    def is_set(self) -> bool: ...

    def set(self) -> None: ...

    def wait(self, timeout: float | None = None) -> bool: ...


class ProcessLockUnavailable(RuntimeError):
    """Raised when another hot worker already owns the process lock."""


class ProcessLock:
    """Non-blocking process-lifetime flock with a diagnostic PID payload."""

    def __init__(self, path: str | os.PathLike[str]) -> None:
        self.path = Path(path).expanduser()
        self._descriptor: int | None = None

    def __enter__(self) -> "ProcessLock":
        self.acquire()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.release()

    def acquire(self) -> None:
        if self._descriptor is not None:
            raise RuntimeError(f"Lock is already held by this object: {self.path}")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_CREAT | os.O_RDWR | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(self.path, flags, 0o600)
        try:
            os.fchmod(descriptor, 0o600)
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            os.ftruncate(descriptor, 0)
            os.write(descriptor, f"{os.getpid()}\n".encode())
        except BlockingIOError as exc:
            os.close(descriptor)
            raise ProcessLockUnavailable(f"Lock is held: {self.path}") from exc
        except Exception:
            os.close(descriptor)
            raise
        self._descriptor = descriptor

    def release(self) -> None:
        if self._descriptor is None:
            return
        try:
            fcntl.flock(self._descriptor, fcntl.LOCK_UN)
        finally:
            os.close(self._descriptor)
            self._descriptor = None


def default_user_runtime_lock_path(lock_file_name: str) -> Path:
    runtime_dir = Path(os.environ.get("XDG_RUNTIME_DIR") or f"/run/user/{os.getuid()}")
    if runtime_dir.is_dir():
        # Keep the inode directly under the user runtime root. A systemd unit
        # must not own/remove the parent directory while another process holds it.
        return runtime_dir / lock_file_name
    suffix = lock_file_name.removeprefix("spx-spark-")
    return Path(tempfile.gettempdir()) / f"spx-spark-{os.getuid()}-{suffix}"


def default_lock_path() -> Path:
    return default_user_runtime_lock_path(LOCK_FILE_NAME)


def print_event(event: dict[str, object]) -> None:
    print(json.dumps(event, sort_keys=True), flush=True)


def run_market_features_cycle() -> int:
    # Import once on the first cycle; subsequent calls reuse the same interpreter
    # and module graph instead of paying subprocess and import cost every five seconds.
    from spx_spark.application.market_features import service

    return service.run(["--json"])


def run_locked_market_features_once(
    cycle: Callable[[], int],
    *,
    lock_path: str | os.PathLike[str] | None = None,
) -> int:
    """Run a direct one-shot service invocation under the shared owner lock."""

    resolved_lock_path = Path(lock_path) if lock_path is not None else default_lock_path()
    return run_locked_once(cycle, lock_path=resolved_lock_path, task_name="market_features")


def run_locked_once(
    cycle: Callable[[], int],
    *,
    lock_path: str | os.PathLike[str],
    task_name: str,
) -> int:
    """Run one direct invocation under the same lock as its persistent owner."""

    resolved_lock_path = Path(lock_path)
    try:
        with ProcessLock(resolved_lock_path):
            return int(cycle())
    except ProcessLockUnavailable as exc:
        print_event(
            {
                "task": task_name,
                "event": "owner_lock_unavailable",
                "ok": False,
                "error": str(exc),
                "lock_path": str(resolved_lock_path),
            }
        )
        return 75


def run_worker_loop(
    cycle: Callable[[], int],
    *,
    interval_seconds: float,
    stop_event: StopEvent,
    max_consecutive_failures: int = DEFAULT_MAX_CONSECUTIVE_FAILURES,
    max_cycles: int | None = None,
    monotonic: Callable[[], float] = time.monotonic,
    utcnow: Callable[[], datetime] = lambda: datetime.now(tz=timezone.utc),
    emit: Callable[[dict[str, object]], None] = print_event,
    task_name: str = "market_features_hot_worker",
) -> int:
    """Run non-overlapping cycles on a start-anchored cadence.

    A slow cycle never overlaps itself. When it exceeds the configured interval,
    the next observation begins immediately instead of queueing catch-up work.
    Repeated failures exit so systemd can rebuild all process-local state.
    """

    if interval_seconds <= 0:
        raise ValueError("hot-worker interval must be positive")
    if max_consecutive_failures <= 0:
        raise ValueError("max consecutive failures must be positive")
    if max_cycles is not None and max_cycles <= 0:
        raise ValueError("max cycles must be positive when provided")

    cycle_number = 0
    consecutive_failures = 0
    while not stop_event.is_set():
        cycle_number += 1
        started_at = utcnow()
        started_monotonic = monotonic()
        error: str | None = None
        exit_code = 1
        try:
            exit_code = int(cycle())
        except Exception as exc:  # noqa: BLE001 - isolate a failed observation cycle
            error = f"{type(exc).__name__}:{exc}"
        finished_monotonic = monotonic()
        finished_at = utcnow()
        duration_seconds = max(finished_monotonic - started_monotonic, 0.0)
        ok = exit_code == 0 and error is None
        consecutive_failures = 0 if ok else consecutive_failures + 1
        emit(
            {
                "task": task_name,
                "event": "cycle_finished",
                "cycle": cycle_number,
                "ok": ok,
                "exit_code": exit_code,
                "error": error,
                "started_at": started_at.isoformat(),
                "finished_at": finished_at.isoformat(),
                "duration_ms": duration_seconds * 1000.0,
                "interval_seconds": interval_seconds,
                "overrun_ms": max(duration_seconds - interval_seconds, 0.0) * 1000.0,
                "consecutive_failures": consecutive_failures,
            }
        )
        if consecutive_failures >= max_consecutive_failures:
            return 1
        if max_cycles is not None and cycle_number >= max_cycles:
            break

        remaining = max(interval_seconds - duration_seconds, 0.0)
        if stop_event.wait(remaining):
            break
    return 0


def install_stop_handlers(stop_event: StopEvent) -> None:
    def request_stop(signum: int, frame: FrameType | None) -> None:  # noqa: ARG001
        stop_event.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the persistent market-feature hot worker.")
    parser.add_argument(
        "--interval-seconds",
        type=float,
        help="Start-to-start cadence; defaults to market_features.interval_seconds.",
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
    interval_seconds = (
        float(args.interval_seconds)
        if args.interval_seconds is not None
        else float(app.market_features.interval_seconds)
    )
    lock_path = args.lock_path or default_lock_path()
    stop_event = threading.Event()
    install_stop_handlers(stop_event)

    try:
        with ProcessLock(lock_path):
            print_event(
                {
                    "task": "market_features_hot_worker",
                    "event": "started",
                    "ok": True,
                    "pid": os.getpid(),
                    "interval_seconds": interval_seconds,
                    "lock_path": str(lock_path),
                }
            )
            exit_code = run_worker_loop(
                run_market_features_cycle,
                interval_seconds=interval_seconds,
                stop_event=stop_event,
                max_consecutive_failures=args.max_consecutive_failures,
                max_cycles=1 if args.once else None,
            )
    except ProcessLockUnavailable as exc:
        print_event(
            {
                "task": "market_features_hot_worker",
                "event": "lock_unavailable",
                "ok": False,
                "error": str(exc),
                "lock_path": str(lock_path),
            }
        )
        return 75

    print_event(
        {
            "task": "market_features_hot_worker",
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
