from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

from spx_spark import alert_engine, iv_surface
from spx_spark.config import load_dotenv
from spx_spark.hyperliquid import collector as hyperliquid_collector
from spx_spark.ibkr import collector as ibkr_collector


TaskFn = Callable[[], int]


@dataclass(frozen=True)
class ServiceLoopSettings:
    hyperliquid_enabled: bool
    ibkr_enabled: bool
    iv_surface_enabled: bool
    alert_enabled: bool
    hyperliquid_interval_seconds: int
    ibkr_interval_seconds: int
    iv_surface_interval_seconds: int
    alert_interval_seconds: int
    heartbeat_seconds: int
    ibkr_skip_options: bool

    @classmethod
    def from_env(cls) -> "ServiceLoopSettings":
        load_dotenv()
        return cls(
            hyperliquid_enabled=env_bool("SPX_SERVICE_ENABLE_HYPERLIQUID", True),
            ibkr_enabled=env_bool("SPX_SERVICE_ENABLE_IBKR", False),
            iv_surface_enabled=env_bool("SPX_SERVICE_ENABLE_IV_SURFACE", True),
            alert_enabled=env_bool("SPX_SERVICE_ENABLE_ALERTS", True),
            hyperliquid_interval_seconds=env_int("SPX_SERVICE_HYPERLIQUID_INTERVAL_SECONDS", 30),
            ibkr_interval_seconds=env_int("SPX_SERVICE_IBKR_INTERVAL_SECONDS", 60),
            iv_surface_interval_seconds=env_int("SPX_SERVICE_IV_SURFACE_INTERVAL_SECONDS", 300),
            alert_interval_seconds=env_int("SPX_SERVICE_ALERT_INTERVAL_SECONDS", 30),
            heartbeat_seconds=env_int("SPX_SERVICE_HEARTBEAT_SECONDS", 60),
            ibkr_skip_options=env_bool("SPX_SERVICE_IBKR_SKIP_OPTIONS", False),
        )


@dataclass
class ServiceTask:
    name: str
    interval_seconds: int
    fn: TaskFn
    next_run_monotonic: float = 0.0


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return int(raw)


def run_hyperliquid() -> int:
    return hyperliquid_collector.run(["--json"])


def make_run_ibkr(*, skip_options: bool) -> TaskFn:
    def run_ibkr() -> int:
        args = ["--json"]
        if skip_options:
            args.append("--skip-options")
        return ibkr_collector.run(args)

    return run_ibkr


def run_iv_surface() -> int:
    return iv_surface.run(["--json"])


def run_alert_engine() -> int:
    return alert_engine.run(["--json"])


def build_tasks(settings: ServiceLoopSettings) -> list[ServiceTask]:
    tasks: list[ServiceTask] = []
    if settings.hyperliquid_enabled:
        tasks.append(ServiceTask("hyperliquid", settings.hyperliquid_interval_seconds, run_hyperliquid))
    if settings.ibkr_enabled:
        tasks.append(
            ServiceTask(
                "ibkr",
                settings.ibkr_interval_seconds,
                make_run_ibkr(skip_options=settings.ibkr_skip_options),
            )
        )
    if settings.iv_surface_enabled:
        tasks.append(ServiceTask("iv_surface", settings.iv_surface_interval_seconds, run_iv_surface))
    if settings.alert_enabled:
        tasks.append(ServiceTask("alert_engine", settings.alert_interval_seconds, run_alert_engine))
    return tasks


def run_task(task: ServiceTask) -> dict[str, object]:
    started = time.perf_counter()
    now = datetime.now(tz=timezone.utc).isoformat()
    stdout = io.StringIO()
    stderr = io.StringIO()
    try:
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = task.fn()
        ok = code == 0
        error = None
    except Exception as exc:  # noqa: BLE001
        code = 1
        ok = False
        error = str(exc)
    stdout_text = stdout.getvalue()
    stderr_text = stderr.getvalue()
    event: dict[str, object] = {
        "task": task.name,
        "ok": ok,
        "exit_code": code,
        "duration_ms": (time.perf_counter() - started) * 1000.0,
        "finished_at": now,
        "error": error,
        "stdout_chars": len(stdout_text),
        "stderr_chars": len(stderr_text),
    }
    if not ok:
        tail_chars = env_int("SPX_SERVICE_OUTPUT_TAIL_CHARS", 1200)
        if stdout_text:
            event["stdout_tail"] = stdout_text[-tail_chars:]
        if stderr_text:
            event["stderr_tail"] = stderr_text[-tail_chars:]
    return event


def print_event(event: dict[str, object]) -> None:
    print(json.dumps(event, sort_keys=True), flush=True)


def run_once(tasks: list[ServiceTask]) -> int:
    results = [run_task(task) for task in tasks]
    for result in results:
        print_event(result)
    return 0 if all(result["ok"] for result in results) else 1


def run_loop(tasks: list[ServiceTask], *, heartbeat_seconds: int) -> int:
    last_heartbeat = 0.0
    while True:
        now = time.monotonic()
        for task in tasks:
            if now < task.next_run_monotonic:
                continue
            print_event(run_task(task))
            task.next_run_monotonic = time.monotonic() + max(task.interval_seconds, 1)

        if now - last_heartbeat >= heartbeat_seconds:
            print_event(
                {
                    "task": "heartbeat",
                    "ok": True,
                    "finished_at": datetime.now(tz=timezone.utc).isoformat(),
                    "scheduled_tasks": [task.name for task in tasks],
                }
            )
            last_heartbeat = now
        time.sleep(1.0)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the SPX Spark 24h service loop.")
    parser.add_argument("--once", action="store_true", help="Run each enabled task once and exit.")
    parser.add_argument("--print-config", action="store_true", help="Print resolved service settings.")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = ServiceLoopSettings.from_env()
    tasks = build_tasks(settings)
    if args.print_config:
        print(
            json.dumps(
                {"settings": asdict(settings), "tasks": [task.name for task in tasks]},
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    if not tasks:
        print_event({"task": "startup", "ok": False, "error": "no service tasks enabled"})
        return 1
    if args.once:
        return run_once(tasks)
    return run_loop(tasks, heartbeat_seconds=settings.heartbeat_seconds)


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
