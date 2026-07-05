from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import signal
import subprocess
import sys
import time
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from spx_spark import alert_engine, iv_surface
from spx_spark.config import load_dotenv
from spx_spark.hyperliquid import collector as hyperliquid_collector
from spx_spark.ibkr import collector as ibkr_collector


TaskFn = Callable[[], int]
DEFAULT_TASK_TIMEOUT_SECONDS = 120


@dataclass(frozen=True)
class ServiceLoopSettings:
    hyperliquid_enabled: bool
    polymarket_enabled: bool
    ibkr_enabled: bool
    iv_surface_enabled: bool
    alert_enabled: bool
    hyperliquid_interval_seconds: int
    polymarket_interval_seconds: int
    ibkr_interval_seconds: int
    iv_surface_interval_seconds: int
    alert_interval_seconds: int
    heartbeat_seconds: int
    ibkr_skip_options: bool
    ibkr_connect_retry_seconds: int
    ibkr_conflict_probe_seconds: int

    @classmethod
    def from_env(cls) -> "ServiceLoopSettings":
        load_dotenv()
        return cls(
            hyperliquid_enabled=env_bool("SPX_SERVICE_ENABLE_HYPERLIQUID", True),
            polymarket_enabled=env_bool("SPX_SERVICE_ENABLE_POLYMARKET", False),
            ibkr_enabled=env_bool("SPX_SERVICE_ENABLE_IBKR", False),
            iv_surface_enabled=env_bool("SPX_SERVICE_ENABLE_IV_SURFACE", True),
            alert_enabled=env_bool("SPX_SERVICE_ENABLE_ALERTS", True),
            hyperliquid_interval_seconds=env_int("SPX_SERVICE_HYPERLIQUID_INTERVAL_SECONDS", 30),
            polymarket_interval_seconds=env_int("SPX_SERVICE_POLYMARKET_INTERVAL_SECONDS", 60),
            ibkr_interval_seconds=env_int("SPX_SERVICE_IBKR_INTERVAL_SECONDS", 60),
            iv_surface_interval_seconds=env_int("SPX_SERVICE_IV_SURFACE_INTERVAL_SECONDS", 300),
            alert_interval_seconds=env_int("SPX_SERVICE_ALERT_INTERVAL_SECONDS", 30),
            heartbeat_seconds=env_int("SPX_SERVICE_HEARTBEAT_SECONDS", 60),
            ibkr_skip_options=env_bool("SPX_SERVICE_IBKR_SKIP_OPTIONS", False),
            ibkr_connect_retry_seconds=env_int("IBKR_CONNECT_RETRY_SECONDS", 300),
            ibkr_conflict_probe_seconds=env_int("IBKR_CONFLICT_PROBE_SECONDS", 300),
        )


@dataclass
class ServiceTask:
    name: str
    interval_seconds: int
    fn: TaskFn
    command: tuple[str, ...] | None = None
    failure_interval_seconds: int | None = None
    conflict_probe_seconds: int | None = None
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


@contextmanager
def task_timeout(seconds: int):
    if seconds <= 0:
        yield
        return

    def raise_timeout(signum, frame) -> None:  # noqa: ARG001
        raise TimeoutError(f"service task exceeded {seconds}s timeout")

    previous = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, raise_timeout)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def run_hyperliquid() -> int:
    return hyperliquid_collector.run(["--json"])


def run_polymarket() -> int:
    from spx_spark.polymarket import collector as polymarket_collector

    return polymarket_collector.run(["--json"])


def make_run_ibkr(*, skip_options: bool) -> TaskFn:
    def run_ibkr() -> int:
        args = ["--json", "--no-table"]
        if skip_options:
            args.append("--skip-options")
        return ibkr_collector.run(args)

    return run_ibkr


def run_iv_surface() -> int:
    return iv_surface.run(["--json"])


def run_alert_engine() -> int:
    return alert_engine.run(["--json"])


def console_script(name: str) -> str:
    return str(Path(sys.executable).with_name(name))


def build_tasks(settings: ServiceLoopSettings) -> list[ServiceTask]:
    tasks: list[ServiceTask] = []
    if settings.hyperliquid_enabled:
        tasks.append(
            ServiceTask(
                "hyperliquid",
                settings.hyperliquid_interval_seconds,
                run_hyperliquid,
                command=(console_script("spx-spark-hyperliquid-collector"), "--json"),
            )
        )
    if settings.polymarket_enabled:
        tasks.append(
            ServiceTask(
                "polymarket",
                settings.polymarket_interval_seconds,
                run_polymarket,
                command=(console_script("spx-spark-polymarket-collector"), "--json"),
            )
        )
    if settings.ibkr_enabled:
        ibkr_command = [console_script("spx-spark-ibkr-collector"), "--json", "--no-table"]
        if settings.ibkr_skip_options:
            ibkr_command.append("--skip-options")
        tasks.append(
            ServiceTask(
                "ibkr",
                settings.ibkr_interval_seconds,
                make_run_ibkr(skip_options=settings.ibkr_skip_options),
                command=tuple(ibkr_command),
                failure_interval_seconds=settings.ibkr_connect_retry_seconds,
                conflict_probe_seconds=settings.ibkr_conflict_probe_seconds,
            )
        )
    if settings.iv_surface_enabled:
        tasks.append(
            ServiceTask(
                "iv_surface",
                settings.iv_surface_interval_seconds,
                run_iv_surface,
                command=(console_script("spx-spark-iv-surface"), "--json"),
            )
        )
    if settings.alert_enabled:
        tasks.append(
            ServiceTask(
                "alert_engine",
                settings.alert_interval_seconds,
                run_alert_engine,
                command=(console_script("spx-spark-alert-engine"), "--json"),
            )
        )
    return tasks


def run_task(task: ServiceTask) -> dict[str, object]:
    started = time.perf_counter()
    now = datetime.now(tz=timezone.utc).isoformat()
    stdout = io.StringIO()
    stderr = io.StringIO()
    try:
        timeout_seconds = env_int("SPX_SERVICE_TASK_TIMEOUT_SECONDS", DEFAULT_TASK_TIMEOUT_SECONDS)
        if task.command is not None:
            code, stdout_text, stderr_text, error = run_task_command(task.command, timeout_seconds)
            ok = code == 0
        else:
            with task_timeout(timeout_seconds), contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                code = task.fn()
            ok = code == 0
            error = None
            stdout_text = stdout.getvalue()
            stderr_text = stderr.getvalue()
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
    if task.name == "ibkr":
        add_ibkr_summary_fields(event, stdout_text)
    if task.name == "alert_engine":
        add_alert_summary_fields(event, stdout_text)
    return event


def run_task_command(command: tuple[str, ...], timeout_seconds: int) -> tuple[int, str, str, str | None]:
    try:
        completed = subprocess.run(
            list(command),
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds if timeout_seconds > 0 else None,
        )
    except subprocess.TimeoutExpired as exc:
        return (
            124,
            normalize_timeout_output(exc.stdout),
            normalize_timeout_output(exc.stderr),
            f"service task exceeded {timeout_seconds}s timeout",
        )
    return completed.returncode, completed.stdout, completed.stderr, None


def normalize_timeout_output(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode(errors="replace")
    return value


def add_ibkr_summary_fields(event: dict[str, object], stdout_text: str) -> None:
    try:
        summary = json.loads(stdout_text)
    except json.JSONDecodeError:
        return
    if not isinstance(summary, dict):
        return
    provider_state = summary.get("provider_state")
    if isinstance(provider_state, dict):
        event["provider_status"] = provider_state.get("status")
        event["provider_reason"] = provider_state.get("reason")
        event["provider_connected"] = provider_state.get("connected")
    event["competing_session"] = bool(summary.get("competing_session"))
    if isinstance(summary.get("error_count"), int):
        event["error_count"] = summary["error_count"]
    if isinstance(summary.get("provider_error_count"), int):
        event["provider_error_count"] = summary["provider_error_count"]


def add_alert_summary_fields(event: dict[str, object], stdout_text: str) -> None:
    try:
        summary = json.loads(stdout_text)
    except json.JSONDecodeError:
        return
    if not isinstance(summary, dict):
        return
    if isinstance(summary.get("alert_count"), int):
        event["alert_count"] = summary["alert_count"]
    notification = summary.get("notification")
    if not isinstance(notification, dict):
        return
    event["notification_enabled"] = notification.get("enabled")
    event["notification_selected_count"] = notification.get("selected_count")
    event["notification_sent_count"] = notification.get("sent_count")
    event["notification_skipped_reason"] = notification.get("skipped_reason")
    sinks = notification.get("sinks")
    if isinstance(sinks, list):
        event["notification_sinks"] = [
            {
                "sink": sink.get("sink"),
                "attempted": sink.get("attempted"),
                "ok": sink.get("ok"),
                "dry_run": sink.get("dry_run"),
                "exit_code": sink.get("exit_code"),
                "error": sink.get("error"),
            }
            for sink in sinks
            if isinstance(sink, dict)
        ]


def next_delay_seconds(task: ServiceTask, result: dict[str, object]) -> int:
    delay = max(task.interval_seconds, 1)
    if task.name != "ibkr":
        return delay
    provider_status = str(result.get("provider_status") or "")
    provider_reason = str(result.get("provider_reason") or "").lower()
    competing = bool(result.get("competing_session")) or "competing session" in provider_reason
    unavailable = provider_status == "unavailable" or result.get("ok") is False
    if competing and task.conflict_probe_seconds is not None:
        return max(task.conflict_probe_seconds, 1)
    if unavailable and task.failure_interval_seconds is not None:
        return max(task.failure_interval_seconds, 1)
    return delay


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
            result = run_task(task)
            print_event(result)
            task.next_run_monotonic = time.monotonic() + next_delay_seconds(task, result)

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
