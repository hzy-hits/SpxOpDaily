from __future__ import annotations

import argparse
import contextlib
import io
import json
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from spx_spark import alert_engine, intraday_shock, iv_surface
from spx_spark.config import env_bool, env_int, load_dotenv
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
    intraday_shock_enabled: bool
    alert_enabled: bool
    hyperliquid_interval_seconds: int
    polymarket_interval_seconds: int
    ibkr_interval_seconds: int
    iv_surface_interval_seconds: int
    intraday_shock_interval_seconds: int
    alert_interval_seconds: int
    heartbeat_seconds: int
    ibkr_skip_options: bool
    ibkr_connect_retry_seconds: int
    ibkr_conflict_probe_seconds: int
    ibkr_positions_enabled: bool = False
    ibkr_positions_interval_seconds: int = 60
    schwab_chains_enabled: bool = False
    schwab_chains_interval_seconds: int = 300
    max_concurrent_tasks: int = 4

    @classmethod
    def from_env(cls) -> "ServiceLoopSettings":
        load_dotenv()
        return cls(
            hyperliquid_enabled=env_bool("SPX_SERVICE_ENABLE_HYPERLIQUID", True),
            polymarket_enabled=env_bool("SPX_SERVICE_ENABLE_POLYMARKET", False),
            ibkr_enabled=env_bool("SPX_SERVICE_ENABLE_IBKR", False),
            iv_surface_enabled=env_bool("SPX_SERVICE_ENABLE_IV_SURFACE", True),
            intraday_shock_enabled=env_bool("SPX_SERVICE_ENABLE_INTRADAY_SHOCK", False),
            alert_enabled=env_bool("SPX_SERVICE_ENABLE_ALERTS", True),
            hyperliquid_interval_seconds=env_int("SPX_SERVICE_HYPERLIQUID_INTERVAL_SECONDS", 30),
            polymarket_interval_seconds=env_int("SPX_SERVICE_POLYMARKET_INTERVAL_SECONDS", 60),
            ibkr_interval_seconds=env_int("SPX_SERVICE_IBKR_INTERVAL_SECONDS", 60),
            iv_surface_interval_seconds=env_int("SPX_SERVICE_IV_SURFACE_INTERVAL_SECONDS", 300),
            intraday_shock_interval_seconds=env_int(
                "SPX_SERVICE_INTRADAY_SHOCK_INTERVAL_SECONDS", 5
            ),
            alert_interval_seconds=env_int("SPX_SERVICE_ALERT_INTERVAL_SECONDS", 30),
            heartbeat_seconds=env_int("SPX_SERVICE_HEARTBEAT_SECONDS", 60),
            ibkr_positions_enabled=env_bool("IBKR_POSITIONS_ENABLED", False),
            ibkr_positions_interval_seconds=env_int("IBKR_POSITIONS_POLL_SECONDS", 60),
            schwab_chains_enabled=env_bool("SPX_SERVICE_SCHWAB_CHAINS_ENABLED", False),
            schwab_chains_interval_seconds=env_int("SPX_SERVICE_SCHWAB_CHAINS_INTERVAL_SECONDS", 300),
            ibkr_skip_options=env_bool("SPX_SERVICE_IBKR_SKIP_OPTIONS", False),
            ibkr_connect_retry_seconds=env_int("IBKR_CONNECT_RETRY_SECONDS", 60),
            ibkr_conflict_probe_seconds=env_int("IBKR_CONFLICT_PROBE_SECONDS", 60),
            max_concurrent_tasks=env_int("SPX_SERVICE_MAX_CONCURRENT_TASKS", 4),
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


@contextmanager
def task_timeout(seconds: int):
    # SIGALRM only works in the main thread. In-process fn tasks running on a
    # worker thread get no hard timeout; real service tasks all run as child
    # processes (ServiceTask.command), which have their own subprocess timeout.
    if seconds <= 0 or threading.current_thread() is not threading.main_thread():
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


def run_intraday_shock() -> int:
    return intraday_shock.run(["--json"])


def run_ibkr_positions() -> int:
    from spx_spark.ibkr import position_watcher

    return position_watcher.run(["--json"])


def run_schwab_collector() -> int:
    from spx_spark.schwab import collector as schwab_collector

    return schwab_collector.run()


def console_script(name: str) -> str:
    return str(Path(sys.executable).with_name(name))


def build_tasks(settings: ServiceLoopSettings) -> list[ServiceTask]:
    tasks: list[ServiceTask] = []
    # Keep the lightweight shock path first so it is not queued behind slow
    # collectors or an LLM-backed full alert review when several tasks become
    # due on the same tick.
    if settings.intraday_shock_enabled:
        tasks.append(
            ServiceTask(
                "intraday_shock",
                settings.intraday_shock_interval_seconds,
                run_intraday_shock,
                command=(console_script("spx-spark-intraday-shock"), "--json"),
            )
        )
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
    if settings.ibkr_positions_enabled:
        tasks.append(
            ServiceTask(
                "ibkr_positions",
                settings.ibkr_positions_interval_seconds,
                run_ibkr_positions,
                command=(console_script("spx-spark-ibkr-positions"), "--json"),
            )
        )
    if settings.schwab_chains_enabled:
        tasks.append(
            ServiceTask(
                "schwab_chains",
                settings.schwab_chains_interval_seconds,
                run_schwab_collector,
                command=(console_script("spx-spark-schwab-collector"),),
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
    if task.name in {"alert_engine", "intraday_shock"}:
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


def submit_due_tasks(
    tasks: list[ServiceTask],
    in_flight: dict[str, Future],
    submit: Callable[[ServiceTask], Future],
    *,
    now: float,
) -> None:
    for task in tasks:
        if task.name in in_flight or now < task.next_run_monotonic:
            continue
        in_flight[task.name] = submit(task)


def drain_finished_tasks(
    tasks: list[ServiceTask],
    in_flight: dict[str, Future],
) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    for task in tasks:
        future = in_flight.get(task.name)
        if future is None or not future.done():
            continue
        del in_flight[task.name]
        event = future_event(task, future)
        events.append(event)
        task.next_run_monotonic = time.monotonic() + next_delay_seconds(task, event)
    return events


def future_event(task: ServiceTask, future: Future) -> dict[str, object]:
    try:
        return future.result()
    except Exception as exc:  # noqa: BLE001 - run_task already catches; this is a backstop
        return {
            "task": task.name,
            "ok": False,
            "exit_code": 1,
            "duration_ms": None,
            "finished_at": datetime.now(tz=timezone.utc).isoformat(),
            "error": f"task future failed: {exc}",
            "stdout_chars": 0,
            "stderr_chars": 0,
        }


def run_loop(
    tasks: list[ServiceTask],
    *,
    heartbeat_seconds: int,
    max_concurrent_tasks: int = 4,
) -> int:
    """Concurrent scheduler: one slow task no longer delays every other task.

    Each task runs on a worker thread (real tasks are child processes with
    their own timeout) and at most one instance of a given task is in flight,
    so a hung IBKR cycle cannot skew the alert/Hyperliquid cadence and tasks
    can never pile up on themselves.
    """
    executor = ThreadPoolExecutor(
        max_workers=max(max_concurrent_tasks, 1),
        thread_name_prefix="spx-task",
    )
    in_flight: dict[str, Future] = {}
    last_heartbeat = 0.0
    try:
        while True:
            now = time.monotonic()
            submit_due_tasks(tasks, in_flight, lambda task: executor.submit(run_task, task), now=now)
            for event in drain_finished_tasks(tasks, in_flight):
                print_event(event)

            if now - last_heartbeat >= heartbeat_seconds:
                print_event(
                    {
                        "task": "heartbeat",
                        "ok": True,
                        "finished_at": datetime.now(tz=timezone.utc).isoformat(),
                        "scheduled_tasks": [task.name for task in tasks],
                        "in_flight_tasks": sorted(in_flight),
                    }
                )
                last_heartbeat = now
            time.sleep(0.5)
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


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
    return run_loop(
        tasks,
        heartbeat_seconds=settings.heartbeat_seconds,
        max_concurrent_tasks=settings.max_concurrent_tasks,
    )


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
