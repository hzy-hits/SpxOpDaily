"""Subprocess / in-process task runner and stdout summary enrichment."""

from __future__ import annotations

import contextlib
import io
import json
import signal
import subprocess
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone

from spx_spark.application.runtime.registry import ServiceTask
from spx_spark.application.runtime.settings import (
    DEFAULT_OUTPUT_TAIL_CHARACTERS,
    DEFAULT_TASK_TIMEOUT_SECONDS,
)
from spx_spark.config import env_int


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
            with (
                task_timeout(timeout_seconds),
                contextlib.redirect_stdout(stdout),
                contextlib.redirect_stderr(stderr),
            ):
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
        tail_chars = env_int("SPX_SERVICE_OUTPUT_TAIL_CHARS", DEFAULT_OUTPUT_TAIL_CHARACTERS)
        if stdout_text:
            event["stdout_tail"] = stdout_text[-tail_chars:]
        if stderr_text:
            event["stderr_tail"] = stderr_text[-tail_chars:]
    if task.name == "ibkr":
        add_ibkr_summary_fields(event, stdout_text)
    if task.name in {"alert_engine", "intraday_shock"}:
        add_alert_summary_fields(event, stdout_text)
    if task.name == "greek_shadow":
        add_greek_shadow_summary_fields(event, stdout_text)
    if task.name == "realtime_engine":
        add_realtime_engine_summary_fields(event, stdout_text)
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
    intraday_path = summary.get("intraday_path")
    if isinstance(intraday_path, dict):
        event["intraday_path_status"] = intraday_path.get("status")
        event["intraday_path_play"] = intraday_path.get("play")
        event["intraday_path_blocks"] = intraday_path.get("blocks")
    outcome = summary.get("outcome_tracking")
    if isinstance(outcome, dict):
        event["outcome_tracking_status"] = outcome.get("status")
        event["outcome_records_emitted"] = outcome.get("records_emitted")
        event["outcome_tracking_error"] = outcome.get("error")
    if summary.get("option_structure_error"):
        event["option_structure_error"] = summary.get("option_structure_error")
    greek_events = summary.get("greek_shadow_events")
    if isinstance(greek_events, list):
        event["greek_shadow_event_statuses"] = [
            {
                "status": row.get("status"),
                "reference_status": row.get("reference_status"),
                "reason": row.get("reason"),
            }
            for row in greek_events
            if isinstance(row, dict)
        ]
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


def add_greek_shadow_summary_fields(event: dict[str, object], stdout_text: str) -> None:
    try:
        summary = json.loads(stdout_text)
    except json.JSONDecodeError:
        return
    if not isinstance(summary, dict):
        return
    event["shadow_status"] = summary.get("status")
    event["shadow_reference_status"] = summary.get("reference_status")
    event["shadow_reason"] = summary.get("reason")
    event["shadow_expiry"] = summary.get("expiry")


def add_realtime_engine_summary_fields(
    event: dict[str, object],
    stdout_text: str,
) -> None:
    try:
        summary = json.loads(stdout_text)
    except json.JSONDecodeError:
        return
    if not isinstance(summary, dict):
        return
    tick = summary.get("tick")
    if not isinstance(tick, dict):
        return
    health = tick.get("health")
    if not isinstance(health, dict):
        return
    event["engine_mode"] = health.get("mode")
    event["engine_health"] = health
