"""Compatibility facade for the 24h service loop.

Implementation lives under ``spx_spark.application.runtime``
(settings / registry / runner / scheduler / health / tasks).
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict

from spx_spark.application.runtime.registry import (
    ServiceTask,
    TaskFn,
    build_tasks,
    console_script,
    make_run_ibkr,
    run_alert_engine,
    run_greek_shadow,
    run_hyperliquid,
    run_ibkr_positions,
    run_intraday_shock,
    run_iv_surface,
    run_polymarket,
    run_provider_failover,
    run_realtime_engine,
    run_schwab_collector,
    run_steven,
)
from spx_spark.application.runtime.runner import (
    add_alert_summary_fields,
    add_greek_shadow_summary_fields,
    add_ibkr_summary_fields,
    normalize_timeout_output,
    run_task,
    run_task_command,
    task_timeout,
)
from spx_spark.application.runtime.scheduler import (
    drain_finished_tasks,
    future_event,
    next_delay_seconds,
    print_event,
    run_loop,
    run_once,
    submit_due_tasks,
)
from spx_spark.application.runtime.settings import (
    DEFAULT_MAX_CONCURRENT_TASKS,
    DEFAULT_OUTPUT_TAIL_CHARACTERS,
    DEFAULT_TASK_TIMEOUT_SECONDS,
    ServiceLoopSettings,
)

__all__ = [
    "DEFAULT_MAX_CONCURRENT_TASKS",
    "DEFAULT_OUTPUT_TAIL_CHARACTERS",
    "DEFAULT_TASK_TIMEOUT_SECONDS",
    "ServiceLoopSettings",
    "ServiceTask",
    "TaskFn",
    "add_alert_summary_fields",
    "add_greek_shadow_summary_fields",
    "add_ibkr_summary_fields",
    "build_tasks",
    "console_script",
    "drain_finished_tasks",
    "future_event",
    "main",
    "make_run_ibkr",
    "next_delay_seconds",
    "normalize_timeout_output",
    "parse_args",
    "print_event",
    "run",
    "run_alert_engine",
    "run_greek_shadow",
    "run_hyperliquid",
    "run_ibkr_positions",
    "run_intraday_shock",
    "run_iv_surface",
    "run_loop",
    "run_once",
    "run_polymarket",
    "run_provider_failover",
    "run_realtime_engine",
    "run_schwab_collector",
    "run_steven",
    "run_task",
    "run_task_command",
    "submit_due_tasks",
    "task_timeout",
]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the SPX Spark 24h service loop.")
    parser.add_argument("--once", action="store_true", help="Run each enabled task once and exit.")
    parser.add_argument("--print-config", action="store_true", help="Print resolved service settings.")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    # Load typed AppSettings once at the process entry; ServiceLoopSettings builds
    # from the RuntimePolicy / AlertPolicy / Schwab slices (no runtime_value).
    from spx_spark.settings import load_app_settings

    app_settings = load_app_settings()
    settings = ServiceLoopSettings.from_app_settings(app_settings)
    tasks = build_tasks(settings)
    if args.print_config:
        print(
            json.dumps(
                {
                    "settings": asdict(settings),
                    "tasks": [task.name for task in tasks],
                    "app_settings_sources": {
                        path: source.origin for path, source in app_settings.sources.items()
                    },
                },
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
