"""Task registry: ServiceTask model, runners, and build_tasks composition."""

from __future__ import annotations

import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from spx_spark import (
    alert_engine,
    greek_shadow,
    intraday_shock,
    iv_surface,
    provider_failover_controller,
)
from spx_spark.application.runtime.settings import ServiceLoopSettings
from spx_spark.application.runtime.tasks import (
    DEFAULT_TASK_CRITICALITY,
    TaskRuntimeState,
)
from spx_spark.domain.health import TaskCriticality
from spx_spark.hyperliquid import collector as hyperliquid_collector
from spx_spark.ibkr import collector as ibkr_collector


TaskFn = Callable[[], int]


@dataclass
class ServiceTask:
    name: str
    interval_seconds: int
    fn: TaskFn
    command: tuple[str, ...] | None = None
    failure_interval_seconds: int | None = None
    conflict_probe_seconds: int | None = None
    next_run_monotonic: float = 0.0
    criticality: TaskCriticality = TaskCriticality.IMPORTANT
    max_consecutive_failures: int = 5
    runtime: TaskRuntimeState | None = None

    def __post_init__(self) -> None:
        if self.runtime is None:
            criticality = DEFAULT_TASK_CRITICALITY.get(self.name, self.criticality)
            self.criticality = criticality
            self.runtime = TaskRuntimeState(
                name=self.name,
                criticality=criticality,
                max_consecutive_failures=self.max_consecutive_failures,
            )


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


def run_realtime_engine() -> int:
    from spx_spark.application.realtime.composition import run_realtime_engine_cycle

    return run_realtime_engine_cycle()


def run_intraday_shock() -> int:
    return intraday_shock.run(["--json"])


def run_notification_recovery() -> int:
    from spx_spark.notifier import missed_queue

    return missed_queue.run()


def run_globex_trend() -> int:
    from spx_spark.application.globex_trend import service

    return service.run(["--json"])


def run_market_features() -> int:
    from spx_spark.application.market_features import service

    return service.run(["--json"])


def run_greek_shadow() -> int:
    return greek_shadow.run(["--json"])


def run_steven() -> int:
    from spx_spark.strategy import steven as steven_strategy

    return steven_strategy.run(["--json"])


def run_ibkr_positions() -> int:
    from spx_spark.ibkr import position_watcher

    return position_watcher.run(["--json"])


def run_schwab_collector() -> int:
    from spx_spark.schwab import collector as schwab_collector

    return schwab_collector.run()


def run_provider_failover() -> int:
    return provider_failover_controller.run(["--json"])


def console_script(name: str) -> str:
    return str(Path(sys.executable).with_name(name))


def build_tasks(settings: ServiceLoopSettings) -> list[ServiceTask]:
    tasks: list[ServiceTask] = []
    if settings.provider_failover_enabled:
        tasks.append(
            ServiceTask(
                "provider_failover",
                settings.provider_failover_interval_seconds,
                run_provider_failover,
                command=(console_script("spx-spark-provider-failover"), "--json"),
            )
        )
    if settings.globex_trend_enabled:
        tasks.append(
            ServiceTask(
                "globex_trend",
                settings.globex_trend_interval_seconds,
                run_globex_trend,
                command=(
                    sys.executable,
                    "-m",
                    "spx_spark.application.globex_trend.service",
                    "--json",
                ),
            )
        )
    if settings.market_features_enabled:
        tasks.append(
            ServiceTask(
                "market_features",
                settings.market_features_interval_seconds,
                run_market_features,
                command=(
                    sys.executable,
                    "-m",
                    "spx_spark.application.market_features.service",
                    "--json",
                ),
            )
        )
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
    if settings.realtime_engine_enabled:
        tasks.append(
            ServiceTask(
                "realtime_engine",
                settings.realtime_engine_interval_seconds,
                run_realtime_engine,
                command=(sys.executable, "-m", "spx_spark.application.realtime.composition"),
            )
        )
    if settings.notification_recovery_enabled:
        tasks.append(
            ServiceTask(
                "notification_recovery",
                settings.notification_recovery_interval_seconds,
                run_notification_recovery,
                command=(
                    sys.executable,
                    "-c",
                    "from spx_spark.notifier.missed_queue import run; raise SystemExit(run())",
                ),
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
    # Steven observe-only guidance (default disabled via steven.enabled=false).
    if settings.steven_enabled:
        tasks.append(
            ServiceTask(
                "steven",
                settings.steven_interval_seconds,
                run_steven,
                command=(console_script("spx-spark-steven"), "--json"),
            )
        )
    # Shadow telemetry is intentionally last so the 4-worker scheduler never
    # queues live alerts behind higher-Greeks calculation at a shared tick.
    if settings.greek_shadow_enabled:
        tasks.append(
            ServiceTask(
                "greek_shadow",
                settings.greek_shadow_interval_seconds,
                run_greek_shadow,
                command=(console_script("spx-spark-greek-shadow"), "--json"),
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
