"""Typed settings for the 24h service loop."""

from __future__ import annotations

from dataclasses import dataclass

from spx_spark.config import (
    env_bool,
    env_int,
    ibkr_account_read_enabled,
    ibkr_legacy_position_poller_enabled,
    load_dotenv,
)
from spx_spark.settings import AppSettings, load_app_settings
from spx_spark.settings.runtime import RuntimeSettingsSlice


DEFAULT_TASK_TIMEOUT_SECONDS = 120
DEFAULT_OUTPUT_TAIL_CHARACTERS = 1200
DEFAULT_MAX_CONCURRENT_TASKS = 4


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
    provider_failover_enabled: bool = True
    provider_failover_interval_seconds: int = 15
    ibkr_positions_enabled: bool = False
    ibkr_positions_interval_seconds: int = 60
    schwab_chains_enabled: bool = True
    schwab_chains_interval_seconds: int = 5
    max_concurrent_tasks: int = 4
    greek_shadow_enabled: bool = False
    greek_shadow_interval_seconds: int = 60
    steven_enabled: bool = False
    steven_interval_seconds: int = 30
    realtime_engine_enabled: bool = True
    realtime_engine_interval_seconds: int = 15
    notification_recovery_enabled: bool = True
    notification_recovery_interval_seconds: int = 60
    globex_trend_enabled: bool = False
    globex_trend_interval_seconds: int = 30
    market_features_enabled: bool = False
    market_features_interval_seconds: int = 60

    @classmethod
    def from_app_settings(cls, app: AppSettings) -> "ServiceLoopSettings":
        """Build loop toggles from typed AppSettings + process env overrides."""

        runtime: RuntimeSettingsSlice = app.runtime
        return cls(
            hyperliquid_enabled=env_bool(
                "SPX_SERVICE_ENABLE_HYPERLIQUID",
                runtime.hyperliquid_enabled,
            ),
            polymarket_enabled=env_bool(
                "SPX_SERVICE_ENABLE_POLYMARKET",
                runtime.polymarket_enabled,
            ),
            ibkr_enabled=env_bool("SPX_SERVICE_ENABLE_IBKR", runtime.ibkr_enabled),
            iv_surface_enabled=env_bool(
                "SPX_SERVICE_ENABLE_IV_SURFACE",
                runtime.iv_surface_enabled,
            ),
            intraday_shock_enabled=env_bool(
                "SPX_SERVICE_ENABLE_INTRADAY_SHOCK",
                runtime.intraday_shock_enabled,
            ),
            alert_enabled=env_bool(
                "SPX_SERVICE_ENABLE_ALERTS",
                runtime.alerts_enabled,
            ),
            hyperliquid_interval_seconds=env_int(
                "SPX_SERVICE_HYPERLIQUID_INTERVAL_SECONDS",
                runtime.hyperliquid_interval_seconds,
            ),
            polymarket_interval_seconds=env_int(
                "SPX_SERVICE_POLYMARKET_INTERVAL_SECONDS",
                runtime.polymarket_interval_seconds,
            ),
            ibkr_interval_seconds=env_int(
                "SPX_SERVICE_IBKR_INTERVAL_SECONDS",
                runtime.ibkr_interval_seconds,
            ),
            iv_surface_interval_seconds=env_int(
                "SPX_SERVICE_IV_SURFACE_INTERVAL_SECONDS",
                runtime.iv_surface_interval_seconds,
            ),
            intraday_shock_interval_seconds=env_int(
                "SPX_SERVICE_INTRADAY_SHOCK_INTERVAL_SECONDS",
                runtime.intraday_shock_interval_seconds,
            ),
            alert_interval_seconds=env_int(
                "SPX_SERVICE_ALERT_INTERVAL_SECONDS",
                runtime.alert_interval_seconds,
            ),
            heartbeat_seconds=env_int(
                "SPX_SERVICE_HEARTBEAT_SECONDS",
                runtime.heartbeat_seconds,
            ),
            ibkr_positions_enabled=(
                ibkr_account_read_enabled() and ibkr_legacy_position_poller_enabled()
            ),
            ibkr_positions_interval_seconds=env_int(
                "IBKR_POSITIONS_POLL_SECONDS",
                runtime.ibkr_positions_poll_interval_seconds,
            ),
            schwab_chains_enabled=env_bool(
                "SPX_SERVICE_SCHWAB_CHAINS_ENABLED",
                app.schwab.service_loop_enabled,
            ),
            schwab_chains_interval_seconds=env_int(
                "SPX_SERVICE_SCHWAB_CHAINS_INTERVAL_SECONDS",
                app.schwab.collection_interval_seconds,
            ),
            ibkr_skip_options=env_bool(
                "SPX_SERVICE_IBKR_SKIP_OPTIONS",
                runtime.ibkr_skip_options,
            ),
            ibkr_connect_retry_seconds=env_int(
                "IBKR_CONNECT_RETRY_SECONDS",
                runtime.ibkr_connect_retry_seconds,
            ),
            ibkr_conflict_probe_seconds=env_int(
                "IBKR_CONFLICT_PROBE_SECONDS",
                runtime.ibkr_conflict_probe_seconds,
            ),
            provider_failover_enabled=env_bool(
                "PROVIDER_FAILOVER_ENABLED",
                runtime.provider_failover_enabled,
            ),
            provider_failover_interval_seconds=env_int(
                "PROVIDER_FAILOVER_INTERVAL_SECONDS",
                runtime.provider_failover_interval_seconds,
            ),
            max_concurrent_tasks=env_int(
                "SPX_SERVICE_MAX_CONCURRENT_TASKS",
                runtime.max_concurrent_tasks,
            ),
            greek_shadow_enabled=env_bool(
                "SPX_SERVICE_ENABLE_GREEK_SHADOW",
                runtime.greek_shadow_enabled,
            ),
            greek_shadow_interval_seconds=env_int(
                "SPX_SERVICE_GREEK_SHADOW_INTERVAL_SECONDS",
                runtime.greek_shadow_interval_seconds,
            ),
            steven_enabled=env_bool(
                "SPX_SERVICE_ENABLE_STEVEN",
                app.alerts.steven_enabled,
            ),
            steven_interval_seconds=env_int(
                "SPX_SERVICE_STEVEN_INTERVAL_SECONDS",
                runtime.alert_interval_seconds,
            ),
            realtime_engine_enabled=env_bool(
                "SPX_SERVICE_ENABLE_REALTIME_ENGINE",
                runtime.realtime_engine_enabled,
            ),
            realtime_engine_interval_seconds=env_int(
                "SPX_SERVICE_REALTIME_ENGINE_INTERVAL_SECONDS",
                runtime.realtime_engine_interval_seconds,
            ),
            notification_recovery_enabled=env_bool(
                "SPX_SERVICE_ENABLE_NOTIFICATION_RECOVERY",
                runtime.notification_recovery_enabled,
            ),
            notification_recovery_interval_seconds=env_int(
                "SPX_SERVICE_NOTIFICATION_RECOVERY_INTERVAL_SECONDS",
                runtime.notification_recovery_interval_seconds,
            ),
            globex_trend_enabled=env_bool(
                "SPX_SERVICE_ENABLE_GLOBEX_TREND",
                app.globex_trend.enabled,
            ),
            globex_trend_interval_seconds=env_int(
                "SPX_SERVICE_GLOBEX_TREND_INTERVAL_SECONDS",
                app.globex_trend.interval_seconds,
            ),
            market_features_enabled=env_bool(
                "SPX_SERVICE_ENABLE_MARKET_FEATURES",
                app.market_features.enabled,
            ),
            market_features_interval_seconds=env_int(
                "SPX_SERVICE_MARKET_FEATURES_INTERVAL_SECONDS",
                app.market_features.interval_seconds,
            ),
        )

    @classmethod
    def from_env(cls) -> "ServiceLoopSettings":
        load_dotenv()
        return cls.from_app_settings(load_app_settings())
