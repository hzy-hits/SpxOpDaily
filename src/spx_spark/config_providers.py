"""Alternative-data provider and maintenance configuration contracts."""

from __future__ import annotations

from dataclasses import dataclass

from spx_spark.config import env_bool, env_csv_preserve, env_float, env_int, env_str, load_dotenv
from spx_spark.settings import settings_csv, settings_value


@dataclass(frozen=True)
class HyperliquidSettings:
    api_base_url: str
    dex: str
    coin: str
    request_timeout_seconds: float
    include_book: bool
    include_trades: bool
    book_depth_levels: int
    large_trade_notional_threshold: float

    @classmethod
    def from_env(cls) -> "HyperliquidSettings":
        load_dotenv()
        return cls(
            api_base_url=env_str(
                "HYPERLIQUID_API_BASE_URL", str(settings_value("hyperliquid.api_base_url"))
            ),
            dex=env_str("HYPERLIQUID_DEX", str(settings_value("hyperliquid.dex"))),
            coin=env_str("HYPERLIQUID_COIN", str(settings_value("hyperliquid.coin"))),
            request_timeout_seconds=env_float(
                "HYPERLIQUID_REQUEST_TIMEOUT_SECONDS",
                float(settings_value("hyperliquid.request_timeout_seconds")),
            ),
            include_book=env_bool(
                "HYPERLIQUID_INCLUDE_BOOK", bool(settings_value("hyperliquid.include_book"))
            ),
            include_trades=env_bool(
                "HYPERLIQUID_INCLUDE_TRADES", bool(settings_value("hyperliquid.include_trades"))
            ),
            book_depth_levels=env_int(
                "HYPERLIQUID_BOOK_DEPTH_LEVELS",
                int(settings_value("hyperliquid.book_depth_levels")),
            ),
            large_trade_notional_threshold=env_float(
                "HYPERLIQUID_LARGE_TRADE_NOTIONAL_THRESHOLD",
                float(settings_value("hyperliquid.large_trade_notional_threshold")),
            ),
        )


@dataclass(frozen=True)
class PolymarketSettings:
    gamma_api_base_url: str
    request_timeout_seconds: float
    search_terms: list[str]
    event_slugs: list[str]
    market_slugs: list[str]
    max_results_per_query: int
    max_markets_per_run: int
    min_liquidity: float
    min_volume_24h: float
    min_relevance_score: float
    include_closed: bool
    user_agent: str

    @classmethod
    def from_env(cls) -> "PolymarketSettings":
        load_dotenv()
        return cls(
            gamma_api_base_url=env_str(
                "POLYMARKET_GAMMA_API_BASE_URL",
                str(settings_value("polymarket.gamma_api_base_url")),
            ),
            request_timeout_seconds=env_float(
                "POLYMARKET_REQUEST_TIMEOUT_SECONDS",
                float(settings_value("polymarket.request_timeout_seconds")),
            ),
            search_terms=env_csv_preserve(
                "POLYMARKET_SEARCH_TERMS",
                settings_csv("polymarket.search_terms"),
            ),
            event_slugs=env_csv_preserve(
                "POLYMARKET_EVENT_SLUGS", settings_csv("polymarket.event_slugs")
            ),
            market_slugs=env_csv_preserve(
                "POLYMARKET_MARKET_SLUGS", settings_csv("polymarket.market_slugs")
            ),
            max_results_per_query=env_int(
                "POLYMARKET_MAX_RESULTS_PER_QUERY",
                int(settings_value("polymarket.max_results_per_query")),
            ),
            max_markets_per_run=env_int(
                "POLYMARKET_MAX_MARKETS_PER_RUN",
                int(settings_value("polymarket.max_markets_per_run")),
            ),
            min_liquidity=env_float(
                "POLYMARKET_MIN_LIQUIDITY", float(settings_value("polymarket.min_liquidity"))
            ),
            min_volume_24h=env_float(
                "POLYMARKET_MIN_VOLUME_24H", float(settings_value("polymarket.min_volume_24h"))
            ),
            min_relevance_score=env_float(
                "POLYMARKET_MIN_RELEVANCE_SCORE",
                float(settings_value("polymarket.min_relevance_score")),
            ),
            include_closed=env_bool(
                "POLYMARKET_INCLUDE_CLOSED", bool(settings_value("polymarket.include_closed"))
            ),
            user_agent=env_str(
                "POLYMARKET_USER_AGENT",
                str(settings_value("polymarket.user_agent")),
            ),
        )


@dataclass(frozen=True)
class MaintenanceSettings:
    data_root: str
    logs_root: str
    output_root: str
    data_budget_gb: float
    raw_retention_days: int
    alert_window_retention_days: int
    feature_1s_retention_days: int
    feature_5s_retention_days: int
    log_retention_days: int
    trash_retention_days: int
    warn_pct: float
    compact_pct: float
    degraded_pct: float
    prune_pct: float
    critical_pct: float

    @classmethod
    def from_env(cls) -> "MaintenanceSettings":
        load_dotenv()
        return cls(
            data_root=env_str(
                "MAINTENANCE_DATA_ROOT", str(settings_value("maintenance.data_root"))
            ),
            logs_root=env_str(
                "MAINTENANCE_LOGS_ROOT", str(settings_value("maintenance.logs_root"))
            ),
            output_root=env_str(
                "MAINTENANCE_OUTPUT_ROOT", str(settings_value("maintenance.output_root"))
            ),
            data_budget_gb=env_float(
                "MAINTENANCE_DATA_BUDGET_GB", float(settings_value("maintenance.data_budget_gb"))
            ),
            raw_retention_days=env_int(
                "MAINTENANCE_RAW_RETENTION_DAYS",
                int(settings_value("maintenance.raw_retention_days")),
            ),
            alert_window_retention_days=env_int(
                "MAINTENANCE_ALERT_WINDOW_RETENTION_DAYS",
                int(settings_value("maintenance.alert_window_retention_days")),
            ),
            feature_1s_retention_days=env_int(
                "MAINTENANCE_FEATURE_1S_RETENTION_DAYS",
                int(settings_value("maintenance.feature_1s_retention_days")),
            ),
            feature_5s_retention_days=env_int(
                "MAINTENANCE_FEATURE_5S_RETENTION_DAYS",
                int(settings_value("maintenance.feature_5s_retention_days")),
            ),
            log_retention_days=env_int(
                "MAINTENANCE_LOG_RETENTION_DAYS",
                int(settings_value("maintenance.log_retention_days")),
            ),
            trash_retention_days=env_int(
                "MAINTENANCE_TRASH_RETENTION_DAYS",
                int(settings_value("maintenance.trash_retention_days")),
            ),
            warn_pct=env_float(
                "MAINTENANCE_WARN_PCT", float(settings_value("maintenance.warn_pct"))
            ),
            compact_pct=env_float(
                "MAINTENANCE_COMPACT_PCT", float(settings_value("maintenance.compact_pct"))
            ),
            degraded_pct=env_float(
                "MAINTENANCE_DEGRADED_PCT", float(settings_value("maintenance.degraded_pct"))
            ),
            prune_pct=env_float(
                "MAINTENANCE_PRUNE_PCT", float(settings_value("maintenance.prune_pct"))
            ),
            critical_pct=env_float(
                "MAINTENANCE_CRITICAL_PCT", float(settings_value("maintenance.critical_pct"))
            ),
        )
