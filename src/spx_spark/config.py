from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta

from spx_spark.market_calendar import ET as NY_TZ
from spx_spark.market_calendar import default_spxw_expiry as _default_spxw_expiry
from spx_spark.settings import settings_csv, settings_value
from spx_spark.runtime_config import (
    runtime_schwab_option_chain_underliers,
    runtime_schwab_symbols_by_type,
)

default_spxw_expiry = _default_spxw_expiry

DEFAULT_SLOW_POLL_LABELS = (
    "index:VIX",
    "index:VIX1D",
    "index:VIX9D",
    "index:VIX3M",
    "index:VVIX",
    "index:SKEW",
    "stock:QQQ",
    "stock:IWM",
    "stock:DIA",
    "stock:HYG",
    "stock:LQD",
    "stock:TLT",
    "stock:IEF",
    "stock:SHY",
    "stock:UUP",
    "stock:GLD",
    "stock:USO",
    "stock:RSP",
    "stock:XLU",
)


def env_str(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip()


def env_int(name: str, default: int) -> int:
    raw = env_str(name)
    if not raw:
        return default
    return int(raw)


def env_float(name: str, default: float) -> float:
    raw = env_str(name)
    if not raw:
        return default
    return float(raw)


def env_bool(name: str, default: bool) -> bool:
    raw = env_str(name)
    if not raw:
        return default
    normalized = raw.lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean-like value, got {raw!r}")


def env_csv(name: str, default: str) -> list[str]:
    raw = env_str(name, default)
    return [part.strip().upper() for part in raw.split(",") if part.strip()]


def env_csv_preserve(name: str, default: str) -> list[str]:
    raw = env_str(name, default)
    return [part.strip() for part in raw.split(",") if part.strip()]


def load_dotenv(path: str = ".env") -> None:
    """Load a small .env file without requiring python-dotenv."""
    # Unit tests pin SPX_SPARK_DISABLE_DOTENV so workspace deployment .env
    # values cannot change Settings.from_env() behavior.
    disabled = os.getenv("SPX_SPARK_DISABLE_DOTENV", "").strip().lower()
    if disabled in {"1", "true", "yes", "y", "on"}:
        return
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def next_equity_futures_month(today: date | None = None) -> str:
    """Return the next quarterly CME equity futures contract month as YYYYMM."""
    if today is None:
        today = datetime.now(tz=NY_TZ).date()

    quarterly_months = (3, 6, 9, 12)
    year = today.year
    candidates: list[date] = []
    for offset_year in (0, 1):
        candidate_year = year + offset_year
        for month in quarterly_months:
            third_friday = _third_friday(candidate_year, month)
            if third_friday >= today + timedelta(days=7):
                candidates.append(third_friday)
    expiry = min(candidates)
    return f"{expiry.year}{expiry.month:02d}"


def _third_friday(year: int, month: int) -> date:
    first = date(year, month, 1)
    days_until_friday = (4 - first.weekday()) % 7
    first_friday = first + timedelta(days=days_until_friday)
    return first_friday + timedelta(days=14)


def parse_hhmm(value: str) -> time:
    try:
        hour_raw, minute_raw = value.split(":", 1)
        hour = int(hour_raw)
        minute = int(minute_raw)
    except ValueError as exc:
        raise ValueError(f"Expected HH:MM time, got {value!r}") from exc
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError(f"Expected HH:MM time, got {value!r}")
    return time(hour=hour, minute=minute)


def is_time_in_window(current: time, start: time, stop: time) -> bool:
    if start == stop:
        return True
    if start < stop:
        return start <= current < stop
    return current >= start or current < stop


@dataclass(frozen=True)
class SchwabSettings:
    api_base_url: str
    access_token: str
    token_file: str
    verify_indexes: list[str]
    verify_equities: list[str]
    verify_futures: list[str]
    verify_option_chains: list[str]
    option_chain_strike_count: int
    quote_fields: str
    request_timeout_seconds: float
    app_key: str = ""
    app_secret: str = ""
    callback_url: str = field(default_factory=lambda: "https://127.0.0.1:8182")
    oauth_bind_host: str = field(default_factory=lambda: "127.0.0.1")
    oauth_bind_port: int = field(default_factory=lambda: 8183)
    oauth_state_file: str = field(default_factory=lambda: "runtime/schwab-oauth-state.json")
    oauth_state_ttl_seconds: int = field(default_factory=lambda: 900)
    gateway_bind_host: str = field(default_factory=lambda: "127.0.0.1")
    gateway_bind_port: int = field(default_factory=lambda: 8184)
    gateway_url: str = ""

    @classmethod
    def from_env(cls) -> "SchwabSettings":
        load_dotenv()
        return cls(
            api_base_url=env_str("SCHWAB_API_BASE_URL", str(settings_value("schwab.api_base_url"))),
            access_token=env_str("SCHWAB_ACCESS_TOKEN"),
            token_file=env_str("SCHWAB_TOKEN_FILE", str(settings_value("schwab.token_file"))),
            verify_indexes=env_csv(
                "SCHWAB_VERIFY_INDEXES",
                ",".join(runtime_schwab_symbols_by_type("index")),
            ),
            verify_equities=env_csv(
                "SCHWAB_VERIFY_EQUITIES",
                ",".join(runtime_schwab_symbols_by_type("equity")),
            ),
            verify_futures=env_csv(
                "SCHWAB_VERIFY_FUTURES",
                ",".join(runtime_schwab_symbols_by_type("future")),
            ),
            verify_option_chains=env_csv(
                "SCHWAB_VERIFY_OPTION_CHAINS",
                ",".join(runtime_schwab_option_chain_underliers()),
            ),
            option_chain_strike_count=env_int(
                "SCHWAB_OPTION_CHAIN_STRIKE_COUNT",
                int(settings_value("schwab.option_chain_strike_count")),
            ),
            quote_fields=env_str(
                "SCHWAB_QUOTE_FIELDS",
                str(settings_value("schwab.quote_fields")),
            ),
            request_timeout_seconds=env_float(
                "SCHWAB_REQUEST_TIMEOUT_SECONDS",
                float(settings_value("schwab.request_timeout_seconds")),
            ),
            app_key=env_str("SCHWAB_APP_KEY"),
            app_secret=env_str("SCHWAB_APP_SECRET"),
            callback_url=env_str("SCHWAB_CALLBACK_URL", str(settings_value("schwab.callback_url"))),
            oauth_bind_host=env_str(
                "SCHWAB_OAUTH_BIND_HOST", str(settings_value("schwab.oauth_bind_host"))
            ),
            oauth_bind_port=env_int(
                "SCHWAB_OAUTH_BIND_PORT", int(settings_value("schwab.oauth_bind_port"))
            ),
            oauth_state_file=env_str(
                "SCHWAB_OAUTH_STATE_FILE",
                str(settings_value("schwab.oauth_state_file")),
            ),
            oauth_state_ttl_seconds=env_int(
                "SCHWAB_OAUTH_STATE_TTL_SECONDS",
                int(settings_value("schwab.oauth_state_ttl_seconds")),
            ),
            gateway_bind_host=env_str(
                "SCHWAB_GATEWAY_BIND_HOST", str(settings_value("schwab.gateway_bind_host"))
            ),
            gateway_bind_port=env_int(
                "SCHWAB_GATEWAY_BIND_PORT", int(settings_value("schwab.gateway_bind_port"))
            ),
            gateway_url=env_str("SCHWAB_GATEWAY_URL"),
        )


@dataclass(frozen=True)
class SchwabStreamSettings:
    mode: str
    canonical_symbols: tuple[str, ...]
    flush_interval_seconds: float
    symbol_refresh_interval_seconds: float
    reconnect_min_seconds: float
    reconnect_max_seconds: float
    websocket_open_timeout_seconds: float
    shadow_latest_path: str
    option_hot_symbol_limit: int
    option_symbol_refresh_seconds: float
    option_plan_max_age_seconds: float
    validation_future_symbols: tuple[str, ...] = ()
    futures_option_probe_symbol: str = ""

    def __post_init__(self) -> None:
        if self.mode not in {"off", "shadow", "live"}:
            raise ValueError("SCHWAB_STREAMING_MODE must be off, shadow, or live")
        if not self.canonical_symbols:
            raise ValueError("SCHWAB_STREAM_SYMBOLS cannot be empty")
        if len(set(self.canonical_symbols)) != len(self.canonical_symbols):
            raise ValueError("SCHWAB_STREAM_SYMBOLS cannot contain duplicates")
        if self.flush_interval_seconds <= 0:
            raise ValueError("SCHWAB_STREAM_FLUSH_SECONDS must be positive")
        if self.symbol_refresh_interval_seconds <= 0:
            raise ValueError("SCHWAB_STREAM_SYMBOL_REFRESH_SECONDS must be positive")
        if self.reconnect_min_seconds <= 0:
            raise ValueError("SCHWAB_STREAM_RECONNECT_MIN_SECONDS must be positive")
        if self.reconnect_max_seconds < self.reconnect_min_seconds:
            raise ValueError("SCHWAB_STREAM_RECONNECT_MAX_SECONDS cannot be below minimum")
        if self.websocket_open_timeout_seconds <= 0:
            raise ValueError("SCHWAB_STREAM_OPEN_TIMEOUT_SECONDS must be positive")
        if self.option_hot_symbol_limit <= 0:
            raise ValueError("SCHWAB_STREAM_OPTION_HOT_SYMBOL_LIMIT must be positive")
        if self.option_symbol_refresh_seconds <= 0:
            raise ValueError("SCHWAB_STREAM_OPTION_REFRESH_SECONDS must be positive")
        if self.option_plan_max_age_seconds <= 0:
            raise ValueError("SCHWAB_STREAM_OPTION_PLAN_MAX_AGE_SECONDS must be positive")
        if len(set(self.validation_future_symbols)) != len(self.validation_future_symbols):
            raise ValueError("SCHWAB_STREAM_VALIDATION_FUTURES cannot contain duplicates")
        if set(self.validation_future_symbols) & set(self.canonical_symbols):
            raise ValueError("validation futures must remain outside the production universe")
        if "," in self.futures_option_probe_symbol:
            raise ValueError("SCHWAB_STREAM_FUTURES_OPTION_PROBE must contain one symbol")

    @classmethod
    def from_env(cls, *, data_root: str | None = None) -> "SchwabStreamSettings":
        load_dotenv()
        root = data_root or env_str(
            "MARKET_DATA_DATA_ROOT",
            env_str("MAINTENANCE_DATA_ROOT", str(settings_value("maintenance.data_root"))),
        )
        configured_shadow_path = str(settings_value("schwab.streaming.shadow_latest_path")).strip()
        return cls(
            mode=env_str(
                "SCHWAB_STREAMING_MODE",
                str(settings_value("schwab.streaming.mode")),
            ).lower(),
            canonical_symbols=tuple(
                symbol.upper()
                for symbol in env_csv_preserve(
                    "SCHWAB_STREAM_SYMBOLS",
                    settings_csv("schwab.streaming.canonical_symbols"),
                )
            ),
            flush_interval_seconds=env_float(
                "SCHWAB_STREAM_FLUSH_SECONDS",
                float(settings_value("schwab.streaming.flush_interval_seconds")),
            ),
            symbol_refresh_interval_seconds=env_float(
                "SCHWAB_STREAM_SYMBOL_REFRESH_SECONDS",
                float(settings_value("schwab.streaming.symbol_refresh_interval_seconds")),
            ),
            reconnect_min_seconds=env_float(
                "SCHWAB_STREAM_RECONNECT_MIN_SECONDS",
                float(settings_value("schwab.streaming.reconnect_min_seconds")),
            ),
            reconnect_max_seconds=env_float(
                "SCHWAB_STREAM_RECONNECT_MAX_SECONDS",
                float(settings_value("schwab.streaming.reconnect_max_seconds")),
            ),
            websocket_open_timeout_seconds=env_float(
                "SCHWAB_STREAM_OPEN_TIMEOUT_SECONDS",
                float(settings_value("schwab.streaming.websocket_open_timeout_seconds")),
            ),
            shadow_latest_path=os.getenv("SCHWAB_STREAM_SHADOW_LATEST_PATH")
            or configured_shadow_path
            or f"{root.rstrip('/')}/latest/schwab_stream_shadow.json",
            option_hot_symbol_limit=env_int(
                "SCHWAB_STREAM_OPTION_HOT_SYMBOL_LIMIT",
                int(settings_value("schwab.streaming.option_hot_symbol_limit")),
            ),
            option_symbol_refresh_seconds=env_float(
                "SCHWAB_STREAM_OPTION_REFRESH_SECONDS",
                float(settings_value("schwab.streaming.option_symbol_refresh_seconds")),
            ),
            option_plan_max_age_seconds=env_float(
                "SCHWAB_STREAM_OPTION_PLAN_MAX_AGE_SECONDS",
                float(settings_value("schwab.streaming.option_plan_max_age_seconds")),
            ),
            validation_future_symbols=tuple(
                symbol.upper()
                for symbol in env_csv_preserve(
                    "SCHWAB_STREAM_VALIDATION_FUTURES",
                    settings_csv("schwab.streaming.validation_future_symbols"),
                )
            ),
            futures_option_probe_symbol=env_str(
                "SCHWAB_STREAM_FUTURES_OPTION_PROBE",
                str(settings_value("schwab.streaming.futures_option_probe_symbol")),
            ).strip().upper(),
        )


@dataclass(frozen=True)
class StorageSettings:
    data_root: str
    latest_state_path: str
    raw_file_name: str
    include_raw_payload: bool
    latest_stale_after_seconds: float
    slow_index_stale_after_seconds: float
    slow_index_labels: frozenset[str]
    delayed_stale_after_seconds: float = field(default_factory=lambda: 60)
    provider_priority: tuple[str, ...] = field(
        default_factory=lambda: tuple(
            str(item).lower()
            for item in [
                "schwab",
                "ibkr",
                "hyperliquid",
                "polymarket",
                "internal",
                "mock",
                "unknown",
            ]
        )
    )

    def __post_init__(self) -> None:
        if not self.provider_priority:
            raise ValueError("MARKET_DATA_PROVIDER_PRIORITY cannot be empty")
        if len(set(self.provider_priority)) != len(self.provider_priority):
            raise ValueError("MARKET_DATA_PROVIDER_PRIORITY cannot contain duplicates")
        known = {str(item).lower() for item in settings_value("market_data.known_providers")}
        invalid = sorted(set(self.provider_priority) - known)
        if invalid:
            raise ValueError(
                "MARKET_DATA_PROVIDER_PRIORITY contains unsupported providers: " + ",".join(invalid)
            )

    @classmethod
    def from_env(cls) -> "StorageSettings":
        load_dotenv()
        data_root = env_str(
            "MARKET_DATA_DATA_ROOT",
            env_str("MAINTENANCE_DATA_ROOT", str(settings_value("maintenance.data_root"))),
        )
        return cls(
            data_root=data_root,
            latest_state_path=env_str(
                "MARKET_DATA_LATEST_STATE_PATH",
                f"{data_root.rstrip('/')}/latest/state.json",
            ),
            raw_file_name=env_str(
                "MARKET_DATA_RAW_FILE_NAME", str(settings_value("storage.raw_file_name"))
            ),
            include_raw_payload=env_bool(
                "MARKET_DATA_INCLUDE_RAW_PAYLOAD",
                bool(settings_value("storage.include_raw_payload")),
            ),
            latest_stale_after_seconds=env_float(
                "MARKET_DATA_LATEST_STALE_AFTER_SECONDS",
                float(settings_value("market_data.latest_stale_after_seconds")),
            ),
            slow_index_stale_after_seconds=env_float(
                "MARKET_DATA_SLOW_INDEX_STALE_AFTER_SECONDS",
                env_float(
                    "IBKR_SLOW_INDEX_STALE_AFTER_SECONDS",
                    float(settings_value("market_data.slow_index_stale_after_seconds")),
                ),
            ),
            # Preserve case: these must match canonical ids like "index:SKEW".
            slow_index_labels=frozenset(
                env_csv_preserve(
                    "MARKET_DATA_SLOW_INDEX_LABELS",
                    settings_csv("market_data.slow_index_labels"),
                )
            ),
            delayed_stale_after_seconds=env_float(
                "MARKET_DATA_DELAYED_STALE_AFTER_SECONDS",
                float(settings_value("market_data.delayed_stale_after_seconds")),
            ),
            provider_priority=tuple(
                provider.lower()
                for provider in env_csv_preserve(
                    "MARKET_DATA_PROVIDER_PRIORITY",
                    settings_csv("market_data.provider_priority"),
                )
            ),
        )


@dataclass(frozen=True)
class IvSurfaceSettings:
    data_root: str
    latest_surface_path: str
    raw_file_name: str
    wide_quote_spread_bps: float
    diff_max_gap_seconds: float

    @classmethod
    def from_env(cls) -> "IvSurfaceSettings":
        load_dotenv()
        data_root = env_str(
            "MARKET_DATA_DATA_ROOT",
            env_str("MAINTENANCE_DATA_ROOT", str(settings_value("maintenance.data_root"))),
        )
        return cls(
            data_root=data_root,
            latest_surface_path=env_str(
                "IV_SURFACE_LATEST_PATH",
                f"{data_root.rstrip('/')}/latest/iv_surface.json",
            ),
            raw_file_name=env_str(
                "IV_SURFACE_RAW_FILE_NAME", str(settings_value("iv_surface.raw_file_name"))
            ),
            wide_quote_spread_bps=env_float(
                "IV_SURFACE_WIDE_QUOTE_SPREAD_BPS",
                float(settings_value("iv_surface.wide_quote_spread_bps")),
            ),
            diff_max_gap_seconds=env_float(
                "IV_SURFACE_DIFF_MAX_GAP_SECONDS",
                float(settings_value("iv_surface.diff_max_gap_seconds")),
            ),
        )


def outbox_alert_evaluation_enabled() -> bool:
    return env_bool(
        "SPX_OUTBOX_ALERT_EVALUATION_ENABLED",
        bool(settings_value("notification.outbox_alert_evaluation_enabled")),
    )


def outbox_delivery_enabled() -> bool:
    return env_bool(
        "SPX_OUTBOX_DELIVERY_ENABLED",
        bool(settings_value("notification.outbox_delivery_enabled")),
    )


def direct_alert_delivery_enabled() -> bool:
    """When false, alert_engine skips direct notify so the outbox owns delivery."""

    return env_bool(
        "SPX_DIRECT_ALERT_DELIVERY_ENABLED",
        bool(settings_value("notification.direct_delivery_enabled")),
    )


def shock_direct_delivery_enabled() -> bool:
    """When true, intraday_shock may notify_payload on the latency-critical path.

    Shock events are not yet outbox ``ALERT_CANDIDATE`` rows. Keep this true so
    the fast path stays live while ``direct_delivery_enabled`` is false and the
    outbox owns periodic alert_engine delivery. Flip false only after shock is
    migrated onto the outbox path.
    """

    return env_bool(
        "SPX_SHOCK_DIRECT_DELIVERY_ENABLED",
        bool(settings_value("notification.shock_direct_delivery_enabled")),
    )


@dataclass(frozen=True)
class NotificationSettings:
    enabled: bool
    min_severity: str
    cooldown_seconds: int
    state_path: str
    openclaw_enabled: bool
    openclaw_command: str
    openclaw_channel: str
    openclaw_account: str
    openclaw_target: str
    openclaw_dry_run: bool
    openclaw_timeout_seconds: float
    openclaw_agent_enabled: bool
    openclaw_agent_deliver: bool
    openclaw_agent_name: str
    openclaw_agent_model: str
    openclaw_agent_session_key: str
    openclaw_agent_thinking: str
    openclaw_agent_timeout_seconds: float
    codex_enabled: bool
    codex_deliver: bool
    codex_command: str
    codex_model: str
    codex_reasoning_effort: str
    codex_cwd: str
    codex_sandbox: str
    codex_timeout_seconds: float
    codex_output_max_chars: int
    codex_require_delivery_cue: bool
    deepseek_enabled: bool = field(default_factory=lambda: bool(False))
    deepseek_deliver: bool = field(default_factory=lambda: True)
    deepseek_model: str = field(default_factory=lambda: "deepseek-v4-flash")
    deepseek_url: str = field(
        default_factory=lambda: "https://api.deepseek.com/v1/chat/completions"
    )
    deepseek_env_file: str = field(default_factory=lambda: "/home/ubuntu/.hermes/.env")
    deepseek_timeout_seconds: float = field(default_factory=lambda: 30.0)
    deepseek_max_tokens: int = field(default_factory=lambda: 6400)
    deepseek_output_max_chars: int = field(default_factory=lambda: 6400)
    deepseek_temperature: float = field(default_factory=lambda: 0.1)
    grok_enabled: bool = field(default_factory=lambda: False)
    grok_deliver: bool = field(default_factory=lambda: True)
    grok_command: str = field(default_factory=lambda: "agent")
    grok_model: str = field(default_factory=lambda: "grok-4.5")
    grok_reasoning_effort: str = field(default_factory=lambda: "medium")
    grok_cwd: str = field(default_factory=lambda: ".")
    grok_timeout_seconds: float = field(default_factory=lambda: 120.0)
    grok_output_max_chars: int = field(default_factory=lambda: 6400)
    review_min_time_sensitive_score: float = field(default_factory=lambda: 30.0)
    bark_enabled: bool = field(default_factory=lambda: False)
    bark_url: str = ""
    bark_group: str = field(default_factory=lambda: "spx-spark")
    # Ops/engineering pushes (IBKR session, data degradation, channel failures)
    # land in this Bark group so the trade group stays readable.
    bark_ops_group: str = field(default_factory=lambda: "spx-ops")
    bark_level: str = field(default_factory=lambda: "timeSensitive")
    bark_timeout_seconds: float = field(default_factory=lambda: 10.0)
    # When true, trading pushes also send the full markdown into Bark's App
    # detail view (lockscreen still uses the short body summary).
    bark_markdown_enabled: bool = field(default_factory=lambda: True)
    # Friend channel: trading content only (maps/status/review/market alerts),
    # never engineering noise (data degradation, session drops, token expiry).
    bark_friend_enabled: bool = field(default_factory=lambda: False)
    bark_friend_url: str = ""
    # Feishu custom-bot webhook: trading reading surface (interactive cards).
    # Ops stay on Bark main; leave disabled until webhook URL is configured.
    feishu_enabled: bool = field(default_factory=lambda: False)
    feishu_webhook_url: str = ""
    feishu_secret: str = ""
    feishu_timeout_seconds: float = field(default_factory=lambda: 10.0)
    # Rewrite direct-push events (position/system/off-hours vol) with the
    # configured writer before sending; falls back to the raw template on any
    # writer failure so critical events are never lost.
    direct_push_llm_enabled: bool = field(default_factory=lambda: bool(False))
    # Kind-level rate limit for magnitude-bucketed alerts: the per-bucket
    # dedup key lets a drifting value re-alert on every bucket step (observed
    # put_skew up:1 -> up:28 = 19 pushes/day), so the same kind+instrument is
    # capped to one push per this window unless the bucket jumps >= 2 steps,
    # the direction flips, or severity is critical.
    kind_rate_limit_seconds: float = field(default_factory=lambda: 3600.0)
    missed_queue_path: str = ""
    # SQLite ledger for every human-delivery attempt, including direct report
    # paths that do not pass through the domain-event outbox.
    delivery_receipt_path: str = ""
    # Retry policy for non-terminal outbox outcomes such as reviewer timeouts.
    outbox_max_attempts: int = 5
    outbox_retry_base_seconds: float = 60.0
    outbox_retry_max_seconds: float = 900.0
    outbox_claim_stale_after_seconds: float = 180.0
    # Append-only, owner-readable evidence for every LLM review decision. The
    # audit contains allowlisted alert facts and redacted model output only.
    review_audit_path: str = ""

    @classmethod
    def from_env(cls) -> "NotificationSettings":
        load_dotenv()
        data_root = env_str(
            "MARKET_DATA_DATA_ROOT",
            env_str("MAINTENANCE_DATA_ROOT", str(settings_value("maintenance.data_root"))),
        )
        return cls(
            enabled=env_bool("ALERT_NOTIFY_ENABLED", bool(settings_value("notification.enabled"))),
            min_severity=env_str(
                "ALERT_NOTIFY_MIN_SEVERITY", str(settings_value("notification.min_severity"))
            ).lower(),
            cooldown_seconds=env_int(
                "ALERT_NOTIFY_COOLDOWN_SECONDS",
                int(settings_value("notification.cooldown_seconds")),
            ),
            state_path=env_str(
                "ALERT_NOTIFY_STATE_PATH",
                f"{data_root.rstrip('/')}/latest/alert_notify_state.json",
            ),
            openclaw_enabled=env_bool(
                "ALERT_NOTIFY_OPENCLAW_ENABLED",
                bool(settings_value("notification.openclaw_enabled")),
            ),
            openclaw_command=env_str(
                "ALERT_NOTIFY_OPENCLAW_COMMAND",
                str(settings_value("notification.openclaw_command")),
            ),
            openclaw_channel=env_str(
                "ALERT_NOTIFY_OPENCLAW_CHANNEL",
                str(settings_value("notification.openclaw_channel")),
            ),
            openclaw_account=env_str("ALERT_NOTIFY_OPENCLAW_ACCOUNT"),
            openclaw_target=env_str("ALERT_NOTIFY_OPENCLAW_TARGET"),
            openclaw_dry_run=env_bool(
                "ALERT_NOTIFY_OPENCLAW_DRY_RUN",
                bool(settings_value("notification.openclaw_dry_run")),
            ),
            openclaw_timeout_seconds=env_float(
                "ALERT_NOTIFY_OPENCLAW_TIMEOUT_SECONDS",
                float(settings_value("notification.openclaw_timeout_seconds")),
            ),
            openclaw_agent_enabled=env_bool(
                "ALERT_NOTIFY_OPENCLAW_AGENT_ENABLED",
                bool(settings_value("notification.openclaw_agent_enabled")),
            ),
            openclaw_agent_deliver=env_bool(
                "ALERT_NOTIFY_OPENCLAW_AGENT_DELIVER",
                bool(settings_value("notification.openclaw_agent_deliver")),
            ),
            openclaw_agent_name=env_str(
                "ALERT_NOTIFY_OPENCLAW_AGENT_NAME",
                str(settings_value("notification.openclaw_agent_name")),
            ),
            openclaw_agent_model=env_str(
                "ALERT_NOTIFY_OPENCLAW_AGENT_MODEL",
                str(settings_value("notification.openclaw_agent_model")),
            ),
            openclaw_agent_session_key=env_str(
                "ALERT_NOTIFY_OPENCLAW_AGENT_SESSION_KEY",
                str(settings_value("notification.openclaw_agent_session_key")),
            ),
            openclaw_agent_thinking=env_str(
                "ALERT_NOTIFY_OPENCLAW_AGENT_THINKING",
                str(settings_value("notification.openclaw_agent_thinking")),
            ),
            openclaw_agent_timeout_seconds=env_float(
                "ALERT_NOTIFY_OPENCLAW_AGENT_TIMEOUT_SECONDS",
                float(settings_value("notification.openclaw_agent_timeout_seconds")),
            ),
            codex_enabled=env_bool(
                "ALERT_NOTIFY_CODEX_ENABLED",
                bool(settings_value("notification.codex_enabled")),
            ),
            codex_deliver=env_bool(
                "ALERT_NOTIFY_CODEX_DELIVER",
                bool(settings_value("notification.codex_deliver")),
            ),
            codex_command=env_str(
                "ALERT_NOTIFY_CODEX_COMMAND",
                str(settings_value("notification.codex_command")),
            ),
            codex_model=env_str(
                "ALERT_NOTIFY_CODEX_MODEL", str(settings_value("notification.codex_model"))
            ),
            codex_reasoning_effort=env_str(
                "ALERT_NOTIFY_CODEX_REASONING_EFFORT",
                str(settings_value("notification.codex_reasoning_effort")),
            ),
            codex_cwd=env_str(
                "ALERT_NOTIFY_CODEX_CWD", str(settings_value("notification.codex_cwd"))
            ),
            codex_sandbox=env_str(
                "ALERT_NOTIFY_CODEX_SANDBOX", str(settings_value("notification.codex_sandbox"))
            ),
            codex_timeout_seconds=env_float(
                "ALERT_NOTIFY_CODEX_TIMEOUT_SECONDS",
                float(settings_value("notification.codex_timeout_seconds")),
            ),
            codex_output_max_chars=env_int(
                "ALERT_NOTIFY_CODEX_OUTPUT_MAX_CHARS",
                int(settings_value("notification.codex_output_max_chars")),
            ),
            codex_require_delivery_cue=env_bool(
                "ALERT_NOTIFY_CODEX_REQUIRE_DELIVERY_CUE",
                bool(settings_value("notification.codex_require_delivery_cue")),
            ),
            deepseek_enabled=env_bool(
                "ALERT_NOTIFY_DEEPSEEK_ENABLED",
                bool(settings_value("notification.deepseek_enabled")),
            ),
            deepseek_deliver=env_bool(
                "ALERT_NOTIFY_DEEPSEEK_DELIVER",
                bool(settings_value("notification.deepseek_deliver")),
            ),
            deepseek_model=env_str(
                "ALERT_NOTIFY_DEEPSEEK_MODEL",
                str(settings_value("notification.deepseek_model")),
            ),
            deepseek_url=env_str(
                "ALERT_NOTIFY_DEEPSEEK_URL",
                str(settings_value("notification.deepseek_url")),
            ),
            deepseek_env_file=env_str(
                "ALERT_NOTIFY_DEEPSEEK_ENV_FILE",
                str(settings_value("notification.deepseek_env_file")),
            ),
            deepseek_timeout_seconds=env_float(
                "ALERT_NOTIFY_DEEPSEEK_TIMEOUT_SECONDS",
                float(settings_value("notification.deepseek_timeout_seconds")),
            ),
            deepseek_max_tokens=env_int(
                "ALERT_NOTIFY_DEEPSEEK_MAX_TOKENS",
                int(settings_value("notification.deepseek_max_tokens")),
            ),
            deepseek_output_max_chars=env_int(
                "ALERT_NOTIFY_DEEPSEEK_OUTPUT_MAX_CHARS",
                int(settings_value("notification.deepseek_output_max_chars")),
            ),
            deepseek_temperature=env_float(
                "ALERT_NOTIFY_DEEPSEEK_TEMPERATURE",
                float(settings_value("notification.deepseek_temperature")),
            ),
            grok_enabled=env_bool(
                "ALERT_NOTIFY_GROK_ENABLED",
                bool(settings_value("notification.grok_enabled")),
            ),
            grok_deliver=env_bool(
                "ALERT_NOTIFY_GROK_DELIVER",
                bool(settings_value("notification.grok_deliver")),
            ),
            grok_command=env_str(
                "ALERT_NOTIFY_GROK_COMMAND",
                str(settings_value("notification.grok_command")),
            ),
            grok_model=env_str(
                "ALERT_NOTIFY_GROK_MODEL",
                str(settings_value("notification.grok_model")),
            ),
            grok_reasoning_effort=env_str(
                "ALERT_NOTIFY_GROK_REASONING_EFFORT",
                str(settings_value("notification.grok_reasoning_effort")),
            ),
            grok_cwd=env_str(
                "ALERT_NOTIFY_GROK_CWD",
                str(settings_value("notification.grok_cwd")),
            ),
            grok_timeout_seconds=env_float(
                "ALERT_NOTIFY_GROK_TIMEOUT_SECONDS",
                float(settings_value("notification.grok_timeout_seconds")),
            ),
            grok_output_max_chars=env_int(
                "ALERT_NOTIFY_GROK_OUTPUT_MAX_CHARS",
                int(settings_value("notification.grok_output_max_chars")),
            ),
            review_min_time_sensitive_score=env_float(
                "ALERT_REVIEW_MIN_TIME_SENSITIVE_SCORE",
                float(settings_value("notification.review_min_time_sensitive_score")),
            ),
            bark_enabled=env_bool(
                "ALERT_NOTIFY_BARK_ENABLED",
                bool(settings_value("notification.bark_enabled")),
            ),
            bark_url=env_str("ALERT_NOTIFY_BARK_URL").rstrip("/"),
            bark_group=env_str(
                "ALERT_NOTIFY_BARK_GROUP", str(settings_value("notification.bark_group"))
            ),
            bark_ops_group=env_str(
                "ALERT_NOTIFY_BARK_OPS_GROUP", str(settings_value("notification.bark_ops_group"))
            ),
            bark_level=env_str(
                "ALERT_NOTIFY_BARK_LEVEL", str(settings_value("notification.bark_level"))
            ),
            bark_timeout_seconds=env_float(
                "ALERT_NOTIFY_BARK_TIMEOUT_SECONDS",
                float(settings_value("notification.bark_timeout_seconds")),
            ),
            bark_markdown_enabled=env_bool(
                "ALERT_NOTIFY_BARK_MARKDOWN_ENABLED",
                bool(settings_value("notification.bark_markdown_enabled")),
            ),
            bark_friend_enabled=env_bool(
                "ALERT_NOTIFY_BARK_FRIEND_ENABLED",
                bool(settings_value("notification.bark_friend_enabled")),
            ),
            bark_friend_url=env_str("ALERT_NOTIFY_BARK_FRIEND_URL").rstrip("/"),
            feishu_enabled=env_bool(
                "ALERT_NOTIFY_FEISHU_ENABLED",
                bool(settings_value("notification.feishu_enabled")),
            ),
            feishu_webhook_url=env_str("ALERT_NOTIFY_FEISHU_WEBHOOK_URL").rstrip("/"),
            feishu_secret=env_str("ALERT_NOTIFY_FEISHU_SECRET"),
            feishu_timeout_seconds=env_float(
                "ALERT_NOTIFY_FEISHU_TIMEOUT_SECONDS",
                float(settings_value("notification.feishu_timeout_seconds")),
            ),
            direct_push_llm_enabled=env_bool(
                "ALERT_NOTIFY_DIRECT_PUSH_LLM_ENABLED",
                bool(settings_value("notification.direct_push_llm_enabled")),
            ),
            kind_rate_limit_seconds=env_float(
                "ALERT_NOTIFY_KIND_RATE_LIMIT_SECONDS",
                float(settings_value("notification.kind_rate_limit_seconds")),
            ),
            missed_queue_path=env_str(
                "ALERT_NOTIFY_MISSED_QUEUE_PATH",
                f"{data_root.rstrip('/')}/latest/weixin_missed_queue.jsonl",
            ),
            delivery_receipt_path=env_str(
                "ALERT_NOTIFY_DELIVERY_RECEIPT_PATH",
                f"{data_root.rstrip('/')}/ledger/notification_delivery.sqlite",
            ),
            outbox_max_attempts=env_int(
                "ALERT_NOTIFY_OUTBOX_MAX_ATTEMPTS",
                int(settings_value("notification.outbox_max_attempts")),
            ),
            outbox_retry_base_seconds=env_float(
                "ALERT_NOTIFY_OUTBOX_RETRY_BASE_SECONDS",
                float(settings_value("notification.outbox_retry_base_seconds")),
            ),
            outbox_retry_max_seconds=env_float(
                "ALERT_NOTIFY_OUTBOX_RETRY_MAX_SECONDS",
                float(settings_value("notification.outbox_retry_max_seconds")),
            ),
            outbox_claim_stale_after_seconds=env_float(
                "ALERT_NOTIFY_OUTBOX_CLAIM_STALE_AFTER_SECONDS",
                float(settings_value("notification.outbox_claim_stale_after_seconds")),
            ),
            review_audit_path=env_str(
                "ALERT_NOTIFY_REVIEW_AUDIT_PATH",
                f"{data_root.rstrip('/')}/latest/alert_review_audit.jsonl",
            ),
        )


def resolve_shock_notify_enabled(
    *,
    no_notify: bool = False,
    settings: NotificationSettings | None = None,
) -> bool:
    """Whether intraday_shock should call ``notify_payload`` on this cycle.

    Shock stays on the latency-critical direct path by default
    (``shock_direct_delivery_enabled``). That flag is independent of
    ``direct_delivery_enabled`` / outbox ownership for periodic alert_engine
    candidates, so cutting over alert_engine to outbox does not silence shock
    or create a second owner for the same shock events.
    """

    if no_notify:
        return False
    notification_settings = settings or NotificationSettings.from_env()
    if not notification_settings.enabled:
        return False
    return shock_direct_delivery_enabled()


@dataclass(frozen=True)
class SamplingSettings:
    strike_step: int
    window_points: int
    hot_window_points: int
    group_count: int
    group_interval_seconds: int
    degraded_group_count: int
    degraded_group_interval_seconds: int
    group_strategy: str
    hot_human_cadence_seconds: int
    hot_execution_cadence_seconds: int
    include_next_expiry: bool
    default_mode: str
    # The next (1DTE) expiry only needs ATM-vicinity lines for term structure
    # and next-day expected move; walls/GEX need the wide window on 0DTE only.
    next_expiry_window_points: int = field(default_factory=lambda: 30)
    next_expiry_hot_window_points: int = field(default_factory=lambda: 10)

    @classmethod
    def from_env(cls) -> "SamplingSettings":
        load_dotenv()
        return cls(
            strike_step=env_int(
                "SAMPLING_STRIKE_STEP", int(settings_value("sampling.strike_step"))
            ),
            window_points=env_int(
                "SAMPLING_WINDOW_POINTS", int(settings_value("sampling.window_points"))
            ),
            hot_window_points=env_int(
                "SAMPLING_HOT_WINDOW_POINTS", int(settings_value("sampling.hot_window_points"))
            ),
            group_count=env_int(
                "SAMPLING_GROUP_COUNT", int(settings_value("sampling.group_count"))
            ),
            group_interval_seconds=env_int(
                "SAMPLING_GROUP_INTERVAL_SECONDS",
                int(settings_value("sampling.group_interval_seconds")),
            ),
            degraded_group_count=env_int(
                "SAMPLING_DEGRADED_GROUP_COUNT",
                int(settings_value("sampling.degraded_group_count")),
            ),
            degraded_group_interval_seconds=env_int(
                "SAMPLING_DEGRADED_GROUP_INTERVAL_SECONDS",
                int(settings_value("sampling.degraded_group_interval_seconds")),
            ),
            group_strategy=env_str(
                "SAMPLING_GROUP_STRATEGY", str(settings_value("sampling.group_strategy"))
            ).lower(),
            hot_human_cadence_seconds=env_int(
                "SAMPLING_HOT_HUMAN_CADENCE_SECONDS",
                int(settings_value("sampling.hot_human_cadence_seconds")),
            ),
            hot_execution_cadence_seconds=env_int(
                "SAMPLING_HOT_EXECUTION_CADENCE_SECONDS",
                int(settings_value("sampling.hot_execution_cadence_seconds")),
            ),
            include_next_expiry=env_bool(
                "SAMPLING_INCLUDE_NEXT_EXPIRY",
                bool(settings_value("sampling.include_next_expiry")),
            ),
            default_mode=env_str(
                "SAMPLING_DEFAULT_MODE", str(settings_value("sampling.default_mode"))
            ),
            next_expiry_window_points=env_int(
                "SAMPLING_NEXT_EXPIRY_WINDOW_POINTS",
                int(settings_value("sampling.next_expiry_window_points")),
            ),
            next_expiry_hot_window_points=env_int(
                "SAMPLING_NEXT_EXPIRY_HOT_WINDOW_POINTS",
                int(settings_value("sampling.next_expiry_hot_window_points")),
            ),
        )


from spx_spark import config_ibkr as _config_ibkr  # noqa: E402

IbkrSettings = _config_ibkr.IbkrSettings
IbkrStreamSettings = _config_ibkr.IbkrStreamSettings
IbkrPositionSettings = _config_ibkr.IbkrPositionSettings
IbkrBrokerSettings = _config_ibkr.IbkrBrokerSettings
ibkr_account_read_enabled = _config_ibkr.ibkr_account_read_enabled
ibkr_legacy_position_poller_enabled = _config_ibkr.ibkr_legacy_position_poller_enabled
RuntimePolicySettings = _config_ibkr.RuntimePolicySettings

from spx_spark import config_providers as _config_providers  # noqa: E402

HyperliquidSettings = _config_providers.HyperliquidSettings
PolymarketSettings = _config_providers.PolymarketSettings
MaintenanceSettings = _config_providers.MaintenanceSettings
