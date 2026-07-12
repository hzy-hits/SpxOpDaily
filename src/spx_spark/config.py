from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from spx_spark.market_calendar import ET as NY_TZ
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, default_spxw_expiry
from spx_spark.settings import settings_csv, settings_value
from spx_spark.runtime_config import (
    runtime_schwab_option_chain_underliers,
    runtime_schwab_symbols_by_type,
)

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
class IbkrSettings:
    host: str
    port: int
    client_id: int
    market_data_type: int
    es_expiry: str
    mes_expiry: str
    verify_indexes: list[str]
    verify_stocks: list[str]
    verify_futures: list[str]
    option_expiry: str
    option_strike_window_points: int
    option_strike_step: int
    max_option_lines: int
    quote_wait_seconds: float
    stale_after_seconds: float
    qualify_contracts: bool
    request_timeout_seconds: float
    # Slow-updating indices (SKEW/VVIX tick every few minutes) use a longer
    # stale threshold so they do not flap between live and stale.
    slow_index_stale_after_seconds: float = field(
        default_factory=lambda: 300.0
    )
    slow_index_labels: frozenset[str] = field(
        default_factory=lambda: frozenset(
            str(item) for item in ['index:SKEW', 'index:VVIX']
        )
    )
    # IBKR index CFDs such as IBUS500 (S&P 500 CFD). Defaults empty in code so
    # existing callers are unaffected; from_env defaults to IBUS500.
    verify_cfds: list[str] = field(default_factory=list)

    @classmethod
    def from_env(cls) -> "IbkrSettings":
        load_dotenv()
        auto_futures_expiry = next_equity_futures_month()
        return cls(
            host=env_str("IBKR_HOST", str(settings_value("ibkr.host"))),
            port=env_int("IBKR_PORT", int(settings_value("ibkr.port"))),
            client_id=env_int("IBKR_CLIENT_ID", int(settings_value("ibkr.client_id"))),
            market_data_type=env_int(
                "IBKR_MARKET_DATA_TYPE", int(settings_value("ibkr.market_data_type"))
            ),
            es_expiry=env_str("IBKR_ES_EXPIRY", auto_futures_expiry) or auto_futures_expiry,
            mes_expiry=env_str("IBKR_MES_EXPIRY", auto_futures_expiry) or auto_futures_expiry,
            verify_indexes=env_csv(
                "IBKR_VERIFY_INDEXES", settings_csv("ibkr.verify_indexes"),
            ),
            verify_stocks=env_csv(
                "IBKR_VERIFY_STOCKS", settings_csv("ibkr.verify_stocks"),
            ),
            verify_futures=env_csv("IBKR_VERIFY_FUTURES", settings_csv("ibkr.verify_futures")),
            verify_cfds=env_csv("IBKR_VERIFY_CFDS", settings_csv("ibkr.verify_cfds")),
            option_expiry=env_str("IBKR_OPTION_EXPIRY", default_spxw_expiry())
            or default_spxw_expiry(),
            option_strike_window_points=env_int(
                "IBKR_OPTION_STRIKE_WINDOW_POINTS",
                int(settings_value("ibkr.option_strike_window_points")),
            ),
            option_strike_step=env_int(
                "IBKR_OPTION_STRIKE_STEP", int(settings_value("ibkr.option_strike_step"))
            ),
            max_option_lines=env_int(
                "IBKR_MAX_OPTION_LINES", int(settings_value("ibkr.max_option_lines"))
            ),
            quote_wait_seconds=env_float(
                "IBKR_QUOTE_WAIT_SECONDS", float(settings_value("ibkr.quote_wait_seconds"))
            ),
            stale_after_seconds=env_float(
                "IBKR_STALE_AFTER_SECONDS", float(settings_value("ibkr.stale_after_seconds"))
            ),
            slow_index_stale_after_seconds=env_float(
                "IBKR_SLOW_INDEX_STALE_AFTER_SECONDS",
                float(settings_value("ibkr.slow_index_stale_after_seconds")),
            ),
            # Preserve case: these must match row labels like "index:SKEW".
            slow_index_labels=frozenset(
                env_csv_preserve("IBKR_SLOW_INDEX_LABELS", settings_csv("ibkr.slow_index_labels"))
            ),
            qualify_contracts=env_bool(
                "IBKR_QUALIFY_CONTRACTS", bool(settings_value("ibkr.qualify_contracts"))
            ),
            request_timeout_seconds=env_float(
                "IBKR_REQUEST_TIMEOUT_SECONDS",
                float(settings_value("ibkr.request_timeout_seconds")),
            ),
        )


@dataclass(frozen=True)
class IbkrStreamSettings:
    """Settings for the persistent streaming IBKR collector."""

    client_id: int
    flush_interval_seconds: float
    policy_check_seconds: float
    replan_drift_points: float
    max_option_lines: int
    hot_lane_share: float
    reconnect_min_seconds: float
    reconnect_max_seconds: float
    skip_options: bool
    farm_broken_restart_seconds: float
    gateway_restart_cooldown_seconds: float
    auto_restart_gateway_on_farm_broken: bool
    spy_option_lines: int = field(
        default_factory=lambda: 16
    )
    spy_strike_step: int = field(
        default_factory=lambda: 2
    )
    slow_poll_labels: tuple[str, ...] = field(
        default_factory=lambda: tuple(
            str(item) for item in ['index:VIX', 'index:VIX1D', 'index:VIX9D', 'index:VIX3M', 'index:VVIX', 'index:SKEW', 'stock:QQQ', 'stock:IWM', 'stock:DIA', 'stock:HYG', 'stock:LQD', 'stock:TLT', 'stock:IEF', 'stock:SHY', 'stock:UUP', 'stock:GLD', 'stock:USO', 'stock:RSP', 'stock:XLU']
        )
    )
    slow_poll_interval_seconds: float = field(
        default_factory=lambda: 300.0
    )
    slow_poll_hold_seconds: float = field(
        default_factory=lambda: 10.0
    )
    slow_poll_chunk_size: int = field(
        default_factory=lambda: 6
    )
    atm_state_path: str = field(
        default_factory=lambda: ''
    )
    freeze_quotes_on_connectivity_loss: bool = field(
        default_factory=lambda: bool(
            True
        )
    )

    @classmethod
    def from_env(cls) -> "IbkrStreamSettings":
        load_dotenv()
        return cls(
            # Distinct from the snapshot collector's client id so an accidental
            # overlap does not kick the other API session.
            client_id=env_int("IBKR_STREAM_CLIENT_ID", int(settings_value("ibkr_stream.client_id"))),
            flush_interval_seconds=env_float(
                "IBKR_STREAM_FLUSH_SECONDS",
                float(settings_value("ibkr_stream.flush_interval_seconds")),
            ),
            policy_check_seconds=env_float(
                "IBKR_STREAM_POLICY_CHECK_SECONDS",
                float(settings_value("ibkr_stream.policy_check_seconds")),
            ),
            replan_drift_points=env_float(
                "IBKR_STREAM_REPLAN_DRIFT_POINTS",
                float(settings_value("ibkr_stream.replan_drift_points")),
            ),
            max_option_lines=env_int(
                "IBKR_STREAM_MAX_OPTION_LINES",
                int(settings_value("ibkr_stream.max_option_lines")),
            ),
            hot_lane_share=env_float(
                "IBKR_STREAM_HOT_LANE_SHARE", float(settings_value("ibkr_stream.hot_lane_share"))
            ),
            reconnect_min_seconds=env_float(
                "IBKR_STREAM_RECONNECT_MIN_SECONDS",
                float(settings_value("ibkr_stream.reconnect_min_seconds")),
            ),
            reconnect_max_seconds=env_float(
                "IBKR_STREAM_RECONNECT_MAX_SECONDS",
                float(settings_value("ibkr_stream.reconnect_max_seconds")),
            ),
            skip_options=env_bool(
                "IBKR_STREAM_SKIP_OPTIONS", bool(settings_value("ibkr_stream.skip_options"))
            ),
            farm_broken_restart_seconds=env_float(
                "IBKR_FARM_BROKEN_RESTART_SECONDS",
                float(settings_value("ibkr_stream.farm_broken_restart_seconds")),
            ),
            gateway_restart_cooldown_seconds=env_float(
                "IBKR_GATEWAY_RESTART_COOLDOWN_SECONDS",
                float(settings_value("ibkr_stream.gateway_restart_cooldown_seconds")),
            ),
            auto_restart_gateway_on_farm_broken=env_bool(
                "IBKR_AUTO_RESTART_GATEWAY_ON_FARM_BROKEN",
                bool(settings_value("ibkr_stream.auto_restart_gateway_on_farm_broken")),
            ),
            spy_option_lines=env_int(
                "IBKR_STREAM_SPY_OPTION_LINES",
                int(settings_value("ibkr_stream.spy_option_lines")),
            ),
            spy_strike_step=env_int(
                "IBKR_STREAM_SPY_STRIKE_STEP",
                int(settings_value("ibkr_stream.spy_strike_step")),
            ),
            slow_poll_labels=tuple(
                env_csv_preserve(
                    "IBKR_STREAM_SLOW_POLL_LABELS", settings_csv("ibkr_stream.slow_poll_labels"),
                )
            ),
            slow_poll_interval_seconds=env_float(
                "IBKR_STREAM_SLOW_POLL_INTERVAL_SECONDS",
                float(settings_value("ibkr_stream.slow_poll_interval_seconds")),
            ),
            slow_poll_hold_seconds=env_float(
                "IBKR_STREAM_SLOW_POLL_HOLD_SECONDS",
                float(settings_value("ibkr_stream.slow_poll_hold_seconds")),
            ),
            slow_poll_chunk_size=env_int(
                "IBKR_STREAM_SLOW_POLL_CHUNK_SIZE",
                int(settings_value("ibkr_stream.slow_poll_chunk_size")),
            ),
            atm_state_path=env_str("IBKR_ATM_STATE_PATH", str(settings_value("ibkr_stream.atm_state_path"))),
            freeze_quotes_on_connectivity_loss=env_bool(
                "IBKR_STREAM_FREEZE_QUOTES_ON_CONNECTIVITY_LOSS",
                bool(settings_value("ibkr_stream.freeze_quotes_on_connectivity_loss")),
            ),
        )


@dataclass(frozen=True)
class IbkrPositionSettings:
    enabled: bool
    client_id: int
    poll_interval_seconds: int
    snapshot_path: str | None
    state_path: str
    max_snapshot_age_seconds: float

    @classmethod
    def from_env(cls) -> "IbkrPositionSettings":
        load_dotenv()
        data_root = env_str(
            "MARKET_DATA_DATA_ROOT",
            env_str("MAINTENANCE_DATA_ROOT", str(settings_value("maintenance.data_root"))),
        )
        default_snapshot = f"{data_root.rstrip('/')}/latest/ibkr_positions.json"
        default_state = f"{data_root.rstrip('/')}/latest/ibkr_position_state.json"
        snapshot_path = env_str("IBKR_POSITIONS_SNAPSHOT_PATH", default_snapshot) or default_snapshot
        state_path = env_str("IBKR_POSITIONS_STATE_PATH", default_state) or default_state
        poll_interval_seconds = env_int(
            "IBKR_POSITIONS_POLL_SECONDS",
            int(settings_value("ibkr_positions.poll_interval_seconds")),
        )
        return cls(
            enabled=ibkr_account_read_enabled(),
            client_id=env_int(
                "IBKR_POSITIONS_CLIENT_ID", int(settings_value("ibkr_positions.client_id"))
            ),
            poll_interval_seconds=poll_interval_seconds,
            snapshot_path=snapshot_path,
            state_path=state_path,
            max_snapshot_age_seconds=env_float(
                "IBKR_POSITIONS_MAX_SNAPSHOT_AGE_SECONDS",
                float(max(3 * poll_interval_seconds, 180)),
            ),
        )


@dataclass(frozen=True)
class IbkrBrokerSettings:
    account_read_enabled: bool
    position_shadow_enabled: bool
    position_shadow_interval_seconds: int
    position_shadow_path: str
    execution_mode: str

    def __post_init__(self) -> None:
        if self.position_shadow_interval_seconds <= 0:
            raise ValueError("IBKR_BROKER_POSITION_SHADOW_SECONDS must be positive")
        if self.execution_mode not in {"manual", "shadow", "live"}:
            raise ValueError("IBKR_EXECUTION_MODE must be manual, shadow, or live")
        if self.execution_mode == "live" and not self.account_read_enabled:
            raise ValueError(
                "IBKR_EXECUTION_MODE=live requires IBKR_BROKER_ACCOUNT_READ_ENABLED=true"
            )

    @property
    def position_shadow_active(self) -> bool:
        return self.account_read_enabled and self.position_shadow_enabled

    @classmethod
    def from_env(cls) -> "IbkrBrokerSettings":
        load_dotenv()
        data_root = env_str(
            "MARKET_DATA_DATA_ROOT",
            env_str("MAINTENANCE_DATA_ROOT", str(settings_value("maintenance.data_root"))),
        )
        configured_path = str(settings_value("ibkr_broker.position_shadow_path")).strip()
        return cls(
            account_read_enabled=ibkr_account_read_enabled(),
            position_shadow_enabled=env_bool(
                "IBKR_BROKER_POSITION_SHADOW_ENABLED",
                bool(settings_value("ibkr_broker.position_shadow_enabled")),
            ),
            position_shadow_interval_seconds=env_int(
                "IBKR_BROKER_POSITION_SHADOW_SECONDS",
                int(settings_value("ibkr_broker.position_shadow_interval_seconds")),
            ),
            position_shadow_path=os.getenv("IBKR_BROKER_POSITION_SHADOW_PATH")
            or configured_path
            or f"{data_root.rstrip('/')}/latest/ibkr_positions_shadow.json",
            execution_mode=env_str(
                "IBKR_EXECUTION_MODE",
                str(settings_value("ibkr_broker.execution_mode")),
            ).lower(),
        )


def ibkr_account_read_enabled() -> bool:
    load_dotenv()
    return env_bool(
        "IBKR_BROKER_ACCOUNT_READ_ENABLED",
        env_bool(
            "IBKR_POSITIONS_ENABLED",
            bool(settings_value("ibkr_broker.account_read_enabled")),
        ),
    )


def ibkr_legacy_position_poller_enabled() -> bool:
    load_dotenv()
    return env_bool(
        "IBKR_LEGACY_POSITION_POLLER_ENABLED",
        env_bool(
            "IBKR_POSITIONS_ENABLED",
            bool(settings_value("ibkr_broker.legacy_position_poller_enabled")),
        ),
    )


@dataclass(frozen=True)
class RuntimePolicySettings:
    ibkr_schedule_enabled: bool
    ibkr_schedule_timezone: str
    ibkr_schedule_start: time
    ibkr_schedule_stop: time
    ibkr_connect_retry_seconds: int
    ibkr_conflict_retry_minutes: int
    ibkr_conflict_probe_seconds: int
    ibkr_fallback_provider: str
    strict_no_session_fight: bool
    weekend_maintenance_mode: bool
    runtime_mode_path: str
    agent_override_default_ttl_minutes: int

    @classmethod
    def from_env(cls) -> "RuntimePolicySettings":
        load_dotenv()
        return cls(
            ibkr_schedule_enabled=env_bool(
                "IBKR_SCHEDULE_ENABLED", bool(settings_value("runtime_policy.ibkr_schedule_enabled"))
            ),
            ibkr_schedule_timezone=env_str(
                "IBKR_SCHEDULE_TZ", str(settings_value("runtime_policy.ibkr_schedule_timezone"))
            ),
            ibkr_schedule_start=parse_hhmm(
                env_str("IBKR_SCHEDULE_START", str(settings_value("runtime_policy.ibkr_schedule_start")))
            ),
            ibkr_schedule_stop=parse_hhmm(
                env_str("IBKR_SCHEDULE_STOP", str(settings_value("runtime_policy.ibkr_schedule_stop")))
            ),
            ibkr_connect_retry_seconds=env_int(
                "IBKR_CONNECT_RETRY_SECONDS",
                int(settings_value("runtime_policy.ibkr_connect_retry_seconds")),
            ),
            ibkr_conflict_retry_minutes=env_int(
                "IBKR_CONFLICT_RETRY_MINUTES",
                int(settings_value("runtime_policy.ibkr_conflict_retry_minutes")),
            ),
            ibkr_conflict_probe_seconds=env_int(
                "IBKR_CONFLICT_PROBE_SECONDS",
                int(settings_value("runtime_policy.ibkr_conflict_probe_seconds")),
            ),
            ibkr_fallback_provider=env_str(
                "IBKR_FALLBACK_PROVIDER", str(settings_value("runtime_policy.ibkr_fallback_provider"))
            ).lower(),
            strict_no_session_fight=env_bool(
                "STRICT_NO_SESSION_FIGHT", bool(settings_value("runtime_policy.strict_no_session_fight"))
            ),
            weekend_maintenance_mode=env_bool(
                "WEEKEND_MAINTENANCE_MODE",
                bool(settings_value("runtime_policy.weekend_maintenance_mode")),
            ),
            runtime_mode_path=env_str(
                "RUNTIME_MODE_PATH", str(settings_value("runtime_policy.runtime_mode_path"))
            ),
            agent_override_default_ttl_minutes=env_int(
                "AGENT_OVERRIDE_DEFAULT_TTL_MINUTES",
                int(settings_value("runtime_policy.agent_override_default_ttl_minutes")),
            ),
        )

    def ibkr_window_is_open(self, now: datetime | None = None) -> bool:
        if not self.ibkr_schedule_enabled:
            return False
        timezone = ZoneInfo(self.ibkr_schedule_timezone)
        if now is None:
            now = datetime.now(tz=timezone)
        elif now.tzinfo is None:
            now = now.replace(tzinfo=timezone)
        else:
            now = now.astimezone(timezone)
        return is_time_in_window(now.time(), self.ibkr_schedule_start, self.ibkr_schedule_stop)

    def market_data_collection_allowed(self, now: datetime | None = None) -> bool:
        timezone = ZoneInfo(self.ibkr_schedule_timezone)
        if now is None:
            now = datetime.now(tz=timezone)
        elif now.tzinfo is None:
            now = now.replace(tzinfo=timezone)
        else:
            now = now.astimezone(timezone)
        if self.weekend_maintenance_mode:
            now_et = now.astimezone(NY_TZ)
            calendar_day = now_et.date()
            if not DEFAULT_MARKET_CALENDAR.is_trading_day(calendar_day):
                next_wall_day = calendar_day + timedelta(days=1)
                futures_reopen = (
                    now_et.time() >= time(18, 0)
                    and DEFAULT_MARKET_CALENDAR.is_trading_day(next_wall_day)
                )
                if not futures_reopen:
                    return False
        return self.ibkr_window_is_open(now)

    @property
    def should_retry_after_conflict(self) -> bool:
        return not self.strict_no_session_fight and self.ibkr_conflict_retry_minutes > 0

    @property
    def should_probe_after_conflict(self) -> bool:
        return self.ibkr_conflict_probe_seconds > 0


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
    callback_url: str = field(
        default_factory=lambda: 'https://127.0.0.1:8182'
    )
    oauth_bind_host: str = field(
        default_factory=lambda: '127.0.0.1'
    )
    oauth_bind_port: int = field(
        default_factory=lambda: 8183
    )
    oauth_state_file: str = field(
        default_factory=lambda: 'runtime/schwab-oauth-state.json'
    )
    oauth_state_ttl_seconds: int = field(
        default_factory=lambda: 900
    )
    gateway_bind_host: str = field(
        default_factory=lambda: '127.0.0.1'
    )
    gateway_bind_port: int = field(
        default_factory=lambda: 8184
    )
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

    def __post_init__(self) -> None:
        if self.mode not in {"off", "shadow", "live"}:
            raise ValueError("SCHWAB_STREAM_MODE must be off, shadow, or live")
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

    @classmethod
    def from_env(cls, *, data_root: str | None = None) -> "SchwabStreamSettings":
        load_dotenv()
        root = data_root or env_str(
            "MARKET_DATA_DATA_ROOT",
            env_str("MAINTENANCE_DATA_ROOT", str(settings_value("maintenance.data_root"))),
        )
        configured_shadow_path = str(
            settings_value("schwab.streaming.shadow_latest_path")
        ).strip()
        return cls(
            mode=env_str(
                "SCHWAB_STREAM_MODE",
                str(settings_value("schwab.streaming.mode")),
            ).lower(),
            canonical_symbols=tuple(
                symbol.upper()
                for symbol in env_csv_preserve(
                    "SCHWAB_STREAM_SYMBOLS", settings_csv("schwab.streaming.canonical_symbols"),
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
        )


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
                "POLYMARKET_SEARCH_TERMS", settings_csv("polymarket.search_terms"),
            ),
            event_slugs=env_csv_preserve("POLYMARKET_EVENT_SLUGS", settings_csv("polymarket.event_slugs")),
            market_slugs=env_csv_preserve("POLYMARKET_MARKET_SLUGS", settings_csv("polymarket.market_slugs")),
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
            data_root=env_str("MAINTENANCE_DATA_ROOT", str(settings_value("maintenance.data_root"))),
            logs_root=env_str("MAINTENANCE_LOGS_ROOT", str(settings_value("maintenance.logs_root"))),
            output_root=env_str("MAINTENANCE_OUTPUT_ROOT", str(settings_value("maintenance.output_root"))),
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
            warn_pct=env_float("MAINTENANCE_WARN_PCT", float(settings_value("maintenance.warn_pct"))),
            compact_pct=env_float(
                "MAINTENANCE_COMPACT_PCT", float(settings_value("maintenance.compact_pct"))
            ),
            degraded_pct=env_float(
                "MAINTENANCE_DEGRADED_PCT", float(settings_value("maintenance.degraded_pct"))
            ),
            prune_pct=env_float("MAINTENANCE_PRUNE_PCT", float(settings_value("maintenance.prune_pct"))),
            critical_pct=env_float(
                "MAINTENANCE_CRITICAL_PCT", float(settings_value("maintenance.critical_pct"))
            ),
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
    delayed_stale_after_seconds: float = field(
        default_factory=lambda: 60
    )
    provider_priority: tuple[str, ...] = field(
        default_factory=lambda: tuple(
            str(item).lower() for item in ['schwab', 'ibkr', 'hyperliquid', 'polymarket', 'internal', 'mock', 'unknown']
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
                "MARKET_DATA_PROVIDER_PRIORITY contains unsupported providers: "
                + ",".join(invalid)
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
                    "MARKET_DATA_SLOW_INDEX_LABELS", settings_csv("market_data.slow_index_labels"),
                )
            ),
            delayed_stale_after_seconds=env_float(
                "MARKET_DATA_DELAYED_STALE_AFTER_SECONDS",
                float(settings_value("market_data.delayed_stale_after_seconds")),
            ),
            provider_priority=tuple(
                provider.lower()
                for provider in env_csv_preserve(
                    "MARKET_DATA_PROVIDER_PRIORITY", settings_csv("market_data.provider_priority"),
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
    deepseek_enabled: bool = field(
        default_factory=lambda: bool(
            False
        )
    )
    deepseek_deliver: bool = field(
        default_factory=lambda: True
    )
    deepseek_model: str = field(
        default_factory=lambda: 'deepseek-v4-flash'
    )
    deepseek_url: str = field(
        default_factory=lambda: 'https://api.deepseek.com/v1/chat/completions'
    )
    deepseek_env_file: str = field(
        default_factory=lambda: '/home/ubuntu/.hermes/.env'
    )
    deepseek_timeout_seconds: float = field(
        default_factory=lambda: 30.0
    )
    deepseek_max_tokens: int = field(
        default_factory=lambda: 6400
    )
    deepseek_output_max_chars: int = field(
        default_factory=lambda: 6400
    )
    deepseek_temperature: float = field(
        default_factory=lambda: 0.1
    )
    review_min_time_sensitive_score: float = field(
        default_factory=lambda: 30.0
    )
    bark_enabled: bool = field(
        default_factory=lambda: False
    )
    bark_url: str = ""
    bark_group: str = field(default_factory=lambda: 'spx-spark')
    # Ops/engineering pushes (IBKR session, data degradation, channel failures)
    # land in this Bark group so the trade group stays readable.
    bark_ops_group: str = field(
        default_factory=lambda: 'spx-ops'
    )
    bark_level: str = field(default_factory=lambda: 'timeSensitive')
    bark_timeout_seconds: float = field(
        default_factory=lambda: 10.0
    )
    # When true, trading pushes also send the full markdown into Bark's App
    # detail view (lockscreen still uses the short body summary).
    bark_markdown_enabled: bool = field(
        default_factory=lambda: True
    )
    # Friend channel: trading content only (maps/status/review/market alerts),
    # never engineering noise (data degradation, session drops, token expiry).
    bark_friend_enabled: bool = field(
        default_factory=lambda: False
    )
    bark_friend_url: str = ""
    # Feishu custom-bot webhook: trading reading surface (interactive cards).
    # Ops stay on Bark main; leave disabled until webhook URL is configured.
    feishu_enabled: bool = field(
        default_factory=lambda: False
    )
    feishu_webhook_url: str = ""
    feishu_secret: str = ""
    feishu_timeout_seconds: float = field(
        default_factory=lambda: 10.0
    )
    # Rewrite direct-push events (position/system/off-hours vol) with the
    # DeepSeek writer before sending; falls back to the raw template on any
    # writer failure so critical events are never lost.
    direct_push_llm_enabled: bool = field(
        default_factory=lambda: bool(
            False
        )
    )
    # Kind-level rate limit for magnitude-bucketed alerts: the per-bucket
    # dedup key lets a drifting value re-alert on every bucket step (observed
    # put_skew up:1 -> up:28 = 19 pushes/day), so the same kind+instrument is
    # capped to one push per this window unless the bucket jumps >= 2 steps,
    # the direction flips, or severity is critical.
    kind_rate_limit_seconds: float = field(
        default_factory=lambda: 3600.0
    )
    missed_queue_path: str = ""
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
            codex_cwd=env_str("ALERT_NOTIFY_CODEX_CWD", str(settings_value("notification.codex_cwd"))),
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
    next_expiry_window_points: int = field(
        default_factory=lambda: 30
    )
    next_expiry_hot_window_points: int = field(
        default_factory=lambda: 10
    )

    @classmethod
    def from_env(cls) -> "SamplingSettings":
        load_dotenv()
        return cls(
            strike_step=env_int("SAMPLING_STRIKE_STEP", int(settings_value("sampling.strike_step"))),
            window_points=env_int(
                "SAMPLING_WINDOW_POINTS", int(settings_value("sampling.window_points"))
            ),
            hot_window_points=env_int(
                "SAMPLING_HOT_WINDOW_POINTS", int(settings_value("sampling.hot_window_points"))
            ),
            group_count=env_int("SAMPLING_GROUP_COUNT", int(settings_value("sampling.group_count"))),
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
