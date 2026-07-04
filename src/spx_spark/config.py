from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo


NY_TZ = ZoneInfo("America/New_York")


def _env(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip()


def _env_int(name: str, default: int) -> int:
    raw = _env(name)
    if not raw:
        return default
    return int(raw)


def _env_float(name: str, default: float) -> float:
    raw = _env(name)
    if not raw:
        return default
    return float(raw)


def _env_bool(name: str, default: bool) -> bool:
    raw = _env(name)
    if not raw:
        return default
    normalized = raw.lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean-like value, got {raw!r}")


def _env_csv(name: str, default: str) -> list[str]:
    raw = _env(name, default)
    return [part.strip().upper() for part in raw.split(",") if part.strip()]


def load_dotenv(path: str = ".env") -> None:
    """Load a small .env file without requiring python-dotenv."""
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


def default_spxw_expiry(today: date | None = None) -> str:
    """Return today in NY time, or the next weekday when today is a weekend."""
    if today is None:
        today = datetime.now(tz=NY_TZ).date()
    while today.weekday() >= 5:
        today += timedelta(days=1)
    return today.strftime("%Y%m%d")


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

    @classmethod
    def from_env(cls) -> "IbkrSettings":
        load_dotenv()
        auto_futures_expiry = next_equity_futures_month()
        return cls(
            host=_env("IBKR_HOST", "127.0.0.1"),
            port=_env_int("IBKR_PORT", 4002),
            client_id=_env_int("IBKR_CLIENT_ID", 171),
            market_data_type=_env_int("IBKR_MARKET_DATA_TYPE", 1),
            es_expiry=_env("IBKR_ES_EXPIRY", auto_futures_expiry) or auto_futures_expiry,
            mes_expiry=_env("IBKR_MES_EXPIRY", auto_futures_expiry) or auto_futures_expiry,
            verify_indexes=_env_csv("IBKR_VERIFY_INDEXES", "SPX,VIX,VIX1D,VIX9D,VIX3M,VVIX,SKEW"),
            verify_stocks=_env_csv(
                "IBKR_VERIFY_STOCKS",
                "SPY,QQQ,IWM,HYG,LQD,TLT,IEF,SHY,UUP,GLD,USO",
            ),
            verify_futures=_env_csv("IBKR_VERIFY_FUTURES", "ES,MES"),
            option_expiry=_env("IBKR_OPTION_EXPIRY", default_spxw_expiry())
            or default_spxw_expiry(),
            option_strike_window_points=_env_int("IBKR_OPTION_STRIKE_WINDOW_POINTS", 50),
            option_strike_step=_env_int("IBKR_OPTION_STRIKE_STEP", 5),
            max_option_lines=_env_int("IBKR_MAX_OPTION_LINES", 40),
            quote_wait_seconds=_env_float("IBKR_QUOTE_WAIT_SECONDS", 8.0),
            stale_after_seconds=_env_float("IBKR_STALE_AFTER_SECONDS", 10.0),
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
            ibkr_schedule_enabled=_env_bool("IBKR_SCHEDULE_ENABLED", True),
            ibkr_schedule_timezone=_env("IBKR_SCHEDULE_TZ", "Asia/Shanghai"),
            ibkr_schedule_start=parse_hhmm(_env("IBKR_SCHEDULE_START", "00:00")),
            ibkr_schedule_stop=parse_hhmm(_env("IBKR_SCHEDULE_STOP", "00:00")),
            ibkr_connect_retry_seconds=_env_int("IBKR_CONNECT_RETRY_SECONDS", 300),
            ibkr_conflict_retry_minutes=_env_int("IBKR_CONFLICT_RETRY_MINUTES", 0),
            ibkr_conflict_probe_seconds=_env_int("IBKR_CONFLICT_PROBE_SECONDS", 300),
            ibkr_fallback_provider=_env("IBKR_FALLBACK_PROVIDER", "schwab").lower(),
            strict_no_session_fight=_env_bool("STRICT_NO_SESSION_FIGHT", True),
            weekend_maintenance_mode=_env_bool("WEEKEND_MAINTENANCE_MODE", True),
            runtime_mode_path=_env("RUNTIME_MODE_PATH", "runtime/mode.json"),
            agent_override_default_ttl_minutes=_env_int(
                "AGENT_OVERRIDE_DEFAULT_TTL_MINUTES",
                120,
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
        if self.weekend_maintenance_mode and now.weekday() >= 5:
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

    @classmethod
    def from_env(cls) -> "SchwabSettings":
        load_dotenv()
        return cls(
            api_base_url=_env("SCHWAB_API_BASE_URL", "https://api.schwabapi.com"),
            access_token=_env("SCHWAB_ACCESS_TOKEN"),
            token_file=_env("SCHWAB_TOKEN_FILE", "runtime/schwab-token.json"),
            verify_indexes=_env_csv(
                "SCHWAB_VERIFY_INDEXES",
                "$SPX,$VIX,$VIX1D,$VIX9D,$VIX3M,$VVIX,$SKEW",
            ),
            verify_equities=_env_csv("SCHWAB_VERIFY_EQUITIES", "SPY,QQQ,IWM,HYG,LQD,TLT,IEF"),
            verify_futures=_env_csv("SCHWAB_VERIFY_FUTURES", "/ES,/MES"),
            verify_option_chains=_env_csv("SCHWAB_VERIFY_OPTION_CHAINS", "SPX,XSP,SPY,QQQ,IWM"),
            option_chain_strike_count=_env_int("SCHWAB_OPTION_CHAIN_STRIKE_COUNT", 10),
            quote_fields=_env(
                "SCHWAB_QUOTE_FIELDS",
                "quote,reference,extended,regular",
            ),
            request_timeout_seconds=_env_float("SCHWAB_REQUEST_TIMEOUT_SECONDS", 12.0),
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
            data_root=_env("MAINTENANCE_DATA_ROOT", "data"),
            logs_root=_env("MAINTENANCE_LOGS_ROOT", "logs"),
            output_root=_env("MAINTENANCE_OUTPUT_ROOT", "logs"),
            data_budget_gb=_env_float("MAINTENANCE_DATA_BUDGET_GB", 80.0),
            raw_retention_days=_env_int("MAINTENANCE_RAW_RETENTION_DAYS", 10),
            alert_window_retention_days=_env_int("MAINTENANCE_ALERT_WINDOW_RETENTION_DAYS", 60),
            feature_1s_retention_days=_env_int("MAINTENANCE_FEATURE_1S_RETENTION_DAYS", 30),
            feature_5s_retention_days=_env_int("MAINTENANCE_FEATURE_5S_RETENTION_DAYS", 90),
            log_retention_days=_env_int("MAINTENANCE_LOG_RETENTION_DAYS", 14),
            trash_retention_days=_env_int("MAINTENANCE_TRASH_RETENTION_DAYS", 7),
            warn_pct=_env_float("MAINTENANCE_WARN_PCT", 70.0),
            compact_pct=_env_float("MAINTENANCE_COMPACT_PCT", 80.0),
            degraded_pct=_env_float("MAINTENANCE_DEGRADED_PCT", 85.0),
            prune_pct=_env_float("MAINTENANCE_PRUNE_PCT", 90.0),
            critical_pct=_env_float("MAINTENANCE_CRITICAL_PCT", 95.0),
        )


@dataclass(frozen=True)
class StorageSettings:
    data_root: str
    latest_state_path: str
    raw_file_name: str
    include_raw_payload: bool
    latest_stale_after_seconds: float

    @classmethod
    def from_env(cls) -> "StorageSettings":
        load_dotenv()
        data_root = _env("MARKET_DATA_DATA_ROOT", _env("MAINTENANCE_DATA_ROOT", "data"))
        return cls(
            data_root=data_root,
            latest_state_path=_env(
                "MARKET_DATA_LATEST_STATE_PATH",
                f"{data_root.rstrip('/')}/latest/state.json",
            ),
            raw_file_name=_env("MARKET_DATA_RAW_FILE_NAME", "quotes.jsonl"),
            include_raw_payload=_env_bool("MARKET_DATA_INCLUDE_RAW_PAYLOAD", False),
            latest_stale_after_seconds=_env_float("MARKET_DATA_LATEST_STALE_AFTER_SECONDS", 15.0),
        )


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

    @classmethod
    def from_env(cls) -> "SamplingSettings":
        load_dotenv()
        return cls(
            strike_step=_env_int("SAMPLING_STRIKE_STEP", 5),
            window_points=_env_int("SAMPLING_WINDOW_POINTS", 200),
            hot_window_points=_env_int("SAMPLING_HOT_WINDOW_POINTS", 50),
            group_count=_env_int("SAMPLING_GROUP_COUNT", 4),
            group_interval_seconds=_env_int("SAMPLING_GROUP_INTERVAL_SECONDS", 4),
            degraded_group_count=_env_int("SAMPLING_DEGRADED_GROUP_COUNT", 20),
            degraded_group_interval_seconds=_env_int("SAMPLING_DEGRADED_GROUP_INTERVAL_SECONDS", 3),
            group_strategy=_env("SAMPLING_GROUP_STRATEGY", "interleaved").lower(),
            hot_human_cadence_seconds=_env_int("SAMPLING_HOT_HUMAN_CADENCE_SECONDS", 8),
            hot_execution_cadence_seconds=_env_int("SAMPLING_HOT_EXECUTION_CADENCE_SECONDS", 2),
            include_next_expiry=_env_bool("SAMPLING_INCLUDE_NEXT_EXPIRY", True),
            default_mode=_env("SAMPLING_DEFAULT_MODE", "human_alert"),
        )
