"""IBKR configuration contracts."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from spx_spark.config import (
    env_bool,
    env_csv,
    env_csv_preserve,
    env_float,
    env_int,
    env_str,
    is_time_in_window,
    load_dotenv,
    next_equity_futures_month,
    parse_hhmm,
)
from spx_spark.market_calendar import ET as NY_TZ
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, default_spxw_expiry
from spx_spark.settings import settings_csv, settings_value


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
    slow_index_stale_after_seconds: float = field(default_factory=lambda: 300.0)
    slow_index_labels: frozenset[str] = field(
        default_factory=lambda: frozenset(str(item) for item in ["index:SKEW", "index:VVIX"])
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
                "IBKR_VERIFY_INDEXES",
                settings_csv("ibkr.verify_indexes"),
            ),
            verify_stocks=env_csv(
                "IBKR_VERIFY_STOCKS",
                settings_csv("ibkr.verify_stocks"),
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
    market_data_line_capacity: int = 100
    spy_option_lines: int = field(default_factory=lambda: 16)
    spy_strike_step: int = field(default_factory=lambda: 2)
    slow_poll_labels: tuple[str, ...] = field(
        default_factory=lambda: tuple(
            str(item)
            for item in [
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
            ]
        )
    )
    slow_poll_interval_seconds: float = field(default_factory=lambda: 300.0)
    slow_poll_hold_seconds: float = field(default_factory=lambda: 10.0)
    slow_poll_chunk_size: int = field(default_factory=lambda: 6)
    atm_state_path: str = field(default_factory=lambda: "")
    freeze_quotes_on_connectivity_loss: bool = field(default_factory=lambda: bool(True))
    data_flow_silence_seconds: float = field(default_factory=lambda: 120.0)

    @classmethod
    def from_env(cls) -> "IbkrStreamSettings":
        load_dotenv()
        return cls(
            # Distinct from the snapshot collector's client id so an accidental
            # overlap does not kick the other API session.
            client_id=env_int(
                "IBKR_STREAM_CLIENT_ID", int(settings_value("ibkr_stream.client_id"))
            ),
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
            market_data_line_capacity=env_int(
                "IBKR_MARKET_DATA_LINE_CAPACITY",
                int(settings_value("ibkr_stream.market_data_line_capacity")),
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
                    "IBKR_STREAM_SLOW_POLL_LABELS",
                    settings_csv("ibkr_stream.slow_poll_labels"),
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
            atm_state_path=env_str(
                "IBKR_ATM_STATE_PATH", str(settings_value("ibkr_stream.atm_state_path"))
            ),
            freeze_quotes_on_connectivity_loss=env_bool(
                "IBKR_STREAM_FREEZE_QUOTES_ON_CONNECTIVITY_LOSS",
                bool(settings_value("ibkr_stream.freeze_quotes_on_connectivity_loss")),
            ),
            data_flow_silence_seconds=env_float(
                "IBKR_STREAM_DATA_FLOW_SILENCE_SECONDS",
                float(settings_value("ibkr_stream.data_flow_silence_seconds")),
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
        snapshot_path = (
            env_str("IBKR_POSITIONS_SNAPSHOT_PATH", default_snapshot) or default_snapshot
        )
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
                "IBKR_SCHEDULE_ENABLED",
                bool(settings_value("runtime_policy.ibkr_schedule_enabled")),
            ),
            ibkr_schedule_timezone=env_str(
                "IBKR_SCHEDULE_TZ", str(settings_value("runtime_policy.ibkr_schedule_timezone"))
            ),
            ibkr_schedule_start=parse_hhmm(
                env_str(
                    "IBKR_SCHEDULE_START", str(settings_value("runtime_policy.ibkr_schedule_start"))
                )
            ),
            ibkr_schedule_stop=parse_hhmm(
                env_str(
                    "IBKR_SCHEDULE_STOP", str(settings_value("runtime_policy.ibkr_schedule_stop"))
                )
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
                "IBKR_FALLBACK_PROVIDER",
                str(settings_value("runtime_policy.ibkr_fallback_provider")),
            ).lower(),
            strict_no_session_fight=env_bool(
                "STRICT_NO_SESSION_FIGHT",
                bool(settings_value("runtime_policy.strict_no_session_fight")),
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
                futures_reopen = now_et.time() >= time(
                    18, 0
                ) and DEFAULT_MARKET_CALENDAR.is_trading_day(next_wall_day)
                if not futures_reopen:
                    return False
        return self.ibkr_window_is_open(now)

    @property
    def should_retry_after_conflict(self) -> bool:
        return not self.strict_no_session_fight and self.ibkr_conflict_retry_minutes > 0

    @property
    def should_probe_after_conflict(self) -> bool:
        return self.ibkr_conflict_probe_seconds > 0
