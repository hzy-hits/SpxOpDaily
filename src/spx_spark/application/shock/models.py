"""Shock monitor settings, samples, and durable state IO."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from spx_spark.config import env_bool, env_csv_preserve, env_float, env_int
from spx_spark.marketdata import Provider, as_utc
from spx_spark.settings import AppSettings, ShockSettings, load_app_settings
from spx_spark.settings.shock import DEFAULT_SHOCK_SETTINGS


STATE_SCHEMA_VERSION = 1
SHOCK_KIND = "intraday_price_shock"
RECLAIM_KIND = "intraday_price_reclaim"


@dataclass(frozen=True)
class IntradayShockSettings:
    state_path: str
    anchor_provider_priority: tuple[str, ...] = ("schwab", "ibkr")
    require_schwab_streaming_anchors: bool = True
    provider_switch_reset_seconds: int = 30
    one_minute_seconds: int = 60
    three_minute_seconds: int = 180
    one_minute_threshold_bps: float = 20.0
    three_minute_threshold_bps: float = 35.0
    es_confirm_ratio: float = 0.50
    max_spx_age_seconds: float = 15.0
    max_es_age_seconds: float = 10.0
    max_anchor_skew_seconds: float = 5.0
    reclaim_window_seconds: int = 300
    event_expiry_seconds: int = 600
    reclaim_fraction: float = 0.60
    es_reclaim_fraction: float = 0.40
    reclaim_hold_fraction: float = 0.55
    es_reclaim_hold_fraction: float = 0.35
    reclaim_confirm_samples: int = 2
    completion_hold_seconds: int = 60
    rearm_recovery_fraction: float = 0.40
    rearm_neutral_seconds: int = 300
    retry_seconds: int = 30
    gth_dip_reclaim_enabled: bool = True
    gth_short_horizon_seconds: int = 900
    gth_long_horizon_seconds: int = 3600
    gth_short_min_drawdown_points: float = 10.0
    gth_long_min_drawdown_points: float = 14.0
    gth_short_min_descent_seconds: int = 300
    gth_long_min_descent_seconds: int = 1200
    gth_expected_move_fraction: float = 0.10
    gth_reclaim_fraction: float = 0.40
    gth_min_reclaim_points: float = 5.0
    gth_confirm_samples: int = 2
    gth_confirm_hold_seconds: int = 60
    gth_session_warmup_seconds: int = 3600
    gth_max_signals_per_session: int = 3
    gth_cooldown_seconds: int = 3600

    def __post_init__(self) -> None:
        if not self.anchor_provider_priority:
            raise ValueError("intraday shock anchor provider priority cannot be empty")
        supported = {Provider.SCHWAB.value, Provider.IBKR.value}
        invalid = sorted(set(self.anchor_provider_priority) - supported)
        if invalid:
            raise ValueError(
                "intraday shock anchors must be direct Schwab or IBKR providers: "
                + ",".join(invalid)
            )
        if self.provider_switch_reset_seconds <= 0:
            raise ValueError("intraday shock provider switch reset seconds must be positive")

    @classmethod
    def from_policy(
        cls,
        policy: ShockSettings,
        *,
        state_path: str | None = None,
    ) -> "IntradayShockSettings":
        data_root = (
            os.getenv("MARKET_DATA_DATA_ROOT")
            or os.getenv("MAINTENANCE_DATA_ROOT")
            or policy.data_root
        )
        return cls(
            state_path=state_path
            or os.getenv("ALERT_INTRADAY_SHOCK_STATE_PATH")
            or f"{data_root.rstrip('/')}/latest/intraday_shock_state.json",
            anchor_provider_priority=tuple(
                provider.lower()
                for provider in env_csv_preserve(
                    "ALERT_INTRADAY_ANCHOR_PROVIDER_PRIORITY",
                    ",".join(policy.anchor_provider_priority),
                )
            ),
            require_schwab_streaming_anchors=env_bool(
                "ALERT_INTRADAY_REQUIRE_SCHWAB_STREAMING_ANCHORS",
                policy.require_schwab_streaming_anchors,
            ),
            provider_switch_reset_seconds=env_int(
                "ALERT_INTRADAY_PROVIDER_SWITCH_RESET_SECONDS",
                policy.provider_switch_reset_seconds,
            ),
            one_minute_seconds=env_int(
                "ALERT_INTRADAY_SHOCK_1M_SECONDS",
                policy.one_minute_seconds,
            ),
            three_minute_seconds=env_int(
                "ALERT_INTRADAY_SHOCK_3M_SECONDS",
                policy.three_minute_seconds,
            ),
            one_minute_threshold_bps=env_float(
                "ALERT_INTRADAY_SHOCK_1M_BPS",
                policy.one_minute_threshold_bps,
            ),
            three_minute_threshold_bps=env_float(
                "ALERT_INTRADAY_SHOCK_3M_BPS",
                policy.three_minute_threshold_bps,
            ),
            es_confirm_ratio=env_float(
                "ALERT_INTRADAY_SHOCK_ES_CONFIRM_RATIO",
                policy.es_confirm_ratio,
            ),
            max_spx_age_seconds=env_float(
                "ALERT_INTRADAY_SHOCK_SPX_MAX_AGE_SECONDS",
                policy.max_spx_age_seconds,
            ),
            max_es_age_seconds=env_float(
                "ALERT_INTRADAY_SHOCK_ES_MAX_AGE_SECONDS",
                policy.max_es_age_seconds,
            ),
            max_anchor_skew_seconds=env_float(
                "ALERT_INTRADAY_SHOCK_MAX_ANCHOR_SKEW_SECONDS",
                policy.max_anchor_skew_seconds,
            ),
            reclaim_window_seconds=env_int(
                "ALERT_INTRADAY_RECLAIM_WINDOW_SECONDS",
                policy.reclaim_window_seconds,
            ),
            event_expiry_seconds=env_int(
                "ALERT_INTRADAY_EVENT_EXPIRY_SECONDS",
                policy.event_expiry_seconds,
            ),
            reclaim_fraction=env_float(
                "ALERT_INTRADAY_RECLAIM_FRACTION",
                policy.reclaim_fraction,
            ),
            es_reclaim_fraction=env_float(
                "ALERT_INTRADAY_RECLAIM_ES_FRACTION",
                policy.es_reclaim_fraction,
            ),
            reclaim_hold_fraction=env_float(
                "ALERT_INTRADAY_RECLAIM_HOLD_FRACTION",
                policy.reclaim_hold_fraction,
            ),
            es_reclaim_hold_fraction=env_float(
                "ALERT_INTRADAY_RECLAIM_ES_HOLD_FRACTION",
                policy.es_reclaim_hold_fraction,
            ),
            reclaim_confirm_samples=env_int(
                "ALERT_INTRADAY_RECLAIM_CONFIRM_SAMPLES",
                policy.reclaim_confirm_samples,
            ),
            completion_hold_seconds=env_int(
                "ALERT_INTRADAY_COMPLETION_HOLD_SECONDS",
                policy.completion_hold_seconds,
            ),
            rearm_recovery_fraction=env_float(
                "ALERT_INTRADAY_REARM_RECOVERY_FRACTION",
                policy.rearm_recovery_fraction,
            ),
            rearm_neutral_seconds=env_int(
                "ALERT_INTRADAY_REARM_NEUTRAL_SECONDS",
                policy.rearm_neutral_seconds,
            ),
            retry_seconds=env_int(
                "ALERT_INTRADAY_DELIVERY_RETRY_SECONDS",
                policy.retry_seconds,
            ),
            gth_dip_reclaim_enabled=policy.gth_dip_reclaim_enabled,
            gth_short_horizon_seconds=policy.gth_short_horizon_seconds,
            gth_long_horizon_seconds=policy.gth_long_horizon_seconds,
            gth_short_min_drawdown_points=policy.gth_short_min_drawdown_points,
            gth_long_min_drawdown_points=policy.gth_long_min_drawdown_points,
            gth_short_min_descent_seconds=policy.gth_short_min_descent_seconds,
            gth_long_min_descent_seconds=policy.gth_long_min_descent_seconds,
            gth_expected_move_fraction=policy.gth_expected_move_fraction,
            gth_reclaim_fraction=policy.gth_reclaim_fraction,
            gth_min_reclaim_points=policy.gth_min_reclaim_points,
            gth_confirm_samples=policy.gth_confirm_samples,
            gth_confirm_hold_seconds=policy.gth_confirm_hold_seconds,
            gth_session_warmup_seconds=policy.gth_session_warmup_seconds,
            gth_max_signals_per_session=policy.gth_max_signals_per_session,
            gth_cooldown_seconds=policy.gth_cooldown_seconds,
        )

    @classmethod
    def from_app_settings(cls, app: AppSettings) -> "IntradayShockSettings":
        return cls.from_policy(app.shock)

    @classmethod
    def from_env(cls) -> "IntradayShockSettings":
        try:
            return cls.from_app_settings(load_app_settings())
        except (FileNotFoundError, KeyError, ValueError, TypeError):
            # Tests / minimal environments without full runtime.yaml still work.
            return cls.from_policy(DEFAULT_SHOCK_SETTINGS)


@dataclass(frozen=True)
class PriceSample:
    at: datetime
    spx: float
    es: float
    spx_source_at: datetime | None = None
    es_source_at: datetime | None = None
    provider: str = Provider.UNKNOWN.value

    def to_dict(self) -> dict[str, object]:
        return {
            "at": as_utc(self.at).isoformat(),
            "spx": self.spx,
            "es": self.es,
            "spx_source_at": as_utc(self.spx_source_at).isoformat()
            if self.spx_source_at is not None
            else None,
            "es_source_at": as_utc(self.es_source_at).isoformat()
            if self.es_source_at is not None
            else None,
            "provider": self.provider,
        }


def _parse_datetime(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return as_utc(parsed)


def _sample_from_dict(value: object) -> PriceSample | None:
    if not isinstance(value, dict):
        return None
    at = _parse_datetime(value.get("at"))
    spx = value.get("spx")
    es = value.get("es")
    if at is None or not isinstance(spx, int | float) or not isinstance(es, int | float):
        return None
    if float(spx) <= 0 or float(es) <= 0:
        return None
    return PriceSample(
        at=at,
        spx=float(spx),
        es=float(es),
        spx_source_at=_parse_datetime(value.get("spx_source_at")),
        es_source_at=_parse_datetime(value.get("es_source_at")),
        provider=str(value.get("provider") or Provider.UNKNOWN.value),
    )


def empty_monitor_state(session_date: str) -> dict[str, object]:
    return {
        "schema_version": STATE_SCHEMA_VERSION,
        "session_date": session_date,
        "samples": [],
        "active_event": None,
        "rearm": None,
        "last_event": None,
        "updated_at": None,
    }


def load_monitor_state(path: str, *, session_date: str) -> dict[str, object]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return empty_monitor_state(session_date)
    if not isinstance(payload, dict) or payload.get("session_date") != session_date:
        return empty_monitor_state(session_date)
    if payload.get("schema_version") != STATE_SCHEMA_VERSION:
        return empty_monitor_state(session_date)
    return payload
