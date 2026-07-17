"""Typed contracts and constants for the Steven observe-only strategy."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from spx_spark.config import env_bool, env_float, env_int, env_str
from spx_spark.features.bar_builder import SpxBar
from spx_spark.features.exposure_map import ExposureMap
from spx_spark.intraday_strategy import STRATEGY_KINDS
from spx_spark.settings import settings_value
from spx_spark.strategy.micopedia import VALID_TIME_PHASES, normalize_tags

RETROSPECTIVE_SOURCES_ALLOWED = False

CONTRACT_SCHEMA_VERSION = "steven_guidance_contract.v0.1"

CONTRACT_SOURCE = "steven_spx_options_framework_house_proxy"

STATE_SCHEMA_VERSION = "steven_state.v0.1"

EPISODE_SCHEMA_VERSION = "steven_episode_event.v0.1"

ANCHOR_SOURCES = frozenset({"index:SPX", "chain_implied"})

MACHINE_STATES = frozenset(
    {
        "DATA_INVALID",
        "OBSERVE_ONLY",
        "REGIME_UNKNOWN",
        "BULLISH_DIP_WATCH",
        "BEARISH_BREAK_WATCH",
        "RANGE_PIN_WATCH",
        "EVENT_WAIT",
        "SETUP_CONFIRMED",
        "EXIT_REVIEW",
        "LOCKOUT_OR_REMAP",
    }
)

WATCH_STATES = frozenset({"BULLISH_DIP_WATCH", "BEARISH_BREAK_WATCH", "RANGE_PIN_WATCH"})

EVENT_WAIT_TAGS = frozenset({"fomc", "cpi", "nfp", "pce", "headline"})

EXPRESSION_FAMILIES = frozenset(
    {"none", "bullish_defined_risk", "bearish_defined_risk", "range_defined_risk"}
)

CONFIDENCE_ORDER = {"low": 0, "medium": 1, "high": 2}

ALERT_CONTEXT_KINDS = frozenset({"intraday_price_shock", "intraday_price_reclaim"}) | STRATEGY_KINDS

COMPLETED_SHOCK_PHASES = frozenset({"completed", "expired"})


@dataclass(frozen=True)
class StevenSettings:
    enabled: bool = False
    regime_weighting: str = "oi_weighted"
    regime_dex_neutral_band: float = 100000.0
    regime_min_expiries: int = 2
    regime_agreement_min_ratio: float = 0.67
    pin_max_distance_points: float = 25.0
    wall_confluence_points: float = 10.0
    max_snapshot_age_seconds: float = 900.0
    data_recovery_hold_seconds: float = 60.0
    event_wait_cooldown_seconds: float = 900.0
    event_stabilize_bars: int = 5
    event_stabilize_range_points: float = 10.0
    dip_watch_max_distance_points: float = 30.0
    break_watch_max_distance_points: float = 30.0
    pin_watch_max_distance_points: float = 20.0
    pin_min_net_gamma_ratio: float = 0.15
    trigger_level_tolerance_points: float = 5.0
    trigger_hold_bars: int = 2
    watch_exit_hold_seconds: float = 120.0
    invalidation_hold_bars: int = 2
    lockout_minutes: float = 30.0
    max_daily_setups: int = 2
    episode_revision_min_level_move_points: float = 10.0
    alert_context_enabled: bool = False
    alert_context_max_age_seconds: float = 120.0
    bars_source_max_age_seconds: float = 90.0

    @classmethod
    def from_env(cls) -> StevenSettings:
        return cls(
            enabled=env_bool("SPX_STEVEN_ENABLED", bool(settings_value("steven.enabled"))),
            regime_weighting=env_str(
                "SPX_STEVEN_REGIME_WEIGHTING",
                str(settings_value("steven.regime_weighting")),
            ),
            regime_dex_neutral_band=env_float(
                "SPX_STEVEN_REGIME_DEX_NEUTRAL_BAND",
                float(settings_value("steven.regime_dex_neutral_band")),
            ),
            regime_min_expiries=env_int(
                "SPX_STEVEN_REGIME_MIN_EXPIRIES",
                int(settings_value("steven.regime_min_expiries")),
            ),
            regime_agreement_min_ratio=env_float(
                "SPX_STEVEN_REGIME_AGREEMENT_MIN_RATIO",
                float(settings_value("steven.regime_agreement_min_ratio")),
            ),
            pin_max_distance_points=env_float(
                "SPX_STEVEN_PIN_MAX_DISTANCE_POINTS",
                float(settings_value("steven.pin_max_distance_points")),
            ),
            wall_confluence_points=env_float(
                "SPX_STEVEN_WALL_CONFLUENCE_POINTS",
                float(settings_value("steven.wall_confluence_points")),
            ),
            max_snapshot_age_seconds=env_float(
                "SPX_STEVEN_MAX_SNAPSHOT_AGE_SECONDS",
                float(settings_value("steven.max_snapshot_age_seconds")),
            ),
            data_recovery_hold_seconds=env_float(
                "SPX_STEVEN_DATA_RECOVERY_HOLD_SECONDS",
                float(settings_value("steven.data_recovery_hold_seconds")),
            ),
            event_wait_cooldown_seconds=env_float(
                "SPX_STEVEN_EVENT_WAIT_COOLDOWN_SECONDS",
                float(settings_value("steven.event_wait_cooldown_seconds")),
            ),
            event_stabilize_bars=env_int(
                "SPX_STEVEN_EVENT_STABILIZE_BARS",
                int(settings_value("steven.event_stabilize_bars")),
            ),
            event_stabilize_range_points=env_float(
                "SPX_STEVEN_EVENT_STABILIZE_RANGE_POINTS",
                float(settings_value("steven.event_stabilize_range_points")),
            ),
            dip_watch_max_distance_points=env_float(
                "SPX_STEVEN_DIP_WATCH_MAX_DISTANCE_POINTS",
                float(settings_value("steven.dip_watch_max_distance_points")),
            ),
            break_watch_max_distance_points=env_float(
                "SPX_STEVEN_BREAK_WATCH_MAX_DISTANCE_POINTS",
                float(settings_value("steven.break_watch_max_distance_points")),
            ),
            pin_watch_max_distance_points=env_float(
                "SPX_STEVEN_PIN_WATCH_MAX_DISTANCE_POINTS",
                float(settings_value("steven.pin_watch_max_distance_points")),
            ),
            pin_min_net_gamma_ratio=env_float(
                "SPX_STEVEN_PIN_MIN_NET_GAMMA_RATIO",
                float(settings_value("steven.pin_min_net_gamma_ratio")),
            ),
            trigger_level_tolerance_points=env_float(
                "SPX_STEVEN_TRIGGER_LEVEL_TOLERANCE_POINTS",
                float(settings_value("steven.trigger_level_tolerance_points")),
            ),
            trigger_hold_bars=env_int(
                "SPX_STEVEN_TRIGGER_HOLD_BARS",
                int(settings_value("steven.trigger_hold_bars")),
            ),
            watch_exit_hold_seconds=env_float(
                "SPX_STEVEN_WATCH_EXIT_HOLD_SECONDS",
                float(settings_value("steven.watch_exit_hold_seconds")),
            ),
            invalidation_hold_bars=env_int(
                "SPX_STEVEN_INVALIDATION_HOLD_BARS",
                int(settings_value("steven.invalidation_hold_bars")),
            ),
            lockout_minutes=env_float(
                "SPX_STEVEN_LOCKOUT_MINUTES",
                float(settings_value("steven.lockout_minutes")),
            ),
            max_daily_setups=env_int(
                "SPX_STEVEN_MAX_DAILY_SETUPS",
                int(settings_value("steven.max_daily_setups")),
            ),
            episode_revision_min_level_move_points=env_float(
                "SPX_STEVEN_EPISODE_REVISION_MIN_LEVEL_MOVE_POINTS",
                float(settings_value("steven.episode_revision_min_level_move_points")),
            ),
            alert_context_enabled=env_bool(
                "SPX_STEVEN_ALERT_CONTEXT_ENABLED",
                bool(settings_value("steven.alert_context_enabled")),
            ),
            alert_context_max_age_seconds=env_float(
                "SPX_STEVEN_ALERT_CONTEXT_MAX_AGE_SECONDS",
                float(settings_value("steven.alert_context_max_age_seconds")),
            ),
            bars_source_max_age_seconds=env_float(
                "SPX_STEVEN_BARS_SOURCE_MAX_AGE_SECONDS",
                float(settings_value("steven.bars_source_max_age_seconds")),
            ),
        )


@dataclass(frozen=True)
class StevenInputs:
    created_at: datetime
    as_of: datetime
    underlier_price: float | None = None
    underlier_source: str | None = None
    exposure: ExposureMap | None = None
    bars_1m: tuple[SpxBar, ...] = ()
    bars_5m: tuple[SpxBar, ...] = ()
    shock_state: dict[str, Any] | None = None
    es_volume: dict[str, Any] | None = None
    hl_volume: dict[str, Any] | None = None
    session_phase: str = "unknown"
    event_tags: tuple[str, ...] = ()
    consumed_event_tags: tuple[str, ...] = ()
    previous_state: str = "OBSERVE_ONLY"
    previous_state_since: datetime | None = None
    trading_date: str | None = None
    daily_setup_count: int = 0
    lockout_until: datetime | None = None
    data_healthy_since: datetime | None = None
    watch_exit_since: datetime | None = None
    settings: StevenSettings = field(default_factory=StevenSettings)

    def __post_init__(self) -> None:
        object.__setattr__(self, "created_at", _as_utc(self.created_at))
        object.__setattr__(self, "as_of", _as_utc(self.as_of))
        phase = self.session_phase.strip().lower().replace("-", "_")
        if phase not in VALID_TIME_PHASES:
            phase = "unknown"
        object.__setattr__(self, "session_phase", phase)
        object.__setattr__(self, "event_tags", normalize_tags(self.event_tags))
        object.__setattr__(
            self, "consumed_event_tags", normalize_tags(self.consumed_event_tags)
        )
        state = self.previous_state if self.previous_state in MACHINE_STATES else "OBSERVE_ONLY"
        object.__setattr__(self, "previous_state", state)
        if self.previous_state_since is not None:
            object.__setattr__(self, "previous_state_since", _as_utc(self.previous_state_since))
        if self.lockout_until is not None:
            object.__setattr__(self, "lockout_until", _as_utc(self.lockout_until))
        if self.data_healthy_since is not None:
            object.__setattr__(self, "data_healthy_since", _as_utc(self.data_healthy_since))
        if self.watch_exit_since is not None:
            object.__setattr__(self, "watch_exit_since", _as_utc(self.watch_exit_since))


@dataclass(frozen=True)
class StevenSignal:
    created_at: datetime
    as_of: datetime
    status: str
    machine_state: str
    regime: str
    regime_breadth: dict[str, Any]
    map: dict[str, Any]
    trigger: dict[str, Any]
    invalidation: dict[str, Any]
    expression_family: str
    confidence: str
    flow_confirmation: dict[str, Any]
    data_quality: dict[str, Any]
    warnings: tuple[str, ...]
    transition_rule: str | None = None
    data_healthy_since: datetime | None = None
    watch_exit_since: datetime | None = None
    lockout_until: datetime | None = None
    daily_setup_count: int = 0
    consumed_event_tags: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": CONTRACT_SCHEMA_VERSION,
            "source": CONTRACT_SOURCE,
            "created_at": self.created_at.isoformat(),
            "as_of": self.as_of.isoformat(),
            "status": self.status,
            "machine_state": self.machine_state,
            "regime": self.regime,
            "regime_breadth": dict(self.regime_breadth),
            "map": {
                "support": list(self.map.get("support") or []),
                "resistance": list(self.map.get("resistance") or []),
                "pin": self.map.get("pin"),
                "acceleration": list(self.map.get("acceleration") or []),
            },
            "trigger": dict(self.trigger),
            "invalidation": dict(self.invalidation),
            "expression_family": self.expression_family,
            "confidence": self.confidence,
            "flow_confirmation": dict(self.flow_confirmation),
            "data_quality": dict(self.data_quality),
            "warnings": list(self.warnings),
        }


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
