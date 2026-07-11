"""Steven SPX options framework — observe-only Guidance Contract v0.1.

Hard gates (see docs/steven-framework-integration.md §4):
1. Missing/stale SPX/SPXW anchor → DATA_INVALID
2. Proxy metrics never raise confidence above medium / never drive regime
3. No price trigger → never confirmed_for_review
4. Retrospective timestamps rejected on episode write
5. Active shock / event tags → EVENT_WAIT
6. Hyperliquid SP500 never used as underlier anchor
7. Expression family is bounded defined-risk names only
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from spx_spark.alert_model import Alert
from spx_spark.config import StorageSettings, env_bool, env_float, env_int, env_str
from spx_spark.features.bar_builder import SpxBar, SpxBarBuilder, bar_hold
from spx_spark.features.exposure_map import (
    ExposureMap,
    ExpiryExposure,
    build_exposure_map,
    net_dex_proxy_by_expiry,
    persist_exposure_map,
)
from spx_spark.intraday_strategy import STRATEGY_KINDS
from spx_spark.market_calendar import ET
from spx_spark.runtime_config import runtime_value
from spx_spark.state_io import atomic_write_json_secure
from spx_spark.storage import LatestState, LatestStateStore, configured_quote_use_decision
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
            enabled=env_bool("SPX_STEVEN_ENABLED", bool(runtime_value("steven.enabled"))),
            regime_weighting=env_str(
                "SPX_STEVEN_REGIME_WEIGHTING",
                str(runtime_value("steven.regime_weighting")),
            ),
            regime_dex_neutral_band=env_float(
                "SPX_STEVEN_REGIME_DEX_NEUTRAL_BAND",
                float(runtime_value("steven.regime_dex_neutral_band")),
            ),
            regime_min_expiries=env_int(
                "SPX_STEVEN_REGIME_MIN_EXPIRIES",
                int(runtime_value("steven.regime_min_expiries")),
            ),
            regime_agreement_min_ratio=env_float(
                "SPX_STEVEN_REGIME_AGREEMENT_MIN_RATIO",
                float(runtime_value("steven.regime_agreement_min_ratio")),
            ),
            pin_max_distance_points=env_float(
                "SPX_STEVEN_PIN_MAX_DISTANCE_POINTS",
                float(runtime_value("steven.pin_max_distance_points")),
            ),
            wall_confluence_points=env_float(
                "SPX_STEVEN_WALL_CONFLUENCE_POINTS",
                float(runtime_value("steven.wall_confluence_points")),
            ),
            max_snapshot_age_seconds=env_float(
                "SPX_STEVEN_MAX_SNAPSHOT_AGE_SECONDS",
                float(runtime_value("steven.max_snapshot_age_seconds")),
            ),
            data_recovery_hold_seconds=env_float(
                "SPX_STEVEN_DATA_RECOVERY_HOLD_SECONDS",
                float(runtime_value("steven.data_recovery_hold_seconds")),
            ),
            event_wait_cooldown_seconds=env_float(
                "SPX_STEVEN_EVENT_WAIT_COOLDOWN_SECONDS",
                float(runtime_value("steven.event_wait_cooldown_seconds")),
            ),
            event_stabilize_bars=env_int(
                "SPX_STEVEN_EVENT_STABILIZE_BARS",
                int(runtime_value("steven.event_stabilize_bars")),
            ),
            event_stabilize_range_points=env_float(
                "SPX_STEVEN_EVENT_STABILIZE_RANGE_POINTS",
                float(runtime_value("steven.event_stabilize_range_points")),
            ),
            dip_watch_max_distance_points=env_float(
                "SPX_STEVEN_DIP_WATCH_MAX_DISTANCE_POINTS",
                float(runtime_value("steven.dip_watch_max_distance_points")),
            ),
            break_watch_max_distance_points=env_float(
                "SPX_STEVEN_BREAK_WATCH_MAX_DISTANCE_POINTS",
                float(runtime_value("steven.break_watch_max_distance_points")),
            ),
            pin_watch_max_distance_points=env_float(
                "SPX_STEVEN_PIN_WATCH_MAX_DISTANCE_POINTS",
                float(runtime_value("steven.pin_watch_max_distance_points")),
            ),
            pin_min_net_gamma_ratio=env_float(
                "SPX_STEVEN_PIN_MIN_NET_GAMMA_RATIO",
                float(runtime_value("steven.pin_min_net_gamma_ratio")),
            ),
            trigger_level_tolerance_points=env_float(
                "SPX_STEVEN_TRIGGER_LEVEL_TOLERANCE_POINTS",
                float(runtime_value("steven.trigger_level_tolerance_points")),
            ),
            trigger_hold_bars=env_int(
                "SPX_STEVEN_TRIGGER_HOLD_BARS",
                int(runtime_value("steven.trigger_hold_bars")),
            ),
            watch_exit_hold_seconds=env_float(
                "SPX_STEVEN_WATCH_EXIT_HOLD_SECONDS",
                float(runtime_value("steven.watch_exit_hold_seconds")),
            ),
            invalidation_hold_bars=env_int(
                "SPX_STEVEN_INVALIDATION_HOLD_BARS",
                int(runtime_value("steven.invalidation_hold_bars")),
            ),
            lockout_minutes=env_float(
                "SPX_STEVEN_LOCKOUT_MINUTES",
                float(runtime_value("steven.lockout_minutes")),
            ),
            max_daily_setups=env_int(
                "SPX_STEVEN_MAX_DAILY_SETUPS",
                int(runtime_value("steven.max_daily_setups")),
            ),
            episode_revision_min_level_move_points=env_float(
                "SPX_STEVEN_EPISODE_REVISION_MIN_LEVEL_MOVE_POINTS",
                float(runtime_value("steven.episode_revision_min_level_move_points")),
            ),
            alert_context_enabled=env_bool(
                "SPX_STEVEN_ALERT_CONTEXT_ENABLED",
                bool(runtime_value("steven.alert_context_enabled")),
            ),
            alert_context_max_age_seconds=env_float(
                "SPX_STEVEN_ALERT_CONTEXT_MAX_AGE_SECONDS",
                float(runtime_value("steven.alert_context_max_age_seconds")),
            ),
            bars_source_max_age_seconds=env_float(
                "SPX_STEVEN_BARS_SOURCE_MAX_AGE_SECONDS",
                float(runtime_value("steven.bars_source_max_age_seconds")),
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


def trading_date_et(as_of: datetime) -> str:
    return _as_utc(as_of).astimezone(ET).date().isoformat()


def episode_id_for(trading_date: str) -> str:
    return f"steven:{trading_date}"


def front_expiry(exposure: ExposureMap | None) -> ExpiryExposure | None:
    if exposure is None or not exposure.expiries:
        return None
    return exposure.expiries[0]


def status_for_machine_state(machine_state: str) -> str:
    if machine_state == "DATA_INVALID":
        return "invalid"
    if machine_state == "SETUP_CONFIRMED":
        return "confirmed_for_review"
    if machine_state in WATCH_STATES or machine_state == "EVENT_WAIT":
        return "watch"
    return "observe_only"


def _cap_confidence(value: str, ceiling: str) -> str:
    if CONFIDENCE_ORDER.get(value, 0) <= CONFIDENCE_ORDER.get(ceiling, 0):
        return value
    return ceiling


def classify_regime(inputs: StevenInputs) -> tuple[str, dict[str, Any]]:
    """Classify regime from net_dex_proxy only — no vex/cex/vanna/charm inputs."""
    settings = inputs.settings
    weighting = settings.regime_weighting
    if weighting not in {"oi_weighted", "volume_weighted"}:
        weighting = "oi_weighted"
    breadth: dict[str, Any] = {
        "expiries_total": 0,
        "expiries_bullish": 0,
        "expiries_bearish": 0,
        "agreement_ratio": None,
        "weighting": weighting,
    }
    if inputs.exposure is None:
        return "unknown", breadth
    try:
        per_expiry = net_dex_proxy_by_expiry(inputs.exposure, weighting=weighting)
    except ValueError:
        return "unknown", breadth
    values = [value for value in per_expiry.values() if value is not None]
    breadth["expiries_total"] = len(values)
    if not values:
        return "unknown", breadth
    band = settings.regime_dex_neutral_band
    bullish = sum(1 for value in values if value > band)
    bearish = sum(1 for value in values if value < -band)
    breadth["expiries_bullish"] = bullish
    breadth["expiries_bearish"] = bearish
    classified = bullish + bearish
    if classified <= 0:
        breadth["agreement_ratio"] = 0.0
        return "unknown", breadth
    agreement = max(bullish, bearish) / len(values)
    breadth["agreement_ratio"] = agreement
    if (
        len(values) >= settings.regime_min_expiries
        and agreement >= settings.regime_agreement_min_ratio
    ):
        if bullish > bearish and bullish >= settings.regime_min_expiries:
            return "bullish", breadth
        if bearish > bullish and bearish >= settings.regime_min_expiries:
            return "bearish", breadth
    if bullish > 0 and bearish > 0:
        return "mixed", breadth
    if len(values) < settings.regime_min_expiries:
        return "unknown", breadth
    return "unknown", breadth


def build_map_levels(inputs: StevenInputs) -> tuple[dict[str, Any], tuple[str, ...]]:
    warnings: list[str] = []
    empty = {"support": [], "resistance": [], "pin": None, "acceleration": []}
    front = front_expiry(inputs.exposure)
    if front is None:
        return empty, tuple(warnings)
    support = [wall.strike for wall in front.walls.put_walls[:4]]
    resistance = [wall.strike for wall in front.walls.call_walls[:4]]
    pin = None
    net_gamma = front.oi_weighted.net_gamma_ratio
    if (
        front.walls.pin_candidate is not None
        and net_gamma is not None
        and net_gamma > 0
    ):
        pin = front.walls.pin_candidate
    acceleration: list[float] = []
    if front.gamma_flip_zone is not None:
        acceleration = [float(front.gamma_flip_zone[0]), float(front.gamma_flip_zone[1])]
    if inputs.exposure is not None and len(inputs.exposure.expiries) >= 2:
        next_expiry = inputs.exposure.expiries[1]
        confluence = inputs.settings.wall_confluence_points
        if support and next_expiry.walls.put_walls:
            next_put = next_expiry.walls.put_walls[0].strike
            if abs(next_put - support[0]) <= confluence:
                warnings.append(f"multi_expiry_wall_confluence:{support[0]}")
        if resistance and next_expiry.walls.call_walls:
            next_call = next_expiry.walls.call_walls[0].strike
            if abs(next_call - resistance[0]) <= confluence:
                warnings.append(f"multi_expiry_wall_confluence:{resistance[0]}")
    for strike in front.strikes:
        if strike.oi_weighted.vex_proxy is not None and abs(strike.oi_weighted.vex_proxy) >= 1e8:
            warnings.append(f"extreme_vex_proxy:{strike.strike}")
        if strike.oi_weighted.cex_proxy is not None and abs(strike.oi_weighted.cex_proxy) >= 1e8:
            warnings.append(f"extreme_cex_proxy:{strike.strike}")
    if front.gex_weighting_divergence is not None and abs(front.gex_weighting_divergence) >= 1e6:
        warnings.append(f"extreme_gex_weighting_divergence:{front.gex_weighting_divergence}")
    return (
        {
            "support": support,
            "resistance": resistance,
            "pin": pin,
            "acceleration": acceleration,
        },
        tuple(warnings),
    )


def _bars_touched_level(bars: Sequence[SpxBar], level: float, tolerance: float) -> bool:
    lo = level - tolerance
    hi = level + tolerance
    for bar in bars:
        if bar.low <= hi and bar.high >= lo:
            return True
    return False


def evaluate_trigger(
    inputs: StevenInputs,
    map_levels: Mapping[str, Any],
) -> dict[str, Any]:
    blank = {
        "kind": "none",
        "level": None,
        "direction": "none",
        "confirmed": False,
        "confirmed_at": None,
        "source_event_id": None,
    }
    settings = inputs.settings
    bars = inputs.bars_1m
    if not bars:
        return blank
    support = list(map_levels.get("support") or [])
    resistance = list(map_levels.get("resistance") or [])
    pin = map_levels.get("pin")
    tol = settings.trigger_level_tolerance_points
    hold_n = settings.trigger_hold_bars
    prev = inputs.previous_state

    if prev == "BULLISH_DIP_WATCH" and support:
        level = max(support)
        if _bars_touched_level(bars, level, tol) and bar_hold(bars, level, "above", hold_n):
            return {
                "kind": "dip_hold",
                "level": level,
                "direction": "up",
                "confirmed": True,
                "confirmed_at": inputs.as_of.isoformat(),
                "source_event_id": None,
            }
    if prev == "BEARISH_BREAK_WATCH" and support:
        level = max(support)
        if _bars_touched_level(bars, level, tol) and bar_hold(bars, level, "below", hold_n):
            return {
                "kind": "break_hold",
                "level": level,
                "direction": "down",
                "confirmed": True,
                "confirmed_at": inputs.as_of.isoformat(),
                "source_event_id": None,
            }
    if prev == "RANGE_PIN_WATCH":
        candidates: list[tuple[float, str]] = []
        if resistance:
            candidates.append((min(resistance), "below"))
        if support:
            candidates.append((max(support), "above"))
        if pin is not None:
            # range reject can also bounce off pin edges via support/resistance
            pass
        for level, side in candidates:
            if _bars_touched_level(bars, level, tol) and bar_hold(bars, level, side, hold_n):
                return {
                    "kind": "range_reject",
                    "level": level,
                    "direction": "up" if side == "above" else "down",
                    "confirmed": True,
                    "confirmed_at": inputs.as_of.isoformat(),
                    "source_event_id": None,
                }
    return blank


def evaluate_flow(inputs: StevenInputs, trigger: Mapping[str, Any]) -> dict[str, Any]:
    sources: list[str] = []
    directions: list[str] = []
    if isinstance(inputs.es_volume, dict):
        sources.append("es_volume")
        direction = inputs.es_volume.get("direction")
        if isinstance(direction, str) and direction in {"up", "down"}:
            directions.append(direction)
    if isinstance(inputs.hl_volume, dict):
        sources.append("hl_volume")
        direction = inputs.hl_volume.get("direction")
        if isinstance(direction, str) and direction in {"up", "down"}:
            directions.append(direction)
    trigger_dir = trigger.get("direction")
    if not sources:
        return {"status": "none", "sources": [], "quality": "weak_proxy"}
    if not isinstance(trigger_dir, str) or trigger_dir not in {"up", "down"} or not directions:
        return {"status": "weak", "sources": sources, "quality": "weak_proxy"}
    if all(item == trigger_dir for item in directions):
        return {"status": "aligned", "sources": sources, "quality": "weak_proxy"}
    if all(item != trigger_dir for item in directions):
        return {"status": "opposed", "sources": sources, "quality": "weak_proxy"}
    return {"status": "weak", "sources": sources, "quality": "weak_proxy"}


def build_invalidation(
    inputs: StevenInputs,
    map_levels: Mapping[str, Any],
    regime: str,
    machine_state: str,
) -> dict[str, Any]:
    support = list(map_levels.get("support") or [])
    resistance = list(map_levels.get("resistance") or [])
    pin = map_levels.get("pin")
    if machine_state in {"BULLISH_DIP_WATCH", "SETUP_CONFIRMED"} and regime == "bullish" and support:
        return {
            "level": max(support),
            "side": "below",
            "reason": "close_below_support_invalidates_bullish_dip",
        }
    if machine_state in {"BEARISH_BREAK_WATCH", "SETUP_CONFIRMED"} and regime == "bearish" and support:
        return {
            "level": max(support),
            "side": "above",
            "reason": "close_above_broken_support_invalidates_bearish_break",
        }
    if machine_state in {"RANGE_PIN_WATCH", "SETUP_CONFIRMED"} and pin is not None:
        return {
            "level": float(pin),
            "side": "none",
            "reason": "range_pin_thesis_exits_on_pin_acceptance_or_target",
        }
    if resistance or support:
        return {"level": None, "side": "none", "reason": "no_active_invalidation"}
    return {"level": None, "side": "none", "reason": "map_unavailable"}


def candidate_expression_family(machine_state: str, regime: str) -> str:
    if machine_state != "SETUP_CONFIRMED":
        return "none"
    if regime == "bullish":
        return "bullish_defined_risk"
    if regime == "bearish":
        return "bearish_defined_risk"
    if regime == "mixed":
        return "range_defined_risk"
    return "none"


def classify_confidence(
    *,
    data_quality: Mapping[str, Any],
    flow: Mapping[str, Any],
    regime: str,
    trigger: Mapping[str, Any],
) -> str:
    score = 0
    if data_quality.get("anchor_ok"):
        score += 1
    if data_quality.get("exposure_quality") == "ok":
        score += 1
    if data_quality.get("oi_quality") == "ibkr_ok":
        score += 1
    if regime in {"bullish", "bearish", "mixed"}:
        score += 1
    if trigger.get("confirmed"):
        score += 1
    if flow.get("status") == "aligned":
        score += 1
    if score >= 5:
        confidence = "high"
    elif score >= 3:
        confidence = "medium"
    else:
        confidence = "low"
    confidence = _cap_confidence(confidence, "medium")
    if data_quality.get("oi_quality") == "schwab_unverified" or flow.get("status") == "none":
        confidence = _cap_confidence(confidence, "low")
    return confidence


def data_invalid_conditions(inputs: StevenInputs) -> bool:
    if inputs.underlier_price is None:
        return True
    if inputs.underlier_source not in ANCHOR_SOURCES:
        return True
    front = front_expiry(inputs.exposure)
    if front is None or front.quality == "unavailable":
        return True
    age = front.snapshot_age_seconds
    if age is not None and age > inputs.settings.max_snapshot_age_seconds:
        return True
    return False


def _active_shock_event(shock_state: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(shock_state, dict):
        return None
    active = shock_state.get("active_event")
    if not isinstance(active, dict):
        return None
    phase = str(active.get("phase") or "")
    if phase in COMPLETED_SHOCK_PHASES:
        return None
    if phase:
        return active
    return None


def _event_wait_active(inputs: StevenInputs) -> bool:
    if _active_shock_event(inputs.shock_state) is not None:
        return True
    tags = set(inputs.event_tags) & EVENT_WAIT_TAGS
    if not tags:
        return False
    cooldown = inputs.settings.event_wait_cooldown_seconds
    # Manual/upstream tags are treated as active within the cooldown window from
    # previous_state_since when already in EVENT_WAIT, otherwise from as_of.
    if inputs.previous_state == "EVENT_WAIT" and inputs.previous_state_since is not None:
        elapsed = (inputs.as_of - inputs.previous_state_since).total_seconds()
        return elapsed < cooldown
    return True


def _event_stabilized(inputs: StevenInputs) -> bool:
    n = inputs.settings.event_stabilize_bars
    limit = inputs.settings.event_stabilize_range_points
    bars = list(inputs.bars_1m)[-n:]
    if len(bars) < n:
        return False
    return all((bar.high - bar.low) < limit for bar in bars)


def _watch_entry_ok(inputs: StevenInputs, regime: str, map_levels: Mapping[str, Any]) -> str | None:
    spot = inputs.underlier_price
    if spot is None:
        return None
    support = list(map_levels.get("support") or [])
    pin = map_levels.get("pin")
    front = front_expiry(inputs.exposure)
    settings = inputs.settings
    if regime == "bullish" and support:
        if spot - max(support) <= settings.dip_watch_max_distance_points:
            return "BULLISH_DIP_WATCH"
    if regime == "bearish" and support:
        if spot - max(support) <= settings.break_watch_max_distance_points:
            return "BEARISH_BREAK_WATCH"
    # T8: mixed may enter RANGE_PIN_WATCH when pin + gamma ratio hold
    if regime == "mixed" and pin is not None and front is not None:
        net_gamma = front.oi_weighted.net_gamma_ratio
        if (
            abs(spot - float(pin)) <= settings.pin_watch_max_distance_points
            and net_gamma is not None
            and net_gamma >= settings.pin_min_net_gamma_ratio
        ):
            return "RANGE_PIN_WATCH"
    return None


def _watch_still_valid(
    inputs: StevenInputs,
    regime: str,
    map_levels: Mapping[str, Any],
    watch_state: str,
) -> bool:
    spot = inputs.underlier_price
    if spot is None:
        return False
    support = list(map_levels.get("support") or [])
    pin = map_levels.get("pin")
    front = front_expiry(inputs.exposure)
    settings = inputs.settings
    if watch_state == "BULLISH_DIP_WATCH":
        return (
            regime == "bullish"
            and bool(support)
            and spot - max(support) <= settings.dip_watch_max_distance_points
        )
    if watch_state == "BEARISH_BREAK_WATCH":
        return (
            regime == "bearish"
            and bool(support)
            and spot - max(support) <= settings.break_watch_max_distance_points
        )
    if watch_state == "RANGE_PIN_WATCH":
        if pin is None or front is None:
            return False
        net_gamma = front.oi_weighted.net_gamma_ratio
        return (
            abs(spot - float(pin)) <= settings.pin_watch_max_distance_points
            and net_gamma is not None
            and net_gamma >= settings.pin_min_net_gamma_ratio
        )
    return False


def _target_hit(
    inputs: StevenInputs,
    map_levels: Mapping[str, Any],
    regime: str,
) -> bool:
    spot = inputs.underlier_price
    if spot is None:
        return False
    support = list(map_levels.get("support") or [])
    resistance = list(map_levels.get("resistance") or [])
    pin = map_levels.get("pin")
    if regime == "bullish" and resistance:
        return spot >= min(resistance)
    if regime == "bearish" and len(support) >= 2:
        return spot <= sorted(support)[-2]
    if regime == "bearish" and support:
        return spot <= max(support) - inputs.settings.trigger_level_tolerance_points
    if regime == "mixed" and pin is not None:
        return abs(spot - float(pin)) <= inputs.settings.trigger_level_tolerance_points
    return False


def _invalidation_confirmed(
    inputs: StevenInputs,
    invalidation: Mapping[str, Any],
) -> bool:
    level = invalidation.get("level")
    side = invalidation.get("side")
    if level is None or side not in {"above", "below"}:
        return False
    return bar_hold(
        inputs.bars_1m,
        float(level),
        side,
        inputs.settings.invalidation_hold_bars,
    )


def advance_state(
    inputs: StevenInputs,
    *,
    regime: str,
    map_levels: Mapping[str, Any],
    trigger: Mapping[str, Any],
    flow: Mapping[str, Any],
    invalidation: Mapping[str, Any],
) -> tuple[str, str | None, datetime | None, datetime | None, datetime | None, int]:
    """Return (machine_state, rule, data_healthy_since, watch_exit_since, lockout_until, daily_setup_count)."""
    prev = inputs.previous_state
    as_of = inputs.as_of
    settings = inputs.settings
    data_healthy_since = inputs.data_healthy_since
    watch_exit_since = inputs.watch_exit_since
    lockout_until = inputs.lockout_until
    daily_setup_count = inputs.daily_setup_count
    invalid = data_invalid_conditions(inputs)

    # T14 special-case before generic T1 when in SETUP_CONFIRMED
    if invalid and prev == "SETUP_CONFIRMED":
        return "EXIT_REVIEW", "T14", None, None, lockout_until, daily_setup_count

    # T1
    if invalid:
        return "DATA_INVALID", "T1", None, None, lockout_until, daily_setup_count

    if data_healthy_since is None:
        data_healthy_since = as_of

    # T17
    persisted_date = inputs.trading_date
    current_date = trading_date_et(as_of)
    if persisted_date and persisted_date != current_date:
        return "OBSERVE_ONLY", "T17", data_healthy_since, None, None, 0

    # T15
    if prev == "EXIT_REVIEW":
        lockout_until = as_of + timedelta(minutes=settings.lockout_minutes)
        return "LOCKOUT_OR_REMAP", "T15", data_healthy_since, None, lockout_until, daily_setup_count

    # T2
    if prev == "DATA_INVALID":
        held = (
            data_healthy_since is not None
            and (as_of - data_healthy_since).total_seconds() >= settings.data_recovery_hold_seconds
        )
        if held:
            return "OBSERVE_ONLY", "T2", data_healthy_since, None, lockout_until, daily_setup_count
        return "DATA_INVALID", None, data_healthy_since, None, lockout_until, daily_setup_count

    # T3
    if prev in {"OBSERVE_ONLY", "REGIME_UNKNOWN", *WATCH_STATES} and _event_wait_active(inputs):
        return "EVENT_WAIT", "T3", data_healthy_since, None, lockout_until, daily_setup_count

    # T4
    if prev == "EVENT_WAIT":
        if not _event_wait_active(inputs) and _event_stabilized(inputs):
            return "OBSERVE_ONLY", "T4", data_healthy_since, None, lockout_until, daily_setup_count
        return "EVENT_WAIT", None, data_healthy_since, None, lockout_until, daily_setup_count

    # T16
    if prev == "LOCKOUT_OR_REMAP":
        cooled = lockout_until is None or as_of >= lockout_until
        under_cap = daily_setup_count < settings.max_daily_setups
        if cooled and under_cap:
            return "OBSERVE_ONLY", "T16", data_healthy_since, None, None, daily_setup_count
        return "LOCKOUT_OR_REMAP", None, data_healthy_since, None, lockout_until, daily_setup_count

    # T13 from SETUP
    if prev == "SETUP_CONFIRMED":
        if _target_hit(inputs, map_levels, regime) or _invalidation_confirmed(inputs, invalidation):
            return "EXIT_REVIEW", "T13", data_healthy_since, None, lockout_until, daily_setup_count
        return "SETUP_CONFIRMED", None, data_healthy_since, None, lockout_until, daily_setup_count

    # T9/T10/T11 confirmations
    if prev == "BULLISH_DIP_WATCH":
        if (
            trigger.get("kind") == "dip_hold"
            and trigger.get("confirmed")
            and flow.get("status") != "opposed"
        ):
            return (
                "SETUP_CONFIRMED",
                "T9",
                data_healthy_since,
                None,
                lockout_until,
                daily_setup_count + 1,
            )
    if prev == "BEARISH_BREAK_WATCH":
        if (
            trigger.get("kind") == "break_hold"
            and trigger.get("confirmed")
            and flow.get("status") != "opposed"
        ):
            return (
                "SETUP_CONFIRMED",
                "T10",
                data_healthy_since,
                None,
                lockout_until,
                daily_setup_count + 1,
            )
    if prev == "RANGE_PIN_WATCH":
        if trigger.get("kind") == "range_reject" and trigger.get("confirmed"):
            return (
                "SETUP_CONFIRMED",
                "T11",
                data_healthy_since,
                None,
                lockout_until,
                daily_setup_count + 1,
            )

    # T12 watch exit
    if prev in WATCH_STATES:
        still = _watch_still_valid(inputs, regime, map_levels, prev)
        if still:
            return prev, None, data_healthy_since, None, lockout_until, daily_setup_count
        since = watch_exit_since or as_of
        if (as_of - since).total_seconds() >= settings.watch_exit_hold_seconds:
            return "OBSERVE_ONLY", "T12", data_healthy_since, None, lockout_until, daily_setup_count
        return prev, None, data_healthy_since, since, lockout_until, daily_setup_count

    # Entries from OBSERVE_ONLY / REGIME_UNKNOWN
    if prev in {"OBSERVE_ONLY", "REGIME_UNKNOWN"}:
        watch = _watch_entry_ok(inputs, regime, map_levels)
        if watch == "BULLISH_DIP_WATCH":
            return "BULLISH_DIP_WATCH", "T6", data_healthy_since, None, lockout_until, daily_setup_count
        if watch == "BEARISH_BREAK_WATCH":
            return "BEARISH_BREAK_WATCH", "T7", data_healthy_since, None, lockout_until, daily_setup_count
        if watch == "RANGE_PIN_WATCH":
            return "RANGE_PIN_WATCH", "T8", data_healthy_since, None, lockout_until, daily_setup_count
        if prev == "OBSERVE_ONLY" and regime in {"unknown", "mixed"}:
            # mixed can take T8 first; if not, fall through to REGIME_UNKNOWN (T5)
            if regime == "mixed" and watch is None:
                return "REGIME_UNKNOWN", "T5", data_healthy_since, None, lockout_until, daily_setup_count
            if regime == "unknown":
                return "REGIME_UNKNOWN", "T5", data_healthy_since, None, lockout_until, daily_setup_count
        return prev, None, data_healthy_since, None, lockout_until, daily_setup_count

    return prev, None, data_healthy_since, watch_exit_since, lockout_until, daily_setup_count


def _data_quality(inputs: StevenInputs) -> dict[str, Any]:
    front = front_expiry(inputs.exposure)
    anchor_ok = (
        inputs.underlier_price is not None and inputs.underlier_source in ANCHOR_SOURCES
    )
    return {
        "anchor_ok": anchor_ok,
        "exposure_quality": front.quality if front is not None else "unavailable",
        "oi_quality": front.oi_quality if front is not None else "missing",
        "iv_source": front.iv_source if front is not None else "missing",
        "snapshot_age_seconds": front.snapshot_age_seconds if front is not None else None,
    }


def _missing_input_warnings(inputs: StevenInputs) -> list[str]:
    warnings: list[str] = []
    if inputs.exposure is None:
        warnings.append("missing_exposure_map")
    if not inputs.bars_1m:
        warnings.append("missing_bars_1m")
    if inputs.shock_state is None:
        warnings.append("missing_shock_state")
    if inputs.es_volume is None:
        warnings.append("missing_es_volume")
    if inputs.hl_volume is None:
        warnings.append("missing_hl_volume")
    if inputs.underlier_price is None:
        warnings.append("missing_underlier_price")
    if inputs.underlier_source is not None and inputs.underlier_source not in ANCHOR_SOURCES:
        warnings.append(f"unsupported_underlier_source:{inputs.underlier_source}")
    return warnings


def build_steven_signal(inputs: StevenInputs) -> StevenSignal:
    try:
        return _build_steven_signal_inner(inputs)
    except Exception as exc:  # noqa: BLE001 — any input combo must not raise
        return StevenSignal(
            created_at=inputs.created_at,
            as_of=inputs.as_of,
            status="invalid",
            machine_state="DATA_INVALID",
            regime="unknown",
            regime_breadth={
                "expiries_total": 0,
                "expiries_bullish": 0,
                "expiries_bearish": 0,
                "agreement_ratio": None,
                "weighting": inputs.settings.regime_weighting,
            },
            map={"support": [], "resistance": [], "pin": None, "acceleration": []},
            trigger={
                "kind": "none",
                "level": None,
                "direction": "none",
                "confirmed": False,
                "confirmed_at": None,
                "source_event_id": None,
            },
            invalidation={"level": None, "side": "none", "reason": "build_error"},
            expression_family="none",
            confidence="low",
            flow_confirmation={"status": "none", "sources": [], "quality": "weak_proxy"},
            data_quality={
                "anchor_ok": False,
                "exposure_quality": "unavailable",
                "oi_quality": "missing",
                "iv_source": "missing",
                "snapshot_age_seconds": None,
            },
            warnings=(f"steven_build_error:{type(exc).__name__}",),
        )


def _build_steven_signal_inner(inputs: StevenInputs) -> StevenSignal:
    regime, breadth = classify_regime(inputs)
    map_levels, map_warnings = build_map_levels(inputs)
    trigger = evaluate_trigger(inputs, map_levels)
    flow = evaluate_flow(inputs, trigger)
    # Provisional invalidation for advance_state (SETUP path)
    provisional_state = inputs.previous_state
    invalidation = build_invalidation(inputs, map_levels, regime, provisional_state)
    (
        machine_state,
        rule,
        data_healthy_since,
        watch_exit_since,
        lockout_until,
        daily_setup_count,
    ) = advance_state(
        inputs,
        regime=regime,
        map_levels=map_levels,
        trigger=trigger,
        flow=flow,
        invalidation=invalidation,
    )
    # Hard gate 3: never confirm without trigger.confirmed
    if machine_state == "SETUP_CONFIRMED" and not trigger.get("confirmed"):
        machine_state = inputs.previous_state if inputs.previous_state in WATCH_STATES else "OBSERVE_ONLY"
        rule = None
        daily_setup_count = inputs.daily_setup_count
    invalidation = build_invalidation(inputs, map_levels, regime, machine_state)
    expression = candidate_expression_family(machine_state, regime)
    data_quality = _data_quality(inputs)
    confidence = classify_confidence(
        data_quality=data_quality,
        flow=flow,
        regime=regime,
        trigger=trigger,
    )
    warnings = list(_missing_input_warnings(inputs))
    warnings.extend(map_warnings)
    if inputs.exposure is not None:
        warnings.extend(inputs.exposure.warnings)
        front = front_expiry(inputs.exposure)
        if front is not None:
            warnings.extend(front.warnings)
    status = status_for_machine_state(machine_state)
    if status == "confirmed_for_review" and not trigger.get("confirmed"):
        status = "watch"
        machine_state = (
            inputs.previous_state if inputs.previous_state in WATCH_STATES else "OBSERVE_ONLY"
        )
        expression = "none"
    return StevenSignal(
        created_at=inputs.created_at,
        as_of=inputs.as_of,
        status=status,
        machine_state=machine_state,
        regime=regime,
        regime_breadth=breadth,
        map=map_levels,
        trigger=trigger,
        invalidation=invalidation,
        expression_family=expression,
        confidence=confidence,
        flow_confirmation=flow,
        data_quality=data_quality,
        warnings=tuple(dict.fromkeys(warnings)),
        transition_rule=rule,
        data_healthy_since=data_healthy_since,
        watch_exit_since=watch_exit_since,
        lockout_until=lockout_until,
        daily_setup_count=daily_setup_count,
    )


def steven_context_note(
    steven_state: Mapping[str, Any] | None,
    *,
    as_of: datetime,
    settings: StevenSettings | None = None,
) -> str | None:
    """Return a single-line observe-only note or None. Pure; no IO."""
    settings = settings or StevenSettings.from_env()
    if not settings.alert_context_enabled:
        return None
    if not isinstance(steven_state, Mapping):
        return None
    if steven_state.get("schema_version") != STATE_SCHEMA_VERSION:
        return None
    updated_raw = steven_state.get("updated_at")
    try:
        updated_at = _as_utc(datetime.fromisoformat(str(updated_raw)))
    except (TypeError, ValueError):
        return None
    age = (_as_utc(as_of) - updated_at).total_seconds()
    if age > settings.alert_context_max_age_seconds:
        return None
    contract = steven_state.get("contract")
    if not isinstance(contract, Mapping):
        return None
    machine_state = steven_state.get("machine_state") or contract.get("machine_state") or "?"
    regime = contract.get("regime") or "?"
    confidence = contract.get("confidence") or "?"
    map_levels = contract.get("map") if isinstance(contract.get("map"), Mapping) else {}
    support = list(map_levels.get("support") or [])
    resistance = list(map_levels.get("resistance") or [])
    support_txt = f"{support[0]:.0f}" if support else "-"
    resistance_txt = f"{resistance[0]:.0f}" if resistance else "-"
    note = (
        f"[Steven observe_only] state={machine_state} regime={regime} conf={confidence} "
        f"support={support_txt}/resistance={resistance_txt}；代理指标，非交易信号"
    )
    if len(note) > 200:
        note = note[:197] + "..."
    return note


def annotate_alerts_with_steven_context(
    alerts: Sequence[Alert],
    steven_state: Mapping[str, Any] | None,
    *,
    as_of: datetime,
    settings: StevenSettings | None = None,
) -> list[Alert]:
    settings = settings or StevenSettings.from_env()
    if not settings.alert_context_enabled:
        return list(alerts)
    try:
        note = steven_context_note(steven_state, as_of=as_of, settings=settings)
    except Exception:  # noqa: BLE001
        return list(alerts)
    if not note:
        return list(alerts)
    annotated: list[Alert] = []
    for alert in alerts:
        if alert.kind in ALERT_CONTEXT_KINDS:
            annotated.append(replace(alert, detail=f"{alert.detail}\n{note}"))
        else:
            annotated.append(alert)
    return annotated


def load_steven_state(path: Path | str) -> tuple[dict[str, Any] | None, str | None]:
    """Load steven_state.json. Returns (payload, reset_reason)."""
    state_path = Path(path)
    if not state_path.exists():
        return None, "missing"
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return None, f"corrupt:{type(exc).__name__}"
    if not isinstance(payload, dict):
        return None, "corrupt:not_object"
    if payload.get("schema_version") != STATE_SCHEMA_VERSION:
        return None, "schema_mismatch"
    return payload, None


def _parse_optional_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        return _as_utc(datetime.fromisoformat(str(value)))
    except (TypeError, ValueError):
        return None


def persist_steven_state(
    signal: StevenSignal,
    *,
    data_root: Path | str,
    trading_date: str,
    episode_seq_last: int,
    previous_payload: Mapping[str, Any] | None = None,
    transition_rule: str | None = None,
) -> dict[str, Any]:
    path = Path(data_root) / "latest" / "steven_state.json"
    history: list[dict[str, Any]] = []
    if isinstance(previous_payload, Mapping):
        raw_history = previous_payload.get("transition_history")
        if isinstance(raw_history, list):
            history = [row for row in raw_history if isinstance(row, dict)]
    prev_state = None
    if isinstance(previous_payload, Mapping):
        prev_state = previous_payload.get("machine_state")
    if transition_rule and prev_state and prev_state != signal.machine_state:
        history.append(
            {
                "at": signal.as_of.isoformat(),
                "from": prev_state,
                "to": signal.machine_state,
                "rule": transition_rule,
            }
        )
    history = history[-50:]
    state_since = signal.as_of.isoformat()
    if (
        isinstance(previous_payload, Mapping)
        and previous_payload.get("machine_state") == signal.machine_state
        and previous_payload.get("state_since")
    ):
        state_since = str(previous_payload.get("state_since"))
    payload = {
        "schema_version": STATE_SCHEMA_VERSION,
        "trading_date": trading_date,
        "machine_state": signal.machine_state,
        "state_since": state_since,
        "updated_at": signal.as_of.isoformat(),
        "episode_id": episode_id_for(trading_date),
        "episode_seq_last": episode_seq_last,
        "daily_setup_count": signal.daily_setup_count,
        "lockout_until": signal.lockout_until.isoformat() if signal.lockout_until else None,
        "data_healthy_since": (
            signal.data_healthy_since.isoformat() if signal.data_healthy_since else None
        ),
        "watch_exit_since": (
            signal.watch_exit_since.isoformat() if signal.watch_exit_since else None
        ),
        "contract": signal.to_dict(),
        "transition_history": history,
    }
    atomic_write_json_secure(path, payload)
    return payload


def append_episode_event(
    *,
    data_root: Path | str,
    trading_date: str,
    seq: int,
    recorded_at: datetime,
    event_kind: str,
    from_state: str | None,
    to_state: str,
    contract: Mapping[str, Any],
    note: str,
) -> dict[str, Any]:
    recorded = _as_utc(recorded_at)
    contract_as_of = _parse_optional_dt(contract.get("as_of"))
    if contract_as_of is not None and recorded < contract_as_of:
        raise ValueError("retrospective episode timestamps are not allowed")
    if RETROSPECTIVE_SOURCES_ALLOWED:
        raise ValueError("retrospective sources must remain disabled")
    event = {
        "schema_version": EPISODE_SCHEMA_VERSION,
        "episode_id": episode_id_for(trading_date),
        "trading_date": trading_date,
        "seq": seq,
        "recorded_at": recorded.isoformat(),
        "event_kind": event_kind,
        "from_state": from_state,
        "to_state": to_state,
        "contract": dict(contract),
        "note": note,
    }
    directory = Path(data_root) / "lake" / "steven" / "episodes" / f"date={trading_date}"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "episode.jsonl"
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True) + "\n")
    return event


def _map_level_moved(
    previous_map: Mapping[str, Any] | None,
    current_map: Mapping[str, Any],
    *,
    min_move: float,
) -> bool:
    if not isinstance(previous_map, Mapping):
        return False
    for key in ("support", "resistance", "acceleration"):
        prev_levels = list(previous_map.get(key) or [])
        curr_levels = list(current_map.get(key) or [])
        if not prev_levels or not curr_levels:
            continue
        if abs(float(prev_levels[0]) - float(curr_levels[0])) >= min_move:
            return True
    prev_pin = previous_map.get("pin")
    curr_pin = current_map.get("pin")
    if prev_pin is not None and curr_pin is not None and abs(float(prev_pin) - float(curr_pin)) >= min_move:
        return True
    return False


def maybe_append_episode_revision(
    *,
    data_root: Path | str,
    trading_date: str,
    signal: StevenSignal,
    previous_payload: Mapping[str, Any] | None,
    settings: StevenSettings,
) -> int:
    """Append episode row on edges / map moves. Returns new episode_seq_last."""
    seq_last = -1
    prev_state = None
    prev_contract = None
    if isinstance(previous_payload, Mapping):
        prev_date = previous_payload.get("trading_date")
        if prev_date == trading_date:
            raw_seq = previous_payload.get("episode_seq_last")
            if isinstance(raw_seq, int):
                seq_last = raw_seq
            prev_state = previous_payload.get("machine_state")
            prev_contract = previous_payload.get("contract")
        # Trading-date rollover starts a fresh episode file/seq.
    contract = signal.to_dict()
    if seq_last < 0:
        append_episode_event(
            data_root=data_root,
            trading_date=trading_date,
            seq=0,
            recorded_at=signal.as_of,
            event_kind="pre_market_map",
            from_state=None,
            to_state=signal.machine_state,
            contract=contract,
            note="initial daily evaluation",
        )
        seq_last = 0
        if prev_state and prev_state != signal.machine_state:
            # still record transition if we came from a prior day reset with same file
            pass
        return seq_last

    events: list[tuple[str, str]] = []
    if prev_state and prev_state != signal.machine_state:
        kind = "state_transition"
        if signal.machine_state == "SETUP_CONFIRMED":
            kind = "trigger"
        if signal.machine_state == "LOCKOUT_OR_REMAP" and prev_state == "EXIT_REVIEW":
            kind = "final_state"
        note = signal.transition_rule or f"{prev_state}->{signal.machine_state}"
        events.append((kind, note))
    elif isinstance(prev_contract, Mapping) and _map_level_moved(
        prev_contract.get("map") if isinstance(prev_contract.get("map"), Mapping) else None,
        signal.map,
        min_move=settings.episode_revision_min_level_move_points,
    ):
        events.append(("map_revision", "map level moved beyond threshold"))

    for kind, note in events:
        seq_last += 1
        append_episode_event(
            data_root=data_root,
            trading_date=trading_date,
            seq=seq_last,
            recorded_at=signal.as_of,
            event_kind=kind,
            from_state=str(prev_state) if prev_state else None,
            to_state=signal.machine_state,
            contract=contract,
            note=note,
        )
    return seq_last


def fold_episode_summary(events: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not events:
        return {
            "episode_id": None,
            "trading_date": None,
            "pre_market_map": None,
            "triggers": [],
            "revisions": [],
            "final_state": None,
            "setup_count": 0,
            "forward_metrics": None,
        }
    first = events[0]
    pre_market = None
    for event in events:
        if event.get("event_kind") == "pre_market_map":
            contract = event.get("contract") if isinstance(event.get("contract"), Mapping) else {}
            pre_market = {
                "map": contract.get("map"),
                "regime": contract.get("regime"),
                "data_quality": contract.get("data_quality"),
            }
            break
    triggers = [
        (event.get("contract") or {}).get("trigger")
        for event in events
        if event.get("event_kind") == "trigger"
    ]
    revisions = [
        {
            "seq": event.get("seq"),
            "from_state": event.get("from_state"),
            "to_state": event.get("to_state"),
            "recorded_at": event.get("recorded_at"),
        }
        for event in events
        if event.get("event_kind") in {"state_transition", "trigger", "map_revision", "final_state"}
    ]
    final_state = None
    for event in reversed(events):
        if event.get("event_kind") == "final_state":
            final_state = event.get("to_state")
            break
    setup_count = sum(1 for event in events if event.get("to_state") == "SETUP_CONFIRMED")
    return {
        "episode_id": first.get("episode_id"),
        "trading_date": first.get("trading_date"),
        "pre_market_map": pre_market,
        "triggers": triggers,
        "revisions": revisions,
        "final_state": final_state,
        "setup_count": setup_count,
        "forward_metrics": None,
    }


def _session_phase_for(as_of: datetime) -> str:
    local = _as_utc(as_of).astimezone(ET)
    minutes = local.hour * 60 + local.minute
    if minutes < 9 * 60 + 30:
        return "premarket"
    if minutes < 10 * 60 + 30:
        return "open"
    if minutes < 15 * 60:
        return "midday"
    if minutes < 16 * 60:
        return "late"
    return "closed"


def _quote_source_at(quote: Any) -> datetime:
    return _as_utc(quote.quote_time or quote.trade_time or quote.received_at)


def _underlier_from_state(state: LatestState) -> tuple[float | None, str | None]:
    """Hard gate 6: only index:SPX or chain_implied — never Hyperliquid SP500."""
    quote = state.best_quote("index:SPX")
    if quote is not None:
        decision = configured_quote_use_decision(quote, as_of=state.as_of)
        price = quote.effective_price
        if decision.pricing_allowed and price is not None and price > 0:
            return float(price), "index:SPX"
    # Prefer exposure/options chain_implied via build_exposure_map underlier when available.
    try:
        exposure = build_exposure_map(state)
    except Exception:  # noqa: BLE001
        exposure = None
    if exposure is not None and getattr(exposure.underlier, "source", None) == "chain_implied":
        price = getattr(exposure.underlier, "price", None)
        if price is not None and price > 0:
            return float(price), "chain_implied"
    if exposure is not None and getattr(exposure.underlier, "source", None) == "index:SPX":
        price = getattr(exposure.underlier, "price", None)
        if price is not None and price > 0:
            return float(price), "index:SPX"
    return None, None


def _load_json_mapping(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _load_bars_from_latest(
    data_root: Path,
    *,
    as_of: datetime,
    max_age_seconds: float,
) -> tuple[tuple[SpxBar, ...], tuple[SpxBar, ...]]:
    path_1m = data_root / "latest" / "spx_bars_1m.json"
    path_5m = data_root / "latest" / "spx_bars_5m.json"
    payload_1m = _load_json_mapping(path_1m)
    if payload_1m is None:
        return (), ()
    updated = _parse_optional_dt(payload_1m.get("updated_at"))
    if updated is None or (_as_utc(as_of) - updated).total_seconds() > max_age_seconds:
        return (), ()

    def _parse_bars(payload: Mapping[str, Any] | None) -> tuple[SpxBar, ...]:
        if payload is None:
            return ()
        rows = payload.get("bars")
        if not isinstance(rows, list):
            return ()
        bars: list[SpxBar] = []
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            start = _parse_optional_dt(row.get("bar_start"))
            if start is None:
                continue
            bars.append(
                SpxBar(
                    bar_start=start,
                    interval_seconds=int(row.get("interval_seconds") or 60),
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    sample_count=int(row.get("sample_count") or 0),
                    quality=str(row.get("quality") or "partial"),
                    gap_before=bool(row.get("gap_before")),
                    provider=str(row.get("provider") or "unknown"),
                )
            )
        return tuple(bars)

    return _parse_bars(payload_1m), _parse_bars(_load_json_mapping(path_5m))


def inputs_from_latest_state(
    state: LatestState,
    *,
    data_root: Path | str | None = None,
    exposure: ExposureMap | None = None,
    bars_1m: tuple[SpxBar, ...] | None = None,
    bars_5m: tuple[SpxBar, ...] | None = None,
    shock_state: dict[str, Any] | None = None,
    es_volume: dict[str, Any] | None = None,
    hl_volume: dict[str, Any] | None = None,
    event_tags: Iterable[str] = (),
    previous_payload: Mapping[str, Any] | None = None,
    settings: StevenSettings | None = None,
    reset_warning: str | None = None,
) -> StevenInputs:
    settings = settings or StevenSettings.from_env()
    root = Path(data_root) if data_root is not None else Path(StorageSettings.from_env().data_root)
    underlier_price, underlier_source = _underlier_from_state(state)
    if exposure is None:
        try:
            exposure = build_exposure_map(state)
        except Exception:  # noqa: BLE001
            exposure = None
    if exposure is not None and underlier_price is None:
        src = getattr(exposure.underlier, "source", None)
        price = getattr(exposure.underlier, "price", None)
        if src in ANCHOR_SOURCES and price is not None:
            underlier_price = float(price)
            underlier_source = str(src)
    if bars_1m is None or bars_5m is None:
        loaded_1m, loaded_5m = _load_bars_from_latest(
            root,
            as_of=state.as_of,
            max_age_seconds=settings.bars_source_max_age_seconds,
        )
        if bars_1m is None:
            bars_1m = loaded_1m
        if bars_5m is None:
            bars_5m = loaded_5m
    if shock_state is None:
        shock_state = _load_json_mapping(root / "latest" / "intraday_shock_state.json")
    if es_volume is None:
        order_map = _load_json_mapping(root / "latest" / "order_map.json")
        if isinstance(order_map, dict):
            payload = order_map.get("es_volume_signal")
            es_volume = payload if isinstance(payload, dict) else None
            hl_payload = order_map.get("hl_volume_signal")
            if hl_volume is None:
                hl_volume = hl_payload if isinstance(hl_payload, dict) else None
    tags = tuple(event_tags)
    if not tags:
        raw_tags = runtime_value("human_focus.event_tags")
        if isinstance(raw_tags, list):
            tags = tuple(str(item) for item in raw_tags)

    previous_state = "OBSERVE_ONLY"
    previous_state_since = None
    trading_date = None
    daily_setup_count = 0
    lockout_until = None
    data_healthy_since = None
    watch_exit_since = None
    if isinstance(previous_payload, Mapping):
        previous_state = str(previous_payload.get("machine_state") or "OBSERVE_ONLY")
        previous_state_since = _parse_optional_dt(previous_payload.get("state_since"))
        trading_date = (
            str(previous_payload["trading_date"])
            if previous_payload.get("trading_date")
            else None
        )
        daily_setup_count = int(previous_payload.get("daily_setup_count") or 0)
        lockout_until = _parse_optional_dt(previous_payload.get("lockout_until"))
        data_healthy_since = _parse_optional_dt(previous_payload.get("data_healthy_since"))
        watch_exit_since = _parse_optional_dt(previous_payload.get("watch_exit_since"))

    return StevenInputs(
        created_at=datetime.now(tz=timezone.utc),
        as_of=state.as_of,
        underlier_price=underlier_price,
        underlier_source=underlier_source,
        exposure=exposure,
        bars_1m=tuple(bars_1m or ()),
        bars_5m=tuple(bars_5m or ()),
        shock_state=shock_state,
        es_volume=es_volume,
        hl_volume=hl_volume,
        session_phase=_session_phase_for(state.as_of),
        event_tags=tags,
        previous_state=previous_state,
        previous_state_since=previous_state_since,
        trading_date=trading_date,
        daily_setup_count=daily_setup_count,
        lockout_until=lockout_until,
        data_healthy_since=data_healthy_since,
        watch_exit_since=watch_exit_since,
        settings=settings,
    )


def _ingest_spx_bar_sample(
    builder: SpxBarBuilder,
    state: LatestState,
) -> None:
    quote = state.best_quote("index:SPX")
    if quote is None:
        return
    if not configured_quote_use_decision(quote, as_of=state.as_of).pricing_allowed:
        return
    price = quote.effective_price
    if price is None or price <= 0:
        return
    builder.ingest(_quote_source_at(quote), float(price), quote.provider.value)


def evaluate_steven_cycle(
    state: LatestState,
    *,
    data_root: Path | str | None = None,
    settings: StevenSettings | None = None,
    bar_builder: SpxBarBuilder | None = None,
    persist: bool = True,
) -> dict[str, Any]:
    settings = settings or StevenSettings.from_env()
    if not settings.enabled:
        return {"enabled": False, "skipped": True}
    root = Path(data_root) if data_root is not None else Path(StorageSettings.from_env().data_root)
    previous_payload, reset_reason = load_steven_state(root / "latest" / "steven_state.json")
    exposure = build_exposure_map(state)
    if persist:
        persist_exposure_map(exposure, root)

    builder = bar_builder or SpxBarBuilder()
    if bar_builder is None:
        bars_1m, bars_5m = _load_bars_from_latest(
            root,
            as_of=state.as_of,
            max_age_seconds=settings.bars_source_max_age_seconds,
        )
        for bar in bars_1m:
            builder._closed_1m.append(bar)
        for bar in bars_5m:
            builder._closed_5m.append(bar)
    _ingest_spx_bar_sample(builder, state)
    trading_date = trading_date_et(state.as_of)
    if persist:
        builder.persist(root, as_of=state.as_of, trading_date=trading_date)

    inputs = inputs_from_latest_state(
        state,
        data_root=root,
        exposure=exposure,
        bars_1m=builder.closed_bars_1m(),
        bars_5m=builder.closed_bars_5m(),
        previous_payload=previous_payload,
        settings=settings,
        reset_warning=reset_reason,
    )
    signal = build_steven_signal(inputs)
    warnings = list(signal.warnings)
    if reset_reason:
        warnings.append(f"steven_state_reset:{reset_reason}")
        signal = replace(signal, warnings=tuple(dict.fromkeys(warnings)))

    seq_last = -1
    if isinstance(previous_payload, Mapping) and isinstance(
        previous_payload.get("episode_seq_last"), int
    ):
        seq_last = int(previous_payload["episode_seq_last"])
    if persist:
        seq_last = maybe_append_episode_revision(
            data_root=root,
            trading_date=trading_date,
            signal=signal,
            previous_payload=previous_payload,
            settings=settings,
        )
        persist_steven_state(
            signal,
            data_root=root,
            trading_date=trading_date,
            episode_seq_last=seq_last,
            previous_payload=previous_payload,
            transition_rule=signal.transition_rule,
        )
    return {
        "enabled": True,
        "skipped": False,
        "trading_date": trading_date,
        "machine_state": signal.machine_state,
        "status": signal.status,
        "episode_seq_last": seq_last,
        "contract": signal.to_dict(),
        "warnings": list(signal.warnings),
    }


def load_steven_state_for_alerts(data_root: Path | str | None = None) -> dict[str, Any] | None:
    root = Path(data_root) if data_root is not None else Path(StorageSettings.from_env().data_root)
    payload, _reason = load_steven_state(root / "latest" / "steven_state.json")
    return payload


def validate_contract_dict(contract: Mapping[str, Any]) -> list[str]:
    """Lightweight schema checks without requiring jsonschema dependency."""
    errors: list[str] = []
    required = {
        "schema_version",
        "source",
        "created_at",
        "as_of",
        "status",
        "machine_state",
        "regime",
        "regime_breadth",
        "map",
        "trigger",
        "invalidation",
        "expression_family",
        "confidence",
        "flow_confirmation",
        "data_quality",
        "warnings",
    }
    missing = required - set(contract)
    if missing:
        errors.append(f"missing:{sorted(missing)}")
    if contract.get("schema_version") != CONTRACT_SCHEMA_VERSION:
        errors.append("schema_version")
    if contract.get("source") != CONTRACT_SOURCE:
        errors.append("source")
    if contract.get("status") not in {"observe_only", "watch", "confirmed_for_review", "invalid"}:
        errors.append("status")
    if contract.get("machine_state") not in MACHINE_STATES:
        errors.append("machine_state")
    if contract.get("regime") not in {"bullish", "bearish", "mixed", "unknown"}:
        errors.append("regime")
    if contract.get("expression_family") not in EXPRESSION_FAMILIES:
        errors.append("expression_family")
    if contract.get("confidence") not in {"low", "medium", "high"}:
        errors.append("confidence")
    if set(contract) - required:
        errors.append(f"additionalProperties:{sorted(set(contract) - required)}")
    return errors


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate Steven observe-only guidance.")
    parser.add_argument("--json", action="store_true", help="Print JSON summary.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Run even when steven.enabled is false (still observe_only).",
    )
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = StevenSettings.from_env()
    if not settings.enabled and not args.force:
        payload = {"enabled": False, "skipped": True}
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        return 0
    if not settings.enabled and args.force:
        settings = replace(settings, enabled=True)
    storage = StorageSettings.from_env()
    state = LatestStateStore(storage).load()
    result = evaluate_steven_cycle(state, data_root=storage.data_root, settings=settings)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(
            f"Steven {result.get('status')} state={result.get('machine_state')} "
            f"date={result.get('trading_date')}"
        )
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
