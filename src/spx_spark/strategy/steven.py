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

from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import datetime
from typing import Any

from spx_spark.alert_model import Alert
from spx_spark.features.bar_builder import SpxBar, bar_hold
from spx_spark.features.exposure_map import (
    ExposureMap,
    ExpiryExposure,
    net_dex_proxy_by_expiry,
)
from spx_spark.market_calendar import ET


from spx_spark.strategy.steven_models import (
    ALERT_CONTEXT_KINDS,
    ANCHOR_SOURCES,
    COMPLETED_SHOCK_PHASES,
    CONFIDENCE_ORDER,
    CONTRACT_SCHEMA_VERSION,
    CONTRACT_SOURCE,
    EPISODE_SCHEMA_VERSION,
    EVENT_WAIT_TAGS,
    EXPRESSION_FAMILIES,
    MACHINE_STATES,
    RETROSPECTIVE_SOURCES_ALLOWED,
    STATE_SCHEMA_VERSION,
    WATCH_STATES,
    StevenInputs,
    StevenSettings,
    StevenSignal,
    _as_utc,
)


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
    if front.walls.pin_candidate is not None and net_gamma is not None and net_gamma > 0:
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
    if (
        machine_state in {"BULLISH_DIP_WATCH", "SETUP_CONFIRMED"}
        and regime == "bullish"
        and support
    ):
        return {
            "level": max(support),
            "side": "below",
            "reason": "close_below_support_invalidates_bullish_dip",
        }
    if (
        machine_state in {"BEARISH_BREAK_WATCH", "SETUP_CONFIRMED"}
        and regime == "bearish"
        and support
    ):
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


from spx_spark.strategy.steven_machine import advance_state  # noqa: E402


def _data_quality(inputs: StevenInputs) -> dict[str, Any]:
    front = front_expiry(inputs.exposure)
    anchor_ok = inputs.underlier_price is not None and inputs.underlier_source in ANCHOR_SOURCES
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
        machine_state = (
            inputs.previous_state if inputs.previous_state in WATCH_STATES else "OBSERVE_ONLY"
        )
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


from spx_spark.strategy.steven_repository import (  # noqa: E402
    load_steven_state,
    persist_steven_state,
    append_episode_event,
    maybe_append_episode_revision,
    fold_episode_summary,
)

from spx_spark.strategy.steven_runtime import (  # noqa: E402
    inputs_from_latest_state,
    evaluate_steven_cycle,
    load_steven_state_for_alerts,
    validate_contract_dict,
    parse_args,
    run,
    main,
)

__all__ = [
    "CONTRACT_SCHEMA_VERSION",
    "CONTRACT_SOURCE",
    "EPISODE_SCHEMA_VERSION",
    "EXPRESSION_FAMILIES",
    "MACHINE_STATES",
    "RETROSPECTIVE_SOURCES_ALLOWED",
    "STATE_SCHEMA_VERSION",
    "load_steven_state",
    "persist_steven_state",
    "append_episode_event",
    "maybe_append_episode_revision",
    "fold_episode_summary",
    "inputs_from_latest_state",
    "evaluate_steven_cycle",
    "load_steven_state_for_alerts",
    "validate_contract_dict",
    "parse_args",
    "run",
    "main",
    "StevenInputs",
    "StevenSettings",
    "StevenSignal",
]

if __name__ == "__main__":
    main()
