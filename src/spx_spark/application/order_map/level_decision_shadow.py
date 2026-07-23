"""Stateful shadow runner for the mutually-exclusive wall/flip machine."""

from __future__ import annotations

import fcntl
import json
import os
from dataclasses import asdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Mapping

from spx_spark.application.order_map.level_decision_machine import (
    LevelDecisionSettings,
    LevelObservation,
    LevelPhase,
    advance_level_decision,
    empty_level_state,
)
from spx_spark.application.order_map.level_decision_outcomes import advance_level_outcomes
from spx_spark.application.order_map.level_decision_outcomes import LevelOutcomeSettings
from spx_spark.application.order_map.trigger_coordinates import resolve_trigger_coordinate
from spx_spark.application.order_map.stable_structure import advance_stable_structure
from spx_spark.config import NotificationSettings, StorageSettings
from spx_spark.domain.analytics import AnalyticsStatus
from spx_spark.ibkr.atm_reference import (
    BASIS_MAX_ABS_POINTS,
    BASIS_MAX_TRADING_DAY_AGE,
    BASIS_MIN_SAMPLES,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET
from spx_spark.marketdata import MarketDataQuality
from spx_spark.notifier.dispatcher import enqueue_notification
from spx_spark.notifier.receipts import NotificationEnvelope
from spx_spark.options_map import build_options_map
from spx_spark.schwab.symbols import active_quarterly_contract_month
from spx_spark.settings.level_decision import LevelDecisionPolicy
from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock
from spx_spark.storage import LatestStateStore, configured_quote_use_decision

if TYPE_CHECKING:
    from spx_spark.application.realtime.contracts import EngineTick


class LevelDecisionShadowError(RuntimeError):
    pass


def default_level_decision_state_path(storage: StorageSettings) -> Path:
    return Path(storage.data_root) / "latest" / "level_decision_shadow_state.json"


def load_level_decision_shadow(storage: StorageSettings) -> dict[str, object]:
    path = default_level_decision_state_path(storage)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _public_state(empty_level_state(datetime.now(tz=timezone.utc)))
    decision = payload.get("decision") if isinstance(payload, dict) else None
    structure = payload.get("structure") if isinstance(payload, dict) else None
    structure_stability = (
        payload.get("structure_stability") if isinstance(payload, dict) else None
    )
    latest_observation = payload.get("latest_observation") if isinstance(payload, dict) else None
    return _public_state(
        decision if isinstance(decision, Mapping) else {},
        formal_signal_enabled=bool(payload.get("formal_signal_enabled")),
        structure=structure if isinstance(structure, Mapping) else None,
        structure_stability=(
            structure_stability if isinstance(structure_stability, Mapping) else None
        ),
        latest_observation=(
            latest_observation if isinstance(latest_observation, Mapping) else None
        ),
    )


def run_level_decision_shadow(
    storage: StorageSettings,
    tick: EngineTick | None,
    *,
    now: datetime,
    policy: LevelDecisionPolicy | None = None,
    notifications_enabled: bool = False,
) -> dict[str, object]:
    policy = policy or LevelDecisionPolicy()
    if not policy.enabled:
        return {"status": "disabled", "actionable": False}
    session = _rth_session(now)

    settings = _machine_settings(policy)
    state_path = default_level_decision_state_path(storage)
    with exclusive_state_lock(state_path):
        persisted = _load_state(state_path)
        live_structure = _live_structure(tick, now=now)
        stability_state, promoted_structure = advance_stable_structure(
            persisted.get("structure_stability")
            if isinstance(persisted.get("structure_stability"), Mapping)
            else {"stable": persisted.get("structure")},
            live_structure,
            now=now,
            interval_seconds=policy.structure_interval_seconds,
            required_confirmations=policy.structure_required_confirmations,
            band_half_width_points=policy.structure_band_half_width_points,
            switch_min_points=policy.structure_switch_min_points,
        )
        frozen_structure = promoted_structure or persisted.get("structure")
        structure_candidate = stability_state.get("candidate")
        structure_change_pending = bool(
            isinstance(structure_candidate, Mapping)
            and _structure_levels(structure_candidate)
        )
        previous = persisted.get("decision")
        observation = _observation(
            storage,
            tick,
            now=now,
            session_date=session or _research_session_date(now),
            frozen_structure=(frozen_structure if isinstance(frozen_structure, Mapping) else None),
            max_frozen_structure_age_sessions=policy.max_frozen_structure_age_sessions,
            active_decision=previous if isinstance(previous, Mapping) else None,
            structure_change_pending=structure_change_pending,
        )
        transition = advance_level_decision(
            previous if isinstance(previous, Mapping) else None,
            observation,
            settings=settings,
        )
        confirmed_now = transition.changed and transition.current_phase is LevelPhase.CONFIRMED
        outcome_state, completed = advance_level_outcomes(
            persisted.get("outcomes") if isinstance(persisted.get("outcomes"), Mapping) else None,
            decision=transition.state,
            spot=observation.spot if observation.quality_ok else None,
            at=now,
            confirmed_now=confirmed_now,
            settings=_outcome_settings(policy),
        )
        payload = {
            "schema_version": 1,
            "mode": "live" if policy.formal_signal_enabled else "shadow",
            "formal_signal_enabled": policy.formal_signal_enabled,
            "decision": transition.state,
            "outcomes": outcome_state,
            "structure": frozen_structure,
            "structure_stability": stability_state,
            "latest_observation": {
                "spot": observation.spot,
                "es": observation.es,
                "quality_ok": observation.quality_ok,
                "quality_reason": observation.quality_reason,
                "spot_source": observation.spot_source,
                "level_source": observation.level_source,
                "trigger_coordinate_kind": observation.trigger_coordinate_kind,
                "trigger_instrument_id": observation.trigger_instrument_id,
                "trigger_basis_points": observation.trigger_basis_points,
                "spx_levels": dict(observation.spx_levels or {}),
                "spx_spot": observation.spx_spot,
            },
            "updated_at": _utc(now).isoformat(),
        }
        atomic_write_json_secure(state_path, payload)
        _append_record(
            _health_path(storage, now),
            _health_record(
                observation,
                transition,
                rth=session is not None,
                formal_signal_enabled=policy.formal_signal_enabled,
                tick=tick,
                settings=settings,
                stable_structure=(
                    frozen_structure if isinstance(frozen_structure, Mapping) else None
                ),
                structure_stability=stability_state,
            ),
        )
        if transition.changed:
            _append_unique(
                _audit_path(storage, now),
                _transition_record(
                    transition,
                    observation,
                    formal_signal_enabled=policy.formal_signal_enabled,
                ),
            )
        for row in completed:
            _append_unique(_outcome_path(storage, now), row)

    delivery = _deliver_transition(
        transition,
        observation,
        storage=storage,
        now=now,
        notify_transitions=policy.notify_transitions,
        formal_signal_enabled=policy.formal_signal_enabled,
        invalidation_buffer_points=policy.break_buffer_points,
        notifications_enabled=notifications_enabled,
    )

    public = _public_state(
        transition.state,
        formal_signal_enabled=policy.formal_signal_enabled,
        structure=frozen_structure if isinstance(frozen_structure, Mapping) else None,
        structure_stability=stability_state,
        latest_observation={
            "spot": observation.spot,
            "es": observation.es,
            "quality_ok": observation.quality_ok,
            "quality_reason": observation.quality_reason,
            "spot_source": observation.spot_source,
            "level_source": observation.level_source,
            "trigger_coordinate_kind": observation.trigger_coordinate_kind,
            "trigger_instrument_id": observation.trigger_instrument_id,
            "trigger_basis_points": observation.trigger_basis_points,
            "spx_levels": dict(observation.spx_levels or {}),
            "spx_spot": observation.spx_spot,
        },
    )
    public.update(
        {
            "status": "updated",
            "changed": transition.changed,
            "quality_ok": observation.quality_ok,
            "quality_reason": observation.quality_reason,
            "spot_source": observation.spot_source,
            "level_source": observation.level_source,
            "completed_outcomes": len(completed),
            "delivery": delivery,
        }
    )
    return public


def _machine_settings(policy: LevelDecisionPolicy) -> LevelDecisionSettings:
    return LevelDecisionSettings(
        approach_points=policy.approach_points,
        test_points=policy.test_points,
        break_buffer_points=policy.break_buffer_points,
        reject_points=policy.reject_points,
        accept_hold_seconds=policy.accept_hold_seconds,
        retest_points=policy.retest_points,
        confirm_move_points=policy.confirm_move_points,
        confirm_hold_seconds=policy.confirm_hold_seconds,
        phase_timeout_seconds=policy.phase_timeout_seconds,
        event_ttl_seconds=policy.event_ttl_seconds,
        data_grace_seconds=policy.data_grace_seconds,
        structure_drift_points=policy.structure_drift_points,
        es_confirm_ratio=policy.es_confirm_ratio,
        terminal_rearm_seconds=policy.terminal_rearm_seconds,
    )


def _outcome_settings(policy: LevelDecisionPolicy) -> LevelOutcomeSettings:
    return LevelOutcomeSettings(
        horizons_seconds=policy.outcome_horizons_seconds,
        sample_tolerance_seconds=policy.outcome_sample_tolerance_seconds,
        no_follow_through_mfe_bps=policy.outcome_no_follow_through_mfe_bps,
        false_confirmation_mae_bps=policy.outcome_false_confirmation_mae_bps,
        follow_through_end_bps=policy.outcome_follow_through_end_bps,
        retention_seconds=policy.outcome_retention_seconds,
    )


def _observation(
    storage: StorageSettings,
    tick: EngineTick,
    *,
    now: datetime,
    session_date: str,
    frozen_structure: Mapping[str, object] | None = None,
    max_frozen_structure_age_sessions: int = 1,
    active_decision: Mapping[str, object] | None = None,
    structure_change_pending: bool = False,
) -> LevelObservation:
    structure_age = _structure_session_age(frozen_structure, now=now)
    structure_usable = (
        structure_age is not None and structure_age <= max_frozen_structure_age_sessions
    )
    spx_levels = _structure_levels(frozen_structure) if structure_usable else {}
    level_source = str((frozen_structure or {}).get("source") or "unavailable")
    quality_reasons: list[str] = []
    if structure_change_pending:
        quality_reasons.append("structure_change_pending")
    if frozen_structure is not None and not structure_usable:
        quality_reasons.append("frozen_structure_session_ttl_expired")
    state = LatestStateStore(storage).load(now=now)
    es_quote = state.best_quote("future:ES")
    es = _positive_float(es_quote.effective_price) if es_quote is not None else None
    if es_quote is None:
        quality_reasons.append("es_unavailable")
    else:
        decision = configured_quote_use_decision(es_quote, as_of=now, settings=storage)
        if not decision.alert_allowed or decision.feed_mode is not MarketDataQuality.LIVE:
            quality_reasons.append("es_not_live")
    basis = _qualified_es_basis(storage, now=now)
    try:
        options_map = build_options_map(state)
    except (LookupError, TypeError, ValueError):
        options_map = None
    coordinate = resolve_trigger_coordinate(
        state,
        options_map,
        now=now,
        qualified_es_basis=basis,
    )
    levels = coordinate.transform_levels(spx_levels)
    spot = coordinate.observed_value
    spx_spot = coordinate.spx_observed_value
    spot_source = coordinate.source
    coordinate_kind = coordinate.kind.value
    coordinate_instrument = coordinate.instrument_id
    coordinate_basis = coordinate.basis_points
    active_kind = str((active_decision or {}).get("trigger_coordinate_kind") or "")
    active_phase = str((active_decision or {}).get("phase") or "far")
    active_basis = _positive_or_negative_float((active_decision or {}).get("trigger_basis_points"))
    if active_phase not in {"far", "invalidated", "expired"} and es is not None:
        if active_kind == "es_equivalent" and active_basis is not None:
            levels = {name: value + active_basis for name, value in spx_levels.items()}
            spot = es
            spx_spot = es - active_basis
            spot_source = f"latched_future:ES+basis:{active_basis:.4f}"
            coordinate_kind = active_kind
            coordinate_instrument = "future:ES"
            coordinate_basis = active_basis
        elif (
            active_kind in {"official_spx", "chain_implied_spx"} and coordinate_kind != active_kind
        ):
            if active_basis is not None:
                levels = dict(spx_levels)
                spot = es - active_basis
                spx_spot = spot
                spot_source = f"latched_spx_coordinate_from_es_basis:{active_basis:.4f}"
                coordinate_kind = active_kind
                coordinate_instrument = str(
                    (active_decision or {}).get("trigger_instrument_id") or "index:SPX"
                )
                coordinate_basis = active_basis
    if coordinate_basis is None and es is not None and spx_spot is not None:
        coordinate_basis = es - spx_spot
    if not coordinate.usable:
        quality_reasons.append("spx_price_unavailable")
    if not levels:
        quality_reasons.append("key_levels_unavailable")
    return LevelObservation(
        at=now,
        spot=spot,
        es=es,
        levels=levels,
        quality_ok=not quality_reasons,
        quality_reason=";".join(dict.fromkeys(quality_reasons)) or None,
        session_date=session_date,
        spot_source=spot_source,
        level_source=level_source,
        spx_levels=spx_levels,
        trigger_coordinate_kind=coordinate_kind,
        trigger_instrument_id=coordinate_instrument,
        trigger_basis_points=coordinate_basis,
        spx_spot=spx_spot,
    )


def _live_structure(tick: EngineTick | None, *, now: datetime) -> dict[str, object] | None:
    if tick is None:
        return None
    analytics = getattr(tick, "analytics", None)
    if analytics is None or analytics.status is not AnalyticsStatus.SUCCESS:
        return None
    if not analytics.expiries or tick.health.factors.get("front_chain_fresh") is not True:
        return None
    front = analytics.expiries[0]
    if getattr(front, "gex_quality", None) != "open_interest_gex":
        return None
    if getattr(front, "wall_method", None) != "oi_gex":
        return None
    levels: dict[str, float] = {}
    _add_level(levels, "put_wall", getattr(front, "put_wall", None))
    flip = getattr(front, "gamma_flip_zone", None)
    if isinstance(flip, tuple | list) and len(flip) >= 2:
        ordered = sorted((float(flip[0]), float(flip[1])))
        _add_level(levels, "flip_low", ordered[0])
        _add_level(levels, "flip_high", ordered[1])
    _add_level(levels, "call_wall", getattr(front, "call_wall", None))
    if not levels:
        return None
    return {
        "levels": levels,
        "expiry": str(getattr(front, "expiry", "") or ""),
        "source": "live_oi_gex",
        "observed_at": _utc(now).isoformat(),
        "session_date": _research_session_date(now),
    }


def _structure_levels(structure: Mapping[str, object] | None) -> dict[str, float]:
    raw = (structure or {}).get("levels")
    if not isinstance(raw, Mapping):
        return {}
    result: dict[str, float] = {}
    for name in ("put_wall", "flip_low", "flip_high", "call_wall"):
        _add_level(result, name, raw.get(name))
    return result


def _structure_session_age(
    structure: Mapping[str, object] | None,
    *,
    now: datetime,
) -> int | None:
    if not isinstance(structure, Mapping):
        return None
    observed_session = _optional_date(structure.get("session_date"))
    if observed_session is None:
        observed_at = _optional_datetime(structure.get("observed_at"))
        observed_session = observed_at.astimezone(ET).date() if observed_at else None
    if observed_session is None:
        return None
    return DEFAULT_MARKET_CALENDAR.trading_days_elapsed(
        observed_session,
        DEFAULT_MARKET_CALENDAR.research_expiry(now),
    )


def _qualified_es_basis(storage: StorageSettings, *, now: datetime) -> float | None:
    path = Path(storage.data_root) / "state" / "ibkr_atm_reference.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    basis = payload.get("basis") if isinstance(payload, dict) else None
    if not isinstance(basis, Mapping):
        return None
    median = _positive_or_negative_float(basis.get("median"))
    sample_count = int(basis.get("sample_count") or 0)
    trading_date = _optional_date(basis.get("trading_date"))
    if (
        median is None
        or abs(median) > BASIS_MAX_ABS_POINTS
        or sample_count < BASIS_MIN_SAMPLES
        or trading_date is None
    ):
        return None
    expected_year, expected_month = active_quarterly_contract_month(now)
    if not str(basis.get("es_contract") or "").startswith(f"{expected_year}{expected_month:02d}"):
        return None
    age = _trading_day_age(
        trading_date,
        DEFAULT_MARKET_CALENDAR.research_expiry(now),
    )
    if age is None or age > BASIS_MAX_TRADING_DAY_AGE:
        return None
    return median


def _public_state(
    state: Mapping[str, object],
    *,
    formal_signal_enabled: bool = False,
    structure: Mapping[str, object] | None = None,
    structure_stability: Mapping[str, object] | None = None,
    latest_observation: Mapping[str, object] | None = None,
) -> dict[str, object]:
    phase = str(state.get("phase") or LevelPhase.FAR.value)
    thesis = str(state.get("thesis") or "none")
    direction = str(state.get("direction") or "") or None
    formal_signal = formal_signal_enabled and phase == LevelPhase.CONFIRMED.value
    observation = latest_observation or {}
    levels = dict((structure or {}).get("levels") or {})
    trigger_value = _positive_float(observation.get("spot", state.get("last_spot")))
    spot = _positive_float(observation.get("spx_spot")) or trigger_value
    es = _positive_float(observation.get("es", state.get("last_es")))
    spot_source = observation.get("spot_source")
    es_basis = es - spot if spot is not None and es is not None else None
    es_equivalent_levels = {
        key: float(value) + es_basis
        for key, value in levels.items()
        if es_basis is not None and isinstance(value, int | float)
    }
    structure_candidate = (
        structure_stability.get("candidate")
        if isinstance(structure_stability, Mapping)
        else None
    )
    public_candidate = (
        {
            "expiry": structure_candidate.get("expiry"),
            "levels": dict(structure_candidate.get("levels") or {}),
            "confirmation_count": structure_candidate.get("confirmation_count"),
            "required_confirmations": structure_candidate.get("required_confirmations"),
            "first_seen_at": structure_candidate.get("first_seen_at"),
            "last_seen_at": structure_candidate.get("last_seen_at"),
        }
        if isinstance(structure_candidate, Mapping)
        else None
    )
    return {
        "mode": "live" if formal_signal_enabled else "shadow",
        "formal_signal_enabled": formal_signal_enabled,
        "formal_signal": formal_signal,
        "level_path_confirmed": formal_signal,
        "actionable": False,
        "action_gate": "trade_intent_required",
        "signal_mode": (
            "direction_confirmed"
            if formal_signal
            else "confirmed_shadow"
            if phase == LevelPhase.CONFIRMED.value
            else "map_only"
        ),
        "phase": phase,
        "thesis": thesis,
        "direction": direction,
        "event_id": state.get("event_id"),
        "level_kind": state.get("level_kind"),
        "level": state.get("spx_level", state.get("level")),
        "trigger_level": state.get("level"),
        "reason": state.get("reason"),
        "phase_at": state.get("phase_at"),
        "started_at": state.get("started_at"),
        "expires_at": state.get("expires_at"),
        "expiry": (structure or {}).get("expiry"),
        "levels": levels,
        "level_bands": dict((structure or {}).get("level_bands") or {}),
        "structure_promoted_at": (structure or {}).get("promoted_at"),
        "structure_duration_seconds": (structure or {}).get("duration_seconds"),
        "structure_change_pending": public_candidate is not None,
        "structure_candidate": public_candidate,
        "spot": spot,
        "es": es,
        "spot_source": spot_source,
        "trigger_value": trigger_value,
        "trigger_coordinate": {
            "kind": observation.get(
                "trigger_coordinate_kind", state.get("trigger_coordinate_kind")
            ),
            "instrument_id": observation.get(
                "trigger_instrument_id", state.get("trigger_instrument_id")
            ),
            "observed_value": trigger_value,
            "target_value": state.get("level"),
            "spx_level": state.get("spx_level", state.get("level")),
            "basis_points": observation.get(
                "trigger_basis_points", state.get("trigger_basis_points")
            ),
        },
        "es_basis_points": es_basis,
        "es_equivalent_levels": es_equivalent_levels,
        "level_source": observation.get("level_source", (structure or {}).get("source")),
        "quality_ok": observation.get("quality_ok"),
        "quality_reason": observation.get("quality_reason"),
        "updated_at": state.get("updated_at"),
    }


def _transition_record(
    transition,
    observation: LevelObservation,
    *,
    formal_signal_enabled: bool,
) -> dict[str, object]:
    state = transition.state
    formal_signal = formal_signal_enabled and transition.current_phase is LevelPhase.CONFIRMED
    return {
        "record_key": (
            f"{state.get('event_id') or 'far'}:"
            f"{state.get('transition_count') or 0}:{transition.current_phase.value}"
        ),
        "event_id": state.get("event_id"),
        "at": _utc(observation.at).isoformat(),
        "previous_phase": transition.previous_phase.value,
        "current_phase": transition.current_phase.value,
        "thesis": state.get("thesis"),
        "direction": state.get("direction"),
        "level_kind": state.get("level_kind"),
        "level": state.get("level"),
        "spx_level": state.get("spx_level", state.get("level")),
        "spot": observation.spot,
        "es": observation.es,
        "levels": dict(observation.levels),
        "quality_ok": observation.quality_ok,
        "quality_reason": observation.quality_reason,
        "spot_source": observation.spot_source,
        "trigger_coordinate_kind": observation.trigger_coordinate_kind,
        "trigger_instrument_id": observation.trigger_instrument_id,
        "trigger_basis_points": observation.trigger_basis_points,
        "level_source": observation.level_source,
        "reason": transition.reason,
        "formal_signal": formal_signal,
        "level_path_confirmed": formal_signal,
        "actionable": False,
        "action_gate": "trade_intent_required",
        "attribution": _transition_attribution(
            transition.current_phase,
            transition.reason,
        ),
        "state": dict(state),
    }


def _health_record(
    observation: LevelObservation,
    transition,
    *,
    rth: bool,
    formal_signal_enabled: bool,
    tick: EngineTick | None,
    settings: LevelDecisionSettings,
    stable_structure: Mapping[str, object] | None,
    structure_stability: Mapping[str, object] | None,
) -> dict[str, object]:
    formal_signal = formal_signal_enabled and transition.current_phase is LevelPhase.CONFIRMED
    engine_health = getattr(tick, "health", None)
    structure_candidate = (
        structure_stability.get("candidate")
        if isinstance(structure_stability, Mapping)
        else None
    )
    return {
        "schema_version": 2,
        "record_key": _utc(observation.at).isoformat(),
        "at": _utc(observation.at).isoformat(),
        "session_date": observation.session_date,
        "session_mode": "rth" if rth else "globex",
        "tick_id": getattr(tick, "tick_id", None),
        "source_snapshot_id": getattr(tick, "source_snapshot_id", None),
        "spot": observation.spot,
        "spx_spot": observation.spx_spot,
        "es": observation.es,
        "levels": dict(observation.levels),
        "spx_levels": dict(observation.spx_levels or {}),
        "quality_ok": observation.quality_ok,
        "quality_reason": observation.quality_reason,
        "spot_source": observation.spot_source,
        "level_source": observation.level_source,
        "trigger_coordinate_kind": observation.trigger_coordinate_kind,
        "trigger_instrument_id": observation.trigger_instrument_id,
        "trigger_basis_points": observation.trigger_basis_points,
        "stable_structure": dict(stable_structure or {}),
        "structure_candidate": (
            dict(structure_candidate) if isinstance(structure_candidate, Mapping) else None
        ),
        "machine_settings": asdict(settings),
        "engine_health": (
            engine_health.to_dict() if hasattr(engine_health, "to_dict") else None
        ),
        "phase": transition.current_phase.value,
        "formal_signal": formal_signal,
        "level_path_confirmed": formal_signal,
        "actionable": False,
        "action_gate": "trade_intent_required",
    }


def _deliver_transition(
    transition,
    observation: LevelObservation,
    *,
    storage: StorageSettings,
    now: datetime,
    notify_transitions: bool,
    formal_signal_enabled: bool,
    invalidation_buffer_points: float,
    notifications_enabled: bool,
) -> dict[str, object] | None:
    del invalidation_buffer_points
    if not transition.changed:
        return None
    state = transition.state
    level_confirmed = formal_signal_enabled and transition.current_phase is LevelPhase.CONFIRMED
    result = {
        "record_key": (
            f"{state.get('event_id') or 'far'}:"
            f"{state.get('transition_count') or 0}:{transition.current_phase.value}"
        ),
        "at": _utc(now).isoformat(),
        "event_id": state.get("event_id"),
        "phase": transition.current_phase.value,
        "formal_signal": level_confirmed,
        "actionable": False,
        "notify_transitions_configured": notify_transitions,
        "delivery_gate": "trade_intent_required",
        "reason": "observation_only",
        "sinks": [],
        "accepted": False,
        "queued": False,
        "delivered": False,
    }
    if level_confirmed and notify_transitions and notifications_enabled:
        notification = NotificationSettings.from_env()
        if not notification.enabled:
            result["reason"] = "notification_disabled"
        elif not any(
            bool(getattr(notification, field, False))
            for field in ("feishu_enabled", "bark_enabled", "bark_friend_enabled")
        ):
            result["reason"] = "no_delivery_sink"
        else:
            event_id = f"level-path:{state.get('event_id')}:confirmed"
            text = _confirmed_path_message(state, observation)
            try:
                enqueued = enqueue_notification(
                    notification,
                    NotificationEnvelope(
                        event_id=event_id,
                        source="level_decision",
                        kind="level_path_confirmed",
                        lane="market_warning",
                        occurred_at=_utc(now),
                    ),
                    title="SPX PATH CONFIRMED",
                    text=text,
                    friend=True,
                    feishu_text=text,
                    enqueued_at=_utc(now),
                )
            except Exception as exc:  # Delivery failure must not roll back the state machine.
                result["reason"] = f"delivery_error:{type(exc).__name__}"
            else:
                result.update(
                    {
                        "notification_event_id": event_id,
                        "reason": "confirmed_market_warning",
                        "targets": list(enqueued.targets),
                        "accepted": enqueued.accepted,
                        "inserted": enqueued.inserted,
                        "duplicate": enqueued.duplicate,
                        "queued": enqueued.queued_for_recovery,
                        # Delivery belongs to the independent outbox consumer.
                        "delivered": enqueued.delivered,
                    }
                )
    _append_unique(_delivery_audit_path(storage, now), result)
    return result


def _confirmed_path_message(
    state: Mapping[str, object],
    observation: LevelObservation,
) -> str:
    thesis = str(state.get("thesis") or "none")
    direction = str(state.get("direction") or "")
    path = {
        ("breakout", "up"): "向上突破",
        ("breakout", "down"): "向下突破",
        ("fade", "up"): "下破拒绝后向上收复",
        ("fade", "down"): "上破拒绝后向下回落",
    }.get((thesis, direction), "关键位路径")
    spx_level = _positive_float(state.get("spx_level", state.get("level")))
    spx_spot = observation.spx_spot
    coordinate = str(state.get("trigger_coordinate_kind") or "unknown")
    spot_label = "SPX" if coordinate == "official_spx" else "SPX代理"
    expires_at = str(state.get("expires_at") or "-")
    return "\n".join(
        (
            f"SPX 路径确认 · {path}",
            f"关键位  {_level_kind_label(state.get('level_kind'))} {_format_level(spx_level)}",
            f"位置    {spot_label} {_format_level(spx_spot)}，ES {_format_level(observation.es)}",
            "状态    回踩与方向保持已经确认，路径有效",
            "执行    尚未通过实时 NBBO、目标空间和收益风险门控；等待 TRADE READY",
            f"时效    {expires_at}",
            "本提醒不连接真实订单、成交或持仓状态。",
        )
    )


def _level_structure_summary(levels: Mapping[str, float]) -> str:
    put_wall = _positive_float(levels.get("put_wall"))
    flip_low = _positive_float(levels.get("flip_low"))
    flip_high = _positive_float(levels.get("flip_high"))
    call_wall = _positive_float(levels.get("call_wall"))
    flip = (
        f"{flip_low:.2f}–{flip_high:.2f}"
        if flip_low is not None and flip_high is not None
        else f"{(flip_low if flip_low is not None else flip_high):.2f}"
        if flip_low is not None or flip_high is not None
        else "-"
    )
    return (
        f"SPX 结构：Put Wall {_format_level(put_wall)} | "
        f"Flip {flip} | Call Wall {_format_level(call_wall)}"
    )


def _es_level_structure_summary(observation: LevelObservation) -> str:
    spx_spot = observation.spx_spot
    if spx_spot is None and observation.spot_source.startswith("es_basis_adjusted:"):
        spx_spot = observation.spot
    if spx_spot is None or observation.es is None:
        return "ES 等价值位：不可用"
    basis = observation.es - spx_spot
    parts: list[str] = []
    for key, label in (
        ("put_wall", "Put Wall"),
        ("flip_low", "Flip Low"),
        ("flip_high", "Flip High"),
        ("call_wall", "Call Wall"),
    ):
        level = _positive_float((observation.spx_levels or observation.levels).get(key))
        if level is not None:
            parts.append(f"{label} {level + basis:.2f}")
    return f"ES 等价值位（basis {basis:.2f}）：" + (" | ".join(parts) or "不可用")


def _level_kind_label(value: object) -> str:
    return {
        "put_wall": "Put Wall",
        "flip_low": "Flip Low",
        "flip_high": "Flip High",
        "call_wall": "Call Wall",
    }.get(str(value or ""), str(value or "-"))


def _format_level(value: float | None) -> str:
    return f"{value:.2f}" if value is not None else "-"


def _transition_attribution(phase: LevelPhase, reason: str) -> str:
    if phase is LevelPhase.CONFIRMED:
        return "confirmed_pending_outcome"
    if phase is LevelPhase.EXPIRED:
        return "no_confirmation"
    if phase is not LevelPhase.INVALIDATED:
        return "state_progression"
    if reason == "structure_drift":
        return "level_error"
    if "data" in reason or "stale" in reason or "unavailable" in reason:
        return "data_error"
    if reason == "crossed_invalidation":
        return "false_break_or_rejection"
    return "invalidated_other"


def _load_state(path: Path) -> dict[str, object]:
    if not path.exists():
        return {
            "schema_version": 1,
            "decision": empty_level_state(datetime.now(tz=timezone.utc)),
            "outcomes": {},
        }
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LevelDecisionShadowError("level-decision state is unreadable") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise LevelDecisionShadowError("unsupported level-decision state schema")
    return payload


def _append_unique(path: Path, row: Mapping[str, object]) -> None:
    record_key = str(row.get("record_key") or "")
    if not record_key:
        raise ValueError("audit record_key is required")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o600)
    with os.fdopen(descriptor, "a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        handle.seek(0)
        for line in handle:
            try:
                existing = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(existing, dict) and existing.get("record_key") == record_key:
                return
        handle.seek(0, os.SEEK_END)
        handle.write(json.dumps(dict(row), ensure_ascii=False, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _append_record(path: Path, row: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
    with os.fdopen(descriptor, "a", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        handle.write(json.dumps(dict(row), ensure_ascii=False, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _audit_path(storage: StorageSettings, now: datetime) -> Path:
    date = _session_date(now)
    return (
        Path(storage.data_root)
        / "features"
        / "level_decision_audit"
        / f"date={date}"
        / "transitions.jsonl"
    )


def _outcome_path(storage: StorageSettings, now: datetime) -> Path:
    date = _session_date(now)
    return (
        Path(storage.data_root)
        / "features"
        / "level_decision_outcomes"
        / f"date={date}"
        / "outcomes.jsonl"
    )


def _delivery_audit_path(storage: StorageSettings, now: datetime) -> Path:
    date = _session_date(now)
    return (
        Path(storage.data_root)
        / "features"
        / "level_decision_delivery"
        / f"date={date}"
        / "deliveries.jsonl"
    )


def _health_path(storage: StorageSettings, now: datetime) -> Path:
    date = _session_date(now)
    return (
        Path(storage.data_root)
        / "features"
        / "level_decision_health"
        / f"date={date}"
        / "samples.jsonl"
    )


def _rth_session(now: datetime) -> str | None:
    local = now.astimezone(ET)
    session = DEFAULT_MARKET_CALENDAR.session(local.date())
    if session is None or not (session.open_at <= local < session.close_at):
        return None
    return session.trading_date.isoformat()


def _session_date(now: datetime) -> str:
    return _research_session_date(now)


def _research_session_date(now: datetime) -> str:
    return DEFAULT_MARKET_CALENDAR.research_expiry(now).isoformat()


def _add_level(target: dict[str, float], name: str, value: object) -> None:
    parsed = _positive_float(value)
    if parsed is not None:
        target[name] = parsed


def _positive_float(value: object) -> float | None:
    if not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if parsed > 0 and parsed == parsed else None


def _positive_or_negative_float(value: object) -> float | None:
    if not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if parsed == parsed else None


def _optional_date(value: object) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _optional_datetime(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return _utc(parsed)


def _trading_day_age(observed: date, current: date) -> int | None:
    return DEFAULT_MARKET_CALENDAR.trading_days_elapsed(observed, current)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("shadow timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)
