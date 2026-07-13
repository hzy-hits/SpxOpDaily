"""Stateful shadow runner for the mutually-exclusive wall/flip machine."""

from __future__ import annotations

import fcntl
import json
import os
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
from spx_spark.application.order_map.prompts import (
    GLOBEX_CONTEXT_SYSTEM_PROMPT,
    globex_writer_output_valid,
)
from spx_spark.application.order_map.trigger_coordinates import resolve_trigger_coordinate
from spx_spark.config import NotificationSettings, StorageSettings
from spx_spark.domain.analytics import AnalyticsStatus
from spx_spark.ibkr.atm_reference import (
    BASIS_MAX_ABS_POINTS,
    BASIS_MAX_TRADING_DAY_AGE,
    BASIS_MIN_SAMPLES,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET
from spx_spark.marketdata import MarketDataQuality
from spx_spark.options_map import build_options_map
from spx_spark.notifier.llm_writer import generate_push_text
from spx_spark.notifier.sinks import deliver_trade_push
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
    latest_observation = payload.get("latest_observation") if isinstance(payload, dict) else None
    return _public_state(
        decision if isinstance(decision, Mapping) else {},
        formal_signal_enabled=bool(payload.get("formal_signal_enabled")),
        structure=structure if isinstance(structure, Mapping) else None,
        latest_observation=(
            latest_observation if isinstance(latest_observation, Mapping) else None
        ),
    )


def run_level_decision_shadow(
    storage: StorageSettings,
    tick: EngineTick,
    *,
    now: datetime,
    policy: LevelDecisionPolicy | None = None,
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
        frozen_structure = (
            live_structure if live_structure is not None else persisted.get("structure")
        )
        observation = _observation(
            storage,
            tick,
            now=now,
            session_date=session or _research_session_date(now),
            frozen_structure=(frozen_structure if isinstance(frozen_structure, Mapping) else None),
            max_frozen_structure_age_sessions=policy.max_frozen_structure_age_sessions,
        )
        previous = persisted.get("decision")
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
    )

    public = _public_state(
        transition.state,
        formal_signal_enabled=policy.formal_signal_enabled,
        structure=frozen_structure if isinstance(frozen_structure, Mapping) else None,
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
) -> LevelObservation:
    structure_age = _structure_session_age(frozen_structure, now=now)
    structure_usable = (
        structure_age is not None and structure_age <= max_frozen_structure_age_sessions
    )
    spx_levels = _structure_levels(frozen_structure) if structure_usable else {}
    level_source = str((frozen_structure or {}).get("source") or "unavailable")
    quality_reasons: list[str] = []
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
    spot_source = coordinate.source
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
        trigger_coordinate_kind=coordinate.kind.value,
        trigger_instrument_id=coordinate.instrument_id,
        trigger_basis_points=coordinate.basis_points,
        spx_spot=coordinate.spx_observed_value,
    )


def _live_structure(tick: EngineTick, *, now: datetime) -> dict[str, object] | None:
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
    return {
        "mode": "live" if formal_signal_enabled else "shadow",
        "formal_signal_enabled": formal_signal_enabled,
        "formal_signal": formal_signal,
        "actionable": formal_signal,
        "signal_mode": (
            "confirmed"
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
        "actionable": formal_signal,
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
) -> dict[str, object]:
    formal_signal = formal_signal_enabled and transition.current_phase is LevelPhase.CONFIRMED
    return {
        "record_key": _utc(observation.at).isoformat(),
        "at": _utc(observation.at).isoformat(),
        "session_date": observation.session_date,
        "session_mode": "rth" if rth else "globex",
        "quality_ok": observation.quality_ok,
        "quality_reason": observation.quality_reason,
        "spot_source": observation.spot_source,
        "level_source": observation.level_source,
        "phase": transition.current_phase.value,
        "formal_signal": formal_signal,
        "actionable": formal_signal,
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
) -> dict[str, object] | None:
    if not transition.changed:
        return None
    state = transition.state
    formal_signal = formal_signal_enabled and transition.current_phase is LevelPhase.CONFIRMED
    if not notify_transitions and not formal_signal:
        return None
    phase = transition.current_phase.value.upper()
    trigger_level = _positive_float(state.get("level"))
    trigger_value = observation.spot
    level = _positive_float(state.get("spx_level", state.get("level")))
    spot = observation.spx_spot
    if spot is None and observation.spot_source.startswith("es_basis_adjusted:"):
        spot = observation.spot
    distance = (
        None if trigger_level is None or trigger_value is None else trigger_value - trigger_level
    )
    direction = str(state.get("direction") or "")
    invalidation = None
    if level is not None and direction == "up":
        invalidation = level - invalidation_buffer_points
    elif level is not None and direction == "down":
        invalidation = level + invalidation_buffer_points
    structure_summary = _level_structure_summary(observation.spx_levels or observation.levels)
    es_structure_summary = _es_level_structure_summary(observation)
    if formal_signal:
        text = "\n".join(
            (
                f"【SPX 关键位正式信号】{str(state.get('thesis') or '-').upper()} CONFIRMED",
                structure_summary,
                es_structure_summary,
                (
                    f"本次触发 {_level_kind_label(state.get('level_kind'))} "
                    f"{level:.2f}；方向 {direction.upper()}"
                ),
                (
                    f"确认 SPX {spot:.2f}（{observation.spot_source}），ES {observation.es:.2f}"
                    if spot is not None and observation.es is not None
                    else f"参考不可用：{observation.quality_reason or 'unknown'}"
                ),
                f"失效位 {invalidation:.2f}；有效至 {state.get('expires_at') or '-'}",
                f"结构来源 {observation.level_source}；正式决策信号，不自动下单。",
            )
        )
    else:
        text = "\n".join(
            (
                f"【Wall/Flip 状态事件】{transition.previous_phase.value.upper()} → {phase}",
                structure_summary,
                es_structure_summary,
                (
                    f"参考 SPX {spot:.2f}（{observation.spot_source}），ES {observation.es:.2f}"
                    if spot is not None and observation.es is not None
                    else f"参考不可用：{observation.quality_reason or 'unknown'}"
                ),
                (
                    f"关键位 {state.get('level_kind') or '-'} {level:.2f}，距离 {distance:+.2f} 点"
                    if level is not None and distance is not None
                    else "关键位尚未建立"
                ),
                (
                    f"路径 {state.get('thesis') or 'none'} / "
                    f"{state.get('direction') or '-'}；原因 {transition.reason}"
                ),
                f"结构来源 {observation.level_source}；当前为 shadow 审计，不是下单信号。",
            )
        )
    template_text = text
    notification = NotificationSettings.from_env()
    writer_prompt = "\n".join(
        (
            "把下面的 SPX 关键位状态机事件改写成简短、可扫读的中文交易便签。",
            "所有价格、方向、阶段、失效位、有效期和数据来源必须原样保留，不得编造、换算或修正数字。",
            "先写当前 SPX 代理相对 Put Wall、Flip、Call Wall 的位置，再写这次状态变化意味着什么。",
            "CONFIRMED 必须给主情景、证伪位和下一步观察条件；非 CONFIRMED 只说明观察条件，不得写成下单信号。",
            "ES-basis 是夜盘 SPX 代理，Hyperliquid 只能交叉验证；不得写『等开盘再说』。",
            "SPX 结构位与 ES 等价值位是两个坐标系，只能引用事实模板给出的对应值，严禁把 SPX strike 直接当 ES 价格。",
            "不得推断输入里没有的历史触碰次数、墙是否弃守、dealer 行为或 gamma 燃料；不用比喻。",
            "不自动下单，不得编造期权合约、权利金、Greeks 或限价。输出 6-10 行。",
            "事实模板:" + text,
        )
    )
    text, writer = generate_push_text(
        text,
        writer_prompt,
        notification,
        system=GLOBEX_CONTEXT_SYSTEM_PROMPT,
    )
    if writer != "template" and not globex_writer_output_valid(text, template_text):
        text, writer = template_text, "template_validation_fallback"
    sinks = deliver_trade_push(
        notification,
        title=(
            f"SPX 关键位正式信号 {direction.upper()}" if formal_signal else f"SPX Wall/Flip {phase}"
        ),
        text=text,
        kind=("level_decision_formal_signal" if formal_signal else "level_decision_transition"),
        lane="trade",
        friend=False,
    )
    result = {
        "record_key": (
            f"{state.get('event_id') or 'far'}:"
            f"{state.get('transition_count') or 0}:{transition.current_phase.value}"
        ),
        "at": _utc(now).isoformat(),
        "event_id": state.get("event_id"),
        "phase": transition.current_phase.value,
        "formal_signal": formal_signal,
        "actionable": formal_signal,
        "text": text,
        "writer": writer,
        "sinks": [sink.to_dict() for sink in sinks],
        "delivered": any(sink.sink == "bark" and sink.ok for sink in sinks),
    }
    _append_unique(_delivery_audit_path(storage, now), result)
    return result


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
