"""Deterministic, structure-first projection for human RTH/GTH desk maps."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Mapping

from spx_spark.analytics.options.density import summarize_strike_surface_shape
from spx_spark.analytics.options.pricing import finite_float
from spx_spark.application.order_map import guidance as guidance_module
from spx_spark.application.order_map.frozen_structure import option_structure_frame_is_live
from spx_spark.application.order_map.level_decision_machine import LevelPhase
from spx_spark.application.order_map.models import SHANGHAI_TZ
from spx_spark.application.order_map.render import (
    _candidate_by_play,
    _dash,
    atm_straddle_session_line,
    underlier_source_label,
)
from spx_spark.application.order_map.state import _session_phase_of, current_session_is_gth
from spx_spark.application.order_map.desk_strategy_view import (
    cross_asset_confirmation_text,
    expected_move_text,
    humanize_strategy_reason,
    iron_condor_desk_line,
    level_kind_label,
    opening_range_state_text,
    phase_label,
    quality_reason_text,
    stage_label,
    strategy_candidate_is_watchable,
    strategy_decision_desk_view,
    strategy_lane_status_lines,
    strategy_market_bias,
    strategy_reason_line,
    thesis_label,
    volume_alignment_text,
)
from spx_spark.application.order_map.status_explanation import (
    humanize_operator_trigger,
    manual_candidate_ready_authorized,
    operator_reason_line,
)

class DeskStage(StrEnum):
    OBSERVING = "OBSERVING"
    WATCHING = "WATCHING"
    ARMED = "ARMED"
    CONFIRMED = "CONFIRMED"
    READY = "READY"
    INVALIDATED = "INVALIDATED"
    EXPIRED = "EXPIRED"
    PAUSED = "PAUSED"

@dataclass(frozen=True, slots=True)
class DeskMapProjection:
    stage: DeskStage
    phase: LevelPhase
    direction: str
    thesis: str
    level_kind: str
    level: float | None
    data_quality: str
    quality_reasons: tuple[str, ...]

@dataclass(frozen=True, slots=True)
class DeskMessageSections:
    """The complete, structured desk-map facts exported to the Rust report owner."""

    title: str
    desk_view: str
    location: str
    structure: str
    primary_path: str
    alternative_path: str
    targets: str
    execution: str
    data_quality: str

_ARMED_PHASES = frozenset(
    {
        LevelPhase.BREAK_PENDING,
        LevelPhase.REJECT_PENDING,
        LevelPhase.ACCEPTED,
        LevelPhase.REJECTED,
        LevelPhase.RETEST,
    }
)


def render_operator_status_brief(
    payload: dict[str, Any],
    changes: list[str],
    now_utc: datetime,
) -> str:
    """Render one compact desk map without giving research shadows authority."""

    sections = build_desk_message_sections(payload, now_utc)
    lines = [
        sections.title,
        f"Desk View  {sections.desk_view}",
        f"Location  {sections.location}",
        f"Structure  {sections.structure}",
        f"Primary  {sections.primary_path}",
        f"Alternative  {sections.alternative_path}",
        f"Targets  {sections.targets}",
        f"Execution  {sections.execution}",
        f"Data Quality  {sections.data_quality}",
    ]
    if changes:
        lines.append(f"Change  {'；'.join(changes[:3])}")
    lines.append(strategy_reason_line(payload) or operator_reason_line(payload))
    return "\n".join(lines)


def build_desk_message_sections(
    payload: Mapping[str, Any],
    now_utc: datetime,
) -> DeskMessageSections:
    """Build the untruncated eight-section input contract for the Rust writer."""

    projection = build_desk_map_projection(payload)
    guidance = guidance_module.build_decision_guidance(payload)
    beijing = now_utc.astimezone(SHANGHAI_TZ)
    session = _session_phase_of(dict(payload), now_utc)
    expiry = str(payload.get("expiry") or "-")
    expiry_text = f"{expiry[4:6]}-{expiry[6:8]}" if len(expiry) == 8 else expiry
    desk_view = _desk_view_line(payload, projection, guidance).removeprefix("Desk View  ")
    if surface_line := _strategy_surface_shape_line(payload):
        desk_view = f"{desk_view}\n{surface_line}"

    return DeskMessageSections(
        title=(
            f"【SPX Desk Map · {beijing.strftime('%H:%M')} · "
            f"0DTE {expiry_text} · {session.get('name_cn')}】"
        ),
        desk_view=desk_view,
        location=_location_line(payload, projection).removeprefix("Location  "),
        structure=_structure_line(payload, now=now_utc).removeprefix("Structure  "),
        primary_path=_primary_path(payload, guidance, projection),
        alternative_path=(
            f"Evidence · {guidance.invalidation_text}"
            if projection.direction in {"up", "down"}
            else "Evidence · 尚无已授权入场方向；当前不存在交易失效位，任一实时结构形成接受/拒绝后重算"
        ),
        targets=_targets_line(payload, projection).removeprefix("Targets  "),
        execution=_execution_line(payload, projection, guidance).removeprefix("Execution  "),
        data_quality=_data_quality_line(payload, projection).removeprefix("Data Quality  "),
    )


def _strategy_surface_shape_line(payload: Mapping[str, Any]) -> str | None:
    decision = _mapping(payload.get("strategy_decision"))
    facts = _mapping(decision.get("market_facts"))
    structure = _mapping(facts.get("structure"))
    context = _mapping(structure.get("strike_differential_context"))
    if not context:
        return None
    shape = str(summarize_strike_surface_shape(context)["desk_line"]).strip()
    if not shape:
        return None
    if shape.startswith("曲面"):
        return f"{shape}（研究，不改结论）"
    return f"曲面 {shape}（研究，不改结论）"


def build_desk_map_projection(payload: Mapping[str, Any]) -> DeskMapProjection:
    decision = _mapping(payload.get("level_decision"))
    intent = _mapping(payload.get("trade_intent"))
    manual = _mapping(payload.get("gth_level_manual_candidate"))
    guidance = guidance_module.build_decision_guidance(payload)
    plans = [row for row in payload.get("plan_candidates") or () if isinstance(row, Mapping)]
    raw_phase = str(decision.get("phase") or LevelPhase.FAR.value).lower()
    invalid_phase = False
    try:
        phase = LevelPhase(raw_phase)
    except ValueError:
        phase = LevelPhase.FAR
        invalid_phase = True
    quality, quality_reasons = _data_quality(
        payload,
        decision,
        invalid_phase=invalid_phase,
    )
    intent_ready, manual_ready, current_plan = _current_ready_sources(
        decision,
        intent,
        manual,
        plans,
    )
    raw_ready = intent.get("status") == "trade_ready" or manual.get("status") == "manual_ready"
    current_ready = intent_ready or manual_ready
    current_path_ready = bool(
        phase is LevelPhase.CONFIRMED
        and decision.get("snapshot_consistent") is not False
        and decision.get("quality_ok") is not False
    )
    required_frame_unavailable = any(
        reason
        in {
            "market_frame:unavailable",
            "option_frame:unavailable",
            "option_l1:unavailable",
        }
        for reason in quality_reasons
    )
    ready = current_ready and current_path_ready and not required_frame_unavailable
    if raw_ready and not current_ready:
        quality = "DEGRADED"
        quality_reasons = tuple(
            dict.fromkeys((*quality_reasons, "ready_opportunity_mismatch"))
        )
    if raw_ready and not current_path_ready:
        quality = "DEGRADED"
        quality_reasons = tuple(
            dict.fromkeys((*quality_reasons, "ready_without_current_confirmed_path"))
        )
    if current_ready and current_path_ready and required_frame_unavailable:
        quality = "DEGRADED"
        quality_reasons = tuple(
            dict.fromkeys((*quality_reasons, "ready_required_frame_unavailable"))
        )
    if raw_ready and not ready:
        stage = DeskStage.PAUSED
    elif ready:
        stage = DeskStage.READY
    elif guidance.action is guidance_module.GuidanceAction.PAUSED:
        stage = DeskStage.PAUSED
    elif phase is LevelPhase.CONFIRMED:
        stage = DeskStage.CONFIRMED
    elif phase in _ARMED_PHASES:
        stage = DeskStage.ARMED
    elif phase in {LevelPhase.APPROACHING, LevelPhase.TESTING}:
        stage = DeskStage.WATCHING
    elif phase in {LevelPhase.INVALIDATED, LevelPhase.EXPIRED}:
        # Terminal transitions are delivered once by the lifecycle lane.  A
        # scheduled Desk Map describes the current operating posture, so an
        # old terminal event is STANDBY/OBSERVING rather than a current expired
        # path that gets repeated every half hour.
        stage = DeskStage.OBSERVING
    else:
        stage = DeskStage.OBSERVING
    decision_direction = _closed_direction(decision.get("direction"))
    thesis = _closed_thesis(decision.get("thesis"))
    direction = (
        decision_direction
        if decision_direction in {"up", "down"}
        and thesis in {"breakout", "fade"}
        and phase in {*_ARMED_PHASES, LevelPhase.CONFIRMED}
        else "none"
    )
    if ready and current_plan is not None:
        if decision_direction not in {"up", "down"}:
            direction = {"C": "up", "P": "down"}.get(
                str(current_plan.get("right") or ""), direction
            )
        play = str(current_plan.get("play") or "")
        if thesis not in {"breakout", "fade"}:
            thesis = "breakout" if "breakout" in play else "fade" if "fade" in play else thesis
    if ready and manual_ready and direction not in {"up", "down"}:
        direction = _closed_direction(manual.get("direction"))
    if ready and manual_ready and thesis not in {"breakout", "fade"}:
        thesis = _closed_thesis(manual.get("thesis"))
    level_kind = str(decision.get("level_kind") or "")
    level = finite_float(decision.get("level"))
    if not level_kind or level is None:
        level_kind = ""
        level = None
    return DeskMapProjection(
        stage=stage,
        phase=phase,
        direction=direction,
        thesis=thesis,
        level_kind=level_kind,
        level=level,
        data_quality=quality,
        quality_reasons=quality_reasons,
    )


def _current_ready_sources(
    decision: Mapping[str, Any],
    intent: Mapping[str, Any],
    manual: Mapping[str, Any],
    plans: list[Mapping[str, Any]],
) -> tuple[bool, bool, Mapping[str, Any] | None]:
    """Bind READY authority and any displayed plan to the current level event."""

    decision_event_id = str(decision.get("event_id") or "").strip()
    intent_event_id = str(intent.get("event_id") or "").strip()
    intent_ready = bool(
        intent.get("status") == "trade_ready"
        and decision_event_id
        and intent_event_id == decision_event_id
    )
    current_plan: Mapping[str, Any] | None = None
    if intent_ready:
        intent_id = str(intent.get("intent_id") or "").strip()
        contract_id = str(intent.get("contract_id") or "").strip()
        if len(plans) == 1:
            plan = plans[0]
            plan_intent_id = str(plan.get("intent_id") or "").strip()
            plan_contract_id = str(plan.get("contract_id") or "").strip()
            if (
                intent_id
                and plan_intent_id == intent_id
                and (not contract_id or plan_contract_id == contract_id)
            ):
                current_plan = plan
            else:
                intent_ready = False
        else:
            intent_ready = False

    manual_source_id = str(manual.get("source_signal_id") or "").strip()
    manual_ready = bool(
        manual_candidate_ready_authorized(dict(manual))
        and decision_event_id
        and manual_source_id == decision_event_id
    )
    return intent_ready, manual_ready, current_plan


def _desk_view_line(
    payload: Mapping[str, Any],
    projection: DeskMapProjection,
    guidance: guidance_module.DecisionGuidance,
) -> str:
    if strategy_line := strategy_decision_desk_view(payload):
        return f"Desk View  {strategy_line}"
    if projection.phase in {LevelPhase.INVALIDATED, LevelPhase.EXPIRED}:
        return (
            "Desk View  NO TRADE · STANDBY · 当前没有有效机会；"
            "旧事件已结束，等待新的价格触发"
        )
    if projection.stage is DeskStage.PAUSED:
        signal = "NO TRADE · 数据或执行门控暂停"
    elif projection.direction in {"up", "down"} and projection.thesis in {
        "breakout",
        "fade",
    }:
        side = "LONG / CALL" if projection.direction == "up" else "SHORT / PUT"
        path = f"{side} {thesis_label(projection.thesis)}"
        if projection.stage is DeskStage.READY:
            signal = f"READY · {path}"
        elif projection.stage is DeskStage.CONFIRMED:
            signal = f"HOLD · {path} · 方向已确认，尚不可入场"
        else:
            signal = f"WATCH · {path} · 条件路径，尚不可入场"
    elif projection.stage is DeskStage.OBSERVING:
        signal = f"NO TRADE · {guidance.bias}仅作背景，尚无价格触发"
    else:
        signal = "NO TRADE · 等待价格接受或拒绝"
    phase_text = (
        "已确认"
        if projection.stage is DeskStage.READY and projection.phase is LevelPhase.FAR
        else phase_label(projection.phase)
    )
    return f"Desk View  {signal} · 状态：{stage_label(projection.stage)}（{phase_text}）"





def _location_line(payload: Mapping[str, Any], projection: DeskMapProjection) -> str:
    decision = _mapping(payload.get("level_decision"))
    gth = current_session_is_gth(payload, decision)
    underlier = _mapping(payload.get("underlier"))
    coordinate = _mapping(payload.get("trigger_coordinate"))
    strategy_spot = _mapping(
        _mapping(_mapping(payload.get("strategy_decision")).get("market_facts")).get("spot")
    )
    kind = str(
        underlier.get("kind") or coordinate.get("kind") or strategy_spot.get("kind") or ""
    ).strip()
    source = str(
        underlier.get("source") or strategy_spot.get("source") or coordinate.get("source") or ""
    )
    actionable_spx = _first_finite(
        underlier.get("price"),
        underlier.get("spx_observed_value"),
        strategy_spot.get("spx"),
        strategy_spot.get("spx_observed_value"),
        coordinate.get("spx_observed_value"),
    )
    observed = _first_finite(
        underlier.get("observed_value"),
        strategy_spot.get("observed_value"),
        coordinate.get("observed_value"),
        actionable_spx,
    )
    market = _mapping(payload.get("minute_market_frame"))
    es = _mapping(market.get("es"))
    es_last = finite_float(payload.get("es_last"))
    if es_last is None:
        es_last = finite_float(es.get("price"))
    if es_last is None:
        es_last = finite_float(strategy_spot.get("es"))
    if actionable_spx is not None or observed is not None:
        display = actionable_spx if actionable_spx is not None else observed
        source_text = _coordinate_source_label(kind=kind, source=source)
        if gth or kind in {"chain_implied_spx", "es_equivalent"}:
            spx_text = f"{_dash(display)}"
            if source_text:
                spx_text += f"（{source_text}）"
            location_head = f"夜盘观察坐标 {spx_text}"
        else:
            spx_text = _dash(display)
            if source and source != "index:SPX":
                spx_text += f"（{underlier_source_label(source)}）"
            location_head = f"SPX {spx_text}"
    else:
        latched = finite_float(decision.get("spot"))
        if gth:
            spx_text = "暂缺（现金 SPX 不适用；等待期权隐含或带基差 ES）"
            if es_last is not None:
                spx_text = f"暂缺 · ES {_dash(es_last)} 可参考（现金 SPX 不适用）"
            elif latched is not None:
                spx_text += (
                    f" · latched {latched:g}（{_decision_spot_reference_label(decision)}；"
                    "非当前可行动坐标）"
                )
            location_head = f"夜盘观察坐标 {spx_text}"
        else:
            spx_text = "unavailable"
            if latched is not None:
                spx_text += (
                    f" · reference {latched:g}（{_decision_spot_reference_label(decision)}；"
                    "latched/proxy，not actionable）"
                )
            location_head = f"SPX {spx_text}"
    level_text = ""
    if projection.level is not None:
        distance = (
            abs(actionable_spx - projection.level) if actionable_spx is not None else None
        )
        level_text = f" · {level_kind_label(projection.level_kind)} {projection.level:g}" + (
            f" · 距离 {distance:.1f}pt" if distance is not None else ""
        )
    vwap = finite_float(es.get("vwap"))
    vwap_distance = finite_float(es.get("vwap_distance_points"))
    vwap_text = "ES VWAP unavailable"
    if vwap is not None:
        vwap_text = f"ES VWAP {_dash(vwap)}"
    if vwap_distance is not None:
        vwap_text += f"（偏离 {_dash(vwap_distance)}pt）"
    elif vwap is not None:
        vwap_text += "（偏离 unavailable）"
    return (
        f"Location  {location_head} · ES {_available_number(es_last)}"
        f"{level_text} · {_gamma_location_text(payload, actionable_spx)} · {vwap_text} · "
        f"{_opening_range_text(payload)} · "
        f"{expected_move_text(payload)}"
    )


def _coordinate_source_label(*, kind: str, source: str) -> str:
    if kind == "chain_implied_spx" or source == "chain_implied":
        return "期权隐含"
    if kind == "es_equivalent" or source.startswith("future:ES"):
        return "ES折算"
    if kind == "official_spx" or source == "index:SPX":
        return "现金指数"
    if source:
        return underlier_source_label(source)
    return ""


def _first_finite(*values: object) -> float | None:
    for value in values:
        parsed = finite_float(value)
        if parsed is not None:
            return parsed
    return None


def _decision_spot_reference_label(decision: Mapping[str, Any]) -> str:
    coordinate = str(decision.get("trigger_coordinate_kind") or "").strip().lower()
    source = str(
        decision.get("trigger_instrument_id")
        or decision.get("spot_source")
        or decision.get("level_source")
        or ""
    ).strip()
    if coordinate == "official_spx":
        return "official SPX reference"
    if coordinate == "chain_implied_spx":
        return "SPXW chain-implied proxy"
    if coordinate == "es_equivalent":
        return "ES-equivalent proxy"
    if source:
        return underlier_source_label(source)
    return "decision-state reference"


def _primary_path(
    payload: Mapping[str, Any],
    guidance: guidance_module.DecisionGuidance,
    projection: DeskMapProjection,
) -> str:
    market = _mapping(payload.get("minute_market_frame"))
    es = _mapping(market.get("es"))
    volume = _mapping(market.get("volume"))
    cross_asset = _mapping(market.get("cross_asset"))
    if projection.direction in {"up", "down"} and projection.level is not None:
        direction = "LONG" if projection.direction == "up" else "SHORT"
        path = "价格接受突破" if projection.thesis == "breakout" else "价格拒绝回归"
        basis = (
            f"方向来源  {direction} 来自 {level_kind_label(projection.level_kind)} "
            f"{projection.level:g} 的{path}；Gamma 不提供第一步方向"
        )
        if projection.stage is DeskStage.READY:
            trigger = "当前路径已确认；执行仅以独立 MANUAL READY 卡的实时合约与报价为准"
        elif projection.stage is DeskStage.CONFIRMED:
            trigger = "当前路径已确认；等待实时合约、报价与盈亏比通过执行门控"
        else:
            trigger = humanize_operator_trigger(guidance.trigger_text)
    else:
        strategy_bias = strategy_market_bias(_mapping(payload.get("strategy_decision")))
        bias = strategy_bias if strategy_bias != "中性/未定" else guidance.bias
        basis = (
            "方向来源  入场方向尚无价格接受/拒绝确认；"
            f"市场偏向 {bias}观察（不等于入场授权）"
        )
        trigger = _observing_trigger(payload)
    flow_parts = [
        (
            f"ES 15m {_signed_points(es.get('return_15m_points'))} / "
            f"60m {_signed_points(es.get('return_60m_points'))}"
        ),
        f"量价 {volume_alignment_text(volume.get('price_volume_alignment_5m'))}",
    ]
    decision = _mapping(payload.get("level_decision"))
    if not current_session_is_gth(payload, decision):
        flow_parts.append(
            "ES/SPY " + cross_asset_confirmation_text(cross_asset.get("es_spy_direction_confirmation_15m"))
        )
        flow_state = _mapping(_mapping(payload.get("intraday_shock_state")).get("captured_net_premium_divergence"))
        if summary := str(flow_state.get("desk_summary") or "").strip():
            flow_parts.append(summary)
    flow = "流确认  " + " · ".join(flow_parts)
    return f"Evidence · {basis}\n下一触发  {trigger}\n{flow}"


def _structure_line(payload: Mapping[str, Any], *, now: datetime) -> str:
    decision = _mapping(payload.get("level_decision"))
    frozen = _mapping(decision.get("levels"))
    live = _live_levels(payload)
    frozen_text = _levels_text(frozen) if frozen else "unavailable"
    live_text = _levels_text(live)
    same = bool(frozen) and all(
        finite_float(frozen.get(key)) == finite_float(live.get(key))
        for key in ("put_wall", "flip_low", "flip_high", "call_wall")
    )
    frame = _mapping(payload.get("option_structure_frame"))
    l1 = _mapping(frame.get("l1"))
    structure = _mapping(frame.get("structure"))
    frame_flip = structure.get("flip_zone")
    frame_has_walls = any(
        finite_float(structure.get(key)) is not None for key in ("put_wall", "call_wall")
    ) or (
        isinstance(frame_flip, list | tuple)
        and len(frame_flip) >= 2
        and finite_float(frame_flip[0]) is not None
        and finite_float(frame_flip[1]) is not None
    )
    frame_ready = (
        str(frame.get("quality") or "").lower() == "ready"
        and str(l1.get("quality") or "").lower() == "ready"
        and structure.get("frozen") is not True
        and frame_has_walls
    )
    live_frame = option_structure_frame_is_live(payload, now=now)
    gth = current_session_is_gth(payload, decision)
    provider_block_reason = _gth_provider_block_reason(payload)
    if frame_ready:
        # GTH/RTH option-frame walls own the first Structure line.
        levels = f"Put/Flip/Call {live_text} · event=live"
        source_label = "source=live"
        current_levels = live
    elif live_frame:
        levels = (
            f"Put/Flip/Call {frozen_text} · event=live"
            if same
            else f"event {frozen_text} · live {live_text}"
        )
        source_label = "source=live"
        current_levels = frozen if frozen else live
    elif gth:
        if provider_block_reason == "ibkr_competing_session":
            levels = (
                f"Put/Flip/Call {frozen_text} · GTH实时期权行情暂停"
                "（IBKR 10197；参考位，非当前确认）"
                if frozen
                else f"Put/Flip/Call {live_text} · GTH实时期权行情暂停（IBKR 10197）"
            )
        else:
            levels = (
                f"Put/Flip/Call {frozen_text} · GTH期权帧未就绪（参考位，非当前确认）"
                if frozen
                else f"Put/Flip/Call {live_text} · GTH期权帧未就绪"
            )
        source_label = "source=gth_reference_pending"
        current_levels = frozen if frozen else live
    else:
        levels = (
            f"Put/Flip/Call {frozen_text} · event=frozen/reference"
            if same
            else f"event {frozen_text} · reference {live_text}"
        )
        source_label = "source=frozen/reference · live confirmation unavailable"
        current_levels = frozen if frozen else live
    change_line = _structure_change_line(
        decision,
        current_levels=current_levels,
        source_label=source_label,
    )
    parts = [f"Structure  {levels}"]
    if change_line:
        parts.append(change_line)
    volatility = _mapping(frame.get("volatility"))
    if straddle_line := atm_straddle_session_line(dict(volatility)):
        parts.append(straddle_line)
    parts.append(f"Gamma职责  {_gamma_feedback_text(payload)}")
    lane_lines = strategy_lane_status_lines(payload)
    if lane_lines:
        parts.extend(lane_lines)
    elif current_session_is_gth(payload, decision):
        parts.append(f"铁鹰  {iron_condor_desk_line({})}")
    return "\n".join(parts)


def _structure_change_line(
    decision: Mapping[str, Any],
    *,
    current_levels: Mapping[str, Any],
    source_label: str,
) -> str | None:
    """Explain wall-map migration so operators do not see levels silently vanish."""

    pending = decision.get("structure_change_pending") is True
    candidate = _mapping(_mapping(decision.get("structure_candidate")).get("levels"))
    previous = _mapping(decision.get("previous_structure_levels"))
    if pending and candidate:
        diff = _structure_level_diff_text(current_levels, candidate)
        if diff:
            return f"Structure change pending: {diff} · {source_label} · confirming"
    if previous:
        diff = _structure_level_diff_text(previous, current_levels)
        if diff:
            return f"Structure change: {diff} · {source_label}"
    return None


def _structure_level_diff_text(
    before: Mapping[str, Any],
    after: Mapping[str, Any],
) -> str | None:
    parts: list[str] = []
    for key, label in (
        ("call_wall", "Call Wall"),
        ("put_wall", "Put Wall"),
        ("flip", "Flip"),
    ):
        if key == "flip":
            before_text = _flip_text(before)
            after_text = _flip_text(after)
            if before_text is None and after_text is None:
                continue
            if before_text == after_text:
                continue
            parts.append(
                f"{label} {before_text or 'unavailable'} → {after_text or 'unavailable'}"
            )
            continue
        left = finite_float(before.get(key))
        right = finite_float(after.get(key))
        if left is None and right is None:
            continue
        if left is not None and right is not None and abs(left - right) < 1e-9:
            continue
        parts.append(f"{label} {_available_number(left)} → {_available_number(right)}")
    return " · ".join(parts) if parts else None


def _flip_text(levels: Mapping[str, Any]) -> str | None:
    low = finite_float(levels.get("flip_low"))
    high = finite_float(levels.get("flip_high"))
    if low is None and high is None:
        return None
    if low is not None and high is not None:
        return f"{_available_number(low)}–{_available_number(high)}"
    return _available_number(low if low is not None else high)


def _live_levels(payload: Mapping[str, Any]) -> dict[str, object]:
    frame_structure = _mapping(_mapping(payload.get("option_structure_frame")).get("structure"))
    frame_flip = frame_structure.get("flip_zone")
    if isinstance(frame_flip, list | tuple) and len(frame_flip) >= 2:
        flip_low, flip_high = frame_flip[:2]
    else:
        raw_flip = payload.get("flip_zone")
        flip_low, flip_high = (
            raw_flip[:2]
            if isinstance(raw_flip, list | tuple) and len(raw_flip) >= 2
            else (None, None)
        )
    by_play = _candidate_by_play(dict(payload))

    def candidate_level(play: str) -> object:
        candidate = by_play.get(play)
        return candidate.get("level") if isinstance(candidate, dict) else None

    return {
        "put_wall": frame_structure.get("put_wall")
        if finite_float(frame_structure.get("put_wall")) is not None
        else candidate_level("put_wall_bounce_call"),
        "flip_low": flip_low,
        "flip_high": flip_high,
        "call_wall": frame_structure.get("call_wall")
        if finite_float(frame_structure.get("call_wall")) is not None
        else candidate_level("call_wall_fade_put"),
    }


def _gamma_feedback_text(payload: Mapping[str, Any]) -> str:
    frame = _mapping(payload.get("option_structure_frame"))
    structure = _mapping(frame.get("structure"))
    proxy = _mapping(payload.get("signed_gex_proxy"))
    state = str(
        structure.get("gamma_state")
        or proxy.get("gamma_state")
        or payload.get("gamma_state")
        or "unknown"
    )
    frame_quality = str(frame.get("quality") or "").lower()
    gex_quality = str(structure.get("gex_quality") or payload.get("gex_quality") or "")
    if frame and (frame_quality != "ready" or gex_quality not in {"", "open_interest_gex"}):
        state = "unknown"
    assumption = "Call+/Put− OI proxy；dealer sign unknown"
    ratio = finite_float(structure.get("net_gamma_ratio"))
    if ratio is None:
        ratio = finite_float(proxy.get("net_gamma_ratio"))
    ratio_text = f"；净比 {ratio:+.2f}" if ratio is not None else ""
    if state == "positive_gamma_pin":
        return (
            f"代理正 Gamma（{assumption}{ratio_text}）· 反馈偏压制/回归；"
            "只有价格拒绝后才支持反向，Gamma 不给 LONG/SHORT"
        )
    if state in {"negative_gamma_acceleration", "negative_gamma_expansion", "negative_gamma"}:
        return (
            f"代理负 Gamma（{assumption}{ratio_text}）· 反馈偏放大；"
            "只有价格接受后才支持顺势，Gamma 不给第一步方向"
        )
    if state == "zero_gamma_transition":
        return (
            f"Gamma 过渡（{assumption}{ratio_text}）· 情景敏感；"
            "价格选边前 NO TRADE"
        )
    return f"Gamma unavailable（{assumption}）· 不参与方向"


def _gamma_location_text(payload: Mapping[str, Any], spot: float | None) -> str:
    structure = _mapping(_mapping(payload.get("option_structure_frame")).get("structure"))
    live = _live_levels(payload)
    low = finite_float(live.get("flip_low"))
    high = finite_float(live.get("flip_high"))
    zero = finite_float(structure.get("zero_gamma"))
    if zero is None:
        zero = finite_float(payload.get("zero_gamma"))
    parts: list[str] = []
    if spot is not None and low is not None and high is not None:
        low, high = sorted((low, high))
        if spot < low:
            parts.append(f"Flip 下方 {low - spot:.1f}pt")
        elif spot > high:
            parts.append(f"Flip 上方 {spot - high:.1f}pt")
        else:
            parts.append(f"Flip 区间内 {low:g}–{high:g}")
    else:
        parts.append("Flip 位置 unavailable")
    if spot is not None and zero is not None:
        relation = "上方" if spot >= zero else "下方"
        parts.append(f"ZG {_dash(zero)} {relation} {abs(spot - zero):.1f}pt")
    else:
        parts.append("ZG unavailable")
    return "Gamma位置 " + " · ".join(parts)


def _observing_trigger(payload: Mapping[str, Any]) -> str:
    decision = _mapping(payload.get("level_decision"))
    phase = str(decision.get("phase") or "far").lower()
    event_level = finite_float(decision.get("level"))
    event_kind = str(decision.get("level_kind") or "")
    if (
        phase
        in {
            LevelPhase.APPROACHING.value,
            LevelPhase.TESTING.value,
            *[item.value for item in _ARMED_PHASES],
        }
        and event_level is not None
        and event_kind
    ):
        return (
            f"观察当前 {level_kind_label(event_kind)} {event_level:g} 的接受或拒绝；"
            "当前事件确认前 NO TRADE"
        )
    live = _live_levels(payload)
    put_wall = finite_float(live.get("put_wall"))
    call_wall = finite_float(live.get("call_wall"))
    low = finite_float(live.get("flip_low"))
    high = finite_float(live.get("flip_high"))
    underlier = _mapping(payload.get("underlier"))
    spot = finite_float(underlier.get("price"))
    if spot is None:
        spot = finite_float(_mapping(payload.get("level_decision")).get("spot"))
    flip_distance = (
        min(abs(spot - low), abs(spot - high))
        if spot is not None and low is not None and high is not None
        else None
    )
    wall_distances = [
        abs(spot - level)
        for level in (put_wall, call_wall)
        if spot is not None and level is not None
    ]
    nearest_wall_distance = min(wall_distances) if wall_distances else None
    if low is not None and high is not None and (
        spot is None
        or min(low, high) <= spot <= max(low, high)
        or nearest_wall_distance is None
        or (flip_distance is not None and flip_distance <= nearest_wall_distance)
    ):
        return (
            f"等待当前 Flip {min(low, high):g}–{max(low, high):g} 的接受或拒绝；"
            "确认前 NO TRADE"
        )
    if put_wall is not None or call_wall is not None:
        return (
            f"等待当前 Put {_available_number(put_wall)} / Call "
            f"{_available_number(call_wall)} 的接受或拒绝；确认前 NO TRADE"
        )
    return "等待实时关键位与价格接受/拒绝同时可用；当前 NO TRADE"


def _targets_line(payload: Mapping[str, Any], projection: DeskMapProjection) -> str:
    strategy_decision = _mapping(payload.get("strategy_decision"))
    if strategy_decision:
        candidate = _mapping(strategy_decision.get("candidate"))
        if not candidate or strategy_decision.get("decision_type") == "NO_TRADE":
            return "Targets  当前不做，无交易目标"
        targets = strategy_decision.get("targets") or ()
        target = next(
            (
                finite_float(_mapping(item).get("price"))
                for item in targets
                if finite_float(_mapping(item).get("price")) is not None
            ),
            finite_float(candidate.get("target_spx")),
        )
        return (
            f"Targets  primary {target:g}"
            if target is not None
            else "Targets  strategy_decision 候选缺少有效目标"
        )
    plans = [row for row in payload.get("plan_candidates") or () if isinstance(row, Mapping)]
    if projection.direction not in {"up", "down"} or projection.stage not in {
        DeskStage.CONFIRMED,
        DeskStage.READY,
    }:
        live = _live_levels(payload)
        return (
            "Targets  当前无交易目标 · 实时结构 "
            f"Put {_available_number(live.get('put_wall'))} / "
            f"Call {_available_number(live.get('call_wall'))}"
        )
    decision = _mapping(payload.get("level_decision"))
    intent = _mapping(payload.get("trade_intent"))
    manual = _mapping(payload.get("gth_level_manual_candidate"))
    intent_ready, manual_ready, current_plan = _current_ready_sources(
        decision,
        intent,
        manual,
        plans,
    )
    if projection.stage is DeskStage.READY and intent_ready and current_plan is not None:
        target = finite_float(current_plan.get("target_spx"))
        if target is not None:
            return f"Targets  primary {target:g}"
    if projection.stage is DeskStage.READY and manual_ready:
        target = finite_float(manual.get("target_spx"))
        if target is not None:
            return f"Targets  primary {target:g}"
    return "Targets  当前无可执行目标 · 等待当前机会生成并校验有效目标位"


def _execution_line(
    payload: Mapping[str, Any],
    projection: DeskMapProjection,
    guidance: guidance_module.DecisionGuidance,
) -> str:
    strategy_decision = _mapping(payload.get("strategy_decision"))
    if strategy_decision:
        candidate = _mapping(strategy_decision.get("candidate"))
        execution = _mapping(strategy_decision.get("execution"))
        if (
            strategy_candidate_is_watchable(payload, strategy_decision)
            and execution.get("action") == "MANUAL_LIMIT"
        ):
            opportunity = str(candidate.get("opportunity_id") or "-")
            short_opportunity = (
                opportunity if len(opportunity) <= 42 else opportunity[:39] + "..."
            )
            return (
                "Execution  可看 · 人工限价候选 · "
                f"机会 {short_opportunity}"
            )
        if current_session_is_gth(payload, _mapping(payload.get("level_decision"))):
            if _gth_provider_block_reason(payload) == "ibkr_competing_session":
                return (
                    "Execution  PAUSED · IBKR 10197实时行情冲突 · "
                    "等待新鲜SPXW双边报价自动恢复"
                )
            return "Execution  扫描中 · 仅人工候选可做"
        reasons = list(_mapping(strategy_decision.get("why_not")).get("reasons") or ())
        blocker = humanize_strategy_reason(
            str(reasons[0]) if reasons else "no_supported_strategy_candidate"
        )
        return f"Execution  等待 · 不做 · {blocker}"
    intent = _mapping(payload.get("trade_intent"))
    plans = [row for row in payload.get("plan_candidates") or () if isinstance(row, Mapping)]
    if projection.stage is DeskStage.READY:
        manual = _mapping(payload.get("gth_level_manual_candidate"))
        decision = _mapping(payload.get("level_decision"))
        intent_ready, manual_ready, _ = _current_ready_sources(
            decision,
            intent,
            manual,
            plans,
        )
        key = str(
            (intent.get("semantic_key") if intent_ready else None)
            or (intent.get("intent_id") if intent_ready else None)
            or (manual.get("candidate_id") if manual_ready else None)
            or (plans[0].get("contract_id") if len(plans) == 1 else None)
            or "-"
        )
        short_key = key if len(key) <= 42 else key[:39] + "..."
        return (
            f"Execution  READY · 独立 MANUAL READY 卡承载实时合约与报价 · opportunity {short_key}"
        )
    if projection.stage is DeskStage.CONFIRMED:
        return "Execution  HOLD · 方向已确认，执行门控未完成"
    if projection.stage is DeskStage.PAUSED:
        if "ready_opportunity_mismatch" in projection.quality_reasons:
            return "Execution  PAUSED · READY 不属于当前价格事件，禁止使用旧机会或旧目标"
        if "ready_required_frame_unavailable" in projection.quality_reasons:
            return "Execution  PAUSED · 市场、期权结构或 L1 帧缺失，禁止执行 READY"
        if "ready_without_current_confirmed_path" in projection.quality_reasons:
            return "Execution  PAUSED · 执行卡与当前价格路径不一致，禁止使用旧 READY"
        return f"Execution  PAUSED · {guidance.action_text}"
    if projection.phase in {LevelPhase.INVALIDATED, LevelPhase.EXPIRED}:
        return "Execution  WAIT · 当前没有可执行机会；新事件确认后再评估"
    return "Execution  WAIT · 尚无确定性结构入场"


def _data_quality(
    payload: Mapping[str, Any],
    decision: Mapping[str, Any],
    *,
    invalid_phase: bool = False,
) -> tuple[str, tuple[str, ...]]:
    reasons: list[str] = []
    if (
        current_session_is_gth(payload, decision)
        and _gth_provider_block_reason(payload) == "ibkr_competing_session"
    ):
        reasons.append("ibkr_competing_session")
    if invalid_phase:
        reasons.append("unknown_level_phase")
    if decision.get("snapshot_consistent") is False:
        reasons.append("decision_snapshot_inconsistent")
    if decision.get("quality_ok") is False:
        reasons.extend(str(decision.get("quality_reason") or "level_quality_failed").split(";"))
    if decision.get("quality_status") == "degraded":
        reasons.extend(str(decision.get("quality_reason") or "market_data_degraded").split(";"))

    market = _mapping(payload.get("minute_market_frame"))
    market_quality = str(market.get("quality") or "").lower()
    if not market:
        reasons.append("market_frame:unavailable")
    elif not market_quality:
        reasons.append("market_frame:unavailable")
    elif market_quality != "ready":
        reasons.append(f"market_frame:{market_quality}")
    frame = _mapping(payload.get("option_structure_frame"))
    frame_quality = str(frame.get("quality") or "").lower()
    if not frame:
        reasons.append("option_frame:unavailable")
    elif not frame_quality:
        reasons.append("option_frame:unavailable")
    elif frame_quality != "ready":
        reasons.append(f"option_frame:{frame_quality}")
    exposure = _mapping(frame.get("exposure"))
    structure = _mapping(frame.get("structure"))
    l1 = _mapping(frame.get("l1"))
    l1_quality = str(l1.get("quality") or "").lower()
    if not l1:
        reasons.append("option_l1:unavailable")
    elif not l1_quality:
        reasons.append("option_l1:unavailable")
    elif l1_quality != "ready":
        reasons.append(f"option_l1:{l1_quality}")
    oi_quality = str(exposure.get("oi_quality") or "")
    if oi_quality and oi_quality != "ibkr_ok":
        reasons.append(f"oi:{oi_quality}")
    gex_quality = str(structure.get("gex_quality") or "")
    if gex_quality and gex_quality != "open_interest_gex":
        reasons.append(f"gex:{gex_quality}")
    strategy_quality = _mapping(_mapping(payload.get("strategy_decision")).get("data_quality"))
    if strategy_quality and str(strategy_quality.get("status") or "").lower() != "ready":
        _extend_reasons(reasons, strategy_quality.get("reasons"), prefix="strategy:")
        if not strategy_quality.get("reasons"):
            reasons.append("strategy:data_quality_degraded")
    if current_session_is_gth(payload, decision):
        for warning in payload.get("warnings") or ():
            token = str(warning or "").strip()
            if token == "ibkr_feed_unavailable" or "ibkr feed unavailable" in token.lower():
                reasons.append(token)
    if current_session_is_gth(payload, decision):
        reasons = [reason for reason in reasons if not _rth_only_quality_reason(reason)]
    else:
        reasons = [
            reason for reason in reasons if not _rth_expected_ibkr_absent_reason(reason)
        ]
    unique = tuple(dict.fromkeys(reason for reason in reasons if reason))
    return ("DEGRADED", unique) if unique else ("READY", ())


def _advisory_quality_reasons(
    payload: Mapping[str, Any], decision: Mapping[str, Any]
) -> tuple[str, ...]:
    reasons: list[str] = []
    market = _mapping(payload.get("minute_market_frame"))
    market_diagnostics = _mapping(market.get("diagnostics"))
    market_state = _mapping(market_diagnostics.get("rth_market_state"))
    if str(market_state.get("status") or "").lower() in {"provisional", "uncertain"}:
        _extend_reasons(reasons, market_state.get("reasons"), prefix="market_state:")
    _extend_reasons(reasons, market_diagnostics.get("warnings"))

    frame = _mapping(payload.get("option_structure_frame"))
    density = _mapping(frame.get("density"))
    clipped = finite_float(density.get("clipped_mass_fraction"))
    if clipped is not None and clipped >= 0.10:
        reasons.append(f"density_clipped:{clipped:.0%}")
    for section in (
        frame.get("diagnostics"),
        _mapping(frame.get("exposure")),
        _mapping(frame.get("structure")),
        density,
        _mapping(frame.get("l1")).get("diagnostics"),
    ):
        _extend_reasons(reasons, _mapping(section).get("warnings"))
    _extend_reasons(reasons, payload.get("warnings"))
    if current_session_is_gth(payload, decision):
        reasons = [reason for reason in reasons if not _rth_only_quality_reason(reason)]
    else:
        reasons = [
            reason for reason in reasons if not _rth_expected_ibkr_absent_reason(reason)
        ]
    return tuple(dict.fromkeys(reason for reason in reasons if reason))


def _data_quality_line(
    payload: Mapping[str, Any], projection: DeskMapProjection
) -> str:
    if not projection.quality_reasons:
        status = "执行数据 READY · 决策坐标、结构与实时报价可用"
    else:
        primary = quality_reason_text(projection.quality_reasons[0])
        count = len(projection.quality_reasons)
        secondary = (
            f" · 次要影响：{quality_reason_text(projection.quality_reasons[1])}"
            if count >= 2
            else ""
        )
        status = f"执行数据 {projection.data_quality} · 主要影响：{primary}{secondary} · 共 {count} 项"
    advisory = _advisory_quality_reasons(payload, _mapping(payload.get("level_decision")))
    if advisory:
        status += (
            f"\n研究层 DEGRADED · {quality_reason_text(advisory[0])}"
            "（不改变 NO TRADE/READY 授权）"
        )
    return f"Data Quality  {status}"

def _opening_range_text(payload: Mapping[str, Any]) -> str:
    state = _rth_market_state(payload)
    lineage = _mapping(state.get("input_lineage"))
    values = _mapping(lineage.get("values"))
    raw_state = str(values.get("opening_range_state") or "").strip()
    if not raw_state:
        availability = _mapping(state.get("input_availability"))
        fields = _mapping(availability.get("fields"))
        opening = _mapping(fields.get("opening_range_state"))
        raw_state = str(opening.get("value") or "").strip()
    if not raw_state:
        return "OR unavailable"

    diagnostics = _mapping(lineage.get("diagnostics"))
    opening = _mapping(diagnostics.get("opening_range"))
    orh = finite_float(opening.get("orh"))
    orl = finite_float(opening.get("orl"))
    state_text = opening_range_state_text(raw_state)
    if opening.get("status") == "ready" and orh is not None and orl is not None:
        return f"OR {state_text}（ORL {orl:g} / ORH {orh:g}）"
    return f"OR {state_text}（区间值 unavailable）"


def _rth_market_state(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    market = _mapping(payload.get("minute_market_frame"))
    diagnostics = _mapping(market.get("diagnostics"))
    direct = _mapping(diagnostics.get("rth_market_state"))
    if direct:
        return direct
    shadow = _mapping(payload.get("spring_gamma_v3_shadow"))
    return _mapping(shadow.get("rth_market_state"))


def _levels_text(levels: Mapping[str, Any]) -> str:
    return (
        f"{_available_number(levels.get('put_wall'))} / "
        f"{_available_number(levels.get('flip_low'))}–"
        f"{_available_number(levels.get('flip_high'))} / "
        f"{_available_number(levels.get('call_wall'))}"
    )


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _gth_provider_block_reason(payload: Mapping[str, Any]) -> str:
    control = _mapping(payload.get("strategy_entry_control"))
    if control.get("allowed") is True:
        return ""
    return str(control.get("reason") or "")


def _rth_only_quality_reason(reason: str) -> bool:
    # Cash-session and RTH-only diagnostics are expected N/A overnight.
    # GTH structure also uses analytical-only IBKR legs; that is not a
    # human-facing outage while the option frame remains ready.
    return (
        reason.startswith("market_state:")
        or reason == "rth_heartbeat_degraded_snapshot"
        or reason.startswith("analytical_leg_rejected:analytical_only_non_executable")
        or reason.startswith("cash_index_")
    )


def _rth_expected_ibkr_absent_reason(reason: str) -> bool:
    # RTH prices and SPXW NBBO come from Schwab. IBKR Live is shared with the
    # user's TWS/mobile session, so 10197 / feed-down is expected and must not
    # mark the desk DEGRADED while Schwab frames remain ready.
    token = str(reason or "").strip()
    lowered = token.lower()
    return (
        token
        in {
            "oi:schwab_unverified",
            "schwab_oi_unverified",
            "schwab_unverified",
            "ibkr_feed_unavailable",
            "open interest wall scope:ibkr_hot_lane",
            "open interest wall scope:schwab_rth_lane",
        }
        or "ibkr feed unavailable" in lowered
        or "stale spxw option quotes suppressed" in lowered
    )


def _extend_reasons(
    reasons: list[str],
    value: object,
    *,
    prefix: str = "",
) -> None:
    if not isinstance(value, list | tuple):
        return
    reasons.extend(
        f"{prefix}{text}" for item in value if item is not None and (text := str(item).strip())
    )


def _available_number(value: object) -> str:
    parsed = finite_float(value)
    return f"{parsed:g}" if parsed is not None else "unavailable"


def _signed_points(value: object) -> str:
    parsed = finite_float(value)
    return f"{parsed:+g}pt" if parsed is not None else "unavailable"

def _closed_direction(value: object) -> str:
    direction = str(value or "none").lower()
    return direction if direction in {"up", "down"} else "none"


def _closed_thesis(value: object) -> str:
    thesis = str(value or "none").lower()
    return thesis if thesis in {"breakout", "fade"} else "none"
