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
    underlier_source_label,
)
from spx_spark.application.order_map.state import _session_phase_of, current_session_is_gth
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
    lines.append(operator_reason_line(payload))
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
    desk_view = _desk_view_line(projection, guidance).removeprefix("Desk View  ")
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
            guidance.invalidation_text
            if projection.direction in {"up", "down"}
            else "尚无单边方向；当前不存在交易失效位，任一实时结构形成接受/拒绝后重算"
        ),
        targets=_targets_line(payload, projection).removeprefix("Targets  "),
        execution=_execution_line(payload, projection, guidance).removeprefix("Execution  "),
        data_quality=_data_quality_line(projection).removeprefix("Data Quality  "),
    )


def _strategy_surface_shape_line(payload: Mapping[str, Any]) -> str | None:
    decision = _mapping(payload.get("strategy_decision"))
    facts = _mapping(decision.get("market_facts"))
    structure = _mapping(facts.get("structure"))
    context = _mapping(structure.get("strike_differential_context"))
    if not context:
        return None
    return str(summarize_strike_surface_shape(context)["desk_line"])


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
    projection: DeskMapProjection,
    guidance: guidance_module.DecisionGuidance,
) -> str:
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
        path = f"{side} {_thesis_label(projection.thesis)}"
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
    phase_label = (
        "已确认"
        if projection.stage is DeskStage.READY and projection.phase is LevelPhase.FAR
        else _phase_label(projection.phase)
    )
    return f"Desk View  {signal} · 状态：{_stage_label(projection.stage)}（{phase_label}）"


def _location_line(payload: Mapping[str, Any], projection: DeskMapProjection) -> str:
    underlier = _mapping(payload.get("underlier"))
    spx = finite_float(underlier.get("price"))
    actionable_spx = spx
    source = str(underlier.get("source") or "")
    if spx is None:
        decision = _mapping(payload.get("level_decision"))
        spx = finite_float(decision.get("spot"))
        spx_text = "unavailable"
        if spx is not None:
            spx_text += (
                f" · reference {spx:g}（{_decision_spot_reference_label(decision)}；"
                "latched/proxy，not actionable）"
            )
    else:
        spx_text = _dash(spx)
        if source and source != "index:SPX":
            spx_text += f"（{underlier_source_label(source)}）"
    level_text = ""
    if projection.level is not None:
        distance = (
            abs(actionable_spx - projection.level) if actionable_spx is not None else None
        )
        level_text = f" · {_level_kind_label(projection.level_kind)} {projection.level:g}" + (
            f" · 距离 {distance:.1f}pt" if distance is not None else ""
        )
    market = _mapping(payload.get("minute_market_frame"))
    es = _mapping(market.get("es"))
    es_last = finite_float(payload.get("es_last"))
    if es_last is None:
        es_last = finite_float(es.get("price"))
    vwap = finite_float(es.get("vwap"))
    vwap_distance = finite_float(es.get("vwap_distance_points"))
    vwap_text = "ES VWAP unavailable"
    if vwap is not None:
        vwap_text = f"ES VWAP {vwap:g}"
    if vwap_distance is not None:
        vwap_text += f"（偏离 {vwap_distance:+g}pt）"
    elif vwap is not None:
        vwap_text += "（偏离 unavailable）"
    return (
        f"Location  SPX {spx_text} · ES {_available_number(es_last)}"
        f"{level_text} · {_gamma_location_text(payload, actionable_spx)} · {vwap_text} · "
        f"{_opening_range_text(payload)} · "
        f"{_expected_move_text(payload)}"
    )


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
            f"方向来源  {direction} 来自 {_level_kind_label(projection.level_kind)} "
            f"{projection.level:g} 的{path}；Gamma 不提供第一步方向"
        )
        if projection.stage is DeskStage.READY:
            trigger = "当前路径已确认；执行仅以独立 MANUAL READY 卡的实时合约与报价为准"
        elif projection.stage is DeskStage.CONFIRMED:
            trigger = "当前路径已确认；等待实时合约、报价与盈亏比通过执行门控"
        else:
            trigger = humanize_operator_trigger(guidance.trigger_text)
    else:
        basis = f"方向来源  尚无价格接受/拒绝确认；{guidance.bias}仅为 ES/量价背景"
        trigger = _observing_trigger(payload)
    flow = (
        f"流确认  ES 15m {_signed_points(es.get('return_15m_points'))} / "
        f"60m {_signed_points(es.get('return_60m_points'))} · "
        f"量价 {_volume_alignment_text(volume.get('price_volume_alignment_5m'))} · "
        "ES/SPY "
        f"{_cross_asset_confirmation_text(cross_asset.get('es_spy_direction_confirmation_15m'))}"
    )
    return f"{basis}\n下一触发  {trigger}\n{flow}"


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
    live_frame = option_structure_frame_is_live(payload, now=now)
    if live_frame:
        levels = (
            f"Put/Flip/Call {frozen_text} · event=live"
            if same
            else f"event {frozen_text} · live {live_text}"
        )
        source_label = "source=live"
    else:
        levels = (
            f"Put/Flip/Call {frozen_text} · event=frozen/reference"
            if same
            else f"event {frozen_text} · reference {live_text}"
        )
        source_label = "source=frozen/reference · live confirmation unavailable"
    change_line = _structure_change_line(
        decision,
        current_levels=frozen if frozen else live,
        source_label=source_label,
    )
    parts = [f"Structure  {levels}"]
    if change_line:
        parts.append(change_line)
    parts.append(f"Gamma职责  {_gamma_feedback_text(payload)}")
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
        parts.append(f"ZG {zero:g} {relation} {abs(spot - zero):.1f}pt")
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
            f"观察当前 {_level_kind_label(event_kind)} {event_level:g} 的接受或拒绝；"
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
    market_diagnostics = _mapping(market.get("diagnostics"))
    market_state = _mapping(market_diagnostics.get("rth_market_state"))
    if str(market_state.get("status") or "").lower() in {"provisional", "uncertain"}:
        _extend_reasons(reasons, market_state.get("reasons"), prefix="market_state:")
    _extend_reasons(reasons, market_diagnostics.get("warnings"))

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
    density = _mapping(frame.get("density"))
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
    clipped = finite_float(density.get("clipped_mass_fraction"))
    if clipped is not None and clipped >= 0.10:
        reasons.append(f"density_clipped:{clipped:.0%}")
    for section in (
        frame.get("diagnostics"),
        exposure,
        structure,
        density,
        l1.get("diagnostics"),
    ):
        _extend_reasons(reasons, _mapping(section).get("warnings"))
    warnings = payload.get("warnings")
    _extend_reasons(reasons, warnings)
    if current_session_is_gth(payload, decision):
        reasons = [reason for reason in reasons if not _rth_only_quality_reason(reason)]
    unique = tuple(dict.fromkeys(reason for reason in reasons if reason))
    return ("DEGRADED", unique) if unique else ("READY", ())


def _data_quality_line(projection: DeskMapProjection) -> str:
    if not projection.quality_reasons:
        status = "READY · 决策坐标与结构快照可用"
    else:
        primary = _quality_reason_text(projection.quality_reasons[0])
        count = len(projection.quality_reasons)
        secondary = (
            f" · 次要影响：{_quality_reason_text(projection.quality_reasons[1])}"
            if count >= 2
            else ""
        )
        status = f"{projection.data_quality} · 主要影响：{primary}{secondary} · 共 {count} 项"
    return f"Data Quality  {status}"


def _quality_reason_text(reason: str) -> str:
    labels = {
        "spx_price_unavailable": "SPX 触发坐标不可用，不能确认方向",
        "option_frame:unavailable": "期权结构帧不可用，Gamma 与墙位不可靠",
        "option_frame:degraded": "期权结构帧降级",
        "option_l1:unavailable": "SPXW 双边报价不可用",
        "option_l1:degraded": "SPXW 报价覆盖降级",
        "oi:missing": "OI 不可用，Gamma 代理失效",
        "oi:schwab_unverified": "Schwab OI 未验证，Gamma 代理仅供审计",
        "gex:no_open_interest_gex": "缺少 OI-GEX，不能解释 Gamma 机制",
        "decision_snapshot_inconsistent": "旧事件与当前结构不一致",
        "unknown_level_phase": "状态机阶段非法",
        "market_frame:unavailable": "市场帧不可用，ES 流确认不能验证",
        "market_frame:degraded": "市场帧降级，ES 流确认需谨慎",
        "ready_opportunity_mismatch": "READY 不属于当前价格事件，旧机会与旧目标已禁用",
        "ready_required_frame_unavailable": "必需数据帧缺失，READY 已暂停",
        "ready_without_current_confirmed_path": "旧 READY 与当前价格路径不一致，已禁止执行",
    }
    if reason.startswith("density_clipped:"):
        return f"概率密度裁剪偏高（{reason.partition(':')[2]}）"
    if reason.startswith("underlier_mismatch:"):
        return "标的坐标不匹配，墙位与 Gamma 告警已抑制"
    return labels.get(reason, reason.replace("_", " "))


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
    state_text = _opening_range_state_text(raw_state)
    if opening.get("status") == "ready" and orh is not None and orl is not None:
        return f"OR {state_text}（ORL {orl:g} / ORH {orh:g}）"
    return f"OR {state_text}（区间值 unavailable）"


def _opening_range_state_text(value: str) -> str:
    token = value.upper()
    return {
        "ABOVE_ORH_CONFIRMED": "上沿上方确认",
        "BREAKOUT_ABOVE_ORH": "突破上沿待确认",
        "INSIDE": "区间内",
        "BREAKDOWN_BELOW_ORL": "跌破下沿待确认",
        "BELOW_ORL_CONFIRMED": "下沿下方确认",
    }.get(token, token)


def _expected_move_text(payload: Mapping[str, Any]) -> str:
    frame = _mapping(payload.get("option_structure_frame"))
    volatility = _mapping(frame.get("volatility"))
    expected_move = finite_float(payload.get("expected_move_points"))
    if expected_move is None:
        expected_move = finite_float(volatility.get("expected_move_points_0dte"))
    if expected_move is None or expected_move <= 0:
        return "EM unavailable"

    # A live 0DTE EM shrinks to expiry; publish usage only for an explicitly
    # matching numerator/denominator horizon, never an earlier session range.
    usage = _aligned_expected_move_usage(payload)
    if usage is None:
        return f"EM ±{expected_move:g}pt"
    label, fraction = usage
    return f"EM ±{expected_move:g}pt · {label} 已用 {fraction:.0%}"


def _aligned_expected_move_usage(payload: Mapping[str, Any]) -> tuple[str, float] | None:
    day_move = _mapping(payload.get("day_move"))
    fraction = finite_float(day_move.get("em_used_fraction"))
    numerator_horizon = str(day_move.get("em_numerator_horizon_id") or "").strip()
    denominator_horizon = str(day_move.get("em_denominator_horizon_id") or "").strip()
    if (
        fraction is None
        or fraction < 0
        or not numerator_horizon
        or numerator_horizon != denominator_horizon
    ):
        return None
    label = str(day_move.get("em_usage_label") or "matched horizon").strip()
    return label, fraction


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


def _rth_only_quality_reason(reason: str) -> bool:
    # These reasons come from rth_market_state and are expected N/A outside
    # the cash session; they must not make an otherwise valid GTH frame look
    # broken.  The structured wire still retains every applicable GTH reason.
    return reason.startswith("market_state:") or reason == "rth_heartbeat_degraded_snapshot"


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


def _volume_alignment_text(value: object) -> str:
    return {
        "price_volume_aligned": "同向确认",
        "price_volume_divergent": "背离",
        "price_without_volume_confirmation": "价格缺少量能确认",
        "volume_without_price_progress": "放量但价格未推进",
        "flat": "平稳",
        "unavailable": "unavailable",
    }.get(str(value or ""), "unavailable")


def _cross_asset_confirmation_text(value: object) -> str:
    return {
        "confirmed": "同向确认",
        "divergent": "背离",
        "unavailable": "unavailable",
    }.get(str(value or ""), "unavailable")


def _closed_direction(value: object) -> str:
    direction = str(value or "none").lower()
    return direction if direction in {"up", "down"} else "none"


def _closed_thesis(value: object) -> str:
    thesis = str(value or "none").lower()
    return thesis if thesis in {"breakout", "fade"} else "none"


def _thesis_label(value: str) -> str:
    return {"breakout": "BREAKOUT", "fade": "FADE"}.get(value, "SETUP")


def _level_kind_label(value: str) -> str:
    return {
        "put_wall": "Put Wall",
        "flip_low": "Flip Low",
        "flip_high": "Flip High",
        "call_wall": "Call Wall",
    }.get(value, "Level")


def _phase_label(phase: LevelPhase) -> str:
    return {
        LevelPhase.FAR: "尚未触发",
        LevelPhase.APPROACHING: "接近关键位",
        LevelPhase.TESTING: "测试关键位",
        LevelPhase.BREAK_PENDING: "突破待确认",
        LevelPhase.REJECT_PENDING: "拒绝待确认",
        LevelPhase.ACCEPTED: "已接受，等待回踩",
        LevelPhase.REJECTED: "已拒绝，等待回踩",
        LevelPhase.RETEST: "回踩确认中",
        LevelPhase.CONFIRMED: "已确认",
        LevelPhase.INVALIDATED: "已失效",
        LevelPhase.EXPIRED: "已过期",
    }[phase]


def _stage_label(stage: DeskStage) -> str:
    return {
        DeskStage.OBSERVING: "观察中",
        DeskStage.WATCHING: "接近结构",
        DeskStage.ARMED: "条件形成中",
        DeskStage.CONFIRMED: "方向已确认",
        DeskStage.READY: "执行候选已就绪",
        DeskStage.INVALIDATED: "已失效",
        DeskStage.EXPIRED: "已过期",
        DeskStage.PAUSED: "已暂停",
    }[stage]
