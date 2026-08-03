"""Deterministic, structure-first projection for human RTH/GTH desk maps."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Mapping

from spx_spark.analytics.options.pricing import finite_float
from spx_spark.application.order_map import guidance as guidance_module
from spx_spark.application.order_map.level_decision_machine import LevelPhase
from spx_spark.application.order_map.models import SHANGHAI_TZ
from spx_spark.application.order_map.render import (
    _candidate_by_play,
    _dash,
    underlier_source_label,
)
from spx_spark.application.order_map.state import _session_phase_of
from spx_spark.application.order_map.status_explanation import (
    humanize_operator_trigger,
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

    return DeskMessageSections(
        title=(
            f"【SPX Desk Map · {beijing.strftime('%H:%M')} · "
            f"0DTE {expiry_text} · {session.get('name_cn')}】"
        ),
        desk_view=_without_prefix(_desk_view_line(projection, guidance), "Desk View  "),
        location=_without_prefix(_location_line(payload, projection), "Location  "),
        structure=_without_prefix(_structure_line(payload), "Structure  "),
        primary_path=_primary_path(payload, guidance),
        alternative_path=guidance.invalidation_text,
        targets=_without_prefix(_targets_line(payload), "Targets  "),
        execution=_without_prefix(
            _execution_line(payload, projection, guidance),
            "Execution  ",
        ),
        data_quality=_without_prefix(
            _data_quality_line(payload, projection),
            "Data Quality  ",
        ),
    )


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
    ready = intent.get("status") == "trade_ready" or manual.get("status") == "manual_ready"
    if ready:
        stage = DeskStage.READY
    elif guidance.action is guidance_module.GuidanceAction.PAUSED:
        stage = DeskStage.PAUSED
    elif phase is LevelPhase.CONFIRMED:
        stage = DeskStage.CONFIRMED
    elif phase in _ARMED_PHASES:
        stage = DeskStage.ARMED
    elif phase in {LevelPhase.APPROACHING, LevelPhase.TESTING}:
        stage = DeskStage.WATCHING
    elif phase is LevelPhase.INVALIDATED:
        stage = DeskStage.INVALIDATED
    elif phase is LevelPhase.EXPIRED:
        stage = DeskStage.EXPIRED
    else:
        stage = DeskStage.OBSERVING
    decision_direction = _closed_direction(decision.get("direction"))
    direction = (
        decision_direction
        if decision_direction in {"up", "down"}
        else _closed_direction(guidance.bias_direction)
    )
    thesis = _closed_thesis(decision.get("thesis"))
    if ready and len(plans) == 1:
        if decision_direction not in {"up", "down"}:
            direction = {"C": "up", "P": "down"}.get(str(plans[0].get("right") or ""), direction)
        play = str(plans[0].get("play") or "")
        if thesis not in {"breakout", "fade"}:
            thesis = "breakout" if "breakout" in play else "fade" if "fade" in play else thesis
    if ready and direction not in {"up", "down"}:
        direction = _closed_direction(manual.get("direction"))
    if ready and thesis not in {"breakout", "fade"}:
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


def _desk_view_line(
    projection: DeskMapProjection,
    guidance: guidance_module.DecisionGuidance,
) -> str:
    if projection.stage is DeskStage.PAUSED:
        signal = "PAUSED"
    elif projection.stage in {DeskStage.INVALIDATED, DeskStage.EXPIRED}:
        signal = "NO ACTIVE SETUP"
    elif projection.direction == "up" and projection.thesis in {"breakout", "fade"}:
        signal = f"CALL {_thesis_label(projection.thesis)}"
    elif projection.direction == "down" and projection.thesis in {"breakout", "fade"}:
        signal = f"PUT {_thesis_label(projection.thesis)}"
    elif projection.stage is DeskStage.OBSERVING:
        signal = f"NO SETUP · {guidance.bias}（context）"
    else:
        signal = "STRUCTURE PENDING"
    phase_label = (
        "CONFIRMED"
        if projection.stage is DeskStage.READY and projection.phase is LevelPhase.FAR
        else _phase_label(projection.phase)
    )
    return f"Desk View  {signal} · {projection.stage.value} · {phase_label}"


def _location_line(payload: Mapping[str, Any], projection: DeskMapProjection) -> str:
    underlier = _mapping(payload.get("underlier"))
    spx = finite_float(underlier.get("price"))
    if spx is None:
        decision = _mapping(payload.get("level_decision"))
        spx = finite_float(decision.get("spot"))
    source = str(underlier.get("source") or "")
    spx_text = _dash(spx)
    if source and source != "index:SPX":
        spx_text += f"（{underlier_source_label(source)}）"
    level_text = ""
    if projection.level is not None:
        distance = abs(spx - projection.level) if spx is not None else None
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
        f"{level_text} · {vwap_text} · {_opening_range_text(payload)} · "
        f"{_expected_move_text(payload)}"
    )


def _primary_path(
    payload: Mapping[str, Any],
    guidance: guidance_module.DecisionGuidance,
) -> str:
    market = _mapping(payload.get("minute_market_frame"))
    es = _mapping(market.get("es"))
    volume = _mapping(market.get("volume"))
    cross_asset = _mapping(market.get("cross_asset"))
    return (
        f"{humanize_operator_trigger(guidance.trigger_text)} · "
        f"ES动量 15m {_signed_points(es.get('return_15m_points'))} / "
        f"60m {_signed_points(es.get('return_60m_points'))} · "
        f"量价 {_volume_alignment_text(volume.get('price_volume_alignment_5m'))} · "
        "ES/SPY "
        f"{_cross_asset_confirmation_text(cross_asset.get('es_spy_direction_confirmation_15m'))}"
    )


def _structure_line(payload: Mapping[str, Any]) -> str:
    decision = _mapping(payload.get("level_decision"))
    frozen = _mapping(decision.get("levels"))
    by_play = _candidate_by_play(dict(payload))
    flip_zone = payload.get("flip_zone")
    live_flip = flip_zone if isinstance(flip_zone, list) and len(flip_zone) >= 2 else ()

    def candidate_level(play: str) -> object:
        candidate = by_play.get(play)
        return candidate.get("level") if isinstance(candidate, dict) else None

    live = {
        "put_wall": candidate_level("put_wall_bounce_call"),
        "flip_low": live_flip[0] if live_flip else None,
        "flip_high": live_flip[1] if live_flip else None,
        "call_wall": candidate_level("call_wall_fade_put"),
    }
    frozen_text = _levels_text(frozen) if frozen else "unavailable"
    live_text = _levels_text(live)
    same = bool(frozen) and all(
        finite_float(frozen.get(key)) == finite_float(live.get(key))
        for key in ("put_wall", "flip_low", "flip_high", "call_wall")
    )
    if same:
        return f"Structure  Put/Flip/Call {frozen_text} · frozen=live"
    return f"Structure  event {frozen_text} · live {live_text}"


def _targets_line(payload: Mapping[str, Any]) -> str:
    plans = [row for row in payload.get("plan_candidates") or () if isinstance(row, Mapping)]
    if len(plans) == 1 and finite_float(plans[0].get("target_spx")) is not None:
        return f"Targets  primary {finite_float(plans[0].get('target_spx')):g}"
    decision = _mapping(payload.get("level_decision"))
    levels = _mapping(decision.get("levels"))
    return (
        f"Targets  downside Put {_dash(levels.get('put_wall'))} · "
        f"upside Call {_dash(levels.get('call_wall'))}"
    )


def _execution_line(
    payload: Mapping[str, Any],
    projection: DeskMapProjection,
    guidance: guidance_module.DecisionGuidance,
) -> str:
    intent = _mapping(payload.get("trade_intent"))
    plans = [row for row in payload.get("plan_candidates") or () if isinstance(row, Mapping)]
    if projection.stage is DeskStage.READY:
        manual = _mapping(payload.get("gth_level_manual_candidate"))
        key = str(
            intent.get("semantic_key")
            or intent.get("intent_id")
            or manual.get("candidate_id")
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
        return f"Execution  PAUSED · {guidance.action_text}"
    if projection.stage in {DeskStage.INVALIDATED, DeskStage.EXPIRED}:
        return f"Execution  CLOSED · {projection.stage.value} · 等待离开 reset band 后重新武装"
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
    if market_quality and market_quality != "ready":
        reasons.append(f"market_frame:{market_quality}")
    market_diagnostics = _mapping(market.get("diagnostics"))
    market_state = _mapping(market_diagnostics.get("rth_market_state"))
    if str(market_state.get("status") or "").lower() in {"provisional", "uncertain"}:
        _extend_reasons(reasons, market_state.get("reasons"), prefix="market_state:")
    _extend_reasons(reasons, market_diagnostics.get("warnings"))

    frame = _mapping(payload.get("option_structure_frame"))
    frame_quality = str(frame.get("quality") or "").lower()
    if frame_quality and frame_quality != "ready":
        reasons.append(f"option_frame:{frame_quality}")
    exposure = _mapping(frame.get("exposure"))
    structure = _mapping(frame.get("structure"))
    density = _mapping(frame.get("density"))
    l1 = _mapping(frame.get("l1"))
    l1_quality = str(l1.get("quality") or "").lower()
    if l1_quality and l1_quality != "ready":
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
    unique = tuple(dict.fromkeys(reason for reason in reasons if reason))
    return ("DEGRADED", unique) if unique else ("READY", ())


def _data_quality_line(
    payload: Mapping[str, Any],
    projection: DeskMapProjection,
) -> str:
    if not projection.quality_reasons:
        status = "READY · 决策坐标与结构快照可用"
    else:
        status = f"{projection.data_quality} · {'; '.join(projection.quality_reasons)}"
    return (
        f"Data Quality  {status} · {_volatility_iv_text(payload)} · {_frame_quality_text(payload)}"
    )


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

    day_move = _mapping(payload.get("day_move"))
    used = finite_float(day_move.get("em_used_fraction"))
    used_label = "GTH"
    if used is None:
        market = _mapping(payload.get("minute_market_frame"))
        es = _mapping(market.get("es"))
        used = finite_float(es.get("gth_expected_move_used"))
    if used is None:
        lineage = _mapping(_rth_market_state(payload).get("input_lineage"))
        diagnostics = _mapping(lineage.get("diagnostics"))
        same_time = _mapping(diagnostics.get("same_time_range"))
        current_range = finite_float(same_time.get("current_range_points"))
        if current_range is not None and current_range >= 0:
            used = current_range / expected_move
            used_label = "RTH range"
    if used is None:
        market = _mapping(payload.get("minute_market_frame"))
        es = _mapping(market.get("es"))
        used = finite_float(es.get("overnight_expected_move_used"))
        used_label = "overnight range"
    used_text = (
        f"{used_label} 已用 {used:.0%}"
        if used is not None and used >= 0
        else "已用比例 unavailable"
    )
    return f"EM ±{expected_move:g}pt · {used_text}"


def _rth_market_state(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    market = _mapping(payload.get("minute_market_frame"))
    diagnostics = _mapping(market.get("diagnostics"))
    direct = _mapping(diagnostics.get("rth_market_state"))
    if direct:
        return direct
    shadow = _mapping(payload.get("spring_gamma_v3_shadow"))
    return _mapping(shadow.get("rth_market_state"))


def _volatility_iv_text(payload: Mapping[str, Any]) -> str:
    option_frame = _mapping(payload.get("option_structure_frame"))
    option_volatility = _mapping(option_frame.get("volatility"))
    market_frame = _mapping(payload.get("minute_market_frame"))
    market_volatility = _mapping(market_frame.get("volatility"))
    parts: list[str] = []
    atm_iv = finite_float(option_volatility.get("atm_iv_0dte"))
    if atm_iv is not None:
        parts.append(f"ATM IV 0DTE {atm_iv * 100:.2f}%")
    iv_changes = [
        finite_float(option_volatility.get(f"atm_iv_change_{minutes}m")) for minutes in (5, 15, 60)
    ]
    if any(value is not None for value in iv_changes):
        parts.append(
            "IVΔ 5/15/60m "
            + "/".join(
                f"{value * 100:+.2f}vol" if value is not None else "unavailable"
                for value in iv_changes
            )
        )
    vix1d = finite_float(market_volatility.get("vix1d"))
    vix = finite_float(market_volatility.get("vix"))
    if vix1d is not None or vix is not None:
        parts.append(f"VIX1D/VIX {_available_number(vix1d)}/{_available_number(vix)}")
    return f"Vol/IV {' · '.join(parts)}" if parts else "Vol/IV unavailable"


def _frame_quality_text(payload: Mapping[str, Any]) -> str:
    market = _mapping(payload.get("minute_market_frame"))
    options = _mapping(payload.get("option_structure_frame"))
    l1 = _mapping(options.get("l1"))
    return (
        "Frames "
        f"market={str(market.get('quality') or 'unavailable').upper()} · "
        f"options={str(options.get('quality') or 'unavailable').upper()} · "
        f"L1={str(l1.get('quality') or 'unavailable').upper()}"
    )


def _levels_text(levels: Mapping[str, Any]) -> str:
    return (
        f"{_available_number(levels.get('put_wall'))} / "
        f"{_available_number(levels.get('flip_low'))}–"
        f"{_available_number(levels.get('flip_high'))} / "
        f"{_available_number(levels.get('call_wall'))}"
    )


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


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


def _without_prefix(value: str, prefix: str) -> str:
    return value.removeprefix(prefix)


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
        LevelPhase.FAR: "NO SETUP",
        LevelPhase.APPROACHING: "APPROACHING",
        LevelPhase.TESTING: "TESTING",
        LevelPhase.BREAK_PENDING: "BREAK PENDING",
        LevelPhase.REJECT_PENDING: "REJECT PENDING",
        LevelPhase.ACCEPTED: "ACCEPTED",
        LevelPhase.REJECTED: "REJECTED",
        LevelPhase.RETEST: "RETEST",
        LevelPhase.CONFIRMED: "CONFIRMED",
        LevelPhase.INVALIDATED: "INVALIDATED",
        LevelPhase.EXPIRED: "EXPIRED",
    }[phase]
