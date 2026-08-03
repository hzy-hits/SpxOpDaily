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

    projection = build_desk_map_projection(payload)
    guidance = guidance_module.build_decision_guidance(payload)
    beijing = now_utc.astimezone(SHANGHAI_TZ)
    session = _session_phase_of(payload, now_utc)
    expiry = str(payload.get("expiry") or "-")
    expiry_text = f"{expiry[4:6]}-{expiry[6:8]}" if len(expiry) == 8 else expiry
    lines = [
        (
            f"【SPX Desk Map · {beijing.strftime('%H:%M')} · "
            f"0DTE {expiry_text} · {session.get('name_cn')}】"
        ),
        _desk_view_line(projection, guidance),
        _location_line(payload, projection),
        _structure_line(payload),
        f"Primary  {humanize_operator_trigger(guidance.trigger_text)}",
        f"Alternative  {guidance.invalidation_text}",
        _targets_line(payload),
        _execution_line(payload, projection, guidance),
        _data_quality_line(projection),
    ]
    if changes:
        lines.append(f"Change  {'；'.join(changes[:3])}")
    lines.append(operator_reason_line(payload))
    return "\n".join(lines)


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
    decision_direction = str(decision.get("direction") or "none")
    direction = (
        decision_direction
        if decision_direction in {"up", "down"}
        else str(guidance.bias_direction or "none")
    )
    thesis = str(decision.get("thesis") or "none")
    if ready and len(plans) == 1:
        if decision_direction not in {"up", "down"}:
            direction = {"C": "up", "P": "down"}.get(str(plans[0].get("right") or ""), direction)
        play = str(plans[0].get("play") or "")
        if thesis not in {"breakout", "fade"}:
            thesis = "breakout" if "breakout" in play else "fade" if "fade" in play else thesis
    return DeskMapProjection(
        stage=stage,
        phase=phase,
        direction=direction,
        thesis=thesis,
        level_kind=str(decision.get("level_kind") or ""),
        level=finite_float(decision.get("level")),
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
        level_text = (
            f" · {_level_kind_label(projection.level_kind)} {projection.level:g}"
            + (f" · 距离 {distance:.1f}pt" if distance is not None else "")
        )
    return f"Location  SPX {spx_text} · ES {_dash(payload.get('es_last'))}{level_text}"


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
            "Execution  READY · 独立 MANUAL READY 卡承载实时合约与报价 "
            f"· opportunity {short_key}"
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
        reasons.append(str(decision.get("quality_reason") or "market_data_degraded"))

    frame = _mapping(payload.get("option_structure_frame"))
    exposure = _mapping(frame.get("exposure"))
    structure = _mapping(frame.get("structure"))
    density = _mapping(frame.get("density"))
    oi_quality = str(exposure.get("oi_quality") or "")
    if oi_quality and oi_quality != "ibkr_ok":
        reasons.append(f"oi:{oi_quality}")
    gex_quality = str(structure.get("gex_quality") or "")
    if gex_quality and gex_quality != "open_interest_gex":
        reasons.append(f"gex:{gex_quality}")
    clipped = finite_float(density.get("clipped_mass_fraction"))
    if clipped is not None and clipped >= 0.10:
        reasons.append(f"density_clipped:{clipped:.0%}")
    warnings = payload.get("warnings")
    if isinstance(warnings, list):
        reasons.extend(str(item) for item in warnings[:3] if item)
    unique = tuple(dict.fromkeys(reason for reason in reasons if reason))
    return ("DEGRADED", unique) if unique else ("READY", ())


def _data_quality_line(projection: DeskMapProjection) -> str:
    if not projection.quality_reasons:
        return "Data Quality  READY · 决策坐标与结构快照可用"
    return f"Data Quality  {projection.data_quality} · {'; '.join(projection.quality_reasons[:4])}"


def _levels_text(levels: Mapping[str, Any]) -> str:
    return (
        f"{_dash(levels.get('put_wall'))} / "
        f"{_dash(levels.get('flip_low'))}–{_dash(levels.get('flip_high'))} / "
        f"{_dash(levels.get('call_wall'))}"
    )


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


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
