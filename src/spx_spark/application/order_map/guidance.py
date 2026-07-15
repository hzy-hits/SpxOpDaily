"""Deterministic operator guidance derived from the decision projections."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any, Mapping


class GuidanceAction(StrEnum):
    TRADE_READY = "trade_ready"
    WAIT_FOR_TRIGGER = "wait_for_trigger"
    PAUSED = "paused"


@dataclass(frozen=True, slots=True)
class DecisionGuidance:
    bias: str
    bias_direction: str
    trend_score: float | None
    mean_reversion_score: float | None
    action: GuidanceAction
    action_text: str
    trigger_text: str
    invalidation_text: str
    gate_reason: str | None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def build_decision_guidance(payload: Mapping[str, Any]) -> DecisionGuidance:
    """Compress market bias and execution gates into one operator-facing brief."""

    regime = _mapping(payload.get("regime_decision"))
    decision = _mapping(payload.get("level_decision"))
    intent = _mapping(payload.get("trade_intent"))
    plans = [row for row in payload.get("plan_candidates") or () if isinstance(row, Mapping)]
    direction = str(regime.get("direction") or "none")
    mode = str(regime.get("mode") or "unavailable")
    trend_score = _number(regime.get("trend_score"))
    reversion_score = _number(regime.get("mean_reversion_score"))
    levels = _levels(payload, decision)
    bias = _bias_label(mode, direction)

    if len(plans) == 1 and intent.get("status") == "trade_ready":
        plan = plans[0]
        contract = _contract(plan)
        return DecisionGuidance(
            bias=bias,
            bias_direction=direction,
            trend_score=trend_score,
            mean_reversion_score=reversion_score,
            action=GuidanceAction.TRADE_READY,
            action_text=f"唯一 TradeReady：{contract}，按实时 NBBO 入场上限执行",
            trigger_text=_trade_ready_trigger(plan),
            invalidation_text=_trade_ready_invalidation(plan),
            gate_reason=None,
        )

    gate_reason = _gate_reason(decision, intent, payload)
    if gate_reason is not None:
        return DecisionGuidance(
            bias=bias,
            bias_direction=direction,
            trend_score=trend_score,
            mean_reversion_score=reversion_score,
            action=GuidanceAction.PAUSED,
            action_text=f"暂停新开仓：{_reason_label(gate_reason)}",
            trigger_text=_trigger_text(mode, direction, levels),
            invalidation_text=_invalidation_text(mode, direction, levels),
            gate_reason=gate_reason,
        )

    return DecisionGuidance(
        bias=bias,
        bias_direction=direction,
        trend_score=trend_score,
        mean_reversion_score=reversion_score,
        action=GuidanceAction.WAIT_FOR_TRIGGER,
        action_text=_waiting_action(decision, intent),
        trigger_text=_trigger_text(mode, direction, levels),
        invalidation_text=_invalidation_text(mode, direction, levels),
        gate_reason=None,
    )


def _bias_label(mode: str, direction: str) -> str:
    if mode == "trending":
        return {"up": "趋势偏多", "down": "趋势偏空"}.get(direction, "趋势方向不明")
    if mode == "mean_reverting":
        return "均值回归"
    if mode == "transition":
        return {"up": "过渡偏多", "down": "过渡偏空"}.get(direction, "方向过渡")
    return "证据不足"


def _gate_reason(
    decision: Mapping[str, Any],
    intent: Mapping[str, Any],
    payload: Mapping[str, Any],
) -> str | None:
    if decision.get("snapshot_consistent") is False:
        return str(decision.get("quality_reason") or "decision_snapshot_inconsistent")
    if decision and decision.get("quality_ok") is False:
        return str(decision.get("quality_reason") or "level_observation_quality_failed")
    invalidations = _mapping(payload.get("decision_context")).get("invalidations")
    if isinstance(invalidations, list) and "decision_projection_mismatch" in invalidations:
        return "decision_projection_mismatch"
    if intent.get("status") == "blocked":
        reasons = intent.get("block_reasons")
        if isinstance(reasons, list) and reasons:
            return str(reasons[0])
        return "trade_intent_blocked"
    return None


def _waiting_action(decision: Mapping[str, Any], intent: Mapping[str, Any]) -> str:
    phase = str(decision.get("phase") or "far").upper()
    if phase == "FAR":
        return "当前不进场；等待价格进入关键位测试"
    if phase in {"APPROACHING", "TESTING"}:
        return "只观察，不预判突破或反转"
    if phase in {"BREAK_PENDING", "REJECT_PENDING", "ACCEPTED", "REJECTED", "RETEST"}:
        return "等待状态机 CONFIRMED 和成交量跟随"
    if phase == "CONFIRMED" and intent.get("status") != "trade_ready":
        return "方向已确认，但执行门控未完成"
    return "当前不进场；等待新事件"


def _trigger_text(mode: str, direction: str, levels: Mapping[str, float]) -> str:
    put_wall = levels.get("put_wall")
    flip_low = levels.get("flip_low")
    flip_high = levels.get("flip_high")
    call_wall = levels.get("call_wall")
    if mode == "trending" and direction == "down":
        level = flip_low if flip_low is not None else put_wall
        return _level_condition(level, "下方保持", "且状态机 CONFIRMED 后才评估 Put")
    if mode == "trending" and direction == "up":
        level = flip_high if flip_high is not None else call_wall
        return _level_condition(level, "上方保持", "且状态机 CONFIRMED 后才评估 Call")
    if mode == "mean_reverting":
        if put_wall is not None and call_wall is not None:
            return (
                f"只做墙位拒绝：Put {put_wall:g} 反弹或 Call {call_wall:g} 回落，"
                "均需 REJECTED→CONFIRMED"
            )
        return "只在墙位拒绝路径 REJECTED→CONFIRMED 后评估"
    if flip_low is not None and flip_high is not None:
        return f"等待 SPX 离开 Flip {flip_low:g}–{flip_high:g} 并完成 CONFIRMED"
    return "等待关键位状态机生成方向确认"


def _invalidation_text(mode: str, direction: str, levels: Mapping[str, float]) -> str:
    flip_low = levels.get("flip_low")
    flip_high = levels.get("flip_high")
    if mode == "trending" and direction == "down" and flip_high is not None:
        return f"SPX 收回 {flip_high:g} 且 ES 量价不再同向时，偏空判断取消"
    if mode == "trending" and direction == "up" and flip_low is not None:
        return f"SPX 跌回 {flip_low:g} 且 ES 量价不再同向时，偏多判断取消"
    return "状态机路径失效或跨资产转为背离时取消当前判断"


def _trade_ready_trigger(plan: Mapping[str, Any]) -> str:
    trigger = _number(plan.get("level"))
    return f"SPX {trigger:g} 已确认触发" if trigger is not None else "标的触发已确认"


def _trade_ready_invalidation(plan: Mapping[str, Any]) -> str:
    invalidation = _number(plan.get("invalidation_spx"))
    target = _number(plan.get("target_spx"))
    if invalidation is not None and target is not None:
        return f"SPX {invalidation:g} 失效；目标 {target:g}"
    if invalidation is not None:
        return f"SPX {invalidation:g} 失效"
    return "按 TradeReady 风险字段执行"


def _level_condition(level: float | None, relation: str, suffix: str) -> str:
    return f"SPX {level:g} {relation}{suffix}" if level is not None else suffix.lstrip("且")


def _levels(payload: Mapping[str, Any], decision: Mapping[str, Any]) -> dict[str, float]:
    decision_levels = _mapping(decision.get("levels"))
    result = {
        key: value
        for key in ("put_wall", "flip_low", "flip_high", "call_wall")
        if (value := _number(decision_levels.get(key))) is not None
    }
    flip = payload.get("flip_zone")
    if isinstance(flip, list | tuple) and len(flip) >= 2:
        parsed = sorted(value for item in flip[:2] if (value := _number(item)) is not None)
        if len(parsed) == 2:
            result["flip_low"], result["flip_high"] = parsed
    for candidate in payload.get("candidates") or ():
        if not isinstance(candidate, Mapping):
            continue
        level = _number(candidate.get("level"))
        if level is None:
            continue
        play = str(candidate.get("play") or "")
        if play == "put_wall_bounce_call":
            result.setdefault("put_wall", level)
        elif play == "call_wall_fade_put":
            result.setdefault("call_wall", level)
    return result


def _reason_label(reason: str) -> str:
    labels = {
        "decision_snapshot_level_drift": "墙位已移动，旧状态事件与当前结构不一致",
        "decision_snapshot_expiry_mismatch": "状态事件到期日与当前期权链不一致",
        "decision_snapshot_structure_unavailable": "当前结构暂不可用",
        "decision_snapshot_inconsistent": "决策快照内部不一致",
        "decision_projection_mismatch": "市场帧、期权帧和决策帧不同步",
        "es_not_live": "ES 行情不满足实时门槛",
        "spx_price_unavailable": "SPX 触发坐标不可用",
        "key_levels_unavailable": "Put Wall、Flip 或 Call Wall 不完整",
        "level_observation_quality_failed": "关键位观察质量未通过",
        "follow_through_hold_pending": "确认后的持续时间不足",
        "follow_through_distance_pending": "确认后的价格跟随不足",
        "regime_direction_conflict": "趋势方向与关键位路径冲突",
        "trade_intent_blocked": "执行门控未通过",
    }
    parts = [part.strip() for part in reason.split(";") if part.strip()]
    if len(parts) > 1:
        return "；".join(labels.get(part, part.replace("_", " ")) for part in parts)
    return labels.get(reason, reason.replace("_", " "))


def _contract(plan: Mapping[str, Any]) -> str:
    strike = _number(plan.get("strike"))
    right = str(plan.get("right") or "")
    return f"SPXW {strike:g}{right}" if strike is not None and right else "SPXW 合约"


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) else None
