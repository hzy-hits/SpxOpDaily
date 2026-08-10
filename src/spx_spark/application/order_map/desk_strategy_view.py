"""strategy_decision-owned desk conclusion text for Desk Map."""

from __future__ import annotations

from typing import Any, Mapping

from spx_spark.analytics.options.pricing import finite_float


def strategy_decision_desk_view(payload: Mapping[str, Any]) -> str | None:
    decision = _mapping(payload.get("strategy_decision"))
    if not decision:
        return None
    why_not = _mapping(decision.get("why_not"))
    candidate = _mapping(decision.get("candidate"))
    nearest = candidate or _mapping(why_not.get("nearest_candidate"))
    reasons = [str(reason) for reason in why_not.get("reasons") or ()]
    failed_gates = []
    for gate in nearest.get("failed_gates") or ():
        mapped = _mapping(gate)
        failed_gates.append(str(mapped.get("gate") or gate))
    if not failed_gates:
        failed_gates = [str(reason) for reason in nearest.get("rejection_reasons") or ()]
    if not failed_gates:
        failed_gates = reasons[1:]
    decision_type = str(decision.get("decision_type") or "NO_TRADE")
    primary_blocker = reasons[0] if reasons else "none"
    reauthorize = str(why_not.get("reauthorize_on") or "active manual candidate")
    return (
        f"Decision: {decision_type} · Primary blocker: {primary_blocker} · "
        f"Nearest candidate: {strategy_candidate_label(nearest)} · "
        f"Failed gates: {', '.join(failed_gates) if failed_gates else 'none'} · "
        f"Reauthorize when: {reauthorize}"
    )


def strategy_candidate_label(candidate: Mapping[str, Any]) -> str:
    if not candidate:
        return "none"
    strategy_type = str(candidate.get("strategy_type") or "candidate")
    raw_legs = candidate.get("legs") or (candidate.get("long"), candidate.get("short"))
    strikes = [
        strike
        for leg in raw_legs
        if (strike := finite_float(_mapping(leg).get("strike"))) is not None
    ]
    strike_text = "/".join(f"{strike:g}" for strike in strikes)
    return f"{strategy_type} {strike_text}".strip()


def strategy_reason_line(payload: Mapping[str, Any]) -> str | None:
    decision = _mapping(payload.get("strategy_decision"))
    if not decision:
        return None
    candidate = _mapping(decision.get("candidate"))
    if decision.get("action_authority") == "manual" and candidate:
        return (
            "原因  strategy_decision 已授权人工候选："
            f"{candidate.get('setup_kind') or 'setup'}"
        )
    reasons = list(_mapping(decision.get("why_not")).get("reasons") or ())
    return (
        "原因  strategy_decision NO_TRADE："
        f"{str(reasons[0]) if reasons else 'no_supported_strategy_candidate'}"
    )


def quality_reason_text(reason: str) -> str:
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


def volume_alignment_text(value: object) -> str:
    return {
        "price_volume_aligned": "同向确认",
        "price_volume_divergent": "背离",
        "price_without_volume_confirmation": "价格缺少量能确认",
        "volume_without_price_progress": "放量但价格未推进",
        "flat": "平稳",
        "unavailable": "unavailable",
    }.get(str(value or ""), "unavailable")


def cross_asset_confirmation_text(value: object) -> str:
    return {
        "confirmed": "同向确认",
        "divergent": "背离",
        "unavailable": "unavailable",
    }.get(str(value or ""), "unavailable")


def thesis_label(value: str) -> str:
    return {"breakout": "BREAKOUT", "fade": "FADE"}.get(value, "SETUP")


def level_kind_label(value: str) -> str:
    return {
        "put_wall": "Put Wall",
        "flip_low": "Flip Low",
        "flip_high": "Flip High",
        "call_wall": "Call Wall",
    }.get(value, "Level")


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def opening_range_state_text(value: str) -> str:
    token = value.upper()
    return {
        "ABOVE_ORH_CONFIRMED": "上沿上方确认",
        "BREAKOUT_ABOVE_ORH": "突破上沿待确认",
        "INSIDE": "区间内",
        "BREAKDOWN_BELOW_ORL": "跌破下沿待确认",
        "BELOW_ORL_CONFIRMED": "下沿下方确认",
    }.get(token, token)


def phase_label(phase: object) -> str:
    key = getattr(phase, "value", phase)
    return {
        "far": "尚未触发",
        "approaching": "接近关键位",
        "testing": "测试关键位",
        "break_pending": "突破待确认",
        "reject_pending": "拒绝待确认",
        "accepted": "已接受，等待回踩",
        "rejected": "已拒绝，等待回踩",
        "retest": "回踩确认中",
        "confirmed": "已确认",
        "invalidated": "已失效",
        "expired": "已过期",
    }.get(str(key), str(key))


def stage_label(stage: object) -> str:
    key = getattr(stage, "value", stage)
    return {
        "OBSERVING": "观察中",
        "WATCHING": "接近结构",
        "ARMED": "条件形成中",
        "CONFIRMED": "方向已确认",
        "READY": "执行候选已就绪",
        "INVALIDATED": "已失效",
        "EXPIRED": "已过期",
        "PAUSED": "已暂停",
    }.get(str(key), str(key))

def expected_move_text(payload: Mapping[str, Any]) -> str:
    frame = _mapping(payload.get("option_structure_frame"))
    volatility = _mapping(frame.get("volatility"))
    expected_move = finite_float(payload.get("expected_move_points"))
    if expected_move is None:
        expected_move = finite_float(volatility.get("expected_move_points_0dte"))
    if expected_move is None or expected_move <= 0:
        return "EM unavailable"

    # A live 0DTE EM shrinks to expiry; publish usage only for an explicitly
    # matching numerator/denominator horizon, never an earlier session range.
    usage = aligned_expected_move_usage(payload)
    if usage is None:
        return f"EM ±{expected_move:g}pt"
    label, fraction = usage
    return f"EM ±{expected_move:g}pt · {label} 已用 {fraction:.0%}"


def aligned_expected_move_usage(payload: Mapping[str, Any]) -> tuple[str, float] | None:
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


