"""strategy_decision-owned desk conclusion text for Desk Map."""

from __future__ import annotations

from typing import Any, Mapping

from spx_spark.analytics.options.pricing import finite_float
from spx_spark.application.order_map.path_distribution import path_distribution_desk_text
from spx_spark.application.order_map.state import current_session_is_gth
from spx_spark.application.order_map.strategy_regime import (
    pin_stable_center,
    pin_stable_next_step_text,
    pin_stable_watch_phase,
    pin_watch_center,
)


def strategy_candidate_is_watchable(
    payload: Mapping[str, Any],
    decision: Mapping[str, Any] | None = None,
) -> bool:
    """True when strategy_decision may be shown as a human RTH watch card.

    GTH 15-minute maps show the live iron-condor / width scan. Ranked winners
    still go through trade_ready, so this card never becomes 可看 in GTH.
    """

    decision = _mapping(decision or payload.get("strategy_decision"))
    candidate = _mapping(decision.get("candidate"))
    if (
        decision.get("action_authority") != "manual"
        or not candidate
        or str(decision.get("decision_type") or "NO_TRADE") == "NO_TRADE"
    ):
        return False
    if current_session_is_gth(payload, _mapping(payload.get("level_decision"))):
        return False
    return True


def strategy_decision_desk_view(payload: Mapping[str, Any]) -> str | None:
    """Render the human Base Case owned by strategy_decision.

    Keep machine reason codes out of the operator-facing first screen. Audit
    detail remains on strategy_decision.why_not for logs and funnel analysis.
    """

    decision = _mapping(payload.get("strategy_decision"))
    if not decision:
        return None
    why_not = _mapping(decision.get("why_not"))
    candidate = _mapping(decision.get("candidate"))
    nearest = candidate or _mapping(why_not.get("nearest_candidate"))
    reasons = [str(reason) for reason in why_not.get("reasons") or () if str(reason).strip()]
    failed_gates = _failed_gate_codes(nearest, reasons)
    watchable = strategy_candidate_is_watchable(payload, decision)
    gth_session = current_session_is_gth(
        payload, _mapping(payload.get("level_decision"))
    )
    if gth_session:
        return _gth_scan_desk_view(payload, decision, reasons)
    regime = _mapping(decision.get("regime"))
    pin_center = pin_watch_center(regime)
    trade_center = pin_stable_center(regime)
    if watchable:
        conclusion = f"可看 · {strategy_candidate_label(candidate)}"
    elif trade_center is not None:
        conclusion = f"观察 · 稳定钉住 {trade_center:g}"
    elif pin_center is not None:
        conclusion = f"观察 · 今日中轴 {pin_center:g}"
    else:
        conclusion = "不做"
    primary = humanize_strategy_reason(reasons[0]) if reasons else "暂无明确阻断原因"
    if pin_center is not None and not watchable:
        facts = _mapping(decision.get("market_facts"))
        if pin_stable_watch_phase(finite_float(facts.get("minutes_to_close"))) == "look":
            primary = (
                "钉住已稳，11–13 可看今日蝶"
                if trade_center is not None
                else "11–13 可看今日蝶（观察，未到交易钉）"
            )
    gth_no_trade = not watchable and gth_session
    nearest_line = (
        "无"
        if gth_no_trade
        else _nearest_candidate_line(nearest, failed_gates)
    )
    reauthorize = str(why_not.get("reauthorize_on") or "").strip()
    if not reauthorize or _looks_like_machine_token(reauthorize):
        reauthorize = _pin_stable_next_step(decision, pin_center) or (
            "等待价格触发、精确报价与赔率同时通过后再评估"
        )
    return "\n".join(
        (
            f"结论  {conclusion}",
            f"主因  {primary}",
            f"最近候选  {nearest_line}",
            f"下一步  {reauthorize}",
        )
    )


def _gth_scan_desk_view(
    payload: Mapping[str, Any],
    decision: Mapping[str, Any],
    reasons: list[str],
) -> str:
    del payload
    ic_line = iron_condor_desk_line(_mapping(decision.get("iron_condor_map")))
    funnel = _mapping(decision.get("rejection_funnel"))
    scanned = funnel.get("candidate_enumerated")
    scan_text = f"扫描 {int(scanned)} 组" if isinstance(scanned, int | float) else "扫描进行中"
    candidate = _mapping(decision.get("candidate"))
    decision_type = str(decision.get("decision_type") or "NO_TRADE")
    setup = str(candidate.get("setup_kind") or "")
    gth_scan_winner = decision_type != "NO_TRADE" and setup in {
        "GTH_WIDTH_SCAN",
        "GTH_DELTA_SCAN",
        "GTH_ATM_PIN",
        "IRON_CONDOR_DELTA",
    }
    if gth_scan_winner:
        conclusion = f"扫描赢家已推送 · {strategy_candidate_label(candidate)}"
        path_text = path_distribution_desk_text(
            _mapping(_mapping(candidate.get("edge")).get("path_distribution"))
        )
        if path_text:
            conclusion = f"{conclusion} · {path_text}"
    else:
        conclusion = f"无过门赢家 · {scan_text}"
    quality = _mapping(decision.get("data_quality"))
    quality_reasons = [
        str(reason)
        for reason in quality.get("reasons") or ()
        if str(reason).strip()
    ]
    if quality_reasons:
        primary = quality_reason_text(quality_reasons[0])
    elif reasons:
        primary = humanize_strategy_reason(reasons[0])
    else:
        primary = "1 分钟报价持续重算 5–50 点价差与 5–20Δ 10 点翼宽铁鹰"
    return "\n".join(
        (
            f"结论  {conclusion}",
            f"主因  {primary}",
            f"铁鹰  {ic_line}",
            "下一步  过门赢家单独推送交易卡；铁鹰随 delta 每周期重算，未过门价差不是可看",
        )
    )


def strategy_candidate_label(candidate: Mapping[str, Any]) -> str:
    if not candidate:
        return "无"
    strategy_type = humanize_strategy_type(str(candidate.get("strategy_type") or "candidate"))
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
    if current_session_is_gth(payload, _mapping(payload.get("level_decision"))):
        return "原因  夜盘 Desk Map 展示 5–20Δ 卖权 10 点翼宽铁鹰与宽度扫描；交易卡只推过门赢家"
    if strategy_candidate_is_watchable(payload, decision):
        return f"原因  已给出人工候选：{strategy_candidate_label(candidate)}"
    reasons = [str(reason) for reason in _mapping(decision.get("why_not")).get("reasons") or ()]
    primary = humanize_strategy_reason(reasons[0]) if reasons else "尚无支持交易的候选"
    return f"原因  {primary}"


def humanize_strategy_type(strategy_type: str) -> str:
    return {
        "NO_TRADE": "不做",
        "CALL_DEBIT_VERTICAL": "Call 价差",
        "PUT_DEBIT_VERTICAL": "Put 价差",
        "CALL_BUTTERFLY": "Call 蝶式",
        "PUT_BUTTERFLY": "Put 蝶式",
        "IRON_CONDOR": "铁鹰",
    }.get(str(strategy_type or "").upper(), str(strategy_type or "候选").replace("_", " "))


def humanize_strategy_reason(reason: str) -> str:
    token = str(reason or "").strip()
    exact = {
        "level_source_not_confirmed": "尚未出现确认的价格触发（墙位/翻区未接受或拒绝）",
        "level_source_formal_signal_absent": "旧 formal signal 未形成，不能当作入场依据",
        "trend_background_cannot_authorize_entry": "趋势背景不能单独授权入场，需价格触发确认",
        "confirmed_price_trigger_unavailable": "价格触发尚未确认，不能枚举方向价差",
        "quote_refresh_required": "精确双边报价需要刷新",
        "vertical_exact_two_leg_quote_unavailable": "两腿精确报价暂不可用",
        "vertical_exact_spread_unavailable": "两腿精确价差暂不可用",
        "max_debit_fraction_exceeded": "权利金相对翼宽偏贵",
        "direction_valid_but_entry_too_late": "方向成立但入场已偏晚",
        "entry_window_not_open": "结构已出现，但入场窗口尚未打开",
        "entry_too_late": "入场窗口已过，继续追价不合规",
        "rth_entry_window_not_open": "结构已出现，正在等待下一根 5 分钟确认",
        "rth_entry_window_too_late": "入场窗口已过，继续追价不合规",
        "session_episode_reclaim_progress_too_late": "失败突破已走完过半，不再追价",
        "rth_setup_invalidated": "结构已被下一根 5 分钟否定",
        "trend_pullback_path_not_confirmed": "回踩成立，但趋势背景尚未确认",
        "trend_pullback_path_unevaluable": "回踩成立，但趋势向量不完整，不能评估",
        "vertical_path_inputs_unavailable": "路径输入不足，不能评估价差",
        "vertical_short_beyond_target": "短腿超过目标位，翼宽与方向目标不匹配",
        "vertical_width_exceeds_remaining_move": "翼宽大于剩余期望波动，方向杠杆过大",
        "vertical_remaining_move_unavailable": "剩余期望波动不可用，不能给方向价差定宽",
        "event_settlement_exact_two_leg_quote_unavailable": "事件结算价差缺少两腿精确报价",
        "path_inputs_unavailable": "趋势路径输入缺失，不能判断 TREND/TRANSITION",
        "path_inputs_not_aligned": "路径输入齐全，但尚未形成趋势或平衡",
        "gth_dip_reclaim_evidence_unavailable": "夜盘回踩收复证据不足",
        "gth_confirmed_level_candidate_unavailable": "夜盘确认墙位候选暂不可用",
        "gth_width_scan_no_fresh_quote": "夜盘没有 1 分钟内的两腿新鲜报价",
        "gth_scan_geometry_or_payoff_unavailable": "夜盘扫描缺少目标、失效位或权利金",
        "debit_long_beyond_remaining_move": "长腿超出剩余期望波动，20 分钟/到期都很难碰到",
        "gth_delta_scan_long_above_cap": "夜盘 delta 扫描长腿超过 20Δ",
        "iron_condor_delta_quotes_unavailable": "5–20Δ 卖权铁鹰缺少带 delta 的新鲜报价",
        "iron_condor_four_leg_quote_unavailable": "铁鹰四腿报价不齐",
        "iron_condor_credit_unavailable": "铁鹰保守贷记尚未形成",
        "iron_condor_credit_fraction": "铁鹰贷记相对翼宽不在可接受区间",
        "iron_condor_spot_outside_shorts": "现价已离开铁鹰短腿区间",
        "iron_condor_short_delta_band": "铁鹰短腿不在 5–20Δ",
        "iron_condor_wing_too_wide": "铁鹰翼宽不是 10 点定义风险",
        "strategy_event_expired": "旧策略事件已过期",
        "gth_dip_reclaim_signal_expired": "夜盘回踩收复信号已过期",
        "source_entry_quality_blocked": "来源入场质量未过门",
        "source_entry_quality_has_block_reasons": "来源入场质量仍有阻断",
        "signal_session_mismatch": "信号时段与当前盘段不一致",
        "long_leg_quote_unavailable": "多头腿报价不可用",
        "short_leg_quote_unavailable": "空头腿报价不可用",
        "spread_net_nbbo_invalid": "组合净买卖价无效",
        "spread_entry_limit_invalid": "入场限价无效",
        "chain_implied_target_unavailable": "隐含目标位不可用",
        "gth_reclaim_too_old": "夜盘收复信号过旧",
        "spread_exit_at_elapsed": "预定退出时点已过",
        "spread_reward_risk_unavailable": "收益风险比不可计算",
        "surface_shape_d3_slope_up": "曲面近端偏多（仅研究，不单独开仓）",
        "surface_shape_d3_slope_down": "曲面近端偏空（仅研究，不单独开仓）",
        "surface_shape_d4_trough": "曲面呈槽形（仅研究）",
        "surface_shape_d4_peak": "曲面呈峰形（仅研究）",
        "surface_shape_low_snr": "曲面信号噪声比偏低（仅研究）",
        "pricing_not_authorized": "定价未授权，不能当作可执行候选",
        "spx_price_unavailable": "触发坐标不可用（RTH 需现金 SPX；GTH 需期权隐含或 ES 折算）",
        "macro_entry_not_authorized": "宏观事件窗口禁止新建议",
        "session_not_open_for_spxw_strategy": "当前不在可评估 SPXW 的时段",
        "no_supported_strategy_candidate": "没有通过门控的可交易候选",
        "gth_butterfly_rth_only": "夜盘不授权蝶式，只在 RTH 稳定钉住评估",
        "gth_vertical_requires_aligned_trend": "夜盘方向价差要求 TREND 且与路径同向",
        "butterfly_requires_pin_stable": "蝶式要求稳定钉住环境",
        "butterfly_shock_veto": "冲击状态未平复，禁止新开蝶式",
        "butterfly_body_far_from_value_center": "蝶式身体偏离价值中枢过远",
        "butterfly_body_far_from_q_mode": "蝶式身体偏离概率峰值过远",
        "butterfly_body_value_center_distance": "蝶式身体偏离价值中枢过远",
        "butterfly_body_q_mode_distance": "蝶式身体偏离概率峰值过远",
        "butterfly_spot_outside_wings": "现价已在蝶式帐篷外",
        "butterfly_entry_too_early": "距收盘过早，该翼宽蝶式尚未授权",
        "butterfly_five_wide_look_mass_not_concentrated": "午盘 5 点蝶要求质量堆在中轴 ±5 内",
        "butterfly_unresolved_nearby_wall": "附近墙位仍在剩余期望位移内，不是稳定钉住",
        "butterfly_three_leg_bbo_unavailable": "三腿双边报价不齐",
        "butterfly_structure_capability_unavailable": "期权结构帧未就绪，不能评估蝶式",
        "butterfly_value_center_or_density_unavailable": "价值中枢或密度不足，不能评估蝶式",
        "butterfly_expiry_unavailable": "蝶式缺少到期日",
        "shock_active": "盘中冲击进行中",
        "shock_post_shock_discovery": "冲击后中枢重建中",
    }
    if token in exact:
        return exact[token]
    if token.startswith("surface_shape_"):
        return "曲面形状仅供研究，不能单独构成入场"
    if "quote" in token or "pricing" in token or "nbbo" in token:
        return "精确报价尚未就绪"
    if "expired" in token or "stale" in token or "too_old" in token:
        return "相关信号或报价已经过期"
    if "trigger" in token or "level_source" in token or "signal" in token:
        return "价格触发尚未确认"
    if "shock" in token:
        return "冲击状态不允许该策略"
    if _looks_like_machine_token(token):
        return "执行条件尚未完整"
    return token


def quality_reason_text(reason: str) -> str:
    labels = {
        "spx_price_unavailable": "触发坐标不可用（GTH 以期权隐含/ES 为准，不要求现金 SPX）",
        "option_frame:unavailable": "期权结构帧不可用，Gamma 与墙位不可靠",
        "option_frame:degraded": "期权结构帧降级",
        "option_l1:unavailable": "SPXW 双边报价不可用",
        "option_l1:degraded": "SPXW 报价覆盖降级",
        "oi:missing": "OI 不可用，Gamma 代理失效",
        "oi:schwab_unverified": "Schwab OI 未验证，Gamma 代理仅供审计",
        "gex:no_open_interest_gex": "缺少 OI-GEX，不能解释 Gamma 机制",
        "ibkr_feed_unavailable": "IBKR 源不可用，已抑制陈旧 SPXW 报价",
        "schwab_oi_unverified": "Schwab OI 未验证，Gamma 代理仅供审计",
        "open interest wall scope:schwab_rth_lane": "RTH 墙位使用 Schwab OI（IBKR 热通道不可用）",
        "open interest wall scope:ibkr_hot_lane": "墙位范围仅含 IBKR 热通道 OI",
        "decision_snapshot_inconsistent": "旧事件与当前结构不一致",
        "unknown_level_phase": "状态机阶段非法",
        "market_frame:unavailable": "市场帧不可用，ES 流确认不能验证",
        "market_frame:degraded": "市场帧降级，ES 流确认需谨慎",
        "ready_opportunity_mismatch": "READY 不属于当前价格事件，旧机会与旧目标已禁用",
        "ready_required_frame_unavailable": "必需数据帧缺失，READY 已暂停",
        "ready_without_current_confirmed_path": "旧 READY 与当前价格路径不一致，已禁止执行",
    }
    token = str(reason or "")
    if token in labels:
        return labels[token]
    lowered = token.lower()
    if token.startswith("analytical_leg_rejected:analytical_only_non_executable"):
        return "结构腿仅分析用、不可当作执行报价"
    if "ibkr feed unavailable" in lowered or "stale spxw option quotes suppressed" in lowered:
        return "IBKR 源不可用，已抑制陈旧 SPXW 报价"
    if token.startswith("density_clipped:"):
        return f"概率密度裁剪偏高（{token.partition(':')[2]}）"
    if token.startswith("underlier_mismatch:"):
        return "标的坐标不匹配，墙位与 Gamma 告警已抑制"
    if "entitlement" in lowered or "greeks feed not live" in lowered:
        return "Greeks 实时权限不可用，分析腿暂不可用"
    if lowered.startswith("analytical leg rejected"):
        return "期权分析腿被拒，结构解释降级"
    return token.replace("_", " ")


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
        return f"EM ±{expected_move:.1f}pt"
    label, fraction = usage
    return f"EM ±{expected_move:.1f}pt · {label} 已用 {fraction:.0%}"


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


def _pin_stable_next_step(
    decision: Mapping[str, Any], pin_center: float | None
) -> str | None:
    if pin_center is None:
        return None
    facts = _mapping(decision.get("market_facts"))
    return pin_stable_next_step_text(finite_float(facts.get("minutes_to_close")))


def _failed_gate_codes(nearest: Mapping[str, Any], reasons: list[str]) -> list[str]:
    failed_gates: list[str] = []
    for gate in nearest.get("failed_gates") or ():
        mapped = _mapping(gate)
        failed_gates.append(str(mapped.get("gate") or gate))
    if not failed_gates:
        failed_gates = [str(reason) for reason in nearest.get("rejection_reasons") or ()]
    if not failed_gates:
        failed_gates = reasons[1:]
    return [code for code in failed_gates if str(code).strip()]


def _nearest_candidate_line(nearest: Mapping[str, Any], failed_gates: list[str]) -> str:
    if not nearest:
        return "无"
    label = strategy_candidate_label(nearest)
    actionable = [
        code
        for code in failed_gates
        if not str(code).startswith("surface_shape_")
    ]
    if not actionable:
        return label
    return f"{label}（卡在：{humanize_strategy_reason(actionable[0])}）"


def iron_condor_desk_line(structure: Mapping[str, Any]) -> str:
    if not structure:
        return "5–20Δ 10宽 尚未计算"
    strikes = [
        f"{float(strike):g}"
        for strike in structure.get("strikes") or ()
        if finite_float(strike) is not None
    ]
    strike_text = "/".join(strikes) if strikes else "—"
    if str(structure.get("status") or "") != "ready":
        reason = humanize_strategy_reason(str(structure.get("reason") or "iron_condor_credit_unavailable"))
        return f"卖{_short_delta_label(structure)} {_wing_width_label(structure)} {strike_text} · {reason}"
    economics = _mapping(structure.get("economics"))
    quote = _mapping(structure.get("quote"))
    credit = finite_float(quote.get("credit")) or finite_float(economics.get("max_gain_points"))
    credit_text = f"{credit:g}" if credit is not None else "—"
    loss = finite_float(economics.get("max_loss_points"))
    line = (
        f"卖{_short_delta_label(structure)} {_wing_width_label(structure)} "
        f"{strike_text} 贷记 {credit_text}"
    )
    if loss is not None:
        line += f" 最大亏损 {loss:g}"
    path_text = path_distribution_desk_text(_mapping(structure.get("path_distribution")))
    if path_text:
        return f"{line} · {path_text}"
    return line


def _short_delta_label(structure: Mapping[str, Any]) -> str:
    delta = finite_float(structure.get("short_abs_delta"))
    if delta is None:
        return "5–20Δ"
    return f"{int(round(delta * 100))}Δ"


def _wing_width_label(structure: Mapping[str, Any]) -> str:
    width = finite_float(structure.get("wing_width"))
    if width is None:
        economics = _mapping(structure.get("economics"))
        width = finite_float(economics.get("width_points"))
    if width is None:
        strikes = [
            finite_float(strike) for strike in structure.get("strikes") or ()
        ]
        if len(strikes) >= 2 and strikes[0] is not None and strikes[1] is not None:
            width = abs(strikes[1] - strikes[0])
    if width is None:
        return "10宽"
    return f"{width:g}宽"


def _looks_like_machine_token(value: str) -> bool:
    token = str(value or "").strip()
    if not token or " " in token or any("\u4e00" <= char <= "\u9fff" for char in token):
        return False
    return "_" in token or token.isupper()


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}
