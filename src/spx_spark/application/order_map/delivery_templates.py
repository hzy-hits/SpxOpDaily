"""Order-map message rendering and template adapters."""

from __future__ import annotations

from collections.abc import Mapping
from math import isfinite
from typing import Any
from zoneinfo import ZoneInfo

from spx_spark.application.order_map.delivery import (
    _beijing_clock,
    _butterfly_leg_text,
    _direction_cn,
    _finite_number,
    _fmt_premium,
    _fmt_strike,
    _gamma_proxy_line,
    _ict_liquidity_line,
    _iron_condor_strike_text,
    _percent,
    _pin_watch_title,
    _range_text,
    _render_pin_stable_watch,
    _setup_cn,
    _strategy_card_title,
    _strike_pair,
    _timestamp,
    _usd_loss,
    _vertical_leg_text,
)
from spx_spark.application.order_map.guidance import price_action_playbook_text
from spx_spark.application.order_map.models import SHANGHAI_TZ
from spx_spark.notifier.templates import RenderedMessage, Template, register


def _render_strategy_candidate(
    decision: dict[str, Any], candidate: dict[str, Any], *, quote_status: str | None = None
) -> str:
    quote, economics = candidate.get("quote") or {}, candidate.get("economics") or {}
    setup = str(candidate.get("setup_kind") or "")
    title = _strategy_card_title(candidate)
    until = _beijing_clock(candidate.get("opportunity_valid_until"))
    loss = _usd_loss(economics.get("max_loss_points"))
    if setup == "IRON_CONDOR_DELTA":
        credit = quote.get("credit")
        strikes = _iron_condor_strike_text(candidate)
        invalidation = _strike_pair(candidate.get("invalidation_spx")) or "-"
        take_profit = float(credit) * 0.50 if isinstance(credit, int | float) else None
        stop_buyback = float(credit) * 3.00 if isinstance(credit, int | float) else None
        put_short = candidate.get("put_short") or {}
        call_short = candidate.get("call_short") or {}
        put_delta = _finite_number(candidate.get("put_short_abs_delta"))
        call_delta = _finite_number(candidate.get("call_short_abs_delta"))
        if put_delta is None:
            raw = _finite_number(put_short.get("delta"))
            put_delta = abs(raw) if raw is not None else None
        if call_delta is None:
            raw = _finite_number(call_short.get("delta"))
            call_delta = abs(raw) if raw is not None else None
        put_distance = _finite_number(candidate.get("put_short_distance_points"))
        call_distance = _finite_number(candidate.get("call_short_distance_points"))
        spot = _finite_number(candidate.get("spot"))
        if spot is None:
            spot = _finite_number(
                ((decision.get("market_facts") or {}).get("spot") or {}).get("spx")
            )
        put_strike = _finite_number(put_short.get("strike"))
        call_strike = _finite_number(call_short.get("strike"))
        if put_distance is None and spot is not None and put_strike is not None:
            put_distance = spot - put_strike
        if call_distance is None and spot is not None and call_strike is not None:
            call_distance = call_strike - spot
        put_delta_text = f"{put_delta * 100:.1f}Δ" if put_delta is not None else "-"
        call_delta_text = f"{call_delta * 100:.1f}Δ" if call_delta is not None else "-"
        put_distance_text = f"{put_distance:.1f}点" if put_distance is not None else "-"
        call_distance_text = f"{call_distance:.1f}点" if call_distance is not None else "-"
        gamma_risk = candidate.get("gamma_risk") or {}
        management_plan = candidate.get("management_plan") or {}
        hard_exit_et = str(management_plan.get("hard_exit_et") or "15:45")
        transition = candidate.get("gth_transition") or {}
        transition_line = ""
        if transition.get("status") == "qualified":
            expansion = _finite_number(transition.get("straddle_expansion_fraction"))
            contraction = _finite_number(
                transition.get("straddle_contraction_from_high_fraction")
            )
            if transition.get("entry_kind") == "smooth_convergence":
                decay = _finite_number(transition.get("straddle_decay_15m"))
                move = _finite_number(transition.get("move_15m_atr"))
                if decay is not None and move is not None:
                    transition_line = f"GTH 平缓收敛：15m跨式衰减 {decay:.1%}，5/15m IV不升，位移 {move:.2f} ATR（人工试验，前向未验证）"
            elif expansion is not None and contraction is not None:
                transition_line = (
                    f"GTH 波动：跨式先扩张 {expansion:.1%}、后从峰值收缩 "
                    f"{contraction:.1%}（不是 dealer 持仓推断）"
                )
        if gamma_risk.get("status") == "ready":
            net_gamma = _finite_number(gamma_risk.get("net_gamma_per_spx_point"))
            delta_shock = _finite_number(gamma_risk.get("delta_shock_10_trader_delta"))
            gcr10 = _finite_number(gamma_risk.get("gcr10"))
            nearest_delta = _finite_number(gamma_risk.get("nearest_short_abs_delta"))
            gamma_state = str(gamma_risk.get("state") or "-")
            gamma_line = (
                f"Gamma风控（解释）：净Γ {net_gamma:.4f} · 10点Delta冲击 {delta_shock:.1f}Δ · "
                f"GCR10 {gcr10 * 100:.1f}% · 最近短腿 {nearest_delta * 100:.1f}Δ · {gamma_state}"
                if None not in {net_gamma, delta_shock, gcr10, nearest_delta}
                else "Gamma风控（解释）：数据不完整，不改变现有授权"
            )
        else:
            gamma_line = "Gamma风控（解释）：数据不可用，不改变现有授权"
        lines = [
            f"【{title}】",
            "",
            "## 结论",
            f"铁鹰 逐边卖≤20Δ 10点翼宽 {strikes} · 只许限价",
            "",
            "## 执行",
            f"四腿 {strikes}",
            (
                f"实际选腿 Put {put_delta_text}（距SPX {put_distance_text}） · "
                f"Call {call_delta_text}（距SPX {call_distance_text}）"
            ),
            f"净贷记 ≥ {_fmt_premium(credit)} · 提交前刷新报价 · 禁止市价",
            f"观点有效至 {until}（北京）",
            "",
            "## 风险",
            f"最大亏损 {loss} · 短腿 {invalidation} 被触及时刷新四腿回购价",
            gamma_line,
            *([transition_line] if transition_line else []),
            f"止损：回购价 ≥ {_fmt_premium(stop_buyback)}（入场贷记 3倍，净亏200%）",
            *(["该止损阈值超过翼宽；正常到期价值范围内不可达，应按展示的最大亏损评估风险。"]
              if stop_buyback is not None and isinstance(economics.get("width_points"), int | float)
              and stop_buyback > float(economics["width_points"]) else []),
            "",
            "## 目标",
            f"止盈：回购价 ≤ {_fmt_premium(take_profit)}（入场贷记 50%）",
            f"{hard_exit_et} ET 前未触发则用新鲜四腿报价平仓",
        ]
    elif str(candidate.get("strategy_type") or "").endswith("_BUTTERFLY"):
        ask = quote.get("ask")
        invalidation = _range_text(candidate.get("invalidation_spx"))
        target = _fmt_strike(candidate.get("target_spx") or candidate.get("center"))
        width = economics.get("width_points") or candidate.get("width")
        debit_frac = economics.get("debit_fraction_of_width")
        payoff = ""
        if isinstance(width, int | float) and isinstance(debit_frac, int | float):
            debit_warning = "（偏贵）" if float(debit_frac) >= 0.40 else ""
            payoff = (
                f" · 翼宽 {_fmt_strike(width)} 点 · 借记占 {_percent(debit_frac)}{debit_warning}"
            )
        thesis = [f"目标位置 SPX {target}；进入目标区间不等于组合盈利。"]
        convergence = candidate.get("close_convergence")
        convergence = convergence if isinstance(convergence, dict) else {}
        if setup == "CLOSE_CONVERGENCE_60M":
            target_at = _timestamp(convergence.get("target_at"))
            target_clock = (
                f"{target_at.astimezone(ZoneInfo('America/New_York')):%Y-%m-%d %H:%M %Z} / "
                f"{target_at.astimezone(SHANGHAI_TZ):%Y-%m-%d %H:%M} 北京"
                if target_at is not None else "暂缺"
            )
            thesis.extend((
                f"60分钟收敛 P10/P50/P90 "
                f"{_fmt_strike(convergence.get('q10'))}/"
                f"{_fmt_strike(convergence.get('q50'))}/"
                f"{_fmt_strike(convergence.get('q90'))}",
                f"持有至预测目标 {target_clock}，以新鲜组合报价退出",
            ))
        lines = [
            f"【{title}】",
            "",
            "## 方向判断",
            f"{_setup_cn(setup)} · {_direction_cn(candidate.get('direction'))} · 只许限价",
            *thesis,
            "",
            "## 执行",
            f"{_butterfly_leg_text(candidate)}",
            f"净借记 ≤ {_fmt_premium(ask)} · 提交前刷新报价 · 禁止市价",
            f"观点有效至 {until}（北京）",
            "",
            "## 风险",
            f"最大亏损 {loss} · SPX 离开 {invalidation} 时重新评估",
            "",
            "## 结构经济性",
            f"净借记 {_fmt_premium(ask)}{payoff}",
            f"到期盈亏平衡（不含费用）{_fmt_premium(economics.get('breakeven_low'))}"
            f"—{_fmt_premium(economics.get('breakeven_high'))}；不代表提前退出可卖价。",
        ]
        if setup == "CLOSE_CONVERGENCE_60M":
            risk = candidate.get("convergence_risk")
            risk = risk if isinstance(risk, dict) else {}
            expected = _finite_number(risk.get("expected_pnl_points"))
            tail_loss = _finite_number(risk.get("cvar10_loss_points"))
            objective = _finite_number(risk.get("objective_points"))
            expected_text = (
                f"${expected * 100:+.2f}"
                if expected is not None and isfinite(expected) else "暂缺"
            )
            tail_text = (
                f"${tail_loss * 100:.2f}"
                if tail_loss is not None and isfinite(tail_loss) and tail_loss >= 0 else "暂缺"
            )
            lines.extend(
                (
                    f"模型每套预计净收益 {expected_text} · 最差10%情景平均亏损 {tail_text}",
                    "以上已计跨价及双边费用，为保持入场IV不变的模型估计，前向未验证；到期收益图不代表目标时刻可卖收益。",
                )
            )
            if objective is None or not isfinite(objective):
                lines.append("风险调整收益暂缺；排名不代表已确认经济优势。")
            elif objective <= 0:
                lines.append("风险调整收益不为正；候选排名不代表已确认经济优势。")
    elif setup == "EVENT_SETTLEMENT_THRESHOLD":
        view = candidate.get("view") if isinstance(candidate.get("view"), dict) else {}
        direction = str(candidate.get("direction") or "")
        relation = "高于" if direction == "UP" else "低于"
        legs = _vertical_leg_text(candidate)
        ask = quote.get("ask")
        gap = view.get("breakeven_gap_points")
        gap_text = (
            f"要比观点阈值再走 {float(gap):+.1f} 点才到盈亏平衡"
            if isinstance(gap, int | float)
            else "观点与结构阈值差暂缺"
        )
        lines = [
            f"【{title}】",
            "",
            "## 方向判断",
            f"事件结算观点 · SPX 到期结算{relation}前收 {_fmt_strike(view.get('threshold_level'))}",
            str(view.get("macro_event_name") or "宏观事件"),
            "",
            "## 执行",
            f"{legs} · 净借记 ≤ {_fmt_premium(ask)} · 禁止市价",
            f"有效至 {until}（北京）· 事件发布后原观点过期",
            "",
            "## 风险",
            f"最大亏损 {loss} · 跳空时普通止损不保证成交",
            "",
            "## 结构经济性",
            f"盈亏平衡 {_fmt_strike(economics.get('breakeven_spx'))} · {gap_text}",
            "到期盈亏平衡不含费用；结算观点不等于已验证的净收益优势。",
        ]
    else:
        legs = _vertical_leg_text(candidate)
        ask = quote.get("ask")
        invalidation = _fmt_strike(candidate.get("invalidation_spx"))
        target = _fmt_strike(candidate.get("target_spx"))
        width = economics.get("width_points")
        debit_frac = economics.get("debit_fraction_of_width")
        payoff = ""
        if isinstance(width, int | float) and isinstance(debit_frac, int | float):
            debit_warning = "（偏贵）" if float(debit_frac) >= 0.40 else ""
            payoff = (
                f" · 翼宽 {_fmt_strike(width)} 点 · 借记占 {_percent(debit_frac)}{debit_warning}"
            )
        lines = [
            f"【{title}】",
            "",
            "## 方向判断",
            f"{_setup_cn(setup)} · {_direction_cn(candidate.get('direction'))} · 只许限价",
            f"目标 SPX {target}；方向成立不等于组合盈利。",
            "",
            "## 执行",
            f"{legs}",
            f"净借记 ≤ {_fmt_premium(ask)} · 提交前刷新报价 · 禁止市价",
            f"观点有效至 {until}（北京）",
            "",
            "## 风险",
            f"最大亏损 {loss} · SPX 跌破 {invalidation} 时重新评估"
            if str(candidate.get("direction") or "") == "UP"
            else f"最大亏损 {loss} · SPX 升破 {invalidation} 时重新评估",
            "",
            "## 结构经济性",
            f"净借记 {_fmt_premium(ask)}{payoff}",
            f"到期盈亏平衡（不含费用）{_fmt_premium(economics.get('breakeven_spx'))}；不代表提前退出可卖价。",
        ]
    if setup != "IRON_CONDOR_DELTA":
        advisories = (candidate.get("edge") or {}).get("advisories") or ()
        if setup != "CLOSE_CONVERGENCE_60M":
            if "candidate_utility_not_positive" in advisories:
                lines.append("研究风险调整评分不为正；方向信号不构成经济优势证明。")
            if "candidate_lower_bound_not_positive" in advisories:
                lines.append("研究估计收益下界不为正；不能据此确认经济优势。")
        lines.extend(("", "## 盘口风险"))
        bid = _finite_number(quote.get("bid"))
        ask = _finite_number(quote.get("ask"))
        if bid is not None and ask is not None and isfinite(bid) and isfinite(ask) and ask >= bid:
            lines.append(
                f"报价快照：组合卖出参考 {bid:.2f} / 买入参考 {ask:.2f} · "
                f"买卖价差 {ask - bid:.2f} 点（每套 ${(ask - bid) * 100:.2f}，不含费用）；非成交保证。"
            )
        else:
            lines.append("组合双边报价不完整或异常；买卖价差暂缺。")
        if quote_status == "ready":
            lines.append("入队时仍在报价有效期内；提交前必须刷新。")
        elif quote_status == "refresh_required":
            lines.append("报价已过期或时效未确认；刷新组合报价并重新确认后才可提交限价单。")
        else:
            lines.append("报价时效未确认；提交前必须刷新。")
        lines.append(
            f"报价有效截至 {_beijing_clock(candidate.get('quote_valid_until'))}（北京）；"
            "观点有效期不延长报价有效期。"
        )
    environment = (
        (decision.get("regime") or {}).get("rth_environment") or {}
        if isinstance(decision.get("regime"), dict)
        else {}
    )
    breadth_advisory = (
        isinstance(environment, dict)
        and environment.get("status") == "degraded"
        and set(environment.get("missing") or ()) == {"breadth_above_vwap"}
    )
    data_line = "不下自动单 · 人工限价"
    if breadth_advisory:
        data_line += " · 市场广度缺失，仅作提醒，不阻断本方向候选"
    lines.extend(
        (
            "",
            "## 数据",
            data_line,
        )
    )
    del decision
    lines.extend((
        "",
        f"本次报价有效截至 {_beijing_clock(candidate.get('quote_valid_until'))}（北京）；提交前必须刷新",
        "SPX 结构失效线用于重新评估；回放退出按止盈止损与时间合同执行。",
    ))
    return "\n".join(lines)



def _markdown_sections(text: str) -> tuple[Mapping[str, str], ...]:
    sections: list[Mapping[str, str]] = []
    title = ""
    lines: list[str] = []
    for line in text.splitlines():
        if line.startswith("## "):
            if title or any(item.strip() for item in lines):
                sections.append({"title": title, "content": "\n".join(lines).strip()})
            title = line[3:].strip()
            lines = []
        else:
            lines.append(line)
    if title or any(item.strip() for item in lines):
        sections.append({"title": title, "content": "\n".join(lines).strip()})
    return tuple(section for section in sections if section["content"])


def _render_trade_ready_template(context: Mapping[str, Any]) -> RenderedMessage:
    decision = context["decision"]
    candidate = context["candidate"]
    text = _render_strategy_candidate(
        decision, candidate, quote_status=context.get("quote_status")
    ) + str(context.get("extra") or "")
    text = (
        f"{text}\n\n## 决策参考\n"
        f"盘型：{price_action_playbook_text(decision)}\n"
        f"{_gamma_proxy_line(decision)}\n"
        f"{_ict_liquidity_line(decision, candidate)}"
    )
    if context.get("quote_status") == "refresh_required":
        text = "本次报价已过期或时效未确认；刷新组合报价并重新确认后才可提交限价单。\n\n" + text
    return RenderedMessage(
        title=_strategy_card_title(candidate),
        text=text,
        feishu_text=text,
        sections=_markdown_sections(text),
    )


def _render_pin_watch_template(context: Mapping[str, Any]) -> RenderedMessage:
    decision = context["decision"]
    text = _render_pin_stable_watch(
        decision,
        center=context["center"],
        minutes=context["minutes"],
        phase=context["phase"],
    )
    return RenderedMessage(
        title=_pin_watch_title(context["regime"], context["center"]),
        text=text,
        feishu_text=text,
        sections=_markdown_sections(text),
    )


register(Template("trade_ready", 1, "ready", ("bark", "feishu"), _render_trade_ready_template))
register(Template("pin_stable_watch", 1, "watch", ("bark", "feishu"), _render_pin_watch_template))
