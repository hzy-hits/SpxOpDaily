from __future__ import annotations

import json


def format_alert_message(payload: dict[str, object], alerts: list[dict[str, object]]) -> str:
    window = payload.get("window")
    window_name = "unknown"
    priority = "unknown"
    if isinstance(window, dict):
        window_name = str(window.get("name") or "unknown")
        priority = str(window.get("priority") or "unknown")

    lines = [
        "SPX/SPXW alert",
        f"window: {window_name} priority={priority}",
        f"as_of: {payload.get('as_of')}",
        f"alerts: {len(alerts)}",
    ]
    for alert in alerts[:6]:
        severity = alert.get("severity")
        title = alert.get("title")
        detail = alert.get("detail")
        lines.append(f"- [{severity}] {title}")
        if detail:
            lines.append(f"  {detail}")
    if len(alerts) > 6:
        lines.append(f"... {len(alerts) - 6} more alerts suppressed in push body")
    return "\n".join(lines)


def build_agent_prompt(payload: dict[str, object], alerts: list[dict[str, object]]) -> str:
    compact_payload = {
        "as_of": payload.get("as_of"),
        "window": compact_window(payload.get("window")),
        "alerts": alerts[:12],
        "human_focus_context": payload.get("human_focus_context"),
    }
    return "\n".join(
        (
            "你是 SPX Spark 的盘中告警分析 agent，读者只交易 SPX/SPXW 0DTE/1DTE 期权(买 call/put 或垂直价差)，"
            "盘前挂了限价单，可能正在盯盘也可能在睡觉。这条推送要回答他的一个问题：市场发生了什么，我挂的单/持仓要不要动。",
            "只根据下面的 JSON 做判断；不要给自动下单指令，不要假设缺失数据。",
            "人类只交易 SPX/SPXW；结论必须落在 SPX/SPXW/ES、期权墙、gamma、IV surface 上，"
            "VIX/VIX1D/VVIX/SKEW 作 vol regime 上下文，SPY/QQQ 可少量引用作确认；不要提加密或预测市场数据源。",
            "如果 options_map 警告含 underlier_mismatch 或 gamma_state 以 unknown 开头，只说明数据降级，不下 wall/gamma 结论。",
            "剧本必须双向对称：跌破关键位讲防守，但价格收复关键位并站稳时必须明确说反弹剧本激活，不允许只重复防守结论。",
            "输出中文，最多 12 行，结论先行：",
            "第 1 行=一句话说清发生了什么以及这对挂单/持仓意味着什么(例如『价格逼近 7500 put wall，反弹买 call 的挂单可能马上成交』)。",
            "然后 2-3 行证据：引用触发告警的关键数字 + gamma 地形(flip_zone、zero gamma、墙位触及/收破概率)。",
            "然后 1-2 行 if/then：接下来价格若到哪个具体位置，剧本如何分岔，该盯什么。",
            "最后 1 行 vol regime 与数据质量(VIX/VIX1D、dip_context、有无 degraded)。",
            "引用数字要具体，例如『7550 墙触及概率约 24%、收在上方约 12%』『flip zone 7475-7495，跌进去 gamma 转负』；"
            "但不要把 JSON 里所有数字复述一遍，只挑改变判断的那几个。",
            json.dumps(compact_payload, ensure_ascii=False, sort_keys=True),
        )
    )


def compact_window(window: object) -> dict[str, object] | None:
    if not isinstance(window, dict):
        return None
    return {
        "name": window.get("name"),
        "priority": window.get("priority"),
        "cadence_seconds": window.get("cadence_seconds"),
        "summary_cadence_seconds": window.get("summary_cadence_seconds"),
        "spxw_sampling_mode": window.get("spxw_sampling_mode"),
        "user_unattended": window.get("user_unattended"),
    }


def compact_analysis_payload(
    payload: dict[str, object],
    alerts: list[dict[str, object]],
) -> dict[str, object]:
    market_context = payload.get("market_context")
    algorithm_quality: object = None
    if isinstance(market_context, dict):
        algorithm_quality = {
            "quality_summary": market_context.get("quality_summary"),
            "note": (
                "Non-focus market context may be used only as hidden algorithm scoring input; "
                "never mention individual non-SPX/SPXW/ES instruments to the human."
            ),
        }

    return {
        "as_of": payload.get("as_of"),
        "window": compact_window(payload.get("window")),
        "visible_scope": ("SPX", "SPXW", "ES"),
        "human_focus_context": payload.get("human_focus_context"),
        "algorithm_quality": algorithm_quality,
        "alerts": alerts[:8],
    }


def build_codex_prompt(
    payload: dict[str, object],
    alerts: list[dict[str, object]],
    previous_push: dict[str, object] | None = None,
) -> str:
    compact_payload = compact_analysis_payload(payload, alerts)
    previous_text = ""
    if isinstance(previous_push, dict) and previous_push.get("text"):
        previous_text = json.dumps(
            {"at": previous_push.get("at"), "kind": previous_push.get("kind"), "text": previous_push.get("text")},
            ensure_ascii=False,
        )
    return "\n".join(
        (
            "你是 SPX Spark 的快速告警确认 agent。",
            "只根据下面的本机 JSON 判断是否需要推送给人类。不要给自动下单指令，不要编造缺失数据。",
            "人类只交易 SPX/SPXW；结论和检查项必须落在 SPX/SPXW/ES、期权墙、gamma、IV surface 上，"
            "VIX/VIX1D/VVIX/SKEW 等波动率指数作 vol regime 上下文，SPY/QQQ 等指数 ETF 可少量引用作确认上下文。",
            "不要提加密、链上、预测市场类数据源；隐藏算法上下文只能影响是否推送，不能进入人类可见解释。",
            "凡是 research_only、stale、missing、unknown、coverage 不足或 IV surface stale，默认不外发；只说明数据质量。",
            "带 source_gate 的告警默认不外发，唯一例外是 broker_unavailable_fallback、ibkr_session_state、ibkr_positions、iv_surface；"
            "ibkr_positions 表示 IBKR 实盘 SPXW 持仓变化或风险；iv_surface 表示 SPXW IV 曲面期限差或异动。",
            "如果 SPXW 期权 freshness gate 失败，不得基于 wall/gamma/IV 做看盘结论。",
            "如果 options_map 警告含 underlier_mismatch，或 gamma_state 以 unknown 开头，不得基于 wall/gamma 下结论，只能说明数据降级。",
            "gamma_state 为 zero_gamma_transition（micopedia 为 transition）表示零 gamma 交叉区：突破后波动可能放大，不得把靠近墙位直接当作支撑确认。",
            "剧本必须双向对称：跌破关键位后要讲防守；但若价格随后收复该关键位（回到 put wall/flip zone 上方）并持续站稳，这是反弹剧本被激活的信号，必须明确说『XX 已收复，反弹剧本激活，若回踩不破 XX 可视为确认』，不允许在价格已回升时仍只重复防守结论。",
            "推送的核心职责之一是替读者对抗 FOMO 和恐慌，每条外发内容按当下位置至少命中其一：",
            "(a) 价格在两关键位中间快速移动时，明确写『半路，不追，计划位在 XX/XX』——半山腰追单是要拦下的第一行为；",
            "(b) 当日移动已接近或超过 expected_move_points 时，明确写『日内已走完预期波幅的约 X%，顺方向追单赔率差』；",
            "(c) 价格已进入 put 墙支撑带时，明确写『支撑带内是计划中的接多区，不是割肉点；防守只在跌破 XX(带下沿)后执行』；急拉进 call 墙带时对称提醒不追多；",
            "(d) 读者若可能持有浮亏仓位（价格深跌后反弹途中），提醒『离场决策看位置不看盈亏：现在在 XX 位置，剧本是 XX，按剧本而不是按回本冲动操作』。",
            "墙位阶梯（human_focus_context 里的 wall_ladder）有上下各 4 档：判断支撑/阻力时看整条阶梯而不是单点；相邻 put 墙 OI 接近时按支撑带表述（如 7460-7500 带），价格在带内磨底不等于支撑失效。",
            "previous_push 是最近一条已外发推送；若本次结论与它实质相同（同方向、同关键位、无新概率/位置增量），判为不需要推送。若结论方向相对它发生反转（防守→反弹或反之），必须点明『相对上一条，剧本已变』并说明触发原因。",
            ("previous_push: " + previous_text) if previous_text else "previous_push: null",
            "如果 ES/SPX anchor 缺失，不得把任何链上或 proxy 数据当作交易确认。",
            "如果 window.user_unattended 为 true，说明人类大概率在睡觉：只有 critical/high 且数据质量完好的 SPX/SPXW 风险才值得外发，其余一律不推送。",
            "发送决策必须优先参考 Micopedia、SPXW call wall/put wall/zero gamma、以及过去 1 小时 IV surface/期权变化。",
            "单一指标（如仅 put skew 变陡）默认不足以外发；需要 gamma 状态、VIX/vol regime 或价格行为中至少一项共振确认，否则判为不需要推送。",
            "输出中文，最多 10 行，结论先行：第一行之后紧跟一句话说清发生了什么、对读者挂的单/持仓意味着什么；"
            "再给 2-3 行证据(触发数字 + gamma 地形与概率)；再给 1-2 行 if/then(价格到哪个位置剧本如何分岔、该盯什么)；"
            "最后 1 行 vol regime(VIX/VIX1D、dip_context)与数据质量、快照时间。",
            "引用数字要具体，例如『7550 墙触及概率约 24%、收在上方约 12%』『flip zone 7475-7495，跌进去 gamma 转负』"
            "『尾部保护贵(dip_context=expensive_tail_protection)，急跌大概率是保护盘驱动』；"
            "只挑改变判断的数字，不要复述全部 JSON。",
            "如果数据质量不足，明确说 degraded。",
            "如果值得外发，第一行必须用 `需要看盘:` 开头；如果不值得外发，第一行必须用 `不需要推送:` 开头。",
            "你的回复不会直接发给人类：只有以 `需要看盘:` 开头并通过范围校验的内容才会被转发，其余仅记录在案。",
            json.dumps(compact_payload, ensure_ascii=False, sort_keys=True),
        )
    )
