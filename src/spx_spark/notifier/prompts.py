from __future__ import annotations

import json

# Shared Micopedia + Steven observe-only doctrine for every human-facing prompt.
# Keep wording short: writers must not invent execution authority or vendor DEX.
FRAMEWORK_GUARDRAILS = (
    "框架口径（Micopedia + Steven，observe_only）：先 regime，再 map，再 flow，再 trigger，"
    "再 expression，最后 exit；输出是检查清单与解释，不是下单授权、不是可执行交易信号。",
    "GEX/gamma_state 与任何 net_dex_proxy/dagex_proxy/vex_proxy/cex_proxy 都是自家结构代理；"
    "不得写成 vendor Net DEX/DAGEX，不得把代理指标置信度说成 high。",
    "锚只能是 SPX/ES（或明确的 chain_implied）；Hyperliquid SP500 只是弱研究代理，"
    "绝不能单独确认破位、墙失效或 SETUP。",
    "若 JSON 含 steven / steven_context：只作 observe_only 附注，不得抬 severity、不得改成买卖指令。",
    "严格按 as_of 判断会话：09:30-16:00 ET 是 SPX RTH，此时 ES 只能称 RTH/日内路径，"
    "不得称 GTH 或用夜盘薄流动性解释；GTH 是 SPX 期权的现金盘外时段，ES 自身是 Globex。",
    "12:00-13:00 ET 是搭档睡前的午盘趋势确认窗：用完整上午的 ES/SPX 路径、VWAP、量价、墙位和 vol regime"
    "决定平仓、减仓或带保护持有。原则上不因午前噪音过早平仓，但硬止损、结构失效和风险上限始终优先。",
)


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


def direct_push_category(alerts: list[dict[str, object]]) -> str:
    kinds = {str(alert.get("kind") or "") for alert in alerts}
    if any(kind.startswith("spxw_position_") for kind in kinds):
        return "持仓事件"
    if kinds & {
        "ibkr_session_interrupted",
        "ibkr_session_restored",
        "ibkr_session_login",
    }:
        return "系统事件"
    if kinds & {"put_skew_steepening_5m", "atm_iv_jump_5m"}:
        return "盘外波动率信号"
    if kinds & {"flip_reclaim_call", "call_wall_breakout_call"}:
        return "0DTE Call 结构确认"
    return "事件"


def build_direct_push_prompt(payload: dict[str, object], alerts: list[dict[str, object]]) -> str:
    """Writer prompt for bypass (direct-push) events: the decision to push is
    already made, the LLM only rewrites the raw event into readable Chinese."""
    focus = payload.get("human_focus_context")
    compact_focus: dict[str, object] | None = None
    if isinstance(focus, dict):
        compact_focus = {
            "prices": focus.get("prices"),
            "spxw_options": focus.get("spxw_options"),
        }
    compact_payload = {
        "as_of": payload.get("as_of"),
        "window": compact_window(payload.get("window")),
        "alerts": alerts[:6],
        "human_focus_context": compact_focus,
        "regime_decision": payload.get("regime_decision"),
        "breakout_filter": payload.get("breakout_filter"),
    }
    category = direct_push_category(alerts)
    return "\n".join(
        (
            "你在交易台值班，一个即时事件刚触发，要发给只做 SPX/SPXW 0DTE/1DTE 买方的搭档。",
            "推送决定已经做出，不用判断要不要推；你的活是把原始事件 JSON 翻译成他扫一眼就知道『发生了什么、跟我有什么关系、要不要动手』的便签。",
            *FRAMEWORK_GUARDRAILS,
            f"事件类别是「{category}」，第一行用【{category}】开头，一句话说清发生了什么，关键数字照抄。",
            "然后按类别补 2-4 行，站在他的仓位和挂单角度说：",
            "- 持仓事件：哪条腿、数量/方向怎么变的、浮盈浮亏多少，此刻价格站在关键位的哪一侧——这决定他是该锁利润还是该按剧本扛；",
            "- 系统事件(IBKR 会话中断/恢复)：行情数据和已挂限价单受不受影响，他要做什么——多数时候是知悉即可，需要检查挂单就直说；",
            "- 盘外波动率信号(skew 急陡/ATM IV 跳升)：谁在抢什么(如机构买下行保护)、这通常领先什么，拿什么确认(价格/gamma/VIX)——信号≠行动，确认位没到就只是提高警觉；",
            "- 0DTE Call 结构确认：写清是收复冻结 flip 还是突破旧 call wall、回踩观察位和失效线；强调已由 SPX/ES 新鲜样本确认，但不追价、不自动下单；",
            "只用 JSON 里的事实，数字不编不改；数据 degraded 时如实说明。",
            "greeks_reference_0dte 是严格 SPXW 当日到期的只读情景层；position_sign/direction 为 unknown 时，负 gamma 只表示潜在放大，绝不等于看跌或自动买 put。",
            "breakout_filter 是代码裁决：blocked/pending 不得写成突破成立；只有 supported 且 actionable=true 才能称为通过假突破过滤。",
            "总共不超过 6 行。像口头交接，不像播报稿；不写免责声明。",
            json.dumps(compact_payload, ensure_ascii=False, sort_keys=True),
        )
    )


def build_agent_prompt(payload: dict[str, object], alerts: list[dict[str, object]]) -> str:
    compact_payload = {
        "as_of": payload.get("as_of"),
        "window": compact_window(payload.get("window")),
        "alerts": alerts[:12],
        "human_focus_context": payload.get("human_focus_context"),
        "regime_decision": payload.get("regime_decision"),
        "breakout_filter": payload.get("breakout_filter"),
    }
    return "\n".join(
        (
            "盘中告警触发了，你要给搭档发一条便签。他只做 SPX/SPXW 0DTE/1DTE 买方(call/put/垂直价差)，"
            "盘前挂了限价单，此刻可能盯盘也可能在睡觉。他扫一眼要能回答：市场在干什么，我挂的单/持仓要不要动。",
            "动笔前先想清楚(不写出来)：这个告警背后是谁在动手——对冲盘、抢保护的、还是单纯流动性薄？"
            "价格此刻站的位置对他的挂单意味着成交概率变高还是剧本作废？",
            "只根据下面的 JSON 做判断；不下单指令，不假设缺失数据。",
            *FRAMEWORK_GUARDRAILS,
            "搭档只交易 SPX/SPXW；结论落在 SPX/SPXW/ES、期权墙、gamma、IV surface 上，"
            "VIX/VIX1D/VVIX/SKEW 作 vol regime 上下文，SPY/QQQ 可少量引用作确认；不提加密或预测市场数据源。",
            "options_map 警告含 underlier_mismatch 或 gamma_state 以 unknown 开头时，只说明数据降级，不下 wall/gamma 结论。",
            "greeks_reference_0dte 只解释严格 SPXW 当日到期合约对价格、时间和 IV 的敏感度；它不改变候选方向、排序或限价，position_sign unknown 时负 gamma 不等于下跌。",
            "regime_decision 与 breakout_filter 是代码生成的确定性结论；不得自行翻案。blocked/pending 不得描述成有效突破，supported 且 actionable=true 才可称为突破过滤通过。",
            "剧本必须双向：跌破关键位讲防守，但价格收复关键位并站稳时必须明说反弹剧本激活，不许在价格回升时还重复防守结论。",
            "输出中文，最多 12 行，结论先行：",
            "第一句话说清发生了什么、对挂单/持仓意味着什么(如『价格逼近 7500 put wall，反弹买 call 的挂单可能马上成交』)。",
            "然后 2-3 行证据：触发告警的关键数字 + gamma 地形(flip_zone、zero gamma、墙位触及/收破概率)。",
            "然后 1-2 行 if/then：价格到哪个具体位置剧本分岔，该盯什么——这是判断的证伪条件。",
            "最后 1 行 vol regime 与数据质量(VIX/VIX1D、dip_context、有无 degraded)。",
            "数字要具体(『7550 墙触及概率约 24%、收在上方约 12%』『flip zone 7475-7495，跌进去 gamma 转负』)，"
            "但只挑改变判断的那几个，别把 JSON 复述一遍。",
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
        "regime_decision": payload.get("regime_decision"),
        "breakout_filter": payload.get("breakout_filter"),
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
            {
                "at": previous_push.get("at"),
                "kind": previous_push.get("kind"),
                "text": previous_push.get("text"),
            },
            ensure_ascii=False,
        )
    return "\n".join(
        (
            "你是交易台的资深值班交易员，负责决定一个告警值不值得打断搭档。搭档只做 SPX/SPXW 0DTE/1DTE 买方，"
            "挂了限价单，被打断一次的代价不低——推得多他就不看了，推得少他会错过必须动手的时刻。",
            "先在心里过三个问题再下判断(不写出来)：这个告警背后是谁在动手、还是只是数据噪声？"
            "它改变搭档任何一张挂单或持仓的赔率吗？上一条推送之后市场给出新信息了吗？三个都答不上来就不推。",
            "只根据下面的本机 JSON 判断。不下单指令，不编造缺失数据。",
            *FRAMEWORK_GUARDRAILS,
            "搭档只交易 SPX/SPXW；结论和检查项必须落在 SPX/SPXW/ES、期权墙、gamma、IV surface 上，"
            "VIX/VIX1D/VVIX/SKEW 等波动率指数作 vol regime 上下文，SPY/QQQ 等指数 ETF 可少量引用作确认上下文。",
            "不要提加密、链上、预测市场类数据源；隐藏算法上下文只能影响是否推送，不能进入人类可见解释。",
            "凡是 research_only、stale、missing、unknown、coverage 不足或 IV surface stale，默认不外发；只说明数据质量。",
            "带 source_gate 的告警默认不外发，唯一例外是 ibkr_session_state、ibkr_positions、iv_surface；"
            "ibkr_positions 表示 IBKR 实盘 SPXW 持仓变化或风险；iv_surface 表示 SPXW IV 曲面期限差或异动。",
            "如果 SPXW 期权 freshness gate 失败，不得基于 wall/gamma/IV 做看盘结论。",
            "如果 options_map 警告含 underlier_mismatch，或 gamma_state 以 unknown 开头，不得基于 wall/gamma 下结论，只能说明数据降级。",
            "gamma_state 为 zero_gamma_transition（micopedia 为 transition）表示零 gamma 交叉区：突破后波动可能放大，不得把靠近墙位直接当作支撑确认。",
            "greeks_reference_0dte 是严格 SPXW 当日到期的 reference-only 情景层；只用于解释价格/时间/IV 冲击。position_sign/direction unknown 时不得推导 dealer 净方向，负 gamma 不等于看跌，也不能单独触发推送。",
            "regime_decision 与 breakout_filter 是代码裁决，不是让你二次猜测的原始指标；blocked/pending 不得写成突破确认，supported 且 actionable=true 才能升级 breakout。",
            "剧本必须双向对称：跌破关键位后要讲防守；但若价格随后收复该关键位（回到 put wall/flip zone 上方）并持续站稳，这是反弹剧本被激活的信号，必须明确说『XX 已收复，反弹剧本激活，若回踩不破 XX 可视为确认』，不允许在价格已回升时仍只重复防守结论。",
            "你的另一半职责是替搭档对抗 FOMO 和恐慌——告警最容易在他情绪最高的时刻到达，每条外发内容按当下位置至少命中其一：",
            "(a) 价格在两关键位中间快速移动时，明确写『半路，不追，计划位在 XX/XX』——半山腰追单是要拦下的第一行为；",
            "(b) 当日移动已接近或超过 expected_move_points 时，明确写『日内已走完预期波幅的约 X%，顺方向追单赔率差』；",
            "(c) 价格已进入 put 墙支撑带时，明确写『支撑带内是计划中的接多区，不是割肉点；防守只在跌破 XX(带下沿)后执行』；急拉进 call 墙带时对称提醒不追多；",
            "(d) 读者若可能持有浮亏仓位（价格深跌后反弹途中），提醒『离场决策看位置不看盈亏：现在在 XX 位置，剧本是 XX，按剧本而不是按回本冲动操作』。",
            "墙位阶梯（human_focus_context 里的 wall_ladder）有上下各 4 档：判断支撑/阻力时看整条阶梯而不是单点；相邻 put 墙 OI 接近时按支撑带表述（如 7460-7500 带），价格在带内磨底不等于支撑失效。",
            "rn_density（若 quality=ok）是市场定价的收盘分布：价格逼近 p10/p90（80% 区间边缘）时应指出『已到市场定价的尾部，顺方向继续赌需要新信息』；prob_below_put_wall/prob_above_call_wall 给出收在墙外的市场定价概率，可直接引用。",
            "previous_push 是最近一条已外发推送；若本次结论与它实质相同（同方向、同关键位、无新概率/位置增量），判为不需要推送。若结论方向相对它发生反转（防守→反弹或反之），必须点明『相对上一条，剧本已变』并说明触发原因。",
            ("previous_push: " + previous_text) if previous_text else "previous_push: null",
            "如果 ES/SPX anchor 缺失，不得把任何链上或 proxy 数据当作交易确认。",
            "如果 window.user_unattended 为 true，说明人类大概率在睡觉：只有 critical/high 且数据质量完好的 SPX/SPXW 风险才值得外发，其余一律不推送。",
            "发送决策必须优先参考 Micopedia decision stack、Steven observe_only 附注（若有）、"
            "SPXW call wall/put wall/zero gamma、以及过去 1 小时 IV surface/期权变化。",
            "单一指标（如仅 put skew 变陡）默认不足以外发；需要 gamma 状态、VIX/vol regime 或价格行为中至少一项共振确认，否则判为不需要推送。",
            "输出中文，最多 10 行，像交易台口头交接不像播报稿，结论先行：第一行之后紧跟一句话说清发生了什么、"
            "对搭档挂的单/持仓意味着什么；再给 2-3 行证据(触发数字 + gamma 地形与概率)；"
            "再给 1-2 行 if/then(价格到哪个位置剧本如何分岔、该盯什么——这也是判断的证伪条件)；"
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
