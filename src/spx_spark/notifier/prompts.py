from __future__ import annotations

import json

from spx_spark.notifier.policy import (
    is_position_holding_alert,
    is_system_event_alert,
)

# Shared Micopedia + Steven observe-only doctrine for every human-facing prompt.
# Keep wording short: writers must not invent execution authority or vendor DEX.
FRAMEWORK_GUARDRAILS = (
    "框架口径（Micopedia + Steven，observe_only）：先 regime，再 map，再 flow，再 trigger，"
    "再 expression，最后 exit；输出是检查清单与解释，不是下单授权、不是可执行交易信号。",
    "GEX/gamma_state 与任何 net_dex_proxy/dagex_proxy/vex_proxy/cex_proxy 都是自家结构代理；"
    "不得写成 vendor Net DEX/DAGEX，不得把代理指标置信度说成 high。",
    "公开 OI、成交量与 NBBO 不能识别参与者身份或净仓方向；不得据此声称 dealer/做市商买卖、移仓、"
    "防守/弃守，也不得把墙位或 pin 因果归于 gamma。B-L 与触达概率是风险中性启发，不是真实胜率。",
    "锚只能是 SPX/ES（或明确的 chain_implied）；Hyperliquid SP500 只是弱研究代理，"
    "绝不能单独确认破位、墙失效或 SETUP。",
    "若 JSON 含 steven / steven_context：只作 observe_only 附注，不得抬 severity、不得改成买卖指令。",
    "严格按 as_of 判断会话：09:30-16:00 ET 是 SPX RTH，此时 ES 只能称 RTH/日内路径，"
    "不得称 GTH 或用夜盘薄流动性解释；GTH 是 SPX 期权的现金盘外时段，ES 自身是 Globex。",
    "13:00-15:30 ET 仍允许新的 MANUAL READY；15:30 后停止新入场，撤销未成交意图并在"
    "15:45 ET 前退出。硬止损、结构失效和风险上限始终优先。",
)

# Shared voice for every human-facing market message.  The delivery protocol
# may still require an internal first-line cue; pipeline.py strips that cue
# before the text reaches Bark or Feishu.
DESK_STYLE_GUARDRAILS = (
    "写作身份是机构自营台的 SPX 指数期权交易员，不是散户喊单群、加密货币频道、财经主播或客服。",
    "语气必须冷静、精确、有决断但不过度自信；攻击性体现在入场价格苛刻、失效立即撤退和风险预算明确，"
    "不体现在口号、情绪或夸张措辞。",
    "禁用『需要看盘』『半路』『不追』『剧本』『墙内』『接多区』『砸』『抢』『扛』『顶上』『狼性』等喊单式表达；"
    "分别改写为事件结论、执行区间、风险回报不足、主策略、结构区间、流量驱动、持有条件或策略升级。",
    "人类可见正文使用固定栏目：`## Desk View`、`## Execution`、`## Risk`，确有目标时增加 `## Targets`，"
    "数据降级时增加 `## Data Quality`；不要使用『证据/条件/盯』或四段式资讯播报。",
    "生产计划只保留一个主方向、一个执行区间、一个明确失效条件和最多两个目标；相反方向只写成状态转换。"
    "Convexity Idea Radar 是例外：可并列最佳 Call/Put 条件假设，但无执行权限。",
    "区分事实与判断：价格、墙位、NBBO、Greeks 是事实；方向倾向与执行结论是判断。不得把观察状态写成入场授权。",
)


def format_alert_message(payload: dict[str, object], alerts: list[dict[str, object]]) -> str:
    window = payload.get("window")
    window_name = "unknown"
    priority = "unknown"
    if isinstance(window, dict):
        window_name = str(window.get("name") or "unknown")
        priority = str(window.get("priority") or "unknown")

    if alerts and all(is_system_event_alert(alert) for alert in alerts):
        return _format_system_alert(payload, alerts, window_name, priority)
    if alerts and all(is_position_holding_alert(alert) for alert in alerts):
        return _format_position_alert(payload, alerts, window_name, priority)

    kinds = {str(alert.get("kind") or "") for alert in alerts}
    management_only = bool(kinds) and kinds <= {"gth_advisory_management"}
    decision = payload.get("strategy_decision")
    if not isinstance(decision, dict):
        decision = _load_latest_strategy_decision()
        if decision:
            payload = {**payload, "strategy_decision": decision}
    decision = decision if isinstance(decision, dict) else {}
    role, contract_line = _message_role_and_contract_line(decision)

    if management_only:
        badge = "🟠 只管理 · 不开新仓"
        lines = [badge, f"方向  {_direct_alert_direction(alerts)}"]
        lines.append("动作  仅管理已人工参与的前序机会；未验证持仓时不操作")
    elif role == "MANUAL_CANDIDATE":
        badge = "🟢 结构事件 · 同期有人工候选"
        lines = [badge, f"方向  {_direct_alert_direction(alerts)}"]
        lines.append("等待  以同周期策略候选卡为准，本条仅提供结构背景")
    elif role == "NO_TRADE":
        badge = "🔴 只观察"
        lines = [badge, f"方向  {_direct_alert_direction(alerts)}"]
        lines.append("等待  对应关键位确认，并由执行层生成精确 SPXW 合约与新鲜报价")
    else:
        badge = "🔴 只观察"
        lines = [badge, f"方向  {_direct_alert_direction(alerts)}"]
        lines.append("等待  对应关键位确认，并由执行层生成精确 SPXW 合约与新鲜报价")

    lines.append(contract_line)
    for alert in alerts[:6]:
        title = alert.get("title")
        detail = alert.get("detail")
        lines.append(f"事件  {title}")
        if detail:
            lines.append(f"解释  {detail}")
    if len(alerts) > 6:
        lines.append(f"审计  另有 {len(alerts) - 6} 个低优先级事件")
    lines.append(f"数据  as_of={payload.get('as_of')} · window={window_name} · priority={priority}")
    lines.append(_decision_audit_line(decision, role=role, payload=payload))
    return "\n".join(lines)


def _load_latest_strategy_decision() -> dict[str, object]:
    """Best-effort same-host latest decision for STRUCTURE_EVENT messages.

    Alert payloads often omit strategy_decision. Prefer an explicit payload
    field; otherwise read the MF-written latest snapshot. Missing/unreadable
    files yield an empty dict so the template stays honest.
    """

    import os
    from pathlib import Path

    roots = (
        os.environ.get("SPX_DATA_ROOT"),
        os.environ.get("MARKET_DATA_DATA_ROOT"),
        os.environ.get("MAINTENANCE_DATA_ROOT"),
    )
    for root in roots:
        if not root:
            continue
        path = Path(root) / "latest" / "strategy_decision.json"
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            continue
        if isinstance(payload, dict) and payload.get("decision_id"):
            return payload
    return {}


def _message_role_and_contract_line(decision: dict[str, object]) -> tuple[str, str]:
    if not decision:
        return "STRUCTURE_EVENT", "策略决策 本周期不可用"
    decision_type = str(decision.get("decision_type") or "")
    authority = str(decision.get("action_authority") or "")
    decision_id = str(decision.get("decision_id") or "")
    short_id = decision_id[9:21] if decision_id.startswith("strategy:") else decision_id[:12]
    if authority == "manual" and decision_type and decision_type != "NO_TRADE":
        return (
            "MANUAL_CANDIDATE",
            f"合约  本周期存在人工候选（{short_id or 'unknown'}），以候选卡为准",
        )
    if decision_type == "NO_TRADE":
        why = decision.get("why_not") if isinstance(decision.get("why_not"), dict) else {}
        reasons = why.get("reasons") if isinstance(why, dict) else None
        reason = ""
        if isinstance(reasons, list) and reasons:
            reason = str(reasons[0])
        suffix = f"；原因 {reason}" if reason else ""
        return "NO_TRADE", f"合约  当前没有可执行合约{suffix}"
    return "STRUCTURE_EVENT", "策略决策 本周期不可用"


def _decision_audit_line(
    decision: dict[str, object], *, role: str, payload: dict[str, object]
) -> str:
    decision_id = str(decision.get("decision_id") or "-")
    short_id = decision_id[:12] if decision_id != "-" else "-"
    why = decision.get("why_not") if isinstance(decision.get("why_not"), dict) else {}
    reasons = why.get("reasons") if isinstance(why, dict) else None
    top_reason = str(reasons[0]) if isinstance(reasons, list) and reasons else "-"
    if role == "MANUAL_CANDIDATE":
        top_reason = str(decision.get("decision_type") or "manual_candidate")
    policy = str(decision.get("policy_version") or "-")
    git_sha = str(decision.get("runtime_git_sha") or payload.get("runtime_git_sha") or "-")
    degraded = payload.get("delivery_degraded_count")
    degraded_note = ""
    try:
        count = int(degraded) if degraded is not None else 0
    except (TypeError, ValueError):
        count = 0
    if count > 0:
        degraded_note = f" · 投递 degraded({count})"
    return (
        f"决策 id={short_id} 角色={role} 原因={top_reason} "
        f"版本 policy={policy} git={git_sha}{degraded_note}"
    )


def _format_system_alert(
    payload: dict[str, object],
    alerts: list[dict[str, object]],
    window_name: str,
    priority: str,
) -> str:
    lines = ["⚠️ 系统状态"]
    for alert in alerts[:6]:
        lines.append(f"状态  {alert.get('title')}")
        if alert.get("detail"):
            lines.append(f"影响  {alert.get('detail')}")
    lines.append("交易  本条不是 Call/Put 信号")
    lines.append(f"数据  as_of={payload.get('as_of')} · window={window_name} · priority={priority}")
    return "\n".join(lines)


def _format_position_alert(
    payload: dict[str, object],
    alerts: list[dict[str, object]],
    window_name: str,
    priority: str,
) -> str:
    lines = ["🟠 持仓事件"]
    for alert in alerts[:6]:
        lines.append(f"状态  {alert.get('title')}")
        if alert.get("detail"):
            lines.append(f"动作  {alert.get('detail')}")
    lines.append(f"数据  as_of={payload.get('as_of')} · window={window_name} · priority={priority}")
    return "\n".join(lines)


def _direct_alert_direction(alerts: list[dict[str, object]]) -> str:
    for alert in alerts:
        context = alert.get("audit_context")
        context = context if isinstance(context, dict) else {}
        right = str(context.get("option_right") or "").upper()
        direction = str(context.get("direction") or "").lower()
        kind = str(alert.get("kind") or "").lower()
        title = str(alert.get("title") or "").lower()
        if right == "C" or direction in {"up", "bullish"} or kind.endswith("_call"):
            return "偏多（仅事件背景，不是入场授权）"
        if right == "P" or direction in {"down", "bearish"}:
            return "偏空（仅事件背景，不是入场授权）"
        if any(token in title for token in ("bullish", "多头", "偏多", "call")):
            return "偏多（仅事件背景，不是入场授权）"
        if any(token in title for token in ("bearish", "空头", "偏空", "put")):
            return "偏空（仅事件背景，不是入场授权）"
    return "未定（仅事件背景，不是入场授权）"


def direct_push_category(alerts: list[dict[str, object]]) -> str:
    kinds = {str(alert.get("kind") or "") for alert in alerts}
    if "gth_directional_advisory" in kinds:
        return "GTH Call/Put 方向提示"
    if "gth_advisory_management" in kinds:
        return "GTH 机会管理"
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


def direct_push_header(alerts: list[dict[str, object]]) -> str:
    kinds = {str(alert.get("kind") or "") for alert in alerts}
    if "gth_directional_advisory" in kinds:
        right = _gth_advisory_right(alerts)
        return f"SPX GTH | {right} ADVISORY"
    if "gth_advisory_management" in kinds:
        right = _gth_advisory_right(alerts)
        return f"SPX GTH | {right} MANAGEMENT"
    if "gth_dip_reclaim_call" in kinds:
        return "SPX 0DTE | CALL RECLAIM"
    if kinds & {"flip_reclaim_call", "call_wall_breakout_call"}:
        return "SPX 0DTE | CALL STRUCTURE"
    if any(kind.startswith("spxw_position_") for kind in kinds):
        return "SPX | POSITION RISK"
    if kinds & {
        "ibkr_session_interrupted",
        "ibkr_session_restored",
        "ibkr_session_login",
        "market_data_ibkr_fallback_activated",
        "market_data_all_providers_unavailable",
        "market_data_schwab_restored",
    }:
        return "SPX | SYSTEM STATUS"
    if kinds & {"put_skew_steepening_5m", "atm_iv_jump_5m"}:
        return "SPX 0DTE | VOLATILITY"
    return "SPX 0DTE | TACTICAL UPDATE"


def _gth_advisory_right(alerts: list[dict[str, object]]) -> str:
    for alert in alerts:
        context = alert.get("audit_context")
        if isinstance(context, dict):
            right = str(context.get("option_right") or "").upper()
            if right == "C":
                return "CALL"
            if right == "P":
                return "PUT"
    return "DIRECTIONAL"


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
    header = direct_push_header(alerts)
    return "\n".join(
        (
            "你在 SPX 指数期权自营台值班。一个即时事件已通过代码门控，需写成机构级 tactical update。",
            "推送决定已经做出，不要再判断是否发送；只把 JSON 转成可执行、可证伪的风险简报。",
            *FRAMEWORK_GUARDRAILS,
            *DESK_STYLE_GUARDRAILS,
            f"事件类别是「{category}」。正文首行必须写 `**{header}**`，随后按固定栏目表达。",
            "按类别保留必要信息：",
            "- 持仓事件：哪条腿、数量/方向怎么变的、浮盈浮亏多少，此刻价格站在关键位的哪一侧——这决定他是该锁利润还是该按剧本扛；",
            "- 系统事件(IBKR 会话中断/恢复)：行情数据和已挂限价单受不受影响，他要做什么——多数时候是知悉即可，需要检查挂单就直说；",
            "- 盘外波动率信号(skew 急陡/ATM IV 跳升)：只写观测到的曲面变化、相对历史幅度及价格/VIX确认；不得推断是谁在买保护——信号≠行动；",
            "- 0DTE Call 结构确认：写清收复 flip 或突破 call wall、首选执行区间和失效线；注明 SPX/ES 确认状态与自动下单关闭；",
            "- GTH 方向提示：m1 只写 Call/Put 方向机会并明确无合约、无限价、不可执行；m2 只写前序机会的条件式止盈/抬止损，禁止写成新入场；",
            "只用 JSON 里的事实，数字不编不改；数据 degraded 时如实说明。",
            "不要输出 JSON、系统思考或索取更多数据。",
            "greeks_reference_0dte 是严格 SPXW 当日到期的只读情景层；position_sign/direction 为 unknown 时，负 gamma 只表示潜在放大，绝不等于看跌或自动买 put。",
            "breakout_filter 是代码裁决：blocked/pending 不得写成突破成立；只有 supported 且 actionable=true 才能称为通过假突破过滤。",
            "总共不超过 9 行；不写免责声明。",
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
            "动笔前先想清楚(不写出来)：这个告警是价格、报价、波动曲面真实变化，还是数据/流动性噪声？"
            "价格此刻站的位置对他的挂单意味着触发更近还是主策略已失效？",
            "只根据下面的 JSON 做判断；不下单指令，不假设缺失数据。",
            *FRAMEWORK_GUARDRAILS,
            *DESK_STYLE_GUARDRAILS,
            "搭档只交易 SPX/SPXW；结论落在 SPX/SPXW/ES、期权墙、gamma、IV surface 上，"
            "VIX/VIX1D/VVIX/SKEW 作 vol regime 上下文，SPY/QQQ 可少量引用作确认；不提加密或预测市场数据源。",
            "options_map 警告含 underlier_mismatch 或 gamma_state 以 unknown 开头时，只说明数据降级，不下 wall/gamma 结论。",
            "greeks_reference_0dte 只解释严格 SPXW 当日到期合约对价格、时间和 IV 的敏感度；它不改变候选方向、排序或限价，position_sign unknown 时负 gamma 不等于下跌。",
            "regime_decision 与 breakout_filter 是代码生成的确定性结论；不得自行翻案。blocked/pending 不得描述成有效突破，supported 且 actionable=true 才可称为突破过滤通过。",
            "剧本必须双向：跌破关键位讲防守，但价格收复关键位并站稳时必须明说反弹剧本激活，不许在价格回升时还重复防守结论。",
            "输出中文，最多 12 行，采用机构 tactical update 栏目，结论先行：",
            "第一句话说清发生了什么、对挂单/持仓意味着什么(如『价格逼近 7500 put wall，结构测试临近但接受/拒绝尚未确认』)。",
            "然后 2-3 行证据：触发告警的关键数字 + gamma 地形(flip_zone、zero gamma、墙位触及/收破概率)。",
            "然后 1-2 行 if/then：价格到哪个具体位置主策略失效并发生状态转换。",
            "最后 1 行 vol regime 与数据质量(VIX/VIX1D、dip_context、有无 degraded)。",
            "数字要具体(『15m 风险中性墙触达启发约 24%、终值越墙约 12%』『flip zone 7475-7495，跌进去 gamma 转负』)，"
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
            "你是 SPX 指数期权自营台的 senior trader，负责判断一个事件是否足以改变当日 0DTE 风险配置。"
            "接收者是专业交易员；发送成本高，只有新增信息改变方向倾向、执行区间、失效条件或退出决策时才外发。",
            "先在内部回答三个问题：这是真实价格/报价变化还是数据噪声？它是否改变现有策略的风险回报？"
            "相较上一条通知是否出现新的可证伪信息？任一项无法成立时，不发送。",
            "只根据下面的本机 JSON 判断。不下单指令，不编造缺失数据。",
            *FRAMEWORK_GUARDRAILS,
            *DESK_STYLE_GUARDRAILS,
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
            "生产方向表达必须单一：跌破关键位时主策略转为风险控制；重新收复并持续站稳时，主策略才切回反弹。"
            "相反方向只作为当前判断的失效与状态转换；若输入含 Convexity Idea Radar，则另行保留最佳 Call/Put 条件假设。",
            "风险回报纪律按机构口径表达：价格位于两个执行区间之间时写『当前价格不具备入场赔率，首选执行区间为 XX』；",
            "当日移动接近或超过 expected_move_points 时只量化 EM 已使用比例，不得补算剩余空间、尾部概率或胜率；",
            "价格进入 put wall 下方结构区时，仅在价格路径确认拒绝后给出 Call 候选；进入 call wall 上方结构区时，"
            "仅在确认拒绝后讨论 Put，确认接受则保留相反方向假设；"
            "不得用『接多』『割肉』『不追』等散户表达。",
            "wall_ladder 是上下各最多 4 档的 OI/GEX 集中地图；相邻档可称结构集中带，但没有价格接受/拒绝确认时，"
            "不得直接命名为支撑、阻力或硬墙。",
            "rn_density（quality=ok）只表示输入所列到期终值的风险中性分布，不是 13:00 区间或真实胜率；"
            "prob_below_put_wall/prob_above_call_wall 只有同时写明 horizon 与风险中性语义时才可引用。",
            "previous_push 是最近一条已外发推送；若本次结论与它实质相同（同方向、同关键位、无新概率/位置增量），判为不需要推送。"
            "若方向相对上一条反转，必须写明『Desk View 已由 X 调整为 Y』及触发该调整的价格条件。",
            ("previous_push: " + previous_text) if previous_text else "previous_push: null",
            "如果 ES/SPX anchor 缺失，不得把任何链上或 proxy 数据当作交易确认。",
            "如果 window.user_unattended 为 true，说明人类大概率在睡觉：只有 critical/high 且数据质量完好的 SPX/SPXW 风险才值得外发，其余一律不推送。",
            "发送决策必须优先参考 Micopedia decision stack、Steven observe_only 附注（若有）、"
            "SPXW call wall/put wall/zero gamma、以及过去 1 小时 IV surface/期权变化。",
            "单一指标（如仅 put skew 变陡）默认不足以外发；需要 gamma 状态、VIX/vol regime 或价格行为中至少一项共振确认，否则判为不需要推送。",
            "内部协议行之后，输出中文且最多 11 行。正文首行使用 `**SPX 0DTE | TACTICAL UPDATE**`；"
            "随后使用 `## Desk View` 给出唯一主判断，`## Execution` 给出首选执行区间与当前是否授权，"
            "`## Risk` 给出精确失效位，确有目标时使用 `## Targets`；数据降级才增加 `## Data Quality`。",
            "引用数字要具体，例如『15m 风险中性墙触达启发约 24%、终值越墙约 12%』『flip zone 7475-7495，跌进去 gamma 转负』"
            "『尾部保护相对昂贵(dip_context=expensive_tail_protection)，但公开报价不能识别买方身份』；"
            "只挑改变判断的数字，不要复述全部 JSON。",
            "如果数据质量不足，明确说 degraded。",
            "如果值得外发，第一行必须且只能写 `需要看盘:`；如果不值得外发，第一行必须且只能写 `不需要推送:`。",
            "第一行是内部传输协议，不属于正文，系统会在发送给人类前删除；不得在正文再次出现这两个短语。",
            json.dumps(compact_payload, ensure_ascii=False, sort_keys=True),
        )
    )
