"""LLM prompt builders for order-map and status pushes."""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

from spx_spark.analytics.options.pricing import finite_float
from spx_spark.application.order_map.models import PLAY_ORDER, SHANGHAI_TZ
from spx_spark.application.order_map.render import (
    _candidate_by_play,
    _dash,
    _es_volume_line,
    _fmt_prob,
    _globex_trend_line,
    _greeks_reference_line,
    _hl_volume_line,
    _market_feature_lines,
    _l1_liquidity_text,
    _rn_density_line,
    _wall_ladder_lines,
    render_research_only_template,
    render_template,
)
from spx_spark.application.order_map.state import _session_phase_of
from spx_spark.notifier.llm_writer import previous_push_json


GLOBEX_CONTEXT_SYSTEM_PROMPT = "\n".join(
    (
        "你是 SPX 夜盘状态便签的事实编辑器，不是预测模型。只允许使用输入 JSON 和模板中明确提供的事实。",
        "ES-basis SPX 是非 RTH 的结构分析代理；SPX levels 和 es_equivalent_levels 是不同坐标系，禁止自行换算或混用。",
        "上一 RTH 的冻结 OI/GEX 只能称为旧结构参考，不能据此断言 dealer 仓位、墙已守住/弃守、历史触碰次数或 gamma 方向。",
        "允许给条件式主情景、确认阈值和证伪阈值，但每个判断必须能指向输入中的价格、状态机阶段或量价标签。",
        "ES 阈值只能使用 es_equivalent_levels 明列的值；没有下方结构位就明确写没有，不得为了双向表达自造整数关口。",
        "禁止比喻、拟人、夸张修辞和隐藏因果；禁止使用『无引力』『气垫』『燃料』『卖方收工』『真金白银』等措辞。",
        "现金 SPX 或新期权链缺失时，不生成期权价格、Greeks、概率、限价或下单指令。不得写『等开盘再说』。",
        "输出简洁中文，先结论，再位置，再双向条件，最后写数据限制。数字逐字引用，不改写。",
    )
)

_NUMBER_PATTERN = re.compile(r"(?<![A-Za-z_])[-+]?\d+(?:\.\d+)?")
_TEMPLATE_CANDIDATE_PATTERN = re.compile(
    r"(?:\[地图候选\]|计划\d+·).*?SPXW\s+(\d{4}[CP])"
)
_GLOBEX_FORBIDDEN_PHRASES = (
    "无引力",
    "气垫",
    "gamma 燃料",
    "卖方收工",
    "真金白银",
    "JSON 中部被截断",
    "补齐 JSON",
    "完整 JSON",
    "I need the full JSON",
    "I'll pull",
)


def globex_writer_output_valid(text: str, template: str) -> bool:
    """Reject invented prices and causal decoration in an off-hours brief."""
    if any(phrase in text for phrase in _GLOBEX_FORBIDDEN_PHRASES):
        return False
    template_header = template.splitlines()[0].strip() if template.strip() else ""
    if template_header.startswith("【SPX 15m｜") and not text.startswith(template_header):
        return False
    allowed = [float(value) for value in _NUMBER_PATTERN.findall(template)]
    for raw in _NUMBER_PATTERN.findall(text):
        value = float(raw)
        if value in {0.0, 1.0}:
            continue
        tolerance = 0.11 if abs(value) < 100_000 else 0.0
        if not any(abs(value - candidate) <= tolerance for candidate in allowed):
            return False
    return True


def actionable_writer_output_valid(text: str, template: str) -> bool:
    """Require numeric fidelity and conditional-execution semantics."""

    if not globex_writer_output_valid(text, template):
        return False
    contracts = tuple(dict.fromkeys(_TEMPLATE_CANDIDATE_PATTERN.findall(template)))
    if contracts and any(contract not in text for contract in contracts):
        return False
    if contracts and "当前不可预挂" not in text:
        return False
    if "【条件计划｜" in template:
        return (
            text.startswith("【SPX 15m｜")
            and "\n\n" in text
            and "【条件计划｜" in text
        )
    return True


def build_order_prompt(
    payload: dict[str, Any],
    template: str,
    previous_push: dict[str, Any] | None = None,
) -> str:
    writer_payload = {key: value for key, value in payload.items() if not key.startswith("_")}
    if payload.get("research_only") is True:
        return "\n".join(
            (
                "这是 SPX 市场状态，不是系统停摆。ES-basis SPX 代理驱动关键位状态机，ES 是连续价格发现锚，Hyperliquid 只作交叉验证。",
                "给出主情景、当前关键位阶段、确认条件和证伪条件；不要只罗列数据，也不要写『等开盘』。",
                "可以给 breakout/fade 的条件判断，但不得编造期权模型价、限价、触达概率、ETA 或直接下单指令。",
                "首行写『市场状态:』，末行用一句话说明当前缺失的数据及其对不可执行定价的具体影响。",
                "previous_push:" + previous_push_json(previous_push),
                "JSON:" + json.dumps(writer_payload, ensure_ascii=False, separators=(",", ":")),
                "模板:" + template,
            )
        )
    return "\n".join(
        (
            "这条是当天第一张『条件交易地图』。搭档下午刚坐到屏幕前，要用它确定触发位、候选合约和触发后的价格参考。",
            "动笔前先在心里过一遍(不写出来)：今天的 OI 是怎么摆的——put 侧是密集防线还是孤零零一档？dealer 在现价附近是"
            "正 gamma 压波动还是负 gamma 放大波动？今天的 play 里哪张是真机会、哪张只是模板凑数？想清楚再落笔，观点要有取舍，"
            "所有候选同等推荐等于没推荐。",
            "框架口径：Micopedia/Steven observe_only（regime→map→flow→trigger→expression→exit）；"
            "条件交易地图是计划参考不是自动下单；GEX/*_proxy 是结构代理；Hyperliquid 只作弱次级证据，不作 SPX 锚。",
            "regime_decision 与 breakout_filter 是代码生成的确定性决策层，不得自行改判。"
            "breakout_filter.verdict=blocked 时删除/降级同事件 breakout 候选并优先说明假突破风险；pending 时不得写突破成立；"
            "supported 且 actionable=true 才能把 CONFIRMED breakout 写成可执行候选。"
            "说明时引用 impulse_score、barrier_score、local_abs_gex_share、next_wall_distance_points 和 OI/Volume DEX 分歧中真正改变判断的字段。",
            "",
            "输出中文，最多 18 行。第一行以『条件执行参考:』开头，复述模板第一行的日期与时间。",
            "接着给地形定调：pin 还是 transition，为什么(gamma 状态+价格相对 flip 的位置)，今天哪类 play 优先。",
            "墙位讲阶梯不讲孤点(数据在 wall_ladder，OI 定位 + 每档 BS 情景价)：相邻 put 墙 OI 接近(差三成以内)就说成一条支撑带并给出"
            "破了之后的二、三档；第一档独大才说单点硬墙。call 侧同理。"
            "每档 put 墙对应 Call、每档 call 墙对应 Put 的触位情景价只能作为标的触发后的价格参考，不得写成现在可预挂的期权订单。",
            "rn_density(B-L 风险中性分布)可用时引用：市场把收盘定价在哪个中位、80% 区间在哪；给垂直价差选腿时"
            "买腿放赌的方向内、卖腿放 80% 区间外沿附近最划算；quality 非 ok 时注明并降权。",
            "max_pain 可用时必须同时报告合并 OI 的 settlement_strike、Call OI峰和 Put OI峰。Max Pain 只表示当前"
            "采样窗口内的到期赔付最小点，OI峰只表示持仓集中，不得单独解释为支撑、阻力或方向预测；quality 非 ok 时降权。",
            "spxw_0dte_greeks_reference 是严格当日到期、只读的情景参考层，只解释价格/时间/IV 冲击。"
            "position_sign/direction=unknown 时负 gamma 不等于下跌；不得据此改变候选方向、排序、限价或新增下单动作。",
            "conditional_call_bias 只有 status=confirmed 才有效，它来自 5 秒 SPX/ES 价格路径对冻结 flip/旧 call wall 的确认，"
            "不是 Gamma 猜方向；confirmed 时优先讲对应 call 的回踩位与失效线，watch/neutral 不新增动作。",
            "",
            "然后逐条 play(最多 3 条；conditional_call_bias confirmed 时用对应 Call 替换已被证伪的同层 Put；每条 2-3 行)，每条都要把账算给他看：",
            "- 墙位价 vs 先手挡价的取舍：墙位价便宜但常在墙前几点反转吃不到，先手挡成交率高；预估价已含触达前的"
            "时间衰减与 vol 斜率(BS 重定价)，比现价低不是便宜，是时间价值正常流失；",
            "- 赔率账：触达概率、到位预估价、现价放一起，这笔单赌的是一次多大概率的什么事，赔付幅度配不配得上这个概率；",
            "- execution_quote_status=executable 时才可给条件价；range_only 只能报告早/基准/晚触的范围和门控原因，不得给限价；",
            "- underlier_triggered_limit 必须先由 trigger_coordinate 指定的同坐标价格触及 target，再用届时实时 mid/IV 重算并提交限价；",
            "- 禁止把 limit_aggressive/limit_conservative 写成现在可预挂。预估高于现价会立即成交，预估低于现价也可能被 theta 提前打成。",
            "",
            "最后 2-3 行 if/then：开盘前参考价/ES 走到哪些具体位置，哪张单赔率变差该撤或改价，哪个剧本作废——这就是这张图的证伪条件。",
            "es_volume 可用且 label 非 no_baseline/session_reset 时，读量价事件(event_id)而不是只读放量/缩量："
            "字段含 direction(涨跌)、location(贴墙/flip/中间/破位侧)、sequence、break_outcome(holds/reclaimed/pending)、play_hints。"
            "用法按 play 对号入座——put wall 反弹想看 elevated_sell_into_support 后出现 quiet_reclaim_after_sell_test；"
            "flip 破位 put 想看 elevated_break_holds / quiet_breakdown_holds，最怕 break_reclaimed；"
            "call wall fade 想看 elevated_buy_into_resistance 后滞涨，最怕 quiet 站稳在墙上方。"
            "quiet_mid_range / elevated_mid_range 都是半路，不追单。",
            "hl_volume(HL SP500 永续，24/7 薄代理)只当次级证据：与 ES 同向加一分确认，分歧提示 crypto 侧先动或噪声；"
            "aggressor_buy_ratio/book_imbalance 是 ES 没有的方向色彩；ES 停盘/周末时它是唯一量价源，但绝不单独确认破位。",
            "每张单的 touch_eta_minutes 是按布朗缩放估的到位耗时：给出时效纪律——约 2 倍该时间价格还没来，"
            "赔率已被 theta 吃掉，写明大约几点(北京)前不来就撤单。",
            "session_phase 是搭档的时钟：这张图会跨欧盘、美盘数据小时和开盘使用，建议要写清哪些单是欧盘就能成交的埋伏、"
            "哪些要等美盘数据落地校准后才算数；不许把『等开盘』当默认建议。",
            "day_move.em_used_fraction ≥ 0.7 时点明：日内已走完预期波幅的多少，顺方向追单赔率差；挂单纪律是等价格来找你，不去半路追它。",
            "previous_push 是上一条推送正文；关键位相对它有实质变化就在定调处说『剧本有变』并指出哪张单要改，没变化不必提。",
            "previous_push:" + previous_push_json(previous_push),
            "JSON:" + json.dumps(writer_payload, ensure_ascii=False, separators=(",", ":")),
            "模板:" + template,
        )
    )


def _level_probs_line(payload: dict[str, Any]) -> str:
    by_play = _candidate_by_play(payload)
    parts: list[str] = []
    for play in PLAY_ORDER:
        candidate = by_play.get(play)
        if candidate is None:
            continue
        parts.append(
            f"{candidate.get('level_label') or '-'} 触达≈{_fmt_prob(candidate.get('prob_touch'))}"
        )
    return "; ".join(parts) if parts else "-"


def _compact_level_line(payload: dict[str, Any]) -> str:
    decision = payload.get("level_decision")
    if not isinstance(decision, dict):
        return f"候选 {_level_probs_line(payload)}"
    levels = decision.get("levels") if isinstance(decision.get("levels"), dict) else {}
    return (
        f"Put {_dash(levels.get('put_wall'))}｜"
        f"Flip {_dash(levels.get('flip_low'))}–{_dash(levels.get('flip_high'))}｜"
        f"Call {_dash(levels.get('call_wall'))}"
    )


def _compact_decision_line(payload: dict[str, Any]) -> str | None:
    decision = payload.get("level_decision")
    if not isinstance(decision, dict):
        return None
    phase = str(decision.get("phase") or "far").upper()
    level = finite_float(decision.get("level"))
    spot = finite_float(decision.get("spot"))
    if spot is None and isinstance(payload.get("underlier"), dict):
        spot = finite_float(payload["underlier"].get("price"))
    if spot is not None and level is not None:
        side = "高" if spot >= level else "低"
        distance = f"｜现价{side}{abs(spot - level):.1f}点"
    else:
        distance = ""
    phase_label, guidance = {
        "FAR": ("远离", "未进入触发区"),
        "APPROACHING": ("接近", "等待测试"),
        "TESTING": ("测试", "等待突破或拒绝"),
        "BREAK_PENDING": ("待确认突破", "等待持续确认"),
        "REJECT_PENDING": ("待确认拒绝", "等待持续确认"),
        "ACCEPTED": ("突破接受", "等待回踩"),
        "REJECTED": ("拒绝接受", "等待回踩"),
        "RETEST": ("回踩", "等待最终确认"),
        "CONFIRMED": ("已确认", "路径有效"),
        "INVALIDATED": ("已失效", "等待重置"),
        "EXPIRED": ("已过期", "系统自动重建事件"),
    }.get(phase, ("观察", "继续监控"))
    kind = {
        "put_wall": "Put Wall",
        "flip_low": "Flip下沿",
        "flip_high": "Flip上沿",
        "call_wall": "Call Wall",
    }.get(str(decision.get("level_kind") or ""), str(decision.get("level_kind") or "-"))
    return f"状态  {phase}（{phase_label}）｜{kind} {_dash(level)}{distance}｜{guidance}"


def _compact_breakout_filter_line(payload: dict[str, Any]) -> str | None:
    value = payload.get("breakout_filter")
    if not isinstance(value, dict):
        return None
    verdict = str(value.get("verdict") or "not_applicable")
    if verdict == "not_applicable":
        return None
    label = {
        "blocked": "拦截",
        "pending": "待确认",
        "supported": "通过",
        "unavailable": "不可用",
    }.get(verdict, verdict)
    local_share = finite_float(value.get("local_abs_gex_share"))
    local_text = f"｜附近GEX {local_share:.0%}" if local_share is not None else ""
    dex_text = "｜OI/成交DEX背离" if value.get("oi_volume_dex_divergent") is True else ""
    return (
        f"突破过滤  {label}｜动能 {_dash(value.get('impulse_score'))} / "
        f"阻力 {_dash(value.get('barrier_score'))}{local_text}{dex_text}"
    )


def _compact_price_line(payload: dict[str, Any]) -> str:
    underlier = payload.get("underlier") if isinstance(payload.get("underlier"), dict) else {}
    day_move = payload.get("day_move") if isinstance(payload.get("day_move"), dict) else {}
    points = finite_float(day_move.get("points"))
    em_used = finite_float(day_move.get("em_used_fraction"))
    change = f"{points:+.1f}" if points is not None else "-"
    used = f"{em_used:.0%}" if em_used is not None else "-"
    return (
        f"价格  SPX {_dash(underlier.get('price'))}｜ES {_dash(payload.get('es_last'))}｜"
        f"昨收 {change}｜EM已用 {used}"
    )


def _compact_clock_line(phase: dict[str, Any]) -> str | None:
    parts: list[str] = []
    since_open = phase.get("minutes_since_us_open")
    to_open = phase.get("minutes_to_us_open")
    to_bed = phase.get("minutes_to_bedtime")
    if isinstance(since_open, int):
        parts.append(f"开盘后 {since_open} 分钟")
    elif isinstance(to_open, int):
        parts.append(f"距开盘 {to_open} 分钟")
    if isinstance(to_bed, int) and to_bed <= 180:
        parts.append(f"距收官 {to_bed} 分钟")
    return "时钟  " + "｜".join(parts) if parts else None


def _gamma_label(value: Any) -> str:
    return {
        "positive_gamma_pin": "正Gamma",
        "negative_gamma_expansion": "负Gamma",
        "zero_gamma_transition": "ZeroGamma过渡",
    }.get(str(value or ""), str(value or "-"))


def _compact_oi_line(payload: dict[str, Any]) -> str | None:
    value = payload.get("max_pain")
    if not isinstance(value, dict):
        return None
    quality = {"ok": "正常", "degraded": "降级", "failed": "不可用"}.get(
        str(value.get("quality") or ""),
        str(value.get("quality") or "-"),
    )
    return (
        f"OI    Max Pain {_dash(value.get('settlement_strike'))}｜"
        f"Call峰 {_dash(value.get('call_oi_peak_strike'))}"
        f"（{int(value.get('call_oi_peak') or 0):,}）｜"
        f"Put峰 {_dash(value.get('put_oi_peak_strike'))}"
        f"（{int(value.get('put_oi_peak') or 0):,}）｜"
        f"{int(value.get('oi_strike_count') or 0)}档 {quality}"
    )


def _compact_flow_line(payload: dict[str, Any]) -> str | None:
    market = payload.get("minute_market_frame")
    if not isinstance(market, dict):
        return _globex_trend_line(payload)
    es = market.get("es") if isinstance(market.get("es"), dict) else {}
    volume = market.get("volume") if isinstance(market.get("volume"), dict) else {}
    cross = market.get("cross_asset") if isinstance(market.get("cross_asset"), dict) else {}
    alignment = {
        "price_volume_aligned": "量价同向",
        "price_volume_divergent": "量价背离",
    }.get(str(volume.get("price_volume_alignment_5m") or ""), "量价-")
    confirmation = {
        "confirmed": "同向",
        "divergent": "背离",
    }.get(str(cross.get("es_spy_direction_confirmation_15m") or ""), "-")
    return (
        f"ES确认  15m {_dash(es.get('return_15m_points'))}｜"
        f"60m {_dash(es.get('return_60m_points'))}｜"
        f"VWAP {_dash(es.get('vwap_distance_points'))}｜{alignment}｜ES/SPY {confirmation}"
    )


def _compact_option_line(payload: dict[str, Any]) -> str | None:
    options = payload.get("option_structure_frame")
    vol = payload.get("vol_context") if isinstance(payload.get("vol_context"), dict) else {}
    parts = [
        f"VIX1D/VIX {_ratio(vol.get('vix1d'), vol.get('vix'))}",
        f"SKEW {_dash(vol.get('skew'))}",
    ]
    if isinstance(options, dict):
        l1 = options.get("l1") if isinstance(options.get("l1"), dict) else {}
        parts.append(f"L1流动性 {_l1_liquidity_text(l1)}")
    return "波动  " + "｜".join(parts)


def _ratio(numerator: Any, denominator: Any) -> str:
    top = finite_float(numerator)
    bottom = finite_float(denominator)
    return f"{top / bottom:.2f}" if top is not None and bottom else "-"


def _compact_candidate_lines(payload: dict[str, Any], *, limit: int = 2) -> list[str]:
    candidates = [
        item for item in payload.get("candidates") or [] if isinstance(item, dict)
    ]
    spot = finite_float(
        (payload.get("underlier") or {}).get("price")
        if isinstance(payload.get("underlier"), dict)
        else None
    )
    if spot is None:
        return []

    support_calls = [
        item
        for item in candidates
        if item.get("right") == "C" and (finite_float(item.get("level")) or spot + 1) <= spot
    ]
    resistance_puts = [
        item
        for item in candidates
        if item.get("right") == "P" and (finite_float(item.get("level")) or spot - 1) >= spot
    ]
    selected: list[dict[str, Any]] = []
    for group in (support_calls, resistance_puts):
        if group:
            selected.append(
                min(group, key=lambda item: abs((finite_float(item.get("level")) or spot) - spot))
            )
    if len(selected) < limit:
        for item in sorted(
            candidates,
            key=lambda row: abs((finite_float(row.get("level")) or spot) - spot),
        ):
            if item not in selected:
                selected.append(item)
            if len(selected) >= limit:
                break

    labels = {
        "put_wall_bounce_call": "支撑反弹",
        "flip_breakdown_put": "Flip跌破",
        "call_wall_fade_put": "冲墙回落",
        "flip_reclaim_call": "Flip收复",
        "call_wall_breakout_call": "Call墙突破",
    }
    lines: list[str] = []
    for index, item in enumerate(selected[:limit], start=1):
        if item.get("strike") is None or item.get("right") not in {"C", "P"}:
            continue
        low = item.get("projection_range_low")
        high = item.get("projection_range_high")
        if low is None:
            low = item.get("projected_mid")
        if high is None:
            high = item.get("projected_mid")
        contract = f"{_dash(item.get('strike'))}{item.get('right') or ''}"
        if item.get("execution_quote_status") == "range_only":
            price_text = f"仅情景 {_dash(low)}–{_dash(high)}"
        else:
            price_text = f"参考 {_dash(low)}–{_dash(high)}"
        play_label = labels.get(
            str(item.get("play") or ""),
            "Call候选" if item.get("right") == "C" else "Put候选",
        )
        lines.append(
            f"计划{index}·{play_label}  SPX {_dash(item.get('level'))}触发｜"
            f"SPXW {contract}｜触达 {_fmt_prob(item.get('prob_touch'))}｜{price_text}"
        )
    return lines


def render_status_template(
    payload: dict[str, Any],
    changes: list[str],
    now_utc: datetime,
) -> str:
    if payload.get("research_only") is True:
        return render_research_only_template(payload, title="市场状态")
    beijing = now_utc.astimezone(SHANGHAI_TZ)
    phase = _session_phase_of(payload, now_utc)

    expiry = str(payload.get("expiry") or "-")
    expiry_text = f"{expiry[4:6]}-{expiry[6:8]}" if len(expiry) == 8 else expiry
    lines = [
        f"【SPX 15m｜{beijing.strftime('%H:%M')}｜0DTE {expiry_text}｜{phase.get('name_cn')}】",
        *([line] if (line := _compact_clock_line(phase)) else []),
        _compact_price_line(payload),
        (
            f"结构  {_gamma_label(payload.get('gamma_state'))}｜"
            f"{_compact_level_line(payload)}｜ZG {_dash(payload.get('zero_gamma'))}｜"
            f"EM ±{_dash(payload.get('expected_move_points'))}"
        ),
        *([line] if (line := _compact_oi_line(payload)) else []),
        *([line] if (line := _compact_decision_line(payload)) else []),
        *([line] if (line := _compact_breakout_filter_line(payload)) else []),
        "",
        *([line] if (line := _compact_flow_line(payload)) else []),
        *(
            [line]
            if (line := _es_volume_line(payload)) and _compact_flow_line(payload) is None
            else []
        ),
        *([line] if (line := _compact_option_line(payload)) else []),
        "",
        "【条件计划｜标的触发后执行】",
        *_compact_candidate_lines(payload),
        "执行  触位后按实时 mid/IV 重算｜当前不可预挂",
    ]
    if changes:
        lines.append(f"变化  {'；'.join(changes)}")
    else:
        lines.append("变化  关键位无实质变化")

    warnings = payload.get("warnings")
    if isinstance(warnings, list) and warnings:
        lines.append(f"数据  {'；'.join(str(item) for item in warnings)}")
    return "\n".join(lines)


def _detail_candidate_lines(payload: dict[str, Any]) -> list[str]:
    full_lines = render_template(payload).splitlines()
    start = next(
        (index for index, line in enumerate(full_lines) if re.match(r"^1\) \[地图候选\]", line)),
        None,
    )
    if start is None:
        return []
    rendered: list[str] = []
    detail_labels = {
        "触达概率": "触达与定价",
        "BS审计": "BS 审计",
        "条件执行": "条件执行",
        "时效": "时效",
        "先手挡": "先手挡",
    }
    for raw in full_lines[start:]:
        line = raw.strip()
        candidate = re.match(r"^(\d+)\) \[地图候选\] (.+)$", line)
        if candidate:
            rendered.append(f"### 计划 {candidate.group(1)}")
            rendered.append(f"**{candidate.group(2)}**")
            continue
        if line.startswith("注:"):
            rendered.append(f"> **说明**　{line.removeprefix('注:').strip()}")
            continue
        label = next((key for key in detail_labels if line.startswith(key)), None)
        if label is not None:
            content = line.removeprefix(label).removeprefix(":").strip()
            rendered.append(f"- **{detail_labels[label]}**　{content}")
        elif line:
            rendered.append(line)
    return rendered


def _detail_ladder_lines(payload: dict[str, Any]) -> list[str]:
    rendered: list[str] = []
    for raw in _wall_ladder_lines(payload):
        line = raw.strip()
        if line.endswith(":"):
            rendered.append(f"**{line.removesuffix(':')}**")
        elif line:
            rendered.append(f"- {line}")
    return rendered


def render_feishu_status_detail_template(
    payload: dict[str, Any],
    changes: list[str],
    now_utc: datetime,
) -> str:
    """Full-fidelity status report arranged as Feishu-readable sections."""
    if payload.get("research_only") is True:
        return render_status_template(payload, changes, now_utc)

    compact_blocks = render_status_template(payload, changes, now_utc).split("\n\n")
    overview = compact_blocks[0].splitlines()
    header = overview.pop(0)
    phase = _session_phase_of(payload, now_utc)
    if traits := str(phase.get("traits") or "").strip():
        overview.append(f"**时段提示**　{traits}")
    change_line = next(
        (
            line
            for block in compact_blocks
            for line in block.splitlines()
            if line.startswith("变化  ")
        ),
        None,
    )
    if change_line:
        overview.append(change_line)

    greek_and_vol = [
        line
        for line in (
            _greeks_reference_line(payload),
            _compact_option_line(payload),
        )
        if line
    ]
    vol = payload.get("vol_context") if isinstance(payload.get("vol_context"), dict) else {}
    greek_and_vol.append(
        f"Vol全景: VIX {_dash(vol.get('vix'))}｜VIX1D {_dash(vol.get('vix1d'))}｜"
        f"VVIX {_dash(vol.get('vvix'))}｜SKEW {_dash(vol.get('skew'))}"
    )

    market_confirmation = [
        line
        for line in (
            _globex_trend_line(payload),
            *_market_feature_lines(payload),
            _es_volume_line(payload),
            _hl_volume_line(payload),
        )
        if line
    ]

    full_lines = render_template(payload).splitlines()
    context_prefixes = (
        "SPX 代理:",
        "SPX 结构:",
        "ES 等价值位:",
        "位置判断:",
        "关键位决策:",
        "结构口径:",
    )
    key_level_context = [line for line in full_lines if line.startswith(context_prefixes)]
    density = _rn_density_line(payload)

    sections: list[tuple[str, list[str]]] = [
        ("市场概览", overview),
        ("Greeks 与波动", greek_and_vol),
        ("ES 与跨资产确认", market_confirmation),
        ("关键位状态", key_level_context),
        ("墙位阶梯", _detail_ladder_lines(payload)),
        ("风险中性分布", [density] if density else []),
        ("条件计划与 BS 审计", _detail_candidate_lines(payload)),
    ]
    blocks = [header]
    blocks.extend(
        f"## {title}\n" + "\n".join(lines)
        for title, lines in sections
        if lines
    )
    return "\n\n".join(blocks)


def build_status_prompt(
    payload: dict[str, Any],
    template: str,
    previous_push: dict[str, Any] | None = None,
) -> str:
    writer_payload = _status_writer_payload(payload)
    if payload.get("research_only") is True:
        return "\n".join(
            (
                "这是每 15 分钟一次的 SPX 市场状态。非 RTH 并非无行情：ES-basis SPX 代理是关键位状态机的主参考，ES 是连续锚，Hyperliquid 是次级交叉验证。",
                "请像交易接班便签一样写，不要复述 JSON。模板已经包含经过舍入的 ES 路径、VWAP、量价、联动和墙位；"
                "用这些可见事实定义当前偏多/偏空/中性，并判断代理 SPX 在 Put Wall、Flip、Call Wall 的阶段。",
                "必须给一个主情景，以及升级到 breakout/fade 所需的具体确认条件和证伪条件。结构若来自上一 RTH，要明确结构日期与质量，不得把旧 OI 当成今天新链。",
                "夜盘可做方向与关键位准备，不许写『等开盘再说』。但现金 SPX 与新 0DTE 链不可用时，不得编造 Greeks、期权模型价、限价、触达概率、ETA 或直接下单指令。",
                "SPX levels 与 es_equivalent_levels 是两个坐标系。谈 ES 阈值只能逐字引用 es_equivalent_levels，严禁把 SPX strike 当 ES 价格，也不得自行换算。",
                "不得推断输入中没有的历史触碰次数、墙是否弃守、dealer 行为或 gamma 燃料。避免比喻，用结构、价格和条件直接表达。",
                "输出中文且总共不超过 16 行，首行逐字保留模板标题；使用 ## 结论、## 位置与路径、## 双向条件、## 数据限制四段，"
                "每段最多 3 条短句。只允许引用模板中已经出现的数字，禁止输出 JSON 字段名、高精度小数或补算新数字。"
                "末行说明下一条最值得盯的代理价格及改变判断的阈值。",
                "previous_push:" + previous_push_json(previous_push),
                "JSON:" + json.dumps(writer_payload, ensure_ascii=False, separators=(",", ":")),
                "模板:" + template,
            )
        )
    return "\n".join(
        (
            "这是每 15 分钟一次的 SPX 决策摘要，不是完整研究报告。",
            "先读取 regime_decision 与 breakout_filter：blocked=突破被结构阻力拦截，pending=证据不足，"
            "supported 且 actionable=true=突破过滤通过。不得绕过代码 verdict，也不得把 DEX proxy 写成 dealer 实仓。",
            "输出中文，第一行逐字保留模板标题；先给剧本维持/有变，再给当前位置和状态机结论。",
            "只保留会改变当前决策的内容：时段、SPX/ES、wall/flip、状态机、ES 路径与量价、"
            "Max Pain/OI 或波动率中最重要的一项、最多两个条件候选、相对上次变化和下一确认/证伪阈值。",
            "禁止复述完整 Greeks、完整墙位阶梯、B-L 全分布、HL 全指标或 JSON 字段；它们留在后台审计。",
            "保持模板的分段、空行和字段顺序。每个候选只占一行，逐字保留合约、SPX 触发位、触达概率和触位区间；"
            "统一执行行必须保留『当前不可预挂』。",
            "候选必须先由 SPX 点位触发，再按实时 mid/IV 重算；不得把情景价写成当前挂单价。",
            "框架仍是 observe_only；仓位方向未知时，负 gamma 不等于下跌，不得据此改变候选方向。",
            "EXPIRED 表示系统自动重建事件，不得写成等待价格离开或停止监控。",
            "没有实质变化时直接写『剧本维持』，不要为了填满行数重复指标。",
            "previous_push:" + previous_push_json(previous_push),
            "JSON:" + json.dumps(writer_payload, ensure_ascii=False, separators=(",", ":")),
            "模板:" + template,
        )
    )


def _status_writer_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Keep status prompts bounded so the CLI behaves like a one-shot LLM API."""

    keys = (
        "beijing_time",
        "expiry",
        "session_phase",
        "research_only",
        "analysis_mode",
        "pricing_reference",
        "level_decision",
        "regime_decision",
        "breakout_filter",
        "warnings",
    )
    compact = {key: payload.get(key) for key in keys if key in payload}
    candidates = payload.get("candidates")
    if isinstance(candidates, list):
        candidate_keys = (
            "play",
            "level_label",
            "level",
            "strike",
            "right",
            "prob_touch",
            "projection_range_low",
            "projection_range_high",
            "execution_quote_status",
        )
        compact["candidates"] = [
            {key: item.get(key) for key in candidate_keys if key in item}
            for item in candidates[:2]
            if isinstance(item, dict)
        ]
    return compact
