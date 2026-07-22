"""LLM prompt builders for order-map and status pushes."""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

from spx_spark.analytics.options.pricing import finite_float
from spx_spark.application.order_map import guidance as guidance_module
from spx_spark.application.order_map.call_spread_shadow import (
    compact_skew_spread_shadow_line,
    skew_spread_shadow_detail_lines,
)
from spx_spark.application.order_map.models import PLAY_ORDER, SHANGHAI_TZ
from spx_spark.application.order_map.exposure_presentation import exposure_strike_lines
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
    _wall_rank_persistence_line,
    render_research_only_template,
    render_template,
    underlier_source_label,
)
from spx_spark.application.order_map.state import _session_phase_of
from spx_spark.application.order_map.strike_coverage_presentation import (
    strike_price_coverage_line as _strike_price_coverage_line,
)
from spx_spark.application.order_map.writer_validation import (
    actionable_writer_output_valid as actionable_writer_output_valid,
    globex_writer_output_valid as globex_writer_output_valid,
)
from spx_spark.application.order_map.wall_ladder_presentation import (
    detail_ladder_lines as _detail_ladder_lines,
)
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


def build_order_prompt(
    payload: dict[str, Any],
    template: str,
    previous_push: dict[str, Any] | None = None,
) -> str:
    writer_payload = {key: value for key, value in payload.items() if not key.startswith("_")}
    if "plan_candidates" in writer_payload:
        writer_payload.pop("candidates", None)
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
            "框架口径：Micopedia/Steven（regime→map→flow→trigger→expression→exit）；"
            "TradeReady 仅表示已通过代码决策门控、可供操作员执行，自动下单仍关闭；"
            "GEX/*_proxy 是结构代理；Hyperliquid 只作弱次级证据，不作 SPX 锚。",
            "signed_gex_proxy 与 option_structure_frame.exposure 来自 SPXW 0DTE 期权链，不是 ES 期货自身的 GEX/DEX。"
            "读取 net/abs GEX、OI/成交量加权 net/abs DEX proxy、key_strikes 8档地图及 coverage/warnings；只在非 null 且质量足够时用于确认或反驳突破，"
            "OI 与成交量口径方向背离时必须优先考虑假突破，不得把 proxy 写成 dealer 实仓。",
            "regime_decision 与 breakout_filter 是代码生成的确定性决策层，不得自行改判。"
            "breakout_filter.verdict=blocked 时删除/降级同事件 breakout 候选并优先说明假突破风险；pending 时不得写突破成立；"
            "supported 且 actionable=true 才能把 CONFIRMED breakout 写成可执行候选。"
            "说明时引用 impulse_score、barrier_score、local_abs_gex_share、next_wall_distance_points 和 OI/Volume DEX 分歧中真正改变判断的字段。",
            guidance_module.SESSION_EPISODE_PROMPT_RULE,
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
            "spxw_0dte_greeks_reference 是严格当日到期的情景层；position_sign/direction=unknown 时负 gamma 不等于下跌。"
            "greek_decision.direction_authority=none，永远不得用 Greeks 新造多空方向；mode=decision_grade 时只允许按代码结果解释同方向合约置信度、"
            "等待成本和退出，mode=explanation_only 时不得改变排序、限价或动作。",
            "conditional_call_bias 只有 status=confirmed 才有效，它来自 5 秒 SPX/ES 价格路径对冻结 flip/旧 call wall 的确认，"
            "不是 Gamma 猜方向；confirmed 时优先讲对应 call 的回踩位与失效线，watch/neutral 不新增动作。",
            "",
            "然后逐条处理 plan_candidates（最多 1 条）；只有这里的条目可称为计划。"
            "order_style=live_nbbo_limit 表示 TradeReady：必须逐字保留 NBBO、买入上限、失效位、目标和意图到期时间，"
            "不得写『当前不可预挂』。observation_candidates 最多 1 条且是唯一主观察策略，不得补写执行或挂单动作；"
            "opposing_invalidation 只写成主策略失效条件，禁止平铺成第二套反向计划。",
            "- 墙位价 vs 先手挡价的取舍：墙位价便宜但常在墙前几点反转吃不到，先手挡成交率高；预估价已含触达前的"
            "时间衰减与 vol 斜率(BS 重定价)，比现价低不是便宜，是时间价值正常流失；",
            "- 赔率账：触达概率、到位预估价、现价放一起，这笔单赌的是一次多大概率的什么事，赔付幅度配不配得上这个概率；",
            "- execution_quote_status=executable 时才可给条件价；range_only 只能报告早/基准/晚触的范围和门控原因，不得给限价；",
            "- underlier_triggered_limit 必须先由 trigger_coordinate 指定的同坐标价格触及 target，再用届时实时 mid/IV 重算并提交限价；",
            "- 仅对 underlier_triggered_limit，禁止把 limit_aggressive/limit_conservative 写成现在可预挂。"
            "live_nbbo_limit 是触发已确认后冻结的实时限价意图，不得改写成未触发情景价；",
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
            "day_move.em_used_fraction ≥ 0.7 时点明：从当日 GTH 开始已走完预期波幅的多少，顺方向追单赔率差；挂单纪律是等价格来找你，不去半路追它。",
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
    by_play = _candidate_by_play(payload)
    decision = payload.get("level_decision")
    if not isinstance(decision, dict):
        return f"候选 {_level_probs_line(payload)}"
    frozen_levels = decision.get("levels") if isinstance(decision.get("levels"), dict) else {}
    flip_zone = payload.get("flip_zone")
    live_flip = flip_zone if isinstance(flip_zone, list) and len(flip_zone) >= 2 else None

    if frozen_levels:
        put_wall = frozen_levels.get("put_wall")
        call_wall = frozen_levels.get("call_wall")
        flip_low = frozen_levels.get("flip_low")
        flip_high = frozen_levels.get("flip_high")
        return (
            f"Put {_dash(put_wall)}　Flip {_dash(flip_low)}–{_dash(flip_high)}　"
            f"Call {_dash(call_wall)}"
        )

    def candidate_level(play: str, fallback: str) -> object:
        candidate = by_play.get(play)
        if isinstance(candidate, dict) and candidate.get("level") is not None:
            return candidate.get("level")
        return frozen_levels.get(fallback)

    put_wall = candidate_level("put_wall_bounce_call", "put_wall")
    call_wall = candidate_level("call_wall_fade_put", "call_wall")
    flip_low = live_flip[0] if live_flip is not None else frozen_levels.get("flip_low")
    flip_high = live_flip[1] if live_flip is not None else frozen_levels.get("flip_high")
    if all(value is None for value in (put_wall, flip_low, flip_high, call_wall)):
        return f"候选 {_level_probs_line(payload)}"
    return (
        f"Put {_dash(put_wall)}　Flip {_dash(flip_low)}–{_dash(flip_high)}　Call {_dash(call_wall)}"
    )


def _compact_structure_candidate_line(payload: dict[str, Any]) -> str | None:
    decision = payload.get("level_decision")
    if not isinstance(decision, dict) or decision.get("structure_change_pending") is not True:
        return None
    candidate = decision.get("structure_candidate")
    if not isinstance(candidate, dict):
        return None
    levels = candidate.get("levels")
    if not isinstance(levels, dict) or not levels:
        return None
    count = int(finite_float(candidate.get("confirmation_count")) or 0)
    required = int(finite_float(candidate.get("required_confirmations")) or 0)
    progress = f"{count}/{required}" if required > 0 else "确认中"
    return (
        "结构更新  新链 Put "
        f"{_dash(levels.get('put_wall'))}　Flip {_dash(levels.get('flip_low'))}–"
        f"{_dash(levels.get('flip_high'))}　Call {_dash(levels.get('call_wall'))}　"
        f"稳定确认 {progress}，旧结构暂停"
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
        distance = f"　现价{side}{abs(spot - level):.1f}点"
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
    return f"状态  {phase}（{phase_label}）　事件位 {kind} {_dash(level)}{distance}　{guidance}"


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
    local_text = f"　附近GEX {local_share:.0%}" if local_share is not None else ""
    dex_text = "　OI/成交DEX背离" if value.get("oi_volume_dex_divergent") is True else ""
    return (
        f"突破过滤  {label}　动能 {_dash(value.get('impulse_score'))} / "
        f"阻力 {_dash(value.get('barrier_score'))}{local_text}{dex_text}"
    )


def _compact_price_line(payload: dict[str, Any]) -> str:
    underlier = payload.get("underlier") if isinstance(payload.get("underlier"), dict) else {}
    day_move = payload.get("day_move") if isinstance(payload.get("day_move"), dict) else {}
    points = finite_float(day_move.get("points"))
    em_used = finite_float(day_move.get("em_used_fraction"))
    change = f"{points:+.1f}" if points is not None else "-"
    used = f"{em_used:.0%}" if em_used is not None else "-"
    source = underlier.get("source")
    spx_text = _dash(underlier.get("price"))
    if source and source != "index:SPX":
        # GTH pushes price off the option chain, not the frozen cash print;
        # say so, otherwise the SPX/ES gap reads as a data bug.
        spx_text = f"{spx_text}({underlier_source_label(source)})"
    return (
        f"价格  SPX {spx_text}　ES {_dash(payload.get('es_last'))}　"
        f"较昨收 {change}　GTH EM已用 {used}"
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
    return "时钟  " + "　".join(parts) if parts else None


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
        f"OI    Max Pain {_dash(value.get('settlement_strike'))}　"
        f"Call峰 {_dash(value.get('call_oi_peak_strike'))}"
        f"（{int(value.get('call_oi_peak') or 0):,}）　"
        f"Put峰 {_dash(value.get('put_oi_peak_strike'))}"
        f"（{int(value.get('put_oi_peak') or 0):,}）　"
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
        f"ES确认  15m {_dash(es.get('return_15m_points'))}　"
        f"60m {_dash(es.get('return_60m_points'))}　"
        f"VWAP {_dash(es.get('vwap_distance_points'))}　{alignment}　ES/SPY {confirmation}"
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
        diagnostics = l1.get("diagnostics") if isinstance(l1.get("diagnostics"), dict) else {}
        provider_counts = diagnostics.get("selected_provider_counts")
        provider_text = ""
        if isinstance(provider_counts, dict):
            providers = [
                f"{str(provider).upper()} {int(count)}"
                for provider, count in sorted(provider_counts.items())
                if isinstance(count, (int, float)) and count > 0
            ]
            if providers:
                provider_text = f"（{' + '.join(providers)}）"
        parts.append(f"L1流动性 {_l1_liquidity_text(l1)}{provider_text}")
    return "波动  " + "　".join(parts)


def _compact_guidance_lines(payload: dict[str, Any]) -> list[str]:
    guidance = guidance_module.build_decision_guidance(payload)
    scores = ""
    if guidance.trend_score is not None and guidance.mean_reversion_score is not None:
        scores = f"（趋势 {guidance.trend_score:g} / 回归 {guidance.mean_reversion_score:g}）"
    gate = "可执行" if guidance.action.value == "trade_ready" else "未通过执行门控"
    return [
        f"判断  {guidance.bias}{scores}　{gate}",
        f"动作  {guidance.action_text}",
        f"确认  {guidance.trigger_text}",
        f"证伪  {guidance.invalidation_text}",
    ]


def _ratio(numerator: Any, denominator: Any) -> str:
    top = finite_float(numerator)
    bottom = finite_float(denominator)
    return f"{top / bottom:.2f}" if top is not None and bottom else "-"


def _compact_candidate_lines(payload: dict[str, Any], *, limit: int = 2) -> list[str]:
    classified = "plan_candidates" in payload
    plans = [item for item in payload.get("plan_candidates") or [] if isinstance(item, dict)]
    candidates = plans or [
        item
        for item in (
            payload.get("observation_candidates") if classified else payload.get("candidates")
        )
        or []
        if isinstance(item, dict)
    ]
    is_plan = bool(plans) or not classified
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
        "level_breakout_call": "突破确认",
        "level_breakout_put": "跌破确认",
        "level_fade_call": "下破拒绝",
        "level_fade_put": "上破拒绝",
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
        live_intent = is_plan and item.get("order_style") == "live_nbbo_limit"
        if live_intent:
            price_text = (
                f"实时 {_dash(item.get('decision_bid'))}/{_dash(item.get('decision_ask'))}　"
                f"入场≤{_dash(item.get('limit_aggressive'))}"
            )
        elif not is_plan:
            price_text = f"情景 {_dash(low)}–{_dash(high)}"
        elif item.get("execution_quote_status") == "range_only":
            price_text = f"仅情景 {_dash(low)}–{_dash(high)}"
        else:
            price_text = f"参考 {_dash(low)}–{_dash(high)}"
        play_label = labels.get(
            str(item.get("play") or ""),
            "Call候选" if item.get("right") == "C" else "Put候选",
        )
        prefix = "计划" if is_plan else "观察"
        trigger_text = "已确认" if live_intent else "触发"
        probability_text = "" if live_intent else f"　触达 {_fmt_prob(item.get('prob_touch'))}"
        lines.append(
            f"{prefix}{index} · {play_label}  SPX {_dash(item.get('level'))}{trigger_text} → "
            f"SPXW {contract}{probability_text}　{price_text}"
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
    classified_candidates = "plan_candidates" in payload
    has_plan = bool(payload.get("plan_candidates")) if classified_candidates else True
    candidate_lines = _compact_candidate_lines(payload)
    if has_plan and candidate_lines:
        plans = [item for item in payload.get("plan_candidates") or [] if isinstance(item, dict)]
        live_plan = (
            plans[0]
            if len(plans) == 1 and plans[0].get("order_style") == "live_nbbo_limit"
            else None
        )
        candidate_section = [
            (
                "【条件计划】决策门控已通过，标的触发后执行"
                if classified_candidates
                else "【条件计划】标的触发后执行"
            ),
            *candidate_lines,
            (
                f"风险  SPX {_dash(live_plan.get('invalidation_spx'))}失效　"
                f"目标 {_dash(live_plan.get('target_spx'))}　"
                f"意图至 {live_plan.get('intent_expires_at') or '-'}"
                if live_plan is not None
                else "执行  触位后按实时 mid/IV 重算；当前不可预挂"
            ),
        ]
    elif candidate_lines:
        candidate_section = [
            "【观察情景】尚未通过决策门控",
            *candidate_lines,
            "说明  仅观察触位后的报价与方向确认；当前不是下单计划",
        ]
    else:
        candidate_section = []
    lines = [
        f"【SPX 15m · {beijing.strftime('%H:%M')} · 0DTE {expiry_text} · {phase.get('name_cn')}】",
        *_compact_guidance_lines(payload),
        "",
        *([line] if (line := _compact_clock_line(phase)) else []),
        _compact_price_line(payload),
        (
            f"结构  {_gamma_label(payload.get('gamma_state'))}　"
            f"{_compact_level_line(payload)}　ZG {_dash(payload.get('zero_gamma'))}　"
            f"EM ±{_dash(payload.get('expected_move_points'))}"
        ),
        *([line] if (line := _compact_structure_candidate_line(payload)) else []),
        *([line] if (line := _compact_oi_line(payload)) else []),
        *([line] if (line := _wall_rank_persistence_line(payload)) else []),
        *([line] if (line := _strike_price_coverage_line(payload)) else []),
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
        *([line] if (line := compact_skew_spread_shadow_line(payload)) else []),
        *(["", *candidate_section] if candidate_section else []),
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
        (
            index
            for index, line in enumerate(full_lines)
            if re.match(r"^1\) \[(?:地图候选|条件计划|观察情景)\]", line)
        ),
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
        candidate = re.match(r"^(\d+)\) \[(地图候选|条件计划|观察情景)\] (.+)$", line)
        if candidate:
            heading = "观察" if candidate.group(2) == "观察情景" else "计划"
            rendered.append(f"### {heading} {candidate.group(1)}")
            rendered.append(f"**{candidate.group(3)}**")
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
        f"Vol全景: VIX {_dash(vol.get('vix'))}　VIX1D {_dash(vol.get('vix1d'))}　"
        f"VVIX {_dash(vol.get('vvix'))}　SKEW {_dash(vol.get('skew'))}"
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
        ("0DTE 暴露地图", exposure_strike_lines(payload)),
        ("ES 与跨资产确认", market_confirmation),
        ("关键位状态", key_level_context),
        ("当前布局参考", _detail_ladder_lines(payload)),
        ("风险中性分布", [density] if density else []),
        ("Call / Put Skew Spread Shadow", skew_spread_shadow_detail_lines(payload)),
        (
            "条件计划与 BS 审计"
            if payload.get("plan_candidates") or "plan_candidates" not in payload
            else "观察情景与 BS 审计",
            _detail_candidate_lines(payload),
        ),
    ]
    blocks = [header]
    blocks.extend(f"## {title}\n" + "\n".join(lines) for title, lines in sections if lines)
    return "\n\n".join(blocks)


def render_feishu_delivery_text(
    payload: dict[str, Any],
    changes: list[str],
    now_utc: datetime,
    summary: str,
) -> str:
    """Place the LLM summary above deterministic full-fidelity Feishu sections."""

    detail = render_feishu_status_detail_template(payload, changes, now_utc)
    _header, separator, body = detail.partition("\n\n")
    if detail == summary:
        return summary
    if separator and body:
        blocks = [block for block in body.split("\n\n") if block]
        decision = payload.get("level_decision")
        phase = str(decision.get("phase") or "far") if isinstance(decision, dict) else "far"
        has_plan = bool(payload.get("plan_candidates"))
        active = phase in {
            "approaching",
            "testing",
            "break_pending",
            "reject_pending",
            "accepted",
            "rejected",
            "retest",
            "confirmed",
        }
        allowed_titles = (
            (
                "## 0DTE 暴露地图\n",
                "## 关键位状态\n",
                "## 当前布局参考\n",
                "## Call / Put Skew Spread Shadow\n",
                "## 条件计划与 BS 审计\n",
            )
            if has_plan
            else (
                "## 0DTE 暴露地图\n",
                "## 关键位状态\n",
                "## 当前布局参考\n",
                "## Call / Put Skew Spread Shadow\n",
            )
            if active
            else (
                "## 0DTE 暴露地图\n",
                "## 当前布局参考\n",
                "## Call / Put Skew Spread Shadow\n",
            )
        )
        detail_blocks = [
            block for block in blocks if any(block.startswith(title) for title in allowed_titles)
        ]
        if not detail_blocks:
            return summary
        return f"{summary}\n\n" + "\n\n".join(detail_blocks)
    return detail or summary


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
            "模板开头的判断/动作/确认/证伪是代码生成的当前操作结论，必须保留其语义，不能降级成指标罗列。",
            "先读取 regime_decision 与 breakout_filter：blocked=突破被结构阻力拦截，pending=证据不足，"
            "supported 且 actionable=true=突破过滤通过。不得绕过代码 verdict，也不得把 DEX proxy 写成 dealer 实仓。",
            "exposure_context 是 SPXW 0DTE 期权链推导的实时结构，不是 ES 期货自身的 GEX/DEX。"
            "net/abs GEX 用于判断结构净方向与集中度，OI/成交量加权 DEX proxy 用于检查突破方向是否得到 delta 暴露共振；"
            "key_strikes 是最多 8 个墙位/ATM/Flip/ZG 与暴露集中档，只引用其中真正改变当前判断的近端档位；"
            "OI 与成交两者背离时优先提示假突破风险。字段为 null、coverage 不足或 warnings 非空时必须明确降权，禁止补算或猜测。",
            "输出中文，第一行逐字保留模板标题；先给剧本维持/有变，再给当前位置和状态机结论。",
            "只保留会改变当前决策的内容：时段、SPX/ES、wall/flip、状态机、ES 路径与量价、"
            "Max Pain/OI 或波动率中最重要的一项、最多两个情景、相对上次变化和下一确认/证伪阈值。",
            "禁止复述完整 Greeks、完整墙位阶梯、B-L 全分布、HL 全指标或 JSON 字段；它们留在后台审计。",
            "保持模板的分段、空行和字段顺序。只有 plan_candidates 才能称为计划；"
            "observation_candidates 必须称为观察情景，禁止补写执行、开仓、挂单或追价动作。"
            "每个条目只占一行，并逐字保留模板中的执行字段。",
            "order_style=live_nbbo_limit 时，必须保留实时 NBBO、入场上限、失效位、目标和意图到期时间，"
            "不得写『当前不可预挂』；非实时条件情景才保留『当前不可预挂』并等 SPX 触发后重算。",
            "TradeReady 可供操作员执行，但自动下单仍关闭；仓位方向未知时，负 gamma 不等于下跌，不得据此改变候选方向。",
            "call_skew_spread_shadow 与 put_skew_spread_shadow 只能称为只读 Shadow：即使 status=candidate 也不是计划或订单，禁止补写拆腿执行；只能复述组合净借记、定义风险和门控边界。",
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
        "session_episode",
        "trade_candidate",
        "confirmed_gate",
        "call_skew_spread_shadow",
        "put_skew_spread_shadow",
        "warnings",
    )
    compact = {key: payload.get(key) for key in keys if key in payload}
    for shadow_key in ("call_skew_spread_shadow", "put_skew_spread_shadow"):
        shadow = compact.get(shadow_key)
        if not isinstance(shadow, dict):
            continue
        candidate = shadow.get("candidate")
        compact[shadow_key] = {
            key: shadow.get(key)
            for key in ("status", "reason", "automatic_ordering", "operator_action")
        }
        if isinstance(candidate, dict):
            compact[shadow_key]["candidate"] = {
                key: candidate.get(key)
                for key in (
                    "strategy",
                    "long",
                    "short",
                    "executable_debit",
                    "fair_debit",
                    "edge_points",
                    "iv_fit",
                    "defined_risk",
                    "execution",
                )
            }
    compact["decision_guidance"] = guidance_module.build_decision_guidance(payload).to_dict()
    signed_gex = payload.get("signed_gex_proxy")
    if isinstance(signed_gex, dict):
        compact["signed_gex_proxy"] = {
            key: signed_gex.get(key)
            for key in (
                "net_gex",
                "abs_gex",
                "net_gamma_ratio",
                "gamma_state",
                "weighting",
                "sign_method",
                "dealer_position_sign",
            )
        }
    strike_coverage = payload.get("strike_price_coverage")
    if isinstance(strike_coverage, dict):
        compact["strike_price_coverage"] = {
            key: strike_coverage.get(key)
            for key in (
                "expiry",
                "reference_price",
                "center_strike",
                "strike_step_points",
                "radius_strikes",
                "target_pair_count",
                "complete_pair_count",
                "core_complete_pair_count",
                "rotation_assisted_pair_count",
                "missing_call_count",
                "missing_put_count",
                "coverage_ratio",
                "coverage_confidence_95_low",
                "coverage_confidence_95_high",
                "pair_quote_age_p50_seconds",
                "pair_quote_age_p90_seconds",
                "pair_quote_age_max_seconds",
                "complete_min_strike",
                "complete_max_strike",
                "radius_points",
                "point_target_pair_count",
                "point_complete_pair_count",
                "point_coverage_ratio",
                "price_contract",
                "nbbo_interpolation",
                "smoothing_scope",
            )
        }
    exposure_context = _status_exposure_context(payload)
    if exposure_context:
        compact["exposure_context"] = exposure_context
    classified = "plan_candidates" in payload
    plans = payload.get("plan_candidates")
    observations = payload.get("observation_candidates")
    candidates = (
        plans
        if isinstance(plans, list) and plans
        else observations
        if classified and isinstance(observations, list)
        else payload.get("candidates")
    )
    if isinstance(candidates, list):
        candidate_keys = (
            "intent_id",
            "contract_id",
            "play",
            "level_label",
            "level",
            "strike",
            "right",
            "prob_touch",
            "projection_range_low",
            "projection_range_high",
            "execution_quote_status",
            "order_style",
            "decision_bid",
            "decision_ask",
            "limit_aggressive",
            "invalidation_spx",
            "target_spx",
            "intent_expires_at",
            "automatic_ordering",
        )
        key = (
            "plan_candidates"
            if isinstance(plans, list) and plans
            else ("observation_candidates" if classified else "candidates")
        )
        compact[key] = [
            {key: item.get(key) for key in candidate_keys if key in item}
            for item in candidates[:2]
            if isinstance(item, dict)
        ]
    if classified:
        compact["candidate_presentation"] = payload.get("candidate_presentation")
    return compact


def _status_exposure_context(payload: dict[str, Any]) -> dict[str, Any]:
    """Expose bounded SPXW-derived exposure facts to the 15-minute writer."""

    frame = payload.get("option_structure_frame")
    if not isinstance(frame, dict):
        return {}
    exposure = frame.get("exposure")
    if not isinstance(exposure, dict):
        return {}

    def aggregate(name: str) -> dict[str, Any] | None:
        value = exposure.get(name)
        if not isinstance(value, dict):
            return None
        keys = (
            "net_gex",
            "abs_gex",
            "net_gamma_ratio",
            "net_dex_proxy",
            "abs_dex_proxy",
            "net_dex_ratio_proxy",
        )
        return {key: value.get(key) for key in keys}

    return {
        "instrument_scope": "SPXW_0DTE_options_not_ES_options",
        "as_of": frame.get("as_of"),
        "quality": exposure.get("quality"),
        "snapshot_age_seconds": exposure.get("snapshot_age_seconds"),
        "delta_coverage_ratio": exposure.get("delta_coverage_ratio"),
        "iv_coverage_ratio": exposure.get("iv_coverage_ratio"),
        "oi_quality": exposure.get("oi_quality"),
        "dealer_position_sign": exposure.get("dealer_position_sign"),
        "sign_convention": exposure.get("sign_convention"),
        "gex_weighting_divergence": exposure.get("gex_weighting_divergence"),
        "oi_weighted": aggregate("oi_weighted"),
        "volume_weighted": aggregate("volume_weighted"),
        "key_strikes": [row for row in exposure.get("key_strikes") or [] if isinstance(row, dict)][
            :8
        ],
        "warnings": exposure.get("warnings"),
    }
