"""Deterministic morning-map template and LLM prompt builders."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from spx_spark.config import NY_TZ
from spx_spark.notifier.llm_writer import previous_push_json

def _dash(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.1f}".removesuffix(".0")
    return str(value)


def _fmt_gap_points(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:+.0f}"


def _fmt_gap_pct(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:+.2%}"


def _fmt_prob(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.0%}"


def _fmt_oi(value: float | None) -> str:
    if value is None:
        return ""
    return f"(OI {value:.0f})"


def _greeks_reference_line(reference: object) -> str | None:
    if not isinstance(reference, dict) or reference.get("status") not in {"ok", "degraded"}:
        return None
    aggregate = reference.get("aggregate")
    coverage = reference.get("coverage")
    if not isinstance(aggregate, dict) or not isinstance(coverage, dict):
        return None

    def metric(name: str) -> str | None:
        value = aggregate.get(name)
        if not isinstance(value, int | float):
            return None
        if abs(value) >= 1_000:
            return f"{value / 1_000:.1f}k"
        return f"{value:.1f}"

    usable = coverage.get("usable_contract_count")
    total = coverage.get("exact_expiry_contract_count")
    ratio = (
        usable / total
        if isinstance(usable, int | float)
        and isinstance(total, int | float)
        and total > 0
        else None
    )
    coverage_text = f"有效 {usable}/{total}"
    if ratio is not None:
        coverage_text += f"（{ratio:.0%}）"
    gamma = metric("gross_gamma_abs")
    charm = metric("gross_charm_5m_abs")
    vanna = metric("gross_vanna_1vol_abs")
    if None in {gamma, charm, vanna}:
        return f"0DTE 全链敏感度暂不可用｜{coverage_text}，新鲜报价/IV/OI不足"
    excluded = (
        max(int(total - usable), 0)
        if isinstance(usable, int | float) and isinstance(total, int | float)
        else None
    )
    if reference.get("status") != "ok":
        quality = "覆盖或模型质量不足"
    elif excluded:
        quality = f"{excluded}条未纳入"
    else:
        quality = "完整"
    return (
        "0DTE 全链敏感度（OI加权绝对值，非持仓/非方向）: "
        f"Gamma/标的1点 {gamma}｜Charm/5分钟 {charm}｜"
        f"Vanna/IV变动1个百分点 {vanna}｜{coverage_text}，{quality}"
    )


def _confluence_label(value: bool | None) -> str:
    if value is True:
        return "共振"
    if value is False:
        return "不共振"
    return "-"


def _strike_oi(
    top_strikes: list[dict[str, Any]] | None, strike: float | None, kind: str
) -> float | None:
    if strike is None or not top_strikes:
        return None
    key = "call_oi" if kind == "call" else "put_oi"
    for row in top_strikes:
        if isinstance(row, dict) and row.get("strike") == strike:
            oi = row.get(key)
            return float(oi) if oi is not None else None
    return None


def render_template(payload: dict[str, Any]) -> str:
    as_of_raw = payload.get("as_of")
    payload_trading_date = payload.get("trading_date")
    trading_date = (
        payload_trading_date
        if isinstance(payload_trading_date, str) and payload_trading_date
        else "-"
    )
    if trading_date == "-" and isinstance(as_of_raw, str) and as_of_raw:
        try:
            as_of = datetime.fromisoformat(as_of_raw.replace("Z", "+00:00"))
            if as_of.tzinfo is None:
                as_of = as_of.replace(tzinfo=timezone.utc)
            trading_date = as_of.astimezone(NY_TZ).date().isoformat()
        except ValueError:
            pass

    overnight = payload.get("overnight") if isinstance(payload.get("overnight"), dict) else {}
    es_last = overnight.get("es_last")
    gap_points = overnight.get("gap_points")
    gap_pct = overnight.get("gap_pct")
    spx_prev_close = overnight.get("spx_prev_close")

    focus = (
        payload.get("human_focus_context")
        if isinstance(payload.get("human_focus_context"), dict)
        else {}
    )
    spxw = focus.get("spxw_options") if isinstance(focus.get("spxw_options"), dict) else {}
    expiries = spxw.get("expiries") if isinstance(spxw.get("expiries"), list) else []
    front = expiries[0] if expiries and isinstance(expiries[0], dict) else {}

    call_wall = front.get("call_wall")
    put_wall = front.get("put_wall")
    gamma_profile = (
        front.get("gamma_profile") if isinstance(front.get("gamma_profile"), dict) else {}
    )
    zero_gamma = gamma_profile.get("zero_gamma")
    flip_zone = gamma_profile.get("flip_zone")
    top_strikes = (
        gamma_profile.get("top_strikes")
        if isinstance(gamma_profile.get("top_strikes"), list)
        else []
    )

    flip_lo = "-"
    flip_hi = "-"
    if isinstance(flip_zone, list) and len(flip_zone) >= 2:
        flip_lo = _dash(flip_zone[0])
        flip_hi = _dash(flip_zone[1])

    call_oi_suffix = _fmt_oi(_strike_oi(top_strikes, call_wall, "call"))
    put_oi_suffix = _fmt_oi(_strike_oi(top_strikes, put_wall, "put"))

    level_probs = (
        front.get("level_probabilities")
        if isinstance(front.get("level_probabilities"), list)
        else []
    )
    prob_parts: list[str] = []
    seen_levels: set[str] = set()
    for item in level_probs:
        if not isinstance(item, dict):
            continue
        level = item.get("level")
        level_key = _dash(level)
        if level_key in seen_levels:
            continue
        seen_levels.add(level_key)
        prob_touch = item.get("prob_touch")
        prob_close = item.get("prob_close_beyond")
        prob_parts.append(f"触及 {level_key}≈{_fmt_prob(prob_touch)}/收破≈{_fmt_prob(prob_close)}")
    prob_line = "; ".join(prob_parts) if prob_parts else "-"

    wall_confluence = (
        spxw.get("wall_confluence") if isinstance(spxw.get("wall_confluence"), dict) else None
    )
    if wall_confluence:
        spy_put = wall_confluence.get("spy_put_wall_spx")
        spy_call = wall_confluence.get("spy_call_wall_spx")
        spy_line = (
            f"put 墙折算 {_dash(spy_put)}({_confluence_label(wall_confluence.get('put_wall_confluent'))}), "
            f"call 墙折算 {_dash(spy_call)}({_confluence_label(wall_confluence.get('call_wall_confluent'))})"
        )
    else:
        spy_line = "无 SPY 数据"

    micopedia = focus.get("micopedia") if isinstance(focus.get("micopedia"), dict) else {}
    regime = _dash(micopedia.get("regime"))
    vix_ratio = micopedia.get("vix_ratio")
    vix_ratio_text = f"{vix_ratio:.2f}" if isinstance(vix_ratio, int | float) else "-"
    dip_context = _dash(micopedia.get("dip_context"))

    event_tags = micopedia.get("event_tags")
    if isinstance(event_tags, list) and event_tags:
        events = ", ".join(str(tag) for tag in event_tags)
    else:
        events = "无"

    watchlist = micopedia.get("trigger_watchlist")
    if isinstance(watchlist, list) and watchlist:
        watch_text = "; ".join(str(item) for item in watchlist[:3])
    else:
        watch_text = "-"

    greeks_line = _greeks_reference_line(spxw.get("greeks_reference_0dte"))

    lines = [
        f"【盘前地图 {trading_date}】",
        (
            f"隔夜: ES {_dash(es_last)}({_fmt_gap_points(gap_points)} 点/{_fmt_gap_pct(gap_pct)} vs 昨结), "
            f"SPX 昨收 {_dash(spx_prev_close)}"
        ),
        (
            f"gamma 地形: call wall {_dash(call_wall)}{call_oi_suffix}, "
            f"put wall {_dash(put_wall)}{put_oi_suffix}, "
            f"zero gamma {_dash(zero_gamma)}, flip zone {flip_lo}-{flip_hi}"
        ),
        f"概率锥: {prob_line}",
        *([greeks_line] if greeks_line else []),
        f"SPY 对照: {spy_line}",
        f"regime: {regime}, VIX1D/VIX={vix_ratio_text}, dip_context={dip_context}",
        f"事件: {events}",
        f"开盘前 2 小时关注: {watch_text}",
    ]
    return "\n".join(lines)


def build_map_prompt(
    payload: dict[str, Any],
    template: str,
    previous_push: dict[str, Any] | None = None,
) -> str:
    return "\n".join(
        (
            "这条是『盘前地图』，开盘铃前最后一份便签。搭档挂好的单马上要接受开盘检验，他要的是：开盘头一小时的剧本，"
            "以及第一根急拉/急跌出现时他该做什么、不该做什么。",
            "动笔前先想清楚(不写出来)：隔夜 gap 是谁推的、开盘后大概率被回补还是被延续？做市商今天开在正 gamma 还是负 gamma，"
            "开盘的波动会被吸收还是被放大？昨天的墙隔夜有没有被 OI 变化掏空？",
            "框架口径：Micopedia/Steven 都是 observe_only（regime→map→flow→trigger→expression→exit）；"
            "GEX 与 *_proxy 曝露是结构代理不是 vendor DEX；不下单授权；Hyperliquid 不作 SPX 锚。",
            "",
            "输出中文，最多 14 行，第一行必须是模板的第一行。",
            "开头定调：相对 previous_push(下午以来最近一条)剧本有变还是延续——墙位/flip 移位、gap 改变优先 play 才算有变，"
            "有变就点名哪张单要改。",
            "隔夜 gap 给结论不给流水账：方向、幅度相对预期波幅算大还是小、对挂单意味着什么。",
            "地形一句话：pin/transition/negative，墙位与 flip zone 在哪，开盘价落在地形的哪个位置。",
            "开盘剧本写成双向 if/then(3-4 行)：开盘后 30-60 分钟，站上/跌破哪些具体点位(引用触及/收破概率)分别激活什么剧本、"
            "盯哪张单；急跌时结合 dip_context 说清是回调买点还是加速风险——这是搭档最容易在开盘慌手的地方，话要说死：到什么位置之前不动作。",
            "1 行 vol：VIX1D/VIX 比值与 SKEW，今天 vol 卖得贵还是便宜、有无事件定价。",
            "human_focus_context.spxw_options.greeks_reference_0dte 只覆盖严格 SPXW 当日到期，是价格/时间/IV 情景参考；"
            "position_sign/direction=unknown 时负 gamma 不等于下跌，不得改变原候选方向、排序或限价。",
            "1 行 SPY 墙位对照：共振增强可信度，不共振就明说墙位参考价值打折。",
            "previous_push:" + previous_push_json(previous_push),
            "JSON:" + json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            "模板:" + template,
        )
    )
