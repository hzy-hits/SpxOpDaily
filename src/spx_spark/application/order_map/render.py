"""Deterministic order-map message templates."""

from __future__ import annotations

from typing import Any

from spx_spark.application.order_map.models import PLAY_ORDER, PLAY_TEMPLATE_LINES
from spx_spark.analytics.options.pricing import finite_float


def _dash(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.1f}".removesuffix(".0")
    return str(value)


def _fmt_premium(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.2f}"


def _fmt_prob(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.0%}"


def _fmt_eta_minutes(minutes: float) -> str:
    if minutes >= 90.0:
        return f"{minutes / 60.0:.1f} 小时".replace(".0 ", " ")
    return f"{minutes:.0f} 分钟"


def _day_move_line(payload: dict[str, Any]) -> str | None:
    day_move = payload.get("day_move") if isinstance(payload.get("day_move"), dict) else {}
    points = day_move.get("points")
    if points is None:
        return None
    em_used = day_move.get("em_used_fraction")
    em_text = f",已用当日预期波幅的 {em_used:.0%}" if isinstance(em_used, (int, float)) else ""
    return f"较昨收: {points:+.1f} 点{em_text}"


ES_VOLUME_LABEL_TEXT = {
    "elevated": "放量",
    "quiet": "缩量",
    "normal": "正常",
}

ES_VOLUME_DIRECTION_TEXT = {
    "up": "上涨",
    "down": "下跌",
    "flat": "横盘",
}

ES_VOLUME_LOCATION_TEXT = {
    "at_put_wall": "贴put墙",
    "at_call_wall": "贴call墙",
    "in_flip": "在flip区",
    "below_put_wall": "破put墙下方",
    "above_call_wall": "破call墙上方",
    "mid_range": "中间地带",
    "unknown": "位置未知",
}

ES_VOLUME_EVENT_TEXT = {
    "elevated_sell_into_support": "放量砸支撑",
    "elevated_buy_into_resistance": "放量撞阻力",
    "quiet_sell_near_support": "缩量阴跌近支撑",
    "quiet_buy_near_resistance": "缩量摸高近阻力",
    "quiet_reclaim_after_sell_test": "缩量收回(测支撑后)",
    "quiet_mid_range": "中间缩量漂移",
    "elevated_mid_range": "中间放量对打",
    "elevated_break_holds": "放量破位站稳",
    "quiet_breakdown_holds": "缩量破位站稳",
    "break_holds": "破位站稳",
    "break_reclaimed": "破位后收回",
    "elevated_move": "放量移动",
    "quiet_move": "缩量移动",
    "normal_pace": "节奏正常",
    "unclassified": "未分类",
}


def _es_volume_line(payload: dict[str, Any]) -> str | None:
    signal = payload.get("es_volume") if isinstance(payload.get("es_volume"), dict) else None
    if not signal:
        return None
    ratio = signal.get("pace_ratio")
    delta = signal.get("delta")
    window = signal.get("window_minutes")
    if ratio is None or delta is None or window is None:
        return None
    label = ES_VOLUME_LABEL_TEXT.get(str(signal.get("label")), "正常")
    baseline_text = "近几窗" if signal.get("baseline") == "recent_windows" else "当日均值"
    parts = [
        f"ES 量价: 最近{window:.0f}分钟 {int(delta):,} 手, "
        f"节奏为{baseline_text}的 {ratio:.1f} 倍({label})"
    ]
    direction = signal.get("direction")
    price_delta = signal.get("price_delta")
    if direction is not None:
        dir_text = ES_VOLUME_DIRECTION_TEXT.get(str(direction), str(direction))
        if isinstance(price_delta, (int, float)):
            parts.append(f"价{price_delta:+.1f}({dir_text})")
        else:
            parts.append(dir_text)
    location = signal.get("location")
    if location and location != "unknown":
        parts.append(ES_VOLUME_LOCATION_TEXT.get(str(location), str(location)))
    event_id = signal.get("event_id")
    if event_id:
        parts.append(ES_VOLUME_EVENT_TEXT.get(str(event_id), str(event_id)))
    break_outcome = signal.get("break_outcome")
    if break_outcome in {"holds", "reclaimed", "pending"}:
        outcome_text = {"holds": "破位确认中/站稳", "reclaimed": "已收回", "pending": "破位观察中"}[
            str(break_outcome)
        ]
        parts.append(outcome_text)
    return " · ".join(parts)


def _hl_volume_line(payload: dict[str, Any]) -> str | None:
    signal = payload.get("hl_volume") if isinstance(payload.get("hl_volume"), dict) else None
    if not signal:
        return None
    ratio = signal.get("pace_ratio")
    window = signal.get("window_minutes")
    delta = signal.get("delta_notional")
    if ratio is None or window is None or delta is None:
        return None
    label = ES_VOLUME_LABEL_TEXT.get(str(signal.get("label")), "正常")
    parts = [
        f"HL永续量价(24/7薄代理): 最近{window:.0f}分钟名义 ${delta / 1e4:,.0f}万, "
        f"节奏 {ratio:.1f} 倍({label})"
    ]
    aggressor = signal.get("aggressor_buy_ratio")
    if isinstance(aggressor, (int, float)):
        parts.append(f"主动买占比 {aggressor:.0%}")
    imbalance = signal.get("book_imbalance")
    if isinstance(imbalance, (int, float)):
        parts.append(f"盘口失衡 {imbalance:+.2f}")
    return " · ".join(parts)


def _rn_density_line(payload: dict[str, Any]) -> str | None:
    density = payload.get("rn_density") if isinstance(payload.get("rn_density"), dict) else None
    if not density or density.get("median") is None:
        return None
    quality = density.get("quality") or "-"
    parts = [f"中位 {_dash(density.get('median'))}"]
    p10, p90 = density.get("p10"), density.get("p90")
    if p10 is not None and p90 is not None:
        parts.append(f"80%区间 {_dash(p10)}-{_dash(p90)}")
    below = density.get("prob_below_put_wall")
    if below is not None:
        parts.append(f"收破put墙 {_fmt_prob(below)}")
    above = density.get("prob_above_call_wall")
    if above is not None:
        parts.append(f"越call墙 {_fmt_prob(above)}")
    suffix = f" [{quality}]" if quality != "ok" else ""
    return "收盘分布(B-L市场定价): " + ", ".join(parts) + suffix


def _greeks_reference_line(payload: dict[str, Any]) -> str | None:
    reference = payload.get("spxw_0dte_greeks_reference")
    if not isinstance(reference, dict) or reference.get("status") not in {"ok", "degraded"}:
        return None
    aggregate = reference.get("aggregate")
    coverage = reference.get("coverage")
    if not isinstance(aggregate, dict) or not isinstance(coverage, dict):
        return None

    def metric(name: str) -> str:
        value = finite_float(aggregate.get(name))
        return f"{value:.2e}" if value is not None else "-"

    usable = coverage.get("usable_contract_count")
    total = coverage.get("exact_expiry_contract_count")
    return (
        "0DTE Greeks(只读/仓位符号未知, OI×100): "
        f"Gamma {metric('gross_gamma_abs')}, "
        f"Charm5m {metric('gross_charm_5m_abs')}, "
        f"Vanna1vol {metric('gross_vanna_1vol_abs')}; "
        f"覆盖 {usable}/{total} [{reference.get('status')}]"
    )


def _wall_ladder_lines(payload: dict[str, Any]) -> list[str]:
    ladder = payload.get("wall_ladder") if isinstance(payload.get("wall_ladder"), dict) else {}
    lines: list[str] = []
    for key, label, default_right in (
        ("put_walls", "put 墙阶梯(下方支撑→买 call)", "C"),
        ("call_walls", "call 墙阶梯(上方阻力→买 put)", "P"),
    ):
        rungs = [rung for rung in (ladder.get(key) or []) if isinstance(rung, dict)]
        if not rungs:
            continue
        # Payload order is GEX rank; rungs[0] is the primary wall. Display in
        # spatial order (nearest first) so it reads as an actual ladder.
        primary_strike = rungs[0].get("strike")
        spatial = sorted(
            rungs,
            key=lambda rung: -(rung.get("strike") or 0.0),
            reverse=(key == "call_walls"),
        )
        lines.append(f"{label} (★=主墙):")
        for rung in spatial:
            strike = _dash(rung.get("strike"))
            star = "★" if rung.get("strike") == primary_strike else " "
            oi = rung.get("open_interest")
            oi_text = f"OI {int(oi)}" if isinstance(oi, (int, float)) and oi > 0 else "OI -"
            prob = rung.get("prob_touch")
            prob_text = f",触达{_fmt_prob(prob)}" if prob is not None else ""
            right = str(rung.get("option_right") or default_right)
            opt_strike = rung.get("option_strike")
            opt_label = (
                f"{_dash(opt_strike)}{right}" if opt_strike is not None else f"{strike}{right}"
            )
            projected = rung.get("projected_mid")
            aggressive = rung.get("limit_aggressive")
            conservative = rung.get("limit_conservative")
            current = rung.get("current_mid")
            if projected is not None:
                stale_tag = " [stale]" if rung.get("degraded") else ""
                price_text = (
                    f"{opt_label} 到位预估{_fmt_premium(projected)}"
                    f"(现{_fmt_premium(current)}) "
                    f"限价{_fmt_premium(aggressive)}/{_fmt_premium(conservative)}"
                    f"{stale_tag}"
                )
            else:
                price_text = f"{opt_label} 参考价-"
            lines.append(f"  {star}{strike} ({oi_text}{prob_text}) → {price_text}")
    return lines


def _candidate_by_play(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    raw_candidates = payload.get("candidates")
    if not isinstance(raw_candidates, list):
        return {}
    mapped: dict[str, dict[str, Any]] = {}
    for item in raw_candidates:
        if isinstance(item, dict) and isinstance(item.get("play"), str):
            mapped[item["play"]] = item
    return mapped


def render_research_only_template(
    payload: dict[str, Any],
    *,
    title: str = "研究地图",
) -> str:
    reference = (
        payload.get("research_reference")
        if isinstance(payload.get("research_reference"), dict)
        else {}
    )
    pricing = (
        payload.get("pricing_reference")
        if isinstance(payload.get("pricing_reference"), dict)
        else {}
    )
    lines = [
        f"【{title} {payload.get('beijing_time') or '-'}】(0DTE={payload.get('expiry') or '-'})",
        (
            f"研究参考: {_dash(reference.get('price'))}"
            f"({reference.get('source') or '-'}); 不可执行定价"
        ),
        (
            f"定价闸门: {pricing.get('gate_state') or '-'} — "
            f"{pricing.get('reason') or '缺少可执行锚点'}"
        ),
        (
            f"gamma: {payload.get('gamma_state') or '-'}, "
            f"zero gamma {_dash(payload.get('zero_gamma'))}, "
            f"预期波幅 ±{_dash(payload.get('expected_move_points'))} 点"
        ),
    ]
    candidates = payload.get("research_candidates")
    if isinstance(candidates, list):
        for item in candidates:
            if not isinstance(item, dict):
                continue
            distance = item.get("distance_points")
            distance_text = (
                f"{float(distance):+.1f}点" if isinstance(distance, (int, float)) else "距离未知"
            )
            observed_markets: list[str] = []
            observed_options = item.get("observed_options")
            if isinstance(observed_options, list):
                for observed in observed_options:
                    if not isinstance(observed, dict):
                        continue
                    quality = observed.get("quote_quality") or "unknown"
                    freshness = observed.get("quote_freshness") or "unknown"
                    observed_markets.append(
                        f"{item.get('strike')}{observed.get('right') or ''} "
                        f"{_fmt_premium(observed.get('observed_bid'))}/"
                        f"{_fmt_premium(observed.get('observed_ask'))} "
                        f"[{quality}/{freshness}]"
                    )
            lines.append(
                f"研究情景: {item.get('level_kind') or '-'} {_dash(item.get('level'))} "
                f"({distance_text}); 观察报价 "
                f"{'; '.join(observed_markets) if observed_markets else '-'}"
            )
    divergence = pricing.get("divergence_bps")
    if isinstance(divergence, (int, float)):
        lines.append(f"HL 与定价候选分歧: {float(divergence):+.0f} bps")
    lines.append("仅供研究观察：无模型重定价、触达概率、ETA、限价或下单建议。")
    warnings = payload.get("warnings")
    if isinstance(warnings, list) and warnings:
        lines.append(f"数据警告: {'; '.join(str(item) for item in warnings)}")
    return "\n".join(lines)


def render_template(payload: dict[str, Any]) -> str:
    if payload.get("research_only") is True:
        return render_research_only_template(payload)
    trading_date = payload.get("trading_date") or "-"
    beijing_time = payload.get("beijing_time") or "14:00"
    expiry = payload.get("expiry") or "-"

    underlier = payload.get("underlier") if isinstance(payload.get("underlier"), dict) else {}
    underlier_price = underlier.get("price")
    underlier_source = underlier.get("source") or "-"

    expected_move = payload.get("expected_move_points")
    gamma_state = payload.get("gamma_state") or "-"
    zero_gamma = payload.get("zero_gamma")
    flip_zone = payload.get("flip_zone") if isinstance(payload.get("flip_zone"), list) else None

    flip_lo = _dash(flip_zone[0]) if flip_zone and len(flip_zone) >= 2 else "-"
    flip_hi = _dash(flip_zone[1]) if flip_zone and len(flip_zone) >= 2 else "-"

    lines = [
        f"【挂单地图 {trading_date}】(北京 {beijing_time},0DTE={expiry})",
        *(
            [f"时段: {phase.get('name_cn')} — {phase.get('traits')}"]
            if isinstance(phase := payload.get("session_phase"), dict) and phase.get("name_cn")
            else []
        ),
        (
            f"参考价: {_dash(underlier_price)}({underlier_source}), "
            f"预期波幅 ±{_dash(expected_move)} 点"
        ),
        (f"gamma: {gamma_state}, zero gamma {_dash(zero_gamma)}, flip zone {flip_lo}-{flip_hi}"),
    ]
    greeks_line = _greeks_reference_line(payload)
    if greeks_line:
        lines.append(greeks_line)
    day_move_line = _day_move_line(payload)
    if day_move_line:
        lines.append(day_move_line)
    es_volume_line = _es_volume_line(payload)
    if es_volume_line:
        lines.append(es_volume_line)
    hl_volume_line = _hl_volume_line(payload)
    if hl_volume_line:
        lines.append(hl_volume_line)
    ladder_lines = _wall_ladder_lines(payload)
    lines.extend(ladder_lines)
    density_line = _rn_density_line(payload)
    if density_line:
        lines.append(density_line)

    by_play = _candidate_by_play(payload)
    bias = payload.get("conditional_call_bias")
    preferred_play = (
        str(bias.get("play") or "")
        if isinstance(bias, dict) and bias.get("status") == "confirmed"
        else ""
    )
    render_order = (
        (preferred_play, *(play for play in PLAY_ORDER if play != preferred_play))
        if preferred_play in PLAY_ORDER
        else PLAY_ORDER
    )
    index = 0
    for play in render_order:
        candidate = by_play.get(play)
        if candidate is None:
            continue
        index += 1
        level_label = candidate.get("level_label") or "-"
        strike = candidate.get("strike")
        right = candidate.get("right") or ""
        headline = PLAY_TEMPLATE_LINES[play].format(
            level_label=level_label,
            strike=strike,
            right=right,
        )
        lines.append(f"{index}) {headline}")
        lines.append(
            "   触达概率≈"
            f"{_fmt_prob(candidate.get('prob_touch'))}, "
            f"到位时预估价≈{_fmt_premium(candidate.get('projected_mid'))}"
            f"(现价 {_fmt_premium(candidate.get('current_mid'))})"
        )
        if candidate.get("order_style") == "stop_trigger":
            lines.append(
                "   注意: 预估价高于现价,被动限价会立即成交;"
                "此单需破位确认后下条件单/市价,不适合提前挂"
            )
        else:
            lines.append(
                "   挂单参考: 激进 "
                f"{_fmt_premium(candidate.get('limit_aggressive'))} / 保守 "
                f"{_fmt_premium(candidate.get('limit_conservative'))}"
            )
            eta = candidate.get("touch_eta_minutes")
            if isinstance(eta, (int, float)) and eta > 0:
                lines.append(
                    f"   时效: 预计 ≈{_fmt_eta_minutes(float(eta))} 到位; "
                    "超约 2 倍时间未到, 赔率已变质, 先撤"
                )
            frontrun_level = candidate.get("frontrun_level")
            if frontrun_level is not None:
                lines.append(
                    f"   先手挡 {_dash(frontrun_level)}: 限价 "
                    f"{_fmt_premium(candidate.get('frontrun_limit'))}, "
                    f"触达≈{_fmt_prob(candidate.get('frontrun_prob_touch'))}"
                    "(墙前反转也能吃到)"
                )

    lines.append(
        "注: 墙位是 OI 真实聚集处(多在整数位),但价格常在墙前几点反转;"
        "先手挡=向现价方向让 30% 距离,成交率高、价格稍差。"
        "预估价按 BS 重定价,已计触达前的时间衰减与 vol 斜率(跌到位 IV 上抬/涨到位 IV 回落);"
        "保守价≈预估×0.85。"
        "提醒: 0DTE 权利金随时间单边衰减,纯权利金限价单可能在指数未到位时就被时间衰减打成;"
        "要严格按点位入场,用指数条件单(SPX 到 XX 触发限价)更精确。仅供参考,不是订单指令。"
    )

    warnings = payload.get("warnings")
    if isinstance(warnings, list) and warnings:
        lines.append(f"数据警告: {'; '.join(str(item) for item in warnings)}")

    return "\n".join(lines)
