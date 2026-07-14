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
    em_text = f"，GTH 以来已用预期波幅的 {em_used:.0%}" if isinstance(em_used, (int, float)) else ""
    return f"较昨收: {points:+.1f} 点{em_text}"


def _globex_trend_line(payload: dict[str, Any]) -> str | None:
    trend = payload.get("globex_trend")
    if not isinstance(trend, dict):
        return None
    metrics = trend.get("metrics")
    if not isinstance(metrics, dict) or metrics.get("price") is None:
        return None
    labels = {"neutral": "中性", "bearish": "偏空", "bullish": "偏多"}

    def points(key: str) -> str:
        value = finite_float(metrics.get(key))
        return "-" if value is None else f"{value:+.1f}"

    regime = str(trend.get("regime") or "neutral")
    phase = payload.get("session_phase")
    phase_name = str(phase.get("name") or "") if isinstance(phase, dict) else ""
    path_label = "ES RTH路径" if phase_name.startswith("us_") else "ES Globex路径"
    candidate = trend.get("candidate_regime")
    candidate_text = ""
    if isinstance(candidate, str):
        count = int(trend.get("candidate_observations") or 0)
        candidate_text = f"; 候选{labels.get(candidate, candidate)}({count})"
    return (
        f"{path_label}: {labels.get(regime, regime)}{candidate_text}; "
        f"15m {points('return_15m_points')}, 60m {points('return_60m_points')}, "
        f"180m {points('return_180m_points')}; "
        f"趋势腿回撤 {points('drawdown_from_regime_high_points')}, "
        f"趋势腿反弹 {points('rebound_from_regime_low_points')} 点"
    )


def _market_feature_lines(payload: dict[str, Any]) -> list[str]:
    market = payload.get("minute_market_frame")
    options = payload.get("option_structure_frame")
    if not isinstance(market, dict):
        return []
    es = market.get("es") if isinstance(market.get("es"), dict) else {}
    volume = market.get("volume") if isinstance(market.get("volume"), dict) else {}
    cross = market.get("cross_asset") if isinstance(market.get("cross_asset"), dict) else {}
    alignment = {
        "price_volume_aligned": "量价同向",
        "price_without_volume_confirmation": "价格缺少成交量确认",
        "volume_without_price_progress": "放量但价格未推进",
        "flat": "价量平稳",
        "unavailable": "窗口不足",
    }.get(str(volume.get("price_volume_alignment_5m") or ""), "窗口不足")
    volume_provider = volume.get("recent_volume_provider") or cross.get("selected_es_provider")
    lines = [
        (
            "ES统一帧: "
            f"1/5/15/60/180m {_dash(es.get('return_1m_points'))}/"
            f"{_dash(es.get('return_5m_points'))}/{_dash(es.get('return_15m_points'))}/"
            f"{_dash(es.get('return_60m_points'))}/{_dash(es.get('return_180m_points'))}; "
            f"VWAP {_dash(es.get('vwap'))}(偏离{_dash(es.get('vwap_distance_points'))})"
        ),
        (
            f"量价帧: 5m增量 {_dash(volume.get('volume_delta_5m'))}; "
            f"{alignment}; "
            f"ES/SPY {cross.get('es_spy_direction_confirmation_15m') or 'unavailable'}; "
            f"源 {volume_provider or '-'}"
        ),
    ]
    if isinstance(options, dict):
        structure = options.get("structure") if isinstance(options.get("structure"), dict) else {}
        option_vol = (
            options.get("volatility") if isinstance(options.get("volatility"), dict) else {}
        )
        l1 = options.get("l1") if isinstance(options.get("l1"), dict) else {}
        lines.append(
            f"期权统一帧: wall迁移 P{_dash(structure.get('put_wall_migration_points'))}/"
            f"C{_dash(structure.get('call_wall_migration_points'))}; "
            f"ATM IV 5/15/60m {_dash(option_vol.get('atm_iv_change_5m'))}/"
            f"{_dash(option_vol.get('atm_iv_change_15m'))}/"
            f"{_dash(option_vol.get('atm_iv_change_60m'))}; "
            f"L1流动性 {_l1_liquidity_text(l1)}"
        )
    macro = payload.get("macro_event")
    if isinstance(macro, dict) and macro.get("mode") != "normal":
        event = macro.get("active_event") if isinstance(macro.get("active_event"), dict) else {}
        lines.append(
            f"宏观事件时钟: {macro.get('mode')}，{event.get('name') or '-'} "
            f"{event.get('release_at') or '-'}；新入场={'允许' if macro.get('entry_allowed') else '禁止'}"
        )
    greek = payload.get("greek_decision")
    presentation = payload.get("candidate_presentation")
    primary_play = presentation.get("play") if isinstance(presentation, dict) else None
    if isinstance(greek, dict):
        score_rows = greek.get("contract_scores") if isinstance(greek.get("contract_scores"), dict) else {}
        selected = next(
            (
                row
                for candidate in payload.get("candidates") or []
                if isinstance(candidate, dict)
                and candidate.get("play") == primary_play
                and isinstance((row := score_rows.get(str(candidate.get("contract_id") or ""))), dict)
            ),
            None,
        )
        if selected:
            lines.append(
                f"Greeks决策层: {greek.get('mode')}，同方向合约置信调整 "
                f"{float(selected.get('confidence_adjustment') or 0):+.0f}；"
                f"Theta15m损耗 {_fmt_prob(selected.get('theta_15m_loss_fraction'))}，"
                f"IV-3vol损耗 {_fmt_prob(selected.get('iv_down_3vol_loss_fraction'))}"
            )
    return lines


def _l1_liquidity_text(l1: dict[str, Any]) -> str:
    quality = str(l1.get("quality") or "unavailable")
    metrics = l1.get("metrics") if isinstance(l1.get("metrics"), dict) else {}
    score = metrics.get("liquidity_score")
    if not isinstance(score, int | float):
        return "不可用"
    suffix = "（降级）" if quality == "degraded" else ""
    return f"{_dash(score)}{suffix}"


def _max_pain_line(payload: dict[str, Any]) -> str | None:
    value = payload.get("max_pain")
    if not isinstance(value, dict):
        return None
    settlement = finite_float(value.get("settlement_strike"))
    call_strike = finite_float(value.get("call_oi_peak_strike"))
    put_strike = finite_float(value.get("put_oi_peak_strike"))
    if settlement is None or call_strike is None or put_strike is None:
        return None

    def count(raw: Any) -> str:
        parsed = finite_float(raw)
        return f"{parsed:,.0f}" if parsed is not None else "-"

    quality = str(value.get("quality") or "unknown")
    strike_count = value.get("oi_strike_count")
    coverage = f", {strike_count} strikes" if isinstance(strike_count, int) else ""
    return (
        f"OI结构: Max Pain {_dash(settlement)}; "
        f"Call OI峰 {_dash(call_strike)}({count(value.get('call_oi_peak'))}); "
        f"Put OI峰 {_dash(put_strike)}({count(value.get('put_oi_peak'))}) "
        f"[{quality}{coverage}]"
    )


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

    def metric(name: str) -> str | None:
        value = finite_float(aggregate.get(name))
        if value is None:
            return None
        if abs(value) >= 1_000:
            return f"{value / 1_000:.1f}k"
        return f"{value:.1f}"

    usable = coverage.get("usable_contract_count")
    total = coverage.get("exact_expiry_contract_count")
    ratio = (
        usable / total
        if isinstance(usable, int | float) and isinstance(total, int | float) and total > 0
        else None
    )
    coverage_text = f"有效 {usable}/{total}"
    if ratio is not None:
        coverage_text += f"（{ratio:.0%}）"
    gamma = metric("gross_gamma_abs")
    charm = metric("gross_charm_5m_abs")
    vanna = metric("gross_vanna_1vol_abs")
    if None in {gamma, charm, vanna}:
        return f"0DTE 全链敏感度暂不可用　{coverage_text}，新鲜报价/IV/OI不足"
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
        f"Gamma/标的1点 {gamma}　Charm/5分钟 {charm}　"
        f"Vanna/IV变动1个百分点 {vanna}　{coverage_text}，{quality}"
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
                    f"{opt_label} BS触位情景{_fmt_premium(projected)}"
                    f"(现{_fmt_premium(current)}) "
                    f"触发后参考{_fmt_premium(aggressive)}/{_fmt_premium(conservative)}"
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


def _presented_candidates(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], str]:
    if "plan_candidates" not in payload:
        rows = [item for item in payload.get("candidates") or [] if isinstance(item, dict)]
        return rows, "legacy"
    plans = [item for item in payload.get("plan_candidates") or [] if isinstance(item, dict)]
    if plans:
        return plans, "plan"
    observations = [
        item for item in payload.get("observation_candidates") or [] if isinstance(item, dict)
    ]
    return observations, "observation"


def render_research_only_template(
    payload: dict[str, Any],
    *,
    title: str = "市场状态",
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
    expiry = str(payload.get("expiry") or "-")
    expiry_text = f"{expiry[4:6]}-{expiry[6:8]}" if len(expiry) == 8 else expiry
    phase = payload.get("session_phase")
    phase_name = str(phase.get("name_cn") or "盘外") if isinstance(phase, dict) else "盘外"
    header = (
        f"【SPX 15m · {payload.get('beijing_time') or '-'} · 0DTE {expiry_text} · {phase_name}】"
        if title == "市场状态"
        else f"【{title} {payload.get('beijing_time') or '-'}】(0DTE={expiry})"
    )
    lines = [
        header,
        (f"跨市场参考: {_dash(reference.get('price'))}({reference.get('source') or '-'})"),
    ]
    if line := _globex_trend_line(payload):
        lines.append(line)
    lines.extend(_market_feature_lines(payload))
    lines.extend(_level_decision_lines(payload))
    gamma_state = str(payload.get("gamma_state") or "")
    if gamma_state and not gamma_state.startswith("unknown"):
        lines.append(
            f"期权结构: gamma={gamma_state}, zero gamma "
            f"{_dash(payload.get('zero_gamma'))}, "
            f"预期波幅 ±{_dash(payload.get('expected_move_points'))} 点"
        )
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
                f"关键位情景: {item.get('level_kind') or '-'} {_dash(item.get('level'))} "
                f"({distance_text}); 观察报价 "
                f"{'; '.join(observed_markets) if observed_markets else '-'}"
            )
    divergence = pricing.get("divergence_bps")
    if isinstance(divergence, (int, float)):
        lines.append(f"HL 与定价候选分歧: {float(divergence):+.0f} bps")
    gate = str(pricing.get("gate_state") or "missing")
    lines.append(f"执行限制: {gate}；当前为不可执行定价，不生成期权模型价、概率、限价或下单建议。")
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
        f"【条件交易地图 {trading_date}】(北京 {beijing_time},0DTE={expiry})",
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
    max_pain_line = _max_pain_line(payload)
    if max_pain_line:
        lines.append(max_pain_line)
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
    lines.extend(_level_decision_lines(payload))
    ladder_lines = _wall_ladder_lines(payload)
    lines.extend(ladder_lines)
    density_line = _rn_density_line(payload)
    if density_line:
        lines.append(density_line)

    presented_candidates, presentation_role = _presented_candidates(payload)
    by_play = _candidate_by_play({"candidates": presented_candidates})
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
        if presentation_role == "observation":
            headline = headline.replace("买 call", "call 报价观察").replace(
                "买 put", "put 报价观察"
            )
        role_label = {
            "plan": "条件计划",
            "observation": "观察情景",
        }.get(presentation_role, "地图候选")
        lines.append(f"{index}) [{role_label}] {headline}")
        if presentation_role == "plan" and candidate.get("order_style") == "live_nbbo_limit":
            lines.append(
                "   实时执行: NBBO "
                f"{_fmt_premium(candidate.get('decision_bid'))}/"
                f"{_fmt_premium(candidate.get('decision_ask'))}; "
                f"买入上限 {_fmt_premium(candidate.get('limit_aggressive'))}"
            )
            lines.append(
                f"   风险: SPX {_dash(candidate.get('invalidation_spx'))} 失效; "
                f"目标 {_dash(candidate.get('target_spx'))}; "
                f"意图至 {candidate.get('intent_expires_at') or '-'}"
            )
            continue
        range_low = candidate.get("projection_range_low")
        range_high = candidate.get("projection_range_high")
        quote_executable = candidate.get("execution_quote_status") != "range_only"
        if range_low is None:
            range_low = candidate.get("projected_mid")
        if range_high is None:
            range_high = candidate.get("projected_mid")
        if quote_executable and presentation_role != "observation":
            lines.append(
                "   触达概率≈"
                f"{_fmt_prob(candidate.get('prob_touch'))}, "
                f"触位基准价≈{_fmt_premium(candidate.get('projected_mid'))}, "
                f"早/晚触区间 {_fmt_premium(range_low)}–{_fmt_premium(range_high)}"
                f"(现价 {_fmt_premium(candidate.get('current_mid'))})"
            )
        elif presentation_role == "observation":
            lines.append(
                "   观察用途：保留触位后的报价情景与方向验证；当前未通过决策门控，不生成下单计划"
            )
        else:
            reasons = ",".join(str(item) for item in candidate.get("execution_quote_reasons") or ())
            lines.append(
                "   报价门控未通过：只保留早/晚触情景区间 "
                f"{_fmt_premium(range_low)}–{_fmt_premium(range_high)}；"
                f"不给条件价（{reasons or 'quote_quality'}）"
            )
        iv_now = finite_float(candidate.get("projection_iv_now"))
        iv_touch = finite_float(candidate.get("projection_iv_at_touch"))
        tau_touch = finite_float(candidate.get("projection_tau_at_touch_minutes"))
        touch_fraction = finite_float(candidate.get("projection_touch_time_fraction"))
        if iv_now is not None and iv_touch is not None and tau_touch is not None:
            lines.append(
                f"   BS审计: IV {iv_now:.1%}→{iv_touch:.1%}, "
                f"触位时剩余≈{_fmt_eta_minutes(tau_touch)}"
                + (f",耗时假设占当前剩余{touch_fraction:.0%}" if touch_fraction is not None else "")
            )
        if quote_executable and presentation_role != "observation":
            lines.append(
                f"   条件执行: SPX 触及 {_dash(candidate.get('level'))} 后再提交; "
                f"触发后限价参考 {_fmt_premium(candidate.get('limit_aggressive'))} / "
                f"{_fmt_premium(candidate.get('limit_conservative'))}; 当前不可预挂"
            )
        eta = candidate.get("touch_eta_minutes")
        if isinstance(eta, (int, float)) and eta > 0:
            lines.append(
                f"   时效: 预计 ≈{_fmt_eta_minutes(float(eta))} 到位; "
                "该 ETA 仅是情景输入, 到时必须用实时 mid/IV 重算"
            )
        frontrun_level = candidate.get("frontrun_level")
        if frontrun_level is not None:
            lines.append(
                f"   先手挡 {_dash(frontrun_level)}: 触发后参考 "
                f"{_fmt_premium(candidate.get('frontrun_limit'))}, "
                f"触达≈{_fmt_prob(candidate.get('frontrun_prob_touch'))}"
                "(同样不可提前挂期权价)"
            )

    opposing = payload.get("opposing_invalidation")
    if isinstance(opposing, dict):
        lines.append(
            f"主策略失效条件: 若价格确认走向 {opposing.get('level_label') or opposing.get('level')}，"
            f"则当前主策略失效；`{opposing.get('play')}` 不作为并列反向计划。"
        )

    lines.append(
        "注: 墙位是 OI 真实聚集处(多在整数位),但价格常在墙前几点反转;"
        "先手挡=向现价方向让 30% 距离,成交率高、价格稍差。"
        "BS情景价已计触达前的时间衰减与 vol 斜率(跌到位 IV 上抬/涨到位 IV 回落);"
        "保守参考≈情景价×0.85。所有价格都依赖未来触达时间,不是当前可预挂的期权订单;"
        "必须由 SPX 点位触发后用实时 mid/IV 重算。仅供参考,不是订单指令。"
    )

    warnings = payload.get("warnings")
    if isinstance(warnings, list) and warnings:
        lines.append(f"数据警告: {'; '.join(str(item) for item in warnings)}")

    return "\n".join(lines)


def _level_decision_lines(payload: dict[str, Any]) -> list[str]:
    decision = payload.get("level_decision")
    if not isinstance(decision, dict):
        return []
    phase = str(decision.get("phase") or "far")
    kind = str(decision.get("level_kind") or "-")
    level_value = finite_float(decision.get("level"))
    level = _dash(level_value)
    thesis = str(decision.get("thesis") or "none")
    direction = str(decision.get("direction") or "-")
    spot = finite_float(decision.get("spot"))
    es = finite_float(decision.get("es"))
    levels = decision.get("levels") if isinstance(decision.get("levels"), dict) else {}
    bands = decision.get("level_bands") if isinstance(decision.get("level_bands"), dict) else {}
    es_levels = (
        decision.get("es_equivalent_levels")
        if isinstance(decision.get("es_equivalent_levels"), dict)
        else {}
    )
    put_wall = _dash(levels.get("put_wall"))
    flip_low = _dash(levels.get("flip_low"))
    flip_high = _dash(levels.get("flip_high"))
    call_wall = _dash(levels.get("call_wall"))
    distance = (
        f"，距触发位 {spot - level_value:+.1f} 点"
        if spot is not None and level_value is not None
        else ""
    )
    status = _level_phase_guidance(
        phase,
        thesis=thesis,
        direction=direction,
        formal_signal=decision.get("formal_signal") is True,
    )
    source = str(decision.get("spot_source") or "-")
    lines = [
        f"SPX 代理: {_dash(spot)}({source})；ES {_dash(es)}",
        (
            "SPX 稳定结构: Put Wall "
            f"{_band(bands.get('put_wall'), put_wall)} | Flip "
            f"{_band(bands.get('flip_low'), flip_low)}–{_band(bands.get('flip_high'), flip_high)} | "
            f"Call Wall {_band(bands.get('call_wall'), call_wall)}"
        ),
        *(
            [
                "ES 等价值位: Put Wall "
                f"{_dash(es_levels.get('put_wall'))} | Flip "
                f"{_dash(es_levels.get('flip_low'))}–{_dash(es_levels.get('flip_high'))} | "
                f"Call Wall {_dash(es_levels.get('call_wall'))}"
            ]
            if es_levels
            else []
        ),
        _level_position_line(spot, levels),
        f"关键位决策: {phase.upper()}，{kind} {level}{distance}；{status}",
    ]
    expiry = str(decision.get("expiry") or "-")
    level_source = str(decision.get("level_source") or "-")
    if level_source != "-":
        source_label = (
            "上一有效 RTH 冻结 OI/GEX"
            if "frozen" in level_source
            else "实时 OI/GEX"
            if "live" in level_source
            else level_source
        )
        lines.append(f"结构口径: {source_label}（expiry={expiry}）")
    return lines


def _band(value: object, fallback: str) -> str:
    if not isinstance(value, dict):
        return fallback
    return f"{_dash(value.get('low'))}–{_dash(value.get('high'))}"


def _level_position_line(spot: float | None, levels: dict[str, Any]) -> str:
    if spot is None:
        return "位置判断: SPX 代理不可用"
    parts: list[str] = []
    for key, label in (
        ("put_wall", "Put Wall"),
        ("flip_low", "Flip Low"),
        ("call_wall", "Call Wall"),
    ):
        level = finite_float(levels.get(key))
        if level is None:
            continue
        delta = spot - level
        relation = "高于" if delta >= 0 else "低于"
        parts.append(f"{relation}{label} {abs(delta):.1f}点")
    return "位置判断: " + ("；".join(parts) if parts else "关键位不可用")


def _level_phase_guidance(
    phase: str,
    *,
    thesis: str,
    direction: str,
    formal_signal: bool,
) -> str:
    if formal_signal:
        return f"正式信号 {thesis}/{direction} 已确认"
    return {
        "far": "远离触发区，不追单",
        "approaching": "正在接近，尚未完成关键位测试",
        "testing": "正在测试，等待突破或拒绝路径互斥",
        "break_pending": "突破候选，等待持续时间与 ES 同向确认",
        "reject_pending": "拒绝候选，等待持续时间与 ES 同向确认",
        "accepted": "方向已接受，等待回踩",
        "rejected": "拒绝已成立，等待回踩",
        "retest": "正在回踩，等待最终确认",
        "invalidated": "本轮已失效，等待价格离开后重新测试",
        "expired": "本轮已过期，等待价格离开后重新测试，不追单",
    }.get(phase, "观察中，尚未形成正式信号")
