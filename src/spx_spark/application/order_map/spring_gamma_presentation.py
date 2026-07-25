"""Bounded, non-authoritative Spring Gamma v3 report presentation."""

from __future__ import annotations

from typing import Any

from spx_spark.analytics.options.pricing import finite_float
from spx_spark.application.order_map.render import (
    render_research_only_template as _render_research_only_template,
)


SPRING_GAMMA_V3_SHADOW_SYSTEM_RULE = (
    "Spring Gamma v3 的方向分数未校准，墙触达概率仅为风险中性启发式，"
    "Shadow 与 RTH 15分钟状态窗均无方向/执行权限；"
    "若输入存在该 Shadow 或状态窗，必须逐字保留模板中的确定性摘要行，"
    "不得据此修改生产 guidance、候选、裁决、限价或下单动作。"
)


def render_research_only_template(
    payload: dict[str, Any],
    *,
    title: str = "市场状态",
) -> str:
    """Add the read-only v3 projection to the GTH status template."""

    rendered = _render_research_only_template(payload, title=title)
    shadow_line = spring_gamma_v3_shadow_line(payload)
    if shadow_line is None:
        return rendered
    lines = rendered.splitlines()
    insert_at = next(
        (index for index, line in enumerate(lines) if line.startswith("执行限制:")),
        len(lines),
    )
    lines.insert(insert_at, shadow_line)
    return "\n".join(lines)


def spring_gamma_v3_shadow_line(payload: dict[str, Any]) -> str | None:
    """Render one bounded Spring Gamma v3 status line."""

    state_window_line = _rth_state_window_line(payload.get("spring_gamma_v3_state_window"))
    shadow = payload.get("spring_gamma_v3_shadow")
    if not isinstance(shadow, dict):
        return state_window_line
    status = str(shadow.get("status") or "unknown").strip().upper()
    direction = shadow.get("direction")
    direction_payload = direction if isinstance(direction, dict) else {}
    decision = direction_payload.get("decision") if direction_payload else direction
    decision_label = {
        "up": "偏多",
        "down": "偏空",
        "flat": "中性",
        "neutral": "中性",
        "abstain": "弃权",
    }.get(str(decision or "").strip().lower(), "未知")
    details = [f"方向诊断 {decision_label}"]
    diagnostic = str(direction_payload.get("diagnostic_es_direction") or "").strip().lower()
    if str(decision or "").strip().lower() == "abstain" and diagnostic in {
        "up",
        "down",
    }:
        diagnostic_label = "偏多" if diagnostic == "up" else "偏空"
        details.append(f"原始 ES 诊断 {diagnostic_label}（仅诊断）")

    score = _finite_shadow_value(
        direction_payload.get("composite_score"),
        direction_payload.get("score"),
        shadow.get("direction_score"),
    )
    if score is not None:
        details.append(f"方向分数 {score:.2f}")

    wall_probability = _spring_gamma_wall_probability(shadow)
    if wall_probability is not None:
        probability, horizon, level_name = wall_probability
        target = " ".join(value for value in (horizon, level_name) if value)
        details.append(f"墙触达概率 {probability:.2f}" + (f"（{target}）" if target else ""))
    if status == "ABSTAIN":
        reasons = shadow.get("abstain_reasons")
        primary_reason = (
            str(reasons[0]).strip() if isinstance(reasons, list) and reasons else "未提供"
        )
        details.append(f"首要原因 {' '.join(primary_reason.split())}")

    summary = (
        f"Spring Gamma v3 Shadow  {status} · {' · '.join(details)}；"
        "方向分数未校准；墙触达概率为风险中性启发式；无方向/执行权限"
    )
    state_lines = _rth_market_state_lines(shadow)
    lines = [
        *([state_window_line] if state_window_line else []),
        *state_lines,
        summary,
    ]
    return "\n".join(lines)


def spring_gamma_v3_writer_summary(shadow: object) -> dict[str, Any] | None:
    """Return only the small read-only subset allowed into writer prompts."""

    if not isinstance(shadow, dict):
        return None
    direction = shadow.get("direction")
    direction_payload = direction if isinstance(direction, dict) else {}
    abstain_reasons = shadow.get("abstain_reasons")
    compact: dict[str, Any] = {
        key: shadow.get(key)
        for key in (
            "schema_version",
            "status",
            "as_of",
            "session",
            "expiry",
            "calibration_status",
            "direction_authority",
            "action_authority",
            "actionable",
            "automatic_ordering",
            "abstain",
        )
    }
    compact["direction"] = {
        key: (
            round(float(direction_payload.get(key)), 2)
            if _finite_shadow_value(direction_payload.get(key)) is not None
            else direction_payload.get(key)
        )
        for key in (
            "decision",
            "diagnostic_es_direction",
            "composite_score",
            "p_up",
            "p_down",
        )
        if key in direction_payload
    }
    wall_probability = _spring_gamma_wall_probability(shadow)
    if wall_probability is not None:
        probability, horizon, level_name = wall_probability
        compact["wall_probability"] = round(probability, 2)
        compact["wall_probability_horizon"] = horizon
        compact["wall_probability_level"] = level_name
    if isinstance(abstain_reasons, list):
        compact["abstain_reasons"] = [str(reason) for reason in abstain_reasons[:5]]
    if (market_state := _compact_rth_market_state(shadow)) is not None:
        compact["rth_market_state"] = market_state
    option_overlay = shadow.get("option_overlay")
    if isinstance(option_overlay, dict):
        compact["option_overlay"] = {
            "status": option_overlay.get("status"),
            "market_state_independent": option_overlay.get("market_state_independent"),
            "reasons": [
                str(reason) for reason in option_overlay.get("reasons", [])[:5] if str(reason)
            ]
            if isinstance(option_overlay.get("reasons"), list)
            else [],
        }
    compact["semantics"] = (
        "direction_score_uncalibrated_wall_probability_risk_neutral_heuristic_"
        "no_direction_or_execution_authority"
    )
    return compact


def _rth_market_state_lines(shadow: dict[str, Any]) -> list[str]:
    if str(shadow.get("session") or "").lower() != "rth":
        return []
    state = _compact_rth_market_state(shadow)
    if state is None:
        return []
    state_name = str(state.get("state") or "UNCERTAIN")
    direction_score = _finite_shadow_value(state.get("D"))
    quality = state.get("Q") if isinstance(state.get("Q"), dict) else {}
    volatility = state.get("V") if isinstance(state.get("V"), dict) else {}
    availability = (
        state.get("input_availability") if isinstance(state.get("input_availability"), dict) else {}
    )
    er = _finite_shadow_value(quality.get("efficiency_ratio"))
    crosses = quality.get("vwap_cross_count")
    range_ratio = _finite_shadow_value(volatility.get("same_time_range_ratio"))
    breadth = _finite_shadow_value(state.get("breadth_above_vwap"))
    available = availability.get("available_count")
    required = availability.get("required_count")
    metrics = [
        f"D {direction_score:+.2f}/10" if direction_score is not None else "D -",
        f"ER {er:.2f}" if er is not None else "ER -",
        f"VWAP穿越 {int(crosses)}" if isinstance(crosses, int) else "VWAP穿越 -",
        f"Range {range_ratio:.2f}x" if range_ratio is not None else "Range -",
        f"宽度 {breadth:.2%}" if breadth is not None else "宽度 -",
        (
            f"数据 {int(available)}/{int(required)}"
            if isinstance(available, int) and isinstance(required, int)
            else "数据 -"
        ),
    ]
    wait, trigger, expression = _state_playbook(
        state_name,
        option_overlay=shadow.get("option_overlay"),
    )
    lines = [
        f"RTH状态 Shadow  {state_name} · {' · '.join(metrics)}　只读",
    ]
    moving_average_line = _moving_average_line(state.get("moving_averages"))
    if moving_average_line is not None:
        lines.append(moving_average_line)
    return [
        *lines,
        f"状态路径  等待位置：{wait}",
        f"状态路径  触发确认：{trigger}",
        f"状态路径  期权结构：{expression}",
    ]


def _moving_average_line(value: object) -> str | None:
    if not isinstance(value, dict):
        return None
    price = _finite_shadow_value(value.get("price"))
    sma20 = _finite_shadow_value(value.get("sma20"))
    sma50 = _finite_shadow_value(value.get("sma50"))
    if price is None and sma20 is None and sma50 is None:
        return None
    spx20 = _finite_shadow_value(value.get("spx_equivalent_sma20"))
    spx50 = _finite_shadow_value(value.get("spx_equivalent_sma50"))
    projection = (
        f" · SPX等价值 MA20/50 {_shadow_dash(spx20)}/{_shadow_dash(spx50)}"
        if spx20 is not None or spx50 is not None
        else ""
    )
    precision = " · 贴线区不确认突破" if value.get("spx_projection_near_line") is True else ""
    return (
        f"ES 5m均线  P/MA20/MA50 {_shadow_dash(price)}/{_shadow_dash(sma20)}/"
        f"{_shadow_dash(sma50)} · {value.get('relation') or '-'}{projection}{precision}"
        "（基差投影，非SPX自身均线）　只读"
    )


def _shadow_dash(value: float | None) -> str:
    return f"{value:.2f}" if value is not None else "-"


def _rth_state_window_line(window: object) -> str | None:
    if (
        not isinstance(window, dict)
        or window.get("schema_version") != "spring_gamma_v3_state_window.v1"
        or window.get("session") != "rth"
    ):
        return None
    sample_count = window.get("sample_count")
    total_slots = window.get("five_minute_slot_count")
    sample_text = int(sample_count) if isinstance(sample_count, int) else 0
    slot_text = int(total_slots) if isinstance(total_slots, int) else 0
    states = window.get("states")
    counts = window.get("counts")
    slot_counts = window.get("five_minute_slot_counts")
    state_names = [str(state) for state in states if str(state)] if isinstance(states, list) else []
    details: list[str] = []
    if isinstance(counts, dict):
        for state in state_names:
            count = counts.get(state)
            slots = slot_counts.get(state) if isinstance(slot_counts, dict) else None
            if isinstance(count, int) and isinstance(slots, int):
                details.append(f"{state} {count}样本/{slots}档")
            elif isinstance(count, int):
                details.append(f"{state} {count}样本")
    if not details:
        details.append("无有效状态样本")
    latest_state = str(window.get("latest_state") or "-")
    return (
        f"RTH状态15m  {' · '.join(details)} · 最新 {latest_state} · "
        f"覆盖 {sample_text}样本/{slot_text}档　只读"
    )


def _compact_rth_market_state(shadow: dict[str, Any]) -> dict[str, Any] | None:
    source = shadow.get("rth_market_state")
    if not isinstance(source, dict) or source.get("schema_version") != "market_state_5m.v1":
        return None
    quality = source.get("Q") if isinstance(source.get("Q"), dict) else {}
    volatility = source.get("V") if isinstance(source.get("V"), dict) else {}
    components = (
        source.get("direction_components")
        if isinstance(source.get("direction_components"), dict)
        else {}
    )
    breadth = _finite_shadow_value(
        source.get("breadth_above_vwap"),
        components.get("breadth_above_vwap_ratio"),
        (
            source.get("input_lineage", {}).get("values", {}).get("breadth_above_vwap")
            if isinstance(source.get("input_lineage"), dict)
            and isinstance(source.get("input_lineage", {}).get("values"), dict)
            else None
        ),
    )
    input_lineage = (
        source.get("input_lineage") if isinstance(source.get("input_lineage"), dict) else {}
    )
    input_diagnostics = (
        input_lineage.get("diagnostics")
        if isinstance(input_lineage.get("diagnostics"), dict)
        else {}
    )
    raw_moving = (
        input_diagnostics.get("moving_averages")
        if isinstance(input_diagnostics.get("moving_averages"), dict)
        else {}
    )
    moving_averages = {
        key: raw_moving.get(key)
        for key in (
            "status",
            "timeframe",
            "session",
            "price",
            "sma20",
            "sma50",
            "distance_to_sma20_points",
            "distance_to_sma50_points",
            "relation",
            "latest_bar_end",
            "contract_identity",
            "es_spx_basis_points",
            "basis_contract_identity",
            "basis_contract_identity_matches_sma",
            "spx_equivalent_sma20",
            "spx_equivalent_sma50",
            "projection_method",
            "spx_projection_near_line",
            "spx_projection_near_line_tolerance_points",
            "action_authority",
        )
        if key in raw_moving
    }
    return {
        "schema_version": source.get("schema_version"),
        "rule_version": source.get("rule_version"),
        "state": source.get("state"),
        "status": source.get("status"),
        "D": source.get("D"),
        "Q": {
            "quality": quality.get("quality"),
            "efficiency_ratio": quality.get("efficiency_ratio"),
            "vwap_cross_count": quality.get("vwap_cross_count"),
        },
        "V": {
            "state": volatility.get("state"),
            "same_time_range_ratio": volatility.get("same_time_range_ratio"),
        },
        "breadth_above_vwap": breadth,
        "moving_averages": moving_averages,
        "input_availability": source.get("input_availability"),
        "pin_proxy_candidate": source.get("pin_proxy_candidate"),
        "action_authority": "none",
        "actionable": False,
    }


def _state_playbook(
    state: str,
    *,
    option_overlay: object,
) -> tuple[str, str, str]:
    overlay = option_overlay if isinstance(option_overlay, dict) else {}
    option_ready = str(overlay.get("status") or "") == "ready"
    if state == "TREND_UP":
        wait = "VWAP/ORH与上涨腿回撤区（本层未计算回撤比例）"
        trigger = "仅记录外部level lifecycle确认；D≥6、ER>0.45且VWAP穿越≤2"
        expression = (
            "方向映射Call；具体价差仅以独立实时双腿Shadow为准"
            if option_ready
            else "期权overlay不可用；不映射价差"
        )
    elif state == "TREND_DOWN":
        wait = "VWAP/ORL与下跌腿反弹区（本层未计算反弹比例）"
        trigger = "仅记录外部level lifecycle确认；D≤-6、ER>0.45且VWAP穿越≤2"
        expression = (
            "方向映射Put；具体价差仅以独立实时双腿Shadow为准"
            if option_ready
            else "期权overlay不可用；不映射价差"
        )
    elif state == "LOW_VOL_RANGE":
        wait = "实时墙位边缘（只读观察）"
        trigger = "需外部墙位拒绝/收回；Pin仍缺整数strike与跨式衰减确认"
        expression = "本层不选择期权结构"
    elif state == "HIGH_VOL_CHOP":
        wait = "高波动低效率环境（只读标签）"
        trigger = "ER或VWAP穿越改变后重新评分"
        expression = "本层不选择期权结构"
    else:
        wait = "等待8项输入完整并匹配明确状态"
        trigger = "未确认"
        expression = "本层不选择期权结构"
    return wait, trigger, expression


def _finite_shadow_value(*values: object) -> float | None:
    for value in values:
        number = finite_float(value)
        if number is not None:
            return number
    return None


def _spring_gamma_wall_probability(
    shadow: dict[str, Any],
) -> tuple[float, str | None, str | None] | None:
    wall_contract = shadow.get("wall_probability")
    direction = shadow.get("direction")
    direction_payload = direction if isinstance(direction, dict) else {}
    decision = direction_payload.get("decision") if direction_payload else direction
    normalized_decision = str(decision or "").strip().lower()
    if normalized_decision not in {"up", "down"}:
        if (
            normalized_decision != "abstain"
            or not isinstance(wall_contract, dict)
            or str(wall_contract.get("probability_status") or "").lower()
            not in {"ready", "partial"}
        ):
            return None
        normalized_decision = str(wall_contract.get("direction") or "").strip().lower()
        if normalized_decision not in {"up", "down"}:
            return None
    direct_containers = (
        shadow.get("direction"),
        shadow.get("risk"),
        shadow.get("level_gate"),
        shadow.get("opportunity"),
    )
    if not isinstance(wall_contract, dict):
        direct = finite_float(wall_contract)
        if direct is not None:
            return direct, None, None
    else:
        direct = _finite_shadow_value(
            wall_contract.get("value"),
            wall_contract.get("probability"),
            wall_contract.get("touch_probability"),
            wall_contract.get("touch_probability_2x_reflection"),
        )
        if direct is not None:
            return direct, None, None
    for container in direct_containers:
        if not isinstance(container, dict):
            continue
        value = container.get("wall_probability")
        if isinstance(value, dict):
            value = next(
                (
                    value.get(key)
                    for key in (
                        "value",
                        "probability",
                        "touch_probability",
                        "touch_probability_2x_reflection",
                    )
                    if value.get(key) is not None
                ),
                None,
            )
        number = finite_float(value)
        if number is not None:
            return number, None, None

    if not isinstance(wall_contract, dict):
        return None
    probabilities = wall_contract.get("wall_probabilities")
    if not isinstance(probabilities, dict):
        return None
    path = wall_contract.get("path")
    path_payload = path if isinstance(path, dict) else {}
    spot = _finite_shadow_value(
        path_payload.get("underlier"),
        wall_contract.get("underlier"),
    )
    stable_levels = wall_contract.get("stable_levels")
    stable_level_payload = stable_levels if isinstance(stable_levels, dict) else {}
    candidates: list[tuple[float, float, float, str, str]] = []
    for horizon_key, horizon_rows in probabilities.items():
        if not isinstance(horizon_rows, dict):
            continue
        horizon_number = finite_float(str(horizon_key).strip().lower().removesuffix("m"))
        for level_key, raw_row in horizon_rows.items():
            if not isinstance(raw_row, dict):
                continue
            if str(raw_row.get("status") or "available").lower() != "available":
                continue
            probability = finite_float(raw_row.get("touch_probability_2x_reflection"))
            level = _finite_shadow_value(
                raw_row.get("level"),
                stable_level_payload.get(level_key),
            )
            if probability is None or not 0.0 <= probability <= 1.0:
                continue
            if spot is not None and level is not None:
                signed_distance = level - spot
                if (normalized_decision == "up" and signed_distance < 0) or (
                    normalized_decision == "down" and signed_distance > 0
                ):
                    continue
                distance = abs(signed_distance)
            else:
                distance = float("inf")
            candidates.append(
                (
                    distance,
                    horizon_number if horizon_number is not None else float("inf"),
                    probability,
                    str(horizon_key),
                    str(level_key),
                )
            )
    if not candidates:
        return None
    _, _, probability, horizon_key, level_key = min(candidates)
    horizon_label = horizon_key if horizon_key.lower().endswith("m") else f"{horizon_key}m"
    level_label = {
        "put_wall": "Put Wall",
        "flip_low": "Flip Low",
        "flip_high": "Flip High",
        "call_wall": "Call Wall",
    }.get(level_key, level_key)
    return probability, horizon_label, level_label
