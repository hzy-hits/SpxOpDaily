"""Bounded, non-authoritative Spring Gamma v3 report presentation."""

from __future__ import annotations

from typing import Any

from spx_spark.analytics.options.pricing import finite_float
from spx_spark.application.order_map.render import (
    render_research_only_template as _render_research_only_template,
)


SPRING_GAMMA_V3_SHADOW_SYSTEM_RULE = (
    "Spring Gamma v3 的方向分数未校准，墙触达概率仅为风险中性启发式，"
    "Shadow 无方向/执行权限；"
    "若输入存在该 Shadow，必须逐字保留模板中的确定性摘要行，"
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

    shadow = payload.get("spring_gamma_v3_shadow")
    if not isinstance(shadow, dict):
        return None
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

    return (
        f"Spring Gamma v3 Shadow  {status} · {' · '.join(details)}；"
        "方向分数未校准；墙触达概率为风险中性启发式；无方向/执行权限"
    )


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
    compact["semantics"] = (
        "direction_score_uncalibrated_wall_probability_risk_neutral_heuristic_"
        "no_direction_or_execution_authority"
    )
    return compact


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
