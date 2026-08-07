"""Compact, non-authoritative presentation for competing hypotheses."""

from __future__ import annotations

from typing import Any, Mapping

from spx_spark.analytics.options.pricing import finite_float


def compact_convexity_idea_radar(value: object) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    tests, evidence = _mapping(value.get("boundary_tests")), _mapping(value.get("option_evidence"))
    return {
        "schema_version": value.get("schema_version"), "status": value.get("status"),
        "mode": value.get("mode"), "action_authority": "none", "mandate": value.get("mandate"),
        "spot": value.get("spot"), "destination_map": value.get("destination_map"),
        "market_state": value.get("market_state"), "volatility_context": value.get("volatility_context"),
        "levels": value.get("levels"), "hypotheses": value.get("hypotheses"),
        "boundary_tests": {key: tests.get(key) for key in (
            "lower", "upper", "active_event", "risk_neutral_wall_probabilities")},
        "option_evidence": {"call": evidence.get("call"), "put": evidence.get("put")},
        "tensions": value.get("tensions"), "data_quality": value.get("data_quality"),
        "semantics": value.get("semantics"),
    }


def render_convexity_idea_radar_lines(payload: Mapping[str, Any]) -> list[str]:
    radar = _mapping(payload.get("convexity_idea_radar"))
    if not radar:
        return []
    mandate = _mapping(radar.get("mandate"))
    lines = [
        f"凸性假设  0DTE · {mandate.get('phase') or '-'} · 只读、可证伪，不是方向信号"
    ]
    if radar.get("status") in {"closed", "inactive"}:
        return [*lines, "当前 session 不生成新假设；不延用旧合约或旧分支。"]
    destination = _mapping(radar.get("destination_map"))
    values = tuple(_number(destination.get(key)) for key in ("p10", "median", "p90"))
    lines.append(
        f"Q终值 P10/中位/P90 {values[0]:.2f}/{values[1]:.2f}/{values[2]:.2f} · 风险中性"
        if None not in values else "Q终值不可用；不得由墙位或 LLM 补造概率"
    )
    tests = _mapping(radar.get("boundary_tests"))
    lower, upper = _mapping(tests.get("lower")), _mapping(tests.get("upper"))
    if lower.get("level") is not None or upper.get("level") is not None:
        lines.append(
            f"可证伪路径  下测 {_level(lower)}：拒绝/接受；上测 {_level(upper)}：拒绝/接受；均需价格确认"
        )
    evidence = _mapping(radar.get("option_evidence"))
    lines.append(
        f"相对价值  Call {_edge(_mapping(evidence.get('call')))}；"
        f"Put {_edge(_mapping(evidence.get('put')))}；不负责最终候选排名"
    )
    return lines


def _level(value: Mapping[str, Any]) -> str:
    level = _number(value.get("level"))
    return f"{value.get('name') or '-'} {level:.2f}" if level is not None else "不可用"


def _edge(value: Mapping[str, Any]) -> str:
    if value.get("edge_status") == "observed_local_skew_edge":
        vertical = _mapping(value.get("vertical"))
        return f"观察到 {float(vertical.get('edge_points') or 0):.2f}点"
    return "未验证" if value.get("edge_status") == "not_observed" else "不可用"


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    return finite_float(value)
