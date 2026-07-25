"""Bounded LLM and deterministic text views of the convexity idea packet."""

from __future__ import annotations

from typing import Any, Mapping

from spx_spark.analytics.options.pricing import finite_float


def compact_convexity_idea_radar(value: object) -> dict[str, Any] | None:
    """Bound the packet before it enters a one-shot LLM prompt."""

    if not isinstance(value, Mapping):
        return None
    boundary_tests = _mapping(value.get("boundary_tests"))
    option_evidence = _mapping(value.get("option_evidence"))
    return {
        "schema_version": value.get("schema_version"),
        "status": value.get("status"),
        "mode": value.get("mode"),
        "action_authority": "none",
        "mandate": value.get("mandate"),
        "spot": value.get("spot"),
        "gth_prior": value.get("gth_prior"),
        "destination_map": value.get("destination_map"),
        "market_state": value.get("market_state"),
        "volatility_context": value.get("volatility_context"),
        "levels": value.get("levels"),
        "boundary_tests": {
            "lower": boundary_tests.get("lower"),
            "upper": boundary_tests.get("upper"),
            "active_event": boundary_tests.get("active_event"),
            "risk_neutral_wall_probabilities": boundary_tests.get(
                "risk_neutral_wall_probabilities"
            ),
        },
        "option_evidence": {
            "call": option_evidence.get("call"),
            "put": option_evidence.get("put"),
        },
        "hypotheses": value.get("hypotheses"),
        "tensions": value.get("tensions"),
        "data_quality": value.get("data_quality"),
        "semantics": value.get("semantics"),
    }


def render_convexity_idea_radar_lines(payload: Mapping[str, Any]) -> list[str]:
    """Render the deterministic facts the LLM must not reinterpret."""

    radar = payload.get("convexity_idea_radar")
    if not isinstance(radar, Mapping):
        return []
    mandate = _mapping(radar.get("mandate"))
    remaining = mandate.get("minutes_to_hard_exit")
    clock = (
        f"剩余 {int(remaining)} 分钟"
        if mandate.get("phase") == "rth_active" and isinstance(remaining, int | float)
        else str(mandate.get("phase") or "-")
    )
    lines = [
        f"凸性雷达  0DTE Call/Put · 13:00 ET 硬退出 · {clock} · "
        "只读假设，不是方向信号"
    ]
    if radar.get("status") in {"closed", "inactive"}:
        lines.append("凸性雷达已停止新想法；不延用上下分支、skew 证据或旧合约。")
        return lines

    destination = _mapping(radar.get("destination_map"))
    p10 = _number(destination.get("p10"))
    median = _number(destination.get("median"))
    p90 = _number(destination.get("p90"))
    if p10 is not None and median is not None and p90 is not None:
        lines.append(
            f"目的地图  0DTE {destination.get('terminal_time_et') or '-'}期权隐含 "
            "P10/中位/P90 "
            f"{p10:.2f}/{median:.2f}/{p90:.2f} · "
            f"{destination.get('quality') or '-'} · 风险中性而非真实胜率"
        )
    else:
        lines.append("目的地图  不可用；不得由墙位或 LLM 补造收盘区间/概率")

    volatility_text = _volatility_text(_mapping(radar.get("volatility_context")))
    if volatility_text:
        lines.append(f"波动估值  {volatility_text}；Greeks/IV 不是方向 Alpha")

    tests = _mapping(radar.get("boundary_tests"))
    lower = _mapping(tests.get("lower"))
    upper = _mapping(tests.get("upper"))
    if lower.get("level") is not None or upper.get("level") is not None:
        lines.append(
            f"边界分叉  下测 {_level_text(lower)}：拒绝→Call / 接受→Put；"
            f"上测 {_level_text(upper)}：拒绝→Put / 接受→Call；"
            "均需状态机确认"
        )

    probabilities = _mapping(tests.get("risk_neutral_wall_probabilities"))
    probability_text = _wall_probability_text(
        _mapping(probabilities.get("horizons")),
        lower_name=str(lower.get("name") or ""),
        upper_name=str(upper.get("name") or ""),
    )
    if probability_text:
        lines.append(f"墙触达启发  {probability_text}；风险中性反射近似，不是真实概率")

    evidence = _mapping(radar.get("option_evidence"))
    lines.append(
        "相对价值证据  "
        f"Call {_evidence_text(_mapping(evidence.get('call')))}；"
        f"Put {_evidence_text(_mapping(evidence.get('put')))}；"
        "无局部 skew 边际不等于没有方向机会"
    )
    return lines


def _wall_probability_text(
    horizons: Mapping[str, Any],
    *,
    lower_name: str,
    upper_name: str,
) -> str | None:
    def side_text(name: str) -> str | None:
        if not name:
            return None
        values: list[str] = []
        observed = False
        for horizon in ("15m", "30m", "60m"):
            row = _mapping(_mapping(horizons.get(horizon)).get(name))
            probability = _number(row.get("prob_touch"))
            if probability is None or row.get("strategy_usable") is not True:
                values.append("-")
                continue
            observed = True
            values.append(f"{probability * 100:.2f}%")
        return "/".join(values) if observed else None

    lower = side_text(lower_name)
    upper = side_text(upper_name)
    if lower is None and upper is None:
        return None
    return f"下 15/30/60m {lower or '-/-/-'}；上 15/30/60m {upper or '-/-/-'}"


def _volatility_text(value: Mapping[str, Any]) -> str | None:
    iv_0dte = _number(value.get("atm_iv_0dte"))
    iv_1dte = _number(value.get("atm_iv_1dte"))
    put_skew = _number(value.get("put_skew_25d_0dte"))
    call_skew = _number(value.get("call_skew_25d_0dte"))
    range_ratio = _number(value.get("same_time_range_ratio"))
    parts: list[str] = []
    if iv_0dte is not None or iv_1dte is not None:
        parts.append(
            "ATM IV 0/1DTE "
            f"{iv_0dte * 100:.2f}%" if iv_0dte is not None else "ATM IV 0/1DTE -"
        )
        parts[-1] += f"/{iv_1dte * 100:.2f}%" if iv_1dte is not None else "/-"
    if put_skew is not None or call_skew is not None:
        put_text = f"{put_skew * 100:+.2f}%" if put_skew is not None else "-"
        call_text = f"{call_skew * 100:+.2f}%" if call_skew is not None else "-"
        parts.append(f"25Δ P/C skew {put_text}/{call_text}")
    if range_ratio is not None:
        parts.append(f"同刻区间比 {range_ratio:.2f}x")
    return " · ".join(parts) or None


def _level_text(value: Mapping[str, Any]) -> str:
    level = _number(value.get("level"))
    if level is None:
        return "不可用"
    labels = {
        "put_wall": "Put Wall",
        "flip_low": "Flip Low",
        "flip_high": "Flip High",
        "call_wall": "Call Wall",
    }
    name = str(value.get("name") or "")
    return f"{labels.get(name, name or '-')} {level:.2f}"


def _evidence_text(value: Mapping[str, Any]) -> str:
    status = str(value.get("edge_status") or "unknown")
    if status == "observed_local_skew_edge":
        vertical = _mapping(value.get("vertical"))
        return (
            f"局部skew边际 {float(vertical.get('edge_points') or 0):.2f}点"
            f"（借记 {float(vertical.get('executable_debit') or 0):.2f}）"
        )
    if status == "not_observed":
        return "未发现可执行局部skew边际"
    return "证据不可用"


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    return finite_float(value)
