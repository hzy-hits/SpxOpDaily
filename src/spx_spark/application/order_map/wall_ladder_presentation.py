"""Feishu table presentation for priced option wall ladders."""

from __future__ import annotations

from typing import Any

from spx_spark.analytics.options.pricing import finite_float


def _dash(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.1f}".removesuffix(".0")
    return str(value)


def primary_wall_strike(
    payload: dict[str, Any],
    key: str,
    rungs: list[dict[str, Any]],
) -> float | None:
    decision = payload.get("level_decision")
    levels = decision.get("levels") if isinstance(decision, dict) else None
    level_key = "put_wall" if key == "put_walls" else "call_wall"
    stable = finite_float(levels.get(level_key)) if isinstance(levels, dict) else None
    if stable is not None:
        return (
            stable
            if any(
                (strike := finite_float(rung.get("strike"))) is not None
                and abs(strike - stable) < 1e-6
                for rung in rungs
            )
            else None
        )
    return finite_float(rungs[0].get("strike")) if rungs else None


def detail_ladder_lines(payload: dict[str, Any]) -> list[str]:
    ladder = payload.get("wall_ladder") if isinstance(payload.get("wall_ladder"), dict) else {}
    put_rungs = [rung for rung in ladder.get("put_walls") or [] if isinstance(rung, dict)]
    call_rungs = [rung for rung in ladder.get("call_walls") or [] if isinstance(rung, dict)]
    selected: list[tuple[dict[str, Any], str, str]] = []

    def has_pricing(rung: dict[str, Any]) -> bool:
        return (
            finite_float(rung.get("current_mid")) is not None
            and finite_float(rung.get("projected_mid")) is not None
        )

    if put_rungs:
        primary_strike = primary_wall_strike(payload, "put_walls", put_rungs)
        primary = next(
            (
                rung
                for rung in put_rungs
                if finite_float(rung.get("strike")) == primary_strike
            ),
            None,
        )
        support_rungs = sorted(
            [rung for rung in put_rungs if has_pricing(rung)][:3],
            key=lambda rung: -(rung.get("strike") or 0.0),
        )
        secondary_index = 0
        for rung in support_rungs:
            if primary is not None and rung is primary:
                label = "主 Put Wall"
            elif primary is None and rung is put_rungs[0]:
                label = "OI 主峰候选"
            else:
                label = "次级支撑" if secondary_index == 0 else "外侧支撑"
                secondary_index += 1
            selected.append((rung, label, "C"))

    if call_rungs:
        primary_strike = primary_wall_strike(payload, "call_walls", call_rungs)
        primary = next(
            (
                rung
                for rung in call_rungs
                if finite_float(rung.get("strike")) == primary_strike
            ),
            None,
        )
        priced_call_rungs = [rung for rung in call_rungs if has_pricing(rung)]
        underlier = payload.get("underlier") if isinstance(payload.get("underlier"), dict) else {}
        spot = finite_float(underlier.get("price"))

        def call_distance(rung: dict[str, Any]) -> float:
            distance = finite_float(rung.get("distance_points"))
            strike = finite_float(rung.get("strike"))
            if distance is not None:
                return abs(distance)
            if strike is not None and spot is not None:
                return abs(strike - spot)
            return float("inf")

        if priced_call_rungs:
            nearest = min(priced_call_rungs, key=call_distance)
            ranked = (
                primary
                if primary is not None and has_pricing(primary)
                else priced_call_rungs[0]
            )
            secondary = priced_call_rungs[1] if len(priced_call_rungs) > 1 else None
            call_selection = sorted(
                {
                    id(rung): rung for rung in (nearest, ranked, secondary) if rung is not None
                }.values(),
                key=lambda rung: rung.get("strike") or 0.0,
            )
            for rung in call_selection:
                label = (
                    "主 Call Wall"
                    if primary is not None and rung is primary
                    else "Call GEX 主峰候选"
                    if primary is None and rung is call_rungs[0]
                    else "近端 Call GEX"
                    if rung is nearest
                    else "次级 Call GEX"
                )
                selected.append((rung, label, "P"))

    if not selected:
        return []
    rendered = [
        "| SPX 墙位 | 结构 | 合约 | 当前 mid | 触位情景 | 触发后参考 |",
        "| ---: | --- | --- | ---: | ---: | ---: |",
    ]
    for rung, label, default_right in selected:
        strike = rung.get("strike")
        right = str(rung.get("option_right") or default_right)
        option_strike = rung.get("option_strike")
        contract = f"{_dash(option_strike if option_strike is not None else strike)}{right}"
        current = finite_float(rung.get("current_mid"))
        projected = finite_float(rung.get("projected_mid"))
        range_low = finite_float(rung.get("projection_range_low"))
        range_high = finite_float(rung.get("projection_range_high"))
        if range_low is None:
            range_low = projected
        if range_high is None:
            range_high = projected
        timing_capped = rung.get("projection_timing_capped") is True
        quote_executable = rung.get("execution_quote_status") != "range_only"
        quote_reasons = tuple(str(item) for item in rung.get("execution_quote_reasons") or ())
        gate_label = (
            "源分歧"
            if "provider_mid_divergence_exceeded" in quote_reasons
            else "报价门控"
        )
        if not quote_executable:
            bs_range = "暂不估值"
        elif timing_capped and range_high is not None:
            bs_range = f"早触≈{range_high:.2f} / 晚触重算"
        elif range_low is not None and range_high is not None:
            low, high = min(range_low, range_high), max(range_low, range_high)
            bs_range = f"{low:.2f}" if abs(high - low) < 0.005 else f"{low:.2f}–{high:.2f}"
        else:
            bs_range = "-"
        limits = [
            value
            for value in (
                finite_float(rung.get("limit_aggressive")),
                finite_float(rung.get("limit_conservative")),
            )
            if value is not None
        ]
        reference = (
            "触位重算"
            if timing_capped or not quote_executable
            else f"{min(limits):.2f}–{max(limits):.2f}"
            if limits
            else "-"
        )
        current_text = (
            f"{current:.2f}（{gate_label}）"
            if current is not None and not quote_executable
            else f"{current:.2f}"
            if current is not None
            else "-"
        )
        rendered.append(
            f"| {_dash(strike)} | {label} | {contract} | {current_text} | {bs_range} | {reference} |"
            if current is not None and projected is not None
            else f"| {_dash(strike)} | {label} | {contract} | - | - | {reference} |"
        )
    rendered.append(
        "> 触位情景是标的到墙时的早/基准/晚到达估值，不是当前合理价；“晚触重算”表示时间估计已触及上限。"
    )
    return rendered
