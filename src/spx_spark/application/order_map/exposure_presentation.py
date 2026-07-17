"""Human-readable SPXW 0DTE exposure map presentation."""

from __future__ import annotations

from typing import Any

from spx_spark.analytics.options.pricing import finite_float


def exposure_strike_lines(payload: dict[str, Any]) -> list[str]:
    frame = payload.get("option_structure_frame")
    exposure = frame.get("exposure") if isinstance(frame, dict) else None
    if not isinstance(exposure, dict):
        return []
    rows = [row for row in exposure.get("key_strikes") or [] if isinstance(row, dict)]
    if not rows:
        return []

    lines = [
        _aggregate_line("OI代理", exposure.get("oi_weighted")),
        _aggregate_line("成交代理", exposure.get("volume_weighted")),
        "| SPX Strike | 位置 | CΔ / PΔ | CΓ / PΓ | OI GEX净/绝 · DEX净/绝 | 量 GEX净/绝 · DEX净/绝 |",
        "| ---: | --- | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        roles = "·".join(str(item) for item in row.get("roles") or []) or "暴露集中"
        distance = finite_float(row.get("distance_points"))
        location = f"{roles}　{distance:+.1f}点" if distance is not None else roles
        lines.append(
            "| "
            + " | ".join(
                (
                    _strike(row.get("strike")),
                    location,
                    f"{_delta(row.get('call_delta'))} / {_delta(row.get('put_delta'))}",
                    f"{_gamma(row.get('call_gamma'))} / {_gamma(row.get('put_gamma'))}",
                    _weighted_pair(row.get("oi_weighted")),
                    _weighted_pair(row.get("volume_weighted")),
                )
            )
            + " |"
        )
    lines.append(
        "> GEX 使用 Call正/Put负的 OI/成交代理；DEX 为合约 Delta 加权 house proxy。净/绝用于看方向偏置与集中度，均不是 dealer 实仓。"
    )
    return lines


def _aggregate_line(label: str, value: Any) -> str:
    aggregate = value if isinstance(value, dict) else {}
    ratio = finite_float(aggregate.get("net_gamma_ratio"))
    dex_ratio = finite_float(aggregate.get("net_dex_ratio_proxy"))
    return (
        f"{label}　GEX净/绝 {_compact(aggregate.get('net_gex'))}/"
        f"{_compact(aggregate.get('abs_gex'))}"
        f"（{_percent(ratio)}）　DEX净/绝 {_compact(aggregate.get('net_dex_proxy'))}/"
        f"{_compact(aggregate.get('abs_dex_proxy'))}（{_percent(dex_ratio)}）"
    )


def _weighted_pair(value: Any) -> str:
    weighted = value if isinstance(value, dict) else {}
    return (
        f"{_compact(weighted.get('net_gex'))}/{_compact(weighted.get('abs_gex'))} · "
        f"{_compact(weighted.get('net_dex_proxy'))}/{_compact(weighted.get('abs_dex_proxy'))}"
    )


def _strike(value: Any) -> str:
    parsed = finite_float(value)
    if parsed is None:
        return "-"
    return f"{parsed:.1f}".removesuffix(".0")


def _delta(value: Any) -> str:
    parsed = finite_float(value)
    return "-" if parsed is None else f"{parsed:+.2f}"


def _gamma(value: Any) -> str:
    parsed = finite_float(value)
    if parsed is None:
        return "-"
    magnitude = abs(parsed)
    if magnitude >= 0.01:
        return f"{parsed:.3f}"
    if magnitude >= 0.001:
        return f"{parsed:.4f}"
    return f"{parsed:.1e}"


def _compact(value: Any) -> str:
    parsed = finite_float(value)
    if parsed is None:
        return "-"
    magnitude = abs(parsed)
    for scale, suffix in ((1_000_000_000, "B"), (1_000_000, "M"), (1_000, "K")):
        if magnitude >= scale:
            return f"{parsed / scale:+.1f}{suffix}"
    return f"{parsed:+.1f}"


def _percent(value: float | None) -> str:
    return "-" if value is None else f"{value:+.0%}"
