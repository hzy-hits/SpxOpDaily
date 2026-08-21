"""Human-readable options map rendering."""

from __future__ import annotations

import math
import os
import subprocess
import tempfile
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from html import escape
from pathlib import Path
from zoneinfo import ZoneInfo

from spx_spark.analytics.options.models import OptionsMap
from spx_spark.analytics.options.strategy_payoff import (
    RISK_OBJECTIVE_FORMULA,
    butterfly_payoff,
    iron_condor_payoff,
    vertical_payoff,
)

_EASTERN = ZoneInfo("America/New_York")
_PUT_COLOR = "#2563EB"
_CALL_COLOR = "#EA580C"
_WALL_PUT_COLOR = "#1E3A8A"
_WALL_CALL_COLOR = "#9A3412"
_PROFIT_COLOR = "#16A34A"
_LOSS_COLOR = "#DC2626"
_Q_COLOR = "#7C3AED"
_SPOT_COLOR = "#D97706"
_PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"
_MAX_PNG_BYTES = 2_000_000

SvgToPng = Callable[[Path, Path], None]


def format_number(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "-"
    return f"{value:.{digits}f}"


def print_options_map(options_map: OptionsMap) -> None:
    print(f"Options map as of: {options_map.as_of.isoformat()}")
    print(
        f"Underlier: {format_number(options_map.underlier.price)} "
        f"source={options_map.underlier.source or '-'}"
    )
    if options_map.warnings:
        print("Warnings:")
        for warning in options_map.warnings:
            print(f"- {warning}")
    if not options_map.expiries:
        return
    print("\nExpiry map:")
    headers = [
        "expiry",
        "state",
        "opts",
        "atm",
        "straddle",
        "atm_iv",
        "put_skew",
        "call_skew",
        "zero_g",
        "put_wall",
        "call_wall",
    ]
    rows: list[list[str]] = []
    for item in options_map.expiries:
        rows.append(
            [
                item.expiry,
                item.gamma_state,
                str(item.option_count),
                format_number(item.atm_strike, 0),
                format_number(item.atm_straddle_mid),
                format_number(item.atm_iv, 4),
                format_number(item.put_skew_ratio, 3),
                format_number(item.call_skew_ratio, 3),
                format_number(item.zero_gamma, 0),
                format_number(item.put_wall, 0),
                format_number(item.call_wall, 0),
            ]
        )
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in rows))
        for index in range(len(headers))
    ]
    print(" | ".join(header.ljust(widths[index]) for index, header in enumerate(headers)))
    print("-+-".join("-" * width for width in widths))
    for row in rows:
        print(" | ".join(value.ljust(widths[index]) for index, value in enumerate(row)))


def render_open_interest_mirror_svg(
    exposure_map: Mapping[str, object],
    *,
    window_points: float = 100.0,
) -> str:
    """Render a mobile-readable SPXW put/call open-interest mirror chart."""
    expiry = _front_expiry(exposure_map)
    underlier = _number(_mapping(exposure_map.get("underlier")).get("price"))
    if underlier is None:
        raise ValueError("exposure map has no underlier price")
    if window_points <= 0:
        raise ValueError("window_points must be positive")
    strikes = sorted(
        (_mapping(item) for item in _sequence(expiry.get("strikes"))),
        key=lambda item: _number(item.get("strike")) or 0.0,
        reverse=True,
    )
    if not strikes:
        raise ValueError("exposure map has no strike rows")
    numeric_strikes = [
        strike
        for item in strikes
        if (strike := _number(item.get("strike"))) is not None
    ]
    if not numeric_strikes:
        raise ValueError("exposure map has no numeric strikes")
    atm = min(numeric_strikes, key=lambda strike: abs(strike - underlier))
    visible = [
        item
        for item in strikes
        if (strike := _number(item.get("strike"))) is not None
        and abs(strike - atm) <= window_points
    ]
    if not visible:
        raise ValueError("no strikes fall inside the requested ATM window")

    walls = _mapping(expiry.get("walls"))
    put_walls = _wall_rows(walls, "put_walls")
    call_walls = _wall_rows(walls, "call_walls")
    put_ranks = {_number(row.get("strike")): rank for rank, row in enumerate(put_walls, 1)}
    call_ranks = {_number(row.get("strike")): rank for rank, row in enumerate(call_walls, 1)}

    width = 1200
    center = width / 2
    plot_left = 72.0
    center_gap = 132.0
    put_axis = center - center_gap / 2
    call_axis = center + center_gap / 2
    max_bar_width = put_axis - plot_left - 78.0
    row_height = 31.0
    plot_top = 398.0
    plot_bottom = plot_top + len(visible) * row_height
    height = int(plot_bottom + 164)
    max_oi = max(
        1.0,
        *(
            max(
                _number(item.get("put_open_interest")) or 0.0,
                _number(item.get("call_open_interest")) or 0.0,
            )
            for item in visible
        ),
    )

    as_of = _format_as_of(exposure_map.get("as_of"))
    expiry_label = escape(str(expiry.get("expiry") or "unknown"))
    quality = escape(str(expiry.get("oi_quality") or expiry.get("quality") or "unknown"))
    oi_age = _open_interest_age(expiry)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}">',
        "<style>",
        "text { font-family: 'Droid Sans', 'Noto Sans CJK SC', sans-serif; fill: #172033; }",
        ".muted { fill: #667085; } .small { font-size: 18px; } .body { font-size: 21px; }",
        ".rank { font-size: 18px; font-weight: 700; fill: #FFFFFF; }",
        "</style>",
        f'<rect width="{width}" height="{height}" fill="#F8FAFC"/>',
        '<rect x="34" y="28" width="1132" height="318" rx="24" fill="#FFFFFF" stroke="#E2E8F0"/>',
        '<text x="60" y="78" font-size="34" font-weight="700">SPXW 0DTE Open Interest · ATM Mirror</text>',
        f'<text x="60" y="116" class="body muted">Expiry {expiry_label} · SPX {underlier:,.2f} · ATM {atm:,.0f} · {escape(as_of)}</text>',
        '<text x="60" y="150" class="small muted">Bars = open interest (same scale) · P1/C1 = OI-GEX wall rank</text>',
        f'<text x="60" y="196" font-size="23" font-weight="700" fill="{_PUT_COLOR}">PUT WALLS</text>',
        f'<text x="650" y="196" font-size="23" font-weight="700" fill="{_CALL_COLOR}">CALL WALLS</text>',
    ]
    parts.extend(_wall_summary(put_walls, x=60, y=232, prefix="P", color=_WALL_PUT_COLOR))
    parts.extend(_wall_summary(call_walls, x=650, y=232, prefix="C", color=_WALL_CALL_COLOR))
    parts.extend(
        [
            f'<text x="72" y="382" font-size="22" font-weight="700" fill="{_PUT_COLOR}">PUT OI</text>',
            f'<text x="1128" y="382" font-size="22" font-weight="700" text-anchor="end" fill="{_CALL_COLOR}">CALL OI</text>',
            '<text x="600" y="382" font-size="20" font-weight="700" text-anchor="middle">STRIKE</text>',
        ]
    )

    for index, item in enumerate(visible):
        y = plot_top + index * row_height
        strike = _number(item.get("strike")) or 0.0
        put_oi = max(0.0, _number(item.get("put_open_interest")) or 0.0)
        call_oi = max(0.0, _number(item.get("call_open_interest")) or 0.0)
        put_width = max_bar_width * put_oi / max_oi
        call_width = max_bar_width * call_oi / max_oi
        put_rank = put_ranks.get(strike)
        call_rank = call_ranks.get(strike)
        if strike == atm:
            parts.append(
                f'<rect x="42" y="{y - 3:.1f}" width="1116" height="{row_height:.1f}" '
                'rx="8" fill="#FEF3C7"/>'
            )
        parts.append(
            f'<line x1="72" y1="{y + 24:.1f}" x2="1128" y2="{y + 24:.1f}" '
            'stroke="#E9EEF5" stroke-width="1"/>'
        )
        parts.append(
            f'<rect x="{put_axis - put_width:.1f}" y="{y + 2:.1f}" width="{put_width:.1f}" '
            f'height="20" rx="4" fill="{_WALL_PUT_COLOR if put_rank else _PUT_COLOR}" '
            f'opacity="{1.0 if put_rank else 0.72}"/>'
        )
        parts.append(
            f'<rect x="{call_axis:.1f}" y="{y + 2:.1f}" width="{call_width:.1f}" '
            f'height="20" rx="4" fill="{_WALL_CALL_COLOR if call_rank else _CALL_COLOR}" '
            f'opacity="{1.0 if call_rank else 0.72}"/>'
        )
        parts.append(
            f'<text x="{put_axis - put_width - 8:.1f}" y="{y + 19:.1f}" class="small" '
            f'text-anchor="end">{put_oi:,.0f}</text>'
        )
        parts.append(
            f'<text x="{call_axis + call_width + 8:.1f}" y="{y + 19:.1f}" class="small">{call_oi:,.0f}</text>'
        )
        parts.append(
            f'<text x="{center:.1f}" y="{y + 20:.1f}" font-size="20" font-weight="700" '
            f'text-anchor="middle">{strike:,.0f}</text>'
        )
        if put_rank:
            parts.append(_rank_badge(put_axis - 42, y + 2, f"P{put_rank}", _WALL_PUT_COLOR))
        if call_rank:
            parts.append(_rank_badge(call_axis + 10, y + 2, f"C{call_rank}", _WALL_CALL_COLOR))

    footer_y = plot_bottom + 50
    parts.extend(
        [
            f'<text x="60" y="{footer_y:.1f}" class="small muted">OI quality: {quality}{escape(oi_age)}</text>',
            f'<text x="60" y="{footer_y + 34:.1f}" class="small muted">ATM window: ±{window_points:,.0f} points; off-window Top 3 walls remain in the header.</text>',
            f'<text x="60" y="{footer_y + 68:.1f}" class="small muted">OI/GEX are structural proxies; dealer position sign is unknown. Not a direction or trade signal.</text>',
            "</svg>",
        ]
    )
    return "\n".join(parts)


def render_strategy_risk_svg(strategy_decision: Mapping[str, object]) -> str:
    """Render one decision-owned strategy risk sheet without trading authority."""
    facts = _mapping(strategy_decision.get("market_facts"))
    spot = _number(_mapping(facts.get("spot")).get("spx"))
    structure, source = _strategy_risk_structure(strategy_decision)
    strategy_type = str(structure.get("strategy_type") or "NO_SUPPORTED_STRUCTURE")
    path_distribution = _strategy_path_distribution(structure)
    objective = _mapping(path_distribution.get("risk_objective"))
    structure_facts = _mapping(facts.get("structure"))
    q_rows = _q_mass_rows(structure_facts.get("q_local_mass_5pt"))
    domain = _strategy_price_domain(
        structure, structure_facts=structure_facts, q_rows=q_rows, spot=spot
    )
    authority = _strategy_risk_authority(strategy_decision, source=source)
    as_of = _format_as_of(
        strategy_decision.get("available_at") or strategy_decision.get("decision_at")
    )
    objective_points = _number(objective.get("objective_points"))
    shadow_choice = str(objective.get("shadow_choice") or "UNAVAILABLE")
    strategy_label = {
        "CALL_DEBIT_VERTICAL": "Call 方向价差", "PUT_DEBIT_VERTICAL": "Put 方向价差",
        "CALL_BUTTERFLY": "Call 蝶式", "PUT_BUTTERFLY": "Put 蝶式",
        "IRON_CONDOR": "铁鹰"}.get(strategy_type, "暂无可展示结构")
    choice_label = {"STRUCTURE": "结构可研究", "NO_TRADE": "暂不交易"}.get(shadow_choice, "结论暂缺")
    width, height = 1200, 1450
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}">',
        "<style>",
        "text { font-family: 'WenQuanYi Zen Hei', sans-serif; fill: #172033; }",
        ".muted { fill: #667085; } .small { font-size: 17px; } .body { font-size: 20px; }",
        ".label { font-size: 18px; font-weight: 700; } .metric { font-size: 24px; font-weight: 700; }",
        "</style>",
        f'<rect width="{width}" height="{height}" fill="#F8FAFC"/>',
        '<rect x="34" y="28" width="1132" height="282" rx="24" fill="#FFFFFF" stroke="#E2E8F0"/>',
        '<text x="60" y="76" font-size="34" font-weight="700">SPXW 交易策略风险</text>',
        f'<text x="60" y="112" class="body muted">{escape(strategy_label)} · '
        f'SPX {spot:,.2f} · {escape(as_of)}</text>' if spot is not None else
        f'<text x="60" y="112" class="body muted">{escape(strategy_label)} · '
        f'SPX 暂缺 · {escape(as_of)}</text>',
        f'<rect x="820" y="54" width="314" height="42" rx="12" fill="{_authority_background(source)}"/>',
        f'<text x="977" y="82" class="label" text-anchor="middle" fill="{_authority_color(source)}">{escape(authority)}</text>',
        f'<text x="60" y="158" font-size="27" font-weight="700" fill="{_PROFIT_COLOR if shadow_choice == "STRUCTURE" else _LOSS_COLOR}">{escape(choice_label)} · 路径亏损概率 {_percent(_number(objective.get("loss_probability")))} · 目标值 {_points_as_dollars(objective_points)}</text>',
        f'<text x="60" y="192" class="small muted">{escape(_payoff_summary(structure))}</text>',
    ]
    parts.extend(_strategy_objective_cards(objective, y=214, objective_points=objective_points))
    parts.extend(_strategy_q_panel(q_rows, structure=structure, structure_facts=structure_facts, spot=spot, domain=domain, y=330))
    parts.extend(_strategy_payoff_panel(structure, structure_facts=structure_facts, spot=spot, domain=domain, y=600))
    parts.extend(_strategy_pnl_panel(path_distribution, objective=objective, y=990))
    parts.extend(
        [
            '<text x="60" y="1330" class="small muted">Q 是期权隐含的风险中性结算分布，不是真实涨跌概率；路径图来自因果历史回放。</text>',
            f'<text x="60" y="1362" class="small muted">目标函数：{escape(str(objective.get("formula") or RISK_OBJECTIVE_FORMULA))}</text>',
            '<text x="60" y="1394" class="small muted">只作研究解释，不改策略排序、不授权下单；到期图不含退出费用，自动下单关闭。</text>',
            f'<text x="60" y="1426" class="small muted">决策 {escape(str(strategy_decision.get("decision_id") or "unknown"))} · 来源 {escape(source)}</text>',
            "</svg>",
        ]
    )
    return "\n".join(parts)


def write_open_interest_mirror_png(
    exposure_map: Mapping[str, object],
    output_path: str | os.PathLike[str],
    *,
    window_points: float = 100.0,
    converter: SvgToPng | None = None,
) -> Path:
    """Render and atomically replace one Bark-compatible PNG projection."""
    svg = render_open_interest_mirror_svg(exposure_map, window_points=window_points)
    return _write_svg_png(svg, output_path, converter=converter)


def write_strategy_risk_png(
    strategy_decision: Mapping[str, object],
    output_path: str | os.PathLike[str],
    *,
    converter: SvgToPng | None = None,
) -> Path:
    """Render and atomically replace the decision-owned risk PNG."""
    return _write_svg_png(render_strategy_risk_svg(strategy_decision), output_path, converter=converter)


def _write_svg_png(
    svg: str,
    output_path: str | os.PathLike[str],
    *,
    converter: SvgToPng | None,
) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    svg_descriptor, svg_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".svg", dir=path.parent
    )
    png_descriptor, png_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".png", dir=path.parent
    )
    svg_path = Path(svg_name)
    png_path = Path(png_name)
    try:
        os.fchmod(svg_descriptor, 0o600)
        encoded = svg.encode("utf-8")
        with os.fdopen(svg_descriptor, "wb") as handle:
            svg_descriptor = -1
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.close(png_descriptor)
        png_descriptor = -1
        (converter or _convert_svg_to_png)(svg_path, png_path)
        size = png_path.stat().st_size
        with png_path.open("rb") as handle:
            signature = handle.read(len(_PNG_SIGNATURE))
        if signature != _PNG_SIGNATURE:
            raise ValueError("image converter did not produce PNG output")
        if size <= len(_PNG_SIGNATURE) or size > _MAX_PNG_BYTES:
            raise ValueError(f"PNG size outside Bark-safe range: {size}")
        os.chmod(png_path, 0o600)
        os.replace(png_path, path)
        os.chmod(path, 0o600)
        _fsync_directory(path.parent)
        return path
    finally:
        if svg_descriptor >= 0:
            os.close(svg_descriptor)
        if png_descriptor >= 0:
            os.close(png_descriptor)
        svg_path.unlink(missing_ok=True)
        png_path.unlink(missing_ok=True)


def _strategy_risk_structure(decision: Mapping[str, object]) -> tuple[Mapping[str, object], str]:
    candidate = _mapping(decision.get("candidate"))
    if candidate:
        return candidate, "strategy_decision.candidate"
    nearest = _mapping(_mapping(decision.get("why_not")).get("nearest_candidate"))
    iron_condor = _mapping(decision.get("iron_condor_map"))
    if (
        nearest.get("strategy_type") == "IRON_CONDOR"
        and iron_condor.get("status") == "ready"
    ):
        return iron_condor, "strategy_decision.iron_condor_map"
    if nearest and (nearest.get("legs") or nearest.get("long") or nearest.get("short")):
        return nearest, "strategy_decision.nearest_rejected"
    if iron_condor.get("status") == "ready":
        return iron_condor, "strategy_decision.iron_condor_map"
    return {}, "strategy_decision.no_structure"


def _strategy_path_distribution(structure: Mapping[str, object]) -> Mapping[str, object]:
    direct = _mapping(structure.get("path_distribution"))
    if direct:
        return direct
    return _mapping(_mapping(structure.get("edge")).get("path_distribution"))


def _strategy_risk_authority(decision: Mapping[str, object], *, source: str) -> str:
    if source.endswith("candidate") and decision.get("action_authority") == "manual":
        return "人工候选 · 自动下单关闭"
    if source.endswith("nearest_rejected"):
        return "拒绝候选 · 无交易授权"
    if source.endswith("iron_condor_map"):
        return "铁鹰结构图 · 无交易授权"
    return "暂不交易 · 无交易授权"


def _authority_color(source: str) -> str:
    return _PROFIT_COLOR if source.endswith("candidate") else _LOSS_COLOR


def _authority_background(source: str) -> str:
    return "#DCFCE7" if source.endswith("candidate") else "#FEE2E2"


def _strategy_objective_cards(
    objective: Mapping[str, object],
    *,
    y: float,
    objective_points: float | None,
) -> list[str]:
    fields = (
        ("平均路径", _number(objective.get("expected_pnl_points"))),
        ("尾部损失", _number(objective.get("cvar10_loss_points"))),
        ("报价宽度", _number(objective.get("quote_width_points"))),
        ("样本惩罚", _number(objective.get("model_uncertainty_points"))),
        ("目标值", objective_points),
    )
    parts: list[str] = []
    for index, (label, value) in enumerate(fields):
        x = 60 + index * 218
        value_color = (
            _PROFIT_COLOR
            if label == "目标值" and value is not None and value > 0
            else _LOSS_COLOR
            if label == "目标值" and value is not None
            else "#172033"
        )
        parts.extend(
            [
                f'<rect x="{x}" y="{y}" width="202" height="72" rx="12" fill="#F8FAFC" stroke="#E2E8F0"/>',
                f'<text x="{x + 14}" y="{y + 24}" class="small muted">{escape(label)}</text>',
                f'<text x="{x + 14}" y="{y + 54}" class="metric" fill="{value_color}">{_points_as_dollars(value)}</text>',
            ]
        )
    return parts


def _strategy_q_panel(
    q_rows: Sequence[tuple[float, float]],
    *,
    structure: Mapping[str, object],
    structure_facts: Mapping[str, object],
    spot: float | None,
    domain: tuple[float, float],
    y: float,
) -> list[str]:
    left, right = 82.0, 1130.0
    top, bottom = y + 72.0, y + 208.0
    parts = [
        f'<rect x="34" y="{y}" width="1132" height="250" rx="24" fill="#FFFFFF" stroke="#E2E8F0"/>',
        f'<text x="60" y="{y + 40}" font-size="25" font-weight="700">期权隐含结算分布 Q（5点分箱）</text>',
        f'<text x="1140" y="{y + 40}" class="small muted" text-anchor="end">边界截断 {_percent(_number(structure_facts.get("q_clipped_mass_fraction")))}</text>',
        f'<line x1="{left}" y1="{bottom}" x2="{right}" y2="{bottom}" stroke="#94A3B8"/>',
    ]
    visible = [(strike, mass) for strike, mass in q_rows if domain[0] <= strike <= domain[1]]
    if visible:
        maximum = max(mass for _strike, mass in visible) or 1.0
        bar_width = max(8.0, min(42.0, (right - left) * 4.0 / (domain[1] - domain[0])))
        points: list[str] = []
        for strike, mass in visible:
            x = _scale(strike, domain[0], domain[1], left, right)
            height = (bottom - top - 10.0) * mass / maximum
            parts.append(
                f'<rect x="{x - bar_width / 2:.1f}" y="{bottom - height:.1f}" width="{bar_width:.1f}" height="{height:.1f}" rx="5" fill="#C4B5FD"/>'
            )
            points.append(f"{x:.1f},{bottom - height:.1f}")
        parts.append(
            f'<polyline points="{" ".join(points)}" fill="none" stroke="{_Q_COLOR}" stroke-width="3"/>'
        )
    else:
        parts.append(
            f'<text x="600" y="{y + 142}" class="body muted" text-anchor="middle">Q 分布暂缺，不补造概率。</text>'
        )
    parts.extend(
        _price_markers(
            structure,
            structure_facts=structure_facts,
            spot=spot,
            domain=domain,
            left=left,
            right=right,
            top=top,
            bottom=bottom,
        )
    )
    parts.extend(_price_axis(domain, left=left, right=right, y=bottom + 24.0))
    return parts


def _strategy_payoff_panel(
    structure: Mapping[str, object],
    *,
    structure_facts: Mapping[str, object],
    spot: float | None,
    domain: tuple[float, float],
    y: float,
) -> list[str]:
    left, right = 82.0, 1130.0
    top, bottom = y + 82.0, y + 312.0
    series = _strategy_payoff_series(structure, domain=domain)
    parts = [
        f'<rect x="34" y="{y}" width="1132" height="370" rx="24" fill="#FFFFFF" stroke="#E2E8F0"/>',
        f'<text x="60" y="{y + 42}" font-size="25" font-weight="700">到期损益（按保守入场价）</text>',
        f'<text x="1140" y="{y + 42}" class="small muted" text-anchor="end">到期形状不等于盘中路径胜率</text>',
    ]
    if not series:
        parts.append(
            f'<text x="600" y="{y + 195}" class="body muted" text-anchor="middle">当前没有可展示的结构损益。</text>'
        )
        return parts
    pnl_values = [pnl for _price, pnl in series]
    max_abs = max(max(abs(value) for value in pnl_values), 0.25)
    zero_y = _scale(0.0, -max_abs, max_abs, bottom, top)
    parts.extend(
        [
            f'<rect x="{left}" y="{top}" width="{right - left}" height="{zero_y - top}" fill="#F0FDF4"/>',
            f'<rect x="{left}" y="{zero_y}" width="{right - left}" height="{bottom - zero_y}" fill="#FEF2F2"/>',
            f'<line x1="{left}" y1="{zero_y:.1f}" x2="{right}" y2="{zero_y:.1f}" stroke="#64748B" stroke-width="2"/>',
        ]
    )
    polyline: list[str] = []
    for index, (price, pnl) in enumerate(series):
        x = _scale(price, domain[0], domain[1], left, right)
        point_y = _scale(pnl, -max_abs, max_abs, bottom, top)
        polyline.append(f"{x:.1f},{point_y:.1f}")
        if index == 0:
            continue
        previous_price, previous_pnl = series[index - 1]
        previous_x = _scale(previous_price, domain[0], domain[1], left, right)
        previous_y = _scale(previous_pnl, -max_abs, max_abs, bottom, top)
        color = "#86EFAC" if (pnl + previous_pnl) / 2.0 >= 0 else "#FCA5A5"
        parts.append(
            f'<polygon points="{previous_x:.1f},{zero_y:.1f} {previous_x:.1f},{previous_y:.1f} '
            f'{x:.1f},{point_y:.1f} {x:.1f},{zero_y:.1f}" fill="{color}"/>'
        )
    parts.append(
        f'<polyline points="{" ".join(polyline)}" fill="none" stroke="#172033" stroke-width="4" stroke-linejoin="round"/>'
    )
    parts.extend(
        _price_markers(
            structure,
            structure_facts=structure_facts,
            spot=spot,
            domain=domain,
            left=left,
            right=right,
            top=top,
            bottom=bottom,
            labels=False,
        )
    )
    parts.extend(
        [
            f'<text x="{left}" y="{top + 18}" class="small" fill="{_PROFIT_COLOR}">+${max_abs * 100:,.0f}</text>',
            f'<text x="{left}" y="{bottom - 8}" class="small" fill="{_LOSS_COLOR}">−${max_abs * 100:,.0f}</text>',
        ]
    )
    parts.extend(_price_axis(domain, left=left, right=right, y=bottom + 28.0))
    return parts


def _strategy_pnl_panel(
    distribution: Mapping[str, object],
    *,
    objective: Mapping[str, object],
    y: float,
) -> list[str]:
    left, right = 82.0, 1130.0
    top, bottom = y + 82.0, y + 218.0
    histogram = [
        _mapping(row) for row in _sequence(distribution.get("pnl_histogram"))
    ]
    n_paths = int(_number(distribution.get("n_paths")) or 0)
    n_sessions = int(_number(distribution.get("n_sessions")) or 0)
    status = {"insufficient_sample": "样本不足", "estimated_uncalibrated": "尚未校准"}.get(
        str(distribution.get("status") or ""), "暂缺"
    )
    parts = [
        f'<rect x="34" y="{y}" width="1132" height="300" rx="24" fill="#FFFFFF" stroke="#E2E8F0"/>',
        f'<text x="60" y="{y + 42}" font-size="25" font-weight="700">历史路径净损益（执行管理规则后）</text>',
        f'<text x="1140" y="{y + 42}" class="small muted" text-anchor="end">{escape(status)} · {n_paths}条路径 / {n_sessions}个交易日</text>',
    ]
    if histogram:
        lows = [_number(row.get("lower_net_pnl")) for row in histogram]
        highs = [_number(row.get("upper_net_pnl")) for row in histogram]
        finite_lows = [value for value in lows if value is not None]
        finite_highs = [value for value in highs if value is not None]
        x_low = min([*finite_lows, 0.0])
        x_high = max([*finite_highs, 0.0])
        if math.isclose(x_low, x_high):
            x_low, x_high = x_low - 1.0, x_high + 1.0
        maximum = max((_number(row.get("probability")) or 0.0 for row in histogram), default=1.0)
        for row in histogram:
            lower = _number(row.get("lower_net_pnl"))
            upper = _number(row.get("upper_net_pnl"))
            probability = _number(row.get("probability"))
            if lower is None or upper is None or probability is None:
                continue
            x1 = _scale(lower, x_low, x_high, left, right)
            x2 = _scale(upper, x_low, x_high, left, right)
            bar_height = (bottom - top) * probability / max(maximum, 1e-9)
            color = _LOSS_COLOR if (lower + upper) / 2.0 < 0.0 else _PROFIT_COLOR
            parts.append(
                f'<rect x="{x1 + 1:.1f}" y="{bottom - bar_height:.1f}" width="{max(x2 - x1 - 2, 2):.1f}" height="{bar_height:.1f}" rx="5" fill="{color}" opacity="0.68"/>'
            )
        zero_x = _scale(0.0, x_low, x_high, left, right)
        parts.extend(
            [
                f'<line x1="{zero_x:.1f}" y1="{top}" x2="{zero_x:.1f}" y2="{bottom}" stroke="#172033" stroke-width="2"/>',
                f'<text x="{zero_x:.1f}" y="{bottom + 24}" class="small" text-anchor="middle">$0</text>',
                f'<text x="{left}" y="{bottom + 24}" class="small muted">{_dollars(x_low)}</text>',
                f'<text x="{right}" y="{bottom + 24}" class="small muted" text-anchor="end">{_dollars(x_high)}</text>',
            ]
        )
    else:
        reasons = ", ".join(str(reason) for reason in distribution.get("reason_codes") or ())
        parts.append(
            f'<text x="600" y="{y + 160}" class="body muted" text-anchor="middle">路径损益暂缺 · {escape(reasons[:80] or "没有因果路径样本")}</text>'
        )
    metric_y = y + 268.0
    parts.extend(
        [
            f'<text x="60" y="{metric_y}" class="small">亏损概率 {_percent(_number(objective.get("loss_probability")))}</text>',
            f'<text x="260" y="{metric_y}" class="small">平均 {_points_as_dollars(_number(objective.get("expected_pnl_points")))}</text>',
            f'<text x="478" y="{metric_y}" class="small">尾部损失 {_points_as_dollars(_number(objective.get("cvar10_loss_points")))}</text>',
            f'<text x="700" y="{metric_y}" class="small">P10 / 中位 / P90 {_pnl_quantiles(distribution)}</text>',
        ]
    )
    return parts


def _strategy_price_domain(
    structure: Mapping[str, object],
    *,
    structure_facts: Mapping[str, object],
    q_rows: Sequence[tuple[float, float]],
    spot: float | None,
) -> tuple[float, float]:
    values = [strike for strike, _mass in q_rows]
    values.extend(
        value
        for value in (
            spot,
            _number(structure_facts.get("q_p10")),
            _number(structure_facts.get("q_p90")),
            _number(structure_facts.get("put_wall")),
            _number(structure_facts.get("call_wall")),
        )
        if value is not None
    )
    values.extend(
        value
        for value in (_number(leg.get("strike")) for leg in _structure_legs(structure))
        if value is not None
    )
    economics = _mapping(structure.get("economics"))
    values.extend(
        value
        for value in (
            _number(economics.get("breakeven_spx")),
            _number(economics.get("breakeven_low")),
            _number(economics.get("breakeven_high")),
        )
        if value is not None
    )
    if not values:
        return (0.0, 100.0)
    low, high = min(values), max(values)
    if math.isclose(low, high):
        low, high = low - 25.0, high + 25.0
    padding = max((high - low) * 0.08, 10.0)
    return (math.floor((low - padding) / 5.0) * 5.0, math.ceil((high + padding) / 5.0) * 5.0)


def _q_mass_rows(value: object) -> list[tuple[float, float]]:
    rows: list[tuple[float, float]] = []
    for raw_strike, raw_mass in _mapping(value).items():
        try:
            strike = float(raw_strike)
        except (TypeError, ValueError):
            continue
        mass = _number(raw_mass)
        if math.isfinite(strike) and mass is not None and mass >= 0.0:
            rows.append((strike, mass))
    return sorted(rows)


def _strategy_payoff_series(
    structure: Mapping[str, object], *, domain: tuple[float, float]
) -> list[tuple[float, float]]:
    if not structure:
        return []
    result: list[tuple[float, float]] = []
    for index in range(121):
        settlement = domain[0] + (domain[1] - domain[0]) * index / 120.0
        payoff = _strategy_payoff_value(structure, settlement)
        if payoff is None:
            return []
        result.append((settlement, payoff))
    return result


def _strategy_payoff_value(
    structure: Mapping[str, object], settlement: float
) -> float | None:
    strategy_type = str(structure.get("strategy_type") or "")
    quote = _mapping(structure.get("quote"))
    economics = _mapping(structure.get("economics"))
    legs = _structure_legs(structure)
    try:
        if strategy_type in {"CALL_DEBIT_VERTICAL", "PUT_DEBIT_VERTICAL"}:
            if len(legs) < 2:
                return None
            return vertical_payoff(
                settlement,
                long_strike=float(legs[0]["strike"]),
                short_strike=float(legs[1]["strike"]),
                net_debit=float(quote["ask"]),
                right=str(structure.get("right") or legs[0].get("right") or ""),
            )
        if strategy_type.endswith("_BUTTERFLY"):
            return butterfly_payoff(
                settlement,
                center=float(structure["center"]),
                width=float(structure["width"]),
                net_debit=float(quote["ask"]),
            )
        if strategy_type == "IRON_CONDOR":
            strikes = [float(value) for value in structure.get("strikes") or ()]
            if len(strikes) != 4:
                return None
            return iron_condor_payoff(
                settlement,
                put_long=strikes[0],
                put_short=strikes[1],
                call_short=strikes[2],
                call_long=strikes[3],
                net_credit=float(quote.get("credit") or economics["max_gain_points"]),
            )
    except (KeyError, TypeError, ValueError):
        return None
    return None


def _structure_legs(structure: Mapping[str, object]) -> list[Mapping[str, object]]:
    legs = [_mapping(item) for item in _sequence(structure.get("legs"))]
    if legs and all(legs):
        return legs
    long_leg = _mapping(structure.get("long"))
    short_leg = _mapping(structure.get("short"))
    return [long_leg, short_leg] if long_leg and short_leg else []


def _price_markers(
    structure: Mapping[str, object],
    *,
    structure_facts: Mapping[str, object],
    spot: float | None,
    domain: tuple[float, float],
    left: float,
    right: float,
    top: float,
    bottom: float,
    labels: bool = True,
) -> list[str]:
    markers: list[tuple[str, float | None, str]] = [
        ("SPX", spot, _SPOT_COLOR),
        ("PW", _number(structure_facts.get("put_wall")), _WALL_PUT_COLOR),
        ("CW", _number(structure_facts.get("call_wall")), _WALL_CALL_COLOR),
        ("Q50", _number(structure_facts.get("q_median")), _Q_COLOR),
    ]
    markers.extend(
        (f"K{index + 1} {format_number(_number(leg.get('strike')), 0)}", _number(leg.get("strike")), "#0F172A")
        for index, leg in enumerate(_structure_legs(structure))
    )
    combined: list[tuple[str, float, str]] = []
    for label, value, color in markers:
        if value is None:
            continue
        match = next(
            (
                index
                for index, (_labels, observed, _color) in enumerate(combined)
                if math.isclose(value, observed, rel_tol=0.0, abs_tol=1e-6)
            ),
            None,
        )
        if match is None:
            combined.append((label, value, color))
        else:
            labels, observed, observed_color = combined[match]
            combined[match] = (f"{labels}/{label}", observed, observed_color)
    parts: list[str] = []
    for index, (label, value, color) in enumerate(combined):
        if not domain[0] <= value <= domain[1]:
            continue
        x = _scale(value, domain[0], domain[1], left, right)
        solid = any(part == "SPX" or part.startswith("K") for part in label.split("/"))
        dash = "none" if solid else "6 5"
        parts.append(
            f'<line x1="{x:.1f}" y1="{top}" x2="{x:.1f}" y2="{bottom}" stroke="{color}" stroke-width="{3 if "SPX" in label.split("/") else 1.5}" stroke-dasharray="{dash}" opacity="0.72"/>'
        )
        if labels:
            label_y = top + 18.0 + (index % 4) * 22.0
            parts.append(
                f'<text x="{x + 4:.1f}" y="{label_y:.1f}" class="small" fill="{color}">{escape(label)}</text>'
            )
    return parts


def _price_axis(
    domain: tuple[float, float], *, left: float, right: float, y: float
) -> list[str]:
    parts: list[str] = []
    for index in range(5):
        value = domain[0] + (domain[1] - domain[0]) * index / 4.0
        x = left + (right - left) * index / 4.0
        parts.append(
            f'<text x="{x:.1f}" y="{y:.1f}" class="small muted" text-anchor="{("start" if index == 0 else "end" if index == 4 else "middle")}">{value:,.0f}</text>'
        )
    return parts


def _payoff_summary(structure: Mapping[str, object]) -> str:
    economics = _mapping(structure.get("economics"))
    quote = _mapping(structure.get("quote"))
    loss = _number(economics.get("max_loss_points"))
    strikes = "/".join(format_number(_number(leg.get("strike")), 0) for leg in _structure_legs(structure)) or "—"
    credit = str(structure.get("strategy_type") or "") == "IRON_CONDOR"
    premium = _number(quote.get("credit" if credit else "ask"))
    breakevens = [
        value
        for value in (
            _number(economics.get("breakeven_spx")),
            _number(economics.get("breakeven_low")),
            _number(economics.get("breakeven_high")),
        )
        if value is not None
    ]
    be = "/".join(f"{value:,.1f}" for value in breakevens) or "—"
    return f"执行价 {strikes} · 净{'贷记' if credit else '借记'} {_points_as_dollars(premium)} · 最大亏损 {_points_as_dollars(loss)} · 盈亏平衡 {be}"


def _pnl_quantiles(distribution: Mapping[str, object]) -> str:
    values = [
        _number(distribution.get(key))
        for key in ("p10_net_pnl", "p50_net_pnl", "p90_net_pnl")
    ]
    if any(value is None for value in values):
        return "暂缺"
    return "/".join(_dollars(float(value)) for value in values if value is not None)


def _points_as_dollars(value: float | None) -> str:
    return "—" if value is None else _dollars(value * 100.0)


def _dollars(value: float) -> str:
    return f"-${abs(value):,.0f}" if value < 0.0 else f"${value:,.0f}"


def _percent(value: float | None) -> str:
    return "—" if value is None else f"{value * 100:.1f}%"


def _scale(value: float, low: float, high: float, out_low: float, out_high: float) -> float:
    if math.isclose(low, high):
        return 0.5 * (out_low + out_high)
    return out_low + (value - low) * (out_high - out_low) / (high - low)


def _convert_svg_to_png(svg_path: Path, png_path: Path) -> None:
    completed = subprocess.run(
        (
            "convert", "-background", "white",
            "-font", "WenQuanYi-Zen-Hei",
            "-density", "144",
            str(svg_path),
            "-resize", "1200x",
            str(png_path),
        ),
        capture_output=True,
        check=False,
        text=True,
        timeout=20.0,
    )
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip()
        raise RuntimeError(f"ImageMagick conversion failed: {detail[:300]}")


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    descriptor = os.open(path, flags)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _front_expiry(exposure_map: Mapping[str, object]) -> Mapping[str, object]:
    expiries = [_mapping(item) for item in _sequence(exposure_map.get("expiries"))]
    if not expiries:
        raise ValueError("exposure map has no expiries")
    return min(expiries, key=lambda item: str(item.get("expiry") or "99999999"))


def _wall_rows(walls: Mapping[str, object], key: str) -> list[Mapping[str, object]]:
    return [_mapping(item) for item in _sequence(walls.get(key))][:3]


def _wall_summary(
    rows: Sequence[Mapping[str, object]],
    *,
    x: float,
    y: float,
    prefix: str,
    color: str,
) -> list[str]:
    output: list[str] = []
    for index in range(3):
        row_y = y + index * 32
        if index >= len(rows):
            label = "unavailable"
        else:
            strike = _number(rows[index].get("strike"))
            oi = _number(rows[index].get("open_interest"))
            label = (
                f"{strike:,.0f}    OI {oi:,.0f}"
                if strike is not None and oi is not None
                else "unavailable"
            )
        output.append(
            f'<circle cx="{x + 15:.1f}" cy="{row_y - 6:.1f}" r="14" fill="{color}"/>'
        )
        output.append(
            f'<text x="{x + 15:.1f}" y="{row_y:.1f}" class="rank" text-anchor="middle">{prefix}{index + 1}</text>'
        )
        output.append(
            f'<text x="{x + 42:.1f}" y="{row_y:.1f}" class="body">{escape(label)}</text>'
        )
    return output


def _rank_badge(x: float, y: float, label: str, color: str) -> str:
    return (
        f'<rect x="{x:.1f}" y="{y:.1f}" width="34" height="20" rx="5" fill="{color}"/>'
        f'<text x="{x + 17:.1f}" y="{y + 16:.1f}" class="rank" text-anchor="middle">{label}</text>'
    )


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _sequence(value: object) -> Sequence[object]:
    return value if isinstance(value, Sequence) and not isinstance(value, (str, bytes)) else ()


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _format_as_of(value: object) -> str:
    if not isinstance(value, str):
        return "as-of unknown"
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(_EASTERN).strftime("%Y-%m-%d %H:%M ET")
    except ValueError:
        return value


def _open_interest_age(expiry: Mapping[str, object]) -> str:
    freshness = _mapping(expiry.get("freshness"))
    open_interest = _mapping(freshness.get("open_interest"))
    all_rows = _mapping(open_interest.get("all"))
    seconds = _number(all_rows.get("max_seconds"))
    if seconds is None:
        return ""
    if seconds >= 60:
        return f" · max source age {seconds / 60:.1f}m"
    return f" · max source age {seconds:.0f}s"
