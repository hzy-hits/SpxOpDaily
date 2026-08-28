"""Render the captured SPXW net-premium proxy as one fixed PNG."""

from __future__ import annotations

import math
import os
from collections.abc import Mapping, Sequence
from datetime import datetime, time
from html import escape
from pathlib import Path
from zoneinfo import ZoneInfo

from spx_spark.options_map.render import SvgToPng, _write_svg_png


_ET = ZoneInfo("America/New_York")
_FLOW_KEY = "captured_net_premium_divergence"
_CALL_NET_INDEX = 6
_PUT_NET_INDEX = 7
_BG = "#07131E"
_CARD = "#0B1B2A"
_BORDER = "#294052"
_GRID = "#263B4D"
_TEXT = "#F2F7FB"
_MUTED = "#91A6B8"
_SPX = "#7C8CFF"
_DIRECTIONAL = "#35D07F"
_CALL = "#25C7D9"
_PUT = "#FF7B6B"


def render_net_premium_flow_svg(state: Mapping[str, object]) -> str:
    """Render a session-cumulative flow proxy with an independent SPX axis."""

    tape = _flow_tape(state)
    points = _timeline(tape)
    if not points:
        raise ValueError("captured net-premium timeline unavailable")
    snapshot = _mapping(tape.get("snapshot"))
    session = _mapping(snapshot.get("session"))
    latest = points[-1]
    session_call = _number(session.get("call_net"), latest["call"])
    session_put = _number(session.get("put_net"), latest["put"])
    session_directional = _number(session.get("directional_net"), session_call - session_put)
    coverage = _number(session.get("coverage"), 0.0)
    inside_share = _number(session.get("inside_share"), 0.0)
    quality = str(session.get("quality") or "unavailable")

    width, height = 1400.0, 900.0
    left, right, top, bottom = 92.0, 1310.0, 286.0, 748.0
    observed = latest["at"].astimezone(_ET)
    session_open = datetime.combine(observed.date(), time(9, 30), tzinfo=_ET)
    session_close = datetime.combine(observed.date(), time(16, 0), tzinfo=_ET)
    x_values = [
        _scale(
            row["at"].astimezone(_ET).timestamp(),
            session_open.timestamp(),
            session_close.timestamp(),
            left,
            right,
        )
        for row in points
    ]
    flow_values = [
        float(value) for row in points for value in (row["call"], row["put"], row["directional"])
    ] + [0.0]
    flow_low, flow_high = _domain(flow_values, minimum_span=100_000.0)
    prices = [float(row["spx"]) for row in points]
    price_low, price_high = _domain(prices, minimum_span=10.0)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width:.0f}" height="{height:.0f}" viewBox="0 0 {width:.0f} {height:.0f}">',
        "<style>",
        "text { font-family: 'WenQuanYi Zen Hei', 'Noto Sans CJK SC', sans-serif; }",
        f".label {{ fill: {_MUTED}; font-size: 16px; }}",
        f".value {{ fill: {_TEXT}; font-size: 29px; font-weight: 700; }}",
        f".axis {{ fill: {_MUTED}; font-size: 15px; font-variant-numeric: tabular-nums; }}",
        "</style>",
        f'<rect width="{width:.0f}" height="{height:.0f}" fill="{_BG}"/>',
        f'<text x="44" y="43" fill="{_TEXT}" font-size="24" font-weight="700">SPX 0DTE Captured Premium Flow</text>',
        f'<text x="1356" y="43" fill="{_MUTED}" font-size="15" text-anchor="end">{observed.strftime("%Y-%m-%d %H:%M ET")}</text>',
    ]
    cards = (
        (
            "SPX · Underlying",
            _price(latest["spx"]),
            "RTH cash coordinate",
            _SPX,
            [row["spx"] for row in points],
        ),
        (
            "Directional Net Flow",
            _money(session_directional),
            "Call net - Put net",
            _DIRECTIONAL,
            [row["directional"] for row in points],
        ),
        (
            "Call Net Premium",
            _money(session_call),
            "Buy Call - Sell Call",
            _CALL,
            [row["call"] for row in points],
        ),
        (
            "Put Net Premium",
            _money(session_put),
            "Buy Put - Sell Put",
            _PUT,
            [row["put"] for row in points],
        ),
    )
    card_gap = 14.0
    card_width = (width - 88.0 - card_gap * 3.0) / 4.0
    for index, (label, value, note, color, series) in enumerate(cards):
        parts.extend(
            _metric_card(
                x=44.0 + index * (card_width + card_gap),
                y=64.0,
                width=card_width,
                label=label,
                value=value,
                note=note,
                color=color,
                series=[float(item) for item in series],
            )
        )

    status_color = "#F4B942" if quality in {"low", "low_coverage", "unavailable"} else _DIRECTIONAL
    parts.extend(
        [
            f'<rect x="44" y="212" width="1312" height="558" rx="18" fill="{_CARD}" stroke="{_BORDER}"/>',
            f'<text x="68" y="248" fill="{_TEXT}" font-size="21" font-weight="700">RTH cumulative proxy</text>',
            f'<text x="1332" y="248" fill="{status_color}" font-size="15" text-anchor="end">{escape(quality.upper())} · classified/volume {coverage:.1%} · inside-spread {inside_share:.1%}</text>',
        ]
    )
    for index in range(5):
        y = top + (bottom - top) * index / 4.0
        flow_tick = flow_high - (flow_high - flow_low) * index / 4.0
        price_tick = price_high - (price_high - price_low) * index / 4.0
        parts.extend(
            [
                f'<line x1="{left:.1f}" y1="{y:.1f}" x2="{right:.1f}" y2="{y:.1f}" stroke="{_GRID}" stroke-width="1"/>',
                f'<text x="{left - 12:.1f}" y="{y + 5:.1f}" class="axis" text-anchor="end">{_money_axis(flow_tick)}</text>',
                f'<text x="{right + 12:.1f}" y="{y + 5:.1f}" class="axis">{price_tick:,.1f}</text>',
            ]
        )
    zero_y = _scale(0.0, flow_low, flow_high, bottom, top)
    if top <= zero_y <= bottom:
        parts.append(
            f'<line x1="{left:.1f}" y1="{zero_y:.1f}" x2="{right:.1f}" y2="{zero_y:.1f}" stroke="#64798A" stroke-width="1.5"/>'
        )
    for hour, label in (
        (9.5, "09:30"),
        (10, "10:00"),
        (11, "11:00"),
        (12, "12:00"),
        (13, "13:00"),
        (14, "14:00"),
        (15, "15:00"),
        (16, "16:00"),
    ):
        x = left + (right - left) * (hour - 9.5) / 6.5
        parts.append(
            f'<text x="{x:.1f}" y="{bottom + 30:.1f}" class="axis" text-anchor="middle">{label}</text>'
        )

    series = (
        ("directional", _DIRECTIONAL, 3.0, "none"),
        ("call", _CALL, 2.4, "none"),
        ("put", _PUT, 2.4, "5 4"),
    )
    for key, color, stroke_width, dash in series:
        y_values = [_scale(float(row[key]), flow_low, flow_high, bottom, top) for row in points]
        parts.append(_polyline(x_values, y_values, color=color, width=stroke_width, dash=dash))
    price_y = [_scale(float(row["spx"]), price_low, price_high, bottom, top) for row in points]
    parts.append(_polyline(x_values, price_y, color=_SPX, width=3.2, dash="none"))
    parts.extend(_divergence_markers(tape, points, x_values, price_y, top=top, bottom=bottom))
    parts.extend(
        _legend(
            y=806.0,
            rows=(
                ("Directional", _DIRECTIONAL),
                ("Call net", _CALL),
                ("Put net", _PUT),
                ("SPX · right axis", _SPX),
            ),
        )
    )
    folded = _history_folded(tape, points)
    parts.extend(
        [
            f'<text x="44" y="850" fill="{_MUTED}" font-size="14">Classification: last ≥ ask = active buy; last ≤ bid = active sell; inside spread = unknown. This is captured Schwab L1 last-trade flow, not full OPRA or BTO/STC.</text>',
            f'<text x="44" y="878" fill="{_MUTED}" font-size="14">Research/confirmation only · no standalone direction or order authority{escape(folded)}</text>',
            "</svg>",
        ]
    )
    return "\n".join(parts)


def write_net_premium_flow_png(
    state: Mapping[str, object],
    output_path: str | os.PathLike[str],
    *,
    converter: SvgToPng | None = None,
) -> Path:
    """Render and atomically replace the fixed captured-flow PNG."""

    return _write_svg_png(
        render_net_premium_flow_svg(state),
        output_path,
        converter=converter,
    )


def _flow_tape(state: Mapping[str, object]) -> Mapping[str, object]:
    nested = state.get(_FLOW_KEY)
    if isinstance(nested, Mapping):
        return nested
    if "minutes" in state and "snapshot" in state:
        return state
    raise ValueError("captured net-premium state unavailable")


def _timeline(tape: Mapping[str, object]) -> list[dict[str, object]]:
    raw_minutes = tape.get("minutes")
    if not isinstance(raw_minutes, Mapping):
        return []
    rows: list[dict[str, object]] = []
    for raw_at, raw in raw_minutes.items():
        if not isinstance(raw, Mapping):
            continue
        observed = _timestamp(raw_at)
        spx = _optional_number(raw.get("spx"))
        if observed is None or spx is None or spx <= 0:
            continue
        rows.append(
            {
                "at": observed,
                "spx": spx,
                "minute_call": _aggregate_value(raw, "call_net", _CALL_NET_INDEX),
                "minute_put": _aggregate_value(raw, "put_net", _PUT_NET_INDEX),
            }
        )
    rows.sort(key=lambda row: row["at"])
    if not rows:
        return []
    snapshot = _mapping(tape.get("snapshot"))
    session = _mapping(snapshot.get("session"))
    session_call = _number(
        session.get("call_net"),
        _aggregate_value(_mapping(tape.get("session")), "call_net", _CALL_NET_INDEX),
    )
    session_put = _number(
        session.get("put_net"),
        _aggregate_value(_mapping(tape.get("session")), "put_net", _PUT_NET_INDEX),
    )
    running_call = session_call - sum(float(row["minute_call"]) for row in rows)
    running_put = session_put - sum(float(row["minute_put"]) for row in rows)
    for row in rows:
        running_call += float(row.pop("minute_call"))
        running_put += float(row.pop("minute_put"))
        row["call"] = running_call
        row["put"] = running_put
        row["directional"] = running_call - running_put
    return rows


def _aggregate_value(aggregate: Mapping[str, object], field: str, packed_index: int) -> float:
    direct = _optional_number(aggregate.get(field))
    if direct is not None:
        return direct
    packed = aggregate.get("totals")
    if isinstance(packed, Sequence) and not isinstance(packed, (str, bytes)):
        if packed_index < len(packed):
            return _number(packed[packed_index], 0.0)
    return 0.0


def _metric_card(
    *,
    x: float,
    y: float,
    width: float,
    label: str,
    value: str,
    note: str,
    color: str,
    series: list[float],
) -> list[str]:
    parts = [
        f'<rect x="{x:.1f}" y="{y:.1f}" width="{width:.1f}" height="124" rx="14" fill="{_CARD}" stroke="{_BORDER}"/>',
        f'<text x="{x + 18:.1f}" y="{y + 30:.1f}" class="label">{escape(label)}</text>',
        f'<text x="{x + 18:.1f}" y="{y + 72:.1f}" class="value" fill="{color}">{escape(value)}</text>',
        f'<text x="{x + 18:.1f}" y="{y + 101:.1f}" fill="{color}" font-size="14">{escape(note)}</text>',
    ]
    if len(series) >= 2:
        spark_left, spark_right = x + width - 92.0, x + width - 16.0
        low, high = _domain(series[-30:], minimum_span=1.0)
        xs = [
            spark_left + (spark_right - spark_left) * index / (len(series[-30:]) - 1)
            for index in range(len(series[-30:]))
        ]
        ys = [_scale(item, low, high, y + 88.0, y + 24.0) for item in series[-30:]]
        parts.append(_polyline(xs, ys, color=color, width=2.0, dash="none"))
    return parts


def _divergence_markers(
    tape: Mapping[str, object],
    points: list[dict[str, object]],
    x_values: list[float],
    price_y: list[float],
    *,
    top: float,
    bottom: float,
) -> list[str]:
    raw_events = tape.get("divergence_events")
    events = (
        [row for row in raw_events if isinstance(row, Mapping)]
        if isinstance(raw_events, list)
        else []
    )
    if not events:
        last = tape.get("last_divergence")
        events = [last] if isinstance(last, Mapping) else []
    output: list[str] = []
    for event in events[-8:]:
        signal_at = _timestamp(event.get("signal_at"))
        if signal_at is None:
            continue
        index = min(
            range(len(points)),
            key=lambda item: abs((points[item]["at"] - signal_at).total_seconds()),
        )
        x, y = x_values[index], price_y[index]
        if not (top <= y <= bottom):
            continue
        bearish = str(event.get("direction") or "").upper() == "BEARISH"
        color, label = (_PUT, "熊背离") if bearish else (_DIRECTIONAL, "牛背离")
        label_y = max(top + 18.0, y - 18.0) if bearish else min(bottom - 8.0, y + 28.0)
        output.extend(
            [
                f'<line x1="{x:.1f}" y1="{top:.1f}" x2="{x:.1f}" y2="{bottom:.1f}" stroke="{color}" stroke-width="1" stroke-dasharray="4 5" opacity="0.55"/>',
                f'<circle cx="{x:.1f}" cy="{y:.1f}" r="6" fill="{color}" stroke="{_TEXT}" stroke-width="2"/>',
                f'<text x="{x + 8:.1f}" y="{label_y:.1f}" fill="{color}" font-size="14" font-weight="700">{label}</text>',
            ]
        )
    return output


def _legend(*, y: float, rows: tuple[tuple[str, str], ...]) -> list[str]:
    output: list[str] = []
    x = 48.0
    for label, color in rows:
        output.extend(
            [
                f'<line x1="{x:.1f}" y1="{y:.1f}" x2="{x + 28:.1f}" y2="{y:.1f}" stroke="{color}" stroke-width="4"/>',
                f'<text x="{x + 38:.1f}" y="{y + 5:.1f}" fill="{_MUTED}" font-size="15">{escape(label)}</text>',
            ]
        )
        x += 210.0
    return output


def _polyline(xs: list[float], ys: list[float], *, color: str, width: float, dash: str) -> str:
    points = " ".join(f"{x:.1f},{y:.1f}" for x, y in zip(xs, ys, strict=True))
    return (
        f'<polyline points="{points}" fill="none" stroke="{color}" '
        f'stroke-width="{width:.1f}" stroke-linejoin="round" stroke-linecap="round" '
        f'stroke-dasharray="{dash}"/>'
    )


def _domain(values: list[float], *, minimum_span: float) -> tuple[float, float]:
    low, high = min(values), max(values)
    span = max(high - low, minimum_span)
    center = 0.5 * (low + high)
    return center - span * 0.58, center + span * 0.58


def _scale(value: float, low: float, high: float, out_low: float, out_high: float) -> float:
    if math.isclose(low, high):
        return 0.5 * (out_low + out_high)
    return out_low + (value - low) * (out_high - out_low) / (high - low)


def _money(value: float) -> str:
    sign = "-" if value < 0 else ""
    absolute = abs(value)
    if absolute >= 1_000_000:
        return f"{sign}${absolute / 1_000_000:.2f}M"
    if absolute >= 1_000:
        return f"{sign}${absolute / 1_000:.0f}K"
    return f"{sign}${absolute:,.0f}"


def _money_axis(value: float) -> str:
    absolute = abs(value)
    sign = "-" if value < 0 else ""
    return (
        f"{sign}${absolute / 1_000_000:.2f}M"
        if absolute >= 1_000_000
        else f"{sign}${absolute / 1_000:.0f}K"
    )


def _price(value: object) -> str:
    return f"${_number(value, 0.0):,.2f}"


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _optional_number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _number(value: object, default: float) -> float:
    number = _optional_number(value)
    return default if number is None else number


def _timestamp(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else None
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _history_folded(tape: Mapping[str, object], points: list[dict[str, object]]) -> str:
    session = _mapping(_mapping(tape.get("snapshot")).get("session"))
    minutes = int(_number(session.get("minutes"), 0.0))
    if minutes <= len(points) + 2:
        return ""
    start = points[0]["at"].astimezone(_ET).strftime("%H:%M")
    return f" · timeline begins {start} ET; earlier cumulative flow is folded into the first point"
