"""Human-readable options map rendering."""

from __future__ import annotations

import os
import subprocess
import tempfile
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from html import escape
from pathlib import Path
from zoneinfo import ZoneInfo

from spx_spark.analytics.options.models import OptionsMap

_EASTERN = ZoneInfo("America/New_York")
_PUT_COLOR = "#2563EB"
_CALL_COLOR = "#EA580C"
_WALL_PUT_COLOR = "#1E3A8A"
_WALL_CALL_COLOR = "#9A3412"
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


def write_open_interest_mirror_png(
    exposure_map: Mapping[str, object],
    output_path: str | os.PathLike[str],
    *,
    window_points: float = 100.0,
    converter: SvgToPng | None = None,
) -> Path:
    """Render and atomically replace one Bark-compatible PNG projection."""
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
        svg = render_open_interest_mirror_svg(
            exposure_map,
            window_points=window_points,
        ).encode("utf-8")
        with os.fdopen(svg_descriptor, "wb") as handle:
            svg_descriptor = -1
            handle.write(svg)
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


def _convert_svg_to_png(svg_path: Path, png_path: Path) -> None:
    completed = subprocess.run(
        (
            "convert",
            "-background",
            "white",
            "-density",
            "144",
            str(svg_path),
            "-resize",
            "1200x",
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
