"""Bounded causal path-history primitives for the GTH detector."""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Mapping


PATH_SAMPLE_SECONDS = 5
MAX_PATH_HISTORY_WINDOWS = 1_000


def advance_path_history(
    raw_history: object,
    raw_progress: object,
    *,
    samples: list[dict[str, object]],
    continuous_started_at: datetime,
    now: datetime,
    horizons: tuple[int, ...],
) -> tuple[dict[str, list[dict[str, object]]], dict[str, int]]:
    """Persist compact, non-overlapping path windows for session-causal ranks."""

    history_source = raw_history if isinstance(raw_history, Mapping) else {}
    progress_source = raw_progress if isinstance(raw_progress, Mapping) else {}
    result: dict[str, list[dict[str, object]]] = {}
    progress: dict[str, int] = {}
    elapsed = max(0.0, (now - continuous_started_at).total_seconds())
    for horizon in horizons:
        key = str(horizon)
        prior_rows = history_source.get(key)
        entries = (
            [dict(item) for item in prior_rows if isinstance(item, Mapping)]
            if isinstance(prior_rows, list)
            else []
        )
        entries = entries[-MAX_PATH_HISTORY_WINDOWS:]
        raw_index = progress_source.get(key)
        processed_index = (
            int(raw_index)
            if isinstance(raw_index, int) and not isinstance(raw_index, bool)
            else len(entries)
        )
        completed_index = int(elapsed // horizon)
        for index in range(processed_index + 1, completed_index + 1):
            window_start = continuous_started_at + timedelta(seconds=(index - 1) * horizon)
            window_end = continuous_started_at + timedelta(seconds=index * horizon)
            rows = window_rows(
                samples,
                window_start=window_start,
                window_end=window_end,
            )
            summary = path_window_summary(
                rows,
                window_start=window_start,
                window_end=window_end,
                require_full_window=True,
            )
            if summary is not None:
                entries.append(
                    {
                        **summary,
                        "window_index": index,
                        "window_kind": "non_overlapping_reference",
                    }
                )
        result[key] = entries[-MAX_PATH_HISTORY_WINDOWS:]
        progress[key] = max(processed_index, completed_index)
    return result, progress


def window_rows(
    samples: list[dict[str, object]],
    *,
    window_start: datetime,
    window_end: datetime,
) -> list[dict[str, object]]:
    """Use the last causal boundary observation plus observations inside the window."""

    eligible = [row for row in samples if (_time(row.get("at")) or window_end) <= window_end]
    inside = [row for row in eligible if (_time(row.get("at")) or window_end) >= window_start]
    predecessor = [row for row in eligible if (_time(row.get("at")) or window_end) < window_start]
    first_inside_at = _time(inside[0].get("at")) if inside else None
    if predecessor and (first_inside_at is None or first_inside_at > window_start):
        inside.insert(0, predecessor[-1])
    return inside


def path_window_summary(
    rows: list[dict[str, object]],
    *,
    window_start: datetime,
    window_end: datetime,
    require_full_window: bool,
) -> dict[str, object] | None:
    """Summarize one path without interpolating any missing market prices."""

    ordered = sorted(
        ((_time(row.get("at")), _finite_number(row.get("es"))) for row in rows),
        key=lambda item: item[0] or window_end,
    )
    points = [
        (observed_at, price)
        for observed_at, price in ordered
        if observed_at is not None and price is not None
    ]
    if len(points) < 2:
        return None
    first_at, first_price = points[0]
    last_at, last_price = points[-1]
    if require_full_window and (
        (first_at - window_start).total_seconds() > PATH_SAMPLE_SECONDS * 2
        or (window_end - last_at).total_seconds() > PATH_SAMPLE_SECONDS * 2
    ):
        return None

    peak = first_price
    peak_at = first_at
    trough = first_price
    trough_at = first_at
    drawdown = 0.0
    rally_base = first_price
    rally_base_at = first_at
    rally_peak = first_price
    rally_peak_at = first_at
    rally = 0.0
    for observed_at, price in points[1:]:
        if price > peak:
            peak = price
            peak_at = observed_at
        candidate_drawdown = peak - price
        if candidate_drawdown >= drawdown:
            drawdown = candidate_drawdown
            trough = price
            trough_at = observed_at
        if price < rally_base:
            rally_base = price
            rally_base_at = observed_at
        candidate_rally = price - rally_base
        if candidate_rally >= rally:
            rally = candidate_rally
            rally_peak = price
            rally_peak_at = observed_at
    recovery = max(0.0, last_price - trough)
    pullback = max(0.0, rally_peak - last_price)
    prices = [price for _, price in points]
    position_percentile = _empirical_midrank(last_price, prices)
    gaps = [(right[0] - left[0]).total_seconds() for left, right in zip(points, points[1:])]
    return {
        "window_started_at": window_start.isoformat(),
        "window_ended_at": window_end.isoformat(),
        "sample_count": len(points),
        "max_sample_gap_seconds": max(gaps, default=0.0),
        "start": first_price,
        "end": last_price,
        "peak": peak,
        "peak_at": peak_at.isoformat(),
        "trough": trough,
        "trough_at": trough_at.isoformat(),
        "drawdown_points": drawdown,
        "recovery_points": recovery,
        "rally_points": rally,
        "rally_base": rally_base,
        "rally_base_at": rally_base_at.isoformat(),
        "rally_peak": rally_peak,
        "rally_peak_at": rally_peak_at.isoformat(),
        "pullback_points": pullback,
        "position_percentile": position_percentile,
    }


def _finite_number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


def _empirical_midrank(value: float, references: list[float]) -> float:
    less = sum(reference < value for reference in references)
    equal = sum(reference == value for reference in references)
    return 100.0 * (less + 0.5 * equal) / len(references)


def _time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
