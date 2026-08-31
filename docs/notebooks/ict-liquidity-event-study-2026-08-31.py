"""Causal ICT/SMC event study on the locally captured ES quote lake.

This is an offline research artifact.  It tests a deliberately small and
objective subset of the ICT vocabulary:

* first RTH sweep/reclaim of PDH/PDL, overnight high/low, and OR15 high/low;
* a causal five-bar structure break after the reclaim (an MSS proxy);
* an aligned displacement bar;
* a three-candle fair-value gap formed after confirmation and first retraced.

Every event is timestamped when it is knowable.  Entries use the next minute's
first observed ES price.  No persisted strategy decision, production threshold,
SPXW payoff approximation, or future pivot label is consumed.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, time, timedelta, timezone
import json
import math
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

import duckdb
import numpy as np

from spx_spark.analytics.options.strategy_payoff import (
    DEFAULT_MANAGEMENT_POLICY,
    conservative_vertical_bbo,
    vertical_economics,
)


UTC = timezone.utc
ET = ZoneInfo("America/New_York")
START = date(2026, 7, 13)
END = date(2026, 8, 28)
DEVELOPMENT_END = date(2026, 8, 12)
VALIDATION_END = date(2026, 8, 21)
HORIZONS = (5, 15, 30, 60)
STAGES = (
    "sweep",
    "sweep_mss",
    "sweep_mss_displacement",
    "sweep_mss_displacement_fvg",
    "sweep_mss_displacement_fvg_htf",
)
LEVEL_PRIORITY = {"PDH": 0, "PDL": 0, "ONH": 1, "ONL": 1, "OR15H": 2, "OR15L": 2}
RNG_SEED = 20260831
OPTION_QUOTE_AGE_SECONDS = 15.0
OPTION_SOURCE_SKEW_SECONDS = 2.0
VERTICAL_LONG_DELTAS = (0.40, 0.50, 0.60)
VERTICAL_WIDTH = 15.0


@dataclass(frozen=True)
class Bar:
    available_at: datetime
    open: float
    high: float
    low: float
    close: float
    observations: int


@dataclass(frozen=True)
class Level:
    name: str
    price: float
    side: str
    available_at: datetime


@dataclass(frozen=True)
class SweepContract:
    minimum_penetration_points: float = 0.5
    minimum_penetration_atr: float = 0.10
    maximum_extension_atr: float = 1.0
    reclaim_bars: int = 3
    mss_lookback_bars: int = 5
    mss_deadline_bars: int = 5
    mss_buffer_points: float = 0.25
    displacement_atr: float = 0.80
    fvg_minimum_points: float = 0.25
    fvg_formation_deadline_bars: int = 5
    fvg_retrace_deadline_bars: int = 10


@dataclass(frozen=True)
class Event:
    session_date: date
    direction: int
    level_names: tuple[str, ...]
    level_price: float
    penetration_index: int
    sweep_index: int
    sweep_extreme: float
    sweep_at: datetime
    atr_at_sweep: float
    prior_15m_move: float
    mss_index: int | None = None
    mss_reference: float | None = None
    displacement_index: int | None = None
    fvg_formed_index: int | None = None
    fvg_retrace_index: int | None = None
    fvg_lower: float | None = None
    fvg_upper: float | None = None
    htf_aligned: bool = False


def _json_safe(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        parsed = float(value)
        return parsed if math.isfinite(parsed) else None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _utc_at(day: date, hour: int, minute: int) -> datetime:
    return datetime.combine(day, time(hour, minute), tzinfo=ET).astimezone(UTC)


def _period(day: date) -> str:
    if day <= DEVELOPMENT_END:
        return "development"
    if day <= VALIDATION_END:
        return "validation"
    return "tail"


def _quote_files(data_root: Path) -> list[str]:
    quote_root = data_root / "lake" / "quotes" / "schema=v1"
    start = START - timedelta(days=4)
    end = END + timedelta(days=1)
    paths: list[str] = []
    current = start
    while current <= end:
        paths.extend(
            str(path)
            for path in sorted(
                (
                    quote_root
                    / f"date={current.isoformat()}"
                    / "provider=schwab"
                ).glob("hour=*/quotes.parquet")
            )
        )
        current += timedelta(days=1)
    return paths


def _load_es_minute_bars(data_root: Path) -> list[Bar]:
    files = _quote_files(data_root)
    if not files:
        raise RuntimeError("No Schwab quote parquet files found")
    connection = duckdb.connect()
    connection.execute("SET TimeZone='UTC'")
    try:
        rows = connection.execute(
            """
            WITH clean AS (
              SELECT received_at, effective_price
              FROM read_parquet(?, union_by_name=true)
              WHERE instrument_id = 'future:ES'
                AND quality = 'live'
                AND lower(coalesce(market_data_type, 'live')) IN ('live', '1')
                AND effective_price > 0
                AND source_at IS NOT NULL
                AND source_at >= received_at - INTERVAL 30 SECOND
                AND source_at <= received_at + INTERVAL 5 SECOND
            )
            SELECT
              date_trunc('minute', received_at) + INTERVAL 1 MINUTE AS available_at,
              arg_min(effective_price, received_at) AS open,
              max(effective_price) AS high,
              min(effective_price) AS low,
              arg_max(effective_price, received_at) AS close,
              count(*) AS observations
            FROM clean
            GROUP BY 1
            ORDER BY 1
            """,
            [files],
        ).fetchall()
    finally:
        connection.close()
    return [
        Bar(
            available_at=row[0].astimezone(UTC),
            open=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
            observations=int(row[5]),
        )
        for row in rows
    ]


def _window(
    bars: Sequence[Bar], start: datetime, end: datetime, *, include_end: bool = True
) -> list[Bar]:
    if include_end:
        return [bar for bar in bars if start < bar.available_at <= end]
    return [bar for bar in bars if start < bar.available_at < end]


def _session_bars(all_bars: Sequence[Bar]) -> dict[date, list[Bar]]:
    result: dict[date, list[Bar]] = {}
    current = START
    while current <= END:
        if current.weekday() < 5:
            rows = _window(all_bars, _utc_at(current, 9, 30), _utc_at(current, 16, 0))
            expected = 390
            if len(rows) >= math.ceil(0.95 * expected):
                result[current] = rows
        current += timedelta(days=1)
    return result


def _levels_for_day(
    day: date,
    all_bars: Sequence[Bar],
    sessions: Mapping[date, Sequence[Bar]],
    prior_day: date,
) -> list[Level]:
    prior = sessions[prior_day]
    overnight = _window(
        all_bars,
        _utc_at(day - timedelta(days=1), 18, 0),
        _utc_at(day, 9, 30),
    )
    opening = [bar for bar in sessions[day] if bar.available_at <= _utc_at(day, 9, 45)]
    if len(overnight) < 800 or len(opening) < 14:
        return []
    open_at = _utc_at(day, 9, 30)
    or_at = _utc_at(day, 9, 45)
    return [
        Level("PDH", max(bar.high for bar in prior), "high", open_at),
        Level("PDL", min(bar.low for bar in prior), "low", open_at),
        Level("ONH", max(bar.high for bar in overnight), "high", open_at),
        Level("ONL", min(bar.low for bar in overnight), "low", open_at),
        Level("OR15H", max(bar.high for bar in opening), "high", or_at),
        Level("OR15L", min(bar.low for bar in opening), "low", or_at),
    ]


def _true_ranges(bars: Sequence[Bar]) -> np.ndarray:
    output = np.full(len(bars), np.nan)
    for index, bar in enumerate(bars):
        previous = bars[index - 1].close if index else bar.open
        output[index] = max(
            bar.high - bar.low,
            abs(bar.high - previous),
            abs(bar.low - previous),
        )
    return output


def _atr_before(bars: Sequence[Bar], index: int, lookback: int = 14) -> float | None:
    if index < 3:
        return None
    values = _true_ranges(bars)[max(0, index - lookback) : index]
    finite = values[np.isfinite(values)]
    return float(np.mean(finite)) if len(finite) >= min(10, lookback) else None


def _prior_move(bars: Sequence[Bar], index: int, minutes: int) -> float:
    start = max(0, index - minutes)
    return float(bars[index].close - bars[start].close)


def _detect_level_sweep(
    day: date,
    bars: Sequence[Bar],
    level: Level,
    contract: SweepContract,
) -> Event | None:
    start = next(
        (index for index, bar in enumerate(bars) if bar.available_at > level.available_at),
        len(bars),
    )
    latest = _utc_at(day, 14, 59)
    penetration_index: int | None = None
    for index in range(start, len(bars)):
        bar = bars[index]
        if bar.available_at > latest:
            break
        atr = _atr_before(bars, index)
        if atr is None or atr <= 0:
            continue
        if penetration_index is not None and index - penetration_index > contract.reclaim_bars:
            penetration_index = None
        if penetration_index is None:
            extension = (
                bar.high - level.price
                if level.side == "high"
                else level.price - bar.low
            )
            minimum = max(
                contract.minimum_penetration_points,
                contract.minimum_penetration_atr * atr,
            )
            if minimum <= extension <= contract.maximum_extension_atr * atr:
                penetration_index = index
        if penetration_index is None:
            continue
        reclaimed = (
            bar.close < level.price if level.side == "high" else bar.close > level.price
        )
        if not reclaimed:
            continue
        direction = -1 if level.side == "high" else 1
        relevant = bars[penetration_index : index + 1]
        extreme = (
            max(row.high for row in relevant)
            if direction < 0
            else min(row.low for row in relevant)
        )
        return Event(
            session_date=day,
            direction=direction,
            level_names=(level.name,),
            level_price=level.price,
            penetration_index=penetration_index,
            sweep_index=index,
            sweep_extreme=extreme,
            sweep_at=bar.available_at,
            atr_at_sweep=atr,
            prior_15m_move=_prior_move(bars, penetration_index, 15),
        )
    return None


def _deduplicate(events: Sequence[Event]) -> list[Event]:
    grouped: list[list[Event]] = []
    for event in sorted(
        events,
        key=lambda item: (
            item.sweep_at,
            item.direction,
            LEVEL_PRIORITY[item.level_names[0]],
        ),
    ):
        if (
            grouped
            and grouped[-1][0].direction == event.direction
            and event.sweep_at - grouped[-1][-1].sweep_at <= timedelta(minutes=5)
        ):
            grouped[-1].append(event)
        else:
            grouped.append([event])
    output: list[Event] = []
    for cluster in grouped:
        chosen = min(cluster, key=lambda item: LEVEL_PRIORITY[item.level_names[0]])
        names = tuple(
            sorted(
                {name for item in cluster for name in item.level_names},
                key=lambda name: (LEVEL_PRIORITY[name], name),
            )
        )
        extreme = (
            max(item.sweep_extreme for item in cluster)
            if chosen.direction < 0
            else min(item.sweep_extreme for item in cluster)
        )
        output.append(replace(chosen, level_names=names, sweep_extreme=extreme))
    return output


def _attach_confirmations(
    event: Event, bars: Sequence[Bar], contract: SweepContract
) -> Event:
    reference_start = max(0, event.penetration_index - contract.mss_lookback_bars)
    reference_rows = bars[reference_start : event.penetration_index]
    if len(reference_rows) < 3:
        return event
    reference = (
        max(bar.high for bar in reference_rows)
        if event.direction > 0
        else min(bar.low for bar in reference_rows)
    )
    mss_index: int | None = None
    deadline = min(len(bars), event.sweep_index + contract.mss_deadline_bars + 1)
    for index in range(event.sweep_index, deadline):
        crossed = (
            bars[index].close >= reference + contract.mss_buffer_points
            if event.direction > 0
            else bars[index].close <= reference - contract.mss_buffer_points
        )
        if crossed:
            mss_index = index
            break
    if mss_index is None:
        return replace(event, mss_reference=reference)

    displacement_index: int | None = None
    for index in range(event.sweep_index, mss_index + 1):
        atr = _atr_before(bars, index)
        body = event.direction * (bars[index].close - bars[index].open)
        if atr is not None and body >= contract.displacement_atr * atr:
            displacement_index = index
            break
    updated = replace(
        event,
        mss_index=mss_index,
        mss_reference=reference,
        displacement_index=displacement_index,
    )
    if displacement_index is None:
        return updated

    formed: int | None = None
    lower: float | None = None
    upper: float | None = None
    formation_end = min(
        len(bars), mss_index + contract.fvg_formation_deadline_bars + 1
    )
    for index in range(max(mss_index, 2), formation_end):
        if bars[index].available_at - bars[index - 2].available_at > timedelta(minutes=3):
            continue
        if event.direction > 0:
            gap = bars[index].low - bars[index - 2].high
            candidate_lower, candidate_upper = bars[index - 2].high, bars[index].low
        else:
            gap = bars[index - 2].low - bars[index].high
            candidate_lower, candidate_upper = bars[index].high, bars[index - 2].low
        if gap >= contract.fvg_minimum_points:
            formed, lower, upper = index, candidate_lower, candidate_upper
            break
    if formed is None or lower is None or upper is None:
        return updated

    retrace: int | None = None
    retrace_end = min(len(bars), formed + contract.fvg_retrace_deadline_bars + 1)
    for index in range(formed + 1, retrace_end):
        overlaps = bars[index].low <= upper and bars[index].high >= lower
        if overlaps:
            retrace = index
            break
    if retrace is None:
        return replace(
            updated,
            fvg_formed_index=formed,
            fvg_lower=lower,
            fvg_upper=upper,
        )
    htf_move = _prior_move(bars, retrace, 60)
    return replace(
        updated,
        fvg_formed_index=formed,
        fvg_retrace_index=retrace,
        fvg_lower=lower,
        fvg_upper=upper,
        htf_aligned=event.direction * htf_move > 0,
    )


def _event_stage_index(event: Event, stage: str) -> int | None:
    if stage == "sweep":
        return event.sweep_index
    if stage == "sweep_mss":
        return event.mss_index
    if stage == "sweep_mss_displacement":
        return event.mss_index if event.displacement_index is not None else None
    if stage == "sweep_mss_displacement_fvg":
        return event.fvg_retrace_index
    if stage == "sweep_mss_displacement_fvg_htf":
        return event.fvg_retrace_index if event.htf_aligned else None
    raise ValueError(f"Unknown stage: {stage}")


def _forward_metrics(
    bars: Sequence[Bar], event: Event, signal_index: int
) -> dict[str, Any] | None:
    entry_index = signal_index + 1
    if entry_index >= len(bars):
        return None
    entry_bar = bars[entry_index]
    if entry_bar.available_at > _utc_at(event.session_date, 15, 0):
        return None
    if entry_index + max(HORIZONS) > len(bars):
        return None
    entry = entry_bar.open
    row: dict[str, Any] = {
        "session_date": event.session_date,
        "period": _period(event.session_date),
        "direction": event.direction,
        "level_names": event.level_names,
        "level_price": event.level_price,
        "sweep_at": event.sweep_at,
        "signal_at": bars[signal_index].available_at,
        "entry_at": entry_bar.available_at - timedelta(minutes=1),
        "entry_price": entry,
        "sweep_extreme": event.sweep_extreme,
        "atr_at_sweep": event.atr_at_sweep,
        "prior_15m_move": event.prior_15m_move,
    }
    for horizon in HORIZONS:
        future = bars[entry_index : entry_index + horizon]
        exit_price = future[-1].close
        aligned = event.direction * (exit_price - entry)
        favorable = (
            max(bar.high for bar in future) - entry
            if event.direction > 0
            else entry - min(bar.low for bar in future)
        )
        adverse = (
            entry - min(bar.low for bar in future)
            if event.direction > 0
            else max(bar.high for bar in future) - entry
        )
        row[f"return_{horizon}m_points"] = aligned
        row[f"return_{horizon}m_after_0_5pt"] = aligned - 0.5
        row[f"mfe_{horizon}m_points"] = favorable
        row[f"mae_{horizon}m_points"] = adverse

    stop = event.sweep_extreme - event.direction * 0.5
    risk = event.direction * (entry - stop)
    row["stop_price"] = stop
    row["risk_points"] = risk
    for multiple in (1.0, 2.0):
        outcome: str = "invalid_risk"
        if 0.5 <= risk <= 30.0:
            target = entry + event.direction * multiple * risk
            outcome = "unresolved"
            for bar in bars[entry_index : entry_index + 60]:
                if event.direction > 0:
                    stop_hit, target_hit = bar.low <= stop, bar.high >= target
                else:
                    stop_hit, target_hit = bar.high >= stop, bar.low <= target
                if stop_hit:
                    outcome = "loss"
                    break
                if target_hit:
                    outcome = "win"
                    break
        row[f"first_touch_{multiple:g}r_60m"] = outcome
    return row


def _candidate_controls(
    sessions: Mapping[date, Sequence[Bar]],
    events: Sequence[Event],
) -> list[dict[str, Any]]:
    event_times: dict[date, list[datetime]] = defaultdict(list)
    for event in events:
        event_times[event.session_date].append(event.sweep_at)
    candidates: list[dict[str, Any]] = []
    for day, bars in sessions.items():
        for index in range(15, len(bars) - max(HORIZONS) - 1):
            bar = bars[index]
            local = bar.available_at.astimezone(ET)
            if not time(9, 45) <= local.time() <= time(14, 59):
                continue
            if local.minute % 5:
                continue
            if any(abs((bar.available_at - at).total_seconds()) <= 15 * 60 for at in event_times[day]):
                continue
            atr = _atr_before(bars, index)
            if atr is None or atr <= 0:
                continue
            entry_index = index + 1
            entry = bars[entry_index].open
            row: dict[str, Any] = {
                "session_date": day,
                "index": index,
                "minute_of_day": local.hour * 60 + local.minute,
                "atr": atr,
                "prior_15m_move": _prior_move(bars, index, 15),
            }
            for direction in (-1, 1):
                for horizon in HORIZONS:
                    future = bars[entry_index : entry_index + horizon]
                    row[f"return_{direction}_{horizon}m"] = direction * (
                        future[-1].close - entry
                    )
            candidates.append(row)
    return candidates


def _attach_matched_controls(
    rows: list[dict[str, Any]], controls: Sequence[Mapping[str, Any]]
) -> None:
    for row in rows:
        local = row["signal_at"].astimezone(ET)
        minute = local.hour * 60 + local.minute
        atr = float(row["atr_at_sweep"])
        direction = int(row["direction"])
        event_prior = direction * float(row["prior_15m_move"])
        eligible = []
        for candidate in controls:
            if candidate["session_date"] == row["session_date"]:
                continue
            minute_distance = abs(int(candidate["minute_of_day"]) - minute)
            candidate_atr = float(candidate["atr"])
            if minute_distance > 20 or not 0.65 <= candidate_atr / atr <= 1.55:
                continue
            candidate_prior = direction * float(candidate["prior_15m_move"])
            score = (
                minute_distance / 20.0
                + abs(math.log(candidate_atr / atr))
                + abs(candidate_prior - event_prior) / max(atr * 5.0, 1.0)
            )
            eligible.append((score, candidate))
        eligible.sort(key=lambda item: (item[0], item[1]["session_date"]))
        selected: list[Mapping[str, Any]] = []
        selected_days: set[date] = set()
        for _, candidate in eligible:
            candidate_day = candidate["session_date"]
            if candidate_day in selected_days:
                continue
            selected.append(candidate)
            selected_days.add(candidate_day)
            if len(selected) == 3:
                break
        row["matched_control_count"] = len(selected)
        for horizon in HORIZONS:
            values = [
                float(candidate[f"return_{direction}_{horizon}m"])
                for candidate in selected
            ]
            control = float(np.mean(values)) if values else math.nan
            row[f"control_{horizon}m_points"] = control
            row[f"delta_vs_control_{horizon}m_points"] = (
                float(row[f"return_{horizon}m_points"]) - control
                if math.isfinite(control)
                else math.nan
            )


def _cluster_bootstrap(
    rows: Sequence[Mapping[str, Any]], field: str, draws: int = 10_000
) -> tuple[float | None, float | None]:
    grouped: dict[date, list[float]] = defaultdict(list)
    for row in rows:
        value = float(row.get(field, math.nan))
        if math.isfinite(value):
            grouped[row["session_date"]].append(value)
    days = sorted(grouped)
    if len(days) < 2:
        return None, None
    session_means = np.asarray(
        [np.mean(grouped[day]) for day in days],
        dtype=float,
    )
    rng = np.random.default_rng(RNG_SEED + sum(ord(char) for char in field))
    values = np.mean(
        rng.choice(session_means, size=(draws, len(session_means)), replace=True),
        axis=1,
    )
    return float(np.quantile(values, 0.025)), float(np.quantile(values, 0.975))


def _session_sign_flip_p(rows: Sequence[Mapping[str, Any]], field: str) -> float | None:
    grouped: dict[date, list[float]] = defaultdict(list)
    for row in rows:
        value = float(row.get(field, math.nan))
        if math.isfinite(value):
            grouped[row["session_date"]].append(value)
    session_means = np.asarray(
        [np.mean(grouped[day]) for day in sorted(grouped)], dtype=float
    )
    if len(session_means) < 2:
        return None
    observed = float(np.mean(session_means))
    rng = np.random.default_rng(RNG_SEED + 17 + sum(ord(char) for char in field))
    draws = np.mean(
        session_means
        * rng.choice(np.asarray((-1.0, 1.0)), size=(20_000, len(session_means))),
        axis=1,
    )
    return float((1 + np.sum(draws >= observed)) / (len(draws) + 1))


def _summarize(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    output: dict[str, Any] = {
        "events": len(rows),
        "sessions": len({row["session_date"] for row in rows}),
    }
    for horizon in HORIZONS:
        field = f"return_{horizon}m_points"
        control_field = f"control_{horizon}m_points"
        delta_field = f"delta_vs_control_{horizon}m_points"
        values = np.asarray([float(row[field]) for row in rows], dtype=float)
        controls = np.asarray(
            [float(row.get(control_field, math.nan)) for row in rows], dtype=float
        )
        deltas = np.asarray(
            [float(row.get(delta_field, math.nan)) for row in rows], dtype=float
        )
        valid_controls = controls[np.isfinite(controls)]
        valid_deltas = deltas[np.isfinite(deltas)]
        low, high = _cluster_bootstrap(rows, delta_field)
        output[f"{horizon}m"] = {
            "mean_points": float(np.mean(values)) if len(values) else None,
            "median_points": float(np.median(values)) if len(values) else None,
            "positive_rate": float(np.mean(values > 0)) if len(values) else None,
            "mean_after_0_5pt": float(np.mean(values - 0.5)) if len(values) else None,
            "mean_mfe": (
                float(np.mean([row[f"mfe_{horizon}m_points"] for row in rows]))
                if rows
                else None
            ),
            "mean_mae": (
                float(np.mean([row[f"mae_{horizon}m_points"] for row in rows]))
                if rows
                else None
            ),
            "matched_control_mean": (
                float(np.mean(valid_controls)) if len(valid_controls) else None
            ),
            "delta_vs_control_mean": (
                float(np.mean(valid_deltas)) if len(valid_deltas) else None
            ),
            "delta_session_bootstrap_95": [low, high],
            "delta_session_sign_flip_p_one_sided": _session_sign_flip_p(
                rows, delta_field
            ),
        }
    for multiple in (1.0, 2.0):
        field = f"first_touch_{multiple:g}r_60m"
        resolved = [row[field] for row in rows if row[field] in {"win", "loss"}]
        output[f"first_touch_{multiple:g}r_60m"] = {
            "resolved": len(resolved),
            "win_rate": (
                sum(value == "win" for value in resolved) / len(resolved)
                if resolved
                else None
            ),
        }
    return output


def _sweep_breakdown(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        level = str(row["level_names"][0])
        groups[f"level:{level}"].append(row)
        groups["direction:bullish" if int(row["direction"]) > 0 else "direction:bearish"].append(row)
        local = row["signal_at"].astimezone(ET).time()
        if local < time(10, 30):
            bucket = "09:30-10:30"
        elif local < time(12, 0):
            bucket = "10:30-12:00"
        elif local < time(14, 0):
            bucket = "12:00-14:00"
        else:
            bucket = "14:00-15:00"
        groups[f"time:{bucket}"].append(row)
    output: dict[str, Any] = {}
    for name, members in sorted(groups.items()):
        summary = _summarize(members)
        period_30m = {}
        for period in ("development", "validation", "tail"):
            period_members = [row for row in members if row["period"] == period]
            period_30m[period] = {
                "events": len(period_members),
                "mean_points": (
                    float(
                        np.mean(
                            [
                                float(row["return_30m_points"])
                                for row in period_members
                            ]
                        )
                    )
                    if period_members
                    else None
                ),
            }
        output[name] = {
            "events": summary["events"],
            "sessions": summary["sessions"],
            "mean_15m_points": summary["15m"]["mean_points"],
            "delta_15m_vs_control": summary["15m"]["delta_vs_control_mean"],
            "mean_30m_points": summary["30m"]["mean_points"],
            "delta_30m_vs_control": summary["30m"]["delta_vs_control_mean"],
            "period_30m": period_30m,
        }
    return output


def _detect_events(
    all_bars: Sequence[Bar],
    sessions: Mapping[date, Sequence[Bar]],
    contract: SweepContract,
) -> tuple[list[Event], dict[str, Any]]:
    days = sorted(sessions)
    events: list[Event] = []
    raw_count = 0
    eligible_days = 0
    for ordinal, day in enumerate(days):
        if ordinal == 0:
            continue
        prior_day = days[ordinal - 1]
        levels = _levels_for_day(day, all_bars, sessions, prior_day)
        if not levels:
            continue
        eligible_days += 1
        raw = [
            event
            for level in levels
            if (event := _detect_level_sweep(day, sessions[day], level, contract))
            is not None
        ]
        raw_count += len(raw)
        events.extend(
            _attach_confirmations(event, sessions[day], contract)
            for event in _deduplicate(raw)
        )
    return events, {
        "complete_rth_sessions": len(sessions),
        "eligible_sessions_with_prior_and_overnight": eligible_days,
        "raw_level_sweeps": raw_count,
        "deduplicated_sweeps": len(events),
    }


def _stage_rows(
    events: Sequence[Event], sessions: Mapping[date, Sequence[Bar]]
) -> dict[str, list[dict[str, Any]]]:
    output: dict[str, list[dict[str, Any]]] = {stage: [] for stage in STAGES}
    for event in events:
        for stage in STAGES:
            index = _event_stage_index(event, stage)
            if index is None:
                continue
            metrics = _forward_metrics(sessions[event.session_date], event, index)
            if metrics is not None:
                metrics["stage"] = stage
                output[stage].append(metrics)
    controls = _candidate_controls(sessions, events)
    for rows in output.values():
        _attach_matched_controls(rows, controls)
    return output


def _day_quote_files(data_root: Path, day: date) -> list[str]:
    return [
        str(path)
        for path in sorted(
            (
                data_root
                / "lake"
                / "quotes"
                / "schema=v1"
                / f"date={day.isoformat()}"
                / "provider=schwab"
            ).glob("hour=*/quotes.parquet")
        )
    ]


def _iso_utc(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _load_option_snapshots(
    connection: duckdb.DuckDBPyConnection,
    files: Sequence[str],
    day: date,
    requested: Sequence[datetime],
    *,
    contract_ids: Sequence[str] | None = None,
) -> dict[datetime, dict[tuple[float, str], dict[str, Any]]]:
    unique = sorted(set(requested))
    if not unique or not files:
        return {}
    values_sql = ",".join("(?)" for _ in unique)
    contract_clause = ""
    parameters: list[object] = [*unique, list(files), day]
    if contract_ids:
        contract_clause = " AND q.instrument_id IN (SELECT unnest(?))"
        parameters.append(sorted(set(contract_ids)))
    rows = connection.execute(
        f"""
        WITH requested(requested_at) AS (VALUES {values_sql}), ranked AS (
          SELECT
            requested.requested_at,
            q.instrument_id,
            q.strike,
            q."right",
            q.bid,
            q.ask,
            q.delta,
            q.source_at,
            q.received_at,
            row_number() OVER (
              PARTITION BY requested.requested_at, q.instrument_id
              ORDER BY q.received_at DESC
            ) AS position
          FROM requested
          JOIN read_parquet(?, union_by_name=true) q
            ON q.received_at <= requested.requested_at
           AND q.received_at >= requested.requested_at - INTERVAL 30 SECOND
          WHERE q.instrument_type = 'option'
            AND q.expiry = ?
            AND q.quality = 'live'
            AND q.source_at IS NOT NULL
            AND q.source_at <= requested.requested_at
            AND q.source_at >= requested.requested_at - INTERVAL 30 SECOND
            {contract_clause}
        )
        SELECT requested_at, instrument_id, strike, "right", bid, ask, delta,
               source_at, received_at
        FROM ranked
        WHERE position = 1
        ORDER BY requested_at, instrument_id
        """,
        parameters,
    ).fetchall()
    snapshots: dict[datetime, dict[tuple[float, str], dict[str, Any]]] = {
        requested_at: {} for requested_at in unique
    }
    for (
        requested_at,
        instrument_id,
        strike,
        right,
        bid,
        ask,
        delta,
        source_at,
        received_at,
    ) in rows:
        if strike is None or right not in {"C", "P"}:
            continue
        snapshots[requested_at.astimezone(UTC)][(float(strike), str(right))] = {
            "contract_id": str(instrument_id),
            "strike": float(strike),
            "right": str(right),
            "bid": float(bid) if bid is not None else None,
            "ask": float(ask) if ask is not None else None,
            "delta": float(delta) if delta is not None else None,
            "source_at": _iso_utc(source_at),
            "received_at": _iso_utc(received_at),
            "provider": "schwab",
        }
    return snapshots


def _select_vertical(
    chain: Mapping[tuple[float, str], Mapping[str, Any]],
    direction: int,
    at: datetime,
    target_delta: float,
) -> tuple[dict[str, Any] | None, str]:
    right = "C" if direction > 0 else "P"
    eligible = []
    for leg in chain.values():
        if leg.get("right") != right:
            continue
        delta = leg.get("delta")
        if not isinstance(delta, int | float) or not math.isfinite(float(delta)):
            continue
        absolute = abs(float(delta))
        if 0.30 <= absolute <= 0.80:
            eligible.append(dict(leg))
    if not eligible:
        return None, "no_30_to_80_delta_long_leg"
    long_leg = min(
        eligible,
        key=lambda leg: abs(abs(float(leg["delta"])) - target_delta),
    )
    short_strike = float(long_leg["strike"]) + direction * VERTICAL_WIDTH
    short_leg = chain.get((short_strike, right))
    if not isinstance(short_leg, Mapping):
        return None, "fixed_15_point_short_leg_missing"
    quote = conservative_vertical_bbo(
        long_leg,
        short_leg,
        now=at,
        max_quote_age_seconds=OPTION_QUOTE_AGE_SECONDS,
        max_source_skew_seconds=OPTION_SOURCE_SKEW_SECONDS,
    )
    if quote.get("status") != "ready":
        reasons = ",".join(str(reason) for reason in quote.get("reasons", ()))
        return None, f"exact_bbo_unready:{reasons or 'unknown'}"
    try:
        economics = vertical_economics(
            long_strike=float(long_leg["strike"]),
            short_strike=float(short_leg["strike"]),
            net_debit=float(quote["ask"]),
            right=right,
        )
    except ValueError:
        return None, "vertical_economics_invalid"
    if float(economics["debit_fraction_of_width"]) > 0.45:
        return None, "debit_fraction_above_45pct"
    return (
        {
            "right": right,
            "long_leg": long_leg,
            "short_leg": dict(short_leg),
            "entry_debit": float(quote["ask"]),
            "entry_bid": float(quote["bid"]),
            "economics": economics,
        },
        "ready",
    )


def _vertical_replay(
    data_root: Path,
    signal_rows: Sequence[Mapping[str, Any]],
    *,
    target_delta: float,
) -> dict[str, Any]:
    by_day: dict[date, list[Mapping[str, Any]]] = defaultdict(list)
    for row in signal_rows:
        by_day[row["session_date"]].append(row)
    connection = duckdb.connect()
    connection.execute("SET TimeZone='UTC'")
    candidates: list[dict[str, Any]] = []
    entry_failures: dict[str, Counter[str]] = defaultdict(Counter)
    try:
        for day, rows in sorted(by_day.items()):
            files = _day_quote_files(data_root, day)
            requested_entries = [row["signal_at"] + timedelta(seconds=5) for row in rows]
            entry_snapshots = _load_option_snapshots(
                connection, files, day, requested_entries
            )
            day_candidates = []
            for row, entry_at in zip(rows, requested_entries, strict=True):
                candidate, reason = _select_vertical(
                    entry_snapshots.get(entry_at, {}),
                    int(row["direction"]),
                    entry_at,
                    target_delta,
                )
                if candidate is None:
                    entry_failures[str(row["stage"])][reason] += 1
                    continue
                day_candidates.append(
                    {
                        "stage": str(row["stage"]),
                        "session_date": day,
                        "period": row["period"],
                        "direction": int(row["direction"]),
                        "level_names": row["level_names"],
                        "signal_at": row["signal_at"],
                        "entry_at": entry_at,
                        **candidate,
                    }
                )
            contract_ids = [
                str(candidate[leg_name]["contract_id"])
                for candidate in day_candidates
                for leg_name in ("long_leg", "short_leg")
            ]
            exit_times = [
                candidate["entry_at"] + timedelta(minutes=horizon)
                for candidate in day_candidates
                for horizon in (15, 30)
            ]
            exit_snapshots = _load_option_snapshots(
                connection,
                files,
                day,
                exit_times,
                contract_ids=contract_ids,
            )
            for candidate in day_candidates:
                fees_points = (
                    DEFAULT_MANAGEMENT_POLICY.fees_per_leg_per_side * 2 * 2 / 100.0
                )
                for horizon in (15, 30):
                    exit_at = candidate["entry_at"] + timedelta(minutes=horizon)
                    chain = exit_snapshots.get(exit_at, {})
                    long_key = (
                        float(candidate["long_leg"]["strike"]),
                        str(candidate["right"]),
                    )
                    short_key = (
                        float(candidate["short_leg"]["strike"]),
                        str(candidate["right"]),
                    )
                    long_leg, short_leg = chain.get(long_key), chain.get(short_key)
                    if not isinstance(long_leg, Mapping) or not isinstance(
                        short_leg, Mapping
                    ):
                        continue
                    quote = conservative_vertical_bbo(
                        long_leg,
                        short_leg,
                        now=exit_at,
                        max_quote_age_seconds=OPTION_QUOTE_AGE_SECONDS,
                        max_source_skew_seconds=OPTION_SOURCE_SKEW_SECONDS,
                    )
                    if quote.get("status") != "ready":
                        continue
                    pnl_points = (
                        float(quote["bid"])
                        - float(candidate["entry_debit"])
                        - fees_points
                    )
                    candidate[f"exit_{horizon}m_bid"] = float(quote["bid"])
                    candidate[f"pnl_{horizon}m_points"] = pnl_points
                    candidate[f"pnl_{horizon}m_usd"] = 100.0 * pnl_points
                    candidate[f"return_{horizon}m_on_debit"] = pnl_points / float(
                        candidate["entry_debit"]
                    )
                candidates.append(candidate)
    finally:
        connection.close()

    def summarize(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        output: dict[str, Any] = {
            "entered": len(rows),
            "sessions": len({row["session_date"] for row in rows}),
            "call_spreads": sum(row["right"] == "C" for row in rows),
            "put_spreads": sum(row["right"] == "P" for row in rows),
        }
        for horizon in (15, 30):
            field = f"pnl_{horizon}m_usd"
            priced = [row for row in rows if isinstance(row.get(field), int | float)]
            values = np.asarray([float(row[field]) for row in priced], dtype=float)
            low, high = _cluster_bootstrap(priced, field)
            output[f"{horizon}m"] = {
                "priced": len(priced),
                "sessions": len({row["session_date"] for row in priced}),
                "mean_usd": float(np.mean(values)) if len(values) else None,
                "median_usd": float(np.median(values)) if len(values) else None,
                "win_rate": float(np.mean(values > 0)) if len(values) else None,
                "cvar10_usd": (
                    float(
                        np.mean(
                            np.sort(values)[: max(1, math.ceil(0.10 * len(values)))]
                        )
                    )
                    if len(values)
                    else None
                ),
                "session_bootstrap_95_mean_usd": [low, high],
            }
        return output

    def paired_vs_sweep(stage: str) -> dict[str, Any]:
        def key(row: Mapping[str, Any]) -> tuple[object, ...]:
            return (
                row["session_date"],
                tuple(row["level_names"]),
                row["direction"],
            )

        sweep_by_event = {
            key(row): row for row in candidates if row["stage"] == "sweep"
        }
        stage_by_event = {
            key(row): row for row in candidates if row["stage"] == stage
        }
        output: dict[str, Any] = {}
        for horizon in (15, 30):
            pnl_field = f"pnl_{horizon}m_usd"
            paired = []
            for event_key in sweep_by_event.keys() & stage_by_event.keys():
                sweep_row = sweep_by_event[event_key]
                stage_row = stage_by_event[event_key]
                if not isinstance(sweep_row.get(pnl_field), int | float):
                    continue
                if not isinstance(stage_row.get(pnl_field), int | float):
                    continue
                paired.append(
                    (
                        stage_row,
                        float(stage_row[pnl_field]) - float(sweep_row[pnl_field]),
                    )
                )
            deltas = np.asarray([delta for _, delta in paired], dtype=float)
            output[f"{horizon}m"] = {
                "pairs": len(paired),
                "sessions": len({row["session_date"] for row, _ in paired}),
                "mean_incremental_usd": (
                    float(np.mean(deltas)) if len(deltas) else None
                ),
                "median_incremental_usd": (
                    float(np.median(deltas)) if len(deltas) else None
                ),
            }
        return output

    by_stage = {}
    for stage in STAGES:
        stage_candidates = [
            candidate for candidate in candidates if candidate["stage"] == stage
        ]
        by_stage[stage] = {
            "source_signals": sum(
                str(row["stage"]) == stage for row in signal_rows
            ),
            "entry_failures": sum(entry_failures[stage].values()),
            "entry_failure_reasons": dict(entry_failures[stage].most_common()),
            "all": summarize(stage_candidates),
            **{
                period: summarize(
                    [
                        candidate
                        for candidate in stage_candidates
                        if candidate["period"] == period
                    ]
                )
                for period in ("development", "validation", "tail")
            },
        }
    return {
        "contract": {
            "stages": STAGES,
            "target_long_delta": target_delta,
            "entry_delay_seconds": 5,
            "structure": (
                f"{target_delta:.0%}-delta nearest long, "
                f"{VERTICAL_WIDTH:.0f}-point debit vertical"
            ),
            "entry": "conservative exact package ask",
            "exit": "conservative exact package bid at fixed 15/30 minutes",
            "maximum_debit_fraction": 0.45,
            "fees_per_leg_per_side_usd": DEFAULT_MANAGEMENT_POLICY.fees_per_leg_per_side,
            "automatic_ordering": False,
            "action_authority": "none",
        },
        "source_signals": len(signal_rows),
        "by_stage": by_stage,
        "paired_vs_sweep": {
            stage: paired_vs_sweep(stage) for stage in STAGES[1:]
        },
        "candidates": candidates,
    }


def _analysis(
    data_root: Path,
    all_bars: Sequence[Bar],
    sessions: Mapping[date, Sequence[Bar]],
) -> dict[str, Any]:
    primary = SweepContract()
    events, coverage = _detect_events(all_bars, sessions, primary)
    rows_by_stage = _stage_rows(events, sessions)
    stage_summary: dict[str, Any] = {}
    for stage, rows in rows_by_stage.items():
        stage_summary[stage] = {
            "all": _summarize(rows),
            **{
                period: _summarize([row for row in rows if row["period"] == period])
                for period in ("development", "validation", "tail")
            },
        }

    sensitivity = []
    for penetration in (0.5, 1.0, 2.0):
        for reclaim_bars in (1, 3, 5):
            contract = replace(
                primary,
                minimum_penetration_points=penetration,
                reclaim_bars=reclaim_bars,
            )
            contract_events, contract_coverage = _detect_events(
                all_bars, sessions, contract
            )
            rows = _stage_rows(contract_events, sessions)["sweep"]
            summary = _summarize(rows)
            sensitivity.append(
                {
                    "minimum_penetration_points": penetration,
                    "reclaim_bars": reclaim_bars,
                    "events": summary["events"],
                    "sessions": summary["sessions"],
                    "mean_15m_points": summary["15m"]["mean_points"],
                    "delta_15m_vs_control": summary["15m"][
                        "delta_vs_control_mean"
                    ],
                    "mean_30m_points": summary["30m"]["mean_points"],
                    "delta_30m_vs_control": summary["30m"][
                        "delta_vs_control_mean"
                    ],
                    "deduplicated_sweeps": contract_coverage[
                        "deduplicated_sweeps"
                    ],
                }
            )

    event_records = []
    for event in events:
        record = asdict(event)
        record["available_stages"] = [
            stage for stage in STAGES if _event_stage_index(event, stage) is not None
        ]
        event_records.append(record)
    return {
        "contract": {
            "research_only": True,
            "action_authority": "none",
            "instrument": "ES quote-derived one-minute OHLC",
            "data_start": START,
            "data_end": END,
            "entry_rule": "next minute first observed price after causal confirmation",
            "cost_diagnostic": "0.5 ES point deducted from directional forward return",
            "same_bar_stop_target_rule": "stop first (conservative)",
            "matched_control": (
                "three distinct other sessions, within 20 minutes of day clock, "
                "similar trailing ATR and aligned prior-15m move"
            ),
            "primary_sweep_contract": asdict(primary),
            "stage_definitions": {
                "sweep": "objective level penetration then close back through level",
                "sweep_mss": "close through causal pre-sweep five-bar opposite extreme",
                "sweep_mss_displacement": "MSS plus aligned body >= 0.8 trailing ATR",
                "sweep_mss_displacement_fvg": (
                    "post-confirmation three-bar gap followed by a later first retrace; "
                    "entry remains next minute, never at a retrospectively chosen touch"
                ),
                "sweep_mss_displacement_fvg_htf": (
                    "same setup with aligned trailing 60-minute price move"
                ),
            },
        },
        "coverage": coverage,
        "session_dates": sorted(sessions),
        "stage_summary": stage_summary,
        "sweep_breakdown": _sweep_breakdown(rows_by_stage["sweep"]),
        "spxw_vertical_replay": {
            f"{target_delta:.2f}": _vertical_replay(
                data_root,
                [row for stage in STAGES for row in rows_by_stage[stage]],
                target_delta=target_delta,
            )
            for target_delta in VERTICAL_LONG_DELTAS
        },
        "sensitivity": sensitivity,
        "events": event_records,
    }


def _format(value: object, digits: int = 2) -> str:
    if not isinstance(value, int | float) or not math.isfinite(float(value)):
        return "—"
    return f"{float(value):.{digits}f}"


def _markdown(artifact: Mapping[str, Any]) -> str:
    lines = [
        "# ES ICT/SMC 因果事件研究 · 2026-08-31",
        "",
        "本报告只研究客观 session level 的 Sweep → MSS proxy → Displacement → FVG retrace。",
        "所有信号在实际可知时刻发布，下一分钟入场；不读取生产候选，也不赋予交易权限。",
        "",
        "## 数据覆盖",
        "",
        f"- 完整 RTH sessions：{artifact['coverage']['complete_rth_sessions']}",
        (
            "- 可用 prior/overnight levels 的 sessions："
            f"{artifact['coverage']['eligible_sessions_with_prior_and_overnight']}"
        ),
        f"- 原始逐 level sweep：{artifact['coverage']['raw_level_sweeps']}",
        f"- 5 分钟同向去重后：{artifact['coverage']['deduplicated_sweeps']}",
        "",
        "## 消融结果",
        "",
        "| 阶段 | 样本/会话 | 15m均值 | 15m对照增量 | 30m均值 | 30m对照增量 | 30m bootstrap 95% |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for stage in STAGES:
        summary = artifact["stage_summary"][stage]["all"]
        ci = summary["30m"]["delta_session_bootstrap_95"]
        lines.append(
            "| "
            + " | ".join(
                (
                    stage,
                    f"{summary['events']}/{summary['sessions']}",
                    _format(summary["15m"]["mean_points"]),
                    _format(summary["15m"]["delta_vs_control_mean"]),
                    _format(summary["30m"]["mean_points"]),
                    _format(summary["30m"]["delta_vs_control_mean"]),
                    f"[{_format(ci[0])}, {_format(ci[1])}]",
                )
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## 时段外推",
            "",
            "| 阶段 | 分段 | 样本/会话 | 15m均值 | 15m对照增量 | 30m均值 | 30m对照增量 |",
            "|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    for stage in STAGES:
        for period in ("development", "validation", "tail"):
            summary = artifact["stage_summary"][stage][period]
            lines.append(
                "| "
                + " | ".join(
                    (
                        stage,
                        period,
                        f"{summary['events']}/{summary['sessions']}",
                        _format(summary["15m"]["mean_points"]),
                        _format(summary["15m"]["delta_vs_control_mean"]),
                        _format(summary["30m"]["mean_points"]),
                        _format(summary["30m"]["delta_vs_control_mean"]),
                    )
                )
                + " |"
            )

    lines.extend(
        [
            "",
            "## SPXW Directional Spread exact-BBO 消融",
            "",
            "预先固定比较最接近 40Δ/50Δ/60Δ 的 long leg，均为 15 点 Debit Vertical；信号后 5 秒按保守 ask 入场，固定 15/30 分钟按保守 bid 离场，含双边手续费，不事后选择最佳 Delta。",
            "",
            "| Long Δ | ICT 阶段 | 信号→入场/会话 | C/P | 15m均值$ | 30m均值$ | 30m胜率 | 30m CVaR10$ | 30m bootstrap 95% |",
            "|---:|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    option_replay = artifact["spxw_vertical_replay"]
    for delta_key, replay in sorted(option_replay.items()):
        for stage in STAGES:
            stage_replay = replay["by_stage"][stage]
            row = stage_replay["all"]
            ci = row["30m"]["session_bootstrap_95_mean_usd"]
            win_rate = row["30m"]["win_rate"]
            lines.append(
                "| "
                + " | ".join(
                    (
                        f"{100.0 * float(delta_key):.0f}Δ",
                        stage,
                        f"{stage_replay['source_signals']}→{row['entered']}/{row['sessions']}",
                        f"{row['call_spreads']}/{row['put_spreads']}",
                        _format(row["15m"]["mean_usd"]),
                        _format(row["30m"]["mean_usd"]),
                        (
                            f"{100.0 * win_rate:.1f}%"
                            if isinstance(win_rate, int | float)
                            else "—"
                        ),
                        _format(row["30m"]["cvar10_usd"]),
                        f"[{_format(ci[0])}, {_format(ci[1])}]",
                    )
                )
                + " |"
            )
    lines.extend(
        [
            "",
            "| Long Δ | ICT 阶段 | 分段 | 定价笔数/会话 | 30m均值$ |",
            "|---:|---|---|---:|---:|",
        ]
    )
    for delta_key, replay in sorted(option_replay.items()):
        for stage in ("sweep", "sweep_mss_displacement"):
            stage_replay = replay["by_stage"][stage]
            for period in ("development", "validation", "tail"):
                row = stage_replay[period]
                lines.append(
                    "| "
                    + " | ".join(
                        (
                            f"{100.0 * float(delta_key):.0f}Δ",
                            stage,
                            period,
                            f"{row['30m']['priced']}/{row['30m']['sessions']}",
                            _format(row["30m"]["mean_usd"]),
                        )
                    )
                    + " |"
                )

    lines.extend(
        [
            "",
            "### 同一事件相对 Sweep 的配对差异",
            "",
            "正值才表示后续确认改变了入场/合约后的 PnL；零值表示表面改善主要来自删掉其他 Sweep，而非更好的入场时点。",
            "",
            "| Long Δ | 后续阶段 | 30m配对/会话 | 30m增量均值$ | 30m增量中位$ |",
            "|---:|---|---:|---:|---:|",
        ]
    )
    for delta_key, replay in sorted(option_replay.items()):
        for stage in STAGES[1:]:
            paired = replay["paired_vs_sweep"][stage]["30m"]
            lines.append(
                "| "
                + " | ".join(
                    (
                        f"{100.0 * float(delta_key):.0f}Δ",
                        stage,
                        f"{paired['pairs']}/{paired['sessions']}",
                        _format(paired["mean_incremental_usd"]),
                        _format(paired["median_incremental_usd"]),
                    )
                )
                + " |"
            )

    lines.append("")
    for delta_key, replay in sorted(option_replay.items()):
        stage_replay = replay["by_stage"]["sweep"]
        reasons = "；".join(
            f"{reason}={count}"
            for reason, count in stage_replay["entry_failure_reasons"].items()
        )
        lines.append(
            f"- {100.0 * float(delta_key):.0f}Δ Sweep 未入场：{reasons or '无'}"
        )

    lines.extend(
        [
            "",
            "## Sweep 来源拆分",
            "",
            "| 分组 | 样本/会话 | 15m均值 | 30m均值 | 30m对照增量 | Dev/Val/Tail 30m |",
            "|---|---:|---:|---:|---:|---:|",
        ]
    )
    for name, row in artifact["sweep_breakdown"].items():
        lines.append(
            "| "
            + " | ".join(
                (
                    name,
                    f"{row['events']}/{row['sessions']}",
                    _format(row["mean_15m_points"]),
                    _format(row["mean_30m_points"]),
                    _format(row["delta_30m_vs_control"]),
                    "/".join(
                        f"{row['period_30m'][period]['events']}:"
                        f"{_format(row['period_30m'][period]['mean_points'])}"
                        for period in ("development", "validation", "tail")
                    ),
                )
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Sweep 参数敏感性（不择优）",
            "",
            "| 最小穿越 | 回收分钟 | 样本/会话 | 15m均值 | 15m对照增量 | 30m均值 | 30m对照增量 |",
            "|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in artifact["sensitivity"]:
        lines.append(
            "| "
            + " | ".join(
                (
                    _format(row["minimum_penetration_points"], 1),
                    str(row["reclaim_bars"]),
                    f"{row['events']}/{row['sessions']}",
                    _format(row["mean_15m_points"]),
                    _format(row["delta_15m_vs_control"]),
                    _format(row["mean_30m_points"]),
                    _format(row["delta_30m_vs_control"]),
                )
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## 研究边界",
            "",
            "- 主研究是 ES 方向事件；SPXW exact-BBO 预先固定比较 40Δ/50Δ/60Δ、15 点结构，各阶段样本仍小，且三档比较带来选择偏差，不能代表一般期权策略 edge。",
            "- `MSS` 是严格因果的五根 K 结构突破代理，不声称等于某位交易员的主观画法。",
            "- FVG 形成后必须等待下一根或更晚 K 线回踩；不允许同根 K 线事后假设理想成交。",
            "- 多个参数和阶段属于探索性比较；任何 bootstrap 区间跨零的结果都不能称为 edge。",
            "- 报告使用 quote-derived OHLC，不等同 CME trade/MBO；微观成交顺序仍有限制。",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path, default=Path("/srv/data/spx-spark/data"))
    parser.add_argument(
        "--output-json",
        type=Path,
        default=Path("docs/research/ict-liquidity-event-study-2026-08-31.json"),
    )
    parser.add_argument(
        "--output-markdown",
        type=Path,
        default=Path("docs/research/ict-liquidity-event-study-2026-08-31.md"),
    )
    args = parser.parse_args()

    bars = _load_es_minute_bars(args.data_root)
    sessions = _session_bars(bars)
    artifact = _analysis(args.data_root, bars, sessions)
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_markdown.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(
        json.dumps(_json_safe(artifact), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    args.output_markdown.write_text(_markdown(artifact), encoding="utf-8")
    print(
        json.dumps(
            {
                "coverage": artifact["coverage"],
                "stages": {
                    stage: artifact["stage_summary"][stage]["all"]
                    for stage in STAGES
                },
                "output_json": str(args.output_json),
                "output_markdown": str(args.output_markdown),
            },
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )


if __name__ == "__main__":
    main()
