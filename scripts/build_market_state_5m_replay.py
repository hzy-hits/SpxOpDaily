#!/usr/bin/env python3
"""Replay the fail-closed RTH five-minute market state from the quote lake.

The replay has two deliberately separate clocks:

* state inputs use only quotes received at or before each replay timestamp;
* 15/30/60-minute ES paths are attached afterwards as evaluation labels.

No production state is read or written, and no production scoring threshold is
overridden here.
"""

from __future__ import annotations

import argparse
import copy
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb
import numpy as np

from spx_spark.application.market_features.es_bar_state import (
    MAX_EDGE_GAP_SECONDS,
    MAX_OK_GAP_SECONDS,
    MIN_OK_SAMPLES,
)
from spx_spark.application.market_features.market_state_5m import (
    RULE_VERSION,
    SCHEMA_VERSION as MODEL_SCHEMA_VERSION,
    TREND_DOWN,
    TREND_UP,
    score_market_state_5m,
)
from spx_spark.application.market_features.market_state_5m_inputs import (
    SECTOR_INSTRUMENTS,
    TARGET_RANGE_BASELINE_SESSIONS,
    build_market_state_5m_inputs,
    update_same_time_range_baselines,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR


ET = ZoneInfo("America/New_York")
UTC = timezone.utc
REPORT_SCHEMA_VERSION = "market_state_5m_replay.v1"
DEFAULT_LAKE_ROOT = Path("/srv/data/spx-spark/data/lake/quotes/schema=v1")
ES_INSTRUMENT = "future:ES"
TRACKED_INSTRUMENTS = (ES_INSTRUMENT, *SECTOR_INSTRUMENTS)
REPLAY_PROVIDERS = ("ibkr", "schwab")
FORWARD_HORIZONS = (15, 30, 60)
QUOTE_MAX_AGE_SECONDS = 90.0
ES_SAMPLE_SECONDS = 5
MARKET_SAMPLE_SECONDS = 60
RTH_OPEN = time(9, 30)
RTH_CLOSE = time(16, 0)
ES_REPLAY_START = time(8, 0)


@dataclass(frozen=True, slots=True)
class QuotePoint:
    """One normalized live quote, with receipt time as its availability clock."""

    provider: str
    instrument_id: str
    source_at: datetime
    received_at: datetime
    price: float
    volume: float | None

    @property
    def trading_date_et(self) -> date:
        return self.source_at.astimezone(ET).date()


@dataclass(frozen=True, slots=True)
class ReplayMaterial:
    bars_by_day: dict[date, list[dict[str, object]]]
    market_samples_by_day: dict[date, list[dict[str, object]]]
    es_five_second_sample_count: dict[date, int]


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("quote timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _local_datetime(day: date, clock: time) -> datetime:
    return datetime.combine(day, clock, tzinfo=ET)


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid ISO date: {value}") from exc


def _parse_at(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return _as_utc(value)
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _as_utc(parsed) if parsed.tzinfo is not None else None


def _partition_date(path: Path) -> date | None:
    for part in path.parts:
        if not part.startswith("date="):
            continue
        try:
            return date.fromisoformat(part.removeprefix("date="))
        except ValueError:
            return None
    return None


def _partition_value(path: Path, key: str) -> str | None:
    prefix = f"{key}="
    return next(
        (part.removeprefix(prefix) for part in path.parts if part.startswith(prefix)),
        None,
    )


def _replay_utc_hours(partition_day: date) -> set[int]:
    start = _local_datetime(partition_day, ES_REPLAY_START).astimezone(UTC)
    end = _local_datetime(partition_day, RTH_CLOSE).astimezone(UTC)
    return set(range(start.hour, end.hour))


def resolve_schema_root(path: Path) -> Path:
    """Accept the schema directory, the quote directory, or the data root."""

    candidates = (
        path,
        path / "schema=v1",
        path / "lake/quotes/schema=v1",
    )
    for candidate in candidates:
        if candidate.is_dir() and candidate.name == "schema=v1":
            return candidate
    return path


def discover_quote_files(
    data_root: Path,
    *,
    earliest_partition: date | None = None,
    latest_partition: date | None = None,
) -> list[Path]:
    """Find both legacy direct partitions and current hourly partitions."""

    root = resolve_schema_root(data_root)
    paths = {
        *root.glob("date=*/provider=*/quotes.parquet"),
        *root.glob("date=*/provider=*/hour=*/quotes.parquet"),
    }
    selected: list[Path] = []
    for path in paths:
        partition_day = _partition_date(path)
        if partition_day is None:
            continue
        if _partition_value(path, "provider") not in REPLAY_PROVIDERS:
            continue
        hour_token = _partition_value(path, "hour")
        if (
            hour_token is not None
            and hour_token.isdigit()
            and int(hour_token) not in _replay_utc_hours(partition_day)
        ):
            continue
        if earliest_partition is not None and partition_day < earliest_partition:
            continue
        if latest_partition is not None and partition_day > latest_partition:
            continue
        selected.append(path)
    return sorted(selected)


def load_live_quote_points(files: list[Path]) -> list[QuotePoint]:
    """Load only live ES/sector rows and retain the receipt-time causal clock."""

    if not files:
        return []
    placeholders = ", ".join("?" for _ in TRACKED_INSTRUMENTS)
    query = f"""
        SELECT
            provider,
            instrument_id,
            source_at,
            received_at,
            coalesce(
                nullif(effective_price, 0),
                nullif(mark, 0),
                nullif(mid, 0),
                nullif(last, 0),
                nullif(close, 0),
                CASE
                    WHEN bid > 0 AND ask >= bid THEN (bid + ask) / 2
                END
            ) AS price,
            volume
        FROM read_parquet(?, hive_partitioning=true, union_by_name=true)
        WHERE quality = 'live'
          AND instrument_id IN ({placeholders})
          AND provider IS NOT NULL
          AND source_at IS NOT NULL
          AND received_at IS NOT NULL
          AND source_at <= received_at + INTERVAL '5 seconds'
          AND coalesce(
                nullif(effective_price, 0),
                nullif(mark, 0),
                nullif(mid, 0),
                nullif(last, 0),
                nullif(close, 0),
                CASE
                    WHEN bid > 0 AND ask >= bid THEN (bid + ask) / 2
                END
              ) > 0
        ORDER BY received_at, source_at, provider, instrument_id
    """
    connection = duckdb.connect()
    try:
        rows = connection.execute(
            query,
            [[str(path) for path in files], *TRACKED_INSTRUMENTS],
        ).fetchall()
    finally:
        connection.close()

    points: list[QuotePoint] = []
    seen: set[tuple[object, ...]] = set()
    for provider, instrument_id, source_at, received_at, price, volume in rows:
        source = _as_utc(source_at)
        received = _as_utc(received_at)
        local = source.astimezone(ET)
        if local.weekday() >= 5:
            continue
        if instrument_id == ES_INSTRUMENT:
            if not ES_REPLAY_START <= local.time().replace(tzinfo=None) < RTH_CLOSE:
                continue
        elif not RTH_OPEN <= local.time().replace(tzinfo=None) < RTH_CLOSE:
            continue
        numeric_volume = (
            float(volume)
            if isinstance(volume, int | float)
            and not isinstance(volume, bool)
            and float(volume) >= 0
            else None
        )
        key = (
            str(provider),
            str(instrument_id),
            source,
            received,
            float(price),
            numeric_volume,
        )
        if key in seen:
            continue
        seen.add(key)
        points.append(
            QuotePoint(
                provider=str(provider),
                instrument_id=str(instrument_id),
                source_at=source,
                received_at=received,
                price=float(price),
                volume=numeric_volume,
            )
        )
    return points


def _provider_tie_rank(provider: str) -> int:
    # Mirrors freshest_quote(): IBKR wins a source-timestamp tie, then Schwab.
    return 2 if provider == "ibkr" else 1 if provider == "schwab" else 0


def _update_latest(
    latest: dict[tuple[str, str], QuotePoint],
    point: QuotePoint,
) -> None:
    key = (point.instrument_id, point.provider)
    previous = latest.get(key)
    if previous is None or (point.source_at, point.received_at) > (
        previous.source_at,
        previous.received_at,
    ):
        latest[key] = point


def _eligible(point: QuotePoint, tick: datetime) -> bool:
    if point.received_at > tick or point.source_at > tick:
        return False
    return max(
        (tick - point.source_at).total_seconds(),
        (tick - point.received_at).total_seconds(),
    ) <= QUOTE_MAX_AGE_SECONDS


def _freshest(
    latest: dict[tuple[str, str], QuotePoint],
    *,
    instrument_id: str,
    tick: datetime,
    provider: str | None = None,
) -> QuotePoint | None:
    candidates = [
        point
        for (candidate_instrument, candidate_provider), point in latest.items()
        if candidate_instrument == instrument_id
        and (provider is None or candidate_provider == provider)
        and _eligible(point, tick)
    ]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda point: (point.source_at, _provider_tie_rank(point.provider)),
    )


def _tick_range(
    day: date,
    *,
    start: time,
    end: time,
    seconds: int,
) -> list[datetime]:
    first = _local_datetime(day, start) + timedelta(seconds=seconds)
    final = _local_datetime(day, end)
    count = int((final - first).total_seconds() // seconds) + 1
    return [(first + timedelta(seconds=index * seconds)).astimezone(UTC) for index in range(count)]


def _five_minute_start(at: datetime) -> datetime:
    stamp = int(_as_utc(at).timestamp())
    return datetime.fromtimestamp(stamp - stamp % 300, tz=UTC)


def _es_five_second_samples(
    points: list[QuotePoint],
    *,
    trading_day: date,
) -> list[tuple[datetime, QuotePoint]]:
    session = DEFAULT_MARKET_CALENDAR.session(trading_day)
    if session is None:
        return []
    events = sorted(
        (point for point in points if point.instrument_id == ES_INSTRUMENT),
        key=lambda point: (point.received_at, point.source_at, point.provider),
    )
    latest: dict[tuple[str, str], QuotePoint] = {}
    samples: list[tuple[datetime, QuotePoint]] = []
    cursor = 0
    last_source_at: datetime | None = None
    for tick in _tick_range(
        trading_day,
        start=ES_REPLAY_START,
        end=session.close_at.time().replace(tzinfo=None),
        seconds=ES_SAMPLE_SECONDS,
    ):
        while cursor < len(events) and events[cursor].received_at <= tick:
            _update_latest(latest, events[cursor])
            cursor += 1
        point = _freshest(latest, instrument_id=ES_INSTRUMENT, tick=tick)
        if point is None or (last_source_at is not None and point.source_at <= last_source_at):
            continue
        last_source_at = point.source_at
        bar_end = _five_minute_start(point.source_at) + timedelta(minutes=5)
        if tick <= bar_end:
            samples.append((tick, point))
    return samples


def _market_samples(
    points: list[QuotePoint],
    *,
    trading_day: date,
) -> list[dict[str, object]]:
    session = DEFAULT_MARKET_CALENDAR.session(trading_day)
    if session is None:
        return []
    events = sorted(
        points,
        key=lambda point: (point.received_at, point.source_at, point.provider),
    )
    providers = sorted({point.provider for point in events})
    latest: dict[tuple[str, str], QuotePoint] = {}
    rows: list[dict[str, object]] = []
    cursor = 0
    for tick in _tick_range(
        trading_day,
        start=RTH_OPEN,
        end=session.close_at.time().replace(tzinfo=None),
        seconds=MARKET_SAMPLE_SECONDS,
    ):
        while cursor < len(events) and events[cursor].received_at <= tick:
            _update_latest(latest, events[cursor])
            cursor += 1
        instruments: dict[str, dict[str, object]] = {}
        for instrument_id in TRACKED_INSTRUMENTS:
            point = _freshest(latest, instrument_id=instrument_id, tick=tick)
            if point is not None:
                instruments[instrument_id] = _normalized_sample_quote(point)
        es_by_provider: dict[str, dict[str, object]] = {}
        for provider in providers:
            point = _freshest(
                latest,
                instrument_id=ES_INSTRUMENT,
                tick=tick,
                provider=provider,
            )
            if point is not None:
                es_by_provider[provider] = _normalized_sample_quote(point)
        rows.append(
            {
                "at": tick.isoformat(),
                "session_id": trading_day.isoformat(),
                "segment": "rth",
                "instruments": instruments,
                "es_by_provider": es_by_provider,
            }
        )
    return rows


def _normalized_sample_quote(point: QuotePoint) -> dict[str, object]:
    return {
        "price": point.price,
        "volume": point.volume,
        "provider": point.provider,
        "source_at": point.source_at.isoformat(),
        "transport_at": point.received_at.isoformat(),
        "quality": "live",
    }


def _bars_from_samples(
    samples: list[tuple[datetime, QuotePoint]],
) -> list[dict[str, object]]:
    buckets: dict[datetime, list[tuple[datetime, QuotePoint]]] = defaultdict(list)
    for sampled_at, point in samples:
        start = _five_minute_start(point.source_at)
        if sampled_at <= start + timedelta(minutes=5):
            buckets[start].append((sampled_at, point))

    provisional: list[dict[str, object]] = []
    for start, rows in sorted(buckets.items()):
        ordered = sorted(rows, key=lambda row: (row[1].source_at, row[0]))
        prices = [row[1].price for row in ordered]
        source_times = [row[1].source_at for row in ordered]
        gaps = [
            (current - previous).total_seconds()
            for previous, current in zip(source_times, source_times[1:], strict=False)
        ]
        max_gap = max(gaps, default=0.0)
        leading_gap = (source_times[0] - start).total_seconds()
        trailing_gap = (
            start + timedelta(minutes=5) - source_times[-1]
        ).total_seconds()
        providers = Counter(row[1].provider for row in ordered)
        provider = max(providers, key=lambda name: (providers[name], name))
        local = start.astimezone(ET)
        segment = (
            "rth"
            if DEFAULT_MARKET_CALENDAR.is_rth_open(start)
            else "us_premarket"
        )
        quality = (
            "ok"
            if len(ordered) >= MIN_OK_SAMPLES
            and max_gap <= MAX_OK_GAP_SECONDS
            and 0 <= leading_gap <= MAX_EDGE_GAP_SECONDS
            and 0 <= trailing_gap <= MAX_EDGE_GAP_SECONDS
            else "partial"
        )
        provisional.append(
            {
                "bar_start": start.isoformat(),
                "bar_end": (start + timedelta(minutes=5)).isoformat(),
                "interval_seconds": 300,
                "open": prices[0],
                "high": max(prices),
                "low": min(prices),
                "close": prices[-1],
                "sample_count": len(ordered),
                "first_source_at": source_times[0].isoformat(),
                "last_source_at": source_times[-1].isoformat(),
                "max_sample_gap_seconds": max_gap,
                "leading_edge_gap_seconds": leading_gap,
                "trailing_edge_gap_seconds": trailing_gap,
                "provider_counts": dict(sorted(providers.items())),
                "provider": provider,
                "segment": segment,
                "trading_date_et": local.date().isoformat(),
                "gap_before": False,
                "quality": quality,
            }
        )

    previous: dict[str, object] | None = None
    result: list[dict[str, object]] = []
    for bar in provisional:
        start = _parse_at(bar["bar_start"])
        previous_start = _parse_at(previous["bar_start"]) if previous else None
        bar["gap_before"] = bool(
            previous is not None
            and (
                start is None
                or previous_start is None
                or start != previous_start + timedelta(minutes=5)
                or previous.get("quality") != "ok"
            )
        )
        result.append(bar)
        previous = bar
    return result


def build_replay_material(points: list[QuotePoint]) -> ReplayMaterial:
    """Build production-shaped bars and minute samples without state writes."""

    by_day: dict[date, list[QuotePoint]] = defaultdict(list)
    for point in points:
        if DEFAULT_MARKET_CALENDAR.is_trading_day(point.trading_date_et):
            by_day[point.trading_date_et].append(point)

    bars_by_day: dict[date, list[dict[str, object]]] = {}
    market_samples_by_day: dict[date, list[dict[str, object]]] = {}
    sample_counts: dict[date, int] = {}
    for trading_day, day_points in sorted(by_day.items()):
        es_samples = _es_five_second_samples(day_points, trading_day=trading_day)
        bars_by_day[trading_day] = _bars_from_samples(es_samples)
        market_samples_by_day[trading_day] = _market_samples(
            day_points,
            trading_day=trading_day,
        )
        sample_counts[trading_day] = len(es_samples)
    return ReplayMaterial(
        bars_by_day=bars_by_day,
        market_samples_by_day=market_samples_by_day,
        es_five_second_sample_count=sample_counts,
    )


def _empty_baseline(now: datetime) -> dict[str, object]:
    return update_same_time_range_baselines(None, bars=[], now=now)


def build_prior_session_baselines(
    bars_by_day: dict[date, list[dict[str, object]]],
    *,
    session_days: list[date] | None = None,
    through_date: date | None = None,
) -> dict[date, dict[str, object]]:
    """Snapshot the baseline before adding each current session.

    This ordering is the leakage guard: every row visible to session D has a
    ``trading_date_et`` strictly earlier than D.
    """

    snapshots, _ = _advance_range_baselines(
        bars_by_day,
        session_days=session_days,
        through_date=through_date,
    )
    return snapshots


def build_completed_baseline_state(
    bars_by_day: dict[date, list[dict[str, object]]],
    *,
    through_date: date,
    session_days: list[date] | None = None,
) -> dict[str, object]:
    """Return production-compatible baseline state completed through one date."""

    _, state = _advance_range_baselines(
        bars_by_day,
        session_days=session_days,
        through_date=through_date,
    )
    return state


def _advance_range_baselines(
    bars_by_day: dict[date, list[dict[str, object]]],
    *,
    session_days: list[date] | None,
    through_date: date | None,
) -> tuple[dict[date, dict[str, object]], dict[str, object]]:
    days = sorted(
        day
        for day in set(session_days or []) | set(bars_by_day)
        if through_date is None or day <= through_date
    )
    if not days:
        fallback_day = through_date or date.today()
        return {}, _empty_baseline(
            _local_datetime(fallback_day, RTH_OPEN).astimezone(UTC)
        )
    state = _empty_baseline(_local_datetime(days[0], RTH_OPEN).astimezone(UTC))
    snapshots: dict[date, dict[str, object]] = {}
    for trading_day in days:
        snapshots[trading_day] = copy.deepcopy(state)
        day_bars = bars_by_day.get(trading_day, [])
        rth_bars = sorted(
            (bar for bar in day_bars if bar.get("segment") == "rth"),
            key=lambda bar: str(bar.get("bar_end") or ""),
        )
        prefix = [bar for bar in day_bars if bar.get("segment") != "rth"]
        for bar in rth_bars:
            prefix.append(bar)
            now = _parse_at(bar.get("bar_end"))
            if now is None:
                continue
            state = update_same_time_range_baselines(
                state,
                bars=prefix,
                now=now,
                max_sessions=TARGET_RANGE_BASELINE_SESSIONS,
            )
    return snapshots, state


def _weekdays(start: date, end: date) -> list[date]:
    count = (end - start).days + 1
    return [
        start + timedelta(days=offset)
        for offset in range(count)
        if DEFAULT_MARKET_CALENDAR.is_trading_day(start + timedelta(days=offset))
    ]


def _replay_times(trading_day: date) -> list[datetime]:
    session = DEFAULT_MARKET_CALENDAR.session(trading_day)
    if session is None:
        return []
    start = _local_datetime(trading_day, time(9, 45))
    end = session.close_at
    count = max(int((end - start).total_seconds() // 300), 0)
    return [(start + timedelta(minutes=5 * index)).astimezone(UTC) for index in range(count)]


def replay_market_states(
    material: ReplayMaterial,
    baselines: dict[date, dict[str, object]],
    *,
    start_date: date,
    end_date: date,
) -> list[dict[str, object]]:
    """Score every RTH five-minute boundary using only its causal prefix."""

    observations: list[dict[str, object]] = []
    for trading_day in _weekdays(start_date, end_date):
        bars = material.bars_by_day.get(trading_day, [])
        samples = material.market_samples_by_day.get(trading_day, [])
        baseline = baselines.get(
            trading_day,
            _empty_baseline(_local_datetime(trading_day, RTH_OPEN).astimezone(UTC)),
        )
        bars_by_end = {
            str(bar.get("bar_end")): bar
            for bar in bars
            if bar.get("segment") == "rth"
        }
        for now in _replay_times(trading_day):
            derived = build_market_state_5m_inputs(
                bars=bars,
                market_samples=samples,
                range_baselines=baseline,
                now=now,
            )
            values = dict(derived["values"])
            scored = score_market_state_5m(now=now, **values)
            current_bar = bars_by_end.get(now.isoformat())
            diagnostics = derived["diagnostics"]
            observations.append(
                {
                    "trading_date": trading_day.isoformat(),
                    "weekday": trading_day.strftime("%A"),
                    "as_of": now.isoformat(),
                    "as_of_et": now.astimezone(ET).isoformat(),
                    "es_close": current_bar.get("close") if current_bar else None,
                    "es_bar_quality": current_bar.get("quality") if current_bar else "missing",
                    "inputs": values,
                    "input_status": derived["status"],
                    "input_available_count": derived["available_count"],
                    "input_missing": derived["missing"],
                    "state": scored["state"],
                    "D": scored["D"],
                    "Q": scored["Q"],
                    "V": scored["V"],
                    "state_status": scored["status"],
                    "state_reasons": scored["reasons"],
                    "pin_proxy_candidate": scored["pin_proxy_candidate"],
                    "lineage": {
                        "vwap": diagnostics["vwap"],
                        "atr": diagnostics["atr"],
                        "same_time_range": diagnostics["same_time_range"],
                        "breadth": diagnostics["breadth"],
                    },
                }
            )
    return observations


def attach_forward_es_paths(
    observations: list[dict[str, object]],
    bars_by_day: dict[date, list[dict[str, object]]],
) -> list[dict[str, object]]:
    """Attach future labels after scoring; these values never enter model inputs."""

    result: list[dict[str, object]] = []
    bars_by_day_end: dict[date, dict[datetime, dict[str, object]]] = {}
    for trading_day, bars in bars_by_day.items():
        bars_by_day_end[trading_day] = {
            end: bar
            for bar in bars
            if bar.get("segment") == "rth"
            and (end := _parse_at(bar.get("bar_end"))) is not None
        }

    for observation in observations:
        row = dict(observation)
        trading_day = date.fromisoformat(str(row["trading_date"]))
        origin_at = _parse_at(row.get("as_of"))
        by_end = bars_by_day_end.get(trading_day, {})
        origin = by_end.get(origin_at) if origin_at is not None else None
        paths: dict[str, dict[str, object]] = {}
        for horizon in FORWARD_HORIZONS:
            label = f"{horizon}m"
            if (
                origin_at is None
                or origin is None
                or origin.get("quality") != "ok"
            ):
                paths[label] = {
                    "status": "unavailable",
                    "reason": "origin_es_bar_missing_or_not_ok",
                    "evaluation_only": True,
                }
                continue
            target_at = origin_at + timedelta(minutes=horizon)
            target = by_end.get(target_at)
            if target is None or target.get("quality") != "ok":
                paths[label] = {
                    "status": "unavailable",
                    "reason": "target_es_bar_missing_or_not_ok",
                    "evaluation_only": True,
                }
                continue
            path = [
                bar
                for end, bar in sorted(by_end.items())
                if origin_at < end <= target_at and bar.get("quality") == "ok"
            ]
            origin_close = float(origin["close"])
            expected = horizon // 5
            paths[label] = {
                "status": "ready" if len(path) == expected else "partial_path",
                "endpoint_points": float(target["close"]) - origin_close,
                "path_high_points": max(float(bar["high"]) for bar in path) - origin_close,
                "path_low_points": min(float(bar["low"]) for bar in path) - origin_close,
                "observed_bar_count": len(path),
                "expected_bar_count": expected,
                "evaluation_only": True,
            }
        row["forward_es"] = paths
        result.append(row)
    return result


def summarize_forward_es_paths(
    observations: list[dict[str, object]],
) -> list[dict[str, object]]:
    grouped: dict[tuple[str, int], list[dict[str, object]]] = defaultdict(list)
    for row in observations:
        state = str(row["state"])
        paths = row.get("forward_es")
        if not isinstance(paths, dict):
            continue
        for horizon in FORWARD_HORIZONS:
            path = paths.get(f"{horizon}m")
            if isinstance(path, dict) and path.get("status") in {"ready", "partial_path"}:
                grouped[(state, horizon)].append(path)

    summaries: list[dict[str, object]] = []
    for (state, horizon), paths in sorted(grouped.items()):
        endpoints = np.asarray([float(path["endpoint_points"]) for path in paths])
        highs = np.asarray([float(path["path_high_points"]) for path in paths])
        lows = np.asarray([float(path["path_low_points"]) for path in paths])
        signed = (
            endpoints
            if state == TREND_UP
            else -endpoints
            if state == TREND_DOWN
            else None
        )
        summaries.append(
            {
                "state": state,
                "horizon_minutes": horizon,
                "sample_count": int(endpoints.size),
                "mean_endpoint_points": float(np.mean(endpoints)),
                "median_endpoint_points": float(np.median(endpoints)),
                "p25_endpoint_points": float(np.quantile(endpoints, 0.25)),
                "p75_endpoint_points": float(np.quantile(endpoints, 0.75)),
                "positive_ratio": float(np.mean(endpoints > 0)),
                "negative_ratio": float(np.mean(endpoints < 0)),
                "mean_path_high_points": float(np.mean(highs)),
                "mean_path_low_points": float(np.mean(lows)),
                "directional_hit_ratio": (
                    float(np.mean(signed > 0)) if signed is not None else None
                ),
                "evaluation_only": True,
            }
        )
    return summaries


def _daily_coverage(
    points: list[QuotePoint],
    material: ReplayMaterial,
    observations: list[dict[str, object]],
    *,
    start_date: date,
    end_date: date,
) -> list[dict[str, object]]:
    points_by_day: dict[date, list[QuotePoint]] = defaultdict(list)
    for point in points:
        points_by_day[point.trading_date_et].append(point)
    observations_by_day: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in observations:
        observations_by_day[str(row["trading_date"])].append(row)

    coverage: list[dict[str, object]] = []
    for trading_day in _weekdays(start_date, end_date):
        session = DEFAULT_MARKET_CALENDAR.session(trading_day)
        expected_rth_bars = (
            session.expected_five_minute_buckets if session is not None else 0
        )
        expected_replay_slots = max(expected_rth_bars - 3, 0)
        day_points = points_by_day.get(trading_day, [])
        es_points = [point for point in day_points if point.instrument_id == ES_INSTRUMENT]
        sector_points = [
            point for point in day_points if point.instrument_id in SECTOR_INSTRUMENTS
        ]
        sectors_present = sorted({point.instrument_id for point in sector_points})
        bars = [
            bar
            for bar in material.bars_by_day.get(trading_day, [])
            if bar.get("segment") == "rth"
        ]
        samples = material.market_samples_by_day.get(trading_day, [])
        day_observations = observations_by_day.get(trading_day.isoformat(), [])
        sector_ready_samples = sum(
            isinstance(row.get("instruments"), dict)
            and sum(
                instrument_id in row["instruments"]
                for instrument_id in SECTOR_INSTRUMENTS
            )
            >= 8
            for row in samples
        )
        complete_inputs = sum(row.get("input_status") == "ready" for row in day_observations)
        coverage.append(
            {
                "trading_date": trading_day.isoformat(),
                "weekday": trading_day.strftime("%A"),
                "es_live_quote_rows": len(es_points),
                "es_live_providers": sorted({point.provider for point in es_points}),
                "es_first_source_at": (
                    min(point.source_at for point in es_points).isoformat()
                    if es_points
                    else None
                ),
                "es_last_source_at": (
                    max(point.source_at for point in es_points).isoformat()
                    if es_points
                    else None
                ),
                "es_five_second_samples": material.es_five_second_sample_count.get(
                    trading_day,
                    0,
                ),
                "es_rth_bars": len(bars),
                "es_rth_ok_bars": sum(bar.get("quality") == "ok" for bar in bars),
                "es_rth_expected_bars": expected_rth_bars,
                "es_rth_ok_bar_coverage_ratio": (
                    sum(bar.get("quality") == "ok" for bar in bars)
                    / expected_rth_bars
                    if expected_rth_bars
                    else 0.0
                ),
                "sector_live_quote_rows": len(sector_points),
                "sectors_present": sectors_present,
                "sector_instruments_present_count": len(sectors_present),
                "sector_minimum_required_count": 8,
                "minute_samples_with_at_least_8_sectors": sector_ready_samples,
                "minute_sample_count": len(samples),
                "replay_slot_count": len(day_observations),
                "replay_expected_slot_count": expected_replay_slots,
                "complete_input_slot_count": complete_inputs,
                "complete_input_slot_ratio": (
                    complete_inputs / len(day_observations) if day_observations else 0.0
                ),
                "state_counts": dict(
                    sorted(Counter(str(row["state"]) for row in day_observations).items())
                ),
            }
        )
    return coverage


def _round_for_report(value: object) -> object:
    if isinstance(value, float):
        if not np.isfinite(value):
            return None
        return round(value, 2)
    if isinstance(value, dict):
        return {str(key): _round_for_report(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_round_for_report(item) for item in value]
    if isinstance(value, tuple):
        return [_round_for_report(item) for item in value]
    return value


def build_replay_outputs(
    points: list[QuotePoint],
    *,
    start_date: date,
    end_date: date,
    source: dict[str, object] | None = None,
) -> tuple[dict[str, object], dict[str, object]]:
    if end_date < start_date:
        raise ValueError("end_date must not precede start_date")
    session_days = _weekdays(
        min(
            [point.trading_date_et for point in points] or [start_date],
        ),
        end_date,
    )
    material = build_replay_material(points)
    baselines, completed_baseline = _advance_range_baselines(
        material.bars_by_day,
        session_days=session_days,
        through_date=end_date,
    )
    causal_observations = replay_market_states(
        material,
        baselines,
        start_date=start_date,
        end_date=end_date,
    )
    observations = attach_forward_es_paths(
        causal_observations,
        material.bars_by_day,
    )
    state_counts = dict(
        sorted(Counter(str(row["state"]) for row in observations).items())
    )
    report = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "generated_at": datetime.now(tz=UTC).isoformat(),
        "model": {
            "schema_version": MODEL_SCHEMA_VERSION,
            "rule_version": RULE_VERSION,
            "production_thresholds_overridden": False,
            "action_authority": "none",
        },
        "window": {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "timezone": "America/New_York",
            "session": "RTH",
        },
        "source": source or {},
        "methodology": {
            "es_bar_sampling_seconds": ES_SAMPLE_SECONDS,
            "market_sample_seconds": MARKET_SAMPLE_SECONDS,
            "quote_max_age_seconds": QUOTE_MAX_AGE_SECONDS,
            "bar_quality": (
                "production thresholds: at least 20 samples, internal/leading/trailing "
                "gaps at most 30 seconds; missing buckets are not synthesized"
            ),
            "replay_boundaries": (
                "exchange-calendar RTH boundaries, 09:45 inclusive to close exclusive"
            ),
            "same_time_range_baseline": (
                "median of up to 20 strictly prior sessions at the same RTH bar end; "
                "production input remains unavailable below 10 sessions"
            ),
            "state_input_clock": (
                "received_at <= replay_as_of and source_at <= replay_as_of; "
                "bar VWAP requires source_at < bar_end; no interpolation"
            ),
            "forward_label_clock": (
                "future ES bars are attached only after state scoring and never enter inputs"
            ),
            "production_state_written": False,
            "numeric_display_decimals": 2,
        },
        "state_counts": state_counts,
        "daily_coverage": _daily_coverage(
            points,
            material,
            observations,
            start_date=start_date,
            end_date=end_date,
        ),
        "forward_es_path_summary": summarize_forward_es_paths(observations),
        "observations": observations,
    }
    return _round_for_report(report), completed_baseline


def build_replay_report(
    points: list[QuotePoint],
    *,
    start_date: date,
    end_date: date,
    source: dict[str, object] | None = None,
) -> dict[str, object]:
    report, _ = build_replay_outputs(
        points,
        start_date=start_date,
        end_date=end_date,
        source=source,
    )
    return report


def write_report(report: dict[str, object], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-root",
        type=Path,
        default=DEFAULT_LAKE_ROOT,
        help="Quote lake schema=v1 directory (or its quote/data parent).",
    )
    parser.add_argument("--start-date", type=_parse_date)
    parser.add_argument("--end-date", type=_parse_date)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--baseline-output",
        type=Path,
        help=(
            "Optional production-compatible market_state_5m_range_baselines.v1 "
            "state completed through --end-date."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    earliest_partition = (
        args.start_date - timedelta(days=60) if args.start_date is not None else None
    )
    files = discover_quote_files(
        args.data_root,
        earliest_partition=earliest_partition,
        latest_partition=args.end_date,
    )
    if not files:
        raise SystemExit(f"no quotes.parquet files found below {args.data_root}")
    points = load_live_quote_points(files)
    es_days = sorted(
        {
            point.trading_date_et
            for point in points
            if point.instrument_id == ES_INSTRUMENT
        }
    )
    if not es_days:
        raise SystemExit("no live future:ES rows found in selected quote partitions")
    start_date = args.start_date or es_days[0]
    end_date = args.end_date or es_days[-1]
    report, completed_baseline = build_replay_outputs(
        points,
        start_date=start_date,
        end_date=end_date,
        source={
            "data_root": str(resolve_schema_root(args.data_root)),
            "parquet_file_count": len(files),
            "live_quote_point_count": len(points),
            "loaded_history_start_date": min(
                point.trading_date_et for point in points
            ).isoformat(),
            "loaded_history_end_date": max(
                point.trading_date_et for point in points
            ).isoformat(),
        },
    )
    write_report(report, args.output)
    if args.baseline_output is not None:
        write_report(completed_baseline, args.baseline_output)
    print(
        f"wrote {args.output} "
        f"({len(report['observations'])} replay slots, "
        f"{len(report['daily_coverage'])} RTH days)"
    )
    if args.baseline_output is not None:
        print(f"wrote {args.baseline_output} (completed prior-session range baseline)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
