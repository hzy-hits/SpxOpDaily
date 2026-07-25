"""Derive the eight strict RTH five-minute market-state inputs.

No price, NBBO, bar, breadth, or historical baseline value is interpolated.
Every missing prerequisite remains explicit so the scoring kernel can fail
closed rather than silently treating missing data as neutral.
"""

from __future__ import annotations

import statistics
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timedelta
from typing import Any

from spx_spark.application.market_features.market_state_5m import (
    MarketStructure,
    OpeningRangeState,
    PriceVsVwap,
)
from spx_spark.config import NY_TZ
from spx_spark.marketdata import as_utc


SCHEMA_VERSION = "market_state_5m_inputs.v1"
SECTOR_INSTRUMENTS = (
    "equity:XLB",
    "equity:XLC",
    "equity:XLE",
    "equity:XLF",
    "equity:XLI",
    "equity:XLK",
    "equity:XLP",
    "equity:XLRE",
    "equity:XLU",
    "equity:XLV",
    "equity:XLY",
)
MIN_BREADTH_INSTRUMENTS = 8
MIN_RANGE_BASELINE_SESSIONS = 10
TARGET_RANGE_BASELINE_SESSIONS = 20
VWAP_START_TOLERANCE_MINUTES = 5
# Minute snapshots can span two nominal buckets plus a few seconds of common
# source-timestamp jitter.  Keep the window tight enough to reject a third
# missing bucket while avoiding a false all-sector outage at 127-128 seconds.
VWAP_MAX_GAP_SECONDS = 135.0
VWAP_MIN_OBSERVED_VOLUME_RATIO = 0.80
BREADTH_MAX_CROSS_SECTION_SKEW_SECONDS = 45.0


def build_market_state_5m_inputs(
    *,
    bars: Sequence[Mapping[str, object]],
    market_samples: Sequence[Mapping[str, object]],
    range_baselines: Mapping[str, object] | None,
    now: datetime,
) -> dict[str, object]:
    """Return scorer-ready values plus auditable lineage diagnostics."""

    at = as_utc(now)
    trading_date = at.astimezone(NY_TZ).date()
    closed = _closed_bars(bars, now=at)
    rth_bars = [
        bar
        for bar in closed
        if bar.get("segment") == "rth"
        and bar.get("trading_date_et") == trading_date.isoformat()
    ]
    atr, atr_diagnostics = _atr_5m(closed)
    vwap_series, vwap_diagnostics = _es_vwap_series(
        market_samples,
        trading_date=trading_date,
        now=at,
    )
    bar_vwaps = _bar_vwaps(rth_bars, vwap_series)

    price_state = _price_vs_vwap(rth_bars, bar_vwaps, atr)
    slope = _vwap_slope(rth_bars, bar_vwaps, atr)
    opening_state, opening_diagnostics = _opening_range_state(rth_bars)
    structure = _market_structure(rth_bars, atr)
    efficiency = _efficiency_ratio(rth_bars)
    crosses = _vwap_cross_count(rth_bars, bar_vwaps)
    range_ratio, range_diagnostics = _same_time_range_ratio(
        rth_bars,
        range_baselines or {},
        trading_date=trading_date,
    )
    breadth, breadth_diagnostics = _breadth_above_vwap(
        market_samples,
        trading_date=trading_date,
        now=at,
    )

    values = {
        "price_vs_vwap": price_state,
        "vwap_slope": slope,
        "opening_range_state": opening_state,
        "market_structure": structure,
        "efficiency_ratio": efficiency,
        "vwap_cross_count": crosses,
        "same_time_range_ratio": range_ratio,
        "breadth_above_vwap": breadth,
    }
    missing = [key for key, value in values.items() if value is None]
    return {
        "schema_version": SCHEMA_VERSION,
        "as_of": at.isoformat(),
        "trading_date_et": trading_date.isoformat(),
        "values": values,
        "status": "ready" if not missing else "incomplete",
        "available_count": len(values) - len(missing),
        "required_count": len(values),
        "missing": missing,
        "diagnostics": {
            "bar_count_all": len(closed),
            "rth_bar_count": len(rth_bars),
            "rth_ok_bar_count": sum(bar.get("quality") == "ok" for bar in rth_bars),
            "atr": atr_diagnostics,
            "vwap": vwap_diagnostics,
            "opening_range": opening_diagnostics,
            "same_time_range": range_diagnostics,
            "breadth": breadth_diagnostics,
            "price_source": "provider_neutral_live_es_5s_sampled_ohlc",
            "nbbo_interpolated": False,
            "missing_values_filled": False,
        },
    }


def update_same_time_range_baselines(
    previous: Mapping[str, object] | None,
    *,
    bars: Sequence[Mapping[str, object]],
    now: datetime,
    max_sessions: int = TARGET_RANGE_BASELINE_SESSIONS,
) -> dict[str, object]:
    """Update one same-time RTH range observation without cross-day leakage."""

    at = as_utc(now)
    trading_date = at.astimezone(NY_TZ).date()
    state = _baseline_state(previous)
    rth_bars = [
        bar
        for bar in _closed_bars(bars, now=at)
        if bar.get("segment") == "rth"
        and bar.get("trading_date_et") == trading_date.isoformat()
    ]
    rth_bars = _continuous_rth_from_open(rth_bars)
    if not rth_bars:
        return state
    last_end = _parse_at(rth_bars[-1].get("bar_end"))
    if last_end is None:
        return state
    slot = last_end.astimezone(NY_TZ).strftime("%H:%M")
    current_range = max(float(bar["high"]) for bar in rth_bars) - min(
        float(bar["low"]) for bar in rth_bars
    )
    slots = {
        str(key): [dict(row) for row in value if isinstance(row, Mapping)]
        for key, value in _mapping(state.get("slots")).items()
        if isinstance(value, list)
    }
    rows = [
        row
        for row in slots.get(slot, [])
        if row.get("trading_date_et") != trading_date.isoformat()
    ]
    current_row = {
        "trading_date_et": trading_date.isoformat(),
        "range_points": round(current_range, 6),
        "bar_end": last_end.isoformat(),
        "source": "live_es_5m_ohlc",
    }
    existing_current = next(
        (
            row
            for row in slots.get(slot, [])
            if row.get("trading_date_et") == trading_date.isoformat()
        ),
        None,
    )
    if existing_current == current_row:
        return state
    rows.append(current_row)
    slots[slot] = rows[-(max_sessions + 1):]
    return {
        "schema_version": "market_state_5m_range_baselines.v1",
        "updated_at": at.isoformat(),
        "target_sessions": max_sessions,
        "slots": slots,
    }


def _closed_bars(
    bars: Sequence[Mapping[str, object]],
    *,
    now: datetime,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for item in bars:
        if not isinstance(item, Mapping):
            continue
        end = _parse_at(item.get("bar_end"))
        if (
            end is None
            or end > now
            or item.get("quality") not in {"ok", "partial"}
            or any(_number(item.get(key)) is None for key in ("open", "high", "low", "close"))
        ):
            continue
        rows.append(dict(item))
    return sorted(rows, key=lambda row: str(row.get("bar_start") or ""))


def _contiguous_ok_tail(
    bars: Sequence[Mapping[str, object]],
) -> list[Mapping[str, object]]:
    tail: list[Mapping[str, object]] = []
    previous_start: datetime | None = None
    for bar in sorted(bars, key=lambda row: str(row.get("bar_start") or "")):
        start = _parse_at(bar.get("bar_start"))
        if start is None or bar.get("quality") != "ok":
            tail = []
            previous_start = None
            continue
        continuous = (
            previous_start is not None
            and start == previous_start + timedelta(minutes=5)
            and bar.get("gap_before") is not True
        )
        tail = [*tail, bar] if continuous else [bar]
        previous_start = start
    return tail


def _continuous_rth_from_open(
    bars: Sequence[Mapping[str, object]],
) -> list[Mapping[str, object]]:
    ordered = sorted(bars, key=lambda row: str(row.get("bar_start") or ""))
    if not ordered or any(bar.get("quality") != "ok" for bar in ordered):
        return []
    starts = [_parse_at(bar.get("bar_start")) for bar in ordered]
    if any(start is None for start in starts):
        return []
    parsed = [start for start in starts if start is not None]
    first = parsed[0].astimezone(NY_TZ)
    if first.time().replace(tzinfo=None) != time(9, 30):
        return []
    if any(
        current != previous + timedelta(minutes=5)
        for previous, current in zip(parsed, parsed[1:])
    ):
        return []
    if any(bar.get("gap_before") is True for bar in ordered[1:]):
        return []
    return ordered


def _atr_5m(
    bars: Sequence[Mapping[str, object]],
) -> tuple[float | None, dict[str, object]]:
    usable = _contiguous_ok_tail(bars)
    true_ranges: list[float] = []
    for previous, current in zip(usable, usable[1:]):
        high = float(current["high"])
        low = float(current["low"])
        prior_close = float(previous["close"])
        true_ranges.append(max(high - low, abs(high - prior_close), abs(low - prior_close)))
    window = true_ranges[-14:]
    value = statistics.fmean(window) if len(window) >= 6 else None
    return value, {
        "value": value,
        "periods_used": len(window),
        "target_periods": 14,
        "minimum_periods": 6,
        "method": "simple_mean_true_range_no_gap_fill",
    }


def _es_vwap_series(
    samples: Sequence[Mapping[str, object]],
    *,
    trading_date: date,
    now: datetime,
) -> tuple[list[tuple[datetime, float]], dict[str, object]]:
    by_provider: dict[str, list[tuple[datetime, float, float]]] = {}
    for row in samples:
        if row.get("segment") != "rth":
            continue
        at = _parse_at(row.get("at"))
        if at is None or at > now or at.astimezone(NY_TZ).date() != trading_date:
            continue
        providers = row.get("es_by_provider")
        if isinstance(providers, Mapping):
            candidates = providers.items()
        else:
            quote = _instrument(row, "future:ES")
            candidates = ((str(quote.get("provider") or ""), quote),) if quote else ()
        for provider, raw_quote in candidates:
            quote = raw_quote if isinstance(raw_quote, Mapping) else {}
            source_at = _parse_at(quote.get("source_at"))
            price = _number(quote.get("price"))
            volume = _number(quote.get("volume"))
            if (
                source_at is None
                or source_at > now
                or source_at > at
                or source_at.astimezone(NY_TZ).date() != trading_date
                or price is None
                or volume is None
                or price <= 0
                or volume < 0
            ):
                continue
            by_provider.setdefault(str(provider), []).append((source_at, volume, price))

    candidates: list[tuple[str, list[tuple[datetime, float]], dict[str, object]]] = []
    for provider, points in by_provider.items():
        series, diagnostics = _provider_vwap_series(
            sorted(set(points)),
            trading_date=trading_date,
        )
        if series:
            candidates.append((provider, series, diagnostics))
    if not candidates:
        return [], {
            "status": "unavailable",
            "reason": "no_complete_rth_es_cumulative_volume_provider",
            "providers_seen": sorted(by_provider),
        }
    fresh_candidates = [
        item
        for item in candidates
        if 0.0 <= (now - item[1][-1][0]).total_seconds() <= VWAP_MAX_GAP_SECONDS
    ]
    if not fresh_candidates:
        return [], {
            "status": "unavailable",
            "reason": "no_fresh_rth_es_cumulative_volume_provider",
            "providers_seen": sorted(by_provider),
        }
    provider, series, diagnostics = max(
        fresh_candidates,
        key=lambda item: (
            len(item[1]),
            item[1][-1][0],
            item[0] == "schwab",
        ),
    )
    return series, {
        "status": "ready",
        "provider": provider,
        "point_count": len(series),
        **diagnostics,
    }


def _provider_vwap_series(
    points: Sequence[tuple[datetime, float, float]],
    *,
    trading_date: date,
) -> tuple[list[tuple[datetime, float]], dict[str, object]]:
    if len(points) < 2:
        return [], {"reason": "fewer_than_two_volume_points"}
    rth_open = datetime.combine(trading_date, time(9, 30), tzinfo=NY_TZ)
    first_at = points[0][0].astimezone(NY_TZ)
    if first_at > rth_open + timedelta(minutes=VWAP_START_TOLERANCE_MINUTES):
        return [], {"reason": "provider_joined_after_rth_open", "first_at": first_at.isoformat()}
    numerator = 0.0
    denominator = 0.0
    series: list[tuple[datetime, float]] = []
    gaps = 0
    resets = 0
    max_gap = 0.0
    skipped_cross_gap_volume = 0.0
    gap_recovery_pending = False
    for previous, current in zip(points, points[1:]):
        gap = (current[0] - previous[0]).total_seconds()
        max_gap = max(max_gap, gap)
        delta = current[1] - previous[1]
        sampling_gap = gap > VWAP_MAX_GAP_SECONDS
        if sampling_gap:
            gaps += 1
            gap_recovery_pending = True
        if delta < 0:
            resets += 1
        if sampling_gap:
            if delta > 0:
                skipped_cross_gap_volume += delta
            continue
        if delta < 0:
            continue
        if delta == 0:
            continue
        numerator += current[2] * delta
        denominator += delta
        observed_volume_ratio = denominator / (
            denominator + skipped_cross_gap_volume
        )
        if (
            gap_recovery_pending
            and observed_volume_ratio < VWAP_MIN_OBSERVED_VOLUME_RATIO
        ):
            continue
        gap_recovery_pending = False
        series.append((current[0], numerator / denominator))
    partial_observed_volume = bool(gaps or resets)
    total_known_volume = denominator + skipped_cross_gap_volume
    observed_volume_ratio = (
        denominator / total_known_volume if total_known_volume > 0 else None
    )
    diagnostics: dict[str, object] = {
        "max_gap_seconds": max_gap,
        "gap_count": gaps,
        "reset_count": resets,
        "observed_volume": denominator,
        "skipped_cross_gap_volume": skipped_cross_gap_volume,
        "observed_volume_ratio": observed_volume_ratio,
        "minimum_observed_volume_ratio": VWAP_MIN_OBSERVED_VOLUME_RATIO,
        "partial_observed_volume": partial_observed_volume,
        "volume_coverage": (
            "partial_observed_deltas"
            if partial_observed_volume
            else "all_observed_deltas"
        ),
    }
    if not series:
        return [], {
            "reason": (
                "cumulative_volume_reset"
                if resets
                else "rth_volume_sampling_gap"
                if gaps
                else "no_positive_volume_delta"
            ),
            **diagnostics,
        }
    return series, {
        "first_at": first_at.isoformat(),
        "last_at": series[-1][0].isoformat(),
        **diagnostics,
    }


def _bar_vwaps(
    bars: Sequence[Mapping[str, object]],
    series: Sequence[tuple[datetime, float]],
) -> dict[str, float]:
    result: dict[str, float] = {}
    for bar in bars:
        end = _parse_at(bar.get("bar_end"))
        if end is None:
            continue
        candidates = [point for point in series if point[0] < end]
        if not candidates:
            continue
        point = candidates[-1]
        if (end - point[0]).total_seconds() <= VWAP_MAX_GAP_SECONDS:
            result[str(bar.get("bar_start"))] = point[1]
    return result


def _price_vs_vwap(
    bars: Sequence[Mapping[str, object]],
    vwaps: Mapping[str, float],
    atr: float | None,
) -> str | None:
    window = _contiguous_ok_tail(bars)[-3:]
    if atr is None or atr <= 0 or len(window) < 2:
        return None
    rows = [
        (float(bar["close"]) - vwaps[str(bar.get("bar_start"))]) / atr
        for bar in window
        if str(bar.get("bar_start")) in vwaps
    ]
    if len(rows) < 2:
        return None
    if all(value > 0.30 for value in rows[-2:]):
        return PriceVsVwap.ABOVE_CONFIRMED.value
    if all(value < -0.30 for value in rows[-2:]):
        return PriceVsVwap.BELOW_CONFIRMED.value
    sides = [_sign(value) for value in rows if abs(value) > 0.05]
    crosses = sum(left != right for left, right in zip(sides, sides[1:]))
    if abs(rows[-1]) <= 0.05 or crosses >= 2:
        return PriceVsVwap.AROUND_OR_CROSS.value
    return PriceVsVwap.ABOVE.value if rows[-1] > 0 else PriceVsVwap.BELOW.value


def _vwap_slope(
    bars: Sequence[Mapping[str, object]],
    vwaps: Mapping[str, float],
    atr: float | None,
) -> float | None:
    window = _contiguous_ok_tail(bars)[-4:]
    values = [
        vwaps[str(bar.get("bar_start"))]
        for bar in window
        if str(bar.get("bar_start")) in vwaps
    ]
    if atr is None or atr <= 0 or len(window) < 4 or len(values) < 4:
        return None
    return (values[-1] - values[-4]) / atr


def _opening_range_state(
    bars: Sequence[Mapping[str, object]],
) -> tuple[str | None, dict[str, object]]:
    opening = [
        bar
        for bar in bars
        if time(9, 30)
        <= (_parse_at(bar.get("bar_start")) or datetime.min.replace(tzinfo=NY_TZ))
        .astimezone(NY_TZ)
        .time()
        < time(9, 45)
        and bar.get("quality") == "ok"
    ]
    first_start = _parse_at(opening[0].get("bar_start")) if opening else None
    expected_starts = (
        {
            datetime.combine(
                first_start.astimezone(NY_TZ).date(),
                clock,
                tzinfo=NY_TZ,
            ).astimezone(first_start.tzinfo)
            for clock in (time(9, 30), time(9, 35), time(9, 40))
        }
        if first_start is not None
        else set()
    )
    observed_starts = {
        start
        for bar in opening
        if (start := _parse_at(bar.get("bar_start"))) is not None
    }
    if len(opening) != 3 or observed_starts != expected_starts:
        return None, {
            "status": "unavailable",
            "reason": "opening_range_requires_three_complete_ok_bars",
            "bar_count": len(opening),
        }
    high = max(float(bar["high"]) for bar in opening)
    low = min(float(bar["low"]) for bar in opening)
    post = [
        bar
        for bar in bars
        if (
            start := _parse_at(bar.get("bar_start"))
        ) is not None
        and start.astimezone(NY_TZ).time() >= time(9, 45)
        and bar.get("quality") == "ok"
    ]
    closes = [float(bar["close"]) for bar in post]
    if len(closes) >= 2 and all(value > high for value in closes[-2:]):
        state = OpeningRangeState.ABOVE_ORH_CONFIRMED.value
    elif closes and closes[-1] > high:
        state = OpeningRangeState.BREAKOUT_ABOVE_ORH.value
    elif len(closes) >= 2 and all(value < low for value in closes[-2:]):
        state = OpeningRangeState.BELOW_ORL_CONFIRMED.value
    elif closes and closes[-1] < low:
        state = OpeningRangeState.BREAKDOWN_BELOW_ORL.value
    else:
        state = OpeningRangeState.INSIDE.value
    return state, {
        "status": "ready",
        "orh": high,
        "orl": low,
        "post_opening_bar_count": len(post),
        "acceptance": "two_closed_5m_bars",
    }


def _market_structure(
    bars: Sequence[Mapping[str, object]],
    atr: float | None,
) -> str | None:
    window = _contiguous_ok_tail(bars)[-6:]
    if len(window) < 6 or atr is None or atr <= 0:
        return None
    prior, recent = window[:3], window[3:]
    tolerance = 0.05 * atr
    prior_high = max(float(bar["high"]) for bar in prior)
    recent_high = max(float(bar["high"]) for bar in recent)
    prior_low = min(float(bar["low"]) for bar in prior)
    recent_low = min(float(bar["low"]) for bar in recent)
    hh = recent_high > prior_high + tolerance
    lh = recent_high < prior_high - tolerance
    hl = recent_low > prior_low + tolerance
    ll = recent_low < prior_low - tolerance
    if hh and hl:
        return MarketStructure.HH_HL.value
    if lh and ll:
        return MarketStructure.LH_LL.value
    if hh and not ll:
        return MarketStructure.HH_ONLY.value
    if hl and not lh:
        return MarketStructure.HL_ONLY.value
    if lh and not hl:
        return MarketStructure.LH_ONLY.value
    if ll and not hh:
        return MarketStructure.LL_ONLY.value
    return MarketStructure.OVERLAP.value


def _efficiency_ratio(
    bars: Sequence[Mapping[str, object]],
) -> float | None:
    window = _contiguous_ok_tail(bars)[-6:]
    if len(window) < 6:
        return None
    prices = [float(window[0]["open"]), *(float(bar["close"]) for bar in window)]
    path = sum(abs(current - previous) for previous, current in zip(prices, prices[1:]))
    return abs(prices[-1] - prices[0]) / path if path > 0 else 0.0


def _vwap_cross_count(
    bars: Sequence[Mapping[str, object]],
    vwaps: Mapping[str, float],
) -> int | None:
    window = _contiguous_ok_tail(bars)[-6:]
    sides = [
        _sign(float(bar["close"]) - vwaps[str(bar.get("bar_start"))])
        for bar in window
        if str(bar.get("bar_start")) in vwaps
    ]
    sides = [side for side in sides if side]
    if len(sides) < 6:
        return None
    return sum(left != right for left, right in zip(sides, sides[1:]))


def _same_time_range_ratio(
    bars: Sequence[Mapping[str, object]],
    baselines: Mapping[str, object],
    *,
    trading_date: date,
) -> tuple[float | None, dict[str, object]]:
    usable = _continuous_rth_from_open(bars)
    if not usable:
        return None, {
            "status": "unavailable",
            "reason": "rth_bars_not_continuous_from_open",
        }
    end = _parse_at(usable[-1].get("bar_end"))
    if end is None:
        return None, {"status": "unavailable", "reason": "latest_bar_end_missing"}
    slot = end.astimezone(NY_TZ).strftime("%H:%M")
    rows = _mapping(baselines.get("slots")).get(slot)
    dated_history: list[tuple[date, float]] = []
    for row in rows or []:
        if not isinstance(row, Mapping):
            continue
        try:
            observed_date = date.fromisoformat(str(row.get("trading_date_et") or ""))
        except ValueError:
            continue
        value = _number(row.get("range_points"))
        if observed_date < trading_date and value is not None and value > 0:
            dated_history.append((observed_date, value))
    history = [
        value
        for _, value in sorted(dated_history, key=lambda item: item[0])[
            -TARGET_RANGE_BASELINE_SESSIONS:
        ]
    ]
    current = max(float(bar["high"]) for bar in usable) - min(
        float(bar["low"]) for bar in usable
    )
    if len(history) < MIN_RANGE_BASELINE_SESSIONS:
        return None, {
            "status": "warming",
            "slot_et": slot,
            "sample_count": len(history),
            "minimum_sessions": MIN_RANGE_BASELINE_SESSIONS,
            "target_sessions": TARGET_RANGE_BASELINE_SESSIONS,
            "current_range_points": current,
        }
    median = statistics.median(history)
    ratio = current / median if median > 0 else None
    return ratio, {
        "status": "ready" if len(history) >= TARGET_RANGE_BASELINE_SESSIONS else "partial",
        "slot_et": slot,
        "sample_count": len(history),
        "target_sessions": TARGET_RANGE_BASELINE_SESSIONS,
        "current_range_points": current,
        "median_range_points": median,
    }


def _breadth_above_vwap(
    samples: Sequence[Mapping[str, object]],
    *,
    trading_date: date,
    now: datetime,
) -> tuple[float | None, dict[str, object]]:
    above = 0
    usable: list[str] = []
    missing: list[str] = []
    latest_times: list[datetime] = []
    providers_used: dict[str, str] = {}
    for instrument_id in SECTOR_INSTRUMENTS:
        by_provider: dict[str, list[tuple[datetime, float, float]]] = {}
        for row in samples:
            if row.get("segment") != "rth":
                continue
            at = _parse_at(row.get("at"))
            quote = _instrument(row, instrument_id)
            source_at = _parse_at(quote.get("source_at")) if quote else None
            price = _number(quote.get("price")) if quote else None
            volume = _number(quote.get("volume")) if quote else None
            if (
                at is None
                or at > now
                or at.astimezone(NY_TZ).date() != trading_date
                or source_at is None
                or source_at > now
                or source_at > at
                or source_at.astimezone(NY_TZ).date() != trading_date
                or price is None
                or volume is None
            ):
                continue
            provider = str(quote.get("provider") or "")
            if not provider:
                continue
            by_provider.setdefault(provider, []).append((source_at, volume, price))
        candidates: list[
            tuple[
                str,
                list[tuple[datetime, float]],
                list[tuple[datetime, float, float]],
            ]
        ] = []
        for provider, raw_points in by_provider.items():
            points = sorted(set(raw_points))
            series, _ = _provider_vwap_series(points, trading_date=trading_date)
            if series:
                candidates.append((provider, series, points))
        fresh_candidates = [
            item
            for item in candidates
            if 0.0
            <= (now - item[1][-1][0]).total_seconds()
            <= VWAP_MAX_GAP_SECONDS
            and 0.0
            <= (now - item[2][-1][0]).total_seconds()
            <= VWAP_MAX_GAP_SECONDS
        ]
        if not fresh_candidates:
            missing.append(instrument_id)
            continue
        provider, series, points = max(
            fresh_candidates,
            key=lambda item: (
                len(item[1]),
                item[2][-1][0],
                item[0] == "schwab",
            ),
        )
        vwap_at, vwap = series[-1]
        latest_at = points[-1][0]
        latest_price = points[-1][2]
        if (
            (now - latest_at).total_seconds() > VWAP_MAX_GAP_SECONDS
            or (now - vwap_at).total_seconds() > VWAP_MAX_GAP_SECONDS
        ):
            missing.append(instrument_id)
            continue
        usable.append(instrument_id)
        providers_used[instrument_id] = provider
        latest_times.append(latest_at)
        above += latest_price > vwap
    if len(usable) < MIN_BREADTH_INSTRUMENTS:
        return None, {
            "status": "unavailable",
            "source": "eleven_sp500_sector_etfs_above_own_rth_vwap",
            "usable_count": len(usable),
            "minimum_usable": MIN_BREADTH_INSTRUMENTS,
            "missing": missing,
        }
    cross_section_skew = (
        (max(latest_times) - min(latest_times)).total_seconds()
        if latest_times
        else None
    )
    if (
        cross_section_skew is None
        or cross_section_skew > BREADTH_MAX_CROSS_SECTION_SKEW_SECONDS
    ):
        return None, {
            "status": "unavailable",
            "reason": "sector_cross_section_timestamp_skew",
            "source": "eleven_sp500_sector_etfs_above_own_rth_vwap",
            "usable_count": len(usable),
            "minimum_usable": MIN_BREADTH_INSTRUMENTS,
            "cross_section_skew_seconds": cross_section_skew,
            "maximum_cross_section_skew_seconds": (
                BREADTH_MAX_CROSS_SECTION_SKEW_SECONDS
            ),
            "providers_used": providers_used,
        }
    return above / len(usable), {
        "status": "ready",
        "source": "eleven_sp500_sector_etfs_above_own_rth_vwap",
        "usable_count": len(usable),
        "above_count": above,
        "minimum_usable": MIN_BREADTH_INSTRUMENTS,
        "cross_section_skew_seconds": cross_section_skew,
        "maximum_cross_section_skew_seconds": BREADTH_MAX_CROSS_SECTION_SKEW_SECONDS,
        "providers_used": providers_used,
        "component_level_breadth_claimed": False,
    }


def _baseline_state(previous: Mapping[str, object] | None) -> dict[str, object]:
    if (
        not isinstance(previous, Mapping)
        or previous.get("schema_version") != "market_state_5m_range_baselines.v1"
    ):
        return {
            "schema_version": "market_state_5m_range_baselines.v1",
            "updated_at": None,
            "target_sessions": TARGET_RANGE_BASELINE_SESSIONS,
            "slots": {},
        }
    return dict(previous)


def _instrument(
    row: Mapping[str, object],
    instrument_id: str,
) -> Mapping[str, object]:
    instruments = row.get("instruments")
    if not isinstance(instruments, Mapping):
        return {}
    quote = instruments.get(instrument_id)
    return quote if isinstance(quote, Mapping) else {}


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    number = float(value)
    return number if number == number and abs(number) != float("inf") else None


def _sign(value: float) -> int:
    return 1 if value > 0 else -1 if value < 0 else 0


def _parse_at(value: object) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        return None
    return as_utc(parsed)


__all__ = [
    "SCHEMA_VERSION",
    "build_market_state_5m_inputs",
    "update_same_time_range_baselines",
]
