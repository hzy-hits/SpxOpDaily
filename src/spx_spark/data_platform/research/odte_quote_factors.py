"""Quote-only SPXW 0DTE vertical factor mining with hold-to-15:45 labels."""

from __future__ import annotations

import argparse
from bisect import bisect_right
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
from scipy.stats import spearmanr

from spx_spark.data_platform.research.odte_level_quotes import QuoteStore
from spx_spark.data_platform.research.odte_level_signals import OptionTick, UnderlierTick
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET, MarketCalendar


WIDTH = 10.0
MAX_QUOTE_AGE_SECONDS = 30.0
MAX_LEG_SKEW_SECONDS = 5.0
HARD_MARK_TIME_ET = time(15, 45)
PROVIDER_PRIORITY = ("schwab", "ibkr")
SPX_INSTRUMENT_ID = "index:SPX"
ES_INSTRUMENT_ID = "future:ES"
FACTOR_NAMES = (
    "session_gth",
    "session_rth",
    "minutes_to_close",
    "direction_call",
    "width",
    "debit_fraction_of_width",
    "quote_spread_fraction",
    "long_abs_delta",
    "short_abs_delta",
    "iv_skew",
    "spot_ret_1m",
    "spot_ret_5m",
    "spot_ret_15m",
    "spot_ret_60m",
    "atm_straddle_mid",
    "hour_et",
)


@dataclass(slots=True)
class SessionResult:
    rows: list[dict[str, Any]]
    coverage: dict[str, Any]


def session_sample_times(
    session_date: date,
    *,
    calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR,
    sampling_minutes: int = 5,
) -> tuple[tuple[datetime, str], ...]:
    """Return exchange-open GTH/RTH samples through 15:45 ET."""

    if sampling_minutes <= 0:
        raise ValueError("sampling_minutes must be positive")
    window = calendar.spx_session_window(session_date)
    if window is None:
        return ()
    cutoff = datetime.combine(session_date, HARD_MARK_TIME_ET, tzinfo=ET)
    cursor = window.session_start
    samples: list[tuple[datetime, str]] = []
    while cursor <= cutoff:
        if calendar.is_spx_gth_open(cursor):
            samples.append((cursor.astimezone(timezone.utc), "gth"))
        elif calendar.is_rth_open(cursor):
            samples.append((cursor.astimezone(timezone.utc), "rth"))
        cursor += timedelta(minutes=sampling_minutes)
    return tuple(samples)


def mine_session(
    store: QuoteStore,
    *,
    session_date: date,
    calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR,
    sampling_minutes: int = 5,
) -> SessionResult:
    """Mine one expiry after one batched underlier and option-window load."""

    samples = session_sample_times(
        session_date,
        calendar=calendar,
        sampling_minutes=sampling_minutes,
    )
    if not samples:
        return SessionResult([], {"session_date": session_date.isoformat(), "scheduled_bars": 0})
    start, close = samples[0][0], datetime.combine(
        session_date, HARD_MARK_TIME_ET, tzinfo=ET
    ).astimezone(timezone.utc)
    providers = store.option_expiry_providers(expiry=session_date, start=start, end=close)
    base_coverage: dict[str, Any] = {
        "session_date": session_date.isoformat(),
        "scheduled_bars": len(samples),
        "scheduled_bars_by_mode": dict(Counter(mode for _, mode in samples)),
        "parquet_providers": list(providers),
        "has_spxw_0dte_parquet": bool(providers),
    }
    if not providers:
        return SessionResult([], base_coverage)

    rth_times = [at for at, mode in samples if mode == "rth"]
    rth_start = rth_times[0] if rth_times else None
    es_series = store.underlier_series(
        instrument_id=ES_INSTRUMENT_ID,
        start=start,
        end=rth_start or close,
    )
    spx_start = (
        rth_start - timedelta(minutes=60, seconds=MAX_QUOTE_AGE_SECONDS)
        if rth_start is not None
        else start
    )
    spx_series = store.underlier_series(
        instrument_id=SPX_INSTRUMENT_ID,
        start=spx_start,
        end=close,
    )
    sampled_spots: dict[datetime, tuple[UnderlierTick, str, list[UnderlierTick], datetime]] = {}
    for at, mode in samples:
        if mode == "gth":
            tick = _underlier_at(es_series, at, MAX_QUOTE_AGE_SECONDS)
            if tick is None:
                continue
            sampled_spots[at] = (tick, ES_INSTRUMENT_ID, es_series, start)
        else:
            tick = _underlier_at(spx_series, at, MAX_QUOTE_AGE_SECONDS)
            if tick is None:
                continue
            sampled_spots[at] = (tick, SPX_INSTRUMENT_ID, spx_series, spx_start)
    if not sampled_spots:
        base_coverage.update(
            {
                "spot_available_bars": 0,
                "live_option_ticks_in_spot_band": 0,
                "rows": 0,
                "labeled_rows": 0,
            }
        )
        return SessionResult([], base_coverage)

    sampled_prices = [tick.price for tick, _source, _series, _floor in sampled_spots.values()]
    strike_min = math.floor(min(sampled_prices) / 5.0) * 5.0 - WIDTH
    strike_max = math.ceil(max(sampled_prices) / 5.0) * 5.0 + WIDTH
    loaded = store.load_option_window(
        expiry=session_date,
        strike_min=strike_min,
        strike_max=strike_max,
        start=start - timedelta(seconds=MAX_QUOTE_AGE_SECONDS),
        end=close,
    )

    rows: list[dict[str, Any]] = []
    exit_cache: dict[tuple[str, str, float, float], tuple[datetime, float] | None] = {}
    spot_modes: Counter[str] = Counter()
    for decision_at, session_mode in samples:
        sampled = sampled_spots.get(decision_at)
        if sampled is None:
            continue
        spot_tick, spot_source, underlier, return_floor = sampled
        spot_modes[session_mode] += 1
        strike = math.floor(spot_tick.price / 5.0 + 0.5) * 5.0
        snapshot = store.option_snapshot(
            expiry=session_date,
            as_of=decision_at,
            max_age_seconds=MAX_QUOTE_AGE_SECONDS,
            strikes=(strike - WIDTH, strike, strike + WIDTH),
        )
        straddle_mid = _atm_straddle_mid(snapshot, strike=strike, as_of=decision_at)
        returns = _spot_returns(
            underlier,
            spot_tick,
            decision_at,
            not_before=return_floor,
        )
        for direction, right, short_strike in (
            ("call", "C", strike + WIDTH),
            ("put", "P", strike - WIDTH),
        ):
            entry = _best_vertical(
                snapshot,
                long_strike=strike,
                short_strike=short_strike,
                right=right,
                as_of=decision_at,
            )
            if entry is None:
                continue
            provider = str(entry["provider"])
            cache_key = (provider, right, strike, short_strike)
            if cache_key not in exit_cache:
                exit_cache[cache_key] = _last_combo_bid(
                    store,
                    expiry=session_date,
                    provider=provider,
                    right=right,
                    long_strike=strike,
                    short_strike=short_strike,
                    start=start,
                    close=close,
                )
            exit_mark = exit_cache[cache_key]
            forward_at = decision_at + timedelta(minutes=15)
            forward_bid = None
            if forward_at <= close:
                forward_snapshot = store.option_snapshot(
                    expiry=session_date,
                    as_of=forward_at,
                    max_age_seconds=MAX_QUOTE_AGE_SECONDS,
                    strikes=(strike, short_strike),
                )
                forward = _vertical_for_provider(
                    forward_snapshot,
                    provider=provider,
                    long_strike=strike,
                    short_strike=short_strike,
                    right=right,
                    as_of=forward_at,
                )
                forward_bid = _number(forward.get("bid")) if forward else None
            entry_ask = float(entry["ask"])
            entry_bid = float(entry["bid"])
            long_tick = entry["long_tick"]
            short_tick = entry["short_tick"]
            exit_bid = exit_mark[1] if exit_mark else None
            at_et = decision_at.astimezone(ET)
            minutes_to_close = (close - decision_at).total_seconds() / 60.0
            rows.append(
                {
                    "session_date": session_date.isoformat(),
                    "decision_at": decision_at.isoformat(),
                    "session_mode": session_mode,
                    "session_gth": session_mode == "gth",
                    "session_rth": session_mode == "rth",
                    "minutes_to_close": minutes_to_close,
                    "direction": direction,
                    "direction_call": direction == "call",
                    "right": right,
                    "width": WIDTH,
                    "spot": spot_tick.price,
                    "spot_source": spot_source,
                    "atm_strike": strike,
                    "long_strike": strike,
                    "short_strike": short_strike,
                    "provider": provider,
                    "entry_combo_ask": entry_ask,
                    "entry_combo_bid": entry_bid,
                    "debit_fraction_of_width": entry_ask / WIDTH,
                    "quote_spread_fraction": (entry_ask - entry_bid) / WIDTH,
                    "long_abs_delta": _absolute(long_tick.delta),
                    "short_abs_delta": _absolute(short_tick.delta),
                    "iv_skew": _iv_skew(long_tick, short_tick),
                    **returns,
                    "atm_straddle_mid": straddle_mid,
                    "hour_et": at_et.hour,
                    "minute_et": at_et.minute,
                    "entry_long_received_at": long_tick.at.isoformat(),
                    "entry_short_received_at": short_tick.at.isoformat(),
                    "exit_at": exit_mark[0].isoformat() if exit_mark else None,
                    "exit_combo_bid": exit_bid,
                    "pnl_hold_to_1545": (
                        exit_bid - entry_ask if exit_bid is not None else None
                    ),
                    "forward_15m_at": forward_at.isoformat() if forward_at <= close else None,
                    "forward_15m_combo_bid": forward_bid,
                    "pnl_15m": forward_bid - entry_ask if forward_bid is not None else None,
                    "label_policy": "hold_to_1545_no_stop_no_trail",
                }
            )

    labeled = sum(row["pnl_hold_to_1545"] is not None for row in rows)
    base_coverage.update(
        {
            "spot_available_bars": len(sampled_spots),
            "spot_available_bars_by_mode": dict(spot_modes),
            "spot_source_counts": dict(
                Counter(source for _tick, source, _series, _floor in sampled_spots.values())
            ),
            "live_option_ticks_in_spot_band": loaded,
            "strike_band": [strike_min, strike_max],
            "rows": len(rows),
            "labeled_rows": labeled,
        }
    )
    return SessionResult(rows, base_coverage)


def _underlier_at(
    ticks: Sequence[UnderlierTick], as_of: datetime, max_age_seconds: float
) -> UnderlierTick | None:
    index = bisect_right(ticks, as_of, key=lambda tick: tick.at) - 1
    if index < 0:
        return None
    tick = ticks[index]
    age = (as_of - tick.at).total_seconds()
    return tick if 0 <= age <= max_age_seconds and tick.price > 0 else None


def _spot_returns(
    ticks: Sequence[UnderlierTick],
    current: UnderlierTick,
    decision_at: datetime,
    *,
    not_before: datetime | None = None,
) -> dict[str, float | None]:
    values: dict[str, float | None] = {}
    for minutes in (1, 5, 15, 60):
        prior_at = decision_at - timedelta(minutes=minutes)
        if not_before is not None and prior_at < not_before:
            values[f"spot_ret_{minutes}m"] = None
            continue
        prior = _underlier_at(ticks, prior_at, MAX_QUOTE_AGE_SECONDS)
        if prior is not None and not_before is not None and prior.at < not_before:
            prior = None
        values[f"spot_ret_{minutes}m"] = (
            current.price / prior.price - 1.0 if prior is not None else None
        )
    return values


def _tick_ready(tick: OptionTick, *, as_of: datetime) -> bool:
    bid, ask = _number(tick.bid), _number(tick.ask)
    if bid is None or ask is None or bid < 0 or ask <= 0 or bid > ask:
        return False
    age = (as_of - tick.at).total_seconds()
    if age < 0 or age > MAX_QUOTE_AGE_SECONDS or tick.source_at is None:
        return False
    source_lag = (tick.at - tick.source_at).total_seconds()
    return -5.0 <= source_lag <= 30.0


def _vertical_for_provider(
    snapshot: Mapping[tuple[str, float, str], OptionTick],
    *,
    provider: str,
    long_strike: float,
    short_strike: float,
    right: str,
    as_of: datetime,
) -> dict[str, Any] | None:
    long_tick = snapshot.get((provider, float(long_strike), right))
    short_tick = snapshot.get((provider, float(short_strike), right))
    if long_tick is None or short_tick is None:
        return None
    if not _tick_ready(long_tick, as_of=as_of) or not _tick_ready(short_tick, as_of=as_of):
        return None
    if abs((long_tick.at - short_tick.at).total_seconds()) > MAX_LEG_SKEW_SECONDS:
        return None
    assert long_tick.bid is not None and long_tick.ask is not None
    assert short_tick.bid is not None and short_tick.ask is not None
    combo_ask = float(long_tick.ask) - float(short_tick.bid)
    combo_bid = max(float(long_tick.bid) - float(short_tick.ask), 0.0)
    if combo_ask <= 0 or combo_bid > combo_ask:
        return None
    return {
        "provider": provider,
        "ask": combo_ask,
        "bid": combo_bid,
        "long_tick": long_tick,
        "short_tick": short_tick,
    }


def _best_vertical(
    snapshot: Mapping[tuple[str, float, str], OptionTick],
    *,
    long_strike: float,
    short_strike: float,
    right: str,
    as_of: datetime,
) -> dict[str, Any] | None:
    for provider in PROVIDER_PRIORITY:
        result = _vertical_for_provider(
            snapshot,
            provider=provider,
            long_strike=long_strike,
            short_strike=short_strike,
            right=right,
            as_of=as_of,
        )
        if result is not None:
            return result
    return None


def _last_combo_bid(
    store: QuoteStore,
    *,
    expiry: date,
    provider: str,
    right: str,
    long_strike: float,
    short_strike: float,
    start: datetime,
    close: datetime,
) -> tuple[datetime, float] | None:
    long_ticks = store.option_series(
        provider=provider,
        expiry=expiry,
        strike=long_strike,
        right=right,
        start=start,
        end=close,
    )
    short_ticks = store.option_series(
        provider=provider,
        expiry=expiry,
        strike=short_strike,
        right=right,
        start=start,
        end=close,
    )
    left, right_index = len(long_ticks) - 1, len(short_ticks) - 1
    while left >= 0 and right_index >= 0:
        long_tick, short_tick = long_ticks[left], short_ticks[right_index]
        mark_at = max(long_tick.at, short_tick.at)
        snapshot = {
            (provider, float(long_strike), right): long_tick,
            (provider, float(short_strike), right): short_tick,
        }
        mark = _vertical_for_provider(
            snapshot,
            provider=provider,
            long_strike=long_strike,
            short_strike=short_strike,
            right=right,
            as_of=mark_at,
        )
        if mark is not None:
            return mark_at, float(mark["bid"])
        if long_tick.at >= short_tick.at:
            left -= 1
        else:
            right_index -= 1
    return None


def _atm_straddle_mid(
    snapshot: Mapping[tuple[str, float, str], OptionTick],
    *,
    strike: float,
    as_of: datetime,
) -> float | None:
    for provider in PROVIDER_PRIORITY:
        call = snapshot.get((provider, float(strike), "C"))
        put = snapshot.get((provider, float(strike), "P"))
        if call is None or put is None:
            continue
        if not _tick_ready(call, as_of=as_of) or not _tick_ready(put, as_of=as_of):
            continue
        if abs((call.at - put.at).total_seconds()) > MAX_LEG_SKEW_SECONDS:
            continue
        call_mid, put_mid = _quote_mid(call), _quote_mid(put)
        if call_mid is not None and put_mid is not None:
            return call_mid + put_mid
    return None


def _quote_mid(tick: OptionTick) -> float | None:
    mid = _number(tick.mid)
    if mid is not None and mid >= 0:
        return mid
    bid, ask = _number(tick.bid), _number(tick.ask)
    return (bid + ask) / 2.0 if bid is not None and ask is not None else None


def _iv_skew(long_tick: OptionTick, short_tick: OptionTick) -> float | None:
    long_iv, short_iv = _number(long_tick.implied_vol), _number(short_tick.implied_vol)
    if long_iv is None or short_iv is None:
        return None
    if _quote_mid(long_tick) is None or _quote_mid(short_tick) is None:
        return None
    return long_iv - short_iv


def _absolute(value: object) -> float | None:
    number = _number(value)
    return abs(number) if number is not None else None


def _factor_number(value: object) -> float | None:
    return float(value) if isinstance(value, bool) else _number(value)


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _pnl_stats(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    values = sorted(
        value
        for row in rows
        if (value := _number(row.get("pnl_hold_to_1545"))) is not None
    )
    if not values:
        return {
            "n": 0,
            "mean": None,
            "median": None,
            "hit_rate": None,
            "p10": None,
            "p90": None,
        }
    return {
        "n": len(values),
        "mean": float(np.mean(values)),
        "median": float(np.median(values)),
        "hit_rate": sum(value > 0 for value in values) / len(values),
        "p10": float(np.quantile(values, 0.10)),
        "p90": float(np.quantile(values, 0.90)),
    }


def _unconditional_pnl(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "overall": _pnl_stats(rows),
        "by_session_mode": {
            mode: _pnl_stats([row for row in rows if row.get("session_mode") == mode])
            for mode in ("gth", "rth")
        },
        "by_direction": {
            direction: _pnl_stats([row for row in rows if row.get("direction") == direction])
            for direction in ("call", "put")
        },
        "by_session_mode_and_direction": {
            mode: {
                direction: _pnl_stats(
                    [
                        row
                        for row in rows
                        if row.get("session_mode") == mode
                        and row.get("direction") == direction
                    ]
                )
                for direction in ("call", "put")
            }
            for mode in ("gth", "rth")
        },
    }


def _factor_correlations(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    scopes = {
        "overall": rows,
        "gth": [row for row in rows if row.get("session_mode") == "gth"],
        "rth": [row for row in rows if row.get("session_mode") == "rth"],
    }
    result: dict[str, Any] = {}
    for scope, scoped_rows in scopes.items():
        factors: dict[str, Any] = {}
        for factor in FACTOR_NAMES:
            pairs = [
                (x, y)
                for row in scoped_rows
                if (x := _factor_number(row.get(factor))) is not None
                and (y := _number(row.get("pnl_hold_to_1545"))) is not None
            ]
            rho = None
            if len(pairs) >= 3:
                x_values, y_values = zip(*pairs, strict=True)
                if len(set(x_values)) > 1 and len(set(y_values)) > 1:
                    value = float(spearmanr(x_values, y_values).statistic)
                    rho = value if math.isfinite(value) else None
            factors[factor] = {"n": len(pairs), "spearman_rho": rho}
        result[scope] = factors
    return result


def _top_factors(correlations: Mapping[str, Any]) -> dict[str, Any]:
    overall = correlations.get("overall")
    if not isinstance(overall, Mapping):
        return {"positive": [], "negative": []}
    ranked = [
        {
            "factor": factor,
            "spearman_rho": values["spearman_rho"],
            "n": values["n"],
        }
        for factor, values in overall.items()
        if isinstance(values, Mapping) and values.get("spearman_rho") is not None
    ]
    return {
        "positive": sorted(
            (item for item in ranked if item["spearman_rho"] > 0),
            key=lambda item: item["spearman_rho"],
            reverse=True,
        )[:10],
        "negative": sorted(
            (item for item in ranked if item["spearman_rho"] < 0),
            key=lambda item: item["spearman_rho"],
        )[:10],
    }


def _quintile_cut(
    rows: Sequence[Mapping[str, Any]],
    *,
    factor: str,
    absolute: bool = False,
    split_direction: bool = False,
) -> dict[str, Any]:
    usable = [
        (row, abs(value) if absolute else value)
        for row in rows
        if (value := _number(row.get(factor))) is not None
        and _number(row.get("pnl_hold_to_1545")) is not None
    ]
    if not usable:
        return {"factor": factor, "n": 0, "boundaries": [], "buckets": {}}
    boundaries = [float(value) for value in np.quantile([value for _, value in usable], (0.2, 0.4, 0.6, 0.8))]
    buckets: defaultdict[tuple[int, str | None], list[Mapping[str, Any]]] = defaultdict(list)
    for row, value in usable:
        quintile = int(np.searchsorted(boundaries, value, side="right")) + 1
        direction = str(row.get("direction")) if split_direction else None
        buckets[(quintile, direction)].append(row)
    result: dict[str, Any] = {}
    for (quintile, direction), bucket_rows in sorted(buckets.items()):
        key = f"q{quintile}" if direction is None else f"q{quintile}_{direction}"
        result[key] = _pnl_stats(bucket_rows)
    return {
        "factor": f"abs({factor})" if absolute else factor,
        "n": len(usable),
        "boundaries": boundaries,
        "buckets": result,
    }


def build_report(
    rows: Sequence[Mapping[str, Any]],
    *,
    session_coverages: Sequence[Mapping[str, Any]],
    requested_dates: Sequence[date],
    calendar_gap_dates: Sequence[date],
    start_date: date,
    end_date: date,
    sampling_minutes: int,
    rows_path: Path,
) -> dict[str, Any]:
    available = [item for item in session_coverages if item.get("has_spxw_0dte_parquet")]
    missing = [item for item in session_coverages if not item.get("has_spxw_0dte_parquet")]
    scheduled_bars = sum(int(item.get("scheduled_bars") or 0) for item in available)
    spot_bars = sum(int(item.get("spot_available_bars") or 0) for item in available)
    nbbo_attempts = spot_bars * 2
    labeled_rows = sum(row.get("pnl_hold_to_1545") is not None for row in rows)
    provider_rows = Counter(str(row.get("provider")) for row in rows)
    provider_sessions: Counter[str] = Counter()
    for item in available:
        provider_sessions.update(str(provider) for provider in item.get("parquet_providers", ()))
    correlations = _factor_correlations(rows)
    missing_nbbo = max(nbbo_attempts - len(rows), 0)
    return {
        "schema_version": "odte_quote_factors.v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "coverage": {
            "requested_start_date": start_date.isoformat(),
            "requested_end_date": end_date.isoformat(),
            "sampling_minutes": sampling_minutes,
            "requested_cash_session_dates": [day.isoformat() for day in requested_dates],
            "dates": [str(item["session_date"]) for item in available],
            "date_count": len(available),
            "skipped_no_spxw_0dte_parquet": [str(item["session_date"]) for item in missing],
            "calendar_weekend_or_holiday_gaps": [day.isoformat() for day in calendar_gap_dates],
            "bars": scheduled_bars,
            "bars_with_spot": spot_bars,
            "bars_with_rows": len({str(row.get("decision_at")) for row in rows}),
            "rows": len(rows),
            "labeled_rows": labeled_rows,
            "providers": {
                "parquet_session_counts": dict(sorted(provider_sessions.items())),
                "emitted_row_counts": dict(sorted(provider_rows.items())),
            },
            "rows_file": str(rows_path),
        },
        "unconditional_pnl": _unconditional_pnl(rows),
        "factor_correlations": correlations,
        "top_factors": _top_factors(correlations),
        "two_way_cuts": {
            "abs_ret_5m_quintile_by_direction": _quintile_cut(
                rows,
                factor="spot_ret_5m",
                absolute=True,
                split_direction=True,
            ),
            "debit_fraction_quintile": _quintile_cut(
                rows,
                factor="debit_fraction_of_width",
            ),
        },
        "honesty": {
            "quote_only": True,
            "production_entry_gates_used": [],
            "entry_universe": "both ATM-rounded 10-wide debit directions when causal live NBBO exists",
            "gth_spot": "future:ES same-session raw prints for ATM location and returns; no ES-to-SPX basis conversion",
            "rth_spot": "index:SPX",
            "entry_price": "conservative_combo_ask_no_mid",
            "exit_policy": "hold_to_last_live_combo_bid_at_or_before_1545_et_no_stop_no_trail",
            "pnl_units": "SPX_points_before_fees",
            "sample_size_rows": len(rows),
            "sample_size_labeled_rows": labeled_rows,
            "missing_spot_bars": scheduled_bars - spot_bars,
            "missing_spot_rate": (
                (scheduled_bars - spot_bars) / scheduled_bars if scheduled_bars else None
            ),
            "nbbo_attempts_with_spot": nbbo_attempts,
            "missing_nbbo_count": missing_nbbo,
            "missing_nbbo_rate": missing_nbbo / nbbo_attempts if nbbo_attempts else None,
            "unlabeled_exit_count": len(rows) - labeled_rows,
            "weekend_holiday_gap_count": len(calendar_gap_dates),
            "no_option_parquet_session_count": len(missing),
            "limitations": [
                "Associations are in-sample and are not a production strategy or causal edge claim.",
                "Rows require live-quality causal ticks; missing NBBO and underlier prints are not imputed.",
                "GTH returns use same-session future:ES changes only; they do not chain into the prior RTH SPX print.",
                "Provider priority selects one same-provider two-leg BBO per direction and sample.",
            ],
        },
        "session_coverage": list(session_coverages),
    }


def run_factor_mining(
    *,
    data_root: Path,
    output_dir: Path,
    start_date: date,
    end_date: date,
    sampling_minutes: int = 5,
    calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR,
) -> dict[str, Any]:
    """Run the requested date range and atomically replace research outputs."""

    if end_date < start_date:
        raise ValueError("end_date must not precede start_date")
    all_dates = [
        start_date + timedelta(days=offset)
        for offset in range((end_date - start_date).days + 1)
    ]
    requested_dates = [day for day in all_dates if calendar.is_trading_day(day)]
    calendar_gaps = [day for day in all_dates if not calendar.is_trading_day(day)]
    rows: list[dict[str, Any]] = []
    coverages: list[dict[str, Any]] = []
    store = QuoteStore(data_root)
    try:
        for day in requested_dates:
            result = mine_session(
                store,
                session_date=day,
                calendar=calendar,
                sampling_minutes=sampling_minutes,
            )
            rows.extend(result.rows)
            coverages.append(result.coverage)
    finally:
        store.close()

    output_dir.mkdir(parents=True, exist_ok=True)
    rows_path = output_dir / "odte_quote_factors.rows.jsonl"
    rows_tmp = output_dir / ".odte_quote_factors.rows.jsonl.tmp"
    with rows_tmp.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    rows_tmp.replace(rows_path)
    report = build_report(
        rows,
        session_coverages=coverages,
        requested_dates=requested_dates,
        calendar_gap_dates=calendar_gaps,
        start_date=start_date,
        end_date=end_date,
        sampling_minutes=sampling_minutes,
        rows_path=rows_path,
    )
    report_path = output_dir / "odte_quote_factors.report.json"
    report_tmp = output_dir / ".odte_quote_factors.report.json.tmp"
    report_tmp.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    report_tmp.replace(report_path)
    return report


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=Path("/srv/data/spx-spark/data"))
    parser.add_argument(
        "--output-dir", type=Path, default=Path("/tmp/strategy-edge-backtest")
    )
    parser.add_argument("--start-date", type=date.fromisoformat, default=date(2026, 7, 6))
    parser.add_argument("--end-date", type=date.fromisoformat, default=date(2026, 8, 17))
    parser.add_argument("--sampling-minutes", type=int, default=5)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    report = run_factor_mining(
        data_root=args.data_root,
        output_dir=args.output_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        sampling_minutes=args.sampling_minutes,
    )
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
