#!/usr/bin/env python3
"""Causal five-minute market-state research replay from IBKR historical bars.

This is deliberately separate from the production quote-lake replay:

* historical bars validate the rule model over a longer market window;
* they do not prove what the live system knew or delivered at that time;
* every feature at boundary T uses only bars ending at or before T;
* same-time range baselines use only sessions strictly before the scored day.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

from ib_async import IB, Future, Stock

from spx_spark.application.market_features.market_state_5m import (
    RULE_VERSION,
    SCHEMA_VERSION,
    TREND_DOWN,
    TREND_UP,
    score_market_state_5m,
)
from spx_spark.application.market_features.market_state_5m_inputs import (
    MIN_BREADTH_INSTRUMENTS,
    MIN_RANGE_BASELINE_SESSIONS,
    SECTOR_INSTRUMENTS,
    TARGET_RANGE_BASELINE_SESSIONS,
    _atr_5m,
    _efficiency_ratio,
    _market_structure,
    _opening_range_state,
    _price_vs_vwap,
    _vwap_cross_count,
    _vwap_slope,
)
from spx_spark.config import NY_TZ


UTC = timezone.utc
RTH_OPEN = time(9, 30)
RTH_CLOSE = time(16, 0)
REPLAY_START = time(9, 45)
HORIZONS_MINUTES = (15, 30, 60)
DEFAULT_SECTOR_SYMBOLS = tuple(
    instrument.split(":", maxsplit=1)[1] for instrument in SECTOR_INSTRUMENTS
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch bounded IBKR historical 5m bars and run a market-time causal "
            "research replay. No orders or account data are requested."
        )
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4002)
    parser.add_argument("--client-id", type=int, default=198)
    parser.add_argument("--es-expiry", default="20260918")
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--duration", default="2 M")
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def _finite(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _ibkr_end_at(day: date) -> str:
    close = datetime.combine(day, RTH_CLOSE, tzinfo=NY_TZ).astimezone(UTC)
    return close.strftime("%Y%m%d-%H:%M:%S")


def _request_bars(
    ib: IB,
    contract: object,
    *,
    end_at: str,
    duration: str,
) -> list[dict[str, object]]:
    bars = ib.reqHistoricalData(
        contract,
        endDateTime=end_at,
        durationStr=duration,
        barSizeSetting="5 mins",
        whatToShow="TRADES",
        useRTH=True,
        formatDate=2,
        keepUpToDate=False,
        timeout=45,
    )
    rows: list[dict[str, object]] = []
    for item in bars:
        start = item.date
        if not isinstance(start, datetime) or start.tzinfo is None:
            continue
        start = start.astimezone(UTC)
        local = start.astimezone(NY_TZ)
        if not (RTH_OPEN <= local.time().replace(tzinfo=None) < RTH_CLOSE):
            continue
        open_px = _finite(item.open)
        high = _finite(item.high)
        low = _finite(item.low)
        close = _finite(item.close)
        volume = _finite(item.volume)
        wap = _finite(item.average)
        if (
            open_px is None
            or high is None
            or low is None
            or close is None
            or volume is None
            or volume < 0
        ):
            continue
        rows.append(
            {
                "bar_start": start.isoformat(),
                "bar_end": (start + timedelta(minutes=5)).isoformat(),
                "interval_seconds": 300,
                "open": open_px,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "wap": wap,
                "quality": "ok",
                "gap_before": False,
                "segment": "rth",
                "trading_date_et": local.date().isoformat(),
            }
        )
    return sorted(rows, key=lambda row: str(row["bar_start"]))


def _by_day(rows: list[dict[str, object]]) -> dict[date, list[dict[str, object]]]:
    result: dict[date, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        result[date.fromisoformat(str(row["trading_date_et"]))].append(row)
    for day_rows in result.values():
        day_rows.sort(key=lambda row: str(row["bar_start"]))
        previous: datetime | None = None
        for row in day_rows:
            start = datetime.fromisoformat(str(row["bar_start"]))
            row["gap_before"] = previous is not None and start != previous + timedelta(minutes=5)
            previous = start
    return dict(result)


def _complete_rth(rows: list[dict[str, object]]) -> bool:
    if len(rows) != 78:
        return False
    starts = [datetime.fromisoformat(str(row["bar_start"])).astimezone(NY_TZ) for row in rows]
    expected = [
        datetime.combine(starts[0].date(), RTH_OPEN, tzinfo=NY_TZ)
        + timedelta(minutes=5 * index)
        for index in range(78)
    ]
    return starts == expected and not any(row.get("gap_before") is True for row in rows)


def _bar_vwaps(rows: list[dict[str, object]]) -> dict[str, float]:
    numerator = 0.0
    denominator = 0.0
    result: dict[str, float] = {}
    for row in rows:
        volume = float(row["volume"])
        price = _finite(row.get("wap"))
        if price is None or price <= 0:
            price = (
                float(row["high"]) + float(row["low"]) + float(row["close"])
            ) / 3.0
        if volume > 0:
            numerator += price * volume
            denominator += volume
        if denominator > 0:
            result[str(row["bar_start"])] = numerator / denominator
    return result


def _same_time_ranges(
    bars_by_day: dict[date, list[dict[str, object]]],
) -> dict[tuple[date, str], float]:
    result: dict[tuple[date, str], float] = {}
    for trading_day, rows in sorted(bars_by_day.items()):
        if not _complete_rth(rows):
            continue
        high = -math.inf
        low = math.inf
        for row in rows:
            high = max(high, float(row["high"]))
            low = min(low, float(row["low"]))
            end = datetime.fromisoformat(str(row["bar_end"])).astimezone(NY_TZ)
            result[(trading_day, end.strftime("%H:%M"))] = high - low
    return result


def _range_ratio(
    *,
    trading_day: date,
    slot: str,
    current_range: float,
    ranges: dict[tuple[date, str], float],
) -> tuple[float | None, int]:
    history = [
        value
        for (observed_day, observed_slot), value in sorted(ranges.items())
        if observed_day < trading_day and observed_slot == slot and value > 0
    ][-TARGET_RANGE_BASELINE_SESSIONS:]
    if len(history) < MIN_RANGE_BASELINE_SESSIONS:
        return None, len(history)
    median = statistics.median(history)
    return (current_range / median if median > 0 else None), len(history)


def _breadth(
    *,
    trading_day: date,
    as_of: datetime,
    sectors: dict[str, dict[date, list[dict[str, object]]]],
) -> tuple[float | None, int]:
    usable = 0
    above = 0
    for symbol, bars_by_day in sectors.items():
        rows = [
            row
            for row in bars_by_day.get(trading_day, [])
            if datetime.fromisoformat(str(row["bar_end"])) <= as_of
        ]
        if not rows or not _complete_prefix(rows):
            continue
        vwaps = _bar_vwaps(rows)
        key = str(rows[-1]["bar_start"])
        vwap = vwaps.get(key)
        if vwap is None:
            continue
        usable += 1
        above += float(rows[-1]["close"]) > vwap
    if usable < MIN_BREADTH_INSTRUMENTS:
        return None, usable
    return above / usable, usable


def _complete_prefix(rows: list[dict[str, object]]) -> bool:
    if not rows:
        return False
    starts = [datetime.fromisoformat(str(row["bar_start"])).astimezone(NY_TZ) for row in rows]
    expected_first = datetime.combine(starts[0].date(), RTH_OPEN, tzinfo=NY_TZ)
    return starts[0] == expected_first and all(
        current == previous + timedelta(minutes=5)
        for previous, current in zip(starts, starts[1:], strict=False)
    )


def _replay_times(trading_day: date) -> list[datetime]:
    start = datetime.combine(trading_day, REPLAY_START, tzinfo=NY_TZ)
    end = datetime.combine(trading_day, RTH_CLOSE, tzinfo=NY_TZ)
    return [
        (start + timedelta(minutes=5 * index)).astimezone(UTC)
        for index in range(int((end - start).total_seconds() // 300))
    ]


def _forward_es(
    rows: list[dict[str, object]],
    *,
    as_of: datetime,
    origin: float | None,
) -> dict[str, object]:
    by_end = {
        datetime.fromisoformat(str(row["bar_end"])): float(row["close"]) for row in rows
    }
    result: dict[str, object] = {}
    for minutes in HORIZONS_MINUTES:
        target = as_of + timedelta(minutes=minutes)
        end = by_end.get(target)
        result[f"{minutes}m"] = {
            "status": "ready" if origin is not None and end is not None else "unavailable",
            "endpoint_points": end - origin if origin is not None and end is not None else None,
        }
    return result


def _score_day(
    trading_day: date,
    *,
    es_rows: list[dict[str, object]],
    sectors: dict[str, dict[date, list[dict[str, object]]]],
    ranges: dict[tuple[date, str], float],
) -> list[dict[str, object]]:
    observations: list[dict[str, object]] = []
    for as_of in _replay_times(trading_day):
        prefix = [
            row
            for row in es_rows
            if datetime.fromisoformat(str(row["bar_end"])) <= as_of
        ]
        atr, _ = _atr_5m(prefix)
        vwaps = _bar_vwaps(prefix)
        opening, _ = _opening_range_state(prefix)
        slot = as_of.astimezone(NY_TZ).strftime("%H:%M")
        current_range = (
            max(float(row["high"]) for row in prefix)
            - min(float(row["low"]) for row in prefix)
            if prefix
            else 0.0
        )
        range_ratio, range_samples = _range_ratio(
            trading_day=trading_day,
            slot=slot,
            current_range=current_range,
            ranges=ranges,
        )
        breadth, breadth_count = _breadth(
            trading_day=trading_day,
            as_of=as_of,
            sectors=sectors,
        )
        values = {
            "price_vs_vwap": _price_vs_vwap(prefix, vwaps, atr),
            "vwap_slope": _vwap_slope(prefix, vwaps, atr),
            "opening_range_state": opening,
            "market_structure": _market_structure(prefix, atr),
            "efficiency_ratio": _efficiency_ratio(prefix),
            "vwap_cross_count": _vwap_cross_count(prefix, vwaps),
            "same_time_range_ratio": range_ratio,
            "breadth_above_vwap": breadth,
        }
        scored = score_market_state_5m(now=as_of, **values)
        origin = float(prefix[-1]["close"]) if prefix else None
        observations.append(
            {
                "trading_date": trading_day.isoformat(),
                "as_of": as_of.isoformat(),
                "as_of_et": as_of.astimezone(NY_TZ).isoformat(),
                "state": scored["state"],
                "D": scored["D"],
                "Q": scored["Q"],
                "V": scored["V"],
                "status": scored["status"],
                "classification_tier": scored["classification_tier"],
                "state_reasons": scored["reasons"],
                "inputs": values,
                "input_missing": [key for key, value in values.items() if value is None],
                "es_close": origin,
                "range_baseline_samples": range_samples,
                "breadth_instruments": breadth_count,
                "forward_es": _forward_es(es_rows, as_of=as_of, origin=origin),
            }
        )
    return observations


def _episode_origins(observations: list[dict[str, object]]) -> list[dict[str, object]]:
    origins: list[dict[str, object]] = []
    previous_state: str | None = None
    previous_at: datetime | None = None
    for row in observations:
        state = str(row["state"])
        at = datetime.fromisoformat(str(row["as_of"]))
        new_episode = (
            state != "UNCERTAIN"
            and (
                state != previous_state
                or previous_at is None
                or at - previous_at > timedelta(minutes=5)
            )
        )
        if new_episode:
            origins.append(row)
        previous_state = state
        previous_at = at
    return origins


def _forward_summary(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    grouped: dict[tuple[str, int], list[float]] = defaultdict(list)
    for row in rows:
        state = str(row["state"])
        direction = 1 if state == TREND_UP else -1 if state == TREND_DOWN else 0
        for minutes in HORIZONS_MINUTES:
            value = row["forward_es"][f"{minutes}m"]["endpoint_points"]
            if isinstance(value, int | float):
                grouped[(state, minutes)].append(float(value) * direction if direction else float(value))
    result: list[dict[str, object]] = []
    for (state, minutes), values in sorted(grouped.items()):
        directional = state in {TREND_UP, TREND_DOWN}
        result.append(
            {
                "state": state,
                "horizon_minutes": minutes,
                "n": len(values),
                "mean_points": statistics.fmean(values),
                "median_points": statistics.median(values),
                "hit_rate": (
                    sum(value > 0 for value in values) / len(values)
                    if directional and values
                    else None
                ),
                "directional": directional,
            }
        )
    return result


def _round_numbers(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _round_numbers(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_round_numbers(item) for item in value]
    if isinstance(value, float):
        return round(value, 4)
    return value


def build_report(args: argparse.Namespace) -> dict[str, object]:
    ib = IB()
    ib.connect(
        args.host,
        args.port,
        clientId=args.client_id,
        readonly=True,
        timeout=10,
    )
    try:
        es = ib.qualifyContracts(Future("ES", args.es_expiry, "CME"))
        if not es:
            raise RuntimeError("IBKR could not qualify the ES contract")
        stocks = {
            symbol: ib.qualifyContracts(Stock(symbol, "SMART", "USD"))
            for symbol in DEFAULT_SECTOR_SYMBOLS
        }
        missing = [symbol for symbol, contracts in stocks.items() if not contracts]
        if missing:
            raise RuntimeError(f"IBKR could not qualify sector contracts: {missing}")
        end_at = _ibkr_end_at(args.end_date)
        es_rows = _request_bars(
            ib,
            es[0],
            end_at=end_at,
            duration=args.duration,
        )
        sector_rows = {
            symbol: _request_bars(
                ib,
                contracts[0],
                end_at=end_at,
                duration=args.duration,
            )
            for symbol, contracts in stocks.items()
        }
    finally:
        ib.disconnect()

    es_by_day = _by_day(es_rows)
    sectors = {symbol: _by_day(rows) for symbol, rows in sector_rows.items()}
    ranges = _same_time_ranges(es_by_day)
    observations: list[dict[str, object]] = []
    for trading_day in sorted(es_by_day):
        if not (args.start_date <= trading_day <= args.end_date):
            continue
        day_rows = es_by_day[trading_day]
        if not _complete_rth(day_rows):
            continue
        observations.extend(
            _score_day(
                trading_day,
                es_rows=day_rows,
                sectors=sectors,
                ranges=ranges,
            )
        )
    episodes = _episode_origins(observations)
    state_counts = Counter(str(row["state"]) for row in observations)
    ready_counts = Counter(
        str(row["state"]) for row in observations if row["status"] == "ready"
    )
    daily = []
    for trading_day in sorted({str(row["trading_date"]) for row in observations}):
        rows = [row for row in observations if row["trading_date"] == trading_day]
        daily.append(
            {
                "trading_date": trading_day,
                "slot_count": len(rows),
                "ready_count": sum(row["status"] == "ready" for row in rows),
                "state_counts": dict(Counter(str(row["state"]) for row in rows)),
                "minimum_range_baseline_samples": min(
                    int(row["range_baseline_samples"]) for row in rows
                ),
                "minimum_breadth_instruments": min(
                    int(row["breadth_instruments"]) for row in rows
                ),
            }
        )
    return _round_numbers(
        {
            "schema_version": "market_state_5m_ibkr_historical_replay.v1",
            "generated_at": datetime.now(tz=UTC).isoformat(),
            "model": {
                "schema_version": SCHEMA_VERSION,
                "rule_version": RULE_VERSION,
                "action_authority": "none",
                "production_thresholds_overridden": False,
            },
            "window": {
                "start_date": args.start_date.isoformat(),
                "end_date": args.end_date.isoformat(),
                "timezone": "America/New_York",
                "session": "RTH",
            },
            "source": {
                "provider": "ibkr_historical_bars",
                "bar_size": "5 mins",
                "what_to_show": "TRADES",
                "use_rth": True,
                "es_contract": f"ES {args.es_expiry}",
                "sector_symbols": list(DEFAULT_SECTOR_SYMBOLS),
                "account_or_order_data_requested": False,
            },
            "methodology": {
                "role": "strategy_research_only",
                "production_delivery_replay": False,
                "knowledge_clock": "market bar end; features use only bars ending at or before as_of",
                "range_baseline": (
                    "up to 20 strictly prior complete sessions at the same RTH bar end"
                ),
                "vwap": "IBKR historical bar WAP weighted by bar volume",
                "breadth": "11 sector ETF closes above their own cumulative RTH VWAP",
                "future_labels": "attached after scoring",
                "thresholds_overridden": False,
            },
            "coverage": {
                "es_bar_count": len(es_rows),
                "sector_bar_counts": {
                    symbol: len(rows) for symbol, rows in sector_rows.items()
                },
                "scored_session_count": len(daily),
                "slot_count": len(observations),
                "episode_count": len(episodes),
            },
            "state_counts": dict(state_counts),
            "ready_state_counts": dict(ready_counts),
            "daily": daily,
            "slot_forward_summary": _forward_summary(observations),
            "episode_forward_summary": _forward_summary(episodes),
            "episodes": episodes,
            "observations": observations,
        }
    )


def main() -> int:
    args = parse_args()
    if args.start_date > args.end_date:
        raise SystemExit("--start-date must be on or before --end-date")
    report = build_report(args)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        f"wrote {args.output} "
        f"({report['coverage']['slot_count']} slots, "
        f"{report['coverage']['scored_session_count']} sessions)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
