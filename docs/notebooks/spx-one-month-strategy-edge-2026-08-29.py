"""Raw IBKR/Schwab history -> fixed price hypotheses -> exact-leg cash outcomes.

Offline research. Outcomes originate in the raw lake; environment attribution
can enrich its audited raw replay rows. No decisions or notifications are inputs.
Providers stay separate; GTH uses IBKR ES and IBKR SPXW. These price-based research
baselines do not grant production strategy authority.
"""

from __future__ import annotations

import argparse
import hashlib
import heapq
import json
import math
from collections import Counter, defaultdict
from dataclasses import asdict, replace
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

import duckdb
import numpy as np
from scipy.optimize import brentq

from spx_spark.analytics.greeks.black_scholes import bs_delta, bs_gamma, bs_price

from spx_spark.analytics.options.strategy_payoff import (
    CLOSE_CONVERGENCE_BUTTERFLY_MANAGEMENT_POLICY,
    DEFAULT_MANAGEMENT_POLICY,
    ManagementPolicy,
    PolicyMark,
    RTH_IRON_CONDOR_MANAGEMENT_POLICY,
    simulate_management_policy,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR

UTC = timezone.utc
ET = ZoneInfo("America/New_York")
CONTRACT = {
    "input": "normalized broker quote lake only",
    "option_clock": "quote_time required, never trade_time/received_at as BBO time",
    "schwab_lane": "historical stream only; REST request-start received_at cannot establish arrival; isolate lanes before latest-book selection",
    "entry_delay_seconds": 15,
    "entry": "strikes frozen at signal; cross BBO at signal+15s, no later price search",
    "depth": "displayed size must cover each entry/exit contract quantity at its crossing side",
    "opportunities": "one first signal per setup/session; filtered and clock baselines overlap, never summed",
    "rth": {"underlier": "SPX", "quote_age_seconds": 15, "leg_skew_seconds": 2},
    "gth": {"underlier": "ES", "provider": "ibkr", "quote_age_seconds": 30, "leg_skew_seconds": 10},
    "anchor": "strike with lowest fresh call+put midpoint straddle; price geometry only",
    "vertical": {
        "width": 15,
        "signals": ["opening_range_accept", "momentum15"],
        "opening_range_accept": "first 15 minute closes; 3 consecutive closes beyond range by 1 point; until 13:30 RTH / 08:00 GTH",
        "momentum15": "check opening+30/60/90m; 16 consecutive minute closes; absolute move>=3; abs(net)/sum(abs(changes))>=0.55",
        "management": asdict(DEFAULT_MANAGEMENT_POLICY),
    },
    "condor": {
        "width": 10,
        "short_distance": "ATM straddle rounded up to 5 points",
        "signals": ["clock_condor", "balance_condor"],
        "management": asdict(RTH_IRON_CONDOR_MANAGEMENT_POLICY),
    },
    "butterfly": {
        "width": 15,
        "center": "option-implied anchor",
        "right": "cheaper fresh package at signal",
        "signals": ["clock_butterfly", "balance_butterfly"],
        "management": asdict(CLOSE_CONVERGENCE_BUTTERFLY_MANAGEMENT_POLICY),
    },
    "balance": {"efficiency_15m_max": 0.55, "rv_5m_over_prior_10m_max": 0.75},
    "gth_clocks_et": {
        "range_start": "03:00",
        "condor": "03:30",
        "butterfly": "08:25",
        "exit": "09:25",
    },
    "rth_clocks_et": {"range_start": "09:30", "condor": "10:00", "butterfly": "15:00"},
    "stop_path_max_quote_gap_seconds": 60,
    "clock_exit_wait_max_seconds": 60,
    "additional_slippage_points_per_package": [0.0, 0.05, 0.10, 0.20],
    "incomplete": "excluded from numeric PnL, retained in every denominator",
    "comparison": "fixed hypotheses, calendar-block diagnostics; no unseen holdout claim",
    "authority": "offline research; automatic_ordering=false; fills unknown",
}


def _finite(value: object) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(value)


def _at(day: date, hour: int, minute: int) -> datetime:
    return datetime.combine(day, time(hour, minute), tzinfo=ET).astimezone(UTC)


def _files(root: Path, provider: str, start: datetime, end: datetime) -> list[str]:
    cursor = start.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    end = end.astimezone(UTC)
    paths = []
    while cursor <= end:
        path = (
            root
            / f"date={cursor:%Y-%m-%d}"
            / f"provider={provider}"
            / f"hour={cursor:%H}"
            / "quotes.parquet"
        )
        if path.exists():
            paths.append(str(path))
        cursor += timedelta(hours=1)
    return paths


def _read_quotes(con, files):
    """Restore receipt events from lossless snapshot deduplication, when present.

    Unique states are not independent observations. In particular, a later
    receipt never changes quote_time or erases an intervening invalidation.
    """
    source = con.read_parquet(files, union_by_name=True, hive_partitioning=True)
    source.create_view("broker_file_input", replace=True)
    if "snapshot_receipts" in source.columns:
        con.sql("""
            SELECT q.* EXCLUDE(receipt,received_at,source_latency_ms,last_update_at),
                   q.receipt.received_at AS received_at,
                   q.receipt.source_latency_ms AS source_latency_ms, q.receipt.last_update_at AS last_update_at
            FROM (SELECT * EXCLUDE(snapshot_receipts), unnest(snapshot_receipts) AS receipt FROM broker_file_input) q
        """).create_view("broker_quotes", replace=True)
    else:
        source.create_view("broker_quotes", replace=True)


def _deduplicate_partition(con, source: Path, target: Path):
    """Dictionary-encode identical complete broker states; preserve every receipt."""
    before = source.stat()
    target.parent.mkdir(parents=True, exist_ok=True)
    con.read_parquet(str(source), hive_partitioning=False, file_row_number=True).create_view(
        "dedup_input", replace=True
    )
    fields = [
        c[0]
        for c in con.execute("DESCRIBE dedup_input").fetchall()
        if c[0] not in {"received_at", "source_latency_ms", "last_update_at", "file_row_number"}
    ]
    names = ",".join('"' + name + '"' for name in fields)
    payload = ",".join('"' + name + '":="' + name + '"' for name in fields)
    # Group on an integer fingerprint for bounded memory; this is NOT the
    # correctness check. The full field-by-field comparison below rejects a
    # hash collision instead of silently merging different broker states.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE repeated_snapshot_keys AS
        SELECT hash({names}) AS snapshot_key FROM dedup_input
        GROUP BY snapshot_key HAVING count(*)>1
    """)
    receipt = "struct_pack(received_at:=received_at,source_latency_ms:=source_latency_ms,last_update_at:=last_update_at,file_row_number:=file_row_number)"
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE dedup_output AS
        WITH marked AS MATERIALIZED (SELECT *,hash({names}) AS snapshot_key FROM dedup_input)
        SELECT {names},received_at,source_latency_ms,last_update_at,[{receipt}] AS snapshot_receipts
        FROM marked ANTI JOIN repeated_snapshot_keys USING(snapshot_key)
        UNION ALL
        SELECT state.*,received_at,source_latency_ms,last_update_at,snapshot_receipts FROM (
          SELECT first(struct_pack({payload})) AS state,
            min(received_at) AS received_at,first(source_latency_ms) AS source_latency_ms,
            first(last_update_at) AS last_update_at,list({receipt}) AS snapshot_receipts
          FROM marked SEMI JOIN repeated_snapshot_keys USING(snapshot_key) GROUP BY snapshot_key
        )
    """)
    con.execute("DROP TABLE repeated_snapshot_keys")
    original = con.execute("SELECT count(*) FROM dedup_input").fetchone()[0]
    unique, restored = con.execute(
        "SELECT count(*),sum(len(snapshot_receipts)) FROM dedup_output"
    ).fetchone()
    # Restore each original ordinal, then compare every field with NULL-safe
    # equality. The bijection avoids a large duplicate 54-column hash table.
    columns = [c[0] for c in con.execute("DESCRIBE dedup_input").fetchall()]
    original_fields = [c for c in columns if c != "file_row_number"]
    con.execute("""
        CREATE OR REPLACE TEMP VIEW restored_input AS
        SELECT q.* EXCLUDE(receipt,received_at,source_latency_ms,last_update_at),
               q.receipt.received_at AS received_at,q.receipt.source_latency_ms AS source_latency_ms,
               q.receipt.last_update_at AS last_update_at,q.receipt.file_row_number AS file_row_number
        FROM (SELECT * EXCLUDE(snapshot_receipts), unnest(snapshot_receipts) AS receipt FROM dedup_output) q
    """)
    checks = " OR ".join(f'a."{name}" IS DISTINCT FROM b."{name}"' for name in original_fields)
    mismatch = con.execute(
        f"""
        SELECT EXISTS(SELECT 1 FROM dedup_input a FULL OUTER JOIN restored_input b
                      USING(file_row_number)
                      WHERE a.file_row_number IS NULL OR b.file_row_number IS NULL OR {checks})
            OR (SELECT count(DISTINCT file_row_number) FROM restored_input) != ?
    """,
        [original],
    ).fetchone()[0]
    if mismatch or restored != original:
        raise AssertionError("deduplication did not preserve source rows")
    temporary = target.with_suffix(".partial")
    con.execute(
        "COPY (SELECT * FROM dedup_output ORDER BY received_at,instrument_id) TO ? (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 1)",
        [str(temporary)],
    )
    if (source.stat().st_size, source.stat().st_mtime_ns) != (before.st_size, before.st_mtime_ns):
        raise RuntimeError("source changed during deduplication")
    temporary.replace(target)
    con.execute("DROP TABLE dedup_output")
    return {
        "input_rows": original,
        "unique_snapshots": unique,
        "duplicate_snapshots": original - unique,
        "restored_rows": restored,
        "source_size": before.st_size,
        "source_mtime_ns": before.st_mtime_ns,
        "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        "output_sha256": hashlib.sha256(target.read_bytes()).hexdigest(),
        "output_bytes": target.stat().st_size,
        "exact_multiset_check": "PASS",
    }


def deduplicate_lake(data_root, output, start, end, providers, *, resume=False):
    if not resume and output.exists() and any(output.iterdir()):
        raise ValueError("deduplicated output must be empty")
    output.mkdir(parents=True, exist_ok=True)
    root = data_root / "lake/quotes/schema=v1"
    paths = set()
    day = start
    while day <= end:
        if DEFAULT_MARKET_CALENDAR.session(day) is not None:
            window = DEFAULT_MARKET_CALENDAR.spx_session_window(day)
            for provider in providers:
                begin = window.session_start if provider == "ibkr" else window.rth_open
                paths.update(
                    _files(
                        root,
                        provider,
                        begin - timedelta(minutes=30),
                        window.session_end + timedelta(seconds=60),
                    )
                )
        day += timedelta(days=1)
    (output / "dedup-contract.json").write_text(
        json.dumps(
            {
                "start": str(start),
                "end": str(end),
                "providers": providers,
                "key": "every original column except received_at, source_latency_ms and last_update_at",
                "receipt_lineage": "all original received_at/source_latency_ms/last_update_at triples; earliest receipt on state row",
                "scope": "per UTC partition, all instruments; no cross-partition merging",
                "verification": "exact original-row bijection and NULL-safe comparison of every column; earlier partitions verified by EXCEPT ALL",
                "originals": "unchanged",
                "independence": "unique snapshots are not independent sessions",
            },
            indent=2,
        )
    )
    completed = {}
    manifest = output / "dedup-manifest.jsonl"
    if resume and manifest.exists():
        completed = {r["source"]: r for r in map(json.loads, manifest.read_text().splitlines())}
    with duckdb.connect() as con:
        con.execute("SET threads=2; SET memory_limit='3072MB'; SET TimeZone='UTC'")
        for n, source in enumerate(sorted(paths), 1):
            source = Path(source)
            target = output / "lake/quotes/schema=v1" / source.relative_to(root)
            if str(source) in completed:
                previous = completed[str(source)]
                if (
                    hashlib.sha256(source.read_bytes()).hexdigest() != previous["source_sha256"]
                    or hashlib.sha256(target.read_bytes()).hexdigest() != previous["output_sha256"]
                ):
                    raise RuntimeError("dedup resume input/output changed")
                continue
            result = _deduplicate_partition(con, source, target)
            with (output / "dedup-manifest.jsonl").open("a") as handle:
                handle.write(
                    json.dumps({"source": str(source), "output": str(target), **result}) + "\n"
                )
            print(
                n, len(paths), source.relative_to(root), result["duplicate_snapshots"], flush=True
            )


def _valid(leg: Mapping[str, Any] | None, at: datetime, age: float) -> bool:
    if not leg:
        return False
    source, received = leg.get("quote_time"), leg.get("received_at")
    bid, ask = leg.get("bid"), leg.get("ask")
    return bool(
        source is not None
        and received is not None
        and source <= received <= at
        and 0 <= (at - source).total_seconds() <= age
        and leg.get("quality") == "live"
        and str(leg.get("market_data_type", "")).lower() in {"live", "1"}
        and _finite(bid)
        and _finite(ask)
        and 0 <= bid <= ask
        and ask > 0
    )


def _cash_quote(
    legs: Sequence[Mapping[str, Any]],
    quantities: Sequence[int],
    at: datetime,
    age: float,
    skew: float,
) -> tuple[float, float] | None:
    if len(legs) != len(quantities) or not legs or any(not _valid(leg, at, age) for leg in legs):
        return None
    if len({leg["provider"] for leg in legs}) != 1:
        return None
    sources = [leg["quote_time"] for leg in legs]
    if (max(sources) - min(sources)).total_seconds() > skew:
        return None
    purchase = sum(q * leg["ask" if q > 0 else "bid"] for leg, q in zip(legs, quantities))
    liquidation = sum(q * leg["bid" if q > 0 else "ask"] for leg, q in zip(legs, quantities))
    return float(purchase), float(liquidation)


def _depth(legs, quantities, *, liquidate=False):
    for leg, q in zip(legs, quantities):
        side = "bid_size" if (q > 0) == liquidate else "ask_size"
        size = leg.get(side) if leg else None
        if not _finite(size) or size < abs(q):
            return False
    return True


def _underlier_minutes(
    con, files, instrument: str, start: datetime, end: datetime
) -> dict[datetime, float]:
    _read_quotes(con, files)
    rows = con.execute(
        """
        WITH ticks AS (
          SELECT date_trunc('minute',received_at)+INTERVAL 1 MINUTE AS bucket_at,
            instrument_id,received_at,quality,market_data_type,
            CASE WHEN bid>0 AND ask>=bid THEN (bid+ask)/2 ELSE coalesce(last,effective_price) END AS price,
            CASE WHEN bid>0 AND ask>=bid THEN quote_time ELSE coalesce(trade_time,quote_time) END AS source
          FROM broker_quotes
          WHERE (instrument_id=? OR (?='future:ES' AND starts_with(instrument_id,'future:ES:')))
            AND received_at>=? AND received_at<?
        ) SELECT bucket_at,instrument_id,arg_max(struct_pack(price:=price,source:=source,received_at:=received_at,
             quality:=quality,market_data_type:=market_data_type),received_at) AS tick
        FROM ticks GROUP BY bucket_at,instrument_id ORDER BY bucket_at,instrument_id
    """,
        [instrument, instrument, start, end],
    ).fetchall()
    path, selected_contract = {}, None
    for at, contract, tick in rows:
        if (
            tick["source"] is None
            or tick["source"] > tick["received_at"]
            or not 0 <= (at - tick["source"]).total_seconds() <= 15
            or tick["quality"] != "live"
            or str(tick["market_data_type"]).lower() not in {"live", "1"}
            or not _finite(tick["price"])
            or tick["price"] <= 0
        ):
            continue
        # Choose from the first usable observation, not end-of-day volume.
        # Keep this dated contract throughout the session; never splice a roll.
        if selected_contract is None:
            selected_contract = contract
        if contract == selected_contract:
            path[at] = float(tick["price"])
    return path


def _window(path: Mapping[datetime, float], at: datetime, minutes: int) -> list[float]:
    times = [at - timedelta(minutes=i) for i in range(minutes - 1, -1, -1)]
    return [path[t] for t in times] if all(t in path for t in times) else []


def _balanced(path: Mapping[datetime, float], at: datetime) -> bool | None:
    values = _window(path, at, 16)
    if not values:
        return None
    changes = np.diff(values)
    gross = float(np.abs(changes).sum())
    efficiency = abs(values[-1] - values[0]) / gross if gross else 0.0
    return bool(
        efficiency <= 0.55
        and float(np.mean(changes[-5:] ** 2)) <= 0.75 * float(np.mean(changes[:10] ** 2))
    )


def _signals(day: date, mode: str, path: Mapping[datetime, float]) -> list[dict[str, Any]]:
    opening = _at(day, 9, 30) if mode == "rth" else _at(day, 3, 0)
    range_end = opening + timedelta(minutes=15)
    end = _at(day, 13, 30) if mode == "rth" else _at(day, 8, 0)
    rows = []
    initial = _window(path, range_end, 15)
    if not initial:
        rows.append({"setup": "opening_range_accept", "status": "UNDERLIER_GAP"})
    else:
        high, low = max(initial), min(initial)
        signal, missing = None, False
        at = range_end + timedelta(minutes=3)
        while at <= end:
            recent = _window(path, at, 3)
            missing = missing or not recent
            if recent and (min(recent) > high + 1 or max(recent) < low - 1):
                signal = {
                    "setup": "opening_range_accept",
                    "signal_at": at,
                    "direction": "UP" if min(recent) > high + 1 else "DOWN",
                    "family": "vertical",
                }
                break
            at += timedelta(minutes=1)
        rows.append(
            signal
            or {
                "setup": "opening_range_accept",
                "status": "UNDERLIER_GAP" if missing else "NO_TRIGGER",
            }
        )
    momentum, seen = None, 0
    for offset in (30, 60, 90):
        at = opening + timedelta(minutes=offset)
        values = _window(path, at, 16)
        if not values:
            continue
        seen += 1
        gross = sum(abs(b - a) for a, b in zip(values, values[1:]))
        move = values[-1] - values[0]
        if abs(move) >= 3 and gross and abs(move) / gross >= 0.55:
            momentum = {
                "setup": "momentum15",
                "family": "vertical",
                "signal_at": at,
                "direction": "UP" if move > 0 else "DOWN",
            }
            break
    rows.append(
        momentum
        or {"setup": "momentum15", "status": "NO_TRIGGER" if seen == 3 else "UNDERLIER_GAP"}
    )
    for family, at in (
        ("condor", opening + timedelta(minutes=30)),
        ("butterfly", _at(day, 15, 0) if mode == "rth" else _at(day, 8, 25)),
    ):
        rows.append(
            {"setup": f"clock_{family}", "family": family, "signal_at": at, "direction": "NEUTRAL"}
        )
        balance = _balanced(path, at)
        rows.append(
            {
                "setup": f"balance_{family}",
                "family": family,
                "signal_at": at,
                "direction": "NEUTRAL",
                **(
                    {}
                    if balance
                    else {"status": "UNDERLIER_GAP" if balance is None else "NO_TRIGGER"}
                ),
            }
        )
    return rows


def _snapshots(con, files, day: date, times: Sequence[datetime], provider: str, age: float, *, include_open_interest=False):
    times = sorted(set(times))
    if not times:
        return {}
    _read_quotes(con, files)
    values = ",".join("(?)" for _ in times)
    rows = con.execute(
        f"""
        WITH wanted(decision_at) AS (VALUES {values}), latest AS (
          SELECT w.decision_at,q.instrument_id,q.strike,q."right",q.bid,q.ask,q.bid_size,q.ask_size,
            q.quote_time,q.received_at,q.quality,q.market_data_type,q.delta,q.implied_vol,
            {"q.open_interest" if include_open_interest else "NULL"} AS open_interest,
            row_number() OVER(PARTITION BY w.decision_at,q.instrument_id ORDER BY q.received_at DESC) AS n
          FROM wanted w JOIN broker_quotes q
            ON q.received_at<=w.decision_at AND q.received_at>=w.decision_at-INTERVAL '{int(age)} seconds'
          WHERE q.trading_class='SPXW' AND q.expiry=?
            AND (q.provider!='schwab' OR q.greeks_model='schwab_stream')
        ) SELECT * EXCLUDE(n) FROM latest WHERE n=1 ORDER BY decision_at,instrument_id
    """,
        [*times, day],
    ).fetchall()
    result = {at: {} for at in times}
    names = [d[0] for d in con.description][1:]
    for at, *values in rows:
        leg = dict(zip(names, values))
        leg["provider"] = provider
        result[at][(leg["strike"], leg["right"])] = leg
    return result


def _structure(signal, chain, *, age: float, skew: float, anchor_override=None):
    at = signal["signal_at"]
    pairs = []
    for strike, right in chain:
        if right != "C":
            continue
        pair = [chain.get((strike, "C")), chain.get((strike, "P"))]
        if any(leg is None for leg in pair) or _cash_quote(pair, (1, 1), at, age, skew) is None:
            continue
        pairs.append((sum((leg["bid"] + leg["ask"]) / 2 for leg in pair), float(strike)))
    if not pairs:
        return None, "ANCHOR_BBO_UNAVAILABLE"
    straddle, anchor = min(pairs)
    if anchor_override is not None:
        anchor = float(anchor_override)
    family = signal["family"]
    if family == "vertical":
        right = "C" if signal["direction"] == "UP" else "P"
        keys = [(anchor, right), (anchor + (15 if right == "C" else -15), right)]
        quantities, width = [1, -1], 15
    elif family == "condor":
        distance = max(5, math.ceil(straddle / 5) * 5)
        keys = [
            (anchor - distance - 10, "P"),
            (anchor - distance, "P"),
            (anchor + distance, "C"),
            (anchor + distance + 10, "C"),
        ]
        quantities, width = [1, -1, -1, 1], 10
    else:
        choices = []
        for right in ("C", "P"):
            keys = [(anchor - 15, right), (anchor, right), (anchor + 15, right)]
            legs = [chain.get(key) for key in keys]
            if any(leg is None for leg in legs):
                continue
            quote = _cash_quote(legs, (1, -2, 1), at, age, skew)
            if quote and 0 < quote[0] < 15:
                choices.append((quote[0], right, keys))
        if not choices:
            return None, "BUTTERFLY_LEGS_UNAVAILABLE"
        keys = min(choices)[2]
        quantities, width = [1, -2, 1], 15
    legs = [chain.get(key) for key in keys]
    if any(leg is None for leg in legs):
        return None, "EXACT_LEGS_MISSING"
    quote = _cash_quote(legs, quantities, at, age, skew)
    if quote is None:
        return None, "EXACT_LEGS_INVALID"
    entry = -quote[0] if family == "condor" else quote[0]
    if not 0 < entry < width:
        return None, "ENTRY_GEOMETRY_INVALID"
    return {
        "anchor": anchor,
        "atm_straddle_points": straddle,
        "legs": legs,
        "quantities": quantities,
        "width": width,
        "signal_package_price": entry,
    }, None


def _contract_events(con, files, ids, start, end, provider):
    if not ids:
        return {}
    _read_quotes(con, files)
    rows = con.execute(
        """
        SELECT instrument_id,strike,"right",bid,ask,bid_size,ask_size,quote_time,
               received_at,quality,market_data_type
        FROM broker_quotes
        WHERE instrument_id IN (SELECT unnest(?)) AND received_at BETWEEN ? AND ?
          AND (provider!='schwab' OR greeks_model='schwab_stream')
        ORDER BY received_at,instrument_id
    """,
        [sorted(set(ids)), start, end],
    ).fetchall()
    names = [d[0] for d in con.description]
    events = defaultdict(list)
    for row in rows:
        leg = dict(zip(names, row))
        leg["provider"] = provider
        events[leg["instrument_id"]].append(leg)
    return events


def _package_path(row, events, end: datetime, age: float, skew: float):
    start = row["entry_at"]
    ids = [leg["instrument_id"] for leg in row["legs"]]
    book = {item["instrument_id"]: item for item in row["legs"]}
    marks = []
    quote = _cash_quote(row["legs"], row["quantities"], start, age, skew)
    if quote is not None and _depth(row["legs"], row["quantities"], liquidate=True):
        marks.append(
            PolicyMark(
                start, quote[1] + (2 * row["entry_price"] if row["family"] == "condor" else 0)
            )
        )
    merged = heapq.merge(*(events.get(i, []) for i in ids), key=lambda leg: leg["received_at"])
    previous = None
    for leg in merged:
        at = leg["received_at"]
        if at < start:
            continue
        if at > end:
            break
        if previous is not None and at != previous:
            quote = _cash_quote([book.get(i) for i in ids], row["quantities"], previous, age, skew)
            if quote is not None and _depth(
                [book.get(i) for i in ids], row["quantities"], liquidate=True
            ):
                value = quote[1] + (2 * row["entry_price"] if row["family"] == "condor" else 0)
                marks.append(PolicyMark(previous, value))
        previous = at
        prior = book.get(leg["instrument_id"])
        if (
            prior is None
            or leg["quote_time"] is None
            or prior["quote_time"] is None
            or leg["quote_time"] >= prior["quote_time"]
        ):
            book[leg["instrument_id"]] = leg
    if previous is not None:
        quote = _cash_quote([book.get(i) for i in ids], row["quantities"], previous, age, skew)
        if quote is not None and _depth(
            [book.get(i) for i in ids], row["quantities"], liquidate=True
        ):
            value = quote[1] + (2 * row["entry_price"] if row["family"] == "condor" else 0)
            marks.append(PolicyMark(previous, value))
    return marks


def _label(row, events, exit_chain, *, day, mode, age, skew):
    entry, family = row["entry_price"], row["family"]
    policy = (
        DEFAULT_MANAGEMENT_POLICY
        if family == "vertical"
        else (
            RTH_IRON_CONDOR_MANAGEMENT_POLICY
            if family == "condor"
            else CLOSE_CONVERGENCE_BUTTERFLY_MANAGEMENT_POLICY
        )
    )
    if mode == "gth":
        policy = replace(
            policy,
            hard_exit_et="09:25",
            policy_version=policy.policy_version + ".research_gth_0925",
        )
    hour, minute = map(int, policy.hard_exit_et.split(":"))
    deadline = _at(day, hour, minute)
    marks = _package_path(row, events, deadline + timedelta(seconds=60), age, skew)
    # A pure hold only needs the scheduled exit book. Missing observations at
    # times where no action can occur do not censor that contract's cash exit.
    if family == "butterfly":
        legs = [exit_chain.get((leg["strike"], leg["right"])) for leg in row["legs"]]
        quote = _cash_quote(legs, row["quantities"], deadline, age, skew)
        marks = (
            [PolicyMark(deadline, quote[1])]
            if quote is not None and _depth(legs, row["quantities"], liquidate=True)
            else [m for m in marks if m.at >= deadline][:1]
        )
    label = simulate_management_policy(
        marks,
        entry_ask=entry,
        leg_count=sum(abs(q) for q in row["quantities"]),
        entry_at=row["entry_at"],
        policy=policy,
        session_date=day,
        max_quote_gap_seconds=None if family == "butterfly" else 60,
    )
    status = (
        "COMPLETE_EXIT"
        if label.policy_pnl_points is not None
        else ("QUOTE_GAP" if label.exit_reason == "quote_gap" else "CENSORED")
    )
    result = {
        **asdict(label),
        "status": status,
        "pnl_usd": None if label.policy_pnl_points is None else 100 * label.policy_pnl_points,
        "contract_count": sum(abs(q) for q in row["quantities"]),
        "management": asdict(policy),
        "mark_count": len(marks),
        "policy_stop_reachable_inside_width": None
        if family != "condor"
        else 3 * entry <= row["width"],
    }
    if family == "butterfly":
        result.update(mfe_points=None, mae_points=None)
    if label.exit_bid is not None:
        result["cash_exit_points"] = (
            2 * entry - label.exit_bid if family == "condor" else label.exit_bid
        )
        expected = (
            entry - result["cash_exit_points"]
            if family == "condor"
            else result["cash_exit_points"] - entry
        ) - label.fees_points
        if not math.isclose(expected * 100, result["pnl_usd"], abs_tol=0.0002):
            raise AssertionError("cash_ledger_does_not_reconcile")
    return result


def _spx_at(con, files, times):
    times = sorted(set(times))
    if not times:
        return {}
    _read_quotes(con, files)
    values = ",".join("(?)" for _ in times)
    rows = con.execute(
        f"""
        WITH wanted(decision_at) AS (VALUES {values}), latest AS (
          SELECT w.decision_at,q.*,row_number() OVER(PARTITION BY w.decision_at ORDER BY q.received_at DESC) AS n
          FROM wanted w JOIN broker_quotes q ON q.received_at<=w.decision_at
            AND q.received_at>=w.decision_at-INTERVAL 15 SECOND
          WHERE q.instrument_id='index:SPX' AND lower(q.market_data_type) IN ('live','1')
        ) SELECT decision_at,received_at,quality,
          CASE WHEN bid>0 AND ask>=bid THEN (bid+ask)/2 ELSE coalesce(last,effective_price) END AS price,
          CASE WHEN bid>0 AND ask>=bid THEN quote_time ELSE coalesce(trade_time,quote_time) END AS source
        FROM latest WHERE n=1
    """,
        times,
    ).fetchall()
    return {
        at: price
        for at, received, quality, price, source in rows
        if quality == "live"
        and source is not None
        and source <= received <= at
        and _finite(price)
        and 0 < price
        and 0 <= (at - source).total_seconds() <= 15
    }


def _intrinsic(row, spot):
    return sum(
        q * max((spot - leg["strike"]) * (1 if leg["right"] == "C" else -1), 0)
        for leg, q in zip(row["legs"], row["quantities"])
    )


def _attribute(con, files, rows, events, path, day, mode, age, skew, exit_chain):
    """Frozen one-change policy comparisons, no search for the best hindsight exit."""
    times = []
    for row in rows:
        times.append(row["entry_at"])
        if row.get("exit_at") is not None:
            times.append(row["exit_at"])
    spots = _spx_at(con, files, times) if mode == "rth" else {}
    for row in rows:
        entry, family = row["entry_price"], row["family"]
        policy = row["management"]
        policy = ManagementPolicy(**policy)
        change = (
            {"time_stop_minutes": 20}
            if family == "vertical"
            else {"credit_stop_loss_multiple": 1.0}
            if family == "condor"
            else {"premium_stop_fraction": 0.5}
        )
        alternative = replace(
            policy, **change, policy_version=policy.policy_version + ".research_alternative"
        )
        hour, minute = map(int, policy.hard_exit_et.split(":"))
        marks = _package_path(
            row, events, _at(day, hour, minute) + timedelta(seconds=60), age, skew
        )
        if family == 'butterfly':
            # The stop ablation must share the hold baseline's scheduled book.
            # Waiting for the next receipt also changes execution time and can
            # falsely attribute a closing-spread spike to the added stop rule.
            deadline = _at(day, hour, minute)
            legs = [exit_chain.get((leg['strike'], leg['right'])) for leg in row['legs']]
            quote = _cash_quote(legs, row['quantities'], deadline, age, skew)
            if quote is not None and _depth(legs, row['quantities'], liquidate=True):
                marks = sorted(
                    [m for m in marks if m.at != deadline] + [PolicyMark(deadline, quote[1])],
                    key=lambda m: m.at,
                )
        label = simulate_management_policy(
            marks,
            entry_ask=entry,
            leg_count=sum(abs(q) for q in row["quantities"]),
            entry_at=row["entry_at"],
            policy=alternative,
            session_date=day,
            max_quote_gap_seconds=60,
        )
        baseline = row.get("pnl_usd")
        net = None if label.policy_pnl_points is None else 100 * label.policy_pnl_points
        a = {
            "alternative_change": change,
            "alternative_pnl_usd": net,
            "alternative_exit_reason": label.exit_reason,
            "alternative_exit_at": label.exit_at,
            "paired_difference_usd": None if baseline is None or net is None else net - baseline,
            "entry_cost_fraction": entry / row["width"],
            "entry_half_spread_usd": 50
            * sum(
                abs(q) * (leg["ask"] - leg["bid"]) for leg, q in zip(row["legs"], row["quantities"])
            ),
            "spx_entry": spots.get(row["entry_at"]),
            "spx_exit": spots.get(row.get("exit_at")),
        }
        values = _window(path, row["signal_at"], 16)
        if values:
            changes = np.diff(values)
            gross = float(np.abs(changes).sum())
            a["signal_net_15m"] = values[-1] - values[0]
            a["signal_efficiency_15m"] = abs(values[-1] - values[0]) / gross if gross else 0.0
        if baseline is not None:
            a["gross_cross_bbo_pnl_usd"] = baseline + 100 * row["fees_points"]
        if baseline is not None and a["spx_entry"] is not None and a["spx_exit"] is not None:
            intrinsic_change = 100 * (
                _intrinsic(row, a["spx_exit"]) - _intrinsic(row, a["spx_entry"])
            )
            a["intrinsic_change_usd"] = intrinsic_change
            a["extrinsic_and_execution_change_usd"] = (
                a["gross_cross_bbo_pnl_usd"] - intrinsic_change
            )
            # This is an accounting identity, not a causal theta/IV decomposition.
            a["spx_move_to_exit"] = a["spx_exit"] - a["spx_entry"]
            a["exit_distance_from_anchor"] = abs(a["spx_exit"] - row["anchor"])
            if family == "vertical":
                a["direction_correct_to_exit"] = (
                    a["spx_move_to_exit"] * (1 if row["direction"] == "UP" else -1) > 0
                )
        row["attribution"] = a


def _attribution_summary(rows):
    groups = defaultdict(list)
    for row in rows:
        groups[(row["provider"], row["mode"], row["setup"])].append(row)
    output = []
    for key, group in sorted(groups.items()):
        entered = [r for r in group if "attribution" in r]
        complete = [r for r in entered if r.get("pnl_usd") is not None]
        pairs = [r for r in complete if r["attribution"]["paired_difference_usd"] is not None]
        classified = [r for r in complete if "direction_correct_to_exit" in r["attribution"]]
        identity = [r for r in complete if "intrinsic_change_usd" in r["attribution"]]

        # Isolate the existing ordinary price cap, without pretending that it
        # reconstructs Delta freshness, transition permission or setup gates.
        priced = [
            r
            for r in entered
            if (
                0.25 <= r["entry_price"] / r["width"] <= 0.55
                if r["family"] == "condor"
                else r["entry_price"] / r["width"] <= 0.45
            )
        ]
        priced_complete = [r for r in priced if r.get("pnl_usd") is not None]

        def avg(values):
            return float(np.mean(values)) if values else None

        output.append(
            {
                "provider": key[0],
                "mode": key[1],
                "setup": key[2],
                "opportunities": len(group),
                "entered": len(entered),
                "complete": len(complete),
                "exit_reasons": dict(Counter(r.get("exit_reason") for r in entered)),
                "alternative_complete": sum(
                    r["attribution"]["alternative_pnl_usd"] is not None for r in entered
                ),
                "ordinary_price_gate_only": {
                    "entered_passing": len(priced),
                    "complete_passing": len(priced_complete),
                    "conditional_mean_usd": avg([r["pnl_usd"] for r in priced_complete]),
                    "authority": "price diagnostic only, not full production eligibility",
                },
                "paired_complete": len(pairs),
                "paired_baseline_mean": avg([r["pnl_usd"] for r in pairs]),
                "paired_alternative_mean": avg(
                    [r["attribution"]["alternative_pnl_usd"] for r in pairs]
                ),
                "paired_difference_mean": avg(
                    [r["attribution"]["paired_difference_usd"] for r in pairs]
                ),
                "paired_alternative_worse": sum(
                    r["attribution"]["paired_difference_usd"] < 0 for r in pairs
                ),
                "direction_classifiable": len(classified),
                "direction_correct_but_loss": sum(
                    r["attribution"]["direction_correct_to_exit"] and r["pnl_usd"] < 0
                    for r in classified
                ),
                "direction_wrong_and_loss": sum(
                    not r["attribution"]["direction_correct_to_exit"] and r["pnl_usd"] < 0
                    for r in classified
                ),
                "cash_decomposition_complete": len(identity),
                "intrinsic_change_mean": avg(
                    [r["attribution"]["intrinsic_change_usd"] for r in identity]
                ),
                "extrinsic_and_execution_change_mean": avg(
                    [r["attribution"]["extrinsic_and_execution_change_usd"] for r in identity]
                ),
            }
        )
    return output


def _model_check(rows):
    """Expanding-session diagnostics; previously explored history is not a holdout."""
    from sklearn.linear_model import Ridge
    from sklearn.pipeline import make_pipeline
    from sklearn.preprocessing import StandardScaler

    groups = defaultdict(list)
    for row in rows:
        if row["setup"] in {"opening_range_accept", "clock_condor", "clock_butterfly"}:
            groups[(row["provider"], row["mode"], row["setup"])].append(row)
    reports = []
    for key, group in sorted(groups.items()):
        history = []
        for row in sorted(group, key=lambda r: r["session_date"]):
            a = row.get("attribution", {})
            if "signal_efficiency_15m" not in a:
                continue
            features = [
                a["entry_cost_fraction"],
                row["atm_straddle_points"] / row["width"],
                abs(a["signal_net_15m"]) / row["width"],
                a["signal_efficiency_15m"],
            ]
            if len(history) >= 15:
                x, y = zip(*[(r["features"], r["pnl"]) for r in history])
                model = make_pipeline(StandardScaler(), Ridge(alpha=10.0)).fit(x, y)
                row["model_check"] = {
                    "training_sessions": len(history),
                    "trained_through": history[-1]["day"],
                    "ridge_expected_usd": float(model.predict([features])[0]),
                    "constant_expected_usd": float(np.mean(y)),
                }
            if row.get("pnl_usd") is not None:
                history.append(
                    {"day": row["session_date"], "features": features, "pnl": row["pnl_usd"]}
                )
        eligible = [r for r in group if "model_check" in r]
        paired = [r for r in eligible if r.get("pnl_usd") is not None]
        reports.append(
            {
                "provider": key[0],
                "mode": key[1],
                "setup": key[2],
                "scorable_opportunities": len(eligible),
                "paired_complete": len(paired),
                "baseline_mean": float(np.mean([r["pnl_usd"] for r in paired])) if paired else None,
                **{
                    name: {
                        "selected": sum(
                            r["model_check"][name + "_expected_usd"] > 0 for r in eligible
                        ),
                        "selected_missing_exit": sum(
                            r["model_check"][name + "_expected_usd"] > 0
                            and r.get("pnl_usd") is None
                            for r in eligible
                        ),
                        "paired_mean_per_opportunity": float(
                            np.mean(
                                [
                                    r["pnl_usd"]
                                    if r["model_check"][name + "_expected_usd"] > 0
                                    else 0
                                    for r in paired
                                ]
                            )
                        )
                        if paired
                        else None,
                        "paired_trades": sum(
                            r["model_check"][name + "_expected_usd"] > 0 for r in paired
                        ),
                    }
                    for name in ("constant", "ridge")
                },
            }
        )
    return reports


def _quality(con, files, day, start, end):
    _read_quotes(con, files)
    result = con.execute(
        """
        SELECT count(*) AS rows,count(*) FILTER(WHERE quote_time IS NULL) AS missing_bbo_clock,
          count(*) FILTER(WHERE quality!='live') AS non_live_quality,
          count(*) FILTER(WHERE market_data_type IS NULL) AS missing_market_data_type,
          count(*) FILTER(WHERE greeks_model='schwab_stream') AS schwab_stream_rows,
          count(*) FILTER(WHERE quote_time>received_at) AS clock_ahead_of_receipt,
          count(*) FILTER(WHERE bid IS NULL OR ask IS NULL OR bid<0 OR ask<=0 OR ask<bid) AS invalid_bbo,
          count(*) FILTER(WHERE received_at-quote_time>INTERVAL 15 SECOND) AS source_age_over_15s,
          count(*) FILTER(WHERE delta IS NOT NULL) AS rows_with_delta
        FROM broker_quotes WHERE trading_class='SPXW' AND expiry=?
          AND received_at>=? AND received_at<?
    """,
        [day, start, end],
    ).fetchone()
    counts = dict(zip([d[0] for d in con.description], result))
    if "snapshot_receipts" in con.sql("SELECT * FROM broker_file_input LIMIT 0").columns:
        counts["unique_snapshots"] = con.execute(
            """
            SELECT count(*) FROM broker_file_input WHERE trading_class='SPXW' AND expiry=?
              AND len(list_filter(snapshot_receipts,r->r.received_at>=? AND r.received_at<?))>0
        """,
            [day, start, end],
        ).fetchone()[0]
    return counts


def _summary(rows):
    groups = defaultdict(list)
    for row in rows:
        groups[(row["provider"], row["mode"], row["setup"])].append(row)
    result = []
    for (provider, mode, setup), group in sorted(groups.items()):
        complete = [r for r in group if r["status"] == "COMPLETE_EXIT"]
        pnl = np.array([r["pnl_usd"] for r in complete])
        ci = None
        if len(pnl) >= 2:
            rng = np.random.default_rng(20260905)
            ci = np.quantile(
                np.mean(rng.choice(pnl, size=(2000, len(pnl)), replace=True), axis=1),
                [0.025, 0.975],
            ).tolist()
        blocks = {}
        for month in sorted({r["session_date"][:7] for r in group}):
            sample = [r["pnl_usd"] for r in complete if r["session_date"].startswith(month)]
            blocks[month] = {
                "complete": len(sample),
                "mean_usd": float(np.mean(sample)) if sample else None,
            }
        result.append(
            {
                "provider": provider,
                "mode": mode,
                "setup": setup,
                "session_opportunities": len(group),
                "statuses": dict(Counter(r["status"] for r in group)),
                "complete_sessions": len(pnl),
                "conditional_mean_usd": float(pnl.mean()) if len(pnl) else None,
                "worst_usd": float(pnl.min()) if len(pnl) else None,
                "positive_sessions": int((pnl > 0).sum()),
                "complete_case_session_bootstrap_95": ci,
                "calendar_blocks": blocks,
                "slippage_mean_usd": {
                    str(s): float(pnl.mean() - 100 * s) if len(pnl) else None
                    for s in CONTRACT["additional_slippage_points_per_package"]
                },
            }
        )
    return result


def run(
    data_root: Path,
    output: Path,
    *,
    start: date | None = None,
    end: date | None = None,
    providers=("schwab", "ibkr"),
    attribution=False,
):
    if output.exists() and any(output.iterdir()):
        raise ValueError("output directory must be empty")
    output.mkdir(parents=True, exist_ok=True)
    root = data_root / "lake/quotes/schema=v1"
    last_closed = datetime.now(ET).date() - timedelta(days=1)
    partitions = sorted(
        {date.fromisoformat(p.name.removeprefix("date=")) for p in root.glob("date=*")}
    )
    if not partitions:
        raise ValueError("quote lake has no date partitions")
    dates = [
        partitions[0] + timedelta(days=i) for i in range((partitions[-1] - partitions[0]).days + 1)
    ]
    days = [
        day
        for day in dates
        if (start is None or day >= start)
        and day <= min(end or last_closed, last_closed)
        and DEFAULT_MARKET_CALENDAR.session(day) is not None
    ]
    source_bytes = Path(__file__).read_bytes()
    (output / "research-source.py").write_bytes(source_bytes)
    contract = {
        **CONTRACT,
        "dates": [str(day) for day in days],
        "providers": list(providers),
        "attribution": {
            "enabled": attribution,
            "model_check": "one per session/setup; min 15 earlier complete sessions; expanding StandardScaler+Ridge(alpha=10), constant mean baseline; predict net dollars from entry cost/width, straddle/width, abs preceding 15m move/width, preceding efficiency; trade iff prediction>0; no tuning, no unseen-holdout claim",
            "alternatives": {
                "vertical": "add 20m time stop",
                "condor": "credit loss stop 100% instead of 200%",
                "butterfly": "add 50% debit stop",
            },
            "comparison": "same frozen signal, legs, entry and fees; paired complete exits only; missing retained",
            "decomposition": "fresh SPX intrinsic change plus extrinsic/execution residual; not separate theta or IV",
        },
        "script_sha256": hashlib.sha256(source_bytes).hexdigest(),
    }
    (output / "contract.json").write_text(json.dumps(contract, indent=2))
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    con.execute("SET threads=2")
    con.execute("SET memory_limit='768MB'")
    con.execute("SET enable_progress_bar=false")
    all_rows, coverage, inputs = [], [], {}
    for day in days:
        cohorts = [(p, "rth") for p in providers] + (
            [("ibkr", "gth")] if "ibkr" in providers else []
        )
        for provider, mode in cohorts:
            session = DEFAULT_MARKET_CALENDAR.spx_session_window(day)
            begin = session.rth_open if mode == "rth" else session.session_start
            finish = session.session_end if mode == "rth" else _at(day, 9, 25)
            paths = _files(
                root, provider, begin - timedelta(minutes=30), finish + timedelta(seconds=60)
            )
            if not paths:
                coverage.append(
                    {
                        "provider": provider,
                        "mode": mode,
                        "day": str(day),
                        "status": "PARTITION_MISSING",
                    }
                )
                missing = [
                    {
                        "provider": provider,
                        "mode": mode,
                        "session_date": str(day),
                        "setup": setup,
                        "status": "PARTITION_MISSING",
                        "fill_status": "UNKNOWN",
                    }
                    for setup in (
                        "opening_range_accept",
                        "momentum15",
                        "clock_condor",
                        "balance_condor",
                        "clock_butterfly",
                        "balance_butterfly",
                    )
                ]
                all_rows.extend(missing)
                with (output / "rows.jsonl").open("a") as handle:
                    for row in missing:
                        handle.write(json.dumps(row) + "\n")
                (output / "coverage.json").write_text(json.dumps(coverage, indent=2, default=str))
                continue
            for p in paths:
                stat = Path(p).stat()
                inputs[p] = {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns}
            age, skew = (15, 2) if mode == "rth" else (30, 10)
            path = _underlier_minutes(
                con, paths, "index:SPX" if mode == "rth" else "future:ES", begin, finish
            )
            signals = _signals(day, mode, path)
            requested = [
                at
                for s in signals
                if "status" not in s
                for at in (s["signal_at"], s["signal_at"] + timedelta(seconds=15))
            ]
            requested.extend(
                [_at(day, 15, 45), _at(day, 15, 55)] if mode == "rth" else [_at(day, 9, 25)]
            )
            snapshots = _snapshots(con, paths, day, requested, provider, age)
            rows = []
            for signal in signals:
                row = {
                    **signal,
                    "provider": provider,
                    "mode": mode,
                    "session_date": str(day),
                    "fill_status": "UNKNOWN",
                }
                if "status" in row:
                    rows.append(row)
                    continue
                structure, reason = _structure(
                    signal, snapshots.get(signal["signal_at"], {}), age=age, skew=skew
                )
                if structure is None:
                    rows.append({**row, "status": reason})
                    continue
                row.update(structure)
                at = signal["signal_at"] + timedelta(seconds=15)
                legs = [
                    snapshots.get(at, {}).get((leg["strike"], leg["right"])) for leg in row["legs"]
                ]
                quote = _cash_quote(legs, row["quantities"], at, age, skew)
                if quote is None:
                    rows.append({**row, "status": "ENTRY_BBO_UNAVAILABLE"})
                    continue
                if not _depth(legs, row["quantities"]):
                    rows.append({**row, "status": "ENTRY_DISPLAYED_SIZE_INSUFFICIENT_OR_UNKNOWN"})
                    continue
                entry = -quote[0] if row["family"] == "condor" else quote[0]
                if not 0 < entry < row["width"]:
                    rows.append({**row, "status": "ENTRY_GEOMETRY_INVALID"})
                    continue
                row.update(legs=legs, entry_at=at, entry_price=entry, status="ENTERED")
                rows.append(row)
            entered = [row for row in rows if row["status"] == "ENTERED"]
            events = _contract_events(
                con,
                paths,
                [leg["instrument_id"] for row in entered for leg in row["legs"]],
                begin,
                finish + timedelta(seconds=60),
                provider,
            )
            for row in entered:
                deadline = _at(day, 15, 55) if mode == "rth" else _at(day, 9, 25)
                row.update(
                    _label(
                        row,
                        events,
                        snapshots.get(deadline, {}),
                        day=day,
                        mode=mode,
                        age=age,
                        skew=skew,
                    )
                )
            if attribution:
                deadline = _at(day, 15, 55) if mode == 'rth' else _at(day, 9, 25)
                _attribute(con, paths, entered, events, path, day, mode, age, skew,
                           snapshots.get(deadline, {}))
            q = {
                "provider": provider,
                "mode": mode,
                "day": str(day),
                "usable_underlier_minutes": len(path),
                "option_data": _quality(con, paths, day, begin, finish),
                "statuses": dict(Counter(row["status"] for row in rows)),
            }
            coverage.append(q)
            all_rows.extend(rows)
            with (output / "rows.jsonl").open("a") as handle:
                for row in rows:
                    handle.write(json.dumps(row, default=str, allow_nan=False) + "\n")
            (output / "coverage.json").write_text(json.dumps(coverage, indent=2, default=str))
            print(day, provider, mode, q["statuses"], flush=True)
    con.close()
    models = _model_check(all_rows) if attribution else []
    if attribution:
        (output / "rows.jsonl").write_text(
            "".join(json.dumps(r, default=str, allow_nan=False) + "\n" for r in all_rows)
        )
    result = {
        "contract": contract,
        "coverage": coverage,
        "results": _summary(all_rows),
        "attribution": _attribution_summary(all_rows) if attribution else [],
        "model_check": models,
        "limitations": [
            "conditional complete-case returns; missing labels are not random",
            "L1 package crossing is assumed; simultaneous fills and slippage unobserved",
            "no separate Greek observation timestamps in historical lake; price geometry used",
            "source and received clocks checked; ingestion-time lineage is not reconstructed",
            "IBKR quote_time derives from ticker updates, not independently proven exchange BBO timestamps",
            "historical Schwab REST arrival cannot be reconstructed; only stream observations price that cohort",
            "fixed retrospective hypotheses, not independently validated alpha or live permission",
        ],
    }
    (output / "report.json").write_text(json.dumps(result, default=str, indent=2, allow_nan=False))
    (output / "input-files.json").write_text(json.dumps(inputs, indent=2))
    (output / "manifest.json").write_text(
        json.dumps(
            {
                p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                for p in output.iterdir()
                if p.is_file() and p.name != "manifest.json"
            },
            indent=2,
        )
    )
    return result


def close_model_attribution(data_root, output, start, end):
    """Test the existing production forecast core on strict raw prefixes only.

    Never call the live estimator's latest-state fallback in a history study.
    Isolate its center selection using the same 15-wide butterfly cash contract.
    """
    from spx_spark.application.market_features.physical_close_convergence import (
        _CloseSessionPath,
        _close_online_pool_distribution,
        _close_modal_center,
    )

    if output.exists() and any(output.iterdir()):
        raise ValueError("close-model output must be empty")
    output.mkdir(parents=True, exist_ok=True)
    source = Path(__file__).read_bytes()
    (output / "research-source.py").write_bytes(source)
    model_source = (
        Path(__file__).parents[2]
        / "src/spx_spark/application/market_features/physical_close_convergence.py"
    )
    (output / "forecast-source.py").write_bytes(model_source.read_bytes())
    (output / "contract.json").write_text(
        json.dumps(
            {
                "dates": [str(start), str(end)],
                "input": "raw Schwab SPX, ES and SPXW only; no latest or cards",
                "forecast": "existing production online pool and modal center; 15 prior complete sessions, 45 calendar-day lookback",
                "coverage": "no imputation at data admission; both prefix and training coverage >=95%; fresh key endpoints",
                "causality": "current arrays after 15:00 replaced by NaN before forecast; only earlier sessions train",
                "pricing": "same 15-wide C/P cheapest butterfly, signal 15:00, entry +15s, cross BBO, hold 15:55, four contract fees",
                "scope": "forecast-core and center ablation, not full production width ranking/authority replay",
                "spot_baseline": "SPX at 15:00 rounded to nearest 5; added as exploratory attribution after the initial pool/ATM comparison",
                "authority": "offline research; automatic_ordering=false; fills UNKNOWN",
                "script_sha256": hashlib.sha256(source).hexdigest(),
                "model_sha256": hashlib.sha256(model_source.read_bytes()).hexdigest(),
            },
            indent=2,
        )
    )
    root = data_root / "lake/quotes/schema=v1"
    history = []
    records = []
    inputs = {}
    with duckdb.connect() as con:
        con.execute("SET threads=1; SET memory_limit='768MB'; SET TimeZone='UTC'")
        day = start
        while day <= end:
            session = DEFAULT_MARKET_CALENDAR.session(day)
            if session is None:
                day += timedelta(days=1)
                continue
            files = _files(root, "schwab", session.open_at, session.close_at)
            record = {"day": str(day), "status": "PARTITION_MISSING"}
            if files:
                for file in files:
                    stat = Path(file).stat()
                    inputs[file] = {"size": stat.st_size, "mtime_ns": stat.st_mtime_ns}
                minute_paths = [
                    _underlier_minutes(con, files, i, session.open_at, session.close_at)
                    for i in ("index:SPX", "future:ES")
                ]
                times = [
                    session.open_at.astimezone(UTC) + timedelta(minutes=i)
                    for i in range(
                        1, int((session.close_at - session.open_at).total_seconds() / 60) + 1
                    )
                ]
                arrays = [np.array([p.get(t, np.nan) for t in times]) for p in minute_paths]
                timeline = np.array([int(t.timestamp()) for t in times])
                index = len(timeline) - 61
                coverage = [float(np.isfinite(a).mean()) for a in arrays]
                full = _CloseSessionPath(day, timeline, *arrays, *coverage)
                prior = [p for p in history if day - timedelta(days=45) <= p.session_date < day]
                record.update(
                    status="TRAINING_OR_PREFIX_UNAVAILABLE",
                    training_sessions=len(prior),
                    spx_coverage=coverage[0],
                    es_coverage=coverage[1],
                )
                anchor = None
                if len(prior) >= 15 and all(
                    np.isfinite(a[: index + 1]).mean() >= 0.95 and np.isfinite(a[index])
                    for a in arrays
                ):
                    prefix = [a.copy() for a in arrays]
                    for a in prefix:
                        a[index + 1 :] = np.nan
                    current = _CloseSessionPath(day, timeline, *prefix, *coverage)
                    try:
                        draws, weights = _close_online_pool_distribution(prior, current)
                        q10, q50, q90 = map(float, np.quantile(draws, [0.1, 0.5, 0.9]))
                        anchor, probability = _close_modal_center(
                            draws, q10=q10, median=q50, q90=q90
                        )
                        actual = float(arrays[0][-1]) if np.isfinite(arrays[0][-1]) else None
                        record.update(
                            status="FORECAST_READY",
                            center=anchor,
                            center_probability=probability,
                            q10=q10,
                            q90=q90,
                            actual_close=actual,
                            online_weights=weights,
                            trained_through=str(prior[-1].session_date),
                        )
                    except (ValueError, RuntimeError, np.linalg.LinAlgError) as exc:
                        record.update(status="FORECAST_UNAVAILABLE", reason=type(exc).__name__)
                signal_at = _at(day, 15, 0)
                entry_at = signal_at + timedelta(seconds=15)
                deadline = _at(day, 15, 55)
                books = _snapshots(con, files, day, [signal_at, entry_at, deadline], "schwab", 15)
                spot_center = (
                    float(round(arrays[0][index] / 5) * 5)
                    if np.isfinite(arrays[0][index])
                    else None
                )
                record["spot_center"] = spot_center
                for name, center in (
                    ("atm", None),
                    ("spot", spot_center),
                    ("production_pool_center", anchor),
                ):
                    if name != "atm" and center is None:
                        continue
                    signal = {"family": "butterfly", "signal_at": signal_at, "direction": "NEUTRAL"}
                    row, reason = _structure(
                        signal, books.get(signal_at, {}), age=15, skew=2, anchor_override=center
                    )
                    if row is None:
                        record[name] = {"status": reason}
                        continue
                    legs = [
                        books[entry_at].get((leg["strike"], leg["right"])) for leg in row["legs"]
                    ]
                    quote = _cash_quote(legs, row["quantities"], entry_at, 15, 2)
                    if (
                        quote is None
                        or not _depth(legs, row["quantities"])
                        or not 0 < quote[0] < 15
                    ):
                        record[name] = {"status": "ENTRY_UNAVAILABLE"}
                        continue
                    row.update(signal, legs=legs, entry_at=entry_at, entry_price=quote[0])
                    events = _contract_events(
                        con,
                        files,
                        [leg["instrument_id"] for leg in legs],
                        deadline,
                        deadline + timedelta(seconds=60),
                        "schwab",
                    )
                    row.update(
                        _label(row, events, books[deadline], day=day, mode="rth", age=15, skew=2)
                    )
                    record[name] = row
                if (
                    min(coverage) >= 0.95
                    and all(np.isfinite(a[index]) for a in arrays)
                    and np.isfinite(arrays[0][-1])
                ):
                    history.append(full)
            records.append(record)
            with (output / "rows.jsonl").open("a") as handle:
                handle.write(json.dumps(record, default=str, allow_nan=False) + "\n")
            print(day, record["status"], record.get("training_sessions"), flush=True)
            day += timedelta(days=1)
    pairs = [
        r
        for r in records
        if all(r.get(n, {}).get("pnl_usd") is not None for n in ("atm", "production_pool_center"))
    ]
    forecast = [
        r for r in records if r["status"] == "FORECAST_READY" and r.get("actual_close") is not None
    ]
    three_way = [r for r in pairs if r.get("spot", {}).get("pnl_usd") is not None]
    spot_pairs = [
        r
        for r in records
        if all(r.get(n, {}).get("pnl_usd") is not None for n in ("spot", "production_pool_center"))
    ]
    result = {
        "statuses": dict(Counter(r["status"] for r in records)),
        "three_way_complete": len(three_way),
        "three_way_means": {
            n: float(np.mean([r[n]["pnl_usd"] for r in three_way])) if three_way else None
            for n in ("atm", "spot", "production_pool_center")
        },
        "pool_vs_spot_complete": len(spot_pairs),
        "pool_vs_spot_means": {
            n: float(np.mean([r[n]["pnl_usd"] for r in spot_pairs])) if spot_pairs else None
            for n in ("spot", "production_pool_center")
        },
        "forecast_center_equals_spot": sum(r["center"] == r.get("spot_center") for r in forecast),
        "forecast_resolved": len(forecast),
        "q10_q90_coverage": float(
            np.mean([r["q10"] <= r["actual_close"] <= r["q90"] for r in forecast])
        )
        if forecast
        else None,
        "paired_complete": len(pairs),
        "paired_means": {
            n: float(np.mean([r[n]["pnl_usd"] for r in pairs])) if pairs else None
            for n in ("atm", "production_pool_center")
        },
        "limitations": [
            "retrospective, not unseen validation",
            "strict raw loader differs from live fallback",
            "center ablation only; production width and price gates not replayed",
        ],
    }
    (output / "report.json").write_text(json.dumps(result, indent=2))
    (output / "input-files.json").write_text(json.dumps(inputs, indent=2))
    (output / "manifest.json").write_text(
        json.dumps(
            {
                p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                for p in output.iterdir()
                if p.is_file()
            },
            indent=2,
        )
    )
    return result


def _context_minutes(con, files, start, end):
    """Observable market context, not an executable quote or a trading permission.

    Schwab context instruments historically have NULL market_data_type. Preserve
    that uncertainty; require live quality and a causal, <=15s source regardless.
    Select the latest arrival before validating, so invalidations cannot vanish.
    Historical REST received_at can be request-start; its recorded clock is not
    proof of exact response availability. These context fields cannot authorize.
    """
    _read_quotes(con, files)
    rows = con.execute("""
        WITH ticks AS (
          SELECT date_trunc('minute',received_at)+INTERVAL 1 MINUTE AS bucket_at,
            instrument_id,received_at,quality,market_data_type,
            CASE WHEN bid>0 AND ask>=bid THEN (bid+ask)/2 ELSE coalesce(last,effective_price) END AS price,
            CASE WHEN bid>0 AND ask>=bid THEN quote_time ELSE coalesce(trade_time,quote_time) END AS source
          FROM broker_quotes WHERE instrument_id IN (
            'index:SPX','future:ES','index:VIX','index:VIX1D','equity:SPY',
            'equity:RSP','equity:HYG','equity:LQD','equity:TLT','equity:UUP','equity:USO')
            AND received_at>=? AND received_at<?
        ) SELECT bucket_at,instrument_id,arg_max(struct_pack(price:=price,source:=source,
            received_at:=received_at,quality:=quality,mode:=market_data_type),received_at) AS tick
          FROM ticks GROUP BY bucket_at,instrument_id ORDER BY bucket_at,instrument_id
    """, [start, end]).fetchall()
    paths = defaultdict(dict)
    modes = Counter()
    for at, instrument, tick in rows:
        if (tick['source'] is None or tick['source'] > tick['received_at']
                or not 0 <= (at-tick['source']).total_seconds() <= 15
                or tick['quality'] != 'live' or not _finite(tick['price'])
                or tick['price'] <= 0
                or str(tick['mode']).lower() not in {'none', 'live', '1'}):
            continue
        paths[instrument][at] = float(tick['price'])
        modes[f"{instrument}:{tick['mode']}"] += 1
    return paths, dict(modes)


def _path_context(path, at, minutes):
    """Endpoint change needs two quotes; range and RV need the complete path."""
    earlier = at-timedelta(minutes=minutes)
    if at not in path or earlier not in path:
        return None
    values = _window(path, at, minutes + 1)
    result = {'net': path[at]-path[earlier], 'range': None, 'efficiency': None,
              'rv_points': None, 'complete_path': bool(values)}
    if not values:
        return result
    changes = np.diff(values)
    gross = float(np.abs(changes).sum())
    return {
        **result, 'range': max(values)-min(values),
        'efficiency': abs(values[-1]-values[0])/gross if gross else 0.0,
        'rv_points': float(np.sqrt(np.sum(changes**2))),
    }


def environment_attribution(data_root, output, raw_replay_rows):
    """Explain an audited raw replay cohort; never read decisions or push records.

    Outcome rows identify frozen legs/times only. Reload contemporaneous context
    and exit BBO from the raw lake. Missing outcomes remain in the denominator.
    This is descriptive attribution of existing hypotheses, not a new backtest.
    """
    output.mkdir(parents=True, exist_ok=False)
    source = Path(raw_replay_rows)
    rows = [json.loads(line) for line in source.read_text().splitlines()]
    rows = [r for r in rows if r['provider'] == 'schwab' and r['mode'] == 'rth']
    contract = {
        'scope': 'Schwab RTH baseline cohorts; no production policy replication claim',
        'outcome_source': str(source), 'outcome_sha256': hashlib.sha256(source.read_bytes()).hexdigest(),
        'context': 'latest arrival; source<=received<=signal; age<=15s; no forward fill',
        'unknown_mode': 'NULL allowed for descriptive context with live quality; not authorization',
        'arrival_limit': 'historical REST received_at may be request-start; source/recorded receipt checks do not prove exact response availability',
        'features': '5/15/60m paths, source-clocked VIX and ETF proxies, frozen-strike straddle',
        'post_exit': 'ex-post path and liquidation accounting only; never an entry filter',
        'macro': 'separate verified event annotation; missing event is not a normal-day certificate',
    }
    (output/'contract.json').write_text(json.dumps(contract, indent=2))
    (output/'research-source.py').write_text(Path(__file__).read_text())
    con = duckdb.connect(config={'threads': 2, 'memory_limit': '768MB'})
    all_files, contexts, enriched = set(), [], []
    for day_text in sorted({r['session_date'] for r in rows}):
        day = date.fromisoformat(day_text)
        group = [r for r in rows if r['session_date'] == day_text]
        start, end = _at(day, 9, 30), _at(day, 16, 1)
        files = _files(data_root / 'lake/quotes/schema=v1', 'schwab', start, end)
        if not files:
            enriched.extend({**r, 'environment_status': 'RAW_PARTITION_MISSING'} for r in group)
            continue
        all_files.update(files)
        paths, modes = _context_minutes(con, files, start, end)
        contexts.append({'session_date': day_text, 'modes': modes, 'paths': {
            instrument: {at.isoformat(): price for at, price in path.items()}
            for instrument, path in paths.items()}})
        times = set()
        for r in group:
            if r.get('entry_at'):
                at = datetime.fromisoformat(r['signal_at'])
                times.update([at, at-timedelta(minutes=15)])
                if r.get('exit_at'):
                    times.add(datetime.fromisoformat(r['exit_at']))
        books = _snapshots(con, files, day, sorted(times), 'schwab', 15)
        for r in group:
            row = dict(r)
            if not r.get('entry_at'):
                row['environment_status'] = 'NO_ENTRY'
                enriched.append(row)
                continue
            at = datetime.fromisoformat(r['signal_at'])
            spx = paths.get('index:SPX', {})
            e = {'as_of': at.isoformat(), 'spx': spx.get(at), 'paths': {}, 'levels': {},
                 'entry_cost_fraction': r['entry_price']/r['width'],
                 'straddle_over_width': r['atm_straddle_points']/r['width']}
            for instrument, path in paths.items():
                e['levels'][instrument] = path.get(at)
                e['paths'][instrument] = {str(m): _path_context(path, at, m) for m in (5, 15, 60)}
            for instrument in ('index:SPX', 'future:ES'):
                path = paths.get(instrument, {})
                prefix = [path[t] for t in sorted(path) if start < t <= at]
                expected = int((at-start).total_seconds()/60)
                complete = len(prefix) == expected and expected > 0
                e[instrument+'_rth_prefix'] = {
                    'observed_minutes': len(prefix), 'expected_minutes': expected,
                    'net': prefix[-1]-prefix[0] if complete else None,
                    'range': max(prefix)-min(prefix) if complete else None,
                    'location': ((prefix[-1]-min(prefix))/(max(prefix)-min(prefix))
                                 if complete and max(prefix)>min(prefix) else None),
                }
            straddles = []
            for t in (at-timedelta(minutes=15), at):
                legs = [books[t].get((r['anchor'], side)) for side in ('C', 'P')]
                quote = _cash_quote(legs, (1, 1), t, 15, 2) if all(legs) else None
                straddles.append(sum(quote)/2 if quote else None)
            e['frozen_strike_straddle_15m'] = straddles
            e['frozen_strike_straddle_change_fraction'] = (
                straddles[1]/straddles[0]-1 if all(straddles) else None)
            if r['family'] == 'condor' and e['spx'] is not None:
                shorts = [leg for leg, q in zip(r['legs'], r['quantities']) if q < 0]
                e['short_buffer_points'] = min(abs(leg['strike']-e['spx']) for leg in shorts)
                e['short_delta_recorded'] = [leg['delta'] for leg in shorts]
                e['delta_clock_status'] = 'INDEPENDENT_GREEKS_TIMESTAMP_UNAVAILABLE'
            post = {}
            for minutes in (20, 60):
                values = _window(spx, at+timedelta(minutes=minutes), minutes+1)
                sign = 1 if r['direction'] == 'UP' else -1
                post[str(minutes)] = ({
                    'spx_move': values[-1]-values[0],
                    'range': max(values)-min(values),
                    'directional_mfe': max((v-values[0])*sign for v in values),
                    'directional_mae': min((v-values[0])*sign for v in values),
                } if values else None)
            row['post_signal_path'] = post
            if r.get('exit_at'):
                exit_at = datetime.fromisoformat(r['exit_at'])
                legs = [books[exit_at].get((leg['strike'], leg['right'])) for leg in r['legs']]
                quote = _cash_quote(legs, r['quantities'], exit_at, 15, 2) if all(legs) else None
                if quote:
                    signed = quote[1]
                    expected = -r['cash_exit_points'] if r['family']=='condor' else r['cash_exit_points']
                    row['exit_quote_audit'] = {
                        'signed_liquidation': signed, 'matches_replay': abs(signed-expected)<1e-8,
                        'mid_signed_value': sum(quote)/2,
                        'half_spread_usd': (quote[0]-quote[1])*50,
                        'negative_debit_liquidation': r['family'] != 'condor' and signed < 0,
                        'legs': legs,
                    }
            row.update(environment_status='OBSERVED_WITH_MISSING_FIELDS', environment=e)
            enriched.append(row)
        print(json.dumps({'day': day_text, 'rows': len(group), 'context_instruments': len(paths)}), flush=True)
    (output/'rows.jsonl').write_text(''.join(json.dumps(r, default=str)+'\n' for r in enriched))
    (output/'market-paths.jsonl').write_text(''.join(json.dumps(r)+'\n' for r in contexts))
    (output/'input-files.json').write_text(json.dumps(sorted(all_files), indent=2))
    con.close()
    return enriched


def _first_directional_range_signal(day, path, direction):
    """First acceptance in this direction, including after an opposite trigger.

    A missing earlier minute makes the first-trigger contract unobservable;
    later prices cannot certify that no earlier signal occurred in the gap.
    """
    signal = {'direction': direction, 'family': 'vertical'}
    opening = _at(day, 9, 30)
    range_end = opening + timedelta(minutes=15)
    initial = _window(path, range_end, 15)
    if not initial:
        return {**signal, 'status': 'UNDERLIER_GAP'}
    high, low = max(initial), min(initial)
    at = range_end + timedelta(minutes=1)
    while at <= _at(day, 13, 30):
        if at not in path:
            return {**signal, 'status': 'UNDERLIER_GAP'}
        if at >= range_end+timedelta(minutes=3):
            recent = _window(path, at, 3)
            accepted = min(recent)>high+1 if direction=='UP' else max(recent)<low-1
            if accepted:
                return {**signal, 'signal_at': at, 'opening_range_high': high,
                        'opening_range_low': low}
        at += timedelta(minutes=1)
    return {**signal, 'status': 'NO_TRIGGER'}


def validate_directional_signal(data_root, output, start, end, providers, *, entry_delay_seconds=15):
    """Frozen full directional policy, not filtering old mixed-direction winners."""
    output.mkdir(parents=True, exist_ok=False)
    source = Path(__file__).read_bytes()
    contract = {
        'name': 'raw_or15_directional_20m',
        'primary': 'first UP acceptance each day, 20m management',
        'controls': 'first DOWN; 10m and 30m horizons; no choosing the best variant',
        'signal': 'first 15 RTH minute closes; 3 closes beyond high/low by >1 point; cutoff 13:30 ET',
        'earlier_opposite_signal': 'does not consume this direction opportunity',
        'missing_prefix': 'cannot establish first trigger, no later reconstruction',
        'structure': '15 point Call/Put debit vertical, ATM minimum straddle anchor at signal',
        'entry': 'legs frozen; cross BBO; debit/width<=0.45 at signal and entry',
        'entry_delay_seconds': entry_delay_seconds,
        'quotes': 'source age<=15s; leg skew<=2s; displayed depth covers quantities; Schwab stream isolated',
        'management': '50% premium stop and existing trail; scheduled 10/20/30m current book; hard close 15:45',
        'clock': 'source-clock validation, receipt<=action, no mid fills or future book search',
        'unknowns': 'missing legs, gaps>60s and incomplete exits retained; no assumed fills',
        'calendar': 'no historical macro filter claimed; not full production authorization',
        'evaluation': 'all raw sessions; chronological month blocks; window previously explored, not untouched holdout',
        'automatic_ordering': False, 'bark_access': False,
        'script_sha256': hashlib.sha256(source).hexdigest(),
    }
    (output/'contract.json').write_text(json.dumps(contract, indent=2))
    (output/'research-source.py').write_bytes(source)
    con = duckdb.connect(config={'threads': 2, 'memory_limit': '768MB'})
    con.execute("SET TimeZone='UTC'")
    all_rows, inputs = [], set()
    day = start
    while day <= end:
        if DEFAULT_MARKET_CALENDAR.session(day) is None:
            day += timedelta(days=1)
            continue
        for provider in providers:
            files = _files(data_root/'lake/quotes/schema=v1', provider,
                           _at(day, 9, 30), _at(day, 14, 1))
            inputs.update(files)
            path = (_underlier_minutes(con, files, 'index:SPX', _at(day, 9, 30), _at(day, 13, 31))
                    if files else {})
            signals = [_first_directional_range_signal(day, path, d) for d in ('UP', 'DOWN')]
            times = sorted({t for s in signals if s.get('signal_at') for t in (
                s['signal_at'], s['signal_at']+timedelta(seconds=entry_delay_seconds),
                *(s['signal_at']+timedelta(seconds=entry_delay_seconds, minutes=m) for m in (10, 20, 30)))})
            books = _snapshots(con, files, day, times, provider, 15) if times else {}
            entries = []
            for signal in signals:
                row = {**signal, 'provider': provider, 'mode': 'rth',
                       'session_date': str(day), 'fill_status': 'UNKNOWN'}
                if not files:
                    row['status'] = 'PARTITION_MISSING'
                if 'status' not in row:
                    structure, reason = _structure(signal, books[signal['signal_at']], age=15, skew=2)
                    if reason:
                        row['status'] = reason
                    else:
                        row.update(structure)
                        if row['signal_package_price']/row['width']>0.45:
                            row['status'] = 'SIGNAL_DEBIT_CAP'
                        else:
                            at = signal['signal_at']+timedelta(seconds=entry_delay_seconds)
                            legs = [books[at].get((leg['strike'], leg['right'])) for leg in row['legs']]
                            quote = _cash_quote(legs, row['quantities'], at, 15, 2)
                            if quote is None:
                                row['status'] = 'ENTRY_BBO_UNAVAILABLE'
                            elif not _depth(legs, row['quantities']):
                                row['status'] = 'ENTRY_DEPTH_UNAVAILABLE'
                            elif not 0<quote[0]/row['width']<=0.45:
                                row['status'] = 'ENTRY_DEBIT_CAP'
                            else:
                                row.update(legs=legs, entry_at=at, entry_price=quote[0], status='ENTERED')
                entries.append(row)
            entered = [r for r in entries if r['status']=='ENTERED']
            events = (_contract_events(con, files,
                      sorted({leg['instrument_id'] for r in entered for leg in r['legs']}),
                      min(r['entry_at'] for r in entered),
                      max(r['entry_at'] for r in entered)+timedelta(minutes=31), provider)
                      if entered else {})
            for entry in entries:
                marks = (_package_path(entry, events, entry['entry_at']+timedelta(minutes=31), 15, 2)
                         if entry['status']=='ENTERED' else [])
                for minutes in (10, 20, 30):
                    row = {**entry, 'setup': f"or15_{entry['direction'].lower()}_{minutes}m"}
                    if entry['status']=='ENTERED':
                        policy = replace(DEFAULT_MANAGEMENT_POLICY, time_stop_minutes=minutes,
                                         policy_version=f'research.or15.{minutes}m')
                        deadline = entry['entry_at']+timedelta(minutes=minutes)
                        legs = [books[deadline].get((leg['strike'], leg['right'])) for leg in row['legs']]
                        quote = _cash_quote(legs, row['quantities'], deadline, 15, 2)
                        timed_marks = marks
                        if quote is not None and _depth(legs, row['quantities'], liquidate=True):
                            timed_marks = sorted([m for m in marks if m.at!=deadline]
                                                 +[PolicyMark(deadline, quote[1])], key=lambda m:m.at)
                        label = simulate_management_policy(timed_marks, entry_ask=row['entry_price'],
                                    leg_count=2, entry_at=row['entry_at'], policy=policy,
                                    session_date=day, max_quote_gap_seconds=60)
                        row.update(asdict(label))
                        row['status'] = ('COMPLETE_EXIT' if label.policy_pnl_points is not None else
                                         'QUOTE_GAP' if label.exit_reason=='quote_gap' else 'CENSORED')
                        row['pnl_usd'] = 100*label.policy_pnl_points if label.policy_pnl_points is not None else None
                    all_rows.append(row)
                    with (output/'rows.jsonl').open('a') as handle:
                        handle.write(json.dumps(row, default=str)+'\n')
            print(json.dumps({'day': str(day), 'provider': provider,
                              'entries': len(entered), 'observed_minutes': len(path)}), flush=True)
        day += timedelta(days=1)
    summaries = _summary(all_rows)
    for summary in summaries:
        group = [r for r in all_rows if (r['provider'], r['setup']) ==
                 (summary['provider'], summary['setup'])]
        incomplete = [r for r in group if 'entry_at' in r and r.get('pnl_usd') is None]
        known = [r['pnl_usd'] for r in group if r.get('pnl_usd') is not None]
        summary['entered'] = sum('entry_at' in r for r in group)
        summary['incomplete_entries'] = len(incomplete)
        summary['missing_full_premium_loss_stress_total_usd'] = (
            sum(known)-sum(100*r['entry_price']+5.28 for r in incomplete))
        summary['stress_scope'] = 'scenario only, not a certified bound on forced liquidation costs'
    (output/'summary.json').write_text(json.dumps(summaries, indent=2))
    (output/'input-files.json').write_text(json.dumps(sorted(inputs), indent=2))
    con.close()
    return summaries


def _quote_implied_greeks(leg, forward, tau, at):
    """Fresh BBO-implied Black delta; never claim a broker Greeks timestamp."""
    if _cash_quote([leg], [1], at, 15, 2) is None or tau <= 0:
        return None
    strike, right = float(leg['strike']), leg['right']
    price = (leg['bid']+leg['ask'])/2
    try:
        iv = brentq(lambda vol: bs_price(forward, strike, vol, tau, right)-price,
                    0.00001, 5.0, xtol=1e-10)
    except ValueError:
        return None
    return {'iv': iv, 'delta': bs_delta(forward, strike, iv, tau, right)}


def _quote_forward(chain, at):
    pairs = []
    for strike, right in chain:
        if right != 'C':
            continue
        legs = [chain.get((strike, 'C')), chain.get((strike, 'P'))]
        if _cash_quote(legs, [1, 1], at, 15, 2) is None:
            continue
        call, put = [(leg['bid']+leg['ask'])/2 for leg in legs]
        pairs.append((call+put, strike, strike+call-put))
    return min(pairs) if pairs else None


def _delta_condor(chain, at, delta_target, width, delta_basis):
    anchor = _quote_forward(chain, at)
    if anchor is None:
        return None, 'ANCHOR_BBO_UNAVAILABLE'
    straddle, strike, forward = anchor
    tau = (_at(at.astimezone(ET).date(), 16, 0)-at).total_seconds()/(365*86400)
    shorts, actual = [], []
    for right in ('P', 'C'):
        choices = []
        for key, leg in chain.items():
            if key[1] != right or (key[0] >= forward if right == 'P' else key[0] <= forward):
                continue
            if _cash_quote([leg], [1], at, 15, 2) is None:
                continue
            greek = _quote_implied_greeks(leg, forward, tau, at) if delta_basis == 'bbo_implied' else None
            delta = greek['delta'] if greek is not None else leg.get('delta') if delta_basis == 'broker_recorded' else None
            if (not _finite(delta) or not 0 < abs(delta) <= delta_target+1e-10
                    or (delta > 0) != (right == 'C')):
                continue
            choices.append((-abs(delta), key[0], leg))
        if not choices:
            return None, 'SHORT_DELTA_UNAVAILABLE'
        neg_delta, _, short = min(choices, key=lambda value: value[:2])
        shorts.append(short)
        actual.append(-neg_delta)
    put, call = shorts
    legs = [chain.get((put['strike']-width, 'P')), put, call,
            chain.get((call['strike']+width, 'C'))]
    quantities = [1, -1, -1, 1]
    quote = _cash_quote(legs, quantities, at, 15, 2)
    if quote is None:
        return None, 'EXACT_LEGS_INVALID'
    credit = -quote[0]
    if not 0 < credit < width:
        return None, 'ENTRY_GEOMETRY_INVALID'
    return dict(family='condor', legs=legs, quantities=quantities, width=width,
                anchor=strike, forward=forward, atm_straddle_points=straddle,
                selected_abs_deltas=actual, signal_package_price=credit), None


def _volatility_at_entry(chain, prior_chain, at, spx):
    """Causal fixed-strike IV change and remaining variance / trailing RV proxy."""
    anchor = _quote_forward(chain, at)
    if anchor is None:
        return {'status': 'VOLATILITY_UNAVAILABLE'}
    straddle, strike, forward = anchor
    day, before = at.astimezone(ET).date(), at-timedelta(minutes=15)
    tau = (_at(day, 16, 0)-at).total_seconds()/(365*86400)
    current = [_quote_implied_greeks(chain.get((strike, right)), forward, tau, at) for right in ('C', 'P')]
    prior_pair = [prior_chain.get((strike, right)) for right in ('C', 'P')]
    prior = None
    if _cash_quote(prior_pair, [1, 1], before, 15, 2) is not None:
        prices = [(leg['bid']+leg['ask'])/2 for leg in prior_pair]
        old_forward = strike+prices[0]-prices[1]
        old_tau = (_at(day, 16, 0)-before).total_seconds()/(365*86400)
        old_iv = [_quote_implied_greeks(leg, old_forward, old_tau, before) for leg in prior_pair]
        if all(old_iv):
            prior = {'straddle': sum(prices), 'iv': float(np.mean([g['iv'] for g in old_iv]))}
    iv = float(np.mean([g['iv'] for g in current])) if all(current) else None
    # Six five-minute closes cover 25 minutes and reduce one-minute bounce.
    times = [at-timedelta(minutes=i) for i in (25, 20, 15, 10, 5, 0)]
    closes = [spx[t] for t in times] if all(t in spx for t in times) else []
    remaining = (_at(day, 16, 0)-at).total_seconds()/60
    rv_sd = float(np.sqrt(np.sum(np.diff(closes)**2)*remaining/25)) if closes else None
    implied_sd = forward*iv*math.sqrt(tau) if iv is not None else None
    context = _path_context(spx, at, 15)
    ratio = implied_sd/rv_sd if implied_sd is not None and rv_sd is not None and rv_sd>0 else None
    return {'status': 'AVAILABLE' if iv is not None else 'IV_UNAVAILABLE',
            'atm_iv': iv, 'fixed_strike': strike, 'fixed_strike_prior_iv': prior['iv'] if prior else None,
            'fixed_strike_straddle_change': straddle/prior['straddle']-1 if prior else None,
            'implied_remaining_sd': implied_sd, 'rv25_projected_remaining_sd': rv_sd,
            'implied_to_trailing_rv_ratio': ratio, 'spx15': context,
            'filters': {
                'implied_gt_trailing_rv': ratio>1 if ratio is not None else None,
                'iv_contracting': iv<prior['iv'] if iv is not None and prior else None,
                'balanced_path': context['efficiency']<=0.35 if context and context['efficiency'] is not None else None,
            }}


def _managed_research_exit(row, marks, exit_chain, deadline, policy, *, pure_hold=False):
    legs = [exit_chain.get((leg['strike'], leg['right'])) for leg in row['legs']]
    quote = _cash_quote(legs, row['quantities'], deadline, 15, 2)
    timed = [m for m in marks if m.at <= deadline]
    if quote is not None and _depth(legs, row['quantities'], liquidate=True):
        value = quote[1]+(2*row['entry_price'] if row['family']=='condor' else 0)
        timed = [m for m in timed if m.at != deadline]+[PolicyMark(deadline, value)]
    if pure_hold:
        timed = [m for m in timed if m.at == deadline]
    label = simulate_management_policy(timed, entry_ask=row['entry_price'],
                entry_at=row['entry_at'], leg_count=sum(abs(q) for q in row['quantities']),
                policy=policy, session_date=deadline.astimezone(ET).date(),
                max_quote_gap_seconds=None if pure_hold else 60)
    pnl = label.policy_pnl_points
    return {**asdict(label), 'status': 'COMPLETE_EXIT' if pnl is not None else
            'QUOTE_GAP' if label.exit_reason=='quote_gap' else 'CENSORED',
            'pnl_usd': 100*pnl if pnl is not None else None, 'management': asdict(policy)}


def scan_condor_volatility(data_root, output, start, end, providers):
    output.mkdir(parents=True, exist_ok=False)
    source = Path(__file__).read_bytes()
    (output/'research-source.py').write_bytes(source)
    contract = {'scope': 'raw RTH 10:00 first fixed attempt; not production first-eligible policy',
        'delta_targets': list(range(10, 21)), 'widths': [5, 10, 15, 20],
        'delta_bases': ['broker_recorded', 'bbo_implied'],
        'broker_greeks_clock': 'independent timestamp absent; not certified fresh Delta',
        'bbo_model': 'r=q=0 project Black kernels; forward from fresh ATM call-put parity; midpoint only for Greeks',
        'entry': 'frozen legs at 10:00; cross BBO at 10:00:15; 0<credit<width; no minimum credit optimized',
        'quotes': 'age<=15s; skew<=2s; received<=action; displayed size sufficient',
        'management': asdict(RTH_IRON_CONDOR_MANAGEMENT_POLICY),
        'vol_filters': 'IV remaining SD > trailing 25m 5m-step RV projected to close; fixed-strike IV contracts over 15m; 15m path efficiency<=0.35',
        'evaluation': 'all 45-day denominators; missing retained; calendar blocks; overlapping structures never independent sessions',
        'fill_status': 'UNKNOWN', 'bark_access': False, 'automatic_ordering': False,
        'script_sha256': hashlib.sha256(source).hexdigest()}
    (output/'contract.json').write_text(json.dumps(contract, indent=2))
    con = duckdb.connect(config={'threads': 2, 'memory_limit': '768MB'})
    con.execute("SET TimeZone='UTC'")
    results, inputs = [], set()
    day = start
    while day <= end:
        if DEFAULT_MARKET_CALENDAR.session(day) is None:
            day += timedelta(days=1)
            continue
        at, entry_at, deadline = _at(day, 10, 0), _at(day, 10, 0)+timedelta(seconds=15), _at(day, 15, 45)
        for provider in providers:
            files = _files(data_root/'lake/quotes/schema=v1', provider, _at(day, 9, 30), deadline)
            inputs.update(files)
            times = [at-timedelta(minutes=15), at, entry_at, deadline]
            books = _snapshots(con, files, day, times, provider, 15) if files else {t:{} for t in times}
            spx = _underlier_minutes(con, files, 'index:SPX', _at(day, 9, 30), at) if files else {}
            vol = _volatility_at_entry(books[at], books[times[0]], at, spx)
            entries = []
            for basis in ('broker_recorded', 'bbo_implied'):
                for delta in range(10, 21):
                    for width in (5, 10, 15, 20):
                        row = dict(provider=provider, mode='rth', session_date=str(day),
                            setup=f'ic_{basis}_d{delta}_w{width}', delta_basis=basis,
                            target_delta=delta/100, width=width, signal_at=at,
                            volatility=vol, fill_status='UNKNOWN')
                        structure, reason = _delta_condor(books[at], at, delta/100, width, basis) if files else (None, 'PARTITION_MISSING')
                        if reason:
                            row['status'] = reason
                        else:
                            row.update(structure)
                            legs = [books[entry_at].get((leg['strike'], leg['right'])) for leg in row['legs']]
                            quote = _cash_quote(legs, row['quantities'], entry_at, 15, 2)
                            if quote is None:
                                row['status'] = 'ENTRY_BBO_UNAVAILABLE'
                            elif not _depth(legs, row['quantities']):
                                row['status'] = 'ENTRY_DEPTH_UNAVAILABLE'
                            elif not 0 < -quote[0] < width:
                                row['status'] = 'ENTRY_GEOMETRY_INVALID'
                            else:
                                credit = -quote[0]
                                row.update(legs=legs, entry_at=entry_at, entry_price=credit, status='ENTERED',
                                    policy_stop_reachable_inside_width=3*credit<=width,
                                    prior_credit_band_pass=0.25<=credit/width<=0.55,
                                    defined_risk_usd=100*(width-credit),
                                    short_put=legs[1]['strike'], short_call=legs[2]['strike'])
                        entries.append(row)
            entered = [r for r in entries if r['status']=='ENTERED']
            events = _contract_events(con, files,
                sorted({leg['instrument_id'] for r in entered for leg in r['legs']}),
                entry_at, deadline, provider) if entered else {}
            cache = {}
            for row in entries:
                if row['status']=='ENTERED':
                    key = tuple(leg['instrument_id'] for leg in row['legs'])
                    if key not in cache:
                        marks = _package_path(row, events, deadline, 15, 2)
                        cache[key] = _managed_research_exit(row, marks, books[deadline], deadline,
                                                          RTH_IRON_CONDOR_MANAGEMENT_POLICY)
                    row.update(cache[key])
                results.append(row)
                with (output/'rows.jsonl').open('a') as handle:
                    handle.write(json.dumps(row, default=str)+'\n')
            print(json.dumps({'day':str(day), 'provider':provider, 'entered':len(entered),
                              'unique_structures':len(cache)}), flush=True)
        day += timedelta(days=1)
    (output/'summary.json').write_text(json.dumps(_summary(results), indent=2))
    (output/'input-files.json').write_text(json.dumps(sorted(inputs), indent=2))
    con.close()


def relax_exit_contracts(data_root, output, start, end, providers, close_centers=None):
    """Rebuild signals from raw, then pair management changes on frozen legs."""
    output.mkdir(parents=True, exist_ok=False)
    source = Path(__file__).read_bytes()
    (output/'research-source.py').write_bytes(source)
    centers = {}
    if close_centers is not None:
        for line in close_centers.read_text().splitlines():
            row = json.loads(line)
            if (row.get('status') == 'FORECAST_READY' and row.get('center') is not None
                    and row.get('trained_through', '9999') < row['day']):
                centers[row['day']] = {'center': row['center'], 'trained_through': row['trained_through']}
    contract = {'scope': 'raw first UP signal and 15:00 butterflies; paired exit ablation',
        'vertical': '20m baseline versus no elapsed-time stop; existing 50% premium stop/trail retained',
        'butterfly': 'ATM and causal raw production-pool center; managed exit versus hold controls',
        'endpoints_et': ['15:45', '15:55', '15:59'],
        'endpoint_scope': 'last-minute quote liquidation sensitivities, not official expiry settlement',
        'geometry': '15-wide; signal and entry debit/width<=0.45; one first attempt',
        'entry': 'signal+15sec frozen legs, cross fresh BBO with depth',
        'centers': str(close_centers) if close_centers else None,
        'centers_sha256': hashlib.sha256(close_centers.read_bytes()).hexdigest() if close_centers else None,
        'center_scope': 'previously audited raw causal forecast only; no cards or outcome-selected dates',
        'automatic_ordering': False, 'bark_access': False, 'fill_status': 'UNKNOWN',
        'script_sha256': hashlib.sha256(source).hexdigest()}
    (output/'contract.json').write_text(json.dumps(contract, indent=2))
    con = duckdb.connect(config={'threads':2, 'memory_limit':'768MB'})
    con.execute("SET TimeZone='UTC'")
    results, inputs = [], set()
    day = start
    while day <= end:
        if DEFAULT_MARKET_CALENDAR.session(day) is None:
            day += timedelta(days=1)
            continue
        deadlines = [_at(day, 15, minute) for minute in (45, 55, 59)]
        for provider in providers:
            files = _files(data_root/'lake/quotes/schema=v1', provider, _at(day, 9, 30), deadlines[-1])
            inputs.update(files)
            spx = _underlier_minutes(con, files, 'index:SPX', _at(day, 9, 30), _at(day, 13, 31)) if files else {}
            signal = _first_directional_range_signal(day, spx, 'UP')
            signal['signal_name'] = 'or15_up'
            signals = [signal]
            if provider == 'schwab':
                for name in ('atm', 'production_pool'):
                    item = {'family':'butterfly', 'signal_at':_at(day, 15, 0),
                            'direction':'NEUTRAL', 'signal_name':f'butterfly_{name}'}
                    if name == 'production_pool':
                        if str(day) not in centers:
                            item['status'] = 'CAUSAL_FORECAST_UNAVAILABLE'
                        else:
                            item.update(center_override=centers[str(day)]['center'],
                                        trained_through=centers[str(day)]['trained_through'])
                    signals.append(item)
            times = sorted(set(deadlines+[time for s in signals if s.get('signal_at') for time in
                         (s['signal_at'], s['signal_at']+timedelta(seconds=15),
                          s['signal_at']+timedelta(minutes=20, seconds=15))]))
            books = _snapshots(con, files, day, times, provider, 15) if files else {t:{} for t in times}
            entries = []
            for signal in signals:
                row = {**signal, 'provider':provider, 'mode':'rth', 'session_date':str(day), 'fill_status':'UNKNOWN'}
                if not files:
                    row['status'] = 'PARTITION_MISSING'
                if 'status' not in row:
                    structure, reason = _structure(signal, books[signal['signal_at']], age=15, skew=2,
                                                   anchor_override=signal.get('center_override'))
                    if reason:
                        row['status'] = reason
                    else:
                        row.update(structure)
                        at = signal['signal_at']+timedelta(seconds=15)
                        legs = [books[at].get((leg['strike'], leg['right'])) for leg in row['legs']]
                        quote = _cash_quote(legs, row['quantities'], at, 15, 2)
                        if row['signal_package_price']/row['width'] > 0.45:
                            row['status'] = 'SIGNAL_DEBIT_CAP'
                        elif quote is None:
                            row['status'] = 'ENTRY_BBO_UNAVAILABLE'
                        elif not _depth(legs, row['quantities']):
                            row['status'] = 'ENTRY_DEPTH_UNAVAILABLE'
                        elif not 0 < quote[0]/row['width'] <= 0.45:
                            row['status'] = 'ENTRY_DEBIT_CAP'
                        else:
                            row.update(legs=legs, entry_at=at, entry_price=quote[0], status='ENTERED')
                entries.append(row)
            entered = [r for r in entries if r['status']=='ENTERED']
            events = _contract_events(con, files, sorted({leg['instrument_id'] for r in entered for leg in r['legs']}),
                                     min(r['entry_at'] for r in entered), deadlines[-1], provider) if entered else {}
            for entry in entries:
                marks = _package_path(entry, events, deadlines[-1], 15, 2) if entry['status']=='ENTERED' else []
                if entry['family']=='vertical':
                    controls = [('20m_baseline', deadlines[0], 20, False),
                                ('managed_1545', deadlines[0], None, False),
                                ('managed_1559', deadlines[2], None, False)]
                else:
                    controls = [(f'managed_{d.astimezone(ET):%H%M}', d, None, False) for d in deadlines]
                    controls += [('hold_1555_baseline', deadlines[1], None, True),
                                 ('hold_1559', deadlines[2], None, True)]
                for name, deadline, minutes, pure_hold in controls:
                    row = {**entry, 'setup':entry['signal_name']+'_'+name}
                    if entry['status']=='ENTERED':
                        policy = replace(CLOSE_CONVERGENCE_BUTTERFLY_MANAGEMENT_POLICY if pure_hold else DEFAULT_MANAGEMENT_POLICY,
                                         time_stop_minutes=minutes, hard_exit_et=deadline.astimezone(ET).strftime('%H:%M'),
                                         policy_version='research.relaxed_exit.'+name)
                        if minutes:
                            timer = entry['entry_at']+timedelta(minutes=minutes)
                            legs = [books[timer].get((leg['strike'], leg['right'])) for leg in row['legs']]
                            quote = _cash_quote(legs, row['quantities'], timer, 15, 2)
                            timer_marks = [m for m in marks if m.at!=timer]
                            if quote is not None and _depth(legs, row['quantities'], liquidate=True):
                                timer_marks.append(PolicyMark(timer, quote[1]))
                        else:
                            timer_marks = marks
                        row.update(_managed_research_exit(row, timer_marks, books[deadline], deadline, policy, pure_hold=pure_hold))
                    results.append(row)
                    with (output/'rows.jsonl').open('a') as handle:
                        handle.write(json.dumps(row, default=str)+'\n')
            print(json.dumps({'day':str(day), 'provider':provider, 'entered':len(entered)}), flush=True)
        day += timedelta(days=1)
    (output/'summary.json').write_text(json.dumps(_summary(results), indent=2))
    (output/'input-files.json').write_text(json.dumps(sorted(inputs), indent=2))
    con.close()


def _range_reclaim_signal(day, path, direction):
    opening = _window(path, _at(day, 9, 45), 15)
    base = dict(family='vertical', direction=direction)
    if not opening:
        return {**base, 'status':'UNDERLIER_GAP'}
    high, low = max(opening), min(opening)
    edge = low if direction=='UP' else high
    sign = 1 if direction=='UP' else -1
    outside, recovered, armed = 0, 0, False
    at = _at(day, 9, 46)
    while at <= _at(day, 13, 30):
        if at not in path:
            return {**base, 'status':'UNDERLIER_GAP'}
        distance = sign*(path[at]-edge)
        outside = outside+1 if distance < -1 else 0
        armed = armed or outside >= 2
        recovered = recovered+1 if armed and distance > 1 else 0
        if recovered >= 2:
            return {**base, 'signal_at':at, 'opening_range_high':high, 'opening_range_low':low}
        at += timedelta(minutes=1)
    return {**base, 'status':'NO_TRIGGER'}


def _price_exit_intent(row, path, deadline, *, ema_span=None):
    """First observable two-close invalidation, or first loss of its evidence."""
    day = row['entry_at'].astimezone(ET).date()
    at = _at(day, 9, 31) if ema_span else row['entry_at'].replace(second=0, microsecond=0)+timedelta(minutes=1)
    ema, count, streak, prior_side = None, 0, 0, 0
    while at < deadline:
        if at not in path:
            if at > row['entry_at']:
                return {'at':at, 'reason':'UNDERLIER_EXIT_GAP', 'censored':True}
            ema, count = None, 0
            at += timedelta(minutes=1)
            continue
        price = path[at]
        if ema_span:
            ema = price if ema is None else ema+2/(ema_span+1)*(price-ema)
            count += 1
        if at > row['entry_at']:
            if ema_span:
                if count < ema_span:
                    return {'at':at, 'reason':'UNDERLIER_EXIT_GAP', 'censored':True}
                side = 1 if (price-ema)*(1 if row['direction']=='UP' else -1)<0 else 0
            else:
                if row['family']=='condor':
                    low, high = row['legs'][1]['strike'], row['legs'][2]['strike']
                else:
                    low, high = min(leg['strike'] for leg in row['legs']), max(leg['strike'] for leg in row['legs'])
                side = -1 if price < low else 1 if price > high else 0
            streak = streak+1 if side and side==prior_side else 1 if side else 0
            prior_side = side
            if streak >= 2:
                return {'at':at, 'reason':f'ema{ema_span}_reversal' if ema_span else 'structure_breach', 'censored':False}
        at += timedelta(minutes=1)
    return {'at':deadline, 'reason':'hard_close', 'censored':False}


def _action_exit_label(row, marks, intent, deadline, *, quote_management=False, latency_seconds=15):
    """Choose a causal exit instruction, then its first valid delayed cash book.

    With price-only management, BBO gaps while no order can be triggered are
    irrelevant. Quote-based TP/SL still requires continuous observable marks.
    """
    marks = sorted(marks, key=lambda m:m.at)
    stop = dict(intent)
    if quote_management:
        policy = RTH_IRON_CONDOR_MANAGEMENT_POLICY if row['family']=='condor' else DEFAULT_MANAGEMENT_POLICY
        policy = replace(policy, hard_exit_et=deadline.astimezone(ET).strftime('%H:%M'))
        baseline = simulate_management_policy(marks, entry_ask=row['entry_price'], entry_at=row['entry_at'],
            leg_count=sum(abs(q) for q in row['quantities']), policy=policy,
            session_date=deadline.astimezone(ET).date(), max_quote_gap_seconds=60)
        if baseline.exit_at is not None and baseline.exit_at < stop['at']:
            stop = {'at':baseline.exit_at, 'reason':baseline.exit_reason, 'censored':False}
        previous = row['entry_at']
        for mark in marks:
            if mark.at > stop['at']:
                break
            if (mark.at-previous).total_seconds()>60:
                stop = {'at':previous+timedelta(seconds=60), 'reason':'QUOTE_GAP', 'censored':True}
                break
            previous = mark.at
        if previous+timedelta(seconds=60) < stop['at']:
            stop = {'at':previous+timedelta(seconds=60), 'reason':'QUOTE_GAP', 'censored':True}
    result = dict(exit_trigger_at=stop['at'], exit_reason=stop['reason'], pnl_usd=None,
                  policy_version='research.price_invalidation.v1', exit_latency_seconds=latency_seconds)
    if stop['censored']:
        return {**result, 'status':stop['reason']}
    action = stop['at']+(timedelta(0) if stop['reason']=='hard_close' else timedelta(seconds=latency_seconds))
    last = min(action+timedelta(seconds=60), _at(deadline.astimezone(ET).date(),16,0)-timedelta(microseconds=1))
    fill = next((m for m in marks if action<=m.at<=last), None)
    if fill is None:
        return {**result, 'exit_action_at':action, 'status':'EXIT_BBO_UNAVAILABLE'}
    fees = sum(abs(q) for q in row['quantities'])*2*1.32/100
    return {**result, 'exit_action_at':action, 'status':'COMPLETE_EXIT', 'exit_at':fill.at,
            'exit_bid':fill.combo_bid, 'fees_points':fees,
            'cash_exit_points':2*row['entry_price']-fill.combo_bid if row['family']=='condor' else fill.combo_bid,
            'pnl_usd':100*(fill.combo_bid-row['entry_price']-fees)}


def _raw_spx_bars(con, files, day):
    """OHLC from causal raw SPX observations; a stale final tick invalidates a bar."""
    if not files:
        return []
    _read_quotes(con, files)
    rows = con.execute("""
      WITH ticks AS (
        SELECT received_at, quality, market_data_type,
          CASE WHEN bid>0 AND ask>=bid THEN (bid+ask)/2 ELSE coalesce(last,effective_price) END AS price,
          CASE WHEN bid>0 AND ask>=bid THEN quote_time ELSE coalesce(trade_time,quote_time) END AS source
        FROM broker_quotes WHERE instrument_id='index:SPX' AND received_at>=? AND received_at<?
      ), clean AS (
        SELECT *,date_trunc('minute',received_at)+INTERVAL 1 MINUTE available_at,
          source<=received_at AND source>=received_at-INTERVAL 15 SECOND
            AND quality='live' AND lower(market_data_type) IN ('1','live') AND price>0 AS valid
        FROM ticks
      ) SELECT available_at,arg_min(price,received_at) FILTER(WHERE valid),
        max(price) FILTER(WHERE valid),min(price) FILTER(WHERE valid),
        arg_max(price,received_at) FILTER(WHERE valid),
        arg_max(coalesce(valid,false),received_at),arg_max(source,received_at)
      FROM clean GROUP BY available_at ORDER BY available_at
    """, [_at(day,9,30),_at(day,16,0)]).fetchall()
    return [dict(bar_start=at-timedelta(minutes=1),available_at=at,open=o,high=h,low=low,close=c)
        for at,o,h,low,c,valid,source in rows if valid and source and (at-source).total_seconds()<=15
        and all(_finite(v) for v in (o,h,low,c))]


def _ict_first_signals(day, bars):
    """Reuse the production pattern at each prefix; SPX OR15 variant, no ES basis."""
    from spx_spark.application.order_map.ict_liquidity import build_ict_liquidity_fact
    found={}
    by_at={b['available_at']:b for b in bars}
    prefix=[]
    at=_at(day,9,31)
    while at<=_at(day,13,30):
        if at not in by_at:
            break
        prefix.append(by_at[at])
        if len(prefix)>15:
            fact=build_ict_liquidity_fact(prefix,session_ranges={},opening_high=max(b['high'] for b in prefix[:15]),
                opening_low=min(b['low'] for b in prefix[:15]),basis=0.,session_date=str(day),decision_at=at)
            if fact['status']=='active' and fact['stage']=='MSS_DISPLACEMENT_CONFIRMED':
                direction=fact['direction']
                if direction not in found:
                    found[direction]=dict(family='vertical',direction=direction,signal_at=at,ict=fact,
                        signal_name='ict_'+direction.lower())
        at+=timedelta(minutes=1)
    return [found.get(d,dict(family='vertical',direction=d,signal_name='ict_'+d.lower(),
        status='UNDERLIER_GAP' if at<=_at(day,13,30) else 'NO_TRIGGER')) for d in ('UP','DOWN')]


def _gamma_position(chain, at):
    """Local OI×BBO gamma concentration, not identified dealer net positions."""
    anchor=_quote_forward(chain,at)
    if anchor is None:
        return {'status':'GAMMA_UNAVAILABLE'}
    _,center,forward=anchor
    tau=(_at(at.astimezone(ET).date(),16,0)-at).total_seconds()/(365*86400)
    weights={}
    seen=0
    for strike in range(int(center)-100,int(center)+101,5):
        for right in ('C','P'):
            leg=chain.get((strike,right))
            if leg is None or not _finite(leg.get('open_interest')) or leg['open_interest']<0:
                continue
            if leg['quote_time'] is None or not leg['quote_time']<=leg['received_at']<=at:
                continue
            greek=_quote_implied_greeks(leg,forward,tau,at)
            if greek is None:
                continue
            seen+=1
            weights[strike,right]=bs_gamma(forward,strike,greek['iv'],tau)*leg['open_interest']
    base={'coverage':seen/82,'valid_contracts':seen,'expected_contracts':82,'forward':forward,
          'dealer_sign':'UNKNOWN','scope':'0DTE local +/-100 points; OI last broker-observed, IV BBO-implied'}
    if seen/82<.8 or sum(weights.values())<=0:
        return {**base,'status':'GAMMA_COVERAGE_UNAVAILABLE'}
    call=[(v,k) for (k,r),v in weights.items() if r=='C' and k>forward and v>0]
    put=[(v,k) for (k,r),v in weights.items() if r=='P' and k<forward and v>0]
    centers=[(weights.get((k,'C'),0)+weights.get((k,'P'),0),k) for k in range(int(center)-20,int(center)+21,5)]
    if not call or not put:
        return {**base,'status':'GAMMA_WALL_UNAVAILABLE'}
    signed=sum(v*(1 if r=='C' else -1) for (k,r),v in weights.items())
    return {**base,'status':'available','call_wall':max(call)[1],'put_wall':max(put)[1],
        'pin_center':max(centers)[1],'call_minus_put_fraction':signed/sum(weights.values()),
        'unsigned_gamma_oi':sum(weights.values())}


def _wall_condor(chain, at, width, gamma):
    if gamma['status']!='available':
        return None,gamma['status']
    put,call=gamma['put_wall'],gamma['call_wall']
    legs=[chain.get((put-width,'P')),chain.get((put,'P')),chain.get((call,'C')),chain.get((call+width,'C'))]
    q=[1,-1,-1,1]
    cash=_cash_quote(legs,q,at,15,2)
    if cash is None:
        return None,'EXACT_LEGS_INVALID'
    tau=(_at(at.astimezone(ET).date(),16,0)-at).total_seconds()/(365*86400)
    greek=[_quote_implied_greeks(leg,gamma['forward'],tau,at) for leg in legs[1:3]]
    if any(g is None or not .10<=abs(g['delta'])<=.20 for g in greek):
        return None,'WALL_SHORT_OUTSIDE_10_20_DELTA'
    credit=-cash[0]
    if not 0<credit<width:
        return None,'ENTRY_GEOMETRY_INVALID'
    return dict(family='condor',legs=legs,quantities=q,width=width,signal_package_price=credit,
        selected_abs_deltas=[abs(g['delta']) for g in greek]),None


def _failed_expansion_signal(day, path, outside_direction):
    """First OR excursion: accept outside, then promptly reject it, with target ahead."""
    base = dict(direction='DOWN' if outside_direction=='UP' else 'UP')
    opening = _window(path, _at(day,9,45),15)
    if not opening:
        return {**base,'status':'UNDERLIER_GAP'}
    high, low = max(opening), min(opening)
    sign = 1 if outside_direction=='UP' else -1
    edge = high if sign==1 else low
    center = 5*round((high+low)/10)
    streak, recovered, armed, extreme = 0, 0, None, edge
    at = _at(day,9,46)
    while at <= _at(day,14,0):
        if at not in path:
            return {**base,'status':'UNDERLIER_GAP'}
        price = path[at]
        distance = sign*(price-edge)
        if armed is None:
            streak = streak+1 if distance>1 else 0
            extreme = (max if sign==1 else min)(extreme,price) if streak else edge
            if streak>=2:
                armed=at
        else:
            if at>armed+timedelta(minutes=15):
                return {**base,'status':'FIRST_EXPANSION_DID_NOT_FAIL_IN_15M'}
            extreme=(max if sign==1 else min)(extreme,price)
            recovered=recovered+1 if distance < -1 else 0
            if recovered>=2:
                return {**base,'signal_at':at,'target_level':center,'invalidation_level':extreme+sign,
                    'outside_direction':outside_direction,'opening_range_high':high,'opening_range_low':low,
                    'expansion_confirmed_at':armed,'signal_spx':price,
                    'target_distance':sign*(price-center),'failed_extreme':extreme}
        at+=timedelta(minutes=1)
    return {**base,'status':'NO_TRIGGER'}


def _mechanism_signals(day, path):
    signals=[]
    for outside in ('UP','DOWN'):
        failed=_failed_expansion_signal(day,path,outside)
        for delta in (.15,.20):
            for width in (10,20):
                signals.append({**failed,'family':'condor','width':width,'delta_target':delta,
                    'mechanism':'failed_expansion','signal_name':f'failure_{outside.lower()}_ic{int(delta*100)}_w{width}'})
        reverse={**failed,'mechanism':'return_to_range','family':'butterfly'}
        if reverse.get('signal_at') and not 2.5<=reverse['target_distance']<=20:
            reverse['status']='RETURN_TARGET_ALREADY_REACHED_OR_TOO_FAR'
        continuation={**_first_directional_range_signal(day,path,outside),
            'mechanism':'continuation_destination','family':'butterfly'}
        if continuation.get('signal_at'):
            at=continuation['signal_at']
            sign=1 if outside=='UP' else -1
            opening=_window(path,_at(day,9,45),15)
            continuation.update(target_level=5*round(path[at]/5)+15*sign,signal_spx=path[at],
                invalidation_level=(max(opening)-1 if sign==1 else min(opening)+1))
        for name, trigger in [('return',reverse),('destination',continuation)]:
            for center in ('target','atm'):
                signals.append({**trigger,'center_rule':center,
                    'signal_name':f'{name}_{outside.lower()}_bf_{center}'})
    # Fixed clocks remain a predeclared control, never a substitute for a missed trigger.
    signals.extend(dict(signal_name=f'ic20_w{width}',family='condor',direction='NEUTRAL',
        width=width,signal_at=_at(day,10,0)) for width in (10,20))
    return signals


def _mechanism_exit_intent(row, path, deadline):
    """Trade a finite price destination; reject a renewed excursion, not a timer."""
    at=row['entry_at'].replace(second=0,microsecond=0)+timedelta(minutes=1)
    sign=1 if row['direction']=='UP' else -1
    streak=0
    while at<deadline:
        if at not in path:
            return dict(at=at,reason='UNDERLIER_EXIT_GAP',censored=True)
        price=path[at]
        # Failure ICs have a reversion direction too, but do not take profit merely
        # because SPX reached the midpoint: their premium TP is a separate variant.
        if row['family']=='butterfly' and sign*(price-row['target_level'])>=0:
            return dict(at=at,reason='price_destination_reached',censored=False)
        invalid=sign*(price-row['invalidation_level'])<0
        streak=streak+1 if invalid else 0
        if streak>=2:
            return dict(at=at,reason='price_hypothesis_failed',censored=False)
        at+=timedelta(minutes=1)
    return dict(at=deadline,reason='hard_close',censored=False)


def _evaluate_mechanism_replay(rows):
    """Report frozen hypotheses, including unpriced first attempts and censored risk."""
    def stats(items):
        entered=[r for r in items if r.get('entry_at')]
        complete=[r for r in entered if r.get('pnl_usd') is not None]
        missing=[r for r in entered if r.get('pnl_usd') is None]
        profits=[r['pnl_usd'] for r in complete]
        net=sum(profits)
        pressure=net-sum(r['defined_risk_usd']+2*1.32*sum(abs(q) for q in r['quantities']) for r in missing)
        return dict(planned_sessions=len({r['session_date'] for r in items}),entered=len(entered),complete=len(complete),
            missing=len(missing),net_usd=net,mean_usd=float(np.mean(profits)) if profits else None,
            wins=sum(p>0 for p in profits),worst_usd=min(profits) if profits else None,
            missing_full_risk_net_usd=pressure,extra_20usd_slippage_net_usd=pressure-20*len(entered),
            remove_best_two_missing_pressure_net_usd=pressure-sum(sorted([p for p in profits if p>0],reverse=True)[:2]),
            mean_entry_points=float(np.mean([r['entry_price'] for r in complete])) if complete else None,
            exit_reasons=dict(Counter(r.get('exit_reason',r['status']) for r in items)),
            statuses=dict(Counter(r['status'] for r in items)))
    def periods(items):
        return dict(all=stats(items),july=stats([r for r in items if r['session_date']<'2026-08-01']),
            august_september=stats([r for r in items if r['session_date']>='2026-08-01']))
    variants={name:periods([r for r in rows if r['setup']==name]) for name in sorted({r['setup'] for r in rows})}
    primary={}
    for mechanism,suffix in [('failed_expansion','ic20_w20_price_quote'),
                              ('return_to_range','bf_target_price_only'),
                              ('continuation_destination','bf_target_price_only')]:
        pool=[r for r in rows if r.get('mechanism')==mechanism and r['setup'].endswith(suffix)]
        chosen=[]
        for day in sorted({r['session_date'] for r in rows}):
            candidates=[r for r in pool if r['session_date']==day and r.get('signal_at')]
            if candidates:
                # Rank observed intent, not quote availability or completed profit.
                chosen.append(min(candidates,key=lambda r:(r['signal_at'],r['signal_name'])))
            else:
                states=Counter(r['status'] for r in pool if r['session_date']==day)
                unavailable=next((s for s in ('PARTITION_MISSING','UNDERLIER_GAP') if states[s]),None)
                chosen.append(dict(session_date=day,status=unavailable or 'NO_SIGNAL',pnl_usd=None,
                    signal_statuses=dict(states)))
        primary[mechanism]={**periods(chosen),'decisions':chosen}
    pairs={}
    for mechanism in ('return_to_range','continuation_destination'):
        for variant in ('price_only','price_quote','quote_only'):
            target=[r for r in rows if r.get('mechanism')==mechanism and r.get('center_rule')=='target'
                    and r['setup'].endswith('_'+variant)]
            controls={(r['session_date'],r['direction']):r for r in rows if r.get('mechanism')==mechanism
                and r.get('center_rule')=='atm' and r['setup'].endswith('_'+variant)}
            paired=[]
            for row in target:
                control=controls.get((row['session_date'],row['direction']))
                if control and row.get('pnl_usd') is not None and control.get('pnl_usd') is not None:
                    paired.append(dict(session_date=row['session_date'],direction=row['direction'],
                        target_pnl=row['pnl_usd'],atm_pnl=control['pnl_usd'],
                        entry_saving_usd=100*(control['entry_price']-row['entry_price']),
                        exit_cash_change_usd=100*(row['cash_exit_points']-control['cash_exit_points']),
                        same_exit_clock=row['exit_at']==control['exit_at']))
            pairs[mechanism+'_'+variant]=dict(count=len(paired),rows=paired,
                target_mean=float(np.mean([p['target_pnl'] for p in paired])) if paired else None,
                atm_mean=float(np.mean([p['atm_pnl'] for p in paired])) if paired else None)
    return dict(scope='all registered variants, no post-outcome rule selection; overlapping variants never summed',
        primary_contract='one first observed signal per day, including unavailable entry; IC20 width20 price_quote; BF target price_only',
        variants=variants,primary=primary,pairs=pairs)


def explore_price_exits(data_root, output, start, end, providers, *, signal_filter=None,
                        entry_delay_seconds=15, exit_latency_seconds=15, position_signals=False,
                        mechanism_signals=False):
    output.mkdir(parents=True, exist_ok=False)
    source = Path(__file__).read_bytes()
    (output/'research-source.py').write_bytes(source)
    contract = {'scope':'raw first directional signal / fixed butterfly and IC clocks; no prior card inputs',
        'vertical_signals':['OR15 first UP/DOWN acceptance','OR15 false break: two outside, two back inside by 1 point'],
        'vertical_primary':'EMA10: two closes against position; no premium/time stop; no fixed holding duration',
        'vertical_controls':['EMA5','EMA20','EMA10 plus existing premium/trail','existing premium/trail only'],
        'butterfly':'15-wide ATM center at 14:00/14:30/15:00; two closes outside wings, no mandatory 15:55 hold',
        'condor':'fresh BBO-implied 20 delta, 10/20-wide, 10:00; two closes outside same short strike plus existing TP/SL',
        'quotes':'15s max source age, 2s skew, depth and availability; all cash crossed BBO',
        'entry':'first signal only, legs frozen, configured entry delay, debit<=45%width at signal and entry',
        'exit':'price or quote trigger plus configured latency; first fresh valid book within 60sec, never best future quote',
        'deadline':'vertical/IC 15:45; butterfly 15:59; last pre-16:00 book only, not settlement',
        'observation':'price-only requires continuous underlier, not irrelevant intrahold BBO; quote TP/SL requires <=60s BBO gaps',
        'evaluation':'all session denominators; July selection / August-September evaluation; explored window not untouched OOS',
        'position_signals':position_signals,
        'position_contract':'SPX OR15 ICT production pattern; gamma OI x BBO gamma +/-100, >=80% grid coverage; BF highest unsigned concentration within ATM +/-20; wall IC 10-20 delta; no dealer inventory claim',
        'signal_filter':signal_filter, 'entry_delay_seconds':entry_delay_seconds,
        'exit_latency_seconds':exit_latency_seconds,
        'bark_access':False,'automatic_ordering':False,'fill_status':'UNKNOWN',
        'script_sha256':hashlib.sha256(source).hexdigest()}
    if position_signals:
        contract.update(vertical_signals=['OR15 first UP/DOWN acceptance','SPX OR15 ICT first MSS+displacement UP/DOWN'],
            butterfly='ATM vs gamma concentration center at 14:00/14:30/15:00, 15-wide',
            condor='10:00 BBO-implied 20 delta vs local wall shorts restricted to 10-20 delta; 10/20-wide')
    if mechanism_signals:
        contract.update(scope='three registered economic mechanisms, no factor threshold search',
            hypotheses=['failed directional expansion leaves overpriced tail premium for IC',
                'buy a displaced butterfly at the prior range midpoint before price returns',
                'buy a directional butterfly at a finite continuation destination instead of betting on a stationary pin'],
            trigger='OR15 closes; first two closes >1 point outside, first two >1 point inside within 15m after acceptance; first excursion only through 14:00, no later rescue',
            butterfly='15-wide; failed-break midpoint rounded5, remaining distance 2.5..20; continuation OR15 three-close acceptance target rounded signal SPX +/-15; target vs same-clock ATM center',
            condor='failed-break 15/20 delta x 10/20 width; fixed10:00 20delta controls; no fitted IV filter',
            exits='price_only: BF target first crossed close or two closes beyond frozen invalidation; IC failure extreme invalidation OR short strike breach. price_quote adds existing TP/SL; quote_only control. No fixed holding duration.',
            evaluation='all 54 variants reported; July/August-September split, missing full-risk stress, paired target/ATM attribution; no selected production signal',
            mechanism_signals=True)
    (output/'contract.json').write_text(json.dumps(contract,indent=2))
    con = duckdb.connect(config={'threads':2,'memory_limit':'768MB'})
    con.execute("SET TimeZone='UTC'")
    results, inputs = [], set()
    day = start
    while day <= end:
        if DEFAULT_MARKET_CALENDAR.session(day) is None:
            day += timedelta(days=1)
            continue
        for provider in providers:
            files = _files(data_root/'lake/quotes/schema=v1',provider,_at(day,9,30),_at(day,16,0))
            inputs.update(files)
            spx = _underlier_minutes(con,files,'index:SPX',_at(day,9,30),_at(day,16,0)) if files else {}
            signals=[]
            for direction in ('UP','DOWN'):
                for name, make_signal in [('or15',_first_directional_range_signal),('reclaim',_range_reclaim_signal)]:
                    signals.append({**make_signal(day,spx,direction),'signal_name':f'{name}_{direction.lower()}'})
            for hour, minute in ((14,0),(14,30),(15,0)):
                signals.append(dict(signal_name=f'butterfly_{hour:02}{minute:02}',family='butterfly',direction='NEUTRAL',signal_at=_at(day,hour,minute)))
            for width in (10,20):
                signals.append(dict(signal_name=f'ic20_w{width}',family='condor',direction='NEUTRAL',width=width,signal_at=_at(day,10,0)))
            if position_signals:
                signals=[s for s in signals if not s['signal_name'].startswith('reclaim')]
                signals.extend(_ict_first_signals(day,_raw_spx_bars(con,files,day)))
                signals.extend([{**s,'signal_name':'gamma_'+s['signal_name']} for s in list(signals)
                    if s['family'] in ('butterfly','condor')])
            if mechanism_signals:
                signals=_mechanism_signals(day,spx)
            if signal_filter is not None:
                signals=[s for s in signals if s['signal_name']==signal_filter]
            times = sorted({t for s in signals if s.get('signal_at') for t in
                (s['signal_at'],s['signal_at']+timedelta(seconds=entry_delay_seconds),
                 s['signal_at']-timedelta(minutes=15))}|{_at(day,9,45)})
            books = _snapshots(con,files,day,times,provider,15) if files else {t:{} for t in times}
            gamma_profiles={}
            if position_signals:
                gamma_times=sorted({t for s in signals if s.get('signal_at') for t in
                    (s['signal_at'],s['signal_at']-timedelta(minutes=10))})
                gamma_books=_snapshots(con,files,day,gamma_times,provider,15,include_open_interest=True) if files else {t:{} for t in gamma_times}
                gamma_profiles={t:_gamma_position(gamma_books[t],t) for t in gamma_times}
            entries=[]
            for signal in signals:
                row = {**signal,'provider':provider,'mode':'rth','session_date':str(day),'fill_status':'UNKNOWN'}
                if not files:
                    row['status']='PARTITION_MISSING'
                if 'status' not in row:
                    at=signal['signal_at']
                    gamma=gamma_profiles.get(at)
                    if gamma is not None:
                        prior=gamma_profiles[at-timedelta(minutes=10)]
                        row.update(gamma_position=gamma,gamma_prior=prior)
                    if signal['signal_name'].startswith('gamma_'):
                        if signal['family']=='condor':
                            structure,reason=_wall_condor(books[at],at,signal['width'],gamma)
                        elif gamma['status']=='available':
                            structure,reason=_structure(signal,books[at],age=15,skew=2,anchor_override=gamma['pin_center'])
                        else:
                            structure,reason=None,gamma['status']
                    else:
                        structure, reason = (_delta_condor(books[at],at,signal.get('delta_target',.20),signal['width'],'bbo_implied')
                            if signal['family']=='condor' else _structure(signal,books[at],age=15,skew=2,
                                anchor_override=signal.get('target_level') if signal.get('center_rule')=='target' else None))
                    if reason:
                        row['status']=reason
                    else:
                        row.update(structure)
                        action=at+timedelta(seconds=entry_delay_seconds)
                        legs=[books[action].get((leg['strike'],leg['right'])) for leg in row['legs']]
                        quote=_cash_quote(legs,row['quantities'],action,15,2)
                        credit=row['family']=='condor'
                        value=(-quote[0] if credit else quote[0]) if quote else None
                        if not credit and row['signal_package_price']/row['width']>.45:
                            row['status']='SIGNAL_DEBIT_CAP'
                        elif quote is None:
                            row['status']='ENTRY_BBO_UNAVAILABLE'
                        elif not _depth(legs,row['quantities']):
                            row['status']='ENTRY_DEPTH_UNAVAILABLE'
                        elif not 0<value/row['width']<1 or not credit and value/row['width']>.45:
                            row['status']='ENTRY_PRICE_INVALID'
                        else:
                            row.update(status='ENTERED',entry_at=action,entry_price=value,legs=legs,
                                entry_context=_path_context(spx,at,15),
                                defined_risk_usd=100*(row['width']-value if credit else value))
                            if credit:
                                row['volatility']=_volatility_at_entry(books[at],books[at-timedelta(minutes=15)],at,spx)
                entries.append(row)
            entered=[r for r in entries if r['status']=='ENTERED']
            events=_contract_events(con,files,sorted({leg['instrument_id'] for r in entered for leg in r['legs']}),
                min(r['entry_at'] for r in entered),_at(day,16,0),provider) if entered else {}
            plans, action_times=[],set()
            for entry in entries:
                deadline=_at(day,15,59) if entry['family']=='butterfly' else _at(day,15,45)
                if entry['family']=='vertical':
                    variants=[('ema10',10,False),('ema5',5,False),('ema20',20,False),('ema10_quote',10,True),('quote_only',None,True)]
                else:
                    variants=[('price_only',None,False),('price_quote',None,True),('quote_only',None,True)]
                marks=_package_path(entry,events,_at(day,16,0)-timedelta(microseconds=1),15,2) if entry['status']=='ENTERED' else []
                for name, span, quote_management in variants:
                    row={**entry,'setup':entry['signal_name']+'_'+name}
                    intent={'at':deadline,'reason':'hard_close','censored':False}
                    if entry['status']=='ENTERED' and name!='quote_only':
                        intent=_price_exit_intent(entry,spx,deadline,ema_span=span)
                        if entry.get('mechanism'):
                            mechanism_intent=_mechanism_exit_intent(entry,spx,deadline)
                            # BF geometry is not an invalidation: the intended return
                            # can start outside its narrow wings. IC retains both sides.
                            intent=(min((intent,mechanism_intent),key=lambda i:i['at'])
                                if entry['family']=='condor' else mechanism_intent)
                    plans.append((row,intent,deadline,quote_management,marks))
                    if entry['status']=='ENTERED':
                        first=_action_exit_label(row,marks,intent,deadline,quote_management=quote_management,latency_seconds=exit_latency_seconds)
                        if first.get('exit_action_at'):
                            action_times.add(first['exit_action_at'])
            exit_books=_snapshots(con,files,day,sorted(action_times),provider,15) if action_times else {}
            for row,intent,deadline,quote_management,marks in plans:
                if row['status']=='ENTERED':
                    # Add only precomputed action clocks. Their stale books cannot
                    # create new TP/SL triggers or refresh evidence during holding.
                    first=_action_exit_label(row,marks,intent,deadline,quote_management=quote_management,latency_seconds=exit_latency_seconds)
                    action=first.get('exit_action_at')
                    timed=marks
                    if action is not None:
                        legs=[exit_books[action].get((leg['strike'],leg['right'])) for leg in row['legs']]
                        quote=_cash_quote(legs,row['quantities'],action,15,2)
                        if quote is not None and _depth(legs,row['quantities'],liquidate=True):
                            value=quote[1]+(2*row['entry_price'] if row['family']=='condor' else 0)
                            timed=sorted([m for m in marks if m.at!=action]+[PolicyMark(action,value)],key=lambda m:m.at)
                    row.update(_action_exit_label(row,timed,intent,deadline,quote_management=quote_management,latency_seconds=exit_latency_seconds))
                results.append(row)
                with (output/'rows.jsonl').open('a') as handle:
                    handle.write(json.dumps(row,default=str)+'\n')
            print(json.dumps({'day':str(day),'provider':provider,'entered':len(entered)}),flush=True)
        day+=timedelta(days=1)
    (output/'summary.json').write_text(json.dumps(_summary(results),indent=2))
    if mechanism_signals:
        (output/'mechanism-analysis.json').write_text(json.dumps(_evaluate_mechanism_replay(results),indent=2,default=str))
    (output/'input-files.json').write_text(json.dumps(sorted(inputs),indent=2))
    con.close()


# Direction is the economic primary hypothesis; the opposite sign is not silently searched.
# These are research proxies, grouped to avoid counting correlated metrics as new evidence.
FACTOR_HYPOTHESES = {
    'rv5': ('volatility', 'low', 'recent five-minute realized variance is low'),
    'rv15': ('volatility', 'low', 'recent fifteen-minute realized variance is low'),
    'rv30': ('volatility', 'low', 'recent thirty-minute realized variance is low'),
    'rv5_over30': ('volatility', 'low', 'short volatility is contracting relative to thirty minutes'),
    'rv15_over_previous15': ('volatility', 'low', 'realized volatility contracts across adjacent windows'),
    'jump_share30': ('jumps', 'low', 'no single one-minute move dominates variance'),
    'bipower_jump_share30': ('jumps', 'low', 'low positive realized-minus-bipower variance proxy'),
    'semivariance_imbalance30': ('jumps', 'low', 'up and down variance are balanced'),
    'efficiency15': ('trend', 'low', 'little directional displacement relative to travelled path'),
    'efficiency30': ('trend', 'low', 'low thirty-minute directional efficiency'),
    'abs_move5_z': ('trend', 'low', 'no outsized latest displacement'),
    'abs_move15_z': ('trend', 'low', 'fifteen-minute displacement is contained'),
    'return_autocorrelation30': ('reversion', 'low', 'negative short-return serial correlation favours reversion'),
    'variance_ratio5_30': ('reversion', 'low', 'five-minute moves smaller than diffusive scaling'),
    'twap_distance30_z': ('reversion', 'low', 'price remains near equal-time average, not volume VWAP'),
    'range15_over30': ('reversion', 'low', 'recent range contracts inside previous thirty-minute range'),
    'atm_iv': ('implied_volatility', 'high', 'more implied option premium'),
    'fixed_atm_iv_change5': ('implied_volatility', 'low', 'fixed-strike IV contracts over five minutes'),
    'fixed_atm_iv_change15': ('implied_volatility', 'low', 'fixed-strike IV contracts over fifteen minutes'),
    'fixed_straddle_change15': ('implied_volatility', 'low', 'fixed ATM straddle contracts; not pure IV change'),
    'implied_variance_over_rv30': ('variance_premium', 'high', 'implied variance exceeds trailing thirty-minute projection'),
    'implied_variance_over_rv15': ('variance_premium', 'high', 'implied variance exceeds trailing fifteen-minute projection'),
    'abs_25delta_skew': ('surface', 'low', 'less asymmetric tail pricing'),
    'wing_curvature': ('surface', 'high', 'wing premium relative to ATM reflects costly protection'),
    'abs_skew_change15': ('surface', 'low', 'fixed-wing skew is stable'),
    'gamma_signed_fraction': ('position', 'high', 'call-minus-put OI gamma proxy, dealer sign unknown'),
    'gamma_oi_total': ('position', 'high', 'large local OI gamma concentration, not dealer inventory'),
    'gamma_coverage': ('data', 'high', 'broad fresh local surface coverage'),
    'pin_distance_width': ('position', 'low', 'current forward close to local gamma center'),
    'pin_stability10': ('position', 'high', 'gamma center agrees with snapshot ten minutes earlier'),
    'wall_balance': ('position', 'high', 'similar distances to upper and lower local walls'),
    'wall_span_over_ivmove': ('geometry', 'high', 'wall corridor wide relative to implied move'),
    'wall_clearance_width': ('geometry', 'high', 'IC shorts outside walls; butterfly center close to gamma center'),
    'entry_reward_fraction': ('geometry', 'high', 'IC credit/width or BF (width-debit)/width is larger'),
    'center_distance_width': ('geometry', 'low', 'forward close to structure center'),
    'min_boundary_over_ivmove': ('geometry', 'high', 'nearest short strike or butterfly wing far from forward'),
    'cash_spread_fraction': ('execution', 'low', 'crossing the package costs less relative to premium'),
    'fee_fraction': ('execution', 'low', 'fees consume less credit or maximum butterfly gross gain'),
    'depth_multiple': ('execution', 'high', 'displayed depth covers more package units'),
    'source_skew_seconds': ('execution', 'low', 'legs are more synchronized'),
    'package_gamma_risk': ('geometry', 'low', 'small absolute gamma curvature relative to premium'),
    'normal_rv_expiry_edge_fraction': ('distribution', 'high', 'zero-drift normal RV expiry payoff proxy exceeds cash cost; not managed-exit EV'),
}


def _raw_option_factors(row, path, chain, old5, old15, at):
    """All observations are signal-time inputs, including cash costs (never entry+15s)."""
    values={name:None for name in FACTOR_HYPOTHESES}
    def ratio(a,b):
        return float(a/b) if a is not None and b is not None and b>0 else None
    windows={n:_window(path,at,n) for n in (5,15,30)}
    returns={n:np.diff(p) for n,p in windows.items() if p}
    variances={n:float(np.mean(r*r)) for n,r in returns.items()}
    for n,v in variances.items():
        values[f'rv{n}']=v
    values['rv5_over30']=ratio(variances.get(5),variances.get(30))
    if 30 in returns:
        r=returns[30]
        total=float(np.sum(r*r))
        previous=float(np.mean(r[:15]**2))
        values['rv15_over_previous15']=ratio(variances.get(15),previous)
        values['jump_share30']=ratio(float(np.max(r*r)),total)
        bipower=float(np.pi/2*np.sum(np.abs(r[1:])*np.abs(r[:-1])))
        values['bipower_jump_share30']=ratio(max(total-bipower,0),total)
        values['semivariance_imbalance30']=ratio(abs(float(np.sum(r[r>0]**2)-np.sum(r[r<0]**2))),total)
        if np.std(r[:-1])>0 and np.std(r[1:])>0:
            values['return_autocorrelation30']=float(np.corrcoef(r[:-1],r[1:])[0,1])
        values['variance_ratio5_30']=ratio(float(np.mean(np.diff(np.asarray(windows[30])[::5])**2)),5*variances[30])
        values['twap_distance30_z']=ratio(abs(windows[30][-1]-float(np.mean(windows[30]))),math.sqrt(total))
        if windows[15]:
            values['range15_over30']=ratio(max(windows[15])-min(windows[15]),max(windows[30])-min(windows[30]))
    for n in (15,30):
        if n in returns:
            values[f'efficiency{n}']=ratio(abs(float(np.sum(returns[n]))),float(np.sum(np.abs(returns[n]))))
    for n in (5,15):
        if n in returns:
            values[f'abs_move{n}_z']=ratio(abs(float(np.sum(returns[n]))),math.sqrt(float(np.sum(returns[n]**2))))
    anchor=_quote_forward(chain,at)
    if anchor is None:
        return values
    straddle,strike,forward=anchor
    tau=(_at(at.astimezone(ET).date(),16,0)-at).total_seconds()/(365*86400)
    remaining=tau*365*1440
    atm=[_quote_implied_greeks(chain.get((strike,right)),forward,tau,at) for right in ('C','P')]
    iv=float(np.mean([g['iv'] for g in atm])) if all(atm) else None
    values['atm_iv']=iv
    implied_move=forward*iv*math.sqrt(tau) if iv is not None else None
    for n in (15,30):
        values[f'implied_variance_over_rv{n}']=ratio(implied_move**2 if implied_move is not None else None,
            variances[n]*remaining if n in variances else None)
    for n,old in ((5,old5),(15,old15)):
        before=at-timedelta(minutes=n)
        pair=[old.get((strike,right)) for right in ('C','P')]
        if _cash_quote(pair,[1,1],before,15,2) is not None:
            prices=[(leg['bid']+leg['ask'])/2 for leg in pair]
            old_forward=strike+prices[0]-prices[1]
            old_iv=[_quote_implied_greeks(leg,old_forward,tau+n/(365*1440),before) for leg in pair]
            if iv is not None and all(old_iv):
                values[f'fixed_atm_iv_change{n}']=iv-float(np.mean([g['iv'] for g in old_iv]))
            if n==15:
                values['fixed_straddle_change15']=ratio(straddle,sum(prices))
                if values['fixed_straddle_change15'] is not None:
                    values['fixed_straddle_change15']-=1
    wings=[]
    for right in ('P','C'):
        choices=[]
        for (k,r),leg in chain.items():
            if r!=right or (k>=forward if right=='P' else k<=forward):
                continue
            g=_quote_implied_greeks(leg,forward,tau,at)
            if g is not None:
                choices.append((abs(abs(g['delta'])-.25),k,g))
        if choices:
            _,k,g=min(choices,key=lambda item:item[:2])
            wings.append((right,k,g))
    if len(wings)==2 and iv is not None:
        skew=wings[1][2]['iv']-wings[0][2]['iv']
        values['abs_25delta_skew']=abs(skew)
        values['wing_curvature']=float(np.mean([g['iv'] for _,_,g in wings]))-iv
        before=at-timedelta(minutes=15)
        old_anchor=_quote_forward(old15,before)
        if old_anchor is not None:
            old=[_quote_implied_greeks(old15.get((k,r)),old_anchor[2],tau+15/(365*1440),before) for r,k,_ in wings]
            if all(old):
                values['abs_skew_change15']=abs(skew-(old[1]['iv']-old[0]['iv']))
    gamma=row.get('gamma_position',{})
    width=row.get('width',15.)
    if gamma.get('status')=='available':
        values.update(gamma_signed_fraction=gamma['call_minus_put_fraction'],gamma_oi_total=gamma['unsigned_gamma_oi'],
            gamma_coverage=gamma['coverage'],pin_distance_width=abs(forward-gamma['pin_center'])/width,
            wall_balance=ratio(min(forward-gamma['put_wall'],gamma['call_wall']-forward),max(forward-gamma['put_wall'],gamma['call_wall']-forward)),
            wall_span_over_ivmove=ratio(gamma['call_wall']-gamma['put_wall'],implied_move))
        prior=row.get('gamma_prior',{})
        if prior.get('status')=='available':
            values['pin_stability10']=float(prior['pin_center']==gamma['pin_center'])
    if not row.get('legs'):
        return values
    legs=[chain.get((leg['strike'],leg['right'])) for leg in row['legs']]
    quantities=row['quantities']
    quote=_cash_quote(legs,quantities,at,15,2)
    if quote is None or any(not leg['quote_time']<=leg['received_at']<=at for leg in legs):
        return values
    credit=row['family']=='condor'
    premium=-quote[0] if credit else quote[0]
    if not 0<premium<width:
        return values
    low,high=(legs[1]['strike'],legs[2]['strike']) if credit else (min(leg['strike'] for leg in legs),max(leg['strike'] for leg in legs))
    center=(low+high)/2
    values.update(entry_reward_fraction=premium/width if credit else (width-premium)/width,
        center_distance_width=abs(center-forward)/width,
        min_boundary_over_ivmove=ratio(min(forward-low,high-forward),implied_move),
        cash_spread_fraction=(quote[0]-quote[1])/premium,
        fee_fraction=sum(abs(q) for q in quantities)*2*1.32/(100*(premium if credit else width-premium)),
        source_skew_seconds=(max(leg['quote_time'] for leg in legs)-min(leg['quote_time'] for leg in legs)).total_seconds(),
        depth_multiple=min(float(leg.get('ask_size' if q>0 else 'bid_size') or 0)/abs(q) for leg,q in zip(legs,quantities)))
    if gamma.get('status')=='available':
        values['wall_clearance_width']=(min(gamma['put_wall']-low,high-gamma['call_wall']) if credit else -abs(center-gamma['pin_center']))/width
    greeks=[_quote_implied_greeks(leg,forward,tau,at) for leg in legs]
    if all(greeks) and implied_move is not None:
        combo_gamma=sum(q*bs_gamma(forward,leg['strike'],g['iv'],tau) for q,leg,g in zip(quantities,legs,greeks))
        values['package_gamma_risk']=abs(combo_gamma)*implied_move**2/premium
    if variances.get(30,0)>0:
        sigma=math.sqrt(variances[30]*remaining)
        expected=0.
        for leg,q in zip(legs,quantities):
            distance=(forward-leg['strike'])*(1 if leg['right']=='C' else -1)
            z=distance/sigma
            intrinsic=distance*(1+math.erf(z/math.sqrt(2)))/2+sigma*math.exp(-z*z/2)/math.sqrt(2*math.pi)
            expected+=q*intrinsic
        fee=sum(abs(q) for q in quantities)*2*1.32/100
        values['normal_rv_expiry_edge_fraction']=(expected-quote[0]-fee)/width
    return values


FACTOR_PAIRS = (
    ('implied_variance_over_rv30','efficiency15'),
    ('implied_variance_over_rv30','jump_share30'),
    ('entry_reward_fraction','cash_spread_fraction'),
    ('pin_distance_width','pin_stability10'),
    ('wall_clearance_width','rv5_over30'),
    ('normal_rv_expiry_edge_fraction','cash_spread_fraction'),
    ('abs_25delta_skew','semivariance_imbalance30'),
    ('gamma_signed_fraction','efficiency15'),
)


def _factor_rule_passes(row, conditions):
    for name,direction,threshold in conditions:
        value=row['factors'].get(name)
        if not _finite(value) or (value<threshold if direction=='high' else value>threshold):
            return False
    return True


def _factor_return(row):
    if not row.get('entry_at'):
        return 0.
    cash=row.get('pnl_usd')
    if cash is None:
        cash=-row['defined_risk_usd']-sum(abs(q) for q in row['quantities'])*2*1.32
    return cash/row['defined_risk_usd']


def _factor_cash_summary(rows):
    entered=[r for r in rows if r.get('entry_at')]
    complete=[r for r in entered if r.get('pnl_usd') is not None]
    cash=[r['pnl_usd'] for r in complete]
    return {'passed_opportunities':len(rows),'entered':len(entered),'complete':len(complete),
        'mean_usd':float(np.mean(cash)) if cash else None,'net_sum_usd':sum(cash),
        'missing_full_loss_stress_sum_usd':sum(_factor_return(r)*r['defined_risk_usd'] for r in entered),
        'missing_dates':[r['session_date'] for r in entered if r.get('pnl_usd') is None],
        'complete_dates':[r['session_date'] for r in complete],
        'worst_usd':min(cash) if cash else None,'win_count':sum(v>0 for v in cash)}


def evaluate_option_factor_rules(rows, output=None, *, cutoff='2026-08-01'):
    """Freeze feature thresholds and model selection using only earlier sessions."""
    training_dates=sorted({r['session_date'] for r in rows if r['session_date']<cutoff})
    testing_dates=sorted({r['session_date'] for r in rows if r['session_date']>=cutoff})
    rules=[]
    for setup in sorted({r['setup'] for r in rows}):
        cohort=[r for r in rows if r['setup']==setup]
        train=[r for r in cohort if r['session_date']<cutoff]
        medians={}
        definitions=[('baseline',[])]
        for name,(_,direction,_) in FACTOR_HYPOTHESES.items():
            values=[r['factors'][name] for r in train if _finite(r['factors'].get(name))]
            if not values:
                continue
            for quantile in (.5,2/3 if direction=='high' else 1/3):
                threshold=float(np.quantile(values,quantile))
                definitions.append((f'{name}:q{quantile:.6f}',[(name,direction,threshold)]))
                if quantile==.5:
                    medians[name]=(name,direction,threshold)
        for first,second in FACTOR_PAIRS:
            if first in medians and second in medians:
                definitions.append((f'{first}&{second}',[medians[first],medians[second]]))
        for name,conditions in definitions:
            selected=[r for r in train if _factor_rule_passes(r,conditions)]
            sufficient=sum(r.get('pnl_usd') is not None for r in selected)>=5
            daily={r['session_date']:_factor_return(r) for r in selected}
            returns=[daily.get(ds,0.) for ds in training_dates]
            score=float(np.mean(returns)) if sufficient else None
            rules.append({'setup':setup,'rule':name,'family':cohort[0]['family'],'conditions':conditions,
                'train_score':score,'train':_factor_cash_summary(selected),'training_daily_risk_returns':returns})
    result={'cutoff':cutoff,'training_dates':training_dates,'testing_dates':testing_dates,
        'registered_factor_count':len(FACTOR_HYPOTHESES),'rules_generated':len(rules),
        'rules_training_eligible':sum(r['train_score'] is not None for r in rules),'selected':{},'all_rules':rules}
    # Selection is complete before any test outcome is read.
    winners={}
    for family in ('condor','butterfly'):
        eligible=[r for r in rules if r['family']==family and r['train_score'] is not None]
        eligible.sort(key=lambda r:(-r['train_score'],len(r['conditions']),r['setup'],r['rule']))
        winners[family]=dict(eligible[0]) if eligible and eligible[0]['train_score']>0 else None
        result['selected'][family]={'winner':winners[family], 'best_training_rule':dict(eligible[0]) if eligible else None}
    for family in ('condor','butterfly'):
        winner=winners[family]
        if winner:
            test=[r for r in rows if r['setup']==winner['setup'] and r['session_date']>=cutoff
                  and _factor_rule_passes(r,winner['conditions'])]
            result['selected'][family]['test']=_factor_cash_summary(test)
            base=[r for r in rows if r['setup']==winner['setup'] and r['session_date']>=cutoff]
            result['selected'][family]['same_setup_unfiltered_test']=_factor_cash_summary(base)
        else:
            result['selected'][family]['test']=None
    # Preserve all held-period results, including those not selected by July.
    for rule in rules:
        test=[r for r in rows if r['setup']==rule['setup'] and r['session_date']>=cutoff
            and _factor_rule_passes(r,rule['conditions'])]
        rule['test']=_factor_cash_summary(test)
    if output is not None:
        (output/'selection.json').write_text(json.dumps(result,indent=2))
        # Joint null bootstrap across eligible rules; no claim of formal FWER.
        eligible=[r for r in rules if r['train_score'] is not None]
        x=np.array([r['training_daily_risk_returns'] for r in eligible]).T
        n=len(training_dates)
        diagnostics={'method':'five-session circular blocks; fixed rules, centered null, maximum studentized mean',
            'caveat':'approximate search diagnostic with 20 sessions; thresholds not retrained inside bootstrap',
            'eligible_rules':len(eligible)}
        if x.size and n>=5:
            scale=x.std(axis=0,ddof=1)/math.sqrt(n)
            valid=scale>1e-12
            score=np.divide(x.mean(axis=0),scale,out=np.zeros(len(scale)),where=valid)
            centered=x-x.mean(axis=0)
            rng=np.random.default_rng(20260906)
            maxima=[]
            for _ in range(20):
                starts=rng.integers(0,n,(100,math.ceil(n/5)))
                indices=((starts[:,:,None]+np.arange(5))%n).reshape(100,-1)[:,:n]
                means=centered[indices].mean(axis=1)
                t=np.divide(means,scale,out=np.zeros_like(means),where=valid)
                maxima.extend(t.max(axis=1).tolist())
            maximum=np.asarray(maxima)
            diagnostics.update(observed_max_t=float(score.max()),max_null_95=float(np.quantile(maximum,.95)),
                max_stat_pvalue=float((1+np.sum(maximum>=score.max()))/(1+len(maximum))))
            for family,winner in winners.items():
                if winner:
                    index=next(i for i,r in enumerate(eligible) if r['setup']==winner['setup'] and r['rule']==winner['rule'])
                    diagnostics[family]={'winner_t':float(score[index]),'max_stat_adjusted_pvalue':float((1+np.sum(maximum>=score[index]))/(1+len(maximum)))}
        (output/'search-multiplicity.json').write_text(json.dumps(diagnostics,indent=2))
    return result


def research_option_factors(data_root, output, raw_replay_root):
    """Rebuild raw features around previously cash-audited raw-only replay outcomes."""
    output.mkdir(parents=True,exist_ok=False)
    source=Path(__file__).read_bytes()
    (output/'research-source.py').write_bytes(source)
    contract={'raw_parent':str(raw_replay_root),'input':'raw Schwab SPX/SPXW, never cards or push records',
        'outcomes':'reuse frozen audited raw replay; all factors re-read at signal, not delayed entry',
        'families':['condor','butterfly'],'factor_registry':FACTOR_HYPOTHESES,
        'selection':'July-only threshold quantiles and rule selection; Aug-Sep fixed evaluation, explored history not untouched OOS',
        'fees':'existing exact quantity crossed-BBO cash labels; missing stays missing',
        'rv_clock':'5/15/30 observed minute closes; variance per one-minute difference (4/14/29 returns), no unobserved opening price',
        'bark_access':False,'automatic_ordering':False,'script_sha256':hashlib.sha256(source).hexdigest()}
    (output/'contract.json').write_text(json.dumps(contract,indent=2))
    rows=[json.loads(line) for line in (raw_replay_root/'rows.jsonl').read_text().splitlines()]
    rows=[r for r in rows if r['provider']=='schwab' and r['family'] in ('condor','butterfly')]
    con=duckdb.connect(config={'threads':2,'memory_limit':'768MB'})
    con.execute("SET TimeZone='UTC'")
    inputs=set()
    for ds in sorted({r['session_date'] for r in rows}):
        day=date.fromisoformat(ds)
        group=[r for r in rows if r['session_date']==ds]
        files=_files(data_root/'lake/quotes/schema=v1','schwab',_at(day,9,30),_at(day,16,0))
        inputs.update(files)
        path=_underlier_minutes(con,files,'index:SPX',_at(day,9,30),_at(day,16,0)) if files else {}
        times=sorted({datetime.fromisoformat(r['signal_at'])-timedelta(minutes=n) for r in group if r.get('signal_at') for n in (0,5,15)})
        books=_snapshots(con,files,day,times,'schwab',15) if files else {t:{} for t in times}
        cache={}
        for r in group:
            at=datetime.fromisoformat(r['signal_at'])
            key=(r['signal_name'],tuple((leg['strike'],leg['right']) for leg in r.get('legs',[])))
            if key not in cache:
                cache[key]=_raw_option_factors(r,path,books[at],books[at-timedelta(minutes=5)],books[at-timedelta(minutes=15)],at)
            r['factors']=cache[key]
            r['factor_at']=r['signal_at']
            with (output/'rows.jsonl').open('a') as f:
                f.write(json.dumps(r,default=str)+'\n')
        print(ds,'factor rows',len(group),flush=True)
    (output/'input-files.json').write_text(json.dumps(sorted(inputs),indent=2))
    (output/'coverage.json').write_text(json.dumps({name:{'rows':sum(_finite(r['factors'][name]) for r in rows),
        'unique_sessions':len({r['session_date'] for r in rows if _finite(r['factors'][name])})} for name in FACTOR_HYPOTHESES},indent=2))
    con.close()
    evaluate_option_factor_rules(rows,output)


def _causal_regime(path, at, baseline_variance):
    values=_window(path,at,16)
    if not values:
        return dict(state='UNKNOWN',expansion_ratio=None)
    changes=np.diff(values)
    net=float(values[-1]-values[0])
    gross=float(np.abs(changes).sum())
    efficiency=abs(net)/gross if gross else 0.
    state=('TREND_UP' if net>0 else 'TREND_DOWN') if efficiency>=.55 and abs(net)>=3 else 'BALANCED' if efficiency<=.35 else 'MIXED'
    ratio=float(np.sqrt(np.mean(changes[-5:]**2)/baseline_variance)) if baseline_variance and baseline_variance>0 else None
    return dict(state=state,net15=net,efficiency15=efficiency,expansion_ratio=ratio)


def _regime_transition_trace(row, path, horizon, *, require_expansion=False):
    """Two observable closes confirm a change; never relabel a prior entry fault."""
    at=row['entry_at'].replace(second=0,microsecond=0)
    initial=_window(path,at,16)
    variance=float(np.mean(np.diff(initial)**2)) if initial else None
    def adverse(fact):
        state=fact['state']
        trend=state.startswith('TREND_') and (row['family']=='condor' or state!='TREND_'+row['direction'])
        return trend and (not require_expansion or fact['expansion_ratio'] is not None and fact['expansion_ratio']>=1.5)
    entry=_causal_regime(path,at,variance)
    previous=_causal_regime(path,at-timedelta(minutes=1),variance)
    result=dict(entry_regime=entry,entry_already_adverse=adverse(entry) and adverse(previous),
        transition_at=None,trace=[])
    if 'UNKNOWN' in (entry['state'],previous['state']):
        return {**result,'status':'ENTRY_REGIME_UNAVAILABLE'}
    armed=not result['entry_already_adverse']
    safe,streak=0,0
    at+=timedelta(minutes=1)
    while at<=horizon:
        fact=_causal_regime(path,at,variance)
        result['trace'].append(dict(at=at,**fact))
        if fact['state']=='UNKNOWN':
            return {**result,'status':'REGIME_PATH_GAP','gap_at':at}
        bad=adverse(fact)
        if not armed:
            safe=safe+1 if not bad else 0
            if safe>=2:
                armed=True
        else:
            streak=streak+1 if bad else 0
            if streak>=2:
                return {**result,'status':'TRANSITION','transition_at':at,'transition_regime':fact}
        at+=timedelta(minutes=1)
    return {**result,'status':'NO_TRANSITION_BEFORE_BASELINE_EXIT'}


def research_regime_transitions(data_root,output,raw_replay_root):
    output.mkdir(parents=True,exist_ok=False)
    source=Path(__file__).read_bytes()
    (output/'research-source.py').write_bytes(source)
    (output/'contract.json').write_text(json.dumps(dict(
        question='Does an observable regime change precede losses, and would a delayed cash exit help?',
        cohort='all45 dates, three frozen daily primary hypotheses from raw option-mechanisms replay; no cards',
        states='15 full one-minute differences; TREND efficiency>=.55 and abs net>=3 SPX points; BALANCED efficiency<=.35; otherwise MIXED; missing UNKNOWN',
        volatility='last5 mean squared SPX changes / frozen entry15 mean squared changes, square-root ratio>=1.5',
        detectors=['two confirmed adverse TREND closes','same plus volatility expansion on both closes'],
        adverse='IC any trend; BF trend opposite the intended return/continuation',
        entry='already adverse on two closes is reported as entry mismatch, not a later transition; rearm only after two nonadverse closes',
        counterfactual='only alerts before original exit trigger; delay15s, first fresh exact BBO within60s; earlier baseline exit wins; no future gap repair',
        evaluation='all alerts including losers saved and winners damaged; same-date paired cash, July/AugSep, unavailable labels retained; explored data not new OOS',
        scope='transparent raw path regime proxy, not reconstructed production HMM or causal proof of macro news',
        parent=str(raw_replay_root),parent_sha256=hashlib.sha256((raw_replay_root/'rows.jsonl').read_bytes()).hexdigest(),
        script_sha256=hashlib.sha256(source).hexdigest(),bark_access=False,automatic_ordering=False),indent=2))
    parent=[json.loads(line) for line in (raw_replay_root/'rows.jsonl').read_text().splitlines()]
    primary=_evaluate_mechanism_replay(parent)['primary']
    cohort=[dict(row,primary=mechanism) for mechanism,group in primary.items() for row in group['decisions']]
    con=duckdb.connect(config={'threads':2,'memory_limit':'768MB'})
    con.execute("SET TimeZone='UTC'")
    results,inputs=[],set()
    for day in sorted({r['session_date'] for r in cohort}):
        d=date.fromisoformat(day)
        files=_files(data_root/'lake/quotes/schema=v1','schwab',_at(d,9,30),_at(d,16,0))
        inputs.update(files)
        path=_underlier_minutes(con,files,'index:SPX',_at(d,9,30),_at(d,16,0)) if files else {}
        plans=[]
        for raw in [r for r in cohort if r['session_date']==day]:
            row={**raw}
            if row.get('entry_at'):
                row['entry_at']=datetime.fromisoformat(row['entry_at'])
                for leg in row['legs']:
                    for key in ('quote_time','received_at'):
                        leg[key]=datetime.fromisoformat(leg[key])
            for expansion in (False,True):
                out=dict(baseline=raw,detector='trend_expansion' if expansion else 'trend',primary=raw['primary'],session_date=day)
                if not row.get('entry_at'):
                    out.update(status='NOT_ENTERED',pnl_usd=None)
                else:
                    horizon=datetime.fromisoformat(row['exit_trigger_at'])
                    trace=_regime_transition_trace(row,path,horizon,require_expansion=expansion)
                    out.update(trace,status=trace['status'],pnl_usd=row.get('pnl_usd'),counterfactual_status=row['status'])
                    if trace['status'] in ('ENTRY_REGIME_UNAVAILABLE','REGIME_PATH_GAP'):
                        out.update(pnl_usd=None,counterfactual_status=trace['status'])
                plans.append((row,out))
        alerts=[(r,o) for r,o in plans if o.get('transition_at') and o['transition_at']<datetime.fromisoformat(r['exit_trigger_at'])]
        actions=sorted({o['transition_at']+timedelta(seconds=15) for _,o in alerts})
        books=_snapshots(con,files,d,actions,'schwab',15) if actions else {}
        events=_contract_events(con,files,sorted({leg['instrument_id'] for r,_ in alerts for leg in r['legs']}),
            min(actions),max(actions)+timedelta(seconds=60),'schwab') if actions else {}
        for row,out in plans:
            if out.get('transition_at') and out['transition_at']<datetime.fromisoformat(row['exit_trigger_at']):
                action=out['transition_at']+timedelta(seconds=15)
                legs=[books[action].get((leg['strike'],leg['right'])) for leg in row['legs']]
                cash=_cash_quote(legs,row['quantities'],action,15,2)
                marks=_package_path(row,events,action+timedelta(seconds=60),15,2)
                if cash is not None and _depth(legs,row['quantities'],liquidate=True):
                    value=cash[1]+(2*row['entry_price'] if row['family']=='condor' else 0)
                    marks=sorted([m for m in marks if m.at!=action]+[PolicyMark(action,value)],key=lambda m:m.at)
                intent=dict(at=out['transition_at'],reason='regime_transition',censored=False)
                alternative=_action_exit_label(row,marks,intent,_at(d,15,59) if row['family']=='butterfly' else _at(d,15,45))
                out.update(counterfactual=alternative,counterfactual_status=alternative['status'],pnl_usd=alternative['pnl_usd'])
            results.append(out)
            with (output/'rows.jsonl').open('a') as h:
                h.write(json.dumps(out,default=str)+'\n')
        print(json.dumps(dict(day=day,alerts=len(alerts))),flush=True)
    (output/'input-files.json').write_text(json.dumps(sorted(inputs),indent=2))
    con.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=Path("/srv/data/spx-spark/data"))
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument(
        "--providers", nargs="+", choices=("ibkr", "schwab"), default=["schwab", "ibkr"]
    )
    parser.add_argument("--deduplicate-only", action="store_true")
    parser.add_argument("--attribution", action="store_true")
    parser.add_argument("--close-model-attribution", action="store_true")
    parser.add_argument("--resume-dedup", action="store_true")
    parser.add_argument("--environment-from-raw-replay", type=Path)
    parser.add_argument("--validate-directional-signal", action="store_true")
    parser.add_argument("--scan-condor-volatility", action="store_true")
    parser.add_argument("--relax-exit-contracts", action="store_true")
    parser.add_argument("--explore-price-exits", action="store_true")
    parser.add_argument("--explore-position-signals", action="store_true")
    parser.add_argument("--explore-mechanism-signals", action="store_true")
    parser.add_argument("--research-option-factors", type=Path)
    parser.add_argument("--research-regime-transitions", type=Path)
    parser.add_argument("--explore-signal", choices=['or15_up','or15_down','reclaim_up','reclaim_down',
        'butterfly_1400','butterfly_1430','butterfly_1500','ic20_w10','ic20_w20'])
    parser.add_argument("--exit-latency-seconds",type=int,choices=(15,30,60),default=15)
    parser.add_argument("--close-centers", type=Path)
    parser.add_argument("--signal-entry-delay-seconds", type=int, choices=(15, 30, 60), default=15)
    args = parser.parse_args()
    if args.research_regime_transitions:
        research_regime_transitions(args.data_root,args.output_root,args.research_regime_transitions)
        return
    if args.research_option_factors:
        research_option_factors(args.data_root,args.output_root,args.research_option_factors)
        return
    if args.explore_price_exits or args.explore_position_signals or args.explore_mechanism_signals:
        if args.start is None or args.end is None:
            parser.error('price exit exploration requires explicit --start and --end')
        explore_price_exits(args.data_root,args.output_root,args.start,args.end,args.providers,
            signal_filter=args.explore_signal,entry_delay_seconds=args.signal_entry_delay_seconds,
            exit_latency_seconds=args.exit_latency_seconds,position_signals=args.explore_position_signals,
            mechanism_signals=args.explore_mechanism_signals)
        return
    if args.relax_exit_contracts:
        if args.start is None or args.end is None:
            parser.error('exit ablation requires explicit --start and --end')
        relax_exit_contracts(args.data_root, args.output_root, args.start, args.end, args.providers, args.close_centers)
        return
    if args.scan_condor_volatility:
        if args.start is None or args.end is None:
            parser.error('condor scan requires explicit --start and --end')
        scan_condor_volatility(args.data_root, args.output_root, args.start, args.end, args.providers)
        return
    if args.validate_directional_signal:
        if args.start is None or args.end is None:
            parser.error('signal validation requires explicit --start and --end')
        validate_directional_signal(args.data_root, args.output_root, args.start, args.end, args.providers,
                                    entry_delay_seconds=args.signal_entry_delay_seconds)
        return
    if args.environment_from_raw_replay:
        environment_attribution(args.data_root, args.output_root, args.environment_from_raw_replay)
        return
    if args.close_model_attribution:
        if args.start is None or args.end is None:
            parser.error("close model study requires --start and --end")
        close_model_attribution(args.data_root, args.output_root, args.start, args.end)
        return
    if args.deduplicate_only:
        if args.start is None or args.end is None:
            parser.error("deduplication requires explicit --start and --end")
        deduplicate_lake(
            args.data_root,
            args.output_root,
            args.start,
            args.end,
            args.providers,
            resume=args.resume_dedup,
        )
        return
    run(
        args.data_root,
        args.output_root,
        start=args.start,
        end=args.end,
        providers=tuple(args.providers),
        attribution=args.attribution,
    )


if __name__ == "__main__":
    main()
