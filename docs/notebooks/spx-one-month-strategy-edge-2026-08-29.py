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

from spx_spark.analytics.greeks.black_scholes import bs_delta, bs_price

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
        and received <= at
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


def _snapshots(con, files, day: date, times: Sequence[datetime], provider: str, age: float):
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
    parser.add_argument("--close-centers", type=Path)
    parser.add_argument("--signal-entry-delay-seconds", type=int, choices=(15, 30, 60), default=15)
    args = parser.parse_args()
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
