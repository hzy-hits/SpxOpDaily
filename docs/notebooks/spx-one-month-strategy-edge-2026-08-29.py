"""Raw IBKR/Schwab history -> fixed price hypotheses -> exact-leg cash outcomes.

Offline research. No decisions, notifications, model artifacts or old reports are
inputs. Providers stay separate; GTH uses IBKR ES and IBKR SPXW. These price-based
research baselines do not grant production strategy authority.
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

from spx_spark.analytics.options.strategy_payoff import (
    CLOSE_CONVERGENCE_BUTTERFLY_MANAGEMENT_POLICY,
    DEFAULT_MANAGEMENT_POLICY,
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
    rows = con.execute(
        """
        WITH ticks AS (
          SELECT date_trunc('minute',received_at)+INTERVAL 1 MINUTE AS bucket_at,
            instrument_id,received_at,quality,market_data_type,
            CASE WHEN bid>0 AND ask>=bid THEN (bid+ask)/2 ELSE coalesce(last,effective_price) END AS price,
            CASE WHEN bid>0 AND ask>=bid THEN quote_time ELSE coalesce(trade_time,quote_time) END AS source
          FROM read_parquet(?,union_by_name=true)
          WHERE (instrument_id=? OR (?='future:ES' AND starts_with(instrument_id,'future:ES:')))
            AND received_at>=? AND received_at<?
        ) SELECT bucket_at,instrument_id,arg_max(struct_pack(price:=price,source:=source,received_at:=received_at,
             quality:=quality,market_data_type:=market_data_type),received_at) AS tick
        FROM ticks GROUP BY bucket_at,instrument_id ORDER BY bucket_at,instrument_id
    """,
        [files, instrument, instrument, start, end],
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
    values = ",".join("(?)" for _ in times)
    rows = con.execute(
        f"""
        WITH wanted(decision_at) AS (VALUES {values}), latest AS (
          SELECT w.decision_at,q.instrument_id,q.strike,q."right",q.bid,q.ask,q.bid_size,q.ask_size,
            q.quote_time,q.received_at,q.quality,q.market_data_type,q.delta,q.implied_vol,
            row_number() OVER(PARTITION BY w.decision_at,q.instrument_id ORDER BY q.received_at DESC) AS n
          FROM wanted w JOIN read_parquet(?,union_by_name=true) q
            ON q.received_at<=w.decision_at AND q.received_at>=w.decision_at-INTERVAL '{int(age)} seconds'
          WHERE q.trading_class='SPXW' AND q.expiry=?
            AND (q.provider!='schwab' OR q.greeks_model='schwab_stream')
        ) SELECT * EXCLUDE(n) FROM latest WHERE n=1 ORDER BY decision_at,instrument_id
    """,
        [*times, files, day],
    ).fetchall()
    result = {at: {} for at in times}
    names = [d[0] for d in con.description][1:]
    for at, *values in rows:
        leg = dict(zip(names, values))
        leg["provider"] = provider
        result[at][(leg["strike"], leg["right"])] = leg
    return result


def _structure(signal, chain, *, age: float, skew: float):
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
    rows = con.execute(
        """
        SELECT instrument_id,strike,"right",bid,ask,bid_size,ask_size,quote_time,
               received_at,quality,market_data_type
        FROM read_parquet(?,union_by_name=true)
        WHERE instrument_id IN (SELECT unnest(?)) AND received_at BETWEEN ? AND ?
          AND (provider!='schwab' OR greeks_model='schwab_stream')
        ORDER BY received_at,instrument_id
    """,
        [files, sorted(set(ids)), start, end],
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


def _quality(con, files, day, start, end):
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
        FROM read_parquet(?,union_by_name=true) WHERE trading_class='SPXW' AND expiry=?
          AND received_at>=? AND received_at<?
    """,
        [files, day, start, end],
    ).fetchone()
    return dict(zip([d[0] for d in con.description], result))


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
    contract = {
        **CONTRACT,
        "dates": [str(day) for day in days],
        "providers": list(providers),
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
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
    result = {
        "contract": contract,
        "coverage": coverage,
        "results": _summary(all_rows),
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=Path("/srv/data/spx-spark/data"))
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument(
        "--providers", nargs="+", choices=("ibkr", "schwab"), default=["schwab", "ibkr"]
    )
    args = parser.parse_args()
    run(
        args.data_root,
        args.output_root,
        start=args.start,
        end=args.end,
        providers=tuple(args.providers),
    )


if __name__ == "__main__":
    main()
