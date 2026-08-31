"""Causal one-month SPXW strategy comparison using exact Schwab BBOs.

This is an offline research artifact.  It deliberately ignores persisted
strategy decisions and policy versions when creating entries.  Price signals
come from the causal SPX minute path; option structures are entered at the
exact conservative package ask/credit and managed from minute-end exact BBOs.
"""

from __future__ import annotations

import argparse
import bisect
import json
import math
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

import duckdb
import numpy as np

from spx_spark.analytics.options.strategy_payoff import (
    DEFAULT_MANAGEMENT_POLICY,
    RTH_IRON_CONDOR_MANAGEMENT_POLICY,
    PolicyMark,
    conservative_iron_condor_bbo,
    conservative_vertical_bbo,
    iron_condor_economics,
    simulate_management_policy,
    vertical_economics,
)


UTC = timezone.utc
ET = ZoneInfo("America/New_York")
START = date(2026, 7, 29)
END = date(2026, 8, 28)
INCOMPLETE_SESSIONS = {date(2026, 8, 18)}
MAX_QUOTE_AGE_SECONDS = 15.0
MAX_SOURCE_SKEW_SECONDS = 2.0
MAX_POLICY_GAP_SECONDS = 90.0
IC_SCAN_TIMES = ((9, 45), (10, 0), (10, 15), (10, 30), (10, 45), (11, 0))
IC_CONTEXT_TIMES = ((9, 35), (9, 40), *IC_SCAN_TIMES)
IC_TARGET_DELTAS = (0.15, 0.175, 0.20)
IC_CREDIT_FLOORS = (0.20, 0.23, 0.25)
IC_STATE_FILTERS = ("none", "path_balance", "straddle_path_balance")
VERTICAL_WIDTHS = (10.0, 15.0, 20.0)
VERTICAL_LONG_DELTAS = (0.40, 0.50, 0.60)
FEE_DOLLARS_PER_LEG_PER_SIDE = DEFAULT_MANAGEMENT_POLICY.fees_per_leg_per_side
DEVELOPMENT_END = date(2026, 8, 12)
VALIDATION_END = date(2026, 8, 21)
RNG_SEED = 20260829


def _finite(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


def _json_safe(value: object) -> object:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (datetime, date)):
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
    local = datetime.combine(day, time(hour, minute), tzinfo=ET)
    return local.astimezone(UTC)


def _day_files(data_root: Path, day: date) -> list[str]:
    root = (
        data_root
        / "lake"
        / "quotes"
        / "schema=v1"
        / f"date={day.isoformat()}"
        / "provider=schwab"
    )
    return [str(path) for path in sorted(root.glob("hour=*/quotes.parquet"))]


def _trading_days(data_root: Path) -> list[date]:
    root = data_root / "lake" / "quotes" / "schema=v1"
    days = []
    for path in sorted(root.glob("date=2026-*/provider=schwab")):
        try:
            day = date.fromisoformat(path.parent.name.removeprefix("date="))
        except ValueError:
            continue
        if START <= day <= END and day.weekday() < 5 and day not in INCOMPLETE_SESSIONS:
            days.append(day)
    return sorted(set(days))


def _load_spot_minutes(
    connection: duckdb.DuckDBPyConnection,
    files: Sequence[str],
    day: date,
) -> tuple[list[datetime], list[float]]:
    start = _utc_at(day, 9, 30) - timedelta(minutes=1)
    end = _utc_at(day, 15, 46)
    rows = connection.execute(
        """
        WITH filtered AS (
          SELECT
            date_trunc('minute', received_at) + INTERVAL 1 MINUTE AS available_at,
            received_at,
            effective_price
          FROM read_parquet(?, union_by_name=true)
          WHERE instrument_id = 'index:SPX'
            AND quality = 'live'
            AND effective_price > 0
            AND received_at >= ? AND received_at < ?
        )
        SELECT available_at, arg_max(effective_price, received_at) AS price
        FROM filtered
        GROUP BY available_at
        ORDER BY available_at
        """,
        [list(files), start, end],
    ).fetchall()
    times = [row[0].astimezone(UTC) for row in rows]
    prices = [float(row[1]) for row in rows]
    return times, prices


def _price_at(
    times: Sequence[datetime], prices: Sequence[float], at: datetime
) -> float | None:
    index = bisect.bisect_right(times, at) - 1
    return prices[index] if index >= 0 else None


def _window_prices(
    times: Sequence[datetime],
    prices: Sequence[float],
    end: datetime,
    minutes: int,
) -> list[float]:
    left = bisect.bisect_left(times, end - timedelta(minutes=minutes))
    right = bisect.bisect_right(times, end)
    return [float(value) for value in prices[left:right]]


def _iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _load_chain_snapshots(
    connection: duckdb.DuckDBPyConnection,
    files: Sequence[str],
    day: date,
    requested: Sequence[datetime],
) -> dict[datetime, dict[str, Any]]:
    unique = sorted(set(requested))
    if not unique:
        return {}
    values_sql = ",".join("(?)" for _ in unique)
    expiry_token = day.strftime("%Y%m%d")
    rows = connection.execute(
        f"""
        WITH requested(requested_at) AS (VALUES {values_sql}), ranked AS (
          SELECT
            requested.requested_at,
            q.instrument_id,
            q.instrument_type,
            q.strike,
            q."right",
            q.bid,
            q.ask,
            q.delta,
            q.implied_vol,
            q.effective_price,
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
          WHERE q.quality = 'live'
            AND q.source_at IS NOT NULL
            AND q.source_at <= requested.requested_at
            AND (
              q.instrument_id = 'index:SPX'
              OR (q.instrument_type = 'option' AND contains(q.instrument_id, ?))
            )
        )
        SELECT requested_at, instrument_id, instrument_type, strike, "right", bid, ask,
               delta, implied_vol, effective_price, source_at, received_at
        FROM ranked WHERE position = 1
        ORDER BY requested_at, instrument_id
        """,
        [*unique, list(files), expiry_token],
    ).fetchall()
    snapshots: dict[datetime, dict[str, Any]] = {
        at: {"spot": None, "chain": {}} for at in unique
    }
    for (
        at,
        instrument_id,
        instrument_type,
        strike,
        right,
        bid,
        ask,
        delta,
        implied_vol,
        effective_price,
        source_at,
        received_at,
    ) in rows:
        available_at = at.astimezone(UTC)
        snapshot = snapshots[available_at]
        if instrument_id == "index:SPX":
            snapshot["spot"] = _finite(effective_price)
            continue
        if instrument_type != "option" or strike is None or right not in {"C", "P"}:
            continue
        snapshot["chain"][(float(strike), str(right))] = {
            "contract_id": str(instrument_id),
            "strike": float(strike),
            "right": str(right),
            "bid": _finite(bid),
            "ask": _finite(ask),
            "delta": _finite(delta),
            "implied_vol": _finite(implied_vol),
            "source_at": _iso_utc(source_at),
            "received_at": _iso_utc(received_at),
            "provider": "schwab",
        }
    return snapshots


def _nearest_delta(
    chain: Mapping[tuple[float, str], Mapping[str, Any]],
    *,
    right: str,
    target: float,
    at_or_below: bool,
) -> dict[str, Any] | None:
    rows = []
    for leg in chain.values():
        if leg.get("right") != right:
            continue
        delta = _finite(leg.get("delta"))
        if delta is None:
            continue
        absolute = abs(delta)
        if at_or_below and not 0.05 <= absolute <= target:
            continue
        rows.append(dict(leg))
    if not rows:
        return None
    return min(rows, key=lambda leg: abs(abs(float(leg["delta"])) - target))


def _atm_straddle(snapshot: Mapping[str, Any]) -> float | None:
    spot = _finite(snapshot.get("spot"))
    chain = snapshot.get("chain")
    if spot is None or not isinstance(chain, Mapping) or not chain:
        return None
    strikes = sorted({float(key[0]) for key in chain})
    strike = min(strikes, key=lambda value: abs(value - spot))
    call = chain.get((strike, "C"))
    put = chain.get((strike, "P"))
    if not isinstance(call, Mapping) or not isinstance(put, Mapping):
        return None
    values = []
    for leg in (call, put):
        bid, ask = _finite(leg.get("bid")), _finite(leg.get("ask"))
        if bid is None or ask is None or bid < 0 or ask < bid:
            return None
        values.append(0.5 * (bid + ask))
    return sum(values)


def _path_state(
    times: Sequence[datetime], prices: Sequence[float], at: datetime
) -> dict[str, Any]:
    last_15 = _window_prices(times, prices, at, 15)
    last_5 = _window_prices(times, prices, at, 5)
    prior_10 = _window_prices(times, prices, at - timedelta(minutes=5), 10)
    changes_15 = np.diff(np.asarray(last_15, dtype=float))
    changes_5 = np.diff(np.asarray(last_5, dtype=float))
    changes_prior = np.diff(np.asarray(prior_10, dtype=float))
    gross = float(np.abs(changes_15).sum()) if len(changes_15) else 0.0
    efficiency = (
        abs(float(last_15[-1] - last_15[0])) / gross
        if len(last_15) >= 2 and gross > 1e-9
        else None
    )
    rv_5 = float(np.mean(np.square(changes_5))) if len(changes_5) >= 3 else None
    rv_prior = (
        float(np.mean(np.square(changes_prior))) if len(changes_prior) >= 5 else None
    )
    return {
        "efficiency_15m": efficiency,
        "rv_5m": rv_5,
        "rv_prior_10m": rv_prior,
        "balance": efficiency is not None and efficiency <= 0.55,
        "rv_contraction": (
            rv_5 is not None and rv_prior is not None and rv_5 <= 0.75 * rv_prior
        ),
    }


def _direction_signals(
    day: date, times: Sequence[datetime], prices: Sequence[float]
) -> list[dict[str, Any]]:
    opening_end = _utc_at(day, 9, 45)
    opening = _window_prices(times, prices, opening_end, 15)
    if len(opening) < 12:
        return []
    opening_high, opening_low = max(opening), min(opening)
    opening_width = opening_high - opening_low
    signals: list[dict[str, Any]] = []
    accepted: dict[str, Any] | None = None
    scan_start, scan_end = _utc_at(day, 9, 48), _utc_at(day, 12, 30)
    for at in times:
        if not scan_start <= at <= scan_end:
            continue
        last_three = _window_prices(times, prices, at, 3)
        if len(last_three) < 3:
            continue
        direction = None
        boundary = None
        if min(last_three[-3:]) >= opening_high + 1.0:
            direction, boundary = "up", opening_high
        elif max(last_three[-3:]) <= opening_low - 1.0:
            direction, boundary = "down", opening_low
        if direction is not None:
            accepted = {
                "setup": "or15_accept_3m",
                "direction": direction,
                "entry_at": at,
                "boundary": boundary,
                "opening_width": opening_width,
            }
            signals.append(accepted)
            break

    for hour, minute in ((10, 0), (10, 30), (11, 0)):
        at = _utc_at(day, hour, minute)
        window = _window_prices(times, prices, at, 15)
        if len(window) < 12:
            continue
        changes = np.diff(np.asarray(window, dtype=float))
        gross = float(np.abs(changes).sum())
        movement = float(window[-1] - window[0])
        efficiency = abs(movement) / gross if gross > 1e-9 else 0.0
        threshold = max(3.0, 0.5 * opening_width)
        if abs(movement) >= threshold and efficiency >= 0.55:
            signals.append(
                {
                    "setup": "momentum15_eff55",
                    "direction": "up" if movement > 0 else "down",
                    "entry_at": at,
                    "movement_15m": movement,
                    "efficiency_15m": efficiency,
                    "opening_width": opening_width,
                }
            )
            break

    if accepted is not None:
        direction = str(accepted["direction"])
        boundary = float(accepted["boundary"])
        pulled_back = False
        resume_start = accepted["entry_at"] + timedelta(minutes=3)
        resume_end = _utc_at(day, 13, 30)
        for at in times:
            if not resume_start <= at <= resume_end:
                continue
            price = _price_at(times, prices, at)
            if price is None:
                continue
            if direction == "up":
                if boundary - 3.0 <= price <= boundary + 2.0:
                    pulled_back = True
                resumed = price >= boundary + 2.0
            else:
                if boundary - 2.0 <= price <= boundary + 3.0:
                    pulled_back = True
                resumed = price <= boundary - 2.0
            recent = _window_prices(times, prices, at, 2)
            aligned = (
                len(recent) >= 2
                and ((recent[-1] > recent[-2]) if direction == "up" else (recent[-1] < recent[-2]))
            )
            if pulled_back and resumed and aligned:
                signals.append(
                    {
                        "setup": "or15_pullback_resume",
                        "direction": direction,
                        "entry_at": at,
                        "boundary": boundary,
                        "opening_width": opening_width,
                    }
                )
                break
    return signals


def _vertical_candidates(
    day: date,
    signal: Mapping[str, Any],
    snapshot: Mapping[str, Any],
) -> list[dict[str, Any]]:
    direction = str(signal["direction"])
    right = "C" if direction == "up" else "P"
    chain = snapshot.get("chain")
    if not isinstance(chain, Mapping):
        return []
    at = signal["entry_at"]
    rows = []
    for target_delta in VERTICAL_LONG_DELTAS:
        long_leg = _nearest_delta(
            chain, right=right, target=target_delta, at_or_below=False
        )
        if long_leg is None:
            continue
        for width in VERTICAL_WIDTHS:
            short_strike = float(long_leg["strike"]) + (
                width if right == "C" else -width
            )
            short_leg = chain.get((short_strike, right))
            if not isinstance(short_leg, Mapping):
                continue
            quote = conservative_vertical_bbo(
                long_leg,
                short_leg,
                now=at,
                max_quote_age_seconds=MAX_QUOTE_AGE_SECONDS,
                max_source_skew_seconds=MAX_SOURCE_SKEW_SECONDS,
            )
            if quote.get("status") != "ready":
                continue
            try:
                economics = vertical_economics(
                    long_strike=float(long_leg["strike"]),
                    short_strike=float(short_leg["strike"]),
                    net_debit=float(quote["ask"]),
                    right=right,
                )
            except ValueError:
                continue
            if float(economics["debit_fraction_of_width"]) > 0.45:
                continue
            rows.append(
                {
                    "family": "directional_vertical",
                    "setup": signal["setup"],
                    "day": day,
                    "entry_at": at,
                    "direction": direction,
                    "right": right,
                    "target_delta": target_delta,
                    "actual_long_delta": abs(float(long_leg["delta"])),
                    "width": width,
                    "legs": [dict(long_leg), dict(short_leg)],
                    "contract_ids": [
                        str(long_leg["contract_id"]),
                        str(short_leg["contract_id"]),
                    ],
                    "strikes": [
                        float(long_leg["strike"]),
                        float(short_leg["strike"]),
                    ],
                    "entry_value": float(quote["ask"]),
                    "economics": economics,
                    "signal": dict(signal),
                }
            )
    return rows


def _iron_condor_candidates(
    day: date,
    snapshots: Mapping[datetime, Mapping[str, Any]],
    path_times: Sequence[datetime],
    path_prices: Sequence[float],
) -> list[dict[str, Any]]:
    straddles = {
        at: _atm_straddle(snapshots[at])
        for at in sorted(snapshots)
        if at in {_utc_at(day, h, m) for h, m in IC_CONTEXT_TIMES}
    }
    rows = []
    for hour, minute in IC_SCAN_TIMES:
        at = _utc_at(day, hour, minute)
        snapshot = snapshots.get(at)
        if not isinstance(snapshot, Mapping):
            continue
        spot = _finite(snapshot.get("spot")) or _price_at(path_times, path_prices, at)
        chain = snapshot.get("chain")
        if spot is None or not isinstance(chain, Mapping):
            continue
        current_straddle = straddles.get(at)
        prior_straddles = [
            value
            for observed_at, value in straddles.items()
            if observed_at < at and value is not None
        ]
        straddle_contraction = (
            current_straddle is not None
            and prior_straddles
            and current_straddle <= 0.97 * max(prior_straddles)
        )
        state = _path_state(path_times, path_prices, at)
        state["atm_straddle"] = current_straddle
        state["straddle_contraction"] = bool(straddle_contraction)
        state["path_balance"] = bool(state["balance"] and state["rv_contraction"])
        state["straddle_path_balance"] = bool(
            straddle_contraction and state["balance"] and state["rv_contraction"]
        )
        for target in IC_TARGET_DELTAS:
            put_short = _nearest_delta(
                chain, right="P", target=target, at_or_below=True
            )
            call_short = _nearest_delta(
                chain, right="C", target=target, at_or_below=True
            )
            if put_short is None or call_short is None:
                continue
            put_long = chain.get((float(put_short["strike"]) - 10.0, "P"))
            call_long = chain.get((float(call_short["strike"]) + 10.0, "C"))
            if not isinstance(put_long, Mapping) or not isinstance(call_long, Mapping):
                continue
            if not float(put_short["strike"]) < spot < float(call_short["strike"]):
                continue
            legs = [dict(put_long), put_short, call_short, dict(call_long)]
            quote = conservative_iron_condor_bbo(
                *legs,
                now=at,
                max_quote_age_seconds=MAX_QUOTE_AGE_SECONDS,
                max_source_skew_seconds=MAX_SOURCE_SKEW_SECONDS,
            )
            if quote.get("status") != "ready":
                continue
            credit = float(quote["credit"])
            try:
                economics = iron_condor_economics(
                    put_long=float(put_long["strike"]),
                    put_short=float(put_short["strike"]),
                    call_short=float(call_short["strike"]),
                    call_long=float(call_long["strike"]),
                    net_credit=credit,
                )
            except ValueError:
                continue
            put_credit = float(put_short["bid"]) - float(put_long["ask"])
            call_credit = float(call_short["bid"]) - float(call_long["ask"])
            side_share = min(put_credit, call_credit) / credit if credit > 0 else -1.0
            rows.append(
                {
                    "family": "iron_condor",
                    "day": day,
                    "entry_at": at,
                    "target_delta": target,
                    "actual_put_delta": abs(float(put_short["delta"])),
                    "actual_call_delta": abs(float(call_short["delta"])),
                    "legs": legs,
                    "contract_ids": [str(leg["contract_id"]) for leg in legs],
                    "strikes": [float(leg["strike"]) for leg in legs],
                    "entry_value": credit,
                    "economics": economics,
                    "credit_fraction": float(economics["credit_fraction_of_width"]),
                    "minimum_side_credit_share": side_share,
                    "state": state,
                }
            )
    return rows


def _load_contract_marks(
    connection: duckdb.DuckDBPyConnection,
    files: Sequence[str],
    contract_ids: Sequence[str],
    earliest: datetime,
    latest: datetime,
) -> dict[str, dict[datetime, dict[str, Any]]]:
    unique = sorted(set(contract_ids))
    if not unique:
        return {}
    rows = connection.execute(
        """
        WITH filtered AS (
          SELECT
            date_trunc('minute', received_at) + INTERVAL 1 MINUTE AS available_at,
            instrument_id, strike, "right", bid, ask, source_at, received_at,
            row_number() OVER (
              PARTITION BY instrument_id,
                           date_trunc('minute', received_at) + INTERVAL 1 MINUTE
              ORDER BY received_at DESC
            ) AS position
          FROM read_parquet(?, union_by_name=true)
          WHERE instrument_id IN (SELECT unnest(?))
            AND quality = 'live'
            AND source_at IS NOT NULL
            AND received_at >= ? - INTERVAL 1 MINUTE
            AND received_at < ?
        )
        SELECT available_at, instrument_id, strike, "right", bid, ask,
               source_at, received_at
        FROM filtered
        WHERE position = 1
        ORDER BY available_at, instrument_id
        """,
        [list(files), unique, earliest, latest + timedelta(minutes=1)],
    ).fetchall()
    result: dict[str, dict[datetime, dict[str, Any]]] = defaultdict(dict)
    for at, instrument_id, strike, right, bid, ask, source_at, received_at in rows:
        available_at = at.astimezone(UTC)
        result[str(instrument_id)][available_at] = {
            "contract_id": str(instrument_id),
            "strike": float(strike),
            "right": str(right),
            "bid": _finite(bid),
            "ask": _finite(ask),
            "source_at": _iso_utc(source_at),
            "received_at": _iso_utc(received_at),
            "provider": "schwab",
        }
    return result


def _candidate_marks(
    candidate: Mapping[str, Any],
    marks: Mapping[str, Mapping[datetime, Mapping[str, Any]]],
    hard_exit: datetime,
) -> list[PolicyMark]:
    entry_at = candidate["entry_at"]
    contract_ids = candidate["contract_ids"]
    rows = []
    at = entry_at + timedelta(minutes=1)
    while at <= hard_exit:
        legs = [marks.get(contract_id, {}).get(at) for contract_id in contract_ids]
        if any(not isinstance(leg, Mapping) for leg in legs):
            at += timedelta(minutes=1)
            continue
        exact_legs = [dict(leg) for leg in legs if isinstance(leg, Mapping)]
        if candidate["family"] == "directional_vertical":
            quote = conservative_vertical_bbo(
                *exact_legs,
                now=at,
                max_quote_age_seconds=MAX_QUOTE_AGE_SECONDS,
                max_source_skew_seconds=MAX_SOURCE_SKEW_SECONDS,
            )
            if quote.get("status") == "ready":
                rows.append(PolicyMark(at=at, combo_bid=float(quote["bid"])))
        else:
            quote = conservative_iron_condor_bbo(
                *exact_legs,
                now=at,
                max_quote_age_seconds=MAX_QUOTE_AGE_SECONDS,
                max_source_skew_seconds=MAX_SOURCE_SKEW_SECONDS,
            )
            if quote.get("status") == "ready":
                signed_value = 2.0 * float(candidate["entry_value"]) - float(quote["ask"])
                rows.append(PolicyMark(at=at, combo_bid=signed_value))
        at += timedelta(minutes=1)
    return rows


def _attach_pnl(
    candidates: Iterable[dict[str, Any]],
    marks: Mapping[str, Mapping[datetime, Mapping[str, Any]]],
) -> None:
    for candidate in candidates:
        hard_exit = _utc_at(candidate["day"], 15, 45)
        policy = (
            DEFAULT_MANAGEMENT_POLICY
            if candidate["family"] == "directional_vertical"
            else RTH_IRON_CONDOR_MANAGEMENT_POLICY
        )
        policy_marks = _candidate_marks(candidate, marks, hard_exit)
        if not policy_marks:
            candidate["pnl_status"] = "no_exit_marks"
            continue
        fixed_exit_at = candidate["entry_at"] + timedelta(minutes=60)
        fixed_mark = next(
            (mark for mark in policy_marks if mark.at == fixed_exit_at),
            None,
        )
        fees_points = (
            FEE_DOLLARS_PER_LEG_PER_SIDE
            * float(len(candidate["contract_ids"]))
            * 2.0
            / 100.0
        )
        if fixed_mark is not None:
            fixed_pnl = (
                float(fixed_mark.combo_bid)
                - float(candidate["entry_value"])
                - fees_points
            )
            candidate["pnl_status"] = "priced"
            candidate["pnl_points"] = fixed_pnl
            candidate["pnl_usd"] = 100.0 * fixed_pnl
            candidate["return_on_entry"] = fixed_pnl / float(candidate["entry_value"])
            candidate["exit_reason"] = "fixed_60m"
            candidate["exit_at"] = fixed_exit_at
        else:
            candidate["pnl_status"] = "fixed_60m_mark_unavailable"
        label = simulate_management_policy(
            policy_marks,
            entry_ask=float(candidate["entry_value"]),
            leg_count=len(candidate["contract_ids"]),
            entry_at=candidate["entry_at"],
            policy=policy,
            session_date=candidate["day"],
        )
        policy_resolved = (
            label.quote_gap_seconds_max <= MAX_POLICY_GAP_SECONDS
            and (
                label.exit_reason not in {"marks_exhausted"}
                or (label.exit_at is not None and label.exit_at >= hard_exit)
            )
        )
        candidate["policy_status"] = "priced" if policy_resolved else "incomplete_exit_marks"
        candidate["policy_pnl_usd"] = (
            100.0 * float(label.policy_pnl_points) if policy_resolved else None
        )
        candidate["policy_exit_reason"] = label.exit_reason
        candidate["policy_exit_at"] = label.exit_at
        candidate["policy_mfe_points"] = label.mfe_points
        candidate["policy_mae_points"] = label.mae_points
        candidate["quote_gap_seconds_max"] = label.quote_gap_seconds_max


def _period(day: date) -> str:
    if day <= DEVELOPMENT_END:
        return "development"
    if day <= VALIDATION_END:
        return "validation"
    return "holdout"


def _cvar10(values: Sequence[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    return mean(ordered[: max(1, math.ceil(0.10 * len(ordered)))])


def _session_bootstrap(rows: Sequence[Mapping[str, Any]]) -> list[float] | None:
    by_day: dict[date, float] = defaultdict(float)
    for row in rows:
        by_day[row["day"]] += float(row["pnl_usd"])
    if len(by_day) < 2:
        return None
    values = np.asarray(list(by_day.values()), dtype=float)
    rng = np.random.default_rng(RNG_SEED)
    sampled = rng.choice(values, size=(10_000, len(values)), replace=True).mean(axis=1)
    return [float(np.quantile(sampled, 0.025)), float(np.quantile(sampled, 0.975))]


def _summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    priced = [row for row in rows if row.get("pnl_status") == "priced"]
    pnls = [float(row["pnl_usd"]) for row in priced]
    policy_priced = [
        row
        for row in rows
        if row.get("policy_status") == "priced"
        and _finite(row.get("policy_pnl_usd")) is not None
    ]
    policy_pnls = [float(row["policy_pnl_usd"]) for row in policy_priced]
    by_day: dict[date, float] = defaultdict(float)
    for row in priced:
        by_day[row["day"]] += float(row["pnl_usd"])
    return {
        "trades": len(priced),
        "sessions": len(by_day),
        "mean_pnl_usd": mean(pnls) if pnls else None,
        "median_pnl_usd": median(pnls) if pnls else None,
        "total_pnl_usd": sum(pnls),
        "win_rate": sum(value > 0 for value in pnls) / len(pnls) if pnls else None,
        "positive_session_rate": (
            sum(value > 0 for value in by_day.values()) / len(by_day) if by_day else None
        ),
        "session_pnl_usd": {
            session.isoformat(): round(value, 2)
            for session, value in sorted(by_day.items())
        },
        "cvar10_usd": _cvar10(pnls),
        "session_bootstrap_95_mean_usd": _session_bootstrap(priced),
        "mean_return_on_entry": (
            mean(float(row["return_on_entry"]) for row in priced) if priced else None
        ),
        "exit_reasons": dict(Counter(str(row.get("exit_reason")) for row in priced)),
        "unpriced": len(rows) - len(priced),
        "policy_resolved": len(policy_priced),
        "policy_unresolved": len(rows) - len(policy_priced),
        "policy_mean_pnl_usd": mean(policy_pnls) if policy_pnls else None,
        "policy_total_pnl_usd": sum(policy_pnls),
        "policy_win_rate": (
            sum(value > 0 for value in policy_pnls) / len(policy_pnls)
            if policy_pnls
            else None
        ),
        "policy_exit_reasons": dict(
            Counter(str(row.get("policy_exit_reason")) for row in policy_priced)
        ),
    }


def _split_summary(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        period: _summary([row for row in rows if _period(row["day"]) == period])
        for period in ("development", "validation", "holdout")
    } | {"all": _summary(rows)}


def _choose_first_ic(
    raw: Sequence[Mapping[str, Any]],
    *,
    target_delta: float,
    credit_floor: float,
    state_filter: str,
) -> list[dict[str, Any]]:
    by_day: dict[date, list[Mapping[str, Any]]] = defaultdict(list)
    for row in raw:
        if not math.isclose(float(row["target_delta"]), target_delta, abs_tol=1e-9):
            continue
        if not credit_floor <= float(row["credit_fraction"]) <= 0.55:
            continue
        if float(row["minimum_side_credit_share"]) < 0.25:
            continue
        if state_filter != "none" and not bool(row["state"].get(state_filter)):
            continue
        by_day[row["day"]].append(row)
    selected = []
    for rows in by_day.values():
        selected.append(dict(min(rows, key=lambda row: row["entry_at"])))
    return sorted(selected, key=lambda row: row["day"])


def _robust_development_score(summary: Mapping[str, Any]) -> float:
    mean_pnl = _finite(summary.get("mean_pnl_usd"))
    cvar = _finite(summary.get("cvar10_usd"))
    if mean_pnl is None or cvar is None:
        return -math.inf
    return mean_pnl + 0.5 * cvar


def _edge_status(periods: Mapping[str, Mapping[str, Any]]) -> str:
    validation = periods["validation"]
    holdout = periods["holdout"]
    all_result = periods["all"]
    if min(int(validation["sessions"]), int(holdout["sessions"])) < 3:
        return "insufficient_support"
    validation_mean = _finite(validation.get("mean_pnl_usd"))
    holdout_mean = _finite(holdout.get("mean_pnl_usd"))
    interval = all_result.get("session_bootstrap_95_mean_usd")
    if validation_mean is None or holdout_mean is None:
        return "insufficient_support"
    if validation_mean <= 0 or holdout_mean <= 0:
        return "not_stable"
    if isinstance(interval, Sequence) and interval and float(interval[0]) > 0:
        return "candidate_edge"
    return "positive_but_unconfirmed"


def _butterfly_reference(repo_root: Path) -> dict[str, Any]:
    path = repo_root / "docs/research/spx-butterfly-cost-buckets-2026-08-29.json"
    if not path.exists():
        return {"status": "unavailable"}
    document = json.loads(path.read_text(encoding="utf-8"))
    return {
        "status": "loaded_existing_exact_bbo_study",
        "data_profile": document.get("data_profile"),
        "existing_60m_lane_update": document.get("existing_60m_lane_update"),
        "cost_bucket_conclusion": document.get("conclusion"),
    }


def run(data_root: Path, repo_root: Path) -> dict[str, Any]:
    connection = duckdb.connect()
    days = _trading_days(data_root)
    all_verticals: list[dict[str, Any]] = []
    all_raw_ic: list[dict[str, Any]] = []
    quality = []
    for day in days:
        files = _day_files(data_root, day)
        if not files:
            continue
        path_times, path_prices = _load_spot_minutes(connection, files, day)
        signals = _direction_signals(day, path_times, path_prices)
        requested = [
            *(_utc_at(day, hour, minute) for hour, minute in IC_CONTEXT_TIMES),
            *(signal["entry_at"] for signal in signals),
        ]
        snapshots = _load_chain_snapshots(connection, files, day, requested)
        verticals = []
        for signal in signals:
            snapshot = snapshots.get(signal["entry_at"], {})
            verticals.extend(_vertical_candidates(day, signal, snapshot))
        raw_ic = _iron_condor_candidates(
            day, snapshots, path_times, path_prices
        )
        candidates = [*verticals, *raw_ic]
        contract_ids = [
            contract_id
            for candidate in candidates
            for contract_id in candidate["contract_ids"]
        ]
        if candidates:
            marks = _load_contract_marks(
                connection,
                files,
                contract_ids,
                min(candidate["entry_at"] for candidate in candidates),
                _utc_at(day, 15, 45),
            )
            _attach_pnl(candidates, marks)
        all_verticals.extend(verticals)
        all_raw_ic.extend(raw_ic)
        quality.append(
            {
                "day": day,
                "period": _period(day),
                "spx_minutes": len(path_times),
                "direction_signals": len(signals),
                "verticals_priced": sum(row.get("pnl_status") == "priced" for row in verticals),
                "raw_ic_candidates": len(raw_ic),
                "raw_ic_priced": sum(row.get("pnl_status") == "priced" for row in raw_ic),
            }
        )
    connection.close()

    vertical_results = {}
    for setup in ("or15_accept_3m", "momentum15_eff55", "or15_pullback_resume"):
        contracts = []
        for target_delta in VERTICAL_LONG_DELTAS:
            for width in VERTICAL_WIDTHS:
                rows = [
                    row
                    for row in all_verticals
                    if row["setup"] == setup
                    and math.isclose(
                        float(row["target_delta"]), target_delta, abs_tol=1e-9
                    )
                    and math.isclose(float(row["width"]), width, abs_tol=1e-9)
                ]
                periods = _split_summary(rows)
                contracts.append(
                    {
                        "target_delta": target_delta,
                        "width": width,
                        "periods": periods,
                        "development_score": _robust_development_score(
                            periods["development"]
                        ),
                        "edge_status": _edge_status(periods),
                    }
                )
        eligible = [
            row
            for row in contracts
            if row["periods"]["development"]["sessions"] >= 5
        ]
        leader = max(
            eligible,
            key=lambda row: (
                float(row["development_score"]),
                -float(row["target_delta"]),
                -float(row["width"]),
            ),
            default=None,
        )
        vertical_results[setup] = {
            "development_frozen_leader": leader,
            "edge_status": (
                leader["edge_status"] if leader is not None else "insufficient_support"
            ),
            "searched_contracts": contracts,
            "top_development_contracts": sorted(
                eligible,
                key=lambda row: float(row["development_score"]),
                reverse=True,
            )[:5],
        }

    ic_results = []
    for target in IC_TARGET_DELTAS:
        for floor in IC_CREDIT_FLOORS:
            for state_filter in IC_STATE_FILTERS:
                rows = _choose_first_ic(
                    all_raw_ic,
                    target_delta=target,
                    credit_floor=floor,
                    state_filter=state_filter,
                )
                periods = _split_summary(rows)
                ic_results.append(
                    {
                        "target_delta": target,
                        "credit_floor": floor,
                        "state_filter": state_filter,
                        "periods": periods,
                        "development_score": _robust_development_score(
                            periods["development"]
                        ),
                        "edge_status": _edge_status(periods),
                    }
                )
    eligible_ic = [
        row for row in ic_results if row["periods"]["development"]["sessions"] >= 5
    ]
    frozen_ic = max(
        eligible_ic,
        key=lambda row: (float(row["development_score"]), -float(row["target_delta"])),
        default=None,
    )
    production_baseline = next(
        (
            row
            for row in ic_results
            if math.isclose(float(row["target_delta"]), 0.20)
            and math.isclose(float(row["credit_floor"]), 0.25)
            and row["state_filter"] == "path_balance"
        ),
        None,
    )
    ranked_ic = sorted(
        eligible_ic,
        key=lambda row: float(row["development_score"]),
        reverse=True,
    )
    butterfly = _butterfly_reference(repo_root)

    family_status = {
        "directional_vertical": {
            key: value["edge_status"] for key, value in vertical_results.items()
        },
        "iron_condor_frozen_on_development": (
            frozen_ic.get("edge_status") if frozen_ic is not None else "insufficient_support"
        ),
        "butterfly_close_convergence": (
            "positive_but_unconfirmed"
            if _finite(
                (butterfly.get("existing_60m_lane_update") or {}).get(
                    "mean_pnl_per_trade_usd"
                )
            )
            and float(
                butterfly["existing_60m_lane_update"]["mean_pnl_per_trade_usd"]
            )
            > 0
            else "not_stable"
        ),
    }
    return _json_safe(
        {
            "schema_version": "spx_one_month_strategy_edge.v1",
            "generated_at": datetime.now(UTC),
            "question": "Which causal SPXW structures showed repeatable net edge over the last complete month?",
            "contract": {
                "window": [START, END],
                "excluded_sessions": sorted(INCOMPLETE_SESSIONS),
                "periods": {
                    "development": [START, DEVELOPMENT_END],
                    "validation": [DEVELOPMENT_END + timedelta(days=1), VALIDATION_END],
                    "holdout": [VALIDATION_END + timedelta(days=1), END],
                },
                "entry": "exact conservative Schwab package ask/credit; available_at<=decision_at",
                "marks": "one-minute exact conservative package BBO; source age<=15s; leg skew<=2s",
                "management_path": "TP/SL labels require uninterrupted exact marks with gaps<=90s",
                "fees_per_contract_per_side_usd": FEE_DOLLARS_PER_LEG_PER_SIDE,
                "directional_vertical": "raw SPX OR15/momentum/pullback signals; 40/50/60-delta long; fixed 10/15/20-wide; exact +60m exit primary; v2 management tracked separately",
                "iron_condor": "first qualifying 09:45-11:00 ET; 15/17.5/20-delta; fixed 10-wide; balanced side credit; TP50/SL200/15:45",
                "butterfly": "reuses frozen causal close-distribution exact-BBO report through 2026-08-28",
                "automatic_ordering": False,
                "authority": "offline_research_only",
            },
            "data_quality": {
                "complete_sessions": len(days),
                "session_rows": quality,
                "future_rows_used": 0,
                "minute_mark_limit": "Intraminute stop touches can be missed; results are minute-decision, not native-tick fills.",
            },
            "directional_vertical": vertical_results,
            "iron_condor": {
                "searched_contracts": ic_results,
                "development_frozen_leader": frozen_ic,
                "production_like_20d_25pct_path_balance": production_baseline,
                "top_development_contracts": ranked_ic[:8],
                "selection_warning": "Leader is selected on development only; validation and holdout are untouched by the ranking.",
            },
            "butterfly": butterfly,
            "decision": {
                "family_status": family_status,
                "production_change_supported": False,
                "reason": "At most five untouched holdout sessions remain; positive lanes require forward confirmation and multiple-comparison control.",
            },
            "limitations": [
                "Twenty-two complete sessions are enough to reject brittle rules, not to prove long-run alpha.",
                "The IC grid compares 27 predeclared contracts; the development leader is exposed to winner's curse.",
                "Minute-end BBOs can miss intraminute stop-outs and do not model queue position or partial fills.",
                "The butterfly reference uses its own expanding causal close-distribution contract and 19 independent OOS sessions.",
                "No persisted production strategy decision or outcome is used to create entries.",
            ],
        }
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path, default=Path("/srv/data/spx-spark/data"))
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[2],
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = run(args.data_root, args.repo_root)
    rendered = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
        print(args.output)
    else:
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
