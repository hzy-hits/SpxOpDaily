"""Causal RTH mining for 0DTE credit spreads, iron condors, and butterflies."""

from __future__ import annotations

import argparse
from bisect import bisect_right
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from hashlib import sha256
import heapq
import itertools
import json
import math
from pathlib import Path
import statistics
from typing import Any

import numpy as np

from spx_spark.analytics.options.strategy_payoff import (
    DEFAULT_MANAGEMENT_POLICY,
    PolicyMark,
    conservative_butterfly_bbo,
    conservative_iron_condor_bbo,
    conservative_vertical_bbo,
    simulate_management_policy,
)
from spx_spark.data_platform.research.odte_level_quotes import QuoteStore
from spx_spark.data_platform.research.odte_level_signals import OptionTick, UnderlierTick
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET, MarketCalendar


SCHEMA_VERSION = "odte_signal_universe.v1"
TRAIN = ("2026-07-06", "2026-07-31")
HOLDOUT = ("2026-08-03", "2026-08-17")
START_DATE = date.fromisoformat(TRAIN[0])
END_DATE = date.fromisoformat(HOLDOUT[1])
SAMPLING_MINUTES = 15
MAX_QUOTE_AGE_SECONDS = 30.0
MAX_LEG_SKEW_SECONDS = 5.0
HARD_MARK_TIME_ET = time(15, 45)
SPX_INSTRUMENT_ID = "index:SPX"
PROVIDER_PRIORITY = ("schwab", "ibkr")
WIDTH = 10.0
STRUCTURES = (
    "call_credit_vertical",
    "put_credit_vertical",
    "call_butterfly",
    "put_butterfly",
    "iron_condor",
)
EXITS = ("hold_1545", "giveback_50")
MODES = ("sampling_points", "session_first", "session_mean")
LCB_Z_90 = 1.6448536269514722


@dataclass(frozen=True, slots=True)
class StructureGeometry:
    name: str
    side: str
    legs: tuple[tuple[float, str], ...]


@dataclass(frozen=True, slots=True)
class RuleSpec:
    name: str
    family: str
    definition: str
    structure: str | None = None
    side: str | None = None
    hour_et: int | None = None
    quantile: str | None = None
    straddle_quantile: str | None = None


@dataclass(slots=True)
class SessionResult:
    rows: list[dict[str, Any]]
    coverage: dict[str, Any]


def _rules() -> tuple[RuleSpec, ...]:
    # fmt: off -- this is the frozen, human-auditable preregistration manifest.
    rules = [
        RuleSpec("call_credit_all", "unconditional", "每个可用 bar 卖 ATM call 10 点 credit vertical", structure="call_credit_vertical"),
        RuleSpec("put_credit_all", "unconditional", "每个可用 bar 卖 ATM put 10 点 credit vertical", structure="put_credit_vertical"),
        RuleSpec("iron_condor_all", "unconditional", "每个可用 bar 卖 ATM 外 15 点、10 点翼铁鹰", structure="iron_condor"),
        RuleSpec("call_fly_all", "unconditional", "每个可用 bar 买 ATM call 10 点 butterfly", structure="call_butterfly"),
        RuleSpec("put_fly_all", "unconditional", "每个可用 bar 买 ATM put 10 点 butterfly", structure="put_butterfly"),
        RuleSpec("credit_expensive_side", "one_side", "每个 bar 只卖 credit_fraction 较贵的一边；同价跳过", side="expensive"),
        RuleSpec("credit_cheaper_side", "one_side", "每个 bar 只卖 credit_fraction 较便宜的一边；同价跳过", side="cheaper"),
        RuleSpec("credit_spot15_same", "direction", "15m SPX 上涨卖 put、下跌卖 call", side="spot15_same"),
        RuleSpec("credit_spot15_reverse", "direction", "15m SPX 上涨卖 call、下跌卖 put", side="spot15_reverse"),
        RuleSpec("call_credit_q5", "premium", "call credit_fraction 位于该结构 train 最高五分位", structure="call_credit_vertical", quantile="q5"),
        RuleSpec("put_credit_q5", "premium", "put credit_fraction 位于该结构 train 最高五分位", structure="put_credit_vertical", quantile="q5"),
        RuleSpec("credit_expensive_q5", "premium", "只卖较贵一边且 credit_fraction 位于其结构 train q5", side="expensive", quantile="q5"),
        RuleSpec("call_credit_q1", "premium", "call credit_fraction 位于该结构 train 最低五分位", structure="call_credit_vertical", quantile="q1"),
        RuleSpec("put_credit_q1", "premium", "put credit_fraction 位于该结构 train 最低五分位", structure="put_credit_vertical", quantile="q1"),
        RuleSpec("credit_cheaper_q1", "premium", "只卖较便宜一边且 credit_fraction 位于其结构 train q1", side="cheaper", quantile="q1"),
        RuleSpec("call_fly_q1", "premium", "call fly debit_fraction 位于该结构 train 最低五分位", structure="call_butterfly", quantile="q1"),
        RuleSpec("put_fly_q1", "premium", "put fly debit_fraction 位于该结构 train 最低五分位", structure="put_butterfly", quantile="q1"),
    ]
    for structure, label in (
        ("call_credit_vertical", "call credit vertical"),
        ("put_credit_vertical", "put credit vertical"),
        ("iron_condor", "iron condor"),
        ("call_butterfly", "call butterfly"),
        ("put_butterfly", "put butterfly"),
    ):
        for hour in range(10, 15):
            rules.append(RuleSpec(f"{structure}_hour_{hour}", "hour", f"仅 ET {hour}:00–{hour}:59 做 {label}", structure=structure, hour_et=hour))
    rules.extend(
        (
            RuleSpec("call_credit_low_straddle", "volatility", "ATM straddle ask 位于 train q1 时卖 call credit", structure="call_credit_vertical", straddle_quantile="q1"),
            RuleSpec("put_credit_low_straddle", "volatility", "ATM straddle ask 位于 train q1 时卖 put credit", structure="put_credit_vertical", straddle_quantile="q1"),
            RuleSpec("iron_condor_low_straddle", "volatility", "ATM straddle ask 位于 train q1 时卖 iron condor", structure="iron_condor", straddle_quantile="q1"),
            RuleSpec("call_fly_high_straddle", "volatility", "ATM straddle ask 位于 train q5 时买 call fly", structure="call_butterfly", straddle_quantile="q5"),
            RuleSpec("put_fly_high_straddle", "volatility", "ATM straddle ask 位于 train q5 时买 put fly", structure="put_butterfly", straddle_quantile="q5"),
            RuleSpec("iron_condor_q5", "premium", "iron condor credit_fraction 位于该结构 train 最高五分位", structure="iron_condor", quantile="q5"),
        )
    )
    # fmt: on
    return tuple(rules)


RULE_SPECS = _rules()

def rth_sample_times(
    session_date: date,
    *,
    calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR,
    sampling_minutes: int = SAMPLING_MINUTES,
) -> tuple[datetime, ...]:
    """Return 15-minute RTH decisions from the open through 15:45 ET."""

    if sampling_minutes <= 0:
        raise ValueError("sampling_minutes must be positive")
    session = calendar.session(session_date)
    if session is None:
        return ()
    cutoff = min(
        session.close_at,
        datetime.combine(session_date, HARD_MARK_TIME_ET, tzinfo=ET),
    )
    cursor = session.open_at
    result = []
    while cursor <= cutoff:
        result.append(cursor.astimezone(timezone.utc))
        cursor += timedelta(minutes=sampling_minutes)
    return tuple(result)


def _geometry(name: str, atm: float) -> StructureGeometry:
    if name == "call_credit_vertical":
        return StructureGeometry(name, "credit", ((atm, "C"), (atm + WIDTH, "C")))
    if name == "put_credit_vertical":
        return StructureGeometry(name, "credit", ((atm, "P"), (atm - WIDTH, "P")))
    if name == "call_butterfly":
        return StructureGeometry(
            name, "debit", ((atm - WIDTH, "C"), (atm, "C"), (atm + WIDTH, "C"))
        )
    if name == "put_butterfly":
        return StructureGeometry(
            name, "debit", ((atm - WIDTH, "P"), (atm, "P"), (atm + WIDTH, "P"))
        )
    if name == "iron_condor":
        return StructureGeometry(
            name,
            "credit",
            ((atm - 25.0, "P"), (atm - 15.0, "P"), (atm + 15.0, "C"), (atm + 25.0, "C")),
        )
    raise ValueError(f"unknown structure: {name}")


def _tick_mapping(tick: OptionTick, provider: str) -> dict[str, Any]:
    source_at = tick.source_at or tick.at
    return {
        "provider": provider,
        "bid": tick.bid,
        "ask": tick.ask,
        "source_at": source_at.isoformat(),
    }


def _structure_bbo(
    geometry: StructureGeometry,
    ticks: Sequence[OptionTick],
    *,
    provider: str,
    now: datetime,
) -> dict[str, Any]:
    legs = [_tick_mapping(tick, provider) for tick in ticks]
    kwargs = {
        "now": now,
        "max_quote_age_seconds": MAX_QUOTE_AGE_SECONDS,
        "max_source_skew_seconds": MAX_LEG_SKEW_SECONDS,
    }
    if geometry.name in {"call_credit_vertical", "put_credit_vertical"}:
        # The credit's short body is bought and long wing sold to close.  The
        # helper's bid is therefore executable entry credit; ask is close debit.
        return conservative_vertical_bbo(legs[0], legs[1], **kwargs)
    if geometry.name.endswith("butterfly"):
        return conservative_butterfly_bbo(legs[0], legs[1], legs[2], **kwargs)
    return conservative_iron_condor_bbo(legs[0], legs[1], legs[2], legs[3], **kwargs)


def _priced_values(
    geometry: StructureGeometry, bbo: Mapping[str, Any], *, entry: bool
) -> tuple[float, float] | None:
    if bbo.get("status") != "ready":
        return None
    bid, ask = _number(bbo.get("bid")), _number(bbo.get("ask"))
    if bid is None or ask is None:
        return None
    entry_value, close_value = (bid, ask) if geometry.side == "credit" else (ask, bid)
    if entry and not 0 < entry_value < WIDTH:
        return None
    if close_value < 0:
        return None
    return entry_value, close_value


def _entry_quote(
    snapshot: Mapping[tuple[str, float, str], OptionTick],
    geometry: StructureGeometry,
    *,
    decision_at: datetime,
) -> tuple[dict[str, Any] | None, str | None]:
    complete_provider_seen = False
    reasons: Counter[str] = Counter()
    for provider in PROVIDER_PRIORITY:
        ticks = [snapshot.get((provider, float(strike), right)) for strike, right in geometry.legs]
        if any(tick is None for tick in ticks):
            continue
        complete_provider_seen = True
        present = [tick for tick in ticks if tick is not None]
        bbo = _structure_bbo(geometry, present, provider=provider, now=decision_at)
        values = _priced_values(geometry, bbo, entry=True)
        if values is not None:
            entry_value, close_value = values
            return {
                "provider": provider,
                "entry_value": entry_value,
                "entry_close_value": close_value,
                "source_times": bbo.get("source_times"),
            }, None
        reasons.update(str(reason) for reason in bbo.get("reasons") or ())
    if not complete_provider_seen:
        return None, "entry_missing_leg"
    return None, next(iter(reasons), "entry_invalid_bbo")


def _atm_straddle_ask(
    snapshot: Mapping[tuple[str, float, str], OptionTick],
    *,
    atm: float,
    decision_at: datetime,
) -> float | None:
    for provider in PROVIDER_PRIORITY:
        call = snapshot.get((provider, atm, "C"))
        put = snapshot.get((provider, atm, "P"))
        if call is None or put is None:
            continue
        if call.ask is None or put.ask is None:
            continue
        times = (call.source_at or call.at, put.source_at or put.at)
        ages = [(decision_at - value).total_seconds() for value in times]
        if any(age < 0 or age > MAX_QUOTE_AGE_SECONDS for age in ages):
            continue
        if abs((times[0] - times[1]).total_seconds()) > MAX_LEG_SKEW_SECONDS:
            continue
        value = float(call.ask) + float(put.ask)
        if value > 0:
            return value
    return None


def _combo_mark_path(
    store: QuoteStore,
    geometry: StructureGeometry,
    *,
    provider: str,
    start: datetime,
    end: datetime,
) -> list[tuple[datetime, float]]:
    series = [
        store.option_series(
            provider=provider,
            expiry=start.astimezone(ET).date(),
            strike=strike,
            right=right,
            start=start - timedelta(seconds=MAX_QUOTE_AGE_SECONDS),
            end=end,
        )
        for strike, right in geometry.legs
    ]
    streams = [
        map(
            lambda tick, leg_index=index: (tick.at, leg_index, tick),
            ticks,
        )
        for index, ticks in enumerate(series)
    ]
    merged = heapq.merge(*streams, key=lambda item: item[0])
    current: list[OptionTick | None] = [None] * len(series)
    marks: list[tuple[datetime, float]] = []
    for at, events in itertools.groupby(merged, key=lambda item: item[0]):
        for _event_at, index, tick in events:
            current[index] = tick
        if at < start or any(tick is None for tick in current):
            continue
        present = [tick for tick in current if tick is not None]
        bbo = _structure_bbo(geometry, present, provider=provider, now=at)
        values = _priced_values(geometry, bbo, entry=False)
        if values is not None:
            marks.append((at, values[1]))
    return marks


def _label_exits(
    geometry: StructureGeometry,
    entry: Mapping[str, Any],
    path: Sequence[tuple[datetime, float]],
    *,
    decision_at: datetime,
    session_date: date,
) -> dict[str, Any] | None:
    entry_value = float(entry["entry_value"])
    marks = [(decision_at, float(entry["entry_close_value"]))]
    index = bisect_right(path, decision_at, key=lambda item: item[0])
    marks.extend(path[index:])
    if not marks:
        return None
    hold_at, hold_value = marks[-1]
    if geometry.side == "credit":
        target = entry_value * 0.50
        giveback_at, giveback_value = next(
            ((at, value) for at, value in marks if value <= target),
            (hold_at, hold_value),
        )
        giveback_reason = (
            "credit_close_debit_at_or_below_half_entry"
            if giveback_value <= target
            else "hard_close"
        )
        pnl_hold = entry_value - hold_value
        pnl_giveback = entry_value - giveback_value
    else:
        policy_marks = [PolicyMark(at=at, combo_bid=value) for at, value in marks]
        label = simulate_management_policy(
            policy_marks,
            entry_ask=entry_value,
            leg_count=len(geometry.legs),
            entry_at=decision_at,
            policy=DEFAULT_MANAGEMENT_POLICY,
            session_date=session_date,
        )
        if label.exit_at is None or label.exit_bid is None:
            return None
        giveback_at, giveback_value = label.exit_at, float(label.exit_bid)
        giveback_reason = label.exit_reason
        pnl_hold = hold_value - entry_value
        pnl_giveback = giveback_value - entry_value
    return {
        "hold_1545_exit_at": hold_at.isoformat(),
        "hold_1545_exit_value": round(hold_value, 6),
        "pnl_hold_1545": round(pnl_hold, 6),
        "giveback_50_exit_at": giveback_at.isoformat(),
        "giveback_50_exit_value": round(giveback_value, 6),
        "giveback_50_exit_reason": giveback_reason,
        "pnl_giveback_50": round(pnl_giveback, 6),
        "mark_count": len(marks),
    }


def _underlier_at(
    ticks: Sequence[UnderlierTick], as_of: datetime, max_age_seconds: float
) -> UnderlierTick | None:
    index = bisect_right(ticks, as_of, key=lambda tick: tick.at) - 1
    if index < 0:
        return None
    tick = ticks[index]
    age = (as_of - tick.at).total_seconds()
    return tick if 0 <= age <= max_age_seconds and tick.price > 0 else None


def mine_session(
    store: QuoteStore,
    *,
    session_date: date,
    calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR,
) -> SessionResult:
    samples = rth_sample_times(session_date, calendar=calendar)
    coverage: dict[str, Any] = {
        "session_date": session_date.isoformat(),
        "scheduled_bars": len(samples),
        "spot_bars": 0,
        "structures": {
            name: {
                "attempts": 0,
                "labeled": 0,
                "entry_missing_leg": 0,
                "entry_invalid_bbo": 0,
                "exit_unavailable": 0,
            }
            for name in STRUCTURES
        },
    }
    if not samples:
        return SessionResult([], coverage)
    session = calendar.session(session_date)
    assert session is not None
    start, close = samples[0], samples[-1]
    providers = store.option_expiry_providers(expiry=session_date, start=start, end=close)
    coverage["parquet_providers"] = list(providers)
    if not providers:
        coverage["no_option_parquet"] = True
        return SessionResult([], coverage)

    spx = store.underlier_series(
        instrument_id=SPX_INSTRUMENT_ID,
        start=start - timedelta(seconds=MAX_QUOTE_AGE_SECONDS),
        end=close,
    )
    sampled: dict[datetime, UnderlierTick] = {}
    for at in samples:
        tick = _underlier_at(spx, at, MAX_QUOTE_AGE_SECONDS)
        if tick is not None:
            sampled[at] = tick
    coverage["spot_bars"] = len(sampled)
    if not sampled:
        return SessionResult([], coverage)

    atms = [math.floor(tick.price / 5.0) * 5.0 for tick in sampled.values()]
    strike_min, strike_max = min(atms) - 25.0, max(atms) + 25.0
    coverage["loaded_option_ticks"] = store.load_option_window(
        expiry=session_date,
        strike_min=strike_min,
        strike_max=strike_max,
        start=start - timedelta(seconds=MAX_QUOTE_AGE_SECONDS),
        end=close,
    )
    coverage["strike_band"] = [strike_min, strike_max]
    path_cache: dict[
        tuple[str, str, tuple[tuple[float, str], ...]], list[tuple[datetime, float]]
    ] = {}
    rows: list[dict[str, Any]] = []
    for decision_at in samples:
        spot_tick = sampled.get(decision_at)
        if spot_tick is None:
            continue
        atm = math.floor(spot_tick.price / 5.0) * 5.0
        strikes = {atm, atm - 10.0, atm + 10.0, atm - 15.0, atm + 15.0, atm - 25.0, atm + 25.0}
        snapshot = store.option_snapshot(
            expiry=session_date,
            as_of=decision_at,
            max_age_seconds=MAX_QUOTE_AGE_SECONDS,
            strikes=strikes,
        )
        straddle = _atm_straddle_ask(snapshot, atm=atm, decision_at=decision_at)
        prior = _underlier_at(spx, decision_at - timedelta(minutes=15), MAX_QUOTE_AGE_SECONDS)
        spot_ret_15m = (
            spot_tick.price / prior.price - 1.0 if prior is not None and prior.at >= start else None
        )
        for name in STRUCTURES:
            structure_coverage = coverage["structures"][name]
            structure_coverage["attempts"] += 1
            geometry = _geometry(name, atm)
            entry, failure = _entry_quote(snapshot, geometry, decision_at=decision_at)
            if entry is None:
                bucket = (
                    "entry_missing_leg" if failure == "entry_missing_leg" else "entry_invalid_bbo"
                )
                structure_coverage[bucket] += 1
                continue
            key = (name, str(entry["provider"]), geometry.legs)
            if key not in path_cache:
                path_cache[key] = _combo_mark_path(
                    store,
                    geometry,
                    provider=str(entry["provider"]),
                    start=start,
                    end=close,
                )
            labels = _label_exits(
                geometry,
                entry,
                path_cache[key],
                decision_at=decision_at,
                session_date=session_date,
            )
            if labels is None:
                structure_coverage["exit_unavailable"] += 1
                continue
            entry_value = float(entry["entry_value"])
            at_et = decision_at.astimezone(ET)
            rows.append(
                {
                    "schema_version": f"{SCHEMA_VERSION}.row",
                    "sampling_minutes": SAMPLING_MINUTES,
                    "session_date": session_date.isoformat(),
                    "decision_at": decision_at.isoformat(),
                    "structure": name,
                    "side": geometry.side,
                    "provider": entry["provider"],
                    "spot": spot_tick.price,
                    "spot_source": SPX_INSTRUMENT_ID,
                    "atm_strike": atm,
                    "legs": [{"strike": strike, "right": right} for strike, right in geometry.legs],
                    "entry_value": round(entry_value, 6),
                    "entry_credit": round(entry_value, 6) if geometry.side == "credit" else None,
                    "entry_debit": round(entry_value, 6) if geometry.side == "debit" else None,
                    "premium_fraction_of_width": round(entry_value / WIDTH, 8),
                    "atm_straddle_ask": straddle,
                    "spot_ret_15m": spot_ret_15m,
                    "hour_et": at_et.hour,
                    "minute_et": at_et.minute,
                    "entry_source_times": entry["source_times"],
                    "label_price_basis": "same_provider_live_nbbo_conservative_no_mid",
                    **labels,
                }
            )
            structure_coverage["labeled"] += 1
    return SessionResult(rows, coverage)


def fit_thresholds(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    by_structure: dict[str, dict[str, float | None]] = {}
    for structure in STRUCTURES:
        values = [
            value
            for row in rows
            if row.get("structure") == structure
            and (value := _number(row.get("premium_fraction_of_width"))) is not None
        ]
        by_structure[structure] = _q1_q5(values)
    unique_straddles: dict[tuple[str, str], float] = {}
    for row in rows:
        value = _number(row.get("atm_straddle_ask"))
        if value is not None:
            unique_straddles[(str(row.get("session_date")), str(row.get("decision_at")))] = value
    return {
        "premium_fraction_by_structure": by_structure,
        "atm_straddle_ask": _q1_q5(list(unique_straddles.values())),
    }


def _q1_q5(values: Sequence[float]) -> dict[str, float | None]:
    if not values:
        return {"q1_max": None, "q5_min": None}
    q20, q80 = np.quantile(values, (0.20, 0.80))
    return {"q1_max": float(q20), "q5_min": float(q80)}


def _select_side(rows: Sequence[Mapping[str, Any]], side: str) -> list[Mapping[str, Any]]:
    grouped: defaultdict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("structure") in {"call_credit_vertical", "put_credit_vertical"}:
            grouped[(str(row.get("session_date")), str(row.get("decision_at")))].append(row)
    selected = []
    for group in grouped.values():
        by_structure = {str(row["structure"]): row for row in group}
        call = by_structure.get("call_credit_vertical")
        put = by_structure.get("put_credit_vertical")
        if call is None or put is None:
            continue
        if side in {"expensive", "cheaper"}:
            call_value = float(call["premium_fraction_of_width"])
            put_value = float(put["premium_fraction_of_width"])
            if call_value == put_value:
                continue
            expensive = call if call_value > put_value else put
            selected.append(
                expensive if side == "expensive" else (put if expensive is call else call)
            )
            continue
        ret = _number(call.get("spot_ret_15m"))
        if ret is None or ret == 0:
            continue
        same = put if ret > 0 else call
        selected.append(same if side == "spot15_same" else (call if same is put else put))
    return selected


def select_rows(
    rows: Sequence[Mapping[str, Any]],
    spec: RuleSpec,
    thresholds: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    selected = _select_side(rows, spec.side) if spec.side else list(rows)
    if spec.structure is not None:
        selected = [row for row in selected if row.get("structure") == spec.structure]
    if spec.hour_et is not None:
        selected = [row for row in selected if row.get("hour_et") == spec.hour_et]
    if spec.quantile is not None:
        limits = thresholds["premium_fraction_by_structure"]

        def premium_inside(row: Mapping[str, Any]) -> bool:
            value = _number(row.get("premium_fraction_of_width"))
            boundary = limits[str(row.get("structure"))][
                "q1_max" if spec.quantile == "q1" else "q5_min"
            ]
            return (
                value is not None
                and boundary is not None
                and (value <= boundary if spec.quantile == "q1" else value >= boundary)
            )

        selected = [row for row in selected if premium_inside(row)]
    if spec.straddle_quantile is not None:
        boundary = thresholds["atm_straddle_ask"][
            "q1_max" if spec.straddle_quantile == "q1" else "q5_min"
        ]
        selected = [
            row
            for row in selected
            if boundary is not None
            and (value := _number(row.get("atm_straddle_ask"))) is not None
            and (value <= boundary if spec.straddle_quantile == "q1" else value >= boundary)
        ]
    return selected


def _pnl_field(exit_name: str) -> str:
    return "pnl_hold_1545" if exit_name == "hold_1545" else "pnl_giveback_50"


def _aggregate(
    rows: Sequence[Mapping[str, Any]], mode: str, pnl_field: str
) -> list[dict[str, Any]]:
    if mode == "sampling_points":
        return [dict(row) for row in rows]
    grouped: defaultdict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["session_date"])].append(row)
    if mode == "session_first":
        return [
            dict(min(group, key=lambda row: str(row["decision_at"]))) for group in grouped.values()
        ]
    return [
        {
            "session_date": day,
            pnl_field: statistics.fmean(float(row[pnl_field]) for row in group),
        }
        for day, group in grouped.items()
    ]


def metrics(rows: Sequence[Mapping[str, Any]], mode: str, exit_name: str) -> dict[str, Any]:
    pnl_field = _pnl_field(exit_name)
    observations = _aggregate(rows, mode, pnl_field)
    values = [float(row[pnl_field]) for row in observations]
    sessions = {str(row["session_date"]) for row in observations}
    mean = statistics.fmean(values) if values else None
    se = statistics.stdev(values) / math.sqrt(len(values)) if len(values) > 1 else None
    return {
        "n": len(values),
        "sessions": len(sessions),
        "mean": mean,
        "median": statistics.median(values) if values else None,
        "hit_rate": sum(value > 0 for value in values) / len(values) if values else None,
        "se": se,
        "lcb_90": mean - LCB_Z_90 * se if mean is not None and se is not None else None,
    }


def _passes(values: Mapping[str, Any], mode: str, *, train: bool = False) -> bool:
    if mode == "sampling_points":
        minimum_n, minimum_sessions = (30, 6) if train else (15, 5)
    else:
        minimum_n, minimum_sessions = 5, 5
    return bool(
        values.get("mean") is not None
        and float(values["mean"]) > 0
        and values.get("lcb_90") is not None
        and float(values["lcb_90"]) > 0
        and int(values["n"]) >= minimum_n
        and int(values["sessions"]) >= minimum_sessions
    )


def _failures(values: Mapping[str, Any], mode: str, *, train: bool = False) -> list[str]:
    if mode == "sampling_points":
        minimum_n, minimum_sessions = (30, 6) if train else (15, 5)
    else:
        minimum_n, minimum_sessions = 5, 5
    checks = (
        ("mean_nonpositive", values.get("mean") is None or float(values["mean"]) <= 0),
        ("lcb_90_nonpositive", values.get("lcb_90") is None or float(values["lcb_90"]) <= 0),
        ("n_below_minimum", int(values["n"]) < minimum_n),
        ("sessions_below_minimum", int(values["sessions"]) < minimum_sessions),
    )
    return [name for name, failed in checks if failed]


def _rule_manifest() -> list[dict[str, Any]]:
    return [
        {
            "name": rule.name,
            "family": rule.family,
            "definition": rule.definition,
            "structure": rule.structure,
            "side": rule.side,
            "hour_et": rule.hour_et,
            "quantile": rule.quantile,
            "straddle_quantile": rule.straddle_quantile,
        }
        for rule in RULE_SPECS
    ]


def explore(rows: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    train = [row for row in rows if TRAIN[0] <= str(row.get("session_date")) <= TRAIN[1]]
    holdout = [row for row in rows if HOLDOUT[0] <= str(row.get("session_date")) <= HOLDOUT[1]]
    thresholds = fit_thresholds(train)
    records: list[dict[str, Any]] = []
    for rule in RULE_SPECS:
        train_selected = select_rows(train, rule, thresholds)
        holdout_selected = select_rows(holdout, rule, thresholds)
        for exit_name in EXITS:
            train_metrics = {mode: metrics(train_selected, mode, exit_name) for mode in MODES}
            holdout_metrics = {mode: metrics(holdout_selected, mode, exit_name) for mode in MODES}
            train_qualified = _passes(
                train_metrics["sampling_points"], "sampling_points", train=True
            )
            holdout_raw = {mode: _passes(holdout_metrics[mode], mode) for mode in MODES}
            holdout_pass = {
                mode: train_qualified and passed for mode, passed in holdout_raw.items()
            }
            # fmt: off -- keep one frozen hypothesis record visibly compact.
            records.append(
                {
                    "schema_version": f"{SCHEMA_VERSION}.hypothesis", "hypothesis": f"{rule.name}__{exit_name}",
                    "entry_rule": rule.name, "family": rule.family, "definition": rule.definition, "exit": exit_name,
                    "train_thresholds": thresholds, "train": train_metrics, "holdout": holdout_metrics,
                    "train_qualified": train_qualified, "holdout_raw_gate_pass": holdout_raw, "holdout_pass": holdout_pass,
                    "robust": train_qualified and all(holdout_raw.values()),
                }
            )
            # fmt: on

    def rank(record: Mapping[str, Any]) -> tuple[Any, ...]:
        holdout = record["holdout"]
        lcbs = [
            float(holdout[mode]["lcb_90"]) if holdout[mode]["lcb_90"] is not None else -math.inf
            for mode in MODES
        ]
        return (
            bool(record["train_qualified"]),
            sum(bool(value) for value in record["holdout_raw_gate_pass"].values()),
            min(lcbs),
            float(holdout["sampling_points"]["mean"] or -math.inf),
        )

    closest = sorted(records, key=rank, reverse=True)[:5]
    closest_details = []
    for record in closest:
        failures = {
            "train_sampling_points": _failures(
                record["train"]["sampling_points"], "sampling_points", train=True
            )
        }
        failures.update(
            {f"holdout_{mode}": _failures(record["holdout"][mode], mode) for mode in MODES}
        )
        closest_details.append({
            "hypothesis": record["hypothesis"], "definition": record["definition"],
            "exit": record["exit"], "train": record["train"], "holdout": record["holdout"],
            "failure_reasons": failures,
        })
    manifest = _rule_manifest()
    manifest_json = json.dumps(manifest, ensure_ascii=False, sort_keys=True)
    train_qualified = [record for record in records if record["train_qualified"]]
    robust = [record for record in records if record["robust"]]
    raw_holdout_robust = [
        record for record in records if all(record["holdout_raw_gate_pass"].values())
    ]
    # fmt: off -- the report schema is data, not branching logic.
    report = {
        "schema_version": f"{SCHEMA_VERSION}.report", "train_window": list(TRAIN), "holdout_window": list(HOLDOUT),
        "preregistered_entry_rules": manifest, "rule_manifest_sha256": sha256(manifest_json.encode()).hexdigest(),
        "base_entry_rules": len(RULE_SPECS), "exits": list(EXITS), "hypotheses_tested": len(records),
        "train_rows": len(train), "holdout_rows": len(holdout), "train_thresholds": thresholds,
        "funnel": {
            "train_qualified": len(train_qualified),
            "holdout_sampling_survivors": sum(record["holdout_pass"]["sampling_points"] for record in records),
            "holdout_session_first_survivors": sum(record["holdout_pass"]["session_first"] for record in records),
            "holdout_session_mean_survivors": sum(record["holdout_pass"]["session_mean"] for record in records),
            "robust_survivors": len(robust), "robust_hypotheses": [record["hypothesis"] for record in robust],
            "raw_holdout_all_modes_before_train_gate": len(raw_holdout_robust),
            "raw_holdout_all_modes_hypotheses": [record["hypothesis"] for record in raw_holdout_robust],
        },
        "closest": closest_details,
        "gate_contract": {
            "train_sampling_points": "mean>0, lcb_90>0, n>=30, sessions>=6", "holdout_sampling_points": "mean>0, lcb_90>0, n>=15, sessions>=5",
            "holdout_session_first": "mean>0, lcb_90>0, sessions>=5", "holdout_session_mean": "mean>0, lcb_90>0, sessions>=5",
            "research_candidate": "train sampling + all three holdout modes pass",
        },
        "honesty": {
            "rth_only": True, "sampling_minutes": SAMPLING_MINUTES, "same_provider": True,
            "live_nbbo_only": True, "mid_used_for_execution": False, "pnl_units": "SPX points before fees",
            "threshold_source": "train only", "holdout_tuning": False,
            "live_path_written": False, "production_candidate": False,
        },
    }
    # fmt: on
    return report, records


def _aggregate_coverage(sessions: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    structures: dict[str, dict[str, Any]] = {}
    for name in STRUCTURES:
        totals = Counter()
        for session in sessions:
            totals.update(session.get("structures", {}).get(name, {}))
        attempts = int(totals["attempts"])
        missing = int(totals["entry_missing_leg"])
        structures[name] = {
            **dict(totals),
            "missing_leg_rate": missing / attempts if attempts else None,
            "label_success_rate": int(totals["labeled"]) / attempts if attempts else None,
        }
    return {
        "sessions": list(sessions),
        "requested_trading_days": len(sessions),
        "sessions_with_spot": sum(int(session.get("spot_bars") or 0) > 0 for session in sessions),
        "scheduled_bars": sum(int(session.get("scheduled_bars") or 0) for session in sessions),
        "spot_bars": sum(int(session.get("spot_bars") or 0) for session in sessions),
        "structures": structures,
    }


def _validate_cached_rows(rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        raise ValueError("cached label file is empty; use --relabel after checking the lake")
    if any(row.get("schema_version") != f"{SCHEMA_VERSION}.row" for row in rows):
        raise ValueError("cached label schema mismatch; use --relabel explicitly")
    if any(row.get("sampling_minutes") != SAMPLING_MINUTES for row in rows):
        raise ValueError("cached sampling interval mismatch; use --relabel explicitly")
    if any(not TRAIN[0] <= str(row.get("session_date")) <= HOLDOUT[1] for row in rows):
        raise ValueError("cached date range mismatch; use --relabel explicitly")


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _fmt_metric(value: Mapping[str, Any]) -> str:
    mean = value.get("mean")
    lcb = value.get("lcb_90")
    return (
        f"n={value['n']}，日={value['sessions']}，mean={mean:.4f}，90% LCB={lcb:.4f}"
        if mean is not None and lcb is not None
        else f"n={value['n']}，日={value['sessions']}，mean={mean}，90% LCB={lcb}"
    )


def render_markdown(report: Mapping[str, Any]) -> str:
    funnel = report["funnel"]
    robust = int(funnel["robust_survivors"])
    # fmt: off -- compact source; the generated Markdown remains expanded.
    lines = [
        "# 0DTE Signal Universe 因果回测报告", "", "## 结论", "",
        (f"一句话：换到 credit vertical、iron condor 与 ATM butterfly 后，冻结 holdout 找到 {robust} 条同时通过采样点、session_first、session_mean 的稳健 EV+ 研究规则；它们仍不是人读卡。" if robust else "一句话：换到 credit vertical、iron condor 与 ATM butterfly 后，冻结 holdout 仍没有同时通过采样点、session_first、session_mean 的稳健 EV+ 规则。"),
        "", "## 预注册规则与口径", "", f"规则在读取 holdout 指标前冻结：{report['base_entry_rules']} 条基础入场/结构规则 × 2 种出场 = {report['hypotheses_tested']} 个假设。manifest SHA-256：`{report['rule_manifest_sha256']}`。", "",
    ]
    lines += [f"- `{rule['name']}`：{rule['definition']}" for rule in report["preregistered_entry_rules"]]
    lines += ["", "执行定价只用同一 provider 的 live NBBO conservative bid/ask，不用 mid。ATM=`floor(SPX/5)*5`；RTH 每 15 分钟采样；train 为 2026-07-06..07-31，holdout 为 2026-08-03..08-17，排除 08-18。q1/q5 与 straddle 阈值只由 train 拟合。PnL 单位为 SPX 点、未扣费用。", "", "`hold_1545` 用 15:45 ET 前最后一笔合法 conservative 平仓价；credit 的 `giveback_50` 在 conservative close debit ≤ entry credit×0.5 时退出，否则 15:45；fly 的 `giveback_50` 复用 `management_policy.v2`。", "", "## 标签覆盖", "", "| 结构 | 尝试 | 成功标记 | 缺腿 | 缺腿率 | BBO 无效 | 出场缺失 |", "| --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for name, values in report["labeling"]["structures"].items():
        lines.append(f"| {name} | {values.get('attempts', 0)} | {values.get('labeled', 0)} | {values.get('entry_missing_leg', 0)} | {float(values.get('missing_leg_rate') or 0):.1%} | {values.get('entry_invalid_bbo', 0)} | {values.get('exit_unavailable', 0)} |")
    lines += ["", "## 漏斗", "", f"- 假设总数：{report['hypotheses_tested']}。", f"- Train 采样点级入围：{funnel['train_qualified']}。", f"- 经 train 门后，holdout 采样点级存活：{funnel['holdout_sampling_survivors']}。", f"- Holdout session_first 存活：{funnel['holdout_session_first_survivors']}。", f"- Holdout session_mean 存活：{funnel['holdout_session_mean_survivors']}。", f"- 三种口径同时存活：{funnel['robust_survivors']}。", f"- 诊断项：忽略 train 门时，holdout 三口径原始过门 {funnel['raw_holdout_all_modes_before_train_gate']} 条；这些规则因 train 未入围，不计研究候选。", ""]
    if robust:
        lines.extend(("## 研究赢家", ""))
        by_name = {record["hypothesis"]: record for record in report["hypothesis_records"]}
        for name in funnel["robust_hypotheses"]:
            record = by_name[name]
            lines += [f"### `{name}`", "", f"If-then：如果 {record['definition']}，则采用 `{record['exit']}`；结构和腿位由规则定义。仅作为研究候选。", "", f"- Train sampling：{_fmt_metric(record['train']['sampling_points'])}。", f"- Holdout sampling：{_fmt_metric(record['holdout']['sampling_points'])}。", f"- Holdout session_first：{_fmt_metric(record['holdout']['session_first'])}。", f"- Holdout session_mean：{_fmt_metric(record['holdout']['session_mean'])}。", ""]
    else:
        lines.extend(("## 最接近的 5 条与死因", ""))
        for index, item in enumerate(report["closest"], 1):
            reasons = "; ".join(f"{scope}={','.join(values) or 'PASS'}" for scope, values in item["failure_reasons"].items())
            lines += [f"{index}. `{item['hypothesis']}`：{item['definition']}；出场 `{item['exit']}`。", f"   Train sampling：{_fmt_metric(item['train']['sampling_points'])}；holdout sampling：{_fmt_metric(item['holdout']['sampling_points'])}；session_first：{_fmt_metric(item['holdout']['session_first'])}；session_mean：{_fmt_metric(item['holdout']['session_mean'])}。死因：{reasons}。"]
        lines.append("")
    lines += ["## 与上一宇宙的差异", "", report["comparison_to_prior_universe"], "", "## Live 路径", "", "未写入。没有修改 `strategy_select`、人读卡、通知、服务、配置或任何生产运行路径；结果只存在于离线研究模块和 `/tmp/strategy-edge-backtest/` 产物。", ""]
    # fmt: on
    return "\n".join(lines)

def _comparison(report: Mapping[str, Any]) -> str:
    if report["funnel"]["robust_survivors"]:
        return "上一轮固定 ATM 10 点借记持有到 15:45 的 114 条假设无 holdout 稳健存活；本轮重新从报价湖标注卖权与蝶式，并加入动态结构选择和管理出场，至少一类结构露出同时跨采样点与 session 的苗头，但仍需独立前向验证。"
    raw_names = report["funnel"]["raw_holdout_all_modes_hypotheses"]
    if not report["funnel"]["train_qualified"] and raw_names:
        names = "、".join(f"`{name}`" for name in raw_names)
        return (
            "上一轮固定 ATM 10 点借记持有到 15:45 的 114 条假设无 holdout 稳健存活；"
            "本轮 96 条卖权/蝶式假设同样没有一条通过 train 采样点门。"
            f"不过 {names} 在 8 月 holdout 的三种口径原始过门，而 7 月 train 均值和 LCB 均为负；"
            "这更像下午 put-credit/iron-condor 的月份或行情状态反转，只能算不稳定苗头，不能倒用 holdout 立规则。"
        )
    return "上一轮固定 ATM 10 点借记持有到 15:45 的 114 条假设无 holdout 稳健存活；本轮换成卖权、铁鹰与蝶式并加入管理出场后仍无稳健存活，说明仅换 payoff 尚未解决问题。"

def run(
    *,
    data_root: Path,
    output_dir: Path,
    relabel: bool = False,
    calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    rows_path = output_dir / "odte_signal_universe.rows.jsonl"
    report_path = output_dir / "odte_signal_universe.report.json"
    rules_path = output_dir / "odte_signal_universe.rules.jsonl"
    markdown_path = output_dir / "codex-signal-universe-last.md"
    cached = rows_path.exists() and not relabel
    if cached:
        rows = [
            json.loads(line)
            for line in rows_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        _validate_cached_rows(rows)
        if not report_path.exists():
            raise ValueError("cached rows exist without coverage report; use --relabel explicitly")
        previous = json.loads(report_path.read_text(encoding="utf-8"))
        labeling = previous.get("labeling")
        if not isinstance(labeling, Mapping):
            raise ValueError("cached coverage is unavailable; use --relabel explicitly")
    else:
        days = [
            START_DATE + timedelta(days=offset)
            for offset in range((END_DATE - START_DATE).days + 1)
            if calendar.is_trading_day(START_DATE + timedelta(days=offset))
        ]
        rows = []
        session_coverages = []
        store = QuoteStore(data_root)
        try:
            for day in days:
                result = mine_session(store, session_date=day, calendar=calendar)
                rows.extend(result.rows)
                session_coverages.append(result.coverage)
        finally:
            store.close()
        temporary = rows_path.with_name(f".{rows_path.name}.tmp")
        with temporary.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
        temporary.replace(rows_path)
        labeling = _aggregate_coverage(session_coverages)
    report, records = explore(rows)
    report["generated_at"] = datetime.now(timezone.utc).isoformat()
    report["cache_reused"] = cached
    report["rows_file"] = str(rows_path)
    report["labeling"] = labeling
    report["hypothesis_records"] = records
    report["comparison_to_prior_universe"] = _comparison(report)
    _write_json(report_path, report)
    rules_tmp = rules_path.with_name(f".{rules_path.name}.tmp")
    with rules_tmp.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    rules_tmp.replace(rules_path)
    markdown_tmp = markdown_path.with_name(f".{markdown_path.name}.tmp")
    markdown_tmp.write_text(render_markdown(report), encoding="utf-8")
    markdown_tmp.replace(markdown_path)
    return report


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    number = float(value)
    return number if math.isfinite(number) else None


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=Path("/srv/data/spx-spark/data"))
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/strategy-edge-backtest"))
    parser.add_argument("--relabel", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    report = run(data_root=args.data_root, output_dir=args.output_dir, relabel=args.relabel)
    print(json.dumps(report["funnel"], ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
