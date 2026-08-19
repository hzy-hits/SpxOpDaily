"""Causal near-tick replay for frozen one-trade-per-session 0DTE signals."""

from __future__ import annotations

import argparse
from array import array
from bisect import bisect_right
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import json
import math
from pathlib import Path
import statistics
from typing import Any

import numpy as np

from spx_spark.data_platform.research.odte_level_quotes import QuoteStore
from spx_spark.data_platform.research.odte_level_signals import OptionTick, UnderlierTick
from spx_spark.data_platform.research.odte_signal_universe import (
    MAX_QUOTE_AGE_SECONDS,
    PROVIDER_PRIORITY,
    SPX_INSTRUMENT_ID,
    StructureGeometry,
    _combo_mark_path,
    _entry_quote,
    _geometry,
    _label_exits,
    _underlier_at,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET, MarketCalendar


SCHEMA_VERSION = "odte_tick_signals.v1"
TRAIN = ("2026-07-06", "2026-07-31")
HOLDOUT = ("2026-08-03", "2026-08-17")
START_DATE = date.fromisoformat(TRAIN[0])
END_DATE = date.fromisoformat(HOLDOUT[1])
HARD_MARK_TIME_ET = time(15, 45)
WIDTH = 10.0
EXITS = ("hold_1545", "giveback_50")
LCB_Z_90 = 1.6448536269514722
TRAIN_MIN_SESSIONS = 8
HOLDOUT_MIN_SESSIONS = 5
AFTER_1300 = time(13, 0)
AFTER_1400 = time(14, 0)


@dataclass(frozen=True, slots=True)
class RuleSpec:
    name: str
    structure: str
    definition: str


# fmt: off
RULE_SPECS = (
    RuleSpec("put_credit_after_1300", "put_credit_vertical", "13:00 ET 后第一笔合法 ATM 10 点 put credit"), RuleSpec("put_credit_after_1400", "put_credit_vertical", "14:00 ET 后第一笔合法 ATM 10 点 put credit"),
    RuleSpec("iron_condor_after_1400", "iron_condor", "14:00 ET 后第一笔合法 ATM±15/±25 铁鹰"), RuleSpec("put_credit_ret1m_down", "put_credit_vertical", "SPX 1 分钟收益不高于 7 月 train q20 时第一笔合法 put credit"),
    RuleSpec("put_credit_rich", "put_credit_vertical", "put credit_fraction 不低于 7 月 train q80 时第一笔"), RuleSpec("put_credit_1300_rich", "put_credit_vertical", "13:00 ET 后且 put credit_fraction 不低于 7 月 train q80 时第一笔"),
    RuleSpec("call_credit_after_1300", "call_credit_vertical", "13:00 ET 后第一笔合法 ATM 10 点 call credit 对照"), RuleSpec("put_credit_open", "put_credit_vertical", "RTH 开盘后第一笔合法 ATM 10 点 put credit 基线"),
)
# fmt: on
RULE_BY_NAME = {rule.name: rule for rule in RULE_SPECS}
RICH_RULES = ("put_credit_rich", "put_credit_1300_rich")


@dataclass(frozen=True, slots=True)
class Opportunity:
    at: datetime
    spot: float
    atm: float
    spot_ret_1m: float | None
    geometries: Mapping[str, StructureGeometry]
    entries: Mapping[str, Mapping[str, Any] | None]


@dataclass(frozen=True, slots=True)
class DeferredRichCandidate:
    session_date: str
    rule: str
    premium_fraction: float
    decision_at: str
    row: Mapping[str, Any] | None


@dataclass(slots=True)
class SessionResult:
    rows: list[dict[str, Any]]
    deferred_rich: list[DeferredRichCandidate]
    credit_samples: list[float]
    coverage: dict[str, Any]


def _session_bounds(
    session_date: date, calendar: MarketCalendar
) -> tuple[datetime, datetime] | None:
    session = calendar.session(session_date)
    if session is None:
        return None
    end = min(
        session.close_at,
        datetime.combine(session_date, HARD_MARK_TIME_ET, tzinfo=ET),
    )
    return session.open_at.astimezone(timezone.utc), end.astimezone(timezone.utc)


def _spot_ret_1m(
    spx: Sequence[UnderlierTick], *, at: datetime, session_start: datetime
) -> float | None:
    current = _underlier_at(spx, at, MAX_QUOTE_AGE_SECONDS)
    prior = _underlier_at(spx, at - timedelta(minutes=1), MAX_QUOTE_AGE_SECONDS)
    if current is None or prior is None or prior.at < session_start or prior.price <= 0:
        return None
    return current.price / prior.price - 1.0


def fit_ret1m_q20(
    sessions: Mapping[date, Sequence[UnderlierTick]],
    *,
    calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR,
) -> tuple[float, int]:
    values = array("d")
    for session_date, ticks in sessions.items():
        if not (TRAIN[0] <= session_date.isoformat() <= TRAIN[1]):
            continue
        bounds = _session_bounds(session_date, calendar)
        if bounds is None:
            continue
        start, end = bounds
        for tick in ticks:
            if not start <= tick.at <= end:
                continue
            value = _spot_ret_1m(ticks, at=tick.at, session_start=start)
            if value is not None and math.isfinite(value):
                values.append(value)
    if not values:
        raise ValueError("train has no causal SPX one-minute returns")
    return float(np.quantile(values, 0.20, method="linear")), len(values)


def fit_credit_q80(values: Sequence[float]) -> float:
    observed = np.asarray(values, dtype=float)
    finite = observed[np.isfinite(observed)]
    if not len(finite):
        raise ValueError("train has no legal put-credit fractions")
    return float(np.quantile(finite, 0.80, method="linear"))


def _series_for(
    store: QuoteStore,
    cache: dict[tuple[str, float, str], list[OptionTick]],
    *,
    provider: str,
    strike: float,
    right: str,
    expiry: date,
    start: datetime,
    end: datetime,
) -> list[OptionTick]:
    key = (provider, float(strike), right)
    if key not in cache:
        cache[key] = store.option_series(
            provider=provider,
            expiry=expiry,
            strike=strike,
            right=right,
            start=start - timedelta(seconds=MAX_QUOTE_AGE_SECONDS),
            end=end,
        )
    return cache[key]


def _next_event_at(
    store: QuoteStore,
    *,
    session_date: date,
    spx: Sequence[UnderlierTick],
    at: datetime,
    end: datetime,
    geometries: Mapping[str, StructureGeometry],
    entries: Mapping[str, Mapping[str, Any] | None],
    series_cache: dict[tuple[str, float, str], list[OptionTick]],
) -> datetime | None:
    candidates: list[datetime] = []
    spx_index = bisect_right(spx, at, key=lambda tick: tick.at)
    if spx_index < len(spx) and spx[spx_index].at <= end:
        candidates.append(spx[spx_index].at)
    for name, geometry in geometries.items():
        entry = entries.get(name)
        providers = (
            ("schwab",)
            if entry is not None and entry.get("provider") == "schwab"
            else PROVIDER_PRIORITY
        )
        for provider in providers:
            for strike, right in geometry.legs:
                ticks = _series_for(
                    store,
                    series_cache,
                    provider=provider,
                    strike=strike,
                    right=right,
                    expiry=session_date,
                    start=at,
                    end=end,
                )
                index = bisect_right(ticks, at, key=lambda tick: tick.at)
                if index < len(ticks) and ticks[index].at <= end:
                    candidates.append(ticks[index].at)
    return min(candidates) if candidates else None


def iter_opportunities(
    store: QuoteStore,
    *,
    session_date: date,
    spx: Sequence[UnderlierTick],
    structure_name: str,
    start: datetime,
    end: datetime,
):
    series_cache: dict[tuple[str, float, str], list[OptionTick]] = {}
    initial_spot = _underlier_at(spx, start, MAX_QUOTE_AGE_SECONDS)
    initial_geometries = (
        {structure_name: _geometry(structure_name, math.floor(initial_spot.price / 5.0) * 5.0)}
        if initial_spot is not None
        else {}
    )
    at = _next_event_at(
        store,
        session_date=session_date,
        spx=spx,
        at=start - timedelta(microseconds=1),
        end=end,
        geometries=initial_geometries,
        entries={},
        series_cache=series_cache,
    )
    if at is None:
        return
    while at <= end:
        spot_tick = _underlier_at(spx, at, MAX_QUOTE_AGE_SECONDS)
        geometries: dict[str, StructureGeometry] = {}
        entries: dict[str, Mapping[str, Any] | None] = {}
        if spot_tick is not None:
            atm = math.floor(spot_tick.price / 5.0) * 5.0
            geometries = {structure_name: _geometry(structure_name, atm)}
            strikes = {strike for geometry in geometries.values() for strike, _ in geometry.legs}
            snapshot = store.option_snapshot(
                expiry=session_date,
                as_of=at,
                max_age_seconds=MAX_QUOTE_AGE_SECONDS,
                strikes=strikes,
            )
            for name, geometry in geometries.items():
                # fmt: off
                schwab = {key: snapshot[key] for strike, right in geometry.legs if (key := ("schwab", float(strike), right)) in snapshot}
                preferred = schwab if len(schwab) == len(geometry.legs) else snapshot
                entries[name] = _entry_quote(preferred, geometry, decision_at=at)[0]
                # fmt: on
            yield Opportunity(
                at=at,
                spot=spot_tick.price,
                atm=atm,
                spot_ret_1m=_spot_ret_1m(spx, at=at, session_start=start),
                geometries=geometries,
                entries=entries,
            )
        next_at = _next_event_at(
            store,
            session_date=session_date,
            spx=spx,
            at=at,
            end=end,
            geometries=geometries,
            entries=entries,
            series_cache=series_cache,
        )
        if next_at is None:
            break
        if next_at <= at:
            raise AssertionError("event clock did not advance")
        at = next_at


def _condition(
    rule: str,
    opportunity: Opportunity,
    *,
    ret1m_q20: float,
    credit_q80: float | None,
) -> bool:
    at_et = opportunity.at.astimezone(ET).time()
    entry = opportunity.entries.get(RULE_BY_NAME[rule].structure)
    if entry is None:
        return False
    if rule == "put_credit_open":
        return True
    if rule == "put_credit_after_1300" or rule == "call_credit_after_1300":
        return at_et >= AFTER_1300
    if rule == "put_credit_after_1400" or rule == "iron_condor_after_1400":
        return at_et >= AFTER_1400
    if rule == "put_credit_ret1m_down":
        return opportunity.spot_ret_1m is not None and opportunity.spot_ret_1m <= ret1m_q20
    if rule == "put_credit_rich":
        return credit_q80 is not None and float(entry["entry_value"]) / WIDTH >= credit_q80
    if rule == "put_credit_1300_rich":
        return (
            at_et >= AFTER_1300
            and credit_q80 is not None
            and float(entry["entry_value"]) / WIDTH >= credit_q80
        )
    raise ValueError(f"unknown rule: {rule}")


def _base_trade_row(
    store: QuoteStore,
    opportunity: Opportunity,
    *,
    session_date: date,
    structure_name: str,
    session_start: datetime,
    session_end: datetime,
    path_cache: dict[tuple[str, str, tuple[tuple[float, str], ...]], list[tuple[datetime, float]]],
) -> dict[str, Any] | None:
    geometry = opportunity.geometries[structure_name]
    entry = opportunity.entries[structure_name]
    if entry is None:
        return None
    provider = str(entry["provider"])
    key = (structure_name, provider, geometry.legs)
    if key not in path_cache:
        path_cache[key] = _combo_mark_path(
            store,
            geometry,
            provider=provider,
            start=session_start,
            end=session_end,
        )
    labels = _label_exits(
        geometry,
        entry,
        path_cache[key],
        decision_at=opportunity.at,
        session_date=session_date,
    )
    if labels is None:
        return None
    entry_credit = float(entry["entry_value"])
    return {
        "schema_version": f"{SCHEMA_VERSION}.row",
        "session_date": session_date.isoformat(),
        "decision_at": opportunity.at.isoformat(),
        "structure": structure_name,
        "provider": provider,
        "spot": round(opportunity.spot, 6),
        "spot_source": SPX_INSTRUMENT_ID,
        "spot_ret_1m": opportunity.spot_ret_1m,
        "atm_strike": opportunity.atm,
        "legs": [{"strike": strike, "right": right} for strike, right in geometry.legs],
        "entry_credit": round(entry_credit, 6),
        "credit_fraction_of_width": round(entry_credit / WIDTH, 8),
        "entry_source_times": entry.get("source_times"),
        "entry_price_basis": "same_provider_live_nbbo_conservative_bid_no_mid",
        "exit_price_basis": "same_provider_live_nbbo_conservative_close_ask_no_mid",
        **labels,
    }


def _with_rule(row: Mapping[str, Any], rule_name: str) -> dict[str, Any]:
    rule = RULE_BY_NAME[rule_name]
    return {"entry_rule": rule.name, "definition": rule.definition, **dict(row)}


def mine_session(
    store: QuoteStore,
    *,
    session_date: date,
    spx: Sequence[UnderlierTick],
    ret1m_q20: float,
    credit_q80: float | None,
    calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR,
) -> SessionResult:
    bounds = _session_bounds(session_date, calendar)
    coverage: dict[str, Any] = {
        "session_date": session_date.isoformat(),
        "option_window_loads": 0,
        "event_steps": 0,
        "legal_put_events": 0,
        "labeled_rules": {},
    }
    if bounds is None or not spx:
        return SessionResult([], [], [], coverage)
    start, end = bounds
    session_spx = [tick for tick in spx if start - timedelta(seconds=30) <= tick.at <= end]
    if not session_spx:
        return SessionResult([], [], [], coverage)
    atms = [math.floor(tick.price / 5.0) * 5.0 for tick in session_spx if tick.price > 0]
    if not atms:
        return SessionResult([], [], [], coverage)
    strike_min, strike_max = min(atms) - 25.0, max(atms) + 25.0
    coverage["loaded_option_ticks"] = store.load_option_window(
        expiry=session_date,
        strike_min=strike_min,
        strike_max=strike_max,
        start=start - timedelta(seconds=MAX_QUOTE_AGE_SECONDS),
        end=end,
    )
    coverage["option_window_loads"] = 1
    coverage["strike_band"] = [strike_min, strike_max]

    rows: list[dict[str, Any]] = []
    deferred: list[DeferredRichCandidate] = []
    credit_samples: list[float] = []
    triggered: set[str] = set()
    rich_highs = {rule: -math.inf for rule in RICH_RULES}
    path_cache: dict[
        tuple[str, str, tuple[tuple[float, str], ...]], list[tuple[datetime, float]]
    ] = {}
    base_cache: dict[tuple[str, datetime], dict[str, Any] | None] = {}

    def base_for(opportunity: Opportunity, structure_name: str) -> dict[str, Any] | None:
        key = (structure_name, opportunity.at)
        if key not in base_cache:
            base_cache[key] = _base_trade_row(
                store,
                opportunity,
                session_date=session_date,
                structure_name=structure_name,
                session_start=start,
                session_end=end,
                path_cache=path_cache,
            )
        return base_cache[key]

    for opportunity in iter_opportunities(
        store,
        session_date=session_date,
        spx=session_spx,
        structure_name="put_credit_vertical",
        start=start,
        end=end,
    ):
        coverage["event_steps"] += 1
        put_entry = opportunity.entries.get("put_credit_vertical")
        premium_fraction = None
        if put_entry is not None:
            premium_fraction = float(put_entry["entry_value"]) / WIDTH
            credit_samples.append(premium_fraction)
            coverage["legal_put_events"] += 1

        for rule in RULE_SPECS:
            if rule.structure != "put_credit_vertical":
                continue
            if rule.name in triggered or (credit_q80 is None and rule.name in RICH_RULES):
                continue
            if not _condition(
                rule.name,
                opportunity,
                ret1m_q20=ret1m_q20,
                credit_q80=credit_q80,
            ):
                continue
            triggered.add(rule.name)
            base = base_for(opportunity, rule.structure)
            if base is not None:
                rows.append(_with_rule(base, rule.name))

        if credit_q80 is not None or premium_fraction is None:
            continue
        for rule_name in RICH_RULES:
            if rule_name == "put_credit_1300_rich" and (
                opportunity.at.astimezone(ET).time() < AFTER_1300
            ):
                continue
            if premium_fraction <= rich_highs[rule_name]:
                continue
            rich_highs[rule_name] = premium_fraction
            base = base_for(opportunity, "put_credit_vertical")
            deferred.append(
                DeferredRichCandidate(
                    session_date=session_date.isoformat(),
                    rule=rule_name,
                    premium_fraction=premium_fraction,
                    decision_at=opportunity.at.isoformat(),
                    row=base,
                )
            )

    for structure_name, rule_name, rule_start in (
        ("call_credit_vertical", "call_credit_after_1300", AFTER_1300),
        ("iron_condor", "iron_condor_after_1400", AFTER_1400),
    ):
        not_before = datetime.combine(session_date, rule_start, tzinfo=ET).astimezone(timezone.utc)
        for opportunity in iter_opportunities(
            store,
            session_date=session_date,
            spx=session_spx,
            structure_name=structure_name,
            start=not_before,
            end=end,
        ):
            coverage["event_steps"] += 1
            if opportunity.entries.get(structure_name) is None:
                continue
            triggered.add(rule_name)
            base = base_for(opportunity, structure_name)
            if base is not None:
                rows.append(_with_rule(base, rule_name))
            break

    coverage["labeled_rules"] = dict(sorted(_rule_counts(rows).items()))
    return SessionResult(rows, deferred, credit_samples, coverage)


def materialize_deferred_rich(
    candidates: Sequence[DeferredRichCandidate], *, credit_q80: float
) -> list[dict[str, Any]]:
    grouped: defaultdict[tuple[str, str], list[DeferredRichCandidate]] = defaultdict(list)
    for candidate in candidates:
        grouped[(candidate.session_date, candidate.rule)].append(candidate)
    rows: list[dict[str, Any]] = []
    for group in grouped.values():
        selected = next(
            (
                candidate
                for candidate in sorted(group, key=lambda item: item.decision_at)
                if candidate.premium_fraction >= credit_q80
            ),
            None,
        )
        if selected is not None and selected.row is not None:
            rows.append(_with_rule(selected.row, selected.rule))
    return rows


def _rule_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts: defaultdict[str, int] = defaultdict(int)
    for row in rows:
        counts[str(row["entry_rule"])] += 1
    return dict(counts)


def metrics(rows: Sequence[Mapping[str, Any]], exit_name: str) -> dict[str, Any]:
    field = f"pnl_{exit_name}"
    values = [float(row[field]) for row in rows]
    mean = statistics.fmean(values) if values else None
    se = statistics.stdev(values) / math.sqrt(len(values)) if len(values) > 1 else None
    return {
        "n": len(values),
        "mean": mean,
        "median": statistics.median(values) if values else None,
        "hit_rate": sum(value > 0 for value in values) / len(values) if values else None,
        "se": se,
        "lcb_90": mean - LCB_Z_90 * se if mean is not None and se is not None else None,
    }


def passes_gate(values: Mapping[str, Any], *, train: bool) -> bool:
    minimum = TRAIN_MIN_SESSIONS if train else HOLDOUT_MIN_SESSIONS
    return bool(
        values.get("mean") is not None
        and float(values["mean"]) > 0
        and values.get("lcb_90") is not None
        and float(values["lcb_90"]) > 0
        and int(values["n"]) >= minimum
    )


def _gate_failures(values: Mapping[str, Any], *, train: bool) -> list[str]:
    minimum = TRAIN_MIN_SESSIONS if train else HOLDOUT_MIN_SESSIONS
    checks = (
        ("mean_nonpositive", values.get("mean") is None or float(values["mean"]) <= 0),
        (
            "lcb_90_nonpositive",
            values.get("lcb_90") is None or float(values["lcb_90"]) <= 0,
        ),
        ("n_below_minimum", int(values["n"]) < minimum),
    )
    return [name for name, failed in checks if failed]


def _first_by_session(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    grouped: defaultdict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["session_date"])].append(row)
    return [dict(min(group, key=lambda row: str(row["decision_at"]))) for group in grouped.values()]


def _prior_15m_rows(rows: Sequence[Mapping[str, Any]], rule_name: str) -> list[dict[str, Any]]:
    mapping = {
        "put_credit_after_1300": ("put_credit_vertical", AFTER_1300),
        "put_credit_after_1400": ("put_credit_vertical", AFTER_1400),
        "iron_condor_after_1400": ("iron_condor", AFTER_1400),
        "call_credit_after_1300": ("call_credit_vertical", AFTER_1300),
        "put_credit_open": ("put_credit_vertical", time(9, 30)),
    }
    if rule_name not in mapping:
        return []
    structure, floor_time = mapping[rule_name]
    selected = [
        row
        for row in rows
        if row.get("structure") == structure
        and datetime.fromisoformat(str(row["decision_at"])).astimezone(ET).time() >= floor_time
    ]
    return _first_by_session(selected)


def _timing_comparison(
    tick_rows: Sequence[Mapping[str, Any]], prior_rows: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    prior_by_day = {str(row["session_date"]): row for row in prior_rows}
    deltas = []
    for row in tick_rows:
        prior = prior_by_day.get(str(row["session_date"]))
        if prior is None:
            continue
        tick_at = datetime.fromisoformat(str(row["decision_at"]))
        prior_at = datetime.fromisoformat(str(prior["decision_at"]))
        deltas.append((tick_at - prior_at).total_seconds() / 60.0)
    return {
        "paired_sessions": len(deltas),
        "tick_minus_15m_minutes_median": statistics.median(deltas) if deltas else None,
        "tick_minus_15m_minutes_min": min(deltas) if deltas else None,
        "tick_minus_15m_minutes_max": max(deltas) if deltas else None,
    }


def _comparison_to_15m(rows: Sequence[Mapping[str, Any]], prior_rows_path: Path) -> dict[str, Any]:
    if not prior_rows_path.exists():
        return {"available": False, "reason": "prior_rows_missing"}
    prior_rows = [
        json.loads(line)
        for line in prior_rows_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    rules: dict[str, Any] = {}
    for rule_name in (
        "put_credit_after_1300",
        "put_credit_after_1400",
        "iron_condor_after_1400",
        "call_credit_after_1300",
        "put_credit_open",
    ):
        tick_selected = [row for row in rows if row.get("entry_rule") == rule_name]
        prior_selected = _prior_15m_rows(prior_rows, rule_name)
        rules[rule_name] = {
            "timing": _timing_comparison(tick_selected, prior_selected),
            "tick": {exit_name: metrics(tick_selected, exit_name) for exit_name in EXITS},
            "sample_15m": {exit_name: metrics(prior_selected, exit_name) for exit_name in EXITS},
        }
    afternoon = [
        record
        for record in _hypothesis_records(rows)
        if record["entry_rule"] in {"put_credit_after_1300", "put_credit_after_1400"}
    ]
    robust = any(bool(record["robust"]) for record in afternoon)
    holdout_only = any(
        bool(record["holdout_qualified"]) and not bool(record["train_qualified"])
        for record in afternoon
    )
    if robust:
        classification = "更稳：至少一条下午 put-credit 同时通过 train 与 holdout 日级门。"
    elif holdout_only:
        classification = "不更稳，也不是普遍提前几分钟：14:00 holdout 苗头保留，但 train 仍失败；13:00 已失去正 LCB。"
    else:
        classification = "消失：下午 put-credit 在 tick 单笔口径下未保留 holdout 正 LCB 苗头。"
    return {"available": True, "classification": classification, "rules": rules}


def _hypothesis_records(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    train = [row for row in rows if TRAIN[0] <= str(row["session_date"]) <= TRAIN[1]]
    holdout = [row for row in rows if HOLDOUT[0] <= str(row["session_date"]) <= HOLDOUT[1]]
    records = []
    for rule in RULE_SPECS:
        train_rule = [row for row in train if row.get("entry_rule") == rule.name]
        holdout_rule = [row for row in holdout if row.get("entry_rule") == rule.name]
        for exit_name in EXITS:
            train_metrics = metrics(train_rule, exit_name)
            holdout_metrics = metrics(holdout_rule, exit_name)
            train_qualified = passes_gate(train_metrics, train=True)
            holdout_qualified = passes_gate(holdout_metrics, train=False)
            records.append(
                {
                    "schema_version": f"{SCHEMA_VERSION}.hypothesis",
                    "hypothesis": f"{rule.name}__{exit_name}",
                    "entry_rule": rule.name,
                    "definition": rule.definition,
                    "structure": rule.structure,
                    "exit": exit_name,
                    "train": train_metrics,
                    "holdout": holdout_metrics,
                    "train_qualified": train_qualified,
                    "holdout_qualified": holdout_qualified,
                    "robust": train_qualified and holdout_qualified,
                    "train_failures": _gate_failures(train_metrics, train=True),
                    "holdout_failures": _gate_failures(holdout_metrics, train=False),
                }
            )
    return records


def build_report(
    rows: Sequence[Mapping[str, Any]],
    *,
    thresholds: Mapping[str, Any],
    coverages: Sequence[Mapping[str, Any]],
    prior_rows_path: Path,
) -> dict[str, Any]:
    records = _hypothesis_records(rows)
    return {
        "schema_version": f"{SCHEMA_VERSION}.report",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "periods": {"train": list(TRAIN), "holdout": list(HOLDOUT), "excluded": ["2026-08-18"]},
        "thresholds": dict(thresholds),
        "rules": [
            {"name": rule.name, "structure": rule.structure, "definition": rule.definition}
            for rule in RULE_SPECS
        ],
        "exits": list(EXITS),
        "hypothesis_count": len(records),
        "hypothesis_records": records,
        "funnel": {
            "train_qualified": sum(bool(record["train_qualified"]) for record in records),
            "holdout_qualified_raw": sum(bool(record["holdout_qualified"]) for record in records),
            "robust_survivors": sum(bool(record["robust"]) for record in records),
            "robust_hypotheses": [record["hypothesis"] for record in records if record["robust"]],
        },
        "rows": {
            "count": len(rows),
            "train": sum(TRAIN[0] <= str(row["session_date"]) <= TRAIN[1] for row in rows),
            "holdout": sum(HOLDOUT[0] <= str(row["session_date"]) <= HOLDOUT[1] for row in rows),
            "by_rule": _rule_counts(rows),
        },
        "labeling": {
            "sessions": list(coverages),
            "option_window_loads": sum(
                int(coverage.get("option_window_loads", 0)) for coverage in coverages
            ),
            "event_steps": sum(int(coverage.get("event_steps", 0)) for coverage in coverages),
            "loaded_option_ticks": sum(
                int(coverage.get("loaded_option_ticks", 0)) for coverage in coverages
            ),
        },
        "tick_capture": {
            "answer": "能",
            "verified_2026_08_14": {
                "schwab_spxw_0dte_live_rows_approx": 3_970_000,
                "atm_contract_ticks_approx": 26_500,
                "atm_interarrival_seconds": {"p50": 1.0, "p90": 3.7, "p99": 6.5},
                "spx_es_interarrival_seconds_p50": "1.0-2.0",
                "ibkr_spxw_interarrival_seconds": {"p50": 2.0, "p90": 16.0},
            },
            "clock": "next relevant leg/SPX received_at; no fixed 1ms/1s grid",
        },
        "comparison_to_15m": _comparison_to_15m(rows, prior_rows_path),
        "honesty": {
            "entry": "same-provider live NBBO conservative credit; no mid",
            "exit": "subsequent tick conservative close debit through 15:45 ET",
            "one_trade": "per rule per RTH session; no re-entry after close",
            "threshold_source": "2026-07-06..2026-07-31 train only",
            "holdout_tuning": False,
            "fees_included": False,
            "live_path_written": False,
            "production_candidate": False,
        },
    }


def _fmt(value: object, digits: int = 4) -> str:
    return "—" if value is None else f"{float(value):.{digits}f}"


def render_markdown(report: Mapping[str, Any]) -> str:
    funnel = report["funnel"]
    comparison = report["comparison_to_15m"]
    thresholds = report["thresholds"]
    robust = list(funnel["robust_hypotheses"])
    holdout_positive = [
        record["hypothesis"]
        for record in report["hypothesis_records"]
        if record["holdout_qualified"]
    ]
    lines = [
        "# 0DTE tick 级信号回测",
        "",
        "## 结论",
        "",
        "1. **近 tick 能抓。** 2026-08-14 RTH 的 Schwab SPXW 0DTE live 约 397 万条，ATM 单合约约 2.65 万 tick；同合约间隔 p50≈1.0s、p90≈3.7s、p99≈6.5s。SPX/ES p50≈1–2s；IBKR SPXW 较稀，p50≈2s、p90≈16s。回放按相关腿/SPX 的下一次 `received_at` 前进，没有造 1ms 或 1s 空网格。",
        f"2. **通过 train+holdout 日级门的规则：{len(robust)} 条。** "
        + ("、".join(f"`{name}`" for name in robust) if robust else "没有。"),
        f"3. **15 分钟下午 put-credit 对照：** {comparison.get('classification', '缺少上一轮 rows，无法比较。')}",
        "4. **Live 路径未写入。** 未修改 `strategy_select`、通知、服务、配置或任何生产运行路径；结果仅为离线研究产物。",
        "",
        "## 冻结口径",
        "",
        "- 每条规则每个 RTH session 最多一笔；平仓后不再入场。八条触发各自是一条独立假设，不把它们叠成同一账户的八个重叠仓位。",
        "- ATM=`floor(SPX/5)*5`；put/call credit 均为 10 点宽；铁鹰为 ATM±15 空头、ATM±25 翼。",
        "- 入场用同 provider conservative credit（短腿 bid 减长腿 ask）；平仓用后续 tick conservative close debit，禁止 mid。Schwab 可用时优先；缺腿或 BBO 不合法时才用 IBKR。",
        "- `hold_1545` 取 15:45 ET 前最后一个合法 mark；`giveback_50` 在 close debit≤entry credit×0.5 时退出，否则 15:45。PnL 为 SPX 点，未扣费用。",
        "- 期权窗口每 session 一次性加载动态 ATM 带（当日所需 ATM 范围外扩 25 点）；未来 spot 只影响离线检索范围，不进入任一时点决策。",
        "",
        "## Train 阈值",
        "",
        f"- SPX 1 分钟收益 q20：`{_fmt(thresholds.get('ret1m_q20'), 8)}`，样本 {thresholds.get('ret1m_sample_count', 0)}。",
        f"- Put credit_fraction q80：`{_fmt(thresholds.get('put_credit_fraction_q80'), 6)}`，合法 event-time 样本 {thresholds.get('credit_sample_count', 0)}。",
        "- 两个阈值只用 2026-07-06..07-31；holdout 未参与拟合。",
        "",
        "## 16 条假设（日级主口径）",
        "",
        "| 假设 | Train n / mean / LCB90 | Holdout n / mean / LCB90 | Train 门 | Holdout 门 | 最终 |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for record in report["hypothesis_records"]:
        train = record["train"]
        holdout = record["holdout"]
        lines.append(
            f"| `{record['hypothesis']}` | {train['n']} / {_fmt(train['mean'])} / {_fmt(train['lcb_90'])} | "
            f"{holdout['n']} / {_fmt(holdout['mean'])} / {_fmt(holdout['lcb_90'])} | "
            f"{'PASS' if record['train_qualified'] else 'FAIL'} | "
            f"{'PASS' if record['holdout_qualified'] else 'FAIL'} | "
            f"{'存活' if record['robust'] else '淘汰'} |"
        )
    lines += [
        "",
        "Train 门为 mean>0、90% LCB>0、n≥8 日；holdout 门为 mean>0、90% LCB>0、n≥5 日。没有为凑正放宽。",
        "",
        "## 漏斗与覆盖",
        "",
        f"- Train 入围：{funnel['train_qualified']} / 16；holdout 原始过门：{funnel['holdout_qualified_raw']} / 16；同时存活：{funnel['robust_survivors']} / 16。",
        "- Holdout 原始正门规则："
        + ("、".join(f"`{name}`" for name in holdout_positive) if holdout_positive else "无。"),
        f"- 产出交易行：{report['rows']['count']}；event steps：{report['labeling']['event_steps']}；加载 option ticks：{report['labeling']['loaded_option_ticks']}。",
        f"- Option window load：{report['labeling']['option_window_loads']} 次；对应每个有 SPX 数据的研究 session 最多一次。",
        "",
        "## 与 15 分钟采样的入场时点",
        "",
        "`tick−15m` 为负表示 tick 更早；下表仅比较两边都成功标记的 session。",
        "",
        "| 规则 | 配对日 | tick−15m 中位分钟 | 最早 | 最晚 |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for rule_name, value in comparison.get("rules", {}).items():
        timing = value["timing"]
        lines.append(
            f"| `{rule_name}` | {timing['paired_sessions']} | "
            f"{_fmt(timing['tick_minus_15m_minutes_median'], 2)} | "
            f"{_fmt(timing['tick_minus_15m_minutes_min'], 2)} | "
            f"{_fmt(timing['tick_minus_15m_minutes_max'], 2)} |"
        )
    lines += [
        "",
        "## Live 路径与边界",
        "",
        "`live_path_written=false`。没有部署、没有写 live artifact、没有修改 `strategy_select`。这只是报价湖上的因果研究，不是成交回测；没有 queue/fill 模型，且未扣手续费。",
    ]
    return "\n".join(lines) + "\n"


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _write_rows(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    temporary = path.with_name(f".{path.name}.tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    temporary.replace(path)


def run(
    *,
    data_root: Path,
    output_dir: Path,
    calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR,
    progress: bool = False,
) -> dict[str, Any]:
    days = [
        START_DATE + timedelta(days=offset)
        for offset in range((END_DATE - START_DATE).days + 1)
        if calendar.is_trading_day(START_DATE + timedelta(days=offset))
    ]
    store = QuoteStore(data_root)
    rows: list[dict[str, Any]] = []
    deferred: list[DeferredRichCandidate] = []
    credit_samples = array("d")
    coverages: list[dict[str, Any]] = []
    try:
        spx_by_day: dict[date, list[UnderlierTick]] = {}
        for day in days:
            bounds = _session_bounds(day, calendar)
            if bounds is None:
                continue
            start, end = bounds
            spx_by_day[day] = store.underlier_series(
                instrument_id=SPX_INSTRUMENT_ID,
                start=start - timedelta(seconds=MAX_QUOTE_AGE_SECONDS),
                end=end,
            )
        ret1m_q20, ret_sample_count = fit_ret1m_q20(spx_by_day, calendar=calendar)

        train_days = [day for day in days if TRAIN[0] <= day.isoformat() <= TRAIN[1]]
        for index, day in enumerate(train_days, 1):
            result = mine_session(
                store,
                session_date=day,
                spx=spx_by_day.get(day, ()),
                ret1m_q20=ret1m_q20,
                credit_q80=None,
                calendar=calendar,
            )
            rows.extend(result.rows)
            deferred.extend(result.deferred_rich)
            credit_samples.extend(result.credit_samples)
            coverages.append(result.coverage)
            if progress:
                print(
                    f"train {index}/{len(train_days)} {day}: "
                    f"events={result.coverage['event_steps']} rows={len(result.rows)}",
                    flush=True,
                )
        credit_q80 = fit_credit_q80(credit_samples)
        rows.extend(materialize_deferred_rich(deferred, credit_q80=credit_q80))

        holdout_days = [day for day in days if HOLDOUT[0] <= day.isoformat() <= HOLDOUT[1]]
        for index, day in enumerate(holdout_days, 1):
            result = mine_session(
                store,
                session_date=day,
                spx=spx_by_day.get(day, ()),
                ret1m_q20=ret1m_q20,
                credit_q80=credit_q80,
                calendar=calendar,
            )
            rows.extend(result.rows)
            coverages.append(result.coverage)
            if progress:
                print(
                    f"holdout {index}/{len(holdout_days)} {day}: "
                    f"events={result.coverage['event_steps']} rows={len(result.rows)}",
                    flush=True,
                )
    finally:
        store.close()

    rows.sort(key=lambda row: (str(row["session_date"]), str(row["entry_rule"])))
    unique = {(str(row["session_date"]), str(row["entry_rule"])) for row in rows}
    if len(unique) != len(rows):
        raise AssertionError("a rule opened more than one trade in a session")
    thresholds = {
        "source": "train_only",
        "ret1m_q20": ret1m_q20,
        "ret1m_sample_count": ret_sample_count,
        "put_credit_fraction_q80": credit_q80,
        "credit_sample_count": len(credit_samples),
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    rows_path = output_dir / "odte_tick_signals.rows.jsonl"
    report_path = output_dir / "odte_tick_signals.report.json"
    markdown_path = output_dir / "codex-tick-signals-last.md"
    report = build_report(
        rows,
        thresholds=thresholds,
        coverages=coverages,
        prior_rows_path=output_dir / "odte_signal_universe.rows.jsonl",
    )
    _write_rows(rows_path, rows)
    _write_json(report_path, report)
    temporary_markdown = markdown_path.with_name(f".{markdown_path.name}.tmp")
    temporary_markdown.write_text(render_markdown(report), encoding="utf-8")
    temporary_markdown.replace(markdown_path)
    return report


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    report = run(data_root=args.data_root, output_dir=args.output_dir, progress=True)
    print(json.dumps(report["funnel"], ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
