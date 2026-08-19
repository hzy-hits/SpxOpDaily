"""Causal GTH/RTH lead test: do parameters move before large breakouts/pullbacks?"""

from __future__ import annotations

import argparse
from bisect import bisect_right
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import json
import math
import random
import sqlite3
from pathlib import Path
from typing import Any

from spx_spark.data_platform.research.odte_level_quotes import QuoteStore
from spx_spark.data_platform.research.odte_level_signals import UnderlierTick
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET, MarketCalendar


SCHEMA_VERSION = "odte_direction_lead.v1"
TRAIN = ("2026-07-06", "2026-07-31")
HOLDOUT = ("2026-08-03", "2026-08-18")
ES_INSTRUMENT_ID = "future:ES"
SPX_INSTRUMENT_ID = "index:SPX"
BREAKOUT_MIN_POINTS = 8.0
BREAKOUT_ATR_MULT = 1.25
PULLBACK_FRAC = 0.40
PULLBACK_MIN_POINTS = 4.0
ATR_LOOKBACK = 30
RANGE_LOOKBACK = 30
COOLDOWN_MINUTES = 15
FORWARD_MINUTES = 5
LEAD_HORIZON_MINUTES = 5
CONTROL_EXCLUSION_MINUTES = 15
MIN_WARMUP = 35
CONTROL_PER_EVENT = 2
AUROC_MIN_EVENTS = 8
LIFT_USEFUL = 1.5

PATH_FEATURES = (
    "abs_ret_1m",
    "abs_ret_5m",
    "abs_ret_15m",
    "efficiency_15m",
    "near_extreme_30m",
    "atr_30m",
    "dist_session_open_abs",
)
FACT_FEATURES = (
    "hmm_trend_prob",
    "pace_ratio",
    "efficiency_fact",
    "atr_5m_fact",
    "abs_iv_change_5m",
    "wall_pressure",
)
PRICE_CONCURRENT = frozenset(
    {"abs_ret_1m", "abs_ret_5m", "abs_ret_15m", "near_extreme_30m"}
)


@dataclass(frozen=True, slots=True)
class MinuteBar:
    start: datetime
    end: datetime
    open: float
    high: float
    low: float
    close: float
    n_ticks: int


@dataclass(frozen=True, slots=True)
class Event:
    session_date: date
    session_mode: str
    kind: str
    direction: str
    start: datetime
    end: datetime
    move_points: float
    threshold: float
    impulse_points: float | None


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _finite(value: object) -> float | None:
    try:
        number = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number):
        return None
    return number


def session_windows(
    session_date: date, *, calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR
) -> tuple[tuple[str, datetime, datetime], ...]:
    window = calendar.spx_session_window(session_date)
    if window is None:
        return ()
    return (
        ("gth", _aware(window.session_start), _aware(window.gth_end)),
        ("rth", _aware(window.rth_open), _aware(window.session_end)),
    )


def build_minute_bars(
    ticks: Sequence[UnderlierTick], *, start: datetime, end: datetime
) -> list[MinuteBar]:
    """One ET-aligned minute bar per minute that has at least one live tick."""

    start = _aware(start)
    end = _aware(end)
    buckets: dict[datetime, list[UnderlierTick]] = defaultdict(list)
    for tick in ticks:
        at = _aware(tick.at)
        if at < start or at >= end or tick.price is None:
            continue
        minute = at.astimezone(ET).replace(second=0, microsecond=0).astimezone(timezone.utc)
        buckets[minute].append(tick)
    bars: list[MinuteBar] = []
    for minute in sorted(buckets):
        series = buckets[minute]
        prices = [float(tick.price) for tick in series]
        bars.append(
            MinuteBar(
                start=minute,
                end=minute + timedelta(minutes=1),
                open=prices[0],
                high=max(prices),
                low=min(prices),
                close=prices[-1],
                n_ticks=len(prices),
            )
        )
    return bars


def _atr(bars: Sequence[MinuteBar], index: int, lookback: int = ATR_LOOKBACK) -> float | None:
    if index < lookback:
        return None
    ranges: list[float] = []
    for offset in range(index - lookback + 1, index + 1):
        bar = bars[offset]
        previous = bars[offset - 1].close if offset else bar.open
        ranges.append(max(bar.high - bar.low, abs(bar.high - previous), abs(bar.low - previous)))
    return sum(ranges) / lookback if ranges else None


def path_features(bars: Sequence[MinuteBar], index: int) -> dict[str, float | None]:
    """Features at the close of ``bars[index]``; no future bars."""

    if index < 1:
        return {name: None for name in PATH_FEATURES} | {
            "ret_1m": None,
            "ret_5m": None,
            "ret_15m": None,
            "range_pos_30m": None,
            "near_extreme_30m": None,
            "dist_session_open": None,
        }
    close = bars[index].close
    ret_1m = close - bars[index - 1].close
    ret_5m = close - bars[index - 5].close if index >= 5 else None
    ret_15m = close - bars[index - 15].close if index >= 15 else None
    window = bars[max(0, index - RANGE_LOOKBACK + 1) : index + 1]
    high = max(bar.high for bar in window)
    low = min(bar.low for bar in window)
    span = high - low
    range_pos = (close - low) / span if span > 1e-9 else None
    path = 0.0
    if index >= 15:
        path = sum(abs(bars[j].close - bars[j - 1].close) for j in range(index - 14, index + 1))
    efficiency = abs(ret_15m) / path if ret_15m is not None and path > 1e-9 else None
    dist_open = close - bars[0].close
    return {
        "ret_1m": ret_1m,
        "ret_5m": ret_5m,
        "ret_15m": ret_15m,
        "abs_ret_1m": abs(ret_1m),
        "abs_ret_5m": None if ret_5m is None else abs(ret_5m),
        "abs_ret_15m": None if ret_15m is None else abs(ret_15m),
        "efficiency_15m": efficiency,
        "range_pos_30m": range_pos,
        "range_edge_30m": None if range_pos is None else min(range_pos, 1.0 - range_pos),
        "near_extreme_30m": None
        if range_pos is None
        else 1.0 - min(range_pos, 1.0 - range_pos),
        "atr_30m": _atr(bars, index),
        "dist_session_open": dist_open,
        "dist_session_open_abs": abs(dist_open),
    }


def detect_events(bars: Sequence[MinuteBar], *, session_date: date, session_mode: str) -> list[Event]:
    """Tag the first minute of a large 5-minute breakout or pullback."""

    events: list[Event] = []
    cooldown_until = bars[0].start if bars else None
    last_index = len(bars) - FORWARD_MINUTES - 1
    for index in range(MIN_WARMUP, last_index + 1):
        start = bars[index + 1].start
        if cooldown_until is not None and start < cooldown_until:
            continue
        atr = _atr(bars, index)
        if atr is None:
            continue
        move = bars[index + FORWARD_MINUTES].close - bars[index].close
        threshold = max(BREAKOUT_MIN_POINTS, BREAKOUT_ATR_MULT * atr)
        prior = bars[index - RANGE_LOOKBACK + 1 : index + 1]
        prior_high = max(bar.high for bar in prior)
        prior_low = min(bar.low for bar in prior)
        future_close = bars[index + FORWARD_MINUTES].close
        impulse_up = bars[index].close - min(bar.low for bar in prior)
        impulse_down = max(bar.high for bar in prior) - bars[index].close
        kind = None
        direction = None
        impulse = None
        if move >= threshold and future_close > prior_high:
            kind, direction, impulse = "breakout", "up", impulse_up
        elif move <= -threshold and future_close < prior_low:
            kind, direction, impulse = "breakout", "down", impulse_down
        else:
            pullback_up = max(PULLBACK_MIN_POINTS, PULLBACK_FRAC * impulse_down)
            pullback_down = max(PULLBACK_MIN_POINTS, PULLBACK_FRAC * impulse_up)
            if impulse_down >= BREAKOUT_MIN_POINTS and move >= pullback_up:
                kind, direction, impulse = "pullback", "up", impulse_down
            elif impulse_up >= BREAKOUT_MIN_POINTS and move <= -pullback_down:
                kind, direction, impulse = "pullback", "down", impulse_up
        if kind is None or direction is None:
            continue
        events.append(
            Event(
                session_date=session_date,
                session_mode=session_mode,
                kind=kind,
                direction=direction,
                start=start,
                end=bars[index + FORWARD_MINUTES].end,
                move_points=move,
                threshold=threshold,
                impulse_points=impulse,
            )
        )
        cooldown_until = start + timedelta(minutes=COOLDOWN_MINUTES)
    return events


def _auroc(scores: Sequence[float], labels: Sequence[int]) -> float | None:
    paired = [(score, label) for score, label in zip(scores, labels, strict=True) if math.isfinite(score)]
    positives = [score for score, label in paired if label == 1]
    negatives = [score for score, label in paired if label == 0]
    if len(positives) < 3 or len(negatives) < 3:
        return None
    greater = 0.0
    ties = 0.0
    for score in positives:
        for other in negatives:
            if score > other:
                greater += 1.0
            elif score == other:
                ties += 1.0
    return (greater + 0.5 * ties) / (len(positives) * len(negatives))


def _mean(values: Sequence[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def gate_metrics(flags: Sequence[bool], labels: Sequence[int]) -> dict[str, Any]:
    selected = [label for flag, label in zip(flags, labels, strict=True) if flag]
    base = sum(labels) / len(labels) if labels else 0.0
    hits = sum(selected)
    precision = hits / len(selected) if selected else None
    recall = hits / sum(labels) if sum(labels) else None
    lift = None if not precision or base <= 0 else precision / base
    return {
        "n": len(labels),
        "n_flagged": len(selected),
        "events": int(sum(labels)),
        "base_rate": base,
        "precision": precision,
        "recall": recall,
        "lift": lift,
        "useful": bool(lift is not None and lift >= LIFT_USEFUL and (precision or 0) > 0),
    }


def load_fact_rows(sqlite_path: Path) -> dict[date, list[tuple[datetime, dict[str, float | None]]]]:
    """Latest-before-T snapshots from persisted Core decisions (August+)."""

    if not sqlite_path.exists():
        return {}
    query = """
        SELECT session_date, decision_at,
               json_extract(attributes_json, '$.market_facts.hmm.posterior.state_00'),
               json_extract(attributes_json, '$.market_facts.hmm.posterior.state_02'),
               json_extract(attributes_json, '$.market_facts.hmm.dominant_state'),
               json_extract(attributes_json, '$.market_facts.es_volume.pace_ratio'),
               json_extract(attributes_json, '$.market_facts.es_volume.label'),
               json_extract(attributes_json, '$.market_facts.es_volume.direction'),
               json_extract(attributes_json, '$.market_facts.path.market_state'),
               json_extract(attributes_json, '$.market_facts.path.opening_range_state'),
               json_extract(attributes_json, '$.market_facts.path.efficiency_ratio_30m'),
               json_extract(attributes_json, '$.market_facts.path.atr_5m'),
               json_extract(attributes_json, '$.market_facts.volatility.atm_iv_change_5m'),
               json_extract(attributes_json, '$.market_facts.volatility.expected_move_points'),
               json_extract(attributes_json, '$.market_facts.structure.put_wall'),
               json_extract(attributes_json, '$.market_facts.structure.call_wall'),
               json_extract(attributes_json, '$.market_facts.spot.spx')
        FROM decisions
        WHERE attributes_json LIKE '%"market_facts"%'
        ORDER BY decision_at
    """
    grouped: dict[date, list[tuple[datetime, dict[str, float | None]]]] = defaultdict(list)
    with sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True) as connection:
        for row in connection.execute(query):
            session = date.fromisoformat(str(row[0]))
            at = datetime.fromisoformat(str(row[1]))
            if at.tzinfo is None:
                at = at.replace(tzinfo=timezone.utc)
            down = _finite(row[2])
            up = _finite(row[3])
            dominant = str(row[4] or "")
            trend_prob = None
            if dominant == "state_02":
                trend_prob = up
            elif dominant == "state_00":
                trend_prob = down
            spot = _finite(row[16])
            put_wall = _finite(row[14])
            call_wall = _finite(row[15])
            wall_pressure = None
            if spot is not None and put_wall is not None and call_wall is not None:
                width = call_wall - put_wall
                if width > 1e-9:
                    wall_pressure = min(abs(spot - put_wall), abs(call_wall - spot)) / width
            grouped[session].append(
                (
                    at.astimezone(timezone.utc),
                    {
                        "hmm_trend_prob": trend_prob,
                        "hmm_up": up,
                        "hmm_down": down,
                        "pace_ratio": _finite(row[5]),
                        "pace_elevated": 1.0 if str(row[6] or "") == "elevated" else 0.0,
                        "efficiency_fact": _finite(row[10]),
                        "atr_5m_fact": _finite(row[11]),
                        "abs_iv_change_5m": None
                        if _finite(row[12]) is None
                        else abs(_finite(row[12]) or 0.0),
                        "expected_move": _finite(row[13]),
                        "wall_pressure": wall_pressure,
                        "or_confirmed": 1.0
                        if str(row[9] or "")
                        in {"ABOVE_ORH_CONFIRMED", "BELOW_ORL_CONFIRMED"}
                        else 0.0,
                    },
                )
            )
    return dict(grouped)


def facts_at(
    rows: Sequence[tuple[datetime, dict[str, float | None]]], as_of: datetime
) -> dict[str, float | None]:
    if not rows:
        return {}
    index = bisect_right(rows, _aware(as_of), key=lambda item: item[0]) - 1
    if index < 0:
        return {}
    stamp, payload = rows[index]
    if _aware(as_of) - stamp > timedelta(minutes=3):
        return {}
    return payload


def _split(session_date: date) -> str:
    iso = session_date.isoformat()
    if TRAIN[0] <= iso <= TRAIN[1]:
        return "train"
    if HOLDOUT[0] <= iso <= HOLDOUT[1]:
        return "holdout"
    return "other"


def mine_session(
    store: QuoteStore,
    *,
    session_date: date,
    fact_rows: Sequence[tuple[datetime, dict[str, float | None]]] = (),
    rng: random.Random,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    minutes: list[dict[str, Any]] = []
    events_out: list[dict[str, Any]] = []
    for session_mode, start, end in session_windows(session_date):
        instrument = ES_INSTRUMENT_ID if session_mode == "gth" else SPX_INSTRUMENT_ID
        ticks = store.underlier_series(instrument_id=instrument, start=start, end=end)
        bars = build_minute_bars(ticks, start=start, end=end)
        if len(bars) < MIN_WARMUP + FORWARD_MINUTES + 1:
            continue
        events = detect_events(bars, session_date=session_date, session_mode=session_mode)
        event_by_start = {event.start: event for event in events}
        upcoming: set[datetime] = set()
        for event in events:
            cursor = event.start
            while cursor < event.start + timedelta(minutes=LEAD_HORIZON_MINUTES):
                upcoming.add(cursor)
                cursor += timedelta(minutes=1)
        exclusion = set(upcoming)
        for event in events:
            cursor = event.start - timedelta(minutes=CONTROL_EXCLUSION_MINUTES)
            stop = event.end + timedelta(minutes=CONTROL_EXCLUSION_MINUTES)
            while cursor <= stop:
                exclusion.add(cursor)
                cursor += timedelta(minutes=1)
        eligible_controls = [
            bar.end
            for bar in bars[MIN_WARMUP:]
            if bar.end + timedelta(minutes=FORWARD_MINUTES) <= bars[-1].end
            and bar.start not in exclusion
        ]
        for index, bar in enumerate(bars):
            if index < MIN_WARMUP or index + FORWARD_MINUTES >= len(bars):
                continue
            features = path_features(bars, index)
            facts = facts_at(fact_rows, bar.end)
            event = event_by_start.get(bars[index + 1].start)
            lead = 1 if bars[index + 1].start in upcoming else 0
            row = {
                "session_date": session_date.isoformat(),
                "split": _split(session_date),
                "session_mode": session_mode,
                "as_of": bar.end.isoformat(),
                "spot": bar.close,
                "y_event_next": 1 if event is not None else 0,
                "y_lead_5m": lead,
                "event_kind": None if event is None else event.kind,
                "event_direction": None if event is None else event.direction,
                "fwd5_points": bars[index + FORWARD_MINUTES].close - bar.close,
                **features,
                **facts,
            }
            minutes.append(row)
        for event in events:
            lead_index = next(
                (index for index, bar in enumerate(bars) if bar.end == event.start), None
            )
            if lead_index is None:
                continue
            payload = {
                "session_date": session_date.isoformat(),
                "split": _split(session_date),
                "session_mode": session_mode,
                "kind": event.kind,
                "direction": event.direction,
                "start": event.start.isoformat(),
                "move_points": event.move_points,
                "threshold": event.threshold,
                "impulse_points": event.impulse_points,
                "role": "event",
                **path_features(bars, lead_index),
                **facts_at(fact_rows, event.start),
            }
            events_out.append(payload)
            picked = eligible_controls[:]
            rng.shuffle(picked)
            for control_end in picked[:CONTROL_PER_EVENT]:
                control_index = next(
                    index for index, bar in enumerate(bars) if bar.end == control_end
                )
                events_out.append(
                    {
                        "session_date": session_date.isoformat(),
                        "split": _split(session_date),
                        "session_mode": session_mode,
                        "kind": event.kind,
                        "direction": event.direction,
                        "start": control_end.isoformat(),
                        "move_points": None,
                        "threshold": event.threshold,
                        "impulse_points": None,
                        "role": "control",
                        **path_features(bars, control_index),
                        **facts_at(fact_rows, control_end),
                    }
                )
    return minutes, events_out


def _subset(
    rows: Sequence[Mapping[str, Any]],
    *,
    split: str | None = None,
    session_mode: str | None = None,
    kind: str | None = None,
) -> list[Mapping[str, Any]]:
    selected = list(rows)
    if split:
        selected = [row for row in selected if row.get("split") == split]
    if session_mode:
        selected = [row for row in selected if row.get("session_mode") == session_mode]
    if kind:
        selected = [
            row
            for row in selected
            if row.get("kind") == kind or row.get("event_kind") == kind
        ]
    return selected


def feature_skill(rows: Sequence[Mapping[str, Any]], feature: str, label_key: str) -> dict[str, Any]:
    scores: list[float] = []
    labels: list[int] = []
    event_values: list[float] = []
    control_values: list[float] = []
    for row in rows:
        value = _finite(row.get(feature))
        if value is None:
            continue
        if label_key == "role":
            label = 1 if row.get("role") == "event" else 0
        else:
            label = int(row.get(label_key) or 0)
        scores.append(value)
        labels.append(label)
        (event_values if label else control_values).append(value)
    return {
        "feature": feature,
        "n": len(scores),
        "events": sum(labels),
        "mean_event": _mean(event_values),
        "mean_other": _mean(control_values),
        "auroc": _auroc(scores, labels),
        "family": "price_concurrent" if feature in PRICE_CONCURRENT else "non_price",
    }


def _gates(row: Mapping[str, Any]) -> dict[str, bool]:
    abs_1m = _finite(row.get("abs_ret_1m")) or 0.0
    abs_5m = _finite(row.get("abs_ret_5m")) or 0.0
    efficiency = _finite(row.get("efficiency_15m")) or 0.0
    edge = _finite(row.get("range_edge_30m"))
    return {
        "already_moving_1m": abs_1m >= 0.35,
        "already_moving_5m": abs_5m >= 1.0,
        "efficient_15m": efficiency >= 0.45,
        "near_30m_extreme": edge is not None and edge <= 0.08,
        "pace_elevated": (_finite(row.get("pace_ratio")) or 0.0) >= 1.5
        or (_finite(row.get("pace_elevated")) or 0.0) >= 1.0,
        "hmm_trend": (_finite(row.get("hmm_trend_prob")) or 0.0) >= 0.55,
        "or_confirmed": (_finite(row.get("or_confirmed")) or 0.0) >= 1.0,
        "iv_moving": (_finite(row.get("abs_iv_change_5m")) or 0.0) >= 0.01,
    }


def build_report(
    minutes: Sequence[Mapping[str, Any]], cases: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    scopes = [
        {"split": "train", "session_mode": None, "kind": None},
        {"split": "holdout", "session_mode": None, "kind": None},
        {"split": "holdout", "session_mode": "gth", "kind": None},
        {"split": "holdout", "session_mode": "rth", "kind": None},
        {"split": "holdout", "session_mode": None, "kind": "breakout"},
        {"split": "holdout", "session_mode": None, "kind": "pullback"},
        {"split": "train", "session_mode": "gth", "kind": None},
        {"split": "train", "session_mode": "rth", "kind": None},
    ]
    early: dict[str, Any] = {}
    case_control: dict[str, Any] = {}
    for scope in scopes:
        key = "|".join(
            str(scope[item] or "all") for item in ("split", "session_mode", "kind")
        )
        minute_rows = _subset(
            minutes,
            split=scope["split"],
            session_mode=scope["session_mode"],
            kind=None,
        )
        if scope["kind"]:
            minute_rows = [
                row
                for row in minute_rows
                if row.get("y_lead_5m") == 0 or row.get("event_kind") == scope["kind"]
            ]
        labels = [int(row.get("y_lead_5m") or 0) for row in minute_rows]
        gate_table = {}
        if minute_rows:
            flags = [_gates(row) for row in minute_rows]
            for gate in flags[0]:
                gate_table[gate] = gate_metrics(
                    [item[gate] for item in flags], labels
                )
        early[key] = {
            "n_minutes": len(minute_rows),
            "events_in_next_5m": int(sum(labels)),
            "base_rate": (sum(labels) / len(labels)) if labels else 0.0,
            "feature_auroc": {
                feature: feature_skill(minute_rows, feature, "y_lead_5m")
                for feature in PATH_FEATURES + FACT_FEATURES
            },
            "gates": gate_table,
        }
        case_rows = _subset(
            cases,
            split=scope["split"],
            session_mode=scope["session_mode"],
            kind=scope["kind"],
        )
        case_control[key] = {
            "n": len(case_rows),
            "events": sum(1 for row in case_rows if row.get("role") == "event"),
            "feature_auroc": {
                feature: feature_skill(case_rows, feature, "role")
                for feature in PATH_FEATURES + FACT_FEATURES
            },
        }

    def _useful_holdout() -> list[str]:
        holdout = early["holdout|all|all"]["gates"]
        return [
            name
            for name, values in holdout.items()
            if values.get("useful")
            and early["train|all|all"]["gates"].get(name, {}).get("useful")
        ]

    useful = _useful_holdout()
    holdout_auroc = early["holdout|all|all"]["feature_auroc"]
    anticipatory = [
        name
        for name, values in holdout_auroc.items()
        if values.get("family") == "non_price"
        and (values.get("auroc") or 0.5) >= 0.60
        and (early["train|all|all"]["feature_auroc"].get(name, {}).get("auroc") or 0.5)
        >= 0.60
        and (values.get("events") or 0) >= AUROC_MIN_EVENTS
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "windows": {"train": TRAIN, "holdout": HOLDOUT},
        "event_contract": {
            "forward_minutes": FORWARD_MINUTES,
            "breakout_min_points": BREAKOUT_MIN_POINTS,
            "breakout_atr_mult": BREAKOUT_ATR_MULT,
            "pullback_frac": PULLBACK_FRAC,
            "lead_horizon_minutes": LEAD_HORIZON_MINUTES,
            "gth_instrument": ES_INSTRUMENT_ID,
            "rth_instrument": SPX_INSTRUMENT_ID,
        },
        "counts": {
            "minutes": len(minutes),
            "case_rows": len(cases),
            "events": sum(1 for row in cases if row.get("role") == "event"),
            "train_events": sum(
                1
                for row in cases
                if row.get("role") == "event" and row.get("split") == "train"
            ),
            "holdout_events": sum(
                1
                for row in cases
                if row.get("role") == "event" and row.get("split") == "holdout"
            ),
        },
        "early_warning": early,
        "case_control": case_control,
        "useful_gates_train_and_holdout": useful,
        "anticipatory_features_train_and_holdout": anticipatory,
        "honesty": {
            "live_path_written": False,
            "price_concurrent_is_not_anticipation": True,
            "july_has_no_persisted_hmm_volume": True,
            "question": "parameters_feel_large_breakout_or_pullback_before_it_starts",
            "answer": (
                "yes_anticipatory"
                if anticipatory
                else "price_twitch_only"
                if any(
                    name in useful
                    for name in ("already_moving_1m", "already_moving_5m", "near_30m_extreme")
                )
                else "no_lead"
            ),
        },
    }


def write_markdown(report: Mapping[str, Any], path: Path) -> None:
    early = report["early_warning"]
    lines = [
        "# GTH/RTH 方向领先检验",
        "",
        "## 结论",
        "",
        "1. **事件合同**：未来 5 分钟突破 ≥ max(8 点, 1.25×30m ATR) 且创出此前 30 分钟新高/新低，记为大突破；先有 ≥8 点冲动、再反向回撤 ≥40%（最少 4 点），记为大回撤。特征只看事件开始前的已收盘 1 分钟。",
        f"2. **Train+holdout 都有用的门**：{', '.join(report['useful_gates_train_and_holdout']) or '无'}。",
        f"3. **非价格参数在两边 AUROC≥0.60**：{', '.join(report['anticipatory_features_train_and_holdout']) or '无'}。",
        f"4. **总判断**：`{report['honesty']['answer']}`。价格已经动了不等于提前感受到；HMM/量比/IV/墙位必须在价格启动前分开看。",
        "5. **未写入 live 路径。**",
        "",
        "## 覆盖",
        "",
        f"- minutes={report['counts']['minutes']} events={report['counts']['events']} "
        f"train={report['counts']['train_events']} holdout={report['counts']['holdout_events']}",
        "",
        "## Holdout 早警告门",
        "",
        "| 门 | flagged | precision | recall | lift | useful |",
        "| --- | ---: | ---: | ---: | ---: | --- |",
    ]
    for name, values in early["holdout|all|all"]["gates"].items():
        lines.append(
            f"| `{name}` | {values['n_flagged']} | "
            f"{values['precision'] if values['precision'] is not None else 'n/a'} | "
            f"{values['recall'] if values['recall'] is not None else 'n/a'} | "
            f"{values['lift'] if values['lift'] is not None else 'n/a'} | "
            f"{values['useful']} |"
        )
    lines.extend(
        [
            "",
            "## Holdout 特征 AUROC（未来 5 分钟会不会出事）",
            "",
            "| 特征 | family | n | events | AUROC | mean_event | mean_other |",
            "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for name, values in early["holdout|all|all"]["feature_auroc"].items():
        lines.append(
            f"| `{name}` | {values['family']} | {values['n']} | {values['events']} | "
            f"{values['auroc'] if values['auroc'] is not None else 'n/a'} | "
            f"{values['mean_event'] if values['mean_event'] is not None else 'n/a'} | "
            f"{values['mean_other'] if values['mean_other'] is not None else 'n/a'} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_study(
    *,
    data_root: Path,
    sqlite_path: Path,
    output_dir: Path,
    start_date: date,
    end_date: date,
    seed: int = 7,
) -> dict[str, Any]:
    calendar = DEFAULT_MARKET_CALENDAR
    store = QuoteStore(data_root)
    facts = load_fact_rows(sqlite_path)
    rng = random.Random(seed)
    minutes: list[dict[str, Any]] = []
    cases: list[dict[str, Any]] = []
    day = start_date
    try:
        while day <= end_date:
            if calendar.is_trading_day(day):
                session_minutes, session_cases = mine_session(
                    store, session_date=day, fact_rows=facts.get(day, ()), rng=rng
                )
                minutes.extend(session_minutes)
                cases.extend(session_cases)
            day += timedelta(days=1)
    finally:
        store.close()
    report = build_report(minutes, cases)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "odte_direction_lead.minutes.jsonl").write_text(
        "".join(json.dumps(row, default=str) + "\n" for row in minutes),
        encoding="utf-8",
    )
    (output_dir / "odte_direction_lead.cases.jsonl").write_text(
        "".join(json.dumps(row, default=str) + "\n" for row in cases),
        encoding="utf-8",
    )
    (output_dir / "odte_direction_lead.report.json").write_text(
        json.dumps(report, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    write_markdown(report, output_dir / "codex-direction-lead-last.md")
    return report


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, default=Path("/srv/data/spx-spark/data"))
    parser.add_argument("--sqlite", type=Path, default=Path("/srv/data/spx-spark/spx.sqlite"))
    parser.add_argument("--output-dir", type=Path, default=Path("/tmp/strategy-edge-backtest"))
    parser.add_argument("--start-date", type=date.fromisoformat, default=date.fromisoformat(TRAIN[0]))
    parser.add_argument("--end-date", type=date.fromisoformat, default=date.fromisoformat(HOLDOUT[1]))
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    run_study(
        data_root=args.data_root,
        sqlite_path=args.sqlite,
        output_dir=args.output_dir,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
