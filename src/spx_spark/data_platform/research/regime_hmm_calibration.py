"""Walk-forward calibration for the advisory online HMM regime filter.

This harness rebuilds per-minute market frames from the parquet quote lake,
runs the exact production observation + forward-filter code path, maps the
posterior through ``assess_regime`` (the v16 index-HMM path owner), and then
evaluates:

1. Same-day RTH close direction versus honest logistic baselines (unchanged
   gate contract).
2. HMM ``TREND`` hit rate and signed forward points at 30m / 60m, split by
   RTH cash-index and GTH Globex-futures baskets.

The output is a versioned calibration report with explicit gates.  Promotion
of the runtime research contract (``evidence_status`` / ``use_scope``) is a
separate, user-approved step that must cite a passing report.  HMM still has
``action_authority=none`` and cannot skip hard gates or order.

Documented reconstruction differences versus the live runtime:

- Frames are rebuilt from minute-level lake quotes, so cash-index source
  skew inside one minute can exceed the live 5s sync tolerance; the harness
  uses a relaxed research tolerance instead of silently dropping the
  component.
- No historical option frame is replayed, so the ES score scale uses the
  bounded local fallback exactly as production does when expected move is
  missing.
- The posterior starts uniform at the first Globex observation of the
  session and is carried through GTH into RTH (closer to production than
  the v1 RTH-open reset).
- RTH D / VWAP-cross / breadth inputs are not in the lake minute frame, so
  the ES-path fallback inside ``assess_regime`` is UNCERTAIN here.  The
  honest non-HMM baselines are the observation-score sign and ES 60m
  momentum, not a reconstructed RTH D TREND call.
"""

from __future__ import annotations

import argparse
import json
import math
import os
from collections.abc import Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

from spx_spark.application.market_features.market import build_minute_market_frame
from spx_spark.application.market_features.prior_rth_context import (
    build_prior_rth_context,
)
from spx_spark.application.order_map.strategy_regime import assess_regime
from spx_spark.application.runtime.market_regime_observation import (
    build_feature_observation,
)
from spx_spark.application.runtime.market_regime_signal import (
    MODEL_VERSION,
    _online_posterior,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.settings.market_features import MarketFeatureSettings


UTC = timezone.utc
ET = ZoneInfo("America/New_York")
SCHEMA_VERSION = "regime_hmm_calibration.v2"
POLICY_VERSION = "hmm_close_direction_logistic.v1"
FEATURE_RECONSTRUCTION = "quote_lake_minute_frames.v2"
CASH_INDEX_INSTRUMENTS = ("index:SPX", "index:NDX", "index:DJI", "index:RUT")
GLOBEX_INDEX_INSTRUMENTS = ("future:ES", "future:NQ", "future:YM", "future:RTY")
LAKE_INSTRUMENTS = (*GLOBEX_INDEX_INSTRUMENTS, *CASH_INDEX_INSTRUMENTS)
DECISION_TIMES_ET = tuple(
    time(hour, minute) for hour in range(10, 16) for minute in (0, 30)
)
# GTH clocks on Globex session D: prior-evening 21:00 ET through 08:00 ET.
GTH_DECISION_TIMES_ET = (
    time(21, 0),
    time(0, 0),
    time(3, 0),
    time(6, 0),
    time(8, 0),
)
RESEARCH_SYNC_TOLERANCE_SECONDS = 65.0
FORWARD_MATCH_TOLERANCE_MINUTES = 2
MIN_TRAIN_DAYS = 10
GATE_MIN_TEST_DAYS = 20
GATE_MIN_TEST_EVENTS = 120
GATE_MAX_ECE = 0.10
PROBABILITY_FLOOR = 1e-6
PATH_SKILL_HORIZONS = ("forward_30m_points", "forward_60m_points", "close_points")


@dataclass(frozen=True, slots=True)
class DecisionEvent:
    """One causal prediction opportunity at a fixed intraday decision time."""

    session_date: str
    at: str
    spx_price: float
    close_price: float
    label: int
    posterior_spread: float
    direction_score: float
    momentum_return_60m: float | None
    session_bucket: str = "rth"
    coordinate: str = "spx"
    es_price: float | None = None
    hmm_path_state: str | None = None
    hmm_path_direction: str | None = None
    hmm_used: bool = False
    cross_index_source: str | None = None
    cross_index_ready: bool = False
    score_direction: str | None = None
    momentum_direction: str | None = None
    forward_30m_points: float | None = None
    forward_60m_points: float | None = None


def lake_quotes_root(data_root: str | Path) -> Path:
    return Path(data_root) / "lake" / "quotes" / "schema=v1"


def list_lake_session_dates(data_root: str | Path) -> list[date]:
    """Trading dates that have a schwab quote partition in the lake."""

    dates: list[date] = []
    root = lake_quotes_root(data_root)
    if not root.exists():
        return dates
    for partition in sorted(root.glob("date=*")):
        raw = partition.name.removeprefix("date=")
        try:
            day = date.fromisoformat(raw)
        except ValueError:
            continue
        if not (partition / "provider=schwab").exists():
            continue
        if DEFAULT_MARKET_CALENDAR.session(day) is None:
            continue
        dates.append(day)
    return dates


def load_day_samples(data_root: str | Path, day: date) -> list[dict[str, object]]:
    """Rebuild normalized minute samples for one session date from the lake."""

    partition = lake_quotes_root(data_root) / f"date={day.isoformat()}" / "provider=schwab"
    if not partition.exists():
        return []
    glob = str(partition / "**" / "*.parquet")
    placeholders = ", ".join("?" for _ in LAKE_INSTRUMENTS)
    connection = duckdb.connect()
    try:
        rows = connection.execute(
            f"""
            SELECT instrument_id,
                   date_trunc('minute', source_at AT TIME ZONE 'UTC') AS minute,
                   arg_max(last, source_at) AS price,
                   arg_max(close, source_at) AS reference_close,
                   arg_max(volume, source_at) AS volume,
                   max(source_at) AS source_at
            FROM read_parquet(?)
            WHERE instrument_id IN ({placeholders})
              AND last IS NOT NULL AND last > 0
              AND source_at IS NOT NULL
            GROUP BY 1, 2
            ORDER BY 2
            """,
            [glob, *LAKE_INSTRUMENTS],
        ).fetchall()
    finally:
        connection.close()
    by_minute: dict[str, dict[str, object]] = {}
    for instrument_id, minute, price, reference_close, volume, source_at in rows:
        if minute is None or price is None or source_at is None:
            continue
        minute_at = minute.replace(tzinfo=UTC) if minute.tzinfo is None else minute.astimezone(UTC)
        at_key = minute_at.isoformat()
        sample = by_minute.setdefault(
            at_key,
            {
                "at": at_key,
                "session_id": _globex_session_id(minute_at),
                "segment": _segment(minute_at),
                "instruments": {},
            },
        )
        instruments = sample["instruments"]
        assert isinstance(instruments, dict)
        instruments[str(instrument_id)] = {
            "price": float(price),
            "reference_close": (
                float(reference_close)
                if isinstance(reference_close, int | float) and reference_close > 0
                else None
            ),
            "volume": float(volume) if isinstance(volume, int | float) else None,
            "price_kind": "last",
            "provider": "schwab",
            "quality": "live",
            "source_at": (
                source_at.astimezone(UTC)
                if source_at.tzinfo is not None
                else source_at.replace(tzinfo=UTC)
            ).isoformat(),
        }
    return [by_minute[at_key] for at_key in sorted(by_minute)]


def _segment(at: datetime) -> str:
    clock = at.astimezone(ET).time()
    if clock >= time(18) or clock < time(3):
        return "asia"
    if clock < time(8):
        return "europe"
    if clock < time(9, 30):
        return "us_premarket"
    if clock < time(16):
        return "rth"
    return "curb"


def _globex_session_id(at: datetime) -> str:
    local = at.astimezone(ET)
    business_date = local.date() + timedelta(days=1) if local.hour >= 18 else local.date()
    return business_date.isoformat()


def rth_decision_clocks(day: date) -> frozenset[datetime]:
    return frozenset(
        datetime.combine(day, decision_time, tzinfo=ET).astimezone(UTC)
        for decision_time in DECISION_TIMES_ET
    )


def gth_decision_clocks(day: date) -> frozenset[datetime]:
    """GTH decision clocks belonging to Globex/RTH session ``day``."""

    clocks: set[datetime] = set()
    for decision_time in GTH_DECISION_TIMES_ET:
        clock_day = day - timedelta(days=1) if decision_time >= time(18) else day
        clocks.add(datetime.combine(clock_day, decision_time, tzinfo=ET).astimezone(UTC))
    return frozenset(clocks)


def _instrument_price(sample: Mapping[str, object], instrument_id: str) -> float | None:
    instruments = sample.get("instruments")
    quote = instruments.get(instrument_id) if isinstance(instruments, Mapping) else None
    if isinstance(quote, Mapping) and isinstance(quote.get("price"), int | float):
        price = float(quote["price"])
        return price if math.isfinite(price) and price > 0 else None
    return None


def _forward_points(
    series: Mapping[datetime, float],
    at: datetime,
    minutes: int,
) -> float | None:
    price_now = series.get(at)
    if price_now is None:
        return None
    target = at + timedelta(minutes=minutes)
    later = series.get(target)
    if later is None:
        for extra in range(1, FORWARD_MATCH_TOLERANCE_MINUTES + 1):
            later = series.get(target + timedelta(minutes=extra))
            if later is not None:
                break
    if later is None:
        return None
    return later - price_now


def _signed_direction(value: float | None) -> str | None:
    if value is None or value == 0:
        return None
    return "UP" if value > 0 else "DOWN"


def _price_vs_vwap(es: Mapping[str, object]) -> str | None:
    distance = es.get("vwap_distance_points")
    if not isinstance(distance, int | float):
        return None
    if distance > 0:
        return "above"
    if distance < 0:
        return "below"
    return "at"


def _assess_hmm_path(
    *,
    posterior: tuple[float, float, float],
    observed_through: datetime,
    frame: object,
) -> dict[str, object]:
    es = frame.es if isinstance(getattr(frame, "es", None), Mapping) else {}
    cross_asset = (
        frame.cross_asset if isinstance(getattr(frame, "cross_asset", None), Mapping) else {}
    )
    cross = cross_asset.get("cross_index") if isinstance(cross_asset, Mapping) else {}
    cross = cross if isinstance(cross, Mapping) else {}
    facts = {
        "path": {
            "price_vs_vwap": _price_vs_vwap(es),
            "vwap_slope": es.get("vwap_slope_15m_points"),
            "efficiency_ratio_30m": es.get("trend_efficiency_30m"),
        },
        "hmm": {
            "status": "available",
            "posterior": {
                "state_00": posterior[0],
                "state_01": posterior[1],
                "state_02": posterior[2],
            },
            "observed_through": observed_through.isoformat(),
        },
        "cross_index": {
            "source": cross.get("source"),
            "status": cross.get("status"),
            "session_open": cross.get("session_open") is True,
            "anchor": cross.get("anchor"),
        },
        "event": {"state": "normal"},
    }
    assessment = assess_regime(facts)
    hmm = assessment.get("hmm") if isinstance(assessment.get("hmm"), Mapping) else {}
    return {
        "path_state": assessment.get("path_state"),
        "path_direction": assessment.get("path_direction"),
        "hmm_used": hmm.get("used") is True,
        "cross_index_source": cross.get("source"),
        "cross_index_ready": (
            cross.get("status") == "ready" and cross.get("session_open") is True
        ),
    }


def build_day_events(
    data_root: str | Path,
    day: date,
    *,
    prior_day: date,
) -> tuple[list[DecisionEvent], dict[str, object]]:
    """Run per-minute production frames and capture decision-time events."""

    session = DEFAULT_MARKET_CALENDAR.session(day)
    if session is None:
        return [], {"session_date": day.isoformat(), "skipped": "no_session"}
    samples = load_day_samples(data_root, day)
    prior_samples = load_day_samples(data_root, prior_day)
    prior_context = build_prior_rth_context(
        prior_samples,
        now=session.open_at,
    )
    policy = MarketFeatureSettings(
        provider_sync_tolerance_seconds=RESEARCH_SYNC_TOLERANCE_SECONDS,
    )
    open_at = session.open_at.astimezone(UTC)
    close_at = session.close_at.astimezone(UTC)
    session_id = day.isoformat()
    parsed: list[tuple[datetime, dict[str, object]]] = []
    spx_by_minute: dict[datetime, float] = {}
    es_by_minute: dict[datetime, float] = {}
    for sample in samples:
        at = datetime.fromisoformat(str(sample["at"]))
        parsed.append((at, sample))
        spx_price = _instrument_price(sample, "index:SPX")
        if spx_price is not None:
            spx_by_minute[at] = spx_price
        es_price = _instrument_price(sample, "future:ES")
        if es_price is not None:
            es_by_minute[at] = es_price
    session_rows = [
        (at, sample)
        for at, sample in parsed
        if sample.get("session_id") == session_id
    ]
    rth_spx = {at: price for at, price in spx_by_minute.items() if open_at <= at <= close_at}
    if len(rth_spx) < 300:
        return [], {
            "session_date": day.isoformat(),
            "skipped": "spx_rth_minutes_insufficient",
            "spx_rth_minutes": len(rth_spx),
        }
    close_price = rth_spx[max(rth_spx)]
    rth_clocks = rth_decision_clocks(day)
    gth_clocks = gth_decision_clocks(day)
    decision_ats = rth_clocks | gth_clocks
    last_decision_at = max(decision_ats)
    posterior = (1 / 3, 1 / 3, 1 / 3)
    events: list[DecisionEvent] = []
    window: list[dict[str, object]] = []
    observed = 0
    hmm_used_events = 0
    gth_events = 0
    rth_events = 0
    for minute_at, sample in session_rows:
        if minute_at > last_decision_at:
            break
        window.append(sample)
        frame = build_minute_market_frame(
            window,
            now=minute_at,
            expected_move_points=None,
            atm_iv=None,
            structural_levels=None,
            volume_baselines=None,
            policy=policy,
        )
        market = {
            "es": frame.es,
            "cross_asset": frame.cross_asset,
            "frame_id": frame.frame_id,
            "as_of": minute_at.isoformat(),
            "session_id": session_id,
            "quality": frame.quality.value,
        }
        observation = build_feature_observation(
            market,
            {},
            prior_context,
            session_day=day,
        )
        if observation is None:
            continue
        observed += 1
        score = float(observation["direction_score"])
        posterior = _online_posterior(score, posterior)
        if minute_at not in decision_ats:
            continue
        es_features = frame.es if isinstance(frame.es, Mapping) else {}
        momentum = (
            float(es_features["return_60m_points"])
            if isinstance(es_features.get("return_60m_points"), int | float)
            else None
        )
        mapped = _assess_hmm_path(
            posterior=posterior,
            observed_through=minute_at,
            frame=frame,
        )
        if minute_at in rth_clocks:
            spx_price = spx_by_minute.get(minute_at)
            if spx_price is None:
                continue
            bucket = "rth"
            coordinate = "spx"
            forward_30 = _forward_points(spx_by_minute, minute_at, 30)
            forward_60 = _forward_points(spx_by_minute, minute_at, 60)
            label = 1 if close_price > spx_price else 0
            rth_events += 1
        else:
            es_price = es_by_minute.get(minute_at)
            if es_price is None:
                continue
            spx_price = spx_by_minute.get(minute_at, es_price)
            bucket = "gth"
            coordinate = "es"
            forward_30 = _forward_points(es_by_minute, minute_at, 30)
            forward_60 = _forward_points(es_by_minute, minute_at, 60)
            label = 0
            gth_events += 1
        if mapped["hmm_used"]:
            hmm_used_events += 1
        events.append(
            DecisionEvent(
                session_date=day.isoformat(),
                at=minute_at.isoformat(),
                spx_price=spx_price,
                close_price=close_price,
                label=label,
                posterior_spread=float(posterior[2] - posterior[0]),
                direction_score=score,
                momentum_return_60m=momentum,
                session_bucket=bucket,
                coordinate=coordinate,
                es_price=es_by_minute.get(minute_at),
                hmm_path_state=str(mapped["path_state"]) if mapped["path_state"] else None,
                hmm_path_direction=(
                    str(mapped["path_direction"]) if mapped["path_direction"] else None
                ),
                hmm_used=bool(mapped["hmm_used"]),
                cross_index_source=(
                    str(mapped["cross_index_source"])
                    if mapped["cross_index_source"]
                    else None
                ),
                cross_index_ready=bool(mapped["cross_index_ready"]),
                score_direction=_signed_direction(score),
                momentum_direction=_signed_direction(momentum),
                forward_30m_points=forward_30,
                forward_60m_points=forward_60,
            )
        )
    diagnostics = {
        "session_date": day.isoformat(),
        "prior_session_date": prior_day.isoformat(),
        "prior_context_status": prior_context.get("status"),
        "spx_rth_minutes": len(rth_spx),
        "session_minutes": len(session_rows),
        "observation_updates": observed,
        "decision_events": len(events),
        "rth_events": rth_events,
        "gth_events": gth_events,
        "hmm_used_events": hmm_used_events,
    }
    return events, diagnostics


def fit_logistic(
    xs: Sequence[float],
    ys: Sequence[int],
    *,
    iterations: int = 50,
) -> tuple[float, float]:
    """Two-parameter logistic fit (intercept, slope) by Newton-Raphson."""

    if len(xs) != len(ys) or not xs:
        raise ValueError("logistic fit requires matched non-empty inputs")
    b0, b1 = 0.0, 0.0
    for _ in range(iterations):
        g0 = g1 = h00 = h01 = h11 = 0.0
        for x, y in zip(xs, ys, strict=True):
            p = _sigmoid(b0 + b1 * x)
            w = p * (1.0 - p)
            g0 += p - y
            g1 += (p - y) * x
            h00 += w
            h01 += w * x
            h11 += w * x * x
        # Light ridge keeps the Hessian invertible on separable samples.
        h00 += 1e-6
        h11 += 1e-6
        det = h00 * h11 - h01 * h01
        if abs(det) < 1e-12:
            break
        step0 = (h11 * g0 - h01 * g1) / det
        step1 = (h00 * g1 - h01 * g0) / det
        b0 -= step0
        b1 -= step1
        if max(abs(step0), abs(step1)) < 1e-10:
            break
    bound = 20.0
    return max(-bound, min(bound, b0)), max(-bound, min(bound, b1))


def _sigmoid(value: float) -> float:
    if value >= 0:
        z = math.exp(-value)
        return 1.0 / (1.0 + z)
    z = math.exp(value)
    return z / (1.0 + z)


def _clip_probability(value: float) -> float:
    return max(PROBABILITY_FLOOR, min(1.0 - PROBABILITY_FLOOR, value))


def brier_score(probabilities: Sequence[float], labels: Sequence[int]) -> float:
    return math.fsum(
        (probability - label) ** 2
        for probability, label in zip(probabilities, labels, strict=True)
    ) / len(labels)


def log_loss(probabilities: Sequence[float], labels: Sequence[int]) -> float:
    return -math.fsum(
        math.log(_clip_probability(probability if label else 1.0 - probability))
        for probability, label in zip(probabilities, labels, strict=True)
    ) / len(labels)


def expected_calibration_error(
    probabilities: Sequence[float],
    labels: Sequence[int],
    *,
    bins: int = 5,
) -> float:
    totals = [0] * bins
    probability_sums = [0.0] * bins
    label_sums = [0.0] * bins
    for probability, label in zip(probabilities, labels, strict=True):
        index = min(int(probability * bins), bins - 1)
        totals[index] += 1
        probability_sums[index] += probability
        label_sums[index] += label
    n = len(labels)
    return math.fsum(
        (totals[index] / n)
        * abs(probability_sums[index] / totals[index] - label_sums[index] / totals[index])
        for index in range(bins)
        if totals[index]
    )


def walk_forward(
    events_by_day: Mapping[str, Sequence[DecisionEvent]],
    *,
    min_train_days: int = MIN_TRAIN_DAYS,
) -> dict[str, object]:
    """Expanding-window evaluation: fit on days < d, predict day d."""

    ordered_days = sorted(events_by_day)
    predictions: dict[str, list[float]] = {"hmm": [], "momentum": [], "base_rate": [], "coin": []}
    labels: list[int] = []
    test_days: list[str] = []
    for index in range(min_train_days, len(ordered_days)):
        train_events = [
            event
            for day in ordered_days[:index]
            for event in events_by_day[day]
        ]
        test_events = list(events_by_day[ordered_days[index]])
        if not train_events or not test_events:
            continue
        train_labels = [event.label for event in train_events]
        base_rate = _clip_probability(sum(train_labels) / len(train_labels))
        hmm_fit = fit_logistic(
            [event.posterior_spread for event in train_events],
            train_labels,
        )
        momentum_train = [
            (event.momentum_return_60m, event.label)
            for event in train_events
            if event.momentum_return_60m is not None
        ]
        momentum_fit = (
            fit_logistic(
                [value for value, _ in momentum_train],
                [label for _, label in momentum_train],
            )
            if len(momentum_train) >= 20
            else None
        )
        test_days.append(ordered_days[index])
        for event in test_events:
            labels.append(event.label)
            predictions["hmm"].append(
                _clip_probability(_sigmoid(hmm_fit[0] + hmm_fit[1] * event.posterior_spread))
            )
            predictions["base_rate"].append(base_rate)
            predictions["coin"].append(0.5)
            if momentum_fit is not None and event.momentum_return_60m is not None:
                momentum_probability = _sigmoid(
                    momentum_fit[0] + momentum_fit[1] * event.momentum_return_60m
                )
            else:
                momentum_probability = base_rate
            predictions["momentum"].append(_clip_probability(momentum_probability))
    metrics = {
        name: (
            {
                "brier": round(brier_score(values, labels), 6),
                "log_loss": round(log_loss(values, labels), 6),
                "ece_5bin": round(expected_calibration_error(values, labels), 6),
            }
            if labels
            else None
        )
        for name, values in predictions.items()
    }
    return {
        "test_days": test_days,
        "n_test_days": len(test_days),
        "n_test_events": len(labels),
        "positive_rate": round(sum(labels) / len(labels), 6) if labels else None,
        "metrics": metrics,
    }


def evaluate_gates(evaluation: Mapping[str, object]) -> dict[str, object]:
    metrics = evaluation.get("metrics")
    metrics = metrics if isinstance(metrics, Mapping) else {}
    hmm = metrics.get("hmm") if isinstance(metrics.get("hmm"), Mapping) else None
    base = metrics.get("base_rate") if isinstance(metrics.get("base_rate"), Mapping) else None
    momentum = metrics.get("momentum") if isinstance(metrics.get("momentum"), Mapping) else None
    n_days = int(evaluation.get("n_test_days") or 0)
    n_events = int(evaluation.get("n_test_events") or 0)
    data_gate = n_days >= GATE_MIN_TEST_DAYS and n_events >= GATE_MIN_TEST_EVENTS
    skill_gate = bool(
        hmm is not None
        and base is not None
        and momentum is not None
        and float(hmm["brier"]) < float(base["brier"])
        and float(hmm["brier"]) < float(momentum["brier"])
    )
    calibration_gate = bool(hmm is not None and float(hmm["ece_5bin"]) <= GATE_MAX_ECE)
    gates = {
        "data_gate": {
            "passed": data_gate,
            "n_test_days": n_days,
            "min_test_days": GATE_MIN_TEST_DAYS,
            "n_test_events": n_events,
            "min_test_events": GATE_MIN_TEST_EVENTS,
        },
        "skill_gate": {
            "passed": skill_gate,
            "requirement": "hmm brier strictly below base_rate and momentum baselines",
        },
        "calibration_gate": {
            "passed": calibration_gate,
            "max_ece_5bin": GATE_MAX_ECE,
        },
    }
    return {
        "gates": gates,
        "verdict": (
            "pass"
            if data_gate and skill_gate and calibration_gate
            else "fail"
        ),
    }


def _horizon_points(event: DecisionEvent, horizon: str) -> float | None:
    if horizon == "forward_30m_points":
        return event.forward_30m_points
    if horizon == "forward_60m_points":
        return event.forward_60m_points
    if horizon == "close_points":
        if event.session_bucket != "rth":
            return None
        return event.close_price - event.spx_price
    raise ValueError(f"unknown horizon {horizon}")


def _signed_forward(points: float | None, direction: str | None) -> float | None:
    if points is None or direction not in {"UP", "DOWN"}:
        return None
    return points if direction == "UP" else -points


def _skill_row(events: Sequence[DecisionEvent], *, direction_of, horizon: str) -> dict[str, object]:
    signed: list[float] = []
    hits = misses = 0
    for event in events:
        value = _signed_forward(_horizon_points(event, horizon), direction_of(event))
        if value is None:
            continue
        signed.append(value)
        if value > 0:
            hits += 1
        elif value < 0:
            misses += 1
    n = hits + misses
    return {
        "n": n,
        "hit_rate": round(hits / n, 6) if n else None,
        "mean_signed_points": round(sum(signed) / len(signed), 6) if signed else None,
        "mean_abs_points": (
            round(sum(abs(value) for value in signed) / len(signed), 6) if signed else None
        ),
    }


def evaluate_path_skill(
    events: Sequence[DecisionEvent],
) -> dict[str, object]:
    """Directional skill of the production HMM path mapping versus baselines."""

    by_bucket: dict[str, list[DecisionEvent]] = {"rth": [], "gth": []}
    for event in events:
        if event.session_bucket in by_bucket:
            by_bucket[event.session_bucket].append(event)
    report: dict[str, object] = {
        "semantics": "research_evidence_only_not_execution_authority",
        "hmm_cannot_skip_hard_gates": True,
        "automatic_ordering": False,
        "baselines": {
            "hmm_trend": "assess_regime TREND UP/DOWN after VWAP contradiction",
            "score_sign": "sign of HMM observation direction_score",
            "momentum_sign": "sign of ES return_60m_points",
        },
    }
    for bucket, bucket_events in by_bucket.items():
        hmm_trend = [
            event
            for event in bucket_events
            if event.hmm_used
            and event.hmm_path_state == "TREND"
            and event.hmm_path_direction in {"UP", "DOWN"}
        ]
        hmm_balanced = [
            event for event in bucket_events if event.hmm_used and event.hmm_path_state == "BALANCED"
        ]
        state_counts: dict[str, int] = {}
        for event in bucket_events:
            key = str(event.hmm_path_state or "missing")
            state_counts[key] = state_counts.get(key, 0) + 1
        horizons = (
            PATH_SKILL_HORIZONS
            if bucket == "rth"
            else tuple(name for name in PATH_SKILL_HORIZONS if name != "close_points")
        )
        bucket_report: dict[str, object] = {
            "n_events": len(bucket_events),
            "hmm_used": sum(1 for event in bucket_events if event.hmm_used),
            "path_state_counts": dict(sorted(state_counts.items())),
            "coordinate": "spx" if bucket == "rth" else "es",
        }
        for horizon in horizons:
            bucket_report[horizon] = {
                "hmm_trend": _skill_row(
                    hmm_trend,
                    direction_of=lambda event: event.hmm_path_direction,
                    horizon=horizon,
                ),
                "score_sign": _skill_row(
                    bucket_events,
                    direction_of=lambda event: event.score_direction,
                    horizon=horizon,
                ),
                "momentum_sign": _skill_row(
                    bucket_events,
                    direction_of=lambda event: event.momentum_direction,
                    horizon=horizon,
                ),
            }
            if hmm_balanced:
                abs_moves = [
                    abs(points)
                    for event in hmm_balanced
                    if (points := _horizon_points(event, horizon)) is not None
                ]
                bucket_report[horizon]["hmm_balanced_mean_abs_points"] = (
                    round(sum(abs_moves) / len(abs_moves), 6) if abs_moves else None
                )
                bucket_report[horizon]["hmm_balanced_n"] = len(abs_moves)
        report[bucket] = bucket_report
    return report


def _day_worker(
    payload: tuple[str, str, str],
) -> tuple[str, list[DecisionEvent], dict[str, object]]:
    data_root, day_s, prior_s = payload
    events, diagnostics = build_day_events(
        data_root,
        date.fromisoformat(day_s),
        prior_day=date.fromisoformat(prior_s),
    )
    return day_s, events, diagnostics


def build_report(
    data_root: str | Path,
    *,
    generated_at: datetime | None = None,
    workers: int = 1,
) -> dict[str, object]:
    generated = (generated_at or datetime.now(tz=UTC)).astimezone(UTC)
    session_dates = list_lake_session_dates(data_root)
    events_by_day: dict[str, list[DecisionEvent]] = {}
    diagnostics: list[dict[str, object]] = []
    jobs: list[tuple[str, str, str]] = []
    for index, day in enumerate(session_dates):
        if index == 0:
            continue
        prior_day = DEFAULT_MARKET_CALENDAR.previous_trading_day(day)
        if prior_day not in session_dates:
            diagnostics.append(
                {"session_date": day.isoformat(), "skipped": "prior_session_not_in_lake"}
            )
            continue
        jobs.append((str(data_root), day.isoformat(), prior_day.isoformat()))
    worker_count = max(1, int(workers))
    if worker_count == 1 or len(jobs) <= 1:
        results = [_day_worker(job) for job in jobs]
    else:
        with ProcessPoolExecutor(max_workers=worker_count) as pool:
            results = list(pool.map(_day_worker, jobs))
    for day_s, events, day_diagnostics in results:
        diagnostics.append(day_diagnostics)
        if events:
            events_by_day[day_s] = events
    rth_events_by_day = {
        day: [event for event in events if event.session_bucket == "rth"]
        for day, events in events_by_day.items()
        if any(event.session_bucket == "rth" for event in events)
    }
    evaluation = walk_forward(rth_events_by_day)
    gates = evaluate_gates(evaluation)
    all_events = [event for events in events_by_day.values() for event in events]
    rth_events = [event for event in all_events if event.session_bucket == "rth"]
    test_days = {
        str(day) for day in (evaluation.get("test_days") or []) if isinstance(day, str)
    }
    oos_events = [event for event in all_events if event.session_date in test_days]
    path_skill = evaluate_path_skill(all_events)
    path_skill["sample"] = "in_sample_all_decision_events"
    path_skill_walk_forward = evaluate_path_skill(oos_events)
    path_skill_walk_forward["sample"] = "walk_forward_test_days_only"
    path_skill_walk_forward["test_days"] = sorted(test_days)
    fitted_policy = None
    if len(rth_events) >= GATE_MIN_TEST_EVENTS:
        intercept, slope = fit_logistic(
            [event.posterior_spread for event in rth_events],
            [event.label for event in rth_events],
        )
        fitted_policy = {
            "policy_version": POLICY_VERSION,
            "feature": "posterior_spread",
            "intercept": round(intercept, 10),
            "slope": round(slope, 10),
            "trained_through": max(rth_events_by_day) if rth_events_by_day else None,
            "n_events": len(rth_events),
        }
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated.isoformat(),
        "model_version": MODEL_VERSION,
        "feature_reconstruction": FEATURE_RECONSTRUCTION,
        "decision_times_et": [value.isoformat(timespec="minutes") for value in DECISION_TIMES_ET],
        "gth_decision_times_et": [
            value.isoformat(timespec="minutes") for value in GTH_DECISION_TIMES_ET
        ],
        "min_train_days": MIN_TRAIN_DAYS,
        "target": "spx_rth_close_above_decision_price",
        "path_skill_target": "signed_forward_points_on_session_coordinate",
        "session_dates_available": [day.isoformat() for day in session_dates],
        "n_days_with_events": len(events_by_day),
        "evaluation": evaluation,
        "path_skill": path_skill,
        "path_skill_walk_forward": path_skill_walk_forward,
        **gates,
        "fitted_policy": fitted_policy,
        "day_diagnostics": diagnostics,
        "semantics": (
            "walk_forward_research_evidence_only_not_execution_authority"
        ),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Walk-forward calibration report for the online HMM regime filter.",
    )
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument(
        "--output",
        type=Path,
        help="Report path (default: <data-root>/reports/research/regime_hmm_calibration.json)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=max(1, min(8, os.cpu_count() or 1)),
        help="Parallel session rebuild workers (default: min(8, CPUs))",
    )
    return parser.parse_args(argv)


def _print_path_skill(path_skill: Mapping[str, object]) -> None:
    for bucket in ("rth", "gth"):
        payload = path_skill.get(bucket)
        if not isinstance(payload, Mapping):
            continue
        print(
            f"path_skill {bucket}: events={payload.get('n_events')} "
            f"hmm_used={payload.get('hmm_used')} "
            f"states={payload.get('path_state_counts')}"
        )
        for horizon in PATH_SKILL_HORIZONS:
            rows = payload.get(horizon)
            if not isinstance(rows, Mapping):
                continue
            parts = []
            for name in ("hmm_trend", "score_sign", "momentum_sign"):
                row = rows.get(name)
                if not isinstance(row, Mapping) or not row.get("n"):
                    continue
                hit = row.get("hit_rate")
                mean = row.get("mean_signed_points")
                parts.append(
                    f"{name} n={row['n']} hit={hit:.3f} mean={mean:+.2f}"
                    if isinstance(hit, float) and isinstance(mean, float)
                    else f"{name} n={row['n']}"
                )
            if parts:
                print(f"  {horizon}: " + " | ".join(parts))


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report = build_report(args.data_root, workers=args.workers)
    output = args.output or (
        Path(args.data_root) / "reports" / "research" / "regime_hmm_calibration.json"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, allow_nan=False, indent=2, sort_keys=True) + "\n")
    evaluation = report["evaluation"]
    assert isinstance(evaluation, dict)
    print(f"report: {output}")
    print(
        "verdict:",
        report["verdict"],
        "| days:",
        evaluation["n_test_days"],
        "| events:",
        evaluation["n_test_events"],
    )
    for name, values in (evaluation.get("metrics") or {}).items():
        if values:
            print(
                f"  {name:>10}: brier={values['brier']:.4f}"
                f" log_loss={values['log_loss']:.4f} ece={values['ece_5bin']:.4f}"
            )
    path_skill = report.get("path_skill")
    if isinstance(path_skill, Mapping):
        print("path_skill in-sample:")
        _print_path_skill(path_skill)
    oos = report.get("path_skill_walk_forward")
    if isinstance(oos, Mapping):
        print("path_skill walk-forward test days:")
        _print_path_skill(oos)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
