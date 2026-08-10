"""Walk-forward calibration for the advisory online HMM regime filter.

This harness rebuilds per-minute market frames from the parquet quote lake,
runs the exact production observation + forward-filter code path, and then
evaluates whether the HMM posterior carries any calibrated predictive skill
for the same-day RTH close direction versus honest baselines.

The output is a versioned calibration report with explicit gates.  Promotion
of the runtime research contract (``evidence_status`` / ``use_scope``) is a
separate, user-approved step that must cite a passing report.

Documented reconstruction differences versus the live runtime:

- Frames are rebuilt from minute-level lake quotes, so cash-index source
  skew inside one minute can exceed the live 5s sync tolerance; the harness
  uses a relaxed research tolerance instead of silently dropping the
  component.
- No historical option frame is replayed, so the ES score scale uses the
  bounded local fallback exactly as production does when expected move is
  missing.
- The posterior starts uniform at the RTH open instead of carrying overnight
  GTH updates.
"""

from __future__ import annotations

import argparse
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

from spx_spark.application.market_features.market import build_minute_market_frame
from spx_spark.application.market_features.prior_rth_context import (
    build_prior_rth_context,
)
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
SCHEMA_VERSION = "regime_hmm_calibration.v1"
POLICY_VERSION = "hmm_close_direction_logistic.v1"
FEATURE_RECONSTRUCTION = "quote_lake_minute_frames.v1"
CASH_INDEX_INSTRUMENTS = ("index:SPX", "index:NDX", "index:DJI", "index:RUT")
LAKE_INSTRUMENTS = ("future:ES", *CASH_INDEX_INSTRUMENTS)
DECISION_TIMES_ET = tuple(
    time(hour, minute) for hour in range(10, 16) for minute in (0, 30)
)
RESEARCH_SYNC_TOLERANCE_SECONDS = 65.0
MIN_TRAIN_DAYS = 10
GATE_MIN_TEST_DAYS = 20
GATE_MIN_TEST_EVENTS = 120
GATE_MAX_ECE = 0.10
PROBABILITY_FLOOR = 1e-6


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
                "session_id": day.isoformat(),
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
    spx_by_minute: dict[datetime, float] = {}
    for sample in samples:
        at = datetime.fromisoformat(str(sample["at"]))
        instruments = sample.get("instruments")
        spx = instruments.get("index:SPX") if isinstance(instruments, Mapping) else None
        if isinstance(spx, Mapping) and isinstance(spx.get("price"), int | float):
            spx_by_minute[at] = float(spx["price"])
    rth_spx = {at: price for at, price in spx_by_minute.items() if open_at <= at <= close_at}
    if len(rth_spx) < 300:
        return [], {
            "session_date": day.isoformat(),
            "skipped": "spx_rth_minutes_insufficient",
            "spx_rth_minutes": len(rth_spx),
        }
    close_price = rth_spx[max(rth_spx)]
    decision_ats = {
        datetime.combine(day, decision_time, tzinfo=ET).astimezone(UTC)
        for decision_time in DECISION_TIMES_ET
    }
    posterior = (1 / 3, 1 / 3, 1 / 3)
    events: list[DecisionEvent] = []
    minutes = sorted(at for at in spx_by_minute if open_at <= at <= close_at)
    frame_samples = [
        sample
        for sample in samples
        if datetime.fromisoformat(str(sample["at"])) <= close_at
    ]
    observed = 0
    for minute_at in minutes:
        window = [
            sample
            for sample in frame_samples
            if datetime.fromisoformat(str(sample["at"])) <= minute_at
        ]
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
            "session_id": day.isoformat(),
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
        if minute_at in decision_ats:
            spx_price = spx_by_minute.get(minute_at)
            if spx_price is None:
                continue
            es_features = frame.es if isinstance(frame.es, Mapping) else {}
            momentum = es_features.get("return_60m_points")
            events.append(
                DecisionEvent(
                    session_date=day.isoformat(),
                    at=minute_at.isoformat(),
                    spx_price=spx_price,
                    close_price=close_price,
                    label=1 if close_price > spx_price else 0,
                    posterior_spread=float(posterior[2] - posterior[0]),
                    direction_score=score,
                    momentum_return_60m=(
                        float(momentum) if isinstance(momentum, int | float) else None
                    ),
                )
            )
    diagnostics = {
        "session_date": day.isoformat(),
        "prior_session_date": prior_day.isoformat(),
        "prior_context_status": prior_context.get("status"),
        "spx_rth_minutes": len(rth_spx),
        "observation_updates": observed,
        "decision_events": len(events),
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


def build_report(
    data_root: str | Path,
    *,
    generated_at: datetime | None = None,
) -> dict[str, object]:
    generated = (generated_at or datetime.now(tz=UTC)).astimezone(UTC)
    session_dates = list_lake_session_dates(data_root)
    events_by_day: dict[str, list[DecisionEvent]] = {}
    diagnostics: list[dict[str, object]] = []
    for index, day in enumerate(session_dates):
        if index == 0:
            continue
        prior_day = DEFAULT_MARKET_CALENDAR.previous_trading_day(day)
        if prior_day not in session_dates:
            diagnostics.append(
                {"session_date": day.isoformat(), "skipped": "prior_session_not_in_lake"}
            )
            continue
        events, day_diagnostics = build_day_events(
            data_root,
            day,
            prior_day=prior_day,
        )
        diagnostics.append(day_diagnostics)
        if events:
            events_by_day[day.isoformat()] = events
    evaluation = walk_forward(events_by_day)
    gates = evaluate_gates(evaluation)
    all_events = [event for events in events_by_day.values() for event in events]
    fitted_policy = None
    if len(all_events) >= GATE_MIN_TEST_EVENTS:
        intercept, slope = fit_logistic(
            [event.posterior_spread for event in all_events],
            [event.label for event in all_events],
        )
        fitted_policy = {
            "policy_version": POLICY_VERSION,
            "feature": "posterior_spread",
            "intercept": round(intercept, 10),
            "slope": round(slope, 10),
            "trained_through": max(events_by_day) if events_by_day else None,
            "n_events": len(all_events),
        }
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated.isoformat(),
        "model_version": MODEL_VERSION,
        "feature_reconstruction": FEATURE_RECONSTRUCTION,
        "decision_times_et": [value.isoformat(timespec="minutes") for value in DECISION_TIMES_ET],
        "min_train_days": MIN_TRAIN_DAYS,
        "target": "spx_rth_close_above_decision_price",
        "session_dates_available": [day.isoformat() for day in session_dates],
        "n_days_with_events": len(events_by_day),
        "evaluation": evaluation,
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
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report = build_report(args.data_root)
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
