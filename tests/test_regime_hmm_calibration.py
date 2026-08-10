"""Walk-forward causality, metric sanity and gate logic for HMM calibration."""

from __future__ import annotations

import math
import random

from spx_spark.data_platform.research.regime_hmm_calibration import (
    MIN_TRAIN_DAYS,
    DecisionEvent,
    brier_score,
    evaluate_gates,
    expected_calibration_error,
    fit_logistic,
    log_loss,
    walk_forward,
)


def _event(day: str, index: int, *, label: int, spread: float) -> DecisionEvent:
    return DecisionEvent(
        session_date=day,
        at=f"{day}T1{index}:00:00+00:00",
        spx_price=7700.0,
        close_price=7710.0 if label else 7690.0,
        label=label,
        posterior_spread=spread,
        direction_score=spread,
        momentum_return_60m=5.0 if label else -5.0,
    )


def _synthetic_days(n_days: int, *, informative: bool) -> dict[str, list[DecisionEvent]]:
    rng = random.Random(7)
    days: dict[str, list[DecisionEvent]] = {}
    for day_index in range(n_days):
        day = f"2026-07-{day_index + 1:02d}"
        events = []
        for event_index in range(6):
            label = rng.random() < 0.5
            spread = (
                (0.6 if label else -0.6) + rng.uniform(-0.2, 0.2)
                if informative
                else rng.uniform(-1.0, 1.0)
            )
            events.append(_event(day, event_index, label=int(label), spread=spread))
        days[day] = events
    return days


def test_fit_logistic_recovers_positive_relationship() -> None:
    xs = [-1.0, -0.8, -0.5, 0.4, 0.7, 1.0, -0.9, 0.8]
    ys = [0, 0, 0, 1, 1, 1, 0, 1]
    intercept, slope = fit_logistic(xs, ys)
    assert slope > 0.0
    assert abs(intercept) < 5.0


def test_metrics_reward_the_perfect_predictor() -> None:
    labels = [1, 0, 1, 1, 0]
    perfect = [0.99, 0.01, 0.99, 0.99, 0.01]
    coin = [0.5] * 5
    assert brier_score(perfect, labels) < brier_score(coin, labels)
    assert log_loss(perfect, labels) < log_loss(coin, labels)
    assert expected_calibration_error(perfect, labels) < 0.05
    assert math.isclose(brier_score(coin, labels), 0.25)


def test_walk_forward_only_tests_days_after_the_training_window() -> None:
    days = _synthetic_days(MIN_TRAIN_DAYS + 4, informative=True)
    result = walk_forward(days)
    ordered = sorted(days)
    assert result["test_days"] == ordered[MIN_TRAIN_DAYS:]
    assert result["n_test_events"] == 4 * 6
    metrics = result["metrics"]
    assert metrics["hmm"]["brier"] < metrics["coin"]["brier"]


def test_uninformative_signal_does_not_pass_the_skill_gate() -> None:
    days = _synthetic_days(MIN_TRAIN_DAYS + 25, informative=False)
    result = walk_forward(days)
    verdict = evaluate_gates(result)
    assert verdict["gates"]["data_gate"]["passed"] is True
    # A random posterior spread must not beat both baselines by luck here.
    assert verdict["verdict"] == "fail" or (
        verdict["gates"]["skill_gate"]["passed"] is False
    )


def test_gates_fail_closed_on_insufficient_data() -> None:
    days = _synthetic_days(MIN_TRAIN_DAYS + 2, informative=True)
    result = walk_forward(days)
    verdict = evaluate_gates(result)
    assert verdict["gates"]["data_gate"]["passed"] is False
    assert verdict["verdict"] == "fail"
