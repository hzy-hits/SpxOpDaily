"""Walk-forward causality, metric sanity and gate logic for HMM calibration."""

from __future__ import annotations

import math
import random
from dataclasses import asdict
from datetime import date, datetime, time, timedelta, timezone

from spx_spark.application.runtime.market_regime_observation import (
    GTH_ES_FEATURE_WEIGHTS,
    GTH_ES_SCALE_FLOOR,
    GTH_OBSERVATION_COMPONENT_WEIGHTS,
    build_feature_observation,
)
from spx_spark.data_platform.research.regime_hmm_calibration import (
    ET,
    MIN_TRAIN_DAYS,
    DecisionEvent,
    TickPath,
    brier_score,
    evaluate_filter_slices,
    evaluate_gates,
    evaluate_gth_observation_recipes,
    evaluate_path_skill,
    expected_calibration_error,
    fit_logistic,
    gth_decision_clocks,
    log_loss,
    rth_decision_clocks,
    walk_forward,
)


def _event(
    day: str,
    index: int,
    *,
    label: int,
    spread: float,
    session_bucket: str = "rth",
    hmm_path_state: str | None = "TREND",
    hmm_path_direction: str | None = None,
    hmm_used: bool = True,
    forward_30m_points: float | None = None,
    forward_60m_points: float | None = None,
) -> DecisionEvent:
    direction = hmm_path_direction or ("UP" if label else "DOWN")
    signed = 4.0 if direction == "UP" else -4.0
    return DecisionEvent(
        session_date=day,
        at=f"{day}T1{index}:00:00+00:00",
        spx_price=7700.0,
        close_price=7710.0 if label else 7690.0,
        label=label,
        posterior_spread=spread,
        direction_score=spread,
        momentum_return_60m=5.0 if label else -5.0,
        session_bucket=session_bucket,
        coordinate="spx" if session_bucket == "rth" else "es",
        hmm_path_state=hmm_path_state,
        hmm_path_direction=direction if hmm_path_state == "TREND" else None,
        hmm_used=hmm_used,
        score_direction="UP" if spread > 0 else "DOWN",
        momentum_direction="UP" if label else "DOWN",
        forward_30m_points=forward_30m_points if forward_30m_points is not None else signed,
        forward_60m_points=forward_60m_points if forward_60m_points is not None else signed * 1.5,
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


def test_gth_decision_clocks_belong_to_the_globex_session() -> None:
    clocks = gth_decision_clocks(date(2026, 8, 5))
    local = sorted(clock.astimezone(ET) for clock in clocks)
    assert [value.time().replace(tzinfo=None) for value in local] == [
        time(21, 0),
        time(0, 0),
        time(3, 0),
        time(6, 0),
        time(8, 0),
    ]
    assert local[0].date().isoformat() == "2026-08-04"
    assert {value.date().isoformat() for value in local[1:]} == {"2026-08-05"}
    rth = rth_decision_clocks(date(2026, 8, 5))
    assert clocks.isdisjoint(rth)


def test_path_skill_counts_signed_hmm_trend_separately_from_gth() -> None:
    events = [
        _event("2026-08-05", 0, label=1, spread=0.6, hmm_path_direction="UP"),
        _event("2026-08-05", 1, label=0, spread=-0.6, hmm_path_direction="DOWN"),
        _event(
            "2026-08-05",
            2,
            label=1,
            spread=0.2,
            session_bucket="gth",
            hmm_path_direction="UP",
            forward_30m_points=3.0,
            forward_60m_points=-1.0,
        ),
        _event(
            "2026-08-05",
            3,
            label=1,
            spread=0.1,
            hmm_path_state="BALANCED",
            hmm_used=True,
            forward_30m_points=0.5,
        ),
    ]
    report = evaluate_path_skill(events)
    rth = report["rth"]
    gth = report["gth"]
    assert rth["n_events"] == 3
    assert gth["n_events"] == 1
    assert rth["forward_30m_points"]["hmm_trend"]["n"] == 2
    assert rth["forward_30m_points"]["hmm_trend"]["hit_rate"] == 1.0
    assert rth["close_points"]["hmm_trend"]["hit_rate"] == 1.0
    assert gth["forward_30m_points"]["hmm_trend"]["hit_rate"] == 1.0
    assert gth["forward_60m_points"]["hmm_trend"]["hit_rate"] == 0.0
    assert "close_points" not in gth
    assert rth["forward_30m_points"]["hmm_balanced_n"] == 1


def test_tick_path_measures_short_horizon_force() -> None:
    start = datetime(2026, 8, 5, 14, 0, tzinfo=timezone.utc)
    times = tuple(start + timedelta(seconds=offset) for offset in (0, 2, 5, 8, 15, 40, 70))
    path = TickPath(times, (100.0, 100.25, 100.5, 100.25, 101.0, 99.5, 102.0))
    assert path.forward_points(start, 5) == 0.5
    assert path.forward_points(start, 15) == 1.0
    mfe, mae = path.excursion(start, 60)
    assert mfe == 1.0
    assert mae == 0.5


def test_path_skill_includes_1m_and_quote_path_force() -> None:
    events = [
        _event(
            "2026-08-05",
            0,
            label=1,
            spread=0.6,
            hmm_path_direction="UP",
            forward_30m_points=4.0,
            forward_60m_points=6.0,
        ),
    ]
    # Frozen dataclass: rebuild with short-horizon fields via replace-like constructor.
    event = events[0]
    short = DecisionEvent(
        **{
            **asdict(event),
            "forward_1m_points": 1.0,
            "forward_5m_points": 2.0,
            "forward_5s_points": 0.25,
            "mfe_60s_points": 1.5,
            "mae_60s_points": 0.4,
            "momentum_1m_direction": "UP",
            "momentum_5m_direction": "UP",
        }
    )
    report = evaluate_path_skill([short])
    rth = report["rth"]
    assert rth["forward_1m_points"]["hmm_trend"]["hit_rate"] == 1.0
    assert rth["forward_5s_points"]["hmm_trend"]["mean_signed_points"] == 0.25
    assert rth["tick_force_60s"]["hmm_trend"]["mean_aligned_mfe"] == 1.5
    assert rth["tick_force_60s"]["hmm_trend"]["mean_adverse"] == 0.4


def test_filter_slices_separate_confirmation_from_weak_trend() -> None:
    base = _event("2026-08-05", 0, label=1, spread=0.6, hmm_path_direction="UP")
    confirmed = DecisionEvent(
        **{
            **asdict(base),
            "hmm_max_probability": 0.80,
            "momentum_1m_direction": "UP",
            "forward_60s_points": 1.0,
            "forward_1m_points": 1.0,
            "forward_5m_points": 1.0,
            "es_path_direction": "UP",
            "cross_index_direction": "DOWN",
        }
    )
    faded = DecisionEvent(
        **{
            **asdict(base),
            "at": "2026-08-05T11:00:00+00:00",
            "hmm_max_probability": 0.56,
            "momentum_1m_direction": "DOWN",
            "forward_60s_points": -1.0,
            "forward_1m_points": -1.0,
            "forward_5m_points": -1.0,
            "es_path_direction": "DOWN",
            "cross_index_direction": "UP",
        }
    )
    report = evaluate_filter_slices([confirmed, faded])
    sixty = report["rth"]["forward_60s_points"]
    assert sixty["hmm_trend_p55"]["n"] == 2
    assert sixty["hmm_trend_p70"]["n"] == 1
    assert sixty["hmm_trend_agree_1m"]["hit_rate"] == 1.0
    assert sixty["hmm_trend_disagree_1m"]["hit_rate"] == 0.0
    assert report["rth"]["mismatch"]["hmm_vs_es_1m"]["agree_rate"] == 0.5


def _globex_market(*, es_up: bool, cross_up: bool) -> dict[str, object]:
    sign = 1.0 if es_up else -1.0
    cross_sign = 1.0 if cross_up else -1.0
    return {
        "es": {
            "return_1m_points": 1.5 * sign,
            "return_5m_points": 2.5 * sign,
            "return_15m_points": 6.0 * sign,
            "return_60m_points": 10.0 * sign,
            "vwap_distance_points": 3.0 * sign,
            "vwap_slope_15m_points": 0.8 * sign,
            "trend_efficiency_60m": 0.8,
        },
        "cross_asset": {
            "cross_index": {
                "source": "globex_index",
                "status": "ready",
                "session_open": True,
                "relative_to_anchor_15m_bps": {
                    "future:ES": 0.0,
                    "future:NQ": 40.0 * cross_sign,
                    "future:YM": 25.0 * cross_sign,
                    "future:RTY": 30.0 * cross_sign,
                },
                "dispersion_15m_bps": 8.0,
                "breadth_15m": {
                    "up_count": 3 if cross_up else 1,
                    "down_count": 1 if cross_up else 3,
                    "flat_count": 0,
                },
                "missing_instruments": [],
                "reason_codes": [],
            }
        },
        "frame_id": "test-gth-obs",
        "as_of": "2026-08-05T08:00:00+00:00",
    }


def test_gth_70_30_weights_follow_es_when_cross_disagrees() -> None:
    market = _globex_market(es_up=True, cross_up=False)
    legacy = build_feature_observation(market, {}, {}, session_day=None)
    weighted = build_feature_observation(
        market,
        {},
        {},
        session_day=None,
        component_weights=GTH_OBSERVATION_COMPONENT_WEIGHTS,
        es_feature_weights=GTH_ES_FEATURE_WEIGHTS,
        es_scale_floor=GTH_ES_SCALE_FLOOR,
    )
    assert legacy is not None and weighted is not None
    assert weighted["direction_score"] > legacy["direction_score"]
    assert weighted["component_weights"]["es_path"] == 0.7
    assert weighted["components"]["es_path"]["feature_weights"]["return_1m_points"] == 0.35


def test_gth_observation_recipes_rank_named_inputs() -> None:
    base = _event("2026-08-05", 0, label=1, spread=0.6, session_bucket="gth")
    event = DecisionEvent(
        **{
            **asdict(base),
            "hmm_recipes": {
                "gth_70_30_fast": {
                    "state": "TREND",
                    "direction": "UP",
                    "probability": 0.8,
                },
                "legacy_20_70_10": {
                    "state": "TREND",
                    "direction": "DOWN",
                    "probability": 0.8,
                },
                "sign_nq_1m": {"state": "TREND", "direction": "UP", "probability": 1.0},
            },
            "forward_60s_points": 1.0,
            "forward_1m_points": 1.0,
            "forward_5m_points": 1.0,
        }
    )
    report = evaluate_gth_observation_recipes([event])
    sixty = report["horizons"]["forward_60s_points"]
    assert sixty["gth_70_30_fast"]["hit_rate"] == 1.0
    assert sixty["legacy_20_70_10"]["hit_rate"] == 0.0
    assert sixty["sign_nq_1m"]["hit_rate"] == 1.0
    assert report["ranked_forward_60s"][0]["name"] in {"gth_70_30_fast", "sign_nq_1m"}
