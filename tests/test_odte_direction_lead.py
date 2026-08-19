from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from spx_spark.data_platform.research.odte_direction_lead import (
    MinuteBar,
    build_minute_bars,
    build_report,
    detect_events,
    gate_metrics,
    path_features,
)
from spx_spark.data_platform.research.odte_level_signals import UnderlierTick
from spx_spark.market_calendar import ET


def _bar(index: int, close: float, *, high: float | None = None, low: float | None = None) -> MinuteBar:
    start = datetime(2026, 8, 17, 10, 0, tzinfo=ET).astimezone(timezone.utc) + timedelta(minutes=index)
    high_value = close if high is None else high
    low_value = close if low is None else low
    return MinuteBar(start, start + timedelta(minutes=1), close, high_value, low_value, close, 3)


def test_detects_first_breakout_bar_only() -> None:
    closes = [100.0] * 40 + [100.0, 100.0, 100.0, 100.0, 112.0] + [112.0] * 10
    bars = [_bar(index, price, high=price + 0.1, low=price - 0.1) for index, price in enumerate(closes)]
    events = detect_events(bars, session_date=date(2026, 8, 17), session_mode="rth")
    breakouts = [event for event in events if event.kind == "breakout"]
    assert len(breakouts) == 1
    assert breakouts[0].direction == "up"
    assert breakouts[0].start == bars[40].start


def test_path_features_do_not_see_the_breakout_bar() -> None:
    closes = [100.0] * 40 + [112.0]
    bars = [_bar(index, price) for index, price in enumerate(closes)]
    features = path_features(bars, 39)
    assert features["ret_1m"] == 0.0
    assert features["abs_ret_5m"] == 0.0


def test_pullback_requires_prior_impulse() -> None:
    up = [100.0 + index * 0.4 for index in range(40)]
    down = [116.0, 114.0, 112.0, 110.0, 108.0]
    bars = [_bar(index, price) for index, price in enumerate(up + down)]
    events = detect_events(bars, session_date=date(2026, 8, 17), session_mode="gth")
    pullbacks = [event for event in events if event.kind == "pullback"]
    assert pullbacks
    assert pullbacks[0].direction == "down"


def test_minute_bars_are_et_aligned() -> None:
    start = datetime(2026, 8, 17, 9, 30, tzinfo=ET).astimezone(timezone.utc)
    ticks = [
        UnderlierTick(start + timedelta(seconds=5), 10.0),
        UnderlierTick(start + timedelta(seconds=40), 11.0),
        UnderlierTick(start + timedelta(minutes=1, seconds=2), 12.0),
    ]
    bars = build_minute_bars(ticks, start=start, end=start + timedelta(minutes=3))
    assert [bar.n_ticks for bar in bars] == [2, 1]
    assert bars[0].open == 10.0
    assert bars[0].close == 11.0


def test_report_marks_no_live_write_and_needs_both_windows() -> None:
    train_minutes = [
        {
            "split": "train",
            "session_mode": "rth",
            "y_lead_5m": 1,
            "abs_ret_1m": 2.0,
            "abs_ret_5m": 2.0,
            "abs_ret_15m": 2.0,
            "efficiency_15m": 0.1,
            "near_extreme_30m": 0.2,
            "atr_30m": 1.0,
            "dist_session_open_abs": 1.0,
        },
        {
            "split": "train",
            "session_mode": "rth",
            "y_lead_5m": 0,
            "abs_ret_1m": 0.0,
            "abs_ret_5m": 0.0,
            "abs_ret_15m": 0.0,
            "efficiency_15m": 0.1,
            "near_extreme_30m": 0.2,
            "atr_30m": 1.0,
            "dist_session_open_abs": 1.0,
        },
    ] * 6
    holdout_minutes = [
        {
            "split": "holdout",
            "session_mode": "gth",
            "y_lead_5m": 0,
            "abs_ret_1m": 0.0,
            "abs_ret_5m": 0.0,
            "abs_ret_15m": 0.0,
            "efficiency_15m": 0.1,
            "near_extreme_30m": 0.2,
            "atr_30m": 1.0,
            "dist_session_open_abs": 1.0,
        }
    ] * 20
    report = build_report(train_minutes + holdout_minutes, [])
    assert report["honesty"]["live_path_written"] is False
    assert report["counts"]["events"] == 0
    metrics = gate_metrics([True, True, False], [1, 0, 0])
    assert metrics["precision"] == 0.5
    assert metrics["n_flagged"] == 2
