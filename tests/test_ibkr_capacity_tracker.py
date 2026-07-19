from __future__ import annotations

from types import SimpleNamespace

from spx_spark.ibkr.stream.capacity_tracker import (
    MarketDataCapacityTracker,
    active_market_data_lines,
    is_ticker_limit_error,
)


def test_ticker_limit_does_not_confuse_message_rate_error() -> None:
    assert not is_ticker_limit_error(100, "Max rate of messages per second has been exceeded")
    assert is_ticker_limit_error(101, "Max number of tickers has been reached")
    assert is_ticker_limit_error(999, "Maximum number of market data lines reached")


def test_tracker_lowers_and_persists_runtime_capacity(tmp_path) -> None:
    path = tmp_path / "capacity.json"
    tracker = MarketDataCapacityTracker(path, configured_capacity=100)

    tracker.observe_success(active_lines=64)
    assert tracker.observe_error(
        error_code=101,
        message="Max number of tickers has been reached",
        active_lines=72,
    )
    assert tracker.effective_capacity == 72

    reloaded = MarketDataCapacityTracker(path, configured_capacity=100)
    assert reloaded.effective_capacity == 72
    assert reloaded.state.observed_lower_bound == 72


def test_tracker_recovers_one_line_after_repeated_full_success(tmp_path) -> None:
    path = tmp_path / "capacity.json"
    tracker = MarketDataCapacityTracker(path, configured_capacity=100)
    tracker.observe_error(error_code=101, message="ticker limit", active_lines=80)

    for _ in range(10):
        tracker.observe_success(active_lines=80)

    assert tracker.effective_capacity == 81


def test_active_line_count_deduplicates_labels_across_lanes() -> None:
    owner = SimpleNamespace(
        base_subs={"SPX": object()},
        hot_subs={"C1": object()},
        rotation_subs={"C1": object(), "P1": object()},
        pinned_subs={"C1": object(), "C2": object()},
        spy_subs={},
        slow_active_subs={"VIX": object()},
    )
    assert active_market_data_lines(owner) == 5
