from __future__ import annotations

from datetime import datetime, timedelta, timezone

from spx_spark.application.market_features.es_bar_state import (
    advance_es_bar_state,
    completed_es_bars,
)


UTC = timezone.utc


def sample(
    at: datetime,
    price: float,
    *,
    provider: str = "schwab",
) -> dict[str, object]:
    return {
        "at": at.isoformat(),
        "segment": "rth",
        "instruments": {
            "future:ES": {
                "price": price,
                "provider": provider,
                "source_at": at.isoformat(),
                "quality": "live",
            }
        },
    }


def test_builds_real_five_minute_ohlc_without_filling_gaps() -> None:
    start = datetime(2026, 7, 24, 13, 30, tzinfo=UTC)
    state: dict[str, object] = {}
    for index in range(60):
        at = start + timedelta(seconds=index * 5)
        state = advance_es_bar_state(
            state,
            sample(at, 7400.0 + (index % 7)),
            now=at,
        )
    next_at = start + timedelta(minutes=5)
    state = advance_es_bar_state(
        state,
        sample(next_at, 7405.0),
        now=next_at,
    )

    bars = completed_es_bars(state)
    assert len(bars) == 1
    assert bars[0]["bar_start"] == start.isoformat()
    assert bars[0]["open"] == 7400.0
    assert bars[0]["high"] == 7406.0
    assert bars[0]["low"] == 7400.0
    assert bars[0]["close"] == 7403.0
    assert bars[0]["sample_count"] == 60
    assert bars[0]["quality"] == "ok"
    assert state["current_bar"]["bar_start"] == next_at.isoformat()


def test_duplicate_and_out_of_order_source_timestamps_do_not_inflate_bar() -> None:
    at = datetime(2026, 7, 24, 13, 30, tzinfo=UTC)
    state = advance_es_bar_state({}, sample(at, 7400.0), now=at)
    duplicate = advance_es_bar_state(
        state,
        sample(at, 7500.0),
        now=at + timedelta(seconds=1),
    )
    older = advance_es_bar_state(
        duplicate,
        sample(at - timedelta(seconds=1), 7300.0),
        now=at + timedelta(seconds=2),
    )

    assert older["current_bar"]["sample_count"] == 1
    assert older["current_bar"]["high"] == 7400.0
    assert (
        older["diagnostics"]["last_rejection"]
        == "es_source_timestamp_duplicate_or_out_of_order"
    )


def test_gap_is_partial_and_next_bar_is_marked_gap_before() -> None:
    start = datetime(2026, 7, 24, 13, 30, tzinfo=UTC)
    state = advance_es_bar_state({}, sample(start, 7400.0), now=start)
    next_at = start + timedelta(minutes=10)
    state = advance_es_bar_state(
        state,
        sample(next_at, 7410.0, provider="ibkr"),
        now=next_at,
    )

    bars = completed_es_bars(state)
    assert len(bars) == 1
    assert bars[0]["quality"] == "partial"
    assert state["current_bar"]["gap_before"] is True
    assert len(bars) == 1


def test_missing_and_future_quotes_fail_closed_without_mutating_samples() -> None:
    at = datetime(2026, 7, 24, 13, 30, tzinfo=UTC)
    missing = advance_es_bar_state({}, {}, now=at)
    future = advance_es_bar_state(
        missing,
        sample(at + timedelta(seconds=6), 7400.0),
        now=at,
    )

    assert future["closed_bars"] == []
    assert future["current_bar"] == {}
    assert future["diagnostics"]["last_rejection"] == "es_source_timestamp_future"


def test_provider_switch_is_visible_but_does_not_split_bar() -> None:
    start = datetime(2026, 7, 24, 13, 30, tzinfo=UTC)
    state = advance_es_bar_state({}, sample(start, 7400.0), now=start)
    at = start + timedelta(seconds=5)
    state = advance_es_bar_state(
        state,
        sample(at, 7400.25, provider="ibkr"),
        now=at,
    )

    assert state["current_bar"]["sample_count"] == 2
    assert state["current_bar"]["provider_counts"] == {"ibkr": 1, "schwab": 1}


def test_late_start_cannot_be_labeled_complete_even_with_many_samples() -> None:
    bucket_start = datetime(2026, 7, 24, 13, 30, tzinfo=UTC)
    first_at = bucket_start + timedelta(minutes=3)
    state: dict[str, object] = {}
    for index in range(24):
        at = first_at + timedelta(seconds=index * 5)
        state = advance_es_bar_state(
            state,
            sample(at, 7400.0 + index / 10),
            now=at,
        )
    next_at = bucket_start + timedelta(minutes=5)
    state = advance_es_bar_state(
        state,
        sample(next_at, 7405.0),
        now=next_at,
    )

    bar = completed_es_bars(state)[0]
    assert bar["sample_count"] == 24
    assert bar["leading_edge_gap_seconds"] == 180.0
    assert bar["quality"] == "partial"
