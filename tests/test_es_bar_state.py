from __future__ import annotations

from datetime import datetime, timedelta, timezone

from spx_spark.application.market_features.es_bar_state import (
    MAX_CLOSED_BARS,
    MAX_RTH_MA_BARS,
    SCHEMA_VERSION,
    advance_es_bar_state,
    completed_es_bars,
)


UTC = timezone.utc


def test_full_bar_state_stays_bounded_and_compact_rth_history_covers_sma200() -> None:
    assert MAX_CLOSED_BARS == 432
    assert MAX_RTH_MA_BARS == 320
    assert MAX_RTH_MA_BARS >= 256


def sample(
    at: datetime,
    price: float,
    *,
    provider: str = "schwab",
    contract_identity: str | None = "ES:202609",
) -> dict[str, object]:
    return {
        "at": at.isoformat(),
        "segment": "rth",
        "instruments": {
            "future:ES": {
                "price": price,
                "provider": provider,
                "contract_identity": contract_identity,
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
    assert len(state["rth_ma_history"]) == 1
    assert "sample_count" not in state["rth_ma_history"][0]
    assert "provider_counts" not in state["rth_ma_history"][0]


def test_hot_full_bars_are_capped_while_older_rth_ma_sessions_are_returned() -> None:
    base = datetime(2026, 7, 10, 0, 0, tzinfo=UTC)

    def stored_bar(
        start: datetime,
        *,
        segment: str,
        trading_day: str,
    ) -> dict[str, object]:
        return {
            "bar_start": start.isoformat(),
            "bar_end": (start + timedelta(minutes=5)).isoformat(),
            "interval_seconds": 300,
            "open": 7400.0,
            "high": 7401.0,
            "low": 7399.0,
            "close": 7400.5,
            "quality": "ok",
            "gap_before": False,
            "segment": segment,
            "trading_date_et": trading_day,
            "contract_identity": "ES:202609",
        }

    full = [
        stored_bar(
            base + timedelta(minutes=5 * index),
            segment="globex",
            trading_day="2026-07-10",
        )
        for index in range(MAX_CLOSED_BARS + 25)
    ]
    rth: list[dict[str, object]] = []
    for day_offset in range(4):
        session_start = datetime(2026, 7, 6 + day_offset, 13, 30, tzinfo=UTC)
        rth.extend(
            stored_bar(
                session_start + timedelta(minutes=5 * index),
                segment="rth",
                trading_day=f"2026-07-{6 + day_offset:02d}",
            )
            for index in range(70)
        )
    at = base + timedelta(minutes=5 * (MAX_CLOSED_BARS + 30))
    previous = {
        "schema_version": SCHEMA_VERSION,
        "interval_seconds": 300,
        "updated_at": (at - timedelta(minutes=5)).isoformat(),
        "last_source_at": (at - timedelta(minutes=5)).isoformat(),
        "last_provider": "schwab",
        "contract_identity": "ES:202609",
        "current_bar": {},
        "closed_bars": full,
        "rth_ma_history": rth,
        "diagnostics": {},
    }

    state = advance_es_bar_state(previous, sample(at, 7410.0), now=at)
    completed = completed_es_bars(state)

    assert len(state["closed_bars"]) == MAX_CLOSED_BARS
    assert len(state["rth_ma_history"]) == 280
    assert len(completed) == MAX_CLOSED_BARS + 280
    assert sum(row.get("segment") == "rth" for row in completed) == 280
    assert all("provider_counts" not in row for row in state["rth_ma_history"])


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
    assert older["diagnostics"]["last_rejection"] == "es_source_timestamp_duplicate_or_out_of_order"


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


def test_known_contract_change_resets_bar_history() -> None:
    start = datetime(2026, 9, 17, 13, 30, tzinfo=UTC)
    state: dict[str, object] = {}
    for index in range(60):
        at = start + timedelta(seconds=index * 5)
        state = advance_es_bar_state(
            state,
            sample(at, 6500.0 + index / 100, contract_identity="ES:202609"),
            now=at,
        )
    next_at = start + timedelta(minutes=5)
    state = advance_es_bar_state(
        state,
        sample(next_at, 6505.0, contract_identity="ES:202609"),
        now=next_at,
    )
    assert len(completed_es_bars(state)) == 1

    roll_at = next_at + timedelta(seconds=5)
    rolled = advance_es_bar_state(
        state,
        sample(roll_at, 6520.0, contract_identity="ES:202612"),
        now=roll_at,
    )

    assert completed_es_bars(rolled) == []
    assert rolled["rth_ma_history"] == []
    assert rolled["contract_identity"] == "ES:202612"
    assert rolled["current_bar"]["contract_identity"] == "ES:202612"
    assert rolled["diagnostics"]["contract_reset_from"] == "ES:202609"
    assert rolled["diagnostics"]["contract_reset_to"] == "ES:202612"


def test_selected_generic_quote_does_not_borrow_other_provider_identity() -> None:
    at = datetime(2026, 9, 17, 13, 30, tzinfo=UTC)
    payload = sample(at, 6500.0, provider="ibkr", contract_identity=None)
    payload["es_by_provider"] = {
        "schwab": {
            "price": 6501.0,
            "provider": "schwab",
            "source_at": at.isoformat(),
            "contract_identity": "ES:202612",
        }
    }

    state = advance_es_bar_state({}, payload, now=at)

    assert state["contract_identity"] is None
    assert state["current_bar"]["contract_identity"] is None


def test_unknown_only_bar_after_known_contract_is_partial() -> None:
    start = datetime(2026, 9, 17, 13, 30, tzinfo=UTC)
    known = advance_es_bar_state(
        {},
        sample(start, 6500.0, contract_identity="ES:202609"),
        now=start,
    )
    unknown_start = start + timedelta(minutes=5)
    state = advance_es_bar_state(
        known,
        sample(unknown_start, 6505.0, contract_identity=None),
        now=unknown_start,
    )
    for index in range(1, 60):
        at = unknown_start + timedelta(seconds=index * 5)
        state = advance_es_bar_state(
            state,
            sample(at, 6505.0 + index / 100, contract_identity=None),
            now=at,
        )
    next_at = unknown_start + timedelta(minutes=5)
    state = advance_es_bar_state(
        state,
        sample(next_at, 6510.0, contract_identity=None),
        now=next_at,
    )

    unknown_bar = completed_es_bars(state)[-1]
    assert unknown_bar["contract_identity_ambiguous"] is True
    assert unknown_bar["quality"] == "partial"


def test_unknown_contract_observation_makes_mixed_bar_partial() -> None:
    start = datetime(2026, 9, 17, 13, 30, tzinfo=UTC)
    state: dict[str, object] = {}
    for index in range(60):
        at = start + timedelta(seconds=index * 5)
        identity = None if index == 1 else "ES:202609"
        state = advance_es_bar_state(
            state,
            sample(at, 6500.0 + index / 100, contract_identity=identity),
            now=at,
        )
    next_at = start + timedelta(minutes=5)
    state = advance_es_bar_state(
        state,
        sample(next_at, 6505.0, contract_identity="ES:202609"),
        now=next_at,
    )

    mixed = completed_es_bars(state)[0]
    assert mixed["contract_identity"] is None
    assert mixed["contract_identity_ambiguous"] is True
    assert mixed["quality"] == "partial"


def test_cross_provider_contract_conflict_rejects_until_sources_align() -> None:
    start = datetime(2026, 9, 17, 13, 30, tzinfo=UTC)
    state = advance_es_bar_state(
        {},
        sample(start, 6500.0, contract_identity="ES:202609"),
        now=start,
    )
    conflict_at = start + timedelta(seconds=5)
    conflict = sample(conflict_at, 6520.0, contract_identity="ES:202612")
    conflict["es_by_provider"] = {
        "schwab": {"contract_identity": "ES:202612"},
        "ibkr": {"contract_identity": "ES:202609"},
    }

    rejected = advance_es_bar_state(state, conflict, now=conflict_at)

    assert rejected["contract_identity"] == "ES:202609"
    assert rejected["current_bar"]["close"] == 6500.0
    assert rejected["diagnostics"]["last_rejection"] == "es_contract_identity_provider_conflict"

    aligned_at = conflict_at + timedelta(seconds=5)
    aligned = sample(aligned_at, 6521.0, contract_identity="ES:202612")
    aligned["es_by_provider"] = {
        "schwab": {"contract_identity": "ES:202612"},
        "ibkr": {"contract_identity": "ES:202612"},
    }
    rolled = advance_es_bar_state(rejected, aligned, now=aligned_at)

    assert rolled["contract_identity"] == "ES:202612"
    assert rolled["current_bar"]["open"] == 6521.0
    assert rolled["diagnostics"]["contract_reset_from"] == "ES:202609"


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
