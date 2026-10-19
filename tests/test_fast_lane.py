from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from spx_spark.application.fast_lane import run_fast_lane_cycle
from spx_spark.clock import FixedClock
from spx_spark.marketdata import InstrumentId, MarketDataQuality, Provider, Quote
from spx_spark.storage import LatestState


def _state(now: datetime, spot: float) -> LatestState:
    quote = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.INTERNAL,
        received_at=now,
        quote_time=now,
        trade_time=now,
        quality=MarketDataQuality.LIVE,
        last=spot,
    )
    return LatestState(
        created_at=now,
        as_of=now,
        quotes=(quote,),
        best_quotes=(quote,),
    )


def _decision(now: datetime, direction: str) -> dict[str, object]:
    return {
        "decision_id": "strategy:fast-lane",
        "decision_at": (now - timedelta(milliseconds=250)).isoformat(),
        "candidate": {
            "direction": direction,
            "trigger_level": 7700.0,
            "opportunity_valid_until": (now + timedelta(minutes=5)).isoformat(),
        },
    }


def test_fast_lane_publishes_armed_and_watch_without_authority(tmp_path) -> None:
    now = datetime(2026, 10, 16, 14, 0, tzinfo=timezone.utc)
    storage = SimpleNamespace(data_root=str(tmp_path), latest_stale_after_seconds=15)
    clock = FixedClock(now)

    armed = run_fast_lane_cycle(
        storage_settings=storage,
        state=_state(now, 7701.0),
        decision=_decision(now, "UP"),
        now=now,
        clock=clock,
    )
    assert armed["state"] == "armed"
    assert armed["authority"] == "none"
    assert 0.0 <= armed["latency_ms"] <= 1000.0
    assert armed["ready_budget_ms"] == 3000.0
    assert armed["fast_interval_ms"] == 1000.0
    assert armed["evidence_age_ms"] == 0.0
    assert json.loads((tmp_path / "latest" / "fast_lane.json").read_text())["state"] == "armed"

    watch = run_fast_lane_cycle(
        storage_settings=storage,
        state=_state(now, 7699.0),
        decision=_decision(now, "UP"),
        now=now,
        clock=clock,
    )
    assert watch["state"] == "watch"
    assert watch["trigger_level"] == 7700.0


def test_fast_lane_rejects_wrong_coordinate_stale_and_future_quotes(tmp_path):
    from dataclasses import replace

    now = datetime(2026, 10, 16, 14, 0, tzinfo=timezone.utc)
    storage = SimpleNamespace(data_root=str(tmp_path), latest_stale_after_seconds=15)
    state = _state(now, 7701.0)
    quote = state.quotes[0]
    variants = [
        replace(quote, instrument=InstrumentId.equity("SPY")),
        replace(quote, trade_time=now - timedelta(seconds=16)),
        replace(quote, trade_time=now + timedelta(seconds=1)),
        replace(quote, trade_time=None),
        replace(quote, quality=MarketDataQuality.FROZEN),
    ]
    for altered in variants:
        result = run_fast_lane_cycle(
            storage_settings=storage, state=replace(state, quotes=(altered,), best_quotes=(altered,)),
            decision=_decision(now, "UP"), now=now, persist=False,
        )
        assert result["state"] == "none"
        assert result["authority"] == "none"


@pytest.mark.parametrize(
    ("trade_age", "quote_age", "expected_state", "expected_price"),
    [(3600, 0, "watch", 7699), (0, 3600, "armed", 7701),
     (-1, 0, "watch", 7699), (3600, 3600, "none", 7701)],
)
def test_fast_lane_uses_the_selected_price_fields_clock(
    tmp_path, trade_age, quote_age, expected_state, expected_price,
):
    now = datetime(2026, 10, 16, 14, tzinfo=timezone.utc)
    base = _state(now, 7701)
    quote = replace(
        base.quotes[0], bid=7698, ask=7700,
        trade_time=now - timedelta(seconds=trade_age),
        quote_time=now - timedelta(seconds=quote_age),
    )
    result = run_fast_lane_cycle(
        storage_settings=SimpleNamespace(data_root=str(tmp_path), latest_stale_after_seconds=15),
        state=replace(base, quotes=(quote,), best_quotes=(quote,)),
        decision=_decision(now, "UP"), now=now, persist=False,
    )
    assert result["state"] == expected_state
    assert result["spot"] == expected_price
    assert result["evidence_age_ms"] == (trade_age if expected_price == 7701 else quote_age) * 1000
    assert result["authority"] == "none"


def test_fast_lane_does_not_treat_close_or_an_untimed_last_as_live(tmp_path):
    now = datetime(2026, 10, 16, 14, tzinfo=timezone.utc)
    base = _state(now, 7701)
    for quote in (
        replace(base.quotes[0], trade_time=None),
        replace(base.quotes[0], last=None, mark=7701, close=7701),
        replace(base.quotes[0], trade_time=now.replace(tzinfo=None)),
    ):
        result = run_fast_lane_cycle(
            storage_settings=SimpleNamespace(data_root=str(tmp_path), latest_stale_after_seconds=15),
            state=replace(base, quotes=(quote,), best_quotes=(quote,)),
            decision=_decision(now, "UP"), now=now, persist=False,
        )
        assert result["state"] == "none"
