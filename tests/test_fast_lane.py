from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

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
        replace(quote, quote_time=now - timedelta(seconds=16)),
        replace(quote, quote_time=now + timedelta(seconds=1)),
        replace(quote, quote_time=None),
        replace(quote, quality=MarketDataQuality.FROZEN),
    ]
    for altered in variants:
        result = run_fast_lane_cycle(
            storage_settings=storage, state=replace(state, quotes=(altered,), best_quotes=(altered,)),
            decision=_decision(now, "UP"), now=now, persist=False,
        )
        assert result["state"] == "none"
        assert result["authority"] == "none"
