from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from spx_spark.application.market_features.trade_candidate import (
    advance_trade_candidate,
    gate_trade_intent,
    virtual_entry_intent,
)
from spx_spark.marketdata import InstrumentId, MarketDataQuality, Provider, Quote
from spx_spark.storage import LatestState


UTC = timezone.utc
NOW = datetime(2026, 7, 15, 15, 50, 51, tzinfo=UTC)
OPTION_ID = "option:SPX:SPXW:20260715:7560:P"


def test_target_before_entry_quote_retires_candidate_without_fill_claim(tmp_path) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    armed = advance_trade_candidate(
        storage,
        _latest(NOW, spx=7551.08, bid=14.6, ask=14.8),
        _intent(),
        now=NOW,
    )
    terminal = advance_trade_candidate(
        storage,
        _latest(NOW + timedelta(seconds=15), spx=7549.9, bid=14.5, ask=14.7),
        {"status": "observing"},
        now=NOW + timedelta(seconds=15),
    )

    assert armed["phase"] == "armed"
    assert terminal["phase"] == "target_passed"
    assert terminal["terminal_reason"] == "target_reached_before_entry_quote"
    assert terminal["execution_claim"] == "none"
    assert virtual_entry_intent(terminal) == {}
    gated = gate_trade_intent(_intent(), terminal)
    assert gated["status"] == "blocked"
    assert gated["block_reasons"] == ["target_reached_before_entry_quote"]
    repeated = advance_trade_candidate(
        storage,
        _latest(NOW + timedelta(seconds=20), spx=7549.5, bid=14.7, ask=14.9),
        _intent(),
        now=NOW + timedelta(seconds=20),
    )
    assert repeated["phase"] == "target_passed"
    assert gate_trade_intent(_intent(), repeated)["status"] == "blocked"
    rows = [
        json.loads(line)
        for line in (
            tmp_path / "features" / "trade_candidates" / "date=2026-07-15" / "events.jsonl"
        )
        .read_text()
        .splitlines()
    ]
    assert [row["event"] for row in rows] == ["candidate_armed", "candidate_terminal"]


def test_displayed_ask_reaching_limit_is_quote_observation_not_fill(tmp_path) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    advance_trade_candidate(
        storage,
        _latest(NOW, spx=7551.08, bid=14.6, ask=14.8),
        _intent(),
        now=NOW,
    )
    terminal = advance_trade_candidate(
        storage,
        _latest(NOW + timedelta(seconds=10), spx=7551.5, bid=14.5, ask=14.6),
        {"status": "observing"},
        now=NOW + timedelta(seconds=10),
    )

    assert terminal["phase"] == "quote_reached_entry"
    assert terminal["broker_order_state"] == "not_connected"
    assert terminal["entry_observation"]["entry_condition"] == ("displayed_ask_at_or_below_limit")
    shadow = virtual_entry_intent(terminal)
    assert shadow["source_intent_id"] == "intent:test-put"
    assert shadow["intent_id"] == "intent:test-put|level:test-put"
    assert shadow["execution_assumption"] == "displayed_quote_only_no_broker_fill"


def test_invalidation_before_entry_quote_retires_candidate(tmp_path) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    advance_trade_candidate(
        storage,
        _latest(NOW, spx=7551.08, bid=14.6, ask=14.8),
        _intent(),
        now=NOW,
    )
    terminal = advance_trade_candidate(
        storage,
        _latest(NOW + timedelta(seconds=10), spx=7563.1, bid=11.0, ask=11.2),
        {"status": "observing"},
        now=NOW + timedelta(seconds=10),
    )

    assert terminal["phase"] == "invalidated"
    assert terminal["terminal_reason"] == "invalidation_reached_before_entry_quote"


def _intent() -> dict[str, object]:
    return {
        "status": "trade_ready",
        "intent_id": "intent:test-put",
        "event_id": "level:test-put",
        "semantic_key": f"2026-07-15|level_breakout_put|7560|{OPTION_ID}",
        "direction": "down",
        "contract_id": OPTION_ID,
        "entry_limit": 14.6,
        "target_spx": 7550.0,
        "invalidation_spx": 7563.0,
        "expires_at": (NOW + timedelta(seconds=90)).isoformat(),
    }


def _latest(now: datetime, *, spx: float, bid: float, ask: float) -> LatestState:
    spx_quote = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.SCHWAB,
        received_at=now,
        last_update_at=now,
        quote_time=now,
        quality=MarketDataQuality.LIVE,
        mark=spx,
    )
    option_quote = Quote(
        instrument=InstrumentId.option(
            "SPX",
            expiry="20260715",
            strike=7560,
            right="P",
            trading_class="SPXW",
        ),
        provider=Provider.SCHWAB,
        received_at=now,
        last_update_at=now,
        quote_time=now,
        quality=MarketDataQuality.LIVE,
        bid=bid,
        ask=ask,
    )
    return LatestState(
        created_at=now,
        as_of=now,
        quotes=(spx_quote, option_quote),
        best_quotes=(spx_quote, option_quote),
    )
