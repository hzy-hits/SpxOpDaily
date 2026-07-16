from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from spx_spark.application.market_features.virtual_strategy import (
    _episode,
    _exit_decision,
)
from spx_spark.settings.market_features import MarketFeatureSettings


UTC = timezone.utc
NOW = datetime(2026, 7, 15, 15, 50, tzinfo=UTC)


def test_trade_episode_preserves_put_direction() -> None:
    episode = _episode(
        source_id="intent:put",
        source_kind="trade_intent",
        direction="down",
        contract_id="option:SPX:SPXW:20260715:7560:P",
        snapshot={"mid": 14.7, "underlier": 7551.0},
        now=NOW,
        stop=NOW + timedelta(minutes=15),
        invalidation_spx=7563.0,
        target_spx=7550.0,
        invalidation_es=None,
    )

    assert episode["direction"] == "down"
    assert episode["execution_assumption"] == "none"


def test_long_put_uses_downside_target_and_upside_invalidation() -> None:
    active = {
        "direction": "down",
        "source_kind": "trade_intent",
        "entry_mid": 14.7,
        "invalidation_spx": 7563.0,
        "target_spx": 7550.0,
        "time_stop_at": (NOW + timedelta(minutes=15)).isoformat(),
    }
    latest = SimpleNamespace(best_quote=lambda _instrument_id: None, as_of=NOW)
    common = {
        "latest": latest,
        "option_structure": {"call_wall": 7560.0},
        "macro_event": {},
        "greek_decision": {},
        "now": NOW,
        "policy": MarketFeatureSettings(),
    }

    assert _exit_decision(active, {"mid": 14.7, "underlier": 7551.0}, **common) == (
        None,
        None,
    )
    assert _exit_decision(active, {"mid": 14.7, "underlier": 7563.0}, **common) == (
        "strategy_invalidation",
        "exit",
    )
    assert _exit_decision(active, {"mid": 14.7, "underlier": 7549.0}, **common) == (
        "underlier_target_reached",
        "take_profit",
    )


def test_long_call_keeps_upside_target_and_downside_invalidation() -> None:
    active = {
        "direction": "up",
        "source_kind": "trade_intent",
        "entry_mid": 10.0,
        "invalidation_spx": 7547.0,
        "target_spx": 7575.0,
        "time_stop_at": (NOW + timedelta(minutes=15)).isoformat(),
    }
    latest = SimpleNamespace(best_quote=lambda _instrument_id: None, as_of=NOW)
    common = {
        "latest": latest,
        "option_structure": {"call_wall": 7550.0},
        "macro_event": {},
        "greek_decision": {},
        "now": NOW,
        "policy": MarketFeatureSettings(),
    }

    assert _exit_decision(active, {"mid": 10.0, "underlier": 7547.0}, **common) == (
        "strategy_invalidation",
        "exit",
    )
    assert _exit_decision(active, {"mid": 10.0, "underlier": 7575.0}, **common) == (
        "underlier_target_reached",
        "take_profit",
    )
