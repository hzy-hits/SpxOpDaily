from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from spx_spark.data_platform.research.odte_level_opportunities import (
    LATENCY_SENSITIVITY_SECONDS,
    OpportunityCostModel,
    _replay_payload,
    build_opportunity_artifacts,
)
from spx_spark.data_platform.research.odte_level_signals import (
    SET_TRADE_READY,
    Signal,
    Skip,
    Trade,
)

T0 = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc)


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def _signal() -> Signal:
    return Signal(
        set_name=SET_TRADE_READY,
        key="intent:alpha",
        at=T0,
        direction="up",
        level=7550.0,
        strike=7550.0,
        expiry=date(2026, 7, 15),
        entry_at=T0,
        thesis="level_breakout_call",
        entry_limit=10.0,
        entry_expires_at=T0 + timedelta(seconds=20),
        entry_provider="schwab",
        contract_id="option:SPX:SPXW:20260715:7550:C",
    )


def _trade(**overrides: object) -> Trade:
    base: dict[str, object] = {
        "set_name": SET_TRADE_READY,
        "profile": "baseline",
        "key": "intent:alpha",
        "at": T0.isoformat(),
        "play": "level_breakout_call",
        "direction": "up",
        "level": 7550.0,
        "level_kind": "flip_high",
        "contract_id": "option:SPX:SPXW:20260715:7550:C",
        "short_contract_id": None,
        "variant": "naked",
        "entry_time": T0.isoformat(),
        "entry_px": 10.0,
        "exit_time": (T0 + timedelta(minutes=5)).isoformat(),
        "exit_px": 11.0,
        "exit_reason": "time_stop",
        "pnl_points": 1.0,
        "pnl_usd": 100.0,
        "mfe_points": 1.2,
        "mae_points": -0.2,
        "underlier_source": "index:SPX",
        "trend_regime": None,
        "session_bucket": "rth",
        "ft_pass_15s2p": None,
        "entry_price_source": "lake_ask",
        "h60_ret": None,
        "h300_ret": None,
        "h900_ret": None,
        "long_provider": "schwab",
        "short_provider": None,
        "entry_latency_seconds": 0,
        "executable_sides": (10.0, None, 11.0, None),
    }
    return Trade(**{**base, **overrides})  # type: ignore[arg-type]


def test_opportunity_joins_occurrences_delivery_and_one_virtual_episode(
    tmp_path: Path,
) -> None:
    features = tmp_path / "features"
    occurrences = [
        {
            "status": "trade_ready",
            "intent_id": "intent:alpha",
            "event_id": f"evaluation:{index}",
            "evaluated_at": (T0 + timedelta(seconds=index)).isoformat(),
            "entry_limit": 10.0,
        }
        for index in range(3)
    ]
    _write_jsonl(features / "trade_intents/date=2026-07-15/events.jsonl", occurrences)
    deliveries = [
        {
            "record_type": "trade_ready_delivery_expectation",
            "intent_id": "intent:alpha",
            "intent_event_id": f"evaluation:{index}",
            "delivery_event_id": f"delivery:{index}",
            "observed_at": (T0 + timedelta(seconds=5 + index)).isoformat(),
        }
        for index in range(3)
    ]
    _write_jsonl(
        features / "trade_intent_producer_ledger/date=2026-07-15/events.jsonl",
        deliveries,
    )
    _write_jsonl(
        features / "virtual_strategy/date=2026-07-15/events.jsonl",
        [
            {
                "source_signal_id": "intent:alpha",
                "episode_id": "episode:1",
                "event": "virtual_opened",
                "opened_at": (T0 + timedelta(seconds=6)).isoformat(),
                "status": "open",
            },
            {
                "source_signal_id": "intent:alpha",
                "episode_id": "episode:1",
                "event": "virtual_closed",
                "closed_at": (T0 + timedelta(minutes=5)).isoformat(),
                "status": "closed",
                "exit_reason": "time_stop",
            },
        ],
    )
    baseline = _trade()
    replay = {
        (SET_TRADE_READY, "intent:alpha", seconds): (
            baseline
            if seconds in (0, 5)
            else Skip(SET_TRADE_READY, "baseline", "intent:alpha", "naked", "limit_not_reached")
        )
        for seconds in LATENCY_SENSITIVITY_SECONDS
    }

    artifacts = build_opportunity_artifacts(
        features,
        [_signal()],
        replay,
        cutoff_at=T0 + timedelta(days=1),
    )

    assert len(artifacts) == 1
    artifact = artifacts[0]
    assert artifact["opportunity_id"] == "intent:alpha"
    assert artifact["occurrence_count"] == 3
    assert [row["event_id"] for row in artifact["occurrences"]] == [
        "evaluation:0",
        "evaluation:1",
        "evaluation:2",
    ]
    assert artifact["delivery_event_count"] == 3
    assert [row["delivery_event_id"] for row in artifact["delivery_events"]] == [
        "delivery:0",
        "delivery:1",
        "delivery:2",
    ]
    assert artifact["virtual_episode_count"] == 1
    assert [row["event"] for row in artifact["virtual_episodes"][0]["events"]] == [
        "virtual_opened",
        "virtual_closed",
    ]
    assert artifact["baseline_status"] == "quote_reached"
    latency_rows = artifact["latency_sensitivity"]
    assert [row["latency_seconds"] for row in latency_rows] == [0, 5, 10, 20, 30]
    assert [row["status"] for row in latency_rows] == [
        "quote_reached",
        "quote_reached",
        "not_reached",
        "not_reached",
        "not_reached",
    ]
    assert "filled" not in json.dumps(artifact)


def test_costed_quote_payload_uses_single_and_vertical_executable_sides() -> None:
    model = OpportunityCostModel(commission_per_contract_side_usd=1.25)
    assert model.to_payload()["slippage_unit"] == (
        "SPX_option_premium_points_per_contract_leg_side"
    )
    assert model.to_payload()["slippage_per_leg_side_points"] == [0.0, 0.05, 0.1, 0.2]
    single = _replay_payload(_trade(), latency_seconds=0, cost_model=model)
    assert single["status"] == "quote_reached"
    assert single["entry"] == {
        "observed_at": T0.isoformat(),
        "natural_ask": 10.0,
        "long_ask": 10.0,
        "short_bid": None,
        "price_source": "lake_ask",
    }
    assert single["exit"]["long_bid"] == 11.0
    single_cost = single["cost"]
    assert single_cost["contract_legs"] == 1
    assert single_cost["charged_contract_sides"] == 2
    assert single_cost["commission_usd"] == 2.5
    assert single_cost["commission_points"] == 0.025
    assert single_cost["reference_slippage_per_leg_side_points"] == 0.05
    assert single_cost["total_slippage_points"] == 0.1
    assert single_cost["net_points"] == 0.875
    assert single_cost["net_pnl_usd"] == 87.5
    assert single_cost["net_return_fraction"] == 0.0875
    assert single_cost["slippage_sensitivity"] == [
        {
            "per_leg_side_slippage_points": 0.0,
            "charged_contract_sides": 2,
            "total_slippage_points": 0.0,
            "total_slippage_usd": 0.0,
            "net_points": 0.975,
            "net_pnl_usd": 97.5,
            "net_return_fraction": 0.0975,
        },
        {
            "per_leg_side_slippage_points": 0.05,
            "charged_contract_sides": 2,
            "total_slippage_points": 0.1,
            "total_slippage_usd": 10.0,
            "net_points": 0.875,
            "net_pnl_usd": 87.5,
            "net_return_fraction": 0.0875,
        },
        {
            "per_leg_side_slippage_points": 0.1,
            "charged_contract_sides": 2,
            "total_slippage_points": 0.2,
            "total_slippage_usd": 20.0,
            "net_points": 0.775,
            "net_pnl_usd": 77.5,
            "net_return_fraction": 0.0775,
        },
        {
            "per_leg_side_slippage_points": 0.2,
            "charged_contract_sides": 2,
            "total_slippage_points": 0.4,
            "total_slippage_usd": 40.0,
            "net_points": 0.575,
            "net_pnl_usd": 57.5,
            "net_return_fraction": 0.0575,
        },
    ]

    vertical = _replay_payload(
        _trade(
            short_contract_id="option:SPX:SPXW:20260715:7560:C",
            variant="spread10",
            entry_px=5.0,
            exit_px=7.0,
            pnl_points=2.0,
            pnl_usd=200.0,
            short_provider="schwab",
            executable_sides=(10.0, 5.0, 9.0, 2.0),
        ),
        latency_seconds=20,
        cost_model=model,
    )
    assert vertical["entry"]["natural_ask"] == 5.0
    assert vertical["entry"]["long_ask"] == 10.0
    assert vertical["entry"]["short_bid"] == 5.0
    assert vertical["exit"]["natural_bid"] == 7.0
    assert vertical["exit"]["long_bid"] == 9.0
    assert vertical["exit"]["short_ask"] == 2.0
    vertical_cost = vertical["cost"]
    assert vertical_cost["commission_usd"] == 5.0
    assert vertical_cost["commission_points"] == 0.05
    assert vertical_cost["contract_legs"] == 2
    assert vertical_cost["charged_contract_sides"] == 4
    assert vertical_cost["total_slippage_points"] == 0.2
    assert vertical_cost["net_points"] == 1.75
    assert vertical_cost["net_pnl_usd"] == 175.0
    assert vertical_cost["net_return_fraction"] == 0.35
    assert vertical_cost["slippage_sensitivity"] == [
        {
            "per_leg_side_slippage_points": 0.0,
            "charged_contract_sides": 4,
            "total_slippage_points": 0.0,
            "total_slippage_usd": 0.0,
            "net_points": 1.95,
            "net_pnl_usd": 195.0,
            "net_return_fraction": 0.39,
        },
        {
            "per_leg_side_slippage_points": 0.05,
            "charged_contract_sides": 4,
            "total_slippage_points": 0.2,
            "total_slippage_usd": 20.0,
            "net_points": 1.75,
            "net_pnl_usd": 175.0,
            "net_return_fraction": 0.35,
        },
        {
            "per_leg_side_slippage_points": 0.1,
            "charged_contract_sides": 4,
            "total_slippage_points": 0.4,
            "total_slippage_usd": 40.0,
            "net_points": 1.55,
            "net_pnl_usd": 155.0,
            "net_return_fraction": 0.31,
        },
        {
            "per_leg_side_slippage_points": 0.2,
            "charged_contract_sides": 4,
            "total_slippage_points": 0.8,
            "total_slippage_usd": 80.0,
            "net_points": 1.15,
            "net_pnl_usd": 115.0,
            "net_return_fraction": 0.23,
        },
    ]
