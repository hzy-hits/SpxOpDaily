from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from spx_spark.application.market_features.models import (
    DecisionContext,
    FrameQuality,
    L1MicrostructureFrame,
    MinuteMarketFrame,
    OptionStructureFrame,
)
from spx_spark.application.market_features.trade_intent import evaluate_trade_intent
from spx_spark.application.market_features.trade_intent_runtime import process_trade_intent
from spx_spark.marketdata import InstrumentId, MarketDataQuality, Provider, Quote
from spx_spark.notifier.model import SinkResult
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.settings.order_map import OrderMapPolicy
from spx_spark.storage import LatestState


UTC = timezone.utc
NOW = datetime(2026, 7, 14, 15, 0, tzinfo=UTC)


def test_confirmed_path_requires_all_gates_before_trade_ready() -> None:
    market, options, latest, context, repricing = _ready_inputs()

    intent = evaluate_trade_intent(
        context,
        market,
        options,
        latest,
        repricing,
        now=NOW,
        feature_policy=MarketFeatureSettings(),
        order_policy=OrderMapPolicy(),
    )

    assert intent["status"] == "trade_ready"
    assert intent["contract_label"] == "SPXW 7550C"
    assert intent["decision_bid"] == 10.0
    assert intent["decision_ask"] == 10.4
    assert intent["entry_limit"] == pytest.approx(10.1)
    assert intent["invalidation_spx"] == 7547.0
    assert intent["target_spx"] == 7575.0
    assert intent["automatic_ordering"] is False


def test_pending_filter_and_opposing_regime_fail_closed() -> None:
    market, options, latest, context, repricing = _ready_inputs()
    context = DecisionContext(
        **{
            **context.__dict__,
            "regime_decision": {
                "mode": "trending",
                "direction": "down",
                "trend_score": 80.0,
            },
            "breakout_filter": {
                "event_id": "level:test",
                "verdict": "pending",
                "actionable": False,
            },
        }
    )

    intent = evaluate_trade_intent(
        context,
        market,
        options,
        latest,
        repricing,
        now=NOW,
        feature_policy=MarketFeatureSettings(),
        order_policy=OrderMapPolicy(),
    )

    assert intent["status"] == "blocked"
    assert "breakout_filter_not_supported" in intent["block_reasons"]
    assert "regime_direction_conflict" in intent["block_reasons"]


def test_stale_es_anchor_fails_closed() -> None:
    market, options, latest, context, repricing = _ready_inputs()
    market = MinuteMarketFrame(
        **{
            **market.__dict__,
            "es": {
                **market.es,
                "source_at": (NOW - timedelta(seconds=21)).isoformat(),
            },
        }
    )

    intent = evaluate_trade_intent(
        context,
        market,
        options,
        latest,
        repricing,
        now=NOW,
        feature_policy=MarketFeatureSettings(),
        order_policy=OrderMapPolicy(),
    )

    assert intent["status"] == "blocked"
    assert "es_anchor_source_stale" in intent["block_reasons"]


def test_future_repricing_timestamp_fails_closed() -> None:
    market, options, latest, context, repricing = _ready_inputs()
    repricing["as_of"] = (NOW + timedelta(seconds=6)).isoformat()

    intent = evaluate_trade_intent(
        context,
        market,
        options,
        latest,
        repricing,
        now=NOW,
        feature_policy=MarketFeatureSettings(),
        order_policy=OrderMapPolicy(),
    )

    assert intent["status"] == "blocked"
    assert "repricing_timestamp_in_future" in intent["block_reasons"]


def test_live_structure_drift_from_frozen_event_fails_closed() -> None:
    market, options, latest, context, repricing = _ready_inputs()
    options = OptionStructureFrame(
        **{
            **options.__dict__,
            "structure": {**options.structure, "call_wall": 7560.0},
        }
    )

    intent = evaluate_trade_intent(
        context,
        market,
        options,
        latest,
        repricing,
        now=NOW,
        feature_policy=MarketFeatureSettings(),
        order_policy=OrderMapPolicy(),
    )

    assert intent["status"] == "blocked"
    assert "trigger_structure_drift" in intent["block_reasons"]


def test_intent_identity_is_semantic_across_rearmed_event_ids() -> None:
    market, options, latest, context, repricing = _ready_inputs()
    first = evaluate_trade_intent(
        context,
        market,
        options,
        latest,
        repricing,
        now=NOW,
        feature_policy=MarketFeatureSettings(),
        order_policy=OrderMapPolicy(),
    )
    rearmed_level = {**context.level_decision, "event_id": "level:rearmed"}
    rearmed_context = DecisionContext(
        **{
            **context.__dict__,
            "level_decision": rearmed_level,
            "breakout_filter": {
                **context.breakout_filter,
                "event_id": "level:rearmed",
            },
        }
    )
    rearmed_repricing = {**repricing, "event_id": "level:rearmed"}
    second = evaluate_trade_intent(
        rearmed_context,
        market,
        options,
        latest,
        rearmed_repricing,
        now=NOW,
        feature_policy=MarketFeatureSettings(),
        order_policy=OrderMapPolicy(),
    )

    assert first["status"] == "trade_ready"
    assert second["status"] == "trade_ready"
    assert first["intent_id"] == second["intent_id"]
    assert first["semantic_key"] == second["semantic_key"]


def test_trade_ready_delivery_is_semantically_deduplicated(tmp_path, monkeypatch) -> None:
    intent = {
        "status": "trade_ready",
        "intent_id": "intent:test",
        "event_id": "level:test",
        "direction": "up",
        "thesis": "breakout",
        "contract_label": "SPXW 7550C",
        "decision_bid": 10.0,
        "decision_ask": 10.4,
        "entry_limit": 10.1,
        "provider": "ibkr",
        "quote_source_at": NOW.isoformat(),
        "spx_spot": 7554.0,
        "trigger_level": 7550.0,
        "follow_through_points": 4.0,
        "invalidation_spx": 7547.0,
        "target_spx": 7575.0,
        "max_loss_per_contract": 1010.0,
        "expires_at": (NOW + timedelta(seconds=90)).isoformat(),
        "time_stop_at": (NOW + timedelta(minutes=15)).isoformat(),
    }
    calls: list[str] = []
    monkeypatch.setattr(
        "spx_spark.application.market_features.trade_intent_runtime.generate_push_text",
        lambda template, *_args, **_kwargs: (template, "template"),
    )

    def fake_delivery(_settings, _envelope, **kwargs):
        calls.append(str(kwargs["text"]))
        return SimpleNamespace(
            sinks=(SinkResult(sink="feishu", attempted=True, ok=True),),
            delivered=True,
        )

    monkeypatch.setattr(
        "spx_spark.application.market_features.trade_intent_runtime.dispatch_notification",
        fake_delivery,
    )
    storage = SimpleNamespace(data_root=str(tmp_path))
    settings = _notification_settings(tmp_path)

    first = process_trade_intent(storage, intent, now=NOW, settings=settings)
    second = process_trade_intent(
        storage,
        {
            **intent,
            "event_id": "level:rearmed",
            "evaluated_at": (NOW + timedelta(minutes=1)).isoformat(),
        },
        now=NOW + timedelta(minutes=1),
        settings=settings,
    )

    assert first["delivered"] is True
    assert second["reason"] == "already_delivered"
    assert len(calls) == 1


def test_expired_trade_intent_is_not_delivered(tmp_path, monkeypatch) -> None:
    intent = {
        "status": "trade_ready",
        "intent_id": "intent:expired",
        "event_id": "level:expired",
        "expires_at": (NOW - timedelta(seconds=1)).isoformat(),
    }
    monkeypatch.setattr(
        "spx_spark.application.market_features.trade_intent_runtime.dispatch_notification",
        lambda *_args, **_kwargs: pytest.fail("expired intent must not be delivered"),
    )
    storage = SimpleNamespace(data_root=str(tmp_path))
    settings = _notification_settings(tmp_path)

    result = process_trade_intent(storage, intent, now=NOW, settings=settings)

    assert result == {
        "attempted": False,
        "delivered": False,
        "reason": "intent_expired",
    }


def test_disabled_notification_does_not_run_writer_or_hold_delivery_lease(
    tmp_path, monkeypatch
) -> None:
    intent = {
        "status": "trade_ready",
        "intent_id": "intent:disabled",
        "event_id": "level:disabled",
        "expires_at": (NOW + timedelta(seconds=90)).isoformat(),
    }
    monkeypatch.setattr(
        "spx_spark.application.market_features.trade_intent_runtime.generate_push_text",
        lambda *_args, **_kwargs: pytest.fail("disabled notification must not run writer"),
    )
    storage = SimpleNamespace(data_root=str(tmp_path))
    settings = SimpleNamespace(enabled=False)

    result = process_trade_intent(storage, intent, now=NOW, settings=settings)

    assert result["reason"] == "notification_disabled"
    state = json.loads(
        (tmp_path / "latest" / "trade_intent_delivery_state.json").read_text()
    )
    assert state["inflight"] == {}


def test_invalidation_explicitly_rearms_semantic_delivery(tmp_path, monkeypatch) -> None:
    intent = {
        "status": "trade_ready",
        "intent_id": "intent:semantic",
        "semantic_scope": "2026-07-14|level_breakout_call|7550.0000",
        "semantic_key": (
            "2026-07-14|level_breakout_call|7550.0000|"
            "option:SPX:SPXW:20260714:7550:C"
        ),
        "event_id": "level:first",
        "phase": "confirmed",
        "direction": "up",
        "thesis": "breakout",
        "contract_label": "SPXW 7550C",
        "decision_bid": 10.0,
        "decision_ask": 10.4,
        "entry_limit": 10.1,
        "provider": "ibkr",
        "quote_source_at": NOW.isoformat(),
        "spx_spot": 7554.0,
        "trigger_level": 7550.0,
        "follow_through_points": 4.0,
        "invalidation_spx": 7547.0,
        "target_spx": 7575.0,
        "max_loss_per_contract": 1010.0,
        "expires_at": (NOW + timedelta(minutes=5)).isoformat(),
        "time_stop_at": (NOW + timedelta(minutes=15)).isoformat(),
    }
    calls: list[str] = []
    monkeypatch.setattr(
        "spx_spark.application.market_features.trade_intent_runtime.generate_push_text",
        lambda template, *_args, **_kwargs: (template, "template"),
    )

    def fake_delivery(_settings, _envelope, **kwargs):
        calls.append(str(kwargs["text"]))
        return SimpleNamespace(
            sinks=(SinkResult(sink="feishu", attempted=True, ok=True),),
            delivered=True,
        )

    monkeypatch.setattr(
        "spx_spark.application.market_features.trade_intent_runtime.dispatch_notification",
        fake_delivery,
    )
    storage = SimpleNamespace(data_root=str(tmp_path))
    settings = _notification_settings(tmp_path)

    first = process_trade_intent(storage, intent, now=NOW, settings=settings)
    invalidated = process_trade_intent(
        storage,
        {
            "status": "observing",
            "phase": "invalidated",
            "event_id": "level:first",
            "semantic_scope": intent["semantic_scope"],
        },
        now=NOW + timedelta(minutes=1),
        settings=settings,
    )
    rearmed = process_trade_intent(
        storage,
        {**intent, "event_id": "level:second"},
        now=NOW + timedelta(minutes=2),
        settings=settings,
    )

    assert first["delivered"] is True
    assert invalidated["reason"] == "observing"
    assert rearmed["delivered"] is True
    assert len(calls) == 2


def _ready_inputs():
    instrument = InstrumentId.option(
        "SPX",
        expiry="20260714",
        strike=7550,
        right="C",
        trading_class="SPXW",
    )
    quote = Quote(
        instrument=instrument,
        provider=Provider.IBKR,
        received_at=NOW,
        last_update_at=NOW,
        quote_time=NOW,
        quality=MarketDataQuality.LIVE,
        bid=10.0,
        ask=10.4,
    )
    latest = LatestState(
        created_at=NOW,
        as_of=NOW,
        quotes=(quote,),
        best_quotes=(quote,),
    )
    market = MinuteMarketFrame(
        schema_version=1,
        frame_id="market:test",
        session_id="2026-07-14",
        as_of=NOW,
        quality=FrameQuality.READY,
        es={
            "return_1m_points": 1.0,
            "return_5m_points": 4.0,
            "return_15m_points": 8.0,
            "observed_at": NOW.isoformat(),
            "source_at": NOW.isoformat(),
            "transport_at": NOW.isoformat(),
        },
        session_ranges={},
        volume={"price_volume_alignment_5m": "price_volume_aligned"},
        cross_asset={"es_spy_direction_confirmation_15m": "confirmed"},
        volatility={},
        diagnostics={},
    )
    options = OptionStructureFrame(
        schema_version=1,
        frame_id="options:test",
        as_of=NOW,
        quality=FrameQuality.READY,
        front_expiry="20260714",
        next_expiry="20260715",
        structure={
            "call_wall": 7550.0,
            "put_wall": 7500.0,
            "flip_zone": [7525.0, 7530.0],
            "call_walls": [{"strike": 7575.0, "gex": 100.0}],
            "put_walls": [],
        },
        volatility={"expected_move_points_0dte": 40.0},
        concentration={},
        density={},
        l1=L1MicrostructureFrame(
            quality=FrameQuality.READY,
            expiry="20260714",
            contract_count=20,
            metrics={"liquidity_score": 90.0},
            diagnostics={},
        ),
        diagnostics={},
    )
    level = {
        "formal_signal_enabled": True,
        "formal_signal": True,
        "quality_ok": True,
        "event_id": "level:test",
        "phase": "confirmed",
        "phase_at": (NOW - timedelta(seconds=60)).isoformat(),
        "expires_at": (NOW + timedelta(minutes=3)).isoformat(),
        "updated_at": NOW.isoformat(),
        "expiry": "20260714",
        "thesis": "breakout",
        "direction": "up",
        "level_kind": "call_wall",
        "level": 7550.0,
        "spot": 7554.0,
    }
    context = DecisionContext(
        schema_version=1,
        context_id="decision:test",
        as_of=NOW,
        session_id="2026-07-14",
        market_frame_id=market.frame_id,
        option_frame_id=options.frame_id,
        trend={"regime": "bullish"},
        level_decision=level,
        confirmations={},
        invalidations=(),
        data_quality={"market": "ready", "options": "ready", "option_l1": "ready"},
        regime_decision={"mode": "trending", "direction": "up", "trend_score": 80.0},
        breakout_filter={
            "event_id": "level:test",
            "verdict": "supported",
            "actionable": True,
            "evidence": ["es_horizons_aligned_2"],
        },
    )
    repricing = {
        "event_id": "level:test",
        "as_of": NOW.isoformat(),
        "expiry": "20260714",
        "candidates": [
            {
                "play": "level_breakout_call",
                "contract_id": instrument.canonical_id,
                "strike": 7550,
                "right": "C",
                "execution_quote_status": "executable",
            }
        ],
    }
    return market, options, latest, context, repricing


def _notification_settings(tmp_path):
    return SimpleNamespace(
        enabled=True,
        feishu_enabled=True,
        bark_enabled=False,
        bark_friend_enabled=False,
        missed_queue_path=str(tmp_path / "missed.jsonl"),
    )
