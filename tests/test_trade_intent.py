from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

import spx_spark.application.market_features.trade_intent_runtime as trade_intent_runtime
from spx_spark.application.market_features.models import (
    DecisionContext,
    FrameQuality,
    L1MicrostructureFrame,
    MinuteMarketFrame,
    OptionStructureFrame,
)
from spx_spark.application.market_features.play_outcome_stats import PlayOutcomeStats
from spx_spark.application.market_features.service import _resolve_action_clock
from spx_spark.application.market_features.trade_intent import evaluate_trade_intent
from spx_spark.application.market_features.trade_intent_runtime import (
    _action_revalidation,
    _writer_output_valid,
    process_trade_intent,
    render_trade_intent,
)
from spx_spark.config import NotificationSettings
from spx_spark.marketdata import InstrumentId, MarketDataQuality, Provider, Quote
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
    assert intent["schema_version"] == 3
    assert str(intent["policy_version"]).startswith("rth_trade_intent.v3+sha256:")
    assert intent["valid_until"] == intent["expires_at"]
    assert intent["coordinate"]["kind"] == "official_spx"
    assert intent["block_reasons"] == []
    assert intent["contract_label"] == "SPXW 7550C"
    assert intent["decision_bid"] == 10.0
    assert intent["decision_ask"] == 10.4
    assert intent["entry_limit"] == pytest.approx(10.1)
    assert intent["invalidation_spx"] == 7547.0
    assert intent["target_spx"] == 7575.0
    assert intent["remaining_target_room_points"] == 21.0
    assert intent["remaining_reward_risk"] == 3.0
    assert intent["expires_at"] == (NOW + timedelta(seconds=20)).isoformat()
    assert intent["automatic_ordering"] is False


def test_trade_ready_includes_play_stats_when_provided() -> None:
    market, options, latest, context, repricing = _ready_inputs()
    stats = PlayOutcomeStats(
        play="level_breakout_call",
        level_kind="call_wall",
        sample_count=23,
        winrate=0.6087,
        avg_return=0.032,
        median_return=0.021,
        window_days=20,
        horizon="300",
        as_of=NOW.isoformat(),
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
        play_stats=stats,
    )

    assert intent["status"] == "trade_ready"
    assert intent["play_stats"] == {
        "play": "level_breakout_call",
        "level_kind": "call_wall",
        "window_days": 20,
        "horizon_seconds": 300,
        "sample_count": 23,
        "winrate": 0.6087,
        "avg_return_fraction": 0.032,
        "median_return_fraction": 0.021,
    }


def test_trade_ready_omits_play_stats_when_unavailable() -> None:
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
    assert "play_stats" not in intent


def test_render_trade_intent_shows_play_stats_section() -> None:
    intent = {
        **_render_intent(),
        "play_stats": {
            "play": "level_fade_put",
            "level_kind": "call_wall",
            "window_days": 20,
            "horizon_seconds": 300,
            "sample_count": 23,
            "winrate": 0.61,
            "avg_return_fraction": 0.032,
            "median_return_fraction": 0.021,
        },
    }

    text = render_trade_intent(intent)

    assert "## 同类信号" in text
    assert "近20日 level_fade_put@call_wall（300s口径）: n=23 胜率61% 均值+3.2%" in text
    assert text.index("## 风险") < text.index("## 同类信号") < text.index("## 时效")


def test_render_trade_intent_hides_play_stats_when_absent() -> None:
    text = render_trade_intent(_render_intent())

    assert "## 同类信号" not in text


def test_llm_writer_output_must_preserve_play_stats() -> None:
    intent = {
        **_render_intent(),
        "play_stats": {
            "play": "level_fade_put",
            "level_kind": "call_wall",
            "window_days": 20,
            "horizon_seconds": 300,
            "sample_count": 23,
            "winrate": 0.61,
            "avg_return_fraction": 0.032,
        },
    }
    template = render_trade_intent(intent)
    without_stats = "\n".join(
        line
        for line in template.splitlines()
        if "同类信号" not in line
        and "level_fade_put@call_wall" not in line
    )

    assert _writer_output_valid(template, intent)
    assert not _writer_output_valid(without_stats, intent)


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


def test_confirmed_session_recovery_blocks_opposite_single_level_signal() -> None:
    market, options, latest, context, repricing = _ready_inputs()
    context = DecisionContext(
        **{
            **context.__dict__,
            "session_episode": {
                "phase": "recovery",
                "reversal_direction": "down",
                "break_level": 7560.0,
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
    assert "session_episode_direction_conflict" in intent["block_reasons"]


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


def test_remaining_target_room_and_reward_risk_fail_closed() -> None:
    market, options, latest, context, repricing = _ready_inputs()
    context = DecisionContext(
        **{
            **context.__dict__,
            "level_decision": {**context.level_decision, "spot": 7574.0},
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
    assert intent["remaining_target_room_points"] == 1.0
    assert intent["remaining_reward_risk"] == pytest.approx(1.0 / 27.0)
    assert "remaining_target_room_insufficient" in intent["block_reasons"]
    assert "remaining_reward_risk_insufficient" in intent["block_reasons"]


def test_default_reward_risk_floor_blocks_sub_one_ratio() -> None:
    market, options, latest, context, repricing = _ready_inputs()
    context = replace(
        context,
        level_decision={**context.level_decision, "spot": 7563.0},
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

    assert intent["remaining_target_room_points"] == 12.0
    assert intent["remaining_reward_risk"] == pytest.approx(0.75)
    assert intent["status"] == "blocked"
    assert "remaining_reward_risk_insufficient" in intent["block_reasons"]


def test_rth_intent_policy_blocks_premarket_trade_ready() -> None:
    market, options, latest, context, repricing = _ready_inputs()
    premarket = datetime(2026, 7, 14, 9, 22, tzinfo=UTC)
    quote = replace(
        latest.best_quotes[0],
        received_at=premarket,
        last_update_at=premarket,
        quote_time=premarket,
    )
    latest = replace(
        latest,
        created_at=premarket,
        as_of=premarket,
        quotes=(quote,),
        best_quotes=(quote,),
    )
    market = replace(
        market,
        as_of=premarket,
        es={
            **market.es,
            "observed_at": premarket.isoformat(),
            "source_at": premarket.isoformat(),
            "transport_at": premarket.isoformat(),
        },
    )
    options = replace(options, as_of=premarket)
    level = {
        **context.level_decision,
        "phase_at": (premarket - timedelta(seconds=60)).isoformat(),
        "expires_at": (premarket + timedelta(minutes=3)).isoformat(),
        "updated_at": premarket.isoformat(),
        "trigger_coordinate": {
            **context.level_decision["trigger_coordinate"],
            "as_of": premarket.isoformat(),
        },
    }
    context = replace(context, as_of=premarket, level_decision=level)
    repricing = {**repricing, "as_of": premarket.isoformat()}

    intent = evaluate_trade_intent(
        context,
        market,
        options,
        latest,
        repricing,
        now=premarket,
        feature_policy=MarketFeatureSettings(),
        order_policy=OrderMapPolicy(),
    )

    assert intent["status"] == "blocked"
    assert intent["block_reasons"] == [
        "rth_session_required",
        "rth_confirmation_required",
    ]


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
        **_runtime_contract(NOW + timedelta(seconds=90)),
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
        "spx_spark.application.market_features.trade_intent_runtime._action_now",
        lambda: NOW,
    )
    monkeypatch.setattr(
        "spx_spark.application.market_features.trade_intent_runtime._action_revalidation",
        lambda *_args, **_kwargs: (None, {"quote_revalidation": "test_stub"}),
    )

    def fake_enqueue(_settings, _envelope, **kwargs):
        calls.append(str(kwargs["text"]))
        return SimpleNamespace(
            accepted=True,
            inserted=True,
            duplicate=False,
            delivered=False,
            queued_for_recovery=True,
            outcome="pending",
            targets=("feishu",),
        )

    monkeypatch.setattr(
        "spx_spark.application.market_features.trade_intent_runtime.enqueue_notification",
        fake_enqueue,
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

    assert first["accepted"] is True
    assert first["delivered"] is False
    assert second["reason"] == "already_accepted"
    assert len(calls) == 1


def test_enqueue_ack_crash_replays_immutable_trade_ready_payload(
    tmp_path,
    monkeypatch,
) -> None:
    intent = {
        **_runtime_contract(NOW + timedelta(minutes=5)),
        "status": "trade_ready",
        "intent_id": "intent:crash-replay",
        "event_id": "level:crash-replay",
        "evaluated_at": NOW.isoformat(),
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
    settings = replace(
        NotificationSettings.from_env(),
        enabled=True,
        feishu_enabled=True,
        feishu_webhook_url="https://open.feishu.cn/test",
        bark_enabled=False,
        bark_friend_enabled=False,
        missed_queue_path=str(tmp_path / "missed.jsonl"),
        delivery_receipt_path=str(tmp_path / "receipts.sqlite"),
        delivery_outbox_enabled=True,
        delivery_outbox_path=str(tmp_path / "delivery-outbox.sqlite"),
        delivery_outbox_legacy_shadow_enabled=True,
    )
    action_times = iter((NOW + timedelta(seconds=1), NOW + timedelta(seconds=122)))
    action_quotes = iter(
        (
            {
                "quote_revalidation": "performed",
                "bid": 10.0,
                "mid": 10.2,
                "ask": 10.4,
                "recomputed_entry_limit": 10.1,
            },
            {
                "quote_revalidation": "performed",
                "bid": 10.05,
                "mid": 10.2,
                "ask": 10.35,
                "recomputed_entry_limit": 10.1,
            },
        )
    )
    monkeypatch.setattr(trade_intent_runtime, "_action_now", lambda: next(action_times))
    monkeypatch.setattr(
        trade_intent_runtime,
        "_action_revalidation",
        lambda *_args, **_kwargs: (None, next(action_quotes)),
    )

    original_write = trade_intent_runtime.atomic_write_json_secure
    state_writes = 0

    def crash_after_first_enqueue(path, payload):
        nonlocal state_writes
        if path.name == "trade_intent_delivery_state.json":
            state_writes += 1
            if state_writes == 2:
                raise RuntimeError("simulated crash after durable enqueue")
        return original_write(path, payload)

    monkeypatch.setattr(
        trade_intent_runtime,
        "atomic_write_json_secure",
        crash_after_first_enqueue,
    )
    storage = SimpleNamespace(data_root=str(tmp_path))

    with pytest.raises(RuntimeError, match="simulated crash"):
        process_trade_intent(
            storage,
            intent,
            now=NOW,
            settings=settings,
            feature_policy=MarketFeatureSettings(),
        )

    replay = process_trade_intent(
        storage,
        intent,
        now=NOW + timedelta(seconds=121),
        settings=settings,
        feature_policy=MarketFeatureSettings(),
    )

    assert replay["accepted"] is True
    assert replay["duplicate"] is True
    assert replay["inserted"] is False
    state = json.loads((tmp_path / "latest" / "trade_intent_delivery_state.json").read_text())
    assert "delivered" not in state
    assert "intent:crash-replay" in state["accepted"]
    with sqlite3.connect(settings.delivery_outbox_path) as connection:
        rows = connection.execute(
            "SELECT event_id, occurred_at, text FROM notification_delivery_events"
        ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "intent:crash-replay"
    assert rows[0][1] == NOW.isoformat(timespec="microseconds")
    assert "10 / 10.4" in rows[0][2]
    assert "10.05" not in rows[0][2]


def test_legacy_delivered_state_migrates_to_accepted(tmp_path) -> None:
    state_path = tmp_path / "latest" / "trade_intent_delivery_state.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "delivered": {"intent:legacy": NOW.isoformat()},
                "semantic_keys": {"intent:legacy": "legacy-key"},
            }
        ),
        encoding="utf-8",
    )

    process_trade_intent(
        SimpleNamespace(data_root=str(tmp_path)),
        {"status": "observing", "event_id": "level:observing"},
        now=NOW,
        settings=SimpleNamespace(enabled=False),
    )

    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["schema_version"] == 2
    assert state["accepted"] == {"intent:legacy": NOW.isoformat()}
    assert "delivered" not in state


def test_expired_trade_intent_is_not_delivered(tmp_path, monkeypatch) -> None:
    intent = {
        **_runtime_contract(NOW - timedelta(seconds=1)),
        "status": "trade_ready",
        "intent_id": "intent:expired",
        "event_id": "level:expired",
        "expires_at": (NOW - timedelta(seconds=1)).isoformat(),
    }
    monkeypatch.setattr(
        "spx_spark.application.market_features.trade_intent_runtime.enqueue_notification",
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


def test_action_revalidation_reloads_quote_and_recomputes_limit(monkeypatch) -> None:
    market, options, latest, context, repricing = _ready_inputs()
    policy = MarketFeatureSettings()
    intent = evaluate_trade_intent(
        context,
        market,
        options,
        latest,
        repricing,
        now=NOW,
        feature_policy=policy,
        order_policy=OrderMapPolicy(),
    )
    changed_quote = replace(
        latest.best_quotes[0],
        received_at=NOW + timedelta(seconds=1),
        last_update_at=NOW + timedelta(seconds=1),
        quote_time=NOW + timedelta(seconds=1),
        bid=10.2,
        ask=10.6,
    )
    changed_latest = LatestState(
        created_at=NOW + timedelta(seconds=1),
        as_of=NOW + timedelta(seconds=1),
        quotes=(changed_quote,),
        best_quotes=(changed_quote,),
    )

    class Store:
        def __init__(self, _storage) -> None:
            pass

        def load(self, *, now):
            assert now == NOW + timedelta(seconds=1)
            return changed_latest

    monkeypatch.setattr(
        "spx_spark.application.market_features.trade_intent_runtime.LatestStateStore",
        Store,
    )
    reason, evidence = _action_revalidation(
        SimpleNamespace(),
        intent,
        now=NOW + timedelta(seconds=1),
        feature_policy=policy,
        expected_policy_version=str(intent["policy_version"]),
    )

    assert reason == "action_entry_limit_changed"
    assert evidence["entry_limit"] == pytest.approx(10.1)
    assert evidence["recomputed_entry_limit"] == pytest.approx(10.3)
    assert evidence["quote_state_created_at"] == changed_latest.created_at.isoformat()


def test_ready_action_revalidation_requires_feature_policy() -> None:
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

    reason, evidence = _action_revalidation(
        SimpleNamespace(),
        intent,
        now=NOW,
        feature_policy=None,
        expected_policy_version=str(intent["policy_version"]),
    )

    assert reason == "action_feature_policy_unavailable"
    assert evidence["quote_revalidation"] == "blocked"


def test_ready_action_revalidation_requires_declared_provider(monkeypatch) -> None:
    market, options, latest, context, repricing = _ready_inputs()
    policy = MarketFeatureSettings()
    intent = evaluate_trade_intent(
        context,
        market,
        options,
        latest,
        repricing,
        now=NOW,
        feature_policy=policy,
        order_policy=OrderMapPolicy(),
    )

    class Store:
        def __init__(self, _storage) -> None:
            pass

        def load(self, *, now):
            assert now == NOW
            return latest

    monkeypatch.setattr(
        "spx_spark.application.market_features.trade_intent_runtime.LatestStateStore",
        Store,
    )
    reason, _evidence = _action_revalidation(
        SimpleNamespace(),
        {**intent, "provider": None},
        now=NOW,
        feature_policy=policy,
        expected_policy_version=str(intent["policy_version"]),
    )

    assert reason == "action_quote_provider_unavailable"


def test_action_clock_is_deterministic_for_injected_run_time() -> None:
    replay_clock = _resolve_action_clock(
        NOW,
        evaluation_time_injected=True,
        action_clock=None,
    )
    custom_now = NOW + timedelta(seconds=3)
    custom_clock = _resolve_action_clock(
        NOW,
        evaluation_time_injected=True,
        action_clock=lambda: custom_now,
    )

    assert replay_clock() == NOW
    assert custom_clock() == custom_now


def test_stale_action_quote_is_blocked_before_enqueue(tmp_path, monkeypatch) -> None:
    market, options, latest, context, repricing = _ready_inputs()
    policy = MarketFeatureSettings()
    intent = evaluate_trade_intent(
        context,
        market,
        options,
        latest,
        repricing,
        now=NOW,
        feature_policy=policy,
        order_policy=OrderMapPolicy(),
    )
    action_now = NOW + timedelta(seconds=6)

    class Store:
        def __init__(self, _storage) -> None:
            pass

        def load(self, *, now):
            assert now == action_now
            return latest

    monkeypatch.setattr(
        "spx_spark.application.market_features.trade_intent_runtime.LatestStateStore",
        Store,
    )
    monkeypatch.setattr(
        "spx_spark.application.market_features.trade_intent_runtime._action_now",
        lambda: action_now,
    )
    monkeypatch.setattr(
        "spx_spark.application.market_features.trade_intent_runtime.enqueue_notification",
        lambda *_args, **_kwargs: pytest.fail("stale action must not enqueue"),
    )

    result = process_trade_intent(
        SimpleNamespace(data_root=str(tmp_path)),
        intent,
        now=NOW,
        settings=_notification_settings(tmp_path),
        feature_policy=policy,
        expected_policy_version=str(intent["policy_version"]),
    )

    assert result["reason"] == "action_quote_source_stale"
    assert result["action_revalidated_at"] == action_now.isoformat()
    state = json.loads((tmp_path / "latest" / "trade_intent_delivery_state.json").read_text())
    assert state["last_action_revalidation"]["source_age_seconds"] == 6.0
    assert state["inflight"] == {}


def test_disabled_notification_does_not_run_writer_or_hold_delivery_lease(
    tmp_path, monkeypatch
) -> None:
    intent = {
        **_runtime_contract(NOW + timedelta(seconds=90)),
        "status": "trade_ready",
        "intent_id": "intent:disabled",
        "event_id": "level:disabled",
        "expires_at": (NOW + timedelta(seconds=90)).isoformat(),
    }
    monkeypatch.setattr(
        "spx_spark.application.market_features.trade_intent_runtime.enqueue_notification",
        lambda *_args, **_kwargs: pytest.fail("disabled notification must not enqueue"),
    )
    storage = SimpleNamespace(data_root=str(tmp_path))
    settings = SimpleNamespace(enabled=False)

    result = process_trade_intent(storage, intent, now=NOW, settings=settings)

    assert result["reason"] == "notification_disabled"
    state = json.loads((tmp_path / "latest" / "trade_intent_delivery_state.json").read_text())
    assert state["inflight"] == {}


def test_invalidation_explicitly_rearms_semantic_delivery(tmp_path, monkeypatch) -> None:
    intent = {
        **_runtime_contract(NOW + timedelta(minutes=5)),
        "status": "trade_ready",
        "intent_id": "intent:semantic",
        "semantic_scope": "2026-07-14|level_breakout_call|7550.0000",
        "semantic_key": (
            "2026-07-14|level_breakout_call|7550.0000|option:SPX:SPXW:20260714:7550:C"
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
        "spx_spark.application.market_features.trade_intent_runtime._action_now",
        lambda: NOW,
    )
    monkeypatch.setattr(
        "spx_spark.application.market_features.trade_intent_runtime._action_revalidation",
        lambda *_args, **_kwargs: (None, {"quote_revalidation": "test_stub"}),
    )

    def fake_enqueue(_settings, _envelope, **kwargs):
        calls.append(str(kwargs["text"]))
        return SimpleNamespace(
            accepted=True,
            inserted=True,
            duplicate=False,
            delivered=False,
            queued_for_recovery=True,
            outcome="pending",
            targets=("feishu",),
        )

    monkeypatch.setattr(
        "spx_spark.application.market_features.trade_intent_runtime.enqueue_notification",
        fake_enqueue,
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

    assert first["accepted"] is True
    assert invalidated["reason"] == "observing"
    assert rearmed["accepted"] is True
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
        "trigger_coordinate": {
            "kind": "official_spx",
            "instrument_id": "index:SPX",
            "observed_value": 7554.0,
            "target_value": 7550.0,
            "spx_observed_value": 7554.0,
            "basis_points": 0.0,
            "as_of": NOW.isoformat(),
        },
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


def _render_intent() -> dict:
    return {
        "status": "trade_ready",
        "intent_id": "intent:render",
        "event_id": "level:render",
        "direction": "down",
        "thesis": "fade",
        "contract_label": "SPXW 7550P",
        "decision_bid": 10.0,
        "decision_ask": 10.4,
        "entry_limit": 10.1,
        "provider": "ibkr",
        "quote_source_at": NOW.isoformat(),
        "spx_spot": 7554.0,
        "trigger_level": 7550.0,
        "follow_through_points": 4.0,
        "invalidation_spx": 7553.0,
        "target_spx": 7525.0,
        "remaining_target_room_points": 29.0,
        "remaining_reward_risk": 3.0,
        "max_loss_per_contract": 1010.0,
        "expires_at": (NOW + timedelta(seconds=90)).isoformat(),
        "time_stop_at": (NOW + timedelta(minutes=15)).isoformat(),
    }


def _runtime_contract(valid_until: datetime) -> dict[str, object]:
    return {
        "schema_version": 3,
        "policy_version": "rth_trade_intent.v3+sha256:test",
        "valid_until": valid_until.isoformat(),
        "coordinate": {
            "kind": "official_spx",
            "instrument_id": "index:SPX",
            "observed_value": 7554.0,
            "target_value": 7550.0,
            "spx_observed_value": 7554.0,
            "basis_points": 0.0,
            "as_of": NOW.isoformat(),
        },
        "block_reasons": [],
    }


def _notification_settings(tmp_path):
    return SimpleNamespace(
        enabled=True,
        feishu_enabled=True,
        bark_enabled=False,
        bark_friend_enabled=False,
        missed_queue_path=str(tmp_path / "missed.jsonl"),
    )
