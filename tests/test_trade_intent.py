from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

import spx_spark.application.market_features.trade_intent_delivery_contract as trade_intent_delivery_contract
import spx_spark.application.market_features.trade_intent_runtime as trade_intent_runtime
from spx_spark.application.market_features.models import (
    DecisionContext,
    FrameQuality,
    L1MicrostructureFrame,
    MinuteMarketFrame,
    OptionStructureFrame,
)
from spx_spark.application.market_features.play_outcome_stats import PlayOutcomeStats
from spx_spark.application.market_features.service import (
    _apply_provider_entry_control,
    _resolve_action_clock,
)
from spx_spark.application.market_features.trade_intent import (
    TRADE_INTENT_CONTRACT_VERSION,
    evaluate_trade_intent,
    live_trade_intent_authority_issues,
    trade_intent_policy_version,
)
from spx_spark.application.market_features.trade_intent_runtime import (
    _action_revalidation,
    _ready_contract_reason,
    _trade_ready_delivery_event_id,
    _writer_output_valid,
    process_trade_intent,
    render_trade_intent,
)
from spx_spark.config import NotificationSettings
from spx_spark.marketdata import InstrumentId, MarketDataQuality, Provider, Quote
from spx_spark.notifier.dispatcher import consume_pending_notifications
from spx_spark.notifier.model import SinkResult
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.settings.order_map import OrderMapPolicy
from spx_spark.storage import LatestState
from spx_spark.strategy_contract import policy_version


UTC = timezone.utc
NOW = datetime(2026, 7, 14, 15, 0, tzinfo=UTC)


def test_trade_intent_policy_hash_is_stable_and_resets_for_lane_clock_contract() -> None:
    feature_policy = MarketFeatureSettings(trade_confirmed_pilot_enabled=True)
    order_policy = OrderMapPolicy()

    current = trade_intent_policy_version(feature_policy, order_policy)
    repeated = trade_intent_policy_version(feature_policy, order_policy)
    legacy = policy_version(
        "rth_trade_intent.v3",
        {
            "market_features": feature_policy,
            "order_map": order_policy,
        },
    )

    assert TRADE_INTENT_CONTRACT_VERSION == "rth_manual_levels_0930_1530.v6"
    assert current == repeated
    assert current.startswith("rth_trade_intent.v3+sha256:")
    assert current != legacy


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
    assert intent["opportunity_id"] == intent["intent_id"]
    assert intent["reentry_generation"] == 0
    assert intent["schema_version"] == 3
    assert str(intent["policy_version"]).startswith("rth_trade_intent.v3+sha256:")
    assert intent["valid_until"] == intent["expires_at"]
    assert intent["coordinate"]["kind"] == "official_spx"
    assert intent["block_reasons"] == []
    assert intent["contract_label"] == "SPXW 7550C"
    assert intent["decision_bid"] == 10.0
    assert intent["decision_ask"] == 10.4
    assert intent["entry_limit"] == pytest.approx(10.4)
    assert intent["entry_rule"] == "bid_plus_spread_fraction_to_ask"
    assert intent["entry_spread_fraction"] == 1.0
    assert intent["invalidation_spx"] == 7547.0
    assert intent["target_spx"] == 7575.0
    assert intent["remaining_target_room_points"] == 21.0
    assert intent["remaining_reward_risk"] == 3.0
    assert intent["expires_at"] == (NOW + timedelta(seconds=20)).isoformat()
    assert intent["automatic_ordering"] is False
    assert intent["strategy_id"] == "rth_level_manual"
    assert intent["lifecycle_status"] == "legacy_production"
    assert intent["runtime_status"] == "production_runtime"
    assert intent["wall_signal"] == "present"
    assert intent["execution_eligible"] is True
    assert intent["quote_observation_eligible"] is False
    assert intent["priority"] == "high"
    assert intent["shadow_mode"] is False


def test_july_30_manual_limit_uses_natural_ask_instead_of_passive_mid() -> None:
    market, options, latest, context, repricing = _ready_inputs()
    quote = replace(latest.best_quotes[0], bid=21.1, ask=21.2)
    latest = replace(latest, quotes=(quote,), best_quotes=(quote,))

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
    assert intent["decision_bid"] == 21.1
    assert intent["decision_ask"] == 21.2
    assert intent["entry_limit"] == pytest.approx(21.2)
    assert intent["max_loss_per_contract"] == 2120.0


def test_qualified_rth_es_coordinate_is_valid_manual_delivery_authority() -> None:
    market, options, latest, context, repricing = _ready_inputs()
    level = {
        **context.level_decision,
        "trigger_coordinate": {
            "kind": "es_equivalent",
            "instrument_id": "future:ES",
            "observed_value": 7581.955,
            "target_value": 7577.955,
            "spx_level": 7550.0,
            "spx_observed_value": 7554.0,
            "basis_points": 27.955,
            "as_of": NOW.isoformat(),
        },
    }
    context = replace(context, level_decision=level)
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

    assert intent["status"] == "trade_ready"
    assert intent["coordinate"]["kind"] == "es_equivalent"
    assert (
        _ready_contract_reason(
            intent,
            now=NOW,
            expected_policy_version=str(intent["policy_version"]),
        )
        is None
    )


def test_incoherent_rth_es_coordinate_is_rejected_before_delivery() -> None:
    intent = {
        **_runtime_contract(NOW + timedelta(seconds=20)),
        "spx_spot": 7381.92,
        "trigger_level": 7390.0,
        "coordinate": {
            "kind": "es_equivalent",
            "instrument_id": "future:ES",
            "observed_value": 7409.875,
            "target_value": 7417.955,
            "spx_level": 7390.0,
            "basis_points": 10.0,
        },
    }

    assert _ready_contract_reason(intent, now=NOW) == "source_es_coordinate_spot_incoherent"


def test_chain_implied_coordinate_is_not_rth_manual_delivery_authority() -> None:
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
    intent = {
        **intent,
        "coordinate": {
            "kind": "chain_implied_spx",
            "instrument_id": "synthetic:SPXW_PARITY",
            "observed_value": intent["spx_spot"],
            "target_value": intent["trigger_level"],
            "spx_level": intent["trigger_level"],
            "spx_observed_value": intent["spx_spot"],
            "basis_points": None,
            "as_of": NOW.isoformat(),
        },
    }

    assert (
        _ready_contract_reason(
            intent,
            now=NOW,
            expected_policy_version=str(intent["policy_version"]),
        )
        == "source_coordinate_mismatch"
    )


def test_provider_failover_control_is_diagnostic_for_exact_manual_quote() -> None:
    intent = {
        "status": "trade_ready",
        "execution_eligible": True,
        "block_reasons": [],
    }
    control = {
        "allowed": False,
        "reason": "control_state_stale",
        "mode": "ibkr_fallback",
    }

    observed = _apply_provider_entry_control(intent, control)

    assert observed["status"] == "trade_ready"
    assert observed["execution_eligible"] is True
    assert observed["block_reasons"] == []
    assert observed["provider_incident_warning"] == "control_state_stale"
    assert observed["provider_failover_control"] == control


def test_reviewed_pilot_keeps_exact_quote_but_softens_redundant_context_gates() -> None:
    market, options, latest, context, repricing = _ready_inputs()
    market = replace(
        market,
        es={
            **market.es,
            "return_1m_points": -0.5,
            "return_5m_points": 1.0,
        },
        volume={"price_volume_alignment_5m": "unavailable"},
        cross_asset={"es_spy_direction_confirmation_15m": "divergent"},
    )
    options = replace(
        options,
        volatility={},
        l1=replace(options.l1, quality=FrameQuality.UNAVAILABLE),
    )
    context = replace(
        context,
        invalidations=("es_spy_direction_divergent", "hot_option_liquidity_low"),
        breakout_filter={
            "event_id": "level:test",
            "verdict": "pending",
            "actionable": False,
        },
    )

    intent = evaluate_trade_intent(
        context,
        market,
        options,
        latest,
        repricing,
        now=NOW,
        feature_policy=MarketFeatureSettings(
            trade_confirmed_pilot_enabled=True,
            trade_min_reward_risk=0.4,
        ),
        order_policy=OrderMapPolicy(),
    )

    assert intent["status"] == "trade_ready"
    assert intent["strategy_lane"] == "long_0dte_rth_upper_breakout_call_manual"
    assert intent["pilot_mode"] is False
    assert intent["wall_signal"] == "present"
    assert intent["execution_eligible"] is True
    assert intent["quote_observation_eligible"] is False
    assert intent["priority"] == "high"
    assert intent["shadow_mode"] is False
    assert intent["automatic_ordering"] is False
    assert set(intent["pilot_diagnostics"]) == {
        "breakout_filter_not_supported",
        "es_return_1m_points_opposes_direction",
        "price_volume_not_directionally_aligned",
        "es_spy_direction_divergent",
        "option_l1_not_ready",
        "expected_move_unavailable",
        "hot_option_liquidity_low",
    }


def test_joint_immediate_reversal_is_diagnostic_after_level_confirmation() -> None:
    market, options, latest, context, repricing = _ready_inputs()
    market = replace(
        market,
        es={
            **market.es,
            "return_1m_points": -1.0,
            "return_5m_points": -2.0,
        },
    )

    intent = evaluate_trade_intent(
        context,
        market,
        options,
        latest,
        repricing,
        now=NOW,
        feature_policy=MarketFeatureSettings(
            trade_confirmed_pilot_enabled=True,
            trade_min_reward_risk=0.4,
        ),
        order_policy=OrderMapPolicy(),
    )

    assert intent["status"] == "trade_ready"
    assert "es_1m_5m_jointly_oppose_direction" in intent["pilot_diagnostics"]


def test_breakout_filter_block_vetoes_manual_entry_after_level_confirmation() -> None:
    market, options, latest, context, repricing = _ready_inputs()
    context = replace(
        context,
        breakout_filter={
            "event_id": "level:test",
            "verdict": "blocked",
            "actionable": False,
        },
    )

    intent = evaluate_trade_intent(
        context,
        market,
        options,
        latest,
        repricing,
        now=NOW,
        feature_policy=MarketFeatureSettings(trade_confirmed_pilot_enabled=True),
        order_policy=OrderMapPolicy(),
    )

    assert intent["status"] == "blocked"
    assert "breakout_filter_blocked" in intent["block_reasons"]
    assert "breakout_filter_blocked" in intent["pilot_diagnostics"]


def test_reviewed_pilot_is_upside_breakout_only() -> None:
    market, options, latest, context, repricing = _ready_inputs()
    context = replace(
        context,
        level_decision={
            **context.level_decision,
            "thesis": "fade",
        },
    )

    intent = evaluate_trade_intent(
        context,
        market,
        options,
        latest,
        repricing,
        now=NOW,
        feature_policy=MarketFeatureSettings(trade_confirmed_pilot_enabled=True),
        order_policy=OrderMapPolicy(),
    )

    assert intent["status"] == "blocked"
    assert "unsupported_level_path" in intent["block_reasons"]


def test_manual_signal_does_not_emit_trade_ready_after_1530_et() -> None:
    late = datetime(2026, 7, 14, 19, 50, tzinfo=UTC)
    market, options, latest, context, repricing = _retimed_inputs(late)

    intent = evaluate_trade_intent(
        context,
        market,
        options,
        latest,
        repricing,
        now=late,
        feature_policy=MarketFeatureSettings(trade_confirmed_pilot_enabled=True),
        order_policy=OrderMapPolicy(),
    )

    assert intent["status"] == "blocked"
    assert intent["execution_eligible"] is False
    assert "strategy_entry_window_closed" in intent["block_reasons"]


def test_confirmed_level_remains_manual_ready_after_1300_et() -> None:
    afternoon = datetime(2026, 7, 14, 17, 25, tzinfo=UTC)
    market, options, latest, context, repricing = _retimed_inputs(afternoon)

    intent = evaluate_trade_intent(
        context,
        market,
        options,
        latest,
        repricing,
        now=afternoon,
        feature_policy=MarketFeatureSettings(trade_confirmed_pilot_enabled=True),
        order_policy=OrderMapPolicy(),
    )

    assert intent["status"] == "trade_ready"
    assert "strategy_entry_window_closed" not in intent["block_reasons"]


def test_trade_ready_entry_and_time_stop_respect_close_buffers() -> None:
    at = datetime(2026, 7, 14, 19, 29, 50, tzinfo=UTC)
    entry_end = datetime(2026, 7, 14, 19, 30, tzinfo=UTC)
    time_stop = datetime(2026, 7, 14, 19, 44, 50, tzinfo=UTC)
    hard_exit = datetime(2026, 7, 14, 19, 45, tzinfo=UTC)
    market, options, latest, context, repricing = _retimed_inputs(at)

    intent = evaluate_trade_intent(
        context,
        market,
        options,
        latest,
        repricing,
        now=at,
        feature_policy=MarketFeatureSettings(trade_confirmed_pilot_enabled=True),
        order_policy=OrderMapPolicy(),
    )

    assert intent["status"] == "trade_ready"
    assert intent["expires_at"] == entry_end.isoformat()
    assert intent["valid_until"] == entry_end.isoformat()
    assert intent["time_stop_at"] == time_stop.isoformat()
    assert intent["entry_window_end_at"] == entry_end.isoformat()
    assert intent["hard_exit_at"] == hard_exit.isoformat()


@pytest.mark.parametrize(
    ("at", "reason"),
    [
        (
            datetime(2026, 7, 14, 13, 29, 59, tzinfo=UTC),
            "strategy_entry_window_not_open",
        ),
        (
            datetime(2026, 7, 14, 19, 30, tzinfo=UTC),
            "strategy_entry_window_closed",
        ),
    ],
)
def test_strategy_entry_window_is_half_open(at: datetime, reason: str) -> None:
    market, options, latest, context, repricing = _retimed_inputs(at)

    intent = evaluate_trade_intent(
        context,
        market,
        options,
        latest,
        repricing,
        now=at,
        feature_policy=MarketFeatureSettings(trade_confirmed_pilot_enabled=True),
        order_policy=OrderMapPolicy(),
    )

    assert intent["status"] == "blocked"
    assert reason in intent["block_reasons"]


def test_strategy_entry_window_opens_during_first_two_rth_minutes() -> None:
    at = datetime(2026, 7, 14, 13, 31, tzinfo=UTC)
    market, options, latest, context, repricing = _retimed_inputs(at)

    intent = evaluate_trade_intent(
        context,
        market,
        options,
        latest,
        repricing,
        now=at,
        feature_policy=MarketFeatureSettings(trade_confirmed_pilot_enabled=True),
        order_policy=OrderMapPolicy(),
    )

    assert intent["status"] == "trade_ready"
    assert intent["entry_window_start_at"] == "2026-07-14T13:30:00+00:00"


def test_evening_gth_uses_next_trading_session_window() -> None:
    at = datetime(2026, 7, 27, 1, 32, 43, tzinfo=UTC)
    market, options, latest, context, repricing = _retimed_inputs(at)

    intent = evaluate_trade_intent(
        context,
        market,
        options,
        latest,
        repricing,
        now=at,
        feature_policy=MarketFeatureSettings(trade_confirmed_pilot_enabled=True),
        order_policy=OrderMapPolicy(),
    )

    assert intent["status"] == "blocked"
    assert intent["entry_window_start_at"] == "2026-07-27T13:30:00+00:00"
    assert intent["entry_window_end_at"] == "2026-07-27T19:30:00+00:00"
    assert intent["hard_exit_at"] == "2026-07-27T19:45:00+00:00"
    assert intent["valid_until"] == (at + timedelta(minutes=3)).isoformat()
    assert intent["execution_eligible"] is False
    assert "rth_session_required" in intent["block_reasons"]
    assert "strategy_entry_window_not_open" in intent["block_reasons"]
    assert "strategy_entry_window_closed" not in intent["block_reasons"]


def test_flip_low_breakdown_put_is_exact_quote_manual_ready() -> None:
    market, options, latest, context, repricing = _put_shadow_inputs(
        thesis="breakout",
        level_kind="flip_low",
        level=7525.0,
    )

    intent = evaluate_trade_intent(
        context,
        market,
        options,
        latest,
        repricing,
        now=NOW,
        feature_policy=MarketFeatureSettings(
            trade_confirmed_pilot_enabled=True,
            trade_min_reward_risk=0.4,
        ),
        order_policy=OrderMapPolicy(),
    )

    assert intent["status"] == "trade_ready"
    assert intent["strategy_lane"] == "long_0dte_rth_flip_low_breakdown_put_manual"
    assert intent["contract_label"] == "SPXW 7525P"
    assert intent["wall_signal"] == "present"
    assert intent["execution_eligible"] is True
    assert intent["quote_observation_eligible"] is False
    assert intent["priority"] == "normal"
    assert intent["shadow_mode"] is False
    assert intent["promotion_status"] == "manual_live"
    assert intent["automatic_ordering"] is False


@pytest.mark.parametrize(("level_kind", "level"), [("call_wall", 7550.0), ("flip_high", 7530.0)])
def test_upper_rejection_put_is_exact_quote_manual_ready(
    level_kind: str,
    level: float,
) -> None:
    market, options, latest, context, repricing = _put_shadow_inputs(
        thesis="fade",
        level_kind=level_kind,
        level=level,
    )

    intent = evaluate_trade_intent(
        context,
        market,
        options,
        latest,
        repricing,
        now=NOW,
        feature_policy=MarketFeatureSettings(
            trade_confirmed_pilot_enabled=True,
            trade_min_reward_risk=0.4,
        ),
        order_policy=OrderMapPolicy(),
    )

    assert intent["status"] == "trade_ready"
    assert intent["strategy_lane"] == "long_0dte_rth_upper_rejection_put_manual"
    assert intent["wall_signal"] == "present"
    assert intent["execution_eligible"] is True
    assert intent["quote_observation_eligible"] is False
    assert intent["priority"] == "normal"
    assert intent["shadow_mode"] is False
    assert intent["automatic_ordering"] is False


def test_upper_rejection_put_treats_bearish_regime_as_priority_not_hard_gate() -> None:
    market, options, latest, context, repricing = _put_shadow_inputs(
        thesis="fade",
        level_kind="call_wall",
        level=7550.0,
    )
    context = replace(
        context,
        regime_decision={"mode": "trending", "direction": "down", "trend_score": -80.0},
    )

    intent = evaluate_trade_intent(
        context,
        market,
        options,
        latest,
        repricing,
        now=NOW,
        feature_policy=MarketFeatureSettings(
            trade_confirmed_pilot_enabled=True,
            trade_min_reward_risk=0.4,
        ),
        order_policy=OrderMapPolicy(),
    )

    assert intent["status"] == "trade_ready"
    assert "fade_regime_not_mean_reverting" not in intent["block_reasons"]


def test_put_manual_ready_keeps_opposing_regime_as_diagnostic() -> None:
    market, options, latest, context, repricing = _put_shadow_inputs(
        thesis="breakout",
        level_kind="flip_low",
        level=7525.0,
    )
    context = replace(
        context,
        regime_decision={"mode": "trending", "direction": "up", "trend_score": 80.0},
    )

    intent = evaluate_trade_intent(
        context,
        market,
        options,
        latest,
        repricing,
        now=NOW,
        feature_policy=MarketFeatureSettings(
            trade_confirmed_pilot_enabled=True,
            trade_min_reward_risk=0.4,
        ),
        order_policy=OrderMapPolicy(),
    )

    assert intent["status"] == "trade_ready"
    assert "regime_direction_conflict" not in intent["block_reasons"]
    assert "regime_direction_conflict" in intent["pilot_diagnostics"]


def test_put_wall_breakdown_is_manual_ready() -> None:
    market, options, latest, context, repricing = _put_shadow_inputs(
        thesis="breakout",
        level_kind="put_wall",
        level=7500.0,
    )

    intent = evaluate_trade_intent(
        context,
        market,
        options,
        latest,
        repricing,
        now=NOW,
        feature_policy=MarketFeatureSettings(
            trade_confirmed_pilot_enabled=True,
            trade_min_reward_risk=0.4,
        ),
        order_policy=OrderMapPolicy(),
    )

    assert intent["status"] == "trade_ready"
    assert intent["strategy_lane"] == "long_0dte_rth_put_wall_breakdown_put_manual"
    assert intent["wall_signal"] == "present"
    assert intent["execution_eligible"] is True
    assert intent["quote_observation_eligible"] is False
    assert intent["priority"] == "normal"
    assert intent["shadow_mode"] is False
    assert intent["block_reasons"] == []
    assert intent["automatic_ordering"] is False


def test_legacy_pilot_flag_no_longer_suppresses_manual_lanes() -> None:
    market, options, latest, context, repricing = _put_shadow_inputs(
        thesis="breakout",
        level_kind="put_wall",
        level=7500.0,
    )

    disabled = evaluate_trade_intent(
        context,
        market,
        options,
        latest,
        repricing,
        now=NOW,
        feature_policy=MarketFeatureSettings(
            trade_confirmed_pilot_enabled=False,
            trade_min_reward_risk=0.4,
        ),
        order_policy=OrderMapPolicy(),
    )

    assert disabled["status"] == "trade_ready"
    assert disabled["strategy_lane"] == "long_0dte_rth_put_wall_breakdown_put_manual"
    assert disabled["execution_eligible"] is True
    assert disabled["quote_observation_eligible"] is False
    assert disabled["block_reasons"] == []

    flip_market, flip_options, flip_latest, flip_context, flip_repricing = _put_shadow_inputs(
        thesis="breakout",
        level_kind="flip_low",
        level=7525.0,
    )
    blocked = evaluate_trade_intent(
        flip_context,
        flip_market,
        flip_options,
        flip_latest,
        flip_repricing,
        now=NOW,
        feature_policy=MarketFeatureSettings(
            trade_confirmed_pilot_enabled=False,
            trade_min_reward_risk=0.4,
        ),
        order_policy=OrderMapPolicy(),
    )

    assert blocked["status"] == "trade_ready"
    assert blocked["strategy_lane"] == "long_0dte_rth_flip_low_breakdown_put_manual"
    assert blocked["execution_eligible"] is True
    assert blocked["quote_observation_eligible"] is False
    assert blocked["shadow_mode"] is False
    assert blocked["block_reasons"] == []


def test_put_manual_ready_has_human_notification_authority(tmp_path) -> None:
    market, options, latest, context, repricing = _put_shadow_inputs(
        thesis="breakout",
        level_kind="flip_low",
        level=7525.0,
    )
    intent = evaluate_trade_intent(
        context,
        market,
        options,
        latest,
        repricing,
        now=NOW,
        feature_policy=MarketFeatureSettings(
            trade_confirmed_pilot_enabled=True,
            trade_min_reward_risk=0.4,
        ),
        order_policy=OrderMapPolicy(),
    )

    assert intent["status"] == "trade_ready"
    assert intent["automatic_ordering"] is False
    assert live_trade_intent_authority_issues(intent) == ()
    result = process_trade_intent(
        SimpleNamespace(data_root=str(tmp_path)),
        intent,
        now=NOW,
        settings=SimpleNamespace(enabled=False),
    )
    assert result["reason"] == "notification_disabled"


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
        "semantics": "matched_touch_quote_outcomes_not_live_fills",
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


def test_render_trade_intent_shows_historical_edge_beside_trade_odds() -> None:
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

    assert "🟢 MANUAL READY · PUT" in text
    assert "level_fade_put@call_wall" not in text
    assert "买入  SPXW 07-15 7550P" in text
    assert "赔率  剩余目标/止损距离 3.00:1" in text
    assert "历史  同类触位报价 23 笔" in text
    assert "5分钟正收益率 61.0%" in text
    assert "非实盘胜率" in text


def test_render_trade_intent_keeps_diagnostics_out_of_action_card() -> None:
    text = render_trade_intent(
        {
            **_render_intent(),
            "greek_confidence": {
                "raw_score": 5.0,
                "delta": 0.48,
                "gamma_per_point": 0.02,
                "theta_15m_loss_fraction": 0.1234,
                "iv_down_3vol_loss_fraction": 0.0876,
            },
            "pilot_diagnostics": [
                "rth_spy_confirmation_unavailable",
                "option_l1_not_ready",
            ],
            "moving_average_context": {
                "price": 7603.0,
                "sma20": 7595.0,
                "sma50": 7580.0,
                "sma200": 7550.0,
                "atr_5m": 10.0,
                "distance_to_sma50_atr": 2.3,
                "distance_to_sma200_atr": 5.3,
                "ma50_slope_3_atr": 0.31,
                "ma50_slope_6_atr": 0.48,
                "ma200_slope_3_atr": 0.04,
                "ma200_slope_6_atr": 0.08,
                "ma50_ma200_spread_atr": 3.0,
                "cross_direction": "golden",
                "bars_since_cross": 27,
                "cross_persistent_2_bars": True,
                "cross_fresh": False,
                "regime_state": "TREND_EXTENDED",
                "regime_direction": "up",
                "same_direction_convexity": "do_not_chase",
                "relation": "bullish_stack",
                "spx_equivalent_sma20": 7550.0,
                "spx_equivalent_sma50": 7535.0,
                "spx_equivalent_sma200": 7505.0,
                "spx_projection_near_line": True,
                "action_authority": "none",
            },
        }
    )

    assert "希腊风险" not in text
    assert "MA50/200" not in text
    assert "rth_spy_confirmation_unavailable" not in text
    assert "限价  ≤ 10.10" in text
    assert "止损  SPX 收回 7553.00" in text
    assert "目标  SPX 7525.00" in text


def test_render_trade_intent_hides_play_stats_when_absent() -> None:
    text = render_trade_intent(_render_intent())

    assert "## 同类信号" not in text


def test_render_trade_intent_shows_spring_gamma_without_making_it_a_gate() -> None:
    text = render_trade_intent(
        {
            **_render_intent(),
            "spring_gamma": {
                "status": "ready",
                "preferred_side": "CALL",
                "score": 0.63,
                "non_blocking": True,
                "execution_authority": False,
            },
        }
    )

    assert "方向模型  Spring v3（ES）偏多（0.63）" in text
    assert "与本卡背离" in text
    assert "只作排序，不作门禁" in text


def test_llm_writer_output_must_preserve_action_ticket_fields() -> None:
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
    without_limit = "\n".join(
        line for line in template.splitlines() if not line.startswith("限价  ")
    )

    assert _writer_output_valid(template, intent)
    assert not _writer_output_valid(without_limit, intent)


def test_llm_writer_rejects_ma_cross_as_standalone_trade_trigger() -> None:
    intent = {
        **_render_intent(),
        "moving_average_context": {
            "regime_state": "REGIME_TRANSITION",
            "regime_direction": "up",
            "same_direction_convexity": "wait_for_wall_confirmation",
        },
    }
    safe = render_trade_intent(intent) + "\nREGIME_TRANSITION 均线背景；等待wall/flip接受或拒绝。"
    unsafe = safe + "\n金叉买Call。"

    assert _writer_output_valid(safe, intent)
    assert not _writer_output_valid(unsafe, intent)


def test_pending_filter_and_opposing_regime_are_diagnostics() -> None:
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

    assert intent["status"] == "trade_ready"
    assert "breakout_filter_not_supported" in intent["pilot_diagnostics"]
    assert "regime_direction_conflict" in intent["pilot_diagnostics"]


def test_confirmed_session_recovery_is_a_diagnostic() -> None:
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

    assert intent["status"] == "trade_ready"
    assert "session_episode_direction_conflict" in intent["pilot_diagnostics"]


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


def test_live_structure_drift_does_not_cancel_frozen_event() -> None:
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

    assert intent["status"] == "trade_ready"
    assert "trigger_structure_drift" not in intent["block_reasons"]


def test_remaining_target_room_and_reward_risk_are_hard_entry_gates() -> None:
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


def test_default_reward_risk_floor_demotes_sub_one_rth_late_entry() -> None:
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
    assert intent["reward_risk_kind"] == "underlier_remaining_distance"
    assert intent["reward_risk_threshold"] == 1.0
    assert intent["opportunity_disposition"] == "late_chase_observation"
    assert intent["status"] == "blocked"
    assert "remaining_reward_risk_insufficient" in intent["block_reasons"]


def test_sufficiently_sampled_negative_historical_edge_is_diagnostic_by_default() -> None:
    market, options, latest, context, repricing = _ready_inputs()
    stats = PlayOutcomeStats(
        play="level_breakout_call",
        level_kind="call_wall",
        sample_count=54,
        winrate=0.3889,
        avg_return=-0.02161,
        median_return=-0.018245,
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
    assert {
        "historical_winrate_below_floor",
        "historical_average_return_non_positive",
        "historical_median_return_non_positive",
    }.issubset(intent["pilot_diagnostics"])
    assert intent["block_reasons"] == []
    assert intent["play_stats"]["sample_count"] == 54


def test_historical_edge_can_be_restored_as_an_explicit_hard_gate() -> None:
    market, options, latest, context, repricing = _ready_inputs()
    stats = PlayOutcomeStats(
        play="level_breakout_call",
        level_kind="call_wall",
        sample_count=54,
        winrate=0.3889,
        avg_return=-0.02161,
        median_return=-0.018245,
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
        feature_policy=MarketFeatureSettings(play_stats_hard_gate_enabled=True),
        order_policy=OrderMapPolicy(),
        play_stats=stats,
    )

    assert intent["status"] == "blocked"
    assert "historical_winrate_below_floor" in intent["block_reasons"]


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
        "strategy_entry_window_not_open",
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
    rearmed_level = {
        **context.level_decision,
        "event_id": "level:rearmed",
        "reentry_generation": 1,
    }
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
    assert first["reentry_generation"] == 0
    assert second["reentry_generation"] == 1


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
    envelopes = []
    monkeypatch.setattr(
        "spx_spark.application.market_features.trade_intent_runtime._action_now",
        lambda: NOW,
    )
    monkeypatch.setattr(
        "spx_spark.application.market_features.trade_intent_runtime._action_revalidation",
        lambda *_args, **_kwargs: (None, {"quote_revalidation": "test_stub"}),
    )

    def fake_enqueue(_settings, _envelope, **kwargs):
        envelopes.append(_envelope)
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
    assert envelopes[0].expires_at == NOW + timedelta(seconds=90)


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
    monkeypatch.setattr(
        trade_intent_delivery_contract,
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

    replay_intent = {
        **intent,
        "evaluated_at": (NOW + timedelta(seconds=121)).isoformat(),
        "quote_source_at": (NOW + timedelta(seconds=121)).isoformat(),
        "decision_bid": 10.05,
        "decision_ask": 10.35,
    }
    replay = process_trade_intent(
        storage,
        replay_intent,
        now=NOW + timedelta(seconds=121),
        settings=settings,
        feature_policy=MarketFeatureSettings(),
    )

    assert replay["accepted"] is True
    assert replay["duplicate"] is True
    assert replay["inserted"] is False
    state = json.loads((tmp_path / "latest" / "trade_intent_delivery_state.json").read_text())
    assert "delivered" not in state
    delivery_event_id = _trade_ready_delivery_event_id(intent)
    assert delivery_event_id in state["accepted"]
    with sqlite3.connect(settings.delivery_outbox_path) as connection:
        rows = connection.execute(
            "SELECT event_id, occurred_at, expires_at, text FROM notification_delivery_events"
        ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == delivery_event_id
    assert rows[0][1] == NOW.isoformat(timespec="microseconds")
    assert rows[0][2] == (NOW + timedelta(minutes=5)).isoformat(timespec="microseconds")
    assert "10.00 / 10.40" in rows[0][3]
    assert "10.05" not in rows[0][3]

    deliveries: list[object] = []
    monkeypatch.setattr(
        "spx_spark.notifier.dispatcher.deliver_trade_push",
        lambda *_args, **_kwargs: deliveries.append(object()),
    )
    consumed = consume_pending_notifications(
        settings,
        now=NOW + timedelta(minutes=6),
        notify_dead_letters=False,
    )

    assert consumed["jobs"] == 0
    assert consumed["attempted_targets"] == 0
    assert deliveries == []
    with sqlite3.connect(settings.delivery_outbox_path) as connection:
        status = connection.execute(
            "SELECT status FROM notification_delivery_events WHERE event_id = ?",
            (delivery_event_id,),
        ).fetchone()[0]
    assert status == "dead_letter"


def test_enqueue_ack_crash_then_invalidation_cancels_stale_ready(
    tmp_path,
    monkeypatch,
) -> None:
    semantic_scope = "2026-07-14|level_breakout_call|7550.0000"
    intent = {
        **_runtime_contract(NOW + timedelta(minutes=5)),
        "status": "trade_ready",
        "intent_id": "intent:crash-invalidation",
        "semantic_scope": semantic_scope,
        "semantic_key": (f"{semantic_scope}|option:SPX:SPXW:20260714:7550:C"),
        "event_id": "level:crash-invalidation:first",
        "evaluated_at": NOW.isoformat(),
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
        delivery_outbox_legacy_shadow_enabled=False,
    )
    monkeypatch.setattr(
        trade_intent_runtime,
        "_action_revalidation",
        lambda *_args, **_kwargs: (
            None,
            {"quote_revalidation": "test_stub"},
        ),
    )
    original_write = trade_intent_runtime.atomic_write_json_secure
    state_writes = 0

    def crash_after_enqueue(path, payload):
        nonlocal state_writes
        if path.name == "trade_intent_delivery_state.json":
            state_writes += 1
            if state_writes == 2:
                raise RuntimeError("simulated crash after durable enqueue")
        return original_write(path, payload)

    monkeypatch.setattr(
        trade_intent_runtime,
        "atomic_write_json_secure",
        crash_after_enqueue,
    )
    monkeypatch.setattr(
        trade_intent_delivery_contract,
        "atomic_write_json_secure",
        crash_after_enqueue,
    )
    storage = SimpleNamespace(data_root=str(tmp_path))

    with pytest.raises(RuntimeError, match="simulated crash"):
        process_trade_intent(
            storage,
            intent,
            now=NOW,
            action_now=NOW,
            settings=settings,
            feature_policy=MarketFeatureSettings(),
        )

    delivery_event_id = _trade_ready_delivery_event_id(intent)
    crashed_state = json.loads(
        (tmp_path / "latest" / "trade_intent_delivery_state.json").read_text()
    )
    assert crashed_state["accepted"] == {}
    assert len(crashed_state["delivery_lifecycle_events"]) == 1
    lifecycle = crashed_state["delivery_lifecycle_events"][0]
    assert lifecycle["event_id"] == delivery_event_id
    assert lifecycle["semantic_key"] == intent["semantic_key"]
    assert lifecycle["semantic_scope"] == semantic_scope
    assert lifecycle["targets"] == ["feishu"]
    assert len(lifecycle["payload_fingerprint"]) == 64

    invalidated = process_trade_intent(
        storage,
        {
            "status": "observing",
            "phase": "invalidated",
            "event_id": intent["event_id"],
            "semantic_scope": semantic_scope,
        },
        now=NOW + timedelta(seconds=1),
        settings=settings,
    )
    consumed = consume_pending_notifications(
        settings,
        now=NOW + timedelta(seconds=2),
        notify_dead_letters=False,
    )

    assert invalidated["reason"] == "observing"
    assert consumed["jobs"] == 0
    assert consumed["delivered_targets"] == 0
    with sqlite3.connect(settings.delivery_outbox_path) as connection:
        old_status = connection.execute(
            "SELECT status FROM notification_delivery_events WHERE event_id = ?",
            (delivery_event_id,),
        ).fetchone()
    assert old_status == ("dead_letter",)

    rearmed_intent = {
        **intent,
        "reentry_generation": 1,
        "event_id": "level:crash-invalidation:second",
        "evaluated_at": (NOW + timedelta(seconds=3)).isoformat(),
        "quote_source_at": (NOW + timedelta(seconds=3)).isoformat(),
    }
    rearmed = process_trade_intent(
        storage,
        rearmed_intent,
        now=NOW + timedelta(seconds=3),
        action_now=NOW + timedelta(seconds=3),
        settings=settings,
        feature_policy=MarketFeatureSettings(),
    )
    rearmed_delivery_event_id = _trade_ready_delivery_event_id(rearmed_intent)

    assert rearmed["accepted"] is True
    assert rearmed_delivery_event_id != delivery_event_id
    with sqlite3.connect(settings.delivery_outbox_path) as connection:
        statuses = dict(
            connection.execute(
                "SELECT event_id, status FROM notification_delivery_events"
            ).fetchall()
        )
    assert statuses == {
        delivery_event_id: "dead_letter",
        rearmed_delivery_event_id: "pending",
    }


def test_failed_rth_cancellation_blocks_rearmed_lifecycle(
    tmp_path,
    monkeypatch,
) -> None:
    semantic_scope = "2026-07-14|level_breakout_call|7550.0000"
    intent = {
        **_runtime_contract(NOW + timedelta(minutes=5)),
        "status": "trade_ready",
        "intent_id": "intent:cancellation-gate",
        "semantic_scope": semantic_scope,
        "semantic_key": (f"{semantic_scope}|option:SPX:SPXW:20260714:7550:C"),
        "event_id": "level:cancellation-gate:first",
        "evaluated_at": NOW.isoformat(),
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
        delivery_outbox_legacy_shadow_enabled=False,
    )
    monkeypatch.setattr(
        trade_intent_runtime,
        "_action_revalidation",
        lambda *_args, **_kwargs: (
            None,
            {"quote_revalidation": "test_stub"},
        ),
    )
    storage = SimpleNamespace(data_root=str(tmp_path))
    first = process_trade_intent(
        storage,
        intent,
        now=NOW,
        action_now=NOW,
        settings=settings,
        feature_policy=MarketFeatureSettings(),
    )
    monkeypatch.setattr(
        trade_intent_runtime,
        "cancel_pending_notification",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("outbox unavailable")),
    )

    process_trade_intent(
        storage,
        {
            "status": "observing",
            "phase": "invalidated",
            "event_id": intent["event_id"],
            "semantic_scope": semantic_scope,
        },
        now=NOW + timedelta(seconds=1),
        settings=settings,
    )
    rearmed = process_trade_intent(
        storage,
        {
            **intent,
            "reentry_generation": 1,
            "event_id": "level:cancellation-gate:second",
            "evaluated_at": (NOW + timedelta(seconds=2)).isoformat(),
            "quote_source_at": (NOW + timedelta(seconds=2)).isoformat(),
        },
        now=NOW + timedelta(seconds=2),
        action_now=NOW + timedelta(seconds=2),
        settings=settings,
        feature_policy=MarketFeatureSettings(),
    )
    state = json.loads((tmp_path / "latest" / "trade_intent_delivery_state.json").read_text())

    first_delivery_event_id = _trade_ready_delivery_event_id(intent)
    assert first["accepted"] is True
    assert rearmed["reason"] == "lifecycle_cancellation_pending"
    assert state["pending_delivery_cancellation_event_ids"] == [first_delivery_event_id]
    with sqlite3.connect(settings.delivery_outbox_path) as connection:
        rows = connection.execute("SELECT event_id FROM notification_delivery_events").fetchall()
    assert rows == [(first_delivery_event_id,)]


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
    assert state["schema_version"] == 3
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


def test_mislabeled_put_trade_ready_is_rejected_before_notification(
    tmp_path,
    monkeypatch,
) -> None:
    intent = {
        **_runtime_contract(NOW + timedelta(seconds=90)),
        "intent_id": "intent:unsafe-put",
        "event_id": "level:unsafe-put",
        "strategy_lane": "long_0dte_rth_flip_low_breakdown_put_shadow",
        "direction": "down",
        "contract_id": "option:SPX:SPXW:20260714:7550:P",
    }
    monkeypatch.setattr(
        "spx_spark.application.market_features.trade_intent_runtime.enqueue_notification",
        lambda *_args, **_kwargs: pytest.fail("Put shadow must not notify"),
    )

    result = process_trade_intent(
        SimpleNamespace(data_root=str(tmp_path)),
        intent,
        now=NOW,
        settings=_notification_settings(tmp_path),
    )

    assert result == {
        "attempted": False,
        "delivered": False,
        "reason": "put_lane_live_execution_forbidden",
    }
    persisted = json.loads((tmp_path / "latest" / "trade_intent.json").read_text(encoding="utf-8"))
    assert persisted["signal_status"] == "trade_ready"
    assert persisted["status"] == "delivery_blocked"
    assert persisted["notification_status"] == "blocked"
    audit_path = (
        tmp_path / "features" / "trade_intents" / f"date={NOW.date().isoformat()}" / "events.jsonl"
    )
    audit = [json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines()]
    assert all(row["status"] != "trade_ready" for row in audit)


def test_forged_put_wall_trade_ready_is_rejected_before_notification(
    tmp_path,
    monkeypatch,
) -> None:
    intent = {
        **_runtime_contract(NOW + timedelta(seconds=90)),
        "intent_id": "intent:unsafe-put-wall",
        "event_id": "level:unsafe-put-wall",
        "strategy_lane": "long_0dte_rth_put_wall_breakdown_disabled",
        "direction": "down",
        "contract_id": "option:SPX:SPXW:20260714:7500:P",
        "level_kind": "put_wall",
        "thesis": "breakout",
    }
    monkeypatch.setattr(
        "spx_spark.application.market_features.trade_intent_runtime.enqueue_notification",
        lambda *_args, **_kwargs: pytest.fail("disabled Put Wall must not notify"),
    )

    result = process_trade_intent(
        SimpleNamespace(data_root=str(tmp_path)),
        intent,
        now=NOW,
        settings=_notification_settings(tmp_path),
    )

    assert result == {
        "attempted": False,
        "delivered": False,
        "reason": "put_lane_live_execution_forbidden",
    }


def test_legacy_v1_trade_ready_is_no_longer_execution_authority(tmp_path) -> None:
    result = process_trade_intent(
        SimpleNamespace(data_root=str(tmp_path)),
        {
            "schema_version": 1,
            "status": "trade_ready",
            "intent_id": "intent:legacy-v1",
            "expires_at": (NOW + timedelta(seconds=90)).isoformat(),
        },
        now=NOW,
        settings=SimpleNamespace(enabled=False),
    )

    assert result == {
        "attempted": False,
        "delivered": False,
        "reason": "strategy_schema_unsupported",
    }


def test_action_revalidation_audits_moving_live_quote_without_blocking(
    monkeypatch,
) -> None:
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

    assert reason is None
    assert evidence["entry_limit"] == pytest.approx(10.4)
    assert evidence["recomputed_entry_limit"] == pytest.approx(10.6)
    assert evidence["entry_limit_changed"] is True
    assert evidence["entry_limit_policy"] == "immutable_decision_limit"
    assert evidence["reason"] is None
    assert evidence["quote_state_created_at"] == changed_latest.created_at.isoformat()


def test_moving_live_quote_enqueues_immutable_trade_ready_card_once(
    tmp_path,
    monkeypatch,
) -> None:
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
    action_at = NOW + timedelta(seconds=1)
    moving_quote = replace(
        latest.best_quotes[0],
        received_at=action_at,
        last_update_at=action_at,
        quote_time=action_at,
        bid=10.2,
        ask=10.6,
    )
    moving_latest = LatestState(
        created_at=action_at,
        as_of=action_at,
        quotes=(moving_quote,),
        best_quotes=(moving_quote,),
    )

    class Store:
        def __init__(self, _storage) -> None:
            pass

        def load(self, *, now):
            assert now == action_at
            return moving_latest

    monkeypatch.setattr(
        "spx_spark.application.market_features.trade_intent_runtime.LatestStateStore",
        Store,
    )
    storage = SimpleNamespace(data_root=str(tmp_path))
    settings = _outbox_notification_settings(tmp_path)

    first = process_trade_intent(
        storage,
        intent,
        now=NOW,
        action_now=action_at,
        settings=settings,
        feature_policy=policy,
        order_policy=OrderMapPolicy(),
        expected_policy_version=str(intent["policy_version"]),
    )
    second = process_trade_intent(
        storage,
        intent,
        now=NOW + timedelta(seconds=2),
        action_now=NOW + timedelta(seconds=2),
        settings=settings,
        feature_policy=policy,
        order_policy=OrderMapPolicy(),
        expected_policy_version=str(intent["policy_version"]),
    )

    assert first["accepted"] is True
    assert second["reason"] == "outbox_event_reconciled"
    with sqlite3.connect(settings.delivery_outbox_path) as connection:
        rows = connection.execute("SELECT text FROM notification_delivery_events").fetchall()
    assert len(rows) == 1
    assert "NBBO  10.00 / 10.40（决策快照）" in rows[0][0]
    assert "限价  ≤ 10.40" in rows[0][0]
    assert "类型  单腿 · 仅人工提交" in rows[0][0]
    state = json.loads((tmp_path / "latest" / "trade_intent_delivery_state.json").read_text())
    audit = state["last_action_revalidation"]
    assert audit["entry_limit"] == pytest.approx(10.4)
    assert audit["recomputed_entry_limit"] == pytest.approx(10.6)
    assert audit["entry_limit_changed"] is True
    assert audit["entry_limit_policy"] == "immutable_decision_limit"
    intent_audit_path = (
        tmp_path / "features" / "trade_intents" / f"date={NOW.date().isoformat()}" / "events.jsonl"
    )
    persisted_statuses = [
        json.loads(line)["status"]
        for line in intent_audit_path.read_text(encoding="utf-8").splitlines()
    ]
    assert persisted_statuses == ["ready_pending_delivery", "trade_ready"]


def test_trade_ready_producer_worker_receipt_end_to_end_is_idempotent(
    tmp_path,
    monkeypatch,
) -> None:
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
    action_at = NOW + timedelta(seconds=1)
    action_quote = replace(
        latest.best_quotes[0],
        received_at=action_at,
        last_update_at=action_at,
        quote_time=action_at,
    )
    action_latest = LatestState(
        created_at=action_at,
        as_of=action_at,
        quotes=(action_quote,),
        best_quotes=(action_quote,),
    )

    class Store:
        def __init__(self, _storage) -> None:
            pass

        def load(self, *, now):
            assert now == action_at
            return action_latest

    deliveries: list[frozenset[str]] = []

    def fake_sink_success(_settings, **kwargs):
        targets = frozenset(kwargs["targets"])
        deliveries.append(targets)
        return [SinkResult(sink=target, attempted=True, ok=True) for target in targets]

    monkeypatch.setattr(
        "spx_spark.application.market_features.trade_intent_runtime.LatestStateStore",
        Store,
    )
    monkeypatch.setattr(
        "spx_spark.notifier.dispatcher.deliver_trade_push",
        fake_sink_success,
    )
    storage = SimpleNamespace(data_root=str(tmp_path))
    settings = _outbox_notification_settings(tmp_path)

    produced = process_trade_intent(
        storage,
        intent,
        now=NOW,
        action_now=action_at,
        settings=settings,
        feature_policy=policy,
        order_policy=OrderMapPolicy(),
        expected_policy_version=str(intent["policy_version"]),
    )
    event_id = _trade_ready_delivery_event_id(intent)
    consumed = consume_pending_notifications(
        replace(settings),
        now=action_at,
        notify_dead_letters=False,
        worker_id="rth-e2e-consumer:new-instance",
        completion_clock=lambda: action_at,
    )
    duplicate_consumer = consume_pending_notifications(
        replace(settings),
        now=action_at + timedelta(seconds=1),
        notify_dead_letters=False,
        worker_id="rth-e2e-consumer:duplicate",
        completion_clock=lambda: action_at + timedelta(seconds=1),
    )
    duplicate_tick = process_trade_intent(
        storage,
        intent,
        now=NOW + timedelta(seconds=2),
        action_now=NOW + timedelta(seconds=2),
        settings=settings,
        feature_policy=policy,
        order_policy=OrderMapPolicy(),
        expected_policy_version=str(intent["policy_version"]),
    )

    assert produced["accepted"] is True
    assert produced["inserted"] is True
    assert consumed["jobs"] == 1
    assert consumed["delivered_targets"] == 1
    assert duplicate_consumer["jobs"] == 0
    assert duplicate_tick["reason"] == "outbox_event_reconciled"
    assert deliveries == [frozenset({"feishu"})]

    with sqlite3.connect(settings.delivery_outbox_path) as connection:
        outbox_event = connection.execute(
            "SELECT event_id, source, kind, lane, status FROM notification_delivery_events"
        ).fetchone()
        outbox_targets = connection.execute(
            "SELECT event_id, sink, status FROM notification_delivery_targets"
        ).fetchall()
    with sqlite3.connect(settings.delivery_receipt_path) as connection:
        receipts = connection.execute(
            "SELECT event_id, source, kind, lane, outcome, sinks_json "
            "FROM notification_delivery_receipts"
        ).fetchall()

    assert outbox_event == (
        event_id,
        "trade_intent",
        "trade_intent",
        "trade_ready",
        "delivered",
    )
    assert outbox_targets == [(event_id, "feishu", "delivered")]
    assert len(receipts) == 1
    receipt_event_id, source, kind, lane, outcome, sinks_json = receipts[0]
    assert receipt_event_id == event_id
    assert (source, kind, lane, outcome) == (
        "trade_intent",
        "trade_intent",
        "trade_ready",
        "delivered",
    )
    assert json.loads(sinks_json) == [
        {
            "sink": "feishu",
            "attempted": True,
            "ok": True,
            "error": None,
            "verdict": "delivered",
        }
    ]


def test_producer_outbox_e2e_reconciles_both_state_divergence_directions(
    tmp_path,
    monkeypatch,
) -> None:
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
    monkeypatch.setattr(
        trade_intent_runtime,
        "_action_revalidation",
        lambda *_args, **_kwargs: (
            None,
            {"quote_revalidation": "test_stub"},
        ),
    )
    storage = SimpleNamespace(data_root=str(tmp_path))
    settings = _outbox_notification_settings(tmp_path)
    state_path = tmp_path / "latest" / "trade_intent_delivery_state.json"
    delivery_event_id = _trade_ready_delivery_event_id(intent)

    first = process_trade_intent(
        storage,
        intent,
        now=NOW,
        action_now=NOW,
        settings=settings,
        feature_policy=policy,
        expected_policy_version=str(intent["policy_version"]),
    )

    assert first["accepted"] is True
    assert first["inserted"] is True
    with sqlite3.connect(settings.delivery_outbox_path) as connection:
        original_rows = connection.execute(
            "SELECT event_id, text FROM notification_delivery_events"
        ).fetchall()
    assert len(original_rows) == 1
    assert original_rows[0][0] == delivery_event_id
    assert "限价  ≤ 10.40" in original_rows[0][1]

    # Outbox durable, local acknowledgement missing: restore local acceptance
    # without creating a second outbox row.
    local_state = json.loads(state_path.read_text())
    local_state["accepted"] = {}
    local_state["semantic_keys"] = {}
    local_state["inflight"] = {}
    state_path.write_text(json.dumps(local_state), encoding="utf-8")
    restored = process_trade_intent(
        storage,
        intent,
        now=NOW + timedelta(seconds=1),
        action_now=NOW + timedelta(seconds=1),
        settings=settings,
        feature_policy=policy,
        expected_policy_version=str(intent["policy_version"]),
    )

    assert restored["reason"] == "outbox_event_reconciled"
    assert restored["accepted"] is True
    restored_state = json.loads(state_path.read_text())
    assert delivery_event_id in restored_state["accepted"]
    assert restored_state["last_delivery_reconciliation"]["action"] == "restore_local_acceptance"

    # Local acknowledgement durable, outbox row missing: clear the stale
    # projection and re-enqueue the exact immutable event before expiry.
    with sqlite3.connect(settings.delivery_outbox_path) as connection:
        connection.execute(
            "DELETE FROM notification_delivery_targets WHERE event_id = ?",
            (delivery_event_id,),
        )
        connection.execute(
            "DELETE FROM notification_delivery_events WHERE event_id = ?",
            (delivery_event_id,),
        )
    retried = process_trade_intent(
        storage,
        intent,
        now=NOW + timedelta(seconds=2),
        action_now=NOW + timedelta(seconds=2),
        settings=settings,
        feature_policy=policy,
        expected_policy_version=str(intent["policy_version"]),
    )

    assert retried["accepted"] is True
    assert retried["inserted"] is True
    retried_state = json.loads(state_path.read_text())
    assert (
        retried_state["last_delivery_reconciliation"]["status"] == "local_accepted_outbox_missing"
    )
    with sqlite3.connect(settings.delivery_outbox_path) as connection:
        retried_rows = connection.execute(
            "SELECT event_id, text FROM notification_delivery_events"
        ).fetchall()
    assert retried_rows == original_rows


@pytest.mark.parametrize(
    ("mutation", "expected_reason"),
    (
        ("payload", "outbox_reconciliation_payload_mismatch"),
        ("targets", "outbox_reconciliation_target_mismatch"),
        ("dead_letter", "outbox_reconciliation_terminal_or_invalid_status"),
        ("cancelled", "outbox_reconciliation_cancelled"),
    ),
)
def test_trade_ready_reconciliation_rejects_mutated_outbox_contract(
    tmp_path,
    monkeypatch,
    mutation,
    expected_reason,
) -> None:
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
    monkeypatch.setattr(
        trade_intent_runtime,
        "_action_revalidation",
        lambda *_args, **_kwargs: (
            None,
            {"quote_revalidation": "test_stub"},
        ),
    )
    storage = SimpleNamespace(data_root=str(tmp_path))
    settings = _outbox_notification_settings(tmp_path)
    event_id = _trade_ready_delivery_event_id(intent)
    first = process_trade_intent(
        storage,
        intent,
        now=NOW,
        action_now=NOW,
        settings=settings,
        feature_policy=policy,
        expected_policy_version=str(intent["policy_version"]),
    )
    assert first["accepted"] is True

    with sqlite3.connect(settings.delivery_outbox_path) as connection:
        if mutation == "payload":
            connection.execute(
                "UPDATE notification_delivery_events SET text = ? WHERE event_id = ?",
                ("mutated ticket", event_id),
            )
        elif mutation == "targets":
            connection.execute(
                "UPDATE notification_delivery_targets SET sink = ? WHERE event_id = ?",
                ("bark", event_id),
            )
        elif mutation == "dead_letter":
            connection.execute(
                "UPDATE notification_delivery_targets SET status = ? WHERE event_id = ?",
                ("dead_letter", event_id),
            )
            connection.execute(
                "UPDATE notification_delivery_events SET status = ? WHERE event_id = ?",
                ("dead_letter", event_id),
            )
        else:
            connection.execute(
                """
                INSERT INTO notification_delivery_cancellations (
                    event_id, reason, cancelled_at
                ) VALUES (?, ?, ?)
                """,
                (event_id, "source_invalidated", NOW.isoformat()),
            )

    replay = process_trade_intent(
        storage,
        intent,
        now=NOW + timedelta(seconds=1),
        action_now=NOW + timedelta(seconds=1),
        settings=settings,
        feature_policy=policy,
        expected_policy_version=str(intent["policy_version"]),
    )

    assert replay["accepted"] is False
    assert replay["reason"] == expected_reason
    state = json.loads((tmp_path / "latest" / "trade_intent_delivery_state.json").read_text())
    assert state["last_delivery_reconciliation"]["action"] == "hard_fault"
    projection = json.loads((tmp_path / "latest" / "trade_intent.json").read_text())
    assert projection["status"] == "delivery_blocked"
    assert projection["notification_status"] == "blocked"


def test_reconciliation_exception_does_not_acquire_lease_and_next_tick_retries(
    tmp_path,
    monkeypatch,
) -> None:
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
    monkeypatch.setattr(
        trade_intent_runtime,
        "_action_revalidation",
        lambda *_args, **_kwargs: (None, {"quote_revalidation": "test_stub"}),
    )
    real_inspect = trade_intent_runtime.inspect_notification_event
    inspections = 0

    def fail_first_inspection(*args, **kwargs):
        nonlocal inspections
        inspections += 1
        if inspections == 1:
            raise sqlite3.OperationalError("database is busy")
        return real_inspect(*args, **kwargs)

    monkeypatch.setattr(
        trade_intent_runtime,
        "inspect_notification_event",
        fail_first_inspection,
    )
    storage = SimpleNamespace(data_root=str(tmp_path))
    settings = _outbox_notification_settings(tmp_path)
    state_path = tmp_path / "latest" / "trade_intent_delivery_state.json"

    failed = process_trade_intent(
        storage,
        intent,
        now=NOW,
        action_now=NOW,
        settings=settings,
        feature_policy=policy,
        expected_policy_version=str(intent["policy_version"]),
    )

    assert failed["reason"] == "outbox_reconciliation_exception:OperationalError"
    assert json.loads(state_path.read_text())["inflight"] == {}
    projection = json.loads((tmp_path / "latest" / "trade_intent.json").read_text())
    assert projection["status"] == "ready_pending_delivery"
    assert projection["notification_status"] == "pending"

    retried = process_trade_intent(
        storage,
        intent,
        now=NOW + timedelta(seconds=1),
        action_now=NOW + timedelta(seconds=1),
        settings=settings,
        feature_policy=policy,
        expected_policy_version=str(intent["policy_version"]),
    )

    assert retried["accepted"] is True
    assert retried["inserted"] is True
    assert json.loads(state_path.read_text())["inflight"] == {}


def test_live_lease_owner_freezes_lifecycle_payload_contract(tmp_path, monkeypatch) -> None:
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

    class SimulatedOwnerCrash(BaseException):
        pass

    revalidations = 0

    def crash_owner(*_args, **_kwargs):
        nonlocal revalidations
        revalidations += 1
        raise SimulatedOwnerCrash

    monkeypatch.setattr(trade_intent_runtime, "_action_revalidation", crash_owner)
    storage = SimpleNamespace(data_root=str(tmp_path))
    settings = _outbox_notification_settings(tmp_path)
    state_path = tmp_path / "latest" / "trade_intent_delivery_state.json"

    with pytest.raises(SimulatedOwnerCrash):
        process_trade_intent(
            storage,
            intent,
            now=NOW,
            action_now=NOW,
            settings=settings,
            feature_policy=policy,
            expected_policy_version=str(intent["policy_version"]),
        )

    owner_state = json.loads(state_path.read_text())
    event_id = _trade_ready_delivery_event_id(intent)
    owner_lifecycle = next(
        item for item in owner_state["delivery_lifecycle_events"] if item["event_id"] == event_id
    )
    mutated_intent = {**intent, "target_spx": float(intent["target_spx"]) + 5.0}
    assert render_trade_intent(mutated_intent) != render_trade_intent(intent)

    contender = process_trade_intent(
        storage,
        mutated_intent,
        now=NOW + timedelta(seconds=1),
        action_now=NOW + timedelta(seconds=1),
        settings=settings,
        feature_policy=policy,
        expected_policy_version=str(intent["policy_version"]),
    )

    assert contender["reason"] == "delivery_in_progress"
    contender_state = json.loads(state_path.read_text())
    contender_lifecycle = next(
        item
        for item in contender_state["delivery_lifecycle_events"]
        if item["event_id"] == event_id
    )
    assert contender_lifecycle["payload_fingerprint"] == owner_lifecycle["payload_fingerprint"]
    assert revalidations == 1


@pytest.mark.parametrize(
    ("race", "expected_reason"),
    (
        ("outbox_cancellation", "outbox_ack_cancelled"),
        ("lifecycle_terminal", "lifecycle_terminal_before_delivery_ack"),
    ),
)
def test_cancellation_or_terminal_after_enqueue_never_promotes_trade_ready(
    tmp_path,
    monkeypatch,
    race,
    expected_reason,
) -> None:
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
    monkeypatch.setattr(
        trade_intent_runtime,
        "_action_revalidation",
        lambda *_args, **_kwargs: (None, {"quote_revalidation": "test_stub"}),
    )
    storage = SimpleNamespace(data_root=str(tmp_path))
    settings = _outbox_notification_settings(tmp_path)
    real_enqueue = trade_intent_runtime.enqueue_notification

    def enqueue_then_invalidate(settings_arg, envelope, **kwargs):
        enqueued = real_enqueue(settings_arg, envelope, **kwargs)
        if race == "outbox_cancellation":
            trade_intent_runtime.cancel_pending_notification(
                settings_arg,
                envelope.event_id,
                now=kwargs["enqueued_at"],
                reason="concurrent_source_invalidation",
            )
        else:
            process_trade_intent(
                storage,
                {
                    "status": "observing",
                    "phase": "invalidated",
                    "event_id": intent["event_id"],
                    "semantic_scope": intent["semantic_scope"],
                },
                now=kwargs["enqueued_at"],
                settings=settings_arg,
            )
        return enqueued

    monkeypatch.setattr(trade_intent_runtime, "enqueue_notification", enqueue_then_invalidate)
    result = process_trade_intent(
        storage,
        intent,
        now=NOW,
        action_now=NOW,
        settings=settings,
        feature_policy=policy,
        expected_policy_version=str(intent["policy_version"]),
    )

    assert result["accepted"] is False
    assert result["reason"] == expected_reason
    state = json.loads((tmp_path / "latest" / "trade_intent_delivery_state.json").read_text())
    event_id = _trade_ready_delivery_event_id(intent)
    assert event_id not in state.get("accepted", {})
    projection = json.loads((tmp_path / "latest" / "trade_intent.json").read_text())
    assert projection["status"] == "delivery_blocked"
    assert projection["notification_status"] == "blocked"
    audit_path = (
        tmp_path / "features" / "trade_intents" / f"date={NOW.date().isoformat()}" / "events.jsonl"
    )
    assert all(
        json.loads(row)["status"] != "trade_ready" for row in audit_path.read_text().splitlines()
    )
    with sqlite3.connect(settings.delivery_outbox_path) as connection:
        outbox_status = connection.execute(
            "SELECT status FROM notification_delivery_events WHERE event_id = ?",
            (event_id,),
        ).fetchone()
    assert outbox_status == ("dead_letter",)


def test_stale_pending_projection_cannot_overwrite_durable_acceptance(tmp_path) -> None:
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
    storage = SimpleNamespace(data_root=str(tmp_path))
    state_path = tmp_path / "latest" / "trade_intent_delivery_state.json"
    latest_path = tmp_path / "latest" / "trade_intent.json"
    state_path.parent.mkdir(parents=True)
    event_id = _trade_ready_delivery_event_id(intent)
    accepted_projection = trade_intent_delivery_contract.delivery_projection(
        intent,
        delivery_event_id=event_id,
        notification_status="outbox_accepted",
        reason="outbox_enqueue_accepted",
    )
    state_path.write_text(
        json.dumps(
            {
                "accepted": {event_id: NOW.isoformat()},
                "last_status": "trade_ready",
                "last_signature": "accepted-signature",
            }
        )
    )
    latest_path.write_text(json.dumps(accepted_projection))

    persisted = trade_intent_delivery_contract.persist_delivery_projection(
        storage,
        intent,
        now=NOW + timedelta(seconds=1),
        delivery_event_id=event_id,
        reason="delivery_in_progress",
        preserve_accepted=True,
    )

    assert persisted is False
    assert json.loads(latest_path.read_text())["status"] == "trade_ready"
    assert json.loads(state_path.read_text())["last_status"] == "trade_ready"


def test_local_acceptance_without_outbox_reconciliation_is_hard_fault(
    tmp_path,
) -> None:
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
    delivery_event_id = _trade_ready_delivery_event_id(intent)
    state_path = tmp_path / "latest" / "trade_intent_delivery_state.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text(
        json.dumps(
            {
                "accepted": {delivery_event_id: NOW.isoformat()},
                "semantic_keys": {
                    delivery_event_id: str(intent["semantic_key"]),
                },
                "inflight": {},
            }
        ),
        encoding="utf-8",
    )

    result = process_trade_intent(
        SimpleNamespace(data_root=str(tmp_path)),
        intent,
        now=NOW + timedelta(seconds=1),
        settings=_notification_settings(tmp_path),
        feature_policy=policy,
        expected_policy_version=str(intent["policy_version"]),
    )

    assert result == {
        "attempted": False,
        "delivered": False,
        "accepted": False,
        "reason": "accepted_outbox_reconciliation_unavailable",
    }
    state = json.loads(state_path.read_text())
    assert state["last_delivery_reconciliation"]["action"] == "hard_fault"


def test_enqueue_exception_releases_lease_and_next_tick_retries(
    tmp_path,
    monkeypatch,
) -> None:
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
    monkeypatch.setattr(
        trade_intent_runtime,
        "_action_revalidation",
        lambda *_args, **_kwargs: (
            None,
            {"quote_revalidation": "test_stub"},
        ),
    )
    real_enqueue = trade_intent_runtime.enqueue_notification
    enqueue_calls = 0

    def flaky_enqueue(*args, **kwargs):
        nonlocal enqueue_calls
        enqueue_calls += 1
        if enqueue_calls == 1:
            raise RuntimeError("simulated enqueue outage")
        return real_enqueue(*args, **kwargs)

    monkeypatch.setattr(
        trade_intent_runtime,
        "enqueue_notification",
        flaky_enqueue,
    )
    storage = SimpleNamespace(data_root=str(tmp_path))
    settings = _outbox_notification_settings(tmp_path)
    state_path = tmp_path / "latest" / "trade_intent_delivery_state.json"

    failed = process_trade_intent(
        storage,
        intent,
        now=NOW,
        action_now=NOW,
        settings=settings,
        feature_policy=policy,
        expected_policy_version=str(intent["policy_version"]),
    )

    assert failed["accepted"] is False
    assert failed["reason"] == "notification_enqueue_exception"
    assert failed["enqueue_exception_type"] == "RuntimeError"
    failed_state = json.loads(state_path.read_text())
    assert failed_state["inflight"] == {}
    assert failed_state["last_action_revalidation"]["reason"] == "notification_enqueue_exception"
    assert failed_state["last_action_revalidation"]["enqueue_exception_type"] == "RuntimeError"
    failed_projection = json.loads((tmp_path / "latest" / "trade_intent.json").read_text())
    assert failed_projection["status"] == "ready_pending_delivery"
    assert failed_projection["notification_status"] == "pending"

    retried = process_trade_intent(
        storage,
        intent,
        now=NOW + timedelta(seconds=1),
        action_now=NOW + timedelta(seconds=1),
        settings=settings,
        feature_policy=policy,
        expected_policy_version=str(intent["policy_version"]),
    )

    assert retried["accepted"] is True
    assert retried["inserted"] is True
    assert enqueue_calls == 2
    with sqlite3.connect(settings.delivery_outbox_path) as connection:
        row_count = connection.execute(
            "SELECT COUNT(*) FROM notification_delivery_events"
        ).fetchone()[0]
    assert row_count == 1


def test_orphaned_inflight_lease_expires_before_signal_ttl(
    tmp_path,
    monkeypatch,
) -> None:
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

    class SimulatedProcessCrash(BaseException):
        pass

    revalidation_calls = 0

    def crash_once(*_args, **_kwargs):
        nonlocal revalidation_calls
        revalidation_calls += 1
        if revalidation_calls == 1:
            raise SimulatedProcessCrash
        return None, {"quote_revalidation": "test_stub"}

    monkeypatch.setattr(
        trade_intent_runtime,
        "_action_revalidation",
        crash_once,
    )
    storage = SimpleNamespace(data_root=str(tmp_path))
    settings = _outbox_notification_settings(tmp_path)
    state_path = tmp_path / "latest" / "trade_intent_delivery_state.json"
    delivery_event_id = _trade_ready_delivery_event_id(intent)

    with pytest.raises(SimulatedProcessCrash):
        process_trade_intent(
            storage,
            intent,
            now=NOW,
            action_now=NOW,
            settings=settings,
            feature_policy=policy,
            expected_policy_version=str(intent["policy_version"]),
        )

    crashed_state = json.loads(state_path.read_text())
    lease = crashed_state["inflight"][delivery_event_id]
    assert datetime.fromisoformat(lease["expires_at"]) == NOW + timedelta(seconds=5)
    assert datetime.fromisoformat(lease["expires_at"]) < datetime.fromisoformat(
        str(intent["valid_until"])
    )

    still_leased = process_trade_intent(
        storage,
        intent,
        now=NOW + timedelta(seconds=4),
        action_now=NOW + timedelta(seconds=4),
        settings=settings,
        feature_policy=policy,
        expected_policy_version=str(intent["policy_version"]),
    )
    recovered = process_trade_intent(
        storage,
        intent,
        now=NOW + timedelta(seconds=6),
        action_now=NOW + timedelta(seconds=6),
        settings=settings,
        feature_policy=policy,
        expected_policy_version=str(intent["policy_version"]),
    )

    assert still_leased["reason"] == "delivery_in_progress"
    assert recovered["accepted"] is True
    assert recovered["inserted"] is True
    assert revalidation_calls == 2


@pytest.mark.parametrize(
    "missing_field",
    (
        "decision_bid",
        "decision_ask",
        "trigger_level",
        "spx_spot",
        "invalidation_spx",
        "target_spx",
        "time_stop_at",
        "max_loss_per_contract",
    ),
)
def test_action_revalidation_rejects_incomplete_manual_card(
    missing_field: str,
) -> None:
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
    incomplete = {key: value for key, value in intent.items() if key != missing_field}

    reason, evidence = _action_revalidation(
        SimpleNamespace(),
        incomplete,
        now=NOW,
        feature_policy=policy,
        expected_policy_version=str(intent["policy_version"]),
    )

    expected = (
        "manual_card_time_stop_unavailable"
        if missing_field == "time_stop_at"
        else f"manual_card_field_missing:{missing_field}"
    )
    assert reason == expected
    assert evidence["reason"] == expected
    assert "quote_revalidation" not in evidence


def test_action_revalidation_rejects_spread_deterioration_even_if_limit_is_unchanged(
    monkeypatch,
) -> None:
    market, options, latest, context, repricing = _ready_inputs()
    feature_policy = MarketFeatureSettings()
    order_policy = OrderMapPolicy()
    intent = evaluate_trade_intent(
        context,
        market,
        options,
        latest,
        repricing,
        now=NOW,
        feature_policy=feature_policy,
        order_policy=order_policy,
    )
    action_at = NOW + timedelta(seconds=1)
    wide_quote = replace(
        latest.best_quotes[0],
        received_at=action_at,
        last_update_at=action_at,
        quote_time=action_at,
        bid=9.05,
        ask=12.15,
    )
    wide_latest = LatestState(
        created_at=action_at,
        as_of=action_at,
        quotes=(wide_quote,),
        best_quotes=(wide_quote,),
    )

    class Store:
        def __init__(self, _storage) -> None:
            pass

        def load(self, *, now):
            assert now == action_at
            return wide_latest

    monkeypatch.setattr(
        "spx_spark.application.market_features.trade_intent_runtime.LatestStateStore",
        Store,
    )
    reason, evidence = _action_revalidation(
        SimpleNamespace(),
        intent,
        now=action_at,
        feature_policy=feature_policy,
        order_policy=order_policy,
        expected_policy_version=str(intent["policy_version"]),
    )

    assert reason == "action_execution_quote_spread_points_exceeded"
    assert evidence["entry_limit"] == pytest.approx(10.4)
    assert evidence["execution_quote_gate"]["spread_points"] == pytest.approx(3.1)


def test_unexecutable_action_quote_is_blocked_before_enqueue(
    tmp_path,
    monkeypatch,
) -> None:
    market, options, latest, context, repricing = _ready_inputs()
    feature_policy = MarketFeatureSettings()
    order_policy = OrderMapPolicy()
    intent = evaluate_trade_intent(
        context,
        market,
        options,
        latest,
        repricing,
        now=NOW,
        feature_policy=feature_policy,
        order_policy=order_policy,
    )
    action_at = NOW + timedelta(seconds=1)
    wide_quote = replace(
        latest.best_quotes[0],
        received_at=action_at,
        last_update_at=action_at,
        quote_time=action_at,
        bid=9.05,
        ask=12.15,
    )
    wide_latest = LatestState(
        created_at=action_at,
        as_of=action_at,
        quotes=(wide_quote,),
        best_quotes=(wide_quote,),
    )

    class Store:
        def __init__(self, _storage) -> None:
            pass

        def load(self, *, now):
            assert now == action_at
            return wide_latest

    monkeypatch.setattr(
        "spx_spark.application.market_features.trade_intent_runtime.LatestStateStore",
        Store,
    )
    monkeypatch.setattr(
        "spx_spark.application.market_features.trade_intent_runtime.enqueue_notification",
        lambda *_args, **_kwargs: pytest.fail("unexecutable action quote must not enqueue"),
    )

    result = process_trade_intent(
        SimpleNamespace(data_root=str(tmp_path)),
        intent,
        now=NOW,
        action_now=action_at,
        settings=_notification_settings(tmp_path),
        feature_policy=feature_policy,
        order_policy=order_policy,
        expected_policy_version=str(intent["policy_version"]),
    )

    assert result["reason"] == "action_execution_quote_spread_points_exceeded"
    state = json.loads((tmp_path / "latest" / "trade_intent_delivery_state.json").read_text())
    assert state["last_action_revalidation"]["execution_quote_gate"][
        "spread_points"
    ] == pytest.approx(3.1)
    assert state["inflight"] == {}


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


def test_explicit_reentry_generation_rearms_after_invalidation(tmp_path, monkeypatch) -> None:
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
        {**intent, "event_id": "level:second", "reentry_generation": 1},
        now=NOW + timedelta(minutes=2),
        settings=settings,
    )

    assert first["accepted"] is True
    assert invalidated["reason"] == "observing"
    assert rearmed["accepted"] is True
    assert len(calls) == 2


def test_expiry_cancels_occurrence_without_rearming_same_opportunity(
    tmp_path,
    monkeypatch,
) -> None:
    semantic_scope = "2026-07-14|level_breakout_call|7550.0000"
    intent = {
        **_runtime_contract(NOW + timedelta(minutes=5)),
        "status": "trade_ready",
        "intent_id": "intent:expiry-rearm",
        "semantic_scope": semantic_scope,
        "semantic_key": (f"{semantic_scope}|option:SPX:SPXW:20260714:7550:C"),
        "event_id": "level:expiry-rearm:first",
        "evaluated_at": NOW.isoformat(),
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
        "invalidation_spx": 7547.0,
        "target_spx": 7575.0,
        "max_loss_per_contract": 1010.0,
        "expires_at": (NOW + timedelta(minutes=5)).isoformat(),
        "time_stop_at": (NOW + timedelta(minutes=15)).isoformat(),
    }
    monkeypatch.setattr(
        trade_intent_runtime,
        "_action_revalidation",
        lambda *_args, **_kwargs: (
            None,
            {"quote_revalidation": "test_stub"},
        ),
    )
    storage = SimpleNamespace(data_root=str(tmp_path))
    settings = _outbox_notification_settings(tmp_path)

    first = process_trade_intent(
        storage,
        intent,
        now=NOW,
        action_now=NOW,
        settings=settings,
    )
    terminal = {
        "status": "observing",
        "phase": "expired",
        "event_id": intent["event_id"],
        "semantic_scope": semantic_scope,
    }
    expired = process_trade_intent(
        storage,
        terminal,
        now=NOW + timedelta(seconds=1),
        settings=settings,
    )
    expired_replay = process_trade_intent(
        storage,
        terminal,
        now=NOW + timedelta(seconds=2),
        settings=settings,
    )
    same_event_replay = process_trade_intent(
        storage,
        intent,
        now=NOW + timedelta(seconds=3),
        action_now=NOW + timedelta(seconds=3),
        settings=settings,
    )
    rearmed_intent = {
        **intent,
        "event_id": "level:expiry-rearm:second",
        "evaluated_at": (NOW + timedelta(seconds=4)).isoformat(),
        "quote_source_at": (NOW + timedelta(seconds=4)).isoformat(),
    }
    rearmed = process_trade_intent(
        storage,
        rearmed_intent,
        now=NOW + timedelta(seconds=4),
        action_now=NOW + timedelta(seconds=4),
        settings=settings,
    )

    first_delivery_id = _trade_ready_delivery_event_id(intent)
    second_delivery_id = _trade_ready_delivery_event_id(rearmed_intent)
    assert first["accepted"] is True
    assert expired["reason"] == "observing"
    assert expired_replay["reason"] == "observing"
    assert same_event_replay["reason"] == "already_accepted"
    assert rearmed["reason"] == "already_accepted"
    assert first_delivery_id != second_delivery_id

    state = json.loads((tmp_path / "latest" / "trade_intent_delivery_state.json").read_text())
    assert state["semantic_keys"] == {
        first_delivery_id: intent["semantic_key"],
    }
    assert state["terminal_delivery_event_ids"] == [first_delivery_id]
    assert state["pending_delivery_cancellation_event_ids"] == []
    assert state["pending_delivery_cancellation_reasons"] == {}
    assert state["delivery_lifecycle_events"] == []

    with sqlite3.connect(settings.delivery_outbox_path) as connection:
        statuses = dict(
            connection.execute(
                "SELECT event_id, status FROM notification_delivery_events"
            ).fetchall()
        )
        cancellations = connection.execute(
            "SELECT event_id, reason FROM notification_delivery_cancellations"
        ).fetchall()
    assert statuses == {
        first_delivery_id: "dead_letter",
    }
    assert cancellations == [
        (first_delivery_id, "trade_intent_lifecycle_expired"),
    ]

    with sqlite3.connect(settings.delivery_receipt_path) as connection:
        terminal_receipts = connection.execute(
            "SELECT event_id, outcome FROM notification_delivery_receipts"
        ).fetchall()
    assert terminal_receipts == [
        (first_delivery_id, "cancelled_before_delivery"),
    ]


def test_rearmed_semantic_intent_uses_distinct_durable_delivery_event(
    tmp_path,
    monkeypatch,
) -> None:
    intent = {
        **_runtime_contract(NOW + timedelta(minutes=5)),
        "status": "trade_ready",
        "intent_id": "intent:semantic-real-outbox",
        "semantic_scope": "2026-07-14|level_breakout_call|7550.0000",
        "semantic_key": (
            "2026-07-14|level_breakout_call|7550.0000|option:SPX:SPXW:20260714:7550:C"
        ),
        "event_id": "level:first",
        "evaluated_at": NOW.isoformat(),
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
        delivery_outbox_legacy_shadow_enabled=False,
    )
    monkeypatch.setattr(
        "spx_spark.application.market_features.trade_intent_runtime._action_revalidation",
        lambda *_args, **_kwargs: (None, {"quote_revalidation": "test_stub"}),
    )
    storage = SimpleNamespace(data_root=str(tmp_path))

    first = process_trade_intent(
        storage,
        intent,
        now=NOW,
        action_now=NOW,
        settings=settings,
    )
    process_trade_intent(
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
    rearmed_intent = {
        **intent,
        "reentry_generation": 1,
        "event_id": "level:second",
        "evaluated_at": (NOW + timedelta(minutes=2)).isoformat(),
        "quote_source_at": (NOW + timedelta(minutes=2)).isoformat(),
        "decision_bid": 10.1,
        "decision_ask": 10.5,
        "entry_limit": 10.2,
        "max_loss_per_contract": 1020.0,
    }
    rearmed = process_trade_intent(
        storage,
        rearmed_intent,
        now=NOW + timedelta(minutes=2),
        action_now=NOW + timedelta(minutes=2),
        settings=settings,
    )

    first_delivery_id = _trade_ready_delivery_event_id(intent)
    second_delivery_id = _trade_ready_delivery_event_id(rearmed_intent)
    assert first["accepted"] is True
    assert rearmed["accepted"] is True
    assert first_delivery_id != second_delivery_id
    with sqlite3.connect(settings.delivery_outbox_path) as connection:
        rows = connection.execute(
            "SELECT event_id, status, text FROM notification_delivery_events ORDER BY event_id"
        ).fetchall()
    assert {row[0] for row in rows} == {first_delivery_id, second_delivery_id}
    statuses = {row[0]: row[1] for row in rows}
    assert statuses[first_delivery_id] == "dead_letter"
    assert statuses[second_delivery_id] == "pending"
    assert len({row[2] for row in rows}) == 2


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
        macro_event={"mode": "normal", "entry_allowed": True},
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


def _retimed_inputs(at: datetime):
    market, options, latest, context, repricing = _ready_inputs()
    quote = replace(
        latest.best_quotes[0],
        received_at=at,
        last_update_at=at,
        quote_time=at,
    )
    latest = replace(
        latest,
        created_at=at,
        as_of=at,
        quotes=(quote,),
        best_quotes=(quote,),
    )
    market = replace(
        market,
        as_of=at,
        es={
            **market.es,
            "observed_at": at.isoformat(),
            "source_at": at.isoformat(),
            "transport_at": at.isoformat(),
        },
    )
    options = replace(options, as_of=at)
    context = replace(
        context,
        as_of=at,
        level_decision={
            **context.level_decision,
            "phase_at": (at - timedelta(seconds=60)).isoformat(),
            "expires_at": (at + timedelta(minutes=3)).isoformat(),
            "updated_at": at.isoformat(),
            "trigger_coordinate": {
                **context.level_decision["trigger_coordinate"],
                "as_of": at.isoformat(),
            },
        },
    )
    repricing = {**repricing, "as_of": at.isoformat()}
    return market, options, latest, context, repricing


def _put_shadow_inputs(
    *,
    thesis: str,
    level_kind: str,
    level: float,
):
    market, options, latest, context, _repricing = _ready_inputs()
    spot = level - 4.0
    instrument = InstrumentId.option(
        "SPX",
        expiry="20260714",
        strike=int(level),
        right="P",
        trading_class="SPXW",
    )
    quote = replace(latest.best_quotes[0], instrument=instrument)
    latest = replace(latest, quotes=(quote,), best_quotes=(quote,))
    market = replace(
        market,
        es={
            **market.es,
            "return_1m_points": -1.0,
            "return_5m_points": -4.0,
            "return_15m_points": -8.0,
        },
    )
    play = "level_breakout_put" if thesis == "breakout" else "level_fade_put"
    context = replace(
        context,
        trend={"regime": "bearish"},
        level_decision={
            **context.level_decision,
            "thesis": thesis,
            "direction": "down",
            "level_kind": level_kind,
            "level": level,
            "spot": spot,
            "trigger_coordinate": {
                **context.level_decision["trigger_coordinate"],
                "observed_value": spot,
                "target_value": level,
                "spx_observed_value": spot,
            },
        },
        regime_decision={
            "mode": "trending" if thesis == "breakout" else "mean_reverting",
            "direction": "down",
            "trend_score": -80.0,
        },
    )
    repricing = {
        "event_id": "level:test",
        "as_of": NOW.isoformat(),
        "expiry": "20260714",
        "candidates": [
            {
                "play": play,
                "contract_id": instrument.canonical_id,
                "strike": int(level),
                "right": "P",
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
        "contract_id": "option:SPX:SPXW:20260715:7550:P",
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
        "status": "trade_ready",
        "strategy_lane": "long_0dte_rth_upside_breakout_pilot",
        "shadow_mode": False,
        "execution_eligible": True,
        "quote_observation_eligible": False,
        "automatic_ordering": False,
        "direction": "up",
        "contract_id": "option:SPX:SPXW:20260714:7550:C",
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


def _outbox_notification_settings(tmp_path) -> NotificationSettings:
    return replace(
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
        delivery_outbox_legacy_shadow_enabled=False,
    )
