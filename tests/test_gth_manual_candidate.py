from __future__ import annotations

import hashlib
import inspect
import json
import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

import spx_spark.application.market_features.gth_manual_candidate as candidate_module
import spx_spark.application.market_features.gth_level_manual_candidate as level_candidate_module
import spx_spark.application.market_features.service as market_feature_service
import spx_spark.application.order_map.service as order_map_service
from spx_spark.application.market_features.gth_candidate_lifecycle import (
    cancellation_scope,
    seed_replayed_candidate_ids,
)
from spx_spark.application.market_features.gth_manual_candidate import (
    _direct_es_reference,
    _notification_intent,
    evaluate_gth_manual_candidate,
    process_gth_manual_candidate,
)
from spx_spark.application.market_features.gth_level_manual_candidate import (
    _apply_active_plan_coherence,
    evaluate_gth_level_manual_candidate,
    process_gth_level_manual_candidate,
)
from spx_spark.application.market_features.play_outcome_stats import PlayOutcomeStats
from spx_spark.application.market_features.trade_intent import (
    live_trade_intent_authority_issues,
)
from spx_spark.application.market_features.virtual_strategy_state import (
    flush_pending_notifications,
)
from spx_spark.application.market_features.virtual_strategy_support import (
    _contract_snapshot,
)
from spx_spark.config import NotificationSettings
from spx_spark.data_platform.research.odte_level_gth_candidates import (
    load_gth_level_candidate_signals,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    Provider,
    Quote,
)
from spx_spark.options_map import actionable_chain_implied_reference
from spx_spark.notifier.dispatcher import (
    consume_pending_notifications,
    enqueue_notification,
)
from spx_spark.notifier.model import SinkResult
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.storage import LatestState
from spx_spark.strategy_contract import policy_version


UTC = timezone.utc
NOW = datetime(2026, 7, 15, 3, 0, tzinfo=UTC)


def test_runtime_only_consumes_unified_gth_manual_candidate() -> None:
    market_source = inspect.getsource(market_feature_service.run)
    order_map_source = inspect.getsource(order_map_service.build_order_payload_with_retry)

    assert "process_gth_manual_candidate(" not in market_source
    assert "process_gth_level_manual_candidate(" in market_source
    assert '"gth_manual_candidate": gth_manual_candidate' not in market_source
    assert '"gth_manual_candidate"' not in order_map_source
    assert '"gth_level_manual_candidate"' in order_map_source


def test_manual_candidate_is_ready_without_cash_spx(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW)

    candidate = evaluate_gth_manual_candidate(
        object(),
        _signal(NOW),
        macro_event={"mode": "normal", "entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    assert candidate["status"] == "manual_ready"
    assert candidate["kind"] == "gth_spxw_manual_spread_candidate"
    assert candidate["candidate_scope"] == "manual_live"
    assert candidate["manual_action_eligible"] is True
    assert candidate["execution_eligible"] is False
    assert candidate["automatic_ordering"] is False
    assert candidate["broker_submission_allowed"] is False
    assert candidate["rth_trade_ready_authority"] is False
    assert candidate["simulation_only"] is False
    assert candidate["order_type"] == "NET_DEBIT_LIMIT"
    assert candidate["quote_basis"] == "synthetic_from_leg_nbbo"
    assert candidate["entry_limit"] == 11.0
    assert candidate["max_loss_per_spread"] == 1100.0
    assert candidate["reward_risk_at_limit"] == pytest.approx(2.6364)
    assert candidate["target_coordinate"]["kind"] == "chain_implied_spx"
    assert candidate["current_parity_spx"] == 7530.0
    assert candidate["invalidation_coordinate"]["kind"] == "raw_es"
    assert candidate["block_reasons"] == []
    assert "trade_intent_not_trade_ready" in live_trade_intent_authority_issues(candidate)
    assert "trade_intent_execution_authority_missing" in live_trade_intent_authority_issues(
        candidate
    )


def test_gth_signal_candidate_keeps_five_minute_opportunity_near_quote_age_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    quote_at = NOW - timedelta(seconds=14.9)
    _patch_ready_market(monkeypatch, now=quote_at)

    candidate = evaluate_gth_manual_candidate(
        object(),
        _signal(NOW),
        macro_event={"mode": "normal", "entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    assert candidate["status"] == "manual_ready"
    assert candidate["valid_until"] == (NOW + timedelta(minutes=5)).isoformat()


def test_confirmed_gth_flip_low_breakdown_builds_put_manual_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)

    candidate = evaluate_gth_level_manual_candidate(
        object(),
        _level_signal(NOW, direction="down", level_kind="flip_low", level=7375.0),
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
        prior_session={
            "status": "ready",
            "session_date": "2026-07-14",
            "return_fraction": -0.0152,
            "return_points": -112.63,
            "close_location_fraction": 0.02,
            "tail_return_fraction": -0.004,
            "shock_direction": "down",
            "close_zone": "lower",
            "path_class": "shock_down_close_low",
        },
        gth_position_fraction=0.25,
    )

    assert candidate["status"] == "manual_ready"
    assert candidate["path_kind"] == "flip_low_breakdown_put"
    assert candidate["position_type"] == "put_debit_spread"
    assert candidate["long_contract_id"].endswith(":7375:P")
    assert candidate["short_contract_id"].endswith(":7335:P")
    assert candidate["decision_bid"] == 10.0
    assert candidate["decision_ask"] == 12.0
    assert candidate["entry_limit"] == 12.0
    assert candidate["entry_rule"] == "manual_debit_limit_at_or_below_decision_ask"
    assert candidate["invalidation_spx"] == 7383.0
    assert candidate["invalidation_es"] == 7413.0
    assert candidate["target_spx"] == 7300.0
    assert candidate["automatic_ordering"] is False
    assert candidate["execution_eligible"] is False
    assert candidate["broker_submission_allowed"] is False
    assert candidate["prior_session"]["chase_risk"] == "elevated"
    assert "prior_session_same_direction_chase_risk_elevated" in candidate["ranking_diagnostics"]
    assert candidate["block_reasons"] == []
    card = _notification_intent(candidate, event_id="put-ready", now=NOW)
    assert "🟢 MANUAL READY · PUT SPREAD" in card["text"]
    assert "买入  SPXW 07-15 7375P" in card["text"]
    assert "卖出  SPXW 07-15 7335P" in card["text"]
    assert "NBBO  10.00 / 12.00" in card["text"]
    assert "限价  净借记 ≤ 12.00" in card["text"]
    assert "触发  SPX 跌破 Flip Low 7375.00 并确认" in card["text"]
    assert "前日  -1.52%" in card["text"]
    assert "本票同向追单风险偏高" in card["text"]
    assert "止损  SPX 收回 7383.00；ES 升至 7413.00" in card["text"]
    assert "目标  SPX 7300.00（Put Wall）" in card["text"]
    assert "到期最大赔付比" in card["text"]
    assert "非胜率或期望收益" in card["text"]
    assert "赔率  最大收益/最大亏损" not in card["text"]
    assert "退出  " in card["text"]
    assert "有效  剩余 " in card["text"]
    assert "自动下单关闭" in card["text"]
    assert card["lane"] == "gth_level_manual_candidate"


def test_gth_level_candidate_keeps_five_minute_opportunity_near_quote_age_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    quote_at = NOW - timedelta(seconds=14.9)
    _patch_ready_market(
        monkeypatch,
        now=quote_at,
        parity_price=7368.0,
        es_price=7398.0,
    )

    candidate = evaluate_gth_level_manual_candidate(
        object(),
        _level_signal(NOW, direction="down", level_kind="flip_low", level=7375.0),
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    assert candidate["status"] == "manual_ready"
    assert candidate["valid_until"] == (NOW + timedelta(minutes=5)).isoformat()


def test_prior_down_shock_blocks_floor_chasing_put(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)

    candidate = evaluate_gth_level_manual_candidate(
        object(),
        _level_signal(NOW, direction="down", level_kind="flip_low", level=7375.0),
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
        prior_session={
            "status": "ready",
            "session_date": "2026-07-14",
            "return_fraction": -0.0152,
            "return_points": -112.63,
            "close_location_fraction": 0.02,
            "tail_return_fraction": -0.004,
            "shock_direction": "down",
            "close_zone": "lower",
            "path_class": "shock_down_close_low",
        },
        gth_position_fraction=0.02,
    )

    assert candidate["status"] == "blocked"
    assert "prior_session_same_direction_chase_risk_high" in candidate["block_reasons"]


def test_confirmed_gth_breakout_cannot_fight_established_es_regime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)

    candidate = evaluate_gth_level_manual_candidate(
        object(),
        _level_signal(NOW, direction="down", level_kind="flip_low", level=7375.0),
        trend_state={"regime": "bullish"},
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    assert candidate["status"] == "blocked"
    assert candidate["trend_regime"] == "bullish"
    assert "gth_trend_regime_opposes_breakout" in candidate["block_reasons"]


def test_confirmed_gth_breakout_without_crossing_or_retest_evidence_is_blocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW, parity_price=7384.3, es_price=7411.5)
    level_signal = _level_signal(
        NOW,
        direction="up",
        level_kind="flip_high",
        level=7380.0,
    )
    level_signal.pop("breakout_inside_seen_at")
    level_signal.pop("breakout_retest_seen_at")

    candidate = evaluate_gth_level_manual_candidate(
        object(),
        level_signal,
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    assert candidate["status"] == "blocked"
    assert "breakout_inside_crossing_evidence_missing" in candidate["block_reasons"]
    assert "breakout_retest_evidence_missing" in candidate["block_reasons"]


def test_confirmed_gth_level_blocks_sufficiently_sampled_negative_touch_quote_outcomes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)
    stats = PlayOutcomeStats(
        play="level_breakout_put",
        level_kind="flip_low",
        sample_count=40,
        winrate=0.4,
        avg_return=-0.02,
        median_return=-0.01,
        window_days=20,
        horizon="300",
        as_of=NOW.isoformat(),
    )

    candidate = evaluate_gth_level_manual_candidate(
        object(),
        _level_signal(NOW, direction="down", level_kind="flip_low", level=7375.0),
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
        play_stats=stats,
    )

    assert candidate["status"] == "blocked"
    assert candidate["historical_edge_authority"] == "negative_safety_veto_only"
    assert candidate["play_stats"]["semantics"] == ("matched_touch_quote_outcomes_not_live_fills")
    assert "historical_winrate_below_floor" in candidate["historical_edge_diagnostics"]
    assert "historical_average_return_non_positive" in candidate["historical_edge_diagnostics"]
    assert "historical_median_return_non_positive" in candidate["historical_edge_diagnostics"]
    assert "historical_average_return_non_positive" in candidate["block_reasons"]
    assert "historical_median_return_non_positive" in candidate["block_reasons"]
    assert "historical_winrate_below_floor" not in candidate["block_reasons"]


def test_confirmed_gth_level_does_not_veto_negative_history_below_sample_floor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)
    stats = PlayOutcomeStats(
        play="level_breakout_put",
        level_kind="flip_low",
        sample_count=29,
        winrate=0.4,
        avg_return=-0.02,
        median_return=-0.01,
        window_days=20,
        horizon="300",
        as_of=NOW.isoformat(),
    )

    candidate = evaluate_gth_level_manual_candidate(
        object(),
        _level_signal(NOW, direction="down", level_kind="flip_low", level=7375.0),
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
        play_stats=stats,
    )

    assert candidate["status"] == "manual_ready"
    assert candidate["historical_edge_diagnostics"]
    assert candidate["block_reasons"] == []


def test_gth_ready_card_labels_quote_outcomes_as_not_live_winrate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)
    stats = PlayOutcomeStats(
        play="level_breakout_put",
        level_kind="flip_low",
        sample_count=40,
        winrate=0.55,
        avg_return=0.03,
        median_return=0.01,
        window_days=20,
        horizon="300",
        as_of=NOW.isoformat(),
    )

    candidate = evaluate_gth_level_manual_candidate(
        object(),
        _level_signal(NOW, direction="down", level_kind="flip_low", level=7375.0),
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
        play_stats=stats,
    )
    card = _notification_intent(candidate, event_id="put-ready", now=NOW)

    assert candidate["status"] == "manual_ready"
    assert "历史  同类触位报价 40笔" in card["text"]
    assert "5分钟正收益率 55.0%" in card["text"]
    assert "非实盘胜率" in card["text"]


def test_rearmed_same_gamma_path_keeps_one_semantic_candidate_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)
    first_level = _level_signal(
        NOW,
        direction="down",
        level_kind="flip_low",
        level=7375.0,
    )
    rearmed_level = {
        **first_level,
        "event_id": "level:rearmed:same-flip-low",
        "reentry_generation": 0,
    }

    first = evaluate_gth_level_manual_candidate(
        object(),
        first_level,
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )
    rearmed = evaluate_gth_level_manual_candidate(
        object(),
        rearmed_level,
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    assert first["status"] == rearmed["status"] == "manual_ready"
    assert first["candidate_id"] == rearmed["candidate_id"]
    assert first["source_signal_id"] != rearmed["source_signal_id"]


def test_opposite_gamma_signal_waits_for_prior_plan_invalidation() -> None:
    active_put = {
        "candidate_id": "prior-put",
        "direction": "down",
        "invalidation_spx": 7383.0,
        "target_spx": 7300.0,
        "exit_at": (NOW + timedelta(minutes=15)).isoformat(),
    }
    call_candidate = {
        "candidate_id": "new-call",
        "status": "manual_ready",
        "manual_action_eligible": True,
        "direction": "up",
        "current_parity_spx": 7371.0,
        "block_reasons": [],
        "gate_contract": {"hard_block_reasons": []},
    }

    blocked, still_active = _apply_active_plan_coherence(
        call_candidate,
        active_put,
        now=NOW,
    )
    released, cleared = _apply_active_plan_coherence(
        {**call_candidate, "current_parity_spx": 7384.0},
        active_put,
        now=NOW,
    )

    assert blocked["status"] == "blocked"
    assert blocked["signal_absence_reason"] == "active_manual_plan_not_invalidated"
    assert blocked["block_reasons"] == ["opposite_signal_conflicts_with_active_plan"]
    assert still_active == active_put
    assert released["status"] == "manual_ready"
    assert released["replaces_prior_plan"]["release_reason"] == "prior_put_invalidated"
    assert cleared == {}


@pytest.mark.parametrize("level_kind", ("flip_high", "call_wall"))
def test_confirmed_gth_upper_acceptance_builds_call_manual_ready(
    monkeypatch: pytest.MonkeyPatch,
    level_kind: str,
) -> None:
    trigger = 7380.0 if level_kind == "flip_high" else 7450.0
    parity = 7390.0 if level_kind == "flip_high" else 7458.0
    es = parity + 30.0
    _patch_ready_market(monkeypatch, now=NOW, parity_price=parity, es_price=es)

    candidate = evaluate_gth_level_manual_candidate(
        object(),
        _level_signal(NOW, direction="up", level_kind=level_kind, level=trigger),
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    assert candidate["status"] == "manual_ready"
    assert candidate["path_kind"] == "upper_acceptance_call"
    assert candidate["position_type"] == "call_debit_spread"
    assert candidate["long_contract_id"].endswith(f":{int(trigger)}:C")
    assert candidate["entry_limit"] == 12.0
    assert candidate["automatic_ordering"] is False


def test_expiry_payoff_geometry_without_time_stop_edge_authority_is_watch_only(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(
        monkeypatch,
        now=NOW,
        parity_price=7734.3,
        es_price=7760.62,
        spread_bid=15.20,
        spread_mid=15.40,
        spread_ask=15.60,
        edge_authority=None,
    )
    source = _level_signal(
        NOW,
        direction="up",
        level_kind="call_wall",
        level=7730.0,
    )
    source["levels"] = {
        **dict(source["levels"]),
        "call_wall": 7770.0,
    }
    source["es_basis_points"] = 26.32

    candidate = process_gth_level_manual_candidate(
        SimpleNamespace(data_root=str(tmp_path)),
        object(),
        source,
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    assert candidate["status"] == "structure_watch"
    assert candidate["manual_action_eligible"] is False
    assert candidate["operator_notification_eligible"] is False
    assert candidate["operator_action"] == "observe_only"
    assert candidate["edge_authority"] == "none"
    assert candidate["edge_authority_reason"] == (
        "first_touch_time_stop_net_pnl_authority_unavailable"
    )
    assert candidate["expiry_payoff_ratio_role"] == "diagnostic_only"
    assert candidate["reward_risk_at_limit"] == pytest.approx(1.5641)
    assert candidate["trigger_level"] == 7730.0
    assert candidate["current_parity_spx"] == 7734.3
    assert candidate["decision_bid"] == 15.20
    assert candidate["decision_ask"] == 15.60
    assert candidate["entry_limit"] == pytest.approx(15.60)
    assert candidate["target_spx"] == 7770.0
    assert candidate["exit_at"] == (NOW + timedelta(minutes=15)).isoformat()
    assert candidate["exact_spread_snapshot"]["quality"]["status"] == "ok"
    assert candidate["automatic_ordering"] is False
    assert candidate["broker_submission_allowed"] is False
    with pytest.raises(ValueError, match="manual-ready candidate"):
        _notification_intent(candidate, event_id="must-not-send", now=NOW)

    state = json.loads((tmp_path / "latest" / "gth_level_manual_candidate_state.json").read_text())
    assert state["pending_notifications"] == []
    assert state["accepted_notification_event_ids"] == []
    replay_path = (
        tmp_path
        / "features"
        / "gth_level_manual_candidates"
        / f"date={DEFAULT_MARKET_CALENDAR.research_expiry(NOW).isoformat()}"
        / "events.jsonl"
    )
    replay = [json.loads(row) for row in replay_path.read_text().splitlines()]
    assert len(replay) == 1
    assert replay[0]["status"] == "structure_watch"
    assert replay[0]["exact_spread_snapshot"]["ask"] == 15.60
    loaded = load_gth_level_candidate_signals(tmp_path / "features")
    assert len(loaded) == 1
    assert loaded[0].entry_px == 15.60


def test_confirmed_gth_lower_rejection_builds_call_manual_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW, parity_price=7310.0, es_price=7340.0)

    candidate = evaluate_gth_level_manual_candidate(
        object(),
        _level_signal(
            NOW,
            thesis="fade",
            direction="up",
            level_kind="put_wall",
            level=7300.0,
        ),
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    assert candidate["status"] == "manual_ready"
    assert candidate["path_kind"] == "lower_rejection_call"
    assert candidate["position_type"] == "call_debit_spread"
    assert candidate["target_spx"] == 7375.0
    assert candidate["target_wall_kind"] == "flip_low"
    assert candidate["invalidation_spx"] == 7297.0
    assert candidate["automatic_ordering"] is False


@pytest.mark.parametrize(
    (
        "direction",
        "position_type",
        "path_kind",
        "right",
        "target_spx",
        "invalidation_spx",
    ),
    [
        ("up", "call_debit_spread", "trend_transition_call", "C", 7393.0, 7365.0),
        ("down", "put_debit_spread", "trend_transition_put", "P", 7343.0, 7371.0),
    ],
)
def test_current_gth_trend_transition_builds_bounded_manual_card(
    monkeypatch: pytest.MonkeyPatch,
    direction: str,
    position_type: str,
    path_kind: str,
    right: str,
    target_spx: float,
    invalidation_spx: float,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)
    trend_state = _trend_transition_state(NOW, direction=direction)

    candidate = evaluate_gth_level_manual_candidate(
        object(),
        {},
        trend_state=trend_state,
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    event = trend_state["last_transition"]
    assert candidate["status"] == "manual_ready"
    assert candidate["source_kind"] == "gth_es_trend_transition"
    assert candidate["source_signal_id"] == event["event_id"]
    assert candidate["source_event_id"] == event["event_id"]
    assert candidate["position_type"] == position_type
    assert candidate["path_kind"] == path_kind
    assert candidate["long_contract_id"].endswith(f":{right}")
    assert candidate["short_contract_id"].endswith(f":{right}")
    assert candidate["execution_mode"] == "manual_only"
    assert candidate["automatic_ordering"] is False
    assert candidate["broker_submission_allowed"] is False
    assert candidate["entry_limit"] == candidate["decision_ask"]
    assert candidate["reward_risk_at_limit"] >= 1.0
    assert candidate["outcome_baselines"]["confirmation_time"]["at"] == NOW.isoformat()
    assert candidate["outcome_baselines"]["confirmation_time"]["parity_spx"] == 7368.0
    assert candidate["target_spx"] == target_spx
    assert candidate["invalidation_spx"] == invalidation_spx
    assert candidate["valid_until"] == (NOW + timedelta(minutes=5)).isoformat()
    card = _notification_intent(candidate, event_id="trend-ready", now=NOW)
    assert f"ES 趋势已确认切换为{'多' if direction == 'up' else '空'}头" in card["text"]
    assert "趋势延续" not in card["text"]


def test_gth_trend_transition_rechecks_and_m1_do_not_rearm_the_same_event(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)
    pending_counts: list[int] = []

    def fake_flush(path, **_kwargs):
        state = candidate_module.read_json_object(path)
        pending = list(state.get("pending_notifications") or [])
        pending_counts.append(len(pending))
        if not pending:
            return {"attempted": False, "accepted": False}
        accepted = set(state.get("accepted_notification_event_ids") or [])
        accepted.add(str(pending[0]["event_id"]))
        state["pending_notifications"] = []
        state["accepted_notification_event_ids"] = sorted(accepted)
        candidate_module.atomic_write_json_secure(path, state)
        return {"attempted": True, "accepted": True, "outcome": "queued"}

    monkeypatch.setattr(level_candidate_module, "flush_pending_notifications", fake_flush)
    storage = SimpleNamespace(data_root=str(tmp_path))
    trend_state = _trend_transition_state(NOW, direction="up")
    common = {
        "trend_state": trend_state,
        "macro_event": {"entry_allowed": True},
        "now": NOW,
        "policy": MarketFeatureSettings(),
        "new_entries_allowed": True,
        "new_entries_block_reason": "allowed",
        "notification": SimpleNamespace(),
    }

    first = process_gth_level_manual_candidate(storage, object(), {}, **common)
    trend_state["last_continuation"] = {
        "event_type": "continuation",
        "event_id": "globex-cont:2026-07-15:gth:3:up:m1",
        "session_id": "2026-07-15:gth",
        "at": NOW.isoformat(),
    }
    replay = process_gth_level_manual_candidate(storage, object(), {}, **common)

    assert first["candidate_id"] == replay["candidate_id"]
    assert first["source_signal_id"] == replay["source_signal_id"]
    assert first["notification_accepted"] is True
    assert replay["notification_attempted"] is False
    assert pending_counts == [1, 0]
    gate_path = (
        tmp_path / "features" / "gth_manual_signal_gates" / "date=2026-07-15" / "events.jsonl"
    )
    replay_path = (
        tmp_path / "features" / "gth_level_manual_candidates" / "date=2026-07-15" / "events.jsonl"
    )
    assert len(gate_path.read_text().splitlines()) == 1
    assert len(replay_path.read_text().splitlines()) == 1


def test_gth_trend_quote_refresh_preserves_lifecycle_and_single_replay(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pending_counts: list[int] = []
    cancellations: list[str] = []

    def fake_flush(path, **_kwargs):
        state = candidate_module.read_json_object(path)
        pending = list(state.get("pending_notifications") or [])
        pending_counts.append(len(pending))
        if not pending:
            return {"attempted": False, "accepted": False}
        event_id = str(pending[0]["event_id"])
        state["pending_notifications"] = []
        state["accepted_notification_event_ids"] = [event_id]
        candidate_module.atomic_write_json_secure(path, state)
        return {"attempted": True, "accepted": True, "outcome": "queued"}

    monkeypatch.setattr(level_candidate_module, "flush_pending_notifications", fake_flush)
    monkeypatch.setattr(
        level_candidate_module,
        "cancel_pending_notification",
        lambda _settings, event_id, **_kwargs: cancellations.append(event_id),
    )
    storage = SimpleNamespace(data_root=str(tmp_path))
    trend_state = _trend_transition_state(NOW, direction="up")
    common = {
        "trend_state": trend_state,
        "macro_event": {"entry_allowed": True},
        "policy": MarketFeatureSettings(),
        "new_entries_allowed": True,
        "new_entries_block_reason": "allowed",
        "notification": SimpleNamespace(),
    }

    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)
    ready = process_gth_level_manual_candidate(storage, object(), {}, now=NOW, **common)
    monkeypatch.setattr(
        level_candidate_module,
        "spread_snapshot_decision",
        lambda *_args, **_kwargs: (
            {},
            [
                "long_leg_transport_stale",
                "short_leg_transport_stale",
                "long_leg_quote_unavailable",
                "short_leg_quote_unavailable",
            ],
        ),
    )
    refresh = process_gth_level_manual_candidate(
        storage,
        object(),
        {},
        now=NOW + timedelta(seconds=1),
        **common,
    )
    _patch_ready_market(
        monkeypatch,
        now=NOW + timedelta(seconds=2),
        parity_price=7368.0,
        es_price=7398.0,
    )
    recovered = process_gth_level_manual_candidate(
        storage,
        object(),
        {},
        now=NOW + timedelta(seconds=2),
        **common,
    )

    event_id = f"{ready['candidate_id']}:ready"
    state = candidate_module.read_json_object(
        tmp_path / "latest" / "gth_level_manual_candidate_state.json"
    )
    replay_path = (
        tmp_path / "features" / "gth_level_manual_candidates" / "date=2026-07-15" / "events.jsonl"
    )
    assert refresh["status"] == "refresh_pending"
    assert refresh["source_lifecycle_class"] == "identified"
    assert recovered["candidate_id"] == ready["candidate_id"]
    assert recovered["notification_attempted"] is False
    assert pending_counts == [1, 0]
    assert cancellations == []
    assert event_id in state["accepted_notification_event_ids"]
    assert event_id not in state["settled_notification_event_ids"]
    assert state["replayed_candidate_ids"] == [ready["candidate_id"]]
    assert len(replay_path.read_text().splitlines()) == 1


@pytest.mark.parametrize(
    ("source_updates", "expected_reason"),
    [
        (
            {"phase": "invalidated", "formal_signal": False},
            "level_source_invalidated",
        ),
        ({"quality_ok": False}, "level_source_quality_invalid"),
    ],
)
def test_explicit_level_source_absence_cancels_prior_ready_lifecycle(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    source_updates: dict[str, object],
    expected_reason: str,
) -> None:
    cancellations: list[str] = []

    def fake_flush(path, **_kwargs):
        state = level_candidate_module.read_json_object(path)
        pending = list(state.get("pending_notifications") or [])
        if not pending:
            return {"attempted": False, "accepted": False}
        event_id = str(pending[0]["event_id"])
        state["pending_notifications"] = []
        state["accepted_notification_event_ids"] = [event_id]
        level_candidate_module.atomic_write_json_secure(path, state)
        return {"attempted": True, "accepted": True, "outcome": "queued"}

    monkeypatch.setattr(level_candidate_module, "flush_pending_notifications", fake_flush)
    monkeypatch.setattr(
        level_candidate_module,
        "cancel_pending_notification",
        lambda _settings, event_id, **_kwargs: cancellations.append(event_id),
    )
    storage = SimpleNamespace(data_root=str(tmp_path))
    source = _level_signal(
        NOW,
        direction="down",
        level_kind="flip_low",
        level=7375.0,
    )
    common = {
        "macro_event": {"entry_allowed": True},
        "policy": MarketFeatureSettings(),
        "new_entries_allowed": True,
        "new_entries_block_reason": "allowed",
        "notification": SimpleNamespace(),
    }

    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)
    ready = process_gth_level_manual_candidate(
        storage,
        object(),
        source,
        now=NOW,
        **common,
    )
    ended_source = {**source, **source_updates}
    ended = process_gth_level_manual_candidate(
        storage,
        object(),
        ended_source,
        now=NOW + timedelta(seconds=1),
        **common,
    )

    event_id = f"{ready['candidate_id']}:ready"
    state = level_candidate_module.read_json_object(
        tmp_path / "latest" / "gth_level_manual_candidate_state.json"
    )
    assert ended["status"] == "blocked"
    assert ended["source_lifecycle_class"] == "explicit_absence"
    assert ended["source_tombstone_id"] == source["event_id"]
    assert expected_reason in ended["block_reasons"]
    assert cancellations == [event_id]
    assert event_id not in state["accepted_notification_event_ids"]
    assert event_id in state["settled_notification_event_ids"]


@pytest.mark.parametrize(
    ("source_updates", "visible_label", "terminal_suffix"),
    [
        (
            {"phase": "invalidated", "formal_signal": False},
            "EXIT REVIEW",
            "exit",
        ),
        ({"quality_ok": False}, "READY CANCELLED", "cancel"),
    ],
)
def test_delivered_level_ready_emits_one_external_terminal_lifecycle(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    source_updates: dict[str, object],
    visible_label: str,
    terminal_suffix: str,
) -> None:
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
        rust_trader_notification_owner=False,
    )
    deliveries: list[frozenset[str]] = []

    def fake_sink_success(_settings, **kwargs):
        targets = frozenset(kwargs["targets"])
        deliveries.append(targets)
        return [SinkResult(sink=target, attempted=True, ok=True) for target in targets]

    monkeypatch.setattr(
        "spx_spark.notifier.dispatcher.deliver_trade_push",
        fake_sink_success,
    )
    storage = SimpleNamespace(data_root=str(tmp_path))
    source = _level_signal(
        NOW,
        direction="down",
        level_kind="flip_low",
        level=7375.0,
    )
    common = {
        "macro_event": {"entry_allowed": True},
        "policy": MarketFeatureSettings(),
        "new_entries_allowed": True,
        "new_entries_block_reason": "allowed",
        "notification": settings,
    }

    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)
    ready = process_gth_level_manual_candidate(
        storage,
        object(),
        source,
        now=NOW,
        **common,
    )
    ready_event_id = f"{ready['candidate_id']}:ready"
    ready_state = level_candidate_module.read_json_object(
        tmp_path / "latest" / "gth_level_manual_candidate_state.json"
    )
    active_plan = ready_state["active_manual_plan"]
    assert active_plan["ready_event_id"] == ready_event_id
    assert active_plan["long_contract_id"] == ready["long_contract_id"]
    assert active_plan["short_contract_id"] == ready["short_contract_id"]
    assert active_plan["expiry"] == ready["expiry"]
    assert active_plan["invalidation_coordinate"] == ready["invalidation_coordinate"]
    assert active_plan["automatic_ordering"] is False
    consumed_ready = consume_pending_notifications(
        settings,
        now=NOW + timedelta(milliseconds=250),
        notify_dead_letters=False,
        completion_clock=lambda: NOW + timedelta(milliseconds=250),
    )
    ended_source = {**source, **source_updates}
    ended = process_gth_level_manual_candidate(
        storage,
        object(),
        ended_source,
        now=NOW + timedelta(seconds=1),
        **common,
    )
    terminal_event_id = f"{ready_event_id}:{terminal_suffix}"
    consumed_terminal = consume_pending_notifications(
        settings,
        now=NOW + timedelta(seconds=2),
        notify_dead_letters=False,
        completion_clock=lambda: NOW + timedelta(seconds=2),
    )
    repeated = process_gth_level_manual_candidate(
        storage,
        object(),
        ended_source,
        now=NOW + timedelta(seconds=3),
        **common,
    )
    duplicate_consumer = consume_pending_notifications(
        settings,
        now=NOW + timedelta(seconds=4),
        notify_dead_letters=False,
    )

    assert consumed_ready["delivered_targets"] == 1
    assert ended["terminal_notification_accepted"] is True
    assert consumed_terminal["delivered_targets"] == 1
    assert repeated["terminal_notification_attempted"] is False
    assert duplicate_consumer["jobs"] == 0
    assert deliveries == [frozenset({"feishu"}), frozenset({"feishu"})]
    with sqlite3.connect(settings.delivery_outbox_path) as connection:
        rows = connection.execute(
            "SELECT event_id, kind, lane, status, title, text, operator_opportunity_id "
            "FROM notification_delivery_events ORDER BY created_at"
        ).fetchall()
        cancellation = connection.execute(
            "SELECT reason FROM notification_delivery_cancellations WHERE event_id = ?",
            (ready_event_id,),
        ).fetchone()
    with sqlite3.connect(settings.delivery_receipt_path) as connection:
        receipts = connection.execute(
            "SELECT event_id, kind, lane, outcome FROM notification_delivery_receipts "
            "ORDER BY attempted_at"
        ).fetchall()
    assert [row[0] for row in rows] == [ready_event_id, terminal_event_id]
    assert rows[1][1:4] == ("virtual_strategy_exit", "strategy_lifecycle", "delivered")
    assert visible_label in rows[1][4]
    assert visible_label in rows[1][5]
    assert f"原卡  {ready_event_id}" in rows[1][5]
    assert "系统不知道你的成交状态" in rows[1][5]
    assert "不代表订单已撤销、仓位已平仓" in rows[1][5]
    assert rows[1][6] == ready["source_signal_id"]
    assert receipts == [
        (
            ready_event_id,
            "gth_spxw_level_manual_spread_candidate",
            "gth_level_manual_candidate",
            "delivered",
        ),
        (
            terminal_event_id,
            "virtual_strategy_exit",
            "strategy_lifecycle",
            "delivered",
        ),
    ]
    assert cancellation == ("source_candidate_no_longer_manual_ready",)
    state = level_candidate_module.read_json_object(
        tmp_path / "latest" / "gth_level_manual_candidate_state.json"
    )
    assert state["active_manual_plan"] == {}
    assert terminal_event_id in state["accepted_notification_event_ids"]
    assert state["pending_terminal_receipt_checks"] == []


def test_delayed_ready_receipt_emits_cancel_then_planned_exit_once(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
        rust_trader_notification_owner=False,
    )
    deliveries: list[frozenset[str]] = []

    def fake_sink_success(_settings, **kwargs):
        targets = frozenset(kwargs["targets"])
        deliveries.append(targets)
        return [SinkResult(sink=target, attempted=True, ok=True) for target in targets]

    monkeypatch.setattr(
        "spx_spark.notifier.dispatcher.deliver_trade_push",
        fake_sink_success,
    )
    storage = SimpleNamespace(data_root=str(tmp_path))
    source = _level_signal(
        NOW,
        direction="down",
        level_kind="flip_low",
        level=7375.0,
    )
    common = {
        "macro_event": {"entry_allowed": True},
        "policy": MarketFeatureSettings(),
        "new_entries_allowed": True,
        "new_entries_block_reason": "allowed",
        "notification": settings,
    }

    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)
    ready = process_gth_level_manual_candidate(
        storage,
        object(),
        source,
        now=NOW,
        **common,
    )
    ready_event_id = f"{ready['candidate_id']}:ready"
    consume_pending_notifications(
        settings,
        now=NOW + timedelta(milliseconds=250),
        notify_dead_letters=False,
        completion_clock=lambda: NOW + timedelta(milliseconds=250),
    )
    lookups = iter(
        (
            SimpleNamespace(observable=True, receipt=None),
            SimpleNamespace(
                observable=True,
                receipt=SimpleNamespace(
                    receipt_id="late-ready-receipt",
                    delivered_at=NOW + timedelta(milliseconds=250),
                ),
            ),
        )
    )
    monkeypatch.setattr(
        level_candidate_module,
        "_external_ready_receipt",
        lambda _settings, _event_id: next(lookups),
    )
    ended_source = {**source, "quality_ok": False}

    first_end = process_gth_level_manual_candidate(
        storage,
        object(),
        ended_source,
        now=NOW + timedelta(seconds=1),
        **common,
    )
    waiting_state = level_candidate_module.read_json_object(
        tmp_path / "latest" / "gth_level_manual_candidate_state.json"
    )
    check = waiting_state["pending_terminal_receipt_checks"][0]
    check_until = datetime.fromisoformat(str(check["check_until"]))
    assert first_end["terminal_notification_attempted"] is False
    assert check["causation_event_id"] == ready_event_id
    assert check_until >= NOW + timedelta(seconds=121)
    assert check_until <= NOW + timedelta(seconds=601)

    late_receipt = process_gth_level_manual_candidate(
        storage,
        object(),
        ended_source,
        now=NOW + timedelta(seconds=2),
        **common,
    )
    cancel_event_id = f"{ready_event_id}:cancel"
    assert late_receipt["terminal_notification_accepted"] is True
    consume_pending_notifications(
        settings,
        now=NOW + timedelta(seconds=3),
        notify_dead_letters=False,
        completion_clock=lambda: NOW + timedelta(seconds=3),
    )
    cancelled_state = level_candidate_module.read_json_object(
        tmp_path / "latest" / "gth_level_manual_candidate_state.json"
    )
    monitor = cancelled_state["manual_plan_monitors"][0]
    assert monitor["ready_event_id"] == ready_event_id
    assert monitor["active_plan"]["long_contract_id"] == ready["long_contract_id"]
    assert monitor["active_plan"]["short_contract_id"] == ready["short_contract_id"]
    assert monitor["ready_receipt_id"] == "late-ready-receipt"

    exit_at = datetime.fromisoformat(str(ready["exit_at"]))
    planned_exit = process_gth_level_manual_candidate(
        storage,
        object(),
        ended_source,
        now=exit_at + timedelta(seconds=1),
        **common,
    )
    exit_event_id = f"{ready_event_id}:exit"
    assert planned_exit["terminal_notification_accepted"] is True
    consume_pending_notifications(
        settings,
        now=exit_at + timedelta(seconds=2),
        notify_dead_letters=False,
        completion_clock=lambda: exit_at + timedelta(seconds=2),
    )
    repeated = process_gth_level_manual_candidate(
        storage,
        object(),
        ended_source,
        now=exit_at + timedelta(seconds=3),
        **common,
    )
    duplicate_consumer = consume_pending_notifications(
        settings,
        now=exit_at + timedelta(seconds=4),
        notify_dead_letters=False,
    )

    assert repeated["terminal_notification_attempted"] is False
    assert duplicate_consumer["jobs"] == 0
    assert deliveries == [
        frozenset({"feishu"}),
        frozenset({"feishu"}),
        frozenset({"feishu"}),
    ]
    with sqlite3.connect(settings.delivery_outbox_path) as connection:
        events = connection.execute(
            "SELECT event_id, status, text FROM notification_delivery_events ORDER BY created_at"
        ).fetchall()
    assert [row[0] for row in events] == [
        ready_event_id,
        cancel_event_id,
        exit_event_id,
    ]
    assert [row[1] for row in events] == ["delivered", "delivered", "delivered"]
    assert "READY CANCELLED" in events[1][2]
    assert "EXIT REVIEW" in events[2][2]
    assert "系统不知道你的成交状态" in events[2][2]
    final_state = level_candidate_module.read_json_object(
        tmp_path / "latest" / "gth_level_manual_candidate_state.json"
    )
    assert final_state["manual_plan_monitors"] == []
    assert final_state["pending_terminal_receipt_checks"] == []


def test_receipt_recovered_after_exit_emits_exit_without_stale_cancel_once(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
        rust_trader_notification_owner=False,
    )
    deliveries: list[frozenset[str]] = []

    def fake_sink_success(_settings, **kwargs):
        targets = frozenset(kwargs["targets"])
        deliveries.append(targets)
        return [SinkResult(sink=target, attempted=True, ok=True) for target in targets]

    monkeypatch.setattr(
        "spx_spark.notifier.dispatcher.deliver_trade_push",
        fake_sink_success,
    )
    storage = SimpleNamespace(data_root=str(tmp_path))
    source = _level_signal(
        NOW,
        direction="down",
        level_kind="flip_low",
        level=7375.0,
    )
    common = {
        "macro_event": {"entry_allowed": True},
        "policy": MarketFeatureSettings(),
        "new_entries_allowed": True,
        "new_entries_block_reason": "allowed",
        "notification": settings,
    }

    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)
    ready = process_gth_level_manual_candidate(
        storage,
        object(),
        source,
        now=NOW,
        **common,
    )
    ready_event_id = f"{ready['candidate_id']}:ready"
    consume_pending_notifications(
        settings,
        now=NOW + timedelta(milliseconds=250),
        notify_dead_letters=False,
        completion_clock=lambda: NOW + timedelta(milliseconds=250),
    )
    unavailable = SimpleNamespace(
        observable=False,
        receipt=None,
        error="python_delivery_receipt_query_failed",
    )
    lookups = iter(
        (
            unavailable,
            unavailable,
            SimpleNamespace(
                observable=True,
                receipt=SimpleNamespace(
                    receipt_id="recovered-ready-receipt",
                    delivered_at=NOW + timedelta(milliseconds=250),
                ),
                error=None,
            ),
        )
    )
    monkeypatch.setattr(
        level_candidate_module,
        "_external_ready_receipt",
        lambda _settings, _event_id: next(lookups),
    )
    # Quality loss would normally produce a pre-entry CANCEL.  Once receipt
    # recovery happens after the plan's time stop, only EXIT remains current.
    ended_source = {**source, "quality_ok": False}

    first_end = process_gth_level_manual_candidate(
        storage,
        object(),
        ended_source,
        now=NOW + timedelta(seconds=1),
        **common,
    )
    assert first_end["terminal_notification_attempted"] is False
    degraded_state = level_candidate_module.read_json_object(
        tmp_path / "latest" / "gth_level_manual_candidate_state.json"
    )
    degraded = degraded_state["pending_terminal_receipt_checks"][0]
    assert degraded["receipt_lookup_status"] == "degraded_ledger_unavailable"
    assert degraded["receipt_lookup_degraded"] is True
    assert degraded["receipt_lookup_error"] == "python_delivery_receipt_query_failed"
    assert degraded["receipt_lookup_attempts"] == 1

    after_old_window = process_gth_level_manual_candidate(
        storage,
        object(),
        ended_source,
        now=NOW + timedelta(minutes=11),
        **common,
    )
    assert after_old_window["terminal_notification_attempted"] is False
    retained_state = level_candidate_module.read_json_object(
        tmp_path / "latest" / "gth_level_manual_candidate_state.json"
    )
    retained = retained_state["pending_terminal_receipt_checks"][0]
    assert datetime.fromisoformat(str(retained["check_until"])) < NOW + timedelta(minutes=11)
    assert datetime.fromisoformat(str(retained["recovery_until"])) == NOW + timedelta(
        seconds=1,
        days=1,
    )
    assert retained["receipt_lookup_attempts"] == 2
    assert retained_state["terminal_receipt_audit_failures"] == []

    recovered = process_gth_level_manual_candidate(
        storage,
        object(),
        ended_source,
        now=NOW + timedelta(minutes=20),
        **common,
    )
    terminal_event_id = f"{ready_event_id}:exit"
    assert recovered["terminal_notification_accepted"] is True
    consumed = consume_pending_notifications(
        settings,
        now=NOW + timedelta(minutes=20, seconds=1),
        notify_dead_letters=False,
        completion_clock=lambda: NOW + timedelta(minutes=20, seconds=1),
    )
    repeated = process_gth_level_manual_candidate(
        storage,
        object(),
        ended_source,
        now=NOW + timedelta(minutes=21),
        **common,
    )

    assert consumed["delivered_targets"] == 1
    assert repeated["terminal_notification_attempted"] is False
    assert deliveries == [frozenset({"feishu"}), frozenset({"feishu"})]
    with sqlite3.connect(settings.delivery_outbox_path) as connection:
        terminal_events = connection.execute(
            "SELECT event_id, status, occurred_at, expires_at "
            "FROM notification_delivery_events "
            "WHERE event_id != ? ORDER BY created_at",
            (ready_event_id,),
        ).fetchall()
    assert len(terminal_events) == 1
    terminal_id, terminal_status, occurred_at, expires_at = terminal_events[0]
    assert (terminal_id, terminal_status) == (terminal_event_id, "delivered")
    assert not terminal_id.endswith(":cancel")
    assert datetime.fromisoformat(str(occurred_at)) == NOW + timedelta(seconds=1)
    assert datetime.fromisoformat(str(expires_at)) == NOW + timedelta(minutes=35)
    final_state = level_candidate_module.read_json_object(
        tmp_path / "latest" / "gth_level_manual_candidate_state.json"
    )
    assert final_state["pending_terminal_receipt_checks"] == []
    assert final_state["terminal_receipt_audit_failures"] == []


def test_undelivered_level_ready_is_only_cancelled_internally(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
        rust_trader_notification_owner=False,
    )
    storage = SimpleNamespace(data_root=str(tmp_path))
    source = _level_signal(
        NOW,
        direction="down",
        level_kind="flip_low",
        level=7375.0,
    )
    common = {
        "macro_event": {"entry_allowed": True},
        "policy": MarketFeatureSettings(),
        "new_entries_allowed": True,
        "new_entries_block_reason": "allowed",
        "notification": settings,
    }

    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)
    ready = process_gth_level_manual_candidate(
        storage,
        object(),
        source,
        now=NOW,
        **common,
    )
    ready_event_id = f"{ready['candidate_id']}:ready"
    ended = process_gth_level_manual_candidate(
        storage,
        object(),
        {**source, "phase": "invalidated", "formal_signal": False},
        now=NOW + timedelta(seconds=1),
        **common,
    )

    assert ended["terminal_notification_attempted"] is False
    with sqlite3.connect(settings.delivery_outbox_path) as connection:
        events = connection.execute(
            "SELECT event_id, status FROM notification_delivery_events"
        ).fetchall()
        targets = connection.execute(
            "SELECT event_id, sink, status FROM notification_delivery_targets"
        ).fetchall()
    assert events == [(ready_event_id, "dead_letter")]
    assert targets == [(ready_event_id, "feishu", "dead_letter")]
    state = level_candidate_module.read_json_object(
        tmp_path / "latest" / "gth_level_manual_candidate_state.json"
    )
    assert state["active_manual_plan"] == {}
    assert state["pending_notifications"] == []
    assert state["pending_terminal_receipt_checks"] == []


def test_empty_source_frame_is_transient_and_preserves_prior_ready_lifecycle(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cancellations: list[str] = []

    def fake_flush(path, **_kwargs):
        state = level_candidate_module.read_json_object(path)
        pending = list(state.get("pending_notifications") or [])
        if not pending:
            return {"attempted": False, "accepted": False}
        event_id = str(pending[0]["event_id"])
        state["pending_notifications"] = []
        state["accepted_notification_event_ids"] = [event_id]
        level_candidate_module.atomic_write_json_secure(path, state)
        return {"attempted": True, "accepted": True, "outcome": "queued"}

    monkeypatch.setattr(level_candidate_module, "flush_pending_notifications", fake_flush)
    monkeypatch.setattr(
        level_candidate_module,
        "cancel_pending_notification",
        lambda _settings, event_id, **_kwargs: cancellations.append(event_id),
    )
    storage = SimpleNamespace(data_root=str(tmp_path))
    source = _level_signal(
        NOW,
        direction="down",
        level_kind="flip_low",
        level=7375.0,
    )
    common = {
        "macro_event": {"entry_allowed": True},
        "policy": MarketFeatureSettings(),
        "new_entries_allowed": True,
        "new_entries_block_reason": "allowed",
        "notification": SimpleNamespace(),
    }

    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)
    ready = process_gth_level_manual_candidate(
        storage,
        object(),
        source,
        now=NOW,
        **common,
    )
    missing = process_gth_level_manual_candidate(
        storage,
        object(),
        {},
        now=NOW + timedelta(seconds=1),
        **common,
    )

    event_id = f"{ready['candidate_id']}:ready"
    state = level_candidate_module.read_json_object(
        tmp_path / "latest" / "gth_level_manual_candidate_state.json"
    )
    assert missing["source_lifecycle_class"] == "transient_absence"
    assert missing["source_tombstone_id"] is None
    assert cancellations == []
    assert event_id in state["accepted_notification_event_ids"]
    assert event_id not in state["settled_notification_event_ids"]


@pytest.mark.parametrize(
    "reason",
    ["level_source_invalidated", "level_source_not_confirmed"],
)
def test_level_tombstone_does_not_cancel_unrelated_trend_source(reason: str) -> None:
    candidate = {
        "status": "blocked",
        "source_signal_id": None,
        "source_tombstone_id": "level:down:flip_low:current",
        "block_reasons": [reason],
    }
    lifecycle_events = {
        "trend-candidate:ready": "globex-trend:2026-07-15:gth:3:bullish",
        "level-candidate:ready": "level:down:flip_low:current",
    }

    cancelled = cancellation_scope(candidate, lifecycle_events, now=NOW)

    assert cancelled == {"level-candidate:ready"}


def test_session_boundary_tombstone_cancels_all_source_lifecycles() -> None:
    candidate = {
        "status": "blocked",
        "source_signal_id": None,
        "source_tombstone_id": None,
        "block_reasons": ["trend_transition_session_mismatch"],
    }
    lifecycle_events = {
        "trend-candidate:ready": "globex-trend:2026-07-14:gth:3:bullish",
        "level-candidate:ready": "level:down:flip_low:prior",
    }

    cancelled = cancellation_scope(candidate, lifecycle_events, now=NOW)

    assert cancelled == set(lifecycle_events)


def test_legacy_replay_seed_uses_last_ready_candidate_and_existing_journal(
    tmp_path,
) -> None:
    replay_path = tmp_path / "events.jsonl"
    replay_path.write_text(
        "\n".join(
            (
                json.dumps(
                    {
                        "status": "manual_ready",
                        "candidate_id": "candidate:from-journal",
                    }
                ),
                "{not-json",
            )
        ),
        encoding="utf-8",
    )

    replayed = seed_replayed_candidate_ids(
        {
            "last_candidate": {
                "status": "manual_ready",
                "candidate_id": "candidate:from-state",
            }
        },
        replay_journal_path=replay_path,
    )

    assert replayed == {"candidate:from-journal", "candidate:from-state"}


def test_legacy_replay_journal_prevents_duplicate_ready_recovery(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)
    monkeypatch.setattr(
        level_candidate_module,
        "flush_pending_notifications",
        lambda *_args, **_kwargs: {"attempted": False, "accepted": False},
    )
    storage = SimpleNamespace(data_root=str(tmp_path))
    source = _level_signal(
        NOW,
        direction="down",
        level_kind="flip_low",
        level=7375.0,
    )
    common = {
        "macro_event": {"entry_allowed": True},
        "policy": MarketFeatureSettings(),
        "new_entries_allowed": True,
        "new_entries_block_reason": "allowed",
    }
    legacy_ready = evaluate_gth_level_manual_candidate(
        object(),
        source,
        now=NOW,
        **common,
    )
    replay_record = level_candidate_module._replay_candidate_record(
        legacy_ready,
        now=NOW,
    )
    assert replay_record is not None
    replay_path = (
        tmp_path / "features" / "gth_level_manual_candidates" / "date=2026-07-15" / "events.jsonl"
    )
    replay_path.parent.mkdir(parents=True)
    replay_path.write_text(json.dumps(replay_record) + "\n", encoding="utf-8")
    state_path = tmp_path / "latest" / "gth_level_manual_candidate_state.json"
    level_candidate_module.atomic_write_json_secure(
        state_path,
        {
            "last_gate_record_key": "legacy-before-replayed-candidate-ids",
            "last_candidate": {
                "status": "blocked",
                "candidate_id": legacy_ready["candidate_id"],
            },
        },
    )

    recovered = process_gth_level_manual_candidate(
        storage,
        object(),
        source,
        now=NOW,
        notification=SimpleNamespace(),
        **common,
    )

    state = level_candidate_module.read_json_object(state_path)
    assert recovered["status"] == "manual_ready"
    assert state["replayed_candidate_ids"] == [legacy_ready["candidate_id"]]
    assert len(replay_path.read_text(encoding="utf-8").splitlines()) == 1


def test_opposite_active_plan_conflict_does_not_cancel_prior_card(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cancellations: list[str] = []

    def fake_flush(path, **_kwargs):
        state = candidate_module.read_json_object(path)
        pending = list(state.get("pending_notifications") or [])
        if not pending:
            return {"attempted": False, "accepted": False}
        event_id = str(pending[0]["event_id"])
        state["pending_notifications"] = []
        state["accepted_notification_event_ids"] = [event_id]
        candidate_module.atomic_write_json_secure(path, state)
        return {"attempted": True, "accepted": True, "outcome": "queued"}

    monkeypatch.setattr(level_candidate_module, "flush_pending_notifications", fake_flush)
    monkeypatch.setattr(
        level_candidate_module,
        "cancel_pending_notification",
        lambda _settings, event_id, **_kwargs: cancellations.append(event_id),
    )
    storage = SimpleNamespace(data_root=str(tmp_path))
    common = {
        "macro_event": {"entry_allowed": True},
        "policy": MarketFeatureSettings(),
        "new_entries_allowed": True,
        "new_entries_block_reason": "allowed",
        "notification": SimpleNamespace(),
    }
    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)
    active = process_gth_level_manual_candidate(
        storage,
        object(),
        {},
        trend_state=_trend_transition_state(NOW, direction="up", sequence=3),
        now=NOW,
        **common,
    )
    _patch_ready_market(
        monkeypatch,
        now=NOW + timedelta(seconds=1),
        parity_price=7368.0,
        es_price=7398.0,
    )
    opposite = process_gth_level_manual_candidate(
        storage,
        object(),
        {},
        trend_state=_trend_transition_state(
            NOW + timedelta(seconds=1),
            direction="down",
            sequence=4,
        ),
        now=NOW + timedelta(seconds=1),
        **common,
    )

    event_id = f"{active['candidate_id']}:ready"
    state = candidate_module.read_json_object(
        tmp_path / "latest" / "gth_level_manual_candidate_state.json"
    )
    assert opposite["status"] == "blocked"
    assert "opposite_signal_conflicts_with_active_plan" in opposite["block_reasons"]
    assert cancellations == []
    assert event_id in state["accepted_notification_event_ids"]
    assert state["active_manual_plan"]["candidate_id"] == active["candidate_id"]


def test_accepted_card_recovers_active_plan_after_activation_write_crash(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_flush(path, **_kwargs):
        state = candidate_module.read_json_object(path)
        pending = list(state.get("pending_notifications") or [])
        event_id = str(pending[0]["event_id"])
        state["pending_notifications"] = []
        state["accepted_notification_event_ids"] = [event_id]
        candidate_module.atomic_write_json_secure(path, state)
        return {"attempted": True, "accepted": True, "outcome": "queued"}

    original_write = level_candidate_module.atomic_write_json_secure

    def crash_on_activation_write(path, payload):
        if payload.get("active_manual_plan"):
            raise RuntimeError("simulated crash before active plan commit")
        original_write(path, payload)

    monkeypatch.setattr(level_candidate_module, "flush_pending_notifications", fake_flush)
    monkeypatch.setattr(
        level_candidate_module,
        "atomic_write_json_secure",
        crash_on_activation_write,
    )
    storage = SimpleNamespace(data_root=str(tmp_path))
    common = {
        "macro_event": {"entry_allowed": True},
        "policy": MarketFeatureSettings(),
        "new_entries_allowed": True,
        "new_entries_block_reason": "allowed",
        "notification": SimpleNamespace(),
    }
    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)
    bullish = _trend_transition_state(NOW, direction="up", sequence=3)

    with pytest.raises(RuntimeError, match="active plan commit"):
        process_gth_level_manual_candidate(
            storage,
            object(),
            {},
            trend_state=bullish,
            now=NOW,
            **common,
        )

    state_path = tmp_path / "latest" / "gth_level_manual_candidate_state.json"
    crashed_state = candidate_module.read_json_object(state_path)
    candidate_id = str(crashed_state["last_candidate"]["candidate_id"])
    assert crashed_state["accepted_notification_event_ids"] == [f"{candidate_id}:ready"]
    assert crashed_state["active_manual_plan"] == {}

    monkeypatch.setattr(
        level_candidate_module,
        "atomic_write_json_secure",
        original_write,
    )
    _patch_ready_market(
        monkeypatch,
        now=NOW + timedelta(seconds=1),
        parity_price=7368.0,
        es_price=7398.0,
    )
    opposite = process_gth_level_manual_candidate(
        storage,
        object(),
        {},
        trend_state=_trend_transition_state(
            NOW + timedelta(seconds=1),
            direction="down",
            sequence=4,
        ),
        now=NOW + timedelta(seconds=1),
        **common,
    )
    recovered_state = candidate_module.read_json_object(state_path)

    assert opposite["status"] == "blocked"
    assert "opposite_signal_conflicts_with_active_plan" in opposite["block_reasons"]
    assert recovered_state["active_manual_plan"]["candidate_id"] == candidate_id
    assert recovered_state["active_manual_plan"]["direction"] == "up"


def test_static_gth_trend_regime_does_not_create_a_manual_card(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)

    candidate = evaluate_gth_level_manual_candidate(
        object(),
        {},
        trend_state={"session_id": "2026-07-15:gth", "regime": "bullish"},
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    assert candidate["status"] == "blocked"
    assert candidate["block_reasons"] == ["source_signal_unavailable"]


def test_fresh_trend_transition_takes_priority_over_fresh_confirmed_level(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW, parity_price=7458.0, es_price=7488.0)
    level = _level_signal(NOW, direction="up", level_kind="call_wall", level=7450.0)

    candidate = evaluate_gth_level_manual_candidate(
        object(),
        level,
        trend_state=_trend_transition_state(NOW, direction="up", price=7488.0),
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    assert candidate["status"] == "manual_ready"
    assert candidate["source_kind"] == "gth_es_trend_transition"
    assert candidate["source_signal_id"] != level["event_id"]
    assert candidate["path_kind"] == "trend_transition_call"


def test_expired_transition_falls_back_to_fresh_confirmed_level(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)
    level = _level_signal(NOW, direction="down", level_kind="flip_low", level=7375.0)
    candidate = evaluate_gth_level_manual_candidate(
        object(),
        level,
        trend_state=_trend_transition_state(NOW - timedelta(seconds=301), direction="down"),
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    assert candidate["status"] == "manual_ready"
    assert candidate["source_kind"] == "gth_confirmed_level_path"
    assert candidate["source_signal_id"] == level["event_id"]


def test_level_policy_hash_and_candidate_id_include_negative_history_veto(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy = MarketFeatureSettings()
    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)
    candidate = evaluate_gth_level_manual_candidate(
        object(),
        _level_signal(NOW, direction="down", level_kind="flip_low", level=7375.0),
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=policy,
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )
    expected_policy = policy_version(
        "gth_level_manual_candidate.v1",
        {
            "quote_max_age_seconds": policy.gth_manual_candidate_quote_max_age_seconds,
            "ttl_seconds": policy.gth_manual_candidate_ttl_seconds,
            "directional_source": "confirmed_frozen_level_path.v2",
            "breakout_crossing": "inside_to_outside_required",
            "breakout_extension": "outside_retest_zone_before_return_required",
            "breakout_retest": "required",
            "max_debit_fraction": policy.gth_manual_candidate_max_debit_fraction,
            "max_net_spread_fraction": policy.gth_manual_candidate_max_net_spread_fraction,
            "min_parity_pairs": policy.gth_manual_candidate_min_parity_pairs,
            "target_room_buffer_points": policy.gth_manual_candidate_target_room_buffer_points,
            "expiry_payoff_ratio_diagnostic_floor": (policy.gth_manual_candidate_min_reward_risk),
            "operator_edge_authority": "validated_first_touch_time_stop_net_pnl",
            "negative_play_stats_veto_enabled": policy.gth_negative_play_stats_veto_enabled,
            "play_stats_min_samples": policy.play_stats_min_samples,
            "invalidation_buffer_points": policy.trade_invalidation_buffer_points,
            "time_stop_minutes": policy.trade_time_stop_minutes,
            "spread_width_points": {"min": 5.0, "default": 25.0, "max": 40.0},
        },
    )
    identity = "|".join(
        (
            "gth_level_manual_candidate.v1",
            expected_policy,
            "flip_low_breakdown_put",
            "option:SPX:SPXW:20260715:7375:P",
            "option:SPX:SPXW:20260715:7335:P",
        )
    )

    assert candidate["policy_version"] == expected_policy
    assert candidate["candidate_id"] == (
        "gth-level-manual:" + hashlib.sha256(identity.encode()).hexdigest()[:24]
    )


def test_gth_trend_transition_with_stale_confirmation_source_is_blocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)
    trend_state = _trend_transition_state(NOW, direction="up")
    trend_state["last_transition"]["source_at"] = (NOW - timedelta(seconds=16)).isoformat()

    candidate = evaluate_gth_level_manual_candidate(
        object(),
        {},
        trend_state=trend_state,
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    assert candidate["status"] == "blocked"
    assert candidate["block_reasons"] == ["trend_transition_source_stale_at_confirmation"]


def test_schwab_gth_trend_can_source_an_ibkr_executable_card(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)
    monkeypatch.setattr(
        level_candidate_module,
        "_direct_es_reference",
        _direct_es_reference,
    )
    schwab_es = Quote(
        instrument=InstrumentId.future("ES"),
        provider=Provider.SCHWAB,
        received_at=NOW,
        last_update_at=NOW,
        quote_time=NOW,
        quality=MarketDataQuality.LIVE,
        bid=7397.75,
        ask=7398.25,
    )
    latest = LatestState(NOW, NOW, (schwab_es,), (schwab_es,))
    candidate = evaluate_gth_level_manual_candidate(
        latest,
        {},
        trend_state=_trend_transition_state(NOW, direction="up", provider="schwab"),
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    assert candidate["status"] == "manual_ready"
    assert candidate["target_coordinate"]["provider"] == "ibkr"
    assert candidate["invalidation_coordinate"]["provider"] == "schwab"
    assert candidate["exact_spread_snapshot"]["long"]["provider"] == "ibkr"
    assert candidate["exact_spread_snapshot"]["short"]["provider"] == "ibkr"


def test_unsupported_gth_trend_provider_is_blocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)
    candidate = evaluate_gth_level_manual_candidate(
        object(),
        {},
        trend_state=_trend_transition_state(NOW, direction="up", provider="hyperliquid"),
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    assert candidate["status"] == "blocked"
    assert candidate["block_reasons"] == ["trend_transition_provider_unsupported"]


def test_gth_provider_control_is_a_hard_manual_card_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)
    candidate = evaluate_gth_level_manual_candidate(
        object(),
        {},
        trend_state=_trend_transition_state(NOW, direction="up"),
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=False,
        new_entries_block_reason="entries_not_explicitly_allowed",
    )

    assert candidate["status"] == "blocked"
    assert "provider_entry_control_blocked" in candidate["block_reasons"]
    assert candidate["provider_incident_warning"] == "entries_not_explicitly_allowed"


@pytest.mark.parametrize(
    ("es_price", "parity_price", "expected_reason"),
    [
        (7390.0, 7368.0, "invalidation_reached_before_candidate"),
        (7430.0, 7400.0, "target_room_below_parity_uncertainty_bound"),
    ],
)
def test_gth_call_transition_blocks_reversal_and_late_target(
    monkeypatch: pytest.MonkeyPatch,
    es_price: float,
    parity_price: float,
    expected_reason: str,
) -> None:
    _patch_ready_market(
        monkeypatch,
        now=NOW,
        parity_price=parity_price,
        es_price=es_price,
    )
    candidate = evaluate_gth_level_manual_candidate(
        object(),
        {},
        trend_state=_trend_transition_state(NOW, direction="up"),
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    assert candidate["status"] == "blocked"
    assert expected_reason in candidate["block_reasons"]


def test_expired_gth_trend_transition_is_blocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stale_at = NOW - timedelta(seconds=301)
    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)

    candidate = evaluate_gth_level_manual_candidate(
        object(),
        {},
        trend_state=_trend_transition_state(
            stale_at,
            direction="up",
            session_id="2026-07-15:gth",
        ),
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    assert candidate["status"] == "blocked"
    assert "source_signal_expired" in candidate["block_reasons"]


def test_wrong_session_gth_trend_transition_is_blocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)

    candidate = evaluate_gth_level_manual_candidate(
        object(),
        {},
        trend_state=_trend_transition_state(
            NOW,
            direction="up",
            session_id="2026-07-14:gth",
        ),
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    assert candidate["status"] == "blocked"
    assert candidate["block_reasons"] == ["trend_transition_session_mismatch"]


def test_gth_trend_transition_without_exact_leg_quote_is_blocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)
    monkeypatch.setattr(
        level_candidate_module,
        "spread_snapshot_decision",
        lambda *_args, **_kwargs: ({}, ["long_leg_quote_unavailable"]),
    )

    candidate = evaluate_gth_level_manual_candidate(
        object(),
        {},
        trend_state=_trend_transition_state(NOW, direction="up"),
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    assert candidate["status"] == "blocked"
    assert "long_leg_quote_unavailable" in candidate["block_reasons"]
    assert candidate["manual_action_eligible"] is False


def test_gth_put_wall_breakdown_is_manual_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW, parity_price=7290.0, es_price=7320.0)

    candidate = evaluate_gth_level_manual_candidate(
        object(),
        _level_signal(NOW, direction="down", level_kind="put_wall", level=7300.0),
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    assert candidate["status"] == "manual_ready"
    assert candidate["manual_action_eligible"] is True
    assert candidate["path_kind"] == "put_wall_breakdown_put"


def test_gth_manual_candidate_uses_fresh_bbo_without_vendor_greeks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    quotes = (
        _option_quote(7505, "C", 14.0),
        _option_quote(7545, "C", 3.0),
    )
    latest = LatestState(NOW, NOW, quotes, quotes)
    monkeypatch.setattr(
        candidate_module,
        "actionable_chain_implied_reference",
        lambda *_args, **_kwargs: {
            "kind": "chain_implied_spx",
            "instrument_id": "synthetic:SPXW_PARITY",
            "price": 7530.0,
            "lower_bound": 7529.5,
            "upper_bound": 7530.5,
            "uncertainty_points": 0.5,
            "pair_count": 5,
            "selected_pair_count": 5,
            "dispersion_points": 1.0,
            "provider": "ibkr",
            "source_at": NOW.isoformat(),
            "transport_at": NOW.isoformat(),
        },
    )
    monkeypatch.setattr(
        candidate_module,
        "_direct_es_reference",
        lambda *_args, **_kwargs: {
            "kind": "raw_es",
            "instrument_id": "future:ES",
            "price": 7552.0,
            "provider": "ibkr",
            "source_at": NOW.isoformat(),
            "transport_at": NOW.isoformat(),
        },
    )

    candidate = evaluate_gth_manual_candidate(
        latest,
        _signal(NOW),
        macro_event={"mode": "normal", "entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    assert candidate["status"] == "manual_ready"
    snapshot = candidate["exact_spread_snapshot"]
    assert snapshot["mid"] == pytest.approx(11.0)
    assert snapshot["long"]["iv"] is None
    assert snapshot["short"]["iv"] is None
    assert snapshot["long"]["quality"]["greeks"] == "optional_unavailable"
    assert snapshot["short"]["quality"]["greeks"] == "optional_unavailable"
    assert snapshot["delta"] is None
    assert candidate["automatic_ordering"] is False


@pytest.mark.parametrize(
    "pricing_provenance",
    (
        {
            "pricing_market_data_type": 3,
            "pricing_live_entitlement": True,
            "pricing_live_entitlement_source": "stale_live_claim",
        },
        {
            "pricing_market_data_type": 2,
            "pricing_live_entitlement": True,
            "pricing_live_entitlement_source": "stale_live_claim",
        },
        {
            "pricing_live_entitlement": False,
            "pricing_live_entitlement_source": "explicit_test_denial",
        },
    ),
    ids=("delayed", "frozen", "explicit-false"),
)
def test_gth_bbo_rejects_non_live_field_pricing_entitlement(
    pricing_provenance: dict[str, object],
) -> None:
    quote = replace(
        _option_quote(7505, "C", 14.0),
        raw=pricing_provenance,
    )
    latest = LatestState(NOW, NOW, (quote,), (quote,))

    assert quote.quality is MarketDataQuality.LIVE
    assert (
        candidate_module._gth_bbo_contract_snapshot(
            latest,
            quote.instrument.canonical_id,
            now=NOW,
        )
        == {}
    )


@pytest.mark.parametrize(
    ("now", "ready"),
    (
        (datetime(2026, 7, 15, 0, 14, 59, tzinfo=UTC), False),
        (datetime(2026, 7, 15, 0, 15, 0, tzinfo=UTC), True),
        (datetime(2026, 7, 15, 13, 24, 29, tzinfo=UTC), True),
        (datetime(2026, 7, 15, 13, 24, 30, tzinfo=UTC), False),
        (datetime(2026, 7, 15, 13, 25, 0, tzinfo=UTC), False),
    ),
)
def test_manual_candidate_respects_gth_open_and_close_buffer(
    monkeypatch: pytest.MonkeyPatch,
    now: datetime,
    ready: bool,
) -> None:
    _patch_ready_market(monkeypatch, now=now)

    candidate = evaluate_gth_manual_candidate(
        object(),
        _signal(now),
        macro_event={"mode": "normal", "entry_allowed": True},
        now=now,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    assert (candidate["status"] == "manual_ready") is ready
    if not ready:
        assert any(
            reason in candidate["block_reasons"]
            for reason in ("spx_gth_session_required", "gth_entry_clock_closed")
        )


def test_target_wall_is_hard_but_parity_room_is_diagnostic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW, parity_price=7543.5)
    signal = _signal(NOW)

    too_close = evaluate_gth_manual_candidate(
        object(),
        signal,
        macro_event={"mode": "normal", "entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )
    no_target_signal = {
        **signal,
        "spread": {**signal["spread"], "target_wall": None},
    }
    no_target = evaluate_gth_manual_candidate(
        object(),
        no_target_signal,
        macro_event={"mode": "normal", "entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    assert too_close["status"] == "manual_ready"
    assert "target_room_below_parity_uncertainty_bound" in (too_close["ranking_diagnostics"])
    assert no_target["status"] == "blocked"
    assert "target_wall_unavailable" in no_target["block_reasons"]


def test_macro_provider_and_es_invalidation_remain_hard_gates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW, es_price=7546.0)

    candidate = evaluate_gth_manual_candidate(
        object(),
        _signal(NOW),
        macro_event={"mode": "pre_event", "entry_allowed": False},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=False,
        new_entries_block_reason="ibkr_unhealthy",
    )

    assert candidate["status"] == "blocked"
    assert "macro_entry_blocked" in candidate["block_reasons"]
    assert candidate["provider_incident_warning"] == "ibkr_unhealthy"
    assert "invalidation_reached_before_candidate" in candidate["block_reasons"]


def test_blocked_or_shadow_source_cannot_become_manual_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW)
    signal = _signal(NOW)
    signal["entry_quality"] = {
        "mode": "shadow",
        "policy_version": "gth_trend_alignment_shadow_v1",
        "verdict": "blocked",
        "block_reasons": ["trend_60m_not_positive"],
        "features": {"return_15m_points": 3.375, "return_60m_points": -10.125},
    }

    candidate = evaluate_gth_manual_candidate(
        object(),
        signal,
        macro_event={"mode": "normal", "entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    assert candidate["status"] == "blocked"
    assert "source_entry_quality_not_decision_grade" in candidate["block_reasons"]
    assert "source_entry_quality_blocked" in candidate["block_reasons"]


def test_old_reclaim_and_subminimum_reward_risk_are_hard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW)
    stale_signal = {
        **_signal(NOW),
        "trough_at": (NOW - timedelta(minutes=21)).isoformat(),
    }
    stale = evaluate_gth_manual_candidate(
        object(),
        stale_signal,
        macro_event={"mode": "normal", "entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )
    poor_reward_risk = evaluate_gth_manual_candidate(
        object(),
        _signal(NOW),
        macro_event={"mode": "normal", "entry_allowed": True},
        now=NOW,
        policy=replace(
            MarketFeatureSettings(),
            gth_manual_candidate_min_reward_risk=3.0,
        ),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    assert "gth_reclaim_too_old" in stale["block_reasons"]
    assert poor_reward_risk["status"] == "blocked"
    assert "spread_reward_risk_insufficient" in poor_reward_risk["block_reasons"]


def test_qualified_parity_reference_requires_three_cofresh_ibkr_pairs() -> None:
    quotes = tuple(
        quote
        for strike, call_mid, put_mid in (
            (7525.0, 20.0, 15.0),
            (7530.0, 15.0, 15.0),
            (7535.0, 12.0, 17.0),
        )
        for quote in (
            _option_quote(strike, "C", call_mid),
            _option_quote(strike, "P", put_mid),
        )
    )
    state = LatestState(NOW, NOW, quotes, quotes)

    reference = actionable_chain_implied_reference(
        state,
        expiry="20260715",
        as_of=NOW,
        required_provider=Provider.IBKR,
    )

    assert reference is not None
    assert reference["price"] == 7530.0
    assert reference["pair_count"] == 3
    assert reference["selected_pair_count"] == 3
    assert reference["dispersion_points"] == 0.0
    assert reference["uncertainty_points"] == pytest.approx(0.2)
    assert reference["lower_bound"] == pytest.approx(7529.8)
    assert reference["upper_bound"] == pytest.approx(7530.2)
    assert {pair["strike"] for pair in reference["selected_pairs"]} == {
        7525.0,
        7530.0,
        7535.0,
    }
    json.dumps(reference)
    assert (
        actionable_chain_implied_reference(
            LatestState(NOW, NOW, quotes[:4], quotes[:4]),
            expiry="20260715",
            as_of=NOW,
            required_provider=Provider.IBKR,
        )
        is None
    )


def test_candidate_notification_is_durable_and_idempotent(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW)
    pending_counts: list[int] = []

    def fake_flush(path, **_kwargs):
        state = candidate_module.read_json_object(path)
        pending = list(state.get("pending_notifications") or [])
        pending_counts.append(len(pending))
        if not pending:
            return {"attempted": False, "accepted": False}
        accepted = set(state.get("accepted_notification_event_ids") or [])
        accepted.add(str(pending[0]["event_id"]))
        state["pending_notifications"] = []
        state["accepted_notification_event_ids"] = sorted(accepted)
        candidate_module.atomic_write_json_secure(path, state)
        return {
            "attempted": True,
            "accepted": True,
            "outcome": "queued",
        }

    monkeypatch.setattr(candidate_module, "flush_pending_notifications", fake_flush)
    storage = SimpleNamespace(data_root=str(tmp_path))
    kwargs = {
        "macro_event": {"mode": "normal", "entry_allowed": True},
        "now": NOW,
        "policy": MarketFeatureSettings(),
        "new_entries_allowed": True,
        "new_entries_block_reason": "allowed",
        "notification": SimpleNamespace(),
    }

    first = process_gth_manual_candidate(storage, object(), _signal(NOW), **kwargs)
    second = process_gth_manual_candidate(storage, object(), _signal(NOW), **kwargs)

    assert first["notification_accepted"] is True
    assert second["notification_attempted"] is False
    assert pending_counts == [1, 0]
    state = candidate_module.read_json_object(
        tmp_path / "latest" / "gth_manual_candidate_state.json"
    )
    assert state["pending_notifications"] == []
    assert len(state["accepted_notification_event_ids"]) == 1


def test_level_candidate_notification_is_durable_and_idempotent(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW, parity_price=7368.0, es_price=7398.0)
    pending_counts: list[int] = []

    def fake_flush(path, **_kwargs):
        state = candidate_module.read_json_object(path)
        pending = list(state.get("pending_notifications") or [])
        pending_counts.append(len(pending))
        if not pending:
            return {"attempted": False, "accepted": False}
        accepted = set(state.get("accepted_notification_event_ids") or [])
        accepted.add(str(pending[0]["event_id"]))
        state["pending_notifications"] = []
        state["accepted_notification_event_ids"] = sorted(accepted)
        candidate_module.atomic_write_json_secure(path, state)
        return {"attempted": True, "accepted": True, "outcome": "queued"}

    monkeypatch.setattr(
        level_candidate_module,
        "flush_pending_notifications",
        fake_flush,
    )
    storage = SimpleNamespace(data_root=str(tmp_path))
    kwargs = {
        "macro_event": {"entry_allowed": True},
        "now": NOW,
        "policy": MarketFeatureSettings(),
        "new_entries_allowed": True,
        "new_entries_block_reason": "allowed",
        "notification": SimpleNamespace(),
    }
    signal = _level_signal(
        NOW,
        direction="down",
        level_kind="flip_low",
        level=7375.0,
    )

    first = process_gth_level_manual_candidate(storage, object(), signal, **kwargs)
    second = process_gth_level_manual_candidate(storage, object(), signal, **kwargs)
    reentry_signal = {
        **signal,
        "event_id": "level:down:flip_low:reentry:1",
        "reentry_generation": 1,
    }
    reentry = process_gth_level_manual_candidate(
        storage,
        object(),
        reentry_signal,
        **kwargs,
    )
    duplicate_reentry = process_gth_level_manual_candidate(
        storage,
        object(),
        reentry_signal,
        **kwargs,
    )

    assert first["notification_accepted"] is True
    assert second["notification_attempted"] is False
    assert reentry["notification_accepted"] is True
    assert duplicate_reentry["notification_attempted"] is False
    assert reentry["candidate_id"] != first["candidate_id"]
    assert pending_counts == [1, 0, 1, 0]
    state = candidate_module.read_json_object(
        tmp_path / "latest" / "gth_level_manual_candidate_state.json"
    )
    assert state["pending_notifications"] == []
    assert state["accepted_notification_event_ids"] == sorted(
        (
            f"{first['candidate_id']}:ready",
            f"{reentry['candidate_id']}:ready",
        )
    )
    gate_rows = [
        json.loads(line)
        for line in (
            tmp_path / "features" / "gth_manual_signal_gates" / "date=2026-07-15" / "events.jsonl"
        )
        .read_text()
        .splitlines()
    ]
    assert len(gate_rows) == 2
    assert {row["candidate_id"] for row in gate_rows} == {
        first["candidate_id"],
        reentry["candidate_id"],
    }
    assert all(row["status"] == "manual_ready" for row in gate_rows)
    assert all(row["gate_contract"]["hard_block_reasons"] == [] for row in gate_rows)
    replay_rows = [
        json.loads(line)
        for line in (
            tmp_path
            / "features"
            / "gth_level_manual_candidates"
            / "date=2026-07-15"
            / "events.jsonl"
        )
        .read_text()
        .splitlines()
    ]
    assert len(replay_rows) == 2
    assert {row["candidate_id"] for row in replay_rows} == {
        first["candidate_id"],
        reentry["candidate_id"],
    }
    assert all(row["schema_version"] == 3 for row in replay_rows)
    assert all(row["event"] == "gth_level_manual_candidate_evaluated" for row in replay_rows)
    assert all(row["coordinate"]["kind"] == "chain_implied_spx" for row in replay_rows)
    assert all(row["coordinate"]["target_value"] == 7375.0 for row in replay_rows)
    assert all(row["coordinate"]["basis_points"] == 0.0 for row in replay_rows)
    loaded = load_gth_level_candidate_signals(tmp_path / "features")
    assert {item.key for item in loaded} == {
        first["candidate_id"],
        reentry["candidate_id"],
    }


def test_blocked_level_candidate_is_gate_audit_not_replay_signal(
    tmp_path,
) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))

    candidate = process_gth_level_manual_candidate(
        storage,
        object(),
        {},
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
        notification=SimpleNamespace(),
    )

    assert candidate["status"] == "blocked"
    gate_path = (
        tmp_path / "features" / "gth_manual_signal_gates" / "date=2026-07-15" / "events.jsonl"
    )
    assert gate_path.exists()
    assert json.loads(gate_path.read_text().strip())["status"] == "blocked"
    replay_path = (
        tmp_path / "features" / "gth_level_manual_candidates" / "date=2026-07-15" / "events.jsonl"
    )
    assert not replay_path.exists()
    assert load_gth_level_candidate_signals(tmp_path / "features") == []


def test_manual_ready_outbox_consumer_receipt_end_to_end_is_idempotent(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW)
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
    deliveries: list[frozenset[str]] = []

    def fake_sink_success(_settings, **kwargs):
        targets = frozenset(kwargs["targets"])
        deliveries.append(targets)
        return [SinkResult(sink=target, attempted=True, ok=True) for target in targets]

    monkeypatch.setattr(
        "spx_spark.notifier.dispatcher.deliver_trade_push",
        fake_sink_success,
    )
    storage = SimpleNamespace(data_root=str(tmp_path))
    kwargs = {
        "macro_event": {"mode": "normal", "entry_allowed": True},
        "now": NOW,
        "policy": MarketFeatureSettings(),
        "new_entries_allowed": True,
        "new_entries_block_reason": "allowed",
        "notification": settings,
    }
    signal = _signal(NOW)

    produced = process_gth_manual_candidate(
        storage,
        object(),
        signal,
        **kwargs,
    )
    duplicate_tick = process_gth_manual_candidate(
        storage,
        object(),
        signal,
        **kwargs,
    )
    event_id = f"{produced['candidate_id']}:ready"
    state = candidate_module.read_json_object(
        tmp_path / "latest" / "gth_manual_candidate_state.json"
    )

    assert produced["status"] == "manual_ready"
    assert produced["notification_accepted"] is True
    assert duplicate_tick["notification_attempted"] is False
    assert state["accepted_notification_event_ids"] == [event_id]
    assert state["notification_lifecycle_events"] == [
        {
            "event_id": event_id,
            "source_signal_id": produced["source_signal_id"],
        }
    ]

    consumed = consume_pending_notifications(
        replace(settings),
        now=NOW + timedelta(seconds=1),
        notify_dead_letters=False,
        worker_id="gth-e2e-consumer:new-instance",
        completion_clock=lambda: NOW + timedelta(seconds=1),
    )
    duplicate_consumer = consume_pending_notifications(
        replace(settings),
        now=NOW + timedelta(seconds=2),
        notify_dead_letters=False,
        worker_id="gth-e2e-consumer:duplicate",
        completion_clock=lambda: NOW + timedelta(seconds=2),
    )
    duplicate_after_delivery = process_gth_manual_candidate(
        storage,
        object(),
        signal,
        **kwargs,
    )
    final_consumer = consume_pending_notifications(
        replace(settings),
        now=NOW + timedelta(seconds=3),
        notify_dead_letters=False,
        worker_id="gth-e2e-consumer:final",
        completion_clock=lambda: NOW + timedelta(seconds=3),
    )

    assert consumed["jobs"] == 1
    assert consumed["delivered_targets"] == 1
    assert duplicate_consumer["jobs"] == 0
    assert final_consumer["jobs"] == 0
    assert duplicate_after_delivery["notification_attempted"] is False
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
        "gth_manual_candidate",
        "gth_spxw_manual_spread_candidate",
        "gth_manual_candidate",
        "delivered",
    )
    assert outbox_targets == [(event_id, "feishu", "delivered")]
    assert len(receipts) == 1
    receipt_event_id, source, kind, lane, outcome, sinks_json = receipts[0]
    assert {event_id, outbox_event[0], receipt_event_id} == {event_id}
    assert (source, kind, lane, outcome) == (
        "gth_manual_candidate",
        "gth_spxw_manual_spread_candidate",
        "gth_manual_candidate",
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


def test_declared_spread_width_cannot_expand_risk_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW)
    signal = _signal(NOW)
    signal["spread"] = {
        **signal["spread"],
        "short_strike": 7510,
        "width_points": 400,
    }

    candidate = evaluate_gth_manual_candidate(
        object(),
        signal,
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    assert candidate["status"] == "blocked"
    assert "spread_width_contract_mismatch" in candidate["block_reasons"]
    assert "spread_debit_risk_cap_exceeded" in candidate["block_reasons"]


@pytest.mark.parametrize(
    ("spread_patch", "reason"),
    (
        ({"invalidation_es": 7400.0}, "gth_invalidation_contract_mismatch"),
        ({"exit_at": None}, "spread_exit_at_unavailable"),
        (
            {"exit_at": (NOW - timedelta(seconds=1)).isoformat()},
            "spread_exit_at_elapsed",
        ),
    ),
)
def test_source_risk_and_exit_contract_cannot_be_weakened(
    monkeypatch: pytest.MonkeyPatch,
    spread_patch: dict[str, object],
    reason: str,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW)
    signal = _signal(NOW)
    signal["spread"] = {**signal["spread"], **spread_patch}

    candidate = evaluate_gth_manual_candidate(
        object(),
        signal,
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    assert candidate["status"] == "blocked"
    assert reason in candidate["block_reasons"]


def test_net_debit_limit_must_round_to_positive_five_cent_increment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW)
    monkeypatch.setattr(
        candidate_module,
        "spread_snapshot_decision",
        lambda *_args, **_kwargs: (
            {
                "bid": 0.01,
                "mid": 0.03,
                "ask": 0.04,
                "long_quote_age_seconds": 0.0,
                "short_quote_age_seconds": 0.0,
                "long_transport_age_seconds": 0.0,
                "short_transport_age_seconds": 0.0,
            },
            [],
        ),
    )
    candidate = evaluate_gth_manual_candidate(
        object(),
        _signal(NOW),
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    assert candidate["status"] == "blocked"
    assert "spread_entry_limit_invalid" in candidate["block_reasons"]


def test_parity_rejects_zero_bid_and_wide_intervals() -> None:
    zero_bid_quotes = tuple(
        quote
        for strike in (7525.0, 7530.0, 7535.0)
        for quote in (
            _option_quote(strike, "C", 15.0, bid=0.0, ask=30.0),
            _option_quote(strike, "P", 15.0, bid=0.0, ask=30.0),
        )
    )
    wide_quotes = tuple(
        quote
        for strike in (7525.0, 7530.0, 7535.0)
        for quote in (
            _option_quote(strike, "C", 15.0, bid=10.0, ask=20.0),
            _option_quote(strike, "P", 15.0, bid=10.0, ask=20.0),
        )
    )

    assert (
        actionable_chain_implied_reference(
            LatestState(NOW, NOW, zero_bid_quotes, zero_bid_quotes),
            expiry="20260715",
            as_of=NOW,
            required_provider=Provider.IBKR,
        )
        is None
    )
    assert (
        actionable_chain_implied_reference(
            LatestState(NOW, NOW, wide_quotes, wide_quotes),
            expiry="20260715",
            as_of=NOW,
            required_provider=Provider.IBKR,
        )
        is None
    )


def test_nbbo_coordinates_require_quote_clock_not_fresh_trade_clock() -> None:
    quotes = tuple(
        quote
        for strike in (7525.0, 7530.0, 7535.0)
        for quote in (
            _option_quote(
                strike,
                "C",
                15.0,
                quote_time=None,
                trade_time=NOW,
            ),
            _option_quote(
                strike,
                "P",
                15.0,
                quote_time=None,
                trade_time=NOW,
            ),
        )
    )
    state = LatestState(NOW, NOW, quotes, quotes)

    assert (
        actionable_chain_implied_reference(
            state,
            expiry="20260715",
            as_of=NOW,
            required_provider=Provider.IBKR,
        )
        is None
    )
    assert (
        _contract_snapshot(
            state,
            quotes[0].instrument.canonical_id,
            now=NOW,
        )
        == {}
    )


def test_es_reference_does_not_freshen_stale_last_with_quote_clock() -> None:
    stale_trade = NOW - timedelta(minutes=5)
    es = Quote(
        instrument=InstrumentId.future("ES"),
        provider=Provider.IBKR,
        received_at=NOW,
        last_update_at=NOW,
        quote_time=NOW,
        trade_time=stale_trade,
        quality=MarketDataQuality.LIVE,
        last=7552.0,
    )
    state = LatestState(NOW, NOW, (es,), (es,))

    assert _direct_es_reference(state, now=NOW, max_age_seconds=15.0) is None


def test_gth_es_reference_does_not_fallback_to_schwab() -> None:
    es = Quote(
        instrument=InstrumentId.future("ES"),
        provider=Provider.SCHWAB,
        received_at=NOW,
        last_update_at=NOW,
        quote_time=NOW,
        quality=MarketDataQuality.LIVE,
        bid=7397.75,
        ask=7398.25,
    )
    state = LatestState(NOW, NOW, (es,), (es,))

    assert _direct_es_reference(state, now=NOW, max_age_seconds=15.0) is None


def test_notification_labels_wall_and_synthetic_quote() -> None:
    candidate = {
        "status": "manual_ready",
        "manual_action_eligible": True,
        "long_contract_id": "option:SPXW:20260715:7505:C",
        "short_contract_id": "option:SPXW:20260715:7545:C",
        "decision_bid": 10.0,
        "decision_mid": 11.0,
        "decision_ask": 12.0,
        "entry_limit": 11.0,
        "max_loss_per_spread": 1100.0,
        "invalidation_es": 7546.0,
        "current_parity_spx": 7530.0,
        "current_parity_upper_bound": 7530.5,
        "target_wall_kind": "call_wall",
        "target_spx": 7545.0,
        "exit_at": (NOW + timedelta(hours=1)).isoformat(),
        "valid_until": (NOW + timedelta(seconds=20)).isoformat(),
        "candidate_id": "candidate",
        "source_signal_id": "signal",
    }

    intent = _notification_intent(candidate, event_id="event", now=NOW)

    assert "🟢 MANUAL READY · CALL SPREAD" in intent["text"]
    assert "买入  SPXW 07-15 7505C" in intent["text"]
    assert "卖出  SPXW 07-15 7545C" in intent["text"]
    assert "目标  SPX 7545.00（Call Wall）" in intent["text"]
    assert "SPX parity 7530.00" in intent["text"]
    assert "退出  12:00 北京时间" in intent["text"]
    assert "不是交易所原生组合 BBO" in intent["text"]
    assert "账户 GTH 权限未验证" in intent["text"]
    assert "有效  剩余 20 秒" in intent["text"]
    assert intent["expires_at"] == candidate["valid_until"]


def test_flush_atomically_records_accepted_event_and_removes_intent(
    tmp_path,
) -> None:
    state_path = tmp_path / "state.json"
    candidate_module.atomic_write_json_secure(
        state_path,
        {
            "pending_notifications": [
                {
                    "event_id": "event",
                    "source": "gth_manual_candidate",
                    "kind": "gth_candidate",
                    "lane": "gth_manual_candidate",
                    "occurred_at": NOW.isoformat(),
                    "expires_at": (NOW + timedelta(seconds=20)).isoformat(),
                    "title": "candidate",
                    "text": "body",
                }
            ]
        },
    )
    calls: list[str] = []

    def enqueue(_settings, envelope, **_kwargs):
        calls.append(envelope.event_id)
        return SimpleNamespace(
            accepted=True,
            inserted=True,
            duplicate=False,
            delivered=False,
            queued_for_recovery=True,
            outcome="pending",
            targets=("bark",),
        )

    result = flush_pending_notifications(
        state_path,
        settings=SimpleNamespace(),
        now=NOW,
        enqueue=enqueue,
    )
    replay = flush_pending_notifications(
        state_path,
        settings=SimpleNamespace(),
        now=NOW,
        enqueue=enqueue,
    )
    state = candidate_module.read_json_object(state_path)

    assert result["accepted"] is True
    assert replay["attempted"] is False
    assert calls == ["event"]
    assert state["pending_notifications"] == []
    assert state["accepted_notification_event_ids"] == ["event"]


def test_blocked_recheck_cancels_unenqueued_candidate_intent(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW)
    monkeypatch.setattr(
        candidate_module,
        "flush_pending_notifications",
        lambda *_args, **_kwargs: {
            "attempted": True,
            "accepted": False,
            "outcome": "enqueue_error",
        },
    )
    storage = SimpleNamespace(data_root=str(tmp_path))
    common = {
        "now": NOW,
        "policy": MarketFeatureSettings(),
        "new_entries_allowed": True,
        "new_entries_block_reason": "allowed",
        "notification": SimpleNamespace(),
    }
    process_gth_manual_candidate(
        storage,
        object(),
        _signal(NOW),
        macro_event={"entry_allowed": True},
        **common,
    )
    process_gth_manual_candidate(
        storage,
        object(),
        _signal(NOW),
        macro_event={"entry_allowed": False},
        **common,
    )

    state = candidate_module.read_json_object(
        tmp_path / "latest" / "gth_manual_candidate_state.json"
    )
    assert state["last_candidate"]["status"] == "blocked"
    assert state["pending_notifications"] == []


def test_outbox_ack_crash_then_source_loss_then_ready_cannot_collide(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW)
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
    durable_enqueues = 0

    def enqueue_then_lose_ack(*args, **kwargs):
        nonlocal durable_enqueues
        enqueue_notification(*args, **kwargs)
        durable_enqueues += 1
        raise RuntimeError("simulated crash after durable enqueue")

    def crash_flush(path, **kwargs):
        return flush_pending_notifications(
            path,
            **kwargs,
            enqueue=enqueue_then_lose_ack,
        )

    monkeypatch.setattr(candidate_module, "flush_pending_notifications", crash_flush)
    storage = SimpleNamespace(data_root=str(tmp_path))
    common = {
        "policy": MarketFeatureSettings(),
        "new_entries_allowed": True,
        "new_entries_block_reason": "allowed",
        "notification": settings,
    }
    signal = _signal(NOW)

    first = process_gth_manual_candidate(
        storage,
        object(),
        signal,
        macro_event={"entry_allowed": True},
        now=NOW,
        **common,
    )
    lost_source = process_gth_manual_candidate(
        storage,
        object(),
        {},
        macro_event={"entry_allowed": False},
        now=NOW + timedelta(milliseconds=500),
        **common,
    )
    recovered = process_gth_manual_candidate(
        storage,
        object(),
        signal,
        macro_event={"entry_allowed": True},
        now=NOW + timedelta(seconds=1),
        **common,
    )

    assert first["notification_outcome"] == "enqueue_error:RuntimeError"
    assert lost_source["status"] == "observing"
    assert recovered["status"] == "manual_ready"
    assert recovered["notification_attempted"] is False
    assert durable_enqueues == 1
    state = candidate_module.read_json_object(
        tmp_path / "latest" / "gth_manual_candidate_state.json"
    )
    event_id = f"{first['candidate_id']}:ready"
    assert state["pending_notifications"] == []
    assert event_id in state["settled_notification_event_ids"]
    with sqlite3.connect(settings.delivery_outbox_path) as connection:
        row = connection.execute(
            "SELECT status FROM notification_delivery_events WHERE event_id = ?",
            (event_id,),
        ).fetchone()
    assert row == ("dead_letter",)


def test_accepted_ready_is_cancelled_before_blocked_card_can_deliver(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW)
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
    storage = SimpleNamespace(data_root=str(tmp_path))
    common = {
        "policy": MarketFeatureSettings(),
        "new_entries_allowed": True,
        "new_entries_block_reason": "allowed",
        "notification": settings,
    }
    signal = _signal(NOW)

    ready = process_gth_manual_candidate(
        storage,
        object(),
        signal,
        macro_event={"entry_allowed": True},
        now=NOW,
        **common,
    )
    state_path = tmp_path / "latest" / "gth_manual_candidate_state.json"
    accepted_state = candidate_module.read_json_object(state_path)
    event_id = f"{ready['candidate_id']}:ready"
    assert ready["notification_accepted"] is True
    assert accepted_state["pending_notifications"] == []
    assert event_id in accepted_state["accepted_notification_event_ids"]

    blocked = process_gth_manual_candidate(
        storage,
        object(),
        signal,
        macro_event={"entry_allowed": False},
        now=NOW + timedelta(milliseconds=500),
        **common,
    )
    consumed = consume_pending_notifications(
        settings,
        now=NOW + timedelta(seconds=1),
        notify_dead_letters=False,
    )
    final_state = candidate_module.read_json_object(state_path)

    assert blocked["status"] == "blocked"
    assert consumed["jobs"] == 0
    assert consumed["delivered_targets"] == 0
    assert event_id not in final_state["accepted_notification_event_ids"]
    assert event_id in final_state["settled_notification_event_ids"]
    assert final_state["notification_lifecycle_events"] == []
    with sqlite3.connect(settings.delivery_outbox_path) as connection:
        row = connection.execute(
            "SELECT status FROM notification_delivery_events WHERE event_id = ?",
            (event_id,),
        ).fetchone()
    assert row == ("dead_letter",)


def test_failed_cancellation_blocks_a_new_gth_ready_lifecycle(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW)
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
    storage = SimpleNamespace(data_root=str(tmp_path))
    common = {
        "policy": MarketFeatureSettings(),
        "new_entries_allowed": True,
        "new_entries_block_reason": "allowed",
        "notification": settings,
    }
    first_signal = _signal(NOW)
    first = process_gth_manual_candidate(
        storage,
        object(),
        first_signal,
        macro_event={"entry_allowed": True},
        now=NOW,
        **common,
    )
    cancellation_times: list[datetime] = []

    def fail_cancellation(*_args, **kwargs):
        cancellation_times.append(kwargs["now"])
        raise RuntimeError("outbox unavailable")

    monkeypatch.setattr(
        candidate_module,
        "cancel_pending_notification",
        fail_cancellation,
    )

    process_gth_manual_candidate(
        storage,
        object(),
        first_signal,
        macro_event={"entry_allowed": False},
        now=NOW + timedelta(milliseconds=500),
        **common,
    )
    second_signal = _signal(NOW + timedelta(seconds=1))
    rearmed = process_gth_manual_candidate(
        storage,
        object(),
        second_signal,
        macro_event={"entry_allowed": True},
        now=NOW + timedelta(seconds=1),
        **common,
    )
    state = candidate_module.read_json_object(
        tmp_path / "latest" / "gth_manual_candidate_state.json"
    )

    assert first["notification_accepted"] is True
    assert rearmed["status"] == "manual_ready"
    assert rearmed["notification_attempted"] is False
    assert state["pending_notification_cancellation_event_ids"] == [
        f"{first['candidate_id']}:ready"
    ]
    assert state["pending_notification_cancellation_at"] == {
        f"{first['candidate_id']}:ready": (NOW + timedelta(milliseconds=500)).isoformat()
    }
    assert cancellation_times == [
        NOW + timedelta(milliseconds=500),
        NOW + timedelta(milliseconds=500),
    ]
    with sqlite3.connect(settings.delivery_outbox_path) as connection:
        rows = connection.execute("SELECT event_id FROM notification_delivery_events").fetchall()
    assert rows == [(f"{first['candidate_id']}:ready",)]


def _trend_transition_state(
    at: datetime,
    *,
    direction: str,
    session_id: str = "2026-07-15:gth",
    provider: str = "ibkr",
    price: float = 7398.0,
    sequence: int = 3,
) -> dict[str, object]:
    regime = "bullish" if direction == "up" else "bearish"
    return {
        "version": 2,
        "session_id": session_id,
        "regime": regime,
        "transition_sequence": sequence,
        "last_transition": {
            "event_type": "transition",
            "event_id": f"globex-trend:{session_id}:{sequence}:{regime}",
            "session_id": session_id,
            "sequence": sequence,
            "from_regime": "neutral",
            "to_regime": regime,
            "reason": "confirmed_multi_horizon_transition",
            "at": at.isoformat(),
            "source_at": at.isoformat(),
            "price": price,
            "provider": provider,
            "metrics": {},
            "operator_action": "observe_only",
            "automatic_ordering": False,
        },
    }


def _patch_ready_market(
    monkeypatch: pytest.MonkeyPatch,
    *,
    now: datetime,
    parity_price: float = 7530.0,
    es_price: float = 7552.0,
    spread_bid: float = 10.0,
    spread_mid: float = 11.0,
    spread_ask: float = 12.0,
    edge_authority: bool | None = True,
) -> None:
    short_bid = 9.0
    short_ask = short_bid + min(1.0, (spread_ask - spread_bid) / 2.0)
    short_mid = (short_bid + short_ask) / 2.0
    monkeypatch.setattr(
        candidate_module,
        "spread_snapshot_decision",
        lambda *_args, **_kwargs: (
            {
                "at": now.isoformat(),
                "bid": spread_bid,
                "mid": spread_mid,
                "ask": spread_ask,
                "quality": {"status": "ok"},
                "long_quote_age_seconds": 0.0,
                "short_quote_age_seconds": 0.0,
                "long_transport_age_seconds": 0.0,
                "short_transport_age_seconds": 0.0,
                "long": {
                    "bid": spread_bid + short_ask,
                    "mid": spread_mid + short_mid,
                    "ask": spread_ask + short_bid,
                    "provider": "ibkr",
                    "source_at": now.isoformat(),
                    "transport_at": now.isoformat(),
                    "quality": {"status": "ok"},
                },
                "short": {
                    "bid": short_bid,
                    "mid": short_mid,
                    "ask": short_ask,
                    "provider": "ibkr",
                    "source_at": now.isoformat(),
                    "transport_at": now.isoformat(),
                    "quality": {"status": "ok"},
                },
            },
            [],
        ),
    )
    monkeypatch.setattr(
        level_candidate_module,
        "spread_snapshot_decision",
        candidate_module.spread_snapshot_decision,
    )
    monkeypatch.setattr(
        candidate_module,
        "actionable_chain_implied_reference",
        lambda *_args, **_kwargs: {
            "kind": "chain_implied_spx",
            "instrument_id": "synthetic:SPXW_PARITY",
            "price": parity_price,
            "lower_bound": parity_price - 0.5,
            "upper_bound": parity_price + 0.5,
            "uncertainty_points": 0.5,
            "pair_count": 5,
            "selected_pair_count": 5,
            "dispersion_points": 1.0,
            "provider": "ibkr",
            "source_at": now.isoformat(),
            "transport_at": now.isoformat(),
        },
    )
    monkeypatch.setattr(
        level_candidate_module,
        "actionable_chain_implied_reference",
        candidate_module.actionable_chain_implied_reference,
    )
    monkeypatch.setattr(
        candidate_module,
        "_direct_es_reference",
        lambda *_args, **_kwargs: {
            "kind": "raw_es",
            "instrument_id": "future:ES",
            "price": es_price,
            "provider": "ibkr",
            "source_at": now.isoformat(),
            "transport_at": now.isoformat(),
        },
    )
    monkeypatch.setattr(
        level_candidate_module,
        "_direct_es_reference",
        candidate_module._direct_es_reference,
    )
    if edge_authority is not None:
        monkeypatch.setattr(
            level_candidate_module,
            "_operator_edge_authority",
            lambda: (
                (
                    level_candidate_module.EDGE_AUTHORITY_REQUIRED,
                    None,
                )
                if edge_authority
                else (
                    "none",
                    level_candidate_module.EDGE_AUTHORITY_UNAVAILABLE_REASON,
                )
            ),
        )


def _signal(now: datetime) -> dict[str, object]:
    session_date = DEFAULT_MARKET_CALENDAR.research_expiry(now).isoformat()
    return {
        "schema_version": 3,
        "policy_version": "gth_dip_reclaim.v4+sha256:test",
        "valid_until": (now + timedelta(minutes=10)).isoformat(),
        "coordinate": {
            "kind": "raw_es",
            "instrument_id": "future:ES",
            "observed_value": 7552.0,
            "target_value": 7550.0,
            "spx_observed_value": None,
            "basis_points": 0.0,
            "as_of": now.isoformat(),
        },
        "block_reasons": [],
        "kind": "gth_dip_reclaim_call",
        "event_id": f"gth-dip:{now.isoformat()}",
        "session_date": session_date,
        "confirmed_at": now.isoformat(),
        "trough": 7546.0,
        "trough_at": (now - timedelta(minutes=5)).isoformat(),
        "entry_quality": {
            "mode": "decision_grade",
            "policy_version": "gth_trend_alignment_live_v2",
            "verdict": "pass",
            "block_reasons": [],
            "features": {
                "session_id": f"{session_date}:gth",
                "return_15m_points": 3.0,
                "return_60m_points": 4.0,
                "return_180m_points": None,
            },
        },
        "spread": {
            "right": "C",
            "expiry_date": session_date,
            "long_strike": 7505,
            "short_strike": 7545,
            "width_points": 40,
            "anchor": "structure_wall",
            "target_wall": 7545.0,
            "target_wall_kind": "call_wall",
            "invalidation_es": 7546.0,
            "exit_at": (now + timedelta(hours=10)).isoformat(),
        },
    }


def _level_signal(
    now: datetime,
    *,
    thesis: str = "breakout",
    direction: str,
    level_kind: str,
    level: float,
) -> dict[str, object]:
    expiry = DEFAULT_MARKET_CALENDAR.research_expiry(now).strftime("%Y%m%d")
    return {
        "formal_signal": True,
        "phase": "confirmed",
        "quality_ok": True,
        "structure_change_pending": False,
        "event_id": f"level:{direction}:{level_kind}:{now.isoformat()}",
        "expires_at": (now + timedelta(minutes=10)).isoformat(),
        "expiry": expiry,
        "thesis": thesis,
        "direction": direction,
        "level_kind": level_kind,
        "level": level,
        **(
            {
                "breakout_inside_seen_at": (now - timedelta(seconds=45)).isoformat(),
                "breakout_extension_seen_at": (now - timedelta(seconds=30)).isoformat(),
                "breakout_retest_seen_at": (now - timedelta(seconds=15)).isoformat(),
            }
            if thesis == "breakout"
            else {}
        ),
        "levels": {
            "put_wall": 7300.0,
            "flip_low": 7375.0,
            "flip_high": 7380.0,
            "call_wall": 7450.0,
        },
        "spot": level - 7.0 if direction == "down" else level + 8.0,
        "es": level + 23.0 if direction == "down" else level + 38.0,
        "es_basis_points": 30.0,
        "trigger_coordinate": {
            "kind": "chain_implied_spx",
            "instrument_id": "synthetic:SPXW_PARITY",
            "observed_value": level - 7.0 if direction == "down" else level + 8.0,
            "target_value": level,
        },
    }


def _option_quote(
    strike: float,
    right: str,
    mid: float,
    *,
    bid: float | None = None,
    ask: float | None = None,
    quote_time: datetime | None = NOW,
    trade_time: datetime | None = None,
) -> Quote:
    return Quote(
        instrument=InstrumentId.option(
            "SPX",
            expiry="20260715",
            strike=strike,
            right=right,
            trading_class="SPXW",
        ),
        provider=Provider.IBKR,
        received_at=NOW,
        last_update_at=NOW,
        quote_time=quote_time,
        trade_time=trade_time,
        quality=MarketDataQuality.LIVE,
        bid=mid - 0.1 if bid is None else bid,
        ask=mid + 0.1 if ask is None else ask,
    )
