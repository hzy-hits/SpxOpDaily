from __future__ import annotations

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
        gth_position_fraction=0.02,
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
    assert candidate["prior_session"]["chase_risk"] == "high"
    assert "prior_session_same_direction_chase_risk_high" in candidate[
        "ranking_diagnostics"
    ]
    assert candidate["block_reasons"] == []
    card = _notification_intent(candidate, event_id="put-ready", now=NOW)
    assert "🟢 MANUAL READY · PUT SPREAD" in card["text"]
    assert "买入  SPXW 07-15 7375P" in card["text"]
    assert "卖出  SPXW 07-15 7335P" in card["text"]
    assert "NBBO  10.00 / 12.00" in card["text"]
    assert "限价  净借记 ≤ 12.00" in card["text"]
    assert "触发  SPX 跌破 Flip Low 7375.00 并确认" in card["text"]
    assert "前日  -1.52%" in card["text"]
    assert "本票同向追单风险高" in card["text"]
    assert "止损  SPX 收回 7383.00；ES 升至 7413.00" in card["text"]
    assert "目标  SPX 7300.00（Put Wall）" in card["text"]
    assert "退出  " in card["text"]
    assert "有效  剩余 " in card["text"]
    assert "自动下单关闭" in card["text"]
    assert card["lane"] == "gth_level_manual_candidate"


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
    rearmed_level = {**first_level, "event_id": "level:rearmed:same-flip-low"}

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


def test_gth_es_continuation_does_not_create_a_chase_card(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_ready_market(monkeypatch, now=NOW, parity_price=7337.0, es_price=7367.0)
    level_context = {
        "formal_signal": True,
        "phase": "confirmed",
        "quality_ok": True,
        "event_id": "level:stale-event",
        "expires_at": (NOW - timedelta(seconds=1)).isoformat(),
        "expiry": "20260715",
        "spot": 7337.0,
        "es": 7367.0,
        "es_basis_points": 30.0,
        "levels": {
            "put_wall": 7300.0,
            "flip_low": 7375.0,
            "flip_high": 7380.0,
            "call_wall": 7450.0,
        },
        "trigger_coordinate": {
            "kind": "chain_implied_spx",
            "instrument_id": "synthetic:SPXW_PARITY",
            "observed_value": 7337.0,
        },
    }
    trend_state = {
        "last_continuation": {
            "event_type": "continuation",
            "event_id": "globex-cont:2026-07-15:gth:3:down:m1",
            "session_id": "2026-07-15:gth",
            "signal_stage": "entry_advisory",
            "advisory_status": "advisory_ready",
            "direction": "down",
            "anchor_price": 7378.0,
            "at": NOW.isoformat(),
        }
    }

    candidate = evaluate_gth_level_manual_candidate(
        object(),
        level_context,
        trend_state=trend_state,
        macro_event={"entry_allowed": True},
        now=NOW,
        policy=MarketFeatureSettings(),
        new_entries_allowed=True,
        new_entries_block_reason="allowed",
    )

    assert candidate["status"] == "blocked"
    assert candidate["source_kind"] is None
    assert candidate["block_reasons"] == ["source_signal_unavailable"]


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


def test_old_reclaim_is_hard_but_subminimum_reward_risk_is_diagnostic(
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
    assert poor_reward_risk["status"] == "manual_ready"
    assert "spread_reward_risk_insufficient" in (poor_reward_risk["ranking_diagnostics"])


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
        state["pending_notifications"] = []
        state["accepted_notification_event_ids"] = [pending[0]["event_id"]]
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

    assert first["notification_accepted"] is True
    assert second["notification_attempted"] is False
    assert pending_counts == [1, 0]
    state = candidate_module.read_json_object(
        tmp_path / "latest" / "gth_level_manual_candidate_state.json"
    )
    assert state["pending_notifications"] == []
    assert len(state["accepted_notification_event_ids"]) == 1
    gate_rows = [
        json.loads(line)
        for line in (
            tmp_path / "features" / "gth_manual_signal_gates" / "date=2026-07-15" / "events.jsonl"
        )
        .read_text()
        .splitlines()
    ]
    assert len(gate_rows) == 1
    assert gate_rows[0]["status"] == "manual_ready"
    assert gate_rows[0]["gate_contract"]["hard_block_reasons"] == []


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


def test_notification_labels_wall_and_synthetic_quote() -> None:
    candidate = {
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
    monkeypatch.setattr(
        candidate_module,
        "cancel_pending_notification",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("outbox unavailable")),
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
    with sqlite3.connect(settings.delivery_outbox_path) as connection:
        rows = connection.execute("SELECT event_id FROM notification_delivery_events").fetchall()
    assert rows == [(f"{first['candidate_id']}:ready",)]


def _patch_ready_market(
    monkeypatch: pytest.MonkeyPatch,
    *,
    now: datetime,
    parity_price: float = 7530.0,
    es_price: float = 7552.0,
) -> None:
    monkeypatch.setattr(
        candidate_module,
        "spread_snapshot_decision",
        lambda *_args, **_kwargs: (
            {
                "bid": 10.0,
                "mid": 11.0,
                "ask": 12.0,
                "long_quote_age_seconds": 0.0,
                "short_quote_age_seconds": 0.0,
                "long_transport_age_seconds": 0.0,
                "short_transport_age_seconds": 0.0,
                "long": {"bid": 20.0, "mid": 20.5, "ask": 21.0},
                "short": {"bid": 9.0, "mid": 9.5, "ask": 10.0},
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
