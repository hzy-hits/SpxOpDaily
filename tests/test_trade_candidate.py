from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from spx_spark.application.market_features.trade_candidate import (
    PUT_SHADOW_EXACT_QUOTE_MAX_AGE_SECONDS,
    PUT_SHADOW_EXACT_QUOTE_POLICY_VERSION,
    _armed_candidate,
    advance_put_shadow_candidates,
    advance_trade_candidate,
    gate_trade_intent,
    virtual_entry_intent,
)
from spx_spark.application.market_features.trade_intent_runtime import (
    process_trade_intent,
)
from spx_spark.application.order_map.put_candidate_presentation import (
    build_put_candidate_report,
)
from spx_spark.marketdata import InstrumentId, MarketDataQuality, Provider, Quote
from spx_spark.storage import LatestState


UTC = timezone.utc
NOW = datetime(2026, 7, 15, 15, 50, 51, tzinfo=UTC)
OPTION_ID = "option:SPX:SPXW:20260715:7560:P"
CALL_OPTION_ID = "option:SPX:SPXW:20260715:7560:C"


def test_target_before_entry_quote_retires_candidate_without_fill_claim(tmp_path) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    armed = advance_trade_candidate(
        storage,
        _latest(NOW, spx=7551.08, bid=14.6, ask=14.8),
        _call_intent(),
        now=NOW,
    )
    terminal = advance_trade_candidate(
        storage,
        _latest(NOW + timedelta(seconds=15), spx=7563.1, bid=14.5, ask=14.7),
        {"status": "observing"},
        now=NOW + timedelta(seconds=15),
    )

    assert armed["phase"] == "armed"
    assert terminal["phase"] == "target_passed"
    assert terminal["terminal_reason"] == "target_reached_before_entry_quote"
    assert terminal["execution_claim"] == "none"
    assert virtual_entry_intent(terminal) == {}
    gated = gate_trade_intent(_call_intent(), terminal)
    assert gated["status"] == "blocked"
    assert gated["block_reasons"] == ["target_reached_before_entry_quote"]
    repeated = advance_trade_candidate(
        storage,
        _latest(NOW + timedelta(seconds=20), spx=7563.5, bid=14.7, ask=14.9),
        _call_intent(),
        now=NOW + timedelta(seconds=20),
    )
    assert repeated["phase"] == "target_passed"
    assert gate_trade_intent(_call_intent(), repeated)["status"] == "blocked"
    rows = [
        json.loads(line)
        for line in (
            tmp_path / "features" / "trade_candidates" / "date=2026-07-15" / "events.jsonl"
        )
        .read_text()
        .splitlines()
    ]
    assert [row["event"] for row in rows] == ["candidate_armed", "candidate_terminal"]


def test_displayed_call_ask_reaching_limit_is_quote_observation_not_fill(tmp_path) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    advance_trade_candidate(
        storage,
        _latest(NOW, spx=7551.08, bid=14.6, ask=14.8),
        _call_intent(),
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
    adapted = virtual_entry_intent(terminal)
    assert adapted["source_intent_id"] == "intent:test-call"
    assert adapted["intent_id"] == "intent:test-call|level:test-call"
    assert adapted["execution_assumption"] == "displayed_quote_only_no_broker_fill"


def test_approved_call_terminal_can_be_adapted_for_virtual_entry() -> None:
    source = {
        **_intent(),
        "intent_id": "intent:test-call",
        "event_id": "level:test-call",
        "direction": "up",
        "play": "level_breakout_call",
        "contract_id": "option:SPX:SPXW:20260715:7560:C",
        "strategy_lane": "long_0dte_rth_upside_breakout_pilot",
    }
    terminal = {
        **_armed_candidate(source, now=NOW),
        "phase": "quote_reached_entry",
        "entry_observation": {
            "entry_condition": "displayed_ask_at_or_below_limit",
        },
    }

    adapted = virtual_entry_intent(terminal)

    assert adapted["source_intent_id"] == "intent:test-call"
    assert adapted["intent_id"] == "intent:test-call|level:test-call"
    assert adapted["execution_assumption"] == "displayed_quote_only_no_broker_fill"


def test_put_shadow_terminal_cannot_be_promoted_by_virtual_entry_adapter() -> None:
    source = _shadow_intent(
        lane="long_0dte_rth_flip_low_breakdown_put_shadow",
        event_id="level:shadow-no-promote",
    )
    terminal = {
        **_armed_candidate(source, now=NOW),
        "phase": "quote_reached_entry",
        "shadow_mode": True,
        "entry_observation": {
            "entry_condition": "displayed_ask_at_or_below_limit",
        },
    }

    assert virtual_entry_intent(terminal) == {}


def test_invalidation_before_entry_quote_retires_candidate(tmp_path) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    advance_trade_candidate(
        storage,
        _latest(NOW, spx=7551.08, bid=14.6, ask=14.8),
        _call_intent(),
        now=NOW,
    )
    terminal = advance_trade_candidate(
        storage,
        _latest(NOW + timedelta(seconds=10), spx=7549.9, bid=11.0, ask=11.2),
        {"status": "observing"},
        now=NOW + timedelta(seconds=10),
    )

    assert terminal["phase"] == "invalidated"
    assert terminal["terminal_reason"] == "invalidation_reached_before_entry_quote"


def test_normal_collector_rejects_put_and_does_not_supersede_approved_call(
    tmp_path,
) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    armed = advance_trade_candidate(
        storage,
        _latest(NOW, spx=7551.08, bid=14.6, ask=14.8),
        _call_intent(),
        now=NOW,
    )

    result = advance_trade_candidate(
        storage,
        _latest(NOW + timedelta(seconds=1), spx=7551.1, bid=14.6, ask=14.8),
        _intent(),
        now=NOW + timedelta(seconds=1),
    )

    assert armed["candidate_id"] == "intent:test-call|level:test-call"
    assert result["candidate_id"] == armed["candidate_id"]
    state = json.loads((tmp_path / "latest" / "trade_candidate_state.json").read_text())
    assert state["active"]["candidate_id"] == armed["candidate_id"]


def test_put_shadow_lanes_advance_independently_and_record_exact_quote(tmp_path) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    flip = _shadow_intent(
        lane="long_0dte_rth_flip_low_breakdown_put_shadow",
        event_id="level:flip-low",
    )
    rejection = _shadow_intent(
        lane="long_0dte_rth_upper_rejection_put_shadow",
        event_id="level:upper-rejection",
    )

    first = advance_put_shadow_candidates(
        storage,
        _latest(NOW, spx=7551.08, bid=14.6, ask=14.8),
        flip,
        now=NOW,
    )
    second = advance_put_shadow_candidates(
        storage,
        _latest(NOW + timedelta(seconds=2), spx=7551.08, bid=14.6, ask=14.8),
        rejection,
        now=NOW + timedelta(seconds=2),
    )

    assert first["phase"] == "armed"
    assert second["phase"] == "armed"
    state = json.loads((tmp_path / "latest" / "put_shadow_candidate_state.json").read_text())
    assert set(state["active_by_lane"]) == {
        "long_0dte_rth_flip_low_breakdown_put_shadow",
        "long_0dte_rth_upper_rejection_put_shadow",
    }

    terminal = advance_put_shadow_candidates(
        storage,
        _latest(NOW + timedelta(seconds=4), spx=7551.08, bid=14.5, ask=14.6),
        {"status": "observing"},
        now=NOW + timedelta(seconds=4),
    )

    assert terminal["phase"] == "observing"
    for row in terminal["lanes"].values():
        assert row["phase"] == "quote_reached_entry"
        assert row["execution_claim"] == "none"
        assert row["entry_observation"]["quote_quality"] == "live"
        assert row["entry_observation"]["quote_pricing_allowed"] is True
        assert row["entry_observation"]["entry_condition"] == ("displayed_ask_at_or_below_limit")
    state = json.loads((tmp_path / "latest" / "put_shadow_candidate_state.json").read_text())
    assert state["active_by_lane"] == {}
    assert len(state["completed_candidates"]) == 2
    rows = [
        json.loads(line)
        for line in (
            tmp_path / "features" / "trade_candidates" / "date=2026-07-15" / "events.jsonl"
        )
        .read_text()
        .splitlines()
    ]
    assert [row["event"] for row in rows] == [
        "candidate_armed",
        "candidate_armed",
        "candidate_terminal",
        "candidate_terminal",
    ]
    assert all(row.get("shadow_mode") is True for row in rows)


def test_put_shadow_flows_to_exact_collector_and_report_without_live_consumers(
    tmp_path,
) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    intent = _shadow_intent(
        lane="long_0dte_rth_flip_low_breakdown_put_shadow",
        event_id="level:end-to-end",
    )

    normal = advance_trade_candidate(
        storage,
        _latest(NOW, spx=7551.08, bid=14.6, ask=14.8),
        intent,
        now=NOW,
    )
    shadow = advance_put_shadow_candidates(
        storage,
        _latest(NOW, spx=7551.08, bid=14.6, ask=14.8),
        intent,
        now=NOW,
    )
    delivery = process_trade_intent(
        storage,
        intent,
        now=NOW,
        settings=SimpleNamespace(enabled=False),
    )
    terminal = advance_put_shadow_candidates(
        storage,
        _latest(NOW + timedelta(seconds=1), spx=7551.08, bid=14.5, ask=14.6),
        {"status": "observing"},
        now=NOW + timedelta(seconds=1),
    )
    exact = next(iter(terminal["lanes"].values()))
    report = build_put_candidate_report(
        {
            "level_decision": {
                "event_id": intent["event_id"],
                "phase": "confirmed",
                "thesis": intent["thesis"],
                "direction": intent["direction"],
                "level_kind": intent["level_kind"],
                "formal_signal": True,
                "levels": {
                    "put_wall": 7500.0,
                    "flip_low": 7560.0,
                    "flip_high": 7565.0,
                    "call_wall": 7600.0,
                },
            },
            "trade_intent": intent,
        }
    )

    assert normal["phase"] == "observing"
    assert shadow["phase"] == "armed"
    assert delivery == {
        "attempted": False,
        "delivered": False,
        "reason": "shadow_ready",
    }
    assert exact["phase"] == "quote_reached_entry"
    assert exact["execution_claim"] == "none"
    assert virtual_entry_intent(exact) == {}
    flip = report["candidates"][0]
    assert flip["wall_signal"]["status"] == "CONFIRMED"
    assert flip["execution_eligible"]["eligible"] is False
    assert flip["priority"]["status"] == "NORMAL"


def test_put_shadow_collector_requires_read_only_authority_contract(tmp_path) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    intent = _shadow_intent(
        lane="long_0dte_rth_flip_low_breakdown_put_shadow",
        event_id="level:unsafe-shadow",
    )
    intent["execution_eligible"] = True

    result = advance_put_shadow_candidates(
        storage,
        _latest(NOW, spx=7551.08, bid=14.6, ask=14.8),
        intent,
        now=NOW,
    )

    assert result["phase"] == "observing"
    state = json.loads((tmp_path / "latest" / "put_shadow_candidate_state.json").read_text())
    assert state["active_by_lane"] == {}
    assert not (
        tmp_path / "features" / "trade_candidates" / "date=2026-07-15" / "events.jsonl"
    ).exists()


def test_put_shadow_collector_requires_frozen_half_open_window_contract(tmp_path) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    lane = "long_0dte_rth_flip_low_breakdown_put_shadow"
    valid = _shadow_intent(lane=lane, event_id="level:window-contract")

    for field, value in (
        ("trade_intent_contract_version", "unknown-window-contract"),
        ("entry_window_start_at", "2026-07-15T13:44:00+00:00"),
        ("hard_exit_at", "2026-07-15T17:01:00+00:00"),
        ("valid_until", "2026-07-15T17:05:00+00:00"),
    ):
        intent = {**valid, field: value, "event_id": f"level:invalid-{field}"}
        result = advance_put_shadow_candidates(
            storage,
            _latest(NOW, spx=7551.08, bid=14.6, ask=14.8),
            intent,
            now=NOW,
        )
        assert result["phase"] == "observing"

    before_open = datetime(2026, 7, 15, 13, 44, 59, tzinfo=UTC)
    result = advance_put_shadow_candidates(
        storage,
        _latest(before_open, spx=7551.08, bid=14.6, ask=14.8),
        valid,
        now=before_open,
    )
    assert result["phase"] == "observing"
    state = json.loads((tmp_path / "latest" / "put_shadow_candidate_state.json").read_text())
    assert state["active_by_lane"] == {}


def test_put_shadow_active_expires_at_1300_before_quote_or_level_events(tmp_path) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    lane = "long_0dte_rth_flip_low_breakdown_put_shadow"
    arm_at = datetime(2026, 7, 15, 16, 59, 50, tzinfo=UTC)
    hard_exit_at = datetime(2026, 7, 15, 17, 0, tzinfo=UTC)
    intent = {
        **_shadow_intent(
            lane=lane,
            event_id="level:hard-exit",
        ),
        "valid_until": hard_exit_at.isoformat(),
        "expires_at": hard_exit_at.isoformat(),
    }
    armed = advance_put_shadow_candidates(
        storage,
        _latest(arm_at, spx=7551.08, bid=14.6, ask=14.8),
        intent,
        now=arm_at,
    )
    terminal = advance_put_shadow_candidates(
        storage,
        _latest(hard_exit_at, spx=7549.0, bid=14.5, ask=14.6),
        {"status": "observing"},
        now=hard_exit_at,
    )

    assert armed["phase"] == "armed"
    assert terminal["lanes"][lane]["phase"] == "expired"
    assert terminal["lanes"][lane]["terminal_reason"] == "entry_window_expired"
    assert terminal["entry_observation"] is None


def test_persisted_put_shadow_active_cannot_observe_entry_before_0945(tmp_path) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    lane = "long_0dte_rth_flip_low_breakdown_put_shadow"
    intent = _shadow_intent(lane=lane, event_id="level:persisted-before-open")
    advance_put_shadow_candidates(
        storage,
        _latest(NOW, spx=7551.08, bid=14.6, ask=14.8),
        intent,
        now=NOW,
    )

    before_open = datetime(2026, 7, 15, 13, 44, 59, tzinfo=UTC)
    terminal = advance_put_shadow_candidates(
        storage,
        _latest(before_open, spx=7549.0, bid=14.5, ask=14.6),
        {"status": "observing"},
        now=before_open,
    )

    assert terminal["lanes"][lane]["phase"] == "expired"
    assert terminal["lanes"][lane]["terminal_reason"] == "put_shadow_entry_window_not_open"
    assert terminal["entry_observation"] is None


def test_persisted_put_shadow_active_cannot_disable_shadow_contract_at_1300(
    tmp_path,
) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    lane = "long_0dte_rth_flip_low_breakdown_put_shadow"
    arm_at = datetime(2026, 7, 15, 16, 59, 50, tzinfo=UTC)
    hard_exit_at = datetime(2026, 7, 15, 17, 0, tzinfo=UTC)
    intent = {
        **_shadow_intent(lane=lane, event_id="level:persisted-contract-drift"),
        "valid_until": hard_exit_at.isoformat(),
        "expires_at": hard_exit_at.isoformat(),
    }
    advance_put_shadow_candidates(
        storage,
        _latest(arm_at, spx=7551.08, bid=14.6, ask=14.8),
        intent,
        now=arm_at,
    )
    state_path = tmp_path / "latest" / "put_shadow_candidate_state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    active = state["active_by_lane"][lane]
    active["shadow_mode"] = False
    active["valid_until"] = "2026-07-15T17:05:00+00:00"
    active["source_intent"]["valid_until"] = "2026-07-15T17:05:00+00:00"
    state_path.write_text(json.dumps(state), encoding="utf-8")

    terminal = advance_put_shadow_candidates(
        storage,
        _latest(hard_exit_at, spx=7549.0, bid=14.5, ask=14.6),
        {"status": "observing"},
        now=hard_exit_at,
    )

    assert terminal["lanes"][lane]["phase"] == "expired"
    assert terminal["lanes"][lane]["terminal_reason"] == ("put_shadow_window_contract_invalid")
    assert terminal["entry_observation"] is None


@pytest.mark.parametrize(
    ("lane", "wrong_field", "wrong_value"),
    [
        (
            "long_0dte_rth_flip_low_breakdown_put_shadow",
            "level_kind",
            "call_wall",
        ),
        (
            "long_0dte_rth_upper_rejection_put_shadow",
            "play",
            "level_breakout_put",
        ),
        (
            "long_0dte_rth_upper_rejection_put_shadow",
            "thesis",
            "breakout",
        ),
    ],
)
def test_put_shadow_collector_rejects_cross_lane_contracts(
    tmp_path,
    lane: str,
    wrong_field: str,
    wrong_value: str,
) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    intent = _shadow_intent(lane=lane, event_id=f"level:wrong-{wrong_field}")
    intent[wrong_field] = wrong_value

    result = advance_put_shadow_candidates(
        storage,
        _latest(NOW, spx=7551.08, bid=14.6, ask=14.8),
        intent,
        now=NOW,
    )

    assert result["phase"] == "observing"
    state = json.loads((tmp_path / "latest" / "put_shadow_candidate_state.json").read_text())
    assert state["active_by_lane"] == {}


@pytest.mark.parametrize(
    ("field", "wrong_value"),
    [
        ("contract_id", "option:SPX:SPXW:20260716:7560:P"),
        ("contract_id", "option:SPX:SPX:20260715:7560:P"),
        ("contract_id", "option:SPY:SPXW:20260715:7560:P"),
        ("session_id", "2026-07-16"),
    ],
)
def test_put_shadow_collector_requires_same_day_spxw_put_contract(
    tmp_path,
    field: str,
    wrong_value: str,
) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    intent = _shadow_intent(
        lane="long_0dte_rth_flip_low_breakdown_put_shadow",
        event_id=f"level:wrong-contract-{field}",
    )
    intent[field] = wrong_value

    result = advance_put_shadow_candidates(
        storage,
        _latest(NOW, spx=7551.08, bid=14.6, ask=14.8),
        intent,
        now=NOW,
    )

    assert result["phase"] == "observing"
    state = json.loads((tmp_path / "latest" / "put_shadow_candidate_state.json").read_text())
    assert state["active_by_lane"] == {}


def test_put_shadow_candidate_policy_versions_the_exact_lifecycle_contract() -> None:
    shadow = _shadow_intent(
        lane="long_0dte_rth_flip_low_breakdown_put_shadow",
        event_id="level:versioned-shadow",
    )

    call_policy = _armed_candidate(_call_intent(), now=NOW)["policy_version"]
    shadow_policy = _armed_candidate(shadow, now=NOW)["policy_version"]
    repeated_policy = _armed_candidate(
        {**shadow, "event_id": "level:rearmed-shadow"},
        now=NOW,
    )["policy_version"]

    assert str(call_policy).startswith("trade_candidate.v3+sha256:")
    assert str(shadow_policy).startswith("trade_candidate.v3+sha256:")
    assert shadow_policy != call_policy
    assert repeated_policy == shadow_policy


@pytest.mark.parametrize("age_seconds", [6.0, 15.0])
def test_put_shadow_exact_quote_accepts_source_and_transport_up_to_fifteen_seconds(
    tmp_path,
    age_seconds: float,
) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    intent = _shadow_intent(
        lane="long_0dte_rth_flip_low_breakdown_put_shadow",
        event_id="level:freshness",
    )
    advance_put_shadow_candidates(
        storage,
        _latest(NOW, spx=7551.08, bid=14.6, ask=14.8),
        intent,
        now=NOW,
    )
    observed_at = NOW + timedelta(seconds=20)
    result = advance_put_shadow_candidates(
        storage,
        _latest(
            observed_at,
            spx=7551.08,
            bid=14.5,
            ask=14.6,
            option_source_age_seconds=age_seconds,
            option_transport_age_seconds=age_seconds,
        ),
        {"status": "observing"},
        now=observed_at,
    )

    terminal = next(iter(result["lanes"].values()))
    observation = terminal["entry_observation"]
    assert terminal["phase"] == "quote_reached_entry"
    assert observation["exact_quote_policy_version"] == (PUT_SHADOW_EXACT_QUOTE_POLICY_VERSION)
    assert observation["exact_quote_max_age_seconds"] == (PUT_SHADOW_EXACT_QUOTE_MAX_AGE_SECONDS)
    assert observation["quote_source_age_seconds"] == age_seconds
    assert observation["quote_transport_age_seconds"] == age_seconds
    assert observation["exact_quote_freshness_ok"] is True


@pytest.mark.parametrize(
    ("source_age_seconds", "transport_age_seconds"),
    [
        (None, 0.0),
        (15.001, 0.0),
        (0.0, 15.001),
        (-0.001, 0.0),
        (0.0, -0.001),
    ],
)
def test_put_shadow_exact_quote_rejects_stale_or_future_source_and_transport(
    tmp_path,
    source_age_seconds: float | None,
    transport_age_seconds: float,
) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    intent = _shadow_intent(
        lane="long_0dte_rth_flip_low_breakdown_put_shadow",
        event_id="level:bad-freshness",
    )
    advance_put_shadow_candidates(
        storage,
        _latest(NOW, spx=7551.08, bid=14.6, ask=14.8),
        intent,
        now=NOW,
    )
    observed_at = NOW + timedelta(seconds=20)
    result = advance_put_shadow_candidates(
        storage,
        _latest(
            observed_at,
            spx=7551.08,
            bid=14.5,
            ask=14.6,
            option_source_age_seconds=source_age_seconds,
            option_transport_age_seconds=transport_age_seconds,
        ),
        {"status": "observing"},
        now=observed_at,
    )

    active = next(iter(result["lanes"].values()))
    observation = active["last_observation"]
    assert active["phase"] == "armed"
    assert observation["quote_pricing_allowed"] is False
    assert observation["exact_quote_freshness_ok"] is False


def test_put_shadow_same_event_state_loss_keeps_one_audited_lifecycle(tmp_path) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    lane = "long_0dte_rth_flip_low_breakdown_put_shadow"
    semantic_key = f"2026-07-15|level_breakout_put|7560|{OPTION_ID}"
    original = _shadow_intent(
        lane=lane,
        event_id="level:first",
        intent_id="intent:stable-put",
        semantic_key=semantic_key,
    )
    first = advance_put_shadow_candidates(
        storage,
        _latest(NOW, spx=7551.08, bid=14.6, ask=14.8),
        original,
        now=NOW,
    )
    assert first["phase"] == "armed"

    state_path = tmp_path / "latest" / "put_shadow_candidate_state.json"
    state_path.unlink()
    replayed = advance_put_shadow_candidates(
        storage,
        _latest(NOW + timedelta(seconds=1), spx=7551.08, bid=14.6, ask=14.8),
        original,
        now=NOW + timedelta(seconds=1),
    )
    assert replayed["phase"] == "armed"
    assert replayed["candidate_id"] == "intent:stable-put|level:first"

    terminal_at = NOW + timedelta(seconds=2)
    terminal = advance_put_shadow_candidates(
        storage,
        _latest(terminal_at, spx=7551.08, bid=14.5, ask=14.6),
        {"status": "observing"},
        now=terminal_at,
    )
    assert next(iter(terminal["lanes"].values()))["phase"] == "quote_reached_entry"

    rows = [
        json.loads(line)
        for line in (
            tmp_path / "features" / "trade_candidates" / "date=2026-07-15" / "events.jsonl"
        )
        .read_text()
        .splitlines()
    ]
    assert [row["event"] for row in rows] == [
        "candidate_armed",
        "candidate_terminal",
    ]


def test_put_shadow_state_migrates_old_semantic_consumption_without_blocking_new_event(
    tmp_path,
) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    state_path = tmp_path / "latest" / "put_shadow_candidate_state.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "consumed_identity_keys": [
                    "intent_id:intent:stable-put",
                    "semantic_key:old-wall-scope",
                ],
                "active_by_lane": {},
                "completed_candidates": {},
            }
        ),
        encoding="utf-8",
    )
    intent = _shadow_intent(
        lane="long_0dte_rth_flip_low_breakdown_put_shadow",
        event_id="level:new-after-v1",
        intent_id="intent:stable-put",
        semantic_key="old-wall-scope",
    )

    result = advance_put_shadow_candidates(
        storage,
        _latest(NOW, spx=7551.08, bid=14.6, ask=14.8),
        intent,
        now=NOW,
    )

    assert result["phase"] == "armed"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert state["schema_version"] == 2
    assert state["identity_contract_version"] == "put_shadow_candidate_lifecycle.v2"
    assert state["consumed_identity_keys"] == ["candidate_id:intent:stable-put|level:new-after-v1"]


def test_put_shadow_new_event_rearms_same_semantic_wall_opportunity(tmp_path) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    lane = "long_0dte_rth_flip_low_breakdown_put_shadow"
    semantic_key = f"2026-07-15|level_breakout_put|7560|{OPTION_ID}"
    first = _shadow_intent(
        lane=lane,
        event_id="level:first",
        intent_id="intent:stable-put",
        semantic_key=semantic_key,
    )
    advance_put_shadow_candidates(
        storage,
        _latest(NOW, spx=7551.08, bid=14.6, ask=14.8),
        first,
        now=NOW,
    )
    first_terminal = advance_put_shadow_candidates(
        storage,
        _latest(NOW + timedelta(seconds=1), spx=7551.08, bid=14.5, ask=14.6),
        {"status": "observing"},
        now=NOW + timedelta(seconds=1),
    )
    assert next(iter(first_terminal["lanes"].values()))["phase"] == ("quote_reached_entry")

    retest = {
        **first,
        "event_id": "level:retest",
    }
    second = advance_put_shadow_candidates(
        storage,
        _latest(NOW + timedelta(seconds=2), spx=7551.08, bid=14.6, ask=14.8),
        retest,
        now=NOW + timedelta(seconds=2),
    )
    assert second["phase"] == "armed"
    assert second["candidate_id"] == "intent:stable-put|level:retest"
    assert second["candidate_id"] != "intent:stable-put|level:first"

    second_terminal = advance_put_shadow_candidates(
        storage,
        _latest(NOW + timedelta(seconds=3), spx=7551.08, bid=14.5, ask=14.6),
        {"status": "observing"},
        now=NOW + timedelta(seconds=3),
    )
    assert next(iter(second_terminal["lanes"].values()))["phase"] == ("quote_reached_entry")
    rows = [
        json.loads(line)
        for line in (
            tmp_path / "features" / "trade_candidates" / "date=2026-07-15" / "events.jsonl"
        )
        .read_text()
        .splitlines()
    ]
    assert [row["event"] for row in rows] == [
        "candidate_armed",
        "candidate_terminal",
        "candidate_armed",
        "candidate_terminal",
    ]
    assert {row["candidate_id"] for row in rows} == {
        "intent:stable-put|level:first",
        "intent:stable-put|level:retest",
    }


def _intent() -> dict[str, object]:
    return {
        "schema_version": 3,
        "policy_version": "rth_trade_intent.v3+sha256:test",
        "valid_until": (NOW + timedelta(seconds=90)).isoformat(),
        "coordinate": {
            "kind": "official_spx",
            "instrument_id": "index:SPX",
            "observed_value": 7551.08,
            "target_value": 7560.0,
            "spx_observed_value": 7551.08,
            "basis_points": 0.0,
            "as_of": NOW.isoformat(),
        },
        "block_reasons": [],
        "status": "trade_ready",
        "strategy_lane": "rth_confirmed_level",
        "shadow_mode": False,
        "execution_eligible": True,
        "quote_observation_eligible": False,
        "automatic_ordering": False,
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


def _call_intent() -> dict[str, object]:
    return {
        **_intent(),
        "strategy_lane": "long_0dte_rth_upside_breakout_pilot",
        "intent_id": "intent:test-call",
        "event_id": "level:test-call",
        "semantic_key": f"2026-07-15|level_breakout_call|7560|{CALL_OPTION_ID}",
        "direction": "up",
        "play": "level_breakout_call",
        "thesis": "breakout",
        "level_kind": "call_wall",
        "contract_id": CALL_OPTION_ID,
        "target_spx": 7563.0,
        "invalidation_spx": 7550.0,
    }


def _shadow_intent(
    *,
    lane: str,
    event_id: str,
    intent_id: str | None = None,
    semantic_key: str | None = None,
) -> dict[str, object]:
    play = (
        "level_breakout_put"
        if lane == "long_0dte_rth_flip_low_breakdown_put_shadow"
        else "level_fade_put"
    )
    thesis = "breakout" if play == "level_breakout_put" else "fade"
    level_kind = "flip_low" if thesis == "breakout" else "call_wall"
    entry_window_start_at = datetime(2026, 7, 15, 13, 45, tzinfo=UTC)
    hard_exit_at = datetime(2026, 7, 15, 17, 0, tzinfo=UTC)
    return {
        **_intent(),
        "status": "shadow_ready",
        "session_id": "2026-07-15",
        "intent_id": intent_id or f"intent:{event_id}",
        "event_id": event_id,
        "semantic_key": semantic_key or f"2026-07-15|{play}|7560.0000|{OPTION_ID}",
        "play": play,
        "thesis": thesis,
        "level_kind": level_kind,
        "strategy_lane": lane,
        "shadow_mode": True,
        "execution_eligible": False,
        "quote_observation_eligible": True,
        "automatic_ordering": False,
        "trade_intent_contract_version": "rth_lanes_0945_1300_put_shadow.v1",
        "entry_window_start_at": entry_window_start_at.isoformat(),
        "hard_exit_at": hard_exit_at.isoformat(),
    }


def _latest(
    now: datetime,
    *,
    spx: float,
    bid: float,
    ask: float,
    option_source_age_seconds: float | None = 0.0,
    option_transport_age_seconds: float = 0.0,
) -> LatestState:
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
        received_at=now - timedelta(seconds=option_transport_age_seconds),
        last_update_at=now - timedelta(seconds=option_transport_age_seconds),
        quote_time=(
            now - timedelta(seconds=option_source_age_seconds)
            if option_source_age_seconds is not None
            else None
        ),
        quality=MarketDataQuality.LIVE,
        bid=bid,
        ask=ask,
    )
    call_quote = Quote(
        instrument=InstrumentId.option(
            "SPX",
            expiry="20260715",
            strike=7560,
            right="C",
            trading_class="SPXW",
        ),
        provider=Provider.SCHWAB,
        received_at=now - timedelta(seconds=option_transport_age_seconds),
        last_update_at=now - timedelta(seconds=option_transport_age_seconds),
        quote_time=(
            now - timedelta(seconds=option_source_age_seconds)
            if option_source_age_seconds is not None
            else None
        ),
        quality=MarketDataQuality.LIVE,
        bid=bid,
        ask=ask,
    )
    return LatestState(
        created_at=now,
        as_of=now,
        quotes=(spx_quote, option_quote, call_quote),
        best_quotes=(spx_quote, option_quote, call_quote),
    )
