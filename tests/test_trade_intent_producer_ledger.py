from __future__ import annotations

import inspect
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from spx_spark.application.market_features import service
from spx_spark.application.market_features.trade_intent_producer_ledger import (
    TradeIntentProducerLedgerError,
    producer_ledger_events_path,
    producer_ledger_state_path,
    record_trade_intent_producer_observation,
)
from spx_spark.application.market_features.trade_intent_runtime import (
    _trade_ready_delivery_event_id,
)
from spx_spark.config import StorageSettings
from spx_spark.market_calendar import ET
from spx_spark.settings.market_features import MarketFeatureSettings


UTC = timezone.utc
NOW = datetime(2026, 7, 30, 14, 1, tzinfo=UTC)


def _storage(tmp_path: Path) -> StorageSettings:
    return StorageSettings(
        data_root=str(tmp_path),
        latest_state_path=str(tmp_path / "latest" / "state.json"),
        raw_file_name="quotes.jsonl",
        include_raw_payload=False,
        latest_stale_after_seconds=90.0,
        slow_index_stale_after_seconds=180.0,
        slow_index_labels=frozenset(),
    )


def _ready_intent(*, event_id: str = "level:first") -> dict[str, object]:
    return {
        "status": "trade_ready",
        "intent_id": "intent:producer-ledger",
        "event_id": event_id,
        "semantic_key": "2026-07-30|breakout|up|7550|option:SPX:SPXW:20260730:7550:C",
        "contract_id": "option:SPX:SPXW:20260730:7550:C",
        "play": "breakout_call",
        "expires_at": (NOW + timedelta(minutes=2)).isoformat(),
    }


def _events(storage: StorageSettings) -> list[dict[str, object]]:
    path = producer_ledger_events_path(storage, NOW.astimezone(ET).date())
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_records_one_heartbeat_per_rth_slot_and_each_new_expectation(
    tmp_path: Path,
) -> None:
    storage = _storage(tmp_path)
    first_intent = _ready_intent()

    first = record_trade_intent_producer_observation(
        storage,
        first_intent,
        now=NOW,
    )
    duplicate = record_trade_intent_producer_observation(
        storage,
        first_intent,
        now=NOW + timedelta(minutes=2),
    )
    rearmed = record_trade_intent_producer_observation(
        storage,
        _ready_intent(event_id="level:second"),
        now=NOW + timedelta(minutes=3),
    )
    next_slot = record_trade_intent_producer_observation(
        storage,
        {"status": "blocked", "event_id": "level:blocked"},
        now=NOW + timedelta(minutes=5),
    )
    events = _events(storage)

    assert first["heartbeat"] == "recorded"
    assert first["delivery_expectation"] == "recorded"
    assert duplicate["heartbeat"] == "duplicate"
    assert duplicate["delivery_expectation"] == "duplicate"
    assert rearmed["heartbeat"] == "duplicate"
    assert rearmed["delivery_expectation"] == "recorded"
    assert next_slot["heartbeat"] == "recorded"
    assert next_slot["delivery_expectation"] == "not_trade_ready"
    assert [row["record_type"] for row in events].count("rth_5m_heartbeat") == 2
    assert [row["record_type"] for row in events].count("trade_ready_delivery_expectation") == 2
    expectations = [
        row for row in events if row["record_type"] == "trade_ready_delivery_expectation"
    ]
    assert expectations[0]["semantic_key"] == first_intent["semantic_key"]
    assert expectations[0]["delivery_event_id"] == _trade_ready_delivery_event_id(first_intent)
    assert expectations[0]["delivery_event_id"] != expectations[1]["delivery_event_id"]


def test_jsonl_is_canonical_for_crash_reconciliation_without_duplicate_append(
    tmp_path: Path,
) -> None:
    storage = _storage(tmp_path)
    intent = _ready_intent()
    record_trade_intent_producer_observation(storage, intent, now=NOW)
    original = _events(storage)
    producer_ledger_state_path(storage).unlink()

    replay = record_trade_intent_producer_observation(
        storage,
        intent,
        now=NOW + timedelta(minutes=1),
    )
    rebuilt = json.loads(producer_ledger_state_path(storage).read_text(encoding="utf-8"))

    assert replay["heartbeat"] == "duplicate"
    assert replay["delivery_expectation"] == "duplicate"
    assert _events(storage) == original
    assert len(rebuilt["records"]) == 2


def test_deadline_latency_and_ttl_breach_are_persisted_in_ledger(
    tmp_path: Path,
) -> None:
    storage = _storage(tmp_path)
    intent = _ready_intent()
    action_now = NOW + timedelta(seconds=125)

    result = record_trade_intent_producer_observation(
        storage,
        intent,
        now=NOW,
        action_now=action_now,
    )
    deadline = result["deadline"]

    assert deadline["evaluation_to_action_revalidation_ms"] == 125_000.0
    assert deadline["intent_ttl_seconds_at_evaluation"] == 120.0
    assert deadline["ttl_remaining_at_action_seconds"] == -5.0
    assert deadline["action_revalidation_exceeded_ttl"] is True
    assert all(row["deadline"] == deadline for row in _events(storage))


def test_malformed_jsonl_fails_closed_and_is_not_extended(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    path = producer_ledger_events_path(storage, NOW.astimezone(ET).date())
    path.parent.mkdir(parents=True)
    original = b'{"record_id":"truncated"'
    path.write_bytes(original)

    with pytest.raises(
        TradeIntentProducerLedgerError,
        match="producer_ledger_jsonl_invalid",
    ):
        record_trade_intent_producer_observation(
            storage,
            _ready_intent(),
            now=NOW,
        )

    assert path.read_bytes() == original


def test_outbox_failure_happens_after_durable_ready_expectation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = _storage(tmp_path)
    intent = _ready_intent()

    def fail_delivery(*_args: object, **_kwargs: object) -> dict[str, object]:
        assert any(
            row.get("delivery_event_id") == _trade_ready_delivery_event_id(intent)
            for row in _events(storage)
        )
        raise RuntimeError("outbox unavailable")

    monkeypatch.setattr(service, "process_trade_intent", fail_delivery)

    with pytest.raises(RuntimeError, match="outbox unavailable"):
        service._record_and_process_trade_intent(
            storage,
            intent,
            now=NOW,
            feature_policy=MarketFeatureSettings(),
            order_policy=object(),
            expected_policy_version="rth_trade_intent.v3+sha256:test",
            action_now=NOW,
        )

    expectations = [
        row for row in _events(storage) if row["record_type"] == "trade_ready_delivery_expectation"
    ]
    assert len(expectations) == 1
    assert expectations[0]["semantic_key"] == intent["semantic_key"]


def test_action_clock_is_sampled_after_durable_ledger_before_delivery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = _storage(tmp_path)
    intent = _ready_intent()
    revalidation_at = NOW + timedelta(seconds=125)
    calls: list[str] = []

    def record_ledger(*_args: object, **kwargs: object) -> dict[str, object]:
        calls.append("ledger_fsync")
        assert "action_now" not in kwargs
        return {
            "ok": True,
            "observed_at": NOW.isoformat(),
            "heartbeat": "recorded",
            "delivery_expectation": "recorded",
            "deadline": {"action_revalidation_at": NOW.isoformat()},
        }

    def action_clock() -> datetime:
        calls.append("action_clock")
        return revalidation_at

    def deliver(*_args: object, **kwargs: object) -> dict[str, object]:
        calls.append("delivery")
        assert kwargs["action_now"] == revalidation_at
        return {"attempted": False, "reason": "test"}

    monkeypatch.setattr(
        service,
        "record_trade_intent_producer_observation",
        record_ledger,
    )
    monkeypatch.setattr(service, "process_trade_intent", deliver)

    diagnostics, delivery = service._record_and_process_trade_intent(
        storage,
        intent,
        now=NOW,
        feature_policy=MarketFeatureSettings(),
        order_policy=object(),
        expected_policy_version="rth_trade_intent.v3+sha256:test",
        action_clock=action_clock,
    )

    assert calls == ["ledger_fsync", "action_clock", "delivery"]
    assert diagnostics["deadline"]["action_revalidation_at"] == revalidation_at.isoformat()
    assert diagnostics["deadline"]["evaluation_to_action_revalidation_ms"] == 125_000.0
    assert diagnostics["deadline"]["ttl_remaining_at_action_seconds"] == -5.0
    assert diagnostics["deadline"]["action_revalidation_exceeded_ttl"] is True
    assert delivery == {"attempted": False, "reason": "test"}


def test_ledger_write_failure_is_diagnostic_and_does_not_block_delivery(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = _storage(tmp_path)
    calls: list[str] = []

    def fail_ledger(*_args: object, **_kwargs: object) -> dict[str, object]:
        calls.append("ledger")
        raise OSError("disk unavailable")

    def deliver(*_args: object, **_kwargs: object) -> dict[str, object]:
        calls.append("delivery")
        return {"attempted": True, "accepted": True}

    monkeypatch.setattr(
        service,
        "record_trade_intent_producer_observation",
        fail_ledger,
    )
    monkeypatch.setattr(service, "process_trade_intent", deliver)

    diagnostics, delivery = service._record_and_process_trade_intent(
        storage,
        _ready_intent(),
        now=NOW,
        feature_policy=MarketFeatureSettings(),
        order_policy=object(),
        expected_policy_version="rth_trade_intent.v3+sha256:test",
        action_now=NOW,
    )

    assert calls == ["ledger", "delivery"]
    assert diagnostics["ok"] is False
    assert diagnostics["observed_at"] == NOW.isoformat()
    assert diagnostics["heartbeat"] == "write_failed"
    assert diagnostics["delivery_expectation"] == "write_failed"
    assert diagnostics["error"] == "OSError:disk unavailable"
    assert diagnostics["deadline"]["evaluation_to_action_revalidation_ms"] == 0.0
    assert diagnostics["deadline"]["action_revalidation_exceeded_ttl"] is False
    assert delivery == {"attempted": True, "accepted": True}


def test_trade_critical_delivery_precedes_greek_and_spring_research() -> None:
    source = inspect.getsource(service.run)

    confirmed_gate = source.index("confirmed_gate = reconcile_confirmed_gate")
    producer_delivery = source.index(
        "producer_ledger, intent_delivery = _record_and_process_trade_intent"
    )
    spring_ticket_context = source.index(
        '"spring_gamma": spring_gamma_operator.spring_gamma_operator_view'
    )
    greek_research = source.index("focused = build_zero_dte_greeks_reference")
    spring_research = source.index("spring_gamma_v3 = _process_spring_gamma_v3_shadow")

    assert (
        confirmed_gate
        < spring_ticket_context
        < producer_delivery
        < greek_research
        < spring_research
    )
    assert "trade_intent_evaluation_to_action_revalidation_ms" in source
    assert "trade_intent_action_revalidation_exceeded_ttl" in source
