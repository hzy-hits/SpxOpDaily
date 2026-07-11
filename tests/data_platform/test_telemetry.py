from __future__ import annotations

import stat
from datetime import date, datetime, timedelta, timezone

from spx_spark.data_platform.adapters.memory import InMemoryDecisionLedger
from spx_spark.data_platform.contracts import (
    DecisionRecord,
    DeliveryRecord,
    EventRecord,
    OutcomeRecord,
)
from spx_spark.data_platform.telemetry import FallbackSpool, OperationalTelemetry


NOW = datetime(2026, 7, 10, 14, 0, tzinfo=timezone.utc)


class FailingLedger(InMemoryDecisionLedger):
    def __init__(self) -> None:
        super().__init__()
        self.fail = True

    def record_event(self, event: EventRecord) -> None:
        if self.fail:
            raise OSError("ledger unavailable")
        super().record_event(event)


def event() -> EventRecord:
    return EventRecord(
        event_key="evt_test",
        event_type="shock",
        session_date=date(2026, 7, 10),
        source_at=NOW,
        available_at=NOW + timedelta(milliseconds=50),
        data_quality="live",
    )


def decision() -> DecisionRecord:
    return DecisionRecord(
        decision_id="dec_test",
        event_key="evt_test",
        strategy_name="intraday",
        strategy_version="v1",
        decision_at=NOW + timedelta(seconds=1),
        available_at=NOW + timedelta(milliseconds=50),
        status="confirmed",
        action="notify",
        side="call",
    )


def test_failed_realtime_write_spools_and_replays(tmp_path) -> None:
    ledger = FailingLedger()
    spool = FallbackSpool(tmp_path / "fallback.jsonl")
    telemetry = OperationalTelemetry(ledger, spool)

    result = telemetry.record_decision_bundle(event=event(), decision=decision())

    assert result.status == "spooled"
    assert stat.S_IMODE(spool.path.stat().st_mode) == 0o600
    assert ledger.get_decision("dec_test") is None

    ledger.fail = False
    replayed = telemetry.replay_fallback()

    assert replayed.replayed == 1
    assert replayed.retained == 0
    assert ledger.get_decision("dec_test") == decision()
    assert spool.path.read_text(encoding="utf-8") == ""


def test_delivery_and_outcome_use_same_fail_open_path(tmp_path) -> None:
    ledger = InMemoryDecisionLedger()
    telemetry = OperationalTelemetry(ledger, FallbackSpool(tmp_path / "fallback.jsonl"))
    assert telemetry.record_decision_bundle(event=event(), decision=decision()).status == "recorded"
    delivery = DeliveryRecord(
        delivery_id="delivery_test",
        decision_id="dec_test",
        channel="bark",
        status="sent",
        attempted_at=NOW + timedelta(seconds=2),
        sent_at=NOW + timedelta(seconds=2),
    )
    outcome = OutcomeRecord(
        outcome_id="outcome_test",
        event_key="evt_test",
        decision_id="dec_test",
        horizon_minutes=5,
        status="complete",
        target_at=NOW + timedelta(minutes=5),
        sampled_at=NOW + timedelta(minutes=5),
        spx_return_bps=10.0,
    )

    assert telemetry.record_delivery(delivery).status == "recorded"
    assert telemetry.record_outcome(outcome).status == "recorded"
    assert tuple(ledger.list_deliveries("dec_test")) == (delivery,)
    assert tuple(ledger.list_outcomes("dec_test")) == (outcome,)


def test_fallback_spool_capacity_is_bounded_without_raising_to_caller(tmp_path) -> None:
    ledger = FailingLedger()
    spool = FallbackSpool(tmp_path / "fallback.jsonl", max_bytes=1)

    result = OperationalTelemetry(ledger, spool).record_event(event())

    assert result.status == "error"
    assert result.error is not None and "FallbackSpoolCapacityError" in result.error
    assert not spool.path.exists()
