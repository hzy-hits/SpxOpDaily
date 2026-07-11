from __future__ import annotations

import json
import stat
from datetime import date, datetime, timedelta, timezone

import pytest

from spx_spark.data_platform.adapters.memory import InMemoryDecisionLedger
from spx_spark.data_platform.cli import _spool_replay_exit_code, _spool_replay_status
from spx_spark.data_platform.contracts import (
    DecisionRecord,
    DeliveryRecord,
    EventRecord,
    OutcomeRecord,
)
from spx_spark.data_platform.ports import LedgerConflictError
from spx_spark.data_platform.telemetry import (
    FallbackSpool,
    OperationalTelemetry,
    SpoolReplayResult,
)


NOW = datetime(2026, 7, 10, 14, 0, tzinfo=timezone.utc)


class FailingLedger(InMemoryDecisionLedger):
    def __init__(self) -> None:
        super().__init__()
        self.fail = True

    def record_event(self, event: EventRecord) -> None:
        if self.fail:
            raise OSError("ledger unavailable")
        super().record_event(event)


class ConflictingLedger(InMemoryDecisionLedger):
    def record_event(self, event: EventRecord) -> None:
        raise LedgerConflictError("conflicting immutable event record")


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


def delivery() -> DeliveryRecord:
    return DeliveryRecord(
        delivery_id="delivery_test",
        decision_id="dec_test",
        channel="bark",
        status="sent",
        attempted_at=NOW + timedelta(seconds=2),
        sent_at=NOW + timedelta(seconds=2),
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
    assert replayed.quarantined == 0
    assert replayed.failures == 0
    assert ledger.get_decision("dec_test") == decision()
    assert spool.path.read_text(encoding="utf-8") == ""


def test_delivery_and_outcome_use_same_fail_open_path(tmp_path) -> None:
    ledger = InMemoryDecisionLedger()
    telemetry = OperationalTelemetry(ledger, FallbackSpool(tmp_path / "fallback.jsonl"))
    assert telemetry.record_decision_bundle(event=event(), decision=decision()).status == "recorded"
    delivery_record = delivery()
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

    assert telemetry.record_delivery(delivery_record).status == "recorded"
    assert telemetry.record_outcome(outcome).status == "recorded"
    assert tuple(ledger.list_deliveries("dec_test")) == (delivery_record,)
    assert tuple(ledger.list_outcomes("dec_test")) == (outcome,)


def test_fallback_spool_capacity_is_bounded_without_raising_to_caller(tmp_path) -> None:
    ledger = FailingLedger()
    spool = FallbackSpool(tmp_path / "fallback.jsonl", max_bytes=1)

    result = OperationalTelemetry(ledger, spool).record_event(event())

    assert result.status == "error"
    assert result.error is not None and "FallbackSpoolCapacityError" in result.error
    assert not spool.path.exists()


def test_terminal_realtime_error_is_visible_without_growing_spool(tmp_path) -> None:
    spool = FallbackSpool(tmp_path / "fallback.jsonl")

    result = OperationalTelemetry(ConflictingLedger(), spool).record_event(event())

    assert result.status == "error"
    assert result.error is not None and "LedgerConflictError" in result.error
    assert not spool.path.exists()


def test_reference_error_spools_until_parent_is_available(tmp_path) -> None:
    ledger = InMemoryDecisionLedger()
    spool = FallbackSpool(tmp_path / "fallback.jsonl")
    telemetry = OperationalTelemetry(ledger, spool)

    assert telemetry.record_delivery(delivery()).status == "spooled"
    assert telemetry.record_decision_bundle(event=event(), decision=decision()).status == "recorded"

    result = telemetry.replay_fallback()

    assert result.replayed == 1
    assert result.retained == 0
    assert result.quarantined == 0
    assert tuple(ledger.list_deliveries("dec_test")) == (delivery(),)


def test_unresolved_reference_is_quarantined_after_dependency_retry(tmp_path) -> None:
    ledger = InMemoryDecisionLedger()
    spool = FallbackSpool(tmp_path / "fallback.jsonl")
    telemetry = OperationalTelemetry(ledger, spool)
    assert telemetry.record_delivery(delivery()).status == "spooled"

    result = telemetry.replay_fallback()

    assert result.invalid == 1
    assert result.quarantined == 1
    assert result.retained == 0
    entry = json.loads(spool.dead_letter_path.read_text(encoding="utf-8"))
    assert entry["reason_type"] == "LedgerReferenceError"


def test_terminal_replay_is_durably_quarantined_and_removed(tmp_path) -> None:
    spool = FallbackSpool(tmp_path / "fallback.jsonl")
    unavailable = FailingLedger()
    assert OperationalTelemetry(unavailable, spool).record_event(event()).status == "spooled"
    raw_line = spool.path.read_text(encoding="utf-8").strip()

    result = spool.replay(ConflictingLedger())

    assert result == SpoolReplayResult(
        replayed=0,
        retained=0,
        invalid=1,
        quarantined=1,
        failures=0,
    )
    assert spool.path.read_text(encoding="utf-8") == ""
    assert stat.S_IMODE(spool.dead_letter_path.stat().st_mode) == 0o600
    rows = [
        json.loads(line)
        for line in spool.dead_letter_path.read_text(encoding="utf-8").splitlines()
    ]
    assert len(rows) == 1
    assert rows[0]["reason_type"] == "LedgerConflictError"
    assert rows[0]["raw_line"] == raw_line
    assert rows[0]["line_sha256"]
    assert rows[0]["record_id"]


def test_identical_terminal_lines_preserve_each_occurrence(tmp_path) -> None:
    spool = FallbackSpool(tmp_path / "fallback.jsonl")
    unavailable = FailingLedger()
    assert OperationalTelemetry(unavailable, spool).record_event(event()).status == "spooled"
    line = spool.path.read_text(encoding="utf-8")
    spool.path.write_text(line + line, encoding="utf-8")

    result = spool.replay(ConflictingLedger())

    rows = [
        json.loads(row)
        for row in spool.dead_letter_path.read_text(encoding="utf-8").splitlines()
    ]
    assert result.quarantined == 2
    assert len(rows) == 2
    assert rows[0]["line_sha256"] == rows[1]["line_sha256"]
    assert rows[0]["record_id"] != rows[1]["record_id"]


def test_invalid_envelope_is_quarantined_instead_of_retried(tmp_path) -> None:
    spool = FallbackSpool(tmp_path / "fallback.jsonl")
    spool.path.write_text("not-json\n", encoding="utf-8")

    result = spool.replay(InMemoryDecisionLedger())

    assert result.invalid == 1
    assert result.quarantined == 1
    assert result.retained == 0
    entry = json.loads(spool.dead_letter_path.read_text(encoding="utf-8"))
    assert entry["raw_line"] == "not-json"
    assert entry["reason_type"] == "JSONDecodeError"


def test_transient_replay_failure_stays_in_active_spool(tmp_path) -> None:
    ledger = FailingLedger()
    spool = FallbackSpool(tmp_path / "fallback.jsonl")
    assert OperationalTelemetry(ledger, spool).record_event(event()).status == "spooled"
    original = spool.path.read_text(encoding="utf-8")

    result = spool.replay(ledger)

    assert result.replayed == 0
    assert result.invalid == 0
    assert result.quarantined == 0
    assert result.retained == 1
    assert result.failures == 0
    assert spool.path.read_text(encoding="utf-8") == original
    assert not spool.dead_letter_path.exists()


def test_unresolved_reference_is_retained_during_same_batch_storage_failure(tmp_path) -> None:
    ledger = FailingLedger()
    spool = FallbackSpool(tmp_path / "fallback.jsonl")
    telemetry = OperationalTelemetry(ledger, spool)
    assert telemetry.record_event(event()).status == "spooled"
    assert telemetry.record_delivery(delivery()).status == "spooled"
    original = spool.path.read_text(encoding="utf-8")

    result = telemetry.replay_fallback()

    assert result.invalid == 0
    assert result.quarantined == 0
    assert result.retained == 2
    assert spool.path.read_text(encoding="utf-8") == original
    assert not spool.dead_letter_path.exists()


def test_dead_letter_write_failure_preserves_active_record(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spool = FallbackSpool(tmp_path / "fallback.jsonl")
    unavailable = FailingLedger()
    assert OperationalTelemetry(unavailable, spool).record_event(event()).status == "spooled"
    original = spool.path.read_text(encoding="utf-8")

    def fail_dead_letter(_records) -> None:
        raise OSError("disk unavailable")

    monkeypatch.setattr(spool, "_append_dead_letters", fail_dead_letter)
    result = spool.replay(ConflictingLedger())

    assert result.invalid == 1
    assert result.quarantined == 0
    assert result.retained == 1
    assert result.failures == 1
    assert spool.path.read_text(encoding="utf-8") == original


def test_dead_letter_hash_deduplicates_after_interrupted_cleanup(tmp_path) -> None:
    spool = FallbackSpool(tmp_path / "fallback.jsonl")
    unavailable = FailingLedger()
    assert OperationalTelemetry(unavailable, spool).record_event(event()).status == "spooled"
    original = spool.path.read_text(encoding="utf-8")

    first = spool.replay(ConflictingLedger())
    assert first.quarantined == 1
    dead_letter = spool.dead_letter_path.read_text(encoding="utf-8")
    spool.path.write_text(original, encoding="utf-8")

    second = spool.replay(ConflictingLedger())

    assert second.quarantined == 1
    assert spool.dead_letter_path.read_text(encoding="utf-8") == dead_letter
    assert spool.path.read_text(encoding="utf-8") == ""


@pytest.mark.parametrize(
    ("result", "status", "exit_code"),
    (
        (SpoolReplayResult(1, 0, 0, 0), "ok", 0),
        (SpoolReplayResult(0, 0, 2, 2), "quarantined", 0),
        (SpoolReplayResult(0, 1, 0, 0), "partial", 1),
        (SpoolReplayResult(0, 0, 1, 0, failures=1), "partial", 1),
    ),
)
def test_replay_cli_status_and_exit_semantics(result, status, exit_code) -> None:
    assert _spool_replay_status(result) == status
    assert _spool_replay_exit_code(result) == exit_code
