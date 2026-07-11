from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from spx_spark.data_platform.adapters.memory import InMemoryDecisionLedger
from spx_spark.data_platform.adapters.sqlite_ledger import SQLiteDecisionLedger
from spx_spark.data_platform.contracts import (
    CompactionManifestRecord,
    DecisionLegRecord,
    DecisionRecord,
    DeliveryRecord,
    EventRecord,
    FeatureSnapshotRecord,
    OutcomeRecord,
)
from spx_spark.data_platform.ports import (
    DecisionLedger,
    LedgerConflictError,
    LedgerReferenceError,
    LookaheadViolationError,
)


NOW = datetime(2026, 7, 10, 14, 30, tzinfo=timezone.utc)


@pytest.fixture(params=("memory", "sqlite"))
def ledger(request: pytest.FixtureRequest, tmp_path: Path) -> DecisionLedger:
    if request.param == "memory":
        return InMemoryDecisionLedger()
    return SQLiteDecisionLedger(tmp_path / "ledger.sqlite3")


def _event() -> EventRecord:
    return EventRecord(
        event_key="evt_1",
        event_type="flip_reclaim",
        session_date=date(2026, 7, 10),
        source_at=NOW,
        received_at=NOW + timedelta(milliseconds=40),
        available_at=NOW + timedelta(milliseconds=50),
        phase="reclaim",
        direction="up",
        data_quality="live",
        attributes={"spx": 5620.5},
    )


def _snapshot() -> FeatureSnapshotRecord:
    return FeatureSnapshotRecord(
        snapshot_id="feat_1",
        event_key="evt_1",
        captured_at=NOW,
        available_at=NOW + timedelta(milliseconds=60),
        gamma_regime="negative",
        payload={"gex": -100.0, "charm": 1.2},
    )


def _decision() -> DecisionRecord:
    return DecisionRecord(
        decision_id="dec_1",
        event_key="evt_1",
        feature_snapshot_id="feat_1",
        strategy_name="flip_reclaim_call",
        strategy_version="v1",
        decision_at=NOW + timedelta(seconds=1),
        available_at=NOW + timedelta(milliseconds=60),
        status="eligible",
        action="alert",
        side="call",
        gamma_regime="negative",
        attributes={"score": 0.81},
    )


def _leg() -> DecisionLegRecord:
    return DecisionLegRecord(
        decision_id="dec_1",
        leg_index=0,
        instrument_id="opaque-contract-1",
        right="C",
        expiry=date(2026, 7, 10),
        strike=5625.0,
        quantity=1.0,
        bid=3.1,
        ask=3.3,
        delta=0.48,
        gamma=0.04,
        quote_source_at=NOW + timedelta(milliseconds=100),
        quote_available_at=NOW + timedelta(milliseconds=150),
    )


def _populate_decision(ledger: DecisionLedger) -> tuple[EventRecord, DecisionRecord]:
    event = _event()
    snapshot = _snapshot()
    decision = _decision()
    ledger.record_event(event)
    ledger.record_feature_snapshot(snapshot)
    ledger.record_decision(decision, (_leg(),))
    return event, decision


def test_decision_aggregate_is_round_trippable_and_retry_safe(ledger: DecisionLedger) -> None:
    event, decision = _populate_decision(ledger)

    ledger.record_event(event)
    ledger.record_decision(decision, (_leg(),))

    assert isinstance(ledger, DecisionLedger)
    assert ledger.get_event(event.event_key) == event
    assert ledger.get_decision(decision.decision_id) == decision
    assert tuple(ledger.list_decision_legs(decision.decision_id)) == (_leg(),)


def test_conflicting_retry_does_not_mutate_decision(ledger: DecisionLedger) -> None:
    _, decision = _populate_decision(ledger)

    with pytest.raises(LedgerConflictError):
        ledger.record_decision(replace(decision, reason="different"), (_leg(),))

    assert ledger.get_decision(decision.decision_id) == decision


def test_missing_parent_and_lookahead_are_rejected_atomically(ledger: DecisionLedger) -> None:
    decision = replace(_decision(), event_key="missing", feature_snapshot_id=None)
    with pytest.raises(LedgerReferenceError):
        ledger.record_decision(decision, ())
    assert ledger.get_decision(decision.decision_id) is None

    ledger.record_event(_event())
    ledger.record_feature_snapshot(_snapshot())
    future_leg = replace(
        _leg(),
        quote_source_at=NOW + timedelta(seconds=2),
        quote_available_at=NOW + timedelta(seconds=2),
    )
    with pytest.raises(LookaheadViolationError):
        ledger.record_decision(_decision(), (future_leg,))
    assert ledger.get_decision("dec_1") is None


def test_delivery_outcome_and_manifest_round_trip(ledger: DecisionLedger) -> None:
    _, decision = _populate_decision(ledger)
    delivery = DeliveryRecord(
        delivery_id="delivery_1",
        decision_id=decision.decision_id,
        channel="telegram",
        status="sent",
        attempted_at=NOW + timedelta(seconds=2),
        sent_at=NOW + timedelta(seconds=2, milliseconds=50),
        provider="bot",
        message_fingerprint="opaque",
    )
    outcome = OutcomeRecord(
        outcome_id="outcome_1",
        event_key="evt_1",
        decision_id=decision.decision_id,
        horizon_minutes=5,
        status="complete",
        target_at=NOW + timedelta(minutes=5),
        sampled_at=NOW + timedelta(minutes=5, seconds=1),
        hypothesis_direction="up",
        spx_return_bps=12.5,
        spx_mfe_bps=18.0,
        spx_mae_bps=-3.0,
        option_pnl=45.0,
    )
    manifest = CompactionManifestRecord(
        source_path="raw/provider=ibkr/date=2026-07-10/hour=10/quotes.jsonl",
        source_sha256="a" * 64,
        source_size=1000,
        source_mtime_ns=12,
        output_path="lake/quotes/date=2026-07-10/hour=10/part.parquet",
        output_sha256="b" * 64,
        row_count=20,
        min_received_at=NOW,
        max_received_at=NOW + timedelta(minutes=1),
        schema_version="v1",
        writer_version="test-v1",
        completed_at=NOW + timedelta(minutes=2),
    )

    ledger.record_delivery(delivery)
    ledger.record_outcome(outcome)
    ledger.record_compaction_manifest(manifest)
    ledger.record_delivery(delivery)
    ledger.record_outcome(outcome)
    ledger.record_compaction_manifest(manifest)

    rebuilt = replace(
        manifest,
        output_sha256="c" * 64,
        completed_at=manifest.completed_at + timedelta(minutes=5),
    )
    ledger.record_compaction_manifest(rebuilt)

    assert tuple(ledger.list_deliveries(decision.decision_id)) == (delivery,)
    assert tuple(ledger.list_outcomes(decision.decision_id)) == (outcome,)
    assert ledger.get_compaction_manifest(manifest.source_path, manifest.source_sha256) == rebuilt
