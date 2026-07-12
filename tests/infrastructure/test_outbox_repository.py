"""SQLite domain-event outbox repository tests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from spx_spark.domain.events import DomainEvent, EventKind
from spx_spark.infrastructure.ledger.outbox import OutboxStatus, SqliteEventOutbox


NOW = datetime(2026, 7, 11, 16, 0, tzinfo=timezone.utc)


def _event(event_id: str, *, kind: EventKind = EventKind.ALERT_CANDIDATE) -> DomainEvent:
    return DomainEvent(
        schema_version=1,
        event_id=event_id,
        kind=kind,
        source_at=NOW,
        available_at=NOW,
        aggregate_id="spx",
        sequence=1,
        payload={"alert": event_id},
    )


def test_append_claim_ack_roundtrip(tmp_path) -> None:
    outbox = SqliteEventOutbox(tmp_path / "outbox.sqlite")
    result = outbox.append([_event("e1"), _event("e2")])
    assert result.accepted == 2
    assert result.duplicate == 0
    assert outbox.writable() is True

    claimed = outbox.claim(consumer_id="notifier-1", limit=10, now=NOW)
    assert {event.event_id for event in claimed} == {"e1", "e2"}
    assert outbox.count_by_status()[OutboxStatus.CLAIMED.value] == 2

    assert outbox.ack(["e1", "e2"], consumer_id="notifier-1") == 2
    assert outbox.count_by_status().get(OutboxStatus.ACKED.value) == 2
    assert outbox.claim(consumer_id="notifier-1", now=NOW) == []


def test_duplicate_append_is_idempotent(tmp_path) -> None:
    outbox = SqliteEventOutbox(tmp_path / "outbox.sqlite")
    first = outbox.append([_event("dup")])
    second = outbox.append([_event("dup")])
    assert first.accepted == 1
    assert second.accepted == 0
    assert second.duplicate == 1
    assert outbox.count_by_status()[OutboxStatus.PENDING.value] == 1


def test_consumer_crash_before_ack_can_reclaim(tmp_path) -> None:
    outbox = SqliteEventOutbox(tmp_path / "outbox.sqlite")
    outbox.append([_event("crash-1")])
    claimed = outbox.claim(consumer_id="worker-a", now=NOW)
    assert len(claimed) == 1
    # Kill before ack: reclaim after stale window.
    later = NOW + timedelta(seconds=60)
    requeued = outbox.requeue_stale_claims(older_than_seconds=30, now=later)
    assert requeued == 1
    again = outbox.claim(consumer_id="worker-b", now=later)
    assert len(again) == 1
    assert again[0].event_id == "crash-1"


def test_retry_exhaustion_dead_letters(tmp_path) -> None:
    outbox = SqliteEventOutbox(tmp_path / "outbox.sqlite", max_attempts=2)
    outbox.append([_event("dlq-1")])
    outbox.claim(consumer_id="n", now=NOW)
    status = outbox.fail("dlq-1", error="transient", consumer_id="n", now=NOW)
    assert status is OutboxStatus.PENDING
    outbox.claim(consumer_id="n", now=NOW + timedelta(seconds=1))
    status = outbox.fail("dlq-1", error="still bad", consumer_id="n", now=NOW)
    assert status is OutboxStatus.DEAD_LETTER
    letters = outbox.dead_letters()
    assert letters[0].event_id == "dlq-1"
    assert outbox.replay_dead_letter("dlq-1") is True
    assert outbox.count_by_status()[OutboxStatus.PENDING.value] == 1


def test_neutral_empty_append_does_not_grow_outbox(tmp_path) -> None:
    outbox = SqliteEventOutbox(tmp_path / "outbox.sqlite")
    result = outbox.append([])
    assert result.accepted == 0
    assert outbox.count_by_status() == {}
