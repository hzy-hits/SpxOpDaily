"""Restart / kill-before-ack notifier idempotency tests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from spx_spark.application.notifications.outbox_consumer import IdempotentOutboxConsumer
from spx_spark.domain.events import DomainEvent, EventKind
from spx_spark.infrastructure.ledger.outbox import SqliteEventOutbox


NOW = datetime(2026, 7, 11, 16, 30, tzinfo=timezone.utc)


def _alert(event_id: str) -> DomainEvent:
    return DomainEvent(
        schema_version=1,
        event_id=event_id,
        kind=EventKind.ALERT_CANDIDATE,
        source_at=NOW,
        available_at=NOW,
        aggregate_id="spx",
        sequence=1,
        payload={"kind": "price_move", "alert_id": event_id},
    )


def test_kill_restart_notifier_does_not_lose_or_double_send(tmp_path) -> None:
    db = tmp_path / "outbox.sqlite"
    outbox = SqliteEventOutbox(db, max_attempts=5)
    outbox.append([_alert("a1"), _alert("a2")])

    sent: list[str] = []
    processed: set[str] = set()

    def deliver(event: DomainEvent) -> bool:
        sent.append(event.event_id)
        return True

    # First claim both, deliver only the first, then "crash" before ack of a2.
    claimed = outbox.claim(consumer_id="notifier", limit=2, now=NOW)
    assert len(claimed) == 2
    assert deliver(claimed[0]) is True
    processed.add(claimed[0].event_id)
    outbox.ack([claimed[0].event_id], consumer_id="notifier")
    # a2 remains CLAIMED — simulate kill.

    # Restart: new process with durable processed set, reclaim stale claims.
    later = NOW + timedelta(seconds=5)
    restarted = IdempotentOutboxConsumer(
        SqliteEventOutbox(db, max_attempts=5),
        consumer_id="notifier",
        deliver=deliver,
        processed_ids=set(processed),
        claim_stale_after_seconds=1.0,
    )
    result = restarted.consume(
        limit=10,
        kinds=(EventKind.ALERT_CANDIDATE,),
        now=later,
    )
    assert result.delivered == 1
    assert result.acked_ids == ["a2"]
    assert sent == ["a1", "a2"]

    # Replaying / reclaiming again must not double-send.
    third = restarted.consume(kinds=(EventKind.ALERT_CANDIDATE,), now=later)
    assert third.claimed == 0
    assert third.delivered == 0
    assert sent == ["a1", "a2"]


def test_duplicate_claim_after_ack_is_skipped(tmp_path) -> None:
    outbox = SqliteEventOutbox(tmp_path / "outbox.sqlite")
    outbox.append([_alert("once")])
    sent: list[str] = []
    processed: set[str] = set()

    def deliver(event: DomainEvent) -> bool:
        sent.append(event.event_id)
        return True

    consumer = IdempotentOutboxConsumer(
        outbox,
        consumer_id="n",
        deliver=deliver,
        processed_ids=processed,
    )
    first = consumer.consume(kinds=(EventKind.ALERT_CANDIDATE,), now=NOW)
    assert first.delivered == 1
    # Force a pending re-insert attempt via duplicate append + manual pending
    # is blocked by PRIMARY KEY; instead put a second claim cycle on same id
    # by replaying from dead letter path is N/A. Simulate processed-set hit:
    processed.add("once")
    # Manually re-open as claimed then consume path for duplicate_skipped:
    outbox.append([_alert("once")])  # duplicate ignored
    second = consumer.consume(kinds=(EventKind.ALERT_CANDIDATE,), now=NOW)
    assert second.claimed == 0
    assert sent == ["once"]
