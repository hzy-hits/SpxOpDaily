"""SQLite domain-event outbox repository tests."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import sqlite3

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

    assert outbox.ack(
        ["e1", "e2"],
        consumer_id="notifier-1",
        outcome="consumed",
        delivered_count=0,
    ) == 2
    assert outbox.count_by_status().get(OutboxStatus.ACKED.value) == 2
    assert outbox.claim(consumer_id="notifier-1", now=NOW) == []
    with sqlite3.connect(outbox.path) as connection:
        assert connection.execute(
            "SELECT DISTINCT settlement_outcome, delivered_count "
            "FROM domain_event_outbox"
        ).fetchall() == [("consumed", 0)]


def test_duplicate_append_is_idempotent(tmp_path) -> None:
    outbox = SqliteEventOutbox(tmp_path / "outbox.sqlite")
    first = outbox.append([_event("dup")])
    second = outbox.append([_event("dup")])
    assert first.accepted == 1
    assert second.accepted == 0
    assert second.duplicate == 1
    assert outbox.count_by_status()[OutboxStatus.PENDING.value] == 1


def test_legacy_acked_rows_are_labeled_unknown_on_reopen(tmp_path) -> None:
    path = tmp_path / "outbox.sqlite"
    outbox = SqliteEventOutbox(path)
    outbox.append([_event("legacy-acked")])
    outbox.claim(consumer_id="legacy-worker", now=NOW)
    assert outbox.ack(["legacy-acked"], consumer_id="legacy-worker") == 1

    SqliteEventOutbox(path)

    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT settlement_outcome FROM domain_event_outbox WHERE event_id = ?",
            ("legacy-acked",),
        ).fetchone() == ("legacy_unknown",)


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


def test_failed_delivery_uses_configured_retry_backoff(tmp_path) -> None:
    outbox = SqliteEventOutbox(
        tmp_path / "outbox.sqlite",
        retry_base_seconds=60.0,
        retry_max_seconds=900.0,
    )
    outbox.append([_event("retry-later")])
    outbox.claim(consumer_id="n", now=NOW)
    assert outbox.fail("retry-later", error="review pending", consumer_id="n", now=NOW) is (
        OutboxStatus.PENDING
    )
    assert outbox.claim(consumer_id="n", now=NOW + timedelta(seconds=59)) == []
    claimed = outbox.claim(consumer_id="n", now=NOW + timedelta(seconds=60))
    assert [event.event_id for event in claimed] == ["retry-later"]


def test_neutral_empty_append_does_not_grow_outbox(tmp_path) -> None:
    outbox = SqliteEventOutbox(tmp_path / "outbox.sqlite")
    result = outbox.append([])
    assert result.accepted == 0
    assert outbox.count_by_status() == {}


def _ack_and_age(outbox: SqliteEventOutbox, event_id: str, *, at: datetime) -> None:
    outbox.append([_event(event_id)])
    outbox.claim(consumer_id="n", now=NOW)
    assert outbox.ack([event_id], consumer_id="n", outcome="consumed") == 1
    with sqlite3.connect(outbox.path) as connection:
        connection.execute(
            "UPDATE domain_event_outbox SET updated_at = ? WHERE event_id = ?",
            (at.isoformat(), event_id),
        )


def test_purge_acked_older_than_deletes_only_expired_acked(tmp_path) -> None:
    outbox = SqliteEventOutbox(tmp_path / "outbox.sqlite", max_attempts=1)
    old = NOW - timedelta(days=40)
    recent = NOW - timedelta(days=2)
    _ack_and_age(outbox, "old-acked", at=old)
    _ack_and_age(outbox, "recent-acked", at=recent)
    outbox.append([_event("old-pending")])
    with sqlite3.connect(outbox.path) as connection:
        connection.execute(
            "UPDATE domain_event_outbox SET updated_at = ? WHERE event_id = ?",
            (old.isoformat(), "old-pending"),
        )
    outbox.claim(consumer_id="n", now=NOW)
    assert outbox.fail("old-pending", error="boom", consumer_id="n", now=NOW) is (
        OutboxStatus.DEAD_LETTER
    )
    with sqlite3.connect(outbox.path) as connection:
        connection.execute(
            "UPDATE domain_event_outbox SET updated_at = ? WHERE event_id = ?",
            (old.isoformat(), "old-pending"),
        )

    deleted = outbox.purge_acked_older_than(days=30, now=NOW)

    assert deleted == 1
    counts = outbox.count_by_status()
    # The recent acked row and the ancient dead-letter both survive retention.
    assert counts.get(OutboxStatus.ACKED.value) == 1
    assert counts.get(OutboxStatus.DEAD_LETTER.value) == 1
    assert [record.event_id for record in outbox.dead_letters()] == ["old-pending"]


def test_purge_acked_older_than_rejects_invalid_days(tmp_path) -> None:
    outbox = SqliteEventOutbox(tmp_path / "outbox.sqlite")

    for days in (0, -1):
        try:
            outbox.purge_acked_older_than(days=days, now=NOW)
        except ValueError:
            continue
        raise AssertionError(f"days={days} should raise ValueError")


def test_purge_acked_with_vacuum_keeps_surviving_rows(tmp_path) -> None:
    outbox = SqliteEventOutbox(tmp_path / "outbox.sqlite")
    _ack_and_age(outbox, "old-acked", at=NOW - timedelta(days=40))
    outbox.append([_event("still-pending")])

    deleted = outbox.purge_acked_older_than(days=30, now=NOW, vacuum=True)

    assert deleted == 1
    assert outbox.count_by_status().get(OutboxStatus.PENDING.value) == 1
    assert outbox.writable() is True
