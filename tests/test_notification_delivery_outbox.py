from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
import sqlite3

import pytest

from spx_spark.notifier.delivery_outbox import (
    DeliveryCancelled,
    DeliveryStatus,
    NotificationDeliveryOutbox,
)
from spx_spark.notifier.receipts import NotificationEnvelope


NOW = datetime(2026, 7, 14, 14, 0, tzinfo=timezone.utc)


def _outbox(tmp_path, *, max_attempts: int = 3) -> NotificationDeliveryOutbox:
    return NotificationDeliveryOutbox(
        tmp_path / "notification-delivery.sqlite",
        max_attempts=max_attempts,
        retry_schedule_seconds=(15.0, 60.0, 300.0, 900.0),
        dead_letter_after_seconds=86400.0,
        claim_stale_after_seconds=180.0,
    )


def _envelope(event_id: str = "event-1") -> NotificationEnvelope:
    return NotificationEnvelope(
        event_id=event_id,
        source="test",
        kind="market_warning",
        lane="market_warning",
        occurred_at=NOW,
    )


def _enqueue(outbox: NotificationDeliveryOutbox, event_id: str = "event-1") -> None:
    assert outbox.enqueue(
        _envelope(event_id),
        title="SPX warning",
        text="warning body",
        feishu_text=None,
        friend=False,
        targets=("bark", "feishu"),
        now=NOW,
    )


def test_enqueue_claim_and_settle_targets_independently(tmp_path) -> None:
    outbox = _outbox(tmp_path)
    _enqueue(outbox)

    jobs = outbox.claim_due(worker_id="inline", limit_targets=10, now=NOW)
    assert len(jobs) == 1
    assert set(jobs[0].targets) == {"bark", "feishu"}
    assert (
        outbox.settle_target(
            "event-1",
            "bark",
            worker_id="inline",
            ok=True,
            error=None,
            now=NOW,
        )
        is DeliveryStatus.DELIVERED
    )
    assert (
        outbox.settle_target(
            "event-1",
            "feishu",
            worker_id="inline",
            ok=False,
            error="temporary outage",
            now=NOW,
        )
        is DeliveryStatus.PENDING
    )

    summary = outbox.summary("event-1")
    assert summary is not None
    assert summary.status is DeliveryStatus.PENDING
    assert summary.delivered_targets == 1
    assert summary.pending_targets == 1
    assert (
        outbox.claim_due(
            worker_id="too-early",
            limit_targets=10,
            now=NOW + timedelta(seconds=14),
        )
        == []
    )
    retry = outbox.claim_due(
        worker_id="recovery",
        limit_targets=10,
        now=NOW + timedelta(seconds=15),
    )
    assert len(retry) == 1
    assert retry[0].targets == ("feishu",)


def test_duplicate_event_id_requires_identical_payload(tmp_path) -> None:
    outbox = _outbox(tmp_path)
    _enqueue(outbox)
    assert (
        outbox.enqueue(
            _envelope(),
            title="SPX warning",
            text="warning body",
            feishu_text=None,
            friend=False,
            targets=("bark", "feishu"),
            now=NOW,
        )
        is False
    )
    with pytest.raises(ValueError, match="collision"):
        outbox.enqueue(
            _envelope(),
            title="SPX warning",
            text="different body",
            feishu_text=None,
            friend=False,
            targets=("bark", "feishu"),
            now=NOW,
        )


def test_duplicate_event_id_requires_exact_target_set(tmp_path) -> None:
    outbox = _outbox(tmp_path)
    _enqueue(outbox)

    with pytest.raises(ValueError, match="target collision"):
        outbox.enqueue(
            _envelope(),
            title="SPX warning",
            text="warning body",
            feishu_text=None,
            friend=False,
            targets=("bark",),
            now=NOW,
        )


def test_pre_enqueue_cancellation_fence_rejects_late_writer(tmp_path) -> None:
    outbox = _outbox(tmp_path)

    assert (
        outbox.cancel_event_with_receipts(
            "cancelled-before-row",
            reason="source_invalidated",
            now=NOW,
        )
        == ()
    )
    assert outbox.cancellation_exists("cancelled-before-row") is True

    with pytest.raises(DeliveryCancelled, match="cancellation-fenced"):
        outbox.enqueue(
            _envelope("cancelled-before-row"),
            title="SPX warning",
            text="warning body",
            feishu_text=None,
            friend=False,
            targets=("bark", "feishu"),
            now=NOW + timedelta(seconds=1),
        )
    assert outbox.contains("cancelled-before-row") is False


def test_event_inspection_requires_payload_targets_and_live_state(tmp_path) -> None:
    outbox = _outbox(tmp_path)
    envelope = _envelope("inspect-exact")
    _enqueue(outbox, envelope.event_id)

    exact = outbox.inspect_event(
        envelope,
        title="SPX warning",
        text="warning body",
        feishu_text=None,
        friend=False,
        targets=("feishu", "bark"),
    )
    assert exact.acceptable is True
    assert exact.reason == "accepted"

    payload_mismatch = outbox.inspect_event(
        envelope,
        title="SPX warning",
        text="different body",
        feishu_text=None,
        friend=False,
        targets=("bark", "feishu"),
    )
    assert payload_mismatch.reason == "payload_mismatch"

    target_mismatch = outbox.inspect_event(
        envelope,
        title="SPX warning",
        text="warning body",
        feishu_text=None,
        friend=False,
        targets=("bark",),
    )
    assert target_mismatch.reason == "target_mismatch"

    job = outbox.claim_due(worker_id="terminal", limit_targets=2, now=NOW)[0]
    for target in job.targets:
        outbox.settle_target(
            envelope.event_id,
            target,
            worker_id="terminal",
            ok=False,
            error="permanent",
            permanent=True,
            now=NOW,
        )
    terminal = outbox.inspect_event(
        envelope,
        title="SPX warning",
        text="warning body",
        feishu_text=None,
        friend=False,
        targets=("bark", "feishu"),
    )
    assert terminal.reason == "terminal_or_invalid_status"


def test_expired_event_is_settled_without_delivery_claim(tmp_path) -> None:
    outbox = _outbox(tmp_path)
    envelope = NotificationEnvelope(
        event_id="short-lived",
        source="test",
        kind="gth_candidate",
        lane="gth_manual_candidate",
        occurred_at=NOW,
        expires_at=NOW + timedelta(seconds=20),
    )
    assert outbox.enqueue(
        envelope,
        title="manual candidate",
        text="short lived",
        feishu_text=None,
        friend=False,
        targets=("bark", "feishu"),
        now=NOW,
    )

    terminal_receipts = []
    assert (
        outbox.claim_due(
            worker_id="late",
            limit_targets=10,
            now=NOW + timedelta(seconds=21),
            terminal_receipts=terminal_receipts,
        )
        == []
    )
    summary = outbox.summary("short-lived")
    assert summary is not None
    assert summary.status is DeliveryStatus.DEAD_LETTER
    assert summary.dead_letter_targets == 2
    assert outbox.count_unacknowledged_dead_letters() == 2
    assert {row["last_error"] for row in outbox.list_dead_letters()} == {
        "notification_expired_before_delivery"
    }
    assert len(terminal_receipts) == 2
    assert {
        receipt.outcome for receipt in outbox.list_terminal_receipts(event_id="short-lived")
    } == {"expired_before_delivery"}


def test_source_invalidation_cancels_claimed_and_pending_targets(tmp_path) -> None:
    outbox = _outbox(tmp_path)
    _enqueue(outbox)
    claimed = outbox.claim_due(worker_id="worker", limit_targets=1, now=NOW)

    assert claimed and len(claimed[0].targets) == 1
    assert (
        outbox.cancel_event(
            "event-1",
            reason="source_candidate_no_longer_manual_ready",
            now=NOW + timedelta(seconds=1),
        )
        == 2
    )

    summary = outbox.summary("event-1")
    assert summary is not None
    assert summary.status is DeliveryStatus.DEAD_LETTER
    assert summary.dead_letter_targets == 2
    assert outbox.count_unacknowledged_dead_letters() == 0
    assert {row["last_error"] for row in outbox.list_dead_letters()} == {
        "source_candidate_no_longer_manual_ready"
    }
    assert {receipt.outcome for receipt in outbox.list_terminal_receipts(event_id="event-1")} == {
        "cancelled_before_delivery"
    }


def test_claim_priority_orders_expiring_work_within_lane(tmp_path) -> None:
    outbox = _outbox(tmp_path)
    envelopes = (
        NotificationEnvelope(
            event_id="later-expiry",
            source="test",
            kind="trade_intent",
            lane="trade_ready",
            occurred_at=NOW - timedelta(minutes=3),
            expires_at=NOW + timedelta(seconds=60),
        ),
        NotificationEnvelope(
            event_id="sooner-expiry",
            source="test",
            kind="trade_intent",
            lane="trade_ready",
            occurred_at=NOW - timedelta(minutes=2),
            expires_at=NOW + timedelta(seconds=20),
        ),
        NotificationEnvelope(
            event_id="no-expiry",
            source="test",
            kind="trade_intent",
            lane="trade_ready",
            occurred_at=NOW - timedelta(minutes=4),
        ),
    )
    for offset, envelope in enumerate(envelopes):
        assert outbox.enqueue(
            envelope,
            title=envelope.event_id,
            text=envelope.event_id,
            feishu_text=None,
            friend=False,
            targets=("bark",),
            now=NOW - timedelta(minutes=3 - offset),
        )

    claimed_ids = []
    for ordinal in range(3):
        jobs = outbox.claim_due(
            worker_id=f"worker-{ordinal}",
            limit_targets=1,
            now=NOW,
        )
        assert len(jobs) == 1
        claimed_ids.append(jobs[0].envelope.event_id)

    assert claimed_ids == [
        "sooner-expiry",
        "later-expiry",
        "no-expiry",
    ]


def test_claim_priority_keeps_safety_and_signals_ahead_of_reports(tmp_path) -> None:
    outbox = _outbox(tmp_path)
    lanes = (
        "scheduled_report",
        "ops",
        "market_warning",
        "trade_ready",
        "position_safety",
    )
    for ordinal, lane in enumerate(lanes):
        assert outbox.enqueue(
            NotificationEnvelope(
                event_id=f"priority-{lane}",
                source="test",
                kind=lane,
                lane=lane,
                occurred_at=NOW - timedelta(minutes=10 - ordinal),
            ),
            title=lane,
            text=lane,
            feishu_text=None,
            friend=False,
            targets=("bark",),
            now=NOW - timedelta(minutes=10 - ordinal),
        )

    claimed_ids = []
    for ordinal in range(len(lanes)):
        jobs = outbox.claim_due(
            worker_id=f"priority-worker-{ordinal}",
            limit_targets=1,
            now=NOW,
        )
        assert len(jobs) == 1
        claimed_ids.append(jobs[0].envelope.event_id)

    assert claimed_ids == [
        "priority-position_safety",
        "priority-trade_ready",
        "priority-market_warning",
        "priority-ops",
        "priority-scheduled_report",
    ]


def test_retry_exhaustion_dead_letters_only_failed_target(tmp_path) -> None:
    outbox = _outbox(tmp_path, max_attempts=2)
    _enqueue(outbox)
    first = outbox.claim_due(worker_id="one", limit_targets=10, now=NOW)[0]
    for target in first.targets:
        outbox.settle_target(
            "event-1",
            target,
            worker_id="one",
            ok=target == "bark",
            error=None if target == "bark" else "down",
            now=NOW,
        )
    second = outbox.claim_due(
        worker_id="two",
        limit_targets=10,
        now=NOW + timedelta(seconds=15),
    )[0]
    assert second.targets == ("feishu",)
    assert (
        outbox.settle_target(
            "event-1",
            "feishu",
            worker_id="two",
            ok=False,
            error="still down",
            now=NOW + timedelta(seconds=15),
        )
        is DeliveryStatus.DEAD_LETTER
    )

    summary = outbox.summary("event-1")
    assert summary is not None
    assert summary.status is DeliveryStatus.DEAD_LETTER
    assert summary.delivered_targets == 1
    assert summary.dead_letter_targets == 1


def test_each_retry_gets_a_distinct_receipt_intent_at_same_clock(tmp_path) -> None:
    outbox = NotificationDeliveryOutbox(
        tmp_path / "notification-delivery.sqlite",
        max_attempts=2,
        retry_schedule_seconds=(0.0,),
        dead_letter_after_seconds=86400.0,
        claim_stale_after_seconds=180.0,
    )
    assert outbox.enqueue(
        _envelope("same-clock-retries"),
        title="SPX warning",
        text="warning body",
        feishu_text=None,
        friend=False,
        targets=("bark",),
        now=NOW,
    )

    for worker in ("first", "second"):
        job = outbox.claim_due(
            worker_id=worker,
            limit_targets=1,
            now=NOW,
        )
        assert job[0].targets == ("bark",)
        outbox.settle_target(
            "same-clock-retries",
            "bark",
            worker_id=worker,
            ok=False,
            error="same failure",
            now=NOW,
        )

    receipts = outbox.list_terminal_receipts(event_id="same-clock-retries")
    assert len(receipts) == 2
    assert {receipt.outcome for receipt in receipts} == {
        "pending",
        "dead_letter",
    }


def test_database_is_owner_readable_only(tmp_path) -> None:
    outbox = _outbox(tmp_path)
    assert outbox.writable() is True
    assert oct(os.stat(outbox.path).st_mode & 0o777) == "0o600"


def test_database_uses_delete_journal_mode_to_avoid_wal_reset_race(tmp_path) -> None:
    outbox = _outbox(tmp_path)

    with sqlite3.connect(outbox.path) as connection:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "delete"


def test_writable_rejects_structurally_corrupt_database(tmp_path) -> None:
    outbox = _outbox(tmp_path)
    with outbox.path.open("r+b") as database:
        database.seek(4096)
        database.write(b"\x00" * 256)

    assert outbox.writable() is False


def test_permanent_failure_dead_letters_on_first_attempt(tmp_path) -> None:
    outbox = _outbox(tmp_path)
    _enqueue(outbox)
    outbox.claim_due(worker_id="inline", limit_targets=10, now=NOW)

    assert (
        outbox.settle_target(
            "event-1",
            "bark",
            worker_id="inline",
            ok=False,
            error="HTTP Error 413: Request Entity Too Large",
            permanent=True,
            now=NOW,
        )
        is DeliveryStatus.DEAD_LETTER
    )

    summary = outbox.summary("event-1")
    assert summary is not None
    assert summary.dead_letter_targets == 1
    assert summary.pending_targets == 0  # feishu is still claimed by inline
    # the dead-lettered target never comes back for retry
    assert outbox.claim_due(worker_id="later", limit_targets=10, now=NOW) == []


def test_dead_letter_list_acknowledge_and_replay(tmp_path) -> None:
    outbox = _outbox(tmp_path, max_attempts=1)
    _enqueue(outbox)
    job = outbox.claim_due(worker_id="inline", limit_targets=10, now=NOW)[0]
    for target in job.targets:
        assert (
            outbox.settle_target(
                "event-1",
                target,
                worker_id="inline",
                ok=False,
                error="down",
                now=NOW,
            )
            is DeliveryStatus.DEAD_LETTER
        )

    dead = outbox.list_dead_letters()
    assert {row["sink"] for row in dead} == {"bark", "feishu"}
    assert all(row["acknowledged"] is False for row in dead)
    assert all(row["title"] == "SPX warning" for row in dead)
    assert outbox.count_unacknowledged_dead_letters() == 2

    assert outbox.acknowledge_dead_letter("event-1", now=NOW) == 2
    assert outbox.acknowledge_dead_letter("event-1", now=NOW) == 0
    assert outbox.count_unacknowledged_dead_letters() == 0
    assert outbox.list_dead_letters(unacknowledged_only=True) == []
    assert all(row["acknowledged"] for row in outbox.list_dead_letters())

    assert outbox.replay_dead_letter("event-1", now=NOW) == 2
    assert outbox.replay_dead_letter("event-1", now=NOW) == 0
    summary = outbox.summary("event-1")
    assert summary is not None
    assert summary.status is DeliveryStatus.PENDING

    retry = outbox.claim_due(worker_id="replay", limit_targets=10, now=NOW)
    assert len(retry) == 1
    assert set(retry[0].targets) == {"bark", "feishu"}
    for target in retry[0].targets:
        outbox.settle_target(
            "event-1",
            target,
            worker_id="replay",
            ok=True,
            error=None,
            now=NOW,
        )
    summary = outbox.summary("event-1")
    assert summary is not None
    assert summary.status is DeliveryStatus.DELIVERED
