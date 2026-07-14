from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os

import pytest

from spx_spark.notifier.delivery_outbox import (
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
    assert outbox.settle_target(
        "event-1",
        "bark",
        worker_id="inline",
        ok=True,
        error=None,
        now=NOW,
    ) is DeliveryStatus.DELIVERED
    assert outbox.settle_target(
        "event-1",
        "feishu",
        worker_id="inline",
        ok=False,
        error="temporary outage",
        now=NOW,
    ) is DeliveryStatus.PENDING

    summary = outbox.summary("event-1")
    assert summary is not None
    assert summary.status is DeliveryStatus.PENDING
    assert summary.delivered_targets == 1
    assert summary.pending_targets == 1
    assert outbox.claim_due(
        worker_id="too-early",
        limit_targets=10,
        now=NOW + timedelta(seconds=14),
    ) == []
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
    assert outbox.enqueue(
        _envelope(),
        title="SPX warning",
        text="warning body",
        feishu_text=None,
        friend=False,
        targets=("bark", "feishu"),
        now=NOW,
    ) is False
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
    assert outbox.settle_target(
        "event-1",
        "feishu",
        worker_id="two",
        ok=False,
        error="still down",
        now=NOW + timedelta(seconds=15),
    ) is DeliveryStatus.DEAD_LETTER

    summary = outbox.summary("event-1")
    assert summary is not None
    assert summary.status is DeliveryStatus.DEAD_LETTER
    assert summary.delivered_targets == 1
    assert summary.dead_letter_targets == 1


def test_database_is_owner_readable_only(tmp_path) -> None:
    outbox = _outbox(tmp_path)
    assert outbox.writable() is True
    assert oct(os.stat(outbox.path).st_mode & 0o777) == "0o600"
