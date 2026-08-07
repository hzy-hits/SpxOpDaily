"""Restart behavior for the unified notification queue."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from spx_spark.config import NotificationSettings
from spx_spark.domain.events import DomainEvent, EventKind
from spx_spark.infrastructure.notifications import (
    NotificationEventQueue,
    begin_attempt,
    create_engine,
    event_rows,
    metadata,
    recover_incomplete_attempts,
)
from spx_spark.notifier.unified_delivery import deliver_notification_event


NOW = datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc)


def _alert(event_id: str) -> DomainEvent:
    return DomainEvent(
        schema_version=1,
        event_id=event_id,
        kind=EventKind.ALERT_CANDIDATE,
        source_at=NOW,
        available_at=NOW,
        aggregate_id="spx",
        sequence=1,
        payload={"alerts": []},
    )


def test_restart_recovers_only_pre_transport_attempt(tmp_path: Path) -> None:
    store = create_engine(tmp_path)
    metadata.create_all(store)
    scheduled: list[int] = []
    queue = NotificationEventQueue(store, schedule=scheduled.append)
    assert queue.append([_alert("a1")]).accepted == 1
    event_id = scheduled[0]
    assert begin_attempt(store, event_id, now=NOW) is not None

    recovery = recover_incomplete_attempts(store, now=NOW)

    assert recovery.retry_event_ids == (event_id,)
    settings = replace(NotificationSettings.from_env(), notification_queue_enabled=True)
    result = deliver_notification_event(event_id, settings=settings, engine=store, now=NOW)
    assert result.ok is True
    assert next(iter(event_rows(store, "a1")))["status"] == "delivered"


def test_duplicate_append_uses_one_idempotent_row(tmp_path: Path) -> None:
    store = create_engine(tmp_path)
    metadata.create_all(store)
    scheduled: list[int] = []
    queue = NotificationEventQueue(store, schedule=scheduled.append)

    first = queue.append([_alert("once")])
    second = queue.append([_alert("once")])

    assert first.accepted == 1
    assert second.duplicate == 1
    assert len(event_rows(store, "once")) == 1
