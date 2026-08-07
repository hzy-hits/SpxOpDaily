from __future__ import annotations

import subprocess
import sys
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from spx_spark.config import NotificationSettings
from spx_spark.infrastructure.notifications import create_engine, event_rows
from spx_spark.notifier import dispatcher
from spx_spark.notifier.model import SinkResult
from spx_spark.notifier.model import NotificationEnvelope


NOW = datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("SPX_DATA_ROOT", str(tmp_path))
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        check=True,
        capture_output=True,
        text=True,
    )
    engine = create_engine(tmp_path)
    monkeypatch.setattr(dispatcher, "_store", lambda _settings: engine)
    monkeypatch.setattr(dispatcher, "_schedule_event", lambda _settings, _event_id: None)
    yield engine
    engine.dispose()


@pytest.fixture
def settings() -> NotificationSettings:
    return replace(
        NotificationSettings.from_env(),
        enabled=True,
        bark_enabled=True,
        bark_url="https://bark.invalid/token",
        feishu_enabled=True,
        feishu_webhook_url="https://feishu.invalid/hook",
        bark_friend_enabled=False,
        notification_queue_enabled=True,
        rust_trader_notification_owner=False,
    )


def envelope(event_id: str, lane: str = "trade_ready") -> NotificationEnvelope:
    return NotificationEnvelope(
        event_id=event_id,
        source="test",
        kind="trade_intent",
        lane=lane,
        occurred_at=NOW,
        expires_at=NOW + timedelta(minutes=5),
    )


def test_duplicate_is_idempotent_and_payload_collision_is_rejected(
    store,
    settings: NotificationSettings,
) -> None:
    first = dispatcher.enqueue_notification(
        settings, envelope("same"), title="READY", text="A", enqueued_at=NOW
    )
    duplicate = dispatcher.enqueue_notification(
        settings, envelope("same"), title="READY", text="A", enqueued_at=NOW
    )

    assert first.inserted is True
    assert duplicate.duplicate is True
    with pytest.raises(ValueError, match="idempotency collision"):
        dispatcher.enqueue_notification(
            settings, envelope("same"), title="READY", text="B", enqueued_at=NOW
        )
    assert len(event_rows(store, "same")) == 2


def test_linked_terminal_message_inherits_cause_route(
    store,
    settings: NotificationSettings,
) -> None:
    dispatcher.enqueue_notification(
        settings,
        envelope("ready"),
        title="READY",
        text="Buy spread",
        enqueued_at=NOW,
    )
    linked = dispatcher.enqueue_linked_notification(
        replace(settings, feishu_enabled=False, feishu_webhook_url=""),
        envelope("exit", lane="execution_safety"),
        causation_event_id="ready",
        title="EXIT",
        text="Close spread",
        enqueued_at=NOW,
    )

    assert linked.targets == ("bark", "feishu")
    assert {row["channel"] for row in event_rows(store, "exit")} == {"bark", "feishu"}


def test_dispatch_records_real_per_target_attempts(
    store,
    settings: NotificationSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def send(_settings, *, targets, **_kwargs):
        channel = next(iter(targets))
        return [SinkResult(channel, attempted=True, ok=True)]

    monkeypatch.setattr(dispatcher, "deliver_trade_push", send)
    result = dispatcher.dispatch_notification(
        settings,
        envelope("dispatch"),
        title="READY",
        text="Buy spread",
        attempted_at=NOW,
    )

    assert result.delivered is True
    assert result.outcome == "delivered"
    assert {row["status"] for row in event_rows(store, "dispatch")} == {"delivered"}
