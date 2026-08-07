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


def envelope(event_id: str = "ready:unified") -> NotificationEnvelope:
    return NotificationEnvelope(
        event_id=event_id,
        source="test",
        kind="trade_intent",
        lane="trade_ready",
        occurred_at=NOW,
        expires_at=NOW + timedelta(minutes=5),
    )


def test_enqueue_is_network_free_and_inspect_uses_same_contract(
    store,
    settings: NotificationSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduled: list[int] = []
    monkeypatch.setattr(
        dispatcher,
        "_schedule_event",
        lambda _settings, event_id: scheduled.append(event_id),
    )
    monkeypatch.setattr(
        dispatcher,
        "deliver_trade_push",
        lambda *_args, **_kwargs: pytest.fail("enqueue performed network I/O"),
    )

    result = dispatcher.enqueue_notification(
        settings,
        envelope(),
        title="SPX READY",
        text="Buy spread",
        enqueued_at=NOW,
    )

    assert result.accepted is True
    assert result.inserted is True
    assert result.targets == ("bark", "feishu")
    assert scheduled == [row["id"] for row in event_rows(store, envelope().event_id)]
    assert dispatcher.inspect_notification_event(
        settings,
        envelope(),
        title="SPX READY",
        text="Buy spread",
    ).acceptable


def test_consumer_delivers_one_frozen_target_per_cycle(
    store,
    settings: NotificationSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(dispatcher, "_schedule_event", lambda _settings, _event_id: None)
    delivered: list[str] = []

    def send(_settings, *, targets, **_kwargs):
        channel = next(iter(targets))
        delivered.append(channel)
        return [SinkResult(channel, attempted=True, ok=True)]

    monkeypatch.setattr(dispatcher, "deliver_trade_push", send)
    dispatcher.enqueue_notification(
        settings,
        envelope(),
        title="SPX READY",
        text="Buy spread",
        enqueued_at=NOW,
    )

    first = dispatcher.consume_pending_notifications(settings, now=NOW)
    second = dispatcher.consume_pending_notifications(settings, now=NOW)

    assert first["jobs"] == second["jobs"] == 1
    assert delivered == ["bark", "feishu"]
    assert {row["status"] for row in event_rows(store, envelope().event_id)} == {"delivered"}


def test_cancellation_fence_blocks_late_enqueue(
    store,
    settings: NotificationSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(dispatcher, "_schedule_event", lambda _settings, _event_id: None)
    assert dispatcher.cancel_pending_notification(
        settings,
        envelope().event_id,
        now=NOW,
        reason="invalidated",
    ) == 0

    late = dispatcher.enqueue_notification(
        settings,
        envelope(),
        title="SPX READY",
        text="Buy spread",
        enqueued_at=NOW,
    )

    assert late.accepted is False
    assert late.outcome == "cancelled_before_enqueue"
    assert dispatcher.inspect_notification_event(
        settings,
        envelope(),
        title="SPX READY",
        text="Buy spread",
    ).reason == "cancelled"
