from __future__ import annotations

import subprocess
import sys
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from spx_spark.notifier.unified_delivery import (
    RetryableDeliveryError,
    deliver_notification_event,
    enqueue_final_notification,
)
from spx_spark.config import NotificationSettings
from spx_spark.infrastructure.notifications import (
    NotificationStatus,
    create_engine,
    event_rows,
)
from spx_spark.notifier.model import SinkResult
from spx_spark.notifier.receipts import NotificationEnvelope


NOW = datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def engine(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("SPX_DATA_ROOT", str(tmp_path))
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        check=True,
        capture_output=True,
        text=True,
    )
    result = create_engine(tmp_path)
    yield result
    result.dispose()


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
        rust_trader_notification_owner=False,
    )


def envelope() -> NotificationEnvelope:
    return NotificationEnvelope(
        event_id="ready:20260807:unified",
        source="test",
        kind="trade_intent",
        lane="trade_ready",
        occurred_at=NOW,
        expires_at=NOW + timedelta(minutes=5),
    )


def test_enqueue_and_deliver_settle_each_target_independently(
    engine,
    settings: NotificationSettings,
) -> None:
    scheduled: list[int] = []
    queued = enqueue_final_notification(
        settings,
        envelope(),
        title="SPX READY",
        text="Buy spread",
        enqueued_at=NOW,
        engine=engine,
        schedule=scheduled.append,
    )

    assert queued.accepted is True
    assert queued.inserted == 2
    assert scheduled == list(queued.event_ids)

    def deliver(_settings, *, targets, **_kwargs):
        channel = next(iter(targets))
        return [SinkResult(channel, attempted=True, ok=True)]

    results = [
        deliver_notification_event(
            event_id,
            settings=settings,
            engine=engine,
            direct_deliver=deliver,
            now=NOW,
        )
        for event_id in queued.event_ids
    ]

    assert {result.channel for result in results} == {"bark", "feishu"}
    assert all(result.ok for result in results)
    assert {row["status"] for row in event_rows(engine, envelope().event_id)} == {
        NotificationStatus.DELIVERED.value
    }


def test_explicit_server_failure_retries_at_most_three_times(
    engine,
    settings: NotificationSettings,
) -> None:
    event_id = enqueue_final_notification(
        replace(settings, feishu_enabled=False, feishu_webhook_url=""),
        envelope(),
        title="SPX READY",
        text="Buy spread",
        enqueued_at=NOW,
        engine=engine,
        schedule=lambda event_id: None,
    ).event_ids[0]

    def fail(_settings, *, targets, **_kwargs):
        channel = next(iter(targets))
        return [
            SinkResult(
                channel,
                attempted=True,
                ok=False,
                error="bark response code=503 message=busy",
            )
        ]

    for _ in range(2):
        with pytest.raises(RetryableDeliveryError):
            deliver_notification_event(
                event_id,
                settings=settings,
                engine=engine,
                direct_deliver=fail,
                now=NOW,
            )
    final = deliver_notification_event(
        event_id,
        settings=settings,
        engine=engine,
        direct_deliver=fail,
        now=NOW,
    )
    assert final.outcome == "retryable_failure"
    assert next(iter(event_rows(engine, envelope().event_id)))["status"] == (
        NotificationStatus.FAILED.value
    )
    assert (
        deliver_notification_event(
            event_id,
            settings=settings,
            engine=engine,
            direct_deliver=fail,
            now=NOW,
        ).outcome
        == "not_due"
    )


def test_unknown_transport_outcome_is_never_automatically_retried(
    engine,
    settings: NotificationSettings,
) -> None:
    event_id = enqueue_final_notification(
        replace(settings, feishu_enabled=False, feishu_webhook_url=""),
        envelope(),
        title="SPX READY",
        text="Buy spread",
        enqueued_at=NOW,
        engine=engine,
        schedule=lambda event_id: None,
    ).event_ids[0]

    def timeout(_settings, *, targets, **_kwargs):
        channel = next(iter(targets))
        return [SinkResult(channel, attempted=True, ok=False, error="timed out")]

    result = deliver_notification_event(
        event_id,
        settings=settings,
        engine=engine,
        direct_deliver=timeout,
        now=NOW,
    )

    assert result.outcome == "transport_outcome_uncertain"
    assert next(iter(event_rows(engine, envelope().event_id)))["status"] == (
        NotificationStatus.UNCERTAIN.value
    )
    assert (
        deliver_notification_event(
            event_id,
            settings=settings,
            engine=engine,
            direct_deliver=timeout,
            now=NOW,
        ).outcome
        == "not_due"
    )
