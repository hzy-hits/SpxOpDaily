from __future__ import annotations

import sqlite3
from dataclasses import replace
from datetime import datetime, timezone

from spx_spark.config import NotificationSettings
from spx_spark.notifier.dispatcher import dispatch_notification
from spx_spark.notifier.missed_queue import append_missed, load_missed
from spx_spark.notifier.receipts import NotificationEnvelope


NOW = datetime(2026, 7, 14, 14, 0, tzinfo=timezone.utc)


def _settings(tmp_path) -> NotificationSettings:
    return replace(
        NotificationSettings.from_env(),
        enabled=True,
        feishu_enabled=True,
        feishu_webhook_url="https://open.feishu.cn/test",
        bark_enabled=True,
        bark_url="https://api.day.app/test",
        bark_friend_enabled=False,
        missed_queue_path=str(tmp_path / "missed.jsonl"),
        delivery_receipt_path=str(tmp_path / "receipts.sqlite"),
    )


def _envelope(event_id: str) -> NotificationEnvelope:
    return NotificationEnvelope(
        event_id=event_id,
        source="test",
        kind="market_warning",
        lane="market_warning",
        occurred_at=NOW,
    )


def test_dispatch_records_receipt_and_queues_failed_feishu(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "spx_spark.notifier.sinks.post_feishu",
        lambda *_: {"code": 19001, "msg": "failed"},
    )
    monkeypatch.setattr(
        "spx_spark.notifier.sinks.post_bark",
        lambda *_: {"code": 200},
    )
    settings = _settings(tmp_path)

    result = dispatch_notification(
        settings,
        _envelope("event-1"),
        title="SPX warning",
        text="warning body",
        attempted_at=NOW,
    )

    assert result.delivered is True
    assert result.queued_for_recovery is True
    assert [entry["entry_id"] for entry in load_missed(settings.missed_queue_path)] == [
        "event-1"
    ]
    with sqlite3.connect(settings.delivery_receipt_path) as connection:
        row = connection.execute(
            "SELECT event_id, lane, outcome, queued_for_recovery "
            "FROM notification_delivery_receipts"
        ).fetchone()
    assert row == ("event-1", "market_warning", "delivered", 1)


def test_next_dispatch_recovers_queue_and_deduplicates_event_ids(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    append_missed(
        settings.missed_queue_path,
        "old body",
        kind="market_warning",
        at=NOW,
        event_id="old-event",
    )
    append_missed(
        settings.missed_queue_path,
        "old body",
        kind="market_warning",
        at=NOW,
        event_id="old-event",
    )
    monkeypatch.setattr(
        "spx_spark.notifier.sinks.post_feishu",
        lambda *_: {"code": 0, "msg": "success"},
    )
    monkeypatch.setattr(
        "spx_spark.notifier.sinks.post_bark",
        lambda *_: {"code": 200},
    )

    result = dispatch_notification(
        settings,
        _envelope("event-2"),
        title="SPX warning",
        text="new body",
        attempted_at=NOW,
    )

    assert result.recovery_sink is not None and result.recovery_sink.ok
    assert load_missed(settings.missed_queue_path) == []
