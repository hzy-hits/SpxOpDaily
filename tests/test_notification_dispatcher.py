from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from spx_spark.config import NotificationSettings
from spx_spark.notifier.delivery_outbox import (
    DeliveryStatus,
    NotificationDeliveryOutbox,
)
from spx_spark.notifier.dispatcher import (
    dispatch_notification,
    recover_pending_notifications,
)
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
        delivery_outbox_enabled=True,
        delivery_outbox_path=str(tmp_path / "delivery-outbox.sqlite"),
        delivery_outbox_max_attempts=4,
        delivery_outbox_retry_schedule_seconds=(15.0, 60.0, 300.0, 900.0),
        delivery_outbox_dead_letter_after_seconds=86400.0,
        delivery_outbox_claim_stale_after_seconds=180.0,
        delivery_outbox_recovery_batch_size=50,
        delivery_outbox_legacy_shadow_enabled=True,
    )


def _envelope(event_id: str) -> NotificationEnvelope:
    return NotificationEnvelope(
        event_id=event_id,
        source="test",
        kind="market_warning",
        lane="market_warning",
        occurred_at=NOW,
    )


def _outbox(settings: NotificationSettings) -> NotificationDeliveryOutbox:
    return NotificationDeliveryOutbox(
        settings.delivery_outbox_path,
        max_attempts=settings.delivery_outbox_max_attempts,
        retry_schedule_seconds=settings.delivery_outbox_retry_schedule_seconds,
        dead_letter_after_seconds=settings.delivery_outbox_dead_letter_after_seconds,
        claim_stale_after_seconds=settings.delivery_outbox_claim_stale_after_seconds,
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
    assert result.outcome == "pending"
    assert [entry["entry_id"] for entry in load_missed(settings.missed_queue_path)] == [
        "event-1"
    ]
    outbox = NotificationDeliveryOutbox(
        settings.delivery_outbox_path,
        max_attempts=settings.delivery_outbox_max_attempts,
        retry_schedule_seconds=settings.delivery_outbox_retry_schedule_seconds,
        dead_letter_after_seconds=settings.delivery_outbox_dead_letter_after_seconds,
        claim_stale_after_seconds=settings.delivery_outbox_claim_stale_after_seconds,
    )
    summary = outbox.summary("event-1")
    assert summary is not None
    assert summary.status is DeliveryStatus.PENDING
    assert summary.delivered_targets == 1
    assert summary.pending_targets == 1
    with sqlite3.connect(settings.delivery_receipt_path) as connection:
        row = connection.execute(
            "SELECT event_id, lane, outcome, queued_for_recovery "
            "FROM notification_delivery_receipts"
        ).fetchone()
    assert row == ("event-1", "market_warning", "pending", 1)


def test_recovery_retries_only_failed_sink_and_clears_jsonl_shadow(
    tmp_path,
    monkeypatch,
) -> None:
    settings = _settings(tmp_path)
    calls = {"bark": 0, "feishu": 0}

    def bark(*_):
        calls["bark"] += 1
        return {"code": 200}

    def feishu_fail(*_):
        calls["feishu"] += 1
        return {"code": 19001, "msg": "failed"}

    monkeypatch.setattr(
        "spx_spark.notifier.sinks.post_feishu",
        feishu_fail,
    )
    monkeypatch.setattr(
        "spx_spark.notifier.sinks.post_bark",
        bark,
    )
    first = dispatch_notification(
        settings,
        _envelope("event-retry"),
        title="SPX warning",
        text="retry body",
        attempted_at=NOW,
    )
    assert first.queued_for_recovery is True
    assert calls == {"bark": 1, "feishu": 1}
    assert len(load_missed(settings.missed_queue_path)) == 1

    monkeypatch.setattr(
        "spx_spark.notifier.sinks.post_feishu",
        lambda *_: calls.__setitem__("feishu", calls["feishu"] + 1)
        or {"code": 0, "msg": "success"},
    )
    recovered = recover_pending_notifications(
        settings,
        now=NOW.replace(second=15),
    )

    assert recovered["delivered_targets"] == 1
    assert recovered["pending_targets"] == 0
    assert calls == {"bark": 1, "feishu": 2}
    assert load_missed(settings.missed_queue_path) == []


def test_legacy_jsonl_entry_is_imported_and_delivered_by_sqlite_worker(
    tmp_path,
    monkeypatch,
) -> None:
    settings = _settings(tmp_path)
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
        lambda *_: (_ for _ in ()).throw(AssertionError("Bark must not replay")),
    )

    recovered = recover_pending_notifications(settings, now=NOW)

    assert recovered["imported_legacy"] == 1
    assert recovered["delivered_targets"] == 1
    assert load_missed(settings.missed_queue_path) == []


def test_permanent_http_error_dead_letters_immediately_and_notifies_ops(
    tmp_path,
    monkeypatch,
) -> None:
    import urllib.error

    settings = _settings(tmp_path)
    bark_calls = {"count": 0}

    def bark_413(url, payload, timeout):
        bark_calls["count"] += 1
        raise urllib.error.HTTPError(url, 413, "Request Entity Too Large", {}, None)

    monkeypatch.setattr("spx_spark.notifier.sinks.post_bark", bark_413)
    monkeypatch.setattr(
        "spx_spark.notifier.sinks.post_feishu",
        lambda *_: {"code": 0, "msg": "success"},
    )

    result = dispatch_notification(
        settings,
        _envelope("event-413"),
        title="SPX warning",
        text="oversized body",
        attempted_at=NOW,
    )

    assert result.outcome == "dead_letter"
    assert bark_calls["count"] == 1  # deterministic 4xx is not retried
    summary = _outbox(settings).summary("event-413")
    assert summary is not None
    assert summary.status is DeliveryStatus.DEAD_LETTER
    assert summary.dead_letter_targets == 1

    # Recovery does not re-attempt the dead letter; the one-shot ops alert also
    # fails while bark is down, so the dead letter stays unacknowledged.
    first = recover_pending_notifications(settings, now=NOW)
    assert bark_calls["count"] == 2  # the ops alert attempt, not a retry
    assert first["ok"] is False
    assert first["attempted_targets"] == 0
    assert first["dead_letter_notified"] == 0
    assert first["dead_letter_unacknowledged"] == 1

    # Once bark recovers the ops alert goes out and the dead letter is acked.
    monkeypatch.setattr(
        "spx_spark.notifier.sinks.post_bark",
        lambda *_: {"code": 200},
    )
    second = recover_pending_notifications(settings, now=NOW)
    assert second["ok"] is True
    assert second["dead_letter_notified"] == 1
    assert second["dead_letter_unacknowledged"] == 0
    assert second["dead_letter_total"] == 1

    # Acknowledged history no longer fails recovery and alerts are not repeated.
    third = recover_pending_notifications(settings, now=NOW)
    assert third["ok"] is True
    assert third["dead_letter_notified"] == 0


def test_recovery_prunes_shadow_entry_once_event_dead_letters(
    tmp_path,
    monkeypatch,
) -> None:
    settings = _settings(tmp_path)
    monkeypatch.setattr(
        "spx_spark.notifier.sinks.post_bark",
        lambda *_: {"code": 200},
    )
    monkeypatch.setattr(
        "spx_spark.notifier.sinks.post_feishu",
        lambda *_: {"code": 19001, "msg": "failed"},
    )

    first = dispatch_notification(
        settings,
        _envelope("event-prune"),
        title="SPX warning",
        text="body",
        attempted_at=NOW,
    )
    assert first.queued_for_recovery is True
    assert len(load_missed(settings.missed_queue_path)) == 1

    # Exhaust the feishu retries (max_attempts=4, schedule 15s/60s/300s).
    recovered = None
    for offset in (15, 75, 375):
        cycle_now = NOW + timedelta(seconds=offset)
        recovered = recover_pending_notifications(
            settings,
            now=cycle_now,
            completion_clock=lambda cycle_now=cycle_now: cycle_now,
        )
    assert recovered is not None
    assert recovered["dead_lettered"] == 1
    assert recovered["pruned_shadow"] == 1
    assert recovered["ok"] is True
    summary = _outbox(settings).summary("event-prune")
    assert summary is not None
    assert summary.status is DeliveryStatus.DEAD_LETTER
    assert load_missed(settings.missed_queue_path) == []


def test_dead_letters_cli_lists_acks_and_replays(tmp_path, monkeypatch, capsys) -> None:
    from spx_spark.notifier.dead_letters import run as dead_letters_run

    settings = _settings(tmp_path)
    monkeypatch.setattr(
        NotificationSettings,
        "from_env",
        classmethod(lambda cls: settings),
    )
    outbox = _outbox(settings)
    outbox.enqueue(
        _envelope("event-cli"),
        title="SPX warning",
        text="body",
        feishu_text=None,
        friend=False,
        targets=("bark",),
        now=NOW,
    )
    outbox.claim_due(worker_id="w", limit_targets=1, now=NOW)
    outbox.settle_target(
        "event-cli",
        "bark",
        worker_id="w",
        ok=False,
        error="HTTP Error 413",
        permanent=True,
        now=NOW,
    )

    assert dead_letters_run(["list"]) == 0
    listing = json.loads(capsys.readouterr().out)
    assert [row["event_id"] for row in listing["dead_letters"]] == ["event-cli"]

    assert dead_letters_run(["ack", "event-cli"]) == 0
    assert outbox.count_unacknowledged_dead_letters() == 0

    assert dead_letters_run(["replay", "event-cli"]) == 0
    summary = outbox.summary("event-cli")
    assert summary is not None
    assert summary.status is DeliveryStatus.PENDING

    assert dead_letters_run(["ack", "missing-event"]) == 1
    assert dead_letters_run(["replay"]) == 1
