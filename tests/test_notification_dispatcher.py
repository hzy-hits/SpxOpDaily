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
    cancel_pending_notification,
    consume_pending_notifications,
    dispatch_notification,
    enqueue_notification,
    recover_pending_notifications,
)
from spx_spark.notifier.missed_queue import append_missed, load_missed
from spx_spark.notifier.receipts import (
    NotificationEnvelope,
    inspect_delivery_receipt_store,
)


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
    assert [entry["entry_id"] for entry in load_missed(settings.missed_queue_path)] == ["event-1"]
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
        rows = connection.execute(
            "SELECT event_id, lane, outcome, queued_for_recovery, sinks_json "
            "FROM notification_delivery_receipts ORDER BY outcome"
        ).fetchall()
    assert [
        (event_id, lane, outcome, queued) for event_id, lane, outcome, queued, _sinks_json in rows
    ] == [
        ("event-1", "market_warning", "delivered", 0),
        ("event-1", "market_warning", "pending", 1),
    ]
    receipts_by_outcome = {
        outcome: json.loads(sinks_json) for _event_id, _lane, outcome, _queued, sinks_json in rows
    }
    assert receipts_by_outcome["delivered"] == [
        {
            "sink": "bark",
            "attempted": True,
            "ok": True,
            "error": None,
            "verdict": "delivered",
        }
    ]
    assert receipts_by_outcome["pending"][0]["sink"] == "feishu"
    assert receipts_by_outcome["pending"][0]["attempted"] is True
    assert receipts_by_outcome["pending"][0]["ok"] is False
    assert receipts_by_outcome["pending"][0]["verdict"] == "pending"


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
        lambda *_: (
            calls.__setitem__("feishu", calls["feishu"] + 1) or {"code": 0, "msg": "success"}
        ),
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


def test_expired_pending_targets_are_audited_without_network_delivery(
    tmp_path,
    monkeypatch,
) -> None:
    settings = _settings(tmp_path)
    envelope = NotificationEnvelope(
        event_id="event-expired-pending",
        source="test",
        kind="market_warning",
        lane="market_warning",
        occurred_at=NOW,
        expires_at=NOW + timedelta(seconds=20),
    )
    enqueue_notification(
        settings,
        envelope,
        title="SPX warning",
        text="short-lived body",
        enqueued_at=NOW,
    )
    monkeypatch.setattr(
        "spx_spark.notifier.dispatcher.deliver_trade_push",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("expired notification must not reach a network sink")
        ),
    )
    expired_at = NOW + timedelta(seconds=21)

    consumed = consume_pending_notifications(
        settings,
        now=expired_at,
        notify_dead_letters=False,
        completion_clock=lambda: expired_at,
    )

    assert consumed["jobs"] == 0
    assert consumed["attempted_targets"] == 0
    assert consumed["delivered_targets"] == 0
    assert consumed["expired_targets"] == 2
    assert consumed["dead_lettered"] == 2
    assert consumed["dead_letter_unacknowledged"] == 2
    assert consumed["terminal_receipts_recorded"] == 2
    assert consumed["terminal_receipts_pending"] == 0
    assert consumed["ok"] is False
    with sqlite3.connect(settings.delivery_outbox_path) as connection:
        targets = connection.execute(
            """
            SELECT sink, status, last_error, acknowledged_at
            FROM notification_delivery_targets
            WHERE event_id = ? ORDER BY sink
            """,
            (envelope.event_id,),
        ).fetchall()
        terminal = connection.execute(
            """
            SELECT sink, outcome, reason, recorded_at
            FROM notification_delivery_terminal_receipts
            WHERE event_id = ? ORDER BY sink
            """,
            (envelope.event_id,),
        ).fetchall()
    assert all(row[1] == "dead_letter" for row in targets)
    assert all(row[2] == "notification_expired_before_delivery" for row in targets)
    assert all(row[3] is None for row in targets)
    assert len(terminal) == 2
    assert all(row[1] == "expired_before_delivery" for row in terminal)
    assert all(row[2] == "notification_expired_before_delivery" for row in terminal)
    assert all(row[3] is not None for row in terminal)
    with sqlite3.connect(settings.delivery_receipt_path) as connection:
        receipt = connection.execute(
            """
            SELECT outcome, queued_for_recovery, sinks_json
            FROM notification_delivery_receipts
            WHERE event_id = ?
            """,
            (envelope.event_id,),
        ).fetchone()
    assert receipt is not None
    assert receipt[0:2] == ("expired_before_delivery", 0)
    assert {sink["sink"] for sink in json.loads(receipt[2])} == {"bark", "feishu"}
    assert all(sink["attempted"] is False for sink in json.loads(receipt[2]))

    repeated = consume_pending_notifications(
        settings,
        now=expired_at + timedelta(seconds=1),
        notify_dead_letters=False,
        completion_clock=lambda: expired_at + timedelta(seconds=1),
    )
    assert repeated["expired_targets"] == 0
    assert repeated["terminal_receipts_recorded"] == 0
    with sqlite3.connect(settings.delivery_receipt_path) as connection:
        receipt_count = connection.execute(
            "SELECT COUNT(*) FROM notification_delivery_receipts WHERE event_id = ?",
            (envelope.event_id,),
        ).fetchone()[0]
    assert receipt_count == 1


def test_claimed_target_rechecks_expiry_before_network(tmp_path, monkeypatch) -> None:
    settings = replace(
        _settings(tmp_path),
        feishu_enabled=False,
        feishu_webhook_url="",
    )
    envelope = NotificationEnvelope(
        event_id="event-expired-after-claim",
        source="test",
        kind="market_warning",
        lane="market_warning",
        occurred_at=NOW,
        expires_at=NOW + timedelta(seconds=20),
    )
    enqueue_notification(
        settings,
        envelope,
        title="SPX warning",
        text="expires while leased",
        enqueued_at=NOW,
    )
    monkeypatch.setattr(
        "spx_spark.notifier.dispatcher.deliver_trade_push",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("claimed expiry must be checked before network I/O")
        ),
    )
    expired_at = NOW + timedelta(seconds=21)

    consumed = consume_pending_notifications(
        settings,
        now=NOW,
        notify_dead_letters=False,
        completion_clock=lambda: expired_at,
    )

    assert consumed["jobs"] == 1
    assert consumed["attempted_targets"] == 0
    assert consumed["delivered_targets"] == 0
    assert consumed["expired_targets"] == 1
    assert consumed["dead_letter_unacknowledged"] == 1
    assert consumed["ok"] is False
    terminal = _outbox(settings).list_terminal_receipts(event_id=envelope.event_id)
    assert [(receipt.sink, receipt.outcome) for receipt in terminal] == [
        ("bark", "expired_before_delivery")
    ]


def test_cancellation_writes_queryable_terminal_receipt(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    envelope = _envelope("event-cancelled")
    enqueue_notification(
        settings,
        envelope,
        title="SPX warning",
        text="cancel before delivery",
        enqueued_at=NOW,
    )
    monkeypatch.setattr(
        "spx_spark.notifier.dispatcher.deliver_trade_push",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("cancelled notification must not be delivered")
        ),
    )

    cancelled = cancel_pending_notification(
        settings,
        envelope.event_id,
        now=NOW + timedelta(seconds=1),
        reason="source_candidate_no_longer_valid",
    )

    assert cancelled == 2
    outbox = _outbox(settings)
    terminal = outbox.list_terminal_receipts(event_id=envelope.event_id)
    assert len(terminal) == 2
    assert {receipt.outcome for receipt in terminal} == {"cancelled_before_delivery"}
    assert {receipt.reason for receipt in terminal} == {"source_candidate_no_longer_valid"}
    assert outbox.count_unrecorded_terminal_receipts() == 0
    assert outbox.count_unacknowledged_dead_letters() == 0
    consumed = consume_pending_notifications(
        settings,
        now=NOW + timedelta(seconds=2),
        notify_dead_letters=False,
        completion_clock=lambda: NOW + timedelta(seconds=2),
    )
    assert consumed["jobs"] == 0
    assert consumed["ok"] is True
    with sqlite3.connect(settings.delivery_receipt_path) as connection:
        receipt = connection.execute(
            """
            SELECT outcome, sinks_json
            FROM notification_delivery_receipts
            WHERE event_id = ?
            """,
            (envelope.event_id,),
        ).fetchone()
    assert receipt is not None
    assert receipt[0] == "cancelled_before_delivery"
    assert {sink["sink"] for sink in json.loads(receipt[1])} == {"bark", "feishu"}


def test_terminal_receipt_mirror_retries_from_atomic_outbox_audit(
    tmp_path,
    monkeypatch,
) -> None:
    from spx_spark.notifier import dispatcher

    settings = _settings(tmp_path)
    envelope = _envelope("event-cancelled-receipt-retry")
    enqueue_notification(
        settings,
        envelope,
        title="SPX warning",
        text="cancel before delivery",
        enqueued_at=NOW,
    )
    real_recorder = dispatcher.record_delivery_receipt
    monkeypatch.setattr(
        dispatcher,
        "record_delivery_receipt",
        lambda *_args, **_kwargs: False,
    )

    assert (
        cancel_pending_notification(
            settings,
            envelope.event_id,
            now=NOW + timedelta(seconds=1),
            reason="source_invalidated",
        )
        == 2
    )
    assert _outbox(settings).count_unrecorded_terminal_receipts() == 2
    failed_sync = consume_pending_notifications(
        settings,
        now=NOW + timedelta(seconds=2),
        notify_dead_letters=False,
        completion_clock=lambda: NOW + timedelta(seconds=2),
    )
    assert failed_sync["dead_letter_unacknowledged"] == 0
    assert failed_sync["terminal_receipts_pending"] == 2
    assert failed_sync["ok"] is False

    monkeypatch.setattr(dispatcher, "record_delivery_receipt", real_recorder)
    recovered = consume_pending_notifications(
        settings,
        now=NOW + timedelta(seconds=3),
        notify_dead_letters=False,
        completion_clock=lambda: NOW + timedelta(seconds=3),
    )
    assert recovered["terminal_receipts_recorded"] == 2
    assert recovered["terminal_receipts_pending"] == 0
    assert recovered["ok"] is True


def test_ordinary_delivery_receipt_mirror_retries_after_sink_success(
    tmp_path,
    monkeypatch,
) -> None:
    from spx_spark.notifier import dispatcher
    from spx_spark.notifier.model import SinkResult

    settings = replace(
        _settings(tmp_path),
        feishu_enabled=False,
        feishu_webhook_url="",
    )
    envelope = _envelope("event-delivered-receipt-retry")
    enqueue_notification(
        settings,
        envelope,
        title="SPX warning",
        text="deliver once",
        enqueued_at=NOW,
    )
    monkeypatch.setattr(
        dispatcher,
        "deliver_trade_push",
        lambda *_args, **_kwargs: [SinkResult(sink="bark", attempted=True, ok=True)],
    )
    real_recorder = dispatcher.record_delivery_receipt
    monkeypatch.setattr(
        dispatcher,
        "record_delivery_receipt",
        lambda *_args, **_kwargs: False,
    )

    delivered = consume_pending_notifications(
        settings,
        now=NOW,
        notify_dead_letters=False,
        completion_clock=lambda: NOW,
    )

    assert delivered["jobs"] == 1
    assert delivered["delivered_targets"] == 1
    assert delivered["terminal_receipts_pending"] == 1
    assert delivered["ok"] is False
    summary = _outbox(settings).summary(envelope.event_id)
    assert summary is not None
    assert summary.status is DeliveryStatus.DELIVERED

    monkeypatch.setattr(dispatcher, "record_delivery_receipt", real_recorder)
    recovered = consume_pending_notifications(
        settings,
        now=NOW + timedelta(seconds=1),
        notify_dead_letters=False,
        completion_clock=lambda: NOW + timedelta(seconds=1),
    )

    assert recovered["jobs"] == 0
    assert recovered["terminal_receipts_recorded"] == 1
    assert recovered["terminal_receipts_pending"] == 0
    assert recovered["ok"] is True
    with sqlite3.connect(settings.delivery_receipt_path) as connection:
        receipt = connection.execute(
            """
            SELECT outcome, queued_for_recovery, sinks_json
            FROM notification_delivery_receipts
            WHERE event_id = ?
            """,
            (envelope.event_id,),
        ).fetchone()
    assert receipt is not None
    assert receipt[0:2] == ("delivered", 0)
    assert json.loads(receipt[2]) == [
        {
            "sink": "bark",
            "attempted": True,
            "ok": True,
            "error": None,
            "verdict": "delivered",
        }
    ]


def test_receipt_store_uses_delete_full_and_exact_outbox_mirror_id(
    tmp_path,
    monkeypatch,
) -> None:
    from spx_spark.notifier import dispatcher
    from spx_spark.notifier.model import SinkResult

    settings = replace(
        _settings(tmp_path),
        feishu_enabled=False,
        feishu_webhook_url="",
    )
    envelope = _envelope("receipt-contract")
    enqueue_notification(
        settings,
        envelope,
        title="SPX warning",
        text="durable receipt",
        enqueued_at=NOW,
    )
    monkeypatch.setattr(
        dispatcher,
        "deliver_trade_push",
        lambda *_args, **_kwargs: [SinkResult(sink="bark", attempted=True, ok=True)],
    )

    consumed = consume_pending_notifications(
        settings,
        now=NOW,
        notify_dead_letters=False,
        completion_clock=lambda: NOW,
    )

    assert consumed["ok"] is True
    terminal = _outbox(settings).list_terminal_receipts(event_id=envelope.event_id)
    assert len(terminal) == 1
    receipt_id = terminal[0].receipt_id
    inspection = inspect_delivery_receipt_store(
        settings.delivery_receipt_path,
        required_mirror_ids=(receipt_id,),
    )
    assert inspection.ok is True
    assert inspection.quick_check == "ok"
    assert inspection.journal_mode == "delete"
    assert inspection.synchronous == "full"
    assert inspection.schema_present is True
    assert inspection.missing_mirror_ids == ()
    with sqlite3.connect(settings.delivery_receipt_path) as connection:
        mirror = connection.execute(
            """
            SELECT m.mirror_id, r.event_id
            FROM notification_delivery_receipt_mirrors AS m
            JOIN notification_delivery_receipts AS r USING (attempt_id)
            """
        ).fetchone()
    assert mirror == (receipt_id, envelope.event_id)


def test_deleted_recorded_receipt_row_is_reopened_and_remirrored(
    tmp_path,
    monkeypatch,
) -> None:
    from spx_spark.notifier import dispatcher
    from spx_spark.notifier.model import SinkResult

    settings = replace(
        _settings(tmp_path),
        feishu_enabled=False,
        feishu_webhook_url="",
    )
    envelope = _envelope("receipt-row-repair")
    enqueue_notification(
        settings,
        envelope,
        title="SPX warning",
        text="repair receipt",
        enqueued_at=NOW,
    )
    monkeypatch.setattr(
        dispatcher,
        "deliver_trade_push",
        lambda *_args, **_kwargs: [SinkResult(sink="bark", attempted=True, ok=True)],
    )
    first = consume_pending_notifications(
        settings,
        now=NOW,
        notify_dead_letters=False,
        completion_clock=lambda: NOW,
    )
    assert first["ok"] is True
    receipt_id = _outbox(settings).list_terminal_receipts(event_id=envelope.event_id)[0].receipt_id
    with sqlite3.connect(settings.delivery_receipt_path) as connection:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute(
            "DELETE FROM notification_delivery_receipts WHERE event_id = ?",
            (envelope.event_id,),
        )
    missing = inspect_delivery_receipt_store(
        settings.delivery_receipt_path,
        required_mirror_ids=(receipt_id,),
    )
    assert missing.ok is False
    assert missing.missing_mirror_ids == (receipt_id,)

    repaired = consume_pending_notifications(
        settings,
        now=NOW + timedelta(seconds=1),
        notify_dead_letters=False,
        completion_clock=lambda: NOW + timedelta(seconds=1),
    )

    assert repaired["jobs"] == 0
    assert repaired["terminal_receipts_repaired"] == 1
    assert repaired["terminal_receipts_recorded"] == 1
    assert repaired["terminal_receipts_pending"] == 0
    assert repaired["receipt_store_ok"] is True
    assert repaired["ok"] is True
    assert (
        inspect_delivery_receipt_store(
            settings.delivery_receipt_path,
            required_mirror_ids=(receipt_id,),
        ).ok
        is True
    )


def test_corrupt_receipt_store_keeps_worker_health_false(
    tmp_path,
    monkeypatch,
) -> None:
    from spx_spark.notifier import dispatcher
    from spx_spark.notifier.model import SinkResult

    settings = replace(
        _settings(tmp_path),
        feishu_enabled=False,
        feishu_webhook_url="",
    )
    envelope = _envelope("receipt-corruption")
    enqueue_notification(
        settings,
        envelope,
        title="SPX warning",
        text="corrupt after mirror",
        enqueued_at=NOW,
    )
    monkeypatch.setattr(
        dispatcher,
        "deliver_trade_push",
        lambda *_args, **_kwargs: [SinkResult(sink="bark", attempted=True, ok=True)],
    )
    assert (
        consume_pending_notifications(
            settings,
            now=NOW,
            notify_dead_letters=False,
            completion_clock=lambda: NOW,
        )["ok"]
        is True
    )
    with open(settings.delivery_receipt_path, "r+b") as database:
        database.seek(0)
        database.write(b"corrupt receipt store")

    unhealthy = consume_pending_notifications(
        settings,
        now=NOW + timedelta(seconds=1),
        notify_dead_letters=False,
        completion_clock=lambda: NOW + timedelta(seconds=1),
    )

    assert unhealthy["jobs"] == 0
    assert unhealthy["receipt_store_ok"] is False
    assert unhealthy["receipt_store_missing_mirror_ids"] == 1
    assert unhealthy["terminal_receipts_pending"] == 1
    assert unhealthy["ok"] is False


def test_idle_hot_cycle_does_not_quick_check_or_scan_all_receipts(
    tmp_path,
    monkeypatch,
) -> None:
    from spx_spark.notifier import receipt_mirror

    settings = _settings(tmp_path)
    first = consume_pending_notifications(
        settings,
        now=NOW,
        notify_dead_letters=False,
        completion_clock=lambda: NOW,
    )
    assert first["receipt_store_ok"] is True
    real_list_terminal_receipts = NotificationDeliveryOutbox.list_terminal_receipts
    list_calls: list[bool] = []

    def list_only_unrecorded(self, **kwargs):
        unrecorded_only = kwargs.get("unrecorded_only") is True
        list_calls.append(unrecorded_only)
        if not unrecorded_only:
            raise AssertionError("idle hot cycle must not scan receipt history")
        return real_list_terminal_receipts(self, **kwargs)

    monkeypatch.setattr(
        NotificationDeliveryOutbox,
        "list_terminal_receipts",
        list_only_unrecorded,
    )

    monkeypatch.setattr(
        receipt_mirror,
        "prepare_delivery_receipt_store",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("idle hot cycle must not prepare or quick-check")
        ),
    )
    monkeypatch.setattr(
        receipt_mirror,
        "inspect_delivery_receipt_store",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("idle hot cycle must not inspect the receipt store")
        ),
    )

    second = consume_pending_notifications(
        settings,
        now=NOW + timedelta(seconds=1),
        notify_dead_letters=False,
        completion_clock=lambda: NOW + timedelta(seconds=1),
    )

    assert second["jobs"] == 0
    assert second["terminal_receipts_pending"] == 0
    assert second["receipt_store_ok"] is True
    assert second["ok"] is True
    assert list_calls == [True]
