from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import threading

import pytest

from spx_spark.config import NotificationSettings
from spx_spark.notifier.delivery_outbox import DeliveryStatus
from spx_spark.notifier.dispatcher import (
    _delivery_outbox,
    consume_pending_notifications,
    enqueue_notification,
)
from spx_spark.notifier.model import SinkResult
from spx_spark.notifier.receipts import NotificationEnvelope


NOW = datetime(2026, 7, 19, 2, 0, tzinfo=timezone.utc)


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


def _envelope(event_id: str = "urgent-template-1") -> NotificationEnvelope:
    return NotificationEnvelope(
        event_id=event_id,
        source="trade_intent",
        kind="trade_intent",
        lane="trade_ready",
        occurred_at=NOW,
    )


def test_enqueue_is_network_free_and_consumer_delivers_exact_template(
    tmp_path,
    monkeypatch,
) -> None:
    settings = _settings(tmp_path)
    deliveries: list[dict[str, object]] = []

    def fail_writer(*_args, **_kwargs):
        raise AssertionError("the fixed urgent template must never invoke an LLM")

    def deliver(_settings, **kwargs):
        deliveries.append(kwargs)
        return [
            SinkResult(sink=target, attempted=True, ok=True) for target in sorted(kwargs["targets"])
        ]

    monkeypatch.setattr(
        "spx_spark.notifier.llm_writer.generate_push_text",
        fail_writer,
    )
    monkeypatch.setattr(
        "spx_spark.notifier.dispatcher.deliver_trade_push",
        deliver,
    )

    result = enqueue_notification(
        settings,
        _envelope(),
        title="SPX TRADE READY",
        text="fixed deterministic execution ticket",
        friend=False,
        feishu_text="fixed deterministic execution ticket",
        enqueued_at=NOW,
    )

    assert result.accepted is True
    assert result.inserted is True
    assert result.duplicate is False
    assert result.outcome == DeliveryStatus.PENDING.value
    assert result.queued_for_recovery is True
    assert deliveries == []
    pending = _delivery_outbox(settings).summary(result.envelope.event_id)
    assert pending is not None
    assert pending.pending_targets == 2

    first_consumed = consume_pending_notifications(
        settings,
        now=NOW,
        completion_clock=lambda: NOW,
    )
    second_consumed = consume_pending_notifications(
        settings,
        now=NOW,
        completion_clock=lambda: NOW,
    )

    assert first_consumed["jobs"] == 1
    assert second_consumed["jobs"] == 1
    assert first_consumed["delivered_targets"] == 1
    assert second_consumed["delivered_targets"] == 1
    assert len(deliveries) == 2
    assert deliveries[0]["text"] == "fixed deterministic execution ticket"
    assert deliveries[0]["feishu_text"] == "fixed deterministic execution ticket"
    assert [entry["targets"] for entry in deliveries] == [
        frozenset({"bark"}),
        frozenset({"feishu"}),
    ]
    final = _delivery_outbox(settings).summary(result.envelope.event_id)
    assert final is not None
    assert final.status is DeliveryStatus.DELIVERED


def test_duplicate_enqueue_is_idempotent_and_delivered_once(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    deliveries: list[frozenset[str]] = []

    def deliver(_settings, **kwargs):
        targets = frozenset(kwargs["targets"])
        deliveries.append(targets)
        return [SinkResult(sink=target, attempted=True, ok=True) for target in targets]

    monkeypatch.setattr(
        "spx_spark.notifier.dispatcher.deliver_trade_push",
        deliver,
    )
    kwargs = {
        "title": "SPX TRADE READY",
        "text": "same immutable ticket",
        "friend": False,
        "feishu_text": "same immutable ticket",
        "enqueued_at": NOW,
    }

    first = enqueue_notification(settings, _envelope("same-event"), **kwargs)
    second = enqueue_notification(settings, _envelope("same-event"), **kwargs)

    assert first.inserted is True
    assert second.accepted is True
    assert second.inserted is False
    assert second.duplicate is True
    consume_pending_notifications(settings, now=NOW)
    consume_pending_notifications(settings, now=NOW)
    assert deliveries == [frozenset({"bark"}), frozenset({"feishu"})]


def test_enqueue_rejects_event_id_collision_without_delivery(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path)
    monkeypatch.setattr(
        "spx_spark.notifier.dispatcher.deliver_trade_push",
        lambda *_args, **_kwargs: pytest.fail("producer must not deliver"),
    )
    enqueue_notification(
        settings,
        _envelope("collision"),
        title="SPX TRADE READY",
        text="first immutable ticket",
        enqueued_at=NOW,
    )

    with pytest.raises(ValueError, match="collision"):
        enqueue_notification(
            settings,
            _envelope("collision"),
            title="SPX TRADE READY",
            text="different ticket",
            enqueued_at=NOW,
        )


def test_delivery_worker_owns_delivery_but_not_fast_loop_dead_letter_alerts(
    tmp_path,
    monkeypatch,
) -> None:
    from spx_spark.notifier import delivery_worker

    settings = _settings(tmp_path)
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        delivery_worker.NotificationSettings,
        "from_env",
        classmethod(lambda cls: settings),
    )

    def consume(_settings, **kwargs):
        calls.append(kwargs)
        return {"ok": True, "jobs": 0}

    monkeypatch.setattr(delivery_worker, "consume_pending_notifications", consume)

    assert delivery_worker.run(["--once"]) == 0
    assert len(calls) == 1
    assert calls[0]["notify_dead_letters"] is False
    assert str(calls[0]["worker_id"]).startswith("notification-delivery:")


def test_dead_letter_duplicate_is_not_reported_as_accepted(tmp_path) -> None:
    settings = replace(
        _settings(tmp_path),
        feishu_enabled=False,
        feishu_webhook_url="",
    )
    envelope = _envelope("terminal-event")
    enqueue_notification(
        settings,
        envelope,
        title="SPX TRADE READY",
        text="immutable ticket",
        enqueued_at=NOW,
    )
    outbox = _delivery_outbox(settings)
    jobs = outbox.claim_due(worker_id="dead-letterer", limit_targets=1, now=NOW)
    assert jobs[0].targets == ("bark",)
    outbox.settle_target(
        envelope.event_id,
        "bark",
        worker_id="dead-letterer",
        ok=False,
        error="permanent",
        permanent=True,
        now=NOW,
    )

    duplicate = enqueue_notification(
        settings,
        envelope,
        title="SPX TRADE READY",
        text="immutable ticket",
        enqueued_at=NOW + timedelta(seconds=1),
    )

    assert duplicate.duplicate is True
    assert duplicate.accepted is False
    assert duplicate.delivered is False
    assert duplicate.queued_for_recovery is False
    assert duplicate.outcome == DeliveryStatus.DEAD_LETTER.value


def test_retry_delay_is_anchored_at_delivery_completion(tmp_path, monkeypatch) -> None:
    settings = replace(
        _settings(tmp_path),
        feishu_enabled=False,
        feishu_webhook_url="",
    )
    enqueue_notification(
        settings,
        _envelope("completion-clock"),
        title="SPX TRADE READY",
        text="immutable ticket",
        enqueued_at=NOW,
    )
    monkeypatch.setattr(
        "spx_spark.notifier.dispatcher.deliver_trade_push",
        lambda *_args, **_kwargs: [
            SinkResult(sink="bark", attempted=True, ok=False, error="timeout")
        ],
    )
    completed_at = NOW + timedelta(seconds=10)

    consumed = consume_pending_notifications(
        settings,
        now=NOW,
        notify_dead_letters=False,
        completion_clock=lambda: completed_at,
    )

    assert consumed["attempted_targets"] == 1
    outbox = _delivery_outbox(settings)
    assert outbox.claim_due(
        worker_id="too-early",
        limit_targets=1,
        now=NOW + timedelta(seconds=24),
    ) == []
    due = outbox.claim_due(
        worker_id="on-time",
        limit_targets=1,
        now=NOW + timedelta(seconds=25),
    )
    assert due[0].targets == ("bark",)


def test_lost_claim_is_recorded_without_crashing_consumer(tmp_path, monkeypatch) -> None:
    settings = replace(
        _settings(tmp_path),
        feishu_enabled=False,
        feishu_webhook_url="",
    )
    enqueue_notification(
        settings,
        _envelope("lost-claim"),
        title="SPX TRADE READY",
        text="immutable ticket",
        enqueued_at=NOW,
    )

    def deliver(_settings, **_kwargs):
        reclaimed = _delivery_outbox(settings).claim_due(
            worker_id="recovery:B",
            limit_targets=1,
            now=NOW + timedelta(seconds=181),
        )
        assert reclaimed[0].targets == ("bark",)
        return [SinkResult(sink="bark", attempted=True, ok=True)]

    monkeypatch.setattr(
        "spx_spark.notifier.dispatcher.deliver_trade_push",
        deliver,
    )

    consumed = consume_pending_notifications(
        settings,
        now=NOW,
        notify_dead_letters=False,
        worker_id="delivery:A",
        completion_clock=lambda: NOW + timedelta(seconds=182),
    )

    assert consumed["lost_claim_targets"] == 1
    assert consumed["delivered_targets"] == 0
    assert consumed["claimed_targets"] == 1


def test_delivery_worker_stops_after_current_single_target(tmp_path, monkeypatch) -> None:
    from spx_spark.notifier import delivery_worker

    settings = _settings(tmp_path)
    stop_event = threading.Event()
    calls = 0
    monkeypatch.setattr(
        delivery_worker.NotificationSettings,
        "from_env",
        classmethod(lambda cls: settings),
    )

    def consume(_settings, **_kwargs):
        nonlocal calls
        calls += 1
        stop_event.set()
        return {"ok": True, "jobs": 1, "attempted_targets": 1}

    monkeypatch.setattr(delivery_worker, "consume_pending_notifications", consume)

    assert delivery_worker.run(["--poll-seconds", "0.01"], stop_event=stop_event) == 0
    assert calls == 1
