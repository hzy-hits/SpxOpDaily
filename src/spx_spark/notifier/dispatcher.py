"""Compatibility boundary backed by the unified Huey notification queue."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone

from spx_spark.notifier.unified_delivery import (
    RetryableDeliveryError,
    cancel_final_notification,
    deliver_notification_event,
    engine_for_settings,
    enqueue_final_notification,
    enqueue_frozen_notification,
    freeze_route,
    frozen_route_for_event,
    inspect_final_notification,
    notification_exists,
    notification_payload,
    notification_payload_fingerprint,
    recover_notification_tasks,
    settings_use_default_database,
)
from spx_spark.config import NotificationSettings
from spx_spark.infrastructure.notifications import (
    NotificationStatus,
    due_event_ids,
    status_counts,
)
from spx_spark.notifier.model import DeliveryEventInspection
from spx_spark.notifier.human_policy import quiet_window_suppresses
from spx_spark.notifier.model import CommandRunner, SinkResult, default_runner
from spx_spark.notifier.model import NotificationEnvelope
from spx_spark.notifier.rust_ingress import deliver_operator_notification_cancellation
from spx_spark.notifier.sinks import deliver_trade_push


@dataclass(frozen=True)
class DispatchResult:
    envelope: NotificationEnvelope
    sinks: tuple[SinkResult, ...]
    outcome: str
    delivered: bool
    queued_for_recovery: bool
    recovery_sink: SinkResult | None = None
    forwarded_to_rust: bool = False


@dataclass(frozen=True)
class EnqueueResult:
    envelope: NotificationEnvelope
    targets: tuple[str, ...]
    outcome: str
    accepted: bool
    inserted: bool
    duplicate: bool
    delivered: bool
    queued_for_recovery: bool
    forwarded_to_rust: bool = False


QUIET_WINDOW_SINK = "quiet_window_policy"


def _quiet_window_sink() -> SinkResult:
    return SinkResult(
        sink=QUIET_WINDOW_SINK,
        attempted=False,
        ok=True,
        error="suppressed from RTH close until the next SPX GTH open",
        verdict="suppressed",
    )


def _schedule_event(settings: NotificationSettings, event_id: int) -> object | None:
    # Explicit non-default paths are isolated test/cutover stores. Importing the
    # process-global Huey app here would enqueue a job against the production
    # database while the event itself lives in the isolated database.
    if not settings_use_default_database(settings):
        return None
    from spx_spark.infrastructure.jobs import deliver_notification_event as task

    return task(event_id)


def _direct_deliver(runner: CommandRunner):
    def deliver(settings: NotificationSettings, **kwargs) -> list[SinkResult]:
        return deliver_trade_push(settings, runner=runner, **kwargs)

    return deliver


def _disabled(settings: NotificationSettings) -> bool:
    return not settings.notification_queue_enabled


def _store(settings: NotificationSettings):
    return engine_for_settings(settings)


def _enqueue_result(result) -> EnqueueResult:
    forwarded = result.targets == ("rust_ingress",) and result.outcome == "delivered"
    delivered = result.outcome == "delivered" and not forwarded
    return EnqueueResult(
        envelope=result.envelope,
        targets=result.targets,
        outcome=result.outcome,
        accepted=result.accepted,
        inserted=result.inserted > 0,
        duplicate=result.inserted == 0 and result.duplicate > 0,
        delivered=delivered,
        queued_for_recovery=result.outcome in {"pending", "processing", "failed"},
        forwarded_to_rust=forwarded,
    )


def cancel_pending_notification(
    settings: NotificationSettings,
    event_id: str,
    *,
    now: datetime,
    reason: str,
) -> int:
    store = _store(settings)
    targets, _operator_targets = frozen_route_for_event(store, event_id)
    if settings.rust_trader_notification_owner or "rust_ingress" in targets:
        result = deliver_operator_notification_cancellation(
            settings,
            event_id=event_id,
            cancelled_at=now,
            reason_code=reason,
        )
        if not result.ok:
            raise RuntimeError(result.error or "rust operator cancellation was not accepted")
    return cancel_final_notification(store, event_id, reason=reason, now=now)


def notification_event_exists(settings: NotificationSettings, event_id: str) -> bool:
    return bool(event_id) and not _disabled(settings) and notification_exists(
        _store(settings), event_id
    )


def notification_event_contract(
    settings: NotificationSettings,
    envelope: NotificationEnvelope,
    *,
    title: str,
    text: str,
    friend: bool = False,
    feishu_text: str | None = None,
) -> tuple[str, tuple[str, ...]]:
    routed, targets = freeze_route(settings, envelope, friend=friend)
    payload = notification_payload(
        routed,
        title=title,
        text=text,
        friend=friend,
        feishu_text=feishu_text,
    )
    return notification_payload_fingerprint(payload), targets


def inspect_notification_event(
    settings: NotificationSettings,
    envelope: NotificationEnvelope,
    *,
    title: str,
    text: str,
    friend: bool = False,
    feishu_text: str | None = None,
    expected_payload_fingerprint: str | None = None,
    expected_targets: tuple[str, ...] | None = None,
) -> DeliveryEventInspection:
    fingerprint, configured_targets = notification_event_contract(
        settings,
        envelope,
        title=title,
        text=text,
        friend=friend,
        feishu_text=feishu_text,
    )
    routed, _targets = freeze_route(settings, envelope, friend=friend)
    if _disabled(settings):
        return DeliveryEventInspection(
            event_id=envelope.event_id,
            exists=False,
            cancelled=False,
            payload_matches=False,
            targets_match=False,
            event_status=None,
            target_statuses=(),
            reason="outbox_unavailable",
        )
    return inspect_final_notification(
        _store(settings),
        routed,
        title=title,
        text=text,
        friend=friend,
        feishu_text=feishu_text,
        targets=expected_targets or configured_targets,
        expected_payload_fingerprint=expected_payload_fingerprint or fingerprint,
    )


def enqueue_notification(
    settings: NotificationSettings,
    envelope: NotificationEnvelope,
    *,
    title: str,
    text: str,
    friend: bool = False,
    feishu_text: str | None = None,
    enqueued_at: datetime | None = None,
) -> EnqueueResult:
    at = enqueued_at or datetime.now(tz=timezone.utc)
    if quiet_window_suppresses(envelope, now=at):
        return EnqueueResult(
            envelope=envelope,
            targets=(QUIET_WINDOW_SINK,),
            outcome="quiet_window_suppressed",
            accepted=True,
            inserted=False,
            duplicate=False,
            delivered=True,
            queued_for_recovery=False,
        )
    if _disabled(settings):
        return EnqueueResult(envelope, (), "outbox_disabled", False, False, False, False, False)
    result = enqueue_final_notification(
        settings,
        envelope,
        title=title,
        text=text,
        friend=friend,
        feishu_text=feishu_text,
        enqueued_at=at,
        engine=_store(settings),
        schedule=lambda event_id: _schedule_event(settings, event_id),
    )
    return _enqueue_result(result)


def enqueue_linked_notification(
    settings: NotificationSettings,
    envelope: NotificationEnvelope,
    *,
    causation_event_id: str,
    title: str,
    text: str,
    friend: bool = False,
    feishu_text: str | None = None,
    enqueued_at: datetime | None = None,
) -> EnqueueResult:
    if _disabled(settings):
        return EnqueueResult(envelope, (), "outbox_disabled", False, False, False, False, False)
    store = _store(settings)
    targets, operator_targets = frozen_route_for_event(store, causation_event_id)
    if not targets:
        return EnqueueResult(
            envelope, (), "causation_event_missing", False, False, False, False, False
        )
    routed = (
        replace(envelope, operator_targets=operator_targets)
        if targets == ("rust_ingress",)
        else replace(envelope, operator_targets=())
    )
    result = enqueue_frozen_notification(
        routed,
        targets,
        title=title,
        text=text,
        friend=friend,
        feishu_text=feishu_text,
        enqueued_at=enqueued_at or datetime.now(tz=timezone.utc),
        engine=store,
        schedule=lambda event_id: _schedule_event(settings, event_id),
    )
    return _enqueue_result(result)


def consume_pending_notifications(
    settings: NotificationSettings,
    *,
    runner: CommandRunner = default_runner,
    now: datetime | None = None,
    notify_dead_letters: bool = True,
    worker_id: str | None = None,
    completion_clock=None,
) -> dict[str, object]:
    del notify_dead_letters, worker_id, completion_clock
    at = now or datetime.now(tz=timezone.utc)
    store = _store(settings)
    retry_recovered, uncertain_recovered = recover_notification_tasks(
        store,
        schedule=lambda _event_id: None,
        now=at,
    )
    ids = due_event_ids(store, limit=1)
    attempted = delivered = forwarded = 0
    for event_id in ids:
        try:
            result = deliver_notification_event(
                event_id,
                settings=settings,
                engine=store,
                direct_deliver=_direct_deliver(runner),
                now=at,
            )
        except RetryableDeliveryError:
            attempted += 1
            continue
        attempted += int(result.attempted)
        delivered += int(result.ok and result.channel != "rust_ingress")
        forwarded += int(result.ok and result.channel == "rust_ingress")
    counts = status_counts(store)
    return {
        "ok": counts.get(NotificationStatus.UNCERTAIN.value, 0) == 0,
        "imported_legacy": 0,
        "jobs": len(ids),
        "attempted_targets": attempted,
        "delivered_targets": delivered,
        "rust_ingress_attempts": int(any(ids)),
        "forwarded_to_rust_targets": forwarded,
        "pending_targets": counts.get(NotificationStatus.PENDING.value, 0),
        "claimed_targets": counts.get(NotificationStatus.PROCESSING.value, 0),
        "dead_lettered": counts.get(NotificationStatus.FAILED.value, 0),
        "expired_targets": 0,
        "lost_claim_targets": 0,
        "dead_letter_total": counts.get(NotificationStatus.FAILED.value, 0),
        "dead_letter_unacknowledged": counts.get(NotificationStatus.FAILED.value, 0),
        "dead_letter_notified": 0,
        "terminal_receipts_recorded": delivered,
        "terminal_receipts_pending": 0,
        "terminal_receipts_repaired": retry_recovered,
        "receipt_store_ok": uncertain_recovered == 0,
        "receipt_store_quick_check": "ok",
        "receipt_store_journal_mode": "delete",
        "receipt_store_synchronous": 2,
        "receipt_store_schema_present": True,
        "receipt_store_missing_mirror_ids": 0,
        "pruned_shadow": 0,
    }


def recover_pending_notifications(
    settings: NotificationSettings,
    *,
    runner: CommandRunner = default_runner,
    now: datetime | None = None,
    completion_clock=None,
) -> dict[str, object]:
    return consume_pending_notifications(
        settings,
        runner=runner,
        now=now,
        completion_clock=completion_clock,
    )


def dispatch_notification(
    settings: NotificationSettings,
    envelope: NotificationEnvelope,
    *,
    title: str,
    text: str,
    friend: bool = False,
    feishu_text: str | None = None,
    runner: CommandRunner = default_runner,
    recover_missed: bool = True,
    attempted_at: datetime | None = None,
) -> DispatchResult:
    del recover_missed
    at = attempted_at or datetime.now(tz=timezone.utc)
    if quiet_window_suppresses(envelope, now=at):
        sink = _quiet_window_sink()
        return DispatchResult(envelope, (sink,), "quiet_window_suppressed", True, False)
    if _disabled(settings):
        sinks = tuple(
            deliver_trade_push(
                settings,
                title=title,
                text=text,
                kind=envelope.kind,
                lane="ops" if envelope.lane == "ops_transition" else "trade",
                friend=friend,
                feishu_text=feishu_text,
                runner=runner,
            )
        )
        delivered = any(sink.ok for sink in sinks)
        attempted = any(sink.attempted for sink in sinks)
        return DispatchResult(
            envelope,
            sinks,
            "delivered" if delivered else "failed" if attempted else "no_sink",
            delivered,
            False,
        )
    queued = enqueue_final_notification(
        settings,
        envelope,
        title=title,
        text=text,
        friend=friend,
        feishu_text=feishu_text,
        enqueued_at=at,
        engine=_store(settings),
        schedule=lambda _event_id: None,
    )
    sinks: list[SinkResult] = []
    for event_id in queued.event_ids:
        try:
            delivered = deliver_notification_event(
                event_id,
                settings=settings,
                engine=_store(settings),
                direct_deliver=_direct_deliver(runner),
                now=at,
            )
        except RetryableDeliveryError as exc:
            sinks.append(SinkResult("notification", attempted=True, ok=False, error=str(exc)))
            continue
        sinks.append(
            SinkResult(
                delivered.channel or "notification",
                attempted=delivered.attempted,
                ok=delivered.ok,
                verdict=delivered.outcome,
            )
        )
    direct_ok = any(sink.ok and sink.sink != "rust_ingress" for sink in sinks)
    forwarded = any(sink.ok and sink.sink == "rust_ingress" for sink in sinks)
    outcome = "forwarded_to_rust" if forwarded else "delivered" if direct_ok else queued.outcome
    return DispatchResult(
        queued.envelope,
        tuple(sinks),
        outcome,
        direct_ok,
        not direct_ok and not forwarded and queued.accepted,
        forwarded_to_rust=forwarded,
    )
