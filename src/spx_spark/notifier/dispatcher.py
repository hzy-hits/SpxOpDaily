"""One durable delivery boundary for every human-facing notification."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import os

from spx_spark.config import NotificationSettings
from spx_spark.notifier.delivery_outbox import (
    DeliveryJob,
    DeliveryStatus,
    NotificationDeliveryOutbox,
)
from spx_spark.notifier.missed_queue import (
    ack_missed_event_ids,
    append_missed,
    flush_missed,
    load_missed,
)
from spx_spark.notifier.model import CommandRunner, SinkResult, default_runner
from spx_spark.notifier.receipts import NotificationEnvelope, record_delivery_receipt
from spx_spark.notifier.sinks import (
    any_delivery_ok,
    deliver_trade_push,
    delivery_target_names,
    im_delivery_failed,
)


@dataclass(frozen=True)
class DispatchResult:
    envelope: NotificationEnvelope
    sinks: tuple[SinkResult, ...]
    outcome: str
    delivered: bool
    queued_for_recovery: bool
    recovery_sink: SinkResult | None = None


@dataclass(frozen=True)
class _JobResult:
    sinks: tuple[SinkResult, ...]
    status: DeliveryStatus
    delivered_targets: int
    pending_targets: int
    dead_lettered_targets: int


def _delivery_outbox(settings: NotificationSettings) -> NotificationDeliveryOutbox:
    return NotificationDeliveryOutbox(
        settings.delivery_outbox_path,
        max_attempts=settings.delivery_outbox_max_attempts,
        retry_schedule_seconds=settings.delivery_outbox_retry_schedule_seconds,
        dead_letter_after_seconds=settings.delivery_outbox_dead_letter_after_seconds,
        claim_stale_after_seconds=settings.delivery_outbox_claim_stale_after_seconds,
    )


def _transport_lane(envelope: NotificationEnvelope) -> str:
    return "ops" if envelope.lane == "ops_transition" else "trade"


def _deliver_claimed_job(
    settings: NotificationSettings,
    outbox: NotificationDeliveryOutbox,
    job: DeliveryJob,
    *,
    worker_id: str,
    runner: CommandRunner,
    attempted_at: datetime,
) -> _JobResult:
    sinks = deliver_trade_push(
        settings,
        title=job.title,
        text=job.text,
        kind=job.envelope.kind,
        lane=_transport_lane(job.envelope),
        friend=job.friend,
        feishu_text=job.feishu_text,
        runner=runner,
        targets=frozenset(job.targets),
    )
    sinks_by_name = {sink.sink: sink for sink in sinks}
    normalized_sinks = list(sinks)
    delivered_targets = 0
    dead_lettered_targets = 0
    for target in job.targets:
        sink = sinks_by_name.get(target)
        if sink is None:
            sink = SinkResult(
                sink=target,
                attempted=False,
                ok=False,
                error="configured delivery target is currently unavailable",
            )
            normalized_sinks.append(sink)
        status = outbox.settle_target(
            job.envelope.event_id,
            target,
            worker_id=worker_id,
            ok=sink.ok,
            error=sink.error,
            permanent=sink.permanent,
            now=attempted_at,
        )
        delivered_targets += int(sink.ok)
        dead_lettered_targets += int(status is DeliveryStatus.DEAD_LETTER)

    summary = outbox.summary(job.envelope.event_id)
    if summary is None:
        raise RuntimeError(f"delivery event disappeared: {job.envelope.event_id}")
    pending = summary.pending_targets + summary.claimed_targets
    if summary.status is DeliveryStatus.DELIVERED:
        ack_missed_event_ids(
            settings.missed_queue_path,
            frozenset({job.envelope.event_id}),
        )
    record_delivery_receipt(
        settings.delivery_receipt_path,
        job.envelope,
        sinks=normalized_sinks,
        outcome=summary.status.value,
        queued_for_recovery=pending > 0,
        attempted_at=attempted_at,
    )
    return _JobResult(
        sinks=tuple(normalized_sinks),
        status=summary.status,
        delivered_targets=delivered_targets,
        pending_targets=pending,
        dead_lettered_targets=dead_lettered_targets,
    )


def _migrate_legacy_queue(
    settings: NotificationSettings,
    outbox: NotificationDeliveryOutbox,
    *,
    now: datetime,
) -> int:
    """Import legacy Feishu-recovery rows without independently flushing them."""

    imported = 0
    for entry in load_missed(settings.missed_queue_path):
        event_id = str(entry.get("entry_id") or "").strip()
        if not event_id or outbox.contains(event_id):
            continue
        at_raw = str(entry.get("at") or "")
        try:
            occurred_at = datetime.fromisoformat(at_raw.replace("Z", "+00:00"))
        except ValueError:
            occurred_at = datetime.now(tz=timezone.utc)
        if occurred_at.tzinfo is None:
            occurred_at = occurred_at.replace(tzinfo=timezone.utc)
        envelope = NotificationEnvelope(
            event_id=event_id,
            source="legacy_missed_queue",
            kind=str(entry.get("kind") or "legacy_missed"),
            lane="scheduled_report",
            occurred_at=occurred_at,
        )
        accepted = outbox.enqueue(
            envelope,
            title="SPX 错过提醒",
            text=str(entry.get("message") or ""),
            feishu_text=None,
            friend=False,
            targets=("feishu",),
            now=now,
        )
        imported += int(accepted)
    return imported


def _notify_dead_letters(
    settings: NotificationSettings,
    outbox: NotificationDeliveryOutbox,
    *,
    runner: CommandRunner,
    now: datetime,
) -> int:
    """Push a one-shot ops alert for new dead letters, then acknowledge them.

    The ops message goes straight to the sinks, never through the outbox, so a
    broken channel cannot turn the alert about dead letters into another dead
    letter. When every configured sink fails, the dead letters stay
    unacknowledged and the next recovery run retries the ops alert; when no
    sink is configured at all, they are acknowledged to avoid poisoning the
    recovery health check forever.
    """

    dead_letters = outbox.list_dead_letters(unacknowledged_only=True)
    if not dead_letters:
        return 0
    sinks = deliver_trade_push(
        settings,
        title="SPX 投递死信告警",
        text=f"{len(dead_letters)} 条告警投递死信，请检查投递链路。",
        kind="status",
        lane="ops",
        friend=False,
        runner=runner,
    )
    attempted = any(sink.attempted for sink in sinks)
    delivered = any(sink.ok for sink in sinks if sink.attempted)
    if attempted and not delivered:
        return 0
    for event_id in {str(entry["event_id"]) for entry in dead_letters}:
        outbox.acknowledge_dead_letter(event_id, now=now)
    return len(dead_letters)


def _prune_terminal_shadow_entries(
    settings: NotificationSettings,
    outbox: NotificationDeliveryOutbox,
) -> int:
    """Drop legacy-shadow rows whose event reached a terminal outbox state.

    The JSONL shadow exists only for rollback to the pre-outbox path; outbox
    mode never calls ``flush_missed``, so rows whose event delivered or
    dead-lettered in SQLite would otherwise linger forever.
    """

    terminal_ids: set[str] = set()
    for entry in load_missed(settings.missed_queue_path):
        event_id = str(entry.get("entry_id") or "")
        if not event_id:
            continue
        summary = outbox.summary(event_id)
        if summary is not None and summary.status in (
            DeliveryStatus.DELIVERED,
            DeliveryStatus.DEAD_LETTER,
        ):
            terminal_ids.add(event_id)
    if not terminal_ids:
        return 0
    ack_missed_event_ids(settings.missed_queue_path, frozenset(terminal_ids))
    return len(terminal_ids)


def recover_pending_notifications(
    settings: NotificationSettings,
    *,
    runner: CommandRunner = default_runner,
    now: datetime | None = None,
) -> dict[str, object]:
    """Drain due sink targets without resending targets already delivered."""

    now = now or datetime.now(tz=timezone.utc)
    outbox = _delivery_outbox(settings)
    imported = _migrate_legacy_queue(settings, outbox, now=now)
    worker_id = f"notification-recovery:{os.getpid()}"
    jobs = outbox.claim_due(
        worker_id=worker_id,
        limit_targets=settings.delivery_outbox_recovery_batch_size,
        now=now,
    )
    attempted_targets = 0
    delivered_targets = 0
    dead_lettered = 0
    for job in jobs:
        result = _deliver_claimed_job(
            settings,
            outbox,
            job,
            worker_id=worker_id,
            runner=runner,
            attempted_at=now,
        )
        attempted_targets += len(job.targets)
        delivered_targets += result.delivered_targets
        dead_lettered += result.dead_lettered_targets
    counts = outbox.count_targets()
    dead_letter_total = counts.get(DeliveryStatus.DEAD_LETTER.value, 0)
    dead_letter_notified = _notify_dead_letters(settings, outbox, runner=runner, now=now)
    # Health is judged only by dead letters nobody has reviewed yet; history
    # alone must not fail the task forever.
    dead_letter_unacknowledged = outbox.count_unacknowledged_dead_letters()
    pruned_shadow = _prune_terminal_shadow_entries(settings, outbox)
    return {
        "ok": dead_letter_unacknowledged == 0,
        "imported_legacy": imported,
        "jobs": len(jobs),
        "attempted_targets": attempted_targets,
        "delivered_targets": delivered_targets,
        "pending_targets": counts.get(DeliveryStatus.PENDING.value, 0),
        "claimed_targets": counts.get(DeliveryStatus.CLAIMED.value, 0),
        "dead_lettered": dead_lettered,
        "dead_letter_total": dead_letter_total,
        "dead_letter_unacknowledged": dead_letter_unacknowledged,
        "dead_letter_notified": dead_letter_notified,
        "pruned_shadow": pruned_shadow,
    }


def _dispatch_via_outbox(
    settings: NotificationSettings,
    envelope: NotificationEnvelope,
    *,
    title: str,
    text: str,
    friend: bool,
    feishu_text: str | None,
    runner: CommandRunner,
    attempted_at: datetime,
) -> DispatchResult:
    transport_lane = _transport_lane(envelope)
    targets = delivery_target_names(
        settings,
        lane=transport_lane,
        friend=friend,
    )
    if not targets:
        record_delivery_receipt(
            settings.delivery_receipt_path,
            envelope,
            sinks=(),
            outcome="no_sink",
            queued_for_recovery=False,
            attempted_at=attempted_at,
        )
        return DispatchResult(
            envelope=envelope,
            sinks=(),
            outcome="no_sink",
            delivered=False,
            queued_for_recovery=False,
        )

    outbox = _delivery_outbox(settings)
    outbox.enqueue(
        envelope,
        title=title,
        text=text,
        feishu_text=feishu_text,
        friend=friend,
        targets=targets,
        now=attempted_at,
    )
    worker_id = f"notification-inline:{os.getpid()}"
    jobs = outbox.claim_due(
        worker_id=worker_id,
        limit_targets=len(targets),
        now=attempted_at,
        event_id=envelope.event_id,
    )
    sinks: tuple[SinkResult, ...] = ()
    if jobs:
        result = _deliver_claimed_job(
            settings,
            outbox,
            jobs[0],
            worker_id=worker_id,
            runner=runner,
            attempted_at=attempted_at,
        )
        sinks = result.sinks
    summary = outbox.summary(envelope.event_id)
    if summary is None:
        raise RuntimeError(f"delivery event disappeared: {envelope.event_id}")
    queued = summary.pending_targets + summary.claimed_targets > 0
    if queued and settings.delivery_outbox_legacy_shadow_enabled:
        append_missed(
            settings.missed_queue_path,
            text,
            kind=envelope.kind,
            at=envelope.occurred_at,
            event_id=envelope.event_id,
        )
    if summary.status is DeliveryStatus.DELIVERED:
        ack_missed_event_ids(settings.missed_queue_path, frozenset({envelope.event_id}))
    return DispatchResult(
        envelope=envelope,
        sinks=sinks,
        outcome=summary.status.value,
        # Preserve existing call-site semantics: one successful human sink is
        # enough to mark the source alert handled, while the outbox continues
        # retrying any other target independently.
        delivered=summary.delivered_targets > 0,
        queued_for_recovery=queued,
    )


def _dispatch_legacy(
    settings: NotificationSettings,
    envelope: NotificationEnvelope,
    *,
    title: str,
    text: str,
    friend: bool,
    feishu_text: str | None,
    runner: CommandRunner,
    recover_missed: bool,
    attempted_at: datetime,
) -> DispatchResult:
    recovery_sink = flush_missed(settings, runner=runner) if recover_missed else None
    sinks = deliver_trade_push(
        settings,
        title=title,
        text=text,
        kind=envelope.kind,
        lane=_transport_lane(envelope),
        friend=friend,
        feishu_text=feishu_text,
        runner=runner,
    )
    delivered = any_delivery_ok(sinks)
    queued = im_delivery_failed(sinks)
    if queued:
        append_missed(
            settings.missed_queue_path,
            text,
            kind=envelope.kind,
            at=envelope.occurred_at,
            event_id=envelope.event_id,
        )
    attempted = any(sink.attempted for sink in sinks)
    outcome = "delivered" if delivered else "failed" if attempted else "no_sink"
    record_delivery_receipt(
        settings.delivery_receipt_path,
        envelope,
        sinks=sinks,
        outcome=outcome,
        queued_for_recovery=queued,
        attempted_at=attempted_at,
    )
    return DispatchResult(
        envelope=envelope,
        sinks=tuple(sinks),
        outcome=outcome,
        delivered=delivered,
        queued_for_recovery=queued,
        recovery_sink=recovery_sink,
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
    """Persist before network I/O, deliver immediately, and retry per sink."""

    envelope.validate()
    attempted_at = attempted_at or datetime.now(tz=timezone.utc)
    if settings.delivery_outbox_enabled and settings.delivery_outbox_path:
        return _dispatch_via_outbox(
            settings,
            envelope,
            title=title,
            text=text,
            friend=friend,
            feishu_text=feishu_text,
            runner=runner,
            attempted_at=attempted_at,
        )
    return _dispatch_legacy(
        settings,
        envelope,
        title=title,
        text=text,
        friend=friend,
        feishu_text=feishu_text,
        runner=runner,
        recover_missed=recover_missed,
        attempted_at=attempted_at,
    )
