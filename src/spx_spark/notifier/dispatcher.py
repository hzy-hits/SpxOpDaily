"""One durable delivery boundary for every human-facing notification."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime, timezone
import os

from spx_spark.config import NotificationSettings
from spx_spark.notifier.delivery_outbox import (
    DeliveryCancelled,
    DeliveryEventInspection,
    DeliveryStatus,
    NotificationDeliveryOutbox,
    TerminalDeliveryReceipt,
    delivery_payload_fingerprint,
)
from spx_spark.notifier.delivery_executor import (
    deliver_claimed_job as _deliver_claimed_job,
)
from spx_spark.notifier.missed_queue import (
    ack_missed_event_ids,
    append_missed,
    flush_missed,
    load_missed,
)
from spx_spark.notifier.model import CommandRunner, SinkResult, default_runner
from spx_spark.notifier.human_policy import quiet_window_suppresses
from spx_spark.notifier.receipts import (
    NotificationEnvelope,
    record_delivery_receipt,
)
from spx_spark.notifier.receipt_mirror import (
    ReceiptMirrorSync as _ReceiptMirrorSync,
    sync_terminal_receipts as _sync_receipt_mirrors,
)
from spx_spark.notifier.rust_ingress import (
    deliver_operator_notification_cancellation,
    operator_notification_role,
)
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
    forwarded_to_rust: bool = False


@dataclass(frozen=True)
class EnqueueResult:
    """Result of the producer-only notification boundary.

    ``accepted`` means the exact event is durably present in the SQLite
    outbox and has not terminally dead-lettered. ``inserted`` distinguishes a
    new row from an idempotent replay; neither case performs network I/O.
    """

    envelope: NotificationEnvelope
    targets: tuple[str, ...]
    outcome: str
    accepted: bool
    inserted: bool
    duplicate: bool
    delivered: bool
    queued_for_recovery: bool
    forwarded_to_rust: bool = False


ASYNC_CLAIM_TARGET_LIMIT = 1
QUIET_WINDOW_SINK = "quiet_window_policy"


def _quiet_window_sink() -> SinkResult:
    return SinkResult(
        sink=QUIET_WINDOW_SINK,
        attempted=False,
        ok=True,
        error="suppressed from RTH close until the next SPX GTH open",
        verdict="suppressed",
    )


def _quiet_dispatch_result(
    settings: NotificationSettings,
    envelope: NotificationEnvelope,
    *,
    attempted_at: datetime,
) -> DispatchResult:
    sink = _quiet_window_sink()
    record_delivery_receipt(
        settings.delivery_receipt_path,
        envelope,
        sinks=(sink,),
        outcome="quiet_window_suppressed",
        queued_for_recovery=False,
        attempted_at=attempted_at,
    )
    return DispatchResult(
        envelope=envelope,
        sinks=(sink,),
        outcome="quiet_window_suppressed",
        delivered=True,
        queued_for_recovery=False,
    )


def _delivery_outbox(settings: NotificationSettings) -> NotificationDeliveryOutbox:
    return NotificationDeliveryOutbox(
        settings.delivery_outbox_path,
        max_attempts=settings.delivery_outbox_max_attempts,
        retry_schedule_seconds=settings.delivery_outbox_retry_schedule_seconds,
        dead_letter_after_seconds=settings.delivery_outbox_dead_letter_after_seconds,
        claim_stale_after_seconds=settings.delivery_outbox_claim_stale_after_seconds,
    )


def _sync_terminal_receipts(
    settings: NotificationSettings,
    outbox: NotificationDeliveryOutbox,
    *,
    now: datetime,
) -> _ReceiptMirrorSync:
    """Mirror atomic outbox terminal audit rows into the standard receipt DB."""

    return _sync_receipt_mirrors(
        settings,
        outbox,
        now=now,
        recorder=record_delivery_receipt,
    )


def _transport_lane(envelope: NotificationEnvelope) -> str:
    return "ops" if envelope.lane == "ops_transition" else "trade"


def _delivery_route(
    settings: NotificationSettings,
    envelope: NotificationEnvelope,
    *,
    friend: bool,
) -> tuple[NotificationEnvelope, tuple[str, ...]]:
    """Freeze either direct Python sinks or one Rust-owned staging target."""

    intended_targets = tuple(
        sorted(
            delivery_target_names(
                settings,
                lane=_transport_lane(envelope),
                friend=friend,
            )
        )
    )
    if (
        settings.rust_trader_notification_owner
        and operator_notification_role(envelope) is not None
    ):
        entries = tuple(settings.rust_operator_notification_target_map)
        mapped_sinks = tuple(entry[0] for entry in entries)
        mapped_keys = tuple(entry[1] for entry in entries)
        if (
            not intended_targets
            or len(set(mapped_sinks)) != len(mapped_sinks)
            or len(set(mapped_keys)) != len(mapped_keys)
        ):
            return envelope, ()
        mapping = {sink: (key, channel) for sink, key, channel in entries}
        if any(sink not in mapping for sink in intended_targets):
            return envelope, ()
        operator_targets = tuple(mapping[sink] for sink in intended_targets)
        return replace(envelope, operator_targets=operator_targets), ("rust_ingress",)
    return replace(
        envelope,
        operator_targets=(),
        operator_opportunity_id=None,
        operator_generation=0,
    ), intended_targets


def cancel_pending_notification(
    settings: NotificationSettings,
    event_id: str,
    *,
    now: datetime,
    reason: str,
) -> int:
    """Cancel a durable notification whose source lifecycle is no longer valid."""

    outbox = (
        _delivery_outbox(settings)
        if getattr(settings, "delivery_outbox_enabled", False)
        and getattr(settings, "delivery_outbox_path", "")
        else None
    )
    rust_owned = getattr(settings, "rust_trader_notification_owner", False) or (
        outbox is not None and "rust_ingress" in outbox.event_targets(event_id)
    )
    if rust_owned:
        cancellation = deliver_operator_notification_cancellation(
            settings,
            event_id=event_id,
            cancelled_at=now,
            reason_code=reason,
        )
        if not cancellation.ok:
            raise RuntimeError(
                cancellation.error or "rust operator cancellation was not accepted"
            )
    if outbox is None:
        return 0
    terminal_receipts = outbox.cancel_event_with_receipts(
        event_id,
        reason=reason,
        now=now,
    )
    if not outbox.cancellation_exists(event_id):
        raise RuntimeError(f"cancellation fence missing for {event_id}")
    cancelled = len(terminal_receipts)
    _sync_terminal_receipts(settings, outbox, now=now)
    ack_missed_event_ids(settings.missed_queue_path, frozenset({event_id}))
    return cancelled


def notification_event_exists(
    settings: NotificationSettings,
    event_id: str,
) -> bool:
    """Read-only reconciliation hook for producer state-ack crash recovery."""

    if (
        not event_id
        or not getattr(settings, "delivery_outbox_enabled", False)
        or not getattr(settings, "delivery_outbox_path", "")
    ):
        return False
    return _delivery_outbox(settings).contains(event_id)


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
    """Reconcile one producer event against its exact durable outbox contract."""

    payload_fingerprint, configured_targets = notification_event_contract(
        settings,
        envelope,
        title=title,
        text=text,
        friend=friend,
        feishu_text=feishu_text,
    )
    routed_envelope, _routed_targets = _delivery_route(
        settings,
        envelope,
        friend=friend,
    )
    targets = configured_targets if expected_targets is None else expected_targets
    if not getattr(settings, "delivery_outbox_enabled", False) or not getattr(
        settings, "delivery_outbox_path", ""
    ):
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
    return _delivery_outbox(settings).inspect_event(
        routed_envelope,
        title=title,
        text=text,
        feishu_text=feishu_text,
        friend=friend,
        targets=targets,
        expected_payload_fingerprint=(
            expected_payload_fingerprint or payload_fingerprint
        ),
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
    """Return the immutable payload and exact configured-target identities."""

    routed_envelope, targets = _delivery_route(settings, envelope, friend=friend)
    return (
        delivery_payload_fingerprint(
            routed_envelope,
            title=title,
            text=text,
            feishu_text=feishu_text,
            friend=friend,
        ),
        targets,
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
    """Persist one final notification template and return without delivery.

    This is the latency-critical producer API.  The supplied text is already
    final: this function never invokes an LLM/reviewer and never opens a Bark
    or Feishu connection.  ``consume_pending_notifications`` owns all claims,
    delivery attempts and retries.

    Replaying the same ``event_id``, payload and exact sink set is successful
    and reported as ``duplicate=True``. Reusing an ``event_id`` for different
    content or targets remains a hard collision in
    ``NotificationDeliveryOutbox``.
    """

    envelope.validate()
    at = enqueued_at or datetime.now(tz=timezone.utc)
    if quiet_window_suppresses(envelope, now=at):
        sink = _quiet_window_sink()
        record_delivery_receipt(
            settings.delivery_receipt_path,
            envelope,
            sinks=(sink,),
            outcome="quiet_window_suppressed",
            queued_for_recovery=False,
            attempted_at=at,
        )
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
    if not settings.delivery_outbox_enabled or not settings.delivery_outbox_path:
        return EnqueueResult(
            envelope=envelope,
            targets=(),
            outcome="outbox_disabled",
            accepted=False,
            inserted=False,
            duplicate=False,
            delivered=False,
            queued_for_recovery=False,
        )

    routed_envelope, targets = _delivery_route(settings, envelope, friend=friend)
    if not targets:
        return EnqueueResult(
            envelope=routed_envelope,
            targets=(),
            outcome="no_sink",
            accepted=False,
            inserted=False,
            duplicate=False,
            delivered=False,
            queued_for_recovery=False,
        )

    return _enqueue_with_frozen_route(
        settings,
        routed_envelope,
        title=title,
        text=text,
        friend=friend,
        feishu_text=feishu_text,
        enqueued_at=at,
        targets=targets,
    )


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
    """Enqueue a lifecycle event to the exact route frozen by its cause.

    A delivered READY may outlive a later configuration edit. Terminal risk
    messages therefore inherit its immutable Python target set and, when Rust
    owns fan-out, its immutable Bark/Feishu target identities.
    """

    envelope.validate()
    at = enqueued_at or datetime.now(tz=timezone.utc)
    if not settings.delivery_outbox_enabled or not settings.delivery_outbox_path:
        return EnqueueResult(
            envelope=envelope,
            targets=(),
            outcome="outbox_disabled",
            accepted=False,
            inserted=False,
            duplicate=False,
            delivered=False,
            queued_for_recovery=False,
        )
    outbox = _delivery_outbox(settings)
    targets = outbox.event_targets(causation_event_id)
    if not targets:
        return EnqueueResult(
            envelope=envelope,
            targets=(),
            outcome="causation_event_missing",
            accepted=False,
            inserted=False,
            duplicate=False,
            delivered=False,
            queued_for_recovery=False,
        )
    if targets == ("rust_ingress",):
        operator_targets = outbox.event_operator_targets(causation_event_id)
        if not operator_targets:
            return EnqueueResult(
                envelope=envelope,
                targets=targets,
                outcome="causation_operator_targets_missing",
                accepted=False,
                inserted=False,
                duplicate=False,
                delivered=False,
                queued_for_recovery=False,
            )
        routed_envelope = replace(envelope, operator_targets=operator_targets)
    else:
        routed_envelope = replace(
            envelope,
            operator_targets=(),
        )
    return _enqueue_with_frozen_route(
        settings,
        routed_envelope,
        title=title,
        text=text,
        friend=friend,
        feishu_text=feishu_text,
        enqueued_at=at,
        targets=targets,
    )


def _enqueue_with_frozen_route(
    settings: NotificationSettings,
    envelope: NotificationEnvelope,
    *,
    title: str,
    text: str,
    friend: bool,
    feishu_text: str | None,
    enqueued_at: datetime,
    targets: tuple[str, ...],
) -> EnqueueResult:
    """Persist one immutable payload against an already-resolved route."""

    outbox = _delivery_outbox(settings)
    try:
        inserted = outbox.enqueue(
            envelope,
            title=title,
            text=text,
            feishu_text=feishu_text,
            friend=friend,
            targets=targets,
            now=enqueued_at,
        )
    except DeliveryCancelled:
        return EnqueueResult(
            envelope=envelope,
            targets=tuple(targets),
            outcome="cancelled_before_enqueue",
            accepted=False,
            inserted=False,
            duplicate=False,
            delivered=False,
            queued_for_recovery=False,
        )
    summary = outbox.summary(envelope.event_id)
    if summary is None:  # Defensive: enqueue and summary share one durable DB.
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
    return EnqueueResult(
        envelope=envelope,
        targets=tuple(targets),
        outcome=summary.status.value,
        accepted=summary.status is not DeliveryStatus.DEAD_LETTER,
        inserted=inserted,
        duplicate=not inserted,
        delivered=(
            summary.delivered_targets > 0 and "rust_ingress" not in targets
        ),
        queued_for_recovery=queued,
        forwarded_to_rust=(
            summary.delivered_targets > 0 and targets == ("rust_ingress",)
        ),
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
    ready_lanes = {
        "trade_ready",
        "gth_manual_candidate",
        "gth_level_manual_candidate",
    }
    ready_without_receipt = sorted(
        {
            str(entry["event_id"])
            for entry in dead_letters
            if str(entry.get("lane") or "") in ready_lanes
            and (
                (summary := outbox.summary(str(entry["event_id"]))) is None
                or summary.delivered_targets == 0
            )
        }
    )
    ready_line = (
        f"\nready_without_receipt={len(ready_without_receipt)}："
        + "、".join(ready_without_receipt[:3])
        if ready_without_receipt
        else ""
    )
    sinks = deliver_trade_push(
        settings,
        title=(
            "SPX READY 未送达告警"
            if ready_without_receipt
            else "SPX 投递死信告警"
        ),
        text=(
            f"{len(dead_letters)} 个投递目标进入死信；outbox 已完成重试与对账。"
            f"{ready_line}"
        ),
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


def consume_pending_notifications(
    settings: NotificationSettings,
    *,
    runner: CommandRunner = default_runner,
    now: datetime | None = None,
    notify_dead_letters: bool = True,
    worker_id: str | None = None,
    completion_clock: Callable[[], datetime] | None = None,
) -> dict[str, object]:
    """Claim and deliver one target, keeping work below the stale-lease TTL."""

    now = now or datetime.now(tz=timezone.utc)
    completion_clock = completion_clock or (lambda: datetime.now(tz=timezone.utc))
    outbox = _delivery_outbox(settings)
    imported = _migrate_legacy_queue(settings, outbox, now=now)
    worker_id = worker_id or f"notification-recovery:{os.getpid()}"
    terminal_receipts: list[TerminalDeliveryReceipt] = []
    jobs = outbox.claim_due(
        worker_id=worker_id,
        # Reserving a backlog lets later targets outlive the claim while the
        # sinks block. The next poll immediately claims the next due target.
        limit_targets=ASYNC_CLAIM_TARGET_LIMIT,
        now=now,
        terminal_receipts=terminal_receipts,
    )
    attempted_targets = 0
    delivered_targets = 0
    rust_ingress_attempts = 0
    forwarded_to_rust_targets = 0
    expired_targets = len(terminal_receipts)
    dead_lettered = expired_targets
    lost_claim_targets = 0
    for job in jobs:
        result = _deliver_claimed_job(
            settings,
            outbox,
            job,
            worker_id=worker_id,
            runner=runner,
            completion_clock=completion_clock,
            deliver=deliver_trade_push,
        )
        attempted_targets += result.attempted_targets
        delivered_targets += result.delivered_targets
        rust_ingress_attempts += result.rust_ingress_attempts
        forwarded_to_rust_targets += result.forwarded_to_rust_targets
        dead_lettered += result.dead_lettered_targets
        lost_claim_targets += result.lost_claim_targets
        expired_targets += result.expired_targets
    receipt_sync = _sync_terminal_receipts(
        settings,
        outbox,
        now=completion_clock(),
    )
    counts = outbox.count_targets()
    dead_letter_total = counts.get(DeliveryStatus.DEAD_LETTER.value, 0)
    dead_letter_notified = (
        _notify_dead_letters(settings, outbox, runner=runner, now=now) if notify_dead_letters else 0
    )
    # Health is judged only by dead letters nobody has reviewed yet; history
    # alone must not fail the task forever.
    dead_letter_unacknowledged = outbox.count_unacknowledged_dead_letters()
    pruned_shadow = _prune_terminal_shadow_entries(settings, outbox)
    return {
        "ok": (
            dead_letter_unacknowledged == 0
            and receipt_sync.pending == 0
            and receipt_sync.inspection.ok
        ),
        "imported_legacy": imported,
        "jobs": len(jobs),
        "attempted_targets": attempted_targets,
        "delivered_targets": delivered_targets,
        "rust_ingress_attempts": rust_ingress_attempts,
        "forwarded_to_rust_targets": forwarded_to_rust_targets,
        "pending_targets": counts.get(DeliveryStatus.PENDING.value, 0),
        "claimed_targets": counts.get(DeliveryStatus.CLAIMED.value, 0),
        "dead_lettered": dead_lettered,
        "expired_targets": expired_targets,
        "lost_claim_targets": lost_claim_targets,
        "dead_letter_total": dead_letter_total,
        "dead_letter_unacknowledged": dead_letter_unacknowledged,
        "dead_letter_notified": dead_letter_notified,
        "terminal_receipts_recorded": receipt_sync.recorded,
        "terminal_receipts_pending": receipt_sync.pending,
        "terminal_receipts_repaired": receipt_sync.repaired,
        "receipt_store_ok": receipt_sync.inspection.ok,
        "receipt_store_quick_check": receipt_sync.inspection.quick_check,
        "receipt_store_journal_mode": receipt_sync.inspection.journal_mode,
        "receipt_store_synchronous": receipt_sync.inspection.synchronous,
        "receipt_store_schema_present": receipt_sync.inspection.schema_present,
        "receipt_store_missing_mirror_ids": len(receipt_sync.inspection.missing_mirror_ids),
        "pruned_shadow": pruned_shadow,
    }


def recover_pending_notifications(
    settings: NotificationSettings,
    *,
    runner: CommandRunner = default_runner,
    now: datetime | None = None,
    completion_clock: Callable[[], datetime] | None = None,
) -> dict[str, object]:
    """Backward-compatible name for one asynchronous consumer cycle."""

    return consume_pending_notifications(
        settings,
        runner=runner,
        now=now,
        completion_clock=completion_clock,
    )


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
    envelope, targets = _delivery_route(settings, envelope, friend=friend)
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
    terminal_receipts: list[TerminalDeliveryReceipt] = []
    jobs = outbox.claim_due(
        worker_id=worker_id,
        limit_targets=len(targets),
        now=attempted_at,
        event_id=envelope.event_id,
        terminal_receipts=terminal_receipts,
    )
    sinks: tuple[SinkResult, ...] = ()
    job_expired_targets = 0
    if jobs:
        result = _deliver_claimed_job(
            settings,
            outbox,
            jobs[0],
            worker_id=worker_id,
            runner=runner,
            completion_clock=lambda: attempted_at,
            deliver=deliver_trade_push,
        )
        sinks = result.sinks
        job_expired_targets = result.expired_targets
    if terminal_receipts or jobs or job_expired_targets:
        _sync_terminal_receipts(settings, outbox, now=attempted_at)
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
        # Rust ACK only proves durable forwarding into the core. Human delivery
        # is measured from Rust target receipts, never this staging target.
        delivered=(
            summary.delivered_targets > 0 and "rust_ingress" not in targets
        ),
        queued_for_recovery=queued,
        forwarded_to_rust=(
            summary.delivered_targets > 0 and targets == ("rust_ingress",)
        ),
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
    if quiet_window_suppresses(envelope, now=attempted_at):
        return _quiet_dispatch_result(
            settings,
            envelope,
            attempted_at=attempted_at,
        )
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
