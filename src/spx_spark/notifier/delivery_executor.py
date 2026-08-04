"""Network-delivery execution for jobs already claimed from the outbox."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

from spx_spark.config import NotificationSettings
from spx_spark.notifier.delivery_outbox import (
    DeliveryClaimLost,
    DeliveryJob,
    DeliveryStatus,
    NotificationDeliveryOutbox,
)
from spx_spark.notifier.human_policy import quiet_window_suppresses
from spx_spark.notifier.missed_queue import ack_missed_event_ids
from spx_spark.notifier.model import CommandRunner, SinkResult
from spx_spark.notifier.rust_ingress import deliver_operator_notification


@dataclass(frozen=True)
class DeliveryJobResult:
    sinks: tuple[SinkResult, ...]
    status: DeliveryStatus
    attempted_targets: int
    delivered_targets: int
    rust_ingress_attempts: int
    forwarded_to_rust_targets: int
    pending_targets: int
    dead_lettered_targets: int
    lost_claim_targets: int
    expired_targets: int


def transport_lane(job: DeliveryJob) -> str:
    return "ops" if job.envelope.lane == "ops_transition" else "trade"


def deliver_claimed_job(
    settings: NotificationSettings,
    outbox: NotificationDeliveryOutbox,
    job: DeliveryJob,
    *,
    worker_id: str,
    runner: CommandRunner,
    completion_clock: Callable[[], datetime],
    deliver: Callable[..., tuple[SinkResult, ...] | list[SinkResult]],
) -> DeliveryJobResult:
    """Authorize a claim immediately before invoking any transport."""

    policy_at = completion_clock()
    preflight = outbox.preflight_claimed_targets(
        job.envelope.event_id,
        job.targets,
        worker_id=worker_id,
        now=policy_at,
    )
    rejected_sinks = [
        SinkResult(
            sink=rejection.sink,
            attempted=False,
            ok=False,
            error=rejection.reason,
            verdict=rejection.outcome,
        )
        for rejection in preflight.rejections
    ]
    expired_targets = sum(
        rejection.outcome == "expired_before_delivery" for rejection in preflight.rejections
    )
    preflight_lost_targets = len(preflight.rejections) - expired_targets
    preflight_dead_letters = sum(
        rejection.status == DeliveryStatus.DEAD_LETTER.value for rejection in preflight.rejections
    )
    authorized_targets = preflight.authorized_targets
    if not authorized_targets:
        summary = _summary(outbox, job)
        return DeliveryJobResult(
            sinks=tuple(rejected_sinks),
            status=summary.status,
            attempted_targets=0,
            delivered_targets=0,
            rust_ingress_attempts=0,
            forwarded_to_rust_targets=0,
            pending_targets=summary.pending_targets + summary.claimed_targets,
            dead_lettered_targets=preflight_dead_letters,
            lost_claim_targets=preflight_lost_targets,
            expired_targets=expired_targets,
        )
    if quiet_window_suppresses(job.envelope, now=policy_at):
        rust_owned = authorized_targets == ("rust_ingress",)
        delivered_targets = 0
        dead_lettered_targets = preflight_dead_letters
        lost_claim_targets = preflight_lost_targets
        for target in authorized_targets:
            sink = SinkResult(
                sink=target,
                attempted=False,
                ok=True,
                error="suppressed from RTH close until the next SPX GTH open",
                verdict="suppressed",
            )
            rejected_sinks.append(sink)
            try:
                status = outbox.settle_target(
                    job.envelope.event_id,
                    target,
                    worker_id=worker_id,
                    ok=True,
                    error=sink.error,
                    permanent=False,
                    attempted=False,
                    receipt_outcome="quiet_window_suppressed",
                    now=policy_at,
                )
            except DeliveryClaimLost:
                lost_claim_targets += 1
                continue
            delivered_targets += int(not rust_owned)
            dead_lettered_targets += int(status is DeliveryStatus.DEAD_LETTER)
        summary = _summary(outbox, job)
        return DeliveryJobResult(
            sinks=tuple(rejected_sinks),
            status=summary.status,
            attempted_targets=0 if rust_owned else len(authorized_targets),
            delivered_targets=delivered_targets,
            rust_ingress_attempts=0,
            forwarded_to_rust_targets=0,
            pending_targets=summary.pending_targets + summary.claimed_targets,
            dead_lettered_targets=dead_lettered_targets,
            lost_claim_targets=lost_claim_targets,
            expired_targets=expired_targets,
        )

    # This call is the transport-start linearization point. Cancellation or
    # expiry committed before the atomic preflight cannot reach this boundary.
    rust_owned = authorized_targets == ("rust_ingress",)
    if rust_owned:
        sinks = (deliver_operator_notification(settings, job),)
    else:
        sinks = deliver(
            settings,
            title=job.title,
            text=job.text,
            kind=job.envelope.kind,
            lane=transport_lane(job),
            friend=job.friend,
            feishu_text=job.feishu_text,
            runner=runner,
            targets=frozenset(authorized_targets),
        )
    sinks_by_name = {sink.sink: sink for sink in sinks}
    normalized_sinks = [*rejected_sinks, *sinks]
    delivered_targets = 0
    forwarded_to_rust_targets = 0
    dead_lettered_targets = preflight_dead_letters
    lost_claim_targets = preflight_lost_targets
    for target in authorized_targets:
        sink = sinks_by_name.get(target)
        if sink is None:
            sink = SinkResult(
                sink=target,
                attempted=False,
                ok=False,
                error="configured delivery target is currently unavailable",
            )
            normalized_sinks.append(sink)
        receipt_at = completion_clock()
        try:
            status = outbox.settle_target(
                job.envelope.event_id,
                target,
                worker_id=worker_id,
                ok=sink.ok,
                error=sink.error,
                permanent=sink.permanent,
                attempted=sink.attempted,
                receipt_outcome=(
                    "forwarded_to_rust" if rust_owned and sink.ok else None
                ),
                now=receipt_at,
            )
        except DeliveryClaimLost:
            # Authorization won the preflight race and transport may already
            # have completed. Audit without overwriting the newer owner.
            outbox.record_unsettled_attempt(
                job.envelope.event_id,
                target,
                attempted=sink.attempted,
                ok=sink.ok,
                error=sink.error or "delivery claim lost before settlement",
                now=receipt_at,
            )
            lost_claim_targets += 1
            continue
        delivered_targets += int(sink.ok and not rust_owned)
        forwarded_to_rust_targets += int(sink.ok and rust_owned)
        dead_lettered_targets += int(status is DeliveryStatus.DEAD_LETTER)

    summary = _summary(outbox, job)
    if summary.status is DeliveryStatus.DELIVERED:
        ack_missed_event_ids(
            settings.missed_queue_path,
            frozenset({job.envelope.event_id}),
        )
    return DeliveryJobResult(
        sinks=tuple(normalized_sinks),
        status=summary.status,
        attempted_targets=0 if rust_owned else len(authorized_targets),
        delivered_targets=delivered_targets,
        rust_ingress_attempts=len(authorized_targets) if rust_owned else 0,
        forwarded_to_rust_targets=forwarded_to_rust_targets,
        pending_targets=summary.pending_targets + summary.claimed_targets,
        dead_lettered_targets=dead_lettered_targets,
        lost_claim_targets=lost_claim_targets,
        expired_targets=expired_targets,
    )


def _summary(outbox: NotificationDeliveryOutbox, job: DeliveryJob):
    summary = outbox.summary(job.envelope.event_id)
    if summary is None:
        raise RuntimeError(f"delivery event disappeared: {job.envelope.event_id}")
    return summary
