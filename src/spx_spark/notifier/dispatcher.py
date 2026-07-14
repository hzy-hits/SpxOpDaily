"""One delivery boundary for live alerts, trade intents, and scheduled reports."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from spx_spark.config import NotificationSettings
from spx_spark.notifier.missed_queue import append_missed, flush_missed
from spx_spark.notifier.model import CommandRunner, SinkResult, default_runner
from spx_spark.notifier.receipts import NotificationEnvelope, record_delivery_receipt
from spx_spark.notifier.sinks import (
    any_delivery_ok,
    deliver_trade_push,
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
    """Deliver, queue failed IM, and write a common durable receipt.

    Recovery happens before the current message so a healthy Feishu channel
    drains its old timeline even when the caller is a scheduled report rather
    than the generic alert pipeline.
    """

    envelope.validate()
    attempted_at = attempted_at or datetime.now(tz=timezone.utc)
    recovery_sink = flush_missed(settings, runner=runner) if recover_missed else None
    transport_lane = "ops" if envelope.lane == "ops_transition" else "trade"
    sinks = deliver_trade_push(
        settings,
        title=title,
        text=text,
        kind=envelope.kind,
        lane=transport_lane,
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
