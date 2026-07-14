"""Outbox deliver fn: DomainEvent ALERT_CANDIDATE → notifier.notify_payload."""

from __future__ import annotations

from datetime import timezone
from dataclasses import dataclass
from typing import Mapping

from spx_spark.config import NotificationSettings
from spx_spark.domain.events import DomainEvent, EventKind
from spx_spark.notifier.pipeline import notify_payload
from spx_spark.notifier.model import NotificationResult
from spx_spark.position_alerts import reconcile_position_event_acknowledgements


def _payload_dict(event: DomainEvent) -> dict[str, object]:
    raw = event.payload
    if not isinstance(raw, Mapping):
        raise TypeError("ALERT_CANDIDATE payload must be a mapping")
    # Nested envelope support for future single-alert events.
    nested = raw.get("notify_payload")
    if isinstance(nested, Mapping):
        return dict(nested)
    return dict(raw)


def notification_settled(result: NotificationResult) -> bool:
    """True when the outbox may ack a terminal policy or delivery outcome."""

    if result.outcome in {"disabled", "filtered", "delivered", "consumed"}:
        return True
    if result.outcome in {"pending", "failed", "no_sink"}:
        return False
    # Compatibility for older/custom NotificationResult constructors.
    return not result.enabled or result.sent_count > 0 or result.selected_count == 0


@dataclass(frozen=True)
class DeliveryDisposition:
    settled: bool
    outcome: str
    delivered_count: int


def deliver_alert_candidate(
    event: DomainEvent,
    *,
    settings: NotificationSettings | None = None,
) -> bool:
    """Deliver one ALERT_CANDIDATE through the existing notifier pipeline.

    Returns True when the consumer should ack; False to release for retry.
    """

    if event.kind is not EventKind.ALERT_CANDIDATE:
        return True
    payload = _payload_dict(event)
    payload["_notification_event_id"] = event.event_id
    if not payload.get("alerts"):
        return True
    notification_settings = settings or NotificationSettings.from_env()
    now = event.source_at
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    result = notify_payload(payload, settings=notification_settings, now=now)
    if result.acknowledged_event_ids:
        reconcile_position_event_acknowledgements(result.acknowledged_event_ids)
    return notification_settled(result)


def deliver_alert_candidate_disposition(
    event: DomainEvent,
    *,
    settings: NotificationSettings | None = None,
) -> DeliveryDisposition:
    if event.kind is not EventKind.ALERT_CANDIDATE:
        return DeliveryDisposition(settled=True, outcome="ignored", delivered_count=0)
    payload = _payload_dict(event)
    if not payload.get("alerts"):
        return DeliveryDisposition(settled=True, outcome="empty", delivered_count=0)
    payload["_notification_event_id"] = event.event_id
    notification_settings = settings or NotificationSettings.from_env()
    now = event.source_at
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    result = notify_payload(payload, settings=notification_settings, now=now)
    if result.acknowledged_event_ids:
        reconcile_position_event_acknowledgements(result.acknowledged_event_ids)
    return DeliveryDisposition(
        settled=notification_settled(result),
        outcome=result.outcome,
        delivered_count=result.sent_count,
    )


def make_deliver_alert_candidate(
    settings: NotificationSettings | None = None,
):
    """Bind NotificationSettings into a DeliverFn-compatible callable."""

    def _deliver(event: DomainEvent) -> DeliveryDisposition:
        return deliver_alert_candidate_disposition(event, settings=settings)

    return _deliver
