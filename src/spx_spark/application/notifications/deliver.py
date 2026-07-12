"""Outbox deliver fn: DomainEvent ALERT_CANDIDATE → notifier.notify_payload."""

from __future__ import annotations

from datetime import timezone
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
    """True when the outbox may ack (terminal success, skip, or disabled).

    Matches alert_engine.run() settlement: a cooldown/policy skip is a final
    outcome, not a retryable failure. Only hard delivery attempts that send
    nothing while selecting alerts should retry — but notify_payload already
    marks skipped_reason in those cases; treat skipped_reason as settled.
    """

    if not result.enabled:
        return True
    if result.sent_count > 0:
        return True
    if result.selected_count == 0:
        return True
    if result.skipped_reason:
        return True
    # Selected alerts but zero successful sinks and no skip reason → retry.
    return False


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


def make_deliver_alert_candidate(
    settings: NotificationSettings | None = None,
):
    """Bind NotificationSettings into a DeliverFn-compatible callable."""

    def _deliver(event: DomainEvent) -> bool:
        return deliver_alert_candidate(event, settings=settings)

    return _deliver
