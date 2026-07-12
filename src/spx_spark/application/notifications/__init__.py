"""Notification application services."""

from __future__ import annotations

from spx_spark.application.notifications.deliver import (
    deliver_alert_candidate,
    make_deliver_alert_candidate,
)
from spx_spark.application.notifications.outbox_consumer import (
    ConsumeResult,
    IdempotentOutboxConsumer,
)

__all__ = [
    "ConsumeResult",
    "IdempotentOutboxConsumer",
    "deliver_alert_candidate",
    "make_deliver_alert_candidate",
]
