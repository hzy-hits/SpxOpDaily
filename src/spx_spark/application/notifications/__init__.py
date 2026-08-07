"""Notification application services."""

from __future__ import annotations

from spx_spark.notifier.alert_candidate_delivery import (
    deliver_alert_candidate,
    make_deliver_alert_candidate,
)
__all__ = [
    "deliver_alert_candidate",
    "make_deliver_alert_candidate",
]
