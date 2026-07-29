"""Shared contracts and serialization helpers for the notification outbox."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

from spx_spark.notifier.receipts import NotificationEnvelope


class DeliveryStatus(str, Enum):
    PENDING = "pending"
    CLAIMED = "claimed"
    DELIVERED = "delivered"
    DEAD_LETTER = "dead_letter"


class DeliveryClaimLost(ValueError):
    """The target lease moved to another consumer before settlement."""


class DeliveryCancelled(ValueError):
    """A durable cancellation fence rejected a later enqueue."""


DELIVERY_SINKS = frozenset({"bark", "feishu", "bark_friend"})


@dataclass(frozen=True)
class DeliveryJob:
    envelope: NotificationEnvelope
    title: str
    text: str
    feishu_text: str | None
    friend: bool
    targets: tuple[str, ...]


@dataclass(frozen=True)
class DeliverySummary:
    status: DeliveryStatus
    delivered_targets: int
    pending_targets: int
    claimed_targets: int
    dead_letter_targets: int


@dataclass(frozen=True)
class TerminalDeliveryReceipt:
    """Content-free outbox receipt intent awaiting receipt-store mirroring."""

    receipt_id: str
    envelope: NotificationEnvelope
    sink: str
    outcome: str
    reason: str
    terminal_at: datetime
    attempted: bool = False
    ok: bool = False
    queued_for_recovery: bool = False


@dataclass(frozen=True)
class DeliveryEventInspection:
    """Exact producer/outbox reconciliation result for one immutable event."""

    event_id: str
    exists: bool
    cancelled: bool
    payload_matches: bool
    targets_match: bool
    event_status: str | None
    target_statuses: tuple[tuple[str, str], ...]
    reason: str

    @property
    def acceptable(self) -> bool:
        return self.reason == "accepted"


@dataclass(frozen=True)
class DeliveryClaimRejection:
    """One claimed target denied immediately before transport."""

    sink: str
    outcome: str
    reason: str
    status: str | None


@dataclass(frozen=True)
class DeliveryClaimPreflight:
    """Atomic authorization result for the targets in one claimed job."""

    authorized_targets: tuple[str, ...]
    rejections: tuple[DeliveryClaimRejection, ...]


CLAIM_PRIORITY_SQL = """
CASE
    WHEN e.lane = 'position_safety' THEN 0
    WHEN e.lane = 'execution_safety' THEN 1
    WHEN e.lane IN ('trade_ready', 'gth_manual_candidate') THEN 2
    WHEN e.lane = 'market_warning' THEN 3
    WHEN e.lane IN ('ops', 'ops_transition') THEN 4
    WHEN e.lane = 'scheduled_report' THEN 5
    ELSE 4
END
"""


SCHEMA = """
CREATE TABLE IF NOT EXISTS notification_delivery_events (
    event_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    kind TEXT NOT NULL,
    lane TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    expires_at TEXT,
    title TEXT NOT NULL,
    text TEXT NOT NULL,
    feishu_text TEXT,
    friend INTEGER NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_notification_events_status
    ON notification_delivery_events(status, updated_at);

CREATE TABLE IF NOT EXISTS notification_delivery_targets (
    event_id TEXT NOT NULL,
    sink TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL,
    next_attempt_at TEXT NOT NULL,
    claimed_by TEXT,
    claimed_at TEXT,
    delivered_at TEXT,
    last_error TEXT,
    acknowledged_at TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (event_id, sink),
    FOREIGN KEY (event_id) REFERENCES notification_delivery_events(event_id)
        ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_notification_targets_due
    ON notification_delivery_targets(status, next_attempt_at);

CREATE TABLE IF NOT EXISTS notification_delivery_terminal_receipts (
    receipt_id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    sink TEXT NOT NULL,
    outcome TEXT NOT NULL,
    reason TEXT NOT NULL,
    terminal_at TEXT NOT NULL,
    attempted INTEGER NOT NULL DEFAULT 0,
    ok INTEGER NOT NULL DEFAULT 0,
    queued_for_recovery INTEGER NOT NULL DEFAULT 0,
    recorded_at TEXT,
    FOREIGN KEY (event_id) REFERENCES notification_delivery_events(event_id)
        ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_notification_terminal_receipts_pending
    ON notification_delivery_terminal_receipts(recorded_at, terminal_at);
CREATE INDEX IF NOT EXISTS idx_notification_terminal_receipts_event
    ON notification_delivery_terminal_receipts(event_id, terminal_at);

CREATE TABLE IF NOT EXISTS notification_delivery_cancellations (
    event_id TEXT PRIMARY KEY,
    reason TEXT NOT NULL,
    cancelled_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_notification_cancellations_at
    ON notification_delivery_cancellations(cancelled_at);
"""


def utc(value: datetime | None = None) -> datetime:
    value = value or datetime.now(tz=timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def iso(value: datetime) -> str:
    return utc(value).isoformat(timespec="microseconds")


def parse(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    return utc(parsed)


def delivery_payload_fingerprint(
    envelope: NotificationEnvelope,
    *,
    title: str,
    text: str,
    feishu_text: str | None,
    friend: bool,
) -> str:
    payload = (
        envelope.source,
        envelope.kind,
        envelope.lane,
        iso(envelope.occurred_at),
        iso(envelope.expires_at) if envelope.expires_at else None,
        title,
        text,
        feishu_text,
        int(friend),
    )
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
