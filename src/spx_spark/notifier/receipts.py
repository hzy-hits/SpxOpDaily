"""Durable, content-free receipts for all human notification attempts."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from spx_spark.notifier.model import SinkResult


_SCHEMA = """
CREATE TABLE IF NOT EXISTS notification_delivery_receipts (
    attempt_id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    source TEXT NOT NULL,
    kind TEXT NOT NULL,
    lane TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    attempted_at TEXT NOT NULL,
    outcome TEXT NOT NULL,
    queued_for_recovery INTEGER NOT NULL,
    sinks_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_notification_receipts_event
    ON notification_delivery_receipts(event_id, attempted_at);
CREATE INDEX IF NOT EXISTS idx_notification_receipts_outcome
    ON notification_delivery_receipts(outcome, attempted_at);
"""


@dataclass(frozen=True)
class NotificationEnvelope:
    event_id: str
    source: str
    kind: str
    lane: str
    occurred_at: datetime

    def validate(self) -> None:
        for label, value in (
            ("event_id", self.event_id),
            ("source", self.source),
            ("kind", self.kind),
            ("lane", self.lane),
        ):
            if not value.strip():
                raise ValueError(f"{label} is required")
        if self.occurred_at.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")


def notification_event_id(
    kind: str,
    *,
    source: str,
    occurred_at: datetime,
    identity: str,
) -> str:
    """Stable semantic delivery id; message text is deliberately excluded."""

    occurred = occurred_at.astimezone(timezone.utc).isoformat(timespec="seconds")
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:20]
    return f"notify:{source}:{kind}:{occurred}:{digest}"


def record_delivery_receipt(
    path: str,
    envelope: NotificationEnvelope,
    *,
    sinks: Sequence[SinkResult],
    outcome: str,
    queued_for_recovery: bool,
    attempted_at: datetime | None = None,
) -> bool:
    """Persist one delivery outcome without storing message bodies or secrets."""

    if not path:
        return False
    envelope.validate()
    attempted_at = attempted_at or datetime.now(tz=timezone.utc)
    if attempted_at.tzinfo is None:
        attempted_at = attempted_at.replace(tzinfo=timezone.utc)
    attempted = attempted_at.astimezone(timezone.utc).isoformat(timespec="microseconds")
    attempt_id = hashlib.sha256(
        f"{envelope.event_id}|{attempted}".encode("utf-8")
    ).hexdigest()
    sink_rows = [
        {
            "sink": sink.sink,
            "attempted": sink.attempted,
            "ok": sink.ok,
            "error": (sink.error or "")[:500] or None,
            "verdict": sink.verdict,
        }
        for sink in sinks
    ]
    database = Path(path)
    try:
        database.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(database, os.O_RDWR | os.O_CREAT, 0o600)
        os.close(descriptor)
        os.chmod(database, 0o600)
        with sqlite3.connect(database, timeout=1.0) as connection:
            connection.execute("PRAGMA busy_timeout=1000")
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(_SCHEMA)
            connection.execute(
                """
                INSERT OR IGNORE INTO notification_delivery_receipts (
                    attempt_id, event_id, source, kind, lane, occurred_at,
                    attempted_at, outcome, queued_for_recovery, sinks_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    attempt_id,
                    envelope.event_id,
                    envelope.source,
                    envelope.kind,
                    envelope.lane,
                    envelope.occurred_at.astimezone(timezone.utc).isoformat(),
                    attempted,
                    outcome,
                    int(queued_for_recovery),
                    json.dumps(sink_rows, ensure_ascii=False, separators=(",", ":")),
                ),
            )
        return True
    except (OSError, sqlite3.Error, ValueError):
        # Receipt telemetry must never change the authoritative delivery result.
        return False
