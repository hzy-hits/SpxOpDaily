"""Durable, per-sink outbox for every human-facing notification."""

from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Iterable, Sequence

from spx_spark.notifier.receipts import NotificationEnvelope


class DeliveryStatus(str, Enum):
    PENDING = "pending"
    CLAIMED = "claimed"
    DELIVERED = "delivered"
    DEAD_LETTER = "dead_letter"


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


_SCHEMA = """
CREATE TABLE IF NOT EXISTS notification_delivery_events (
    event_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    kind TEXT NOT NULL,
    lane TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
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
    updated_at TEXT NOT NULL,
    PRIMARY KEY (event_id, sink),
    FOREIGN KEY (event_id) REFERENCES notification_delivery_events(event_id)
        ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_notification_targets_due
    ON notification_delivery_targets(status, next_attempt_at);
"""


def _utc(value: datetime | None = None) -> datetime:
    value = value or datetime.now(tz=timezone.utc)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return _utc(value).isoformat(timespec="microseconds")


def _parse(value: object) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    return _utc(parsed)


class NotificationDeliveryOutbox:
    """SQLite outbox with independent acknowledgement for every sink."""

    def __init__(
        self,
        path: str | Path,
        *,
        max_attempts: int,
        retry_schedule_seconds: Sequence[float],
        dead_letter_after_seconds: float,
        claim_stale_after_seconds: float,
        busy_timeout_ms: int = 1000,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        schedule = tuple(float(value) for value in retry_schedule_seconds)
        if not schedule or any(value < 0 for value in schedule):
            raise ValueError("retry_schedule_seconds must contain non-negative values")
        if dead_letter_after_seconds <= 0:
            raise ValueError("dead_letter_after_seconds must be > 0")
        if claim_stale_after_seconds <= 0:
            raise ValueError("claim_stale_after_seconds must be > 0")
        self.path = Path(path)
        self.max_attempts = max_attempts
        self.retry_schedule_seconds = schedule
        self.dead_letter_after_seconds = float(dead_letter_after_seconds)
        self.claim_stale_after_seconds = float(claim_stale_after_seconds)
        self.busy_timeout_ms = busy_timeout_ms
        self._prepare()
        self._initialize()

    def _prepare(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        os.close(descriptor)
        os.chmod(self.path, 0o600)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=self.busy_timeout_ms / 1000,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(_SCHEMA)

    def writable(self) -> bool:
        try:
            with self._connect() as connection:
                connection.execute("SELECT 1 FROM notification_delivery_events LIMIT 1")
            return True
        except sqlite3.Error:
            return False

    def contains(self, event_id: str) -> bool:
        with self._connect() as connection:
            return (
                connection.execute(
                    "SELECT 1 FROM notification_delivery_events WHERE event_id = ?",
                    (event_id,),
                ).fetchone()
                is not None
            )

    def enqueue(
        self,
        envelope: NotificationEnvelope,
        *,
        title: str,
        text: str,
        feishu_text: str | None,
        friend: bool,
        targets: Iterable[str],
        now: datetime | None = None,
    ) -> bool:
        envelope.validate()
        normalized_targets = tuple(dict.fromkeys(str(target) for target in targets))
        unknown = set(normalized_targets) - DELIVERY_SINKS
        if unknown:
            raise ValueError(f"unsupported notification sinks: {sorted(unknown)}")
        if not normalized_targets:
            return False
        now_text = _iso(_utc(now))
        accepted = False
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO notification_delivery_events (
                        event_id, source, kind, lane, occurred_at, title, text,
                        feishu_text, friend, status, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        envelope.event_id,
                        envelope.source,
                        envelope.kind,
                        envelope.lane,
                        _iso(envelope.occurred_at),
                        title,
                        text,
                        feishu_text,
                        int(friend),
                        DeliveryStatus.PENDING.value,
                        now_text,
                        now_text,
                    ),
                )
                accepted = cursor.rowcount == 1
                if not accepted:
                    existing = connection.execute(
                        """
                        SELECT source, kind, lane, occurred_at, title, text,
                               feishu_text, friend
                        FROM notification_delivery_events WHERE event_id = ?
                        """,
                        (envelope.event_id,),
                    ).fetchone()
                    expected = (
                        envelope.source,
                        envelope.kind,
                        envelope.lane,
                        _iso(envelope.occurred_at),
                        title,
                        text,
                        feishu_text,
                        int(friend),
                    )
                    if existing is None or tuple(existing) != expected:
                        raise ValueError(
                            f"notification event_id collision for {envelope.event_id}"
                        )
                for target in normalized_targets:
                    connection.execute(
                        """
                        INSERT OR IGNORE INTO notification_delivery_targets (
                            event_id, sink, status, attempts, max_attempts,
                            next_attempt_at, updated_at
                        ) VALUES (?, ?, ?, 0, ?, ?, ?)
                        """,
                        (
                            envelope.event_id,
                            target,
                            DeliveryStatus.PENDING.value,
                            self.max_attempts,
                            now_text,
                            now_text,
                        ),
                    )
                self._refresh_event_status(connection, envelope.event_id, now_text)
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise
        return accepted

    def _requeue_stale_claims(
        self,
        connection: sqlite3.Connection,
        *,
        now: datetime,
    ) -> int:
        cutoff = _iso(now - timedelta(seconds=self.claim_stale_after_seconds))
        now_text = _iso(now)
        rows = connection.execute(
            """
            SELECT DISTINCT event_id FROM notification_delivery_targets
            WHERE status = ? AND claimed_at < ?
            """,
            (DeliveryStatus.CLAIMED.value, cutoff),
        ).fetchall()
        cursor = connection.execute(
            """
            UPDATE notification_delivery_targets
            SET status = ?, claimed_by = NULL, claimed_at = NULL,
                next_attempt_at = ?, updated_at = ?,
                last_error = COALESCE(last_error, 'stale claim recovered')
            WHERE status = ? AND claimed_at < ?
            """,
            (
                DeliveryStatus.PENDING.value,
                now_text,
                now_text,
                DeliveryStatus.CLAIMED.value,
                cutoff,
            ),
        )
        for row in rows:
            self._refresh_event_status(connection, str(row["event_id"]), now_text)
        return cursor.rowcount

    def claim_due(
        self,
        *,
        worker_id: str,
        limit_targets: int,
        now: datetime | None = None,
        event_id: str | None = None,
    ) -> list[DeliveryJob]:
        if limit_targets < 1:
            return []
        now = _utc(now)
        now_text = _iso(now)
        claimed_rows: list[sqlite3.Row] = []
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._requeue_stale_claims(connection, now=now)
                params: list[object] = [DeliveryStatus.PENDING.value, now_text]
                event_clause = ""
                if event_id is not None:
                    event_clause = " AND t.event_id = ?"
                    params.append(event_id)
                params.append(limit_targets)
                rows = connection.execute(
                    f"""
                    SELECT t.event_id, t.sink, e.source, e.kind, e.lane,
                           e.occurred_at, e.title, e.text, e.feishu_text, e.friend
                    FROM notification_delivery_targets AS t
                    JOIN notification_delivery_events AS e USING (event_id)
                    WHERE t.status = ? AND t.next_attempt_at <= ?{event_clause}
                    ORDER BY t.next_attempt_at, e.created_at, t.sink
                    LIMIT ?
                    """,
                    tuple(params),
                ).fetchall()
                touched: set[str] = set()
                for row in rows:
                    cursor = connection.execute(
                        """
                        UPDATE notification_delivery_targets
                        SET status = ?, claimed_by = ?, claimed_at = ?,
                            attempts = attempts + 1, updated_at = ?
                        WHERE event_id = ? AND sink = ? AND status = ?
                        """,
                        (
                            DeliveryStatus.CLAIMED.value,
                            worker_id,
                            now_text,
                            now_text,
                            row["event_id"],
                            row["sink"],
                            DeliveryStatus.PENDING.value,
                        ),
                    )
                    if cursor.rowcount:
                        claimed_rows.append(row)
                        touched.add(str(row["event_id"]))
                for touched_event_id in touched:
                    self._refresh_event_status(connection, touched_event_id, now_text)
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise
        grouped: dict[str, list[sqlite3.Row]] = {}
        for row in claimed_rows:
            grouped.setdefault(str(row["event_id"]), []).append(row)
        jobs: list[DeliveryJob] = []
        for rows in grouped.values():
            first = rows[0]
            jobs.append(
                DeliveryJob(
                    envelope=NotificationEnvelope(
                        event_id=str(first["event_id"]),
                        source=str(first["source"]),
                        kind=str(first["kind"]),
                        lane=str(first["lane"]),
                        occurred_at=_parse(first["occurred_at"]),
                    ),
                    title=str(first["title"]),
                    text=str(first["text"]),
                    feishu_text=(
                        str(first["feishu_text"])
                        if first["feishu_text"] is not None
                        else None
                    ),
                    friend=bool(first["friend"]),
                    targets=tuple(str(row["sink"]) for row in rows),
                )
            )
        return jobs

    def settle_target(
        self,
        event_id: str,
        sink: str,
        *,
        worker_id: str,
        ok: bool,
        error: str | None,
        now: datetime | None = None,
    ) -> DeliveryStatus:
        now = _utc(now)
        now_text = _iso(now)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    """
                    SELECT t.attempts, t.max_attempts, e.created_at
                    FROM notification_delivery_targets AS t
                    JOIN notification_delivery_events AS e USING (event_id)
                    WHERE t.event_id = ? AND t.sink = ? AND t.status = ?
                      AND t.claimed_by = ?
                    """,
                    (event_id, sink, DeliveryStatus.CLAIMED.value, worker_id),
                ).fetchone()
                if row is None:
                    raise ValueError(f"target {event_id}/{sink} is not claimed by {worker_id}")
                if ok:
                    status = DeliveryStatus.DELIVERED
                    next_attempt_at = now_text
                    delivered_at = now_text
                else:
                    attempts = int(row["attempts"])
                    age_seconds = (now - _parse(row["created_at"])).total_seconds()
                    exhausted = attempts >= int(row["max_attempts"])
                    expired = age_seconds >= self.dead_letter_after_seconds
                    if exhausted or expired:
                        status = DeliveryStatus.DEAD_LETTER
                        next_attempt_at = now_text
                    else:
                        status = DeliveryStatus.PENDING
                        delay = self.retry_schedule_seconds[
                            min(max(attempts - 1, 0), len(self.retry_schedule_seconds) - 1)
                        ]
                        next_attempt_at = _iso(now + timedelta(seconds=delay))
                    delivered_at = None
                connection.execute(
                    """
                    UPDATE notification_delivery_targets
                    SET status = ?, next_attempt_at = ?, claimed_by = NULL,
                        claimed_at = NULL, delivered_at = ?, last_error = ?,
                        updated_at = ?
                    WHERE event_id = ? AND sink = ? AND status = ?
                      AND claimed_by = ?
                    """,
                    (
                        status.value,
                        next_attempt_at,
                        delivered_at,
                        None if ok else (error or "delivery failed")[:1000],
                        now_text,
                        event_id,
                        sink,
                        DeliveryStatus.CLAIMED.value,
                        worker_id,
                    ),
                )
                self._refresh_event_status(connection, event_id, now_text)
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise
        return status

    def _refresh_event_status(
        self,
        connection: sqlite3.Connection,
        event_id: str,
        now_text: str,
    ) -> DeliveryStatus:
        counts = {
            str(row["status"]): int(row["count"])
            for row in connection.execute(
                """
                SELECT status, COUNT(*) AS count
                FROM notification_delivery_targets
                WHERE event_id = ? GROUP BY status
                """,
                (event_id,),
            )
        }
        if counts.get(DeliveryStatus.CLAIMED.value, 0):
            status = DeliveryStatus.CLAIMED
        elif counts.get(DeliveryStatus.PENDING.value, 0):
            status = DeliveryStatus.PENDING
        elif counts.get(DeliveryStatus.DEAD_LETTER.value, 0):
            status = DeliveryStatus.DEAD_LETTER
        else:
            status = DeliveryStatus.DELIVERED
        connection.execute(
            """
            UPDATE notification_delivery_events
            SET status = ?, updated_at = ? WHERE event_id = ?
            """,
            (status.value, now_text, event_id),
        )
        return status

    def summary(self, event_id: str) -> DeliverySummary | None:
        with self._connect() as connection:
            event = connection.execute(
                "SELECT status FROM notification_delivery_events WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            if event is None:
                return None
            counts = {
                str(row["status"]): int(row["count"])
                for row in connection.execute(
                    """
                    SELECT status, COUNT(*) AS count
                    FROM notification_delivery_targets
                    WHERE event_id = ? GROUP BY status
                    """,
                    (event_id,),
                )
            }
        return DeliverySummary(
            status=DeliveryStatus(str(event["status"])),
            delivered_targets=counts.get(DeliveryStatus.DELIVERED.value, 0),
            pending_targets=counts.get(DeliveryStatus.PENDING.value, 0),
            claimed_targets=counts.get(DeliveryStatus.CLAIMED.value, 0),
            dead_letter_targets=counts.get(DeliveryStatus.DEAD_LETTER.value, 0),
        )

    def count_targets(self) -> dict[str, int]:
        with self._connect() as connection:
            return {
                str(row["status"]): int(row["count"])
                for row in connection.execute(
                    """
                    SELECT status, COUNT(*) AS count
                    FROM notification_delivery_targets GROUP BY status
                    """
                )
            }
