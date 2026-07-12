"""SQLite domain-event outbox: append / claim / ack / dead-letter / replay."""

from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Sequence

from spx_spark.domain.events import AppendResult, DomainEvent, EventKind


class OutboxStatus(str, Enum):
    PENDING = "pending"
    CLAIMED = "claimed"
    ACKED = "acked"
    DEAD_LETTER = "dead_letter"


@dataclass(frozen=True)
class OutboxRecord:
    event_id: str
    kind: str
    status: OutboxStatus
    attempts: int
    available_at: datetime
    payload: dict[str, object]
    last_error: str | None = None
    claimed_by: str | None = None
    claimed_at: datetime | None = None


_SCHEMA = """
CREATE TABLE IF NOT EXISTS domain_event_outbox (
    event_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    source_at TEXT NOT NULL,
    available_at TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    claimed_by TEXT,
    claimed_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_outbox_status_available
    ON domain_event_outbox(status, available_at);
"""


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _parse_dt(value: object) -> datetime:
    return datetime.fromisoformat(str(value)).astimezone(timezone.utc)


def domain_event_to_row(event: DomainEvent) -> dict[str, object]:
    event.validate()
    return {
        "event_id": event.event_id,
        "kind": event.kind.value,
        "schema_version": event.schema_version,
        "source_at": _iso(event.source_at),
        "available_at": _iso(event.available_at),
        "aggregate_id": event.aggregate_id,
        "sequence": event.sequence,
        "payload_json": json.dumps(
            dict(event.payload),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ),
    }


def row_to_domain_event(row: sqlite3.Row | dict[str, object]) -> DomainEvent:
    mapping = dict(row)
    return DomainEvent(
        schema_version=int(mapping["schema_version"]),
        event_id=str(mapping["event_id"]),
        kind=EventKind(str(mapping["kind"])),
        source_at=_parse_dt(mapping["source_at"]),
        available_at=_parse_dt(mapping["available_at"]),
        aggregate_id=str(mapping["aggregate_id"]),
        sequence=int(mapping["sequence"]),
        payload=json.loads(str(mapping["payload_json"])),
    )


class SqliteEventOutbox:
    """At-least-once outbox. Consumers must ack by event_id for idempotency."""

    def __init__(
        self,
        path: str | Path,
        *,
        max_attempts: int = 5,
        busy_timeout_ms: int = 250,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        self.path = Path(path)
        self.max_attempts = max_attempts
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
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            connection.executescript(_SCHEMA)

    def writable(self) -> bool:
        try:
            with self._connect() as connection:
                connection.execute("SELECT 1 FROM domain_event_outbox LIMIT 1")
            return True
        except sqlite3.Error:
            return False

    def append(self, events: Sequence[DomainEvent]) -> AppendResult:
        if not events:
            return AppendResult(accepted=0, duplicate=0, writable=True)
        accepted = 0
        duplicate = 0
        now = _iso(_utc_now())
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                try:
                    for event in events:
                        row = domain_event_to_row(event)
                        cursor = connection.execute(
                            """
                            INSERT OR IGNORE INTO domain_event_outbox (
                                event_id, kind, schema_version, source_at, available_at,
                                aggregate_id, sequence, payload_json, status, attempts,
                                max_attempts, created_at, updated_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
                            """,
                            (
                                row["event_id"],
                                row["kind"],
                                row["schema_version"],
                                row["source_at"],
                                row["available_at"],
                                row["aggregate_id"],
                                row["sequence"],
                                row["payload_json"],
                                OutboxStatus.PENDING.value,
                                self.max_attempts,
                                now,
                                now,
                            ),
                        )
                        if cursor.rowcount == 1:
                            accepted += 1
                        else:
                            duplicate += 1
                    connection.execute("COMMIT")
                except Exception:
                    connection.execute("ROLLBACK")
                    raise
        except sqlite3.Error:
            return AppendResult(accepted=0, duplicate=0, writable=False)
        return AppendResult(accepted=accepted, duplicate=duplicate, writable=True)

    def claim(
        self,
        *,
        consumer_id: str,
        limit: int = 10,
        now: datetime | None = None,
        kinds: Sequence[str] | None = None,
    ) -> list[DomainEvent]:
        if limit < 1:
            return []
        now = now or _utc_now()
        now_text = _iso(now)
        claimed: list[DomainEvent] = []
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                if kinds:
                    placeholders = ",".join("?" for _ in kinds)
                    rows = connection.execute(
                        f"""
                        SELECT * FROM domain_event_outbox
                        WHERE status = ?
                          AND available_at <= ?
                          AND attempts < max_attempts
                          AND kind IN ({placeholders})
                        ORDER BY available_at ASC, sequence ASC
                        LIMIT ?
                        """,
                        (OutboxStatus.PENDING.value, now_text, *kinds, limit),
                    ).fetchall()
                else:
                    rows = connection.execute(
                        """
                        SELECT * FROM domain_event_outbox
                        WHERE status = ?
                          AND available_at <= ?
                          AND attempts < max_attempts
                        ORDER BY available_at ASC, sequence ASC
                        LIMIT ?
                        """,
                        (OutboxStatus.PENDING.value, now_text, limit),
                    ).fetchall()
                for row in rows:
                    connection.execute(
                        """
                        UPDATE domain_event_outbox
                        SET status = ?, claimed_by = ?, claimed_at = ?,
                            attempts = attempts + 1, updated_at = ?
                        WHERE event_id = ? AND status = ?
                        """,
                        (
                            OutboxStatus.CLAIMED.value,
                            consumer_id,
                            now_text,
                            now_text,
                            row["event_id"],
                            OutboxStatus.PENDING.value,
                        ),
                    )
                    claimed.append(row_to_domain_event(row))
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise
        return claimed

    def ack(self, event_ids: Sequence[str], *, consumer_id: str | None = None) -> int:
        if not event_ids:
            return 0
        now_text = _iso(_utc_now())
        acked = 0
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                for event_id in event_ids:
                    if consumer_id is None:
                        cursor = connection.execute(
                            """
                            UPDATE domain_event_outbox
                            SET status = ?, updated_at = ?, claimed_by = NULL, claimed_at = NULL
                            WHERE event_id = ? AND status = ?
                            """,
                            (
                                OutboxStatus.ACKED.value,
                                now_text,
                                event_id,
                                OutboxStatus.CLAIMED.value,
                            ),
                        )
                    else:
                        cursor = connection.execute(
                            """
                            UPDATE domain_event_outbox
                            SET status = ?, updated_at = ?, claimed_by = NULL, claimed_at = NULL
                            WHERE event_id = ? AND status = ? AND claimed_by = ?
                            """,
                            (
                                OutboxStatus.ACKED.value,
                                now_text,
                                event_id,
                                OutboxStatus.CLAIMED.value,
                                consumer_id,
                            ),
                        )
                    acked += cursor.rowcount
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise
        return acked

    def fail(
        self,
        event_id: str,
        *,
        error: str,
        consumer_id: str | None = None,
        now: datetime | None = None,
    ) -> OutboxStatus:
        """Release a claimed event back to pending, or dead-letter on exhaustion."""

        now = now or _utc_now()
        now_text = _iso(now)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    "SELECT attempts, max_attempts, status, claimed_by FROM domain_event_outbox WHERE event_id = ?",
                    (event_id,),
                ).fetchone()
                if row is None:
                    connection.execute("COMMIT")
                    raise KeyError(event_id)
                if row["status"] != OutboxStatus.CLAIMED.value:
                    connection.execute("COMMIT")
                    return OutboxStatus(row["status"])
                if consumer_id is not None and row["claimed_by"] != consumer_id:
                    connection.execute("COMMIT")
                    return OutboxStatus.CLAIMED
                if int(row["attempts"]) >= int(row["max_attempts"]):
                    status = OutboxStatus.DEAD_LETTER
                else:
                    status = OutboxStatus.PENDING
                connection.execute(
                    """
                    UPDATE domain_event_outbox
                    SET status = ?, last_error = ?, claimed_by = NULL, claimed_at = NULL,
                        updated_at = ?
                    WHERE event_id = ?
                    """,
                    (status.value, error[:500], now_text, event_id),
                )
                connection.execute("COMMIT")
                return status
            except Exception:
                connection.execute("ROLLBACK")
                raise

    def requeue_stale_claims(
        self,
        *,
        older_than_seconds: float,
        now: datetime | None = None,
    ) -> int:
        """Recover claims abandoned by a crashed consumer (kill before ack)."""

        now = now or _utc_now()
        cutoff = _iso(
            datetime.fromtimestamp(now.timestamp() - older_than_seconds, tz=timezone.utc)
        )
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE domain_event_outbox
                SET status = ?, claimed_by = NULL, claimed_at = NULL, updated_at = ?
                WHERE status = ? AND claimed_at IS NOT NULL AND claimed_at <= ?
                  AND attempts < max_attempts
                """,
                (
                    OutboxStatus.PENDING.value,
                    _iso(now),
                    OutboxStatus.CLAIMED.value,
                    cutoff,
                ),
            )
            return cursor.rowcount

    def count_by_status(self) -> dict[str, int]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT status, COUNT(*) AS n FROM domain_event_outbox GROUP BY status"
            ).fetchall()
        return {str(row["status"]): int(row["n"]) for row in rows}

    def dead_letters(self, *, limit: int = 100) -> list[OutboxRecord]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM domain_event_outbox
                WHERE status = ?
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (OutboxStatus.DEAD_LETTER.value, limit),
            ).fetchall()
        records: list[OutboxRecord] = []
        for row in rows:
            records.append(
                OutboxRecord(
                    event_id=str(row["event_id"]),
                    kind=str(row["kind"]),
                    status=OutboxStatus.DEAD_LETTER,
                    attempts=int(row["attempts"]),
                    available_at=_parse_dt(row["available_at"]),
                    payload=json.loads(str(row["payload_json"])),
                    last_error=str(row["last_error"]) if row["last_error"] else None,
                )
            )
        return records

    def replay_dead_letter(self, event_id: str) -> bool:
        """Move a dead-lettered event back to pending for operator replay."""

        now_text = _iso(_utc_now())
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE domain_event_outbox
                SET status = ?, attempts = 0, last_error = NULL,
                    claimed_by = NULL, claimed_at = NULL, updated_at = ?
                WHERE event_id = ? AND status = ?
                """,
                (
                    OutboxStatus.PENDING.value,
                    now_text,
                    event_id,
                    OutboxStatus.DEAD_LETTER.value,
                ),
            )
            return cursor.rowcount == 1
