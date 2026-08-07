"""FROZEN (2026-08): production-fault fixes only; see docs/architecture-simplification-execution-plan-v1.md.
Durable, per-sink outbox for every human-facing notification."""

from __future__ import annotations

import hashlib
import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable, Sequence

from spx_spark.notifier.delivery_outbox_claims import (
    DeliveryOutboxClaimMixin,
)
from spx_spark.notifier.delivery_outbox_contract import (
    CLAIM_PRIORITY_SQL as _CLAIM_PRIORITY_SQL,
    DELIVERY_SINKS,
    SCHEMA as _SCHEMA,
    DeliveryCancelled,
    DeliveryClaimLost,
    DeliveryEventInspection,
    DeliveryJob,
    DeliveryStatus,
    DeliverySummary,
    TerminalDeliveryReceipt,
    delivery_payload_fingerprint,
    iso as _iso,
    operator_targets_json as _operator_targets_json,
    parse as _parse,
    parse_operator_targets as _parse_operator_targets,
    utc as _utc,
)
from spx_spark.notifier.delivery_outbox_read_model import (
    DeliveryOutboxReadModelMixin,
)
from spx_spark.notifier.receipts import NotificationEnvelope


__all__ = [
    "DeliveryCancelled",
    "DeliveryClaimLost",
    "DeliveryEventInspection",
    "DeliveryJob",
    "DeliveryStatus",
    "DeliverySummary",
    "NotificationDeliveryOutbox",
    "TerminalDeliveryReceipt",
    "delivery_payload_fingerprint",
]


class NotificationDeliveryOutbox(DeliveryOutboxClaimMixin, DeliveryOutboxReadModelMixin):
    """SQLite outbox with independent acknowledgement for every sink."""

    def __init__(
        self,
        path: str | Path,
        *,
        max_attempts: int,
        retry_schedule_seconds: Sequence[float],
        dead_letter_after_seconds: float,
        claim_stale_after_seconds: float,
        busy_timeout_ms: int = 5000,
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
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute("PRAGMA foreign_keys=ON")
        return connection

    def _initialize(self) -> None:
        with self._connect() as connection:
            # The notification outbox is intentionally kept out of WAL mode.
            # It has multiple short-lived producer/consumer processes, which
            # matches the WAL-reset corruption race fixed only in SQLite
            # 3.51.3.  This host can run an older system SQLite, and the queue
            # is low-volume enough that rollback-journal serialization is the
            # safer trade-off.
            journal_mode = str(
                connection.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
            ).lower()
            if journal_mode != "delete":
                raise sqlite3.OperationalError(
                    f"notification outbox requires DELETE journal mode, got {journal_mode}"
                )
            connection.executescript(_SCHEMA)
            columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(notification_delivery_targets)")
            }
            if "acknowledged_at" not in columns:
                connection.execute(
                    "ALTER TABLE notification_delivery_targets ADD COLUMN acknowledged_at TEXT"
                )
            event_columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(notification_delivery_events)")
            }
            if "expires_at" not in event_columns:
                connection.execute(
                    "ALTER TABLE notification_delivery_events ADD COLUMN expires_at TEXT"
                )
            if "operator_targets_json" not in event_columns:
                connection.execute(
                    "ALTER TABLE notification_delivery_events "
                    "ADD COLUMN operator_targets_json TEXT NOT NULL DEFAULT '[]'"
                )
            if "operator_opportunity_id" not in event_columns:
                connection.execute(
                    "ALTER TABLE notification_delivery_events "
                    "ADD COLUMN operator_opportunity_id TEXT"
                )
            if "operator_generation" not in event_columns:
                connection.execute(
                    "ALTER TABLE notification_delivery_events "
                    "ADD COLUMN operator_generation INTEGER NOT NULL DEFAULT 0"
                )
            receipt_columns = {
                str(row["name"])
                for row in connection.execute(
                    "PRAGMA table_info(notification_delivery_terminal_receipts)"
                )
            }
            for column in ("attempted", "ok", "queued_for_recovery"):
                if column not in receipt_columns:
                    connection.execute(
                        "ALTER TABLE notification_delivery_terminal_receipts "
                        f"ADD COLUMN {column} INTEGER NOT NULL DEFAULT 0"
                    )

    def writable(self) -> bool:
        try:
            with self._connect() as connection:
                result = connection.execute("PRAGMA quick_check(1)").fetchone()
                if result is None or str(result[0]).lower() != "ok":
                    return False
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

    def cancellation_exists(self, event_id: str) -> bool:
        with self._connect() as connection:
            return (
                connection.execute(
                    """
                    SELECT 1 FROM notification_delivery_cancellations
                    WHERE event_id = ?
                    """,
                    (event_id,),
                ).fetchone()
                is not None
            )

    @staticmethod
    def _terminal_receipt(
        row: sqlite3.Row,
        *,
        outcome: str,
        reason: str,
        terminal_at: datetime,
        attempted: bool = False,
        ok: bool = False,
        queued_for_recovery: bool = False,
    ) -> TerminalDeliveryReceipt:
        terminal_text = _iso(terminal_at)
        attempt_ordinal = str(row["attempts"]) if "attempts" in row.keys() else ""
        receipt_id = hashlib.sha256(
            (
                f"{row['event_id']}|{row['sink']}|{outcome}|"
                f"{reason}|{terminal_text}|{int(attempted)}|{int(ok)}|"
                f"{attempt_ordinal}"
            ).encode("utf-8")
        ).hexdigest()
        return TerminalDeliveryReceipt(
            receipt_id=receipt_id,
            envelope=NotificationEnvelope(
                event_id=str(row["event_id"]),
                source=str(row["source"]),
                kind=str(row["kind"]),
                lane=str(row["lane"]),
                occurred_at=_parse(row["occurred_at"]),
                expires_at=(_parse(row["expires_at"]) if row["expires_at"] is not None else None),
            ),
            sink=str(row["sink"]),
            outcome=outcome,
            reason=reason,
            terminal_at=_utc(terminal_at),
            attempted=attempted,
            ok=ok,
            queued_for_recovery=queued_for_recovery,
        )

    def _record_terminal_receipts(
        self,
        connection: sqlite3.Connection,
        rows: Sequence[sqlite3.Row],
        *,
        outcome: str,
        reason: str,
        terminal_at: datetime,
        attempted: bool = False,
        ok: bool = False,
        queued_for_recovery: bool = False,
    ) -> tuple[TerminalDeliveryReceipt, ...]:
        receipts = tuple(
            self._terminal_receipt(
                row,
                outcome=outcome,
                reason=reason,
                terminal_at=terminal_at,
                attempted=attempted,
                ok=ok,
                queued_for_recovery=queued_for_recovery,
            )
            for row in rows
        )
        for receipt in receipts:
            connection.execute(
                """
                INSERT OR IGNORE INTO notification_delivery_terminal_receipts (
                    receipt_id, event_id, sink, outcome, reason, terminal_at,
                    attempted, ok, queued_for_recovery
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    receipt.receipt_id,
                    receipt.envelope.event_id,
                    receipt.sink,
                    receipt.outcome,
                    receipt.reason,
                    _iso(receipt.terminal_at),
                    int(receipt.attempted),
                    int(receipt.ok),
                    int(receipt.queued_for_recovery),
                ),
            )
        return receipts

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
                if (
                    connection.execute(
                        """
                    SELECT 1 FROM notification_delivery_cancellations
                    WHERE event_id = ?
                    """,
                        (envelope.event_id,),
                    ).fetchone()
                    is not None
                ):
                    raise DeliveryCancelled(
                        f"notification event {envelope.event_id} is cancellation-fenced"
                    )
                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO notification_delivery_events (
                        event_id, source, kind, lane, occurred_at, expires_at,
                        title, text, feishu_text, friend, operator_targets_json,
                        operator_opportunity_id, operator_generation, status,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        envelope.event_id,
                        envelope.source,
                        envelope.kind,
                        envelope.lane,
                        _iso(envelope.occurred_at),
                        _iso(envelope.expires_at) if envelope.expires_at else None,
                        title,
                        text,
                        feishu_text,
                        int(friend),
                        _operator_targets_json(envelope.operator_targets),
                        envelope.operator_opportunity_id,
                        envelope.operator_generation,
                        DeliveryStatus.PENDING.value,
                        now_text,
                        now_text,
                    ),
                )
                accepted = cursor.rowcount == 1
                if not accepted:
                    existing = connection.execute(
                        """
                        SELECT source, kind, lane, occurred_at, expires_at,
                               title, text, feishu_text, friend,
                               operator_targets_json, operator_opportunity_id,
                               operator_generation
                        FROM notification_delivery_events WHERE event_id = ?
                        """,
                        (envelope.event_id,),
                    ).fetchone()
                    expected = (
                        envelope.source,
                        envelope.kind,
                        envelope.lane,
                        _iso(envelope.occurred_at),
                        _iso(envelope.expires_at) if envelope.expires_at else None,
                        title,
                        text,
                        feishu_text,
                        int(friend),
                        _operator_targets_json(envelope.operator_targets),
                        envelope.operator_opportunity_id,
                        envelope.operator_generation,
                    )
                    if existing is None or tuple(existing) != expected:
                        raise ValueError(f"notification event_id collision for {envelope.event_id}")
                    existing_targets = tuple(
                        str(row["sink"])
                        for row in connection.execute(
                            """
                            SELECT sink FROM notification_delivery_targets
                            WHERE event_id = ? ORDER BY sink
                            """,
                            (envelope.event_id,),
                        ).fetchall()
                    )
                    if existing_targets != tuple(sorted(normalized_targets)):
                        raise ValueError(
                            f"notification event_id target collision for {envelope.event_id}"
                        )
                else:
                    for target in normalized_targets:
                        connection.execute(
                            """
                            INSERT INTO notification_delivery_targets (
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
                if connection.in_transaction:
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
        terminal_receipts: list[TerminalDeliveryReceipt] | None = None,
    ) -> list[DeliveryJob]:
        if limit_targets < 1:
            return []
        now = _utc(now)
        now_text = _iso(now)
        claimed_rows: list[sqlite3.Row] = []
        expired_receipts: tuple[TerminalDeliveryReceipt, ...] = ()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._requeue_stale_claims(connection, now=now)
                expired_receipts = self._expire_due_targets(connection, now=now)
                params: list[object] = [DeliveryStatus.PENDING.value, now_text]
                event_clause = ""
                if event_id is not None:
                    event_clause = " AND t.event_id = ?"
                    params.append(event_id)
                params.append(limit_targets)
                rows = connection.execute(
                    f"""
                    SELECT t.event_id, t.sink, e.source, e.kind, e.lane,
                           e.occurred_at, e.expires_at, e.title, e.text,
                           e.feishu_text, e.friend, e.operator_targets_json,
                           e.operator_opportunity_id, e.operator_generation
                    FROM notification_delivery_targets AS t
                    JOIN notification_delivery_events AS e USING (event_id)
                    WHERE t.status = ? AND t.next_attempt_at <= ?{event_clause}
                      AND NOT EXISTS (
                          SELECT 1 FROM notification_delivery_cancellations AS c
                          WHERE c.event_id = t.event_id
                      )
                    -- Strict lane priority protects expiring safety/trade
                    -- cards from old report fan-out. Within a lane, the
                    -- earliest expiry and retry due time retain FIFO order.
                    ORDER BY {_CLAIM_PRIORITY_SQL},
                             CASE WHEN e.expires_at IS NULL THEN 1 ELSE 0 END,
                             e.expires_at,
                             t.next_attempt_at,
                             e.created_at,
                             t.sink
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
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise
        if terminal_receipts is not None:
            terminal_receipts.extend(expired_receipts)
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
                        expires_at=(
                            _parse(first["expires_at"]) if first["expires_at"] is not None else None
                        ),
                        operator_targets=_parse_operator_targets(
                            first["operator_targets_json"]
                        ),
                        operator_opportunity_id=(
                            str(first["operator_opportunity_id"])
                            if first["operator_opportunity_id"] is not None
                            else None
                        ),
                        operator_generation=int(first["operator_generation"]),
                    ),
                    title=str(first["title"]),
                    text=str(first["text"]),
                    feishu_text=(
                        str(first["feishu_text"]) if first["feishu_text"] is not None else None
                    ),
                    friend=bool(first["friend"]),
                    targets=tuple(str(row["sink"]) for row in rows),
                )
            )
        return jobs

    def _expire_due_targets(
        self,
        connection: sqlite3.Connection,
        *,
        now: datetime,
    ) -> tuple[TerminalDeliveryReceipt, ...]:
        """Settle expired pending work without performing network delivery."""

        now_text = _iso(now)
        rows = connection.execute(
            """
            SELECT t.event_id, t.sink, e.source, e.kind, e.lane,
                   e.occurred_at, e.expires_at
            FROM notification_delivery_targets AS t
            JOIN notification_delivery_events AS e USING (event_id)
            WHERE t.status = ? AND e.expires_at IS NOT NULL
              AND e.expires_at <= ?
            ORDER BY t.event_id, t.sink
            """,
            (DeliveryStatus.PENDING.value, now_text),
        ).fetchall()
        if not rows:
            return ()
        connection.execute(
            """
            UPDATE notification_delivery_targets
            SET status = ?, next_attempt_at = ?, claimed_by = NULL,
                claimed_at = NULL,
                last_error = 'notification_expired_before_delivery',
                acknowledged_at = NULL, updated_at = ?
            WHERE status = ? AND event_id IN (
                SELECT event_id FROM notification_delivery_events
                WHERE expires_at IS NOT NULL AND expires_at <= ?
            )
            """,
            (
                DeliveryStatus.DEAD_LETTER.value,
                now_text,
                now_text,
                DeliveryStatus.PENDING.value,
                now_text,
            ),
        )
        receipts = self._record_terminal_receipts(
            connection,
            rows,
            outcome="expired_before_delivery",
            reason="notification_expired_before_delivery",
            terminal_at=now,
        )
        for event_id in {str(row["event_id"]) for row in rows}:
            self._refresh_event_status(connection, event_id, now_text)
        return receipts

    def expire_claimed_targets(
        self,
        event_id: str,
        targets: Iterable[str],
        *,
        worker_id: str,
        now: datetime | None = None,
    ) -> tuple[TerminalDeliveryReceipt, ...]:
        """Atomically settle an expired lease immediately before network I/O."""

        normalized_targets = tuple(dict.fromkeys(str(target) for target in targets))
        if not normalized_targets:
            return ()
        now = _utc(now)
        now_text = _iso(now)
        placeholders = ",".join("?" for _ in normalized_targets)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                rows = connection.execute(
                    f"""
                    SELECT t.event_id, t.sink, e.source, e.kind, e.lane,
                           e.occurred_at, e.expires_at
                    FROM notification_delivery_targets AS t
                    JOIN notification_delivery_events AS e USING (event_id)
                    WHERE t.event_id = ? AND t.sink IN ({placeholders})
                      AND t.status = ? AND t.claimed_by = ?
                      AND e.expires_at IS NOT NULL AND e.expires_at <= ?
                    ORDER BY t.sink
                    """,
                    (
                        event_id,
                        *normalized_targets,
                        DeliveryStatus.CLAIMED.value,
                        worker_id,
                        now_text,
                    ),
                ).fetchall()
                if not rows:
                    connection.execute("COMMIT")
                    return ()
                connection.execute(
                    f"""
                    UPDATE notification_delivery_targets
                    SET status = ?, next_attempt_at = ?, claimed_by = NULL,
                        claimed_at = NULL,
                        last_error = 'notification_expired_before_delivery',
                        acknowledged_at = NULL, updated_at = ?
                    WHERE event_id = ? AND sink IN ({placeholders})
                      AND status = ? AND claimed_by = ?
                    """,
                    (
                        DeliveryStatus.DEAD_LETTER.value,
                        now_text,
                        now_text,
                        event_id,
                        *normalized_targets,
                        DeliveryStatus.CLAIMED.value,
                        worker_id,
                    ),
                )
                receipts = self._record_terminal_receipts(
                    connection,
                    rows,
                    outcome="expired_before_delivery",
                    reason="notification_expired_before_delivery",
                    terminal_at=now,
                )
                self._refresh_event_status(connection, event_id, now_text)
                connection.execute("COMMIT")
            except Exception:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise
        return receipts

    def settle_target(
        self,
        event_id: str,
        sink: str,
        *,
        worker_id: str,
        ok: bool,
        error: str | None,
        permanent: bool = False,
        attempted: bool = True,
        receipt_outcome: str | None = None,
        now: datetime | None = None,
    ) -> DeliveryStatus:
        now = _utc(now)
        now_text = _iso(now)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    """
                    SELECT t.event_id, t.sink, t.attempts, t.max_attempts,
                           e.source, e.kind, e.lane, e.occurred_at,
                           e.expires_at, e.created_at
                    FROM notification_delivery_targets AS t
                    JOIN notification_delivery_events AS e USING (event_id)
                    WHERE t.event_id = ? AND t.sink = ? AND t.status = ?
                      AND t.claimed_by = ?
                    """,
                    (event_id, sink, DeliveryStatus.CLAIMED.value, worker_id),
                ).fetchone()
                if row is None:
                    raise DeliveryClaimLost(
                        f"target {event_id}/{sink} is not claimed by {worker_id}"
                    )
                if ok:
                    status = DeliveryStatus.DELIVERED
                    next_attempt_at = now_text
                    delivered_at = now_text
                else:
                    attempts = int(row["attempts"])
                    age_seconds = (now - _parse(row["created_at"])).total_seconds()
                    exhausted = attempts >= int(row["max_attempts"])
                    expired = age_seconds >= self.dead_letter_after_seconds
                    # Permanent failures (deterministic 4xx) dead-letter on the
                    # first attempt: retrying the identical payload cannot help.
                    if permanent or exhausted or expired:
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
                self._record_terminal_receipts(
                    connection,
                    (row,),
                    outcome=receipt_outcome or status.value,
                    reason=(
                        (error or "delivery failed")[:1000]
                        if not ok
                        else (error or "delivery_succeeded")[:1000]
                    ),
                    terminal_at=now,
                    attempted=attempted,
                    ok=ok,
                    queued_for_recovery=status is DeliveryStatus.PENDING,
                )
                connection.execute("COMMIT")
            except Exception:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise
        return status

    def record_unsettled_attempt(
        self,
        event_id: str,
        sink: str,
        *,
        attempted: bool,
        ok: bool,
        error: str | None,
        now: datetime | None = None,
    ) -> tuple[TerminalDeliveryReceipt, ...]:
        """Audit a completed network attempt whose delivery claim was lost."""

        now = _utc(now)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    """
                    SELECT t.event_id, t.sink, t.status, t.attempts,
                           e.source, e.kind, e.lane, e.occurred_at,
                           e.expires_at
                    FROM notification_delivery_targets AS t
                    JOIN notification_delivery_events AS e USING (event_id)
                    WHERE t.event_id = ? AND t.sink = ?
                    """,
                    (event_id, sink),
                ).fetchone()
                if row is None:
                    connection.execute("COMMIT")
                    return ()
                receipts = self._record_terminal_receipts(
                    connection,
                    (row,),
                    outcome="delivery_claim_lost",
                    reason=(error or "delivery claim lost")[:1000],
                    terminal_at=now,
                    attempted=attempted,
                    ok=ok,
                    queued_for_recovery=(
                        str(row["status"])
                        in {
                            DeliveryStatus.PENDING.value,
                            DeliveryStatus.CLAIMED.value,
                        }
                    ),
                )
                connection.execute("COMMIT")
            except Exception:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise
        return receipts

    def cancel_event_with_receipts(
        self,
        event_id: str,
        *,
        reason: str,
        now: datetime | None = None,
    ) -> tuple[TerminalDeliveryReceipt, ...]:
        """Cancel undelivered targets and atomically append terminal audit rows."""

        now = _utc(now)
        now_text = _iso(now)
        error = (reason or "notification_cancelled_before_delivery")[:1000]
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO notification_delivery_cancellations (
                        event_id, reason, cancelled_at
                    ) VALUES (?, ?, ?)
                    """,
                    (event_id, error, now_text),
                )
                rows = connection.execute(
                    """
                    SELECT t.event_id, t.sink, e.source, e.kind, e.lane,
                           e.occurred_at, e.expires_at
                    FROM notification_delivery_targets AS t
                    JOIN notification_delivery_events AS e USING (event_id)
                    WHERE t.event_id = ? AND t.status IN (?, ?)
                    ORDER BY t.sink
                    """,
                    (
                        event_id,
                        DeliveryStatus.PENDING.value,
                        DeliveryStatus.CLAIMED.value,
                    ),
                ).fetchall()
                if not rows:
                    connection.execute("COMMIT")
                    return ()
                connection.execute(
                    """
                    UPDATE notification_delivery_targets
                    SET status = ?, next_attempt_at = ?, claimed_by = NULL,
                        claimed_at = NULL, last_error = ?, acknowledged_at = ?,
                        updated_at = ?
                    WHERE event_id = ? AND status IN (?, ?)
                    """,
                    (
                        DeliveryStatus.DEAD_LETTER.value,
                        now_text,
                        error,
                        now_text,
                        now_text,
                        event_id,
                        DeliveryStatus.PENDING.value,
                        DeliveryStatus.CLAIMED.value,
                    ),
                )
                receipts = self._record_terminal_receipts(
                    connection,
                    rows,
                    outcome="cancelled_before_delivery",
                    reason=error,
                    terminal_at=now,
                )
                self._refresh_event_status(connection, event_id, now_text)
                connection.execute("COMMIT")
            except Exception:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise
        return receipts

    def cancel_event(
        self,
        event_id: str,
        *,
        reason: str,
        now: datetime | None = None,
    ) -> int:
        """Terminally settle undelivered targets after their source invalidates."""

        return len(self.cancel_event_with_receipts(event_id, reason=reason, now=now))

    def replay_dead_letter(self, event_id: str, *, now: datetime | None = None) -> int:
        """Reset one event's dead-letter targets to pending with a fresh budget.

        Returns the number of targets requeued; the next recovery run picks
        them up like any other pending target.
        """

        now_text = _iso(_utc(now))
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                cursor = connection.execute(
                    """
                    UPDATE notification_delivery_targets
                    SET status = ?, attempts = 0, next_attempt_at = ?,
                        claimed_by = NULL, claimed_at = NULL, last_error = NULL,
                        acknowledged_at = NULL, updated_at = ?
                    WHERE event_id = ? AND status = ?
                      AND NOT EXISTS (
                          SELECT 1 FROM notification_delivery_cancellations
                          WHERE event_id = ?
                      )
                    """,
                    (
                        DeliveryStatus.PENDING.value,
                        now_text,
                        now_text,
                        event_id,
                        DeliveryStatus.DEAD_LETTER.value,
                        event_id,
                    ),
                )
                replayed = cursor.rowcount
                if replayed:
                    self._refresh_event_status(connection, event_id, now_text)
                connection.execute("COMMIT")
            except Exception:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise
        return replayed

    def acknowledge_dead_letter(
        self,
        event_id: str,
        *,
        now: datetime | None = None,
    ) -> int:
        """Flag one event's dead-letter targets as reviewed by an operator.

        Acknowledged dead letters stay in the ledger but no longer fail the
        recovery task's health check. Returns the newly acknowledged count.
        """

        now_text = _iso(_utc(now))
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE notification_delivery_targets
                SET acknowledged_at = ?, updated_at = ?
                WHERE event_id = ? AND status = ? AND acknowledged_at IS NULL
                """,
                (now_text, now_text, event_id, DeliveryStatus.DEAD_LETTER.value),
            )
        return cursor.rowcount
