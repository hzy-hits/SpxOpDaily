"""Read-model and reconciliation methods shared by the delivery outbox."""

from __future__ import annotations

import sqlite3
from datetime import datetime
from typing import Iterable

from spx_spark.notifier.delivery_outbox_contract import (
    DeliveryEventInspection,
    DeliveryStatus,
    DeliverySummary,
    TerminalDeliveryReceipt,
    delivery_payload_fingerprint,
    iso,
    parse,
    utc,
)
from spx_spark.notifier.receipts import NotificationEnvelope


class DeliveryOutboxReadModelMixin:
    """Queries kept separate from the transactional delivery state machine."""

    def event_targets(self, event_id: str) -> tuple[str, ...]:
        """Return the immutable target set used when an event was enqueued."""

        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT sink FROM notification_delivery_targets
                WHERE event_id = ? ORDER BY sink
                """,
                (event_id,),
            ).fetchall()
        return tuple(str(row["sink"]) for row in rows)

    def event_operator_targets(self, event_id: str) -> tuple[tuple[str, str], ...]:
        """Return the frozen Rust fan-out targets for an existing event."""

        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT operator_targets_json
                FROM notification_delivery_events
                WHERE event_id = ?
                """,
                (event_id,),
            ).fetchone()
        if row is None:
            return ()
        return self._parse_operator_targets_json(row["operator_targets_json"])

    def inspect_event(
        self,
        envelope: NotificationEnvelope,
        *,
        title: str,
        text: str,
        feishu_text: str | None,
        friend: bool,
        targets: Iterable[str],
        expected_payload_fingerprint: str | None = None,
    ) -> DeliveryEventInspection:
        """Validate immutable payload, exact target set, fence and live status."""

        envelope.validate()
        expected_targets = tuple(sorted(dict.fromkeys(str(target) for target in targets)))
        with self._connect() as connection:
            cancelled = (
                connection.execute(
                    """
                    SELECT 1 FROM notification_delivery_cancellations
                    WHERE event_id = ?
                    """,
                    (envelope.event_id,),
                ).fetchone()
                is not None
            )
            event = connection.execute(
                """
                SELECT source, kind, lane, occurred_at, expires_at, title, text,
                       feishu_text, friend, operator_targets_json,
                       operator_opportunity_id, operator_generation, status
                FROM notification_delivery_events WHERE event_id = ?
                """,
                (envelope.event_id,),
            ).fetchone()
            target_rows = connection.execute(
                """
                SELECT sink, status FROM notification_delivery_targets
                WHERE event_id = ? ORDER BY sink
                """,
                (envelope.event_id,),
            ).fetchall()
        target_statuses = tuple((str(row["sink"]), str(row["status"])) for row in target_rows)
        if event is None:
            return DeliveryEventInspection(
                event_id=envelope.event_id,
                exists=False,
                cancelled=cancelled,
                payload_matches=False,
                targets_match=not expected_targets and not target_statuses,
                event_status=None,
                target_statuses=target_statuses,
                reason="cancelled" if cancelled else "missing",
            )
        actual_envelope = NotificationEnvelope(
            event_id=envelope.event_id,
            source=str(event["source"]),
            kind=str(event["kind"]),
            lane=str(event["lane"]),
            occurred_at=parse(event["occurred_at"]),
            expires_at=(parse(event["expires_at"]) if event["expires_at"] is not None else None),
            operator_targets=self._parse_operator_targets_json(
                event["operator_targets_json"]
            ),
            operator_opportunity_id=(
                str(event["operator_opportunity_id"])
                if event["operator_opportunity_id"] is not None
                else None
            ),
            operator_generation=int(event["operator_generation"]),
        )
        actual_fingerprint = delivery_payload_fingerprint(
            actual_envelope,
            title=str(event["title"]),
            text=str(event["text"]),
            feishu_text=(str(event["feishu_text"]) if event["feishu_text"] is not None else None),
            friend=bool(event["friend"]),
        )
        expected_fingerprint = expected_payload_fingerprint or delivery_payload_fingerprint(
            envelope,
            title=title,
            text=text,
            feishu_text=feishu_text,
            friend=friend,
        )
        payload_matches = actual_fingerprint == expected_fingerprint
        targets_match = (
            bool(expected_targets)
            and tuple(sink for sink, _status in target_statuses) == expected_targets
        )
        event_status = str(event["status"])
        target_states = tuple(status for _sink, status in target_statuses)
        known_live_states = {
            DeliveryStatus.PENDING.value,
            DeliveryStatus.CLAIMED.value,
            DeliveryStatus.DELIVERED.value,
        }
        if cancelled:
            reason = "cancelled"
        elif not payload_matches:
            reason = "payload_mismatch"
        elif not targets_match:
            reason = "target_mismatch"
        elif event_status not in known_live_states or any(
            status not in known_live_states for status in target_states
        ):
            reason = "terminal_or_invalid_status"
        else:
            expected_status = (
                DeliveryStatus.CLAIMED.value
                if DeliveryStatus.CLAIMED.value in target_states
                else DeliveryStatus.PENDING.value
                if DeliveryStatus.PENDING.value in target_states
                else DeliveryStatus.DELIVERED.value
            )
            reason = "accepted" if event_status == expected_status else "status_inconsistent"
        return DeliveryEventInspection(
            event_id=envelope.event_id,
            exists=True,
            cancelled=cancelled,
            payload_matches=payload_matches,
            targets_match=targets_match,
            event_status=event_status,
            target_statuses=target_statuses,
            reason=reason,
        )

    @staticmethod
    def _parse_operator_targets_json(value: object) -> tuple[tuple[str, str], ...]:
        import json

        try:
            parsed = json.loads(str(value or "[]"))
        except json.JSONDecodeError:
            return ()
        if not isinstance(parsed, list):
            return ()
        return tuple(
            (str(item[0]), str(item[1]))
            for item in parsed
            if isinstance(item, list) and len(item) == 2
        )

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

    def list_dead_letters(
        self,
        *,
        unacknowledged_only: bool = False,
    ) -> list[dict[str, object]]:
        """Dead-letter targets joined with their event, oldest update first."""

        query = """
            SELECT t.event_id, t.sink, t.attempts, t.max_attempts, t.last_error,
                   t.updated_at, t.acknowledged_at, e.title, e.kind, e.lane
            FROM notification_delivery_targets AS t
            JOIN notification_delivery_events AS e USING (event_id)
            WHERE t.status = ?
        """
        if unacknowledged_only:
            query += " AND t.acknowledged_at IS NULL"
        query += " ORDER BY t.updated_at, t.event_id, t.sink"
        with self._connect() as connection:
            rows = connection.execute(
                query,
                (DeliveryStatus.DEAD_LETTER.value,),
            ).fetchall()
        return [
            {
                "event_id": str(row["event_id"]),
                "sink": str(row["sink"]),
                "title": str(row["title"]),
                "kind": str(row["kind"]),
                "lane": str(row["lane"]),
                "attempts": int(row["attempts"]),
                "max_attempts": int(row["max_attempts"]),
                "last_error": row["last_error"],
                "updated_at": str(row["updated_at"]),
                "acknowledged": row["acknowledged_at"] is not None,
            }
            for row in rows
        ]

    def count_unacknowledged_dead_letters(self) -> int:
        """Dead letters no operator has reviewed yet; drives recovery health."""

        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS count FROM notification_delivery_targets
                WHERE status = ? AND acknowledged_at IS NULL
                """,
                (DeliveryStatus.DEAD_LETTER.value,),
            ).fetchone()
        return int(row["count"])

    def list_terminal_receipts(
        self,
        *,
        event_id: str | None = None,
        unrecorded_only: bool = False,
    ) -> list[TerminalDeliveryReceipt]:
        """Return durable outbox receipt intents without message bodies."""

        clauses: list[str] = []
        params: list[object] = []
        if event_id is not None:
            clauses.append("r.event_id = ?")
            params.append(event_id)
        if unrecorded_only:
            clauses.append("r.recorded_at IS NULL")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                f"""
                SELECT r.receipt_id, r.event_id, r.sink, r.outcome, r.reason,
                       r.terminal_at, r.attempted, r.ok, r.queued_for_recovery,
                       e.source, e.kind, e.lane, e.occurred_at, e.expires_at
                FROM notification_delivery_terminal_receipts AS r
                JOIN notification_delivery_events AS e USING (event_id)
                {where}
                ORDER BY r.terminal_at, r.event_id, r.sink
                """,
                tuple(params),
            ).fetchall()
        return [
            TerminalDeliveryReceipt(
                receipt_id=str(row["receipt_id"]),
                envelope=NotificationEnvelope(
                    event_id=str(row["event_id"]),
                    source=str(row["source"]),
                    kind=str(row["kind"]),
                    lane=str(row["lane"]),
                    occurred_at=parse(row["occurred_at"]),
                    expires_at=(
                        parse(row["expires_at"]) if row["expires_at"] is not None else None
                    ),
                ),
                sink=str(row["sink"]),
                outcome=str(row["outcome"]),
                reason=str(row["reason"]),
                terminal_at=parse(row["terminal_at"]),
                attempted=bool(row["attempted"]),
                ok=bool(row["ok"]),
                queued_for_recovery=bool(row["queued_for_recovery"]),
            )
            for row in rows
        ]

    def count_unrecorded_terminal_receipts(self) -> int:
        """Outbox receipt intents not yet mirrored to the receipt ledger."""

        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) AS count
                FROM notification_delivery_terminal_receipts
                WHERE recorded_at IS NULL
                """
            ).fetchone()
        return int(row["count"])

    def mark_terminal_receipts_recorded(
        self,
        receipt_ids: Iterable[str],
        *,
        now: datetime | None = None,
    ) -> int:
        normalized = tuple(dict.fromkeys(str(value) for value in receipt_ids if value))
        if not normalized:
            return 0
        now_text = iso(utc(now))
        recorded = 0
        with self._connect() as connection:
            for offset in range(0, len(normalized), 400):
                batch = normalized[offset : offset + 400]
                placeholders = ",".join("?" for _ in batch)
                cursor = connection.execute(
                    f"""
                    UPDATE notification_delivery_terminal_receipts
                    SET recorded_at = ?
                    WHERE receipt_id IN ({placeholders}) AND recorded_at IS NULL
                    """,
                    (now_text, *batch),
                )
                recorded += cursor.rowcount
        return recorded

    def mark_terminal_receipts_unrecorded(
        self,
        receipt_ids: Iterable[str],
    ) -> int:
        """Re-open mirror intents whose receipt-ledger rows are not provable."""

        normalized = tuple(dict.fromkeys(str(value) for value in receipt_ids if value))
        if not normalized:
            return 0
        reopened = 0
        with self._connect() as connection:
            for offset in range(0, len(normalized), 400):
                batch = normalized[offset : offset + 400]
                placeholders = ",".join("?" for _ in batch)
                cursor = connection.execute(
                    f"""
                    UPDATE notification_delivery_terminal_receipts
                    SET recorded_at = NULL
                    WHERE receipt_id IN ({placeholders})
                      AND recorded_at IS NOT NULL
                    """,
                    batch,
                )
                reopened += cursor.rowcount
        return reopened
