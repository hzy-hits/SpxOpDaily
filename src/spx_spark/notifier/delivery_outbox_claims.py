"""Pre-transport claim authorization for the notification outbox."""

from __future__ import annotations

from datetime import datetime
from typing import Iterable

from spx_spark.notifier.delivery_outbox_contract import (
    DeliveryClaimPreflight,
    DeliveryClaimRejection,
    DeliveryStatus,
    iso,
    parse,
    utc,
)


class DeliveryOutboxClaimMixin:
    """Atomically reject stale, cancelled or expired delivery claims."""

    def preflight_claimed_targets(
        self,
        event_id: str,
        targets: Iterable[str],
        *,
        worker_id: str,
        now: datetime | None = None,
    ) -> DeliveryClaimPreflight:
        normalized_targets = tuple(dict.fromkeys(str(target) for target in targets))
        if not normalized_targets:
            return DeliveryClaimPreflight(authorized_targets=(), rejections=())
        now = utc(now)
        now_text = iso(now)
        placeholders = ",".join("?" for _ in normalized_targets)
        authorized: list[str] = []
        rejections: list[DeliveryClaimRejection] = []
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                rows = connection.execute(
                    f"""
                    SELECT t.event_id, t.sink, t.status, t.attempts,
                           t.claimed_by, t.last_error,
                           e.source, e.kind, e.lane, e.occurred_at,
                           e.expires_at,
                           c.reason AS cancellation_reason
                    FROM notification_delivery_targets AS t
                    JOIN notification_delivery_events AS e USING (event_id)
                    LEFT JOIN notification_delivery_cancellations AS c
                      USING (event_id)
                    WHERE t.event_id = ? AND t.sink IN ({placeholders})
                    """,
                    (event_id, *normalized_targets),
                ).fetchall()
                rows_by_sink = {str(row["sink"]): row for row in rows}
                status_changed = False
                for sink in normalized_targets:
                    row = rows_by_sink.get(sink)
                    if row is None:
                        rejections.append(
                            DeliveryClaimRejection(
                                sink=sink,
                                outcome="delivery_target_missing_before_transport",
                                reason="delivery target missing before transport",
                                status=None,
                            )
                        )
                        continue
                    status = str(row["status"])
                    claimed_by = str(row["claimed_by"]) if row["claimed_by"] is not None else None
                    cancellation_reason = (
                        str(row["cancellation_reason"])
                        if row["cancellation_reason"] is not None
                        else None
                    )
                    if cancellation_reason is not None:
                        changed = self._cancel_preflight_target(
                            connection,
                            row,
                            reason=cancellation_reason,
                            now=now,
                            now_text=now_text,
                        )
                        status_changed = status_changed or changed
                        rejections.append(
                            DeliveryClaimRejection(
                                sink=sink,
                                outcome="cancelled_before_delivery",
                                reason=cancellation_reason,
                                status=(DeliveryStatus.DEAD_LETTER.value if changed else status),
                            )
                        )
                        continue
                    expires_at = parse(row["expires_at"]) if row["expires_at"] is not None else None
                    if expires_at is not None and expires_at <= now:
                        changed = self._expire_preflight_target(
                            connection,
                            row,
                            worker_id=worker_id,
                            now=now,
                            now_text=now_text,
                        )
                        status_changed = status_changed or changed
                        if changed or row["last_error"] == ("notification_expired_before_delivery"):
                            outcome = "expired_before_delivery"
                            reason = "notification_expired_before_delivery"
                            rejection_status = DeliveryStatus.DEAD_LETTER.value
                        else:
                            outcome = "delivery_claim_invalid_before_transport"
                            reason = "expired target is no longer owned before transport"
                            rejection_status = status
                            self._record_invalid_claim_receipt(
                                connection,
                                row,
                                reason=reason,
                                now=now,
                            )
                        rejections.append(
                            DeliveryClaimRejection(
                                sink=sink,
                                outcome=outcome,
                                reason=reason,
                                status=rejection_status,
                            )
                        )
                        continue
                    if status != DeliveryStatus.CLAIMED.value or claimed_by != worker_id:
                        reason = (
                            "delivery claim no longer owned before transport "
                            f"(status={status}, owner={claimed_by or '-'})"
                        )
                        self._record_invalid_claim_receipt(
                            connection,
                            row,
                            reason=reason,
                            now=now,
                        )
                        rejections.append(
                            DeliveryClaimRejection(
                                sink=sink,
                                outcome="delivery_claim_invalid_before_transport",
                                reason=reason,
                                status=status,
                            )
                        )
                        continue
                    authorized.append(sink)
                if status_changed:
                    self._refresh_event_status(
                        connection,
                        event_id,
                        now_text,
                    )
                connection.execute("COMMIT")
            except Exception:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise
        return DeliveryClaimPreflight(
            authorized_targets=tuple(authorized),
            rejections=tuple(rejections),
        )

    def _cancel_preflight_target(
        self,
        connection,
        row,
        *,
        reason: str,
        now: datetime,
        now_text: str,
    ) -> bool:
        cursor = connection.execute(
            """
            UPDATE notification_delivery_targets
            SET status = ?, next_attempt_at = ?, claimed_by = NULL,
                claimed_at = NULL, last_error = ?, acknowledged_at = ?,
                updated_at = ?
            WHERE event_id = ? AND sink = ? AND status IN (?, ?)
            """,
            (
                DeliveryStatus.DEAD_LETTER.value,
                now_text,
                reason[:1000],
                now_text,
                now_text,
                row["event_id"],
                row["sink"],
                DeliveryStatus.PENDING.value,
                DeliveryStatus.CLAIMED.value,
            ),
        )
        if not cursor.rowcount:
            return False
        self._record_terminal_receipts(
            connection,
            (row,),
            outcome="cancelled_before_delivery",
            reason=reason[:1000],
            terminal_at=now,
        )
        return True

    def _expire_preflight_target(
        self,
        connection,
        row,
        *,
        worker_id: str,
        now: datetime,
        now_text: str,
    ) -> bool:
        status = str(row["status"])
        claimed_by = str(row["claimed_by"]) if row["claimed_by"] is not None else None
        if status == DeliveryStatus.PENDING.value:
            ownership_clause = "status = ?"
            ownership_params = (DeliveryStatus.PENDING.value,)
        elif status == DeliveryStatus.CLAIMED.value and claimed_by == worker_id:
            ownership_clause = "status = ? AND claimed_by = ?"
            ownership_params = (DeliveryStatus.CLAIMED.value, worker_id)
        else:
            return False
        cursor = connection.execute(
            f"""
            UPDATE notification_delivery_targets
            SET status = ?, next_attempt_at = ?, claimed_by = NULL,
                claimed_at = NULL,
                last_error = 'notification_expired_before_delivery',
                acknowledged_at = NULL, updated_at = ?
            WHERE event_id = ? AND sink = ? AND {ownership_clause}
            """,
            (
                DeliveryStatus.DEAD_LETTER.value,
                now_text,
                now_text,
                row["event_id"],
                row["sink"],
                *ownership_params,
            ),
        )
        if not cursor.rowcount:
            return False
        self._record_terminal_receipts(
            connection,
            (row,),
            outcome="expired_before_delivery",
            reason="notification_expired_before_delivery",
            terminal_at=now,
        )
        return True

    def _record_invalid_claim_receipt(
        self,
        connection,
        row,
        *,
        reason: str,
        now: datetime,
    ) -> None:
        status = str(row["status"])
        self._record_terminal_receipts(
            connection,
            (row,),
            outcome="delivery_claim_invalid_before_transport",
            reason=reason[:1000],
            terminal_at=now,
            attempted=False,
            ok=False,
            queued_for_recovery=status
            in {
                DeliveryStatus.PENDING.value,
                DeliveryStatus.CLAIMED.value,
            },
        )
