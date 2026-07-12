"""Idempotent outbox consumer for alert-candidate delivery."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Protocol, Sequence

from spx_spark.domain.events import DomainEvent, EventKind
from spx_spark.infrastructure.ledger.outbox import OutboxStatus, SqliteEventOutbox


DeliverFn = Callable[[DomainEvent], bool]


class ProcessedIdSet(Protocol):
    def __contains__(self, event_id: object) -> bool: ...

    def add(self, event_id: str) -> None: ...


@dataclass
class ConsumeResult:
    claimed: int = 0
    delivered: int = 0
    duplicate_skipped: int = 0
    failed: int = 0
    dead_lettered: int = 0
    acked_ids: list[str] = field(default_factory=list)


class IdempotentOutboxConsumer:
    """Claim → deliver → ack. Crash before ack leaves the event reclaimable.

    ``processed_ids`` is the durable dedup set so a restarted consumer that
    reclaims an already-delivered event_id never double-sends.
    """

    def __init__(
        self,
        outbox: SqliteEventOutbox,
        *,
        consumer_id: str,
        deliver: DeliverFn,
        processed_ids: ProcessedIdSet | None = None,
        claim_stale_after_seconds: float = 30.0,
    ) -> None:
        self.outbox = outbox
        self.consumer_id = consumer_id
        self.deliver = deliver
        self.processed_ids: ProcessedIdSet = (
            processed_ids if processed_ids is not None else set()
        )
        self.claim_stale_after_seconds = claim_stale_after_seconds

    def consume(
        self,
        *,
        limit: int = 10,
        kinds: Sequence[EventKind] | None = None,
        now: datetime | None = None,
    ) -> ConsumeResult:
        now = now or datetime.now(tz=timezone.utc)
        self.outbox.requeue_stale_claims(
            older_than_seconds=self.claim_stale_after_seconds,
            now=now,
        )
        kind_values = [kind.value for kind in kinds] if kinds else None
        claimed = self.outbox.claim(
            consumer_id=self.consumer_id,
            limit=limit,
            now=now,
            kinds=kind_values,
        )
        result = ConsumeResult(claimed=len(claimed))
        for event in claimed:
            if event.event_id in self.processed_ids:
                self.outbox.ack([event.event_id], consumer_id=self.consumer_id)
                result.duplicate_skipped += 1
                result.acked_ids.append(event.event_id)
                continue
            try:
                ok = self.deliver(event)
            except Exception as exc:  # noqa: BLE001
                status = self.outbox.fail(
                    event.event_id,
                    error=str(exc),
                    consumer_id=self.consumer_id,
                    now=now,
                )
                result.failed += 1
                if status is OutboxStatus.DEAD_LETTER:
                    result.dead_lettered += 1
                continue
            if not ok:
                status = self.outbox.fail(
                    event.event_id,
                    error="deliver_returned_false",
                    consumer_id=self.consumer_id,
                    now=now,
                )
                result.failed += 1
                if status is OutboxStatus.DEAD_LETTER:
                    result.dead_lettered += 1
                continue
            self.processed_ids.add(event.event_id)
            self.outbox.ack([event.event_id], consumer_id=self.consumer_id)
            result.delivered += 1
            result.acked_ids.append(event.event_id)
        return result
