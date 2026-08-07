"""SQLAlchemy Core storage for one notification event row per frozen target."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Mapping, Sequence

import sqlalchemy as sa
from sqlalchemy import Engine

from spx_spark.domain.events import AppendResult, DomainEvent


CANCELLATION_CHANNEL = "__cancellation__"


class NotificationStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    DELIVERED = "delivered"
    FAILED = "failed"
    UNCERTAIN = "uncertain"


metadata = sa.MetaData()
events = sa.Table(
    "notification_events",
    metadata,
    sa.Column("id", sa.Integer(), primary_key=True),
    sa.Column("idempotency_key", sa.Text(), nullable=False),
    sa.Column("logical_event_id", sa.Text(), nullable=False),
    sa.Column("source", sa.Text(), nullable=False),
    sa.Column("kind", sa.Text(), nullable=False),
    sa.Column("lane", sa.Text(), nullable=False),
    sa.Column("channel", sa.Text(), nullable=False),
    sa.Column("payload_json", sa.Text(), nullable=False),
    sa.Column("payload_sha256", sa.Text(), nullable=False),
    sa.Column("status", sa.Text(), nullable=False),
    sa.Column("expires_at", sa.DateTime(), nullable=True),
    sa.Column("cancelled_at", sa.DateTime(), nullable=True),
    sa.Column("cancel_reason", sa.Text(), nullable=True),
    sa.Column("last_error", sa.Text(), nullable=True),
    sa.Column("created_at", sa.DateTime(), nullable=False),
    sa.Column("updated_at", sa.DateTime(), nullable=False),
)
attempts = sa.Table(
    "notification_attempts",
    metadata,
    sa.Column("id", sa.Integer(), primary_key=True),
    sa.Column("event_id", sa.Integer(), nullable=False),
    sa.Column("attempt_no", sa.Integer(), nullable=False),
    sa.Column("started_at", sa.DateTime(), nullable=False),
    sa.Column("finished_at", sa.DateTime(), nullable=True),
    sa.Column("outcome", sa.Text(), nullable=True),
    sa.Column("attempted", sa.Boolean(), nullable=False),
    sa.Column("ok", sa.Boolean(), nullable=False),
    sa.Column("error_code", sa.Text(), nullable=True),
    sa.Column("error_detail", sa.Text(), nullable=True),
)


@dataclass(frozen=True, slots=True)
class NotificationDraft:
    logical_event_id: str
    source: str
    kind: str
    lane: str
    payload: Mapping[str, object]
    channels: tuple[str, ...]
    expires_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class EnqueueBatch:
    event_ids: tuple[int, ...]
    inserted: int
    duplicate: int
    cancelled: bool


@dataclass(frozen=True, slots=True)
class Attempt:
    attempt_id: int
    event_id: int
    attempt_no: int
    channel: str
    payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class Recovery:
    retry_event_ids: tuple[int, ...]
    uncertain_event_ids: tuple[int, ...]


def create_database_engine(database_path: Path, *, timeout_seconds: float = 5.0) -> Engine:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    engine = sa.create_engine(
        f"sqlite:///{database_path}",
        connect_args={"timeout": timeout_seconds},
    )
    sa.event.listen(engine, "connect", _configure_sqlite_connection)
    return engine


def _configure_sqlite_connection(connection, _record) -> None:
    cursor = connection.cursor()
    try:
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA synchronous=NORMAL")
    finally:
        cursor.close()


def create_engine(data_root: Path) -> Engine:
    return create_database_engine(data_root / "spx.sqlite")


def _now(value: datetime | None = None) -> datetime:
    result = value or datetime.now(tz=timezone.utc)
    if result.tzinfo is None:
        raise ValueError("notification timestamp must be timezone-aware")
    return result.astimezone(timezone.utc).replace(tzinfo=None)


def _payload(value: Mapping[str, object]) -> tuple[str, str]:
    text = json.dumps(
        dict(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return text, hashlib.sha256(text.encode("utf-8")).hexdigest()


def _key(logical_event_id: str, channel: str) -> str:
    return hashlib.sha256(f"{logical_event_id}\0{channel}".encode()).hexdigest()


def _domain_event_payload(event: DomainEvent) -> dict[str, object]:
    event.validate()
    return {
        "schema_version": event.schema_version,
        "event_id": event.event_id,
        "kind": event.kind.value,
        "source_at": event.source_at.isoformat(),
        "available_at": event.available_at.isoformat(),
        "aggregate_id": event.aggregate_id,
        "sequence": event.sequence,
        "payload": dict(event.payload),
    }


class NotificationEventQueue:
    """Realtime-engine append port backed by notification_events."""

    def __init__(
        self,
        engine: Engine,
        *,
        schedule,
    ) -> None:
        self.engine = engine
        self.schedule = schedule

    def writable(self) -> bool:
        try:
            with self.engine.begin() as connection:
                connection.execute(sa.select(sa.literal(1)))
            return True
        except sa.exc.SQLAlchemyError:
            return False

    def append(self, domain_events: Sequence[DomainEvent]) -> AppendResult:
        accepted = duplicate = 0
        scheduled: list[int] = []
        for event in domain_events:
            batch = enqueue(
                self.engine,
                NotificationDraft(
                    logical_event_id=event.event_id,
                    source="alert_pipeline",
                    kind=event.kind.value,
                    lane="alert_candidate",
                    payload={"domain_event": _domain_event_payload(event)},
                    channels=("alert_pipeline",),
                    expires_at=None,
                ),
                now=event.available_at,
            )
            accepted += batch.inserted
            duplicate += batch.duplicate
            scheduled.extend(batch.event_ids)
        for event_id in scheduled:
            self.schedule(event_id)
        return AppendResult(accepted=accepted, duplicate=duplicate, writable=True)


def enqueue(
    engine: Engine, draft: NotificationDraft, *, now: datetime | None = None
) -> EnqueueBatch:
    if not draft.logical_event_id.strip() or not draft.channels:
        raise ValueError("logical event id and at least one channel are required")
    if len(set(draft.channels)) != len(draft.channels):
        raise ValueError("notification channels must be unique")
    at = _now(now)
    payload_json, fingerprint = _payload(draft.payload)
    inserted = duplicate = 0
    event_ids: list[int] = []
    with engine.begin() as connection:
        fence = connection.execute(
            sa.select(events.c.id).where(
                events.c.logical_event_id == draft.logical_event_id,
                events.c.channel == CANCELLATION_CHANNEL,
            )
        ).first()
        if fence is not None:
            return EnqueueBatch((), 0, 0, True)
        for channel in draft.channels:
            existing = connection.execute(
                sa.select(events.c.id, events.c.payload_sha256).where(
                    events.c.logical_event_id == draft.logical_event_id,
                    events.c.channel == channel,
                )
            ).first()
            if existing is not None:
                if existing.payload_sha256 != fingerprint:
                    raise ValueError("notification idempotency collision")
                event_ids.append(int(existing.id))
                duplicate += 1
                continue
            result = connection.execute(
                sa.insert(events).values(
                    idempotency_key=_key(draft.logical_event_id, channel),
                    logical_event_id=draft.logical_event_id,
                    source=draft.source,
                    kind=draft.kind,
                    lane=draft.lane,
                    channel=channel,
                    payload_json=payload_json,
                    payload_sha256=fingerprint,
                    status=NotificationStatus.PENDING.value,
                    expires_at=_now(draft.expires_at) if draft.expires_at else None,
                    created_at=at,
                    updated_at=at,
                )
            )
            event_ids.append(int(result.inserted_primary_key[0]))
            inserted += 1
    return EnqueueBatch(tuple(event_ids), inserted, duplicate, False)


def cancel(
    engine: Engine,
    logical_event_id: str,
    *,
    reason: str,
    now: datetime | None = None,
) -> int:
    at = _now(now)
    cancelled = 0
    with engine.begin() as connection:
        fence = connection.execute(
            sa.select(events.c.id).where(
                events.c.logical_event_id == logical_event_id,
                events.c.channel == CANCELLATION_CHANNEL,
            )
        ).first()
        if fence is None:
            connection.execute(
                sa.insert(events).values(
                    idempotency_key=_key(logical_event_id, CANCELLATION_CHANNEL),
                    logical_event_id=logical_event_id,
                    source="cancellation_fence",
                    kind="cancellation",
                    lane="internal",
                    channel=CANCELLATION_CHANNEL,
                    payload_json="{}",
                    payload_sha256=hashlib.sha256(b"{}").hexdigest(),
                    status=NotificationStatus.FAILED.value,
                    cancelled_at=at,
                    cancel_reason=reason,
                    created_at=at,
                    updated_at=at,
                )
            )
        rows = connection.execute(
            sa.select(events.c.id).where(
                events.c.logical_event_id == logical_event_id,
                events.c.channel != CANCELLATION_CHANNEL,
                events.c.status.in_(
                    (NotificationStatus.PENDING.value, NotificationStatus.FAILED.value)
                ),
            )
        ).all()
        for row in rows:
            connection.execute(
                sa.update(events)
                .where(events.c.id == row.id)
                .values(
                    status=NotificationStatus.FAILED.value,
                    cancelled_at=at,
                    cancel_reason=reason,
                    last_error="cancelled_before_transport",
                    updated_at=at,
                )
            )
            cancelled += 1
    return cancelled


def begin_attempt(
    engine: Engine,
    event_id: int,
    *,
    max_attempts: int = 3,
    now: datetime | None = None,
) -> Attempt | None:
    if max_attempts < 1:
        raise ValueError("max_attempts must be positive")
    at = _now(now)
    with engine.begin() as connection:
        row = (
            connection.execute(sa.select(events).where(events.c.id == event_id)).mappings().first()
        )
        if row is None or row["cancelled_at"] is not None:
            return None
        if row["status"] in {
            NotificationStatus.DELIVERED.value,
            NotificationStatus.PROCESSING.value,
            NotificationStatus.UNCERTAIN.value,
        }:
            return None
        if row["expires_at"] is not None and row["expires_at"] <= at:
            connection.execute(
                sa.update(events)
                .where(events.c.id == event_id)
                .values(
                    status=NotificationStatus.FAILED.value,
                    last_error="expired_before_transport",
                    updated_at=at,
                )
            )
            return None
        previous = connection.execute(
            sa.select(sa.func.max(attempts.c.attempt_no)).where(attempts.c.event_id == event_id)
        ).scalar_one_or_none()
        attempt_no = int(previous or 0) + 1
        if attempt_no > max_attempts:
            return None
        connection.execute(
            sa.update(events)
            .where(events.c.id == event_id)
            .values(status=NotificationStatus.PROCESSING.value, updated_at=at)
        )
        result = connection.execute(
            sa.insert(attempts).values(
                event_id=event_id,
                attempt_no=attempt_no,
                started_at=at,
                outcome="processing",
                attempted=False,
                ok=False,
            )
        )
        return Attempt(
            attempt_id=int(result.inserted_primary_key[0]),
            event_id=event_id,
            attempt_no=attempt_no,
            channel=str(row["channel"]),
            payload=json.loads(str(row["payload_json"])),
        )


def mark_transport_started(engine: Engine, attempt_id: int) -> None:
    with engine.begin() as connection:
        connection.execute(
            sa.update(attempts).where(attempts.c.id == attempt_id).values(attempted=True)
        )


def settle(
    engine: Engine,
    attempt_id: int,
    *,
    status: NotificationStatus,
    outcome: str,
    ok: bool,
    error_code: str | None = None,
    error_detail: str | None = None,
    now: datetime | None = None,
) -> None:
    if ok is not (status is NotificationStatus.DELIVERED):
        raise ValueError("only delivered notification attempts may be ok")
    at = _now(now)
    with engine.begin() as connection:
        event_id = connection.execute(
            sa.select(attempts.c.event_id).where(attempts.c.id == attempt_id)
        ).scalar_one()
        connection.execute(
            sa.update(attempts)
            .where(attempts.c.id == attempt_id)
            .values(
                finished_at=at,
                outcome=outcome,
                ok=ok,
                error_code=error_code,
                error_detail=error_detail,
            )
        )
        connection.execute(
            sa.update(events)
            .where(events.c.id == event_id)
            .values(status=status.value, last_error=error_detail, updated_at=at)
        )


def recover_incomplete_attempts(
    engine: Engine,
    *,
    now: datetime | None = None,
) -> Recovery:
    """Fail pre-transport crashes and quarantine possibly-sent attempts."""

    at = _now(now)
    retry_ids: list[int] = []
    uncertain_ids: list[int] = []
    with engine.begin() as connection:
        rows = connection.execute(
            sa.select(
                events.c.id.label("event_id"),
                attempts.c.id.label("attempt_id"),
                attempts.c.attempted,
            )
            .join(attempts, attempts.c.event_id == events.c.id)
            .where(
                events.c.status == NotificationStatus.PROCESSING.value,
                attempts.c.finished_at.is_(None),
            )
        ).mappings()
        for row in rows:
            transport_started = bool(row["attempted"])
            status = (
                NotificationStatus.UNCERTAIN if transport_started else NotificationStatus.FAILED
            )
            outcome = (
                "transport_outcome_uncertain"
                if transport_started
                else "interrupted_before_transport"
            )
            connection.execute(
                sa.update(attempts)
                .where(attempts.c.id == row["attempt_id"])
                .values(
                    finished_at=at,
                    outcome=outcome,
                    ok=False,
                    error_code=outcome,
                )
            )
            connection.execute(
                sa.update(events)
                .where(events.c.id == row["event_id"])
                .values(status=status.value, last_error=outcome, updated_at=at)
            )
            target = uncertain_ids if transport_started else retry_ids
            target.append(int(row["event_id"]))
    return Recovery(tuple(retry_ids), tuple(uncertain_ids))


def event_rows(engine: Engine, logical_event_id: str) -> Sequence[Mapping[str, object]]:
    with engine.connect() as connection:
        return tuple(
            connection.execute(
                sa.select(events)
                .where(events.c.logical_event_id == logical_event_id)
                .order_by(events.c.channel)
            ).mappings()
        )


def due_event_ids(engine: Engine, *, limit: int = 1) -> tuple[int, ...]:
    if limit < 1:
        return ()
    latest_attempt = (
        sa.select(
            attempts.c.event_id,
            sa.func.max(attempts.c.id).label("attempt_id"),
            sa.func.count(attempts.c.id).label("attempt_count"),
        )
        .group_by(attempts.c.event_id)
        .subquery()
    )
    with engine.connect() as connection:
        rows = connection.execute(
            sa.select(events.c.id)
            .outerjoin(latest_attempt, latest_attempt.c.event_id == events.c.id)
            .outerjoin(attempts, attempts.c.id == latest_attempt.c.attempt_id)
            .where(
                events.c.cancelled_at.is_(None),
                sa.or_(
                    events.c.status == NotificationStatus.PENDING.value,
                    sa.and_(
                        events.c.status == NotificationStatus.FAILED.value,
                        attempts.c.outcome.in_(
                            ("retryable_failure", "interrupted_before_transport")
                        ),
                        latest_attempt.c.attempt_count < 3,
                    ),
                ),
            )
            .order_by(events.c.created_at, events.c.id)
            .limit(limit)
        ).scalars()
        return tuple(int(event_id) for event_id in rows)


def status_counts(engine: Engine) -> dict[str, int]:
    with engine.connect() as connection:
        rows = connection.execute(
            sa.select(events.c.status, sa.func.count(events.c.id)).where(
                events.c.channel != CANCELLATION_CHANNEL
            ).group_by(events.c.status)
        )
        return {str(status): int(count) for status, count in rows}
