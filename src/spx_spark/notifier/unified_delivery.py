"""Unified notification enqueue and single-target Huey delivery."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
import hashlib
import json
import sqlite3
from typing import Callable, Mapping, Sequence

from sqlalchemy import Engine

from spx_spark.app_settings import get_settings
from spx_spark.config import NotificationSettings
from spx_spark.domain.events import DomainEvent, EventKind
from spx_spark.infrastructure.notifications import (
    CANCELLATION_CHANNEL,
    NotificationDraft,
    NotificationStatus,
    begin_attempt,
    cancel,
    create_database_engine,
    enqueue,
    event_rows,
    mark_transport_started,
    metadata,
    recover_incomplete_attempts,
    settle,
)
from spx_spark.notifier.model import (
    DeliveryEventInspection,
    ExternalDeliveryReceipt,
    ExternalDeliveryReceiptLookup,
)
from spx_spark.notifier.human_policy import quiet_window_suppresses
from spx_spark.notifier.model import SinkResult
from spx_spark.notifier.model import NotificationEnvelope
from spx_spark.notifier.rust_ingress import (
    deliver_operator_template,
    operator_notification_role,
)
from spx_spark.notifier.sinks import deliver_trade_push, delivery_target_names


MAX_DELIVERY_ATTEMPTS = 3


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


def inspect_external_delivery_receipt(
    event_id: str,
    *,
    rust_owner: bool,
    rust_ledger_path: str = "",
    python_ledger_path: str = "",
) -> ExternalDeliveryReceiptLookup:
    """Read the earliest proved Bark/Feishu delivery without mutating ledgers."""

    normalized = event_id.strip()
    if not normalized:
        return ExternalDeliveryReceiptLookup(
            observable=False,
            receipt=None,
            error="external_delivery_event_id_unavailable",
        )
    if rust_owner:
        return _inspect_rust_external_receipt(normalized, rust_ledger_path)
    return _inspect_python_external_receipt(normalized, python_ledger_path)


def _inspect_rust_external_receipt(
    event_id: str,
    path: str,
) -> ExternalDeliveryReceiptLookup:
    if not path:
        return ExternalDeliveryReceiptLookup(
            observable=False,
            receipt=None,
            error="rust_delivery_ledger_path_unavailable",
        )
    database = Path(path)
    if not database.is_file():
        return ExternalDeliveryReceiptLookup(
            observable=False,
            receipt=None,
            error="rust_delivery_ledger_unavailable",
        )
    try:
        with sqlite3.connect(f"file:{database}?mode=ro", uri=True, timeout=1.0) as connection:
            connection.execute("PRAGMA query_only=ON")
            row = connection.execute(
                """
                SELECT r.receipt_id, r.target_key, r.channel, r.occurred_at_us
                FROM delivery_receipts AS r
                JOIN notification_events AS e ON e.event_id = r.intent_id
                WHERE r.intent_id = ?
                  AND r.outcome = 'delivered'
                  AND r.attempted = 1
                  AND r.ok = 1
                  AND r.channel IN ('bark', 'feishu')
                ORDER BY r.occurred_at_us, r.receipt_id
                LIMIT 1
                """,
                (event_id,),
            ).fetchone()
    except (OSError, sqlite3.Error, ValueError):
        return ExternalDeliveryReceiptLookup(
            observable=False,
            receipt=None,
            error="rust_delivery_ledger_query_failed",
        )
    if row is None:
        return ExternalDeliveryReceiptLookup(observable=True, receipt=None)
    try:
        delivered_at = datetime.fromtimestamp(int(row[3]) / 1_000_000, tz=timezone.utc)
    except (OSError, OverflowError, TypeError, ValueError):
        return ExternalDeliveryReceiptLookup(
            observable=False,
            receipt=None,
            error="rust_delivery_receipt_timestamp_invalid",
        )
    return ExternalDeliveryReceiptLookup(
        observable=True,
        receipt=ExternalDeliveryReceipt(
            event_id=event_id,
            receipt_id=str(row[0]),
            delivered_at=delivered_at,
            sink=str(row[1]),
            channel=str(row[2]),
            ledger="rust_operations",
        ),
    )


def _inspect_python_external_receipt(
    event_id: str,
    path: str,
) -> ExternalDeliveryReceiptLookup:
    if not path:
        return ExternalDeliveryReceiptLookup(
            observable=False,
            receipt=None,
            error="python_notification_ledger_path_unavailable",
        )
    database = Path(path)
    if not database.is_file():
        return ExternalDeliveryReceiptLookup(
            observable=False,
            receipt=None,
            error="python_notification_ledger_unavailable",
        )
    try:
        with sqlite3.connect(f"file:{database}?mode=ro", uri=True, timeout=1.0) as connection:
            connection.execute("PRAGMA query_only=ON")
            row = connection.execute(
                """
                SELECT a.id, e.channel, a.finished_at
                FROM notification_events AS e
                JOIN notification_attempts AS a ON a.event_id = e.id
                WHERE e.logical_event_id = ?
                  AND e.channel IN ('bark', 'feishu')
                  AND a.outcome = 'delivered'
                  AND a.attempted = 1
                  AND a.ok = 1
                ORDER BY a.finished_at, a.id
                LIMIT 1
                """,
                (event_id,),
            ).fetchone()
    except (OSError, sqlite3.Error, ValueError):
        return ExternalDeliveryReceiptLookup(
            observable=False,
            receipt=None,
            error="python_notification_ledger_query_failed",
        )
    if row is None:
        return ExternalDeliveryReceiptLookup(observable=True, receipt=None)
    try:
        delivered_at = datetime.fromisoformat(str(row[2]))
        if delivered_at.tzinfo is None:
            delivered_at = delivered_at.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return ExternalDeliveryReceiptLookup(
            observable=False,
            receipt=None,
            error="python_notification_receipt_timestamp_invalid",
        )
    channel = str(row[1])
    return ExternalDeliveryReceiptLookup(
        observable=True,
        receipt=ExternalDeliveryReceipt(
            event_id=event_id,
            receipt_id=f"python-notification-attempt:{row[0]}",
            delivered_at=delivered_at.astimezone(timezone.utc),
            sink=channel,
            channel=channel,
            ledger="python_notifications",
        ),
    )


class RetryableDeliveryError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class UnifiedEnqueueResult:
    envelope: NotificationEnvelope
    targets: tuple[str, ...]
    event_ids: tuple[int, ...]
    outcome: str
    inserted: int
    duplicate: int
    accepted: bool


@dataclass(frozen=True, slots=True)
class UnifiedDeliveryResult:
    event_id: int
    channel: str | None
    outcome: str
    attempted: bool
    ok: bool


@lru_cache(maxsize=4)
def _engine(path: str) -> Engine:
    return create_database_engine(Path(path))


def default_engine() -> Engine:
    return _engine(str(get_settings().data_root / "spx.sqlite"))


def settings_use_default_database(settings: NotificationSettings) -> bool:
    configured = settings.notification_database_path
    return not configured or Path(configured) == get_settings().data_root / "spx.sqlite"


def engine_for_settings(settings: NotificationSettings) -> Engine:
    """Use the configured operational database, including isolated test stores."""

    configured_path = (
        Path(settings.notification_database_path)
        if settings.notification_database_path
        else None
    )
    if configured_path is not None and not settings_use_default_database(settings):
        isolated = _engine(str(configured_path))
        metadata.create_all(isolated)
        return isolated
    return default_engine()


def transport_lane(envelope: NotificationEnvelope) -> str:
    return "ops" if envelope.lane == "ops_transition" else "trade"


def freeze_route(
    settings: NotificationSettings,
    envelope: NotificationEnvelope,
    *,
    friend: bool,
) -> tuple[NotificationEnvelope, tuple[str, ...]]:
    intended = tuple(
        sorted(
            delivery_target_names(
                settings,
                lane=transport_lane(envelope),
                friend=friend,
            )
        )
    )
    if settings.rust_trader_notification_owner and operator_notification_role(envelope):
        entries = tuple(settings.rust_operator_notification_target_map)
        mapping = {sink: (key, channel) for sink, key, channel in entries}
        if (
            not intended
            or len(mapping) != len(entries)
            or any(sink not in mapping for sink in intended)
        ):
            return envelope, ()
        return (
            replace(envelope, operator_targets=tuple(mapping[sink] for sink in intended)),
            ("rust_ingress",),
        )
    return (
        replace(
            envelope,
            operator_targets=(),
            operator_opportunity_id=None,
            operator_generation=0,
        ),
        intended,
    )


def notification_payload(
    envelope: NotificationEnvelope,
    *,
    title: str,
    text: str,
    friend: bool,
    feishu_text: str | None,
) -> dict[str, object]:
    return {
        "envelope": _envelope_to_dict(envelope),
        "title": title,
        "text": text,
        "friend": friend,
        "feishu_text": feishu_text,
    }


def notification_payload_fingerprint(payload: Mapping[str, object]) -> str:
    encoded = json.dumps(
        dict(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def enqueue_final_notification(
    settings: NotificationSettings,
    envelope: NotificationEnvelope,
    *,
    title: str,
    text: str,
    friend: bool = False,
    feishu_text: str | None = None,
    enqueued_at: datetime | None = None,
    engine: Engine | None = None,
    schedule: Callable[[int], object] | None = None,
) -> UnifiedEnqueueResult:
    envelope.validate()
    at = enqueued_at or datetime.now(tz=timezone.utc)
    routed, targets = freeze_route(settings, envelope, friend=friend)
    if not targets:
        return UnifiedEnqueueResult(routed, (), (), "no_sink", 0, 0, False)
    payload = notification_payload(
        routed,
        title=title,
        text=text,
        friend=friend,
        feishu_text=feishu_text,
    )
    batch = enqueue(
        engine or default_engine(),
        NotificationDraft(
            logical_event_id=routed.event_id,
            source=routed.source,
            kind=routed.kind,
            lane=routed.lane,
            payload=payload,
            channels=targets,
            expires_at=routed.expires_at,
        ),
        now=at,
    )
    if batch.cancelled:
        return UnifiedEnqueueResult(routed, targets, (), "cancelled_before_enqueue", 0, 0, False)
    if schedule is None:
        from spx_spark.infrastructure.jobs import deliver_notification_event

        schedule = deliver_notification_event
    for event_id in batch.event_ids:
        schedule(event_id)
    return UnifiedEnqueueResult(
        routed,
        targets,
        batch.event_ids,
        "pending",
        batch.inserted,
        batch.duplicate,
        True,
    )


def enqueue_frozen_notification(
    envelope: NotificationEnvelope,
    targets: Sequence[str],
    *,
    title: str,
    text: str,
    friend: bool,
    feishu_text: str | None,
    enqueued_at: datetime,
    engine: Engine,
    schedule: Callable[[int], object],
) -> UnifiedEnqueueResult:
    envelope.validate()
    normalized = tuple(sorted(dict.fromkeys(str(target) for target in targets)))
    if not normalized:
        return UnifiedEnqueueResult(envelope, (), (), "no_sink", 0, 0, False)
    batch = enqueue(
        engine,
        NotificationDraft(
            logical_event_id=envelope.event_id,
            source=envelope.source,
            kind=envelope.kind,
            lane=envelope.lane,
            payload=notification_payload(
                envelope,
                title=title,
                text=text,
                friend=friend,
                feishu_text=feishu_text,
            ),
            channels=normalized,
            expires_at=envelope.expires_at,
        ),
        now=enqueued_at,
    )
    if batch.cancelled:
        return UnifiedEnqueueResult(
            envelope, normalized, (), "cancelled_before_enqueue", 0, 0, False
        )
    for event_id in batch.event_ids:
        schedule(event_id)
    return UnifiedEnqueueResult(
        envelope,
        normalized,
        batch.event_ids,
        "pending",
        batch.inserted,
        batch.duplicate,
        True,
    )


def inspect_final_notification(
    engine: Engine,
    envelope: NotificationEnvelope,
    *,
    title: str,
    text: str,
    friend: bool,
    feishu_text: str | None,
    targets: Sequence[str],
    expected_payload_fingerprint: str | None = None,
) -> DeliveryEventInspection:
    expected_targets = tuple(sorted(dict.fromkeys(str(target) for target in targets)))
    rows = tuple(event_rows(engine, envelope.event_id))
    cancellation = next(
        (row for row in rows if row["channel"] == CANCELLATION_CHANNEL),
        None,
    )
    delivery_rows = tuple(row for row in rows if row["channel"] != CANCELLATION_CHANNEL)
    target_statuses = tuple(
        (str(row["channel"]), str(row["status"])) for row in delivery_rows
    )
    if not delivery_rows:
        return DeliveryEventInspection(
            event_id=envelope.event_id,
            exists=False,
            cancelled=cancellation is not None,
            payload_matches=False,
            targets_match=not expected_targets,
            event_status=None,
            target_statuses=(),
            reason="cancelled" if cancellation is not None else "missing",
        )
    expected_payload = notification_payload(
        envelope,
        title=title,
        text=text,
        friend=friend,
        feishu_text=feishu_text,
    )
    expected_fingerprint = expected_payload_fingerprint or notification_payload_fingerprint(
        expected_payload
    )
    payload_matches = all(
        str(row["payload_sha256"]) == expected_fingerprint for row in delivery_rows
    )
    targets_match = tuple(channel for channel, _status in target_statuses) == expected_targets
    statuses = tuple(status for _channel, status in target_statuses)
    event_status = _aggregate_status(statuses)
    if cancellation is not None:
        reason = "cancelled"
    elif not payload_matches:
        reason = "payload_mismatch"
    elif not targets_match:
        reason = "target_mismatch"
    elif any(
        status in {NotificationStatus.FAILED.value, NotificationStatus.UNCERTAIN.value}
        for status in statuses
    ):
        reason = "terminal_or_invalid_status"
    else:
        reason = "accepted"
    return DeliveryEventInspection(
        event_id=envelope.event_id,
        exists=True,
        cancelled=cancellation is not None,
        payload_matches=payload_matches,
        targets_match=targets_match,
        event_status=event_status,
        target_statuses=target_statuses,
        reason=reason,
    )


def notification_exists(engine: Engine, event_id: str) -> bool:
    return bool(event_rows(engine, event_id))


def cancel_final_notification(
    engine: Engine,
    event_id: str,
    *,
    reason: str,
    now: datetime,
) -> int:
    return cancel(engine, event_id, reason=reason, now=now)


def frozen_route_for_event(
    engine: Engine,
    event_id: str,
) -> tuple[tuple[str, ...], tuple[tuple[str, str], ...]]:
    rows = tuple(
        row for row in event_rows(engine, event_id) if row["channel"] != CANCELLATION_CHANNEL
    )
    if not rows:
        return (), ()
    channels = tuple(sorted(str(row["channel"]) for row in rows))
    payload = json.loads(str(rows[0]["payload_json"]))
    envelope = _envelope_from_dict(_mapping(payload.get("envelope")))
    return channels, envelope.operator_targets


def recover_notification_tasks(
    engine: Engine,
    *,
    schedule: Callable[[int], object],
    now: datetime,
) -> tuple[int, int]:
    recovery = recover_incomplete_attempts(engine, now=now)
    for event_id in recovery.retry_event_ids:
        schedule(event_id)
    return len(recovery.retry_event_ids), len(recovery.uncertain_event_ids)


def _aggregate_status(statuses: Sequence[str]) -> str | None:
    if not statuses:
        return None
    for status in (
        NotificationStatus.UNCERTAIN.value,
        NotificationStatus.FAILED.value,
        NotificationStatus.PROCESSING.value,
        NotificationStatus.PENDING.value,
        NotificationStatus.DELIVERED.value,
    ):
        if status in statuses:
            return status
    return "invalid"


def deliver_notification_event(
    event_id: int,
    *,
    settings: NotificationSettings | None = None,
    engine: Engine | None = None,
    direct_deliver: Callable[..., list[SinkResult]] = deliver_trade_push,
    rust_deliver: Callable[..., SinkResult] = deliver_operator_template,
    now: datetime | None = None,
) -> UnifiedDeliveryResult:
    store = engine or default_engine()
    at = now or datetime.now(tz=timezone.utc)
    attempt = begin_attempt(
        store,
        event_id,
        max_attempts=MAX_DELIVERY_ATTEMPTS,
        now=at,
    )
    if attempt is None:
        return UnifiedDeliveryResult(event_id, None, "not_due", False, False)
    payload = attempt.payload
    if attempt.channel == "alert_pipeline":
        return _deliver_alert_pipeline(
            store,
            attempt,
            payload,
            settings=settings,
            now=at,
        )
    envelope = _envelope_from_dict(_mapping(payload.get("envelope")))
    if quiet_window_suppresses(envelope, now=at):
        settle(
            store,
            attempt.attempt_id,
            status=NotificationStatus.DELIVERED,
            outcome="quiet_window_suppressed",
            ok=True,
            now=at,
        )
        return UnifiedDeliveryResult(
            event_id, attempt.channel, "quiet_window_suppressed", False, True
        )
    notification_settings = settings or NotificationSettings.from_env()
    mark_transport_started(store, attempt.attempt_id)
    try:
        sink = _deliver_one(
            notification_settings,
            envelope,
            attempt.channel,
            payload,
            direct_deliver=direct_deliver,
            rust_deliver=rust_deliver,
        )
    except Exception as exc:  # noqa: BLE001 - transport may already have sent
        detail = f"{type(exc).__name__}:{exc}"
        settle(
            store,
            attempt.attempt_id,
            status=NotificationStatus.UNCERTAIN,
            outcome="transport_outcome_uncertain",
            ok=False,
            error_code="transport_exception",
            error_detail=detail,
            now=at,
        )
        return UnifiedDeliveryResult(event_id, attempt.channel, "uncertain", True, False)
    return _settle_sink(store, attempt, sink, now=at)


def _deliver_alert_pipeline(
    store: Engine,
    attempt,
    payload: Mapping[str, object],
    *,
    settings: NotificationSettings | None,
    now: datetime,
) -> UnifiedDeliveryResult:
    from spx_spark.notifier.alert_candidate_delivery import (
        make_deliver_alert_candidate,
    )

    event = _domain_event_from_dict(_mapping(payload.get("domain_event")))
    try:
        disposition = make_deliver_alert_candidate(
            settings or NotificationSettings.from_env()
        )(event)
    except Exception as exc:
        outcome = "alert_review_failed"
        settle(
            store,
            attempt.attempt_id,
            status=NotificationStatus.FAILED,
            outcome=outcome,
            ok=False,
            error_code=outcome,
            error_detail=f"{type(exc).__name__}:{exc}",
            now=now,
        )
        if attempt.attempt_no < MAX_DELIVERY_ATTEMPTS:
            raise RetryableDeliveryError(str(exc)) from exc
        return UnifiedDeliveryResult(attempt.event_id, attempt.channel, outcome, False, False)
    if disposition.settled:
        outcome = f"alert_{disposition.outcome}"
        settle(
            store,
            attempt.attempt_id,
            status=NotificationStatus.DELIVERED,
            outcome=outcome,
            ok=True,
            now=now,
        )
        return UnifiedDeliveryResult(attempt.event_id, attempt.channel, outcome, False, True)
    settle(
        store,
        attempt.attempt_id,
        status=NotificationStatus.FAILED,
        outcome="alert_review_retryable",
        ok=False,
        error_code="alert_review_retryable",
        now=now,
    )
    if attempt.attempt_no < MAX_DELIVERY_ATTEMPTS:
        raise RetryableDeliveryError("alert candidate review did not settle")
    return UnifiedDeliveryResult(
        attempt.event_id,
        attempt.channel,
        "alert_review_retryable",
        False,
        False,
    )


def _deliver_one(
    settings: NotificationSettings,
    envelope: NotificationEnvelope,
    channel: str,
    payload: Mapping[str, object],
    *,
    direct_deliver: Callable[..., list[SinkResult]],
    rust_deliver: Callable[..., SinkResult],
) -> SinkResult:
    title = str(payload.get("title") or "")
    text = str(payload.get("text") or "")
    if channel == "rust_ingress":
        return rust_deliver(settings, envelope=envelope, title=title, text=text)
    sinks = direct_deliver(
        settings,
        title=title,
        text=text,
        kind=envelope.kind,
        lane=transport_lane(envelope),
        friend=bool(payload.get("friend")),
        feishu_text=(
            str(payload["feishu_text"]) if payload.get("feishu_text") is not None else None
        ),
        targets=frozenset({channel}),
    )
    return next(
        (sink for sink in sinks if sink.sink == channel),
        SinkResult(channel, attempted=False, ok=False, error="frozen target unavailable"),
    )


def _settle_sink(store: Engine, attempt, sink: SinkResult, *, now: datetime):
    if sink.ok:
        status = NotificationStatus.DELIVERED
        outcome = "forwarded_to_rust" if sink.sink == "rust_ingress" else "delivered"
    elif sink.permanent or not sink.attempted:
        status = NotificationStatus.FAILED
        outcome = "permanent_failure"
    elif _explicit_retryable(sink):
        status = NotificationStatus.FAILED
        outcome = "retryable_failure"
    else:
        status = NotificationStatus.UNCERTAIN
        outcome = "transport_outcome_uncertain"
    settle(
        store,
        attempt.attempt_id,
        status=status,
        outcome=outcome,
        ok=sink.ok,
        error_code=None if sink.ok else outcome,
        error_detail=sink.error,
        now=now,
    )
    if outcome == "retryable_failure" and attempt.attempt_no < MAX_DELIVERY_ATTEMPTS:
        raise RetryableDeliveryError(sink.error or "retryable notification failure")
    return UnifiedDeliveryResult(
        attempt.event_id,
        attempt.channel,
        outcome,
        sink.attempted,
        sink.ok,
    )


def _explicit_retryable(sink: SinkResult) -> bool:
    error = sink.error or ""
    return error.startswith(("bark response code=", "feishu response code=")) or error == (
        "rust_ingress_server_busy"
    )


def _mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError("notification payload envelope is missing")
    return value


def _envelope_to_dict(envelope: NotificationEnvelope) -> dict[str, object]:
    return {
        "event_id": envelope.event_id,
        "source": envelope.source,
        "kind": envelope.kind,
        "lane": envelope.lane,
        "occurred_at": envelope.occurred_at.isoformat(),
        "expires_at": envelope.expires_at.isoformat() if envelope.expires_at else None,
        "operator_targets": [list(target) for target in envelope.operator_targets],
        "operator_opportunity_id": envelope.operator_opportunity_id,
        "operator_generation": envelope.operator_generation,
    }


def _envelope_from_dict(value: Mapping[str, object]) -> NotificationEnvelope:
    expires_at = value.get("expires_at")
    envelope = NotificationEnvelope(
        event_id=str(value["event_id"]),
        source=str(value["source"]),
        kind=str(value["kind"]),
        lane=str(value["lane"]),
        occurred_at=datetime.fromisoformat(str(value["occurred_at"])),
        expires_at=datetime.fromisoformat(str(expires_at)) if expires_at else None,
        operator_targets=tuple(
            (str(target[0]), str(target[1]))
            for target in value.get("operator_targets", [])
            if isinstance(target, list) and len(target) == 2
        ),
        operator_opportunity_id=(
            str(value["operator_opportunity_id"])
            if value.get("operator_opportunity_id") is not None
            else None
        ),
        operator_generation=int(value.get("operator_generation") or 0),
    )
    envelope.validate()
    return envelope


def _domain_event_from_dict(value: Mapping[str, object]) -> DomainEvent:
    payload = value.get("payload")
    if not isinstance(payload, Mapping):
        raise ValueError("alert candidate payload is missing")
    return DomainEvent(
        schema_version=int(value["schema_version"]),
        event_id=str(value["event_id"]),
        kind=EventKind(str(value["kind"])),
        source_at=datetime.fromisoformat(str(value["source_at"])),
        available_at=datetime.fromisoformat(str(value["available_at"])),
        aggregate_id=str(value["aggregate_id"]),
        sequence=int(value["sequence"]),
        payload=dict(payload),
    )
