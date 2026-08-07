"""Unified notification enqueue and single-target Huey delivery."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Callable, Mapping

from sqlalchemy import Engine

from spx_spark.app_settings import get_settings
from spx_spark.config import NotificationSettings
from spx_spark.infrastructure.notifications import (
    NotificationDraft,
    NotificationStatus,
    begin_attempt,
    create_engine,
    enqueue,
    mark_transport_started,
    settle,
)
from spx_spark.notifier.human_policy import quiet_window_suppresses
from spx_spark.notifier.model import SinkResult
from spx_spark.notifier.receipts import NotificationEnvelope
from spx_spark.notifier.rust_ingress import (
    deliver_operator_template,
    operator_notification_role,
)
from spx_spark.notifier.sinks import deliver_trade_push, delivery_target_names


MAX_DELIVERY_ATTEMPTS = 3


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
    return create_engine(Path(path))


def default_engine() -> Engine:
    root = get_settings().data_root
    return _engine(str(root))


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
    payload = {
        "envelope": _envelope_to_dict(routed),
        "title": title,
        "text": text,
        "friend": friend,
        "feishu_text": feishu_text,
    }
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
