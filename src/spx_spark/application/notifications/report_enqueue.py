"""Stable, producer-only enqueue boundary for scheduled reports."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from spx_spark.application.order_map.report_clock import RthReportSlot
from spx_spark.config import NotificationSettings
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.notifier.dispatcher import enqueue_notification
from spx_spark.notifier.model import NotificationEnvelope
from spx_spark.notifier.unified_delivery import notification_event_id


_DAILY_REPORT_KINDS = frozenset({"morning_map", "order_map", "post_close_review"})


@dataclass(frozen=True, slots=True)
class DailyReportSemantic:
    trading_date: date
    occurred_at: datetime
    identity: str
    event_id: str


@dataclass(frozen=True, slots=True)
class StatusReportSemantic:
    occurred_at: datetime
    identity: str
    event_id: str
    lane: str
    expires_at: datetime | None
    slot_key: str


def report_trading_date(payload: dict[str, Any], *, now: datetime) -> date:
    """Resolve the semantic trading date without using an invocation timestamp as identity."""

    for key in ("trading_date", "date"):
        raw = payload.get(key)
        if isinstance(raw, date) and not isinstance(raw, datetime):
            return raw
        if raw:
            try:
                return date.fromisoformat(str(raw)[:10])
            except ValueError:
                pass
    raw_as_of = payload.get("as_of")
    if raw_as_of:
        try:
            parsed = datetime.fromisoformat(str(raw_as_of).replace("Z", "+00:00"))
        except ValueError:
            parsed = None
        if parsed is not None and parsed.tzinfo is not None:
            return DEFAULT_MARKET_CALENDAR.research_expiry(parsed)
    return DEFAULT_MARKET_CALENDAR.research_expiry(now)


def canonical_daily_report_time(trading_date: date, *, kind: str) -> datetime:
    """Return one exchange-calendar anchor for a daily report's immutable identity."""

    if kind not in _DAILY_REPORT_KINDS:
        raise ValueError(f"unsupported daily report kind: {kind}")
    session = DEFAULT_MARKET_CALENDAR.session(trading_date)
    if session is None:
        # Forced historical/non-session runs still need a deterministic identity.
        return datetime.combine(trading_date, datetime.min.time(), tzinfo=timezone.utc)
    if kind == "morning_map":
        return session.open_at - timedelta(hours=1)
    if kind == "post_close_review":
        return session.close_at
    window = DEFAULT_MARKET_CALENDAR.spx_session_window(trading_date)
    assert window is not None
    return window.session_start


def daily_report_semantic(
    payload: dict[str, Any],
    *,
    now: datetime,
    kind: str,
    source: str,
    identity_label: str = "trading_date",
) -> DailyReportSemantic:
    """Build the exact immutable identity used by a daily report producer."""

    trading_date = report_trading_date(payload, now=now)
    occurred_at = canonical_daily_report_time(trading_date, kind=kind)
    identity = f"{identity_label}:{trading_date.isoformat()}"
    return DailyReportSemantic(
        trading_date=trading_date,
        occurred_at=occurred_at,
        identity=identity,
        event_id=notification_event_id(
            kind,
            source=source,
            occurred_at=occurred_at,
            identity=identity,
        ),
    )


def stable_report_slot(now: datetime, *, cadence_minutes: int) -> datetime:
    """Floor a timezone-aware invocation to a stable report cadence boundary."""

    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("report slot requires a timezone-aware timestamp")
    if cadence_minutes <= 0 or 60 % cadence_minutes != 0:
        raise ValueError("cadence_minutes must be a positive divisor of 60")
    return now.replace(
        minute=(now.minute // cadence_minutes) * cadence_minutes,
        second=0,
        microsecond=0,
    )


def material_report_identity(
    report: str,
    *,
    trading_date: str,
    occurred_at: datetime,
    fingerprint: dict[str, Any],
) -> str:
    """Identify one cadence slot plus its normalized material state."""

    normalized = json.dumps(
        fingerprint,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:20]
    slot = occurred_at.astimezone(timezone.utc).isoformat()
    return f"{report}:{trading_date}:{slot}:material:{digest}"


def order_map_status_semantic(
    *,
    trading_date: str,
    now: datetime,
    delivery_reason: str,
    current_rth_slot: RthReportSlot | None,
    fingerprint: dict[str, Any],
) -> StatusReportSemantic:
    """Build the exact immutable status identity before narrative generation."""

    if current_rth_slot is not None:
        occurred_at = current_rth_slot.slot_at
        slot_key = f"rth:{current_rth_slot.key}"
        identity = material_report_identity(
            "rth_status",
            trading_date=trading_date,
            occurred_at=occurred_at,
            fingerprint=fingerprint,
        )
    else:
        occurred_at = stable_report_slot(now, cadence_minutes=15)
        slot_key = f"gth:{trading_date}:{occurred_at.astimezone(timezone.utc).isoformat()}"
        identity = material_report_identity(
            "gth_status",
            trading_date=trading_date,
            occurred_at=occurred_at,
            fingerprint=fingerprint,
        )
    lane = "position_safety" if delivery_reason == "open_position_risk" else "scheduled_report"
    expires_at = None if lane == "position_safety" else occurred_at + timedelta(minutes=15)
    return StatusReportSemantic(
        occurred_at=occurred_at,
        identity=identity,
        event_id=notification_event_id(
            "status",
            source="order_map_status",
            occurred_at=occurred_at,
            identity=identity,
        ),
        lane=lane,
        expires_at=expires_at,
        slot_key=slot_key,
    )


def enqueue_report_notification(
    settings: NotificationSettings,
    *,
    source: str,
    kind: str,
    lane: str,
    occurred_at: datetime,
    identity: str,
    title: str,
    text: str,
    friend: bool = True,
    feishu_text: str | None = None,
    expires_at: datetime | None = None,
    enqueued_at: datetime,
) -> dict[str, Any]:
    """Persist a final report without claiming work or invoking a transport."""

    event_id = notification_event_id(
        kind,
        source=source,
        occurred_at=occurred_at,
        identity=identity,
    )
    result = enqueue_notification(
        settings,
        NotificationEnvelope(
            event_id=event_id,
            source=source,
            kind=kind,
            lane=lane,
            occurred_at=occurred_at,
            expires_at=expires_at,
        ),
        title=title,
        text=text,
        friend=friend,
        feishu_text=feishu_text,
        enqueued_at=enqueued_at,
    )
    # Only a replay that observes a fully delivered event may report delivery.
    # A newly queued event, a partial target result, and policy suppression remain false.
    delivered_ok = result.delivered and result.outcome == "delivered"
    queued = result.queued_for_recovery
    return {
        "notification_event_id": event_id,
        "accepted": result.accepted,
        "inserted": result.inserted,
        "duplicate": result.duplicate,
        "outcome": result.outcome,
        "delivery_outcome": result.outcome,
        "targets": list(result.targets),
        "queued": queued,
        "queued_for_recovery": queued,
        "delivered_ok": delivered_ok,
        # Producer-only callers cannot claim a per-sink terminal observation.
        "im_ok": False,
        "bark_ok": False,
        "feishu_ok": False,
    }


def enqueue_order_map_status(
    settings: NotificationSettings,
    *,
    text: str,
    feishu_text: str | None = None,
    title: str,
    trading_date: str,
    now: datetime,
    delivery_reason: str,
    current_rth_slot: RthReportSlot | None,
    fingerprint: dict[str, Any],
) -> dict[str, Any]:
    """Enqueue one RTH/GTH status using a deterministic 15-minute slot."""

    semantic = order_map_status_semantic(
        trading_date=trading_date,
        now=now,
        delivery_reason=delivery_reason,
        current_rth_slot=current_rth_slot,
        fingerprint=fingerprint,
    )
    result = enqueue_report_notification(
        settings,
        source="order_map_status",
        kind="status",
        lane=semantic.lane,
        occurred_at=semantic.occurred_at,
        identity=semantic.identity,
        title=title,
        text=text,
        friend=True,
        feishu_text=feishu_text or text,
        expires_at=semantic.expires_at,
        enqueued_at=now,
    )
    result["occurred_at"] = semantic.occurred_at.isoformat()
    result["report_slot_key"] = semantic.slot_key
    return result
