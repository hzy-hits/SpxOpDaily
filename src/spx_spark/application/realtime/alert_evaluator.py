"""Bridge alert_engine evaluation into RealtimeEngine DomainEvent candidates."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Mapping

from spx_spark.alert_engine import evaluate_payload
from spx_spark.domain.analytics import AnalyticsResult
from spx_spark.domain.events import DomainEvent, EventKind
from spx_spark.domain.market import MarketSnapshot
from spx_spark.notifier.policy import alert_key, context_only_alerts, is_human_visible_alert
from spx_spark.settings import AlertSettings, DEFAULT_ALERT_SETTINGS
from spx_spark.storage import LatestMarketProjectionStore, LatestState


def _utc(now: datetime) -> datetime:
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc)


def alert_batch_event_id(
    payload: Mapping[str, object],
    *,
    now: datetime,
    bucket_seconds: int = 300,
) -> str:
    """Deterministic id: content hash + cooldown-sized time bucket.

    The time bucket allows the same alert set to re-enter the outbox after the
    notifier cooldown window, while identical ticks within a bucket dedupe via
    outbox PRIMARY KEY.
    """

    alerts = payload.get("alerts") or ()
    keys = sorted(
        alert_key(item)
        for item in alerts
        if isinstance(item, dict)
    )
    digest = hashlib.sha256("\n".join(keys).encode("utf-8")).hexdigest()[:16]
    if bucket_seconds < 1:
        raise ValueError("bucket_seconds must be >= 1")
    bucket = int(_utc(now).timestamp()) // bucket_seconds
    return f"alert_candidate:{bucket}:{digest}"


def domain_events_from_payload(
    payload: Mapping[str, object],
    *,
    now: datetime,
    event_bucket_seconds: int = 300,
) -> tuple[DomainEvent, ...]:
    alerts = payload.get("alerts")
    if not isinstance(alerts, list) or not alerts:
        return ()
    as_of_raw = payload.get("as_of")
    if isinstance(as_of_raw, str) and as_of_raw.strip():
        source_at = datetime.fromisoformat(as_of_raw)
        if source_at.tzinfo is None:
            source_at = source_at.replace(tzinfo=timezone.utc)
    else:
        source_at = _utc(now)
    event_id = alert_batch_event_id(
        payload,
        now=source_at,
        bucket_seconds=event_bucket_seconds,
    )
    return (
        DomainEvent(
            schema_version=1,
            event_id=event_id,
            kind=EventKind.ALERT_CANDIDATE,
            source_at=_utc(source_at),
            available_at=_utc(now),
            aggregate_id="spx",
            sequence=0,
            payload=dict(payload),
        ),
    )


@dataclass
class AlertEngineEvaluator:
    """Produce ALERT_CANDIDATE DomainEvents from alert_engine.evaluate_payload.

    Loads LatestState from the projection store (not the thin MarketSnapshot) so
    options/IV/context evaluation matches the legacy alert_engine path.
    Persistence side-effects default off; the dedicated alert_engine task still
    owns gamma-regime / system-event persistence when it runs.
    """

    store: LatestMarketProjectionStore
    persist_system_events: bool = False
    persist_movement_state: bool = False
    persist_gamma_regime: bool = False
    alert_settings: AlertSettings | None = None
    event_bucket_seconds: int = 300

    def evaluate(
        self,
        snapshot: MarketSnapshot,
        analytics: AnalyticsResult | None,
        *,
        now: datetime,
    ) -> tuple[DomainEvent, ...]:
        _ = snapshot, analytics
        state = self.store.load(now=now)
        if not state.quotes and not state.best_quotes:
            return ()
        payload = evaluate_payload(
            state,
            now=now,
            persist_system_events=self.persist_system_events,
            persist_movement_state=self.persist_movement_state,
            persist_gamma_regime=self.persist_gamma_regime,
            alert_settings=self.alert_settings or DEFAULT_ALERT_SETTINGS,
        )
        alerts = payload.get("alerts")
        if isinstance(alerts, list):
            visible = [alert for alert in alerts if isinstance(alert, dict) and is_human_visible_alert(alert)]
            context = context_only_alerts(visible, payload)
            candidates = [alert for alert in visible if alert not in context]
            if not candidates:
                return ()
            payload = {
                **payload,
                "alerts": candidates,
                "alert_count": len(candidates),
            }
        return domain_events_from_payload(
            payload,
            now=now,
            event_bucket_seconds=self.event_bucket_seconds,
        )


def evaluate_state_to_events(
    state: LatestState,
    *,
    now: datetime | None = None,
    persist_system_events: bool = False,
    persist_movement_state: bool = False,
    persist_gamma_regime: bool = False,
    alert_settings: AlertSettings | None = None,
    event_bucket_seconds: int = 300,
) -> tuple[DomainEvent, ...]:
    """Test/helper entry that skips the projection store."""

    now = now or state.as_of
    payload = evaluate_payload(
        state,
        now=now,
        persist_system_events=persist_system_events,
        persist_movement_state=persist_movement_state,
        persist_gamma_regime=persist_gamma_regime,
        alert_settings=alert_settings or DEFAULT_ALERT_SETTINGS,
    )
    return domain_events_from_payload(
        payload,
        now=now,
        event_bucket_seconds=event_bucket_seconds,
    )
