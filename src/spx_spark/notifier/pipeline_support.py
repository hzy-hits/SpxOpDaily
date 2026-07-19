"""Small, side-effect-free helpers shared by the notification pipeline."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

from spx_spark.notifier.model import SinkResult
from spx_spark.notifier.policy import alert_key


def telemetry_alert_key(alert: dict[str, object]) -> str:
    return str(alert.get("decision_id") or alert_key(alert))


def scope_sink(
    sink: SinkResult,
    alerts: list[dict[str, object]],
    *,
    verdict: str | None = None,
) -> SinkResult:
    return replace(
        sink,
        alert_keys=tuple(dict.fromkeys(telemetry_alert_key(alert) for alert in alerts)),
        verdict=verdict or sink.verdict,
    )


def scope_sinks(
    sinks: list[SinkResult],
    alerts: list[dict[str, object]],
    *,
    verdict: str | None = None,
) -> list[SinkResult]:
    return [scope_sink(sink, alerts, verdict=verdict) for sink in sinks]


def stable_notification_time(
    payload: dict[str, object],
    alerts: list[dict[str, object]],
    *,
    fallback: datetime,
) -> datetime:
    """Choose semantic event time, not a retry/poll clock, for outbox identity."""

    def parsed_times(rows: tuple[dict[str, object], ...]) -> list[datetime]:
        candidates: list[datetime] = []
        for row in rows:
            for field in ("source_at", "occurred_at", "event_at", "as_of"):
                raw = row.get(field)
                if not isinstance(raw, str) or not raw.strip():
                    continue
                try:
                    parsed = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
                except ValueError:
                    continue
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                candidates.append(parsed.astimezone(timezone.utc))
                break
        return candidates

    candidates = parsed_times(tuple(alerts))
    if not candidates:
        candidates = parsed_times((payload,))
    return min(candidates) if candidates else fallback.astimezone(timezone.utc)


def successful_delivery_outcome(sinks: list[SinkResult]) -> str:
    return "queued" if any(sink.ok and sink.verdict == "queued" for sink in sinks) else "delivered"


def record_delivered_event_ids(
    alerts: list[dict[str, object]],
    acknowledged_event_ids: set[str],
) -> None:
    for alert in alerts:
        if alert.get("event_id"):
            acknowledged_event_ids.add(str(alert["event_id"]))
        # Shock and reclaim intentionally share one event id. Keep a
        # phase-specific acknowledgement so the monitor can recover if the
        # process exits between committing notifier state and monitor state.
        if str(alert.get("kind") or "") in {
            "intraday_price_shock",
            "intraday_price_reclaim",
            "flip_reclaim_call",
            "call_wall_breakout_call",
        } and alert.get("dedup_group"):
            acknowledged_event_ids.add(str(alert["dedup_group"]))
