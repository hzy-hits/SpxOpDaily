"""Durable virtual-strategy signal and notification state helpers."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Mapping

from spx_spark.application.market_features.virtual_strategy_support import _time, _utc
from spx_spark.config import NotificationSettings
from spx_spark.notifier.dispatcher import enqueue_notification
from spx_spark.notifier.receipts import NotificationEnvelope
from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock, read_json_object

_MAX_CONSUMED_SIGNALS = 200


def load_consumed_signals(
    state: Mapping[str, object],
) -> tuple[list[dict[str, str | None]], set[str]]:
    """Load v2 observations while preserving legacy list order during migration."""

    raw_observations = state.get("consumed_signals")
    raw_items = (
        raw_observations
        if isinstance(raw_observations, list)
        else state.get("consumed_signal_ids")
    )
    if not isinstance(raw_items, list):
        raw_items = []
    observations: list[dict[str, str | None]] = []
    consumed: set[str] = set()
    for item in raw_items:
        if isinstance(item, Mapping):
            signal_id = str(item.get("id") or item.get("signal_id") or "").strip()
            consumed_at_value = item.get("consumed_at")
            consumed_at = (
                str(consumed_at_value) if consumed_at_value is not None else None
            )
        else:
            signal_id = str(item).strip()
            consumed_at = None
        if not signal_id or signal_id in consumed:
            continue
        observations.append({"id": signal_id, "consumed_at": consumed_at})
        consumed.add(signal_id)
    observations = observations[-_MAX_CONSUMED_SIGNALS:]
    return observations, {str(item["id"]) for item in observations}


def mark_signal_consumed(
    observations: list[dict[str, str | None]],
    consumed: set[str],
    *,
    signal_id: str,
    now: datetime,
) -> None:
    signal_id = signal_id.strip()
    if not signal_id or signal_id in consumed:
        return
    observations.append({"id": signal_id, "consumed_at": _utc(now).isoformat()})
    consumed.add(signal_id)
    if len(observations) > _MAX_CONSUMED_SIGNALS:
        expired = observations.pop(0)
        consumed.discard(str(expired["id"]))


def consumed_signal_state(
    observations: list[dict[str, str | None]],
) -> dict[str, object]:
    bounded = observations[-_MAX_CONSUMED_SIGNALS:]
    return {
        "consumed_signals": bounded,
        "consumed_signal_ids": [str(item["id"]) for item in bounded],
    }


def flush_pending_notifications(
    state_path: Path,
    *,
    settings: NotificationSettings,
    now: datetime,
    only_event_id: str | None = None,
    enqueue=enqueue_notification,
) -> dict[str, object]:
    """Replay durable state intents into the idempotent notification outbox."""

    with exclusive_state_lock(state_path):
        state = read_json_object(state_path)
        pending = [
            dict(item)
            for item in state.get("pending_notifications") or []
            if isinstance(item, Mapping)
            and (
                only_event_id is None
                or str(item.get("event_id") or "") == only_event_id
            )
        ]
    accepted_ids: set[str] = set()
    expired_ids: set[str] = set()
    last_result: dict[str, object] = {"attempted": False, "accepted": False}
    for item in pending:
        event_id = str(item.get("event_id") or "")
        occurred_at = _time(item.get("occurred_at"))
        if not event_id or occurred_at is None:
            continue
        expires_at = _time(item.get("expires_at"))
        if expires_at is not None and expires_at <= now:
            expired_ids.add(event_id)
            last_result = {
                "attempted": False,
                "accepted": False,
                "event_id": event_id,
                "outcome": "expired_before_enqueue",
            }
            continue
        last_result = {"attempted": True, "accepted": False, "event_id": event_id}
        try:
            result = enqueue(
                settings,
                NotificationEnvelope(
                    event_id=event_id,
                    source=str(item.get("source") or "virtual_strategy"),
                    kind=str(item.get("kind") or "virtual_strategy_exit"),
                    lane=str(item.get("lane") or "strategy_lifecycle"),
                    occurred_at=occurred_at,
                    expires_at=expires_at,
                    operator_opportunity_id=(
                        str(item.get("operator_opportunity_id"))
                        if item.get("operator_opportunity_id")
                        else None
                    ),
                    operator_generation=_operator_generation(item),
                ),
                title=str(item.get("title") or "SPX VIRTUAL STRATEGY EXIT"),
                text=str(item.get("text") or ""),
                friend=item.get("friend") is True,
                feishu_text=str(item.get("feishu_text") or item.get("text") or ""),
                enqueued_at=_time(item.get("enqueued_at")) or now,
            )
        except Exception as exc:  # The durable pending intent remains replayable.
            last_result["outcome"] = f"enqueue_error:{type(exc).__name__}"
            continue
        last_result.update(
            {
                "accepted": result.accepted,
                "inserted": result.inserted,
                "duplicate": result.duplicate,
                # Quiet-window suppression is an accepted terminal policy
                # outcome, not a transport delivery.
                "delivered": result.delivered and result.outcome == "delivered",
                "suppressed": result.outcome == "quiet_window_suppressed",
                "queued_for_recovery": result.queued_for_recovery,
                "outcome": result.outcome,
                "targets": list(result.targets),
            }
        )
        if result.accepted:
            accepted_ids.add(event_id)
    settled_ids = accepted_ids | expired_ids
    if settled_ids:
        with exclusive_state_lock(state_path):
            state = read_json_object(state_path)
            state["pending_notifications"] = [
                item
                for item in state.get("pending_notifications") or []
                if not isinstance(item, Mapping)
                or str(item.get("event_id") or "") not in settled_ids
            ]
            accepted = {
                str(item)
                for item in state.get("accepted_notification_event_ids") or []
                if item
            }
            accepted.update(accepted_ids)
            state["accepted_notification_event_ids"] = sorted(accepted)[-200:]
            settled = {
                str(item)
                for item in state.get("settled_notification_event_ids") or []
                if item
            }
            settled.update(expired_ids)
            state["settled_notification_event_ids"] = sorted(settled)[-200:]
            atomic_write_json_secure(state_path, state)
    return last_result


def _operator_generation(value: Mapping[str, object]) -> int:
    generation = value.get("operator_generation", 0)
    if isinstance(generation, int) and not isinstance(generation, bool):
        return max(generation, 0)
    return 0
