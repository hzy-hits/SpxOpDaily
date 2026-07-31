"""Coordinate authority and truthful persistence for TradeReady delivery."""

from __future__ import annotations

import math
from datetime import datetime
from pathlib import Path
from typing import Mapping

from spx_spark.application.market_features.trade_intent_runtime_support import (
    _accepted_events,
    _append_jsonl,
    _audit_path,
    _latest_path,
    _number,
    _signature,
    _state_path,
    _trade_ready_delivery_event_id,
    _utc,
)
from spx_spark.config import NotificationSettings, StorageSettings
from spx_spark.ibkr.atm_reference import BASIS_MAX_ABS_POINTS
from spx_spark.notifier.dispatcher import inspect_notification_event
from spx_spark.notifier.receipts import NotificationEnvelope
from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock, read_json_object


TRANSIENT_DELIVERY_REASON_PREFIXES = (
    "action_quote_",
    "action_execution_quote_",
    "outbox_reconciliation_exception:",
)
TRANSIENT_DELIVERY_REASONS = frozenset(
    {
        "delivery_in_progress",
        "notification_enqueue_exception",
        "accepted_outbox_reconciliation_unavailable",
    }
)


def apply_trade_intent_delivery_result(
    intent: Mapping[str, object],
    delivery: Mapping[str, object],
) -> dict[str, object]:
    """Return the externally persisted status for one internal ready candidate."""

    if intent.get("status") != "trade_ready":
        return dict(intent)
    reason = str(delivery.get("reason") or delivery.get("outcome") or "unknown")
    accepted = delivery.get("accepted") is True
    return delivery_projection(
        intent,
        delivery_event_id=_trade_ready_delivery_event_id(intent),
        notification_status=(
            "outbox_accepted" if accepted else notification_status_for_reason(reason)
        ),
        reason=reason,
    )


def reconciled_delivery_result() -> dict[str, object]:
    return {
        "accepted": True,
        "attempted": False,
        "delivered": False,
        "duplicate": True,
        "inserted": False,
        "reason": "outbox_event_reconciled",
    }


def delivery_coordinate_reason(intent: Mapping[str, object]) -> str | None:
    """Authorize coherent official SPX or qualified RTH ES coordinates."""

    coordinate = intent.get("coordinate")
    if not isinstance(coordinate, Mapping):
        return "source_coordinate_unavailable"
    kind = str(coordinate.get("kind") or "")
    if kind == "official_spx":
        return None
    if kind != "es_equivalent":
        return "source_coordinate_mismatch"
    if coordinate.get("instrument_id") != "future:ES":
        return "source_es_coordinate_instrument_mismatch"

    basis = _number(coordinate.get("basis_points"))
    if basis is None or abs(basis) > BASIS_MAX_ABS_POINTS:
        return "source_es_coordinate_basis_invalid"
    fields_reason = _required_coordinate_fields_reason(
        coordinate,
        intent,
        prefix="source_es_coordinate",
    )
    if fields_reason:
        return fields_reason
    observed = float(coordinate["observed_value"])
    target = float(coordinate["target_value"])
    intent_spx_spot = float(intent["spx_spot"])
    intent_trigger = float(intent["trigger_level"])
    coordinate_spx_level = float(coordinate["spx_level"])
    if not math.isclose(observed - basis, intent_spx_spot, abs_tol=0.1):
        return "source_es_coordinate_spot_incoherent"
    if not math.isclose(target - basis, intent_trigger, abs_tol=0.1):
        return "source_es_coordinate_target_incoherent"
    if not math.isclose(coordinate_spx_level, intent_trigger, abs_tol=0.1):
        return "source_es_coordinate_level_incoherent"
    return None


def delivery_projection(
    intent: Mapping[str, object],
    *,
    delivery_event_id: str,
    notification_status: str,
    reason: str,
) -> dict[str, object]:
    """Expose TradeReady only after its immutable outbox event is durable."""

    projected = {
        **intent,
        "signal_status": str(intent.get("status") or ""),
        "notification_event_id": delivery_event_id or None,
        "notification_status": notification_status,
        "notification_reason": reason,
    }
    if notification_status == "outbox_accepted":
        projected["status"] = "trade_ready"
        return projected

    projected["status"] = (
        "ready_pending_delivery" if notification_status == "pending" else "delivery_blocked"
    )
    projected["execution_eligible"] = False
    block_reasons = [
        str(item) for item in intent.get("block_reasons") or () if isinstance(item, str) and item
    ]
    delivery_reason = f"notification:{reason}"
    if delivery_reason not in block_reasons:
        block_reasons.append(delivery_reason)
    projected["block_reasons"] = block_reasons
    return projected


def persist_delivery_projection(
    storage: StorageSettings,
    intent: Mapping[str, object],
    *,
    now: datetime,
    delivery_event_id: str,
    reason: str,
    preserve_accepted: bool = False,
) -> bool:
    """Persist a truthful non-formal status when no outbox event was accepted."""

    projected = delivery_projection(
        intent,
        delivery_event_id=delivery_event_id,
        notification_status=notification_status_for_reason(reason),
        reason=reason,
    )
    persisted_signature = _signature(projected)
    state_path = _state_path(storage)
    now = _utc(now)
    with exclusive_state_lock(state_path):
        state = read_json_object(state_path)
        if preserve_accepted and delivery_event_id in _accepted_events(state):
            return False
        atomic_write_json_secure(_latest_path(storage), projected)
        if persisted_signature != state.get("last_signature"):
            _append_jsonl(_audit_path(storage, now), projected)
        state["last_signature"] = persisted_signature
        state["last_status"] = projected["status"]
        state["last_delivery_event_id"] = delivery_event_id or None
        state["updated_at"] = now.isoformat()
        atomic_write_json_secure(state_path, state)
    return True


def acknowledge_trade_intent_enqueue(
    storage: StorageSettings,
    intent: Mapping[str, object],
    *,
    now: datetime,
    delivery_event_id: str,
    notification: NotificationSettings,
    envelope: NotificationEnvelope,
    text: str,
    payload_fingerprint: str,
    targets: tuple[str, ...],
    action_evidence: Mapping[str, object],
    outbox_configured: bool,
) -> tuple[bool, str]:
    """Atomically verify the lifecycle/outbox contract before exposing TradeReady."""

    now = _utc(now)
    state_path = _state_path(storage)
    with exclusive_state_lock(state_path):
        state = read_json_object(state_path)
        terminal = {str(item) for item in state.get("terminal_delivery_event_ids") or () if item}
        cancellation_pending = {
            str(item) for item in state.get("pending_delivery_cancellation_event_ids") or () if item
        }
        lifecycle = next(
            (
                item
                for item in state.get("delivery_lifecycle_events") or ()
                if isinstance(item, Mapping) and item.get("event_id") == delivery_event_id
            ),
            None,
        )
        reason: str | None = None
        if delivery_event_id in terminal:
            reason = "lifecycle_terminal_before_delivery_ack"
        elif delivery_event_id in cancellation_pending:
            reason = "lifecycle_cancellation_pending"
        elif lifecycle is None:
            reason = "lifecycle_inactive_before_delivery_ack"
        elif outbox_configured:
            frozen_fingerprint = str(lifecycle.get("payload_fingerprint") or "")
            frozen_targets = tuple(
                sorted(str(target) for target in lifecycle.get("targets") or () if target)
            )
            if frozen_fingerprint != payload_fingerprint or frozen_targets != targets:
                reason = "lifecycle_contract_changed_before_delivery_ack"
            else:
                try:
                    inspection = inspect_notification_event(
                        notification,
                        envelope,
                        title="SPX TRADE READY",
                        text=text,
                        friend=True,
                        feishu_text=text,
                        expected_payload_fingerprint=frozen_fingerprint,
                        expected_targets=frozen_targets,
                    )
                except Exception as error:
                    reason = f"outbox_reconciliation_exception:{type(error).__name__}"
                else:
                    if not inspection.acceptable:
                        reason = f"outbox_ack_{inspection.reason}"

        accepted = _accepted_events(state)
        inflight = dict(state.get("inflight") or {})
        inflight.pop(delivery_event_id, None)
        state["inflight"] = inflight
        state["last_action_revalidation"] = dict(action_evidence)
        if reason is None:
            accepted[delivery_event_id] = now.isoformat()
            if len(accepted) > 200:
                accepted = dict(sorted(accepted.items(), key=lambda item: item[1])[-200:])
            state["accepted"] = accepted
            state.pop("delivered", None)
            semantic_keys = {
                str(key): str(value)
                for key, value in dict(state.get("semantic_keys") or {}).items()
                if key in accepted
            }
            intent_id = str(intent.get("intent_id") or "")
            semantic_key = str(intent.get("semantic_key") or "") or (
                f"intent:{intent_id}" if intent_id else ""
            )
            if semantic_key:
                semantic_keys[delivery_event_id] = semantic_key
            state["semantic_keys"] = semantic_keys
            reason = "outbox_enqueue_accepted"
            notification_status = "outbox_accepted"
        else:
            state["last_delivery_reconciliation"] = {
                "event_id": delivery_event_id,
                "status": reason,
                "action": "reject_stale_enqueue_ack",
                "reconciled_at": now.isoformat(),
            }
            notification_status = notification_status_for_reason(reason)
        projected = delivery_projection(
            intent,
            delivery_event_id=delivery_event_id,
            notification_status=notification_status,
            reason=reason,
        )
        persisted_signature = _signature(projected)
        atomic_write_json_secure(_latest_path(storage), projected)
        if persisted_signature != state.get("last_signature"):
            _append_jsonl(_audit_path(storage, now), projected)
        state["last_signature"] = persisted_signature
        state["last_status"] = projected["status"]
        state["updated_at"] = now.isoformat()
        atomic_write_json_secure(state_path, state)
        return notification_status == "outbox_accepted", reason


def record_action_revalidation(
    state_path: Path,
    event_id: str,
    *,
    now: datetime,
    evidence: Mapping[str, object],
) -> None:
    with exclusive_state_lock(state_path):
        state = read_json_object(state_path)
        state["last_action_revalidation"] = dict(evidence)
        state["updated_at"] = _utc(now).isoformat()
        atomic_write_json_secure(state_path, state)


def release_delivery_lease(state_path: Path, event_id: str, *, now: datetime) -> None:
    with exclusive_state_lock(state_path):
        state = read_json_object(state_path)
        inflight = dict(state.get("inflight") or {})
        inflight.pop(event_id, None)
        state["inflight"] = inflight
        state["updated_at"] = _utc(now).isoformat()
        atomic_write_json_secure(state_path, state)


def notification_status_for_reason(reason: str) -> str:
    if reason in TRANSIENT_DELIVERY_REASONS or reason.startswith(
        TRANSIENT_DELIVERY_REASON_PREFIXES
    ):
        return "pending"
    return "blocked"


def _required_coordinate_fields_reason(
    coordinate: Mapping[str, object],
    intent: Mapping[str, object],
    *,
    prefix: str,
) -> str | None:
    values = (
        _number(coordinate.get("observed_value")),
        _number(coordinate.get("target_value")),
        _number(coordinate.get("spx_level")),
        _number(intent.get("spx_spot")),
        _number(intent.get("trigger_level")),
    )
    if any(value is None or value <= 0 for value in values):
        return f"{prefix}_fields_incomplete"
    return None
