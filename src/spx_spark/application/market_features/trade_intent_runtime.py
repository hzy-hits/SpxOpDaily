"""Persistence and human delivery for deterministic trade-ready intents."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Mapping

from spx_spark.application.order_map.execution_quote import evaluate_execution_quote
from spx_spark.application.order_map.pricing import round_to_tick
from spx_spark.application.market_features.trade_intent import (
    live_trade_intent_authority_issues,
)
from spx_spark.application.market_features.trade_intent_delivery_contract import (
    acknowledge_trade_intent_enqueue as _acknowledge_trade_intent_enqueue,
    apply_trade_intent_delivery_result as apply_trade_intent_delivery_result,
    delivery_coordinate_reason as _delivery_coordinate_reason,
    delivery_projection as _delivery_projection,
    persist_delivery_projection as _persist_delivery_projection,
    reconciled_delivery_result as _reconciled_delivery_result,
    record_action_revalidation as _record_action_revalidation,
    release_delivery_lease as _release_delivery_lease,
)
from spx_spark.application.market_features.trade_intent_runtime_support import (
    DELIVERY_LEASE_SECONDS as DELIVERY_LEASE_SECONDS,
    DELIVERY_LEASE_TTL_FRACTION as DELIVERY_LEASE_TTL_FRACTION,
    TRADE_INTENT_SYSTEM_PROMPT as TRADE_INTENT_SYSTEM_PROMPT,
    _accepted_events as _accepted_events,
    _append_jsonl as _append_jsonl,
    _audit_path as _audit_path,
    _datetime as _datetime,
    _delivery_lease as _delivery_lease,
    _delivery_lease_seconds as _delivery_lease_seconds,
    _fmt as _fmt,
    _fmt_fixed as _fmt_fixed,
    _intent_occurred_at as _intent_occurred_at,
    _latest_path as _latest_path,
    _lease_is_live as _lease_is_live,
    _mark_terminal_deliveries as _mark_terminal_deliveries,
    _manual_card_contract_reason as _manual_card_contract_reason,
    _number as _number,
    _opportunity_dedupe_key as _opportunity_dedupe_key,
    _operator_explanation as _operator_explanation,
    _operator_invalidation as _operator_invalidation,
    _operator_trigger as _operator_trigger,
    _signature as _signature,
    _state_path as _state_path,
    _trade_ready_delivery_event_id as _trade_ready_delivery_event_id,
    _utc as _utc,
    _writer_output_valid as _writer_output_valid,
    _writer_prompt as _writer_prompt,
    render_trade_intent as render_trade_intent,
)
from spx_spark.config import NotificationSettings, StorageSettings
from spx_spark.notifier.dispatcher import (
    cancel_pending_notification,
    enqueue_notification,
    notification_event_contract,
    inspect_notification_event,
)
from spx_spark.notifier.model import CommandRunner, default_runner
from spx_spark.notifier.operator_contract import (
    operator_generation,
    operator_opportunity_id,
)
from spx_spark.notifier.operator_cards import (
    parse_time,
)
from spx_spark.notifier.receipts import NotificationEnvelope
from spx_spark.marketdata import Provider, choose_best_quote, instrument_matches_id
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.settings.order_map import DEFAULT_ORDER_MAP_POLICY, OrderMapPolicy
from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock, read_json_object
from spx_spark.storage import LatestStateStore, configured_quote_use_decision
from spx_spark.strategy_contract import (
    STRATEGY_EVENT_SCHEMA_VERSION,
    actionable_strategy_contract_issues,
)


TERMINAL_PHASES = frozenset({"expired", "invalidated"})


def process_trade_intent(
    storage: StorageSettings,
    intent: Mapping[str, object],
    *,
    now: datetime,
    settings: NotificationSettings | None = None,
    feature_policy: MarketFeatureSettings | None = None,
    order_policy: OrderMapPolicy | None = None,
    expected_policy_version: str | None = None,
    action_now: datetime | None = None,
    runner: CommandRunner = default_runner,
) -> dict[str, object]:
    """Record every material gate result and deliver each ready event at most once."""

    now = _utc(now)
    state_path = _state_path(storage)
    latest_path = _latest_path(storage)
    intent_id = str(intent.get("intent_id") or "")
    ready = intent.get("status") == "trade_ready"
    delivery_event_id = _trade_ready_delivery_event_id(intent) if ready else ""
    notification = settings or NotificationSettings.from_env()
    outbox_configured = bool(
        getattr(notification, "delivery_outbox_enabled", False)
        and getattr(notification, "delivery_outbox_path", "")
    )
    expiry_reason = (
        _ready_contract_reason(
            intent,
            now=now,
            expected_policy_version=expected_policy_version,
        )
        if ready
        else None
    )
    event_occurred_at = _intent_occurred_at(intent) if ready else None
    prepared_text = render_trade_intent(intent) if ready else ""
    prepared_envelope = (
        NotificationEnvelope(
            event_id=delivery_event_id,
            source="trade_intent",
            kind="trade_intent",
            lane="trade_ready",
            occurred_at=event_occurred_at,
            expires_at=parse_time(intent.get("valid_until") or intent.get("expires_at")),
            operator_opportunity_id=operator_opportunity_id(
                intent, "event_id", fallback=intent_id
            ),
            operator_generation=operator_generation(intent),
        )
        if ready and delivery_event_id and event_occurred_at is not None
        else None
    )
    prepared_payload_fingerprint = ""
    prepared_targets: tuple[str, ...] = ()
    if prepared_envelope is not None and outbox_configured:
        prepared_payload_fingerprint, prepared_targets = notification_event_contract(
            notification,
            prepared_envelope,
            title="SPX TRADE READY",
            text=prepared_text,
            friend=True,
            feishu_text=prepared_text,
        )
    reconciliation = None
    reconciliation_fault_reason: str | None = None
    durable_event_exists = False
    with exclusive_state_lock(state_path):
        state = read_json_object(state_path)
        accepted = _accepted_events(state)
        semantic_keys = {
            str(key): str(value) for key, value in dict(state.get("semantic_keys") or {}).items()
        }
        semantic_dedupe_key = _opportunity_dedupe_key(intent)
        semantic_scope = str(intent.get("semantic_scope") or "")
        lifecycle_events = {
            str(item.get("event_id") or ""): {
                "semantic_key": str(item.get("semantic_key") or ""),
                "semantic_scope": str(item.get("semantic_scope") or ""),
                "payload_fingerprint": str(item.get("payload_fingerprint") or ""),
                "targets": tuple(
                    sorted(str(target) for target in item.get("targets") or () if target)
                )
                if isinstance(item.get("targets"), (list, tuple))
                else (),
            }
            for item in state.get("delivery_lifecycle_events") or []
            if isinstance(item, Mapping) and item.get("event_id")
        }
        cancellation_pending = {
            str(item) for item in state.get("pending_delivery_cancellation_event_ids") or [] if item
        }
        cancellation_reasons = {
            str(key): str(value)
            for key, value in dict(state.get("pending_delivery_cancellation_reasons") or {}).items()
            if key and value
        }
        cancellation_times = {
            str(key): parsed
            for key, value in dict(
                state.get("pending_delivery_cancellation_at") or {}
            ).items()
            if key and (parsed := _datetime(value)) is not None
        }
        for key in cancellation_pending:
            # Before this reason map existed, only invalidation could create a
            # pending lifecycle cancellation.
            cancellation_reasons.setdefault(
                key,
                "trade_intent_lifecycle_invalidated",
            )
        terminal_delivery_event_ids = {
            str(item) for item in state.get("terminal_delivery_event_ids") or [] if item
        }
        semantic_claimed = bool(semantic_dedupe_key) and semantic_dedupe_key in semantic_keys.values()
        if (
            ready
            and delivery_event_id
            and delivery_event_id not in terminal_delivery_event_ids
            and not semantic_claimed
        ):
            lifecycle_events.setdefault(
                delivery_event_id,
                {
                    "semantic_key": semantic_dedupe_key,
                    "semantic_scope": semantic_scope,
                    "payload_fingerprint": prepared_payload_fingerprint,
                    "targets": prepared_targets,
                },
            )
        inflight = {
            key: value
            for key, value in dict(state.get("inflight") or {}).items()
            if _lease_is_live(
                value,
                now=now,
                max_seconds=(
                    _delivery_lease_seconds(intent, now=now)
                    if key == delivery_event_id
                    else DELIVERY_LEASE_SECONDS
                ),
            )
        }
        delivery_lease_owned_elsewhere = bool(delivery_event_id and delivery_event_id in inflight)
        terminal_phase = str(intent.get("phase") or "")
        if terminal_phase in TERMINAL_PHASES:
            _mark_terminal_deliveries(
                terminal_phase,
                semantic_scope,
                semantic_keys,
                lifecycle_events,
                inflight,
                terminal_delivery_event_ids,
                cancellation_pending,
                cancellation_reasons,
            )
        for key in cancellation_pending:
            cancellation_times.setdefault(key, now)
        if cancellation_pending:
            state["pending_delivery_cancellation_event_ids"] = sorted(
                cancellation_pending
            )[-200:]
            state["pending_delivery_cancellation_reasons"] = {
                key: cancellation_reasons[key]
                for key in sorted(cancellation_pending)[-200:]
            }
            state["pending_delivery_cancellation_at"] = {
                key: cancellation_times[key].isoformat()
                for key in sorted(cancellation_pending)[-200:]
            }
            atomic_write_json_secure(state_path, state)
        for key in sorted(cancellation_pending):
            try:
                cancel_pending_notification(
                    notification,
                    key,
                    now=cancellation_times[key],
                    reason=cancellation_reasons.get(
                        key,
                        "trade_intent_lifecycle_terminal",
                    ),
                )
            except Exception:
                # The lifecycle record makes this cancellation replayable even
                # if the outbox enqueue succeeded before producer state-ack.
                continue
            cancellation_pending.discard(key)
            cancellation_reasons.pop(key, None)
            cancellation_times.pop(key, None)
            accepted.pop(key, None)
            lifecycle_events.pop(key, None)
            inflight.pop(key, None)
        if (
            ready
            and not expiry_reason
            and prepared_envelope is not None
            and outbox_configured
            and delivery_event_id not in terminal_delivery_event_ids
            and (not semantic_claimed or delivery_event_id in lifecycle_events)
        ):
            lifecycle = lifecycle_events[delivery_event_id]
            expected_payload_fingerprint = str(
                lifecycle.get("payload_fingerprint") or prepared_payload_fingerprint
            )
            stored_targets = lifecycle.get("targets")
            expected_targets = (
                tuple(str(target) for target in stored_targets)
                if isinstance(stored_targets, (list, tuple)) and stored_targets
                else prepared_targets
            )
            try:
                reconciliation = inspect_notification_event(
                    notification,
                    prepared_envelope,
                    title="SPX TRADE READY",
                    text=prepared_text,
                    friend=True,
                    feishu_text=prepared_text,
                    expected_payload_fingerprint=expected_payload_fingerprint,
                    expected_targets=expected_targets,
                )
            except Exception as error:
                # Persist the ready decision below before surfacing a transient
                # reconciliation fault; otherwise a short-lived signal can
                # vanish from both the outbox and the producer audit.
                reconciliation_fault_reason = (
                    f"outbox_reconciliation_exception:{type(error).__name__}"
                )
            else:
                durable_event_exists = reconciliation.acceptable
                if reconciliation.reason == "missing" and not delivery_lease_owned_elsewhere:
                    # No immutable outbox row exists yet, so the current
                    # decision snapshot becomes the contract for the enqueue
                    # that follows this state commit.
                    lifecycle["payload_fingerprint"] = prepared_payload_fingerprint
                    lifecycle["targets"] = prepared_targets
                elif reconciliation.acceptable:
                    # Backfill state written by a pre-contract runtime only
                    # after the current snapshot exactly matched the outbox.
                    lifecycle["payload_fingerprint"] = expected_payload_fingerprint
                    lifecycle["targets"] = expected_targets
        local_event_accepted = bool(
            delivery_event_id
            and (
                delivery_event_id in accepted
                or (
                    delivery_event_id in semantic_keys
                    and delivery_event_id not in terminal_delivery_event_ids
                )
            )
        )
        if ready and reconciliation is None and reconciliation_fault_reason is not None:
            state["last_delivery_reconciliation"] = {
                "event_id": delivery_event_id,
                "status": reconciliation_fault_reason,
                "action": "hard_fault",
                "reconciled_at": now.isoformat(),
            }
        if (
            ready
            and reconciliation is not None
            and reconciliation.reason not in {"accepted", "missing"}
        ):
            reconciliation_fault_reason = f"outbox_reconciliation_{reconciliation.reason}"
            state["last_delivery_reconciliation"] = {
                "event_id": delivery_event_id,
                "status": reconciliation_fault_reason,
                "action": "hard_fault",
                "event_status": reconciliation.event_status,
                "target_statuses": list(reconciliation.target_statuses),
                "reconciled_at": now.isoformat(),
            }
        if ready and local_event_accepted and not durable_event_exists:
            if (
                outbox_configured
                and reconciliation is not None
                and reconciliation.reason == "missing"
                and reconciliation_fault_reason is None
            ):
                # Local acceptance is only a projection of durable outbox
                # state. If the durable row disappeared (for example after an
                # outbox restore), remove the stale projection and retry the
                # exact immutable event while it is still valid.
                accepted.pop(delivery_event_id, None)
                semantic_keys.pop(delivery_event_id, None)
                semantic_claimed = bool(
                    semantic_dedupe_key and semantic_dedupe_key in semantic_keys.values()
                )
                state["last_delivery_reconciliation"] = {
                    "event_id": delivery_event_id,
                    "status": "local_accepted_outbox_missing",
                    "action": "retry",
                    "reconciled_at": now.isoformat(),
                }
            elif reconciliation_fault_reason is None:
                reconciliation_fault_reason = "accepted_outbox_reconciliation_unavailable"
                state["last_delivery_reconciliation"] = {
                    "event_id": delivery_event_id,
                    "status": reconciliation_fault_reason,
                    "action": "hard_fault",
                    "reconciled_at": now.isoformat(),
                }
        if durable_event_exists and delivery_event_id:
            if delivery_event_id not in accepted:
                state["last_delivery_reconciliation"] = {
                    "event_id": delivery_event_id,
                    "status": "outbox_event_local_acceptance_missing",
                    "action": "restore_local_acceptance",
                    "reconciled_at": now.isoformat(),
                }
            accepted.setdefault(delivery_event_id, now.isoformat())
            if semantic_dedupe_key:
                semantic_keys[delivery_event_id] = semantic_dedupe_key
        delivery_blocked_by_cancellation = bool(ready and cancellation_pending)
        duplicate = bool(
            delivery_event_id
            and (
                delivery_event_id in accepted
                or delivery_event_id in terminal_delivery_event_ids
                or semantic_claimed
            )
        )
        if durable_event_exists:
            inflight.pop(delivery_event_id, None)
        delivery_in_progress = bool(delivery_event_id and delivery_event_id in inflight)
        if (
            ready
            and not expiry_reason
            and delivery_event_id
            and not duplicate
            and not delivery_blocked_by_cancellation
            and not delivery_in_progress
            and reconciliation_fault_reason is None
        ):
            inflight[delivery_event_id] = _delivery_lease(intent, now=now)
        if ready:
            if durable_event_exists:
                persisted_intent = _delivery_projection(
                    intent,
                    delivery_event_id=delivery_event_id,
                    notification_status="outbox_accepted",
                    reason="outbox_event_reconciled",
                )
            elif expiry_reason:
                persisted_intent = _delivery_projection(
                    intent,
                    delivery_event_id=delivery_event_id,
                    notification_status="blocked",
                    reason=expiry_reason,
                )
            else:
                persisted_intent = _delivery_projection(
                    intent,
                    delivery_event_id=delivery_event_id,
                    notification_status="pending",
                    reason="awaiting_outbox_acceptance",
                )
        else:
            persisted_intent = dict(intent)
        persisted_signature = _signature(persisted_intent)
        atomic_write_json_secure(latest_path, persisted_intent)
        if persisted_signature != state.get("last_signature"):
            _append_jsonl(_audit_path(storage, now), persisted_intent)
        state.update(
            {
                "schema_version": 3,
                "last_signature": persisted_signature,
                "last_status": persisted_intent.get("status"),
                "last_event_id": intent.get("event_id"),
                "last_delivery_event_id": delivery_event_id or None,
                "updated_at": now.isoformat(),
                "accepted": accepted,
                "semantic_keys": semantic_keys,
                "inflight": inflight,
                "delivery_lifecycle_events": [
                    {
                        "event_id": event_id,
                        **lifecycle,
                    }
                    for event_id, lifecycle in sorted(lifecycle_events.items())[-200:]
                ],
                "pending_delivery_cancellation_event_ids": sorted(cancellation_pending)[-200:],
                "pending_delivery_cancellation_reasons": {
                    key: cancellation_reasons[key]
                    for key in sorted(cancellation_pending)[-200:]
                    if key in cancellation_reasons
                },
                "pending_delivery_cancellation_at": {
                    key: cancellation_times[key].isoformat()
                    for key in sorted(cancellation_pending)[-200:]
                    if key in cancellation_times
                },
                "terminal_delivery_event_ids": sorted(terminal_delivery_event_ids)[-200:],
            }
        )
        state.pop("delivered", None)
        atomic_write_json_secure(state_path, state)

    if intent.get("status") != "trade_ready":
        return {
            "attempted": False,
            "delivered": False,
            "reason": str(intent.get("status") or "observing"),
        }
    if expiry_reason:
        _persist_delivery_projection(
            storage,
            intent,
            now=now,
            delivery_event_id=delivery_event_id,
            reason=expiry_reason,
        )
        return {"attempted": False, "delivered": False, "reason": expiry_reason}
    if not intent_id:
        _persist_delivery_projection(
            storage,
            intent,
            now=now,
            delivery_event_id=delivery_event_id,
            reason="intent_id_unavailable",
        )
        return {"attempted": False, "delivered": False, "reason": "intent_id_unavailable"}
    if not delivery_event_id:
        _persist_delivery_projection(
            storage,
            intent,
            now=now,
            delivery_event_id=delivery_event_id,
            reason="notification_event_id_unavailable",
        )
        return {
            "attempted": False,
            "delivered": False,
            "reason": "notification_event_id_unavailable",
        }
    if delivery_blocked_by_cancellation:
        _persist_delivery_projection(
            storage,
            intent,
            now=now,
            delivery_event_id=delivery_event_id,
            reason="lifecycle_cancellation_pending",
        )
        return {
            "attempted": False,
            "delivered": False,
            "reason": "lifecycle_cancellation_pending",
        }
    if reconciliation_fault_reason:
        _persist_delivery_projection(
            storage,
            intent,
            now=now,
            delivery_event_id=delivery_event_id,
            reason=reconciliation_fault_reason,
        )
        return {
            "attempted": False,
            "delivered": False,
            "accepted": False,
            "reason": reconciliation_fault_reason,
        }
    if duplicate:
        if durable_event_exists:
            return _reconciled_delivery_result()
        _persist_delivery_projection(
            storage,
            intent,
            now=now,
            delivery_event_id=delivery_event_id,
            reason="accepted_outbox_reconciliation_unavailable",
        )
        return {"attempted": False, "delivered": False, "reason": "already_accepted"}
    if delivery_in_progress:
        projection_persisted = _persist_delivery_projection(
            storage,
            intent,
            now=now,
            delivery_event_id=delivery_event_id,
            reason="delivery_in_progress",
            preserve_accepted=True,
        )
        if not projection_persisted:
            return _reconciled_delivery_result()
        return {"attempted": False, "delivered": False, "reason": "delivery_in_progress"}

    if not getattr(notification, "enabled", True):
        _release_delivery_lease(state_path, delivery_event_id, now=now)
        _persist_delivery_projection(
            storage,
            intent,
            now=now,
            delivery_event_id=delivery_event_id,
            reason="notification_disabled",
        )
        return {"attempted": False, "delivered": False, "reason": "notification_disabled"}
    if not any(
        bool(getattr(notification, field, False))
        for field in ("feishu_enabled", "bark_enabled", "bark_friend_enabled")
    ):
        _release_delivery_lease(state_path, delivery_event_id, now=now)
        _persist_delivery_projection(
            storage,
            intent,
            now=now,
            delivery_event_id=delivery_event_id,
            reason="no_delivery_sink",
        )
        return {"attempted": False, "delivered": False, "reason": "no_delivery_sink"}
    # The producer path is deterministic and local.  Re-read the wall clock
    # and latest-state projection immediately before the durable enqueue.
    action_now = _utc(action_now or _action_now())
    action_reason, action_evidence = _action_revalidation(
        storage,
        intent,
        now=action_now,
        feature_policy=feature_policy,
        order_policy=order_policy,
        expected_policy_version=expected_policy_version,
    )
    if action_reason:
        _record_action_revalidation(
            state_path,
            delivery_event_id,
            now=action_now,
            evidence=action_evidence,
        )
        _release_delivery_lease(state_path, delivery_event_id, now=action_now)
        _persist_delivery_projection(
            storage,
            intent,
            now=action_now,
            delivery_event_id=delivery_event_id,
            reason=action_reason,
        )
        return {
            "attempted": False,
            "delivered": False,
            "accepted": False,
            "reason": action_reason,
            "action_revalidated_at": action_now.isoformat(),
        }
    card_reason = _manual_card_contract_reason(intent, now=action_now)
    if card_reason:
        action_evidence["reason"] = card_reason
        _record_action_revalidation(
            state_path,
            delivery_event_id,
            now=action_now,
            evidence=action_evidence,
        )
        _release_delivery_lease(state_path, delivery_event_id, now=action_now)
        _persist_delivery_projection(
            storage,
            intent,
            now=action_now,
            delivery_event_id=delivery_event_id,
            reason=card_reason,
        )
        return {
            "attempted": False,
            "delivered": False,
            "accepted": False,
            "reason": card_reason,
            "action_revalidated_at": action_now.isoformat(),
        }
    if event_occurred_at is None or prepared_envelope is None:
        action_evidence["reason"] = "intent_occurred_at_unavailable"
        _record_action_revalidation(
            state_path,
            delivery_event_id,
            now=action_now,
            evidence=action_evidence,
        )
        _release_delivery_lease(state_path, delivery_event_id, now=action_now)
        _persist_delivery_projection(
            storage,
            intent,
            now=action_now,
            delivery_event_id=delivery_event_id,
            reason="intent_occurred_at_unavailable",
        )
        return {
            "attempted": False,
            "delivered": False,
            "accepted": False,
            "reason": "intent_occurred_at_unavailable",
            "action_revalidated_at": action_now.isoformat(),
        }
    # The action-time quote is authoritative for the final gate and audit, but
    # the durable notification payload must remain the immutable decision
    # snapshot. Otherwise a crash between enqueue and state acknowledgement can
    # turn a harmless quote refresh into an event-id collision on replay.
    text = prepared_text
    enqueue_completed = False
    enqueue_exception_type: str | None = None
    try:
        enqueued = enqueue_notification(
            notification,
            prepared_envelope,
            title="SPX TRADE READY",
            text=text,
            friend=True,
            feishu_text=text,
            enqueued_at=action_now,
        )
        enqueue_completed = True
    except Exception as error:
        action_evidence["reason"] = "notification_enqueue_exception"
        enqueue_exception_type = type(error).__name__
        action_evidence["enqueue_exception_type"] = enqueue_exception_type
        _record_action_revalidation(
            state_path,
            delivery_event_id,
            now=action_now,
            evidence=action_evidence,
        )
    finally:
        # This also runs for KeyboardInterrupt/SystemExit. SIGKILL cannot run
        # cleanup, so the signal-bounded lease remains the final crash guard.
        if not enqueue_completed:
            _release_delivery_lease(state_path, delivery_event_id, now=action_now)
    if enqueue_exception_type is not None:
        _persist_delivery_projection(
            storage,
            intent,
            now=action_now,
            delivery_event_id=delivery_event_id,
            reason="notification_enqueue_exception",
        )
        return {
            "attempted": True,
            "accepted": False,
            "delivered": False,
            "reason": "notification_enqueue_exception",
            "enqueue_exception_type": enqueue_exception_type,
            "action_revalidated_at": action_now.isoformat(),
        }
    if enqueued.accepted:
        accepted_after_ack, acknowledgement_reason = _acknowledge_trade_intent_enqueue(
            storage,
            intent,
            now=action_now,
            delivery_event_id=delivery_event_id,
            notification=notification,
            envelope=prepared_envelope,
            text=text,
            payload_fingerprint=prepared_payload_fingerprint,
            targets=prepared_targets,
            action_evidence=action_evidence,
            outbox_configured=outbox_configured,
        )
        if not accepted_after_ack:
            return {
                "accepted": False,
                "attempted": True,
                "delivered": False,
                "reason": acknowledgement_reason,
                "action_revalidated_at": action_now.isoformat(),
            }
    else:
        _record_action_revalidation(
            state_path,
            delivery_event_id,
            now=action_now,
            evidence=action_evidence,
        )
        _release_delivery_lease(state_path, delivery_event_id, now=action_now)
        _persist_delivery_projection(
            storage,
            intent,
            now=action_now,
            delivery_event_id=delivery_event_id,
            reason=f"outbox_enqueue_{enqueued.outcome}",
        )
    return {
        "attempted": True,
        "accepted": enqueued.accepted,
        "inserted": enqueued.inserted,
        "duplicate": enqueued.duplicate,
        "delivered": enqueued.delivered,
        "queued": enqueued.queued_for_recovery,
        "outcome": enqueued.outcome,
        "writer": "template",
        "targets": list(enqueued.targets),
        "action_revalidated_at": action_now.isoformat(),
    }


def _ready_contract_reason(
    intent: Mapping[str, object],
    *,
    now: datetime,
    expected_policy_version: str | None = None,
) -> str | None:
    """Enforce the current v3 contract and explicit manual-alert authority."""

    schema_version = intent.get("schema_version")
    if schema_version == STRATEGY_EVENT_SCHEMA_VERSION:
        authority_issues = live_trade_intent_authority_issues(intent)
        if authority_issues:
            return authority_issues[0]
        issues = actionable_strategy_contract_issues(intent, now=now)
        if issues:
            if "strategy_event_expired" in issues:
                return "intent_expired"
            return issues[0]
        source_policy = str(intent.get("policy_version") or "")
        if not source_policy.startswith("rth_trade_intent.v3+sha256:"):
            return "source_policy_incompatible"
        if expected_policy_version and source_policy != expected_policy_version:
            return "source_policy_version_drift"
        coordinate_reason = _delivery_coordinate_reason(intent)
        if coordinate_reason:
            return coordinate_reason
        return None
    return "strategy_schema_unsupported"


def _action_revalidation(
    storage: StorageSettings,
    intent: Mapping[str, object],
    *,
    now: datetime,
    feature_policy: MarketFeatureSettings | None,
    expected_policy_version: str | None,
    order_policy: OrderMapPolicy | None = None,
) -> tuple[str | None, dict[str, object]]:
    """Fail closed at enqueue time and, in production, reload the market projection."""

    now = _utc(now)
    evidence: dict[str, object] = {
        "intent_id": intent.get("intent_id"),
        "decision_evaluated_at": intent.get("evaluated_at"),
        "action_revalidated_at": now.isoformat(),
        "expected_policy_version": expected_policy_version,
        "source_policy_version": intent.get("policy_version"),
    }
    reason = _ready_contract_reason(
        intent,
        now=now,
        expected_policy_version=expected_policy_version,
    )
    if reason:
        evidence["reason"] = reason
        return reason, evidence
    reason = _manual_card_contract_reason(intent, now=now)
    if reason:
        evidence["reason"] = reason
        return reason, evidence
    if feature_policy is None:
        reason = "action_feature_policy_unavailable"
        evidence["quote_revalidation"] = "blocked"
        evidence["reason"] = reason
        return reason, evidence

    latest = LatestStateStore(storage).load(now=now)
    evidence["quote_revalidation"] = "performed"
    evidence["quote_state_created_at"] = latest.created_at.isoformat()
    contract_id = str(intent.get("contract_id") or "")
    intent_provider = str(intent.get("provider") or "")
    provider = (
        Provider(intent_provider) if intent_provider in {item.value for item in Provider} else None
    )
    quote = (
        choose_best_quote(
            (
                item
                for item in latest.quotes
                if item.provider is provider and instrument_matches_id(item.instrument, contract_id)
            ),
            provider_priority=(provider,),
            as_of=now,
        )
        if contract_id and provider is not None
        else None
    )
    if quote is None:
        reason = "action_quote_unavailable"
        evidence["reason"] = reason
        return reason, evidence
    source_at = quote.quote_time
    transport_at = quote.last_update_at or quote.received_at
    bid = float(quote.bid) if isinstance(quote.bid, int | float) else None
    mid = float(quote.mid) if isinstance(quote.mid, int | float) else None
    ask = float(quote.ask) if isinstance(quote.ask, int | float) else None
    entry_limit = _number(intent.get("entry_limit"))
    entry_fraction = _number(intent.get("entry_spread_fraction"))
    evidence.update(
        {
            "contract_id": contract_id or None,
            "provider": quote.provider.value,
            "quote_source_at": source_at.isoformat() if source_at is not None else None,
            "quote_transport_at": transport_at.isoformat(),
            "bid": bid,
            "mid": mid,
            "ask": ask,
            "entry_limit": entry_limit,
        }
    )
    if not intent_provider:
        reason = "action_quote_provider_unavailable"
    elif intent_provider != quote.provider.value:
        reason = "action_quote_provider_mismatch"
    elif bid is None or mid is None or ask is None or not 0 <= bid <= mid <= ask:
        reason = "action_quote_nbbo_invalid"
    elif source_at is None:
        reason = "action_quote_source_time_unavailable"
    elif entry_limit is None or entry_limit <= 0:
        reason = "action_entry_limit_invalid"
    elif entry_fraction is None or not 0.0 <= entry_fraction <= 1.0:
        reason = "action_entry_rule_invalid"
    else:
        source_age = (now - _utc(source_at)).total_seconds()
        transport_age = (now - _utc(transport_at)).total_seconds()
        evidence["source_age_seconds"] = source_age
        evidence["transport_age_seconds"] = transport_age
        tolerance = max(0.0, feature_policy.provider_sync_tolerance_seconds)
        if source_age < -tolerance:
            reason = "action_quote_source_in_future"
        elif source_age > feature_policy.trade_quote_max_age_seconds:
            reason = "action_quote_source_stale"
        elif transport_age < -tolerance:
            reason = "action_quote_transport_in_future"
        elif transport_age > feature_policy.trade_quote_max_age_seconds:
            reason = "action_quote_transport_stale"
        else:
            use = configured_quote_use_decision(quote, as_of=now)
            evidence["quote_quality_reason"] = use.reason
            if not use.pricing_allowed:
                reason = "action_quote_not_pricing_allowed"
            else:
                execution_gate = evaluate_execution_quote(
                    quote,
                    latest.quotes,
                    as_of=now,
                    policy=order_policy or DEFAULT_ORDER_MAP_POLICY,
                )
                evidence["execution_quote_gate"] = execution_gate.to_dict()
                if not execution_gate.executable:
                    reason = f"action_execution_quote_{execution_gate.reasons[0]}"
                else:
                    action_limit = round_to_tick(bid + entry_fraction * (ask - bid))
                    evidence["recomputed_entry_limit"] = action_limit
                    # A TradeReady card is an immutable, manual decision
                    # snapshot, not an order priced from this later quote.  A
                    # normally moving live market must therefore remain
                    # deliverable while the action-time quote is still fresh
                    # and executable.  Preserve the original entry limit and
                    # risk on the card; retain the recomputed limit only as
                    # audit evidence.
                    evidence["entry_limit_changed"] = not math.isclose(
                        action_limit,
                        entry_limit,
                        abs_tol=1e-9,
                    )
                    evidence["entry_limit_policy"] = "immutable_decision_limit"
                    reason = None
    evidence["reason"] = reason
    return reason, evidence


def _action_now() -> datetime:
    return datetime.now(tz=timezone.utc)
