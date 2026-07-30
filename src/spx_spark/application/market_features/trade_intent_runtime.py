"""Persistence and human delivery for deterministic trade-ready intents."""

from __future__ import annotations

import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from spx_spark.application.order_map.execution_quote import evaluate_execution_quote
from spx_spark.application.order_map.pricing import round_to_tick
from spx_spark.application.market_features.trade_intent import (
    live_trade_intent_authority_issues,
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
    _number as _number,
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
from spx_spark.ibkr.atm_reference import BASIS_MAX_ABS_POINTS
from spx_spark.config import NotificationSettings, StorageSettings
from spx_spark.notifier.dispatcher import (
    cancel_pending_notification,
    enqueue_notification,
    notification_event_contract,
    inspect_notification_event,
)
from spx_spark.notifier.model import CommandRunner, default_runner
from spx_spark.notifier.operator_cards import (
    option_contract_right,
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


TERMINAL_REARM_PHASES = frozenset({"expired", "invalidated"})


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
    signature = _signature(intent)
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
        semantic_key = str(intent.get("semantic_key") or "")
        semantic_dedupe_key = semantic_key or (f"intent:{intent_id}" if intent_id else "")
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
        if ready and delivery_event_id and delivery_event_id not in terminal_delivery_event_ids:
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
        terminal_phase = str(intent.get("phase") or "")
        if terminal_phase in TERMINAL_REARM_PHASES:
            ending_delivery_event_ids = {
                key
                for key, value in semantic_keys.items()
                if not semantic_scope
                or value == semantic_scope
                or value.startswith(f"{semantic_scope}|")
            }
            ending_delivery_event_ids.update(
                event_id
                for event_id, lifecycle in lifecycle_events.items()
                if not semantic_scope
                or lifecycle["semantic_scope"] == semantic_scope
                or lifecycle["semantic_key"] == semantic_scope
                or lifecycle["semantic_key"].startswith(f"{semantic_scope}|")
            )
            # Migrate a v2 producer that crashed after enqueue but before it
            # could persist the v3 lifecycle registry.
            ending_delivery_event_ids.update(inflight)
            terminal_delivery_event_ids.update(ending_delivery_event_ids)
            cancellation_pending.update(ending_delivery_event_ids)
            cancellation_reasons.update(
                {
                    event_id: f"trade_intent_lifecycle_{terminal_phase}"
                    for event_id in ending_delivery_event_ids
                }
            )
        for key in sorted(cancellation_pending):
            try:
                cancel_pending_notification(
                    notification,
                    key,
                    now=now,
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
            accepted.pop(key, None)
            semantic_keys.pop(key, None)
            lifecycle_events.pop(key, None)
            inflight.pop(key, None)
        if (
            ready
            and not expiry_reason
            and prepared_envelope is not None
            and outbox_configured
            and delivery_event_id not in terminal_delivery_event_ids
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
                if reconciliation.reason == "missing":
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
            and (delivery_event_id in accepted or delivery_event_id in semantic_keys)
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
                or (semantic_dedupe_key and semantic_dedupe_key in semantic_keys.values())
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
        ):
            inflight[delivery_event_id] = _delivery_lease(intent, now=now)
        atomic_write_json_secure(latest_path, dict(intent))
        if signature != state.get("last_signature"):
            _append_jsonl(_audit_path(storage, now), dict(intent))
        state.update(
            {
                "schema_version": 3,
                "last_signature": signature,
                "last_status": intent.get("status"),
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
        return {"attempted": False, "delivered": False, "reason": expiry_reason}
    if not intent_id:
        return {"attempted": False, "delivered": False, "reason": "intent_id_unavailable"}
    if not delivery_event_id:
        return {
            "attempted": False,
            "delivered": False,
            "reason": "notification_event_id_unavailable",
        }
    if delivery_blocked_by_cancellation:
        return {
            "attempted": False,
            "delivered": False,
            "reason": "lifecycle_cancellation_pending",
        }
    if reconciliation_fault_reason:
        return {
            "attempted": False,
            "delivered": False,
            "accepted": False,
            "reason": reconciliation_fault_reason,
        }
    if duplicate:
        if durable_event_exists:
            return {
                "attempted": False,
                "delivered": False,
                "accepted": True,
                "inserted": False,
                "duplicate": True,
                "reason": "outbox_event_reconciled",
            }
        return {"attempted": False, "delivered": False, "reason": "already_accepted"}
    if delivery_in_progress:
        return {"attempted": False, "delivered": False, "reason": "delivery_in_progress"}

    if not getattr(notification, "enabled", True):
        _release_delivery_lease(state_path, delivery_event_id, now=now)
        return {"attempted": False, "delivered": False, "reason": "notification_disabled"}
    if not any(
        bool(getattr(notification, field, False))
        for field in ("feishu_enabled", "bark_enabled", "bark_friend_enabled")
    ):
        _release_delivery_lease(state_path, delivery_event_id, now=now)
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
        action_evidence["enqueue_exception_type"] = type(error).__name__
        _record_action_revalidation(
            state_path,
            delivery_event_id,
            now=action_now,
            evidence=action_evidence,
        )
        raise
    finally:
        # This also runs for KeyboardInterrupt/SystemExit. SIGKILL cannot run
        # cleanup, so the signal-bounded lease remains the final crash guard.
        if not enqueue_completed:
            _release_delivery_lease(
                state_path,
                delivery_event_id,
                now=action_now,
            )
    if enqueued.accepted:
        with exclusive_state_lock(state_path):
            state = read_json_object(state_path)
            accepted = _accepted_events(state)
            accepted[delivery_event_id] = action_now.isoformat()
            if len(accepted) > 200:
                accepted = dict(sorted(accepted.items(), key=lambda item: item[1])[-200:])
            state["accepted"] = accepted
            state.pop("delivered", None)
            semantic_keys = {
                str(key): str(value)
                for key, value in dict(state.get("semantic_keys") or {}).items()
                if key in accepted
            }
            semantic_key = str(intent.get("semantic_key") or "")
            semantic_dedupe_key = semantic_key or (f"intent:{intent_id}" if intent_id else "")
            if semantic_dedupe_key:
                semantic_keys[delivery_event_id] = semantic_dedupe_key
            state["semantic_keys"] = semantic_keys
            inflight = dict(state.get("inflight") or {})
            inflight.pop(delivery_event_id, None)
            state["inflight"] = inflight
            state["last_action_revalidation"] = action_evidence
            state["updated_at"] = action_now.isoformat()
            atomic_write_json_secure(state_path, state)
    else:
        _record_action_revalidation(
            state_path,
            delivery_event_id,
            now=action_now,
            evidence=action_evidence,
        )
        _release_delivery_lease(state_path, delivery_event_id, now=action_now)
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


def _delivery_coordinate_reason(intent: Mapping[str, object]) -> str | None:
    """Authorize cash SPX or one internally qualified ES-equivalent RTH lifecycle."""

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
    observed = _number(coordinate.get("observed_value"))
    target = _number(coordinate.get("target_value"))
    coordinate_spx_level = _number(coordinate.get("spx_level"))
    intent_spx_spot = _number(intent.get("spx_spot"))
    intent_trigger = _number(intent.get("trigger_level"))
    if basis is None or abs(basis) > BASIS_MAX_ABS_POINTS:
        return "source_es_coordinate_basis_invalid"
    if (
        observed is None
        or observed <= 0
        or target is None
        or target <= 0
        or coordinate_spx_level is None
        or coordinate_spx_level <= 0
        or intent_spx_spot is None
        or intent_spx_spot <= 0
        or intent_trigger is None
        or intent_trigger <= 0
    ):
        return "source_es_coordinate_fields_incomplete"
    if not math.isclose(observed - basis, intent_spx_spot, abs_tol=0.1):
        return "source_es_coordinate_spot_incoherent"
    if not math.isclose(target - basis, intent_trigger, abs_tol=0.1):
        return "source_es_coordinate_target_incoherent"
    if not math.isclose(coordinate_spx_level, intent_trigger, abs_tol=0.1):
        return "source_es_coordinate_level_incoherent"
    return None


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


def _manual_card_contract_reason(
    intent: Mapping[str, object],
    *,
    now: datetime,
) -> str | None:
    """Reject a green card unless every operator field is complete and coherent."""

    direction = str(intent.get("direction") or "")
    if direction not in {"up", "down"}:
        return "manual_card_direction_invalid"
    thesis = str(intent.get("thesis") or "")
    if thesis not in {"breakout", "fade"}:
        return "manual_card_thesis_invalid"
    right = option_contract_right(intent.get("contract_id"))
    expected_right = "C" if direction == "up" else "P"
    if right is None:
        return "manual_card_exact_contract_unavailable"
    if right != expected_right:
        return "manual_card_contract_direction_mismatch"

    numeric_fields = (
        "decision_bid",
        "decision_ask",
        "entry_limit",
        "trigger_level",
        "spx_spot",
        "invalidation_spx",
        "target_spx",
        "max_loss_per_contract",
    )
    values: dict[str, float] = {}
    for field in numeric_fields:
        value = _number(intent.get(field))
        if value is None:
            return f"manual_card_field_missing:{field}"
        values[field] = value
    bid = values["decision_bid"]
    ask = values["decision_ask"]
    entry = values["entry_limit"]
    if bid < 0 or ask <= 0 or bid > ask:
        return "manual_card_nbbo_invalid"
    if entry <= 0 or not bid <= entry <= ask:
        return "manual_card_entry_limit_outside_nbbo"
    if any(
        values[field] <= 0
        for field in ("trigger_level", "spx_spot", "invalidation_spx", "target_spx")
    ):
        return "manual_card_spx_coordinate_invalid"
    if values["max_loss_per_contract"] <= 0 or not math.isclose(
        values["max_loss_per_contract"],
        entry * 100.0,
        abs_tol=0.01,
    ):
        return "manual_card_max_loss_inconsistent"

    trigger = values["trigger_level"]
    spot = values["spx_spot"]
    invalidation = values["invalidation_spx"]
    target = values["target_spx"]
    if direction == "up" and not invalidation < trigger < target:
        return "manual_card_risk_coordinates_incoherent"
    if direction == "down" and not target < trigger < invalidation:
        return "manual_card_risk_coordinates_incoherent"
    if direction == "up" and not invalidation < spot < target:
        return "manual_card_spot_outside_risk_bounds"
    if direction == "down" and not target < spot < invalidation:
        return "manual_card_spot_outside_risk_bounds"

    if not str(intent.get("provider") or ""):
        return "action_quote_provider_unavailable"
    if parse_time(intent.get("quote_source_at")) is None:
        return "action_quote_source_time_unavailable"
    valid_until = parse_time(intent.get("valid_until") or intent.get("expires_at"))
    if valid_until is None:
        return "manual_card_expiry_unavailable"
    if valid_until <= _utc(now):
        return "manual_card_expired"
    time_stop = parse_time(intent.get("time_stop_at"))
    if time_stop is None:
        return "manual_card_time_stop_unavailable"
    if time_stop <= _utc(now):
        return "manual_card_time_stop_elapsed"
    return None


def _action_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _record_action_revalidation(
    state_path: Path,
    intent_id: str,
    *,
    now: datetime,
    evidence: Mapping[str, object],
) -> None:
    with exclusive_state_lock(state_path):
        state = read_json_object(state_path)
        state["last_action_revalidation"] = dict(evidence)
        state["updated_at"] = now.isoformat()
        atomic_write_json_secure(state_path, state)


def _release_delivery_lease(state_path: Path, intent_id: str, *, now: datetime) -> None:
    with exclusive_state_lock(state_path):
        state = read_json_object(state_path)
        inflight = dict(state.get("inflight") or {})
        inflight.pop(intent_id, None)
        state["inflight"] = inflight
        state["updated_at"] = now.isoformat()
        atomic_write_json_secure(state_path, state)
