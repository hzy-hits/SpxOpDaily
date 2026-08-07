"""Durable lifecycle runtime for GTH level manual candidates."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Mapping

from spx_spark.application.market_features.gth_candidate_lifecycle import (
    active_manual_plan_snapshot,
    cancellation_scope,
    mark_refresh_pending,
    recover_active_manual_plan,
    seed_replayed_candidate_ids,
    terminal_notification_intent,
    unreplayed_candidate,
)
from spx_spark.application.market_features.gth_manual_candidate import (
    _notification_intent,
)
from spx_spark.application.market_features.virtual_strategy_state import (
    flush_pending_notifications,
)
from spx_spark.application.market_features.virtual_strategy_support import (
    _number,
    _time,
)
from spx_spark.config import NotificationSettings, StorageSettings
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.notifier.dispatcher import cancel_pending_notification
from spx_spark.notifier.receipts import (
    ExternalDeliveryReceiptLookup,
    inspect_external_delivery_receipt,
)
from spx_spark.state_io import (
    append_jsonl_secure,
    atomic_write_json_secure,
    exclusive_state_lock,
    read_json_object,
)
from spx_spark.strategy_contract import strategy_event_fields


TERMINAL_RECEIPT_CHECK_MIN_SECONDS = 2 * 60
TERMINAL_RECEIPT_CHECK_MAX_SECONDS = 10 * 60
TERMINAL_RECEIPT_RECOVERY_MAX_SECONDS = 24 * 60 * 60


def persist_gth_level_manual_candidate(
    storage: StorageSettings,
    candidate: Mapping[str, object],
    *,
    now: datetime,
    notification: NotificationSettings | None,
    flush_pending_notifications_fn: Callable[..., Mapping[str, object]] = (
        flush_pending_notifications
    ),
    cancel_pending_notification_fn: Callable[..., int] = cancel_pending_notification,
    external_ready_receipt_fn: (
        Callable[[NotificationSettings, str], ExternalDeliveryReceiptLookup] | None
    ) = None,
    atomic_write_json_secure_fn: Callable[..., None] = atomic_write_json_secure,
) -> dict[str, object]:
    external_ready_receipt_fn = external_ready_receipt_fn or _external_ready_receipt
    state_path = Path(storage.data_root) / "latest" / "gth_level_manual_candidate_state.json"
    projection_path = Path(storage.data_root) / "latest" / "gth_level_manual_candidate.json"
    candidate = dict(candidate)
    notification_event_id: str | None = None
    settings = notification or NotificationSettings.from_env()
    with exclusive_state_lock(state_path):
        state = read_json_object(state_path)
        candidate = mark_refresh_pending(candidate)
        active_plan = recover_active_manual_plan(state, now=now)
        candidate, active_plan = _apply_active_plan_coherence(candidate, active_plan, now=now)
        notification_event_id = (
            f"{candidate['candidate_id']}:ready"
            if candidate.get("status") == "manual_ready"
            else None
        )
        gate_record_key, gate_record = _gate_record(candidate, now=now)
        replay_journal_path = (
            Path(storage.data_root)
            / "features"
            / "gth_level_manual_candidates"
            / f"date={DEFAULT_MARKET_CALENDAR.research_expiry(now).isoformat()}"
            / "events.jsonl"
        )
        replayed_candidate_ids = seed_replayed_candidate_ids(
            state,
            replay_journal_path=replay_journal_path,
        )
        if state.get("last_gate_record_key") != gate_record_key:
            replay_record = _replay_candidate_record(candidate, now=now)
            replay_candidate_id = unreplayed_candidate(candidate, replayed_candidate_ids)
            if replay_record is not None and replay_candidate_id is not None:
                append_jsonl_secure(
                    Path(storage.data_root)
                    / "features"
                    / "gth_level_manual_candidates"
                    / f"date={replay_record['session_date']}"
                    / "events.jsonl",
                    replay_record,
                )
                replayed_candidate_ids.add(replay_candidate_id)
            append_jsonl_secure(
                Path(storage.data_root)
                / "features"
                / "gth_manual_signal_gates"
                / f"date={now.date().isoformat()}"
                / "events.jsonl",
                gate_record,
            )
        accepted = {
            str(item)
            for item in (
                list(state.get("accepted_notification_event_ids") or [])
                + list(state.get("notified_event_ids") or [])
            )
            if item
        }
        settled = {str(item) for item in state.get("settled_notification_event_ids") or [] if item}
        pending = [
            dict(item)
            for item in state.get("pending_notifications") or []
            if isinstance(item, Mapping)
        ]
        terminal_checks = [
            dict(item)
            for item in state.get("pending_terminal_receipt_checks") or []
            if isinstance(item, Mapping)
        ]
        manual_plan_monitors = [
            dict(item)
            for item in state.get("manual_plan_monitors") or []
            if isinstance(item, Mapping)
        ]
        terminal_receipt_audit_failures = [
            dict(item)
            for item in state.get("terminal_receipt_audit_failures") or []
            if isinstance(item, Mapping)
        ]
        lifecycle_events = {
            str(item.get("event_id") or ""): str(item.get("source_signal_id") or "")
            for item in state.get("notification_lifecycle_events") or []
            if isinstance(item, Mapping) and item.get("event_id") and item.get("source_signal_id")
        }
        cancellation_pending = {
            str(item)
            for item in state.get("pending_notification_cancellation_event_ids") or []
            if item
        }
        cancellation_times = {
            str(key): parsed
            for key, value in dict(state.get("pending_notification_cancellation_at") or {}).items()
            if key and (parsed := _time(value)) is not None
        }
        terminal_flush_ids: set[str] = set()
        remaining_terminal_checks: list[dict[str, object]] = []
        known_notification_ids = (
            accepted | settled | {str(item.get("event_id") or "") for item in pending}
        )
        terminal_flush_ids.update(
            str(item.get("event_id") or "")
            for item in pending
            if item.get("causation_event_id")
            and str(item.get("event_id") or "") not in accepted
            and str(item.get("event_id") or "") not in settled
        )
        for check in terminal_checks:
            causation_event_id = str(check.get("causation_event_id") or "")
            occurred_at = _time(check.get("occurred_at"))
            plan = check.get("active_plan")
            terminal_candidate = check.get("candidate")
            if (
                not causation_event_id
                or occurred_at is None
                or not isinstance(plan, Mapping)
                or not isinstance(terminal_candidate, Mapping)
            ):
                continue
            check_until = _time(check.get("check_until")) or _terminal_receipt_check_until(
                plan,
                occurred_at=occurred_at,
            )
            recovery_until = _time(check.get("recovery_until")) or _terminal_receipt_recovery_until(
                occurred_at=occurred_at
            )
            lookup = external_ready_receipt_fn(settings, causation_event_id)
            if lookup.receipt is None:
                updated_check = _terminal_check_after_lookup(
                    check,
                    lookup=lookup,
                    now=now,
                    check_until=check_until,
                    recovery_until=recovery_until,
                )
                if lookup.observable and now >= check_until:
                    continue
                if not lookup.observable and now >= recovery_until:
                    terminal_receipt_audit_failures.append(
                        {
                            **updated_check,
                            "receipt_lookup_status": "degraded_recovery_exhausted",
                            "recovery_exhausted_at": now.isoformat(),
                        }
                    )
                    continue
                remaining_terminal_checks.append(updated_check)
                continue
            recorded_release_reason = (
                str(check.get("release_reason")) if check.get("release_reason") else None
            )
            release_reason = recorded_release_reason or _active_plan_release_reason(
                plan,
                current_spx=_number(candidate.get("current_parity_spx")),
                now=now,
            )
            intent = terminal_notification_intent(
                plan,
                terminal_candidate,
                causation_event_id=causation_event_id,
                occurred_at=occurred_at,
                enqueued_at=now,
                release_reason=release_reason,
            )
            terminal_event_id = str(intent["event_id"])
            if terminal_event_id not in known_notification_ids:
                pending.append(intent)
                known_notification_ids.add(terminal_event_id)
            if terminal_event_id not in accepted and terminal_event_id not in settled:
                terminal_flush_ids.add(terminal_event_id)
            if intent.get("terminal_action") == "cancel":
                manual_plan_monitors = _upsert_manual_plan_monitor(
                    manual_plan_monitors,
                    plan,
                    receipt=lookup.receipt,
                )
        terminal_checks = remaining_terminal_checks
        remaining_manual_plan_monitors: list[dict[str, object]] = []
        for monitor in manual_plan_monitors:
            plan = monitor.get("active_plan")
            if not isinstance(plan, Mapping):
                continue
            ready_event_id = str(monitor.get("ready_event_id") or plan.get("ready_event_id") or "")
            exit_event_id = f"{ready_event_id}:exit" if ready_event_id else ""
            if not ready_event_id or exit_event_id in known_notification_ids:
                continue
            release_reason = _active_plan_release_reason(
                plan,
                current_spx=_number(candidate.get("current_parity_spx")),
                now=now,
            )
            if release_reason is None:
                remaining_manual_plan_monitors.append(monitor)
                continue
            intent = terminal_notification_intent(
                plan,
                candidate,
                causation_event_id=ready_event_id,
                occurred_at=now,
                release_reason=release_reason,
            )
            pending.append(intent)
            known_notification_ids.add(exit_event_id)
            if exit_event_id not in accepted and exit_event_id not in settled:
                terminal_flush_ids.add(exit_event_id)
        manual_plan_monitors = remaining_manual_plan_monitors
        previous = state.get("last_candidate")
        if isinstance(previous, Mapping):
            candidate_id = str(previous.get("candidate_id") or "")
            source_id = str(previous.get("source_signal_id") or "")
            if candidate_id and source_id:
                lifecycle_events.setdefault(f"{candidate_id}:ready", source_id)
        for item in pending:
            event_id = str(item.get("event_id") or "")
            source_id = str(item.get("source_signal_id") or "")
            if event_id and source_id and not item.get("causation_event_id"):
                lifecycle_events.setdefault(event_id, source_id)
        if notification_event_id:
            lifecycle_events.setdefault(
                notification_event_id,
                str(candidate.get("source_signal_id") or ""),
            )
        cancellation_pending.update(cancellation_scope(candidate, lifecycle_events, now=now))
        for event_id in cancellation_pending:
            cancellation_times.setdefault(event_id, now)
        if cancellation_pending:
            state["pending_notification_cancellation_event_ids"] = sorted(cancellation_pending)[
                -200:
            ]
            state["pending_notification_cancellation_at"] = {
                event_id: cancellation_times[event_id].isoformat()
                for event_id in sorted(cancellation_pending)[-200:]
            }
            atomic_write_json_secure_fn(state_path, state)
        for event_id in sorted(cancellation_pending):
            cancelled_at = cancellation_times[event_id]
            terminal_plan = _terminal_plan_for_event(
                event_id,
                active_plan=active_plan,
                previous=previous if isinstance(previous, Mapping) else None,
                now=now,
            )
            release_reason = _active_plan_release_reason(
                terminal_plan,
                current_spx=_number(candidate.get("current_parity_spx")),
                now=now,
            )
            try:
                cancelled_before_delivery = cancel_pending_notification_fn(
                    settings,
                    event_id,
                    now=cancelled_at,
                    reason="source_candidate_no_longer_manual_ready",
                )
            except Exception:
                continue
            cancellation_pending.discard(event_id)
            cancellation_times.pop(event_id, None)
            settled.add(event_id)
            accepted.discard(event_id)
            lifecycle_events.pop(event_id, None)
            pending = [item for item in pending if str(item.get("event_id") or "") != event_id]
            lookup = external_ready_receipt_fn(settings, event_id)
            terminal_check = {
                "causation_event_id": event_id,
                "occurred_at": cancelled_at.isoformat(),
                "check_until": _terminal_receipt_check_until(
                    terminal_plan,
                    occurred_at=cancelled_at,
                ).isoformat(),
                "recovery_until": _terminal_receipt_recovery_until(
                    occurred_at=cancelled_at,
                ).isoformat(),
                "active_plan": terminal_plan,
                "candidate": dict(candidate),
                "release_reason": release_reason,
            }
            if lookup.receipt is not None:
                intent = terminal_notification_intent(
                    terminal_plan,
                    candidate,
                    causation_event_id=event_id,
                    occurred_at=cancelled_at,
                    release_reason=release_reason,
                )
                terminal_event_id = str(intent["event_id"])
                if terminal_event_id not in known_notification_ids:
                    pending.append(intent)
                    known_notification_ids.add(terminal_event_id)
                if terminal_event_id not in accepted and terminal_event_id not in settled:
                    terminal_flush_ids.add(terminal_event_id)
                if intent.get("terminal_action") == "cancel":
                    manual_plan_monitors = _upsert_manual_plan_monitor(
                        manual_plan_monitors,
                        terminal_plan,
                        receipt=lookup.receipt,
                    )
            elif cancelled_before_delivery == 0 and not any(
                str(item.get("causation_event_id") or "") == event_id for item in terminal_checks
            ):
                terminal_checks.append(
                    _terminal_check_after_lookup(
                        terminal_check,
                        lookup=lookup,
                        now=now,
                        check_until=_terminal_receipt_check_until(
                            terminal_plan,
                            occurred_at=cancelled_at,
                        ),
                        recovery_until=_terminal_receipt_recovery_until(
                            occurred_at=cancelled_at,
                        ),
                    )
                )
            if str(terminal_plan.get("ready_event_id") or "") == event_id:
                active_plan = {}
        pending_ids = {str(item.get("event_id") or "") for item in pending}
        if (
            notification_event_id
            and not cancellation_pending
            and notification_event_id not in accepted
            and notification_event_id not in settled
            and notification_event_id not in pending_ids
        ):
            pending.append(
                _notification_intent(
                    candidate,
                    event_id=notification_event_id,
                    now=now,
                )
            )
        state.update(
            {
                "schema_version": 1,
                "updated_at": now.isoformat(),
                "last_gate_record_key": gate_record_key,
                "last_candidate": dict(candidate),
                "replayed_candidate_ids": sorted(replayed_candidate_ids)[-500:],
                "accepted_notification_event_ids": sorted(accepted)[-200:],
                "settled_notification_event_ids": sorted(settled)[-200:],
                "pending_notifications": pending,
                "notification_lifecycle_events": [
                    {"event_id": event_id, "source_signal_id": source_id}
                    for event_id, source_id in sorted(lifecycle_events.items())[-200:]
                ],
                "pending_notification_cancellation_event_ids": sorted(cancellation_pending)[-200:],
                "pending_notification_cancellation_at": {
                    event_id: cancellation_times[event_id].isoformat()
                    for event_id in sorted(cancellation_pending)[-200:]
                    if event_id in cancellation_times
                },
                "pending_terminal_receipt_checks": terminal_checks[-200:],
                "terminal_receipt_audit_failures": terminal_receipt_audit_failures[-200:],
                "manual_plan_monitors": manual_plan_monitors[-100:],
                "active_manual_plan": active_plan,
            }
        )
        atomic_write_json_secure_fn(state_path, state)
        atomic_write_json_secure_fn(projection_path, candidate)
    result = {"attempted": False, "accepted": False}
    if notification_event_id:
        result = flush_pending_notifications_fn(
            state_path,
            settings=settings,
            now=now,
            only_event_id=notification_event_id,
        )
    terminal_result: dict[str, object] = {"attempted": False, "accepted": False}
    for terminal_event_id in sorted(terminal_flush_ids):
        terminal_result = flush_pending_notifications_fn(
            state_path,
            settings=settings,
            now=now,
            only_event_id=terminal_event_id,
        )
    if notification_event_id and result.get("accepted") is True:
        with exclusive_state_lock(state_path):
            state = read_json_object(state_path)
            state["active_manual_plan"] = active_manual_plan_snapshot(
                candidate,
                activated_at=now.isoformat(),
            )
            atomic_write_json_secure_fn(state_path, state)
    return {
        **candidate,
        "notification_attempted": bool(result.get("attempted")),
        "notification_accepted": bool(result.get("accepted")),
        "notification_outcome": result.get("outcome"),
        "terminal_notification_attempted": bool(terminal_result.get("attempted")),
        "terminal_notification_accepted": bool(terminal_result.get("accepted")),
        "terminal_notification_outcome": terminal_result.get("outcome"),
    }


def _external_ready_receipt(
    settings: NotificationSettings,
    event_id: str,
):
    return inspect_external_delivery_receipt(
        event_id,
        rust_owner=bool(getattr(settings, "rust_trader_notification_owner", False)),
        rust_ledger_path=str(getattr(settings, "rust_delivery_ledger_path", "") or ""),
        python_receipt_path=str(getattr(settings, "delivery_receipt_path", "") or ""),
    )


def _terminal_plan_for_event(
    event_id: str,
    *,
    active_plan: Mapping[str, object],
    previous: Mapping[str, object] | None,
    now: datetime,
) -> dict[str, object]:
    if str(active_plan.get("ready_event_id") or "") == event_id:
        return dict(active_plan)
    previous_candidate_id = str((previous or {}).get("candidate_id") or "")
    if previous_candidate_id and f"{previous_candidate_id}:ready" == event_id:
        return active_manual_plan_snapshot(
            previous or {},
            activated_at=(previous or {}).get("evaluated_at") or now.isoformat(),
        )
    return {
        "candidate_id": event_id.removesuffix(":ready"),
        "ready_event_id": event_id,
        "activated_at": now.isoformat(),
    }


def _terminal_receipt_check_until(
    active_plan: Mapping[str, object],
    *,
    occurred_at: datetime,
) -> datetime:
    minimum = occurred_at + timedelta(seconds=TERMINAL_RECEIPT_CHECK_MIN_SECONDS)
    maximum = occurred_at + timedelta(seconds=TERMINAL_RECEIPT_CHECK_MAX_SECONDS)
    ready_valid_until = _time(active_plan.get("valid_until"))
    return min(max(minimum, ready_valid_until or minimum), maximum)


def _terminal_receipt_recovery_until(*, occurred_at: datetime) -> datetime:
    return occurred_at + timedelta(seconds=TERMINAL_RECEIPT_RECOVERY_MAX_SECONDS)


def _terminal_check_after_lookup(
    check: Mapping[str, object],
    *,
    lookup: ExternalDeliveryReceiptLookup,
    now: datetime,
    check_until: datetime,
    recovery_until: datetime,
) -> dict[str, object]:
    attempts = check.get("receipt_lookup_attempts")
    attempts = attempts if isinstance(attempts, int) and not isinstance(attempts, bool) else 0
    observable = getattr(lookup, "observable", False) is True
    error = str(getattr(lookup, "error", "") or "")[:200] or None
    result = {
        **check,
        "check_until": check_until.isoformat(),
        "recovery_until": recovery_until.isoformat(),
        "receipt_lookup_status": (
            "observable_no_receipt" if observable else "degraded_ledger_unavailable"
        ),
        "receipt_lookup_degraded": not observable,
        "receipt_lookup_error": error,
        "receipt_lookup_attempts": min(attempts + 1, 1_000_000),
        "last_receipt_lookup_at": now.isoformat(),
    }
    if not observable:
        result["receipt_ledger_unavailable_since"] = (
            check.get("receipt_ledger_unavailable_since") or now.isoformat()
        )
    return result


def _upsert_manual_plan_monitor(
    monitors: list[dict[str, object]],
    active_plan: Mapping[str, object],
    *,
    receipt: object,
) -> list[dict[str, object]]:
    ready_event_id = str(active_plan.get("ready_event_id") or "")
    exit_at = _time(active_plan.get("exit_at"))
    if not ready_event_id or exit_at is None:
        return monitors
    retained = [
        item for item in monitors if str(item.get("ready_event_id") or "") != ready_event_id
    ]
    delivered_at = getattr(receipt, "delivered_at", None)
    retained.append(
        {
            "ready_event_id": ready_event_id,
            "active_plan": dict(active_plan),
            "ready_receipt_id": str(getattr(receipt, "receipt_id", "") or ""),
            "ready_delivered_at": (
                delivered_at.isoformat() if isinstance(delivered_at, datetime) else None
            ),
            "monitor_until": exit_at.isoformat(),
        }
    )
    return retained[-100:]


def _apply_active_plan_coherence(
    candidate: Mapping[str, object],
    active_plan: Mapping[str, object],
    *,
    now: datetime,
) -> tuple[dict[str, object], dict[str, object]]:
    """Prevent an opposite card until the prior operator plan is resolved."""
    candidate = dict(candidate)
    active = dict(active_plan)
    if candidate.get("status") != "manual_ready" or not active:
        return candidate, active
    direction = str(candidate.get("direction") or "")
    active_direction = str(active.get("direction") or "")
    if direction not in {"up", "down"} or active_direction not in {"up", "down"}:
        return candidate, active
    if direction == active_direction:
        return candidate, active
    release_reason = _active_plan_release_reason(
        active,
        current_spx=_number(candidate.get("current_parity_spx")),
        now=now,
    )
    if release_reason is None:
        reasons = [
            *[str(item) for item in candidate.get("block_reasons") or ()],
            "opposite_signal_conflicts_with_active_plan",
        ]
        gate_contract = candidate.get("gate_contract")
        gate_contract = dict(gate_contract) if isinstance(gate_contract, Mapping) else {}
        return (
            {
                **candidate,
                "status": "blocked",
                "manual_action_eligible": False,
                "signal_absence_reason": "active_manual_plan_not_invalidated",
                "block_reasons": list(dict.fromkeys(reasons)),
                "active_manual_plan": active,
                "gate_contract": {
                    **gate_contract,
                    "hard_block_reasons": list(dict.fromkeys(reasons)),
                },
            },
            active,
        )
    return (
        {
            **candidate,
            "replaces_prior_plan": {
                "candidate_id": active.get("candidate_id"),
                "direction": active_direction,
                "release_reason": release_reason,
            },
        },
        {},
    )


def _active_plan_release_reason(
    active_plan: Mapping[str, object],
    *,
    current_spx: float | None,
    now: datetime,
) -> str | None:
    exit_at = _time(active_plan.get("exit_at"))
    if exit_at is not None and now >= exit_at:
        return "time_exit_elapsed"
    if current_spx is None:
        return None
    direction = str(active_plan.get("direction") or "")
    invalidation = _number(active_plan.get("invalidation_spx"))
    target = _number(active_plan.get("target_spx"))
    if direction == "down":
        if invalidation is not None and current_spx >= invalidation:
            return "prior_put_invalidated"
        if target is not None and current_spx <= target:
            return "prior_put_target_reached"
    elif direction == "up":
        if invalidation is not None and current_spx <= invalidation:
            return "prior_call_invalidated"
        if target is not None and current_spx >= target:
            return "prior_call_target_reached"
    return None


def _gate_record(
    candidate: Mapping[str, object],
    *,
    now: datetime,
) -> tuple[str, dict[str, object]]:
    identity = "|".join(
        (
            now.date().isoformat(),
            str(candidate.get("source_signal_id") or "none"),
            str(candidate.get("candidate_id") or "none"),
            str(candidate.get("status") or "unknown"),
            str(candidate.get("path_kind") or "none"),
            ",".join(str(item) for item in candidate.get("block_reasons") or ()),
        )
    )
    digest = hashlib.sha256(identity.encode()).hexdigest()[:24]
    gate_contract = candidate.get("gate_contract")
    return digest, {
        "schema_version": 1,
        "record_id": f"gth-manual-gate:{digest}",
        "recorded_at": now.isoformat(),
        "source_signal_id": candidate.get("source_signal_id"),
        "source_kind": candidate.get("source_kind"),
        "candidate_id": candidate.get("candidate_id"),
        "status": candidate.get("status"),
        "path_kind": candidate.get("path_kind"),
        "direction": candidate.get("direction"),
        "long_contract_id": candidate.get("long_contract_id"),
        "short_contract_id": candidate.get("short_contract_id"),
        "signal_absence_reason": candidate.get("signal_absence_reason"),
        "block_reasons": list(candidate.get("block_reasons") or ()),
        "gate_contract": (dict(gate_contract) if isinstance(gate_contract, Mapping) else None),
        "session_quote_provider": "ibkr",
    }


def _replay_candidate_record(
    candidate: Mapping[str, object],
    *,
    now: datetime,
) -> dict[str, object] | None:
    """Build replay for fully quoted READY or research WATCH candidates."""
    if candidate.get("status") not in {"manual_ready", "structure_watch", "selector_candidate"}:
        return None
    parity = candidate.get("target_coordinate")
    parity = dict(parity) if isinstance(parity, Mapping) else {}
    observed_spx = _number(candidate.get("current_parity_spx"))
    trigger_level = _number(candidate.get("trigger_level"))
    coordinate = {
        "kind": "chain_implied_spx",
        "instrument_id": "synthetic:SPXW_PARITY",
        "observed_value": observed_spx,
        "target_value": trigger_level,
        "spx_observed_value": observed_spx,
        # Parity is SPX-coordinate; invalidation_es separately carries ES/SPX basis.
        "basis_points": 0.0,
        "as_of": parity.get("source_at") or candidate.get("evaluated_at"),
    }
    return {
        **candidate,
        **strategy_event_fields(
            policy_version_value=str(candidate.get("policy_version") or ""),
            valid_until=candidate.get("valid_until"),
            coordinate=coordinate,
            block_reasons=candidate.get("block_reasons") or (),
        ),
        "event": "gth_level_manual_candidate_evaluated",
        "strategy_id": "gth_level_manual_candidate",
        "strategy_lane": "gth_level_manual_candidate",
        "lifecycle_status": "legacy_production",
        "runtime_status": "production_runtime",
        "session_date": DEFAULT_MARKET_CALENDAR.research_expiry(now).isoformat(),
    }
