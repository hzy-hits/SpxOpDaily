"""Broker-independent lifecycle for the system's own 0DTE strategy episode."""

from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timedelta
from pathlib import Path
from typing import Mapping

from spx_spark.application.market_features.virtual_strategy_rth import (
    evaluate_trade_intent_entry as _evaluate_trade_intent_entry,
    shadow_execution_costs as _shadow_execution_costs,
)
from spx_spark.application.market_features.virtual_strategy_support import (
    _action_underlier_snapshot,
    _active_snapshot_impl,
    _append_audit,
    _cap_rth_trade_episode,
    _contract_snapshot,
    _episode,
    _entry_observed_at,
    _event_contract,
    _exit_clock as _exit_clock,
    _exit_decision,
    _fmt as _fmt,
    _gth_chain_reference,
    _gth_signal_age_seconds,
    _gth_spread_contract_ids,
    _gth_time_stop,
    _latest_created_at,
    _level_reached,
    _lifecycle_transition,
    _number,
    _pct as _pct,
    _record_entry_decision,
    _record_due_horizons,
    _rth_trade_hard_exit as _rth_trade_hard_exit,
    _should_replace_with_gth_spread,
    _spx_reference as _spx_reference,
    _state_path,
    _time,
    _trim_entry_decisions,
    _utc,
)
from spx_spark.application.market_features.virtual_strategy_render import (
    render_virtual_strategy_exit as _render_exit,
)
from spx_spark.application.market_features.virtual_strategy_spread import (
    spread_snapshot as _spread_snapshot_impl,
    spread_snapshot_decision as _spread_snapshot_decision_impl,
)
from spx_spark.application.market_features.virtual_strategy_state import (
    consumed_signal_state as _consumed_signal_state,
    flush_pending_notifications as _flush_pending_notifications_impl,
    load_consumed_signals as _load_consumed_signals,
    mark_signal_consumed as _mark_signal_consumed,
)
from spx_spark.config import NotificationSettings, StorageSettings
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.notifier.dispatcher import enqueue_notification
from spx_spark.notifier.model import CommandRunner, default_runner
from spx_spark.notifier.operator_contract import (
    operator_generation,
    operator_opportunity_id,
)
from spx_spark.notifier.receipts import (
    ExternalDeliveryReceipt,
    inspect_external_delivery_receipt,
)
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock, read_json_object
from spx_spark.storage import LatestState
from spx_spark.strategy_contract import (
    actionable_strategy_contract_issues,
    normalize_block_reasons,
    parse_aware_time,
    policy_version,
    strategy_event_fields,
)


def _flush_pending_notifications(
    state_path: Path,
    *,
    settings: NotificationSettings,
    now: datetime,
    only_event_id: str | None = None,
) -> dict[str, object]:
    return _flush_pending_notifications_impl(
        state_path,
        settings=settings,
        now=now,
        only_event_id=only_event_id,
        enqueue=enqueue_notification,
    )


def _spread_snapshot(
    latest: LatestState,
    *,
    long_contract_id: str,
    short_contract_id: str,
    now: datetime,
    max_quote_age_seconds: float,
    max_quote_skew_seconds: float,
    required_provider: str | None = None,
) -> dict[str, object]:
    return _spread_snapshot_impl(
        latest,
        long_contract_id=long_contract_id,
        short_contract_id=short_contract_id,
        now=now,
        max_quote_age_seconds=max_quote_age_seconds,
        max_quote_skew_seconds=max_quote_skew_seconds,
        required_provider=required_provider,
        contract_snapshot=_contract_snapshot,
    )


def _spread_snapshot_decision(
    latest: LatestState,
    *,
    long_contract_id: str,
    short_contract_id: str,
    now: datetime,
    max_quote_age_seconds: float,
    max_quote_skew_seconds: float,
    required_provider: str | None = None,
) -> tuple[dict[str, object], list[str]]:
    return _spread_snapshot_decision_impl(
        latest,
        long_contract_id=long_contract_id,
        short_contract_id=short_contract_id,
        now=now,
        max_quote_age_seconds=max_quote_age_seconds,
        max_quote_skew_seconds=max_quote_skew_seconds,
        required_provider=required_provider,
        contract_snapshot=_contract_snapshot,
    )


def process_virtual_strategy(
    storage: StorageSettings,
    latest: LatestState,
    *,
    trade_intent: Mapping[str, object],
    gth_signal: Mapping[str, object],
    option_structure: Mapping[str, object],
    macro_event: Mapping[str, object],
    greek_decision: Mapping[str, object],
    now: datetime,
    policy: MarketFeatureSettings,
    new_entries_allowed: bool,
    new_entries_block_reason: str,
    expected_trade_intent_policy_version: str | None = None,
    notification: NotificationSettings | None = None,
    runner: CommandRunner = default_runner,
) -> dict[str, object]:
    """Open/update/close one virtual episode; never reads or writes broker positions."""

    now = _utc(now)
    settings = notification or NotificationSettings.from_env()
    provider_entry_control = {
        "allowed": new_entries_allowed,
        "reason": new_entries_block_reason,
    }
    state_path = _state_path(storage)
    recovery = _flush_pending_notifications(
        state_path,
        settings=settings,
        now=now,
    )
    if not policy.virtual_strategy_enabled:
        return {
            "status": "disabled",
            "notification_attempted": bool(recovery.get("attempted")),
            "notification_recovery": recovery,
        }
    with exclusive_state_lock(state_path):
        state = read_json_object(state_path)
        active = _cap_rth_trade_episode(
            dict(state.get("active") or {}),
            now=now,
        )
        consumed_signals, consumed = _load_consumed_signals(state)
        entry_decisions = {
            str(key): dict(value)
            for key, value in dict(state.get("entry_decisions") or {}).items()
            if isinstance(value, Mapping)
        }
        raw_pending_trade_intent = state.get("pending_trade_intent")
        pending_trade_intent = (
            dict(raw_pending_trade_intent)
            if isinstance(raw_pending_trade_intent, Mapping)
            else {}
        )
        if not active and pending_trade_intent and not new_entries_allowed:
            pending_valid_until = parse_aware_time(pending_trade_intent.get("valid_until"))
            if pending_valid_until is not None and now >= pending_valid_until:
                censored = _pending_trade_intent_provider_censored_decision(
                    pending_trade_intent,
                    provider_reason=new_entries_block_reason,
                    now=now,
                    policy=policy,
                )
                _record_entry_decision(
                    storage,
                    censored,
                    entry_decisions=entry_decisions,
                    now=now,
                )
                pending_id = str(pending_trade_intent.get("intent_id") or "")
                _mark_signal_consumed(
                    consumed_signals,
                    consumed,
                    signal_id=pending_id,
                    now=now,
                )
                pending_trade_intent = {}
        if new_entries_allowed and _should_replace_with_gth_spread(active, gth_signal):
            replacement, entry_decision = _evaluate_gth_spread_entry(
                latest,
                gth_signal=gth_signal,
                now=now,
                policy=policy,
            )
            _record_entry_decision(
                storage,
                entry_decision,
                entry_decisions=entry_decisions,
                now=now,
            )
            if replacement:
                _append_audit(
                    storage,
                    now,
                    {
                        "event": "virtual_superseded",
                        "episode_id": active.get("episode_id"),
                        "source_signal_id": active.get("source_signal_id"),
                        "reason": "replace_legacy_naked_with_gth_debit_spread",
                        **_event_contract(
                            active,
                            block_reasons=("replace_legacy_naked_with_gth_debit_spread",),
                        ),
                    },
                )
                active = replacement
                signal_id = str(active.get("source_signal_id") or "")
                _mark_signal_consumed(
                    consumed_signals,
                    consumed,
                    signal_id=signal_id,
                    now=now,
                )
                _append_audit(storage, now, {"event": "virtual_opened", **active})
            elif entry_decision.get("terminal") is True:
                source_id = str(entry_decision.get("source_signal_id") or "")
                _mark_signal_consumed(
                    consumed_signals,
                    consumed,
                    signal_id=source_id,
                    now=now,
                )
        if not active and new_entries_allowed:
            current_trade_intent = (
                dict(trade_intent)
                if trade_intent.get("status") == "trade_ready"
                and str(trade_intent.get("intent_id") or "")
                else {}
            )
            pending_id = str(pending_trade_intent.get("intent_id") or "")
            current_id = str(current_trade_intent.get("intent_id") or "")
            if pending_id and current_id and pending_id != current_id:
                superseded = _pending_trade_intent_superseded_decision(
                    pending_trade_intent,
                    replacement=current_trade_intent,
                    now=now,
                    policy=policy,
                )
                _record_entry_decision(
                    storage,
                    superseded,
                    entry_decisions=entry_decisions,
                    now=now,
                )
                _mark_signal_consumed(
                    consumed_signals,
                    consumed,
                    signal_id=pending_id,
                    now=now,
                )
                pending_trade_intent = {}
            candidate_trade_intent = (
                pending_trade_intent or current_trade_intent or dict(trade_intent)
            )
            require_external_receipt = bool(
                candidate_trade_intent.get("status") == "trade_ready"
                and getattr(settings, "rust_trader_notification_owner", False)
            )
            external_receipt = None
            external_receipt_observable = True
            external_receipt_error = None
            if require_external_receipt:
                receipt_lookup = inspect_external_delivery_receipt(
                    str(candidate_trade_intent.get("notification_event_id") or ""),
                    rust_owner=True,
                    rust_ledger_path=str(
                        getattr(settings, "rust_delivery_ledger_path", "") or ""
                    ),
                )
                external_receipt = receipt_lookup.receipt
                external_receipt_observable = receipt_lookup.observable
                external_receipt_error = receipt_lookup.error
            active, entry_decision = _new_episode(
                latest,
                trade_intent=candidate_trade_intent,
                gth_signal=gth_signal,
                consumed=consumed,
                now=now,
                policy=policy,
                expected_trade_intent_policy_version=expected_trade_intent_policy_version,
                require_external_receipt=require_external_receipt,
                external_receipt=external_receipt,
                external_receipt_observable=external_receipt_observable,
                external_receipt_error=external_receipt_error,
            )
            if entry_decision:
                _record_entry_decision(
                    storage,
                    entry_decision,
                    entry_decisions=entry_decisions,
                    now=now,
                )
                if entry_decision.get("terminal") is True and not active:
                    source_id = str(entry_decision.get("source_signal_id") or "")
                    _mark_signal_consumed(
                        consumed_signals,
                        consumed,
                        signal_id=source_id,
                        now=now,
                    )
                if entry_decision.get("source_kind") == "trade_intent":
                    pending_trade_intent = (
                        {}
                        if active or entry_decision.get("terminal") is True
                        else dict(candidate_trade_intent)
                    )
            if active:
                pending_trade_intent = {}
                signal_id = str(active.get("source_signal_id") or "")
                _mark_signal_consumed(
                    consumed_signals,
                    consumed,
                    signal_id=signal_id,
                    now=now,
                )
                _append_audit(storage, now, {"event": "virtual_opened", **active})
        if not active:
            state.update(
                {
                    "schema_version": 2,
                    "updated_at": now.isoformat(),
                    "active": None,
                    "pending_trade_intent": pending_trade_intent or None,
                    **_consumed_signal_state(consumed_signals),
                    "entry_decisions": _trim_entry_decisions(entry_decisions),
                    "provider_entry_control": provider_entry_control,
                }
            )
            atomic_write_json_secure(state_path, state)
            return {
                "status": "observing",
                "notification_attempted": False,
                "new_entries_allowed": new_entries_allowed,
                "new_entries_block_reason": (
                    None if new_entries_allowed else new_entries_block_reason
                ),
            }

        state["pending_trade_intent"] = None

        current = _active_snapshot(latest, active, now=now, policy=policy)
        exit_reason, action = _exit_decision(
            active,
            current,
            latest=latest,
            option_structure=option_structure,
            macro_event=macro_event,
            greek_decision=greek_decision,
            now=now,
            policy=policy,
        )
        active["last_observed_at"] = now.isoformat()
        if current:
            active["last"] = current
            entry_mid = _number(active.get("entry_mid"))
            current_mid = _number(current.get("mid"))
            if entry_mid and current_mid is not None:
                return_fraction = current_mid / entry_mid - 1.0
                active["mfe_fraction"] = max(
                    float(active.get("mfe_fraction", 0.0)), return_fraction
                )
                active["mae_fraction"] = min(
                    float(active.get("mae_fraction", 0.0)), return_fraction
                )
                _record_due_horizons(storage, active, current, now=now)
        transition = _lifecycle_transition(
            active,
            current,
            exit_reason=exit_reason,
            action=action,
            now=now,
        )
        transition_audit = transition.get("audit")
        if isinstance(transition_audit, Mapping):
            _append_audit(storage, now, transition_audit)
        if transition["kind"] == "degraded":
            state.update(
                {
                    "schema_version": 2,
                    "updated_at": now.isoformat(),
                    "active": active,
                    **_consumed_signal_state(consumed_signals),
                    "entry_decisions": _trim_entry_decisions(entry_decisions),
                    "provider_entry_control": provider_entry_control,
                }
            )
            atomic_write_json_secure(state_path, state)
            return {
                "status": active.get("status"),
                "episode_id": active.get("episode_id"),
                "contract_id": active.get("contract_id"),
                "health_status": active.get("health_status"),
                "health_reason": active.get("health_reason"),
                "health_since": active.get("health_since"),
                "notification_attempted": False,
                "new_entries_allowed": new_entries_allowed,
            }
        if transition["kind"] == "censored":
            censored = dict(transition["episode"])
            state.update(
                {
                    "schema_version": 2,
                    "updated_at": now.isoformat(),
                    "active": None,
                    "last_censored": censored,
                    **_consumed_signal_state(consumed_signals),
                    "entry_decisions": _trim_entry_decisions(entry_decisions),
                    "provider_entry_control": provider_entry_control,
                }
            )
            atomic_write_json_secure(state_path, state)
            _append_audit(storage, now, {"event": "virtual_censored", **censored})
            return {
                "status": "censored",
                "episode_id": censored.get("episode_id"),
                "censor_reason": exit_reason,
                "notification_attempted": False,
                "new_entries_allowed": new_entries_allowed,
            }
        pending_exit_context = dict(transition.get("pending_exit_context") or {})
        if exit_reason is None:
            state.update(
                {
                    "schema_version": 2,
                    "updated_at": now.isoformat(),
                    "active": active,
                    **_consumed_signal_state(consumed_signals),
                    "entry_decisions": _trim_entry_decisions(entry_decisions),
                    "provider_entry_control": provider_entry_control,
                }
            )
            atomic_write_json_secure(state_path, state)
            return {
                "status": "active",
                "episode_id": active.get("episode_id"),
                "contract_id": active.get("contract_id"),
                "notification_attempted": False,
                "new_entries_allowed": new_entries_allowed,
            }

        exit_bid = _number(current.get("bid"))
        shadow_costs = _shadow_execution_costs(active, exit_bid=exit_bid)
        closed = {
            **active,
            **pending_exit_context,
            **_event_contract(active, block_reasons=()),
            "status": "closed",
            "closed_at": now.isoformat(),
            "exit_reason": exit_reason,
            "exit_action": action,
            "exit_snapshot": current,
            "exit_bid": exit_bid,
            "exit_price_basis": (
                "executable_bid" if _number(current.get("bid")) is not None else None
            ),
            "pnl_status": (
                "executable_quote_observed"
                if _number(current.get("bid")) is not None
                else "unavailable"
            ),
            **shadow_costs,
        }
        text = _render_exit(closed)
        notification_event_id = f"{closed['episode_id']}:{exit_reason}"
        pending_notifications = [
            dict(item)
            for item in state.get("pending_notifications") or []
            if isinstance(item, Mapping)
            and str(item.get("event_id") or "") != notification_event_id
        ]
        pending_notifications.append(
            {
                "event_id": notification_event_id,
                "source": "virtual_strategy",
                "kind": "virtual_strategy_exit",
                "lane": "strategy_lifecycle",
                "occurred_at": now.isoformat(),
                "expires_at": (now + timedelta(minutes=15)).isoformat(),
                "operator_opportunity_id": operator_opportunity_id(
                    closed,
                    "operator_opportunity_id",
                    "source_signal_id",
                    fallback=closed["episode_id"],
                ),
                "operator_generation": operator_generation(closed),
                "title": "SPX VIRTUAL STRATEGY EXIT",
                "text": text,
                "friend": True,
                "feishu_text": text,
                "enqueued_at": now.isoformat(),
            }
        )
        state.update(
            {
                "schema_version": 2,
                "updated_at": now.isoformat(),
                "active": None,
                "last_closed": closed,
                **_consumed_signal_state(consumed_signals),
                "entry_decisions": _trim_entry_decisions(entry_decisions),
                "provider_entry_control": provider_entry_control,
                "pending_notifications": pending_notifications,
            }
        )
        atomic_write_json_secure(state_path, state)
        _append_audit(storage, now, {"event": "virtual_closed", **closed})

    notification_result = _flush_pending_notifications(
        state_path,
        settings=settings,
        now=now,
        only_event_id=notification_event_id,
    )
    return {
        "status": "closed",
        "episode_id": closed.get("episode_id"),
        "exit_reason": exit_reason,
        "exit_action": action,
        "notification_attempted": bool(notification_result.get("attempted")),
        "notification_accepted": bool(notification_result.get("accepted")),
        "notification_inserted": bool(notification_result.get("inserted")),
        "notification_duplicate": bool(notification_result.get("duplicate")),
        "notification_delivered": bool(notification_result.get("delivered")),
        "notification_queued": bool(notification_result.get("queued_for_recovery")),
        "notification_outcome": notification_result.get("outcome"),
        "notification_enqueued_at": now.isoformat(),
        "targets": list(notification_result.get("targets") or []),
        "new_entries_allowed": new_entries_allowed,
    }


def _pending_trade_intent_superseded_decision(
    pending: Mapping[str, object],
    *,
    replacement: Mapping[str, object],
    now: datetime,
    policy: MarketFeatureSettings,
) -> dict[str, object]:
    """Close one immutable waiting contract before accepting a new opportunity."""

    source_id = str(pending.get("intent_id") or "")
    replacement_id = str(replacement.get("intent_id") or "")
    raw_coordinate = pending.get("coordinate")
    coordinate = dict(raw_coordinate) if isinstance(raw_coordinate, Mapping) else None
    reasons = ["superseded_by_new_trade_intent"]
    return {
        **strategy_event_fields(
            policy_version_value=policy_version(
                "virtual_rth_pending_supersession.v1",
                policy,
            ),
            valid_until=parse_aware_time(pending.get("valid_until")),
            coordinate=coordinate,
            block_reasons=reasons,
        ),
        "event": "virtual_entry_decision",
        "decision_id": f"virtual-entry:{source_id or 'unavailable'}",
        "source_signal_id": source_id or None,
        "source_kind": "trade_intent",
        "source_schema_version": pending.get("schema_version"),
        "source_policy_version": pending.get("policy_version"),
        "source_evaluated_at": pending.get("evaluated_at"),
        "entry_observed_at": _entry_observed_at(pending),
        "action_revalidated_at": now.isoformat(),
        "evaluated_at": now.isoformat(),
        "status": "blocked",
        "terminal": True,
        "contract_id": pending.get("contract_id"),
        "entry_limit": pending.get("entry_limit"),
        "shadow_execution_label": "no_fill",
        "superseded_by_source_signal_id": replacement_id or None,
        "external_delivery_event_id": pending.get("notification_event_id"),
        "external_delivery_receipt": None,
        "broker_fill_status": "not_observed",
        "broker_order_state": "not_connected",
        "automatic_ordering": False,
    }


def _pending_trade_intent_provider_censored_decision(
    pending: Mapping[str, object],
    *,
    provider_reason: str,
    now: datetime,
    policy: MarketFeatureSettings,
) -> dict[str, object]:
    """Terminate an expired waiter without claiming no-fill when quotes were gated."""

    source_id = str(pending.get("intent_id") or "")
    raw_coordinate = pending.get("coordinate")
    coordinate = dict(raw_coordinate) if isinstance(raw_coordinate, Mapping) else None
    reasons = ["provider_entry_control_blocked_until_expiry"]
    return {
        **strategy_event_fields(
            policy_version_value=policy_version(
                "virtual_rth_pending_provider_censor.v1",
                policy,
            ),
            valid_until=parse_aware_time(pending.get("valid_until")),
            coordinate=coordinate,
            block_reasons=reasons,
        ),
        "event": "virtual_entry_decision",
        "decision_id": f"virtual-entry:{source_id or 'unavailable'}",
        "source_signal_id": source_id or None,
        "source_kind": "trade_intent",
        "source_schema_version": pending.get("schema_version"),
        "source_policy_version": pending.get("policy_version"),
        "source_evaluated_at": pending.get("evaluated_at"),
        "entry_observed_at": _entry_observed_at(pending),
        "action_revalidated_at": now.isoformat(),
        "evaluated_at": now.isoformat(),
        "status": "blocked",
        "terminal": True,
        "contract_id": pending.get("contract_id"),
        "entry_limit": pending.get("entry_limit"),
        "shadow_execution_label": "censored",
        "censor_reason": "provider_entry_control_unavailable",
        "provider_entry_control_reason": provider_reason,
        "external_delivery_event_id": pending.get("notification_event_id"),
        "external_delivery_receipt": None,
        "broker_fill_status": "not_observed",
        "broker_order_state": "not_connected",
        "automatic_ordering": False,
    }


def _new_episode(
    latest: LatestState,
    *,
    trade_intent: Mapping[str, object],
    gth_signal: Mapping[str, object],
    consumed: set[str],
    now: datetime,
    policy: MarketFeatureSettings,
    expected_trade_intent_policy_version: str | None = None,
    require_external_receipt: bool = False,
    external_receipt: ExternalDeliveryReceipt | None = None,
    external_receipt_observable: bool = True,
    external_receipt_error: str | None = None,
) -> tuple[dict[str, object], dict[str, object] | None]:
    if trade_intent.get("status") == "trade_ready":
        source_id = str(trade_intent.get("intent_id") or "")
        contract_id = str(trade_intent.get("contract_id") or "")
        if source_id and source_id not in consumed and contract_id:
            return _evaluate_trade_intent_entry(
                latest,
                trade_intent=trade_intent,
                now=now,
                policy=policy,
                expected_policy_version=expected_trade_intent_policy_version,
                require_external_receipt=require_external_receipt,
                external_receipt=external_receipt,
                external_receipt_observable=external_receipt_observable,
                external_receipt_error=external_receipt_error,
            )
    if gth_signal.get("kind") != "gth_dip_reclaim_call":
        return {}, None
    if (
        str(gth_signal.get("session_date") or "")
        != DEFAULT_MARKET_CALENDAR.research_expiry(now).isoformat()
    ):
        return {}, None
    source_id = str(gth_signal.get("event_id") or "")
    if not source_id or source_id in consumed:
        return {}, None
    return _evaluate_gth_spread_entry(
        latest,
        gth_signal=gth_signal,
        now=now,
        policy=policy,
    )


def _new_gth_spread_episode(
    latest: LatestState,
    *,
    gth_signal: Mapping[str, object],
    now: datetime,
    policy: MarketFeatureSettings,
) -> dict[str, object]:
    episode, _decision = _evaluate_gth_spread_entry(
        latest,
        gth_signal=gth_signal,
        now=now,
        policy=policy,
    )
    return episode


def _evaluate_gth_spread_entry(
    latest: LatestState,
    *,
    gth_signal: Mapping[str, object],
    now: datetime,
    policy: MarketFeatureSettings,
) -> tuple[dict[str, object], dict[str, object]]:
    """Evaluate one signal without turning repeated quote refreshes into opportunities."""

    now = _utc(now)
    source_id = str(gth_signal.get("event_id") or "")
    session_date = str(gth_signal.get("session_date") or "")
    spread = gth_signal.get("spread")
    decision_policy = policy_version("virtual_gth_exact_spread_entry.v3", policy)

    def result(
        reasons: tuple[str, ...] | list[str],
        *,
        terminal: bool,
        snapshot: Mapping[str, object] | None = None,
        episode: Mapping[str, object] | None = None,
    ) -> tuple[dict[str, object], dict[str, object]]:
        raw_coordinate = gth_signal.get("coordinate")
        coordinate = dict(raw_coordinate) if isinstance(raw_coordinate, Mapping) else None
        valid_until = parse_aware_time(gth_signal.get("valid_until"))
        normalized = normalize_block_reasons(reasons)
        status = "virtual_ready" if episode else "blocked" if terminal else "observing"
        token = (
            source_id
            or hashlib.sha256(
                json.dumps(dict(gth_signal), sort_keys=True, default=str).encode()
            ).hexdigest()[:24]
        )
        decision = {
            **strategy_event_fields(
                policy_version_value=decision_policy,
                valid_until=valid_until,
                coordinate=coordinate,
                block_reasons=normalized,
            ),
            "event": "virtual_entry_decision",
            "decision_id": f"virtual-entry:{token}",
            "source_signal_id": source_id or None,
            "source_kind": "gth_dip_reclaim_call",
            "source_schema_version": gth_signal.get("schema_version"),
            "source_policy_version": gth_signal.get("policy_version"),
            "source_evaluated_at": gth_signal.get("confirmed_at"),
            "session_id": session_date or None,
            "evaluated_at": now.isoformat(),
            "action_revalidated_at": now.isoformat(),
            "quote_state_created_at": _latest_created_at(latest),
            "status": status,
            "terminal": bool(terminal or episode),
            "position_type": "call_debit_spread",
            "simulation_only": True,
            "execution_eligible": False,
            "broker_position_effect": "none",
            "exact_spread_snapshot": dict(snapshot) if snapshot else None,
            "episode_id": episode.get("episode_id") if episode else None,
            "automatic_ordering": False,
        }
        return dict(episode or {}), decision

    if not source_id:
        return result(["source_signal_id_unavailable"], terminal=True)
    if gth_signal.get("kind") != "gth_dip_reclaim_call":
        return result(["source_signal_kind_mismatch"], terminal=True)
    source_contract_issues = actionable_strategy_contract_issues(gth_signal, now=now)
    if source_contract_issues:
        reasons = [
            "signal_expired" if issue == "strategy_event_expired" else issue
            for issue in source_contract_issues
        ]
        return result(reasons, terminal=True)
    if not str(gth_signal.get("policy_version") or "").startswith("gth_dip_reclaim.v4+sha256:"):
        return result(["source_policy_incompatible"], terminal=True)
    coordinate = gth_signal.get("coordinate")
    if not isinstance(coordinate, Mapping) or coordinate.get("kind") != "raw_es":
        return result(["source_coordinate_mismatch"], terminal=True)
    if session_date != DEFAULT_MARKET_CALENDAR.research_expiry(now).isoformat():
        return result(["signal_session_mismatch"], terminal=True)
    if not isinstance(spread, Mapping):
        return result(["exact_spread_contract_unavailable"], terminal=True)
    signal_age = _gth_signal_age_seconds(
        gth_signal,
        now=now,
        future_tolerance_seconds=policy.provider_sync_tolerance_seconds,
    )
    if signal_age is None:
        confirmed_at = _time(gth_signal.get("confirmed_at"))
        if confirmed_at is not None and confirmed_at > now:
            return result(["signal_timestamp_in_future"], terminal=False)
        return result(["signal_time_contract_invalid"], terminal=True)
    if spread.get("expiry_date") != session_date:
        return result(["spread_expiry_mismatch"], terminal=True)
    contract_ids = _gth_spread_contract_ids(spread, session_date=session_date)
    if contract_ids is None:
        return result(["spread_contract_invalid"], terminal=True)
    long_contract_id, short_contract_id = contract_ids
    snapshot, quote_reasons = _spread_snapshot_decision(
        latest,
        long_contract_id=long_contract_id,
        short_contract_id=short_contract_id,
        now=now,
        max_quote_age_seconds=policy.trade_quote_max_age_seconds,
        max_quote_skew_seconds=policy.provider_sync_tolerance_seconds,
        required_provider="ibkr",
    )
    if not snapshot:
        return result(quote_reasons, terminal=False)
    width = _number(spread.get("width_points"))
    long_strike = _number(spread.get("long_strike"))
    short_strike = _number(spread.get("short_strike"))
    executable_ask = _number(snapshot.get("ask"))
    if (
        width is None
        or long_strike is None
        or short_strike is None
        or not math.isclose(width, short_strike - long_strike)
    ):
        return result(["spread_width_invalid"], terminal=True, snapshot=snapshot)
    if executable_ask is None or executable_ask <= 0:
        return result(["spread_debit_not_positive"], terminal=False, snapshot=snapshot)
    if executable_ask >= width:
        return result(["spread_debit_not_below_width"], terminal=False, snapshot=snapshot)
    target_spx = _number(spread.get("target_wall"))
    if target_spx is not None:
        spx_underlier = _gth_chain_reference(
            latest,
            now=now,
            expiry=session_date.replace("-", ""),
            policy=policy,
        )
        if not spx_underlier:
            return result(
                ["chain_implied_target_unavailable"],
                terminal=False,
                snapshot=snapshot,
            )
        snapshot["action_spx_underlier"] = spx_underlier
        if _level_reached(
            _number(spx_underlier.get("price")),
            target_spx,
            direction="up",
            target=True,
        ):
            return result(["target_reached_before_entry_quote"], terminal=True, snapshot=snapshot)
    invalidation_es = _number(spread.get("invalidation_es"))
    signal_trough = _number(gth_signal.get("trough"))
    if invalidation_es is None:
        invalidation_es = signal_trough
    elif signal_trough is not None and not math.isclose(
        invalidation_es,
        signal_trough,
        abs_tol=1e-9,
    ):
        return result(
            ["gth_invalidation_contract_mismatch"],
            terminal=True,
            snapshot=snapshot,
        )
    if invalidation_es is None:
        return result(["gth_invalidation_unavailable"], terminal=True, snapshot=snapshot)
    es_underlier, underlier_reasons = _action_underlier_snapshot(
        latest,
        instrument_id="future:ES",
        now=now,
        max_quote_age_seconds=policy.trade_quote_max_age_seconds,
        future_tolerance_seconds=policy.provider_sync_tolerance_seconds,
    )
    if not es_underlier:
        return result(underlier_reasons, terminal=False, snapshot=snapshot)
    snapshot["action_es_underlier"] = es_underlier
    if _level_reached(
        _number(es_underlier.get("price")),
        invalidation_es,
        direction="up",
        target=False,
    ):
        return result(["invalidation_reached_before_entry_quote"], terminal=True, snapshot=snapshot)
    stop = _gth_time_stop(now, policy=policy)
    signal_stop = _time(spread.get("exit_at"))
    if signal_stop is not None:
        stop = min(stop, signal_stop)
    if stop <= now:
        return result(["gth_exit_clock_elapsed"], terminal=True, snapshot=snapshot)
    position_id = f"{long_contract_id}|-{short_contract_id}"
    episode = _episode(
        source_id=source_id,
        source_kind="gth_dip_reclaim_call",
        direction="up",
        contract_id=position_id,
        snapshot=snapshot,
        now=now,
        stop=stop,
        invalidation_spx=None,
        target_spx=target_spx,
        invalidation_es=invalidation_es,
        source_contract=gth_signal,
        lifecycle_policy=policy,
    )
    episode.update(
        {
            "position_type": "call_debit_spread",
            "long_contract_id": long_contract_id,
            "short_contract_id": short_contract_id,
            "spread_width_points": spread.get("width_points"),
            "entry_basis": "two_leg_decision_quote_snapshot",
            "entry_bid": snapshot.get("bid"),
            "entry_ask": snapshot.get("ask"),
            "signal_age_seconds": signal_age,
            "decision_evaluated_at": gth_signal.get("confirmed_at"),
            "action_revalidated_at": now.isoformat(),
            "quote_state_created_at": _latest_created_at(latest),
            "legs": (
                {"side": "long", "quantity": 1, "contract_id": long_contract_id},
                {"side": "short", "quantity": -1, "contract_id": short_contract_id},
            ),
        }
    )
    return result([], terminal=True, snapshot=snapshot, episode=episode)


def _active_snapshot(
    latest: LatestState,
    active: Mapping[str, object],
    *,
    now: datetime,
    policy: MarketFeatureSettings,
) -> dict[str, object]:
    return _active_snapshot_impl(
        latest,
        active,
        now=now,
        policy=policy,
        spread_snapshot=_spread_snapshot,
    )
