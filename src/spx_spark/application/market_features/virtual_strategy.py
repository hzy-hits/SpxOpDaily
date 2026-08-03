"""Broker-independent lifecycle for the system's own 0DTE strategy episode."""

from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timedelta
from pathlib import Path
from typing import Mapping

from spx_spark.application.market_features.trade_intent import (
    live_trade_intent_authority_issues,
)
from spx_spark.application.market_features.virtual_strategy_support import (
    _action_underlier_snapshot,
    _active_snapshot_impl,
    _append_audit,
    _cap_rth_trade_episode,
    _contract_snapshot,
    _episode,
    _event_contract,
    _exit_clock as _exit_clock,
    _exit_decision,
    _fmt as _fmt,
    _gth_chain_reference,
    _gth_signal_age_seconds,
    _gth_spread_contract_ids,
    _gth_time_stop,
    _latest_created_at,
    _lifecycle_transition,
    _number,
    _pct as _pct,
    _record_entry_decision,
    _record_due_horizons,
    _render_exit,
    _rth_trade_hard_exit as _rth_trade_hard_exit,
    _should_replace_with_gth_spread,
    _spx_reference as _spx_reference,
    _state_path,
    _time,
    _trim_entry_decisions,
    _utc,
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
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock, read_json_object
from spx_spark.storage import LatestState, configured_quote_use_decision
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
            active, entry_decision = _new_episode(
                latest,
                trade_intent=trade_intent,
                gth_signal=gth_signal,
                consumed=consumed,
                now=now,
                policy=policy,
                expected_trade_intent_policy_version=expected_trade_intent_policy_version,
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
            if active:
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

        closed = {
            **active,
            **pending_exit_context,
            **_event_contract(active, block_reasons=()),
            "status": "closed",
            "closed_at": now.isoformat(),
            "exit_reason": exit_reason,
            "exit_action": action,
            "exit_snapshot": current,
            "exit_bid": _number(current.get("bid")),
            "exit_price_basis": (
                "executable_bid" if _number(current.get("bid")) is not None else None
            ),
            "pnl_status": (
                "executable_quote_observed"
                if _number(current.get("bid")) is not None
                else "unavailable"
            ),
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


def _new_episode(
    latest: LatestState,
    *,
    trade_intent: Mapping[str, object],
    gth_signal: Mapping[str, object],
    consumed: set[str],
    now: datetime,
    policy: MarketFeatureSettings,
    expected_trade_intent_policy_version: str | None = None,
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


def _evaluate_trade_intent_entry(
    latest: LatestState,
    *,
    trade_intent: Mapping[str, object],
    now: datetime,
    policy: MarketFeatureSettings,
    expected_policy_version: str | None,
) -> tuple[dict[str, object], dict[str, object]]:
    """Revalidate a quote-reached RTH candidate immediately before virtual action."""

    now = _utc(now)
    source_id = str(trade_intent.get("intent_id") or "")
    contract_id = str(trade_intent.get("contract_id") or "")
    decision_policy = policy_version(
        "virtual_rth_action_revalidation.v3",
        {
            "market_features": policy,
            "expected_source_policy_version": expected_policy_version,
        },
    )

    def result(
        reasons: tuple[str, ...] | list[str],
        *,
        terminal: bool,
        snapshot: Mapping[str, object] | None = None,
        episode: Mapping[str, object] | None = None,
    ) -> tuple[dict[str, object], dict[str, object]]:
        raw_coordinate = trade_intent.get("coordinate")
        coordinate = dict(raw_coordinate) if isinstance(raw_coordinate, Mapping) else None
        normalized = normalize_block_reasons(reasons)
        return dict(episode or {}), {
            **strategy_event_fields(
                policy_version_value=decision_policy,
                valid_until=parse_aware_time(trade_intent.get("valid_until")),
                coordinate=coordinate,
                block_reasons=normalized,
            ),
            "event": "virtual_entry_decision",
            "decision_id": f"virtual-entry:{source_id or 'unavailable'}",
            "source_signal_id": source_id or None,
            "source_kind": "trade_intent",
            "source_schema_version": trade_intent.get("schema_version"),
            "source_policy_version": trade_intent.get("policy_version"),
            "source_evaluated_at": trade_intent.get("evaluated_at"),
            "entry_observed_at": _entry_observed_at(trade_intent),
            "action_revalidated_at": now.isoformat(),
            "quote_state_created_at": _latest_created_at(latest),
            "evaluated_at": now.isoformat(),
            "status": "trade_ready" if episode else "blocked" if terminal else "observing",
            "terminal": bool(terminal or episode),
            "contract_id": contract_id or None,
            "entry_limit": trade_intent.get("entry_limit"),
            "action_quote_snapshot": dict(snapshot) if snapshot else None,
            "episode_id": episode.get("episode_id") if episode else None,
            "automatic_ordering": False,
        }

    if not source_id:
        return result(["source_signal_id_unavailable"], terminal=True)
    if not contract_id:
        return result(["execution_contract_unavailable"], terminal=True)
    authority_issues = live_trade_intent_authority_issues(trade_intent)
    if authority_issues:
        return result(authority_issues, terminal=True)
    contract_issues = list(actionable_strategy_contract_issues(trade_intent, now=now))
    if contract_issues:
        reasons = [
            "intent_expired" if issue == "strategy_event_expired" else issue
            for issue in contract_issues
        ]
        return result(reasons, terminal=True)
    source_policy = str(trade_intent.get("policy_version") or "")
    if not source_policy.startswith("rth_trade_intent.v3+sha256:"):
        return result(["source_policy_incompatible"], terminal=True)
    if expected_policy_version and source_policy != expected_policy_version:
        return result(["source_policy_version_drift"], terminal=True)
    coordinate = trade_intent.get("coordinate")
    if not isinstance(coordinate, Mapping) or coordinate.get("kind") != "official_spx":
        return result(["source_coordinate_mismatch"], terminal=True)

    snapshot, quote_reasons = _trade_intent_action_snapshot(
        latest,
        trade_intent=trade_intent,
        now=now,
        max_quote_age_seconds=policy.trade_quote_max_age_seconds,
        future_tolerance_seconds=policy.provider_sync_tolerance_seconds,
    )
    if not snapshot:
        return result(quote_reasons, terminal=False)
    underlier, underlier_reasons = _action_underlier_snapshot(
        latest,
        instrument_id="index:SPX",
        now=now,
        max_quote_age_seconds=policy.trade_quote_max_age_seconds,
        future_tolerance_seconds=policy.provider_sync_tolerance_seconds,
    )
    if not underlier:
        return result(underlier_reasons, terminal=False, snapshot=snapshot)
    direction = str(trade_intent.get("direction") or "")
    target_spx = _number(trade_intent.get("target_spx"))
    invalidation_spx = _number(trade_intent.get("invalidation_spx"))
    if target_spx is None or invalidation_spx is None:
        return result(["action_underlier_guard_unavailable"], terminal=True, snapshot=snapshot)
    spx = _number(underlier.get("price"))
    if _level_reached(spx, target_spx, direction=direction, target=True):
        snapshot["action_underlier"] = underlier
        return result(["target_reached_before_entry_quote"], terminal=True, snapshot=snapshot)
    if _level_reached(spx, invalidation_spx, direction=direction, target=False):
        snapshot["action_underlier"] = underlier
        return result(["invalidation_reached_before_entry_quote"], terminal=True, snapshot=snapshot)
    snapshot["action_underlier"] = underlier
    stop = _time(trade_intent.get("time_stop_at")) or now + timedelta(
        minutes=policy.trade_time_stop_minutes
    )
    if stop <= now:
        return result(["trade_time_stop_elapsed"], terminal=True, snapshot=snapshot)
    episode = _episode(
        source_id=source_id,
        source_kind="trade_intent",
        direction=direction,
        contract_id=contract_id,
        snapshot=snapshot,
        now=now,
        stop=stop,
        invalidation_spx=invalidation_spx,
        target_spx=target_spx,
        invalidation_es=None,
        source_contract=trade_intent,
        lifecycle_policy=policy,
    )
    if not episode:
        return result(["trade_direction_invalid"], terminal=True, snapshot=snapshot)
    episode.update(
        {
            "decision_evaluated_at": trade_intent.get("evaluated_at"),
            "entry_observed_at": _entry_observed_at(trade_intent),
            "action_revalidated_at": now.isoformat(),
            "quote_state_created_at": _latest_created_at(latest),
            "entry_limit": trade_intent.get("entry_limit"),
            "entry_basis": "action_revalidated_quote_snapshot",
        }
    )
    return result([], terminal=True, snapshot=snapshot, episode=episode)


def _trade_intent_action_snapshot(
    latest: LatestState,
    *,
    trade_intent: Mapping[str, object],
    now: datetime,
    max_quote_age_seconds: float,
    future_tolerance_seconds: float,
) -> tuple[dict[str, object], list[str]]:
    """Reload-sensitive NBBO/limit check for the final virtual-entry boundary."""

    now = _utc(now)
    contract_id = str(trade_intent.get("contract_id") or "")
    quote = latest.best_quote(contract_id) if contract_id else None
    if quote is None:
        return {}, ["action_quote_unavailable"]
    entry_limit = _number(trade_intent.get("entry_limit"))
    if entry_limit is None or entry_limit <= 0:
        return {}, ["action_entry_limit_invalid"]
    observation = trade_intent.get("entry_observation")
    if not isinstance(observation, Mapping):
        return {}, ["entry_observation_unavailable"]
    observation_limit = _number(observation.get("entry_limit"))
    if (
        observation.get("entry_condition") != "displayed_ask_at_or_below_limit"
        or str(observation.get("contract_id") or "") != contract_id
        or observation_limit is None
        or not math.isclose(observation_limit, entry_limit)
    ):
        return {}, ["entry_observation_contract_invalid"]

    provider = str(trade_intent.get("provider") or "")
    if not provider:
        return {}, ["action_quote_provider_unavailable"]
    if quote.provider.value != provider:
        return {}, ["action_quote_provider_mismatch"]
    bid = _number(quote.bid)
    mid = _number(quote.mid)
    ask = _number(quote.ask)
    if bid is None or mid is None or ask is None or not 0 <= bid <= mid <= ask:
        return {}, ["action_quote_nbbo_invalid"]
    # This decision consumes bid/ask, so only the NBBO's own quote clock can
    # authorize freshness. A new last trade cannot freshen the displayed ask.
    source_at = quote.quote_time
    transport_at = quote.last_update_at or quote.received_at
    if source_at is None:
        return {}, ["action_quote_source_time_unavailable"]
    source_age = (now - _utc(source_at)).total_seconds()
    transport_age = (now - _utc(transport_at)).total_seconds()
    time_reasons: list[str] = []
    tolerance = max(0.0, future_tolerance_seconds)
    if source_age < -tolerance:
        time_reasons.append("action_quote_source_in_future")
    elif source_age > max_quote_age_seconds:
        time_reasons.append("action_quote_source_stale")
    if transport_age < -tolerance:
        time_reasons.append("action_quote_transport_in_future")
    elif transport_age > max_quote_age_seconds:
        time_reasons.append("action_quote_transport_stale")
    if time_reasons:
        return {}, time_reasons
    use = configured_quote_use_decision(quote, as_of=now)
    if not use.pricing_allowed:
        return {}, [f"action_quote_quality_{use.reason}"]
    if ask > entry_limit:
        return {}, ["action_entry_limit_not_reached"]

    snapshot = _contract_snapshot(latest, contract_id, now=now)
    if not snapshot:
        return {}, ["action_contract_snapshot_unavailable"]
    snapshot.update(
        {
            "action_revalidated_at": now.isoformat(),
            "source_age_seconds": source_age,
            "transport_age_seconds": transport_age,
            "entry_limit": entry_limit,
            "entry_limit_satisfied": True,
        }
    )
    return snapshot, []


def _entry_observed_at(trade_intent: Mapping[str, object]) -> object:
    observation = trade_intent.get("entry_observation")
    return observation.get("at") if isinstance(observation, Mapping) else None


def _level_reached(
    price: float | None,
    level: float | None,
    *,
    direction: str,
    target: bool,
) -> bool:
    if price is None or level is None or direction not in {"up", "down"}:
        return False
    if target:
        return price >= level if direction == "up" else price <= level
    return price <= level if direction == "up" else price >= level


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
