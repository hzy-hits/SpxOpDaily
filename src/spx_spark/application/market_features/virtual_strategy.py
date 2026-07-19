"""Broker-independent lifecycle for the system's own 0DTE strategy episode."""

from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timedelta
from typing import Mapping

from spx_spark.config import NotificationSettings, StorageSettings
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.notifier.dispatcher import enqueue_notification
from spx_spark.notifier.model import CommandRunner, default_runner
from spx_spark.notifier.receipts import NotificationEnvelope
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock, read_json_object
from spx_spark.storage import LatestState, configured_quote_use_decision
from spx_spark.application.market_features.virtual_strategy_support import (
    _append_audit,
    _contract_snapshot,
    _episode,
    _event_contract,
    _exit_clock as _exit_clock,
    _exit_decision,
    _fmt as _fmt,
    _gth_signal_age_seconds,
    _gth_spread_contract_ids,
    _gth_time_stop,
    _latest_created_at,
    _number,
    _pct as _pct,
    _record_entry_decision,
    _record_due_horizons,
    _render_exit,
    _should_replace_with_gth_spread,
    _spx_reference as _spx_reference,
    _state_path,
    _time,
    _trim_entry_decisions,
    _utc,
)
from spx_spark.strategy_contract import (
    actionable_strategy_contract_issues,
    normalize_block_reasons,
    parse_aware_time,
    policy_version,
    strategy_event_fields,
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
    expected_trade_intent_policy_version: str | None = None,
    notification: NotificationSettings | None = None,
    runner: CommandRunner = default_runner,
) -> dict[str, object]:
    """Open/update/close one virtual episode; never reads or writes broker positions."""

    now = _utc(now)
    if not policy.virtual_strategy_enabled:
        return {"status": "disabled", "notification_attempted": False}
    state_path = _state_path(storage)
    with exclusive_state_lock(state_path):
        state = read_json_object(state_path)
        active = dict(state.get("active") or {})
        consumed = set(str(item) for item in state.get("consumed_signal_ids") or [])
        entry_decisions = {
            str(key): dict(value)
            for key, value in dict(state.get("entry_decisions") or {}).items()
            if isinstance(value, Mapping)
        }
        if _should_replace_with_gth_spread(active, gth_signal):
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
                if signal_id:
                    consumed.add(signal_id)
                _append_audit(storage, now, {"event": "virtual_opened", **active})
            elif entry_decision.get("terminal") is True:
                source_id = str(entry_decision.get("source_signal_id") or "")
                if source_id:
                    consumed.add(source_id)
        if not active:
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
                    if source_id:
                        consumed.add(source_id)
            if active:
                signal_id = str(active.get("source_signal_id") or "")
                if signal_id:
                    consumed.add(signal_id)
                _append_audit(storage, now, {"event": "virtual_opened", **active})
        if not active:
            state.update(
                {
                    "schema_version": 1,
                    "updated_at": now.isoformat(),
                    "active": None,
                    "consumed_signal_ids": sorted(consumed)[-200:],
                    "entry_decisions": _trim_entry_decisions(entry_decisions),
                }
            )
            atomic_write_json_secure(state_path, state)
            return {"status": "observing", "notification_attempted": False}

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
        if exit_reason is None:
            state.update(
                {
                    "schema_version": 1,
                    "updated_at": now.isoformat(),
                    "active": active,
                    "consumed_signal_ids": sorted(consumed)[-200:],
                    "entry_decisions": _trim_entry_decisions(entry_decisions),
                }
            )
            atomic_write_json_secure(state_path, state)
            return {
                "status": "active",
                "episode_id": active.get("episode_id"),
                "contract_id": active.get("contract_id"),
                "notification_attempted": False,
            }

        closed = {
            **active,
            **_event_contract(active, block_reasons=()),
            "status": "closed",
            "closed_at": now.isoformat(),
            "exit_reason": exit_reason,
            "exit_action": action,
            "exit_snapshot": current,
        }
        state.update(
            {
                "schema_version": 1,
                "updated_at": now.isoformat(),
                "active": None,
                "last_closed": closed,
                "consumed_signal_ids": sorted(consumed)[-200:],
                "entry_decisions": _trim_entry_decisions(entry_decisions),
            }
        )
        atomic_write_json_secure(state_path, state)
        _append_audit(storage, now, {"event": "virtual_closed", **closed})

    settings = notification or NotificationSettings.from_env()
    text = _render_exit(closed)
    enqueued_at = now
    result = enqueue_notification(
        settings,
        NotificationEnvelope(
            event_id=f"{closed['episode_id']}:{exit_reason}",
            source="virtual_strategy",
            kind="virtual_strategy_exit",
            lane="strategy_lifecycle",
            occurred_at=now,
        ),
        title="SPX VIRTUAL STRATEGY EXIT",
        text=text,
        friend=True,
        feishu_text=text,
        enqueued_at=enqueued_at,
    )
    return {
        "status": "closed",
        "episode_id": closed.get("episode_id"),
        "exit_reason": exit_reason,
        "exit_action": action,
        "notification_attempted": True,
        "notification_accepted": result.accepted,
        "notification_inserted": result.inserted,
        "notification_duplicate": result.duplicate,
        "notification_delivered": result.delivered,
        "notification_queued": result.queued_for_recovery,
        "notification_outcome": result.outcome,
        "notification_enqueued_at": enqueued_at.isoformat(),
        "targets": list(result.targets),
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
        if (
            source_id
            and source_id not in consumed
            and contract_id
        ):
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
    if (
        bid is None
        or mid is None
        or ask is None
        or not 0 <= bid <= mid <= ask
    ):
        return {}, ["action_quote_nbbo_invalid"]
    source_at = quote.quote_time or quote.trade_time
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


def _action_underlier_snapshot(
    latest: LatestState,
    *,
    instrument_id: str,
    now: datetime,
    max_quote_age_seconds: float,
    future_tolerance_seconds: float,
) -> tuple[dict[str, object], list[str]]:
    """Return one fresh action-time underlier observation without clock fallbacks."""

    quote = latest.best_quote(instrument_id)
    if quote is None:
        return {}, [f"action_underlier_unavailable:{instrument_id}"]
    source_at = quote.quote_time or quote.trade_time
    if source_at is None:
        return {}, [f"action_underlier_source_time_unavailable:{instrument_id}"]
    transport_at = quote.last_update_at or quote.received_at
    source_age = (_utc(now) - _utc(source_at)).total_seconds()
    transport_age = (_utc(now) - _utc(transport_at)).total_seconds()
    tolerance = max(0.0, future_tolerance_seconds)
    reasons: list[str] = []
    if source_age < -tolerance:
        reasons.append(f"action_underlier_source_in_future:{instrument_id}")
    elif source_age > max_quote_age_seconds:
        reasons.append(f"action_underlier_source_stale:{instrument_id}")
    if transport_age < -tolerance:
        reasons.append(f"action_underlier_transport_in_future:{instrument_id}")
    elif transport_age > max_quote_age_seconds:
        reasons.append(f"action_underlier_transport_stale:{instrument_id}")
    if reasons:
        return {}, reasons
    use = configured_quote_use_decision(quote, as_of=_utc(now))
    price = _number(quote.effective_price)
    if not use.pricing_allowed or price is None:
        return {}, [f"action_underlier_not_pricing_allowed:{instrument_id}"]
    return (
        {
            "instrument_id": instrument_id,
            "price": price,
            "provider": quote.provider.value,
            "source_at": _utc(source_at).isoformat(),
            "transport_at": _utc(transport_at).isoformat(),
            "source_age_seconds": source_age,
            "transport_age_seconds": transport_age,
        },
        [],
    )


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
        status = "trade_ready" if episode else "blocked" if terminal else "observing"
        token = source_id or hashlib.sha256(
            json.dumps(dict(gth_signal), sort_keys=True, default=str).encode()
        ).hexdigest()[:24]
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
    if not str(gth_signal.get("policy_version") or "").startswith(
        "gth_dip_reclaim.v3+sha256:"
    ):
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
        spx_underlier, underlier_reasons = _action_underlier_snapshot(
            latest,
            instrument_id="index:SPX",
            now=now,
            max_quote_age_seconds=policy.trade_quote_max_age_seconds,
            future_tolerance_seconds=policy.provider_sync_tolerance_seconds,
        )
        if not spx_underlier:
            return result(underlier_reasons, terminal=False, snapshot=snapshot)
        snapshot["action_spx_underlier"] = spx_underlier
        if _level_reached(
            _number(spx_underlier.get("price")),
            target_spx,
            direction="up",
            target=True,
        ):
            return result(["target_reached_before_entry_quote"], terminal=True, snapshot=snapshot)
    invalidation_es = _number(spread.get("invalidation_es"))
    if invalidation_es is None:
        invalidation_es = _number(gth_signal.get("trough"))
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
    if active.get("position_type") == "call_debit_spread":
        return _spread_snapshot(
            latest,
            long_contract_id=str(active.get("long_contract_id") or ""),
            short_contract_id=str(active.get("short_contract_id") or ""),
            now=now,
            max_quote_age_seconds=policy.trade_quote_max_age_seconds,
            max_quote_skew_seconds=policy.provider_sync_tolerance_seconds,
            required_provider=(
                "ibkr" if active.get("source_kind") == "gth_dip_reclaim_call" else None
            ),
        )
    return _contract_snapshot(latest, str(active.get("contract_id") or ""), now=now)


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
    """Mark a 1x/-1x debit spread from two simultaneously usable leg snapshots."""

    snapshot, _reasons = _spread_snapshot_decision(
        latest,
        long_contract_id=long_contract_id,
        short_contract_id=short_contract_id,
        now=now,
        max_quote_age_seconds=max_quote_age_seconds,
        max_quote_skew_seconds=max_quote_skew_seconds,
        required_provider=required_provider,
    )
    return snapshot


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
    """Return an exact two-leg snapshot or stable, auditable rejection reasons."""

    if not long_contract_id or not short_contract_id:
        return {}, ["spread_contract_id_unavailable"]
    long = _contract_snapshot(latest, long_contract_id, now=now)
    short = _contract_snapshot(latest, short_contract_id, now=now)
    missing_reasons = []
    if not long:
        missing_reasons.append("long_leg_quote_unavailable")
    if not short:
        missing_reasons.append("short_leg_quote_unavailable")
    if missing_reasons:
        return {}, missing_reasons
    long_provider = str(long.get("provider") or "")
    short_provider = str(short.get("provider") or "")
    if not long_provider or not short_provider:
        return {}, ["spread_leg_provider_unavailable"]
    if long_provider != short_provider:
        return {}, ["spread_leg_provider_mismatch"]
    if required_provider and long_provider != required_provider:
        return {}, ["spread_provider_not_ibkr"]
    long_bid = _number(long.get("bid"))
    long_mid = _number(long.get("mid"))
    long_ask = _number(long.get("ask"))
    short_bid = _number(short.get("bid"))
    short_mid = _number(short.get("mid"))
    short_ask = _number(short.get("ask"))
    if (
        long_bid is None
        or long_mid is None
        or long_ask is None
        or short_bid is None
        or short_mid is None
        or short_ask is None
        or not 0 <= long_bid <= long_mid <= long_ask
        or not 0 <= short_bid <= short_mid <= short_ask
    ):
        return {}, ["spread_leg_nbbo_invalid"]
    long_source_at = _time(long.get("source_at"))
    short_source_at = _time(short.get("source_at"))
    if long_source_at is None or short_source_at is None:
        return {}, ["spread_leg_source_time_unavailable"]
    long_transport_at = _time(long.get("transport_at"))
    short_transport_at = _time(short.get("transport_at"))
    if long_transport_at is None or short_transport_at is None:
        return {}, ["spread_leg_transport_time_unavailable"]
    long_age = (now - long_source_at).total_seconds()
    short_age = (now - short_source_at).total_seconds()
    long_transport_age = (now - long_transport_at).total_seconds()
    short_transport_age = (now - short_transport_at).total_seconds()
    source_skew = abs((long_source_at - short_source_at).total_seconds())
    transport_skew = abs((long_transport_at - short_transport_at).total_seconds())
    time_reasons: list[str] = []
    if long_age < -1.0:
        time_reasons.append("long_leg_quote_in_future")
    elif long_age > max_quote_age_seconds:
        time_reasons.append("long_leg_quote_stale")
    if short_age < -1.0:
        time_reasons.append("short_leg_quote_in_future")
    elif short_age > max_quote_age_seconds:
        time_reasons.append("short_leg_quote_stale")
    if long_transport_age < -1.0:
        time_reasons.append("long_leg_transport_in_future")
    elif long_transport_age > max_quote_age_seconds:
        time_reasons.append("long_leg_transport_stale")
    if short_transport_age < -1.0:
        time_reasons.append("short_leg_transport_in_future")
    elif short_transport_age > max_quote_age_seconds:
        time_reasons.append("short_leg_transport_stale")
    if source_skew > max_quote_skew_seconds:
        time_reasons.append("spread_leg_source_timestamp_skew")
    if transport_skew > max_quote_skew_seconds:
        time_reasons.append("spread_leg_transport_timestamp_skew")
    if time_reasons:
        return {}, time_reasons
    net_bid = long_bid - short_ask
    net_mid = long_mid - short_mid
    net_ask = long_ask - short_bid
    if net_mid <= 0 or net_ask <= 0 or not net_bid <= net_mid <= net_ask:
        return {}, ["spread_net_debit_invalid"]

    long_quality = long.get("quality") if isinstance(long.get("quality"), Mapping) else {}
    short_quality = short.get("quality") if isinstance(short.get("quality"), Mapping) else {}
    quality_ok = long_quality.get("status") == "ok" and short_quality.get("status") == "ok"
    if not quality_ok:
        return {}, ["spread_leg_quality_blocked"]
    result: dict[str, object] = {
        "at": now.isoformat(),
        "mid": net_mid,
        "bid": net_bid,
        "ask": net_ask,
        "iv": long.get("iv"),
        "underlier": long.get("underlier"),
        "long_quote_age_seconds": long_age,
        "short_quote_age_seconds": short_age,
        "long_transport_age_seconds": long_transport_age,
        "short_transport_age_seconds": short_transport_age,
        "leg_source_skew_seconds": source_skew,
        "leg_transport_skew_seconds": transport_skew,
        "quality": {
            "status": "ok",
            "long": dict(long_quality),
            "short": dict(short_quality),
        },
        "long": long,
        "short": short,
    }
    for field in (
        "delta",
        "gamma_per_point",
        "color_gamma_per_minute",
        "speed_gamma_per_point",
        "theta_per_minute",
        "vanna_delta_per_vol_point",
    ):
        result[field] = _spread_quote_value(long.get(field), short.get(field))
    return result, []


def _spread_quote_value(long_value: object, short_value: object) -> float | None:
    long_number = _number(long_value)
    short_number = _number(short_value)
    if long_number is None or short_number is None:
        return None
    return long_number - short_number
