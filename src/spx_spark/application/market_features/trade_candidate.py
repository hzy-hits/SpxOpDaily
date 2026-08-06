"""Quote-observed lifecycle for an alert candidate; never represents a broker order."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Mapping

from spx_spark.application.market_features.trade_intent import (
    ET,
    TRADE_INTENT_CONTRACT_VERSION,
    live_trade_intent_authority_issues,
)
from spx_spark.application.market_features.put_shadow_contract import (
    LEGACY_PUT_SHADOW_SOURCE_CONTRACT_VERSION,
    PUT_SHADOW_CANDIDATE_CONTRACT_VERSION,
    PUT_SHADOW_CONSUMED_IDENTITY_LIMIT,
    PUT_SHADOW_ENTRY_WINDOW_START_ET,
    PUT_SHADOW_EXACT_QUOTE_MAX_AGE_SECONDS,
    PUT_SHADOW_EXACT_QUOTE_POLICY_VERSION,
    PUT_SHADOW_HARD_EXIT_ET,
    PUT_SHADOW_LANE_CONTRACTS,
    PUT_SHADOW_LANES,
    PUT_SHADOW_STATE_SCHEMA_VERSION,
)
from spx_spark.application.market_features.trade_candidate_quote import (
    candidate_displayed_quote_decision,
)
from spx_spark.application.order_map.spot import actionable_live_price
from spx_spark.config import StorageSettings
from spx_spark.marketdata import (
    Provider,
    Quote,
    choose_best_quote,
    instrument_matches_id,
    quote_use_decision,
)
from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock, read_json_object
from spx_spark.storage import LatestState
from spx_spark.strategy_contract import (
    STRATEGY_EVENT_SCHEMA_VERSION,
    actionable_strategy_contract_issues,
    parse_aware_time,
    policy_version,
    strategy_event_fields,
)


class CandidatePhase(str, Enum):
    OBSERVING = "observing"
    ARMED = "armed"
    QUOTE_REACHED_ENTRY = "quote_reached_entry"
    TARGET_PASSED = "target_passed"
    INVALIDATED = "invalidated"
    EXPIRED = "expired"
    SUPERSEDED = "superseded"


TERMINAL_PHASES = frozenset(
    {
        CandidatePhase.QUOTE_REACHED_ENTRY,
        CandidatePhase.TARGET_PASSED,
        CandidatePhase.INVALIDATED,
        CandidatePhase.EXPIRED,
        CandidatePhase.SUPERSEDED,
    }
)


def advance_trade_candidate(
    storage: StorageSettings,
    latest: LatestState,
    intent: Mapping[str, object],
    *,
    now: datetime,
) -> dict[str, object]:
    """Advance one alert candidate using displayed quotes and SPX, without fill claims."""

    now = _utc(now)
    state_path = _state_path(storage)
    with exclusive_state_lock(state_path):
        state = read_json_object(state_path)
        active = dict(state.get("active") or {})
        completed_candidates = {
            str(key): dict(value)
            for key, value in dict(state.get("completed_candidates") or {}).items()
            if isinstance(value, Mapping)
        }
        completed = set(completed_candidates)
        completed.update(str(item) for item in state.get("completed_candidate_ids") or [])
        incoming_id = (
            _candidate_id(intent)
            if intent.get("status") == "trade_ready"
            and not live_trade_intent_authority_issues(intent)
            and not actionable_strategy_contract_issues(intent, now=now)
            else ""
        )

        if active and incoming_id and incoming_id != active.get("candidate_id"):
            terminal = _terminal(
                active,
                CandidatePhase.SUPERSEDED,
                "new_trade_candidate_superseded_prior_candidate",
                now=now,
            )
            _append_audit(storage, now, terminal)
            completed.add(str(active.get("candidate_id") or ""))
            completed_candidates[str(active.get("candidate_id") or "")] = terminal
            state["last_terminal"] = terminal
            active = {}

        if not active and incoming_id and incoming_id not in completed:
            active = _armed_candidate(intent, now=now)
            _append_audit(storage, now, {"event": "candidate_armed", **active})

        result: dict[str, object]
        if active:
            phase, reason, observation = _advance_active(active, latest, now=now)
            active["last_observed_at"] = now.isoformat()
            active["last_observation"] = observation
            if phase in TERMINAL_PHASES:
                terminal = _terminal(active, phase, reason, now=now)
                _append_audit(storage, now, terminal)
                completed.add(str(active.get("candidate_id") or ""))
                completed_candidates[str(active.get("candidate_id") or "")] = terminal
                state["last_terminal"] = terminal
                active = {}
                result = terminal
            else:
                result = {"event": "candidate_active", **active}
        else:
            result = (
                dict(completed_candidates[incoming_id])
                if incoming_id and incoming_id in completed_candidates
                else {
                    "schema_version": 1,
                    "phase": CandidatePhase.OBSERVING.value,
                    "automatic_ordering": False,
                    "broker_order_state": "not_connected",
                }
            )

        state.update(
            {
                "schema_version": 1,
                "updated_at": now.isoformat(),
                "active": active or None,
                "completed_candidate_ids": sorted(item for item in completed if item)[-500:],
                "completed_candidates": _trim_completed(completed_candidates),
            }
        )
        atomic_write_json_secure(state_path, state)
        return result


def advance_put_shadow_candidates(
    storage: StorageSettings,
    latest: LatestState,
    intent: Mapping[str, object],
    *,
    now: datetime,
) -> dict[str, object]:
    """Advance independent Put quote-observation lanes without live alerts."""

    now = _utc(now)
    state_path = _put_shadow_state_path(storage)
    incoming_lane = str(intent.get("strategy_lane") or "")
    incoming_id = (
        _candidate_id(intent)
        if intent.get("status") == "shadow_ready"
        and intent.get("shadow_mode") is True
        and intent.get("execution_eligible") is False
        and intent.get("quote_observation_eligible") is True
        and intent.get("automatic_ordering") is False
        and intent.get("direction") == "down"
        and incoming_lane in PUT_SHADOW_LANES
        and _put_shadow_lane_contract_valid(intent)
        and _put_shadow_window_contract_valid(intent, now=now)
        and not actionable_strategy_contract_issues(intent, now=now)
        else ""
    )
    with exclusive_state_lock(state_path):
        state = read_json_object(state_path)
        active_by_lane = {
            str(lane): dict(value)
            for lane, value in dict(state.get("active_by_lane") or {}).items()
            if lane in PUT_SHADOW_LANES and isinstance(value, Mapping)
        }
        completed_candidates = {
            str(key): dict(value)
            for key, value in dict(state.get("completed_candidates") or {}).items()
            if isinstance(value, Mapping)
        }
        completed = set(completed_candidates)
        completed.update(str(item) for item in state.get("completed_candidate_ids") or [])
        consumed_identity_order = [
            str(item)
            for item in state.get("consumed_identity_keys") or []
            if _nonempty_string(item) and str(item).startswith("candidate_id:")
        ]
        consumed_identity_keys = set(consumed_identity_order)
        identity_owner: dict[str, str] = {}
        for candidate_id, candidate in completed_candidates.items():
            _remember_shadow_identity(
                candidate,
                candidate_id=candidate_id,
                consumed_order=consumed_identity_order,
                consumed=consumed_identity_keys,
                owner=identity_owner,
            )
        for active in active_by_lane.values():
            _remember_shadow_identity(
                active,
                candidate_id=str(active.get("candidate_id") or ""),
                consumed_order=consumed_identity_order,
                consumed=consumed_identity_keys,
                owner=identity_owner,
            )
        lane_results: dict[str, dict[str, object]] = {}

        if incoming_id:
            active = active_by_lane.get(incoming_lane, {})
            incoming_identity_keys = set(_put_shadow_identity_keys(intent))
            active_identity_keys = set(_put_shadow_identity_keys(active))
            same_active_opportunity = bool(
                active
                and (
                    incoming_id == active.get("candidate_id")
                    or incoming_identity_keys.intersection(active_identity_keys)
                )
            )
            consumed_owner_id = next(
                (
                    identity_owner[key]
                    for key in incoming_identity_keys
                    if key in consumed_identity_keys and identity_owner.get(key)
                ),
                "",
            )
            already_consumed = bool(
                incoming_id in completed
                or (
                    incoming_identity_keys.intersection(consumed_identity_keys)
                    and not same_active_opportunity
                )
            )
            if same_active_opportunity or already_consumed:
                alias_owner_id = (
                    str(active.get("candidate_id") or "")
                    if same_active_opportunity
                    else consumed_owner_id or incoming_id
                )
                _remember_shadow_identity(
                    intent,
                    candidate_id=alias_owner_id,
                    consumed_order=consumed_identity_order,
                    consumed=consumed_identity_keys,
                    owner=identity_owner,
                )

            if active and not same_active_opportunity and not already_consumed:
                terminal = _terminal(
                    active,
                    CandidatePhase.SUPERSEDED,
                    "new_put_shadow_candidate_superseded_prior_candidate",
                    now=now,
                )
                _append_audit(storage, now, terminal)
                prior_id = str(active.get("candidate_id") or "")
                if prior_id:
                    completed.add(prior_id)
                    completed_candidates[prior_id] = terminal
                active_by_lane.pop(incoming_lane, None)

            if (
                incoming_lane not in active_by_lane
                and incoming_id not in completed
                and not already_consumed
            ):
                active = {
                    **_armed_candidate(intent, now=now),
                    "shadow_mode": True,
                    "execution_claim": "none",
                }
                active_by_lane[incoming_lane] = active
                _remember_shadow_identity(
                    active,
                    candidate_id=incoming_id,
                    consumed_order=consumed_identity_order,
                    consumed=consumed_identity_keys,
                    owner=identity_owner,
                )
                _append_audit(storage, now, {"event": "candidate_armed", **active})
            elif already_consumed and incoming_lane not in active_by_lane:
                prior = completed_candidates.get(consumed_owner_id or incoming_id)
                lane_results[incoming_lane] = (
                    dict(prior)
                    if prior is not None
                    else {
                        "schema_version": 1,
                        "phase": CandidatePhase.OBSERVING.value,
                        "strategy_lane": incoming_lane,
                        "shadow_mode": True,
                        "deduplicated": True,
                        "deduplication_reason": "put_shadow_semantic_identity_consumed",
                        "automatic_ordering": False,
                        "broker_order_state": "not_connected",
                    }
                )

        for lane in sorted(PUT_SHADOW_LANES):
            active = active_by_lane.get(lane)
            if not active:
                continue
            phase, reason, observation = _advance_active(
                active,
                latest,
                now=now,
                put_shadow_lane=lane,
            )
            active["last_observed_at"] = now.isoformat()
            active["last_observation"] = observation
            if phase in TERMINAL_PHASES:
                terminal = _terminal(active, phase, reason, now=now)
                candidate_id = str(active.get("candidate_id") or "")
                _append_audit(storage, now, terminal)
                if candidate_id:
                    completed.add(candidate_id)
                    completed_candidates[candidate_id] = terminal
                active_by_lane.pop(lane, None)
                lane_results[lane] = terminal
            else:
                active_by_lane[lane] = active
                lane_results[lane] = {"event": "candidate_active", **active}

        if incoming_id and incoming_lane not in lane_results:
            lane_results[incoming_lane] = (
                dict(completed_candidates[incoming_id])
                if incoming_id in completed_candidates
                else {
                    "schema_version": 1,
                    "phase": CandidatePhase.OBSERVING.value,
                    "strategy_lane": incoming_lane,
                    "shadow_mode": True,
                    "automatic_ordering": False,
                    "broker_order_state": "not_connected",
                }
            )

        state.update(
            {
                "schema_version": PUT_SHADOW_STATE_SCHEMA_VERSION,
                "identity_contract_version": PUT_SHADOW_CANDIDATE_CONTRACT_VERSION,
                "updated_at": now.isoformat(),
                "active_by_lane": active_by_lane,
                "completed_candidate_ids": sorted(item for item in completed if item)[-500:],
                "completed_candidates": _trim_completed(completed_candidates),
                "consumed_identity_keys": consumed_identity_order[
                    -PUT_SHADOW_CONSUMED_IDENTITY_LIMIT:
                ],
            }
        )
        atomic_write_json_secure(state_path, state)

    current = lane_results.get(incoming_lane, {}) if incoming_lane else {}
    return {
        "schema_version": PUT_SHADOW_STATE_SCHEMA_VERSION,
        "mode": "put_shadow_exact",
        "incoming_lane": incoming_lane if incoming_lane in PUT_SHADOW_LANES else None,
        "phase": current.get("phase", CandidatePhase.OBSERVING.value),
        "candidate_id": current.get("candidate_id"),
        "terminal_reason": current.get("terminal_reason"),
        "entry_observation": current.get("entry_observation"),
        "lanes": lane_results,
        "shadow_mode": True,
        "automatic_ordering": False,
        "broker_order_state": "not_connected",
        "execution_claim": "none",
    }


def virtual_entry_intent(
    candidate: Mapping[str, object],
    *,
    delivery_projection: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Return the quote-reached source with its immutable delivery identity."""

    if candidate.get("phase") != CandidatePhase.QUOTE_REACHED_ENTRY.value:
        return {}
    source = candidate.get("source_intent")
    if not isinstance(source, Mapping):
        return {}
    if candidate.get("shadow_mode") is True or live_trade_intent_authority_issues(source):
        return {}
    projected = delivery_projection if isinstance(delivery_projection, Mapping) else {}
    if projected and projected.get("intent_id") != source.get("intent_id"):
        projected = {}
    return {
        **dict(source),
        "status": "trade_ready",
        "source_intent_id": source.get("intent_id"),
        "intent_id": candidate.get("candidate_id"),
        "entry_observation": candidate.get("entry_observation"),
        "notification_event_id": projected.get("notification_event_id"),
        "notification_status": projected.get("notification_status"),
        "notification_reason": projected.get("notification_reason"),
        "execution_assumption": "displayed_quote_only_no_broker_fill",
    }


def gate_trade_intent(
    intent: Mapping[str, object],
    candidate: Mapping[str, object],
) -> dict[str, object]:
    """Suppress delivery when the no-order candidate already reached a terminal guard."""

    source = candidate.get("source_intent")
    source = source if isinstance(source, Mapping) else {}
    if source.get("intent_id") != intent.get("intent_id"):
        return dict(intent)
    phase = str(candidate.get("phase") or "")
    if phase not in {
        CandidatePhase.TARGET_PASSED.value,
        CandidatePhase.INVALIDATED.value,
        CandidatePhase.EXPIRED.value,
        CandidatePhase.SUPERSEDED.value,
    }:
        return dict(intent)
    reason = str(candidate.get("terminal_reason") or f"candidate_{phase}")
    return {
        **dict(intent),
        "status": "blocked",
        "candidate_phase": phase,
        "block_reasons": list(
            dict.fromkeys(
                [
                    *(str(item) for item in intent.get("block_reasons") or []),
                    reason,
                ]
            )
        ),
    }


def _armed_candidate(intent: Mapping[str, object], *, now: datetime) -> dict[str, object]:
    raw_coordinate = intent.get("coordinate")
    coordinate = dict(raw_coordinate) if isinstance(raw_coordinate, Mapping) else None
    candidate_policy: dict[str, object] = {
        "source_policy_version": intent.get("policy_version"),
    }
    if intent.get("shadow_mode") is True:
        candidate_policy["put_shadow_contract"] = {
            "version": PUT_SHADOW_CANDIDATE_CONTRACT_VERSION,
            "lanes": tuple(sorted(PUT_SHADOW_LANES)),
            "exact_quote_policy_version": PUT_SHADOW_EXACT_QUOTE_POLICY_VERSION,
            "exact_quote_max_age_seconds": PUT_SHADOW_EXACT_QUOTE_MAX_AGE_SECONDS,
            "execution_eligible": False,
            "quote_observation_eligible": True,
            "automatic_ordering": False,
            "identity_key": "candidate_id=intent_id|event_id",
            "new_level_event_rearms": True,
        }
    candidate_policy_version = policy_version(
        "trade_candidate.v3",
        candidate_policy,
    )
    return {
        **strategy_event_fields(
            policy_version_value=candidate_policy_version,
            valid_until=parse_aware_time(intent.get("valid_until")),
            coordinate=coordinate,
            block_reasons=(),
        ),
        "phase": CandidatePhase.ARMED.value,
        "candidate_id": _candidate_id(intent),
        "intent_id": intent.get("intent_id"),
        "event_id": intent.get("event_id"),
        "semantic_key": intent.get("semantic_key"),
        "session_id": intent.get("session_id"),
        "direction": intent.get("direction"),
        "play": intent.get("play"),
        "thesis": intent.get("thesis"),
        "level_kind": intent.get("level_kind"),
        "strategy_lane": intent.get("strategy_lane"),
        "shadow_mode": intent.get("shadow_mode") is True,
        "contract_id": intent.get("contract_id"),
        "entry_limit": intent.get("entry_limit"),
        "target_spx": intent.get("target_spx"),
        "invalidation_spx": intent.get("invalidation_spx"),
        "expires_at": intent.get("expires_at"),
        "trade_intent_contract_version": intent.get("trade_intent_contract_version"),
        "entry_window_start_at": intent.get("entry_window_start_at"),
        "hard_exit_at": intent.get("hard_exit_at"),
        "armed_at": now.isoformat(),
        "automatic_ordering": False,
        "broker_order_state": "not_connected",
        "source_intent": dict(intent),
    }


def _advance_active(
    active: Mapping[str, object],
    latest: LatestState,
    *,
    now: datetime,
    put_shadow_lane: str | None = None,
) -> tuple[CandidatePhase, str | None, dict[str, object]]:
    direction = str(active.get("direction") or "")
    target = _number(active.get("target_spx"))
    invalidation = _number(active.get("invalidation_spx"))
    expires_at = (
        parse_aware_time(active.get("valid_until"))
        if active.get("schema_version") == STRATEGY_EVENT_SCHEMA_VERSION
        else _time(active.get("expires_at"))
    )
    enforced_put_shadow = put_shadow_lane is not None
    shadow_window_start_at: datetime | None = None
    shadow_contract_invalid = False
    if enforced_put_shadow:
        shadow_contract_invalid = not _put_shadow_active_contract_valid(
            active,
            lane=put_shadow_lane,
        )
    elif active.get("shadow_mode") is True:
        shadow_contract_invalid = not _put_shadow_window_fields_valid(active)
    if enforced_put_shadow or active.get("shadow_mode") is True:
        if shadow_contract_invalid:
            expires_at = None
        else:
            shadow_window_start_at = parse_aware_time(active.get("entry_window_start_at"))
            hard_exit_at = parse_aware_time(active.get("hard_exit_at"))
            if hard_exit_at is not None and expires_at is not None:
                expires_at = min(expires_at, hard_exit_at)
    spot = _usable_price(latest, "index:SPX", now=now)
    observation: dict[str, object] = {
        "at": now.isoformat(),
        "spx": spot,
        "contract_id": active.get("contract_id"),
        "entry_limit": active.get("entry_limit"),
    }

    if expires_at is None or now >= expires_at:
        reason = (
            "put_shadow_window_contract_invalid"
            if shadow_contract_invalid
            else "entry_window_expired"
        )
        return CandidatePhase.EXPIRED, reason, observation
    if shadow_window_start_at is not None and now < shadow_window_start_at:
        return CandidatePhase.EXPIRED, "put_shadow_entry_window_not_open", observation
    if _level_reached(spot, target, direction=direction, target=True):
        return (
            CandidatePhase.TARGET_PASSED,
            "target_reached_before_entry_quote",
            observation,
        )
    if _level_reached(spot, invalidation, direction=direction, target=False):
        return (
            CandidatePhase.INVALIDATED,
            "invalidation_reached_before_entry_quote",
            observation,
        )
    contract_id = str(active.get("contract_id") or "")
    source_intent = active.get("source_intent")
    source_intent = source_intent if isinstance(source_intent, Mapping) else {}
    provider_name = str(source_intent.get("provider") or "")
    provider = (
        Provider(provider_name) if provider_name in {item.value for item in Provider} else None
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
        else latest.best_quote(contract_id)
        if contract_id
        else None
    )
    entry_limit = _number(active.get("entry_limit"))
    if quote is not None:
        if enforced_put_shadow or active.get("shadow_mode") is True:
            quote_allowed, quote_reason, quote_contract = _put_shadow_exact_quote_decision(
                quote,
                now=now,
            )
        else:
            quote_allowed, quote_reason, quote_contract = candidate_displayed_quote_decision(
                quote,
                now=now,
                max_age_seconds=PUT_SHADOW_EXACT_QUOTE_MAX_AGE_SECONDS,
            )
        observation.update(
            {
                "provider": quote.provider.value,
                "bid": quote.bid,
                "ask": quote.ask,
                "mid": quote.mid,
                "quote_quality": quote.quality.value,
                "quote_source_at": (
                    quote.quote_time.isoformat() if quote.quote_time is not None else None
                ),
                "quote_transport_at": (quote.last_update_at or quote.received_at).isoformat(),
                "quote_received_at": quote.received_at.isoformat(),
                "quote_pricing_allowed": quote_allowed,
                "quote_quality_reason": quote_reason,
                **quote_contract,
            }
        )
        if (
            quote_allowed
            and entry_limit is not None
            and quote.ask is not None
            and quote.ask <= entry_limit
        ):
            observation["entry_condition"] = "displayed_ask_at_or_below_limit"
            return CandidatePhase.QUOTE_REACHED_ENTRY, "entry_quote_reached", observation
    return CandidatePhase.ARMED, None, observation


def _terminal(
    active: Mapping[str, object],
    phase: CandidatePhase,
    reason: str | None,
    *,
    now: datetime,
) -> dict[str, object]:
    observation = dict(active.get("last_observation") or {})
    terminal_reasons = (
        []
        if phase is CandidatePhase.QUOTE_REACHED_ENTRY
        else [reason or f"candidate_{phase.value}"]
    )
    raw_coordinate = active.get("coordinate")
    coordinate = dict(raw_coordinate) if isinstance(raw_coordinate, Mapping) else None
    payload = {
        **dict(active),
        **strategy_event_fields(
            policy_version_value=str(active.get("policy_version") or "trade_candidate.v3"),
            valid_until=parse_aware_time(active.get("valid_until")),
            coordinate=coordinate,
            block_reasons=terminal_reasons,
        ),
        "event": "candidate_terminal",
        "phase": phase.value,
        "terminal_reason": reason,
        "terminal_at": now.isoformat(),
        "broker_order_state": "not_connected",
        "execution_claim": "none",
    }
    if phase is CandidatePhase.QUOTE_REACHED_ENTRY:
        payload["entry_observation"] = observation
    return payload


def _level_reached(
    spot: float | None,
    level: float | None,
    *,
    direction: str,
    target: bool,
) -> bool:
    if spot is None or level is None or direction not in {"up", "down"}:
        return False
    if target:
        return spot >= level if direction == "up" else spot <= level
    return spot <= level if direction == "up" else spot >= level


def _usable_price(latest: LatestState, instrument_id: str, *, now: datetime) -> float | None:
    return actionable_live_price(latest, instrument_id, as_of=now)


def _append_audit(storage: StorageSettings, now: datetime, payload: Mapping[str, object]) -> None:
    path = (
        Path(storage.data_root)
        / "features"
        / "trade_candidates"
        / f"date={now.date().isoformat()}"
        / "events.jsonl"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    audit_identity = _candidate_audit_identity(payload)
    with exclusive_state_lock(path):
        if audit_identity and _audit_identity_exists(path, audit_identity):
            return
        descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
        try:
            os.write(
                descriptor,
                (json.dumps(dict(payload), ensure_ascii=False, sort_keys=True) + "\n").encode(),
            )
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def _audit_identity_exists(path: Path, expected: str) -> bool:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return False
    for line in lines:
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, Mapping) and _candidate_audit_identity(row) == expected:
            return True
    return False


def _candidate_audit_identity(payload: Mapping[str, object]) -> str:
    event = str(payload.get("event") or "")
    if event not in {"candidate_armed", "candidate_terminal"}:
        return ""
    if payload.get("shadow_mode") is True:
        identities = _put_shadow_identity_keys(payload)
        identity = identities[0] if identities else ""
    else:
        candidate_id = payload.get("candidate_id")
        identity = f"candidate_id:{candidate_id}" if _nonempty_string(candidate_id) else ""
    return f"{event}|{identity}" if identity else ""


def _state_path(storage: StorageSettings) -> Path:
    return Path(storage.data_root) / "latest" / "trade_candidate_state.json"


def _put_shadow_state_path(storage: StorageSettings) -> Path:
    return Path(storage.data_root) / "latest" / "put_shadow_candidate_state.json"


def _candidate_id(intent: Mapping[str, object]) -> str:
    intent_id = str(intent.get("intent_id") or "")
    event_id = str(intent.get("event_id") or "")
    return "|".join((intent_id, event_id)) if intent_id and event_id else ""


def _put_shadow_identity_keys(payload: Mapping[str, object]) -> tuple[str, ...]:
    source = payload.get("source_intent")
    source = source if isinstance(source, Mapping) else {}
    candidate_id = payload.get("candidate_id")
    if not _nonempty_string(candidate_id):
        intent_id = payload.get("intent_id") or source.get("intent_id")
        event_id = payload.get("event_id") or source.get("event_id")
        if _nonempty_string(intent_id) and _nonempty_string(event_id):
            candidate_id = f"{intent_id}|{event_id}"
    return (f"candidate_id:{candidate_id}",) if _nonempty_string(candidate_id) else ()


def _put_shadow_lane_contract_valid(intent: Mapping[str, object]) -> bool:
    lane = str(intent.get("strategy_lane") or "")
    contract = PUT_SHADOW_LANE_CONTRACTS.get(lane)
    if contract is None:
        return False
    play, thesis, level_kinds = contract
    return bool(
        intent.get("play") == play
        and intent.get("thesis") == thesis
        and intent.get("level_kind") in level_kinds
        and _put_shadow_option_identity_valid(intent)
    )


def _put_shadow_option_identity_valid(intent: Mapping[str, object]) -> bool:
    contract_id = str(intent.get("contract_id") or "")
    parts = contract_id.split(":")
    if len(parts) != 6 or parts[:3] != ["option", "SPX", "SPXW"] or parts[5] != "P":
        return False
    try:
        expiry = datetime.strptime(parts[3], "%Y%m%d").date()
        session_day = datetime.strptime(str(intent.get("session_id") or ""), "%Y-%m-%d").date()
        strike = float(parts[4])
    except ValueError:
        return False
    entry_window_start_at = parse_aware_time(intent.get("entry_window_start_at"))
    return bool(
        strike > 0
        and entry_window_start_at is not None
        and expiry == session_day == entry_window_start_at.astimezone(ET).date()
    )


def _put_shadow_window_contract_valid(
    intent: Mapping[str, object],
    *,
    now: datetime,
) -> bool:
    if not _put_shadow_window_fields_valid(intent):
        return False
    entry_window_start_at = parse_aware_time(intent.get("entry_window_start_at"))
    hard_exit_at = parse_aware_time(intent.get("hard_exit_at"))
    return bool(
        entry_window_start_at is not None
        and hard_exit_at is not None
        and entry_window_start_at <= now < hard_exit_at
    )


def _put_shadow_window_fields_valid(payload: Mapping[str, object]) -> bool:
    if payload.get("trade_intent_contract_version") not in {
        LEGACY_PUT_SHADOW_SOURCE_CONTRACT_VERSION,
        TRADE_INTENT_CONTRACT_VERSION,
    }:
        return False
    entry_window_start_at = parse_aware_time(payload.get("entry_window_start_at"))
    hard_exit_at = parse_aware_time(payload.get("hard_exit_at"))
    valid_until = parse_aware_time(payload.get("valid_until"))
    if entry_window_start_at is None or hard_exit_at is None or valid_until is None:
        return False
    session_day = entry_window_start_at.astimezone(ET).date()
    expected_start = datetime.combine(
        session_day,
        PUT_SHADOW_ENTRY_WINDOW_START_ET,
        tzinfo=ET,
    ).astimezone(timezone.utc)
    expected_hard_exit = datetime.combine(
        session_day,
        PUT_SHADOW_HARD_EXIT_ET,
        tzinfo=ET,
    ).astimezone(timezone.utc)
    return bool(
        entry_window_start_at == expected_start
        and hard_exit_at == expected_hard_exit
        and entry_window_start_at < valid_until <= hard_exit_at
    )


def _put_shadow_active_contract_valid(
    active: Mapping[str, object],
    *,
    lane: str,
) -> bool:
    source = active.get("source_intent")
    if not isinstance(source, Mapping):
        return False
    active_entry_limit = _number(active.get("entry_limit"))
    source_entry_limit = _number(source.get("entry_limit"))
    expected_candidate_id = (
        f"{source.get('intent_id')}|{source.get('event_id')}"
        if _nonempty_string(source.get("intent_id")) and _nonempty_string(source.get("event_id"))
        else ""
    )
    return bool(
        lane in PUT_SHADOW_LANES
        and active.get("strategy_lane") == lane
        and active.get("shadow_mode") is True
        and active.get("automatic_ordering") is False
        and active.get("execution_claim") == "none"
        and active.get("broker_order_state") == "not_connected"
        and active.get("direction") == "down"
        and active.get("candidate_id") == expected_candidate_id
        and _put_shadow_lane_contract_valid(active)
        and _put_shadow_window_fields_valid(active)
        and source.get("status") == "shadow_ready"
        and source.get("strategy_lane") == lane
        and source.get("shadow_mode") is True
        and source.get("execution_eligible") is False
        and source.get("quote_observation_eligible") is True
        and source.get("automatic_ordering") is False
        and source.get("direction") == "down"
        and _put_shadow_lane_contract_valid(source)
        and _put_shadow_window_fields_valid(source)
        and active.get("intent_id") == source.get("intent_id")
        and active.get("event_id") == source.get("event_id")
        and active.get("semantic_key") == source.get("semantic_key")
        and active.get("contract_id") == source.get("contract_id")
        and active_entry_limit is not None
        and source_entry_limit is not None
        and active_entry_limit == source_entry_limit
        and active.get("valid_until") == source.get("valid_until")
        and active.get("trade_intent_contract_version")
        == source.get("trade_intent_contract_version")
        and active.get("entry_window_start_at") == source.get("entry_window_start_at")
        and active.get("hard_exit_at") == source.get("hard_exit_at")
    )


def _remember_shadow_identity(
    payload: Mapping[str, object],
    *,
    candidate_id: str,
    consumed_order: list[str],
    consumed: set[str],
    owner: dict[str, str],
) -> None:
    for key in _put_shadow_identity_keys(payload):
        if key not in consumed:
            consumed.add(key)
            consumed_order.append(key)
        if candidate_id:
            owner.setdefault(key, candidate_id)


def _put_shadow_exact_quote_decision(
    quote: Quote,
    *,
    now: datetime,
) -> tuple[bool, str, dict[str, object]]:
    source_at = quote.quote_time
    transport_at = quote.last_update_at or quote.received_at
    source_age = (_utc(now) - _utc(source_at)).total_seconds() if source_at is not None else None
    transport_age = (_utc(now) - _utc(transport_at)).total_seconds()
    use = quote_use_decision(
        quote,
        as_of=now,
        stale_after_seconds=PUT_SHADOW_EXACT_QUOTE_MAX_AGE_SECONDS,
        delayed_stale_after_seconds=PUT_SHADOW_EXACT_QUOTE_MAX_AGE_SECONDS,
    )
    analytical_only = isinstance(quote.raw, Mapping) and quote.raw.get("analytical_only") is True
    valid_nbbo = bool(
        quote.bid is not None
        and quote.mid is not None
        and quote.ask is not None
        and quote.bid >= 0
        and quote.mid > 0
        and quote.ask > 0
        and quote.bid <= quote.mid <= quote.ask
    )
    if source_age is None:
        reason = "put_shadow_quote_source_timestamp_unavailable"
    elif source_age < 0:
        reason = "put_shadow_quote_source_timestamp_in_future"
    elif transport_age < 0:
        reason = "put_shadow_quote_transport_timestamp_in_future"
    elif source_age > PUT_SHADOW_EXACT_QUOTE_MAX_AGE_SECONDS:
        reason = "put_shadow_quote_source_stale"
    elif transport_age > PUT_SHADOW_EXACT_QUOTE_MAX_AGE_SECONDS:
        reason = "put_shadow_quote_transport_stale"
    elif quote.quality.value != "live":
        reason = "put_shadow_quote_not_live"
    elif analytical_only:
        reason = "put_shadow_quote_analytical_only"
    elif not valid_nbbo:
        reason = "put_shadow_quote_invalid_nbbo"
    elif not use.pricing_allowed:
        reason = f"put_shadow_quote_not_pricing_allowed:{use.reason}"
    else:
        reason = "put_shadow_exact_quote_fresh"
    allowed = reason == "put_shadow_exact_quote_fresh"
    return (
        allowed,
        reason,
        {
            "exact_quote_policy_version": PUT_SHADOW_EXACT_QUOTE_POLICY_VERSION,
            "exact_quote_max_age_seconds": PUT_SHADOW_EXACT_QUOTE_MAX_AGE_SECONDS,
            "quote_source_at": source_at.isoformat() if source_at is not None else None,
            "quote_transport_at": transport_at.isoformat(),
            "quote_source_age_seconds": (round(source_age, 6) if source_age is not None else None),
            "quote_transport_age_seconds": round(transport_age, 6),
            "exact_quote_freshness_ok": allowed,
        },
    )


def _trim_completed(
    completed: Mapping[str, Mapping[str, object]],
) -> dict[str, dict[str, object]]:
    rows = sorted(
        ((key, dict(value)) for key, value in completed.items() if key),
        key=lambda item: str(item[1].get("terminal_at") or ""),
    )
    return dict(rows[-500:])


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) else None


def _nonempty_string(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return _utc(datetime.fromisoformat(value))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
