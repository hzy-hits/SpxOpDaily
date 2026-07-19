"""Quote-observed lifecycle for an alert candidate; never represents a broker order."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Mapping

from spx_spark.config import StorageSettings
from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock, read_json_object
from spx_spark.storage import LatestState, configured_quote_use_decision
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


def virtual_entry_intent(candidate: Mapping[str, object]) -> dict[str, object]:
    """Return the source intent only after the displayed ask reached its limit."""

    if candidate.get("phase") != CandidatePhase.QUOTE_REACHED_ENTRY.value:
        return {}
    source = candidate.get("source_intent")
    if not isinstance(source, Mapping):
        return {}
    return {
        **dict(source),
        "status": "trade_ready",
        "source_intent_id": source.get("intent_id"),
        "intent_id": candidate.get("candidate_id"),
        "entry_observation": candidate.get("entry_observation"),
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
    candidate_policy_version = policy_version(
        "trade_candidate.v3",
        {"source_policy_version": intent.get("policy_version")},
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
        "direction": intent.get("direction"),
        "contract_id": intent.get("contract_id"),
        "entry_limit": intent.get("entry_limit"),
        "target_spx": intent.get("target_spx"),
        "invalidation_spx": intent.get("invalidation_spx"),
        "expires_at": intent.get("expires_at"),
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
) -> tuple[CandidatePhase, str | None, dict[str, object]]:
    direction = str(active.get("direction") or "")
    target = _number(active.get("target_spx"))
    invalidation = _number(active.get("invalidation_spx"))
    expires_at = (
        parse_aware_time(active.get("valid_until"))
        if active.get("schema_version") == STRATEGY_EVENT_SCHEMA_VERSION
        else _time(active.get("expires_at"))
    )
    spot = _usable_price(latest, "index:SPX", now=now)
    observation: dict[str, object] = {
        "at": now.isoformat(),
        "spx": spot,
        "contract_id": active.get("contract_id"),
        "entry_limit": active.get("entry_limit"),
    }

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
    if expires_at is None or now >= expires_at:
        return CandidatePhase.EXPIRED, "entry_window_expired", observation

    contract_id = str(active.get("contract_id") or "")
    quote = latest.best_quote(contract_id) if contract_id else None
    entry_limit = _number(active.get("entry_limit"))
    if quote is not None:
        use = configured_quote_use_decision(quote, as_of=now)
        observation.update(
            {
                "provider": quote.provider.value,
                "bid": quote.bid,
                "ask": quote.ask,
                "mid": quote.mid,
                "quote_source_at": (
                    quote.quote_time
                    or quote.trade_time
                    or quote.last_update_at
                    or quote.received_at
                ).isoformat(),
                "quote_pricing_allowed": use.pricing_allowed,
                "quote_quality_reason": use.reason,
            }
        )
        if (
            use.pricing_allowed
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
        [] if phase is CandidatePhase.QUOTE_REACHED_ENTRY else [reason or f"candidate_{phase.value}"]
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
    quote = latest.best_quote(instrument_id)
    if quote is None:
        return None
    decision = configured_quote_use_decision(quote, as_of=now)
    return _number(quote.effective_price) if decision.pricing_allowed else None


def _append_audit(storage: StorageSettings, now: datetime, payload: Mapping[str, object]) -> None:
    path = (
        Path(storage.data_root)
        / "features"
        / "trade_candidates"
        / f"date={now.date().isoformat()}"
        / "events.jsonl"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        os.write(
            descriptor,
            (json.dumps(dict(payload), ensure_ascii=False, sort_keys=True) + "\n").encode(),
        )
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _state_path(storage: StorageSettings) -> Path:
    return Path(storage.data_root) / "latest" / "trade_candidate_state.json"


def _candidate_id(intent: Mapping[str, object]) -> str:
    intent_id = str(intent.get("intent_id") or "")
    event_id = str(intent.get("event_id") or "")
    return "|".join((intent_id, event_id)) if intent_id and event_id else ""


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
