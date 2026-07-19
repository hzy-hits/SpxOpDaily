"""One terminal deterministic gate result for every observed CONFIRMED event."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping

from spx_spark.config import StorageSettings
from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock, read_json_object
from spx_spark.strategy_contract import (
    STRATEGY_EVENT_SCHEMA_VERSION,
    normalize_block_reasons,
    parse_aware_time,
    policy_version,
    strategy_event_fields,
)


TERMINAL_LEVEL_PHASES = frozenset({"invalidated", "expired"})


def reconcile_confirmed_gate(
    storage: StorageSettings,
    level_decision: Mapping[str, object],
    intent: Mapping[str, object],
    *,
    now: datetime,
) -> dict[str, object]:
    """Track a confirmation until it has exactly one ready-or-blocked terminal row."""

    now = _utc(now)
    path = _state_path(storage)
    with exclusive_state_lock(path):
        state = read_json_object(path)
        pending = dict(state.get("pending") or {})
        completed = {
            str(key): dict(value)
            for key, value in dict(state.get("completed") or {}).items()
            if isinstance(value, Mapping)
        }
        event_id = str(level_decision.get("event_id") or "")
        phase = str(level_decision.get("phase") or "far")
        finalized: dict[str, object] | None = None

        if pending and event_id != pending.get("event_id"):
            finalized = _finalize(
                pending,
                status="blocked",
                reasons=[
                    *list(pending.get("last_block_reasons") or []),
                    "confirmed_event_superseded_before_trade_ready",
                ],
                now=now,
            )
            _record_once(storage, finalized, completed=completed, now=now)
            pending = {}

        if event_id and phase == "confirmed" and event_id not in completed:
            if not pending:
                pending = _pending(level_decision, intent, now=now)
            evaluated_event = str(intent.get("event_id") or "")
            evaluated_status = str(intent.get("status") or "observing")
            if evaluated_event == event_id and evaluated_status == "trade_ready":
                finalized = _finalize(
                    pending,
                    status="trade_ready",
                    reasons=[],
                    now=now,
                    intent=intent,
                )
                _record_once(storage, finalized, completed=completed, now=now)
                pending = {}
            else:
                reasons = [str(item) for item in intent.get("block_reasons") or []]
                if evaluated_event != event_id:
                    reasons.append("confirmed_event_missing_from_trade_evaluation")
                    quality_reason = level_decision.get("quality_reason")
                    if quality_reason:
                        reasons.append(str(quality_reason))
                elif evaluated_status != "blocked":
                    reasons.append(f"trade_gate_status_{evaluated_status}")
                pending.update(
                    {
                        "last_evaluated_at": now.isoformat(),
                        "last_trade_status": evaluated_status,
                        "last_block_reasons": list(dict.fromkeys(reasons)),
                    }
                )
        elif pending and event_id == pending.get("event_id") and phase in TERMINAL_LEVEL_PHASES:
            finalized = _finalize(
                pending,
                status="blocked",
                reasons=[
                    *list(pending.get("last_block_reasons") or []),
                    f"confirmed_event_{phase}",
                ],
                now=now,
            )
            _record_once(storage, finalized, completed=completed, now=now)
            pending = {}

        state.update(
            {
                "schema_version": 1,
                "updated_at": now.isoformat(),
                "pending": pending or None,
                "completed": _trim_completed(completed),
            }
        )
        atomic_write_json_secure(path, state)
        if finalized is not None:
            return finalized
        if pending:
            return {"status": "pending", **pending}
        if event_id in completed:
            return {"status": "already_finalized", **completed[event_id]}
        return {"status": "observing", "event_id": event_id or None, "phase": phase}


def _pending(
    level: Mapping[str, object],
    intent: Mapping[str, object],
    *,
    now: datetime,
) -> dict[str, object]:
    raw_coordinate = intent.get("coordinate")
    if not isinstance(raw_coordinate, Mapping):
        raw_coordinate = level.get("trigger_coordinate")
    coordinate = dict(raw_coordinate) if isinstance(raw_coordinate, Mapping) else None
    valid_until = parse_aware_time(intent.get("valid_until")) or parse_aware_time(
        level.get("expires_at")
    )
    source_policy_version = str(intent.get("policy_version") or "") or None
    gate_policy_version = policy_version(
        "confirmed_gate.v3",
        {"source_policy_version": source_policy_version or "unavailable"},
    )
    return {
        **strategy_event_fields(
            policy_version_value=gate_policy_version,
            valid_until=valid_until,
            coordinate=coordinate,
            block_reasons=(),
        ),
        "event_id": level.get("event_id"),
        "confirmed_at": level.get("phase_at") or level.get("confirmed_at") or now.isoformat(),
        "direction": level.get("direction"),
        "thesis": level.get("thesis"),
        "level_kind": level.get("level_kind"),
        "level": level.get("level"),
        "session_id": level.get("session_id") or level.get("expiry"),
        "last_evaluated_at": now.isoformat(),
        "last_trade_status": "observing",
        "last_block_reasons": [],
        "source_schema_version": intent.get("schema_version"),
        "source_policy_version": source_policy_version,
    }


def _finalize(
    pending: Mapping[str, object],
    *,
    status: str,
    reasons: list[object],
    now: datetime,
    intent: Mapping[str, object] | None = None,
) -> dict[str, object]:
    event_id = str(pending.get("event_id") or "")
    record_key = "confirmed-gate:" + hashlib.sha256(event_id.encode()).hexdigest()[:24]
    source_issues: list[str] = []
    if pending.get("source_schema_version") != STRATEGY_EVENT_SCHEMA_VERSION:
        source_issues.append("source_strategy_schema_unsupported")
    if not str(pending.get("source_policy_version") or ""):
        source_issues.append("source_policy_version_unavailable")
    valid_until = parse_aware_time(pending.get("valid_until"))
    if valid_until is None:
        source_issues.append("source_valid_until_unavailable")
    raw_coordinate = pending.get("coordinate")
    coordinate = dict(raw_coordinate) if isinstance(raw_coordinate, Mapping) else None
    if not coordinate or coordinate.get("kind") == "unavailable":
        source_issues.append("source_coordinate_unavailable")
    normalized_reasons = normalize_block_reasons([*reasons, *source_issues])
    effective_status = "blocked" if status == "trade_ready" and source_issues else status
    gate_policy_version = str(pending.get("policy_version") or "") or policy_version(
        "confirmed_gate.v3",
        {"source_policy_version": pending.get("source_policy_version") or "unavailable"},
    )
    return {
        **strategy_event_fields(
            policy_version_value=gate_policy_version,
            valid_until=valid_until,
            coordinate=coordinate,
            block_reasons=normalized_reasons,
        ),
        "record_key": record_key,
        "event_id": event_id,
        "status": effective_status,
        "terminal": True,
        "terminal_at": now.isoformat(),
        "confirmed_at": pending.get("confirmed_at"),
        "direction": pending.get("direction"),
        "thesis": pending.get("thesis"),
        "level_kind": pending.get("level_kind"),
        "level": pending.get("level"),
        "session_id": pending.get("session_id"),
        "source_schema_version": pending.get("source_schema_version"),
        "source_policy_version": pending.get("source_policy_version"),
        "intent_id": intent.get("intent_id") if intent else None,
        "contract_id": intent.get("contract_id") if intent else None,
        "automatic_ordering": False,
        "broker_order_state": "not_connected",
    }


def _record_once(
    storage: StorageSettings,
    payload: Mapping[str, object],
    *,
    completed: dict[str, dict[str, object]],
    now: datetime,
) -> None:
    event_id = str(payload.get("event_id") or "")
    if not event_id or event_id in completed:
        return
    completed[event_id] = dict(payload)
    path = (
        Path(storage.data_root)
        / "features"
        / "confirmed_gate_results"
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


def _trim_completed(
    completed: Mapping[str, Mapping[str, object]],
) -> dict[str, dict[str, object]]:
    rows = sorted(
        ((key, dict(value)) for key, value in completed.items()),
        key=lambda item: str(item[1].get("terminal_at") or ""),
    )
    return dict(rows[-500:])


def _state_path(storage: StorageSettings) -> Path:
    return Path(storage.data_root) / "latest" / "confirmed_gate_state.json"


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
