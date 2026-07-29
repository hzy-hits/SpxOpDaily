"""Durable producer-side evidence for RTH TradeIntent evaluation and delivery."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from spx_spark.config import StorageSettings
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET
from spx_spark.marketdata import as_utc
from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock


SCHEMA_VERSION = "trade_intent_producer_ledger.v1"
RTH_SLOT_SECONDS = 300
MAX_STATE_RECORDS = 2048
STATE_FILE_NAME = "trade_intent_producer_ledger_state.json"


class TradeIntentProducerLedgerError(RuntimeError):
    """Raised when producer evidence cannot be safely persisted."""


def producer_ledger_state_path(storage: StorageSettings) -> Path:
    return Path(storage.data_root).expanduser() / "latest" / STATE_FILE_NAME


def producer_ledger_events_path(
    storage: StorageSettings,
    trading_date: date,
) -> Path:
    return (
        Path(storage.data_root).expanduser()
        / "features"
        / "trade_intent_producer_ledger"
        / f"date={trading_date.isoformat()}"
        / "events.jsonl"
    )


def record_trade_intent_producer_observation(
    storage: StorageSettings,
    intent: Mapping[str, object],
    *,
    now: datetime,
    action_now: datetime | None = None,
) -> dict[str, object]:
    """Record one RTH slot heartbeat and any new TradeReady expectation.

    JSONL is written and fsynced before the compact dedupe state. If a process
    dies between those writes, the next call reconciles the JSONL record ID
    into state without appending a duplicate.
    """

    observed_at = as_utc(now)
    deadline = trade_intent_deadline_diagnostics(
        intent,
        evaluation_now=observed_at,
        action_now=action_now or observed_at,
    )
    local = observed_at.astimezone(ET)
    slot = _rth_slot(observed_at)
    delivery_event_id = (
        _derived_delivery_event_id(intent) if intent.get("status") == "trade_ready" else ""
    )
    semantic_key = str(intent.get("semantic_key") or "")
    records: list[dict[str, object]] = []
    result: dict[str, object] = {
        "ok": True,
        "observed_at": observed_at.isoformat(),
        "rth_slot": slot["slot_id"] if slot is not None else None,
        "heartbeat": "not_rth",
        "delivery_expectation": "not_trade_ready",
        "semantic_key": semantic_key or None,
        "delivery_event_id": delivery_event_id or None,
        "deadline": deadline,
    }

    if slot is not None:
        records.append(
            {
                "schema_version": SCHEMA_VERSION,
                "record_type": "rth_5m_heartbeat",
                "record_id": f"heartbeat:{slot['slot_id']}",
                "observed_at": observed_at.isoformat(),
                "trading_date_et": slot["trading_date_et"],
                "slot_id": slot["slot_id"],
                "slot_index": slot["slot_index"],
                "slot_start": slot["slot_start"],
                "slot_end": slot["slot_end"],
                "trade_intent_status": str(intent.get("status") or "unknown"),
                "intent_event_id": str(intent.get("event_id") or "") or None,
                "deadline": deadline,
            }
        )
        result["heartbeat"] = "pending"

    if intent.get("status") == "trade_ready":
        if semantic_key and delivery_event_id:
            expectation_date = (
                date.fromisoformat(str(slot["trading_date_et"]))
                if slot is not None
                else local.date()
            )
            records.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "record_type": "trade_ready_delivery_expectation",
                    "record_id": f"expectation:{delivery_event_id}",
                    "observed_at": observed_at.isoformat(),
                    "trading_date_et": expectation_date.isoformat(),
                    "slot_id": slot["slot_id"] if slot is not None else None,
                    "semantic_key": semantic_key,
                    "delivery_event_id": delivery_event_id,
                    "intent_id": str(intent.get("intent_id") or "") or None,
                    "intent_event_id": str(intent.get("event_id") or "") or None,
                    "contract_id": str(intent.get("contract_id") or "") or None,
                    "play": str(intent.get("play") or "") or None,
                    "expires_at": intent.get("expires_at"),
                    "deadline": deadline,
                }
            )
            result["delivery_expectation"] = "pending"
        else:
            result["delivery_expectation"] = "identity_unavailable"
            result["identity_issues"] = [
                field
                for field, value in (
                    ("semantic_key", semantic_key),
                    ("delivery_event_id", delivery_event_id),
                )
                if not value
            ]

    if not records:
        return result

    state_path = producer_ledger_state_path(storage)
    with exclusive_state_lock(state_path):
        state = _load_state_strict(state_path)
        state_records = {
            str(item.get("record_id")): dict(item)
            for item in state.get("records") or []
            if isinstance(item, Mapping) and item.get("record_id")
        }
        state_changed = not state
        for record in records:
            record_id = str(record["record_id"])
            trading_date = date.fromisoformat(str(record["trading_date_et"]))
            events_path = producer_ledger_events_path(storage, trading_date)
            jsonl_ids = _strict_jsonl_record_ids(events_path)
            if record_id not in jsonl_ids:
                _append_jsonl_strict(events_path, record)
                status = "recorded"
            else:
                status = "duplicate"
            if record_id not in state_records:
                state_records[record_id] = _state_record(record)
                state_changed = True
            if record["record_type"] == "rth_5m_heartbeat":
                result["heartbeat"] = status
            else:
                result["delivery_expectation"] = status

        if state_changed:
            retained = sorted(
                state_records.values(),
                key=lambda item: (
                    str(item.get("recorded_at") or ""),
                    str(item.get("record_id") or ""),
                ),
            )[-MAX_STATE_RECORDS:]
            atomic_write_json_secure(
                state_path,
                {
                    "schema_version": SCHEMA_VERSION,
                    "updated_at": observed_at.isoformat(),
                    "records": retained,
                },
            )
    return result


def trade_intent_deadline_diagnostics(
    intent: Mapping[str, object],
    *,
    evaluation_now: datetime,
    action_now: datetime,
) -> dict[str, object]:
    """Describe whether action revalidation reached the intent before expiry."""

    evaluated_at = as_utc(evaluation_now)
    revalidated_at = as_utc(action_now)
    candidates = [
        parsed
        for field in ("expires_at", "valid_until")
        if (parsed := _parse_timestamp(intent.get(field))) is not None
    ]
    deadline = min(candidates) if candidates else None
    latency = (revalidated_at - evaluated_at).total_seconds()
    ttl = (deadline - evaluated_at).total_seconds() if deadline is not None else None
    remaining = (deadline - revalidated_at).total_seconds() if deadline is not None else None
    return {
        "evaluation_at": evaluated_at.isoformat(),
        "action_revalidation_at": revalidated_at.isoformat(),
        "evaluation_to_action_revalidation_ms": round(latency * 1000.0, 3),
        "intent_deadline": deadline.isoformat() if deadline is not None else None,
        "intent_ttl_seconds_at_evaluation": round(ttl, 3) if ttl is not None else None,
        "ttl_remaining_at_action_seconds": (round(remaining, 3) if remaining is not None else None),
        "action_revalidation_exceeded_ttl": (
            revalidated_at >= deadline if deadline is not None else None
        ),
        "action_clock_regressed": latency < 0,
    }


def _rth_slot(now: datetime) -> dict[str, object] | None:
    current = as_utc(now).astimezone(ET)
    session = DEFAULT_MARKET_CALENDAR.session(current.date())
    if session is None or not session.open_at <= current < session.close_at:
        return None
    slot_index = int((current - session.open_at).total_seconds() // RTH_SLOT_SECONDS)
    slot_start = session.open_at + timedelta(seconds=slot_index * RTH_SLOT_SECONDS)
    slot_end = slot_start + timedelta(seconds=RTH_SLOT_SECONDS)
    slot_id = f"{session.trading_date.isoformat()}:rth:{slot_index:03d}"
    return {
        "trading_date_et": session.trading_date.isoformat(),
        "slot_id": slot_id,
        "slot_index": slot_index,
        "slot_start": slot_start.isoformat(),
        "slot_end": slot_end.isoformat(),
    }


def _derived_delivery_event_id(intent: Mapping[str, object]) -> str:
    # Keep one implementation of the delivery identity until it is promoted
    # to a public producer contract in trade_intent_runtime.
    from spx_spark.application.market_features.trade_intent_runtime import (
        _trade_ready_delivery_event_id,
    )

    return _trade_ready_delivery_event_id(intent)


def _state_record(record: Mapping[str, object]) -> dict[str, object]:
    return {
        "record_id": record.get("record_id"),
        "record_type": record.get("record_type"),
        "recorded_at": record.get("observed_at"),
        "trading_date_et": record.get("trading_date_et"),
        "slot_id": record.get("slot_id"),
        "semantic_key": record.get("semantic_key"),
        "delivery_event_id": record.get("delivery_event_id"),
    }


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return as_utc(parsed)


def _load_state_strict(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except (OSError, json.JSONDecodeError) as exc:
        raise TradeIntentProducerLedgerError(
            f"producer_ledger_state_unreadable:{type(exc).__name__}"
        ) from exc
    if not isinstance(payload, dict):
        raise TradeIntentProducerLedgerError("producer_ledger_state_not_object")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise TradeIntentProducerLedgerError("producer_ledger_state_schema_mismatch")
    records = payload.get("records")
    if not isinstance(records, list) or any(
        not isinstance(item, Mapping) or not item.get("record_id") for item in records
    ):
        raise TradeIntentProducerLedgerError("producer_ledger_state_records_invalid")
    return payload


def _strict_jsonl_record_ids(path: Path) -> set[str]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return set()
    except OSError as exc:
        raise TradeIntentProducerLedgerError(
            f"producer_ledger_jsonl_unreadable:{type(exc).__name__}"
        ) from exc
    record_ids: set[str] = set()
    for line_number, line in enumerate(lines, start=1):
        if not line:
            raise TradeIntentProducerLedgerError(f"producer_ledger_jsonl_blank_line:{line_number}")
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            raise TradeIntentProducerLedgerError(
                f"producer_ledger_jsonl_invalid:{line_number}"
            ) from exc
        if not isinstance(payload, dict) or not isinstance(payload.get("record_id"), str):
            raise TradeIntentProducerLedgerError(
                f"producer_ledger_jsonl_contract_invalid:{line_number}"
            )
        record_ids.add(payload["record_id"])
    return record_ids


def _append_jsonl_strict(path: Path, payload: Mapping[str, object]) -> None:
    try:
        rendered = (
            json.dumps(
                payload,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode()
    except (TypeError, ValueError) as exc:
        raise TradeIntentProducerLedgerError(
            f"producer_ledger_record_not_json:{type(exc).__name__}"
        ) from exc
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(
        path,
        os.O_APPEND | os.O_CREAT | os.O_WRONLY | getattr(os, "O_CLOEXEC", 0),
        0o600,
    )
    try:
        offset = 0
        while offset < len(rendered):
            written = os.write(descriptor, rendered[offset:])
            if written <= 0:
                raise TradeIntentProducerLedgerError("producer_ledger_jsonl_short_write")
            offset += written
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    _fsync_directory(path.parent)


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
