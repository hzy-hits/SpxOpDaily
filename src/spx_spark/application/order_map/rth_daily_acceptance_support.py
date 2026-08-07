"""Shared validation and persistence helpers for RTH daily acceptance."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

from spx_spark.application.market_features.trade_intent_runtime_support import (
    _trade_ready_delivery_event_id,
)
from spx_spark.market_calendar import MarketSession
from spx_spark.notifier.unified_delivery import notification_event_id


MAX_TRADE_READY_FIRST_DELIVERY_SECONDS = 5.0
LEGAL_TRADE_READY_TERMINAL_OUTCOMES = frozenset(
    {"cancelled_before_delivery", "expired_before_delivery"}
)
MARKET_STATE_SCHEMA = "market_state_5m.v1"
MARKET_STATE_RULE = "market_state_5m_eight_variable_rules.v2"
TRADE_INTENT_PRODUCER_LEDGER_SCHEMA = "trade_intent_producer_ledger.v1"
KNOWN_MARKET_STATES = frozenset(
    {
        "TREND_UP",
        "TREND_DOWN",
        "LOW_VOL_RANGE",
        "HIGH_VOL_CHOP",
        "LOW_VOL_PIN",
        "UNCERTAIN",
    }
)


@dataclass(frozen=True, slots=True)
class OperationalCheck:
    name: str
    measured: object
    threshold: object
    passed: bool
    reason: str


def report_event_id(
    row: Mapping[str, object] | None,
    *,
    session: MarketSession,
    slot: datetime,
) -> str:
    if isinstance(row, Mapping):
        persisted = row.get("notification_event_id")
        if isinstance(persisted, str) and persisted:
            return persisted
    slot_key = f"{session.trading_date.isoformat()}:{slot.strftime('%H:%M')}"
    return notification_event_id(
        "status",
        source="order_map_status",
        occurred_at=slot,
        identity=f"rth_slot:{slot_key}",
    )


def _unified_rows(
    path: str | Path | None,
    event_ids: Iterable[str],
) -> tuple[dict[str, list[sqlite3.Row]], str | None]:
    requested = tuple(dict.fromkeys(str(value) for value in event_ids if value))
    if path is None or not str(path):
        return {}, "not_configured"
    database = Path(path)
    if not database.exists():
        return {}, "outbox_missing"
    if not requested:
        return {}, None
    placeholders = ",".join("?" for _ in requested)
    try:
        with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
            connection.row_factory = sqlite3.Row
            rows = connection.execute(
                """
                SELECT e.id, e.logical_event_id AS event_id, e.channel, e.status,
                       e.created_at, e.expires_at, e.cancelled_at, e.cancel_reason,
                       e.last_error,
                       a.id AS attempt_id, a.finished_at, a.outcome,
                       a.attempted, a.ok
                FROM notification_events AS e
                LEFT JOIN notification_attempts AS a ON a.event_id = e.id
                WHERE e.logical_event_id IN ("""
                + placeholders
                + ") ORDER BY e.logical_event_id, e.channel, a.id",
                requested,
            ).fetchall()
    except (OSError, sqlite3.Error) as exc:
        return {}, f"outbox_query_failed:{type(exc).__name__}"
    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        grouped.setdefault(str(row["event_id"]), []).append(row)
    return grouped, None


def _sqlite_utc(value: object) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def fully_delivered_event_ids(
    path: str | Path | None,
    event_ids: Iterable[str],
) -> frozenset[str]:
    requested = tuple(dict.fromkeys(str(value) for value in event_ids if value))
    grouped, error = _unified_rows(path, requested)
    if error is not None:
        return frozenset()
    delivered: set[str] = set()
    for event_id in requested:
        rows = grouped.get(event_id, [])
        channels = {str(row["channel"]) for row in rows if row["channel"] != "__cancellation__"}
        if channels and all(
            str(row["status"]) == "delivered"
            for row in rows
            if row["channel"] != "__cancellation__"
        ):
            delivered.add(event_id)
    return frozenset(delivered)


def receipt_store_check(
    outbox_path: str | Path | None,
    receipt_path: str | Path | None,
) -> OperationalCheck:
    del receipt_path
    database = Path(outbox_path) if outbox_path else None
    quick_check = "missing"
    schema_present = False
    error: str | None = None
    if database is not None and database.exists():
        try:
            with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
                quick_check = str(connection.execute("PRAGMA quick_check(1)").fetchone()[0]).lower()
                tables = {
                    str(row[0])
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                schema_present = {
                    "notification_events",
                    "notification_attempts",
                } <= tables
        except (OSError, sqlite3.Error) as exc:
            error = f"{type(exc).__name__}:{exc}"
    passed = quick_check == "ok" and schema_present
    return OperationalCheck(
        name="notification_receipt_integrity",
        measured={
            "path": str(database or ""),
            "exists": bool(database and database.exists()),
            "quick_check": quick_check,
            "schema_present": schema_present,
            "error": error,
        },
        threshold="spx.sqlite quick_check=ok and notification event/attempt schema present",
        passed=passed,
        reason=(
            "unified notification attempt store healthy"
            if passed
            else "unified notification attempt store unavailable or invalid"
        ),
    )


def timely_delivered_event_ids(
    path: str | Path | None,
    event_ids: Iterable[str],
    *,
    receipt_path: str | Path | None = None,
) -> tuple[frozenset[str], dict[str, object]]:
    del receipt_path
    requested = tuple(dict.fromkeys(str(value) for value in event_ids if value))
    grouped, error = _unified_rows(path, requested)
    if error is not None:
        return frozenset(), {"status": error, "events": {}}
    accepted: set[str] = set()
    diagnostics: dict[str, object] = {}
    for event_id in requested:
        all_rows = grouped.get(event_id, [])
        channels: dict[str, list[sqlite3.Row]] = {}
        for row in all_rows:
            if row["channel"] != "__cancellation__":
                channels.setdefault(str(row["channel"]), []).append(row)
        reasons: list[str] = []
        receipt_ids: list[str] = []
        first_delivery_seconds: float | None = None
        if not channels:
            reasons.append("missing_event_or_targets")
        created_at = _sqlite_utc(all_rows[0]["created_at"]) if all_rows else None
        expires_at = _sqlite_utc(all_rows[0]["expires_at"]) if all_rows else None
        delivered_times: list[datetime] = []
        for channel, rows in channels.items():
            if str(rows[0]["status"]) != "delivered":
                reasons.append(f"target_not_delivered:{channel}")
                continue
            successful = [
                row
                for row in rows
                if row["ok"] == 1
                and row["finished_at"] is not None
                and (
                    str(row["outcome"]) == "delivered"
                    or (channel == "rust_ingress" and str(row["outcome"]) == "forwarded_to_rust")
                )
            ]
            if not successful:
                reasons.append(f"success_attempt_missing:{channel}")
                continue
            receipt = successful[0]
            receipt_ids.append(str(receipt["attempt_id"]))
            delivered_at = _sqlite_utc(receipt["finished_at"])
            if delivered_at is not None:
                delivered_times.append(delivered_at)
        if created_at is None:
            reasons.append("created_at_invalid")
        elif delivered_times:
            first_delivery_seconds = (min(delivered_times) - created_at).total_seconds()
            if not 0 <= first_delivery_seconds <= MAX_TRADE_READY_FIRST_DELIVERY_SECONDS:
                reasons.append("first_delivery_slo_breached")
        if expires_at is None:
            reasons.append("expires_at_invalid")
        elif created_at is not None and created_at >= expires_at:
            reasons.append("enqueued_at_or_after_expiry")
        elif any(delivered_at > expires_at for delivered_at in delivered_times):
            reasons.append("delivered_after_expiry")
        if not reasons:
            accepted.add(event_id)
        diagnostics[event_id] = {
            "target_count": len(channels),
            "success_receipt_ids": receipt_ids,
            "first_delivery_seconds": (
                round(first_delivery_seconds, 6) if first_delivery_seconds is not None else None
            ),
            "reasons": reasons,
        }
    return frozenset(accepted), {"status": "ready", "events": diagnostics}


def explicitly_terminal_event_ids(
    path: str | Path | None,
    event_ids: Iterable[str],
    *,
    receipt_path: str | Path | None = None,
) -> tuple[frozenset[str], dict[str, object]]:
    del receipt_path
    requested = tuple(dict.fromkeys(str(value) for value in event_ids if value))
    grouped, error = _unified_rows(path, requested)
    if error is not None:
        return frozenset(), {"status": error, "events": {}}
    accepted: set[str] = set()
    diagnostics: dict[str, object] = {}
    for event_id in requested:
        rows = grouped.get(event_id, [])
        targets = [row for row in rows if row["channel"] != "__cancellation__"]
        fenced = any(row["channel"] == "__cancellation__" for row in rows)
        reasons: list[str] = []
        if not fenced:
            reasons.append("cancellation_fence_missing")
        if targets and any(
            row["cancelled_at"] is None and row["last_error"] != "expired_before_transport"
            for row in targets
        ):
            reasons.append("targets_not_source_terminal")
        if not reasons:
            accepted.add(event_id)
        diagnostics[event_id] = {
            "target_count": len({str(row["channel"]) for row in targets}),
            "terminal_outcomes": sorted(
                {
                    str(row["cancel_reason"] or row["last_error"])
                    for row in targets
                    if row["cancel_reason"] or row["last_error"]
                }
            ),
            "reasons": reasons,
        }
    return frozenset(accepted), {"status": "ready", "events": diagnostics}


def trade_ready_delivery_check(
    producer_rows: Iterable[Mapping[str, object]],
    *,
    trade_intent_rows: Iterable[Mapping[str, object]],
    outbox_path: str | Path | None,
    receipt_path: str | Path | None,
) -> OperationalCheck:
    producer_rows = tuple(producer_rows)
    trade_intent_rows = tuple(trade_intent_rows)
    expectation_rows = tuple(
        row for row in producer_rows if row.get("record_type") == "trade_ready_delivery_expectation"
    )
    expectations_by_event: dict[str, str] = {}
    events_by_semantic: dict[str, set[str]] = {}
    expectation_conflicts: set[str] = set()
    malformed_expectations = 0
    for row in expectation_rows:
        semantic = str(row.get("semantic_key") or "")
        event_id = str(row.get("delivery_event_id") or "")
        if not semantic or not event_id:
            malformed_expectations += 1
            continue
        previous = expectations_by_event.get(event_id)
        if previous is not None and previous != semantic:
            expectation_conflicts.add(event_id)
            continue
        expectations_by_event[event_id] = semantic
        events_by_semantic.setdefault(semantic, set()).add(event_id)

    signal_rows = tuple(row for row in trade_intent_rows if _is_trade_ready_signal(row))
    ready_rows = tuple(row for row in signal_rows if row.get("status") == "trade_ready")
    diagnostic_rows = tuple(row for row in signal_rows if row.get("status") != "trade_ready")
    signaled_by_event: dict[str, str] = {}
    audit_conflicts: set[str] = set()
    malformed_signal_rows = 0
    malformed_ready_rows = 0
    for row in signal_rows:
        semantic = str(row.get("semantic_key") or "")
        event_id = _trade_ready_delivery_event_id(row)
        if not semantic or not event_id:
            malformed_signal_rows += 1
            if row.get("status") == "trade_ready":
                malformed_ready_rows += 1
            continue
        previous = signaled_by_event.get(event_id)
        if previous is not None and previous != semantic:
            audit_conflicts.add(event_id)
            continue
        signaled_by_event[event_id] = semantic

    executable_by_event = {
        event_id: semantic
        for row in ready_rows
        if (event_id := _trade_ready_delivery_event_id(row))
        and (semantic := str(row.get("semantic_key") or ""))
    }

    expected_ids = set(expectations_by_event)
    signaled_ids = set(signaled_by_event)
    executable_ids = set(executable_by_event)
    diagnostic_only_ids = signaled_ids - executable_ids
    missing_expectation_events = sorted(signaled_ids - expected_ids)
    expectation_without_audit_events = sorted(expected_ids - signaled_ids)
    semantic_mismatch_events = sorted(
        event_id
        for event_id in expected_ids & signaled_ids
        if expectations_by_event[event_id] != signaled_by_event[event_id]
    )
    delivered, delivery_diagnostics = timely_delivered_event_ids(
        outbox_path,
        expected_ids,
        receipt_path=receipt_path,
    )
    terminal, terminal_diagnostics = explicitly_terminal_event_ids(
        outbox_path,
        expected_ids - set(delivered),
        receipt_path=receipt_path,
    )
    accepted_ids = set(delivered) | set(terminal)
    missing_delivery_events = sorted(expected_ids - accepted_ids)
    delivered_semantics = {
        semantic
        for semantic, event_ids in events_by_semantic.items()
        if event_ids <= set(delivered)
    }
    terminal_semantics = {
        semantic
        for semantic, event_ids in events_by_semantic.items()
        if event_ids <= accepted_ids and bool(event_ids & set(terminal))
    }
    missing_delivery_semantics = sorted(
        {expectations_by_event[event_id] for event_id in missing_delivery_events}
    )
    passed = not any(
        (
            malformed_expectations,
            malformed_signal_rows,
            expectation_conflicts,
            audit_conflicts,
            missing_expectation_events,
            expectation_without_audit_events,
            semantic_mismatch_events,
            missing_delivery_events,
        )
    )
    reason = (
        "producer coverage is evaluated separately; no TradeReady delivery was expected"
        if not expectations_by_event and not signal_rows
        else (
            f"settled TradeReady delivery events "
            f"{len(accepted_ids)}/{len(expectations_by_event)} "
            f"(delivered {len(delivered)}, source-terminal {len(terminal)}); "
            f"malformed expectations {malformed_expectations}; "
            f"malformed signal audit rows {malformed_signal_rows}"
        )
    )
    return OperationalCheck(
        name="trade_ready_notification_delivery",
        measured={
            "expectation_rows": len(expectation_rows),
            "expected_semantics": len(events_by_semantic),
            "expected_events": len(expectations_by_event),
            "signal_rows": len(signal_rows),
            "ready_rows": len(ready_rows),
            "diagnostic_signal_rows": len(diagnostic_rows),
            "audited_events": len(signaled_by_event),
            "signal_events": len(signaled_by_event),
            "executable_ready_events": len(executable_by_event),
            "diagnostic_only_events": len(diagnostic_only_ids),
            "diagnostic_only_event_ids": sorted(diagnostic_only_ids),
            "diagnostic_status_counts": dict(
                sorted(
                    Counter(str(row.get("status") or "unknown") for row in diagnostic_rows).items()
                )
            ),
            "candidate_events": len(expected_ids),
            "timely_delivered_events": len(delivered),
            "explicitly_terminal_events": len(terminal),
            "accepted_events": len(accepted_ids),
            "delivered_semantics": len(delivered_semantics),
            "terminally_settled_semantics": len(terminal_semantics),
            "malformed_expectations": malformed_expectations,
            "malformed_signal_rows": malformed_signal_rows,
            "malformed_ready_rows": malformed_ready_rows,
            "expectation_identity_conflicts": sorted(expectation_conflicts),
            "audit_identity_conflicts": sorted(audit_conflicts),
            "missing_expectation_events": missing_expectation_events,
            "expectation_without_audit_events": expectation_without_audit_events,
            "semantic_mismatch_events": semantic_mismatch_events,
            "missing_delivery_events": missing_delivery_events,
            "missing_delivery_semantics": missing_delivery_semantics,
            "event_diagnostics": delivery_diagnostics,
            "terminal_diagnostics": terminal_diagnostics,
        },
        threshold=(
            "every unique TradeReady expectation event exactly matches a "
            "signal audit event; only status=trade_ready is executable, and "
            "each event is either fully delivered before expiry with "
            "first delivery <=5s and real receipt mirrors, or has explicit "
            "per-target cancellation/expiry receipt mirrors"
        ),
        passed=passed,
        reason=reason,
    )


def _is_trade_ready_signal(row: Mapping[str, object]) -> bool:
    status = str(row.get("status") or "")
    return bool(
        status == "trade_ready"
        or (
            row.get("signal_status") == "trade_ready"
            and status in {"ready_pending_delivery", "delivery_blocked"}
        )
    )


def parse_at(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed


def read_jsonl(path: Path) -> tuple[dict[str, object], ...]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return ()
    rows: list[dict[str, object]] = []
    for line in lines:
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return tuple(rows)


def read_jsonl_with_integrity(
    path: Path,
) -> tuple[tuple[dict[str, object], ...], dict[str, object]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return (), {
            "path": str(path),
            "exists": False,
            "line_count": 0,
            "valid_rows": 0,
            "malformed_lines": 0,
        }
    rows: list[dict[str, object]] = []
    malformed_lines = 0
    for line in lines:
        if not line:
            malformed_lines += 1
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            malformed_lines += 1
            continue
        if not isinstance(row, dict):
            malformed_lines += 1
            continue
        rows.append(row)
    return tuple(rows), {
        "path": str(path),
        "exists": True,
        "line_count": len(lines),
        "valid_rows": len(rows),
        "malformed_lines": malformed_lines,
    }


def read_json_object(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, dict) else {}
