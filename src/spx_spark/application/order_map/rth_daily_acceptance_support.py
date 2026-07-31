"""Shared validation and persistence helpers for RTH daily acceptance."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Mapping

from spx_spark.application.market_features.trade_intent_runtime_support import (
    _trade_ready_delivery_event_id,
)
from spx_spark.market_calendar import MarketSession
from spx_spark.notifier.receipts import (
    inspect_delivery_receipt_store,
    notification_event_id,
)


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


def fully_delivered_event_ids(
    path: str | Path | None,
    event_ids: Iterable[str],
) -> frozenset[str]:
    requested = tuple(dict.fromkeys(str(value) for value in event_ids if value))
    if path is None or not str(path) or not requested:
        return frozenset()
    database = Path(path)
    if not database.exists():
        return frozenset()
    placeholders = ",".join("?" for _ in requested)
    try:
        with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
            rows = connection.execute(
                "SELECT event_id FROM notification_delivery_targets "
                f"WHERE event_id IN ({placeholders}) "
                "GROUP BY event_id "
                "HAVING COUNT(*) > 0 "
                "AND SUM(CASE WHEN status = 'delivered' THEN 0 ELSE 1 END) = 0",
                requested,
            ).fetchall()
    except (OSError, sqlite3.Error):
        return frozenset()
    return frozenset(str(row[0]) for row in rows)


def _verified_receipt_mirror_ids(
    receipt_path: str | Path | None,
    receipt_ids: Iterable[str],
) -> tuple[frozenset[str], dict[str, object]]:
    requested = tuple(dict.fromkeys(str(value) for value in receipt_ids if value))
    inspection = inspect_delivery_receipt_store(
        receipt_path or "",
        required_mirror_ids=requested,
    )
    core_healthy = (
        inspection.exists
        and inspection.quick_check == "ok"
        and inspection.journal_mode == "delete"
        and inspection.synchronous == "full"
        and inspection.schema_present
    )
    verified = set(requested) - set(inspection.missing_mirror_ids) if core_healthy else set()
    return frozenset(verified), {
        "path": str(receipt_path or ""),
        "exists": inspection.exists,
        "quick_check": inspection.quick_check,
        "journal_mode": inspection.journal_mode,
        "synchronous": inspection.synchronous,
        "schema_present": inspection.schema_present,
        "required_mirror_ids": len(requested),
        "verified_mirror_ids": len(verified),
        "missing_mirror_ids": list(inspection.missing_mirror_ids),
        "error": inspection.error,
    }


def receipt_store_check(
    outbox_path: str | Path | None,
    receipt_path: str | Path | None,
) -> OperationalCheck:
    """Verify every durable outbox receipt intent in the real receipt DB."""

    receipt_ids: tuple[str, ...] = ()
    outbox_error: str | None = None
    if outbox_path is None or not str(outbox_path):
        outbox_error = "outbox_not_configured"
    else:
        database = Path(outbox_path)
        if not database.exists():
            outbox_error = "outbox_missing"
        else:
            try:
                with sqlite3.connect(
                    f"file:{database}?mode=ro",
                    uri=True,
                ) as connection:
                    receipt_ids = tuple(
                        str(row[0])
                        for row in connection.execute(
                            "SELECT receipt_id "
                            "FROM notification_delivery_terminal_receipts "
                            "ORDER BY receipt_id"
                        )
                    )
            except (OSError, sqlite3.Error) as exc:
                outbox_error = f"outbox_query_failed:{type(exc).__name__}"
    verified, diagnostics = _verified_receipt_mirror_ids(
        receipt_path,
        receipt_ids,
    )
    passed = (
        outbox_error is None
        and diagnostics["exists"] is True
        and diagnostics["quick_check"] == "ok"
        and diagnostics["journal_mode"] == "delete"
        and diagnostics["synchronous"] == "full"
        and diagnostics["schema_present"] is True
        and len(verified) == len(receipt_ids)
    )
    return OperationalCheck(
        name="notification_receipt_integrity",
        measured={
            **diagnostics,
            "outbox_error": outbox_error,
            "outbox_receipt_ids": len(receipt_ids),
        },
        threshold=(
            "receipt DB quick_check=ok, journal_mode=delete, "
            "synchronous=full, exact schema, and every outbox receipt_id "
            "joins through its durable mirror to a receipt attempt"
        ),
        passed=passed,
        reason=(
            f"receipt store {diagnostics['quick_check']}; mirrored "
            f"{len(verified)}/{len(receipt_ids)} outbox receipt ids"
            + (f"; {outbox_error}" if outbox_error else "")
        ),
    )


def timely_delivered_event_ids(
    path: str | Path | None,
    event_ids: Iterable[str],
    *,
    receipt_path: str | Path | None = None,
) -> tuple[frozenset[str], dict[str, object]]:
    requested = tuple(dict.fromkeys(str(value) for value in event_ids if value))
    if path is None or not str(path) or not requested:
        return frozenset(), {
            "status": "not_configured" if path is None or not str(path) else "nothing_expected",
            "events": {},
        }
    database = Path(path)
    if not database.exists():
        return frozenset(), {"status": "outbox_missing", "events": {}}
    placeholders = ",".join("?" for _ in requested)
    try:
        with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
            connection.row_factory = sqlite3.Row
            target_rows = connection.execute(
                """
                SELECT e.event_id, e.created_at, e.expires_at,
                       t.sink, t.status, t.delivered_at
                FROM notification_delivery_events AS e
                JOIN notification_delivery_targets AS t USING (event_id)
                WHERE e.event_id IN ("""
                + placeholders
                + """)
                ORDER BY e.event_id, t.sink
                """,
                requested,
            ).fetchall()
            receipt_rows = connection.execute(
                """
                SELECT receipt_id, event_id, sink
                FROM notification_delivery_terminal_receipts
                WHERE event_id IN ("""
                + placeholders
                + """)
                  AND outcome = 'delivered'
                  AND attempted = 1
                  AND ok = 1
                  AND recorded_at IS NOT NULL
                ORDER BY event_id, sink, terminal_at, receipt_id
                """,
                requested,
            ).fetchall()
    except (OSError, sqlite3.Error) as exc:
        return frozenset(), {
            "status": f"outbox_query_failed:{type(exc).__name__}",
            "events": {},
        }

    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in target_rows:
        grouped.setdefault(str(row["event_id"]), []).append(row)
    receipt_ids = tuple(str(row["receipt_id"]) for row in receipt_rows)
    verified_receipt_ids, receipt_diagnostics = _verified_receipt_mirror_ids(
        receipt_path,
        receipt_ids,
    )
    receipts_by_event_sink: dict[tuple[str, str], list[str]] = {}
    for row in receipt_rows:
        receipts_by_event_sink.setdefault(
            (str(row["event_id"]), str(row["sink"])),
            [],
        ).append(str(row["receipt_id"]))
    accepted: set[str] = set()
    diagnostics: dict[str, object] = {}
    for event_id in requested:
        rows = grouped.get(event_id, [])
        reasons: list[str] = []
        first_delivery_seconds: float | None = None
        if not rows:
            reasons.append("missing_event_or_targets")
        else:
            created_at = parse_at(rows[0]["created_at"])
            expires_at = parse_at(rows[0]["expires_at"])
            delivered_times = [
                value for row in rows if (value := parse_at(row["delivered_at"])) is not None
            ]
            if any(str(row["status"]) != "delivered" for row in rows):
                reasons.append("targets_not_fully_delivered")
            if len(delivered_times) != len(rows):
                reasons.append("delivered_at_missing")
            if created_at is None:
                reasons.append("created_at_invalid")
            elif delivered_times:
                first_delivery_seconds = (min(delivered_times) - created_at).total_seconds()
                if (
                    first_delivery_seconds < 0
                    or first_delivery_seconds > MAX_TRADE_READY_FIRST_DELIVERY_SECONDS
                ):
                    reasons.append("first_delivery_slo_breached")
            if expires_at is None:
                reasons.append("expires_at_invalid")
            else:
                if created_at is not None and created_at >= expires_at:
                    reasons.append("enqueued_at_or_after_expiry")
                if any(delivered_at > expires_at for delivered_at in delivered_times):
                    reasons.append("delivered_after_expiry")
            success_receipt_ids = {
                receipt_id
                for row in rows
                for receipt_id in receipts_by_event_sink.get(
                    (event_id, str(row["sink"])),
                    (),
                )
                if receipt_id in verified_receipt_ids
            }
            if any(
                not any(
                    receipt_id in verified_receipt_ids
                    for receipt_id in receipts_by_event_sink.get(
                        (event_id, str(row["sink"])),
                        (),
                    )
                )
                for row in rows
            ):
                reasons.append("success_receipt_missing_or_unmirrored")
        if not reasons:
            accepted.add(event_id)
        diagnostics[event_id] = {
            "target_count": len(rows),
            "success_receipt_ids": sorted(success_receipt_ids if rows else ()),
            "first_delivery_seconds": (
                round(first_delivery_seconds, 6) if first_delivery_seconds is not None else None
            ),
            "reasons": reasons,
        }
    return frozenset(accepted), {
        "status": "ready",
        "events": diagnostics,
        "receipt_store": receipt_diagnostics,
    }


def explicitly_terminal_event_ids(
    path: str | Path | None,
    event_ids: Iterable[str],
    *,
    receipt_path: str | Path | None = None,
) -> tuple[frozenset[str], dict[str, object]]:
    """Return expectations settled by an explicit source-terminal receipt.

    A cancellation tombstone or a dead-letter target is not sufficient. Every
    target must be dead-lettered and have its own durable, mirrored
    cancellation/expiry receipt. This keeps terminal source outcomes separate
    from successful human delivery.
    """

    requested = tuple(dict.fromkeys(str(value) for value in event_ids if value))
    if path is None or not str(path) or not requested:
        return frozenset(), {
            "status": ("not_configured" if path is None or not str(path) else "nothing_expected"),
            "events": {},
        }
    database = Path(path)
    if not database.exists():
        return frozenset(), {"status": "outbox_missing", "events": {}}
    placeholders = ",".join("?" for _ in requested)
    try:
        with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
            connection.row_factory = sqlite3.Row
            target_rows = connection.execute(
                """
                SELECT e.event_id, e.status AS event_status,
                       t.sink, t.status AS target_status
                FROM notification_delivery_events AS e
                JOIN notification_delivery_targets AS t USING (event_id)
                WHERE e.event_id IN ("""
                + placeholders
                + """)
                ORDER BY e.event_id, t.sink
                """,
                requested,
            ).fetchall()
            receipt_rows = connection.execute(
                """
                SELECT receipt_id, event_id, sink, outcome, attempted, ok,
                       recorded_at
                FROM notification_delivery_terminal_receipts
                WHERE event_id IN ("""
                + placeholders
                + """)
                  AND outcome IN ('cancelled_before_delivery',
                                  'expired_before_delivery')
                ORDER BY event_id, sink, terminal_at, receipt_id
                """,
                requested,
            ).fetchall()
    except (OSError, sqlite3.Error) as exc:
        return frozenset(), {
            "status": f"outbox_query_failed:{type(exc).__name__}",
            "events": {},
        }

    targets_by_event: dict[str, list[sqlite3.Row]] = {}
    for row in target_rows:
        targets_by_event.setdefault(str(row["event_id"]), []).append(row)
    receipts_by_event_sink: dict[tuple[str, str], list[sqlite3.Row]] = {}
    for row in receipt_rows:
        receipts_by_event_sink.setdefault(
            (str(row["event_id"]), str(row["sink"])),
            [],
        ).append(row)
    receipt_ids = tuple(str(row["receipt_id"]) for row in receipt_rows)
    verified_receipt_ids, receipt_diagnostics = _verified_receipt_mirror_ids(
        receipt_path,
        receipt_ids,
    )

    accepted: set[str] = set()
    diagnostics: dict[str, object] = {}
    for event_id in requested:
        targets = targets_by_event.get(event_id, [])
        reasons: list[str] = []
        terminal_receipt_ids: list[str] = []
        terminal_outcomes: set[str] = set()
        if not targets:
            reasons.append("missing_event_or_targets")
        elif str(targets[0]["event_status"]) != "dead_letter" or any(
            str(row["target_status"]) != "dead_letter" for row in targets
        ):
            reasons.append("targets_not_source_terminal")
        for target in targets:
            sink = str(target["sink"])
            receipts = [
                row
                for row in receipts_by_event_sink.get((event_id, sink), [])
                if (
                    str(row["outcome"]) in LEGAL_TRADE_READY_TERMINAL_OUTCOMES
                    and not bool(row["attempted"])
                    and not bool(row["ok"])
                    and row["recorded_at"] is not None
                    and str(row["receipt_id"]) in verified_receipt_ids
                )
            ]
            if not receipts:
                reasons.append(f"explicit_terminal_receipt_missing:{sink}")
                continue
            receipt = receipts[-1]
            terminal_receipt_ids.append(str(receipt["receipt_id"]))
            terminal_outcomes.add(str(receipt["outcome"]))
        if len(terminal_outcomes) > 1:
            reasons.append("mixed_terminal_outcomes")
        if not reasons:
            accepted.add(event_id)
        diagnostics[event_id] = {
            "target_count": len(targets),
            "terminal_receipt_ids": terminal_receipt_ids,
            "terminal_outcomes": sorted(terminal_outcomes),
            "reasons": reasons,
        }
    return frozenset(accepted), {
        "status": "ready",
        "events": diagnostics,
        "receipt_store": receipt_diagnostics,
    }


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
