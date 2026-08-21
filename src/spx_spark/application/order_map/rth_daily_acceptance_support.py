"""Shared validation and persistence helpers for RTH daily acceptance."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

from spx_spark.market_calendar import MarketSession
from spx_spark.notifier.unified_delivery import notification_event_id


MAX_TRADE_READY_FIRST_DELIVERY_SECONDS = 5.0
MARKET_STATE_SCHEMA = "market_state_5m.v1"
MARKET_STATE_RULE = "market_state_5m_eight_variable_rules.v2"
TRADE_INTENT_PRODUCER_LEDGER_SCHEMA = "trade_intent_producer_ledger.v1"
STRATEGY_SIGNAL_ENGINE_NAME = "strategy_signal_engine_v2"
HUMAN_TRANSPORT_CHANNELS = frozenset({"bark", "bark_friend", "feishu"})
INTERNAL_NOTIFICATION_SOURCES = frozenset(
    {"alert_pipeline", "rust_ingress", "__cancellation__"}
)
ALLOWED_OUTBOX_JOURNAL_MODES = frozenset({"delete", "wal"})
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
    *,
    source: str | None = None,
    lane: str | None = None,
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
    scoped_filters: list[str] = []
    parameters: list[object] = list(requested)
    if source is not None:
        scoped_filters.append("e.source = ?")
        parameters.append(source)
    if lane is not None:
        scoped_filters.append("e.lane = ?")
        parameters.append(lane)
    filters = (
        " AND (e.channel = '__cancellation__' OR ("
        + " AND ".join(scoped_filters)
        + "))"
        if scoped_filters
        else ""
    )
    try:
        with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
            connection.execute("PRAGMA query_only=ON")
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
                + ")"
                + filters
                + " ORDER BY e.logical_event_id, e.channel, a.id",
                parameters,
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
        channels = {
            str(row["channel"])
            for row in rows
            if row["channel"] in HUMAN_TRANSPORT_CHANNELS
        }
        if channels and all(
            str(row["status"]) == "delivered"
            for row in rows
            if row["channel"] in HUMAN_TRANSPORT_CHANNELS
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
                if schema_present:
                    quick_check = str(
                        connection.execute(
                            "PRAGMA quick_check('notification_attempts')"
                        ).fetchone()[0]
                    ).lower()
                    foreign_key_error = connection.execute(
                        "PRAGMA foreign_key_check('notification_attempts')"
                    ).fetchone()
                    if foreign_key_error is not None:
                        quick_check = "foreign_key_error"
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
        threshold=(
            "notification attempt table quick_check=ok, foreign keys valid, "
            "and notification event/attempt schema present"
        ),
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
    source: str | None = None,
    lane: str | None = None,
) -> tuple[frozenset[str], dict[str, object]]:
    del receipt_path
    requested = tuple(dict.fromkeys(str(value) for value in event_ids if value))
    grouped, error = _unified_rows(path, requested, source=source, lane=lane)
    if error is not None:
        return frozenset(), {"status": error, "events": {}}
    accepted: set[str] = set()
    diagnostics: dict[str, object] = {}
    for event_id in requested:
        all_rows = grouped.get(event_id, [])
        channels: dict[str, list[sqlite3.Row]] = {}
        for row in all_rows:
            if row["channel"] in HUMAN_TRANSPORT_CHANNELS:
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
                and str(row["outcome"]) == "delivered"
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
    source: str | None = None,
    lane: str | None = None,
) -> tuple[frozenset[str], dict[str, object]]:
    del receipt_path
    requested = tuple(dict.fromkeys(str(value) for value in event_ids if value))
    grouped, error = _unified_rows(path, requested, source=source, lane=lane)
    if error is not None:
        return frozenset(), {"status": error, "events": {}}
    accepted: set[str] = set()
    diagnostics: dict[str, object] = {}
    for event_id in requested:
        rows = grouped.get(event_id, [])
        targets = [row for row in rows if row["channel"] in HUMAN_TRANSPORT_CHANNELS]
        fenced = any(row["channel"] == "__cancellation__" for row in rows)
        reasons: list[str] = []
        if not targets:
            reasons.append("missing_event_or_targets")
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
    database_path: str | Path | None,
    *,
    trading_date: str,
    outbox_path: str | Path | None,
    receipt_path: str | Path | None,
) -> OperationalCheck:
    """Settle Trade Ready against strategy_decision opportunities and outbox."""

    decision_rows: tuple[tuple[object, ...], ...] = ()
    decision_status = "not_configured"
    database = Path(database_path) if database_path else None
    if database is not None and database.exists():
        try:
            with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
                connection.execute("PRAGMA query_only=ON")
                decision_rows = tuple(
                    connection.execute(
                        """
                        SELECT decision_id, attributes_json
                        FROM decisions
                        WHERE session_date = ?
                          AND strategy_name = ?
                          AND status = 'selected'
                        ORDER BY decision_at, decision_id
                        """,
                        (trading_date, STRATEGY_SIGNAL_ENGINE_NAME),
                    ).fetchall()
                )
            decision_status = "ready"
        except (OSError, sqlite3.Error) as exc:
            decision_status = f"query_failed:{type(exc).__name__}"
    elif database is not None:
        decision_status = "database_missing"

    opportunity_ids: set[str] = set()
    malformed_decisions = 0
    for _decision_id, attributes_json in decision_rows:
        try:
            attributes = json.loads(str(attributes_json))
        except (TypeError, json.JSONDecodeError):
            malformed_decisions += 1
            continue
        if not isinstance(attributes, dict) or attributes.get("action_authority") != "manual":
            continue
        candidate = attributes.get("candidate")
        opportunity_id = (
            str(candidate.get("opportunity_id") or "").strip()
            if isinstance(candidate, dict)
            else ""
        )
        if not opportunity_id:
            malformed_decisions += 1
            continue
        opportunity_ids.add(opportunity_id)
    expectations_by_event = {
        f"{opportunity_id}:ready": opportunity_id for opportunity_id in opportunity_ids
    }
    expected_ids = set(expectations_by_event)
    delivered, delivery_diagnostics = timely_delivered_event_ids(
        outbox_path,
        expected_ids,
        receipt_path=receipt_path,
        source="strategy_decision",
        lane="trade_ready",
    )
    terminal, terminal_diagnostics = explicitly_terminal_event_ids(
        outbox_path,
        expected_ids - set(delivered),
        receipt_path=receipt_path,
        source="strategy_decision",
        lane="trade_ready",
    )
    accepted_ids = set(delivered) | set(terminal)
    missing_delivery_events = sorted(expected_ids - accepted_ids)
    passed = (
        decision_status == "ready"
        and malformed_decisions == 0
        and not missing_delivery_events
    )
    reason = (
        "producer coverage is evaluated separately; no strategy_decision "
        "TradeReady delivery was expected"
        if decision_status == "ready" and not expectations_by_event
        else (
            f"settled strategy_decision TradeReady events "
            f"{len(accepted_ids)}/{len(expectations_by_event)} "
            f"(delivered {len(delivered)}, source-terminal {len(terminal)}); "
            f"unique opportunities {len(opportunity_ids)}; "
            f"decision query {decision_status}"
        )
    )
    return OperationalCheck(
        name="trade_ready_notification_delivery",
        measured={
            "authority": "strategy_decision",
            "trading_date": trading_date,
            "decision_query_status": decision_status,
            "selected_decision_rows": len(decision_rows),
            "malformed_selected_decisions": malformed_decisions,
            "expected_opportunities": len(opportunity_ids),
            "expected_events": len(expectations_by_event),
            "timely_delivered_events": len(delivered),
            "explicitly_terminal_events": len(terminal),
            "accepted_events": len(accepted_ids),
            "missing_delivery_events": missing_delivery_events,
            "event_diagnostics": delivery_diagnostics,
            "terminal_diagnostics": terminal_diagnostics,
        },
        threshold=(
            "every unique manual strategy_decision opportunity has "
            "notification_events(source=strategy_decision,lane=trade_ready) "
            "that are either fully delivered before expiry with first delivery "
            "<=5s and real receipt mirrors, or have explicit per-target "
            "cancellation/expiry receipt mirrors"
        ),
        passed=passed,
        reason=reason,
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
