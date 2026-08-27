"""Post-close operational acceptance for the complete RTH data path.

The regular post-close review validates market-data completeness. This
projection adds the cross-process contracts that unit tests cannot observe:
five-minute TradeIntent producer coverage, half-hour report delivery,
per-event notification settlement, and the outbox/receipt durability chain.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, Mapping

from spx_spark.application.order_map.level_decision_acceptance import (
    build_acceptance_report,
    write_acceptance_report,
)
from spx_spark.application.order_map.desk_projection_export import (
    rust_report_owner_enabled,
)
from spx_spark.application.order_map.report_clock import (
    RTH_REPORT_START_GRACE_SECONDS,
    rth_report_schedule_for_session,
    rth_report_slot_for_session,
)
from spx_spark.application.order_map.rth_daily_acceptance_support import (
    ALLOWED_OUTBOX_JOURNAL_MODES,
    HUMAN_TRANSPORT_CHANNELS,
    INTERNAL_NOTIFICATION_SOURCES,
    KNOWN_MARKET_STATES,
    MARKET_STATE_RULE,
    MARKET_STATE_SCHEMA,
    TRADE_INTENT_PRODUCER_LEDGER_SCHEMA,
    OperationalCheck,
    fully_delivered_event_ids as _fully_delivered_event_ids,
    read_json_object as _read_json_object,
    read_jsonl as _read_jsonl,
    read_jsonl_with_integrity as _read_jsonl_with_integrity,
    receipt_store_check as _receipt_store_check,
    report_event_id as _report_event_id,
    trade_ready_delivery_check as _trade_ready_delivery_check,
)
from spx_spark.config import NotificationSettings, StorageSettings
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, MarketCalendar, MarketSession
from spx_spark.notifier.dispatcher import EnqueueResult, enqueue_notification
from spx_spark.notifier.model import NotificationEnvelope
from spx_spark.notifier.unified_delivery import notification_event_id
from spx_spark.settings import load_app_settings
from spx_spark.settings.level_decision import LevelDecisionPolicy
from spx_spark.state_io import atomic_write_json_secure


SCHEMA_VERSION = "rth_daily_acceptance.v1"
SPRING_MODEL_VERSION = "spring_gamma_v3_decoupled_es_shadow.v3"
MIN_SPRING_MINUTE_COVERAGE = 0.95
MIN_REPORT_SLOT_COVERAGE = 1.0
MIN_REPORT_PROJECTION_COVERAGE = 0.95
MIN_OPTION_OVERLAY_READY_RATIO = 0.75


def build_rth_daily_acceptance(
    data_root: str | Path,
    *,
    trading_date: date,
    level_policy: LevelDecisionPolicy,
    outbox_path: str | Path | None = None,
    receipt_path: str | Path | None = None,
    rust_delivery_ledger_path: str | Path | None = None,
    rust_report_owner: bool | None = None,
    now: datetime | None = None,
    calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR,
    spring_required: bool = True,
    spring_report_required: bool | None = None,
) -> dict[str, object]:
    """Build one deterministic, fail-closed daily operational verdict."""

    generated_at = _as_utc(now or datetime.now(tz=timezone.utc))
    session = calendar.session(trading_date)
    if session is None:
        raise ValueError(f"{trading_date.isoformat()} is not a trading day")
    root = Path(data_root)
    report_owner = (
        rust_report_owner_enabled() if rust_report_owner is None else bool(rust_report_owner)
    )
    report_spring_required = (
        bool(spring_required) if spring_report_required is None else bool(spring_report_required)
    )
    spring_rows = _read_jsonl(
        root
        / "features"
        / "spring_gamma_v3"
        / f"date={trading_date.isoformat()}"
        / "predictions.jsonl"
    )
    report_rows = _read_jsonl(
        root / "audit" / "order_map_pricing" / f"date={trading_date.isoformat()}" / "reports.jsonl"
    )
    trade_intent_path = (
        root / "features" / "trade_intents" / f"date={trading_date.isoformat()}" / "events.jsonl"
    )
    trade_intent_rows, trade_intent_integrity = _read_jsonl_with_integrity(trade_intent_path)
    producer_ledger_path = (
        root
        / "features"
        / "trade_intent_producer_ledger"
        / f"date={trading_date.isoformat()}"
        / "events.jsonl"
    )
    producer_ledger_rows, producer_ledger_integrity = _read_jsonl_with_integrity(
        producer_ledger_path
    )
    post_close_review = _read_json_object(
        root / "reports" / "spx_options_review" / f"date={trading_date.isoformat()}" / "review.json"
    )
    level_report = build_acceptance_report(root, policy=level_policy, now=generated_at)
    level_path = write_acceptance_report(root, level_report)
    checks = [
        *(_spring_checks(session, spring_rows) if spring_required else ()),
        *_report_checks(
            session,
            report_rows,
            outbox_path=outbox_path,
            rust_delivery_ledger_path=rust_delivery_ledger_path,
            rust_report_owner=report_owner,
            spring_report_required=report_spring_required,
        ),
        _trade_intent_producer_coverage_check(
            session,
            producer_ledger_rows,
            integrity=producer_ledger_integrity,
        ),
        _trade_intent_audit_integrity_check(
            trade_intent_rows,
            integrity=trade_intent_integrity,
        ),
        _trade_ready_delivery_check(
            outbox_path,
            trading_date=trading_date.isoformat(),
            outbox_path=outbox_path,
            receipt_path=receipt_path,
        ),
        _post_close_review_check(post_close_review),
        _outbox_check(outbox_path),
        _receipt_store_check(outbox_path, receipt_path),
        _level_decision_signal_check(level_report),
    ]
    passed = all(check.passed for check in checks)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "trading_date": trading_date.isoformat(),
        "status": "complete" if passed else "degraded",
        "spring_required": spring_required,
        "spring_report_required": report_spring_required,
        "rust_report_owner": report_owner,
        "session_clock": {
            "timezone": str(session.open_at.tzinfo),
            "open_at": session.open_at.isoformat(),
            "close_at": session.close_at.isoformat(),
            "expected_five_minute_bars": session.expected_five_minute_buckets,
            "expected_spring_minutes": (_expected_minute_count(session) if spring_required else 0),
            "expected_report_slots": len(_expected_report_slots(session)),
            "report_scheduler_tolerance_seconds": RTH_REPORT_START_GRACE_SECONDS,
        },
        "checks": [asdict(check) for check in checks],
        "failed_checks": [check.name for check in checks if not check.passed],
        "level_decision_acceptance": {
            "path": str(level_path),
            "generated_at": level_report.get("generated_at"),
            "gates": level_report.get("gates"),
            "counts": level_report.get("counts"),
            "acceptance_gates_passed": level_report.get("acceptance_gates_passed"),
            "promoted": level_report.get("promoted"),
        },
    }


def write_rth_daily_acceptance(
    data_root: str | Path,
    report: Mapping[str, object],
) -> tuple[Path, Path]:
    trading_date = str(report["trading_date"])
    root = Path(data_root)
    historical = (
        root / "reports" / "rth_daily_acceptance" / f"date={trading_date}" / "acceptance.json"
    )
    latest = root / "latest" / "rth_daily_acceptance.json"
    atomic_write_json_secure(historical, report)
    atomic_write_json_secure(latest, report)
    return historical, latest


def enqueue_degraded_acceptance(
    report: Mapping[str, object],
    *,
    settings: NotificationSettings,
    occurred_at: datetime,
    enqueue: Callable[..., EnqueueResult] = enqueue_notification,
) -> dict[str, object]:
    """Queue one idempotent ops alert; the delivery worker owns network I/O."""

    if report.get("status") == "complete":
        return {"status": "not_required", "accepted": True}
    trading_date = str(report.get("trading_date") or "")
    failed = report.get("failed_checks")
    failed_names = [str(value) for value in failed] if isinstance(failed, list) else []
    text = (
        f"RTH 每日端到端验收未通过（{trading_date}）。\n"
        f"失败项: {', '.join(failed_names) or 'unknown'}\n"
        "系统保持 fail-closed；请查看 latest/rth_daily_acceptance.json。"
    )
    event_at = _as_utc(occurred_at)
    try:
        result = enqueue(
            settings,
            NotificationEnvelope(
                event_id=notification_event_id(
                    "rth_daily_acceptance_degraded",
                    source="rth_daily_acceptance",
                    occurred_at=event_at,
                    identity=json.dumps(
                        {
                            "trading_date": trading_date,
                            "failed_checks": sorted(failed_names),
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                ),
                source="rth_daily_acceptance",
                kind="rth_daily_acceptance_degraded",
                lane="ops_transition",
                occurred_at=event_at,
            ),
            title="SPX RTH 端到端验收异常",
            text=text,
            friend=False,
            enqueued_at=datetime.now(tz=timezone.utc),
        )
    except Exception as exc:
        return {
            "status": "enqueue_failed",
            "accepted": False,
            "reason": type(exc).__name__,
        }
    return {
        "status": result.outcome,
        "accepted": result.accepted,
        "inserted": result.inserted,
        "duplicate": result.duplicate,
    }


def _spring_checks(
    session: MarketSession,
    rows: Iterable[Mapping[str, object]],
) -> tuple[OperationalCheck, ...]:
    rth_rows = [
        row
        for row in rows
        if row.get("model_version") == SPRING_MODEL_VERSION
        and row.get("session") == "rth"
        and _has_valid_rth_market_state(row)
        and _inside_session(row.get("as_of"), session, include_close=False)
    ]
    minute_slots = {
        observed.replace(second=0, microsecond=0)
        for row in rth_rows
        if (observed := _parse_at(row.get("as_of"))) is not None
    }
    expected = _expected_minute_count(session)
    minute_ratio = len(minute_slots) / expected if expected else 0.0
    ready_count = sum(
        isinstance(row.get("option_overlay"), Mapping)
        and row["option_overlay"].get("status") == "ready"
        for row in rth_rows
    )
    ready_ratio = ready_count / len(rth_rows) if rth_rows else 0.0
    return (
        OperationalCheck(
            name="spring_rth_minute_coverage",
            measured=round(minute_ratio, 6),
            threshold=MIN_SPRING_MINUTE_COVERAGE,
            passed=minute_ratio >= MIN_SPRING_MINUTE_COVERAGE,
            reason=(
                f"Spring RTH minute slots {len(minute_slots)}/{expected} "
                f"({minute_ratio:.1%}); required >= {MIN_SPRING_MINUTE_COVERAGE:.0%}"
            ),
        ),
        OperationalCheck(
            name="spring_option_overlay_ready_ratio",
            measured=round(ready_ratio, 6),
            threshold=MIN_OPTION_OVERLAY_READY_RATIO,
            passed=ready_ratio >= MIN_OPTION_OVERLAY_READY_RATIO,
            reason=(
                f"ready option overlays {ready_count}/{len(rth_rows)} "
                f"({ready_ratio:.1%}); required >= {MIN_OPTION_OVERLAY_READY_RATIO:.0%}"
            ),
        ),
    )


def _report_checks(
    session: MarketSession,
    rows: Iterable[Mapping[str, object]],
    *,
    outbox_path: str | Path | None,
    rust_delivery_ledger_path: str | Path | None = None,
    rust_report_owner: bool = False,
    spring_report_required: bool = True,
) -> tuple[OperationalCheck, ...]:
    expected_slots = _expected_report_slots(session)
    total = len(expected_slots)
    projection_by_slot: dict[datetime, Mapping[str, object]] = {}
    allowed_kinds = {"status_snapshot", "status"} if rust_report_owner else {"status"}
    for row in rows:
        if row.get("report_kind") not in allowed_kinds:
            continue
        observed = _parse_at(row.get("occurred_at")) or _parse_at(row.get("generated_at"))
        slot = _report_slot(observed, session)
        if slot is not None:
            projection_by_slot[slot] = row
    if rust_report_owner:
        ledger = Path(rust_delivery_ledger_path) if rust_delivery_ledger_path else None
        rust_status = "not_configured"
        rust_rows: tuple[sqlite3.Row, ...] = ()
        if ledger is not None and ledger.exists():
            slot_keys = tuple(slot.isoformat() for slot in expected_slots)
            try:
                with sqlite3.connect(f"file:{ledger}?mode=ro", uri=True) as connection:
                    connection.execute("PRAGMA query_only=ON")
                    connection.row_factory = sqlite3.Row
                    rust_rows = tuple(
                        connection.execute(
                            f"""
                            SELECT e.event_id, e.report_slot,
                                   t.target_key, t.channel, t.status
                            FROM notification_events AS e
                            LEFT JOIN notification_targets AS t ON t.event_id = e.event_id
                            WHERE e.lane = 'scheduled_report'
                              AND e.report_slot IN ({','.join('?' for _ in slot_keys)})
                            ORDER BY e.report_slot, t.target_key
                            """,
                            slot_keys,
                        ).fetchall()
                    )
                rust_status = "ready"
            except (OSError, sqlite3.Error) as exc:
                rust_status = f"query_failed:{type(exc).__name__}"
        elif ledger is not None:
            rust_status = "ledger_missing"
        rust_targets_by_slot: dict[str, list[sqlite3.Row]] = {}
        for row in rust_rows:
            rust_targets_by_slot.setdefault(str(row["report_slot"]), []).append(row)
        present = len(rust_targets_by_slot)
        delivered = sum(
            bool(human_targets)
            and all(str(row["status"]) == "delivered" for row in human_targets)
            for rows_for_slot in rust_targets_by_slot.values()
            if (
                human_targets := [
                    row
                    for row in rows_for_slot
                    if str(row["channel"] or "") in HUMAN_TRANSPORT_CHANNELS
                ]
            )
        )
        slot_reason_suffix = (
            f"; rust_ledger={rust_status}; "
            "Python status_snapshot is projection input only"
        )
    else:
        by_slot = projection_by_slot
        present = sum(slot in by_slot for slot in expected_slots)
        event_ids_by_slot = {
            slot: _report_event_id(by_slot.get(slot), session=session, slot=slot)
            for slot in expected_slots
        }
        fully_delivered = _fully_delivered_event_ids(
            outbox_path,
            event_ids_by_slot.values(),
        )
        delivered = sum(
            slot in by_slot and event_ids_by_slot[slot] in fully_delivered
            for slot in expected_slots
        )
        slot_reason_suffix = ""
    projected = sum(
        _has_spring_projection(projection_by_slot.get(slot)) for slot in expected_slots
    )
    summarized = sum(_has_state_window(projection_by_slot.get(slot)) for slot in expected_slots)
    delivery_checks = (
        _ratio_operational_check(
            "rth_report_slot_coverage",
            present,
            total,
            MIN_REPORT_SLOT_COVERAGE,
            "scheduled RTH report slots",
            reason_suffix=slot_reason_suffix,
        ),
        _ratio_operational_check(
            "rth_report_delivery_coverage",
            delivered,
            total,
            MIN_REPORT_SLOT_COVERAGE,
            "delivered scheduled RTH reports",
            reason_suffix=slot_reason_suffix,
        ),
    )
    if not spring_report_required:
        return delivery_checks
    return (
        *delivery_checks,
        _ratio_operational_check(
            "rth_report_spring_projection_coverage",
            projected,
            total,
            MIN_REPORT_PROJECTION_COVERAGE,
            "reports carrying Spring projection",
        ),
        _ratio_operational_check(
            "rth_report_state_window_coverage",
            summarized,
            total,
            MIN_REPORT_PROJECTION_COVERAGE,
            "reports carrying the prior 15-minute state window",
        ),
    )


def _trade_intent_producer_coverage_check(
    session: MarketSession,
    rows: Iterable[Mapping[str, object]],
    *,
    integrity: Mapping[str, object],
) -> OperationalCheck:
    expected_slots = {
        f"{session.trading_date.isoformat()}:rth:{index:03d}"
        for index in range(session.expected_five_minute_buckets)
    }
    observed_slots: set[str] = set()
    record_ids: set[str] = set()
    duplicate_record_ids: set[str] = set()
    malformed_rows = 0
    unexpected_record_types: set[str] = set()
    for row in rows:
        record_id = str(row.get("record_id") or "")
        if record_id in record_ids:
            duplicate_record_ids.add(record_id)
        elif record_id:
            record_ids.add(record_id)
        if (
            row.get("schema_version") != TRADE_INTENT_PRODUCER_LEDGER_SCHEMA
            or str(row.get("trading_date_et") or "") != session.trading_date.isoformat()
            or not record_id
        ):
            malformed_rows += 1
            continue
        record_type = str(row.get("record_type") or "")
        if record_type == "rth_5m_heartbeat":
            slot_id = str(row.get("slot_id") or "")
            if slot_id not in expected_slots:
                malformed_rows += 1
            else:
                observed_slots.add(slot_id)
        elif record_type == "trade_ready_delivery_expectation":
            if not str(row.get("semantic_key") or "") or not str(
                row.get("delivery_event_id") or ""
            ):
                malformed_rows += 1
        else:
            unexpected_record_types.add(record_type or "<missing>")

    missing_slots = sorted(expected_slots - observed_slots)
    parser_ok = (
        integrity.get("exists") is True
        and int(integrity.get("malformed_lines") or 0) == 0
        and int(integrity.get("valid_rows") or 0) > 0
    )
    passed = (
        parser_ok
        and malformed_rows == 0
        and not duplicate_record_ids
        and not unexpected_record_types
        and not missing_slots
    )
    return OperationalCheck(
        name="trade_intent_producer_coverage",
        measured={
            **dict(integrity),
            "expected_slots": len(expected_slots),
            "observed_slots": len(observed_slots),
            "missing_slots": missing_slots,
            "malformed_rows": malformed_rows,
            "duplicate_record_ids": sorted(duplicate_record_ids),
            "unexpected_record_types": sorted(unexpected_record_types),
        },
        threshold=(
            "strict JSONL, exact schema/date, and one durable producer "
            "heartbeat in every RTH five-minute slot"
        ),
        passed=passed,
        reason=(
            f"TradeIntent producer slots {len(observed_slots)}/"
            f"{len(expected_slots)}; malformed rows {malformed_rows}; "
            f"parse errors {int(integrity.get('malformed_lines') or 0)}"
        ),
    )


def _trade_intent_audit_integrity_check(
    rows: Iterable[Mapping[str, object]],
    *,
    integrity: Mapping[str, object],
) -> OperationalCheck:
    row_count = sum(1 for _row in rows)
    passed = (
        integrity.get("exists") is True
        and int(integrity.get("malformed_lines") or 0) == 0
        and row_count > 0
    )
    return OperationalCheck(
        name="trade_intent_audit_integrity",
        measured={**dict(integrity), "rows": row_count},
        threshold="present, non-empty, and every JSONL line parses to an object",
        passed=passed,
        reason=(
            f"TradeIntent audit rows {row_count}; parse errors "
            f"{int(integrity.get('malformed_lines') or 0)}"
        ),
    )


def _post_close_review_check(payload: Mapping[str, object]) -> OperationalCheck:
    verdict = payload.get("verdict")
    status = verdict.get("status") if isinstance(verdict, Mapping) else None
    return OperationalCheck(
        name="post_close_market_data_completeness",
        measured=status,
        threshold="complete",
        passed=status == "complete",
        reason=(
            "post-close market-data completeness is complete"
            if status == "complete"
            else f"post-close market-data completeness is {status or 'missing'}"
        ),
    )


def _outbox_check(path: str | Path | None) -> OperationalCheck:
    human_channels = tuple(sorted(HUMAN_TRANSPORT_CHANNELS))
    internal_sources = tuple(sorted(INTERNAL_NOTIFICATION_SOURCES))
    channel_filter = ",".join("?" for _ in human_channels)
    source_filter = ",".join("?" for _ in internal_sources)
    human_scope = (
        f"channel IN ({channel_filter}) "
        f"AND source NOT IN ({source_filter}) "
        "AND channel != '__cancellation__'"
    )
    if path is None or not str(path):
        return OperationalCheck(
            name="notification_outbox_integrity",
            measured="not_configured",
            threshold="quick_check=ok,journal_mode in {delete,wal}, human transport clear",
            passed=False,
            reason="notification delivery outbox path is not configured",
        )
    database = Path(path)
    if not database.exists():
        return OperationalCheck(
            name="notification_outbox_integrity",
            measured="missing",
            threshold="quick_check=ok,journal_mode in {delete,wal}, human transport clear",
            passed=False,
            reason=f"notification delivery outbox is missing: {database}",
        )
    try:
        with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
            # The unified spx.sqlite also stores the multi-gigabyte market
            # ledger.  A database-wide quick_check made this daily notification
            # acceptance exceed its 15 minute service budget.  Integrity of the
            # notification owner is proven by a table-scoped check here; the
            # targeted queries below additionally exercise its live indexes and
            # rows without rescanning unrelated market tables.
            quick_check = str(
                connection.execute(
                    "PRAGMA quick_check('notification_events')"
                ).fetchone()[0]
            ).lower()
            journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower()
            scope_params = (*human_channels, *internal_sources)
            dead_letters = int(
                connection.execute(
                    f"""
                    SELECT COUNT(*) FROM notification_events
                    WHERE status = 'failed'
                      AND {human_scope}
                    """,
                    scope_params,
                ).fetchone()[0]
            )
            pending_targets = int(
                connection.execute(
                    f"""
                    SELECT COUNT(*) FROM notification_events
                    WHERE status = 'pending'
                      AND {human_scope}
                    """,
                    scope_params,
                ).fetchone()[0]
            )
            claimed_targets = int(
                connection.execute(
                    f"""
                    SELECT COUNT(*) FROM notification_events
                    WHERE status = 'processing'
                      AND {human_scope}
                    """,
                    scope_params,
                ).fetchone()[0]
            )
            uncertain_targets = int(
                connection.execute(
                    f"""
                    SELECT COUNT(*) FROM notification_events
                    WHERE status = 'uncertain'
                      AND {human_scope}
                    """,
                    scope_params,
                ).fetchone()[0]
            )
            unknown_targets = int(
                connection.execute(
                    f"""
                    SELECT COUNT(*) FROM notification_events
                    WHERE {human_scope}
                      AND status NOT IN (
                        'pending', 'processing', 'delivered', 'failed', 'uncertain', 'cancelled'
                      )
                    """,
                    scope_params,
                ).fetchone()[0]
            )
            internal_backlog = int(
                connection.execute(
                    f"""
                    SELECT COUNT(*) FROM notification_events
                    WHERE status IN ('failed', 'uncertain', 'pending', 'processing')
                      AND NOT ({human_scope})
                    """,
                    scope_params,
                ).fetchone()[0]
            )
            terminal_receipt_schema_present = (
                connection.execute(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE type = 'table' "
                    "AND name = 'notification_attempts'"
                ).fetchone()
                is not None
            )
            terminal_receipts_pending = (
                int(
                    connection.execute(
                        f"""
                        SELECT COUNT(*)
                        FROM notification_attempts AS a
                        JOIN notification_events AS e ON e.id = a.event_id
                        WHERE a.finished_at IS NULL
                          AND e.channel IN ({channel_filter})
                          AND e.source NOT IN ({source_filter})
                          AND e.channel != '__cancellation__'
                        """,
                        scope_params,
                    ).fetchone()[0]
                )
                if terminal_receipt_schema_present
                else 0
            )
    except (OSError, sqlite3.Error) as exc:
        return OperationalCheck(
            name="notification_outbox_integrity",
            measured="unreadable",
            threshold="quick_check=ok,journal_mode in {delete,wal}, human transport clear",
            passed=False,
            reason=f"notification delivery outbox check failed: {type(exc).__name__}",
        )
    passed = (
        quick_check == "ok"
        and journal_mode in ALLOWED_OUTBOX_JOURNAL_MODES
        and pending_targets == 0
        and claimed_targets == 0
        and uncertain_targets == 0
        and dead_letters == 0
        and unknown_targets == 0
        and terminal_receipt_schema_present
        and terminal_receipts_pending == 0
    )
    return OperationalCheck(
        name="notification_outbox_integrity",
        measured={
            "quick_check": quick_check,
            "journal_mode": journal_mode,
            "human_transport_channels": human_channels,
            "pending_targets": pending_targets,
            "claimed_targets": claimed_targets,
            "uncertain_targets": uncertain_targets,
            "dead_letter_targets": dead_letters,
            "unknown_targets": unknown_targets,
            "internal_backlog_targets": internal_backlog,
            "terminal_receipt_schema_present": terminal_receipt_schema_present,
            "terminal_receipts_pending": terminal_receipts_pending,
        },
        threshold={
            "quick_check": "ok",
            "journal_mode": sorted(ALLOWED_OUTBOX_JOURNAL_MODES),
            "pending_targets": 0,
            "claimed_targets": 0,
            "uncertain_targets": 0,
            "dead_letter_targets": 0,
            "unknown_targets": 0,
            "terminal_receipt_schema_present": True,
            "terminal_receipts_pending": 0,
            "scope": "human transport channels only",
        },
        passed=passed,
        reason=(
            f"outbox quick_check={quick_check}, journal_mode={journal_mode}, "
            f"human pending_targets={pending_targets}, claimed_targets={claimed_targets}, "
            f"uncertain_targets={uncertain_targets}, "
            f"dead_letter_targets={dead_letters}, unknown_targets={unknown_targets}, "
            f"internal_backlog_targets={internal_backlog} (diagnostic only), "
            f"terminal_receipt_schema_present={terminal_receipt_schema_present}, "
            f"terminal_receipts_pending={terminal_receipts_pending}"
        ),
    )


def _level_decision_signal_check(
    report: Mapping[str, object],
) -> OperationalCheck:
    formal_signal = report.get("formal_signal") is True
    gates_passed = report.get("acceptance_gates_passed") is True
    passed = not formal_signal or gates_passed
    return OperationalCheck(
        name="level_decision_formal_signal_evidence",
        measured={
            "formal_signal": formal_signal,
            "acceptance_gates_passed": gates_passed,
        },
        threshold="formal_signal=false or acceptance_gates_passed=true",
        passed=passed,
        reason=(
            "level-decision remains shadow-only"
            if not formal_signal
            else "formal signal is backed by passed statistical gates"
            if gates_passed
            else "formal signal is enabled before statistical gates pass"
        ),
    )


def _ratio_operational_check(
    name: str,
    numerator: int,
    denominator: int,
    threshold: float,
    label: str,
    *,
    reason_suffix: str = "",
) -> OperationalCheck:
    ratio = numerator / denominator if denominator else 0.0
    return OperationalCheck(
        name=name,
        measured=round(ratio, 6),
        threshold=threshold,
        passed=denominator > 0 and ratio >= threshold,
        reason=(
            f"{label} {numerator}/{denominator} ({ratio:.1%}); "
            f"required >= {threshold:.0%}{reason_suffix}"
        ),
    )


def _expected_minute_count(session: MarketSession) -> int:
    return int((session.close_at - session.open_at).total_seconds() // 60)


def _expected_report_slots(session: MarketSession) -> tuple[datetime, ...]:
    return tuple(slot for slot in rth_report_schedule_for_session(session) if slot.minute % 30 == 0)


def _report_slot(observed: datetime | None, session: MarketSession) -> datetime | None:
    if observed is None:
        return None
    slot = rth_report_slot_for_session(observed, session=session)
    return slot.slot_at if slot is not None else None


def _has_spring_projection(row: Mapping[str, object] | None) -> bool:
    if not isinstance(row, Mapping):
        return False
    diagnostic = row.get("spring_gamma_v3_projection_diagnostic")
    return (
        isinstance(row.get("spring_gamma_v3_shadow"), Mapping)
        and isinstance(diagnostic, Mapping)
        and diagnostic.get("status") == "attached"
    )


def _has_state_window(row: Mapping[str, object] | None) -> bool:
    if not isinstance(row, Mapping):
        return False
    window = row.get("spring_gamma_v3_state_window")
    if not isinstance(window, Mapping):
        return False
    states = window.get("states")
    counts = window.get("counts")
    slot_counts = window.get("five_minute_slot_counts")
    sample_count = _strict_nonnegative_int(window.get("sample_count"))
    slot_count = _strict_nonnegative_int(window.get("five_minute_slot_count"))
    if (
        window.get("schema_version") != "spring_gamma_v3_state_window.v1"
        or window.get("session") != "rth"
        or str(window.get("session_id") or "") != str(row.get("trading_date") or "")
        or str(window.get("expiry") or "") != str(row.get("expiry") or "")
        or window.get("action_authority") != "none"
        or window.get("actionable") is not False
        or not isinstance(states, list)
        or not states
        or len(states) != len(set(states))
        or any(state not in KNOWN_MARKET_STATES for state in states)
        or not isinstance(counts, Mapping)
        or set(counts) != set(states)
        or not isinstance(slot_counts, Mapping)
        or set(slot_counts) != set(states)
        or sample_count is None
        or sample_count <= 0
        or slot_count is None
        or slot_count <= 0
        or slot_count > sample_count
        or window.get("latest_state") not in states
    ):
        return False
    state_counts = [_strict_nonnegative_int(counts.get(state)) for state in states]
    state_slot_counts = [_strict_nonnegative_int(slot_counts.get(state)) for state in states]
    if (
        any(value is None or value <= 0 for value in state_counts)
        or sum(int(value) for value in state_counts if value is not None) != sample_count
        or any(value is None or value <= 0 or value > slot_count for value in state_slot_counts)
    ):
        return False
    window_start = _parse_at(window.get("window_start"))
    window_end = _parse_at(window.get("window_end"))
    latest_at = _parse_at(window.get("latest_state_as_of"))
    report_at = _parse_at(row.get("generated_at")) or _parse_at(row.get("occurred_at"))
    tolerance = _finite_nonnegative(window.get("future_tolerance_seconds"))
    return bool(
        window_start is not None
        and window_end is not None
        and latest_at is not None
        and report_at is not None
        and tolerance is not None
        and window.get("window_minutes") == 15
        and window_end - window_start == timedelta(minutes=15)
        and abs((window_end - report_at).total_seconds()) <= RTH_REPORT_START_GRACE_SECONDS
        and window_start < latest_at
        and latest_at <= window_end + timedelta(seconds=tolerance)
    )


def _has_valid_rth_market_state(row: Mapping[str, object]) -> bool:
    state = row.get("rth_market_state")
    if not isinstance(state, Mapping):
        return False
    token = str(state.get("state") or "")
    availability = state.get("input_availability")
    if not isinstance(availability, Mapping):
        return False
    required = _strict_nonnegative_int(availability.get("required_count"))
    available = _strict_nonnegative_int(availability.get("available_count"))
    complete = availability.get("complete")
    direction = state.get("D")
    if direction is not None and (
        isinstance(direction, bool)
        or not isinstance(direction, (int, float))
        or not math.isfinite(float(direction))
        or not -10.0 <= float(direction) <= 10.0
    ):
        return False
    fields = availability.get("fields")
    if isinstance(fields, Mapping) and (
        len(fields) != 8
        or any(
            not isinstance(value, Mapping) or not isinstance(value.get("available"), bool)
            for value in fields.values()
        )
    ):
        return False
    classification_tier = str(state.get("classification_tier") or "")
    status = str(state.get("status") or "")
    status_valid = (
        (classification_tier == "complete" and token != "UNCERTAIN" and status == "ready")
        or (
            classification_tier == "directional_provisional"
            and token in {"TREND_UP", "TREND_DOWN"}
            and status == "provisional"
        )
        or (token == "UNCERTAIN" and status == "uncertain")
        or (
            classification_tier == "unavailable"
            and token == "UNCERTAIN"
            and status == "unavailable"
        )
    )
    return bool(
        state.get("schema_version") == MARKET_STATE_SCHEMA
        and state.get("rule_version") == MARKET_STATE_RULE
        and token in KNOWN_MARKET_STATES
        and state.get("market_state", token) == token
        and state.get("action_authority") == "none"
        and state.get("actionable") is False
        and required == 8
        and available is not None
        and 0 <= available <= 8
        and isinstance(complete, bool)
        and complete is (available == 8)
        and status_valid
    )


def _strict_nonnegative_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def _finite_nonnegative(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number >= 0 else None


def _inside_session(value: object, session: MarketSession, *, include_close: bool) -> bool:
    observed = _parse_at(value)
    if observed is None:
        return False
    local = observed.astimezone(session.open_at.tzinfo)
    if include_close:
        return session.open_at <= local <= session.close_at
    return session.open_at <= local < session.close_at


def _parse_at(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("RTH daily acceptance timestamps must be timezone-aware")
    return value.astimezone(timezone.utc)


def _resolve_trading_date(value: str, *, now: datetime) -> date:
    if value.lower() == "auto":
        return DEFAULT_MARKET_CALENDAR.completed_review_date(now)
    return date.fromisoformat(value)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build daily RTH end-to-end acceptance")
    parser.add_argument("--date", default="auto", help="NY trading date, YYYY-MM-DD, or auto")
    parser.add_argument("--strict", action="store_true", help="Exit non-zero on degraded verdict")
    parser.add_argument("--json", action="store_true", help="Print the complete report")
    parser.add_argument(
        "--no-notify", action="store_true", help="Do not queue a degraded ops alert"
    )
    args = parser.parse_args(argv)

    now = datetime.now(tz=timezone.utc)
    storage = StorageSettings.from_env()
    notification = NotificationSettings.from_env()
    app_settings = load_app_settings()
    spring_settings = app_settings.spring_gamma_v3
    spring_required = bool(spring_settings.enabled)
    spring_report_required = bool(spring_settings.report_enabled)
    report = build_rth_daily_acceptance(
        storage.data_root,
        trading_date=_resolve_trading_date(args.date, now=now),
        level_policy=app_settings.level_decision,
        outbox_path=notification.notification_database_path,
        receipt_path=None,
        rust_delivery_ledger_path=notification.rust_delivery_ledger_path or None,
        rust_report_owner=rust_report_owner_enabled(),
        now=now,
        spring_required=spring_required,
        spring_report_required=spring_report_required,
    )
    session = DEFAULT_MARKET_CALENDAR.session(date.fromisoformat(str(report["trading_date"])))
    if session is None:  # build_rth_daily_acceptance already validated this date.
        raise RuntimeError("resolved RTH session disappeared")
    report["notification"] = (
        {"status": "disabled", "accepted": False}
        if args.no_notify
        else enqueue_degraded_acceptance(
            report,
            settings=notification,
            occurred_at=session.close_at,
        )
    )
    historical, latest = write_rth_daily_acceptance(storage.data_root, report)
    output = {**report, "paths": {"historical": str(historical), "latest": str(latest)}}
    print(
        json.dumps(
            output
            if args.json
            else {
                "status": report["status"],
                "trading_date": report["trading_date"],
                "failed_checks": report["failed_checks"],
                "latest": str(latest),
            },
            sort_keys=True,
        )
    )
    return 2 if args.strict and report["status"] != "complete" else 0


if __name__ == "__main__":
    raise SystemExit(main())
