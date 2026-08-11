"""RTH notification acceptance against unified event/attempt rows."""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from spx_spark.application.order_map.rth_daily_acceptance import (
    _outbox_check,
    _report_checks,
)
from spx_spark.application.order_map.rth_daily_acceptance_support import (
    explicitly_terminal_event_ids,
    fully_delivered_event_ids,
    receipt_store_check,
    rust_fully_delivered_event_ids,
    rust_scheduled_report_events,
    strategy_decision_trade_ready_delivery_check,
    timely_delivered_event_ids,
)
from spx_spark.infrastructure.notifications import (
    NotificationDraft,
    NotificationStatus,
    begin_attempt,
    cancel,
    create_engine,
    enqueue,
    mark_transport_started,
    metadata,
    settle,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR


NOW = datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc)


def _store(tmp_path: Path):
    engine = create_engine(tmp_path)
    metadata.create_all(engine)
    return engine


def _enqueue(engine, event_id: str, *, expires_at: datetime | None = None) -> tuple[int, ...]:
    return enqueue(
        engine,
        NotificationDraft(
            logical_event_id=event_id,
            source="test",
            kind="trade_intent",
            lane="trade_ready",
            payload={"body": "immutable"},
            channels=("bark", "feishu"),
            expires_at=expires_at or NOW + timedelta(minutes=5),
        ),
        now=NOW,
    ).event_ids


def _deliver(engine, event_ids: tuple[int, ...], *, at: datetime) -> None:
    for event_id in event_ids:
        attempt = begin_attempt(engine, event_id, now=at)
        assert attempt is not None
        mark_transport_started(engine, attempt.attempt_id)
        settle(
            engine,
            attempt.attempt_id,
            status=NotificationStatus.DELIVERED,
            outcome="delivered",
            ok=True,
            now=at,
        )


def test_all_targets_require_real_success_attempts(tmp_path: Path) -> None:
    engine = _store(tmp_path)
    ids = _enqueue(engine, "ready")
    _deliver(engine, ids, at=NOW + timedelta(seconds=1))

    assert fully_delivered_event_ids(tmp_path / "spx.sqlite", ("ready",)) == {"ready"}
    accepted, diagnostics = timely_delivered_event_ids(
        tmp_path / "spx.sqlite", ("ready",)
    )
    assert accepted == {"ready"}
    assert diagnostics["events"]["ready"]["first_delivery_seconds"] == 1.0


def test_first_delivery_after_five_seconds_is_rejected(tmp_path: Path) -> None:
    engine = _store(tmp_path)
    ids = _enqueue(engine, "late")
    _deliver(engine, ids, at=NOW + timedelta(seconds=6))

    accepted, diagnostics = timely_delivered_event_ids(tmp_path / "spx.sqlite", ("late",))

    assert accepted == frozenset()
    assert diagnostics["events"]["late"]["reasons"] == ["first_delivery_slo_breached"]


def test_cancellation_fence_is_an_explicit_terminal_result(tmp_path: Path) -> None:
    engine = _store(tmp_path)
    _enqueue(engine, "cancelled")
    assert cancel(engine, "cancelled", reason="source_invalidated", now=NOW) == 2

    accepted, diagnostics = explicitly_terminal_event_ids(
        tmp_path / "spx.sqlite", ("cancelled",)
    )

    assert accepted == {"cancelled"}
    assert diagnostics["events"]["cancelled"]["reasons"] == []


def test_attempt_store_integrity_uses_spx_sqlite_only(tmp_path: Path) -> None:
    engine = _store(tmp_path)
    engine.dispose()

    check = receipt_store_check(tmp_path / "spx.sqlite", tmp_path / "legacy.sqlite")

    assert check.passed is True
    assert check.measured["schema_present"] is True


def test_rust_scheduled_report_slot_and_delivery_coverage(tmp_path: Path) -> None:
    ledger = tmp_path / "operations.sqlite"
    et = ZoneInfo("America/New_York")
    slot = datetime(2026, 8, 10, 9, 30, tzinfo=et)
    missing = datetime(2026, 8, 10, 12, 0, tzinfo=et)
    with sqlite3.connect(ledger) as connection:
        connection.executescript(
            """
            CREATE TABLE notification_events (
              event_id TEXT PRIMARY KEY,
              report_slot TEXT,
              lane TEXT,
              created_at_us INTEGER
            );
            CREATE TABLE notification_targets (
              target_id TEXT PRIMARY KEY,
              event_id TEXT,
              target_key TEXT,
              channel TEXT,
              status TEXT
            );
            """
        )
        event_id = "scheduled-report:slot-0930"
        connection.execute(
            "INSERT INTO notification_events VALUES (?, ?, 'scheduled_report', 1)",
            (event_id, slot.isoformat()),
        )
        for channel, key in (("bark", "bark-primary"), ("feishu", "feishu-primary")):
            connection.execute(
                "INSERT INTO notification_targets VALUES (?, ?, ?, ?, 'delivered')",
                (f"{event_id}:{key}", event_id, key, channel),
            )

    by_slot, diagnostics = rust_scheduled_report_events(ledger, (slot, missing))
    assert diagnostics["status"] == "ready"
    assert by_slot == {slot: event_id}
    assert rust_fully_delivered_event_ids(ledger, by_slot.values()) == {event_id}


def test_strategy_decision_trade_ready_uses_unique_opportunities(tmp_path: Path) -> None:
    engine = _store(tmp_path)
    database = tmp_path / "spx.sqlite"
    opportunity = "strategy-opportunity:abc123"
    event_id = f"{opportunity}:ready"
    with sqlite3.connect(database) as connection:
        connection.execute(
            """
            CREATE TABLE decisions (
              decision_id TEXT PRIMARY KEY,
              session_date TEXT,
              strategy_name TEXT,
              decision_at TEXT,
              status TEXT,
              attributes_json TEXT
            )
            """
        )
        attrs = {
            "action_authority": "manual",
            "candidate": {"opportunity_id": opportunity},
        }
        for index in range(2):
            connection.execute(
                """
                INSERT INTO decisions VALUES (?, '2026-08-10', 'strategy_signal_engine_v2',
                  ?, 'selected', ?)
                """,
                (
                    f"strategy:{index}",
                    f"2026-08-10T17:0{index}:00+00:00",
                    json.dumps(attrs),
                ),
            )
    ids = enqueue(
        engine,
        NotificationDraft(
            logical_event_id=event_id,
            source="strategy_decision",
            kind="trade_intent",
            lane="trade_ready",
            payload={"body": "candidate"},
            channels=("bark", "feishu"),
            expires_at=NOW + timedelta(minutes=5),
        ),
        now=NOW,
    ).event_ids
    _deliver(engine, ids, at=NOW + timedelta(seconds=2))
    engine.dispose()

    check = strategy_decision_trade_ready_delivery_check(
        database,
        trading_date="2026-08-10",
        outbox_path=database,
        receipt_path=None,
    )
    assert check.passed is True
    assert check.measured["expected_opportunities"] == 1
    assert check.measured["timely_delivered_events"] == 1


def test_outbox_integrity_scopes_human_transport_and_allows_wal(tmp_path: Path) -> None:
    engine = _store(tmp_path)
    database = tmp_path / "spx.sqlite"
    ids = enqueue(
        engine,
        NotificationDraft(
            logical_event_id="human-ok",
            source="strategy_decision",
            kind="trade_intent",
            lane="trade_ready",
            payload={"body": "ok"},
            channels=("bark", "feishu"),
            expires_at=NOW + timedelta(minutes=5),
        ),
        now=NOW,
    ).event_ids
    _deliver(engine, ids, at=NOW + timedelta(seconds=1))
    engine.dispose()
    with sqlite3.connect(database) as connection:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute(
            """
            INSERT INTO notification_events (
              idempotency_key, logical_event_id, source, kind, lane, channel,
              payload_json, payload_sha256, status, created_at, updated_at
            ) VALUES (
              'alert:1:bark', 'alert:1', 'alert_pipeline', 'alert_candidate',
              'alert_candidate', 'bark', '{}', 'x', 'failed', ?, ?
            )
            """,
            (NOW.isoformat(sep=" "), NOW.isoformat(sep=" ")),
        )

    check = _outbox_check(database)
    assert check.passed is True
    assert check.measured["journal_mode"] == "wal"
    assert check.measured["dead_letter_targets"] == 0
    assert check.measured["internal_backlog_targets"] >= 1


def test_rust_owner_report_checks_use_ledger_not_python_status(tmp_path: Path) -> None:
    session = DEFAULT_MARKET_CALENDAR.session(date(2026, 8, 10))
    assert session is not None
    et = ZoneInfo("America/New_York")
    slot = datetime(2026, 8, 10, 9, 30, tzinfo=et)
    ledger = tmp_path / "operations.sqlite"
    with sqlite3.connect(ledger) as connection:
        connection.executescript(
            """
            CREATE TABLE notification_events (
              event_id TEXT PRIMARY KEY,
              report_slot TEXT,
              lane TEXT,
              created_at_us INTEGER
            );
            CREATE TABLE notification_targets (
              target_id TEXT PRIMARY KEY,
              event_id TEXT,
              target_key TEXT,
              channel TEXT,
              status TEXT
            );
            """
        )
        event_id = "scheduled-report:0930"
        connection.execute(
            "INSERT INTO notification_events VALUES (?, ?, 'scheduled_report', 1)",
            (event_id, slot.isoformat()),
        )
        connection.execute(
            "INSERT INTO notification_targets VALUES (?, ?, 'bark-primary', 'bark', 'delivered')",
            (f"{event_id}:bark", event_id),
        )
        connection.execute(
            "INSERT INTO notification_targets VALUES (?, ?, 'feishu-primary', 'feishu', 'delivered')",
            (f"{event_id}:feishu", event_id),
        )
    snapshot = {
        "report_kind": "status_snapshot",
        "occurred_at": slot.isoformat(),
        "generated_at": slot.isoformat(),
        "trading_date": "2026-08-10",
        "expiry": "2026-08-10",
        "spring_gamma_v3_shadow": {"state": "LOW_VOL_PIN"},
        "spring_gamma_v3_projection_diagnostic": {"status": "attached"},
    }
    checks = _report_checks(
        session,
        (snapshot,),
        outbox_path=None,
        rust_delivery_ledger_path=ledger,
        rust_report_owner=True,
        spring_report_required=False,
    )
    by_name = {check.name: check for check in checks}
    assert by_name["rth_report_slot_coverage"].measured == round(1 / 26, 6)
    assert by_name["rth_report_delivery_coverage"].measured == round(1 / 26, 6)
    assert "rth_report_spring_projection_coverage" not in by_name
