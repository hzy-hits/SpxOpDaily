from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone

from spx_spark.application.order_map.rth_daily_acceptance import (
    build_rth_daily_acceptance as _build_rth_daily_acceptance,
    enqueue_degraded_acceptance,
    write_rth_daily_acceptance,
)
from spx_spark.application.market_features.trade_intent_runtime import (
    _trade_ready_delivery_event_id,
)
from spx_spark.config import NotificationSettings
from spx_spark.notifier.dispatcher import EnqueueResult, _sync_terminal_receipts
from spx_spark.notifier.receipts import NotificationEnvelope
from spx_spark.notifier.receipts import (
    notification_event_id,
    prepare_delivery_receipt_store,
)
from spx_spark.market_calendar import ET, MarketCalendar
from spx_spark.notifier.delivery_outbox import NotificationDeliveryOutbox
from spx_spark.settings.level_decision import LevelDecisionPolicy


TRADING_DATE = date(2026, 7, 6)


def _receipt_path(tmp_path):
    return tmp_path / "notification-receipts.sqlite"


def build_rth_daily_acceptance(data_root, **kwargs):
    kwargs.setdefault("receipt_path", _receipt_path(data_root))
    return _build_rth_daily_acceptance(data_root, **kwargs)


def _write_jsonl(path, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def _write_json(path, payload) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _outbox(tmp_path):
    assert prepare_delivery_receipt_store(_receipt_path(tmp_path))
    return NotificationDeliveryOutbox(
        tmp_path / "notification.sqlite",
        max_attempts=3,
        retry_schedule_seconds=(1.0,),
        dead_letter_after_seconds=60.0,
        claim_stale_after_seconds=10.0,
    )


def _sync_receipts(tmp_path, outbox, *, now: datetime) -> None:
    settings = replace(
        NotificationSettings.from_env(),
        delivery_receipt_path=str(_receipt_path(tmp_path)),
    )
    result = _sync_terminal_receipts(settings, outbox, now=now)
    assert result.pending == 0
    assert result.inspection.ok is True


def _enqueue_report_targets(
    outbox,
    session,
    *,
    targets: tuple[str, ...] = ("bark", "feishu"),
) -> None:
    slot = session.open_at
    while slot < session.close_at:
        slot_key = f"{TRADING_DATE.isoformat()}:{slot.strftime('%H:%M')}"
        event_id = notification_event_id(
            "status",
            source="order_map_status",
            occurred_at=slot,
            identity=f"rth_slot:{slot_key}",
        )
        outbox.enqueue(
            NotificationEnvelope(
                event_id=event_id,
                source="order_map_status",
                kind="status",
                lane="scheduled_report",
                occurred_at=slot,
            ),
            title="status",
            text=slot_key,
            feishu_text=slot_key if "feishu" in targets else None,
            friend=False,
            targets=targets,
            now=slot,
        )
        slot += timedelta(minutes=15)


def _deliver_all_targets(outbox) -> None:
    with sqlite3.connect(outbox.path) as connection:
        connection.execute(
            "UPDATE notification_delivery_targets SET status='delivered', delivered_at=updated_at"
        )


def _full_day_rows(session, *, report_lag_seconds: int = 8):
    spring = []
    cursor = session.open_at
    while cursor < session.close_at:
        spring.append(
            {
                "as_of": cursor.astimezone(timezone.utc).isoformat(),
                "model_version": "spring_gamma_v3_rth_state_shadow.v2",
                "session": "rth",
                "rth_market_state": {
                    "schema_version": "market_state_5m.v1",
                    "rule_version": "market_state_5m_eight_variable_rules.v2",
                    "state": "UNCERTAIN",
                    "market_state": "UNCERTAIN",
                    "status": "uncertain",
                    "D": 0,
                    "input_availability": {
                        "required_count": 8,
                        "available_count": 8,
                        "complete": True,
                    },
                    "action_authority": "none",
                    "actionable": False,
                },
                "option_overlay": {"status": "ready"},
            }
        )
        cursor += timedelta(minutes=1)

    reports = []
    cursor = session.open_at
    while cursor < session.close_at:
        observed = cursor + timedelta(seconds=report_lag_seconds)
        reports.append(
            {
                "generated_at": observed.astimezone(timezone.utc).isoformat(),
                "report_kind": "status",
                "trading_date": TRADING_DATE.isoformat(),
                "expiry": TRADING_DATE.strftime("%Y%m%d"),
                "delivered_ok": True,
                "spring_gamma_v3_shadow": {"as_of": observed.isoformat()},
                "spring_gamma_v3_projection_diagnostic": {"status": "attached"},
                "spring_gamma_v3_state_window": {
                    "schema_version": "spring_gamma_v3_state_window.v1",
                    "session_id": TRADING_DATE.isoformat(),
                    "session": "rth",
                    "expiry": TRADING_DATE.strftime("%Y%m%d"),
                    "window_start": (cursor - timedelta(minutes=15)).isoformat(),
                    "window_end": cursor.isoformat(),
                    "window_minutes": 15,
                    "states": ["UNCERTAIN"],
                    "counts": {"UNCERTAIN": 1},
                    "sample_count": 1,
                    "five_minute_slot_count": 1,
                    "five_minute_slot_counts": {"UNCERTAIN": 1},
                    "latest_state": "UNCERTAIN",
                    "latest_state_as_of": cursor.isoformat(),
                    "future_tolerance_seconds": 5.0,
                    "action_authority": "none",
                    "actionable": False,
                },
            }
        )
        cursor += timedelta(minutes=15)
    return spring, reports


def _producer_heartbeat_rows(session):
    rows = []
    for index in range(session.expected_five_minute_buckets):
        slot_start = session.open_at + timedelta(minutes=5 * index)
        slot_id = f"{TRADING_DATE.isoformat()}:rth:{index:03d}"
        rows.append(
            {
                "schema_version": "trade_intent_producer_ledger.v1",
                "record_type": "rth_5m_heartbeat",
                "record_id": f"heartbeat:{slot_id}",
                "observed_at": slot_start.isoformat(),
                "trading_date_et": TRADING_DATE.isoformat(),
                "slot_id": slot_id,
                "slot_index": index,
                "slot_start": slot_start.isoformat(),
                "slot_end": (slot_start + timedelta(minutes=5)).isoformat(),
                "trade_intent_status": "observing",
                "intent_event_id": None,
            }
        )
    return rows


def _seed_complete_day(tmp_path, *, calendar=None, report_lag_seconds: int = 8):
    calendar = calendar or MarketCalendar()
    session = calendar.session(TRADING_DATE)
    assert session is not None
    spring, reports = _full_day_rows(session, report_lag_seconds=report_lag_seconds)
    _write_jsonl(
        tmp_path
        / "features"
        / "spring_gamma_v3"
        / f"date={TRADING_DATE.isoformat()}"
        / "predictions.jsonl",
        spring,
    )
    _write_jsonl(
        tmp_path
        / "audit"
        / "order_map_pricing"
        / f"date={TRADING_DATE.isoformat()}"
        / "reports.jsonl",
        reports,
    )
    _write_json(
        tmp_path
        / "reports"
        / "spx_options_review"
        / f"date={TRADING_DATE.isoformat()}"
        / "review.json",
        {"verdict": {"status": "complete"}},
    )
    _write_jsonl(
        tmp_path
        / "features"
        / "trade_intent_producer_ledger"
        / f"date={TRADING_DATE.isoformat()}"
        / "events.jsonl",
        _producer_heartbeat_rows(session),
    )
    _write_jsonl(
        tmp_path
        / "features"
        / "trade_intents"
        / f"date={TRADING_DATE.isoformat()}"
        / "events.jsonl",
        [{"status": "observing", "event_id": "daily-heartbeat"}],
    )
    return calendar, session


def test_complete_day_uses_one_et_clock_contract_and_writes_both_projections(tmp_path) -> None:
    calendar, session = _seed_complete_day(tmp_path)
    outbox = _outbox(tmp_path)
    _enqueue_report_targets(outbox, session)
    _deliver_all_targets(outbox)

    report = build_rth_daily_acceptance(
        tmp_path,
        trading_date=TRADING_DATE,
        level_policy=LevelDecisionPolicy(),
        outbox_path=outbox.path,
        now=datetime(2026, 7, 6, 22, 0, tzinfo=timezone.utc),
        calendar=calendar,
    )
    historical, latest = write_rth_daily_acceptance(tmp_path, report)

    assert report["status"] == "complete"
    assert report["failed_checks"] == []
    assert report["session_clock"]["timezone"] == str(ET)
    assert report["session_clock"]["expected_spring_minutes"] == 390
    assert report["session_clock"]["expected_report_slots"] == 26
    assert historical.exists()
    assert latest.exists()
    assert (tmp_path / "latest" / "level_decision_acceptance.json").exists()


def test_disabled_spring_is_not_an_operational_acceptance_dependency(tmp_path) -> None:
    calendar, session = _seed_complete_day(tmp_path)
    outbox = _outbox(tmp_path)
    _enqueue_report_targets(outbox, session)
    _deliver_all_targets(outbox)

    report = build_rth_daily_acceptance(
        tmp_path,
        trading_date=TRADING_DATE,
        level_policy=LevelDecisionPolicy(),
        outbox_path=outbox.path,
        now=datetime(2026, 7, 6, 22, 0, tzinfo=timezone.utc),
        calendar=calendar,
        spring_required=False,
    )

    assert report["status"] == "complete"
    assert report["spring_required"] is False
    assert report["session_clock"]["expected_spring_minutes"] == 0
    assert not any(
        name.startswith("spring_") or "spring_" in name for name in report["failed_checks"]
    )
    assert {row["name"] for row in report["checks"]}.isdisjoint(
        {
            "spring_rth_minute_coverage",
            "spring_option_overlay_ready_ratio",
            "rth_report_spring_projection_coverage",
            "rth_report_state_window_coverage",
        }
    )


def test_trade_ready_intent_requires_fully_delivered_outbox_event(tmp_path) -> None:
    calendar, session = _seed_complete_day(tmp_path)
    outbox = _outbox(tmp_path)
    _enqueue_report_targets(outbox, session)
    _deliver_all_targets(outbox)
    intent = {
        "status": "trade_ready",
        "intent_id": "intent:daily-acceptance",
        "event_id": "level:daily-acceptance",
        "semantic_key": "2026-07-06|breakout|up|7500|SPXW-7500C",
    }
    _write_jsonl(
        tmp_path
        / "features"
        / "trade_intents"
        / f"date={TRADING_DATE.isoformat()}"
        / "events.jsonl",
        [intent, intent],
    )
    delivery_event_id = _trade_ready_delivery_event_id(intent)
    producer_path = (
        tmp_path
        / "features"
        / "trade_intent_producer_ledger"
        / f"date={TRADING_DATE.isoformat()}"
        / "events.jsonl"
    )
    producer_rows = [
        json.loads(line) for line in producer_path.read_text(encoding="utf-8").splitlines()
    ]
    producer_rows.append(
        {
            "schema_version": "trade_intent_producer_ledger.v1",
            "record_type": "trade_ready_delivery_expectation",
            "record_id": f"expectation:{delivery_event_id}",
            "observed_at": session.open_at.isoformat(),
            "trading_date_et": TRADING_DATE.isoformat(),
            "slot_id": f"{TRADING_DATE.isoformat()}:rth:000",
            "semantic_key": intent["semantic_key"],
            "delivery_event_id": delivery_event_id,
            "intent_id": intent["intent_id"],
            "intent_event_id": intent["event_id"],
        }
    )
    _write_jsonl(producer_path, producer_rows)

    missing = build_rth_daily_acceptance(
        tmp_path,
        trading_date=TRADING_DATE,
        level_policy=LevelDecisionPolicy(),
        outbox_path=outbox.path,
        now=datetime(2026, 7, 6, 22, 0, tzinfo=timezone.utc),
        calendar=calendar,
    )
    missing_check = next(
        row for row in missing["checks"] if row["name"] == "trade_ready_notification_delivery"
    )

    assert "trade_ready_notification_delivery" in missing["failed_checks"]
    assert missing_check["measured"]["ready_rows"] == 2
    assert missing_check["measured"]["expected_semantics"] == 1
    assert missing_check["measured"]["timely_delivered_events"] == 0

    expires_at = session.open_at + timedelta(seconds=20)
    outbox.enqueue(
        NotificationEnvelope(
            event_id=delivery_event_id,
            source="trade_intent",
            kind="trade_intent",
            lane="trade_ready",
            occurred_at=session.open_at,
            expires_at=expires_at,
        ),
        title="SPX TRADE READY",
        text="ticket",
        feishu_text="ticket",
        friend=True,
        targets=("bark", "feishu"),
        now=session.open_at,
    )
    delivered_at = session.open_at + timedelta(seconds=2)
    with sqlite3.connect(outbox.path) as connection:
        targets = [
            str(row[0])
            for row in connection.execute(
                "SELECT sink FROM notification_delivery_targets WHERE event_id=? ORDER BY sink",
                (delivery_event_id,),
            )
        ]
        connection.execute(
            "UPDATE notification_delivery_targets "
            "SET status='delivered', delivered_at=?, updated_at=? "
            "WHERE event_id=?",
            (
                delivered_at.isoformat(),
                delivered_at.isoformat(),
                delivery_event_id,
            ),
        )
        connection.execute(
            "UPDATE notification_delivery_events "
            "SET status='delivered', updated_at=? WHERE event_id=?",
            (delivered_at.isoformat(), delivery_event_id),
        )
        for sink in targets:
            connection.execute(
                "INSERT INTO notification_delivery_terminal_receipts ("
                "receipt_id, event_id, sink, outcome, reason, terminal_at, "
                "attempted, ok, queued_for_recovery, recorded_at"
                ") VALUES (?, ?, ?, 'delivered', 'delivery_succeeded', ?, 1, 1, 0, NULL)",
                (
                    f"success:{delivery_event_id}:{sink}",
                    delivery_event_id,
                    sink,
                    delivered_at.isoformat(),
                ),
            )
    _sync_receipts(tmp_path, outbox, now=delivered_at)

    delivered = build_rth_daily_acceptance(
        tmp_path,
        trading_date=TRADING_DATE,
        level_policy=LevelDecisionPolicy(),
        outbox_path=outbox.path,
        now=datetime(2026, 7, 6, 22, 0, tzinfo=timezone.utc),
        calendar=calendar,
    )
    delivered_check = next(
        row for row in delivered["checks"] if row["name"] == "trade_ready_notification_delivery"
    )

    assert "trade_ready_notification_delivery" not in delivered["failed_checks"]
    assert delivered_check["passed"] is True
    assert delivered_check["measured"]["timely_delivered_events"] == 1
    assert delivered_check["measured"]["delivered_semantics"] == 1

    with sqlite3.connect(_receipt_path(tmp_path)) as connection:
        connection.execute(
            "DELETE FROM notification_delivery_receipts WHERE event_id=?",
            (delivery_event_id,),
        )
    missing_real_receipt = build_rth_daily_acceptance(
        tmp_path,
        trading_date=TRADING_DATE,
        level_policy=LevelDecisionPolicy(),
        outbox_path=outbox.path,
        now=datetime(2026, 7, 6, 22, 0, tzinfo=timezone.utc),
        calendar=calendar,
    )
    missing_real_checks = {row["name"]: row for row in missing_real_receipt["checks"]}
    missing_real_event = missing_real_checks["trade_ready_notification_delivery"]["measured"][
        "event_diagnostics"
    ]["events"][delivery_event_id]

    assert missing_real_checks["trade_ready_notification_delivery"]["passed"] is False
    assert missing_real_event["reasons"] == ["success_receipt_missing_or_unmirrored"]
    assert missing_real_checks["notification_receipt_integrity"]["passed"] is False
    assert (
        len(missing_real_checks["notification_receipt_integrity"]["measured"]["missing_mirror_ids"])
        == 2
    )

    _sync_receipts(
        tmp_path,
        outbox,
        now=delivered_at + timedelta(seconds=1),
    )
    late_at = session.open_at + timedelta(seconds=6)
    with sqlite3.connect(outbox.path) as connection:
        connection.execute(
            "UPDATE notification_delivery_targets "
            "SET delivered_at=?, updated_at=? WHERE event_id=?",
            (late_at.isoformat(), late_at.isoformat(), delivery_event_id),
        )
    late = build_rth_daily_acceptance(
        tmp_path,
        trading_date=TRADING_DATE,
        level_policy=LevelDecisionPolicy(),
        outbox_path=outbox.path,
        now=datetime(2026, 7, 6, 22, 0, tzinfo=timezone.utc),
        calendar=calendar,
    )
    late_check = next(
        row for row in late["checks"] if row["name"] == "trade_ready_notification_delivery"
    )
    diagnostics = late_check["measured"]["event_diagnostics"]["events"]

    assert "trade_ready_notification_delivery" in late["failed_checks"]
    assert diagnostics[delivery_event_id]["reasons"] == ["first_delivery_slo_breached"]


def test_rearmed_same_semantic_requires_each_delivery_event_outcome(
    tmp_path,
) -> None:
    calendar, session = _seed_complete_day(tmp_path)
    outbox = _outbox(tmp_path)
    _enqueue_report_targets(outbox, session)
    _deliver_all_targets(outbox)
    semantic_key = "2026-07-06|breakout|up|7500|SPXW-7500C"
    intents = [
        {
            "status": "trade_ready",
            "intent_id": "intent:daily-acceptance-rearm",
            "event_id": event_id,
            "semantic_key": semantic_key,
        }
        for event_id in ("level:first-episode", "level:rearmed-episode")
    ]
    delivery_event_ids = [_trade_ready_delivery_event_id(intent) for intent in intents]
    assert len(set(delivery_event_ids)) == 2
    _write_jsonl(
        tmp_path
        / "features"
        / "trade_intents"
        / f"date={TRADING_DATE.isoformat()}"
        / "events.jsonl",
        intents,
    )
    producer_path = (
        tmp_path
        / "features"
        / "trade_intent_producer_ledger"
        / f"date={TRADING_DATE.isoformat()}"
        / "events.jsonl"
    )
    producer_rows = [
        json.loads(line) for line in producer_path.read_text(encoding="utf-8").splitlines()
    ]
    producer_rows.extend(
        {
            "schema_version": "trade_intent_producer_ledger.v1",
            "record_type": "trade_ready_delivery_expectation",
            "record_id": f"expectation:{delivery_event_id}",
            "observed_at": session.open_at.isoformat(),
            "trading_date_et": TRADING_DATE.isoformat(),
            "slot_id": f"{TRADING_DATE.isoformat()}:rth:000",
            "semantic_key": semantic_key,
            "delivery_event_id": delivery_event_id,
            "intent_id": intent["intent_id"],
            "intent_event_id": intent["event_id"],
        }
        for intent, delivery_event_id in zip(
            intents,
            delivery_event_ids,
            strict=True,
        )
    )
    _write_jsonl(producer_path, producer_rows)

    first_event_id = delivery_event_ids[0]
    expires_at = session.open_at + timedelta(seconds=20)
    outbox.enqueue(
        NotificationEnvelope(
            event_id=first_event_id,
            source="trade_intent",
            kind="trade_intent",
            lane="trade_ready",
            occurred_at=session.open_at,
            expires_at=expires_at,
        ),
        title="SPX TRADE READY",
        text="first ticket",
        feishu_text="first ticket",
        friend=True,
        targets=("bark", "feishu"),
        now=session.open_at,
    )
    delivered_at = session.open_at + timedelta(seconds=2)
    with sqlite3.connect(outbox.path) as connection:
        sinks = [
            str(row[0])
            for row in connection.execute(
                "SELECT sink FROM notification_delivery_targets WHERE event_id=? ORDER BY sink",
                (first_event_id,),
            )
        ]
        connection.execute(
            "UPDATE notification_delivery_targets "
            "SET status='delivered', delivered_at=?, updated_at=? "
            "WHERE event_id=?",
            (
                delivered_at.isoformat(),
                delivered_at.isoformat(),
                first_event_id,
            ),
        )
        connection.execute(
            "UPDATE notification_delivery_events "
            "SET status='delivered', updated_at=? WHERE event_id=?",
            (delivered_at.isoformat(), first_event_id),
        )
        for sink in sinks:
            connection.execute(
                "INSERT INTO notification_delivery_terminal_receipts ("
                "receipt_id, event_id, sink, outcome, reason, terminal_at, "
                "attempted, ok, queued_for_recovery, recorded_at"
                ") VALUES (?, ?, ?, 'delivered', 'delivery_succeeded', ?, 1, 1, 0, NULL)",
                (
                    f"success:{first_event_id}:{sink}",
                    first_event_id,
                    sink,
                    delivered_at.isoformat(),
                ),
            )
    _sync_receipts(tmp_path, outbox, now=delivered_at)

    one_of_two = build_rth_daily_acceptance(
        tmp_path,
        trading_date=TRADING_DATE,
        level_policy=LevelDecisionPolicy(),
        outbox_path=outbox.path,
        now=datetime(2026, 7, 6, 22, 0, tzinfo=timezone.utc),
        calendar=calendar,
    )
    one_of_two_check = next(
        row for row in one_of_two["checks"] if row["name"] == "trade_ready_notification_delivery"
    )

    assert one_of_two_check["passed"] is False
    assert one_of_two_check["measured"]["expected_semantics"] == 1
    assert one_of_two_check["measured"]["expected_events"] == 2
    assert one_of_two_check["measured"]["timely_delivered_events"] == 1
    assert one_of_two_check["measured"]["missing_delivery_events"] == [delivery_event_ids[1]]
    assert one_of_two_check["measured"]["missing_delivery_semantics"] == [semantic_key]

    second_event_id = delivery_event_ids[1]
    outbox.enqueue(
        NotificationEnvelope(
            event_id=second_event_id,
            source="trade_intent",
            kind="trade_intent",
            lane="trade_ready",
            occurred_at=session.open_at,
            expires_at=expires_at,
        ),
        title="SPX TRADE READY",
        text="rearmed ticket",
        feishu_text="rearmed ticket",
        friend=True,
        targets=("bark", "feishu"),
        now=session.open_at,
    )
    cancelled_at = session.open_at + timedelta(seconds=3)
    assert (
        outbox.cancel_event(
            second_event_id,
            reason="source_lifecycle_invalidated",
            now=cancelled_at,
        )
        == 2
    )
    _sync_receipts(tmp_path, outbox, now=cancelled_at)

    settled = build_rth_daily_acceptance(
        tmp_path,
        trading_date=TRADING_DATE,
        level_policy=LevelDecisionPolicy(),
        outbox_path=outbox.path,
        now=datetime(2026, 7, 6, 22, 0, tzinfo=timezone.utc),
        calendar=calendar,
    )
    settled_check = next(
        row for row in settled["checks"] if row["name"] == "trade_ready_notification_delivery"
    )

    assert settled_check["passed"] is True
    assert settled_check["measured"]["accepted_events"] == 2
    assert settled_check["measured"]["explicitly_terminal_events"] == 1
    assert settled_check["measured"]["terminally_settled_semantics"] == 1
    assert settled_check["measured"]["missing_delivery_events"] == []


def test_missing_producer_heartbeat_fails_closed_even_with_zero_ready(tmp_path) -> None:
    calendar, session = _seed_complete_day(tmp_path)
    outbox = _outbox(tmp_path)
    _enqueue_report_targets(outbox, session)
    _deliver_all_targets(outbox)
    producer_path = (
        tmp_path
        / "features"
        / "trade_intent_producer_ledger"
        / f"date={TRADING_DATE.isoformat()}"
        / "events.jsonl"
    )
    rows = [json.loads(line) for line in producer_path.read_text(encoding="utf-8").splitlines()][
        :-1
    ]
    _write_jsonl(producer_path, rows)

    report = build_rth_daily_acceptance(
        tmp_path,
        trading_date=TRADING_DATE,
        level_policy=LevelDecisionPolicy(),
        outbox_path=outbox.path,
        now=datetime(2026, 7, 6, 22, 0, tzinfo=timezone.utc),
        calendar=calendar,
    )
    check = next(row for row in report["checks"] if row["name"] == "trade_intent_producer_coverage")

    assert check["passed"] is False
    assert check["measured"]["observed_slots"] == 77
    assert "trade_intent_producer_coverage" in report["failed_checks"]


def test_formal_level_signal_without_evidence_fails_daily_acceptance(tmp_path) -> None:
    calendar, session = _seed_complete_day(tmp_path)
    outbox = _outbox(tmp_path)
    _enqueue_report_targets(outbox, session)
    _deliver_all_targets(outbox)

    report = build_rth_daily_acceptance(
        tmp_path,
        trading_date=TRADING_DATE,
        level_policy=replace(LevelDecisionPolicy(), formal_signal_enabled=True),
        outbox_path=outbox.path,
        now=datetime(2026, 7, 6, 22, 0, tzinfo=timezone.utc),
        calendar=calendar,
    )

    assert report["status"] == "degraded"
    assert "level_decision_formal_signal_evidence" in report["failed_checks"]


def test_scheduler_jitter_is_bounded_instead_of_requiring_exact_second(tmp_path) -> None:
    calendar, _session = _seed_complete_day(tmp_path, report_lag_seconds=121)
    outbox = _outbox(tmp_path)

    report = build_rth_daily_acceptance(
        tmp_path,
        trading_date=TRADING_DATE,
        level_policy=LevelDecisionPolicy(),
        outbox_path=outbox.path,
        now=datetime(2026, 7, 6, 22, 0, tzinfo=timezone.utc),
        calendar=calendar,
    )

    failed = set(report["failed_checks"])
    assert "rth_report_slot_coverage" in failed
    assert "rth_report_delivery_coverage" in failed


def test_early_close_derives_expected_counts_from_market_session(tmp_path) -> None:
    calendar = MarketCalendar(early_closes={TRADING_DATE: time(13, 0)})
    calendar, session = _seed_complete_day(tmp_path, calendar=calendar)
    outbox = _outbox(tmp_path)
    _enqueue_report_targets(outbox, session)
    _deliver_all_targets(outbox)

    report = build_rth_daily_acceptance(
        tmp_path,
        trading_date=TRADING_DATE,
        level_policy=LevelDecisionPolicy(),
        outbox_path=outbox.path,
        now=datetime(2026, 7, 6, 18, 0, tzinfo=timezone.utc),
        calendar=calendar,
    )

    assert report["status"] == "complete"
    assert report["session_clock"]["expected_spring_minutes"] == 210
    assert report["session_clock"]["expected_report_slots"] == 14


def test_missing_cross_process_outputs_are_explicit_failures(tmp_path) -> None:
    outbox = _outbox(tmp_path)
    _write_json(
        tmp_path
        / "reports"
        / "spx_options_review"
        / f"date={TRADING_DATE.isoformat()}"
        / "review.json",
        {"verdict": {"status": "degraded"}},
    )

    report = build_rth_daily_acceptance(
        tmp_path,
        trading_date=TRADING_DATE,
        level_policy=LevelDecisionPolicy(),
        outbox_path=outbox.path,
        now=datetime(2026, 7, 6, 22, 0, tzinfo=timezone.utc),
    )

    assert report["status"] == "degraded"
    assert {
        "spring_rth_minute_coverage",
        "spring_option_overlay_ready_ratio",
        "rth_report_slot_coverage",
        "rth_report_spring_projection_coverage",
        "rth_report_state_window_coverage",
        "post_close_market_data_completeness",
    } <= set(report["failed_checks"])


def test_failed_spring_rows_and_empty_windows_do_not_false_pass(tmp_path) -> None:
    calendar, session = _seed_complete_day(tmp_path)
    outbox = _outbox(tmp_path)
    spring, reports = _full_day_rows(session)
    for row in spring:
        row["rth_market_state"] = {"state": "UNCERTAIN"}
    for row in reports:
        row["spring_gamma_v3_state_window"]["states"] = []
        row["spring_gamma_v3_state_window"]["sample_count"] = 0
        row["spring_gamma_v3_state_window"]["five_minute_slot_count"] = 0
    _write_jsonl(
        tmp_path
        / "features"
        / "spring_gamma_v3"
        / f"date={TRADING_DATE.isoformat()}"
        / "predictions.jsonl",
        spring,
    )
    _write_jsonl(
        tmp_path
        / "audit"
        / "order_map_pricing"
        / f"date={TRADING_DATE.isoformat()}"
        / "reports.jsonl",
        reports,
    )

    report = build_rth_daily_acceptance(
        tmp_path,
        trading_date=TRADING_DATE,
        level_policy=LevelDecisionPolicy(),
        outbox_path=outbox.path,
        now=datetime(2026, 7, 6, 22, 0, tzinfo=timezone.utc),
        calendar=calendar,
    )

    assert "spring_rth_minute_coverage" in report["failed_checks"]
    assert "rth_report_state_window_coverage" in report["failed_checks"]


def test_wrong_state_authority_and_window_identity_do_not_false_pass(tmp_path) -> None:
    calendar, session = _seed_complete_day(tmp_path)
    outbox = _outbox(tmp_path)
    spring, reports = _full_day_rows(session)
    for row in spring:
        row["rth_market_state"]["action_authority"] = "production"
    for row in reports:
        row["spring_gamma_v3_state_window"]["session_id"] = "2026-07-03"
        row["spring_gamma_v3_state_window"]["expiry"] = "20260703"
    _write_jsonl(
        tmp_path
        / "features"
        / "spring_gamma_v3"
        / f"date={TRADING_DATE.isoformat()}"
        / "predictions.jsonl",
        spring,
    )
    _write_jsonl(
        tmp_path
        / "audit"
        / "order_map_pricing"
        / f"date={TRADING_DATE.isoformat()}"
        / "reports.jsonl",
        reports,
    )

    report = build_rth_daily_acceptance(
        tmp_path,
        trading_date=TRADING_DATE,
        level_policy=LevelDecisionPolicy(),
        outbox_path=outbox.path,
        now=datetime(2026, 7, 6, 22, 0, tzinfo=timezone.utc),
        calendar=calendar,
    )

    assert "spring_rth_minute_coverage" in report["failed_checks"]
    assert "rth_report_state_window_coverage" in report["failed_checks"]


def test_non_fifteen_minute_state_windows_do_not_false_pass(tmp_path) -> None:
    calendar, session = _seed_complete_day(tmp_path)
    outbox = _outbox(tmp_path)
    _spring, reports = _full_day_rows(session)
    for row in reports:
        window = row["spring_gamma_v3_state_window"]
        end = datetime.fromisoformat(window["window_end"])
        window["window_start"] = (end - timedelta(minutes=1)).isoformat()
    _write_jsonl(
        tmp_path
        / "audit"
        / "order_map_pricing"
        / f"date={TRADING_DATE.isoformat()}"
        / "reports.jsonl",
        reports,
    )

    report = build_rth_daily_acceptance(
        tmp_path,
        trading_date=TRADING_DATE,
        level_policy=LevelDecisionPolicy(),
        outbox_path=outbox.path,
        now=datetime(2026, 7, 6, 22, 0, tzinfo=timezone.utc),
        calendar=calendar,
    )

    assert "rth_report_state_window_coverage" in report["failed_checks"]


def test_all_configured_outbox_targets_delivered_counts_report_as_delivered(tmp_path) -> None:
    calendar, session = _seed_complete_day(tmp_path)
    outbox = _outbox(tmp_path)
    reports_path = (
        tmp_path
        / "audit"
        / "order_map_pricing"
        / f"date={TRADING_DATE.isoformat()}"
        / "reports.jsonl"
    )
    reports = [json.loads(line) for line in reports_path.read_text(encoding="utf-8").splitlines()]
    for row in reports:
        row["delivered_ok"] = False
    _write_jsonl(reports_path, reports)

    _enqueue_report_targets(outbox, session)
    _deliver_all_targets(outbox)

    report = build_rth_daily_acceptance(
        tmp_path,
        trading_date=TRADING_DATE,
        level_policy=LevelDecisionPolicy(),
        outbox_path=outbox.path,
        now=datetime(2026, 7, 6, 22, 0, tzinfo=timezone.utc),
        calendar=calendar,
    )

    assert "rth_report_delivery_coverage" not in report["failed_checks"]
    assert "notification_outbox_integrity" not in report["failed_checks"]


def test_one_delivered_sink_and_one_pending_sink_fails_closed(tmp_path) -> None:
    calendar, session = _seed_complete_day(tmp_path)
    outbox = _outbox(tmp_path)
    _enqueue_report_targets(outbox, session)
    _deliver_all_targets(outbox)
    first_slot = session.open_at
    first_slot_key = f"{TRADING_DATE.isoformat()}:{first_slot.strftime('%H:%M')}"
    first_event_id = notification_event_id(
        "status",
        source="order_map_status",
        occurred_at=first_slot,
        identity=f"rth_slot:{first_slot_key}",
    )
    with sqlite3.connect(outbox.path) as connection:
        connection.execute(
            "UPDATE notification_delivery_targets "
            "SET status='pending', delivered_at=NULL "
            "WHERE event_id=? AND sink='feishu'",
            (first_event_id,),
        )

    report = build_rth_daily_acceptance(
        tmp_path,
        trading_date=TRADING_DATE,
        level_policy=LevelDecisionPolicy(),
        outbox_path=outbox.path,
        now=datetime(2026, 7, 6, 22, 0, tzinfo=timezone.utc),
        calendar=calendar,
    )
    checks = {row["name"]: row for row in report["checks"]}

    assert checks["rth_report_delivery_coverage"]["measured"] == round(25 / 26, 6)
    assert checks["rth_report_delivery_coverage"]["passed"] is False
    assert checks["notification_outbox_integrity"]["measured"]["pending_targets"] == 1
    assert checks["notification_outbox_integrity"]["passed"] is False
    assert {
        "rth_report_delivery_coverage",
        "notification_outbox_integrity",
    } <= set(report["failed_checks"])


def test_acknowledged_dead_letter_does_not_poison_future_acceptance(tmp_path) -> None:
    calendar, session = _seed_complete_day(tmp_path)
    outbox = _outbox(tmp_path)
    event_id = "acknowledged-historical-dead-letter"
    outbox.enqueue(
        NotificationEnvelope(
            event_id=event_id,
            source="test",
            kind="test",
            lane="ops_transition",
            occurred_at=session.close_at,
        ),
        title="test",
        text="test",
        feishu_text=None,
        friend=False,
        targets=("bark",),
        now=session.close_at,
    )
    with sqlite3.connect(outbox.path) as connection:
        connection.execute(
            "UPDATE notification_delivery_targets "
            "SET status='dead_letter', acknowledged_at=updated_at "
            "WHERE event_id=?",
            (event_id,),
        )

    acknowledged = build_rth_daily_acceptance(
        tmp_path,
        trading_date=TRADING_DATE,
        level_policy=LevelDecisionPolicy(),
        outbox_path=outbox.path,
        now=datetime(2026, 7, 6, 22, 0, tzinfo=timezone.utc),
        calendar=calendar,
    )
    assert "notification_outbox_integrity" not in acknowledged["failed_checks"]

    with sqlite3.connect(outbox.path) as connection:
        connection.execute(
            "UPDATE notification_delivery_targets SET acknowledged_at=NULL WHERE event_id=?",
            (event_id,),
        )
    unacknowledged = build_rth_daily_acceptance(
        tmp_path,
        trading_date=TRADING_DATE,
        level_policy=LevelDecisionPolicy(),
        outbox_path=outbox.path,
        now=datetime(2026, 7, 6, 22, 0, tzinfo=timezone.utc),
        calendar=calendar,
    )
    assert "notification_outbox_integrity" in unacknowledged["failed_checks"]


def test_unmirrored_ordinary_delivery_receipt_fails_outbox_integrity(
    tmp_path,
) -> None:
    calendar, session = _seed_complete_day(tmp_path)
    outbox = _outbox(tmp_path)
    _enqueue_report_targets(outbox, session)
    _deliver_all_targets(outbox)
    event_id = "delivered-before-receipt-mirror"
    outbox.enqueue(
        NotificationEnvelope(
            event_id=event_id,
            source="test",
            kind="test",
            lane="ops_transition",
            occurred_at=session.close_at,
        ),
        title="test",
        text="test",
        feishu_text=None,
        friend=False,
        targets=("bark",),
        now=session.close_at,
    )
    claimed = outbox.claim_due(
        worker_id="daily-acceptance-test",
        limit_targets=1,
        now=session.close_at,
        event_id=event_id,
    )
    assert claimed[0].targets == ("bark",)
    outbox.settle_target(
        event_id,
        "bark",
        worker_id="daily-acceptance-test",
        ok=True,
        error=None,
        now=session.close_at,
    )

    report = build_rth_daily_acceptance(
        tmp_path,
        trading_date=TRADING_DATE,
        level_policy=LevelDecisionPolicy(),
        outbox_path=outbox.path,
        now=datetime(2026, 7, 6, 22, 0, tzinfo=timezone.utc),
        calendar=calendar,
    )
    checks = {row["name"]: row for row in report["checks"]}

    integrity = checks["notification_outbox_integrity"]
    assert integrity["measured"]["terminal_receipt_schema_present"] is True
    assert integrity["measured"]["terminal_receipts_pending"] == 1
    assert integrity["passed"] is False
    assert "notification_outbox_integrity" in report["failed_checks"]


def test_missing_real_receipt_store_fails_daily_integrity(tmp_path) -> None:
    calendar, session = _seed_complete_day(tmp_path)
    outbox = _outbox(tmp_path)
    _enqueue_report_targets(outbox, session)
    _deliver_all_targets(outbox)
    _receipt_path(tmp_path).unlink()

    report = build_rth_daily_acceptance(
        tmp_path,
        trading_date=TRADING_DATE,
        level_policy=LevelDecisionPolicy(),
        outbox_path=outbox.path,
        now=datetime(2026, 7, 6, 22, 0, tzinfo=timezone.utc),
        calendar=calendar,
    )
    receipt_check = next(
        row for row in report["checks"] if row["name"] == "notification_receipt_integrity"
    )

    assert receipt_check["passed"] is False
    assert receipt_check["measured"]["exists"] is False
    assert receipt_check["measured"]["quick_check"] == "missing"
    assert "notification_receipt_integrity" in report["failed_checks"]


def test_degraded_acceptance_alert_uses_stable_close_clock_and_ops_lane() -> None:
    captured = {}

    def enqueue(settings, envelope, **kwargs):
        captured.update({"settings": settings, "envelope": envelope, **kwargs})
        return EnqueueResult(
            envelope=envelope,
            targets=("bark",),
            outcome="pending",
            accepted=True,
            inserted=True,
            duplicate=False,
            delivered=False,
            queued_for_recovery=True,
        )

    result = enqueue_degraded_acceptance(
        {
            "status": "degraded",
            "trading_date": "2026-07-06",
            "failed_checks": ["rth_report_slot_coverage"],
        },
        settings=object(),
        occurred_at=datetime(2026, 7, 6, 16, 0, tzinfo=ET),
        enqueue=enqueue,
    )

    envelope = captured["envelope"]
    assert isinstance(envelope, NotificationEnvelope)
    assert envelope.lane == "ops_transition"
    assert envelope.occurred_at == datetime(2026, 7, 6, 20, 0, tzinfo=timezone.utc)
    assert "rth_report_slot_coverage" in captured["text"]
    assert result == {
        "status": "pending",
        "accepted": True,
        "inserted": True,
        "duplicate": False,
    }
