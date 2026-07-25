from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone

from spx_spark.application.order_map.rth_daily_acceptance import (
    build_rth_daily_acceptance,
    enqueue_degraded_acceptance,
    write_rth_daily_acceptance,
)
from spx_spark.notifier.dispatcher import EnqueueResult
from spx_spark.notifier.receipts import NotificationEnvelope
from spx_spark.notifier.receipts import notification_event_id
from spx_spark.market_calendar import ET, MarketCalendar
from spx_spark.notifier.delivery_outbox import NotificationDeliveryOutbox
from spx_spark.settings.level_decision import LevelDecisionPolicy


TRADING_DATE = date(2026, 7, 6)


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
    return NotificationDeliveryOutbox(
        tmp_path / "notification.sqlite",
        max_attempts=3,
        retry_schedule_seconds=(1.0,),
        dead_letter_after_seconds=60.0,
        claim_stale_after_seconds=10.0,
    )


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
            "UPDATE notification_delivery_targets "
            "SET status='delivered', delivered_at=updated_at"
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
