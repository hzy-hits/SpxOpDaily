from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone

from spx_spark.data_platform.adapters.memory import InMemoryDecisionLedger
from spx_spark.data_platform.adapters.sqlite_ledger import SQLiteDecisionLedger
from spx_spark.data_platform.contracts import DecisionRecord, EventRecord
from spx_spark.data_platform.integration import (
    record_intraday_evaluation,
    record_notification_result,
    record_outcome_rows,
)
from spx_spark.data_platform.settings import DataPlatformSettings
from spx_spark.data_platform.telemetry import FallbackSpool, OperationalTelemetry
from spx_spark.data_platform.research import ResearchCatalog


NOW = datetime(2026, 7, 10, 14, 0, tzinfo=timezone.utc)


def disabled_settings(tmp_path) -> DataPlatformSettings:
    return DataPlatformSettings(
        enabled=False,
        data_root=str(tmp_path),
        ledger_path=str(tmp_path / "runtime/ledger.sqlite3"),
        fallback_spool_path=str(tmp_path / "runtime/fallback.jsonl"),
        fallback_spool_max_bytes=67_108_864,
        lake_root=str(tmp_path / "lake"),
        manifest_root=str(tmp_path / "manifests"),
        research_catalog_path=str(tmp_path / "analytics/research.duckdb"),
        sqlite_busy_timeout_ms=250,
        compaction_min_age_seconds=300,
        raw_delete_enabled=False,
        raw_delete_grace_hours=72,
        writer_version="test-v1",
    )


def test_intraday_decision_delivery_and_outcome_share_stable_links(tmp_path) -> None:
    ledger = InMemoryDecisionLedger()
    telemetry = OperationalTelemetry(ledger, FallbackSpool(tmp_path / "fallback.jsonl"))
    settings = disabled_settings(tmp_path)
    alert = {
        "kind": "intraday_price_reclaim",
        "event_id": "raw-event-never-persist",
        "dedup_group": "raw-event-never-persist:reclaim",
        "severity": "high",
        "quality": "live",
        "value": 0.6,
        "source_gate": "confirmed",
        "title": "V reclaim",
        "detail": "confirmed",
    }
    research = record_intraday_evaluation(
        session_date="2026-07-10",
        source_at=NOW,
        available_at=NOW + timedelta(milliseconds=100),
        spx=6300.0,
        es=6310.0,
        spx_source_at=NOW,
        es_source_at=NOW,
        structure={"gamma_state": "negative", "call_wall": 6320.0},
        path_decision={
            "status": "confirmed",
            "play": "flip_reclaim_call",
            "gamma_state": "negative",
            "conditional_call_bias": True,
            "event_id": "raw-event-never-persist",
            "reasons": ["confirmed"],
            "blocks": [],
        },
        alerts=(alert,),
        strategy_config={"confirm_samples": 2},
        settings=settings,
        telemetry=telemetry,
    )
    assert research.status == "recorded"
    link = research.alert_links[0]
    decorated = {
        **alert,
        "source_at": link.source_at.isoformat(),
        "event_key": link.event_key,
        "decision_id": link.decision_id,
    }
    delivery_ids = record_notification_result(
        payload={"as_of": NOW.isoformat()},
        selected_alerts=(decorated,),
        notification={
            "sinks": [
                {"sink": "deepseek_delivery_gate", "attempted": True, "ok": True},
                {"sink": "bark", "attempted": True, "ok": True},
            ]
        },
        attempted_at=NOW + timedelta(seconds=1),
        settings=settings,
        telemetry=telemetry,
    )
    outcome_ids = record_outcome_rows(
        (
            {
                "event_key": link.event_key,
                "phase": "reclaim",
                "direction": "down",
                "observed_at": NOW.isoformat(),
                "horizon_minutes": 5,
                "target_at": (NOW + timedelta(minutes=5)).isoformat(),
                "sample_at": (NOW + timedelta(minutes=5)).isoformat(),
                "status": "complete",
                "return_bps": 12.0,
                "mfe_bps": 18.0,
                "mae_bps": -2.0,
            },
        ),
        settings=settings,
        telemetry=telemetry,
    )

    assert len(delivery_ids) == 2
    assert len(outcome_ids) == 1
    assert len(ledger.list_deliveries(link.decision_id)) == 2
    assert len(ledger.outcomes) == 1
    combined = " ".join(str(row) for row in ledger.events.values())
    assert "raw-event-never-persist" not in combined


def test_fallback_notification_events_are_scoped_to_attempt_and_content(tmp_path) -> None:
    ledger = InMemoryDecisionLedger()
    spool = FallbackSpool(tmp_path / "fallback.jsonl")
    telemetry = OperationalTelemetry(ledger, spool)
    settings = disabled_settings(tmp_path)
    alert = {
        "kind": "price_move_from_close",
        "dedup_group": "price_move_from_close:SPX",
        "severity": "high",
        "quality": "live",
        "source_at": NOW.isoformat(),
        "title": "SPX move",
        "detail": "first observation",
    }

    first = record_notification_result(
        payload={"as_of": NOW.isoformat()},
        selected_alerts=(alert,),
        notification={"sinks": [{"sink": "feishu", "attempted": True, "ok": False}]},
        attempted_at=NOW + timedelta(seconds=1),
        settings=settings,
        telemetry=telemetry,
    )
    second = record_notification_result(
        payload={"as_of": NOW.isoformat()},
        selected_alerts=({**alert, "quality": "degraded"},),
        notification={"sinks": [{"sink": "feishu", "attempted": True, "ok": True}]},
        attempted_at=NOW + timedelta(seconds=1),
        settings=settings,
        telemetry=telemetry,
    )
    third = record_notification_result(
        payload={"as_of": NOW.isoformat()},
        selected_alerts=(alert,),
        notification={"sinks": [{"sink": "feishu", "attempted": True, "ok": False}]},
        attempted_at=NOW + timedelta(seconds=2),
        settings=settings,
        telemetry=telemetry,
    )

    assert len(first) == len(second) == len(third) == 1
    assert len({first[0], second[0], third[0]}) == 3
    assert len(ledger.events) == 3
    assert len(ledger.decisions) == 3
    assert not spool.path.exists()


def test_delivery_content_change_gets_new_immutable_delivery_id(tmp_path) -> None:
    ledger = InMemoryDecisionLedger()
    spool = FallbackSpool(tmp_path / "fallback.jsonl")
    telemetry = OperationalTelemetry(ledger, spool)
    settings = disabled_settings(tmp_path)
    event_record = EventRecord(
        event_key="evt_test",
        event_type="flip_reclaim_call",
        session_date=date(2026, 7, 10),
        source_at=NOW,
        available_at=NOW,
        data_quality="live",
    )
    decision_record = DecisionRecord(
        decision_id="dec_test",
        event_key="evt_test",
        strategy_name="intraday",
        strategy_version="v1",
        decision_at=NOW,
        available_at=NOW,
        status="selected",
        action="notify",
        side="call",
    )
    assert (
        telemetry.record_decision_bundle(event=event_record, decision=decision_record).status
        == "recorded"
    )
    alert = {
        "kind": "flip_reclaim_call",
        "event_key": "evt_test",
        "decision_id": "dec_test",
        "severity": "high",
        "source_at": NOW.isoformat(),
        "title": "call confirmed",
        "detail": "confirmed",
    }
    attempted_at = NOW + timedelta(seconds=1)

    failed = record_notification_result(
        payload={"as_of": NOW.isoformat()},
        selected_alerts=(alert,),
        notification={
            "sinks": [
                {
                    "sink": "feishu",
                    "attempted": True,
                    "ok": False,
                    "error": "temporary failure",
                }
            ]
        },
        attempted_at=attempted_at,
        settings=settings,
        telemetry=telemetry,
    )
    sent = record_notification_result(
        payload={"as_of": NOW.isoformat()},
        selected_alerts=(alert,),
        notification={"sinks": [{"sink": "feishu", "attempted": True, "ok": True}]},
        attempted_at=attempted_at,
        settings=settings,
        telemetry=telemetry,
    )

    assert len(failed) == len(sent) == 1
    assert failed != sent
    assert {row.status for row in ledger.list_deliveries("dec_test")} == {"failed", "sent"}
    assert not spool.path.exists()


def test_disabled_integration_is_noop(tmp_path) -> None:
    result = record_intraday_evaluation(
        session_date="2026-07-10",
        source_at=NOW,
        available_at=NOW,
        spx=6300.0,
        es=6310.0,
        spx_source_at=NOW,
        es_source_at=NOW,
        structure={},
        path_decision={},
        alerts=(),
        strategy_config={},
        settings=disabled_settings(tmp_path),
    )
    assert result.status == "disabled"


def test_actual_sqlite_catalog_links_scoped_veto_delivery_and_call_outcome(tmp_path) -> None:
    settings = replace(disabled_settings(tmp_path), enabled=True)
    ledger = SQLiteDecisionLedger(settings.ledger_path)
    telemetry = OperationalTelemetry(ledger, FallbackSpool(settings.fallback_spool_path))
    alerts = (
        {
            "kind": "flip_reclaim_call",
            "event_id": "raw-call-id",
            "dedup_group": "raw-call-id:strategy",
            "severity": "high",
            "quality": "live",
            "source_at": NOW.isoformat(),
            "source_event_key": "opaque-source-event",
            "title": "call confirmed",
            "detail": "confirmed",
        },
        {
            "kind": "intraday_price_shock",
            "event_id": "raw-shock-id",
            "dedup_group": "raw-shock-id:shock",
            "severity": "high",
            "quality": "live",
            "source_at": NOW.isoformat(),
            "value": -25.0,
            "title": "shock",
            "detail": "down",
        },
    )
    research = record_intraday_evaluation(
        session_date="2026-07-10",
        source_at=NOW,
        available_at=NOW + timedelta(milliseconds=100),
        spx=6300.0,
        es=6310.0,
        spx_source_at=NOW,
        es_source_at=NOW,
        structure={
            "gamma_state": "negative",
            "net_gex": -123.0,
            "call_wall": 6320.0,
            "put_wall": 6260.0,
            "zero_gamma": 6290.0,
        },
        path_decision={
            "status": "confirmed",
            "play": "flip_reclaim_call",
            "gamma_state": "negative",
            "conditional_call_bias": True,
            "reasons": ["confirmed"],
            "blocks": [],
        },
        alerts=alerts,
        strategy_config={"confirm_samples": 2},
        settings=settings,
        telemetry=telemetry,
    )
    call_link, shock_link = research.alert_links
    decorated = tuple(
        {
            **alert,
            "event_key": link.event_key,
            "decision_id": link.decision_id,
        }
        for alert, link in zip(alerts, research.alert_links, strict=True)
    )
    record_notification_result(
        payload={"as_of": NOW.isoformat()},
        selected_alerts=decorated,
        notification={
            "sinks": [
                {
                    "sink": "deepseek_delivery_gate",
                    "attempted": True,
                    "ok": True,
                    "verdict": "vetoed",
                    "error": "deepseek explicitly vetoed",
                    "alert_keys": [call_link.decision_id],
                },
                {
                    "sink": "bark",
                    "attempted": True,
                    "ok": True,
                    "alert_keys": [shock_link.decision_id],
                },
            ]
        },
        attempted_at=NOW + timedelta(seconds=1),
        settings=settings,
        telemetry=telemetry,
    )
    record_outcome_rows(
        (
            {
                "event_key": call_link.event_key,
                "decision_id": call_link.decision_id,
                "phase": "strategy",
                "direction": "up",
                "observed_at": NOW.isoformat(),
                "horizon_minutes": 5,
                "target_at": (NOW + timedelta(minutes=5)).isoformat(),
                "sample_at": (NOW + timedelta(minutes=5)).isoformat(),
                "status": "complete",
                "return_bps": 12.0,
                "mfe_bps": 18.0,
                "mae_bps": -2.0,
                "path_high_return_bps": 18.0,
                "path_low_return_bps": -2.0,
            },
        ),
        settings=settings,
        telemetry=telemetry,
    )

    with ResearchCatalog.in_memory(tmp_path, sqlite_ledger=settings.ledger_path) as catalog:
        rows = catalog.reader().strategy_outcomes()
        bias = catalog.reader().put_call_bias()

    call_row = next(row for row in rows if row["decision_id"] == call_link.decision_id)
    shock_row = next(row for row in rows if row["decision_id"] == shock_link.decision_id)
    assert call_row["triggered"] is True
    assert call_row["vetoed"] is True
    assert call_row["deepseek_vetoed"] is True
    assert call_row["delivered"] is False
    assert call_row["option_side"] == "CALL"
    assert call_row["net_gamma"] == -123.0
    assert call_row["call_wall"] == 6320.0
    assert call_row["directional_return_bps"] == 12.0
    assert shock_row["delivered"] is True
    assert shock_row["vetoed"] is False
    call_bias = next(row for row in bias if row["option_side"] == "CALL")
    assert call_bias["decision_count"] == 1
    assert call_bias["triggered_count"] == 1
    assert call_bias["vetoed_count"] == 1
    assert call_bias["deepseek_vetoed_count"] == 1
