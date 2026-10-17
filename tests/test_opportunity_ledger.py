from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

from spx_spark.infrastructure.opportunity_ledger import (
    capture_summary,
    capture_week_summary,
    record_current_decision,
)


def _write_decision(tmp_path, decision: dict[str, object]) -> None:
    path = tmp_path / "latest" / "strategy_decision.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(decision))


def test_opportunity_ledger_records_one_row_per_decision_state(tmp_path) -> None:
    now = datetime(2026, 10, 16, 14, 0, tzinfo=timezone.utc)
    _write_decision(
        tmp_path,
        {
            "decision_id": "strategy:authorized",
            "session_date": "2026-10-16",
            "decision_type": "CALL_DEBIT_VERTICAL",
            "correlation_id": "corr:authorized",
            "candidate": {
                "opportunity_id": "opp:authorized",
                "strategy_type": "CALL_DEBIT_VERTICAL",
                "direction": "up",
            },
            "rejection_funnel": {"current_stage": "manual_card_pending"},
        },
    )
    assert record_current_decision(tmp_path, now=now) is not None
    assert record_current_decision(tmp_path, now=now) is None

    _write_decision(
        tmp_path,
        {
            "decision_id": "strategy:rejected",
            "session_date": "2026-10-16",
            "decision_type": "NO_TRADE",
            "rejection_funnel": {
                "current_stage": "setup_detected",
                "primary_blocker": "rth_entry_window_not_open",
            },
        },
    )
    assert record_current_decision(tmp_path, now=now) is not None

    _write_decision(
        tmp_path,
        {
            "decision_id": "strategy:detected",
            "session_date": "2026-10-16",
            "decision_type": "NO_TRADE",
            "rejection_funnel": {
                "current_stage": "setup_detected",
                "setup_detected": 1,
                "primary_blocker": "exact_quote_ready_missing",
            },
        },
    )
    assert record_current_decision(tmp_path, now=now) is not None

    database = tmp_path / "spx.sqlite"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE notification_events (correlation_id TEXT, status TEXT)"
        )
        connection.execute(
            "INSERT INTO notification_events VALUES ('corr:authorized', 'delivered')"
        )

    summary = capture_summary(tmp_path, "2026-10-16")
    assert summary["total"] == 3
    assert summary["by_state"] == {"authorized": 1, "detected": 1, "rejected": 1}
    assert summary["top_blockers"] == {
        "rth_entry_window_not_open": 1,
        "exact_quote_ready_missing": 1,
    }
    assert summary["authorized_delivered"] == 1
    assert summary["authorized_not_delivered"] == 0
    assert summary["acted"] == 0
    assert capture_week_summary(tmp_path, "2026-10-16")["total"] == 3
    ledger = tmp_path / "opportunities" / "date=2026-10-16.jsonl"
    assert len(ledger.read_text().splitlines()) == 3
