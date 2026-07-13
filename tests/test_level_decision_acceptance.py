from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timezone

from spx_spark.application.order_map.level_decision_acceptance import (
    build_acceptance_report,
)
from spx_spark.settings.level_decision import LevelDecisionPolicy


def _write_row(path, row) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(row) + "\n", encoding="utf-8")


def test_empty_acceptance_report_never_promotes(tmp_path) -> None:
    report = build_acceptance_report(
        tmp_path,
        policy=LevelDecisionPolicy(),
        now=datetime(2026, 7, 13, 22, 0, tzinfo=timezone.utc),
    )

    assert report["formal_signal"] is False
    assert report["promoted"] is False
    assert report["eligible_for_explicit_review"] is False


def test_acceptance_report_requires_explicit_review_even_after_count_gates(tmp_path) -> None:
    day = "2026-07-13"
    _write_row(
        tmp_path / "features" / "level_decision_audit" / f"date={day}" / "transitions.jsonl",
        {"event_id": "level:one", "current_phase": "confirmed"},
    )
    _write_row(
        tmp_path / "features" / "level_decision_outcomes" / f"date={day}" / "outcomes.jsonl",
        {"event_id": "level:one", "attribution": "follow_through"},
    )
    _write_row(
        tmp_path / "features" / "level_decision_health" / f"date={day}" / "samples.jsonl",
        {
            "at": "2026-07-13T17:00:00+00:00",
            "session_date": day,
            "session_mode": "rth",
            "quality_ok": True,
        },
    )
    policy = replace(
        LevelDecisionPolicy(),
        acceptance_min_events=1,
        acceptance_min_sessions=1,
        acceptance_min_complete_rth_sessions=1,
        acceptance_expected_sample_seconds=23_400.0,
        acceptance_max_rth_gap_seconds=23_400.0,
    )

    report = build_acceptance_report(
        tmp_path,
        policy=policy,
        now=datetime(2026, 7, 14, 0, 0, tzinfo=timezone.utc),
    )

    assert report["eligible_for_explicit_review"] is True
    assert report["formal_signal"] is False
    assert report["promoted"] is False
    assert report["explicit_review_required"] is True


def test_explicit_operator_override_promotes_without_claiming_gates_passed(tmp_path) -> None:
    report = build_acceptance_report(
        tmp_path,
        policy=replace(LevelDecisionPolicy(), formal_signal_enabled=True),
        now=datetime(2026, 7, 13, 22, 0, tzinfo=timezone.utc),
    )

    assert report["formal_signal"] is True
    assert report["promoted"] is True
    assert report["promotion_basis"] == "explicit_operator_override"
    assert report["acceptance_gates_passed"] is False
