from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone

import pytest

from spx_spark.data_platform.contracts import (
    CompactionManifestRecord,
    DecisionRecord,
    EventRecord,
    LandingWriteReceipt,
)
from spx_spark.data_platform.ids import deterministic_id, make_event_key


NOW = datetime(2026, 7, 10, 14, 30, tzinfo=timezone.utc)


def test_deterministic_ids_canonicalize_timezone_and_mapping_order() -> None:
    eastern = timezone(timedelta(hours=-4))
    first = deterministic_id("event", {"b": 2, "a": 1}, NOW)
    second = deterministic_id(
        "event",
        {"a": 1, "b": 2},
        NOW.astimezone(eastern),
    )

    assert first == second
    assert first.startswith("event_")
    assert len(first.rsplit("_", 1)[1]) == 32
    assert make_event_key("shock", NOW, "down") == make_event_key("shock", NOW, "down")


def test_event_rejects_naive_or_impossible_availability_clock() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        EventRecord(
            event_key="evt",
            event_type="shock",
            session_date=date(2026, 7, 10),
            source_at=NOW.replace(tzinfo=None),
            available_at=NOW,
        )

    with pytest.raises(ValueError, match="cannot precede"):
        EventRecord(
            event_key="evt",
            event_type="shock",
            session_date=date(2026, 7, 10),
            source_at=NOW,
            available_at=NOW - timedelta(seconds=1),
        )


def test_decision_contract_enforces_available_at_before_decision_at() -> None:
    decision = DecisionRecord(
        decision_id="dec",
        strategy_name="flip_reclaim_call",
        strategy_version="v1",
        decision_at=NOW,
        available_at=NOW,
        status="candidate",
        action="alert",
        side="call",
    )

    with pytest.raises(ValueError, match="not yet available"):
        replace(decision, available_at=NOW + timedelta(microseconds=1))


def test_compaction_contract_rejects_invalid_coverage() -> None:
    with pytest.raises(ValueError, match="cannot precede"):
        CompactionManifestRecord(
            source_path="raw/hour=10/quotes.jsonl",
            source_sha256="a" * 64,
            source_size=10,
            source_mtime_ns=1,
            output_path="lake/hour=10/part.parquet",
            output_sha256="b" * 64,
            row_count=1,
            min_received_at=NOW,
            max_received_at=NOW - timedelta(seconds=1),
            schema_version="v1",
            writer_version="test",
            completed_at=NOW,
        )


def test_landing_receipt_reconciles_batch_and_partition_counts() -> None:
    assert LandingWriteReceipt(row_count=3, path_counts={"a": 1, "b": 2}).row_count == 3
    with pytest.raises(ValueError, match="sum"):
        LandingWriteReceipt(row_count=4, path_counts={"a": 1, "b": 2})


def test_empty_compaction_manifest_does_not_require_an_output_file() -> None:
    manifest = CompactionManifestRecord(
        source_path="raw/hour=10/quotes.jsonl",
        source_sha256="a" * 64,
        source_size=0,
        source_mtime_ns=1,
        output_path=None,
        output_sha256=None,
        row_count=0,
        min_received_at=None,
        max_received_at=None,
        schema_version="v1",
        writer_version="test",
        completed_at=NOW,
        status="empty",
    )
    assert manifest.output_path is None
