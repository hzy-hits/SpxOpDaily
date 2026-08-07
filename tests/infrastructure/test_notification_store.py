from __future__ import annotations

import subprocess
import sys
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from spx_spark.infrastructure.notifications import (
    NotificationDraft,
    NotificationStatus,
    begin_attempt,
    cancel,
    create_engine,
    enqueue,
    event_rows,
    mark_transport_started,
    recover_incomplete_attempts,
    settle,
)


NOW = datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def engine(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("SPX_DATA_ROOT", str(tmp_path))
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        check=True,
        capture_output=True,
        text=True,
    )
    result = create_engine(tmp_path)
    yield result
    result.dispose()


def draft(text: str = "ready") -> NotificationDraft:
    return NotificationDraft(
        logical_event_id="ready:20260807:one",
        source="test",
        kind="trade_ready",
        lane="trade_ready",
        payload={"title": "SPX READY", "text": text},
        channels=("bark", "feishu"),
    )


def test_enqueue_is_per_target_idempotent_and_rejects_payload_collision(engine) -> None:
    first = enqueue(engine, draft(), now=NOW)
    replay = enqueue(engine, draft(), now=NOW)

    assert first.inserted == 2
    assert replay.event_ids == first.event_ids
    assert replay.duplicate == 2
    with pytest.raises(ValueError, match="idempotency collision"):
        enqueue(engine, draft("different"), now=NOW)


def test_zero_row_cancellation_is_a_fence_against_late_enqueue(engine) -> None:
    assert cancel(engine, "ready:20260807:one", reason="invalidated", now=NOW) == 0

    result = enqueue(engine, draft(), now=NOW)

    assert result.cancelled is True
    assert result.event_ids == ()


def test_processing_cannot_be_cancelled_after_transport_starts(engine) -> None:
    batch = enqueue(engine, draft(), now=NOW)
    attempt = begin_attempt(engine, batch.event_ids[0], now=NOW)
    assert attempt is not None
    mark_transport_started(engine, attempt.attempt_id)

    assert cancel(engine, draft().logical_event_id, reason="late", now=NOW) == 1
    rows = event_rows(engine, draft().logical_event_id)
    bark = next(row for row in rows if row["channel"] == "bark")
    feishu = next(row for row in rows if row["channel"] == "feishu")
    assert bark["status"] == NotificationStatus.PROCESSING.value
    assert bark["cancelled_at"] is None
    assert feishu["cancel_reason"] == "late"


def test_attempt_settlement_records_target_status(engine) -> None:
    event_id = enqueue(engine, draft(), now=NOW).event_ids[0]
    attempt = begin_attempt(engine, event_id, now=NOW)
    assert attempt is not None
    mark_transport_started(engine, attempt.attempt_id)

    settle(
        engine,
        attempt.attempt_id,
        status=NotificationStatus.DELIVERED,
        outcome="delivered",
        ok=True,
        now=NOW,
    )

    bark = next(
        row for row in event_rows(engine, draft().logical_event_id) if row["channel"] == "bark"
    )
    assert bark["status"] == NotificationStatus.DELIVERED.value


def test_expired_event_never_starts_transport(engine) -> None:
    expired = replace(
        draft(),
        expires_at=datetime(2026, 8, 7, 11, 59, tzinfo=timezone.utc),
    )
    event_id = enqueue(engine, expired, now=NOW).event_ids[0]

    assert begin_attempt(engine, event_id, now=NOW) is None
    row = next(row for row in event_rows(engine, draft().logical_event_id) if row["id"] == event_id)
    assert row["status"] == NotificationStatus.FAILED.value
    assert row["last_error"] == "expired_before_transport"


def test_crash_recovery_retries_only_when_transport_never_started(engine) -> None:
    batch = enqueue(engine, draft(), now=NOW)
    before_transport = begin_attempt(engine, batch.event_ids[0], now=NOW)
    after_transport = begin_attempt(engine, batch.event_ids[1], now=NOW)
    assert before_transport is not None and after_transport is not None
    mark_transport_started(engine, after_transport.attempt_id)

    recovery = recover_incomplete_attempts(engine, now=NOW)

    assert recovery.retry_event_ids == (before_transport.event_id,)
    assert recovery.uncertain_event_ids == (after_transport.event_id,)
    rows = {int(row["id"]): row for row in event_rows(engine, draft().logical_event_id)}
    assert rows[before_transport.event_id]["status"] == NotificationStatus.FAILED.value
    assert rows[after_transport.event_id]["status"] == NotificationStatus.UNCERTAIN.value
