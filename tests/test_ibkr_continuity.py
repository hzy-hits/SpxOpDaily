from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from spx_spark.ibkr.continuity import advance_continuity, record_continuity


def _health(now: datetime, *, healthy: bool = True) -> dict[str, object]:
    return {
        "observed_at": now.isoformat(),
        "max_age_seconds": 90.0,
        "policy_blocked": False,
        "circuit_state": "closed",
        "conflict_count": 0,
        "connected": True,
        "data_plane_healthy": healthy,
    }


def test_continuity_records_gap_and_rearm(tmp_path) -> None:
    now = datetime(2026, 10, 16, 14, 0, tzinfo=timezone.utc)
    previous = None
    healthy = _health(now)
    state, event = advance_continuity(previous, healthy, now=now)
    assert state.state == "healthy"
    assert event is None
    previous = state.to_dict()

    gap_at = now + timedelta(seconds=10)
    state, event = advance_continuity(previous, _health(gap_at, healthy=False), now=gap_at)
    assert state.state == "gap"
    assert event is not None and event["event"] == "gap_start"
    previous = state.to_dict()

    for step in range(1, 4):
        recovered_at = gap_at + timedelta(seconds=10 * step)
        state, event = advance_continuity(previous, _health(recovered_at), now=recovered_at)
        assert event is not None
        assert event["event"] == ("recovering" if step < 3 else "gap_end")
        previous = state.to_dict()
    assert state.state == "healthy"
    assert event is not None and event["gap_seconds"] == 30.0

    path = tmp_path / "latest" / "ibkr_continuity.json"
    gap_at = now + timedelta(seconds=60)
    record_continuity(tmp_path, _health(gap_at, healthy=False), now=gap_at)
    assert json.loads(path.read_text())["state"] == "gap"
    for step in range(1, 4):
        recovered_at = gap_at + timedelta(seconds=10 * step)
        record_continuity(tmp_path, _health(recovered_at), now=recovered_at)
    assert json.loads(path.read_text())["state"] == "healthy"
    ledger = tmp_path / "audit" / "ibkr_continuity.jsonl"
    events = [json.loads(line)["event"] for line in ledger.read_text().splitlines()]
    assert events[0] == "gap_start"
    assert events[-1] == "gap_end"


def test_recovery_requires_distinct_fresh_samples_and_cannot_escape_through_suspect():
    now = datetime(2026, 10, 16, 14, 0, tzinfo=timezone.utc)
    gap, _ = advance_continuity(None, _health(now, healthy=False), now=now)
    at = now + timedelta(seconds=1)
    recovering, _ = advance_continuity(gap.to_dict(), _health(at), now=at)
    for _ in range(3):
        recovering, _ = advance_continuity(recovering.to_dict(), _health(at), now=at)
    assert recovering.state == "recovering"
    assert recovering.healthy_samples == 1
    aged_at = at + timedelta(seconds=21)
    suspect, _ = advance_continuity(recovering.to_dict(), _health(at), now=aged_at)
    assert suspect.state == "gap"
    next_state, _ = advance_continuity(suspect.to_dict(), _health(aged_at), now=aged_at)
    assert next_state.state == "recovering"
    assert next_state.healthy_samples == 1


def test_silent_gap_requires_three_new_observations_and_records_recovery(tmp_path):
    now = datetime(2026, 10, 16, 14, tzinfo=timezone.utc)
    record_continuity(tmp_path, _health(now), now=now)
    heartbeat = now + timedelta(seconds=2)
    record_continuity(tmp_path, _health(heartbeat), now=heartbeat)
    path = tmp_path / "latest" / "ibkr_continuity.json"
    assert json.loads(path.read_text())["last_good_at"] == heartbeat.isoformat()
    assert not (tmp_path / "audit" / "ibkr_continuity.jsonl").exists()

    resumed = heartbeat + timedelta(minutes=5)
    for sample in range(3):
        at = resumed + timedelta(seconds=2 * sample)
        state = record_continuity(tmp_path, _health(at), now=at)
        assert state.state == ("healthy" if sample == 2 else "recovering")
        assert state.healthy_samples == sample + 1
        duplicate = record_continuity(tmp_path, _health(at), now=at)
        assert duplicate.state == state.state
        assert duplicate.healthy_samples == state.healthy_samples
    events = [json.loads(row) for row in (tmp_path / "audit" / "ibkr_continuity.jsonl").read_text().splitlines()]
    assert [row["event"] for row in events] == ["gap_start", "recovering", "gap_end"]
    assert events[0]["reason"] == "health_observation_gap"
    assert events[-1]["gap_start"] == heartbeat.isoformat()
    assert events[-1]["gap_seconds"] == 304


@pytest.mark.parametrize("pause_seconds,expected", [(90, "healthy"), (91, "recovering")])
def test_silent_gap_uses_the_health_expiry_boundary(pause_seconds, expected):
    now = datetime(2026, 10, 16, 14, tzinfo=timezone.utc)
    previous, _ = advance_continuity(None, _health(now), now=now)
    at = now + timedelta(seconds=pause_seconds)
    state, _ = advance_continuity(previous.to_dict(), _health(at), now=at)
    assert state.state == expected


def test_another_silent_gap_restarts_partial_recovery():
    now = datetime(2026, 10, 16, 14, tzinfo=timezone.utc)
    gap, _ = advance_continuity(None, _health(now, healthy=False), now=now)
    at = now + timedelta(seconds=2)
    recovering, _ = advance_continuity(gap.to_dict(), _health(at), now=at)
    at += timedelta(minutes=5)
    state, _ = advance_continuity(recovering.to_dict(), _health(at), now=at)
    assert state.state == "recovering"
    assert state.healthy_samples == 1
