from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

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
