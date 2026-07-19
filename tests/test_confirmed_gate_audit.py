from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from spx_spark.application.market_features.confirmed_gate_audit import (
    reconcile_confirmed_gate,
)


UTC = timezone.utc
NOW = datetime(2026, 7, 15, 17, 49, 27, tzinfo=UTC)


def test_trade_ready_finalizes_confirmed_event_once(tmp_path) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    level = _confirmed("level:up")
    intent = {
        **_contract(),
        "status": "trade_ready",
        "event_id": "level:up",
        "intent_id": "intent:up",
        "contract_id": "option:SPX:SPXW:20260715:7560:C",
    }

    first = reconcile_confirmed_gate(storage, level, intent, now=NOW)
    second = reconcile_confirmed_gate(storage, level, intent, now=NOW + timedelta(seconds=1))

    assert first["status"] == "trade_ready"
    assert first["schema_version"] == 3
    assert first["valid_until"] == (NOW + timedelta(minutes=3)).isoformat()
    assert first["coordinate"]["kind"] == "official_spx"
    assert first["block_reasons"] == []
    assert first["terminal"] is True
    assert second["status"] == "trade_ready"
    rows = _rows(tmp_path)
    assert len(rows) == 1
    assert rows[0]["event_id"] == "level:up"


def test_blocked_confirmation_finalizes_when_level_expires(tmp_path) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    level = _confirmed("level:down")
    pending = reconcile_confirmed_gate(
        storage,
        level,
        {
            **_contract(),
            "status": "blocked",
            "event_id": "level:down",
            "block_reasons": ["remaining_target_room_insufficient"],
        },
        now=NOW,
    )
    expired = reconcile_confirmed_gate(
        storage,
        {**level, "phase": "expired"},
        {"status": "observing"},
        now=NOW + timedelta(minutes=1),
    )

    assert pending["status"] == "pending"
    assert expired["status"] == "blocked"
    assert expired["block_reasons"] == [
        "remaining_target_room_insufficient",
        "confirmed_event_expired",
    ]
    assert len(_rows(tmp_path)) == 1


def test_projection_gap_is_preserved_as_final_block_reason(tmp_path) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    reconcile_confirmed_gate(
        storage,
        _confirmed("level:missed"),
        {**_contract(), "status": "observing", "event_id": None},
        now=NOW,
    )
    result = reconcile_confirmed_gate(
        storage,
        {"phase": "far", "event_id": "level:next"},
        {"status": "observing"},
        now=NOW + timedelta(minutes=1),
    )

    assert result["status"] == "blocked"
    assert "confirmed_event_missing_from_trade_evaluation" in result["block_reasons"]
    assert "confirmed_event_superseded_before_trade_ready" in result["block_reasons"]


def _confirmed(event_id: str) -> dict[str, object]:
    return {
        "event_id": event_id,
        "phase": "confirmed",
        "phase_at": NOW.isoformat(),
        "direction": "up",
        "thesis": "breakout",
        "level_kind": "call_wall",
        "level": 7560.0,
        "expiry": "20260715",
        "expires_at": (NOW + timedelta(minutes=3)).isoformat(),
        "trigger_coordinate": _contract()["coordinate"],
    }


def _contract() -> dict[str, object]:
    return {
        "schema_version": 3,
        "policy_version": "rth_trade_intent.v3+sha256:test",
        "valid_until": (NOW + timedelta(minutes=3)).isoformat(),
        "coordinate": {
            "kind": "official_spx",
            "instrument_id": "index:SPX",
            "observed_value": 7561.0,
            "target_value": 7560.0,
            "spx_observed_value": 7561.0,
            "basis_points": 0.0,
            "as_of": NOW.isoformat(),
        },
        "block_reasons": [],
    }


def _rows(tmp_path) -> list[dict[str, object]]:
    path = tmp_path / "features" / "confirmed_gate_results" / "date=2026-07-15" / "events.jsonl"
    return [json.loads(line) for line in path.read_text().splitlines()]
