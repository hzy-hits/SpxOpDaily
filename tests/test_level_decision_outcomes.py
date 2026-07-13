from __future__ import annotations

from datetime import datetime, timedelta, timezone

from spx_spark.application.order_map.level_decision_outcomes import (
    advance_level_outcomes,
)


NOW = datetime(2026, 7, 13, 14, 30, tzinfo=timezone.utc)
DECISION = {
    "event_id": "level:test",
    "phase": "confirmed",
    "level_kind": "call_wall",
    "level": 6000.0,
    "thesis": "breakout",
    "direction": "up",
}


def test_confirmed_decision_records_short_horizon_follow_through() -> None:
    state, rows = advance_level_outcomes(
        None,
        decision=DECISION,
        spot=6000.0,
        at=NOW,
        confirmed_now=True,
    )
    assert not rows
    state, rows = advance_level_outcomes(
        state,
        decision=DECISION,
        spot=6003.0,
        at=NOW + timedelta(seconds=30),
        confirmed_now=False,
    )
    assert len(rows) == 1
    assert rows[0]["horizon_seconds"] == 30
    assert rows[0]["attribution"] == "follow_through"
    assert rows[0]["mfe_bps"] == 5.0


def test_adverse_path_is_attributed_as_false_confirmation() -> None:
    state, _ = advance_level_outcomes(
        None,
        decision=DECISION,
        spot=6000.0,
        at=NOW,
        confirmed_now=True,
    )
    state, _ = advance_level_outcomes(
        state,
        decision=DECISION,
        spot=6002.0,
        at=NOW + timedelta(seconds=10),
        confirmed_now=False,
    )
    _state, rows = advance_level_outcomes(
        state,
        decision=DECISION,
        spot=5996.0,
        at=NOW + timedelta(seconds=30),
        confirmed_now=False,
    )
    assert rows[0]["attribution"] == "false_confirmation"
    assert rows[0]["mae_bps"] < -5.0


def test_unconfirmed_decision_does_not_create_outcome_observation() -> None:
    state, rows = advance_level_outcomes(
        None,
        decision={"phase": "testing"},
        spot=6000.0,
        at=NOW,
        confirmed_now=False,
    )
    assert rows == ()
    assert state["observations"] == {}
