from __future__ import annotations

from datetime import datetime, timedelta, timezone

from spx_spark.application.order_map.level_decision_outcomes import (
    LevelOutcomeSettings,
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


def test_thirty_minute_outcome_keeps_the_full_path_for_mfe_and_mae() -> None:
    settings = LevelOutcomeSettings(horizons_seconds=(1800,))
    state, _ = advance_level_outcomes(
        None,
        decision=DECISION,
        spot=6000.0,
        at=NOW,
        confirmed_now=True,
        settings=settings,
    )
    for seconds in range(5, 1801, 5):
        spot = 6012.0 if seconds == 600 else 5997.0 if seconds == 1200 else 6001.0
        state, rows = advance_level_outcomes(
            state,
            decision=DECISION,
            spot=spot,
            at=NOW + timedelta(seconds=seconds),
            confirmed_now=False,
            settings=settings,
        )

    assert len(rows) == 1
    assert rows[0]["horizon_seconds"] == 1800
    assert rows[0]["mfe_bps"] == 20.0
    assert rows[0]["mae_bps"] == -5.0
