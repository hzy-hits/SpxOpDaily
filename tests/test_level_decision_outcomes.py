from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

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
    "trigger_coordinate_kind": "official_spx",
    "trigger_instrument_id": "index:SPX",
    "trigger_basis_points": 26.32,
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


def test_chain_implied_event_normalizes_raw_es_with_latched_basis() -> None:
    decision = {
        **DECISION,
        "event_id": "level:chain-to-es",
        "trigger_coordinate_kind": "chain_implied_spx",
        "trigger_instrument_id": "synthetic:SPXW_PARITY",
        "trigger_basis_points": 26.32,
    }
    settings = LevelOutcomeSettings(horizons_seconds=(900,))
    state, _ = advance_level_outcomes(
        None,
        decision=decision,
        spot=7734.3,
        at=NOW,
        confirmed_now=True,
        settings=settings,
    )
    state, rows = advance_level_outcomes(
        state,
        decision=decision,
        spot=7750.875,
        at=NOW + timedelta(seconds=900),
        confirmed_now=False,
        trigger_coordinate_kind="raw_es",
        trigger_instrument_id="future:ES",
        trigger_basis_points=25.0,
        settings=settings,
    )

    observation = state["observations"][decision["event_id"]]
    assert observation["latched_basis_points"] == 26.32
    assert observation["samples"][-1]["source_spot"] == 7750.875
    assert observation["samples"][-1]["spot"] == pytest.approx(7724.555)
    assert observation["samples"][-1]["normalization"] == (
        "subtract_latched_es_spx_basis"
    )
    assert rows[0]["return_bps"] < 0
    assert rows[0]["return_bps"] != pytest.approx(21.43, abs=0.01)
    assert rows[0]["attribution"] != "follow_through"


def test_chain_implied_event_accepts_official_spx_as_same_spx_coordinate() -> None:
    decision = {
        **DECISION,
        "event_id": "level:chain-to-official",
        "trigger_coordinate_kind": "chain_implied_spx",
        "trigger_instrument_id": "synthetic:SPXW_PARITY",
    }
    settings = LevelOutcomeSettings(horizons_seconds=(30,))
    state, _ = advance_level_outcomes(
        None,
        decision=decision,
        spot=7734.3,
        at=NOW,
        confirmed_now=True,
        settings=settings,
    )
    state, rows = advance_level_outcomes(
        state,
        decision=decision,
        spot=7735.3,
        at=NOW + timedelta(seconds=30),
        confirmed_now=False,
        trigger_coordinate_kind="official_spx",
        trigger_instrument_id="index:SPX",
        settings=settings,
    )

    sample = state["observations"][decision["event_id"]]["samples"][-1]
    assert sample["spot"] == 7735.3
    assert sample["trigger_coordinate_kind"] == "official_spx"
    assert sample["normalization"] == "identity_spx_coordinate"
    assert rows[0]["status"] == "complete"


def test_chain_implied_event_skips_raw_es_when_latched_basis_is_unavailable() -> None:
    decision = {
        **DECISION,
        "event_id": "level:chain-to-es-without-basis",
        "trigger_coordinate_kind": "chain_implied_spx",
        "trigger_instrument_id": "synthetic:SPXW_PARITY",
        "trigger_basis_points": None,
    }
    settings = LevelOutcomeSettings(horizons_seconds=(30,), sample_tolerance_seconds=60.0)
    state, _ = advance_level_outcomes(
        None,
        decision=decision,
        spot=7734.3,
        at=NOW,
        confirmed_now=True,
        settings=settings,
    )
    state, rows = advance_level_outcomes(
        state,
        decision=decision,
        spot=7750.875,
        at=NOW + timedelta(seconds=30),
        confirmed_now=False,
        trigger_coordinate_kind="raw_es",
        trigger_instrument_id="future:ES",
        settings=settings,
    )

    observation = state["observations"][decision["event_id"]]
    assert len(observation["samples"]) == 1
    assert observation["last_sample_skip_reason"] == (
        "latched_basis_unavailable_for_es_sample"
    )
    assert rows[0]["status"] == "incomplete"
    assert rows[0]["return_bps"] is None
    assert rows[0]["data_quality_reason"] == "latched_basis_unavailable_for_es_sample"


def test_coordinate_kind_instrument_mismatch_is_skipped_and_censored() -> None:
    settings = LevelOutcomeSettings(horizons_seconds=(30,), sample_tolerance_seconds=60.0)
    state, _ = advance_level_outcomes(
        None,
        decision=DECISION,
        spot=6000.0,
        at=NOW,
        confirmed_now=True,
        settings=settings,
    )
    state, rows = advance_level_outcomes(
        state,
        decision=DECISION,
        spot=6040.0,
        at=NOW + timedelta(seconds=30),
        confirmed_now=False,
        trigger_coordinate_kind="official_spx",
        trigger_instrument_id="future:ES",
        settings=settings,
    )

    observation = state["observations"][DECISION["event_id"]]
    assert len(observation["samples"]) == 1
    assert observation["last_sample_skip_reason"] == "trigger_coordinate_instrument_mismatch"
    assert rows[0]["status"] == "incomplete"
    assert rows[0]["return_bps"] is None
    assert rows[0]["attribution"] == "data_incomplete"
    assert rows[0]["data_quality_reason"] == "trigger_coordinate_instrument_mismatch"


def test_target_near_coordinate_skip_survives_worker_gap_and_censors_horizon() -> None:
    settings = LevelOutcomeSettings(horizons_seconds=(30,), sample_tolerance_seconds=20.0)
    state, _ = advance_level_outcomes(
        None,
        decision=DECISION,
        spot=6000.0,
        at=NOW,
        confirmed_now=True,
        settings=settings,
    )
    state, _ = advance_level_outcomes(
        state,
        decision=DECISION,
        spot=6002.0,
        at=NOW + timedelta(seconds=20),
        confirmed_now=False,
        settings=settings,
    )
    state, _ = advance_level_outcomes(
        state,
        decision=DECISION,
        spot=6004.0,
        at=NOW + timedelta(seconds=29),
        confirmed_now=False,
        trigger_coordinate_kind="official_spx",
        trigger_instrument_id="future:ES",
        settings=settings,
    )
    state, rows = advance_level_outcomes(
        state,
        decision=DECISION,
        spot=6005.0,
        at=NOW + timedelta(seconds=100),
        confirmed_now=False,
        trigger_coordinate_kind="unknown_coordinate",
        trigger_instrument_id="unknown:instrument",
        settings=settings,
    )

    observation = state["observations"][DECISION["event_id"]]
    assert [row["reason"] for row in observation["coordinate_skips"]] == [
        "trigger_coordinate_instrument_mismatch",
        "trigger_coordinate_kind_unverifiable",
    ]
    assert observation["last_sample_skip_reason"] == (
        "trigger_coordinate_kind_unverifiable"
    )
    assert rows[0]["status"] == "incomplete"
    assert rows[0]["return_bps"] is None
    assert rows[0]["coordinate_skip_distance_seconds"] == 1.0
    assert rows[0]["data_quality_reason"] == "trigger_coordinate_instrument_mismatch"


def test_legacy_observation_never_appends_or_labels_unverified_coordinate() -> None:
    target = NOW + timedelta(seconds=30)
    legacy = {
        "schema_version": 1,
        "observations": {
            "level:legacy": {
                "event_id": "level:legacy",
                "level_kind": "call_wall",
                "level": 7730.0,
                "thesis": "breakout",
                "direction": "up",
                "confirmed_at": NOW.isoformat(),
                "start_spot": 7734.3,
                "samples": [{"at": NOW.isoformat(), "spot": 7734.3}],
                "horizons": {
                    "30": {
                        "seconds": 30,
                        "target_at": target.isoformat(),
                        "status": "pending",
                    }
                },
            }
        },
    }
    state, rows = advance_level_outcomes(
        legacy,
        decision={**DECISION, "event_id": "another-event"},
        spot=7750.875,
        at=target,
        confirmed_now=False,
        trigger_coordinate_kind="raw_es",
        trigger_instrument_id="future:ES",
        trigger_basis_points=26.32,
    )

    observation = state["observations"]["level:legacy"]
    assert observation["coordinate_status"] == "legacy_unverifiable"
    assert observation["legacy_samples_ignored"] == 1
    assert len(observation["samples"]) == 1
    assert rows[0]["status"] == "incomplete"
    assert rows[0]["return_bps"] is None
    assert rows[0]["attribution"] == "data_incomplete"
    assert rows[0]["data_quality_reason"] == "legacy_coordinate_contract_missing"
