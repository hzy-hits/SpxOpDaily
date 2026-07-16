from spx_spark.application.order_map.decision_consistency import (
    coherent_level_decision,
    decision_context_matches_frames,
)


def test_level_decision_fails_closed_on_expiry_rollover() -> None:
    decision = {
        "expiry": "20260714",
        "phase": "confirmed",
        "thesis": "breakout",
        "direction": "down",
        "event_id": "old-event",
        "level_kind": "put_wall",
        "level": 7500.0,
        "formal_signal": True,
    }

    result = coherent_level_decision(
        decision,
        expiry="20260715",
        structure={"put_wall": 7525.0, "call_wall": 7600.0},
        max_level_drift_points=2.5,
    )

    assert result["phase"] == "far"
    assert result["formal_signal"] is False
    assert result["snapshot_consistent"] is False
    assert result["quality_reason"] == "decision_snapshot_expiry_mismatch"


def test_level_decision_fails_closed_when_promoted_wall_moves() -> None:
    result = coherent_level_decision(
        {
            "expiry": "20260715",
            "phase": "testing",
            "level_kind": "call_wall",
            "level": 7575.0,
        },
        expiry="20260715",
        structure={"put_wall": 7500.0, "call_wall": 7600.0},
        max_level_drift_points=2.5,
    )

    assert result["phase"] == "far"
    assert result["quality_reason"] == "decision_snapshot_level_drift"


def test_stable_level_decision_uses_its_frozen_structure_not_transient_live_wall() -> None:
    result = coherent_level_decision(
        {
            "expiry": "20260715",
            "phase": "confirmed",
            "thesis": "breakout",
            "direction": "up",
            "event_id": "stable-event",
            "level_kind": "call_wall",
            "level": 7560.0,
            "levels": {
                "put_wall": 7550.0,
                "flip_low": 7550.0,
                "flip_high": 7555.0,
                "call_wall": 7560.0,
            },
            "level_source": "stable_15m_oi_gex",
            "formal_signal": True,
        },
        expiry="20260715",
        structure={"put_wall": 7550.0, "call_wall": 7600.0},
        max_level_drift_points=2.5,
    )

    assert result["phase"] == "confirmed"
    assert result["event_id"] == "stable-event"
    assert result["snapshot_consistent"] is True
    assert result["snapshot_validation_source"] == "decision_frozen_structure"


def test_stable_level_decision_still_fails_closed_on_expiry_rollover() -> None:
    result = coherent_level_decision(
        {
            "expiry": "20260715",
            "phase": "confirmed",
            "level_kind": "call_wall",
            "level": 7560.0,
            "levels": {"call_wall": 7560.0},
            "level_source": "stable_15m_oi_gex",
            "formal_signal": True,
        },
        expiry="20260716",
        structure={"call_wall": 7560.0},
        max_level_drift_points=2.5,
    )

    assert result["phase"] == "far"
    assert result["quality_reason"] == "decision_snapshot_expiry_mismatch"


def test_level_decision_retains_frozen_context_when_current_structure_is_missing() -> None:
    levels = {
        "put_wall": 7550.0,
        "flip_low": 7545.0,
        "flip_high": 7550.0,
        "call_wall": 7600.0,
    }
    result = coherent_level_decision(
        {
            "expiry": "20260715",
            "phase": "testing",
            "level_kind": "put_wall",
            "level": 7550.0,
            "levels": levels,
            "level_bands": {"put_wall": {"low": 7547.5, "high": 7552.5}},
            "formal_signal": True,
        },
        expiry="20260715",
        structure={},
        max_level_drift_points=2.5,
    )

    assert result["phase"] == "far"
    assert result["formal_signal"] is False
    assert result["levels"] == levels
    assert result["expiry"] == "20260715"
    assert result["level_source"] == "frozen_last_usable_structure"
    assert result["quality_reason"] == "decision_snapshot_structure_unavailable"
    assert result["snapshot_consistent"] is False


def test_decision_context_must_reference_rendered_frames() -> None:
    market = {"frame_id": "market:1"}
    option = {"frame_id": "options:1"}
    context = {"market_frame_id": "market:1", "option_frame_id": "options:1"}

    assert decision_context_matches_frames(
        context,
        market_frame=market,
        option_frame=option,
    )
    assert not decision_context_matches_frames(
        {**context, "option_frame_id": "options:old"},
        market_frame=market,
        option_frame=option,
    )
