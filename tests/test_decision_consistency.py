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
