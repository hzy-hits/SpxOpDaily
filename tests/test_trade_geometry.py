from spx_spark.application.market_features.trade_geometry import (
    confirmation_geometry,
)
from spx_spark.settings.level_decision import LevelDecisionPolicy
from spx_spark.settings.market_features import MarketFeatureSettings


def test_geometry_skips_adjacent_wall_that_confirmation_would_consume() -> None:
    geometry = confirmation_geometry(
        trigger_level=7425.0,
        direction=1,
        thesis="breakout",
        walls=[
            {"strike": 7430.0, "gex": 500.0, "open_interest": 1000.0},
            {"strike": 7435.0, "gex": 400.0, "open_interest": 800.0},
        ],
        expected_move_points=40.0,
        feature_policy=MarketFeatureSettings(),
        level_policy=LevelDecisionPolicy(),
    )

    assert geometry.required_target_distance_points == 8.0
    assert geometry.target_spx == 7435.0
    assert geometry.target_source == "gex_oi_wall_ladder"
    assert geometry.feasible is True


def test_fade_geometry_reserves_rejection_distance_before_target_room() -> None:
    geometry = confirmation_geometry(
        trigger_level=7425.0,
        direction=-1,
        thesis="fade",
        walls=[
            {"strike": 7420.0, "gex": 900.0, "open_interest": 1200.0},
            {"strike": 7415.0, "gex": 700.0, "open_interest": 900.0},
        ],
        expected_move_points=40.0,
        feature_policy=MarketFeatureSettings(),
        level_policy=LevelDecisionPolicy(),
    )

    assert geometry.required_target_distance_points == 10.0
    assert geometry.target_spx == 7415.0
    assert geometry.feasible is True


def test_missing_intraday_oi_uses_geometry_floored_expected_move_fallback() -> None:
    geometry = confirmation_geometry(
        trigger_level=7425.0,
        direction=-1,
        thesis="breakout",
        walls=[],
        expected_move_points=40.0,
        feature_policy=MarketFeatureSettings(),
        level_policy=LevelDecisionPolicy(),
    )

    assert geometry.target_spx == 7417.0
    assert geometry.target_source == "expected_move_confirmation_floor_fallback"
    assert geometry.feasible is True
