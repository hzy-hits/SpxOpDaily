from __future__ import annotations

from datetime import datetime

import pytest

from spx_spark.application.market_features.market_state_5m import (
    HIGH_VOL_CHOP,
    LOW_VOL_PIN,
    LOW_VOL_RANGE,
    TREND_DOWN,
    TREND_UP,
    UNCERTAIN,
    MarketStructure,
    OpeningRangeState,
    PriceVsVwap,
    score_market_state_5m,
)
from spx_spark.market_calendar import ET


NOW = datetime(2026, 7, 24, 10, 0, tzinfo=ET)


def inputs(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "now": NOW,
        "price_vs_vwap": PriceVsVwap.ABOVE_CONFIRMED,
        "vwap_slope": 0.31,
        "opening_range_state": OpeningRangeState.ABOVE_ORH_CONFIRMED,
        "market_structure": MarketStructure.HH_HL,
        "efficiency_ratio": 0.66,
        "vwap_cross_count": 1,
        "same_time_range_ratio": 1.0,
        "breadth_above_vwap": 0.66,
    }
    values.update(overrides)
    return values


def score(**overrides: object) -> dict[str, object]:
    return score_market_state_5m(**inputs(**overrides))


def test_maximum_and_minimum_direction_scores_are_bounded_at_ten() -> None:
    up = score()
    down = score(
        price_vs_vwap=PriceVsVwap.BELOW_CONFIRMED,
        vwap_slope=-0.31,
        opening_range_state=OpeningRangeState.BELOW_ORL_CONFIRMED,
        market_structure=MarketStructure.LH_LL,
        breadth_above_vwap=0.34,
    )

    assert up["D"] == 10
    assert up["state"] == TREND_UP
    assert down["D"] == -10
    assert down["state"] == TREND_DOWN
    assert up["actionable"] is False
    assert up["action_authority"] == "none"


@pytest.mark.parametrize(
    ("value", "expected"),
    (
        (0.3001, 2),
        (0.30, 1),
        (0.05, 1),
        (0.0499, 0),
        (-0.0499, 0),
        (-0.05, -1),
        (-0.30, -1),
        (-0.3001, -2),
    ),
)
def test_vwap_slope_score_boundaries_are_symmetric(
    value: float,
    expected: int,
) -> None:
    result = score(vwap_slope=value)

    assert result["direction_components"]["vwap_slope"] == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    (
        (0.6501, 2),
        (0.65, 1),
        (0.5501, 1),
        (0.55, 0),
        (0.45, 0),
        (0.4499, -1),
        (0.35, -1),
        (0.3499, -2),
    ),
)
def test_breadth_score_boundaries_are_symmetric(
    value: float,
    expected: int,
) -> None:
    result = score(breadth_above_vwap=value)

    assert result["direction_components"]["breadth_above_vwap"] == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    (
        (PriceVsVwap.ABOVE_CONFIRMED, 2),
        (PriceVsVwap.ABOVE, 1),
        (PriceVsVwap.AROUND_OR_CROSS, 0),
        (PriceVsVwap.BELOW, -1),
        (PriceVsVwap.BELOW_CONFIRMED, -2),
    ),
)
def test_price_vs_vwap_confirmation_is_an_explicit_score(
    value: PriceVsVwap,
    expected: int,
) -> None:
    assert score(price_vs_vwap=value)["direction_components"]["price_vs_vwap"] == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    (
        (OpeningRangeState.ABOVE_ORH_CONFIRMED, 2),
        (OpeningRangeState.BREAKOUT_ABOVE_ORH, 1),
        (OpeningRangeState.INSIDE, 0),
        (OpeningRangeState.BREAKDOWN_BELOW_ORL, -1),
        (OpeningRangeState.BELOW_ORL_CONFIRMED, -2),
    ),
)
def test_opening_range_confirmation_is_an_explicit_score(
    value: OpeningRangeState,
    expected: int,
) -> None:
    assert (
        score(opening_range_state=value)["direction_components"][
            "opening_range_state"
        ]
        == expected
    )


@pytest.mark.parametrize(
    ("value", "expected"),
    (
        (MarketStructure.HH_HL, 2),
        (MarketStructure.HH_ONLY, 1),
        (MarketStructure.HL_ONLY, 1),
        (MarketStructure.OVERLAP, 0),
        (MarketStructure.LH_ONLY, -1),
        (MarketStructure.LL_ONLY, -1),
        (MarketStructure.LH_LL, -2),
    ),
)
def test_market_structure_scores_only_the_declared_enum(
    value: MarketStructure,
    expected: int,
) -> None:
    assert score(market_structure=value)["direction_components"]["market_structure"] == expected


@pytest.mark.parametrize(
    ("efficiency", "crosses", "expected"),
    (
        (0.66, 1, "high"),
        (0.65, 2, "trend"),
        (0.45, 2, "trend"),
        (0.4499, 2, "mixed"),
        (0.25, 2, "mixed"),
        (0.2499, 0, "chop"),
        (0.80, 3, "chop"),
        (0.80, 2, "mixed"),
    ),
)
def test_q_keeps_er_and_crosses_visible_without_numeric_composite(
    efficiency: float,
    crosses: int,
    expected: str,
) -> None:
    result = score(efficiency_ratio=efficiency, vwap_cross_count=crosses)

    assert result["Q"]["quality"] == expected
    assert result["Q"]["efficiency_ratio"] == efficiency
    assert result["Q"]["vwap_cross_count"] == crosses
    assert result["Q"]["numeric_composite"] is None


@pytest.mark.parametrize(
    ("ratio", "expected"),
    (
        (0.7499, "low"),
        (0.75, "normal"),
        (1.25, "normal"),
        (1.2501, "high"),
        (1.75, "high"),
        (1.7501, "extreme"),
    ),
)
def test_v_uses_declared_same_time_range_boundaries(
    ratio: float,
    expected: str,
) -> None:
    result = score(same_time_range_ratio=ratio)

    assert result["V"]["state"] == expected
    assert result["V"]["same_time_range_ratio"] == ratio


def test_trend_requires_strict_efficiency_and_cross_gates() -> None:
    at_efficiency_boundary = score(efficiency_ratio=0.45)
    too_many_crosses = score(vwap_cross_count=3)

    assert at_efficiency_boundary["state"] == UNCERTAIN
    assert too_many_crosses["state"] == UNCERTAIN


def test_high_vol_chop_has_priority_for_low_efficiency_expansion() -> None:
    result = score(
        price_vs_vwap=PriceVsVwap.AROUND_OR_CROSS,
        vwap_slope=0.0,
        opening_range_state=OpeningRangeState.INSIDE,
        market_structure=MarketStructure.OVERLAP,
        breadth_above_vwap=0.50,
        efficiency_ratio=0.24,
        vwap_cross_count=1,
        same_time_range_ratio=1.2501,
    )

    assert result["D"] == 0
    assert result["state"] == HIGH_VOL_CHOP


def test_low_vol_range_is_neutral_low_efficiency_compression() -> None:
    result = score(
        price_vs_vwap=PriceVsVwap.AROUND_OR_CROSS,
        vwap_slope=0.0,
        opening_range_state=OpeningRangeState.INSIDE,
        market_structure=MarketStructure.OVERLAP,
        breadth_above_vwap=0.50,
        efficiency_ratio=0.24,
        vwap_cross_count=1,
        same_time_range_ratio=0.74,
    )

    assert result["state"] == LOW_VOL_RANGE
    assert result["pin_proxy_candidate"] is False


def test_pin_proxy_remains_unconfirmed_low_vol_range() -> None:
    result = score(
        price_vs_vwap=PriceVsVwap.AROUND_OR_CROSS,
        vwap_slope=0.0,
        opening_range_state=OpeningRangeState.INSIDE,
        market_structure=MarketStructure.OVERLAP,
        breadth_above_vwap=0.50,
        efficiency_ratio=0.19,
        vwap_cross_count=2,
        same_time_range_ratio=0.69,
    )

    assert result["state"] == LOW_VOL_RANGE
    assert result["state"] != LOW_VOL_PIN
    assert result["pin_proxy_candidate"] is True
    assert result["pin_confirmation"] == "proxy_unconfirmed"
    assert result["low_vol_pin_emission_allowed"] is False
    assert "low_vol_pin_proxy_unconfirmed_classified_as_range" in result["reasons"]


def test_before_0945_et_is_uncertain_but_exact_boundary_is_allowed() -> None:
    before = score(now=datetime(2026, 7, 24, 9, 44, 59, tzinfo=ET))
    boundary = score(now=datetime(2026, 7, 24, 9, 45, tzinfo=ET))

    assert before["state"] == UNCERTAIN
    assert before["status"] == "uncertain"
    assert "before_0945_et" in before["reasons"]
    assert boundary["state"] == TREND_UP


@pytest.mark.parametrize(
    "now",
    (
        datetime(2026, 7, 24, 16, 0, tzinfo=ET),
        datetime(2026, 7, 25, 10, 0, tzinfo=ET),
    ),
)
def test_outside_actual_rth_session_is_not_classified(now: datetime) -> None:
    result = score(now=now)

    assert result["state"] == UNCERTAIN
    assert "outside_rth_session" in result["reasons"]


@pytest.mark.parametrize(
    "field",
    (
        "price_vs_vwap",
        "vwap_slope",
        "opening_range_state",
        "market_structure",
        "efficiency_ratio",
        "vwap_cross_count",
        "breadth_above_vwap",
    ),
)
def test_every_directional_input_is_required_and_missing_is_fail_closed(
    field: str,
) -> None:
    result = score(**{field: None})

    assert result["state"] == UNCERTAIN
    assert result["input_availability"]["complete"] is False
    assert result["input_availability"]["available_count"] == 7
    assert result["input_availability"]["fields"][field]["available"] is False
    assert "classification_gate_failed" in result["reasons"]


def test_missing_range_allows_only_a_provisional_directional_state() -> None:
    result = score(same_time_range_ratio=None)

    assert result["state"] == TREND_UP
    assert result["status"] == "provisional"
    assert result["classification_tier"] == "directional_provisional"
    assert result["input_availability"]["complete"] is False
    assert result["input_availability"]["available_count"] == 7
    assert "classification_gate_failed" not in result["reasons"]
    assert (
        "directional_state_provisional_without_same_time_range_ratio"
        in result["reasons"]
    )
    assert result["action_authority"] == "none"
    assert result["actionable"] is False


def test_missing_range_cannot_classify_range_or_chop_state() -> None:
    result = score(
        price_vs_vwap=PriceVsVwap.AROUND_OR_CROSS,
        vwap_slope=0.0,
        opening_range_state=OpeningRangeState.INSIDE,
        market_structure=MarketStructure.OVERLAP,
        breadth_above_vwap=0.50,
        efficiency_ratio=0.24,
        vwap_cross_count=3,
        same_time_range_ratio=None,
    )

    assert result["state"] == UNCERTAIN
    assert result["status"] == "uncertain"
    assert result["classification_tier"] == "directional_provisional"
    assert "volatility_classification_gate_failed" in result["reasons"]


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("price_vs_vwap", "UP"),
        ("opening_range_state", "BREAKOUT"),
        ("market_structure", "TRENDING"),
        ("efficiency_ratio", 1.01),
        ("vwap_cross_count", 1.5),
        ("same_time_range_ratio", -0.01),
        ("breadth_above_vwap", -0.01),
        ("vwap_slope", float("nan")),
    ),
)
def test_invalid_inputs_fail_closed(
    field: str,
    value: object,
) -> None:
    result = score(**{field: value})

    assert result["state"] == UNCERTAIN
    assert result["input_availability"]["fields"][field]["available"] is False


def test_naive_time_fails_closed_and_function_is_deterministic() -> None:
    arguments = inputs(now=datetime(2026, 7, 24, 10, 0))

    first = score_market_state_5m(**arguments)
    second = score_market_state_5m(**arguments)

    assert first == second
    assert first["state"] == UNCERTAIN
    assert "now_timezone_missing" in first["reasons"]
    assert first["input_availability"]["required_count"] == 8
