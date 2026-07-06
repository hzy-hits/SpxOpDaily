from __future__ import annotations

from datetime import datetime, timezone

import pytest

from spx_spark.human_focus import gamma_state_for_micopedia
from spx_spark.options_map import (
    ExpiryOptionsMap,
    OptionCoverage,
    OptionsMap,
    UnderlierReference,
)


def default_coverage() -> OptionCoverage:
    return OptionCoverage(
        total=10,
        live=10,
        stale=0,
        delayed=0,
        unknown_age=0,
        max_age_ms=100.0,
        with_bid_ask=10,
        with_mid=10,
        with_iv=10,
        with_delta=10,
        with_gamma=10,
        with_theta=10,
        with_vega=10,
        with_open_interest=10,
        avg_spread_bps=50.0,
    )


def make_options_map(*, gamma_state: str) -> OptionsMap:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    expiry = ExpiryOptionsMap(
        expiry="20260706",
        option_count=10,
        strike_count=5,
        atm_strike=7500.0,
        atm_call_mid=10.0,
        atm_put_mid=11.0,
        atm_straddle_mid=21.0,
        expected_move_points=50.0,
        expected_move_pct=0.67,
        atm_iv=0.20,
        put_wing_iv=0.24,
        call_wing_iv=0.19,
        put_skew_ratio=1.1,
        call_skew_ratio=0.95,
        net_gex=1000.0,
        abs_gex=5000.0,
        net_gamma_ratio=0.1,
        zero_gamma=7505.0,
        zero_gamma_distance_points=5.0,
        call_wall=7550.0,
        put_wall=7450.0,
        nearest_wall=7550.0,
        nearest_wall_distance_points=50.0,
        gamma_state=gamma_state,
        gex_quality="ok",
        coverage=default_coverage(),
        top_gex_strikes=(),
        warnings=(),
    )
    return OptionsMap(
        created_at=now,
        as_of=now,
        underlier=UnderlierReference(price=7500.0, source="index:SPX"),
        expiries=(expiry,),
        warnings=(),
    )


@pytest.mark.parametrize(
    ("raw_gamma_state", "expected"),
    [
        ("positive_gamma_pin", "pin"),
        ("zero_gamma_transition", "transition"),
        ("negative_gamma_acceleration", "negative"),
        ("mixed_gamma", "unknown"),
        ("unknown", "unknown"),
        ("unknown_no_open_interest", "unknown"),
        ("unknown_underlier_mismatch", "unknown"),
    ],
)
def test_gamma_state_for_micopedia_mapping(raw_gamma_state: str, expected: str) -> None:
    options_map = make_options_map(gamma_state=raw_gamma_state)
    assert gamma_state_for_micopedia(options_map) == expected


def test_gamma_state_for_micopedia_empty_expiries_returns_unknown() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    options_map = OptionsMap(
        created_at=now,
        as_of=now,
        underlier=UnderlierReference(price=7500.0, source="index:SPX"),
        expiries=(),
        warnings=(),
    )
    assert gamma_state_for_micopedia(options_map) == "unknown"
