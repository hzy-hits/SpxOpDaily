from __future__ import annotations

from datetime import datetime, timezone

import pytest

from spx_spark.human_focus import (
    build_human_focus_context,
    gamma_state_for_micopedia,
    micopedia_context,
)
from spx_spark.marketdata import InstrumentId, MarketDataQuality, Provider, Quote
from spx_spark.options_map import (
    ExpiryOptionsMap,
    OptionCoverage,
    OptionsMap,
    StrikeGex,
    UnderlierReference,
)
from spx_spark.storage import LatestState


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
        top_gex_strikes=(
            StrikeGex(
                strike=7550.0,
                call_gex=100.0,
                put_gex=-20.0,
                net_gex=80.0,
                abs_gex=120.0,
                call_open_interest=2500.0,
                put_open_interest=500.0,
            ),
        ),
        level_probabilities=(),
        gamma_flip_zone=(7495.0, 7505.0),
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


def test_expiry_summary_includes_gamma_profile_and_level_probabilities() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    options_map = make_options_map(gamma_state="positive_gamma_pin")
    state = LatestState(created_at=now, as_of=now, quotes=(), best_quotes=())
    context = build_human_focus_context(
        state,
        options_map=options_map,
        iv_surface=None,
        iv_surface_history_1h=None,
        window={"name": "rth"},
    )
    expiry = context["spxw_options"]["expiries"][0]
    assert "gamma_profile" in expiry
    assert expiry["gamma_profile"]["top_strikes"]
    assert "level_probabilities" in expiry


def test_micopedia_context_includes_dip_context_vix_ratio_and_event_tags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    options_map = make_options_map(gamma_state="mixed_gamma")
    vix1d = Quote(
        instrument=InstrumentId.index("VIX1D"),
        provider=Provider.IBKR,
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=19.0,
    )
    vix = Quote(
        instrument=InstrumentId.index("VIX"),
        provider=Provider.IBKR,
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=20.0,
    )
    state = LatestState(
        created_at=now,
        as_of=now,
        quotes=(vix1d, vix),
        best_quotes=(vix1d, vix),
    )
    monkeypatch.setenv("MICOPEDIA_EVENT_TAGS", "fomc")
    context = micopedia_context(state, options_map=options_map, window={"name": "rth"})
    assert context["dip_context"]
    assert context["vix_ratio"] == 0.95
    assert context["regime"] == "high_vol_event"
