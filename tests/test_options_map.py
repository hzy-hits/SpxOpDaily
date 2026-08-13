from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from spx_spark.analytics.options.density import (
    build_strike_differential_context,
    summarize_strike_surface_shape,
    synthetic_call_curve,
)
from spx_spark.analytics.options.models import SyntheticCallPoint
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    OptionGreeks,
    OptionRight,
    Provider,
    Quote,
)
from spx_spark.options_map import (
    StrikeGex,
    bs_gamma,
    build_gex_by_strike,
    build_options_map,
    build_expiry_map,
    build_rn_density,
    build_wall_ladder,
    gex_weight,
    group_spxw_option_quotes,
    interpolated_atm_iv,
    pair_by_strike,
    select_underlier,
    signed_gex,
    structure_quality_ok,
    time_to_expiry_years,
    wing_iv_at_delta,
    zero_gamma_spot_scan,
)
from spx_spark.storage import LatestState


def make_option(
    *,
    expiry: str,
    strike: float,
    right: str,
    mark: float,
    iv: float,
    gamma: float,
    open_interest: float | None,
    now: datetime,
) -> Quote:
    return Quote(
        instrument=InstrumentId.option(
            "SPX",
            expiry=expiry,
            strike=strike,
            right=right,
            trading_class="SPXW",
        ),
        provider=Provider.IBKR,
        provider_symbol=f"SPXW:{expiry}:{strike}:{right}",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        bid=mark - 0.1,
        ask=mark + 0.1,
        mark=mark,
        open_interest=open_interest,
        quote_time=now,
        greeks=OptionGreeks(
            implied_vol=iv,
            delta=0.5 if right == "C" else -0.5,
            gamma=gamma,
            theta=-1.0,
            vega=0.3,
            model="test",
        ),
    )


def test_time_to_expiry_uses_early_close_session() -> None:
    as_of = datetime(2026, 11, 27, 17, 0, tzinfo=timezone.utc)  # 12:00 ET

    years = time_to_expiry_years("20261127", as_of=as_of)

    assert years == pytest.approx(1.0 / (365.0 * 24.0))


def test_underlier_last_never_uses_quote_clock_and_falls_back_to_mid() -> None:
    now = datetime(2026, 7, 27, 14, 0, tzinfo=timezone.utc)
    quote = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.SCHWAB,
        received_at=now,
        last_update_at=now,
        quote_time=now,
        trade_time=None,
        quality=MarketDataQuality.LIVE,
        last=7000.0,
        bid=5999.0,
        ask=6001.0,
    )
    state = LatestState(now, now, (quote,), (quote,))

    reference = select_underlier(state)

    assert reference.price == 6000.0
    assert reference.price_kind == "mid"


@pytest.mark.parametrize("minutes", (16, 15, 5, 1))
def test_time_to_expiry_uses_actual_final_minutes(minutes: int) -> None:
    close = datetime(2026, 7, 6, 20, 0, tzinfo=timezone.utc)

    years = time_to_expiry_years(
        "20260706",
        as_of=close - timedelta(minutes=minutes),
    )

    assert years == pytest.approx(minutes / (365.0 * 24.0 * 60.0))


def test_time_to_expiry_is_zero_at_and_after_close() -> None:
    close = datetime(2026, 7, 6, 20, 0, tzinfo=timezone.utc)

    assert time_to_expiry_years("20260706", as_of=close) == 0.0
    assert (
        time_to_expiry_years(
            "20260706",
            as_of=close + timedelta(seconds=1),
        )
        == 0.0
    )


def make_state(*quotes: Quote, now: datetime) -> LatestState:
    normalized = tuple(
        replace(quote, trade_time=quote.quote_time)
        if quote.instrument.canonical_id == "index:SPX"
        and quote.last is not None
        and quote.trade_time is None
        and quote.quote_time is not None
        else quote
        for quote in quotes
    )
    return LatestState(
        created_at=now,
        as_of=now,
        quotes=normalized,
        best_quotes=normalized,
    )


def test_options_map_builds_atm_straddle_iv_skew_and_walls() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    underlier = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        provider_symbol="index:SPX",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        last=7500.0,
        quote_time=now,
    )
    state = make_state(
        underlier,
        make_option(
            expiry="20260706",
            strike=7450,
            right="P",
            mark=8.0,
            iv=0.24,
            gamma=0.004,
            open_interest=2000,
            now=now,
        ),
        make_option(
            expiry="20260706",
            strike=7500,
            right="C",
            mark=10.0,
            iv=0.20,
            gamma=0.003,
            open_interest=1000,
            now=now,
        ),
        make_option(
            expiry="20260706",
            strike=7500,
            right="P",
            mark=11.0,
            iv=0.22,
            gamma=0.003,
            open_interest=1000,
            now=now,
        ),
        make_option(
            expiry="20260706",
            strike=7550,
            right="C",
            mark=7.5,
            iv=0.19,
            gamma=0.004,
            open_interest=2500,
            now=now,
        ),
        now=now,
    )

    options_map = build_options_map(state)
    expiry = options_map.expiries[0]

    assert options_map.underlier.price == 7500.0
    assert expiry.atm_strike == 7500
    assert expiry.atm_straddle_mid == 21.0
    assert expiry.expected_move_points == pytest.approx(21.0 * 0.85)
    assert round(expiry.atm_iv or 0.0, 2) == 0.21
    assert expiry.put_skew_ratio is not None
    assert expiry.call_skew_ratio is not None
    assert expiry.put_wall == 7450
    assert expiry.call_wall == 7550
    assert expiry.gex_quality == "open_interest_gex"
    assert expiry.coverage.with_open_interest == 4


def _gex_row(
    strike: float,
    *,
    call_gex: float = 0.0,
    put_gex: float = 0.0,
    call_oi: float = 0.0,
    put_oi: float = 0.0,
) -> StrikeGex:
    return StrikeGex(
        strike=strike,
        call_gex=call_gex,
        put_gex=put_gex,
        net_gex=call_gex + put_gex,
        abs_gex=abs(call_gex) + abs(put_gex),
        call_open_interest=call_oi,
        put_open_interest=put_oi,
    )


def test_structure_quality_tolerates_recent_stale_but_not_hard_bad() -> None:
    from dataclasses import replace as dc_replace

    # signed_gex/option_gamma_structural age quotes against the real clock
    # (no as_of parameter in the production call chain), so the fixture must
    # be anchored to now() or the test rots as wall time passes.
    now = datetime.now(tz=timezone.utc)
    quote = make_option(
        expiry="20260708",
        strike=7350.0,
        right="P",
        mark=5.0,
        iv=0.2,
        gamma=0.004,
        open_interest=1200.0,
        now=now,
    )
    # Rotated strike sampled 5 minutes ago and degraded to STALE: still fine
    # for OI/gamma structure (walls), which do not move tick by tick.
    recent_stale = dc_replace(
        quote,
        quality=MarketDataQuality.STALE,
        quote_time=now - timedelta(minutes=5),
        received_at=now - timedelta(minutes=5),
    )
    assert structure_quality_ok(recent_stale, as_of=now) is True
    assert (
        signed_gex(recent_stale, sign=-1.0, underlier=7438.0, as_of=now)
        is not None
    )

    # Too old: excluded.
    old_stale = dc_replace(
        quote,
        quality=MarketDataQuality.STALE,
        quote_time=now - timedelta(hours=1),
        received_at=now - timedelta(hours=1),
    )
    assert structure_quality_ok(old_stale, as_of=now) is False

    # Hard-bad qualities never pass regardless of age.
    missing = dc_replace(quote, quality=MarketDataQuality.MISSING, quote_time=now)
    assert structure_quality_ok(missing, as_of=now) is False


def test_build_wall_ladder_is_side_constrained_and_ranked() -> None:
    rows = [
        _gex_row(7400, put_gex=-3.0, put_oi=3300),
        _gex_row(7450, put_gex=-4.0, put_oi=2900),
        _gex_row(7480, put_gex=-5.0, put_oi=1500),
        _gex_row(7500, put_gex=-6.0, put_oi=3600),
        # Put OI above spot must not appear as "support" (2026-07-07: the top
        # put OI strike 7550 sat above spot all afternoon).
        _gex_row(7550, put_gex=-8.0, put_oi=4900, call_gex=6.0, call_oi=6500),
        _gex_row(7600, call_gex=5.0, call_oi=6533),
        # Call gamma below spot (heavy 2-way ATM volume) must not become a
        # "call wall" under the price.
        _gex_row(7460, call_gex=9.0, call_oi=100),
    ]
    call_walls, put_walls = build_wall_ladder(rows, underlier=7490.0, strike_step=5.0)

    # 7500 (broken, above spot) and 7550 puts are excluded: no longer support.
    assert [wall.strike for wall in put_walls] == [7480, 7450, 7400]
    assert all(wall.strike <= 7490.0 + 2.5 for wall in put_walls)
    assert [wall.strike for wall in call_walls] == [7550, 7600]
    assert put_walls[0].open_interest == 1500
    assert put_walls[0].distance_points == pytest.approx(-10.0)


def test_walls_come_from_ladder_and_respect_spot_side() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    underlier = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        provider_symbol="index:SPX",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        last=7500.0,
        quote_time=now,
    )
    state = make_state(
        underlier,
        # Biggest put GEX sits above spot -> must not be the put wall.
        make_option(
            expiry="20260706",
            strike=7550,
            right="P",
            mark=52.0,
            iv=0.22,
            gamma=0.006,
            open_interest=5000,
            now=now,
        ),
        make_option(
            expiry="20260706",
            strike=7450,
            right="P",
            mark=8.0,
            iv=0.24,
            gamma=0.004,
            open_interest=2000,
            now=now,
        ),
        make_option(
            expiry="20260706",
            strike=7400,
            right="P",
            mark=4.0,
            iv=0.26,
            gamma=0.002,
            open_interest=3000,
            now=now,
        ),
        make_option(
            expiry="20260706",
            strike=7550,
            right="C",
            mark=7.5,
            iv=0.19,
            gamma=0.004,
            open_interest=2500,
            now=now,
        ),
        now=now,
    )

    expiry = build_options_map(state).expiries[0]
    assert expiry.put_wall == 7450
    assert [wall.strike for wall in expiry.put_walls] == [7450, 7400]
    assert expiry.call_wall == 7550
    assert expiry.wall_method == "oi_gex"


def _bs_price(spot: float, strike: float, iv: float, t_years: float, right: str) -> float:
    import math

    sqrt_t = math.sqrt(t_years)
    d1 = (math.log(spot / strike) + 0.5 * iv * iv * t_years) / (iv * sqrt_t)
    d2 = d1 - iv * sqrt_t
    n = lambda x: 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))  # noqa: E731
    call = spot * n(d1) - strike * n(d2)
    if right == "C":
        return call
    return call - spot + strike  # parity, r=0


def test_rn_density_recovers_lognormal_from_bs_chain() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    spot, iv, t_years = 7500.0, 0.12, 1.0 / 365.0
    sigma_points = spot * iv * (t_years**0.5)  # ~47 points

    quotes = []
    for strike in range(7300, 7701, 10):
        for right in ("C", "P"):
            mark = _bs_price(spot, float(strike), iv, t_years, right)
            quotes.append(
                make_option(
                    expiry="20260706",
                    strike=float(strike),
                    right=right,
                    mark=max(mark, 0.05),
                    iv=iv,
                    gamma=0.003,
                    open_interest=1000,
                    now=now,
                )
            )
    pairs = pair_by_strike(quotes)

    density = build_rn_density(
        pairs,
        underlier=spot,
        put_wall=7450.0,
        call_wall=7550.0,
        expected_move_points=sigma_points,
    )

    assert density.quality == "ok"
    # Lognormal median = forward (r=0 -> spot); allow one strike step of slack.
    assert density.median == pytest.approx(spot, abs=10.0)
    # p90-p10 spans ~2.56 sigma for a (log)normal.
    assert density.p90 - density.p10 == pytest.approx(2.56 * sigma_points, rel=0.15)
    # Close below 7450 (-1.06 sigma) ~ 14%; above 7550 (+1.06 sigma) ~ 14%.
    assert density.prob_below_put_wall == pytest.approx(0.145, abs=0.05)
    assert density.prob_above_call_wall == pytest.approx(0.145, abs=0.05)
    assert density.clipped_mass_fraction < 0.05
    assert "strike_differential_context" not in density.to_dict()

    enriched = build_rn_density(
        pairs,
        underlier=spot,
        put_wall=7450.0,
        call_wall=7550.0,
        expected_move_points=sigma_points,
        expiry="20260706",
        as_of=now,
        reference_levels={
            "atm": spot,
            "zero_gamma": 7500.0,
            "flip_midpoint": 7500.0,
            "put_wall": 7450.0,
            "call_wall": 7550.0,
        },
    )
    enriched_payload = enriched.to_dict()
    context = enriched_payload.pop("strike_differential_context")
    assert enriched_payload == density.to_dict()
    assert context["feature_version"] == "strike_differential_context.v1"
    assert context["diagnostics"]["observation_count"] <= 24
    assert any(
        observation["simpson_local_mass"]["rn_density_interval_mass"] is not None
        for reference in context["references"]
        for observation in reference["observations"]
        if observation["simpson_local_mass"]
        and "rn_density_interval_mass" in observation["simpson_local_mass"]
    )

    expiry_map = build_expiry_map("20260706", quotes, spot, as_of=now)
    assert expiry_map.rn_density is not None
    service_context = expiry_map.rn_density.strike_differential_context
    assert service_context is not None
    assert service_context["feature_version"] == context["feature_version"]
    assert service_context["diagnostics"]["observation_count"] <= 24


def test_rn_density_insufficient_strikes() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    quotes = [
        make_option(
            expiry="20260706",
            strike=strike,
            right="C",
            mark=10.0,
            iv=0.2,
            gamma=0.003,
            open_interest=100,
            now=now,
        )
        for strike in (7480.0, 7500.0, 7520.0)
    ]
    density = build_rn_density(pair_by_strike(quotes), underlier=7500.0)
    assert density.quality == "insufficient_strikes"
    assert density.median is None


def _polynomial_curve(
    now: datetime,
    *,
    half_spread: float,
    offsets: tuple[int, ...] = (-2, -1, 0, 1, 2),
) -> tuple[SyntheticCallPoint, ...]:
    def call_mid(strike: float) -> float:
        x = strike - 100.0
        return 100.0 - 2.0 * x + 0.01 * x**2 + 0.0001 * x**3 + 0.000001 * x**4

    return tuple(
        SyntheticCallPoint(
            strike=100.0 + offset * 5.0,
            mid=(mid := call_mid(100.0 + offset * 5.0)),
            bid=mid - half_spread,
            ask=mid + half_spread,
            source_right="C",
            source_at=now,
        )
        for offset in offsets
    )


def _single_observation(
    curve: tuple[SyntheticCallPoint, ...],
    now: datetime,
) -> dict[str, object]:
    context = build_strike_differential_context(
        curve,
        expiry="20260810",
        as_of=now,
        reference_levels={"atm": 100.0},
        scales=(5.0, 10.0),
    )
    return context["references"][0]["observations"][0]


def test_surface_shape_summary_prefers_atm_5pt_and_respects_snr() -> None:
    now = datetime(2026, 8, 10, 14, 0, tzinfo=timezone.utc)
    context = build_strike_differential_context(
        _polynomial_curve(now, half_spread=0.01),
        expiry="20260810",
        as_of=now,
        reference_levels={"q_mode": 95.0, "atm_reference": 100.0},
        scales=(10.0, 5.0),
    )

    summary = summarize_strike_surface_shape(context)

    assert summary["summary_version"] == "operator_summary.v1"
    assert summary["source_feature_version"] == "strike_differential_context.v1"
    assert summary["center"] == 100.0
    assert summary["scale_points"] == 5.0
    assert summary["labels"] == ["atm_reference"]
    assert summary["d3_sign"] == "up"
    assert summary["d4_shape"] == "trough"
    assert summary["snr_quality"] == "high"
    assert summary["rank_prior"] == 0.05
    assert summary["authority"] == "desk_explain_and_rank_soft"
    assert "D3斜率+" in summary["desk_line"]
    assert "D4槽形" in summary["desk_line"]

    low_snr = {
        **context,
        "references": [
            {
                "center": 100.0,
                "labels": ["atm"],
                "observations": [
                    {
                        **context["references"][1]["observations"][1],
                        "quality": "degraded_low_snr",
                        "d3_snr": 0.2,
                        "d4_snr": 0.4,
                    }
                ],
            }
        ],
    }
    low_summary = summarize_strike_surface_shape(low_snr)
    assert low_summary["snr_quality"] == "low"
    assert low_summary["rank_prior"] == 0.0
    assert "surface_shape_low_snr" in low_summary["why_reasons"]
    assert summarize_strike_surface_shape(None)["status"] == "missing"


def test_synthetic_call_curve_uses_otm_side_and_parity_bbo() -> None:
    now = datetime(2026, 8, 10, 14, 0, tzinfo=timezone.utc)
    quotes = [
        make_option(
            expiry="20260810",
            strike=strike,
            right=right,
            mark=mark,
            iv=0.2,
            gamma=0.003,
            open_interest=100,
            now=now,
        )
        for strike, right, mark in (
            (95.0, "C", 25.0),
            (95.0, "P", 2.0),
            (105.0, "C", 3.0),
            (105.0, "P", 20.0),
        )
    ]

    curve = {point.strike: point for point in synthetic_call_curve(pair_by_strike(quotes), 100.0)}

    assert curve[95.0].mid == pytest.approx(7.0)
    assert curve[95.0].bid == pytest.approx(6.9)
    assert curve[95.0].ask == pytest.approx(7.1)
    assert curve[95.0].source_right == "P"
    assert curve[105.0].mid == pytest.approx(3.0)
    assert curve[105.0].source_right == "C"
    assert curve[95.0].source_at == now


def test_strike_differential_identities_polynomial_exactness_and_portfolios() -> None:
    now = datetime(2026, 8, 10, 14, 0, tzinfo=timezone.utc)
    curve = _polynomial_curve(now, half_spread=0.01)
    observation = _single_observation(curve, now)
    h = 5.0
    c_m2, c_m1, c_0, c_p1, c_p2 = (point.mid for point in curve)
    fly_low = c_m2 - 2.0 * c_m1 + c_0
    fly_center = c_m1 - 2.0 * c_0 + c_p1
    fly_high = c_0 - 2.0 * c_p1 + c_p2

    assert observation["fly_mid_points"] == pytest.approx(fly_center)
    assert observation["strike_d2"] == pytest.approx(0.02005)
    assert observation["strike_d3"] == pytest.approx(0.0006)
    assert observation["strike_d4"] == pytest.approx(0.000024)
    assert observation["adjacent_fly_spread_points"] == pytest.approx(fly_high - fly_low)
    assert observation["fly_curvature_points"] == pytest.approx(
        fly_low - 2.0 * fly_center + fly_high
    )
    assert observation["mexican_hat_points"] == pytest.approx(
        4.0 * fly_center - (c_m2 - 2.0 * c_0 + c_p2)
    )
    assert 2.0 * h**3 * observation["strike_d3"] == pytest.approx(
        observation["adjacent_fly_spread_points"]
    )
    assert h**4 * observation["strike_d4"] == pytest.approx(
        observation["fly_curvature_points"]
    )
    assert observation["mexican_hat_points"] == pytest.approx(
        -(h**4) * observation["strike_d4"]
    )
    assert observation["mexican_hat_points"] == pytest.approx(
        2.0 * h**2 * observation["peak_vs_shoulders"]
    )
    richardson = observation["richardson"]
    assert richardson["strike_d2"] == pytest.approx(0.02)
    simpson = observation["simpson_local_mass"]
    assert simpson["state_price_mass_proxy"] == pytest.approx(0.2015)
    quadratic_curve = tuple(
        replace(
            point,
            mid=(mid := 100.0 - 2.0 * (point.strike - 100.0) + 0.01 * (point.strike - 100.0) ** 2),
            bid=mid - 0.01,
            ask=mid + 0.01,
        )
        for point in curve
    )
    quadratic_observation = _single_observation(quadratic_curve, now)
    quadratic_simpson = quadratic_observation["simpson_local_mass"]
    assert quadratic_observation["strike_d2"] == pytest.approx(0.02)
    assert quadratic_observation["strike_d3"] == pytest.approx(0.0, abs=1e-12)
    assert quadratic_observation["strike_d4"] == pytest.approx(0.0, abs=1e-12)
    assert quadratic_simpson["state_price_mass_proxy"] == pytest.approx(0.2)
    linear_curve = tuple(
        replace(
            point,
            mid=(mid := 100.0 - 2.0 * (point.strike - 100.0)),
            bid=mid - 0.01,
            ask=mid + 0.01,
        )
        for point in curve
    )
    linear_observation = _single_observation(linear_curve, now)
    assert linear_observation["strike_d2"] == pytest.approx(0.0, abs=1e-12)
    assert linear_observation["strike_d3"] == pytest.approx(0.0, abs=1e-12)
    assert linear_observation["strike_d4"] == pytest.approx(0.0, abs=1e-12)

    quintic_curve = tuple(
        SyntheticCallPoint(
            strike=strike,
            mid=(mid := 200.0 - 2.0 * (x := strike - 100.0) + 0.01 * x**2 + 1e-9 * x**5),
            bid=mid - 0.01,
            ask=mid + 0.01,
            source_right="C",
            source_at=now,
        )
        for strike in (95.0, 100.0, 105.0, 110.0, 115.0)
    )
    quintic_context = build_strike_differential_context(
        quintic_curve,
        expiry="20260810",
        as_of=now,
        reference_levels={"atm": 105.0},
        scales=(5.0, 10.0),
    )
    quintic_richardson = quintic_context["references"][0]["observations"][0]["richardson"]
    assert quintic_richardson["strike_d2"] == pytest.approx(0.0200025)

    units = observation["virtual_portfolio_units"]
    assert units == {
        "d2_gross": 4,
        "d2_raw_coefficients": [1, -2, 1],
        "d3_gross": 8,
        "d3_raw_coefficients": [-1, 2, 0, -2, 1],
        "d4_gross": 16,
        "d4_raw_coefficients": [1, -4, 6, -4, 1],
        "mexican_hat_gross": 16,
        "mexican_hat_raw_coefficients": [-1, 4, -6, 4, -1],
        "richardson_gross": 64,
        "richardson_raw_coefficients": [-1, 16, -30, 16, -1],
        "simpson_netted_gross": 12,
        "simpson_raw_coefficients": [1, 2, -6, 2, 1],
    }


def test_strike_differential_noise_bounds_are_operator_specific_and_monotone() -> None:
    now = datetime(2026, 8, 10, 14, 0, tzinfo=timezone.utc)
    narrow = _single_observation(_polynomial_curve(now, half_spread=0.01), now)
    widened_curve = list(_polynomial_curve(now, half_spread=0.01))
    widened_curve[0] = replace(
        widened_curve[0],
        bid=widened_curve[0].mid - 0.5,
        ask=widened_curve[0].mid + 0.5,
    )
    wide = _single_observation(tuple(widened_curve), now)
    zero = _single_observation(_polynomial_curve(now, half_spread=0.0), now)

    assert wide["d3_noise_bound"] > narrow["d3_noise_bound"]
    assert wide["d4_noise_bound"] > narrow["d4_noise_bound"]
    assert wide["richardson"]["noise_bound"] > narrow["richardson"]["noise_bound"]
    assert wide["simpson_local_mass"]["noise_bound"] > narrow["simpson_local_mass"]["noise_bound"]
    assert zero["d2_noise_bound"] == 0.0
    assert zero["d3_noise_bound"] == 0.0
    assert zero["d4_noise_bound"] == 0.0
    assert zero["richardson"]["noise_bound"] == 0.0


def test_strike_differential_missing_outer_strikes_keeps_only_exact_d2() -> None:
    now = datetime(2026, 8, 10, 14, 0, tzinfo=timezone.utc)
    observation = _single_observation(
        _polynomial_curve(now, half_spread=0.01, offsets=(-1, 0, 1)),
        now,
    )

    assert observation["quality"] == "unavailable_missing_strikes"
    assert observation["strike_d2"] is not None
    assert observation["strike_d3"] is None
    assert observation["strike_d4"] is None
    assert observation["richardson"]["quality"] == "unavailable_missing_strikes"
    assert observation["simpson_local_mass"]["quality"] == "unavailable_missing_strikes"


def test_local_context_is_independent_of_global_density_and_missing_bbo() -> None:
    now = datetime(2026, 8, 10, 14, 0, tzinfo=timezone.utc)
    quotes = [
        make_option(
            expiry="20260810",
            strike=strike,
            right="C",
            mark=mid,
            iv=0.2,
            gamma=0.003,
            open_interest=100,
            now=now,
        )
        for strike, mid in zip(
            (90.0, 95.0, 100.0, 105.0, 110.0),
            (20.0, 15.0, 11.0, 8.0, 6.0),
            strict=True,
        )
    ]
    density = build_rn_density(
        pair_by_strike(quotes),
        underlier=100.0,
        expiry="20260810",
        as_of=now,
        reference_levels={"atm": 100.0},
    )

    assert density.quality == "insufficient_strikes"
    assert density.strike_differential_context is not None
    observation = density.strike_differential_context["references"][0]["observations"][0]
    assert observation["strike_d2"] == pytest.approx(0.04)

    mid_only = tuple(replace(point, bid=None, ask=None) for point in _polynomial_curve(now, half_spread=0.01))
    degraded = _single_observation(mid_only, now)
    assert degraded["quality"] == "degraded_missing_bbo"
    assert degraded["strike_d2"] is not None
    assert degraded["d2_noise_bound"] is None
    assert degraded["d2_snr"] is None
    assert degraded["richardson"]["noise_bound"] is None


def test_strike_differential_blocks_raw_negative_convexity_without_clipping() -> None:
    now = datetime(2026, 8, 10, 14, 0, tzinfo=timezone.utc)
    curve = tuple(
        SyntheticCallPoint(
            strike=strike,
            mid=mid,
            bid=mid - 0.01,
            ask=mid + 0.01,
            source_right="C",
            source_at=now,
        )
        for strike, mid in zip(
            (90.0, 95.0, 100.0, 105.0, 110.0),
            (20.0, 15.0, 9.0, 2.0, 1.0),
            strict=True,
        )
    )

    observation = _single_observation(curve, now)

    assert observation["quality"] == "blocked_convexity_violation"
    assert observation["fly_mid_points"] == pytest.approx(-1.0)
    assert observation["strike_d2"] == pytest.approx(-0.04)
    assert observation["strike_d3"] is None
    assert observation["strike_d4"] is None


def test_strike_differential_is_causal_and_compact() -> None:
    now = datetime(2026, 8, 10, 14, 0, tzinfo=timezone.utc)
    curve = tuple(
        SyntheticCallPoint(
            strike=float(strike),
            mid=300.0 - float(strike),
            bid=299.99 - float(strike),
            ask=300.01 - float(strike),
            source_right="C",
            source_at=now,
        )
        for strike in range(50, 151, 5)
    )
    future_curve = tuple(
        replace(point, source_at=now + timedelta(seconds=1))
        if point.strike == 100.0
        else point
        for point in curve
    )
    levels = {f"reference_{index}": 70.0 + index * 5.0 for index in range(8)}
    levels.update(atm=100.0, q_mode=99.0)

    context = build_strike_differential_context(
        future_curve,
        expiry="20260810",
        as_of=now,
        reference_levels=levels,
    )

    assert len(context["references"]) == 6
    assert context["diagnostics"]["observation_count"] == 24
    assert context["references"][0]["labels"][:2] == ["atm", "q_mode"]
    assert all(
        observation["quality"] == "unavailable_future_quote"
        for observation in context["references"][0]["observations"]
    )
    assert all(
        observation["reasons"]
        for reference in context["references"]
        for observation in reference["observations"]
    )


def test_future_curve_point_suppresses_global_density_cross_diagnostics() -> None:
    now = datetime(2026, 8, 10, 14, 0, tzinfo=timezone.utc)
    quotes = []
    for strike in range(50, 151, 5):
        x = float(strike) - 100.0
        quote = make_option(
            expiry="20260810",
            strike=float(strike),
            right="C",
            mark=100.0 - x + 0.001 * x**2,
            iv=0.2,
            gamma=0.003,
            open_interest=100,
            now=now,
        )
        quotes.append(
            replace(quote, quote_time=now + timedelta(seconds=1))
            if strike == 150
            else quote
        )

    density = build_rn_density(
        pair_by_strike(quotes),
        underlier=100.0,
        expiry="20260810",
        as_of=now,
        reference_levels={"atm": 100.0},
    )

    context = density.strike_differential_context
    assert context is not None
    assert all("q_mode" not in reference["labels"] for reference in context["references"])
    simpson_rows = [
        observation["simpson_local_mass"]
        for reference in context["references"]
        for observation in reference["observations"]
        if observation["simpson_local_mass"]
        and "rn_density_interval_mass" in observation["simpson_local_mass"]
    ]
    assert simpson_rows
    assert all(row["rn_density_interval_mass"] is None for row in simpson_rows)


def test_options_map_warns_when_open_interest_missing() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    underlier = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        provider_symbol="index:SPX",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        last=7500.0,
        quote_time=now,
    )
    state = make_state(
        underlier,
        make_option(
            expiry="20260706",
            strike=7500,
            right="C",
            mark=10.0,
            iv=0.20,
            gamma=0.003,
            open_interest=None,
            now=now,
        ),
        now=now,
    )

    options_map = build_options_map(state)
    expiry = options_map.expiries[0]

    assert expiry.gamma_state == "unknown_no_open_interest"
    assert "missing open interest; call/put wall and GEX are unavailable" in expiry.warnings


def test_options_map_excludes_stale_quotes_from_iv_and_gex() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    underlier = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        provider_symbol="index:SPX",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        last=7500.0,
        quote_time=now,
    )
    stale_call = replace(
        make_option(
            expiry="20260706",
            strike=7500,
            right="C",
            mark=10.0,
            iv=0.20,
            gamma=0.003,
            open_interest=1000,
            now=now - timedelta(seconds=30),
        ),
        quality=MarketDataQuality.STALE,
    )
    live_put = make_option(
        expiry="20260706",
        strike=7500,
        right="P",
        mark=11.0,
        iv=0.22,
        gamma=0.003,
        open_interest=1000,
        now=now,
    )
    state = make_state(underlier, stale_call, live_put, now=now)

    options_map = build_options_map(state)
    expiry = options_map.expiries[0]

    assert expiry.coverage.stale == 1
    assert expiry.coverage.live == 1
    assert expiry.atm_call_mid is None
    assert expiry.atm_iv == 0.22
    assert expiry.call_wall is None
    assert expiry.put_wall is None
    assert expiry.wall_method == "unavailable"


def test_options_map_underlier_mismatch_when_spx_missing_falls_back_to_es() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    es_underlier = Quote(
        instrument=InstrumentId.future("ES"),
        provider=Provider.IBKR,
        provider_symbol="future:ES",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7510.0,
        quote_time=now,
    )
    state = make_state(
        es_underlier,
        make_option(
            expiry="20260706",
            strike=7450,
            right="P",
            mark=8.0,
            iv=0.24,
            gamma=0.004,
            open_interest=2000,
            now=now,
        ),
        make_option(
            expiry="20260706",
            strike=7500,
            right="C",
            mark=10.0,
            iv=0.20,
            gamma=0.003,
            open_interest=1000,
            now=now,
        ),
        make_option(
            expiry="20260706",
            strike=7500,
            right="P",
            mark=11.0,
            iv=0.22,
            gamma=0.003,
            open_interest=1000,
            now=now,
        ),
        make_option(
            expiry="20260706",
            strike=7550,
            right="C",
            mark=7.5,
            iv=0.19,
            gamma=0.004,
            open_interest=2500,
            now=now,
        ),
        now=now,
    )

    options_map = build_options_map(state)
    expiry = options_map.expiries[0]

    # Chain parity (C7500=10, P7500=11 -> spot 7499) replaces the ES
    # reference, so gamma/GEX are no longer suppressed outside cash hours.
    assert options_map.underlier.source == "chain_implied"
    assert options_map.underlier.price == pytest.approx(7499.0)
    assert not any("underlier_mismatch" in warning for warning in options_map.warnings)
    assert expiry.gamma_state != "unknown_underlier_mismatch"
    assert expiry.put_wall == 7450
    assert expiry.call_wall == 7550


def test_options_map_keeps_es_mismatch_when_chain_parity_unavailable() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    es_underlier = Quote(
        instrument=InstrumentId.future("ES"),
        provider=Provider.IBKR,
        provider_symbol="future:ES",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7510.0,
        quote_time=now,
    )
    # Calls only: no C/P pair at any strike -> parity unavailable.
    state = make_state(
        es_underlier,
        make_option(
            expiry="20260706",
            strike=7500,
            right="C",
            mark=10.0,
            iv=0.20,
            gamma=0.003,
            open_interest=1000,
            now=now,
        ),
        make_option(
            expiry="20260706",
            strike=7550,
            right="C",
            mark=7.5,
            iv=0.19,
            gamma=0.004,
            open_interest=2500,
            now=now,
        ),
        now=now,
    )

    options_map = build_options_map(state)
    expiry = options_map.expiries[0]

    assert options_map.underlier.source == "future:ES"
    assert any("underlier_mismatch" in warning for warning in options_map.warnings)
    assert expiry.gamma_state == "unknown_underlier_mismatch"


def test_options_map_excludes_delayed_quotes_from_iv_and_gex() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    underlier = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        provider_symbol="index:SPX",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        last=7500.0,
        quote_time=now,
    )
    delayed_call = replace(
        make_option(
            expiry="20260706",
            strike=7500,
            right="C",
            mark=10.0,
            iv=0.20,
            gamma=0.003,
            open_interest=1000,
            now=now,
        ),
        quality=MarketDataQuality.DELAYED,
    )
    live_put = make_option(
        expiry="20260706",
        strike=7500,
        right="P",
        mark=11.0,
        iv=0.22,
        gamma=0.003,
        open_interest=1000,
        now=now,
    )
    state = make_state(underlier, delayed_call, live_put, now=now)

    options_map = build_options_map(state)
    expiry = options_map.expiries[0]

    assert expiry.coverage.delayed == 1
    assert expiry.coverage.live == 1
    assert expiry.atm_call_mid is None
    assert expiry.atm_iv == 0.22
    assert expiry.call_wall is None
    assert expiry.put_wall is None
    assert expiry.wall_method == "unavailable"


def test_strike_gex_open_interest_defaults_to_zero_when_missing() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    call = make_option(
        expiry="20260706",
        strike=7500,
        right="C",
        mark=10.0,
        iv=0.20,
        gamma=0.003,
        open_interest=None,
        now=now,
    )
    put = make_option(
        expiry="20260706",
        strike=7500,
        right="P",
        mark=11.0,
        iv=0.22,
        gamma=0.003,
        open_interest=1000,
        now=now,
    )
    pairs = {7500.0: {OptionRight.CALL: call, OptionRight.PUT: put}}

    rows = build_gex_by_strike(pairs, underlier=7500.0, as_of=now)

    assert len(rows) == 1
    assert rows[0].call_open_interest == 0.0
    assert rows[0].put_open_interest == 1000.0


def test_gex_weight_intraday_uses_oi_plus_volume() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    quote = replace(
        make_option(
            expiry="20260706",
            strike=7500,
            right="C",
            mark=10.0,
            iv=0.20,
            gamma=0.001,
            open_interest=100,
            now=now,
        ),
        volume=400,
    )
    oi_only = signed_gex(
        quote,
        sign=1.0,
        underlier=7500.0,
        as_of=now,
        intraday=False,
    )
    intraday = signed_gex(
        quote,
        sign=1.0,
        underlier=7500.0,
        as_of=now,
        intraday=True,
    )
    assert oi_only is not None
    assert intraday is not None
    assert intraday == pytest.approx(oi_only * 5.0)


def test_gex_weight_volume_only_nonzero_for_intraday() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    quote = replace(
        make_option(
            expiry="20260706",
            strike=7500,
            right="C",
            mark=10.0,
            iv=0.20,
            gamma=0.001,
            open_interest=None,
            now=now,
        ),
        volume=50,
    )
    assert gex_weight(quote, intraday=False) is None
    assert gex_weight(quote, intraday=True) == 50.0


def test_volume_only_intraday_gex_is_not_labeled_open_interest() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    underlier = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        received_at=now,
        quote_time=now,
        quality=MarketDataQuality.LIVE,
        last=7500.0,
    )
    rows = tuple(
        replace(
            make_option(
                expiry="20260706",
                strike=7500,
                right=right,
                mark=10.0,
                iv=0.20,
                gamma=0.003,
                open_interest=None,
                now=now,
            ),
            volume=100.0,
        )
        for right in ("C", "P")
    )

    expiry = build_options_map(make_state(underlier, *rows, now=now)).expiries[0]

    assert expiry.net_gex is not None
    assert expiry.gex_quality == "no_open_interest_gex"
    assert expiry.wall_method == "volume_fallback"


def test_partial_open_interest_coverage_cannot_publish_oi_walls() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    underlier = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        received_at=now,
        quote_time=now,
        quality=MarketDataQuality.LIVE,
        last=7500.0,
    )
    rows = [
        make_option(
            expiry="20260706",
            strike=strike,
            right=right,
            mark=10.0,
            iv=0.20,
            gamma=0.003,
            open_interest=100.0 if strike == 7450 and right == "P" else None,
            now=now,
        )
        for strike, right in ((7450, "P"), (7500, "C"), (7500, "P"), (7550, "C"))
    ]

    expiry = build_options_map(make_state(underlier, *rows, now=now)).expiries[0]

    assert expiry.gex_quality == "no_open_interest_gex"
    assert expiry.wall_method == "unavailable"
    assert any(
        "oi_contract_coverage_below_threshold" in warning
        for warning in expiry.warnings
    )


def test_single_strike_open_interest_cannot_publish_oi_walls() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    underlier = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        received_at=now,
        quote_time=now,
        quality=MarketDataQuality.LIVE,
        last=7500.0,
    )
    rows = [
        make_option(
            expiry="20260706",
            strike=7500.0,
            right=right,
            mark=10.0,
            iv=0.20,
            gamma=0.003,
            open_interest=100.0,
            now=now,
        )
        for right in ("C", "P")
    ]

    expiry = build_options_map(make_state(underlier, *rows, now=now)).expiries[0]

    assert expiry.gex_quality == "no_open_interest_gex"
    assert expiry.wall_method != "oi_gex"
    assert any(
        "oi_strike_coverage_below_threshold" in warning
        for warning in expiry.warnings
    )


def test_schwab_open_interest_cannot_publish_oi_walls() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    underlier = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.SCHWAB,
        received_at=now,
        quote_time=now,
        quality=MarketDataQuality.LIVE,
        last=7500.0,
    )
    rows = [
        replace(
            make_option(
                expiry="20260706",
                strike=strike,
                right=right,
                mark=10.0,
                iv=0.20,
                gamma=0.003,
                open_interest=100.0,
                now=now,
            ),
            provider=Provider.SCHWAB,
        )
        for strike, right in (
            (7450.0, "P"),
            (7450.0, "C"),
            (7500.0, "P"),
            (7500.0, "C"),
            (7550.0, "P"),
            (7550.0, "C"),
        )
    ]

    expiry = build_options_map(make_state(underlier, *rows, now=now)).expiries[0]

    assert expiry.gex_quality == "no_open_interest_gex"
    assert expiry.wall_method != "oi_gex"
    assert any(
        "ibkr_hot_lane_missing" in warning
        for warning in expiry.warnings
    )


def test_ibkr_hot_lane_open_interest_publishes_walls_despite_schwab_wide_chain() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    underlier = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.SCHWAB,
        received_at=now,
        quote_time=now,
        quality=MarketDataQuality.LIVE,
        last=7500.0,
    )
    schwab_rows = [
        replace(
            make_option(
                expiry="20260706",
                strike=strike,
                right=right,
                mark=10.0,
                iv=0.20,
                gamma=0.003,
                open_interest=10_000.0,
                now=now,
            ),
            provider=Provider.SCHWAB,
        )
        for strike in (7000.0, 7050.0, 7100.0, 7900.0, 7950.0, 8000.0)
        for right in ("C", "P")
    ]
    ibkr_rows = [
        make_option(
            expiry="20260706",
            strike=strike,
            right=right,
            mark=10.0,
            iv=0.20,
            gamma=0.003,
            open_interest=100.0,
            now=now,
        )
        for strike, right in (
            (7450.0, "P"),
            (7450.0, "C"),
            (7500.0, "P"),
            (7500.0, "C"),
            (7550.0, "P"),
            (7550.0, "C"),
        )
    ]

    expiry = build_options_map(make_state(underlier, *schwab_rows, *ibkr_rows, now=now)).expiries[0]

    assert expiry.gex_quality == "open_interest_gex"
    assert expiry.wall_method == "oi_gex"
    assert expiry.put_wall in {7450.0, 7500.0}
    assert expiry.call_wall in {7500.0, 7550.0}
    assert {expiry.put_wall, expiry.call_wall}.isdisjoint({7000.0, 8000.0})
    assert any("ibkr_hot_lane" in warning for warning in expiry.warnings)


def test_minority_untrusted_open_interest_is_removed_after_coverage_passes() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    underlier = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        received_at=now,
        quote_time=now,
        quality=MarketDataQuality.LIVE,
        last=7500.0,
    )
    rows = [
        make_option(
            expiry="20260706",
            strike=strike,
            right=right,
            mark=10.0,
            iv=0.20,
            gamma=0.003,
            open_interest=100.0,
            now=now,
        )
        for strike, right in (
            (7450.0, "P"),
            (7450.0, "C"),
            (7500.0, "P"),
            (7500.0, "C"),
            (7550.0, "P"),
            (7550.0, "C"),
        )
    ]
    rows[3] = replace(
        rows[3],
        open_interest=10_000.0,
        raw={"open_interest_provider": "schwab"},
    )

    expiry = build_options_map(make_state(underlier, *rows, now=now)).expiries[0]
    atm = next(row for row in expiry.top_gex_strikes if row.strike == 7500.0)

    assert expiry.wall_method == "oi_gex"
    assert expiry.gex_quality == "open_interest_gex"
    assert atm.call_open_interest == 0.0
    assert atm.put_open_interest == 100.0


def test_non_front_partial_open_interest_cannot_publish_oi_walls() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    underlier = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        received_at=now,
        quote_time=now,
        quality=MarketDataQuality.LIVE,
        last=7500.0,
    )
    rows = [
        replace(
            make_option(
                expiry="20260707",
                strike=strike,
                right=right,
                mark=10.0,
                iv=0.20,
                gamma=0.003,
                open_interest=100.0 if strike == 7450 and right == "P" else None,
                now=now,
            ),
            volume=100.0,
        )
        for strike, right in ((7450, "P"), (7500, "C"), (7500, "P"), (7550, "C"))
    ]

    expiry = build_options_map(make_state(underlier, *rows, now=now)).expiries[0]

    assert expiry.expiry == "20260707"
    assert expiry.gex_quality == "no_open_interest_gex"
    assert expiry.wall_method == "unavailable"
    assert expiry.call_wall is None
    assert expiry.put_wall is None


def test_bs_gamma_hand_computed_smoke_value() -> None:
    # S=K=6000, iv=0.2, t=1/365:
    # d1 = 0.5*iv*sqrt(t) = 0.1/sqrt(365) ~= 0.005234
    # phi(d1) ~= 0.398942, gamma = phi/(S*iv*sqrt(t)) ~= 0.006351398
    expected = 0.006351397631673981
    value = bs_gamma(6000.0, 6000.0, 0.2, 1.0 / 365.0)
    assert value == pytest.approx(expected, abs=1e-6)


def test_interpolated_atm_iv_linear_between_strikes() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    call_6000 = make_option(
        expiry="20260706",
        strike=6000,
        right="C",
        mark=10.0,
        iv=0.20,
        gamma=0.001,
        open_interest=100,
        now=now,
    )
    call_6025 = make_option(
        expiry="20260706",
        strike=6025,
        right="C",
        mark=10.0,
        iv=0.22,
        gamma=0.001,
        open_interest=100,
        now=now,
    )
    put_6000 = make_option(
        expiry="20260706",
        strike=6000,
        right="P",
        mark=10.0,
        iv=0.20,
        gamma=0.001,
        open_interest=100,
        now=now,
    )
    put_6025 = make_option(
        expiry="20260706",
        strike=6025,
        right="P",
        mark=10.0,
        iv=0.22,
        gamma=0.001,
        open_interest=100,
        now=now,
    )
    pairs = pair_by_strike([call_6000, call_6025, put_6000, put_6025])
    assert interpolated_atm_iv(pairs, 6010.0) == pytest.approx(0.208, abs=1e-9)


def test_wing_iv_at_delta_selects_closest_valid_quote() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)

    def quote_with_delta(delta: float, iv: float) -> Quote:
        return replace(
            make_option(
                expiry="20260706",
                strike=7500,
                right="P",
                mark=10.0,
                iv=iv,
                gamma=0.001,
                open_interest=100,
                now=now,
            ),
            greeks=OptionGreeks(
                implied_vol=iv,
                delta=delta,
                gamma=0.001,
                theta=-1.0,
                vega=0.3,
                model="test",
            ),
        )

    quotes = [
        quote_with_delta(-0.10, 0.18),
        quote_with_delta(-0.25, 0.24),
        quote_with_delta(-0.40, 0.30),
    ]
    assert wing_iv_at_delta(quotes) == 0.24


def test_zero_gamma_spot_scan_falls_back_when_iv_missing() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    pairs: dict[float, dict[OptionRight, Quote]] = {}
    for strike in range(5900, 6101, 25):
        call = replace(
            make_option(
                expiry="20260706",
                strike=float(strike),
                right="C",
                mark=10.0,
                iv=0.20,
                gamma=0.001,
                open_interest=100,
                now=now,
            ),
            greeks=None,
        )
        put = replace(
            make_option(
                expiry="20260706",
                strike=float(strike),
                right="P",
                mark=10.0,
                iv=0.20,
                gamma=0.001,
                open_interest=100,
                now=now,
            ),
            greeks=None,
        )
        pairs[float(strike)] = {OptionRight.CALL: call, OptionRight.PUT: put}

    zero, flip_zone, method = zero_gamma_spot_scan(
        pairs,
        underlier=6000.0,
        expiry="20260706",
        as_of=now,
        intraday=False,
    )
    assert zero is None
    assert flip_zone is None
    assert method == "insufficient_iv"


def test_zero_gamma_spot_scan_is_unavailable_at_expiry_close() -> None:
    close = datetime(2026, 7, 6, 20, 0, tzinfo=timezone.utc)
    pairs = {
        strike: {
            OptionRight.CALL: make_option(
                expiry="20260706",
                strike=strike,
                right="C",
                mark=10.0,
                iv=0.20,
                gamma=0.001,
                open_interest=100,
                now=close,
            ),
            OptionRight.PUT: make_option(
                expiry="20260706",
                strike=strike,
                right="P",
                mark=10.0,
                iv=0.20,
                gamma=0.001,
                open_interest=100,
                now=close,
            ),
        }
        for strike in (5975.0, 6000.0, 6025.0)
    }

    zero, flip_zone, method = zero_gamma_spot_scan(
        pairs,
        underlier=6000.0,
        expiry="20260706",
        as_of=close,
        intraday=True,
    )

    assert zero is None
    assert flip_zone is None
    assert method == "expiry_elapsed"


@pytest.mark.parametrize(
    "as_of",
    (
        datetime(2026, 7, 6, 20, 0, tzinfo=timezone.utc),
        datetime(2026, 7, 6, 20, 0, 1, tzinfo=timezone.utc),
    ),
)
def test_options_map_does_not_restore_zero_gamma_after_expiry(as_of: datetime) -> None:
    underlier = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        received_at=as_of,
        quote_time=as_of,
        quality=MarketDataQuality.LIVE,
        last=6000.0,
    )
    rows = [
        make_option(
            expiry="20260706",
            strike=strike,
            right=right,
            mark=10.0,
            iv=0.20,
            gamma=gamma,
            open_interest=100,
            now=as_of,
        )
        for strike, right, gamma in (
            (5975.0, "C", 0.001),
            (5975.0, "P", 0.003),
            (6000.0, "C", 0.002),
            (6000.0, "P", 0.002),
            (6025.0, "C", 0.003),
            (6025.0, "P", 0.001),
        )
    ]

    expiry = build_options_map(make_state(underlier, *rows, now=as_of)).expiries[0]

    assert expiry.zero_gamma is None
    assert expiry.gamma_flip_zone is None
    assert expiry.zero_gamma_method == "expiry_elapsed"


def test_build_expiry_map_skew_uses_moneyness_fallback_without_delta() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    underlier = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        provider_symbol="index:SPX",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        last=7500.0,
        quote_time=now,
    )
    state = make_state(
        underlier,
        make_option(
            expiry="20260706",
            strike=7450,
            right="P",
            mark=8.0,
            iv=0.24,
            gamma=0.004,
            open_interest=2000,
            now=now,
        ),
        make_option(
            expiry="20260706",
            strike=7500,
            right="C",
            mark=10.0,
            iv=0.20,
            gamma=0.003,
            open_interest=1000,
            now=now,
        ),
        make_option(
            expiry="20260706",
            strike=7500,
            right="P",
            mark=11.0,
            iv=0.22,
            gamma=0.003,
            open_interest=1000,
            now=now,
        ),
        make_option(
            expiry="20260706",
            strike=7550,
            right="C",
            mark=7.5,
            iv=0.19,
            gamma=0.004,
            open_interest=2500,
            now=now,
        ),
        now=now,
    )
    expiry = build_options_map(state).expiries[0]
    assert expiry.skew_method == "moneyness_fallback"
    assert expiry.put_skew_25d == pytest.approx((expiry.put_wing_iv or 0) - (expiry.atm_iv or 0))
    assert expiry.call_skew_25d == pytest.approx((expiry.call_wing_iv or 0) - (expiry.atm_iv or 0))


def test_zero_gamma_spot_scan_finds_root_in_chain() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    pairs: dict[float, dict[OptionRight, Quote]] = {}
    for strike in range(5900, 6101, 25):
        distance = strike - 6000
        put_oi = 5000 if distance < 0 else 500
        call_oi = 500 if distance < 0 else 5000
        call = make_option(
            expiry="20260706",
            strike=float(strike),
            right="C",
            mark=10.0,
            iv=0.20,
            gamma=0.001,
            open_interest=call_oi,
            now=now,
        )
        put = make_option(
            expiry="20260706",
            strike=float(strike),
            right="P",
            mark=10.0,
            iv=0.20,
            gamma=0.001,
            open_interest=put_oi,
            now=now,
        )
        pairs[float(strike)] = {OptionRight.CALL: call, OptionRight.PUT: put}

    zero, flip_zone, method = zero_gamma_spot_scan(
        pairs,
        underlier=6000.0,
        expiry="20260706",
        as_of=now,
        intraday=False,
    )
    assert method == "spot_scan"
    assert zero is not None
    assert 5900 <= zero <= 6100
    assert flip_zone is not None
    assert flip_zone[0] <= zero <= flip_zone[1]


def test_options_map_accepts_one_precomputed_quote_grouping() -> None:
    now = datetime(2026, 7, 13, 14, 0, tzinfo=timezone.utc)
    state = make_state(
        Quote(
            instrument=InstrumentId.index("SPX"),
            provider=Provider.IBKR,
            received_at=now,
            quote_time=now,
            quality=MarketDataQuality.LIVE,
            last=7500.0,
        ),
        make_option(
            expiry="20260713",
            strike=7500.0,
            right="C",
            mark=10.0,
            iv=0.2,
            gamma=0.003,
            open_interest=1000.0,
            now=now,
        ),
        now=now,
    )
    grouped = group_spxw_option_quotes(state)

    expected = build_options_map(state)
    actual = build_options_map(state, grouped_quotes=grouped)

    assert actual.underlier == expected.underlier
    assert actual.expiries == expected.expiries
    assert actual.warnings == expected.warnings
    assert actual.spy_confluence == expected.spy_confluence
