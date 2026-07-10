from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

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
    build_rn_density,
    build_wall_ladder,
    gex_weight,
    interpolated_atm_iv,
    pair_by_strike,
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


def make_state(*quotes: Quote, now: datetime) -> LatestState:
    return LatestState(
        created_at=now,
        as_of=now,
        quotes=tuple(quotes),
        best_quotes=tuple(quotes),
    )


def test_options_map_builds_atm_straddle_iv_skew_and_walls() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    underlier = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        provider_symbol="index:SPX",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7500.0,
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
    assert signed_gex(recent_stale, sign=-1.0, underlier=7438.0) is not None

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
        mark=7500.0,
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


def test_options_map_warns_when_open_interest_missing() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    underlier = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        provider_symbol="index:SPX",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7500.0,
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
        mark=7500.0,
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
    assert expiry.put_wall == 7500


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
        mark=7500.0,
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
    assert expiry.put_wall == 7500


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

    rows = build_gex_by_strike(pairs, underlier=7500.0)

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
    oi_only = signed_gex(quote, sign=1.0, underlier=7500.0, intraday=False)
    intraday = signed_gex(quote, sign=1.0, underlier=7500.0, intraday=True)
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
        mark=7500.0,
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


def test_build_expiry_map_skew_uses_moneyness_fallback_without_delta() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    underlier = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        provider_symbol="index:SPX",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7500.0,
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
