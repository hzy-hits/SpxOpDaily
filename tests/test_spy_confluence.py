from __future__ import annotations

from datetime import datetime, timezone

from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    OptionGreeks,
    Provider,
    Quote,
)
from spx_spark.options_map import (
    ExpiryOptionsMap,
    OptionCoverage,
    build_spy_confluence,
)
from spx_spark.storage import LatestState


def default_coverage(*, total: int = 4) -> OptionCoverage:
    return OptionCoverage(
        total=total,
        live=total,
        stale=0,
        delayed=0,
        unknown_age=0,
        max_age_ms=100.0,
        with_bid_ask=total,
        with_mid=total,
        with_iv=total,
        with_delta=total,
        with_gamma=total,
        with_theta=total,
        with_vega=total,
        with_open_interest=total,
        avg_spread_bps=50.0,
    )


def make_spy_option(
    *,
    strike: float,
    right: str,
    gamma: float,
    open_interest: float,
    now: datetime,
) -> Quote:
    return Quote(
        instrument=InstrumentId.option("SPY", expiry="20260706", strike=strike, right=right),
        provider=Provider.SCHWAB,
        provider_symbol=f"SPY:{strike}:{right}",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        bid=1.0,
        ask=1.2,
        mark=1.1,
        open_interest=open_interest,
        quote_time=now,
        greeks=OptionGreeks(
            implied_vol=0.2,
            delta=0.5 if right == "C" else -0.5,
            gamma=gamma,
            theta=-1.0,
            vega=0.3,
            model="test",
        ),
    )


def test_confluence_missing_spy_chain() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    state = LatestState(created_at=now, as_of=now, quotes=(), best_quotes=())
    result = build_spy_confluence(state, None)
    assert result.quality == "missing_spy_chain"


def test_confluence_detects_confluent_call_wall() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    spy = Quote(
        instrument=InstrumentId.equity("SPY"),
        provider=Provider.SCHWAB,
        provider_symbol="SPY",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=750.0,
        quote_time=now,
    )
    spx = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        provider_symbol="index:SPX",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7500.0,
        quote_time=now,
    )
    quotes = (
        spy,
        spx,
        make_spy_option(strike=750, right="C", gamma=0.003, open_interest=1000, now=now),
        make_spy_option(strike=750, right="P", gamma=0.003, open_interest=1000, now=now),
        make_spy_option(strike=755, right="C", gamma=0.004, open_interest=5000, now=now),
        make_spy_option(strike=755, right="P", gamma=0.002, open_interest=500, now=now),
        make_spy_option(strike=748, right="P", gamma=0.004, open_interest=2000, now=now),
        make_spy_option(strike=748, right="C", gamma=0.002, open_interest=500, now=now),
    )
    state = LatestState(created_at=now, as_of=now, quotes=quotes, best_quotes=quotes)
    front_spxw = ExpiryOptionsMap(
        expiry="20260706",
        option_count=10,
        strike_count=5,
        atm_strike=7500.0,
        atm_call_mid=10.0,
        atm_put_mid=11.0,
        atm_straddle_mid=21.0,
        expected_move_points=21.0,
        expected_move_pct=0.28,
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
        gamma_state="positive_gamma_pin",
        gex_quality="open_interest_gex",
        coverage=default_coverage(),
        top_gex_strikes=(),
        warnings=(),
    )
    result = build_spy_confluence(state, front_spxw)
    assert result.spy_call_wall_spx == 7550.0
    assert result.call_wall_confluent is True


def test_confluence_maps_strikes_times_ten() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    spy = Quote(
        instrument=InstrumentId.equity("SPY"),
        provider=Provider.SCHWAB,
        provider_symbol="SPY",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=750.0,
        quote_time=now,
    )
    quotes = (
        spy,
        make_spy_option(strike=750, right="C", gamma=0.003, open_interest=1000, now=now),
        make_spy_option(strike=750, right="P", gamma=0.003, open_interest=1000, now=now),
        make_spy_option(strike=748, right="P", gamma=0.004, open_interest=5000, now=now),
        make_spy_option(strike=748, right="C", gamma=0.002, open_interest=500, now=now),
    )
    state = LatestState(created_at=now, as_of=now, quotes=quotes, best_quotes=quotes)
    result = build_spy_confluence(state, None)
    assert result.spy_put_wall_spx == 7480.0
