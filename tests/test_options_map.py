from __future__ import annotations

from datetime import datetime, timezone

from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    OptionGreeks,
    Provider,
    Quote,
)
from spx_spark.options_map import build_options_map
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
    assert round(expiry.atm_iv or 0.0, 2) == 0.21
    assert expiry.put_skew_ratio is not None
    assert expiry.call_skew_ratio is not None
    assert expiry.put_wall == 7450
    assert expiry.call_wall == 7550
    assert expiry.gex_quality == "open_interest_gex"
    assert expiry.coverage.with_open_interest == 4


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
