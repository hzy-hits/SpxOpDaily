from __future__ import annotations

import math
from datetime import datetime, timezone

import pytest

from spx_spark.analytics.greeks.black_scholes import d1, normal_cdf
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    OptionGreeks,
    Provider,
    Quote,
)
from spx_spark.options_map import (
    build_expiry_map,
    pair_by_strike,
    probability_for_level,
)


def make_quote(
    *,
    strike: float,
    right: str,
    delta: float,
    gamma: float = 0.003,
    open_interest: float = 1000.0,
    iv: float | None = 0.2,
    now: datetime,
) -> Quote:
    return Quote(
        instrument=InstrumentId.option(
            "SPX",
            expiry="20260706",
            strike=strike,
            right=right,
            trading_class="SPXW",
        ),
        provider=Provider.IBKR,
        provider_symbol=f"SPXW:20260706:{strike}:{right}",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        bid=1.0,
        ask=1.2,
        mark=1.1,
        open_interest=open_interest,
        quote_time=now,
        greeks=OptionGreeks(
            implied_vol=iv,
            delta=delta,
            gamma=gamma,
            theta=-1.0,
            vega=0.3,
            model="test",
        ),
    )


def test_probability_for_level_uses_call_delta_above_underlier() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    quotes = [
        make_quote(strike=7550, right="C", delta=0.20, now=now),
        make_quote(strike=7500, right="C", delta=0.50, now=now),
    ]
    pairs = pair_by_strike(quotes)
    prob_close, prob_touch, source_strike, _source_delta = probability_for_level(
        7550,
        underlier=7500,
        pairs=pairs,
        strike_step=5.0,
    )
    assert prob_close == 0.20
    assert prob_touch == 0.40
    assert source_strike == 7550


def test_probability_for_level_uses_put_delta_below_underlier() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    quotes = [
        make_quote(strike=7450, right="P", delta=-0.25, now=now),
        make_quote(strike=7500, right="P", delta=-0.50, now=now),
    ]
    pairs = pair_by_strike(quotes)
    prob_close, prob_touch, source_strike, _source_delta = probability_for_level(
        7450,
        underlier=7500,
        pairs=pairs,
        strike_step=5.0,
    )
    assert prob_close == 0.25
    assert prob_touch == 0.50
    assert source_strike == 7450


def test_probability_for_level_refuses_far_strike() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    quotes = [make_quote(strike=7600, right="C", delta=0.15, now=now)]
    pairs = pair_by_strike(quotes)
    result = probability_for_level(
        7550,
        underlier=7500,
        pairs=pairs,
        strike_step=5.0,
    )
    assert result == (None, None, None, None)


def test_probability_for_level_prefers_nd2_when_iv_and_tau_available() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    iv = 0.2
    tau = 2.0 / (365.0 * 24.0)  # two hours to expiry
    quotes = [
        make_quote(strike=7550, right="C", delta=0.20, now=now),
        make_quote(strike=7500, right="C", delta=0.50, now=now),
    ]
    pairs = pair_by_strike(quotes)
    prob_close, prob_touch, source_strike, source_delta = probability_for_level(
        7550,
        underlier=7500,
        pairs=pairs,
        strike_step=5.0,
        tau_years=tau,
    )
    d2 = d1(7500, 7550, iv, tau) - iv * math.sqrt(tau)
    expected = normal_cdf(d2)
    assert prob_close == pytest.approx(expected)
    assert prob_close == pytest.approx(0.0139, rel=1e-3)
    assert prob_touch == pytest.approx(min(1.0, 2 * expected))
    # The delta anchor N(d1) overstates the OTM side versus the N(d2) target.
    assert prob_close < source_delta
    assert source_strike == 7550


def test_probability_for_level_nd2_put_side() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    iv = 0.2
    tau = 2.0 / (365.0 * 24.0)
    quotes = [
        make_quote(strike=7450, right="P", delta=-0.25, now=now),
        make_quote(strike=7500, right="P", delta=-0.50, now=now),
    ]
    pairs = pair_by_strike(quotes)
    prob_close, prob_touch, source_strike, source_delta = probability_for_level(
        7450,
        underlier=7500,
        pairs=pairs,
        strike_step=5.0,
        tau_years=tau,
    )
    d2 = d1(7500, 7450, iv, tau) - iv * math.sqrt(tau)
    expected = normal_cdf(-d2)
    assert prob_close == pytest.approx(expected)
    assert prob_close == pytest.approx(0.01349, rel=1e-3)
    assert prob_touch == pytest.approx(min(1.0, 2 * expected))
    assert prob_close < abs(source_delta)
    assert source_strike == 7450


def test_probability_for_level_falls_back_to_delta_without_iv() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    quotes = [make_quote(strike=7550, right="C", delta=0.20, iv=None, now=now)]
    pairs = pair_by_strike(quotes)
    prob_close, prob_touch, source_strike, _source_delta = probability_for_level(
        7550,
        underlier=7500,
        pairs=pairs,
        strike_step=5.0,
        tau_years=2.0 / (365.0 * 24.0),
    )
    assert prob_close == 0.20
    assert prob_touch == 0.40
    assert source_strike == 7550


def test_expiry_map_populates_level_probabilities_and_flip_zone() -> None:
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    underlier = 7500.0
    quotes = [
        make_quote(strike=7450, right="P", delta=-0.30, gamma=0.004, open_interest=3000, now=now),
        make_quote(strike=7450, right="C", delta=0.70, gamma=0.002, open_interest=500, now=now),
        make_quote(strike=7500, right="P", delta=-0.50, gamma=0.003, open_interest=1000, now=now),
        make_quote(strike=7500, right="C", delta=0.50, gamma=0.003, open_interest=1000, now=now),
        make_quote(strike=7550, right="P", delta=-0.70, gamma=0.002, open_interest=500, now=now),
        make_quote(strike=7550, right="C", delta=0.30, gamma=0.004, open_interest=3000, now=now),
    ]
    expiry = build_expiry_map("20260706", quotes, underlier, as_of=now)
    assert expiry.level_probabilities
    assert expiry.gamma_flip_zone is not None
    left, right = expiry.gamma_flip_zone
    assert left <= right
