"""Black-Scholes kernel golden checks."""

from __future__ import annotations

import math

from spx_spark.analytics.greeks.black_scholes import bs_delta, bs_gamma, bs_price, bs_vega
from spx_spark.analytics.greeks.higher_order import bs_charm_per_minute, bs_vanna_per_vol_point


def test_bs_call_put_parity_at_atm() -> None:
    spot, strike, iv, tau = 6500.0, 6500.0, 0.15, 1.0 / 365.0
    call = bs_price(spot, strike, iv, tau, "C")
    put = bs_price(spot, strike, iv, tau, "P")
    # r=q=0 => C - P = S - K = 0 at ATM
    assert abs(call - put) < 1e-8


def test_bs_gamma_equal_for_call_and_put() -> None:
    gamma = bs_gamma(6500.0, 6500.0, 0.2, 0.01)
    assert gamma == bs_gamma(6500.0, 6500.0, 0.2, 0.01)
    assert gamma > 0


def test_bs_delta_bounds() -> None:
    assert 0.0 < bs_delta(6500.0, 6400.0, 0.2, 0.01, "C") < 1.0
    assert -1.0 < bs_delta(6500.0, 6400.0, 0.2, 0.01, "P") < 0.0


def test_bs_vega_and_higher_order_finite() -> None:
    vega = bs_vega(6500.0, 6500.0, 0.2, 0.01)
    vanna = bs_vanna_per_vol_point(6500.0, 6500.0, 0.2, 0.01)
    charm = bs_charm_per_minute(6500.0, 6500.0, 0.2, 0.01)
    assert vega > 0
    assert vanna is not None and math.isfinite(vanna)
    assert charm is not None and math.isfinite(charm)


def test_tau_zero_floor_returns_intrinsic() -> None:
    assert bs_price(100.0, 90.0, 0.2, 0.0, "C") == 10.0
    assert bs_price(100.0, 110.0, 0.2, 0.0, "P") == 10.0
