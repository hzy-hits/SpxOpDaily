"""Black-Scholes kernels using the project's r=0, q=0 convention."""

from __future__ import annotations

import math

MINUTES_PER_YEAR = 525_600


def normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def normal_pdf(value: float) -> float:
    return math.exp(-0.5 * value * value) / math.sqrt(2.0 * math.pi)


def intrinsic_value(spot: float, strike: float, right: str) -> float:
    if right == "C":
        return max(0.0, spot - strike)
    return max(0.0, strike - spot)


def d1(spot: float, strike: float, iv: float, tau_years: float) -> float:
    root_t = math.sqrt(tau_years)
    return (math.log(spot / strike) + 0.5 * iv * iv * tau_years) / (iv * root_t)


def bs_price(spot: float, strike: float, iv: float, tau_years: float, right: str) -> float:
    """Return the r=0, q=0 Black-Scholes price."""

    intrinsic = intrinsic_value(spot, strike, right)
    if spot <= 0 or strike <= 0 or tau_years <= 0 or iv <= 0:
        return intrinsic
    d1_value = d1(spot, strike, iv, tau_years)
    d2_value = d1_value - iv * math.sqrt(tau_years)
    if right == "C":
        return max(
            intrinsic,
            spot * normal_cdf(d1_value) - strike * normal_cdf(d2_value),
        )
    return max(
        intrinsic,
        strike * normal_cdf(-d2_value) - spot * normal_cdf(-d1_value),
    )


def black76_price(
    forward: float,
    strike: float,
    iv: float,
    tau_years: float,
    right: str,
    *,
    discount_factor: float = 1.0,
) -> float:
    """European option price on a forward under the Black-76 convention."""

    intrinsic = discount_factor * intrinsic_value(forward, strike, right)
    if forward <= 0 or strike <= 0 or tau_years <= 0 or iv <= 0 or not 0 < discount_factor <= 1:
        return intrinsic
    root_t = math.sqrt(tau_years)
    d1_value = (math.log(forward / strike) + 0.5 * iv * iv * tau_years) / (iv * root_t)
    d2_value = d1_value - iv * root_t
    if right == "C":
        value = forward * normal_cdf(d1_value) - strike * normal_cdf(d2_value)
    else:
        value = strike * normal_cdf(-d2_value) - forward * normal_cdf(-d1_value)
    return max(intrinsic, discount_factor * value)


def bs_delta(spot: float, strike: float, iv: float, tau_years: float, right: str) -> float:
    if tau_years <= 0 or iv <= 0:
        if spot > strike:
            return 1.0 if right == "C" else 0.0
        if spot < strike:
            return 0.0 if right == "C" else -1.0
        return 0.5 if right == "C" else -0.5
    call_delta = normal_cdf(d1(spot, strike, iv, tau_years))
    return call_delta if right == "C" else call_delta - 1.0


def bs_gamma(spot: float, strike: float, iv: float, tau_years: float) -> float:
    if spot <= 0 or strike <= 0 or iv <= 0 or tau_years <= 0:
        return 0.0
    return normal_pdf(d1(spot, strike, iv, tau_years)) / (spot * iv * math.sqrt(tau_years))


def bs_vega(spot: float, strike: float, iv: float, tau_years: float) -> float:
    """Return price change per 1.00 absolute volatility, before unit scaling."""

    if spot <= 0 or strike <= 0 or iv <= 0 or tau_years <= 0:
        return 0.0
    return spot * normal_pdf(d1(spot, strike, iv, tau_years)) * math.sqrt(tau_years)
