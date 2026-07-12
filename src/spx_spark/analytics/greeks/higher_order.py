"""Higher-order Black-Scholes Greeks (vanna, charm)."""

from __future__ import annotations

import math

from spx_spark.analytics.greeks.black_scholes import MINUTES_PER_YEAR, d1, normal_pdf


def bs_vanna_per_vol_point(
    spot: float,
    strike: float,
    iv: float,
    tau_years: float,
) -> float | None:
    if spot <= 0 or strike <= 0 or iv <= 0 or tau_years <= 0:
        return None
    sqrt_t = math.sqrt(tau_years)
    d1_value = d1(spot, strike, iv, tau_years)
    d2_value = d1_value - iv * sqrt_t
    return (-normal_pdf(d1_value) * d2_value / iv) * 0.01


def bs_charm_per_minute(
    spot: float,
    strike: float,
    iv: float,
    tau_years: float,
) -> float | None:
    if spot <= 0 or strike <= 0 or iv <= 0 or tau_years <= 0:
        return None
    sqrt_t = math.sqrt(tau_years)
    d1_value = d1(spot, strike, iv, tau_years)
    d2_value = d1_value - iv * sqrt_t
    return (normal_pdf(d1_value) * d2_value / (2.0 * tau_years)) / MINUTES_PER_YEAR
