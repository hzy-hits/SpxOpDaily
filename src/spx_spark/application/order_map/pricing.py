"""Pure order-map pricing helpers: ticks, Taylor/BS projection, ETA."""

from __future__ import annotations

import math
from datetime import datetime, timezone

from spx_spark.analytics.greeks.black_scholes import bs_price
from spx_spark.analytics.options.pricing import finite_float
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import OptionRight, Quote

YEAR_SECONDS = 365.0 * 24.0 * 3600.0
# Fraction of remaining time expected to elapse before the touch, as a
# multiple of the Brownian-scaling estimate (distance/EM)^2. Calibrated on
# 2026-07-07: SPX covered 23.5pts (0.91 EM) in ~59% of the remaining session;
# first passages concentrate earlier than the full scaling suggests.
TOUCH_TIME_FRACTION_COEF = 0.6
TOUCH_TIME_FRACTION_MAX = 0.90
# "Sticky strike plus": on a move down, fixed-strike IV rises by roughly this
# multiple of the local smile slope (and falls on the way up).
VOL_SLOPE_BETA = 1.2
MIN_TAU_AT_TOUCH_HOURS = 0.25


def option_tick(premium: float) -> float:
    """SPX option tick: 0.05 below 3.00, 0.10 at/above."""
    return 0.05 if premium < 3.0 else 0.10


def round_to_tick(premium: float) -> float:
    """Round DOWN to tick (limit buy: 挂低一格比挂高一格好)."""
    tick = option_tick(premium)
    return math.floor(premium / tick + 1e-12) * tick


def project_option_price(
    mid: float, delta: float, gamma: float, spot: float, target: float
) -> float:
    """Second-order Taylor projection, clamped to >= 0.05.

    Fallback only: ignores theta and vol dynamics, which for 0DTE makes buy
    limits fill hours early on pure time decay (2026-07-07: projected 16.04
    for 7500C at the wall, actual at touch 12.45). Prefer
    project_option_price_bs when IV is available.
    """
    move = target - spot
    projected = mid + delta * move + 0.5 * gamma * move * move
    return max(0.05, projected)


def expiry_close_utc(expiry: str) -> datetime | None:
    """SPXW PM-settled close, including calendar early-close sessions."""
    try:
        day = datetime.strptime(expiry, "%Y%m%d").date()
    except ValueError:
        return None
    session = DEFAULT_MARKET_CALENDAR.session(day)
    return session.close_at.astimezone(timezone.utc) if session is not None else None


def smile_slope_per_point(
    pairs: dict[float, dict[OptionRight, Quote]],
    right: str,
    strike: float,
    strike_step: float,
) -> float | None:
    """Local dIV/dK (per index point) via least squares on nearby strikes."""
    right_enum = OptionRight.CALL if right == "C" else OptionRight.PUT
    points: list[tuple[float, float]] = []
    for strike_k in sorted(pairs):
        if abs(strike_k - strike) > 3.0 * strike_step:
            continue
        quote = (pairs.get(strike_k) or {}).get(right_enum)
        if quote is None or quote.greeks is None:
            continue
        iv = finite_float(quote.greeks.implied_vol)
        if iv is not None and iv > 0:
            points.append((strike_k, iv))
    if len(points) < 2:
        return None
    n = float(len(points))
    sx = sum(k for k, _ in points)
    sy = sum(v for _, v in points)
    sxx = sum(k * k for k, _ in points)
    sxy = sum(k * v for k, v in points)
    denom = n * sxx - sx * sx
    if abs(denom) < 1e-9:
        return None
    return (n * sxy - sx * sy) / denom


def project_option_price_bs(
    *,
    mid: float,
    iv: float | None,
    strike: float,
    right: str,
    spot: float,
    target: float,
    tau_now_years: float | None,
    em_points: float | None,
    slope_per_point: float | None,
) -> float | None:
    """Reprice the option at the target level with Black-Scholes.

    Unlike the Taylor projection this accounts for:
    - time decay before the touch (touch time estimated by Brownian scaling
      of distance vs remaining expected move);
    - fixed-strike IV drift along the smile (down moves lift IV, up moves
      compress it).
    The result is ratio-anchored to the current market mid so provider IV or
    model mismatch does not shift the base level.
    """
    if iv is None or iv <= 0 or mid <= 0:
        return None
    if tau_now_years is None or tau_now_years <= 0:
        return None
    anchor = bs_price(spot, strike, iv, tau_now_years, right)
    if anchor <= 0.01:
        return None

    distance = abs(target - spot)
    fraction = 0.05
    if em_points is not None and em_points > 0:
        fraction = min(
            max(TOUCH_TIME_FRACTION_COEF * (distance / em_points) ** 2, 0.05),
            TOUCH_TIME_FRACTION_MAX,
        )
    tau_touch = max(
        tau_now_years * (1.0 - fraction),
        MIN_TAU_AT_TOUCH_HOURS * 3600.0 / YEAR_SECONDS,
    )

    iv_touch = iv
    if slope_per_point is not None:
        # slope is negative across the put skew; a down move (spot > target)
        # then raises fixed-strike IV, an up move compresses it.
        iv_touch = iv - VOL_SLOPE_BETA * slope_per_point * (spot - target)
        iv_touch = min(max(iv_touch, 0.5 * iv), 2.5 * iv)

    projected = bs_price(target, strike, iv_touch, tau_touch, right)
    return max(0.05, mid * projected / anchor)


def touch_eta_minutes(
    distance: float,
    em_points: float | None,
    tau_now_years: float | None,
) -> float | None:
    """Expected minutes until first touch, by the same Brownian scaling the BS
    repricing uses. Discipline rule: if the level has not traded after ~2x
    this estimate, the odds of the play have decayed and the order should be
    pulled (theta has eaten the edge even if the level eventually prints)."""
    if tau_now_years is None or tau_now_years <= 0:
        return None
    if em_points is None or em_points <= 0:
        return None
    fraction = min(
        max(TOUCH_TIME_FRACTION_COEF * (distance / em_points) ** 2, 0.05),
        TOUCH_TIME_FRACTION_MAX,
    )
    return fraction * tau_now_years * YEAR_SECONDS / 60.0
