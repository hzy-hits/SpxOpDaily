"""Option quote accessors, BS gamma wrapper, and time-to-expiry."""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any

from spx_spark.analytics.greeks.black_scholes import bs_gamma as _core_bs_gamma
from spx_spark.analytics.options.constants import (
    BAD_QUALITIES,
    _MIN_TIME_TO_EXPIRY_YEARS,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import OptionRight, Quote


def finite_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(out):
        return None
    return out


def option_mid(quote: Quote | None) -> float | None:
    if quote is None or quote.quality in BAD_QUALITIES:
        return None
    return quote.mid or quote.effective_price


def option_iv(quote: Quote | None) -> float | None:
    if quote is None or quote.quality in BAD_QUALITIES or quote.greeks is None:
        return None
    value = finite_float(quote.greeks.implied_vol)
    return value if value is not None and value > 0 else None


def option_gamma(quote: Quote) -> float | None:
    if quote.quality in BAD_QUALITIES or quote.greeks is None:
        return None
    value = finite_float(quote.greeks.gamma)
    return value if value is not None and value > 0 else None



def usable_delta(quote: Quote | None) -> float | None:
    if quote is None or quote.quality in BAD_QUALITIES or quote.greeks is None:
        return None
    value = finite_float(quote.greeks.delta)
    if value is None or not math.isfinite(value):
        return None
    return value



def weighted_mean(items: list[tuple[float, float]]) -> float | None:
    cleaned = [(value, max(weight, 0.0)) for value, weight in items if value > 0 and weight >= 0]
    denom = sum(weight for _value, weight in cleaned)
    if denom <= 0:
        return None
    return sum(value * weight for value, weight in cleaned) / denom


def bs_gamma(spot: float, strike: float, iv: float, t_years: float) -> float | None:
    """Black-Scholes gamma,r=q=0."""
    if spot <= 0 or strike <= 0 or iv <= 0 or t_years <= 0:
        return None
    return _core_bs_gamma(spot, strike, iv, t_years)



def time_to_expiry_years(expiry: str, *, as_of: datetime) -> float:
    """Years to the calendar session close, floored at fifteen minutes."""
    expiry_date = datetime.strptime(expiry, "%Y%m%d").date()
    session = DEFAULT_MARKET_CALENDAR.session(expiry_date)
    if session is None:
        return _MIN_TIME_TO_EXPIRY_YEARS
    delta_seconds = (session.close_at - as_of.astimezone(session.close_at.tzinfo)).total_seconds()
    if delta_seconds <= 0:
        return _MIN_TIME_TO_EXPIRY_YEARS
    years = delta_seconds / (365.0 * 24.0 * 3600.0)
    return max(years, _MIN_TIME_TO_EXPIRY_YEARS)


def interpolated_atm_iv(
    pairs: dict[float, dict[OptionRight, Quote]],
    underlier: float | None,
) -> float | None:
    """Linearly interpolate ATM IV on each side; average call and put."""
    if underlier is None:
        return None

    def side_iv(right: OptionRight) -> float | None:
        strikes_with_iv: list[tuple[float, float]] = []
        for strike, pair in pairs.items():
            iv = option_iv(pair.get(right))
            if iv is not None:
                strikes_with_iv.append((strike, iv))
        if not strikes_with_iv:
            return None
        below = [(strike, iv) for strike, iv in strikes_with_iv if strike <= underlier]
        above = [(strike, iv) for strike, iv in strikes_with_iv if strike >= underlier]
        if below and above:
            strike_low, iv_low = max(below, key=lambda item: item[0])
            strike_high, iv_high = min(above, key=lambda item: item[0])
            if strike_high == strike_low:
                return iv_low
            weight = (underlier - strike_low) / (strike_high - strike_low)
            return iv_low + weight * (iv_high - iv_low)
        nearest_strike, nearest_iv = min(strikes_with_iv, key=lambda item: abs(item[0] - underlier))
        return nearest_iv

    ivs = [iv for iv in (side_iv(OptionRight.CALL), side_iv(OptionRight.PUT)) if iv is not None]
    if not ivs:
        return None
    return sum(ivs) / len(ivs)


def wing_iv_at_delta(quotes_one_side: list[Quote], target_abs_delta: float = 0.25) -> float | None:
    """Return IV of the quote whose |delta| is closest to target, if within 0.15."""
    candidates: list[tuple[float, float]] = []
    for quote in quotes_one_side:
        delta = usable_delta(quote)
        iv = option_iv(quote)
        if delta is None or iv is None:
            continue
        distance = abs(abs(delta) - target_abs_delta)
        candidates.append((distance, iv))
    if not candidates:
        return None
    distance, iv = min(candidates, key=lambda item: item[0])
    if distance > 0.15:
        return None
    return iv
