"""Level close/touch probability: risk-neutral N(d2) with a delta-anchor fallback."""

from __future__ import annotations

import math

from spx_spark.analytics.greeks.black_scholes import d1, normal_cdf
from spx_spark.analytics.options.pricing import option_iv, usable_delta
from spx_spark.marketdata import OptionRight, Quote


def probability_for_level(
    level: float,
    *,
    underlier: float,
    pairs: dict[float, dict[OptionRight, Quote]],
    strike_step: float,
    tau_years: float | None = None,
) -> tuple[float | None, float | None, float | None, float | None]:
    """Probability that the underlier closes beyond / touches ``level``.

    The anchor is the strike nearest to ``level`` (within 2 * strike_step)
    with a usable delta on the level's side: calls for levels at/above the
    underlier, puts below.

    ``prob_close_beyond`` has two conventions:
    - preferred: the risk-neutral expiry probability N(d2) (call side) or
      N(-d2) (put side) at K=level, using the anchor quote's IV; requires
      the anchor to carry IV and ``tau_years`` to be passed by the caller;
    - fallback: |delta| of the anchor, i.e. N(d1), which systematically
      overstates the OTM side because d1 > d2.

    ``prob_touch`` = min(1, 2 x prob_close_beyond) is the zero-drift
    reflection (first-passage) heuristic: for driftless Brownian motion the
    hitting probability is twice the terminal probability.
    """
    right = OptionRight.CALL if level >= underlier else OptionRight.PUT
    candidates: list[tuple[float, float, float]] = []
    for strike, pair in pairs.items():
        delta = usable_delta(pair.get(right))
        if delta is None:
            continue
        candidates.append((strike, abs(strike - level), delta))
    if not candidates:
        return (None, None, None, None)
    source_strike, distance, source_delta = min(candidates, key=lambda item: item[1])
    if distance > 2 * strike_step:
        return (None, None, None, None)
    prob_close_beyond = _prob_close_beyond_nd2(
        level,
        underlier=underlier,
        right=right,
        iv=option_iv((pairs.get(source_strike) or {}).get(right)),
        tau_years=tau_years,
    )
    if prob_close_beyond is None:
        # Delta-anchor fallback: |delta| ~ N(d1); overstates OTM vs the N(d2) target.
        prob_close_beyond = max(
            0.0, min(1.0, source_delta if right == OptionRight.CALL else abs(source_delta))
        )
    # Zero-drift reflection heuristic: first-passage probability ~ 2x terminal.
    prob_touch = min(1.0, 2 * prob_close_beyond)
    return (prob_close_beyond, prob_touch, source_strike, source_delta)


def _prob_close_beyond_nd2(
    level: float,
    *,
    underlier: float,
    right: OptionRight,
    iv: float | None,
    tau_years: float | None,
) -> float | None:
    """Risk-neutral N(d2) expiry probability, or None when inputs are missing."""

    if iv is None or iv <= 0 or tau_years is None or tau_years <= 0:
        return None
    if underlier <= 0 or level <= 0:
        return None
    d2_value = d1(underlier, level, iv, tau_years) - iv * math.sqrt(tau_years)
    probability = normal_cdf(d2_value) if right == OptionRight.CALL else normal_cdf(-d2_value)
    return max(0.0, min(1.0, probability))
