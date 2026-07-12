"""Chain grouping, ATM/implied spot, and instrument filters."""

from __future__ import annotations

from collections import defaultdict

from spx_spark.analytics.options.pricing import finite_float, option_mid
from spx_spark.marketdata import InstrumentType, OptionRight, Quote


def chain_implied_spot(pairs: dict[float, dict[OptionRight, Quote]]) -> float | None:
    """SPX spot implied by put-call parity at the synthetic ATM strike.

    S ~= K + C(K) - P(K) at the strike where |C - P| is smallest (r~=0 for
    0DTE/1DTE). This is the option market's own SPX-scale reference, so it
    avoids the ES/SPY basis that otherwise forces gamma/wall suppression
    outside SPX cash hours.
    """
    best: tuple[float, float, float, float] | None = None
    for strike, sides in pairs.items():
        call_mid = option_mid(sides.get(OptionRight.CALL))
        put_mid = option_mid(sides.get(OptionRight.PUT))
        if call_mid is None or put_mid is None:
            continue
        diff = abs(call_mid - put_mid)
        if best is None or diff < best[0]:
            best = (diff, strike, call_mid, put_mid)
    if best is None:
        return None
    _, strike, call_mid, put_mid = best
    return strike + call_mid - put_mid



def is_spxw_option(quote: Quote) -> bool:
    instrument = quote.instrument
    if instrument.instrument_type != InstrumentType.OPTION:
        return False
    if (instrument.underlier or instrument.symbol).upper() != "SPX":
        return False
    trading_class = (instrument.trading_class or instrument.provider_symbol or "").upper()
    return trading_class.startswith("SPXW") or quote.instrument.canonical_id.startswith(
        "option:SPX:SPXW:"
    )


def median_strike_step(strikes: list[float]) -> float:
    if len(strikes) < 2:
        return 5.0
    diffs = [strikes[index + 1] - strikes[index] for index in range(len(strikes) - 1)]
    diffs.sort()
    mid = len(diffs) // 2
    if len(diffs) % 2:
        return diffs[mid]
    return (diffs[mid - 1] + diffs[mid]) / 2.0


def pair_by_strike(quotes: list[Quote]) -> dict[float, dict[OptionRight, Quote]]:
    pairs: dict[float, dict[OptionRight, Quote]] = defaultdict(dict)
    for quote in quotes:
        strike = finite_float(quote.instrument.strike)
        right = quote.instrument.right
        if strike is None or strike <= 0 or right is None:
            continue
        pairs[strike][right] = quote
    return pairs


def is_spy_option(quote: Quote) -> bool:
    instrument = quote.instrument
    if instrument.instrument_type != InstrumentType.OPTION:
        return False
    return (instrument.underlier or instrument.symbol).upper() == "SPY"
