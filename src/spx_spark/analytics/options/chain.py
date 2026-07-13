"""Chain grouping, ATM/implied spot, and instrument filters."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import replace

from spx_spark.analytics.options.pricing import finite_float, option_mid
from spx_spark.marketdata import InstrumentType, OptionRight, Provider, Quote


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


def enrich_open_interest(
    selected_quotes: list[Quote] | tuple[Quote, ...],
    structural_quotes: list[Quote] | tuple[Quote, ...],
) -> tuple[Quote, ...]:
    """Attach OI independently of the provider chosen for current pricing.

    SPX option OI is a session-level structural field, while bid/ask is a
    rapidly expiring field. IBKR's rotating hot lane can therefore retain
    valid OI after its quote leaves the live window, and a fresh Schwab quote
    can safely carry that OI without inheriting IBKR's stale price.
    """

    sources: dict[str, list[Quote]] = defaultdict(list)
    for quote in structural_quotes:
        if quote.open_interest is not None:
            sources[quote.instrument.canonical_id].append(quote)

    def source_key(quote: Quote) -> tuple[bool, bool, float]:
        observed_at = quote.structure_time or quote.received_at
        return (
            bool((finite_float(quote.open_interest) or 0.0) > 0),
            quote.provider is Provider.IBKR,
            observed_at.timestamp(),
        )

    enriched: list[Quote] = []
    for quote in selected_quotes:
        candidates = sources.get(quote.instrument.canonical_id)
        if not candidates:
            enriched.append(quote)
            continue
        source = max(candidates, key=source_key)
        structure_time = source.structure_time or source.received_at
        raw = dict(quote.raw or {})
        raw.update(
            {
                "open_interest_provider": source.provider.value,
                "open_interest_observed_at": structure_time.isoformat(),
            }
        )
        enriched.append(
            replace(
                quote,
                open_interest=source.open_interest,
                structure_time=structure_time,
                raw=raw,
            )
        )
    return tuple(enriched)


def is_spy_option(quote: Quote) -> bool:
    instrument = quote.instrument
    if instrument.instrument_type != InstrumentType.OPTION:
        return False
    return (instrument.underlier or instrument.symbol).upper() == "SPY"
