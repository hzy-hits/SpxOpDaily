"""Chain grouping, ATM/implied spot, and instrument filters."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import replace
from datetime import datetime

from spx_spark.analytics.options.quote_policy import (
    option_analytical_lane,
    option_analytical_max_age_seconds,
    option_analytical_pricing_allowed,
    option_field_age_seconds,
    option_field_explicit_delayed,
    option_field_live_entitlement,
    option_field_live_entitlement_source,
    option_field_market_data_type,
    option_field_observed_at,
    option_field_provider,
    option_field_sampling_mode,
)
from spx_spark.analytics.options.pricing import finite_float, option_mid
from spx_spark.marketdata import (
    FUTURE_TIMESTAMP_TOLERANCE_SECONDS,
    InstrumentType,
    OptionGreeks,
    OptionRight,
    Provider,
    Quote,
    as_utc,
)


def chain_implied_spot(pairs: dict[float, dict[OptionRight, Quote]]) -> float | None:
    """SPX spot implied by put-call parity near the synthetic ATM strike.

    S ~= K + C(K) - P(K) at the tightest pairs (r~=0 for 0DTE/1DTE). Like
    parity_forward in application/order_map/pricing.py, take the five pairs
    with the smallest |C - P| and return the median of their per-pair
    implied spots, so one stale or crossed pair cannot shift the whole
    wall/zero-gamma map. This is the option market's own SPX-scale
    reference, so it avoids the ES/SPY basis that otherwise forces
    gamma/wall suppression outside SPX cash hours.
    """
    values: list[tuple[float, float]] = []
    for strike, sides in pairs.items():
        call = sides.get(OptionRight.CALL)
        put = sides.get(OptionRight.PUT)
        if any(
            isinstance(quote.raw, Mapping)
            and quote.raw.get("analytical_only") is True
            and not option_analytical_pricing_allowed(quote)
            for quote in (call, put)
            if quote is not None
        ):
            continue
        call_mid = option_mid(call)
        put_mid = option_mid(put)
        if call_mid is None or put_mid is None:
            continue
        values.append((abs(call_mid - put_mid), strike + call_mid - put_mid))
    if not values:
        return None
    values.sort(key=lambda item: item[0])
    sample = sorted(value for _, value in values[: min(5, len(values))])
    middle = len(sample) // 2
    return sample[middle] if len(sample) % 2 else (sample[middle - 1] + sample[middle]) / 2



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
        observed_at = option_field_observed_at(quote, field="open_interest")
        return (
            bool((finite_float(quote.open_interest) or 0.0) > 0),
            option_field_provider(quote, field="open_interest") is Provider.IBKR,
            observed_at.timestamp(),
        )

    enriched: list[Quote] = []
    for quote in selected_quotes:
        candidates = sources.get(quote.instrument.canonical_id)
        if not candidates:
            enriched.append(quote)
            continue
        source = max(candidates, key=source_key)
        observed_at = option_field_observed_at(source, field="open_interest")
        provider = option_field_provider(source, field="open_interest")
        raw = dict(quote.raw or {})
        raw.update(
            {
                "open_interest_provider": provider.value,
                "open_interest_sampling_mode": option_field_sampling_mode(
                    source,
                    field="open_interest",
                ),
                "open_interest_market_data_type": option_field_market_data_type(
                    source,
                    field="open_interest",
                ),
                "open_interest_explicit_delayed": option_field_explicit_delayed(
                    source,
                    field="open_interest",
                ),
                "open_interest_live_entitlement": option_field_live_entitlement(
                    source,
                    field="open_interest",
                ),
                "open_interest_live_entitlement_source": (
                    option_field_live_entitlement_source(
                        source,
                        field="open_interest",
                    )
                ),
                "open_interest_observed_at": observed_at.isoformat(),
            }
        )
        existing_structure = quote.structure_time
        structure_time = (
            max(as_utc(existing_structure), observed_at)
            if existing_structure is not None
            else observed_at
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


def enrich_option_greeks(
    selected_quotes: list[Quote] | tuple[Quote, ...],
    structural_quotes: list[Quote] | tuple[Quote, ...],
    *,
    as_of: datetime,
    core_max_age_seconds: float,
    rotation_max_age_seconds: float,
    provider_priority: tuple[Provider, ...] = (Provider.SCHWAB, Provider.IBKR),
) -> tuple[Quote, ...]:
    """Attach bounded analytical Greeks independently of pricing provider.

    A current Schwab NBBO may carry a recent IBKR rotation IV, or vice versa,
    without inheriting the other provider's bid/ask.  Field provenance and age
    stay explicit in ``raw`` and no price is interpolated.
    """

    sources: dict[str, list[Quote]] = defaultdict(list)
    rejection_reasons: dict[str, set[str]] = defaultdict(set)
    provider_rank = {provider: index for index, provider in enumerate(provider_priority)}
    at = as_utc(as_of)

    def valid_values(greeks: OptionGreeks | None) -> int:
        if greeks is None:
            return 0
        iv = finite_float(greeks.implied_vol)
        values = (
            finite_float(greeks.delta),
            finite_float(greeks.gamma),
            finite_float(greeks.theta),
            finite_float(greeks.vega),
        )
        return (
            (5 if iv is not None and iv > 0 else 0)
            + sum(value is not None and math.isfinite(value) for value in values)
        )

    for quote in structural_quotes:
        key = quote.instrument.canonical_id
        if valid_values(quote.greeks) <= 0:
            rejection_reasons[key].add("greeks_missing_or_invalid")
            continue
        if not option_field_live_entitlement(quote, field="greeks"):
            reason = option_field_live_entitlement_source(quote, field="greeks")
            rejection_reasons[key].add(
                f"greeks_feed_not_live:{reason or 'entitlement_unknown'}"
            )
            continue
        age = option_field_age_seconds(quote, as_of=at, field="greeks")
        maximum = option_analytical_max_age_seconds(
            quote,
            core_max_age_seconds=core_max_age_seconds,
            rotation_max_age_seconds=rotation_max_age_seconds,
            field="greeks",
        )
        if age < -FUTURE_TIMESTAMP_TOLERANCE_SECONDS:
            rejection_reasons[key].add("greeks_observed_at_in_future")
            continue
        if age > maximum:
            rejection_reasons[key].add(
                f"{option_analytical_lane(quote, field='greeks')}_greeks_stale"
            )
            continue
        sources[key].append(quote)

    def source_key(quote: Quote, *, pricing_provider: Provider) -> tuple[int, bool, int, float]:
        field_provider = option_field_provider(quote, field="greeks")
        return (
            valid_values(quote.greeks),
            field_provider is pricing_provider,
            -provider_rank.get(field_provider, len(provider_rank)),
            option_field_observed_at(quote, field="greeks").timestamp(),
        )

    enriched: list[Quote] = []
    for quote in selected_quotes:
        key = quote.instrument.canonical_id
        candidates = sources.get(key, [])
        raw = dict(quote.raw or {})
        if not candidates:
            raw.update(
                {
                    "greeks_analytical_allowed": False,
                    "greeks_rejection_reasons": sorted(
                        rejection_reasons.get(key) or {"greeks_source_unavailable"}
                    ),
                }
            )
            enriched.append(replace(quote, greeks=None, raw=raw))
            continue
        source = max(
            candidates,
            key=lambda item: source_key(item, pricing_provider=quote.provider),
        )
        observed_at = option_field_observed_at(source, field="greeks")
        age = (at - observed_at).total_seconds()
        lane = option_analytical_lane(source, field="greeks")
        provider = option_field_provider(source, field="greeks")
        maximum = option_analytical_max_age_seconds(
            source,
            core_max_age_seconds=core_max_age_seconds,
            rotation_max_age_seconds=rotation_max_age_seconds,
            field="greeks",
        )
        raw.update(
            {
                "greeks_analytical_allowed": True,
                "greeks_provider": provider.value,
                "greeks_sampling_mode": option_field_sampling_mode(
                    source,
                    field="greeks",
                ),
                "greeks_market_data_type": option_field_market_data_type(
                    source,
                    field="greeks",
                ),
                "greeks_explicit_delayed": option_field_explicit_delayed(
                    source,
                    field="greeks",
                ),
                "greeks_live_entitlement": True,
                "greeks_live_entitlement_source": (
                    option_field_live_entitlement_source(
                        source,
                        field="greeks",
                    )
                ),
                "greeks_observed_at": observed_at.isoformat(),
                "greeks_observed_age_seconds": age,
                "greeks_analytical_lane": lane,
                "greeks_analytical_max_age_seconds": maximum,
                "greeks_rejection_reasons": [],
            }
        )
        existing_structure = quote.structure_time
        structure_time = (
            max(as_utc(existing_structure), observed_at)
            if existing_structure is not None
            else observed_at
        )
        enriched.append(
            replace(
                quote,
                greeks=source.greeks,
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
