"""Quote quality gates and coverage summaries."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone

from spx_spark.analytics.options.constants import (
    BAD_QUALITIES,
    STRUCTURE_MAX_AGE_SECONDS,
    _HARD_BAD_QUALITIES,
)
from spx_spark.analytics.options.models import OptionCoverage
from spx_spark.analytics.options.pricing import (
    finite_float,
    option_gamma,
    option_iv,
    option_mid,
)
from spx_spark.marketdata import MarketDataQuality, Quote


def structure_quality_ok(quote: Quote, *, as_of: datetime | None = None) -> bool:
    """Quality gate for structure features: stale-but-recent samples pass."""
    if quote.quality not in BAD_QUALITIES:
        return True
    if quote.quality in _HARD_BAD_QUALITIES:
        return False
    age_ms = quote.quote_age_ms(as_of or datetime.now(tz=timezone.utc))
    if age_ms is None:
        return False
    return age_ms <= STRUCTURE_MAX_AGE_SECONDS * 1000.0


def option_gamma_structural(quote: Quote, *, as_of: datetime | None = None) -> float | None:
    if quote.greeks is None or not structure_quality_ok(quote, as_of=as_of):
        return None
    value = finite_float(quote.greeks.gamma)
    return value if value is not None and value > 0 else None


def build_coverage(quotes: list[Quote], *, as_of: datetime) -> OptionCoverage:
    quality_counts = Counter(quote.quality for quote in quotes)
    spreads = [quote.spread_bps for quote in quotes if quote.spread_bps is not None]
    ages = [quote.quote_age_ms(as_of) for quote in quotes]
    known_ages = [age for age in ages if age is not None]
    return OptionCoverage(
        total=len(quotes),
        live=quality_counts[MarketDataQuality.LIVE],
        stale=quality_counts[MarketDataQuality.STALE],
        delayed=quality_counts[MarketDataQuality.DELAYED]
        + quality_counts[MarketDataQuality.DELAYED_FROZEN],
        unknown_age=sum(1 for age in ages if age is None),
        max_age_ms=max(known_ages) if known_ages else None,
        with_bid_ask=sum(1 for quote in quotes if quote.mid is not None),
        with_mid=sum(1 for quote in quotes if option_mid(quote) is not None),
        with_iv=sum(1 for quote in quotes if option_iv(quote) is not None),
        with_delta=sum(
            1 for quote in quotes if quote.greeks is not None and quote.greeks.delta is not None
        ),
        with_gamma=sum(1 for quote in quotes if option_gamma(quote) is not None),
        with_theta=sum(
            1 for quote in quotes if quote.greeks is not None and quote.greeks.theta is not None
        ),
        with_vega=sum(
            1 for quote in quotes if quote.greeks is not None and quote.greeks.vega is not None
        ),
        with_open_interest=sum(
            1 for quote in quotes if quote.open_interest is not None and quote.open_interest > 0
        ),
        avg_spread_bps=sum(spreads) / len(spreads) if spreads else None,
    )
