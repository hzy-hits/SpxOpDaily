"""Separate feed mode from transport freshness for quote actionability."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from spx_spark.marketdata import (
    FUTURE_TIMESTAMP_TOLERANCE_SECONDS,
    MarketDataQuality,
    Quote,
    QuoteFreshness,
    as_utc,
    quality_from_market_data_type,
)


@dataclass(frozen=True)
class QuoteUseDecision:
    feed_mode: MarketDataQuality
    freshness: QuoteFreshness
    research_usable: bool
    alert_allowed: bool
    pricing_allowed: bool
    reason: str


def quote_use_decision(
    quote: Quote,
    *,
    as_of: datetime,
    stale_after_seconds: float = 15.0,
    delayed_stale_after_seconds: float = 60.0,
    allow_frozen: bool = False,
    prefer_quote_time: bool = False,
) -> QuoteUseDecision:
    """Separate feed mode from transport freshness and fail closed for actionability."""

    feed_mode = quality_from_market_data_type(quote.market_data_type) or quote.quality
    known_feed = feed_mode in {
        MarketDataQuality.LIVE,
        MarketDataQuality.FROZEN,
        MarketDataQuality.DELAYED,
        MarketDataQuality.DELAYED_FROZEN,
    }
    research_feed = known_feed or feed_mode == MarketDataQuality.SYNTHETIC
    research_usable = quote.has_price and research_feed
    if not quote.has_price:
        return QuoteUseDecision(
            feed_mode=feed_mode,
            freshness=QuoteFreshness.UNKNOWN,
            research_usable=False,
            alert_allowed=False,
            pricing_allowed=False,
            reason="quote_has_no_price",
        )
    if quote.quality == MarketDataQuality.STALE and not known_feed:
        return QuoteUseDecision(
            feed_mode=feed_mode,
            freshness=QuoteFreshness.STALE,
            research_usable=False,
            alert_allowed=False,
            pricing_allowed=False,
            reason="transport_stale",
        )
    if quote.quality in {MarketDataQuality.MISSING, MarketDataQuality.ERROR} or not research_feed:
        return QuoteUseDecision(
            feed_mode=feed_mode,
            freshness=QuoteFreshness.UNKNOWN,
            research_usable=False,
            alert_allowed=False,
            pricing_allowed=False,
            reason=f"feed_quality_{quote.quality.value}",
        )

    transport_time = None if prefer_quote_time else quote.last_update_at
    if transport_time is None and feed_mode in {
        MarketDataQuality.LIVE,
        MarketDataQuality.SYNTHETIC,
    }:
        transport_time = quote.quote_time or quote.trade_time
    if transport_time is None:
        return QuoteUseDecision(
            feed_mode=feed_mode,
            freshness=QuoteFreshness.UNKNOWN,
            research_usable=research_usable,
            alert_allowed=False,
            pricing_allowed=False,
            reason="transport_timestamp_missing",
        )

    age_seconds = (as_utc(as_of) - as_utc(transport_time)).total_seconds()
    if age_seconds < -FUTURE_TIMESTAMP_TOLERANCE_SECONDS:
        return QuoteUseDecision(
            feed_mode=feed_mode,
            freshness=QuoteFreshness.UNKNOWN,
            research_usable=False,
            alert_allowed=False,
            pricing_allowed=False,
            reason="transport_timestamp_in_future",
        )
    threshold = (
        delayed_stale_after_seconds
        if feed_mode in {MarketDataQuality.DELAYED, MarketDataQuality.DELAYED_FROZEN}
        else stale_after_seconds
    )
    freshness = (
        QuoteFreshness.STALE
        if quote.quality == MarketDataQuality.STALE or age_seconds > threshold
        else QuoteFreshness.FRESH
    )
    actionable_feed = feed_mode == MarketDataQuality.LIVE or (
        allow_frozen and feed_mode == MarketDataQuality.FROZEN
    )
    actionable = freshness == QuoteFreshness.FRESH and actionable_feed
    if freshness == QuoteFreshness.STALE:
        research_usable = False
    return QuoteUseDecision(
        feed_mode=feed_mode,
        freshness=freshness,
        research_usable=research_usable,
        alert_allowed=actionable,
        pricing_allowed=actionable,
        reason=(
            f"transport_stale_after_{threshold:g}s"
            if freshness == QuoteFreshness.STALE
            else f"fresh_{feed_mode.value}"
        ),
    )
