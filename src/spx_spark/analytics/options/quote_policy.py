"""Session-aware quote normalization for non-execution option analytics."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime

from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import MarketDataQuality, Provider, Quote, as_utc


def gth_analytical_quote(
    quote: Quote,
    *,
    as_of: datetime,
    max_age_seconds: float,
) -> Quote:
    """Treat a recent IBKR rotation row as live for analytics, never execution."""

    if (
        not DEFAULT_MARKET_CALENDAR.is_spx_gth_open(as_of)
        or quote.provider is not Provider.IBKR
        or quote.quality is not MarketDataQuality.STALE
        or str(quote.market_data_type or "").lower() not in {"1", "live"}
    ):
        return quote
    source_at = quote.quote_time or quote.trade_time or quote.received_at
    transport_at = quote.last_update_at or quote.received_at
    source_age = (as_utc(as_of) - as_utc(source_at)).total_seconds()
    transport_age = (as_utc(as_of) - as_utc(transport_at)).total_seconds()
    if not 0 <= max(source_age, transport_age) <= max_age_seconds:
        return quote
    return replace(quote, quality=MarketDataQuality.LIVE)
