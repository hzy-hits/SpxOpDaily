"""Field-clock-safe displayed-quote gate for candidate lifecycles."""

from __future__ import annotations

from datetime import datetime, timezone

from spx_spark.marketdata import MarketDataQuality, Quote, quote_use_decision


def candidate_displayed_quote_decision(
    quote: Quote,
    *,
    now: datetime,
    max_age_seconds: float,
) -> tuple[bool, str, dict[str, object]]:
    """Authorize displayed bid/ask only from its own fresh quote clock."""

    source_at = quote.quote_time
    transport_at = quote.last_update_at or quote.received_at
    source_age = (
        (_utc(now) - _utc(source_at)).total_seconds()
        if source_at is not None
        else None
    )
    transport_age = (_utc(now) - _utc(transport_at)).total_seconds()
    use = quote_use_decision(
        quote,
        as_of=now,
        stale_after_seconds=max_age_seconds,
        delayed_stale_after_seconds=max_age_seconds,
    )
    valid_nbbo = bool(
        quote.bid is not None
        and quote.mid is not None
        and quote.ask is not None
        and 0 <= quote.bid <= quote.mid <= quote.ask
    )
    if source_age is None:
        reason = "candidate_quote_source_timestamp_unavailable"
    elif source_age < 0:
        reason = "candidate_quote_source_timestamp_in_future"
    elif transport_age < 0:
        reason = "candidate_quote_transport_timestamp_in_future"
    elif source_age > max_age_seconds:
        reason = "candidate_quote_source_stale"
    elif transport_age > max_age_seconds:
        reason = "candidate_quote_transport_stale"
    elif quote.quality is not MarketDataQuality.LIVE:
        reason = "candidate_quote_not_live"
    elif not valid_nbbo:
        reason = "candidate_quote_invalid_nbbo"
    elif not use.pricing_allowed:
        reason = f"candidate_quote_not_pricing_allowed:{use.reason}"
    else:
        reason = "candidate_displayed_quote_fresh"
    allowed = reason == "candidate_displayed_quote_fresh"
    return (
        allowed,
        reason,
        {
            "quote_source_at": source_at.isoformat() if source_at else None,
            "quote_transport_at": transport_at.isoformat(),
            "quote_source_age_seconds": source_age,
            "quote_transport_age_seconds": transport_age,
        },
    )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
