"""Live SPX/ES anchor sampling for the shock monitor."""

from __future__ import annotations

from datetime import datetime

from spx_spark.application.shock.models import IntradayShockSettings, PriceSample
from spx_spark.config import NY_TZ
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import MarketDataQuality, Provider, Quote, as_utc
from spx_spark.storage import LatestState, configured_quote_use_decision

def _quote_source_at(quote: Quote) -> datetime:
    return as_utc(quote.quote_time or quote.trade_time or quote.received_at)


def synchronized_live_sample(
    state: LatestState,
    settings: IntradayShockSettings,
) -> tuple[PriceSample | None, str | None]:
    first_rejection: str | None = None
    found_pair = False
    for provider_name in settings.anchor_provider_priority:
        provider = Provider(provider_name)
        spx = _latest_provider_quote(state, "index:SPX", provider)
        es = _latest_provider_quote(state, "future:ES", provider)
        if spx is None or es is None:
            continue
        found_pair = True
        if (
            provider == Provider.SCHWAB
            and settings.require_schwab_streaming_anchors
            and (
                spx.sampling_mode != "schwab_stream"
                or es.sampling_mode != "schwab_stream"
            )
        ):
            if first_rejection is None:
                first_rejection = "schwab_anchor_not_streaming"
            continue
        sample, rejection = _validated_anchor_pair(state, settings, spx=spx, es=es)
        if sample is not None:
            return sample, None
        if first_rejection is None:
            first_rejection = rejection
    if found_pair:
        return None, first_rejection or "non_live_or_stale_anchor"
    return None, "missing_spx_or_es"


def _latest_provider_quote(
    state: LatestState,
    instrument_id: str,
    provider: Provider,
) -> Quote | None:
    matches = [
        quote
        for quote in state.quotes
        if quote.instrument.canonical_id == instrument_id and quote.provider == provider
    ]
    if not matches:
        return None
    return max(matches, key=lambda quote: as_utc(quote.received_at))


def _validated_anchor_pair(
    state: LatestState,
    settings: IntradayShockSettings,
    *,
    spx: Quote,
    es: Quote,
) -> tuple[PriceSample | None, str | None]:
    spx_decision = configured_quote_use_decision(spx, as_of=state.as_of)
    es_decision = configured_quote_use_decision(es, as_of=state.as_of)
    if (
        not spx_decision.alert_allowed
        or not es_decision.alert_allowed
        or spx_decision.feed_mode != MarketDataQuality.LIVE
        or es_decision.feed_mode != MarketDataQuality.LIVE
    ):
        return None, "non_live_or_stale_anchor"
    spx_price = spx.effective_price
    es_price = es.effective_price
    if spx_price is None or es_price is None or spx_price <= 0 or es_price <= 0:
        return None, "missing_anchor_price"
    spx_at = _quote_source_at(spx)
    es_at = _quote_source_at(es)
    if (as_utc(state.as_of) - spx_at).total_seconds() > settings.max_spx_age_seconds:
        return None, "stale_spx_anchor"
    if (as_utc(state.as_of) - es_at).total_seconds() > settings.max_es_age_seconds:
        return None, "stale_es_anchor"
    if abs((spx_at - es_at).total_seconds()) > settings.max_anchor_skew_seconds:
        return None, "anchor_timestamp_skew"
    return (
        PriceSample(
            at=max(spx_at, es_at),
            spx=float(spx_price),
            es=float(es_price),
            spx_source_at=spx_at,
            es_source_at=es_at,
            provider=spx.provider.value,
        ),
        None,
    )


def rth_session_date(at: datetime) -> str | None:
    at_et = at.astimezone(NY_TZ)
    session = DEFAULT_MARKET_CALENDAR.session(at_et.date())
    if session is None or not (session.open_at <= at_et < session.close_at):
        return None
    return session.trading_date.isoformat()
