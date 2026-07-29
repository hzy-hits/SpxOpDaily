"""Live SPX/ES anchor sampling for the shock monitor."""

from __future__ import annotations

from datetime import datetime

from spx_spark.application.shock.models import IntradayShockSettings, PriceSample
from spx_spark.config import NY_TZ
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import (
    FUTURE_TIMESTAMP_TOLERANCE_SECONDS,
    MarketDataQuality,
    Provider,
    Quote,
    as_utc,
    instrument_matches_id,
)
from spx_spark.storage import LatestState, configured_quote_use_decision

def _live_price_observation(
    quote: Quote,
) -> tuple[float, datetime, str] | None:
    if (
        quote.bid is not None
        and quote.mid is not None
        and quote.ask is not None
        and 0 < quote.bid <= quote.mid <= quote.ask
        and quote.quote_time is not None
    ):
        return float(quote.mid), as_utc(quote.quote_time), "mid"
    if quote.last is not None and quote.last > 0 and quote.trade_time is not None:
        return float(quote.last), as_utc(quote.trade_time), "last"
    return None


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


def live_es_sample(
    state: LatestState,
    settings: IntradayShockSettings,
) -> tuple[tuple[datetime, float, str] | None, str | None]:
    """Resolve one fresh live ES quote without requiring overnight SPX."""

    first_rejection: str | None = None
    for provider_name in settings.anchor_provider_priority:
        provider = Provider(provider_name)
        es = _latest_provider_quote(state, "future:ES", provider)
        if es is None:
            continue
        if (
            provider == Provider.SCHWAB
            and settings.require_schwab_streaming_anchors
            and es.sampling_mode != "schwab_stream"
        ):
            first_rejection = first_rejection or "schwab_es_not_streaming"
            continue
        decision = configured_quote_use_decision(es, as_of=state.as_of)
        observation = _live_price_observation(es)
        if (
            not decision.alert_allowed
            or decision.feed_mode != MarketDataQuality.LIVE
            or observation is None
        ):
            first_rejection = first_rejection or "non_live_or_stale_es"
            continue
        price, source_at, _price_kind = observation
        source_age = (as_utc(state.as_of) - source_at).total_seconds()
        if source_age < -FUTURE_TIMESTAMP_TOLERANCE_SECONDS:
            first_rejection = first_rejection or "future_es_anchor"
            continue
        if source_age > settings.max_es_age_seconds:
            first_rejection = first_rejection or "stale_es_anchor"
            continue
        return (source_at, float(price), provider.value), None
    return None, first_rejection or "missing_es"


def _latest_provider_quote(
    state: LatestState,
    instrument_id: str,
    provider: Provider,
) -> Quote | None:
    matches = [
        quote
        for quote in state.quotes
        if instrument_matches_id(quote.instrument, instrument_id)
        and quote.provider == provider
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
    spx_observation = _live_price_observation(spx)
    es_observation = _live_price_observation(es)
    if spx_observation is None or es_observation is None:
        return None, "missing_anchor_price"
    spx_price, spx_at, _spx_kind = spx_observation
    es_price, es_at, _es_kind = es_observation
    spx_age = (as_utc(state.as_of) - spx_at).total_seconds()
    es_age = (as_utc(state.as_of) - es_at).total_seconds()
    if spx_age < -FUTURE_TIMESTAMP_TOLERANCE_SECONDS:
        return None, "future_spx_anchor"
    if es_age < -FUTURE_TIMESTAMP_TOLERANCE_SECONDS:
        return None, "future_es_anchor"
    if spx_age > settings.max_spx_age_seconds:
        return None, "stale_spx_anchor"
    if es_age > settings.max_es_age_seconds:
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


def gth_session_date(at: datetime) -> str | None:
    if not DEFAULT_MARKET_CALENDAR.is_spx_gth_open(at):
        return None
    return DEFAULT_MARKET_CALENDAR.research_expiry(at).isoformat()
