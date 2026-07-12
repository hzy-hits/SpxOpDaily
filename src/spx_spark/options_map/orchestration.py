"""LatestState orchestration for SPXW options maps (not pure analytics)."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime

from spx_spark.analytics.options.chain import chain_implied_spot, is_spxw_option, pair_by_strike
from spx_spark.analytics.options.constants import BAD_QUALITIES, UNDERLIER_CANDIDATES, UNDERLIER_MISMATCH_SOURCES
from spx_spark.analytics.options.levels import build_spy_confluence
from spx_spark.analytics.options.models import OptionsMap, UnderlierReference
from spx_spark.analytics.options.service import build_expiry_map
from spx_spark.config import StorageSettings
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import OptionRight, Provider, ProviderStatus, Quote
from spx_spark.storage import (
    LatestState,
    configured_quote_use_decision,
    degrade_stale_quote,
    select_best_quotes,
)


def select_underlier(state: LatestState) -> UnderlierReference:
    for instrument_id, multiplier in UNDERLIER_CANDIDATES:
        quote = state.best_quote(instrument_id)
        if quote is None or quote.quality in BAD_QUALITIES:
            continue
        price = quote.effective_price
        if price is not None and price > 0:
            return UnderlierReference(price=price * multiplier, source=instrument_id)
    return UnderlierReference(price=None, source=None)


def ibkr_provider_unavailable(state: LatestState) -> bool:
    for provider_state in state.provider_states:
        if provider_state.provider != Provider.IBKR:
            continue
        if provider_state.status == ProviderStatus.UNAVAILABLE:
            return True
        if (
            provider_state.status == ProviderStatus.DEGRADED
            and provider_state.connected is not True
        ):
            return True
    return False


def actionable_chain_implied_spot(
    state: LatestState,
    *,
    expiry: str,
    as_of: datetime,
    max_leg_skew_seconds: float | None = None,
) -> float | None:
    """SPX spot from fresh, pricing-allowed SPXW call/put parity pairs."""

    quotes = [
        quote
        for quote in state.best_quotes
        if is_spxw_option(quote)
        and (quote.instrument.expiry or "") == expiry
        and configured_quote_use_decision(quote, as_of=as_of).pricing_allowed
    ]
    cofresh_pairs: dict[float, dict[OptionRight, Quote]] = {}
    for strike, sides in pair_by_strike(quotes).items():
        call = sides.get(OptionRight.CALL)
        put = sides.get(OptionRight.PUT)
        if call is None or put is None:
            continue
        call_time = call.quote_time or call.trade_time or call.received_at
        put_time = put.quote_time or put.trade_time or put.received_at
        if max_leg_skew_seconds is None or (
            abs((call_time - put_time).total_seconds()) <= max_leg_skew_seconds
        ):
            cofresh_pairs[strike] = sides
    return chain_implied_spot(cofresh_pairs)


def group_spxw_option_quotes(
    state: LatestState,
    *,
    storage_settings: StorageSettings | None = None,
) -> dict[str, list[Quote]]:
    ibkr_down = ibkr_provider_unavailable(state)
    settings = storage_settings or StorageSettings.from_env()
    candidates = tuple(
        degrade_stale_quote(
            quote,
            as_of=state.as_of,
            stale_after_seconds=settings.latest_stale_after_seconds,
            delayed_stale_after_seconds=settings.delayed_stale_after_seconds,
            slow_stale_after_seconds=settings.slow_index_stale_after_seconds,
            slow_labels=settings.slow_index_labels,
        )
        for quote in state.quotes
        if is_spxw_option(quote) and not (quote.provider == Provider.IBKR and ibkr_down)
    )
    selected = select_best_quotes(
        candidates,
        as_of=state.as_of,
        provider_priority=settings.provider_priority,
    )
    grouped: dict[str, list[Quote]] = defaultdict(list)
    for quote in selected:
        expiry = quote.instrument.expiry or "unknown"
        grouped[expiry].append(quote)
    return grouped


def build_options_map(
    state: LatestState,
    *,
    storage_settings: StorageSettings | None = None,
) -> OptionsMap:
    underlier = select_underlier(state)
    all_grouped = group_spxw_option_quotes(state, storage_settings=storage_settings)
    active_expiries = {
        expiry.strftime("%Y%m%d")
        for expiry in DEFAULT_MARKET_CALENDAR.research_expiries(state.as_of)
    }
    grouped = {
        expiry: quotes for expiry, quotes in all_grouped.items() if expiry in active_expiries
    }

    warnings: list[str] = []
    if set(all_grouped) - set(grouped):
        warnings.append("expired SPXW option rows suppressed after research rollover")
    underlier_mismatch = (
        underlier.source is not None and underlier.source in UNDERLIER_MISMATCH_SOURCES
    )
    if (underlier.price is None or underlier_mismatch) and grouped:
        front_expiry = sorted(grouped)[0]
        implied = chain_implied_spot(pair_by_strike(grouped[front_expiry]))
        reference = underlier.price
        implied_plausible = implied is not None and (
            reference is None or abs(implied / reference - 1.0) <= 0.02
        )
        if implied_plausible:
            underlier = UnderlierReference(price=implied, source="chain_implied")
            underlier_mismatch = False
    if underlier.price is None:
        warnings.append("missing SPX underlier reference")
    elif underlier_mismatch:
        warnings.append(
            "underlier_mismatch: using "
            f"{underlier.source} price for SPX strikes; wall/gamma alerts suppressed"
        )
    if not grouped:
        warnings.append("missing SPXW option quotes")
    if ibkr_provider_unavailable(state):
        warnings.append("IBKR feed unavailable; stale SPXW option quotes suppressed")

    expiries = tuple(
        build_expiry_map(
            expiry,
            quotes,
            underlier.price,
            as_of=state.as_of,
            underlier_mismatch=underlier_mismatch,
        )
        for expiry, quotes in sorted(grouped.items())
    )
    front_spxw = expiries[0] if expiries else None
    spy_quote = state.best_quote("equity:SPY")
    spx_quote = state.best_quote("index:SPX")
    spy_confluence = build_spy_confluence(
        state.best_quotes,
        front_spxw,
        spy_underlier=spy_quote.effective_price if spy_quote is not None else None,
        spx_underlier=spx_quote.effective_price if spx_quote is not None else None,
    )
    return OptionsMap(
        created_at=datetime.now(tz=state.as_of.tzinfo),
        as_of=state.as_of,
        underlier=underlier,
        expiries=expiries,
        warnings=tuple(dict.fromkeys(warnings)),
        spy_confluence=spy_confluence,
    )
