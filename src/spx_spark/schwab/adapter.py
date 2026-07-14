"""Schwab-specific normalization: raw API payloads -> domain quotes/snapshots.

All knowledge about Schwab symbols and payload field names lives here so
that ``spx_spark.marketdata`` stays provider-agnostic.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from spx_spark.marketdata import (
    InstrumentId,
    InstrumentType,
    MarketDataQuality,
    OptionGreeks,
    OptionRight,
    Provider,
    Quote,
    QuoteMarketSession,
    SessionQuoteObservation,
    as_utc,
    bool_or_none,
    classify_quote_quality,
    clean_float,
    elapsed_ms,
    normalize_implied_vol_percent,
    parse_timestamp,
)
from spx_spark.provider_adapter import ProviderSnapshot, provider_state_from_quote_health
from spx_spark.schwab.symbols import find_schwab_instrument


SCHWAB_OCC_OPTION_PATTERN = re.compile(
    r"^(?P<trading_class>[A-Z0-9]{1,6})\s+"
    r"(?P<expiry>\d{6})(?P<right>[CP])(?P<strike>\d{8})$"
)

# Shared quote-freshness policy comes from the documented runtime table.
DEFAULT_SCHWAB_STALE_SECONDS = 15


def first_key(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def schwab_model_float(value: Any) -> float | None:
    """Normalize Schwab's -999 sentinel used for unavailable model fields."""

    parsed = clean_float(value)
    return None if parsed is None or parsed <= -998.0 else parsed


def nested_mapping(mapping: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = mapping.get(key)
    return value if isinstance(value, Mapping) else {}


def _first_from_sections(
    primary: Mapping[str, Any],
    fallback: Mapping[str, Any],
    *keys: str,
) -> Any:
    value = first_key(primary, *keys)
    return value if value is not None else first_key(fallback, *keys)


def _session_observation(
    session: QuoteMarketSession,
    section: Mapping[str, Any],
    *,
    fallback: Mapping[str, Any] | None = None,
) -> SessionQuoteObservation | None:
    fallback = fallback or {}
    quote_time = parse_timestamp(
        _first_from_sections(
            section,
            fallback,
            "quoteTime",
            "quoteTimeInLong",
            "regularMarketQuoteTime",
        )
    )
    trade_time = parse_timestamp(
        _first_from_sections(
            section,
            fallback,
            "tradeTime",
            "tradeTimeInLong",
            "regularMarketTradeTime",
        )
    )
    observation = SessionQuoteObservation(
        session=session,
        quote_time=quote_time,
        trade_time=trade_time,
        bid=clean_float(_first_from_sections(section, fallback, "bidPrice", "bid")),
        ask=clean_float(_first_from_sections(section, fallback, "askPrice", "ask")),
        last=clean_float(
            _first_from_sections(
                section,
                fallback,
                "lastPrice",
                "last",
                "regularMarketLastPrice",
            )
        ),
        mark=clean_float(_first_from_sections(section, fallback, "mark", "markPrice")),
    )
    if observation.source_time is None and observation.effective_price is None:
        return None
    return observation


def _select_equity_session(
    instrument_type: InstrumentType,
    regular: SessionQuoteObservation | None,
    extended: SessionQuoteObservation | None,
) -> SessionQuoteObservation | None:
    if instrument_type not in {InstrumentType.EQUITY, InstrumentType.ETF}:
        return regular
    if extended is None or extended.effective_price is None:
        return regular
    if regular is None or regular.effective_price is None:
        return extended
    extended_time = extended.source_time
    regular_time = regular.source_time
    if extended_time is not None and (regular_time is None or extended_time > regular_time):
        return extended
    return regular


def parse_expiry(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y%m%d")
    text = str(value).strip()
    if not text:
        return None
    if len(text) >= 8 and text[:8].isdigit():
        return text[:8]
    try:
        return datetime.fromisoformat(text[:10]).strftime("%Y%m%d")
    except ValueError:
        return None


def option_instrument_from_schwab_symbol(symbol: str) -> InstrumentId | None:
    """Parse the padded OCC symbol returned by Schwab's quote endpoint."""

    match = SCHWAB_OCC_OPTION_PATTERN.fullmatch(symbol.strip().upper())
    if match is None:
        return None
    compact_expiry = match.group("expiry")
    expiry = f"20{compact_expiry}"
    try:
        datetime.strptime(expiry, "%Y%m%d")
    except ValueError:
        return None
    trading_class = match.group("trading_class")
    underlier = "SPX" if trading_class == "SPXW" else trading_class
    strike = int(match.group("strike")) / 1_000.0
    return InstrumentId.option(
        underlier,
        expiry=expiry,
        strike=strike,
        right=match.group("right"),
        trading_class=trading_class,
        provider_symbol=symbol,
    )


def instrument_from_schwab_symbol(
    symbol: str,
    payload: Mapping[str, Any] | None = None,
) -> InstrumentId:
    raw_symbol = symbol
    option_instrument = option_instrument_from_schwab_symbol(raw_symbol)
    if option_instrument is not None:
        return option_instrument
    configured = find_schwab_instrument(raw_symbol)
    if configured is not None:
        if configured.instrument_type == "index":
            return InstrumentId.index(
                configured.canonical_symbol,
                provider_symbol=raw_symbol,
            )
        if configured.instrument_type == "equity":
            return InstrumentId.equity(
                configured.canonical_symbol,
                provider_symbol=raw_symbol,
            )
        if configured.instrument_type == "future":
            return InstrumentId.future(
                configured.canonical_symbol,
                provider_symbol=raw_symbol,
                exchange="CME",
            )

    clean_symbol = symbol.lstrip("$/")
    if symbol.startswith("$"):
        return InstrumentId.index(clean_symbol, provider_symbol=raw_symbol)
    if symbol.startswith("/"):
        return InstrumentId.future(clean_symbol, provider_symbol=raw_symbol, exchange="CME")

    asset_main = str(payload.get("assetMainType", "") if payload else "").upper()
    asset_sub = str(payload.get("assetSubType", "") if payload else "").upper()
    if "OPTION" in {asset_main, asset_sub}:
        return InstrumentId(
            symbol=clean_symbol,
            instrument_type=InstrumentType.OPTION,
            provider_symbol=raw_symbol,
        )
    if "ETF" in {asset_main, asset_sub}:
        return InstrumentId(
            symbol=clean_symbol,
            instrument_type=InstrumentType.ETF,
            provider_symbol=raw_symbol,
        )
    return InstrumentId.equity(clean_symbol, provider_symbol=raw_symbol)


def quote_from_schwab_payload(
    symbol: str,
    payload: Mapping[str, Any] | None,
    *,
    received_at: datetime | None = None,
    stale_after_seconds: float = DEFAULT_SCHWAB_STALE_SECONDS,
) -> Quote:
    received_at = as_utc(received_at or datetime.now(tz=timezone.utc))
    instrument = instrument_from_schwab_symbol(symbol, payload)

    if payload is None:
        return Quote(
            instrument=instrument,
            provider=Provider.SCHWAB,
            provider_symbol=symbol,
            received_at=received_at,
            quality=MarketDataQuality.MISSING,
            error="symbol missing from Schwab payload",
        )

    quote_section = nested_mapping(payload, "quote")
    reference_section = nested_mapping(payload, "reference")
    regular_section = nested_mapping(payload, "regular")
    extended_section = nested_mapping(payload, "extended")
    regular_observation = _session_observation(
        QuoteMarketSession.REGULAR,
        regular_section,
        fallback=quote_section,
    )
    extended_observation = _session_observation(
        QuoteMarketSession.EXTENDED,
        extended_section,
    )
    selected_observation = _select_equity_session(
        instrument.instrument_type,
        regular_observation,
        extended_observation,
    )
    quote_time = selected_observation.quote_time if selected_observation else None
    trade_time = selected_observation.trade_time if selected_observation else None
    delayed = bool_or_none(
        first_key(payload, "isDelayed", "delayed"),
        first_key(quote_section, "isDelayed", "delayed"),
        first_key(reference_section, "isDelayed", "delayed"),
    )

    quality = classify_quote_quality(
        quote_time=quote_time or trade_time,
        received_at=received_at,
        stale_after_seconds=stale_after_seconds,
        explicit_delayed=delayed,
    )

    return Quote(
        instrument=instrument,
        provider=Provider.SCHWAB,
        provider_symbol=symbol,
        received_at=received_at,
        quality=quality,
        bid=selected_observation.bid if selected_observation else None,
        ask=selected_observation.ask if selected_observation else None,
        last=selected_observation.last if selected_observation else None,
        mark=selected_observation.mark if selected_observation else None,
        close=clean_float(first_key(quote_section, "closePrice", "close")),
        bid_size=clean_float(first_key(quote_section, "bidSize")),
        ask_size=clean_float(first_key(quote_section, "askSize")),
        last_size=clean_float(first_key(quote_section, "lastSize")),
        volume=clean_float(first_key(quote_section, "totalVolume", "volume")),
        open_interest=clean_float(first_key(quote_section, "openInterest")),
        quote_time=quote_time,
        trade_time=trade_time,
        last_update_at=received_at,
        source_latency_ms=elapsed_ms(quote_time or trade_time, received_at),
        market_data_type="delayed" if delayed is True else None,
        market_session=selected_observation.session if selected_observation else None,
        session_observations=tuple(
            observation
            for observation in (regular_observation, extended_observation)
            if observation is not None
        ),
        raw=payload,
    )


def quote_from_schwab_option_contract(
    underlier: str,
    contract: Mapping[str, Any],
    *,
    received_at: datetime | None = None,
    stale_after_seconds: float = DEFAULT_SCHWAB_STALE_SECONDS,
) -> Quote:
    received_at = as_utc(received_at or datetime.now(tz=timezone.utc))
    provider_symbol = str(first_key(contract, "symbol", "optionSymbol") or "")
    right_value = first_key(contract, "putCall", "right")
    right = OptionRight.CALL if str(right_value).upper().startswith("C") else OptionRight.PUT
    expiry = parse_expiry(first_key(contract, "expirationDate", "expiryDate", "expiration"))
    strike = clean_float(first_key(contract, "strikePrice", "strike"))
    trading_class = "SPXW" if provider_symbol.startswith("SPXW") else underlier
    instrument = InstrumentId.option(
        underlier,
        expiry=expiry or "",
        strike=strike or 0.0,
        right=right,
        trading_class=trading_class,
        provider_symbol=provider_symbol,
    )
    quote_time = parse_timestamp(first_key(contract, "quoteTimeInLong", "quoteTime"))
    trade_time = parse_timestamp(first_key(contract, "tradeTimeInLong", "tradeTime"))
    delayed = bool_or_none(first_key(contract, "isDelayed", "delayed"))
    quality = classify_quote_quality(
        quote_time=quote_time or trade_time,
        received_at=received_at,
        stale_after_seconds=stale_after_seconds,
        explicit_delayed=delayed,
    )
    greeks = OptionGreeks(
        implied_vol=normalize_implied_vol_percent(
            first_key(contract, "volatility", "impliedVolatility")
        ),
        delta=schwab_model_float(first_key(contract, "delta")),
        gamma=schwab_model_float(first_key(contract, "gamma")),
        theta=schwab_model_float(first_key(contract, "theta")),
        vega=schwab_model_float(first_key(contract, "vega")),
        rho=schwab_model_float(first_key(contract, "rho")),
        underlier_price=schwab_model_float(
            first_key(contract, "underlyingPrice", "underlierPrice")
        ),
        model="schwab_chain",
    )
    open_interest = clean_float(first_key(contract, "openInterest"))
    has_structure = bool(
        (open_interest is not None and open_interest > 0)
        or any(
            value is not None
            for value in (
                greeks.implied_vol,
                greeks.delta,
                greeks.gamma,
                greeks.theta,
                greeks.vega,
                greeks.rho,
            )
        )
    )

    return Quote(
        instrument=instrument,
        provider=Provider.SCHWAB,
        provider_symbol=provider_symbol,
        received_at=received_at,
        quality=quality,
        bid=clean_float(first_key(contract, "bid")),
        ask=clean_float(first_key(contract, "ask")),
        last=clean_float(first_key(contract, "last")),
        mark=clean_float(first_key(contract, "mark")),
        bid_size=clean_float(first_key(contract, "bidSize")),
        ask_size=clean_float(first_key(contract, "askSize")),
        volume=clean_float(first_key(contract, "totalVolume", "volume")),
        open_interest=open_interest,
        structure_time=received_at if has_structure else None,
        quote_time=quote_time,
        trade_time=trade_time,
        last_update_at=received_at,
        source_latency_ms=elapsed_ms(quote_time or trade_time, received_at),
        market_data_type="delayed" if delayed is True else None,
        greeks=greeks,
        raw=contract,
    )


def quotes_from_quote_payload(
    payload: Mapping[str, Any] | None,
    symbols: list[str],
    *,
    received_at: datetime | None = None,
    stale_after_seconds: float = DEFAULT_SCHWAB_STALE_SECONDS,
) -> tuple[Quote, ...]:
    received_at = received_at or datetime.now(tz=timezone.utc)
    payload = payload or {}
    return tuple(
        quote_from_schwab_payload(
            symbol,
            payload.get(symbol) if isinstance(payload.get(symbol), Mapping) else None,
            received_at=received_at,
            stale_after_seconds=stale_after_seconds,
        )
        for symbol in symbols
    )


def option_quotes_from_chain_payload(
    payload: Mapping[str, Any] | None,
    *,
    underlier: str,
    received_at: datetime | None = None,
    stale_after_seconds: float = DEFAULT_SCHWAB_STALE_SECONDS,
) -> tuple[Quote, ...]:
    if not isinstance(payload, Mapping):
        return ()

    received_at = received_at or datetime.now(tz=timezone.utc)
    quotes: list[Quote] = []
    for expiration_map_name in ("callExpDateMap", "putExpDateMap"):
        expiration_map = payload.get(expiration_map_name)
        if not isinstance(expiration_map, Mapping):
            continue
        for strikes in expiration_map.values():
            if not isinstance(strikes, Mapping):
                continue
            for contracts in strikes.values():
                if not isinstance(contracts, list):
                    continue
                for contract in contracts:
                    if isinstance(contract, Mapping):
                        quotes.append(
                            quote_from_schwab_option_contract(
                                underlier,
                                contract,
                                received_at=received_at,
                                stale_after_seconds=stale_after_seconds,
                            )
                        )
    return tuple(quotes)


def snapshot_from_quote_payload(
    payload: Mapping[str, Any] | None,
    symbols: list[str],
    *,
    received_at: datetime | None = None,
    stale_after_seconds: float = DEFAULT_SCHWAB_STALE_SECONDS,
    connected: bool = True,
    authenticated: bool | None = True,
    latency_ms: float | None = None,
    error_count: int = 0,
    reason: str | None = None,
) -> ProviderSnapshot:
    received_at = received_at or datetime.now(tz=timezone.utc)
    quotes = quotes_from_quote_payload(
        payload,
        symbols,
        received_at=received_at,
        stale_after_seconds=stale_after_seconds,
    )
    state = provider_state_from_quote_health(
        Provider.SCHWAB,
        quotes,
        checked_at=received_at,
        connected=connected,
        authenticated=authenticated,
        latency_ms=latency_ms,
        priority=1,
        error_count=error_count,
        reason=reason,
        unavailable_reason="Schwab not connected",
        degraded_reason="connected but no usable Schwab quotes",
    )
    return ProviderSnapshot(
        provider=Provider.SCHWAB,
        received_at=received_at,
        quotes=quotes,
        provider_states=(state,),
        metadata={
            "sampling_mode": "schwab_rest",
            "selected_market_sessions": {
                session: sum(
                    1
                    for quote in quotes
                    if quote.market_session is not None and quote.market_session.value == session
                )
                for session in ("regular", "extended")
            },
        },
    )


def snapshot_from_chain_payload(
    payload: Mapping[str, Any] | None,
    *,
    underlier: str,
    received_at: datetime | None = None,
    stale_after_seconds: float = DEFAULT_SCHWAB_STALE_SECONDS,
    connected: bool = True,
    authenticated: bool | None = True,
    latency_ms: float | None = None,
    error_count: int = 0,
    reason: str | None = None,
) -> ProviderSnapshot:
    received_at = received_at or datetime.now(tz=timezone.utc)
    quotes = option_quotes_from_chain_payload(
        payload,
        underlier=underlier,
        received_at=received_at,
        stale_after_seconds=stale_after_seconds,
    )
    state = provider_state_from_quote_health(
        Provider.SCHWAB,
        quotes,
        checked_at=received_at,
        connected=connected,
        authenticated=authenticated,
        latency_ms=latency_ms,
        priority=1,
        error_count=error_count,
        reason=reason,
        unavailable_reason="Schwab not connected",
        degraded_reason="connected but no usable Schwab option quotes",
    )
    return ProviderSnapshot(
        provider=Provider.SCHWAB,
        received_at=received_at,
        quotes=quotes,
        provider_states=(state,),
    )
