"""Schwab-specific normalization: raw API payloads -> domain quotes/snapshots.

All knowledge about Schwab symbols and payload field names lives here so
that ``spx_spark.marketdata`` stays provider-agnostic.
"""

from __future__ import annotations

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
    as_utc,
    bool_or_none,
    classify_quote_quality,
    clean_float,
    elapsed_ms,
    normalize_implied_vol_percent,
    parse_timestamp,
)
from spx_spark.provider_adapter import ProviderSnapshot, provider_state_from_quote_health


def first_key(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def nested_mapping(mapping: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = mapping.get(key)
    return value if isinstance(value, Mapping) else {}


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


def instrument_from_schwab_symbol(
    symbol: str,
    payload: Mapping[str, Any] | None = None,
) -> InstrumentId:
    raw_symbol = symbol
    clean_symbol = symbol[1:] if symbol.startswith("$") else symbol
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
    stale_after_seconds: float = 15.0,
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
    quote_time = parse_timestamp(first_key(quote_section, "quoteTime", "quoteTimeInLong"))
    trade_time = parse_timestamp(first_key(quote_section, "tradeTime", "tradeTimeInLong"))
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
        bid=clean_float(first_key(quote_section, "bidPrice", "bid")),
        ask=clean_float(first_key(quote_section, "askPrice", "ask")),
        last=clean_float(first_key(quote_section, "lastPrice", "last")),
        mark=clean_float(first_key(quote_section, "mark", "markPrice")),
        close=clean_float(first_key(quote_section, "closePrice", "close")),
        bid_size=clean_float(first_key(quote_section, "bidSize")),
        ask_size=clean_float(first_key(quote_section, "askSize")),
        last_size=clean_float(first_key(quote_section, "lastSize")),
        volume=clean_float(first_key(quote_section, "totalVolume", "volume")),
        open_interest=clean_float(first_key(quote_section, "openInterest")),
        quote_time=quote_time,
        trade_time=trade_time,
        source_latency_ms=elapsed_ms(quote_time or trade_time, received_at),
        market_data_type="delayed" if delayed is True else None,
        raw=payload,
    )


def quote_from_schwab_option_contract(
    underlier: str,
    contract: Mapping[str, Any],
    *,
    received_at: datetime | None = None,
    stale_after_seconds: float = 15.0,
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
        delta=clean_float(first_key(contract, "delta")),
        gamma=clean_float(first_key(contract, "gamma")),
        theta=clean_float(first_key(contract, "theta")),
        vega=clean_float(first_key(contract, "vega")),
        rho=clean_float(first_key(contract, "rho")),
        underlier_price=clean_float(first_key(contract, "underlyingPrice", "underlierPrice")),
        model="schwab_chain",
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
        open_interest=clean_float(first_key(contract, "openInterest")),
        quote_time=quote_time,
        trade_time=trade_time,
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
    stale_after_seconds: float = 15.0,
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
    stale_after_seconds: float = 15.0,
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
    stale_after_seconds: float = 15.0,
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
    )


def snapshot_from_chain_payload(
    payload: Mapping[str, Any] | None,
    *,
    underlier: str,
    received_at: datetime | None = None,
    stale_after_seconds: float = 15.0,
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
