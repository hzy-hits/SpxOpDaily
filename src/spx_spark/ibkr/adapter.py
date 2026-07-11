"""IBKR-specific normalization: raw verifier rows -> domain quotes/snapshots.

All knowledge about IBKR labels, row fields, and CFD symbol mapping lives
here so that ``spx_spark.marketdata`` stays provider-agnostic.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from spx_spark.marketdata import (
    InstrumentId,
    InstrumentType,
    MarketDataQuality,
    OptionGreeks,
    Provider,
    ProviderState,
    Quote,
    as_utc,
    classify_quote_quality,
    clean_float,
    elapsed_ms,
    normalize_implied_vol,
    parse_timestamp,
)
from spx_spark.provider_adapter import ProviderSnapshot, provider_state_from_quote_health

if TYPE_CHECKING:
    from spx_spark.ibkr.verifier import VerifyRow

# IBKR index CFD symbols and the cash index each one tracks.
CFD_UNDERLIERS: dict[str, str] = {
    "IBUS500": "SPX",
    "IBUS30": "DJI",
    "IBUST100": "NDX",
    "IBUS2000": "RUT",
}


def get_value(row: Any, key: str, default: Any = None) -> Any:
    if isinstance(row, Mapping):
        return row.get(key, default)
    return getattr(row, key, default)


def instrument_from_ibkr_label(
    label: str,
    *,
    kind: str,
    symbol: str,
    exchange: str | None = None,
) -> InstrumentId:
    parts = label.split(":")
    if len(parts) >= 5 and parts[0] == "option":
        trading_class = parts[1]
        underlier = "SPX" if trading_class.startswith("SPX") else trading_class
        return InstrumentId.option(
            underlier,
            expiry=parts[2],
            strike=float(parts[3]),
            right=parts[4],
            trading_class=trading_class,
            provider_symbol=label,
        )
    if len(parts) >= 2 and parts[0] == "index":
        return InstrumentId.index(parts[1], provider_symbol=label, exchange=exchange or "CBOE")
    if len(parts) >= 2 and parts[0] == "future":
        return InstrumentId.future(parts[1], provider_symbol=label, exchange=exchange or "CME")
    if len(parts) >= 2 and parts[0] == "cfd":
        return InstrumentId.cfd(
            parts[1],
            provider_symbol=label,
            exchange=exchange or "SMART",
            underlier=CFD_UNDERLIERS.get(parts[1].upper()),
        )
    if len(parts) >= 2 and parts[0] in {"stock", "equity"}:
        return InstrumentId.equity(parts[1], provider_symbol=label)

    kind_map = {
        "index": InstrumentType.INDEX,
        "future": InstrumentType.FUTURE,
        "option": InstrumentType.OPTION,
        "cfd": InstrumentType.CFD,
        "stock": InstrumentType.EQUITY,
        "equity": InstrumentType.EQUITY,
    }
    return InstrumentId(
        symbol=symbol,
        instrument_type=kind_map.get(kind, InstrumentType.UNKNOWN),
        provider_symbol=label or symbol,
    )


def is_close_only_live_row(row: Any, quote_time: datetime | None) -> bool:
    """Detect farm half-recovery rows that only carry a prior close under mdt=1."""

    try:
        market_data_type = int(get_value(row, "market_data_type"))
    except (TypeError, ValueError):
        return False
    if market_data_type != 1 or quote_time is not None:
        return False
    bid = clean_float(get_value(row, "bid"))
    ask = clean_float(get_value(row, "ask"))
    last = clean_float(get_value(row, "last"))
    close = clean_float(get_value(row, "close"))
    return close is not None and bid is None and ask is None and last is None


def quote_from_ibkr_row(
    row: Any,
    *,
    received_at: datetime | None = None,
    stale_after_seconds: float = 15.0,
    source_session: str | None = None,
) -> Quote:
    received_at = as_utc(received_at or datetime.now(tz=timezone.utc))
    label = str(get_value(row, "label", "") or "")
    kind = str(get_value(row, "kind", "unknown") or "unknown")
    symbol = str(get_value(row, "symbol", "") or label or "UNKNOWN")
    exchange = str(get_value(row, "exchange", "") or "")
    error = get_value(row, "error")
    market_data_type = get_value(row, "market_data_type")
    quote_time = parse_timestamp(get_value(row, "ticker_time"))
    row_stale = bool(get_value(row, "stale")) if get_value(row, "stale") is not None else None

    instrument = instrument_from_ibkr_label(
        label,
        kind=kind,
        symbol=symbol,
        exchange=exchange or None,
    )
    quality = classify_quote_quality(
        market_data_type=market_data_type,
        quote_time=quote_time,
        received_at=received_at,
        stale_after_seconds=stale_after_seconds,
        error=str(error) if error else None,
    )
    if is_close_only_live_row(row, quote_time):
        quality = MarketDataQuality.UNKNOWN
    elif row_stale is True and quality == MarketDataQuality.LIVE:
        quality = MarketDataQuality.STALE

    greeks = None
    if any(
        get_value(row, key) is not None
        for key in ("model_iv", "delta", "gamma", "theta", "vega", "und_price")
    ):
        greeks = OptionGreeks(
            implied_vol=normalize_implied_vol(get_value(row, "model_iv")),
            delta=clean_float(get_value(row, "delta")),
            gamma=clean_float(get_value(row, "gamma")),
            theta=clean_float(get_value(row, "theta")),
            vega=clean_float(get_value(row, "vega")),
            underlier_price=clean_float(get_value(row, "und_price")),
            model="ibkr_model",
        )

    return Quote(
        instrument=instrument,
        provider=Provider.IBKR,
        provider_symbol=label or symbol,
        received_at=received_at,
        quality=quality,
        bid=clean_float(get_value(row, "bid")),
        ask=clean_float(get_value(row, "ask")),
        last=clean_float(get_value(row, "last")),
        mark=clean_float(get_value(row, "market_price")),
        close=clean_float(get_value(row, "close")),
        bid_size=clean_float(get_value(row, "bid_size")),
        ask_size=clean_float(get_value(row, "ask_size")),
        last_size=clean_float(get_value(row, "last_size")),
        volume=clean_float(get_value(row, "volume")),
        open_interest=clean_float(get_value(row, "open_interest")),
        quote_time=quote_time,
        last_update_at=parse_timestamp(get_value(row, "last_update_at")),
        source_latency_ms=elapsed_ms(quote_time, received_at),
        market_data_type=market_data_type,
        greeks=greeks,
        source_session=source_session,
        error=str(error) if error else None,
    )


def quotes_from_rows(
    rows: list[VerifyRow],
    *,
    received_at: datetime,
    stale_after_seconds: float,
    source_session: str | None = None,
) -> tuple[Quote, ...]:
    return tuple(
        quote_from_ibkr_row(
            row,
            received_at=received_at,
            stale_after_seconds=stale_after_seconds,
            source_session=source_session,
        )
        for row in rows
    )


def provider_state_from_quotes(
    quotes: tuple[Quote, ...],
    *,
    checked_at: datetime,
    connected: bool,
    authenticated: bool | None,
    latency_ms: float | None,
    error_count: int = 0,
    reason: str | None = None,
) -> ProviderState:
    return provider_state_from_quote_health(
        Provider.IBKR,
        quotes,
        checked_at=checked_at,
        connected=connected,
        authenticated=authenticated,
        latency_ms=latency_ms,
        priority=0,
        error_count=error_count,
        reason=reason,
        unavailable_reason="IBKR not connected",
        degraded_reason="connected but no usable quotes",
    )


def snapshot_from_rows(
    rows: list[VerifyRow],
    *,
    received_at: datetime,
    stale_after_seconds: float,
    connected: bool,
    authenticated: bool | None,
    latency_ms: float | None,
    error_count: int = 0,
    reason: str | None = None,
    replace_provider_quotes: bool = False,
    source_session: str | None = None,
) -> ProviderSnapshot:
    quotes = quotes_from_rows(
        rows,
        received_at=received_at,
        stale_after_seconds=stale_after_seconds,
        source_session=source_session,
    )
    state = provider_state_from_quotes(
        quotes,
        checked_at=received_at,
        connected=connected,
        authenticated=authenticated,
        latency_ms=latency_ms,
        error_count=error_count,
        reason=reason,
    )
    metadata: dict[str, bool] = {}
    if replace_provider_quotes:
        metadata["replace_provider_quotes"] = True
    return ProviderSnapshot(
        provider=Provider.IBKR,
        received_at=received_at,
        quotes=quotes,
        provider_states=(state,),
        metadata=metadata,
    )
