"""IBKR-specific normalization: raw verifier rows -> domain quotes/snapshots.

All knowledge about IBKR labels, row fields, and CFD symbol mapping lives
here so that ``spx_spark.marketdata`` stays provider-agnostic.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, MarketCalendar
from spx_spark.marketdata import (
    InstrumentId,
    InstrumentType,
    MarketDataQuality,
    OptionGreeks,
    Provider,
    ProviderState,
    Quote,
    QuoteMarketSession,
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
    contract_expiry: str | None = None,
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
        return InstrumentId.future(
            parts[1],
            expiry=contract_expiry,
            provider_symbol=label,
            exchange=exchange or "CME",
        )
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

    del quote_time
    try:
        market_data_type = int(get_value(row, "market_data_type"))
    except (TypeError, ValueError):
        return False
    if market_data_type != 1:
        return False
    # IBKR uses -1 sentinels for absent fields and can stamp ticker_time while
    # replaying only the previous cash close.  A fresh transport clock does not
    # turn that close-only row into a live price.
    bid = _positive_price(get_value(row, "bid"))
    ask = _positive_price(get_value(row, "ask"))
    last = _positive_price(get_value(row, "last"))
    close = clean_float(get_value(row, "close"))
    valid_mid = bid is not None and ask is not None and ask >= bid
    return close is not None and close > 0 and last is None and not valid_mid


def _positive_price(value: object) -> float | None:
    parsed = clean_float(value)
    return parsed if parsed is not None and parsed > 0 else None


def _spx_market_session_for_quote(
    instrument: InstrumentId,
    quote_time: datetime | None,
    *,
    market_calendar: MarketCalendar,
) -> QuoteMarketSession | None:
    """Classify SPX decision quotes from the provider's source timestamp.

    SPXW options can trade in GTH and RTH.  The cash SPX index is accepted
    only in RTH, while ES and every other IBKR instrument remain unlabelled by
    this SPX-specific contract.  Missing timestamps and scheduled gaps fail
    closed instead of inheriting the collector receipt time.
    """

    is_spxw_option = (
        instrument.instrument_type is InstrumentType.OPTION
        and instrument.underlier == "SPX"
        and instrument.trading_class == "SPXW"
    )
    is_spx_index = (
        instrument.instrument_type is InstrumentType.INDEX and instrument.symbol == "SPX"
    )
    if quote_time is None or not (is_spxw_option or is_spx_index):
        return None

    trading_date = market_calendar.spx_session_date_for(quote_time)
    window = market_calendar.spx_session_window(trading_date) if trading_date is not None else None
    segment = window.segment_at(quote_time) if window is not None else None
    if segment == "rth":
        return QuoteMarketSession.REGULAR
    if segment == "gth" and is_spxw_option:
        return QuoteMarketSession.GTH
    return None


def quote_from_ibkr_row(
    row: Any,
    *,
    received_at: datetime | None = None,
    stale_after_seconds: float = 15.0,
    source_session: str | None = None,
    market_calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR,
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
        contract_expiry=(
            str(get_value(row, "contract_expiry")) if get_value(row, "contract_expiry") else None
        ),
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

    sampling_mode = (
        str(get_value(row, "sampling_mode"))
        if get_value(row, "sampling_mode") is not None
        else None
    )
    raw: dict[str, object] = {}
    if greeks is not None:
        greeks_observed_at = parse_timestamp(get_value(row, "greeks_observed_at"))
        if greeks_observed_at is not None:
            raw.update(
                {
                    "greeks_provider": Provider.IBKR.value,
                    "greeks_sampling_mode": sampling_mode,
                    "greeks_market_data_type": market_data_type,
                    "greeks_observed_at": greeks_observed_at.isoformat(),
                }
            )
    open_interest = clean_float(get_value(row, "open_interest"))
    if open_interest is not None:
        open_interest_observed_at = parse_timestamp(get_value(row, "open_interest_observed_at"))
        if open_interest_observed_at is not None:
            raw.update(
                {
                    "open_interest_provider": Provider.IBKR.value,
                    "open_interest_sampling_mode": sampling_mode,
                    "open_interest_market_data_type": market_data_type,
                    "open_interest_observed_at": open_interest_observed_at.isoformat(),
                }
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
        open_interest=open_interest,
        quote_time=quote_time,
        last_update_at=parse_timestamp(get_value(row, "last_update_at")),
        source_latency_ms=elapsed_ms(quote_time, received_at),
        market_data_type=market_data_type,
        greeks=greeks,
        sampling_mode=sampling_mode,
        sampling_group=(
            int(get_value(row, "sampling_group"))
            if get_value(row, "sampling_group") is not None
            else None
        ),
        source_session=source_session,
        market_session=_spx_market_session_for_quote(
            instrument,
            quote_time,
            market_calendar=market_calendar,
        ),
        error=str(error) if error else None,
        raw=raw or None,
    )


def quotes_from_rows(
    rows: list[VerifyRow],
    *,
    received_at: datetime,
    stale_after_seconds: float,
    source_session: str | None = None,
    market_calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR,
) -> tuple[Quote, ...]:
    return tuple(
        quote_from_ibkr_row(
            row,
            received_at=received_at,
            stale_after_seconds=stale_after_seconds,
            source_session=source_session,
            market_calendar=market_calendar,
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
    market_calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR,
) -> ProviderSnapshot:
    quotes = quotes_from_rows(
        rows,
        received_at=received_at,
        stale_after_seconds=stale_after_seconds,
        source_session=source_session,
        market_calendar=market_calendar,
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
