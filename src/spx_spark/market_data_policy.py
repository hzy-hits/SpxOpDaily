"""Explicit provider/session capability and pricing-selection policy."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Iterable

from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import InstrumentId, Provider, Quote


class CapabilityStatus(str, Enum):
    PRODUCTION = "production"
    VALIDATION = "validation"
    FALLBACK = "fallback"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True)
class DataSourceCapability:
    lane: str
    provider: Provider
    instruments: str
    sessions: str
    status: CapabilityStatus
    use: str


DATA_SOURCE_CAPABILITIES: tuple[DataSourceCapability, ...] = (
    DataSourceCapability(
        "schwab_rest_extended_equity",
        Provider.SCHWAB,
        "eligible US equities/ETFs",
        "extended/overnight when provider timestamp advances",
        CapabilityStatus.PRODUCTION,
        "cross-asset and breadth context; never SPX cash replacement",
    ),
    DataSourceCapability(
        "schwab_stream_es_mes",
        Provider.SCHWAB,
        "ES/MES",
        "CME Globex",
        CapabilityStatus.PRODUCTION,
        "overnight path, volume, VWAP, and SPX proxy context",
    ),
    DataSourceCapability(
        "schwab_stream_cross_index_futures",
        Provider.SCHWAB,
        "NQ/RTY/YM",
        "CME/CBOT Globex",
        CapabilityStatus.VALIDATION,
        "source-time acceptance only until explicitly promoted",
    ),
    DataSourceCapability(
        "schwab_stream_es_futures_option_probe",
        Provider.SCHWAB,
        "one configured ES futures option",
        "CME Globex",
        CapabilityStatus.VALIDATION,
        "transport and entitlement probe; not a trading or GEX input",
    ),
    DataSourceCapability(
        "ibkr_spxw_gth",
        Provider.IBKR,
        "SPXW current expiry",
        "Cboe GTH",
        CapabilityStatus.PRODUCTION,
        "exclusive GTH option pricing source",
    ),
    DataSourceCapability(
        "schwab_spxw_gth",
        Provider.SCHWAB,
        "SPXW current expiry",
        "Cboe GTH",
        CapabilityStatus.UNAVAILABLE,
        "frozen structure may be retained; never used for GTH pricing",
    ),
    DataSourceCapability(
        "schwab_spxw_rth",
        Provider.SCHWAB,
        "SPXW current expiry",
        "Cboe RTH",
        CapabilityStatus.PRODUCTION,
        "primary RTH option pricing and structure source",
    ),
    DataSourceCapability(
        "ibkr_spxw_rth",
        Provider.IBKR,
        "SPXW current expiry",
        "Cboe RTH",
        CapabilityStatus.FALLBACK,
        "RTH cross-check and failover when Schwab is not usable",
    ),
)


def pricing_provider_priority(
    instrument: InstrumentId,
    *,
    as_of: datetime,
    configured: Iterable[Provider | str],
) -> tuple[Provider | str, ...]:
    """Apply the SPXW session provider policy before configured fallback order."""

    priority = tuple(configured)
    is_spxw = (instrument.trading_class or "").upper() == "SPXW"
    if not is_spxw:
        return priority
    preferred = _preferred_spxw_provider(as_of)
    return _provider_first(preferred, priority) if preferred is not None else priority


def pricing_candidates(quotes: Iterable[Quote], *, as_of: datetime) -> tuple[Quote, ...]:
    """Fail closed for SPXW GTH instead of silently selecting Schwab frozen rows."""

    candidates = tuple(quotes)
    if not candidates:
        return ()
    instrument = candidates[0].instrument
    is_spxw = (instrument.trading_class or "").upper() == "SPXW"
    is_gth_only = _preferred_spxw_provider(as_of) is Provider.IBKR
    if not is_spxw or not is_gth_only:
        return candidates
    return tuple(quote for quote in candidates if quote.provider is Provider.IBKR)


def _preferred_spxw_provider(as_of: datetime) -> Provider | None:
    if DEFAULT_MARKET_CALENDAR.is_rth_open(as_of):
        return Provider.SCHWAB
    if DEFAULT_MARKET_CALENDAR.is_spx_gth_open(as_of):
        return Provider.IBKR
    return None


def _provider_first(
    provider: Provider,
    configured: tuple[Provider | str, ...],
) -> tuple[Provider | str, ...]:
    return (
        provider,
        *(
            item
            for item in configured
            if str(getattr(item, "value", item)).lower() != provider.value
        ),
    )
