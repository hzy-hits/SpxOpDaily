"""Schwab transport and raw-payload normalization for the LEAPS scanner."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any

from spx_spark.market_calendar import ET
from spx_spark.schwab.verifier import SchwabClient


QUOTE_PATH = "/marketdata/v1/quotes"
PRICE_HISTORY_PATH = "/marketdata/v1/pricehistory"
CHAIN_PATH = "/marketdata/v1/chains"


@dataclass(frozen=True, slots=True)
class EquityQuote:
    provider_symbol: str
    last: float | None
    low_52w: float | None
    high_52w: float | None
    market_cap: float | None
    dividend_yield: float | None
    optionable: bool
    quote_at: datetime | None
    realtime: bool


@dataclass(frozen=True, slots=True)
class DailyClose:
    day: date
    close: float


@dataclass(frozen=True, slots=True)
class OptionContract:
    symbol: str
    expiry: date
    strike: float
    delta: float | None
    bid: float | None
    ask: float | None
    volatility: float | None
    open_interest: int
    total_volume: int
    dte: int
    quote_at: datetime | None


@dataclass(frozen=True, slots=True)
class LeapsChain:
    contracts: tuple[OptionContract, ...]
    max_dte: int
    observed_volume: int
    delayed: bool


def fetch_equity_quote_batch(
    client: SchwabClient,
    provider_symbols: list[str],
) -> dict[str, EquityQuote]:
    """Fetch at most one Schwab quote batch and normalize only requested symbols."""

    if not 1 <= len(provider_symbols) <= 500:
        raise ValueError("Schwab scanner quote batches must contain 1 to 500 symbols")
    _status, payload = client.get_json(
        QUOTE_PATH,
        {
            "symbols": ",".join(provider_symbols),
            "fields": "quote,reference,fundamental,regular",
            "indicative": "false",
        },
    )
    if not isinstance(payload, Mapping):
        raise ValueError("Schwab quote payload must be an object")
    normalized: dict[str, EquityQuote] = {}
    for symbol in provider_symbols:
        raw = payload.get(symbol)
        if isinstance(raw, Mapping):
            normalized[symbol] = _equity_quote(symbol, raw)
    return normalized


def fetch_daily_closes(client: SchwabClient, provider_symbol: str) -> tuple[DailyClose, ...]:
    _status, payload = client.get_json(
        PRICE_HISTORY_PATH,
        {
            "symbol": provider_symbol,
            "periodType": "year",
            "period": 1,
            "frequencyType": "daily",
            "frequency": 1,
            "needExtendedHoursData": "false",
            "needPreviousClose": "true",
        },
    )
    if not isinstance(payload, Mapping) or payload.get("empty") is True:
        return ()
    candles = payload.get("candles")
    if not isinstance(candles, list):
        return ()
    by_day: dict[date, DailyClose] = {}
    for candle in candles:
        if not isinstance(candle, Mapping):
            continue
        close = _positive_float(candle.get("close"))
        at = _datetime_from_millis(candle.get("datetime"))
        if close is None or at is None:
            continue
        day = at.astimezone(ET).date()
        by_day[day] = DailyClose(day=day, close=close)
    return tuple(by_day[day] for day in sorted(by_day))


def fetch_leaps_chain(
    client: SchwabClient,
    provider_symbol: str,
    *,
    as_of: date,
    min_dte: int,
    max_dte: int,
    strike_count: int,
) -> LeapsChain:
    _status, payload = client.get_json(
        CHAIN_PATH,
        {
            "symbol": provider_symbol,
            "contractType": "CALL",
            "strategy": "SINGLE",
            "strikeCount": strike_count,
            "includeUnderlyingQuote": "true",
            "fromDate": (as_of + timedelta(days=min_dte)).isoformat(),
            "toDate": (as_of + timedelta(days=max_dte)).isoformat(),
        },
    )
    if not isinstance(payload, Mapping) or str(payload.get("status") or "") != "SUCCESS":
        return LeapsChain((), 0, 0, bool(_mapping(payload).get("isDelayed")))
    contracts: list[OptionContract] = []
    expiration_map = payload.get("callExpDateMap")
    if isinstance(expiration_map, Mapping):
        for expiry_key, strike_map in expiration_map.items():
            expiry = _expiry_from_key(expiry_key)
            if expiry is None or not isinstance(strike_map, Mapping):
                continue
            for raw_contracts in strike_map.values():
                if not isinstance(raw_contracts, list):
                    continue
                for raw in raw_contracts:
                    if isinstance(raw, Mapping):
                        contract = _option_contract(raw, expiry=expiry)
                        if contract is not None:
                            contracts.append(contract)
    return LeapsChain(
        contracts=tuple(contracts),
        max_dte=max((contract.dte for contract in contracts), default=0),
        observed_volume=sum(contract.total_volume for contract in contracts),
        delayed=bool(payload.get("isDelayed")),
    )


def _equity_quote(provider_symbol: str, raw: Mapping[str, Any]) -> EquityQuote:
    quote = _mapping(raw.get("quote"))
    fundamental = _mapping(raw.get("fundamental"))
    reference = _mapping(raw.get("reference"))
    last = _first_positive(quote, "mark", "lastPrice", "closePrice")
    shares = _positive_float(fundamental.get("sharesOutstanding"))
    raw_yield = _nonnegative_float(fundamental.get("divYield"))
    return EquityQuote(
        provider_symbol=provider_symbol,
        last=last,
        low_52w=_positive_float(quote.get("52WeekLow")),
        high_52w=_positive_float(quote.get("52WeekHigh")),
        market_cap=(last * shares if last is not None and shares is not None else None),
        dividend_yield=(raw_yield / 100.0 if raw_yield is not None else None),
        optionable=bool(reference.get("optionable")),
        quote_at=_datetime_from_millis(quote.get("quoteTime") or quote.get("tradeTime")),
        realtime=bool(raw.get("realtime")),
    )


def _option_contract(raw: Mapping[str, Any], *, expiry: date) -> OptionContract | None:
    symbol = str(raw.get("symbol") or "").strip()
    strike = _positive_float(raw.get("strikePrice"))
    dte = _nonnegative_int(raw.get("daysToExpiration"))
    if not symbol or strike is None or dte is None:
        return None
    volatility = _positive_float(raw.get("volatility"))
    return OptionContract(
        symbol=symbol,
        expiry=expiry,
        strike=strike,
        delta=_finite_float(raw.get("delta")),
        bid=_nonnegative_float(raw.get("bid")),
        ask=_nonnegative_float(raw.get("ask")),
        volatility=(volatility / 100.0 if volatility is not None else None),
        open_interest=_nonnegative_int(raw.get("openInterest")) or 0,
        total_volume=_nonnegative_int(raw.get("totalVolume")) or 0,
        dte=dte,
        quote_at=_datetime_from_millis(raw.get("quoteTimeInLong") or raw.get("tradeTimeInLong")),
    )


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _finite_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed == parsed and abs(parsed) != float("inf") else None


def _positive_float(value: Any) -> float | None:
    parsed = _finite_float(value)
    return parsed if parsed is not None and parsed > 0.0 else None


def _nonnegative_float(value: Any) -> float | None:
    parsed = _finite_float(value)
    return parsed if parsed is not None and parsed >= 0.0 else None


def _nonnegative_int(value: Any) -> int | None:
    parsed = _finite_float(value)
    return int(parsed) if parsed is not None and parsed >= 0.0 else None


def _first_positive(values: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        if (parsed := _positive_float(values.get(key))) is not None:
            return parsed
    return None


def _datetime_from_millis(value: Any) -> datetime | None:
    parsed = _finite_float(value)
    if parsed is None or parsed <= 0.0:
        return None
    try:
        return datetime.fromtimestamp(parsed / 1000.0, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def _expiry_from_key(value: Any) -> date | None:
    raw = str(value).split(":", 1)[0]
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None
