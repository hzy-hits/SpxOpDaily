from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class Provider(str, Enum):
    IBKR = "ibkr"
    SCHWAB = "schwab"
    HYPERLIQUID = "hyperliquid"
    POLYMARKET = "polymarket"
    INTERNAL = "internal"
    MOCK = "mock"
    UNKNOWN = "unknown"


class InstrumentType(str, Enum):
    INDEX = "index"
    EQUITY = "equity"
    ETF = "etf"
    FUTURE = "future"
    OPTION = "option"
    CFD = "cfd"
    CRYPTO_PERP = "crypto_perp"
    PREDICTION_MARKET = "prediction_market"
    UNKNOWN = "unknown"


class OptionRight(str, Enum):
    CALL = "C"
    PUT = "P"


class MarketDataQuality(str, Enum):
    LIVE = "live"
    FROZEN = "frozen"
    DELAYED = "delayed"
    DELAYED_FROZEN = "delayed_frozen"
    SYNTHETIC = "synthetic"
    STALE = "stale"
    UNKNOWN = "unknown"
    MISSING = "missing"
    ERROR = "error"


class ProviderStatus(str, Enum):
    AVAILABLE = "available"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"
    UNKNOWN = "unknown"


QUALITY_RANK: dict[MarketDataQuality, int] = {
    MarketDataQuality.LIVE: 100,
    MarketDataQuality.FROZEN: 85,
    MarketDataQuality.DELAYED: 75,
    MarketDataQuality.DELAYED_FROZEN: 65,
    MarketDataQuality.SYNTHETIC: 50,
    MarketDataQuality.UNKNOWN: 30,
    MarketDataQuality.STALE: 20,
    MarketDataQuality.MISSING: 0,
    MarketDataQuality.ERROR: 0,
}


DEFAULT_PROVIDER_PRIORITY: tuple[Provider, ...] = (
    Provider.IBKR,
    Provider.SCHWAB,
    Provider.HYPERLIQUID,
    Provider.POLYMARKET,
    Provider.INTERNAL,
    Provider.MOCK,
    Provider.UNKNOWN,
)


@dataclass(frozen=True)
class InstrumentId:
    symbol: str
    instrument_type: InstrumentType
    provider_symbol: str | None = None
    exchange: str | None = None
    currency: str = "USD"
    expiry: str | None = None
    strike: float | None = None
    right: OptionRight | None = None
    multiplier: str | None = None
    underlier: str | None = None
    trading_class: str | None = None

    @classmethod
    def index(
        cls,
        symbol: str,
        *,
        provider_symbol: str | None = None,
        exchange: str | None = None,
        currency: str = "USD",
    ) -> InstrumentId:
        return cls(
            symbol=symbol,
            instrument_type=InstrumentType.INDEX,
            provider_symbol=provider_symbol,
            exchange=exchange,
            currency=currency,
        )

    @classmethod
    def equity(
        cls,
        symbol: str,
        *,
        provider_symbol: str | None = None,
        exchange: str | None = None,
        currency: str = "USD",
    ) -> InstrumentId:
        return cls(
            symbol=symbol,
            instrument_type=InstrumentType.EQUITY,
            provider_symbol=provider_symbol,
            exchange=exchange,
            currency=currency,
        )

    @classmethod
    def cfd(
        cls,
        symbol: str,
        *,
        provider_symbol: str | None = None,
        exchange: str | None = None,
        currency: str = "USD",
        underlier: str | None = None,
    ) -> InstrumentId:
        return cls(
            symbol=symbol,
            instrument_type=InstrumentType.CFD,
            provider_symbol=provider_symbol,
            exchange=exchange,
            currency=currency,
            underlier=underlier,
        )

    @classmethod
    def future(
        cls,
        symbol: str,
        *,
        expiry: str | None = None,
        provider_symbol: str | None = None,
        exchange: str | None = None,
        currency: str = "USD",
    ) -> InstrumentId:
        return cls(
            symbol=symbol,
            instrument_type=InstrumentType.FUTURE,
            provider_symbol=provider_symbol,
            exchange=exchange,
            currency=currency,
            expiry=expiry,
        )

    @classmethod
    def option(
        cls,
        underlier: str,
        *,
        expiry: str,
        strike: float,
        right: str | OptionRight,
        trading_class: str | None = None,
        provider_symbol: str | None = None,
        exchange: str | None = None,
        currency: str = "USD",
        multiplier: str | None = "100",
    ) -> InstrumentId:
        parsed_right = normalize_option_right(right)
        return cls(
            symbol=underlier,
            instrument_type=InstrumentType.OPTION,
            provider_symbol=provider_symbol,
            exchange=exchange,
            currency=currency,
            expiry=expiry,
            strike=float(strike),
            right=parsed_right,
            multiplier=multiplier,
            underlier=underlier,
            trading_class=trading_class,
        )

    @property
    def canonical_id(self) -> str:
        if self.instrument_type == InstrumentType.OPTION:
            trading_class = self.trading_class or self.underlier or self.symbol
            return ":".join(
                [
                    InstrumentType.OPTION.value,
                    self.underlier or self.symbol,
                    trading_class,
                    self.expiry or "",
                    format_strike(self.strike),
                    self.right.value if self.right else "",
                ]
            )
        if self.instrument_type == InstrumentType.FUTURE and self.expiry:
            return f"{self.instrument_type.value}:{self.symbol}:{self.expiry}"
        return f"{self.instrument_type.value}:{self.symbol}"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["instrument_type"] = self.instrument_type.value
        payload["right"] = self.right.value if self.right else None
        payload["canonical_id"] = self.canonical_id
        return payload


@dataclass(frozen=True)
class OptionGreeks:
    implied_vol: float | None = None
    delta: float | None = None
    gamma: float | None = None
    theta: float | None = None
    vega: float | None = None
    rho: float | None = None
    underlier_price: float | None = None
    model: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class Quote:
    instrument: InstrumentId
    provider: Provider
    received_at: datetime
    quality: MarketDataQuality
    provider_symbol: str | None = None
    bid: float | None = None
    ask: float | None = None
    last: float | None = None
    mark: float | None = None
    close: float | None = None
    bid_size: float | None = None
    ask_size: float | None = None
    last_size: float | None = None
    volume: float | None = None
    open_interest: float | None = None
    quote_time: datetime | None = None
    trade_time: datetime | None = None
    source_latency_ms: float | None = None
    market_data_type: str | int | None = None
    greeks: OptionGreeks | None = None
    sampling_mode: str | None = None
    sampling_group: int | None = None
    error: str | None = None
    raw: Mapping[str, Any] | None = None

    @property
    def mid(self) -> float | None:
        if self.bid is None or self.ask is None:
            return None
        if self.bid <= 0 or self.ask <= 0 or self.ask < self.bid:
            return None
        return (self.bid + self.ask) / 2

    @property
    def spread(self) -> float | None:
        if self.bid is None or self.ask is None:
            return None
        if self.bid <= 0 or self.ask <= 0 or self.ask < self.bid:
            return None
        return self.ask - self.bid

    @property
    def spread_bps(self) -> float | None:
        spread = self.spread
        mid = self.mid
        if spread is None or mid is None or mid <= 0:
            return None
        return spread / mid * 10_000.0

    @property
    def effective_price(self) -> float | None:
        return first_present(self.mark, self.mid, self.last, self.close)

    @property
    def has_price(self) -> bool:
        return self.effective_price is not None

    @property
    def is_usable(self) -> bool:
        return self.has_price and self.quality not in {
            MarketDataQuality.MISSING,
            MarketDataQuality.ERROR,
        }

    def quote_age_ms(self, as_of: datetime | None = None) -> float | None:
        source_time = self.quote_time or self.trade_time
        if source_time is None:
            return None
        as_of = as_utc(as_of or self.received_at)
        return max((as_of - as_utc(source_time)).total_seconds() * 1000.0, 0.0)

    def to_dict(self, *, include_raw: bool = False) -> dict[str, Any]:
        payload = {
            "instrument": self.instrument.to_dict(),
            "instrument_id": self.instrument.canonical_id,
            "provider": self.provider.value,
            "provider_symbol": self.provider_symbol,
            "received_at": self.received_at.isoformat(),
            "quality": self.quality.value,
            "bid": self.bid,
            "ask": self.ask,
            "last": self.last,
            "mark": self.mark,
            "close": self.close,
            "bid_size": self.bid_size,
            "ask_size": self.ask_size,
            "last_size": self.last_size,
            "volume": self.volume,
            "open_interest": self.open_interest,
            "quote_time": self.quote_time.isoformat() if self.quote_time else None,
            "trade_time": self.trade_time.isoformat() if self.trade_time else None,
            "source_latency_ms": self.source_latency_ms,
            "market_data_type": self.market_data_type,
            "greeks": self.greeks.to_dict() if self.greeks else None,
            "sampling_mode": self.sampling_mode,
            "sampling_group": self.sampling_group,
            "mid": self.mid,
            "spread": self.spread,
            "spread_bps": self.spread_bps,
            "effective_price": self.effective_price,
            "error": self.error,
        }
        if include_raw:
            payload["raw"] = self.raw
        return payload


@dataclass(frozen=True)
class ProviderState:
    provider: Provider
    status: ProviderStatus
    checked_at: datetime
    reason: str | None = None
    connected: bool | None = None
    authenticated: bool | None = None
    latency_ms: float | None = None
    priority: int | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["provider"] = self.provider.value
        payload["status"] = self.status.value
        payload["checked_at"] = self.checked_at.isoformat()
        return payload


@dataclass(frozen=True)
class NormalizedSnapshot:
    created_at: datetime
    quotes: tuple[Quote, ...]
    provider_states: tuple[ProviderState, ...] = ()

    def quotes_for(self, instrument_id: str) -> tuple[Quote, ...]:
        return tuple(quote for quote in self.quotes if quote.instrument.canonical_id == instrument_id)

    def best_quote(
        self,
        instrument_id: str,
        *,
        provider_priority: Iterable[Provider | str] = DEFAULT_PROVIDER_PRIORITY,
    ) -> Quote | None:
        return choose_best_quote(self.quotes_for(instrument_id), provider_priority=provider_priority)

    def to_dict(self) -> dict[str, Any]:
        return {
            "created_at": self.created_at.isoformat(),
            "quotes": [quote.to_dict() for quote in self.quotes],
            "provider_states": [state.to_dict() for state in self.provider_states],
        }


def instrument_from_dict(payload: Mapping[str, Any]) -> InstrumentId:
    try:
        instrument_type = InstrumentType(str(payload.get("instrument_type", "unknown")))
    except ValueError:
        instrument_type = InstrumentType.UNKNOWN

    right_value = payload.get("right")
    right = normalize_option_right(right_value) if right_value else None
    return InstrumentId(
        symbol=str(payload.get("symbol") or ""),
        instrument_type=instrument_type,
        provider_symbol=payload.get("provider_symbol"),
        exchange=payload.get("exchange"),
        currency=str(payload.get("currency") or "USD"),
        expiry=payload.get("expiry"),
        strike=clean_float(payload.get("strike")),
        right=right,
        multiplier=payload.get("multiplier"),
        underlier=payload.get("underlier"),
        trading_class=payload.get("trading_class"),
    )


def greeks_from_dict(payload: Mapping[str, Any] | None) -> OptionGreeks | None:
    if not payload:
        return None
    return OptionGreeks(
        implied_vol=normalize_implied_vol(payload.get("implied_vol")),
        delta=clean_float(payload.get("delta")),
        gamma=clean_float(payload.get("gamma")),
        theta=clean_float(payload.get("theta")),
        vega=clean_float(payload.get("vega")),
        rho=clean_float(payload.get("rho")),
        underlier_price=clean_float(payload.get("underlier_price")),
        model=payload.get("model"),
    )


def quote_from_dict(payload: Mapping[str, Any]) -> Quote:
    instrument_payload = payload.get("instrument")
    if isinstance(instrument_payload, Mapping):
        instrument = instrument_from_dict(instrument_payload)
    else:
        instrument = InstrumentId(
            symbol=str(payload.get("instrument_id") or payload.get("symbol") or "UNKNOWN"),
            instrument_type=InstrumentType.UNKNOWN,
        )

    try:
        provider = Provider(str(payload.get("provider", Provider.UNKNOWN.value)))
    except ValueError:
        provider = Provider.UNKNOWN
    try:
        quality = MarketDataQuality(str(payload.get("quality", MarketDataQuality.UNKNOWN.value)))
    except ValueError:
        quality = MarketDataQuality.UNKNOWN

    return Quote(
        instrument=instrument,
        provider=provider,
        provider_symbol=payload.get("provider_symbol"),
        received_at=parse_timestamp(payload.get("received_at")) or datetime.now(tz=timezone.utc),
        quality=quality,
        bid=clean_float(payload.get("bid")),
        ask=clean_float(payload.get("ask")),
        last=clean_float(payload.get("last")),
        mark=clean_float(payload.get("mark")),
        close=clean_float(payload.get("close")),
        bid_size=clean_float(payload.get("bid_size")),
        ask_size=clean_float(payload.get("ask_size")),
        last_size=clean_float(payload.get("last_size")),
        volume=clean_float(payload.get("volume")),
        open_interest=clean_float(payload.get("open_interest")),
        quote_time=parse_timestamp(payload.get("quote_time")),
        trade_time=parse_timestamp(payload.get("trade_time")),
        source_latency_ms=clean_float(payload.get("source_latency_ms")),
        market_data_type=payload.get("market_data_type"),
        greeks=greeks_from_dict(
            payload.get("greeks") if isinstance(payload.get("greeks"), Mapping) else None
        ),
        sampling_mode=payload.get("sampling_mode"),
        sampling_group=int(payload["sampling_group"])
        if payload.get("sampling_group") is not None
        else None,
        error=payload.get("error"),
        raw=payload.get("raw") if isinstance(payload.get("raw"), Mapping) else None,
    )


def provider_state_from_dict(payload: Mapping[str, Any]) -> ProviderState:
    try:
        provider = Provider(str(payload.get("provider", Provider.UNKNOWN.value)))
    except ValueError:
        provider = Provider.UNKNOWN
    try:
        status = ProviderStatus(str(payload.get("status", ProviderStatus.UNKNOWN.value)))
    except ValueError:
        status = ProviderStatus.UNKNOWN

    return ProviderState(
        provider=provider,
        status=status,
        checked_at=parse_timestamp(payload.get("checked_at")) or datetime.now(tz=timezone.utc),
        reason=payload.get("reason"),
        connected=bool_or_none(payload.get("connected")),
        authenticated=bool_or_none(payload.get("authenticated")),
        latency_ms=clean_float(payload.get("latency_ms")),
        priority=int(payload["priority"]) if payload.get("priority") is not None else None,
    )


def choose_best_quote(
    quotes: Iterable[Quote],
    *,
    provider_priority: Iterable[Provider | str] = DEFAULT_PROVIDER_PRIORITY,
    as_of: datetime | None = None,
) -> Quote | None:
    candidates = [quote for quote in quotes if quote.is_usable]
    if not candidates:
        return None

    provider_rank = normalize_provider_priority(provider_priority)
    as_of = as_utc(as_of or datetime.now(tz=timezone.utc))

    def sort_key(quote: Quote) -> tuple[int, int, float, float]:
        priority = provider_rank.get(quote.provider, len(provider_rank))
        age_ms = quote.quote_age_ms(as_of)
        freshness = -age_ms if age_ms is not None else -10**12
        return (
            QUALITY_RANK[quote.quality],
            -priority,
            freshness,
            1.0 if quote.mid is not None else 0.0,
        )

    return max(candidates, key=sort_key)


def classify_quote_quality(
    *,
    market_data_type: int | str | None = None,
    quote_time: datetime | None = None,
    received_at: datetime | None = None,
    stale_after_seconds: float = 15.0,
    explicit_delayed: bool | None = None,
    error: str | None = None,
) -> MarketDataQuality:
    if error:
        return MarketDataQuality.ERROR

    mapped = quality_from_market_data_type(market_data_type)
    if mapped is not None:
        return mapped
    if explicit_delayed is True:
        return MarketDataQuality.DELAYED

    if quote_time is not None and received_at is not None:
        age_seconds = (as_utc(received_at) - as_utc(quote_time)).total_seconds()
        if age_seconds > stale_after_seconds:
            return MarketDataQuality.STALE
        return MarketDataQuality.LIVE

    return MarketDataQuality.UNKNOWN


def quality_from_market_data_type(value: int | str | None) -> MarketDataQuality | None:
    try:
        numeric = int(value) if value is not None else None
    except (TypeError, ValueError):
        text = str(value).strip().lower() if value is not None else ""
        return {
            "live": MarketDataQuality.LIVE,
            "frozen": MarketDataQuality.FROZEN,
            "delayed": MarketDataQuality.DELAYED,
            "delayed-frozen": MarketDataQuality.DELAYED_FROZEN,
            "delayed_frozen": MarketDataQuality.DELAYED_FROZEN,
        }.get(text)

    return {
        1: MarketDataQuality.LIVE,
        2: MarketDataQuality.FROZEN,
        3: MarketDataQuality.DELAYED,
        4: MarketDataQuality.DELAYED_FROZEN,
    }.get(numeric)


def clean_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result) or math.isinf(result):
        return None
    return result


def normalize_implied_vol(value: Any) -> float | None:
    result = clean_float(value)
    if result is None:
        return None
    if result > 3.0:
        return result / 100.0
    return result


def parse_timestamp(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return as_utc(value)
    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric > 10_000_000_000:
            numeric = numeric / 1000.0
        try:
            return datetime.fromtimestamp(numeric, tz=timezone.utc)
        except (OSError, OverflowError, ValueError):
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.isdigit():
            return parse_timestamp(int(text))
        try:
            return as_utc(datetime.fromisoformat(text.replace("Z", "+00:00")))
        except ValueError:
            return None
    return None


def first_present(*values: float | None) -> float | None:
    for value in values:
        if value is not None and value > 0:
            return value
    return None


def bool_or_none(*values: Any) -> bool | None:
    for value in values:
        if value is None:
            continue
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            text = value.strip().lower()
            if text in {"true", "1", "yes", "y"}:
                return True
            if text in {"false", "0", "no", "n"}:
                return False
        if isinstance(value, (int, float)):
            return bool(value)
    return None


def elapsed_ms(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    return max((as_utc(end) - as_utc(start)).total_seconds() * 1000.0, 0.0)


def as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def normalize_option_right(value: str | OptionRight) -> OptionRight:
    if isinstance(value, OptionRight):
        return value
    text = str(value).strip().upper()
    if text in {"C", "CALL"}:
        return OptionRight.CALL
    if text in {"P", "PUT"}:
        return OptionRight.PUT
    raise ValueError(f"Unsupported option right: {value!r}")


def normalize_provider_priority(
    providers: Iterable[Provider | str],
) -> dict[Provider, int]:
    result: dict[Provider, int] = {}
    for index, provider in enumerate(providers):
        try:
            normalized = provider if isinstance(provider, Provider) else Provider(str(provider))
        except ValueError:
            normalized = Provider.UNKNOWN
        result[normalized] = index
    return result


def format_strike(value: float | None) -> str:
    if value is None:
        return ""
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.4f}".rstrip("0").rstrip(".")
