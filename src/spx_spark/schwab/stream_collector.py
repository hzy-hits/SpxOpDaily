"""Pure Schwab Level-One streaming message assembly and normalization."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import datetime, timezone
from threading import Lock
from typing import Any

from spx_spark.marketdata import (
    OptionGreeks,
    Provider,
    Quote,
    as_utc,
    classify_quote_quality,
    clean_float,
    elapsed_ms,
    normalize_implied_vol_percent,
    parse_timestamp,
)
from spx_spark.provider_adapter import ProviderSnapshot
from spx_spark.schwab.adapter import instrument_from_schwab_symbol, schwab_model_float


SUPPORTED_LEVEL_ONE_SERVICES = frozenset(
    {"LEVELONE_EQUITIES", "LEVELONE_FUTURES", "LEVELONE_OPTIONS"}
)
DEFAULT_STREAM_STALE_SECONDS = 15


class SchwabStreamQuoteAssembler:
    """Merge sparse WebSocket deltas and drain only symbols changed since the last flush."""

    def __init__(self, *, stale_after_seconds: float = DEFAULT_STREAM_STALE_SECONDS) -> None:
        if stale_after_seconds <= 0:
            raise ValueError("stale_after_seconds must be positive")
        self.stale_after_seconds = stale_after_seconds
        self._rows: dict[tuple[str, str], dict[str, Any]] = {}
        self._received_at: dict[tuple[str, str], datetime] = {}
        self._dirty: set[tuple[str, str]] = set()
        self._option_symbols: set[str] | None = None
        self._lock = Lock()

    def retain_option_symbols(self, symbols: list[str]) -> int:
        """Evict option deltas after the hot subscription window rotates."""

        retained = {symbol.strip().upper() for symbol in symbols if symbol.strip()}
        with self._lock:
            self._option_symbols = retained
            expired = [
                key
                for key in self._rows
                if key[0] == "LEVELONE_OPTIONS" and key[1] not in retained
            ]
            for key in expired:
                self._rows.pop(key, None)
                self._received_at.pop(key, None)
                self._dirty.discard(key)
            return len(expired)

    def retained_symbol_counts(self) -> dict[str, int]:
        """Expose bounded cache size for health checks without leaking symbols."""

        with self._lock:
            return dict(Counter(service for service, _symbol in self._rows))

    def ingest(
        self,
        message: Mapping[str, Any],
        *,
        received_at: datetime | None = None,
    ) -> int:
        service = str(message.get("service") or "").upper()
        if service not in SUPPORTED_LEVEL_ONE_SERVICES:
            return 0
        content = message.get("content")
        if not isinstance(content, list):
            return 0
        observed_at = as_utc(received_at or datetime.now(tz=timezone.utc))
        accepted = 0
        with self._lock:
            for item in content:
                if not isinstance(item, Mapping):
                    continue
                symbol = str(item.get("SYMBOL") or item.get("key") or "").strip().upper()
                if not symbol:
                    continue
                if (
                    service == "LEVELONE_OPTIONS"
                    and self._option_symbols is not None
                    and symbol not in self._option_symbols
                ):
                    continue
                key = (service, symbol)
                merged = dict(self._rows.get(key, {}))
                for field_name, value in item.items():
                    if field_name == "key" or value is None:
                        continue
                    merged[str(field_name)] = value
                merged["SYMBOL"] = symbol
                self._rows[key] = merged
                self._received_at[key] = observed_at
                self._dirty.add(key)
                accepted += 1
        return accepted

    def drain_snapshot(self) -> ProviderSnapshot | None:
        with self._lock:
            dirty = sorted(self._dirty)
            self._dirty.clear()
            rows = [
                (service, dict(self._rows[(service, symbol)]), self._received_at[(service, symbol)])
                for service, symbol in dirty
            ]
        quotes = tuple(
            quote
            for service, fields, received_at in rows
            if (
                quote := quote_from_stream_fields(
                    service,
                    fields,
                    received_at=received_at,
                    stale_after_seconds=self.stale_after_seconds,
                )
            ).effective_price
            is not None
        )
        if not quotes:
            return None
        return ProviderSnapshot(
            provider=Provider.SCHWAB,
            received_at=max(quote.received_at for quote in quotes),
            quotes=quotes,
            metadata={"sampling_mode": "schwab_stream"},
        )


def quote_from_stream_fields(
    service: str,
    fields: Mapping[str, Any],
    *,
    received_at: datetime,
    stale_after_seconds: float = DEFAULT_STREAM_STALE_SECONDS,
) -> Quote:
    normalized_service = service.strip().upper()
    if normalized_service not in SUPPORTED_LEVEL_ONE_SERVICES:
        raise ValueError(f"Unsupported Schwab streaming service: {service}")
    symbol = str(fields.get("SYMBOL") or fields.get("key") or "").strip().upper()
    if not symbol:
        raise ValueError("Schwab streaming content has no symbol")
    received_at = as_utc(received_at)
    quote_time = parse_timestamp(fields.get("QUOTE_TIME_MILLIS"))
    trade_time = parse_timestamp(fields.get("TRADE_TIME_MILLIS"))
    quality = classify_quote_quality(
        quote_time=quote_time or trade_time,
        received_at=received_at,
        stale_after_seconds=stale_after_seconds,
        explicit_delayed=False,
    )
    open_interest = clean_float(fields.get("OPEN_INTEREST"))
    greeks = None
    structure_time = None
    if normalized_service == "LEVELONE_OPTIONS":
        greeks = OptionGreeks(
            implied_vol=normalize_implied_vol_percent(fields.get("VOLATILITY")),
            delta=schwab_model_float(fields.get("DELTA")),
            gamma=schwab_model_float(fields.get("GAMMA")),
            theta=schwab_model_float(fields.get("THETA")),
            vega=schwab_model_float(fields.get("VEGA")),
            rho=schwab_model_float(fields.get("RHO")),
            underlier_price=schwab_model_float(fields.get("UNDERLYING_PRICE")),
            model="schwab_stream",
        )
        if (open_interest or 0.0) > 0 or any(
            value is not None
            for value in (
                greeks.implied_vol,
                greeks.delta,
                greeks.gamma,
                greeks.theta,
                greeks.vega,
                greeks.rho,
            )
        ):
            structure_time = received_at
    return Quote(
        instrument=instrument_from_schwab_symbol(symbol),
        provider=Provider.SCHWAB,
        provider_symbol=symbol,
        received_at=received_at,
        quality=quality,
        bid=clean_float(fields.get("BID_PRICE")),
        ask=clean_float(fields.get("ASK_PRICE")),
        last=clean_float(fields.get("LAST_PRICE")),
        mark=clean_float(fields.get("MARK")),
        close=clean_float(fields.get("CLOSE_PRICE")),
        bid_size=clean_float(fields.get("BID_SIZE")),
        ask_size=clean_float(fields.get("ASK_SIZE")),
        last_size=clean_float(fields.get("LAST_SIZE")),
        volume=clean_float(fields.get("TOTAL_VOLUME")),
        open_interest=open_interest,
        structure_time=structure_time,
        quote_time=quote_time,
        trade_time=trade_time,
        last_update_at=received_at,
        source_latency_ms=elapsed_ms(quote_time or trade_time, received_at),
        market_data_type="live",
        greeks=greeks,
        sampling_mode="schwab_stream",
        raw={"service": normalized_service, "fields": dict(fields)},
    )
