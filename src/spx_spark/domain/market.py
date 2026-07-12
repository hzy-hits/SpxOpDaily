"""Market snapshot domain contracts (stdlib-only)."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol


class _InstrumentLike(Protocol):
    symbol: str
    instrument_type: Any
    underlier: str | None
    expiry: str | None

    @property
    def canonical_id(self) -> str: ...


class _QuoteLike(Protocol):
    instrument: _InstrumentLike
    provider: Any
    received_at: datetime


def _require_aware_utc(label: str, value: datetime) -> None:
    if value.tzinfo is None:
        raise ValueError(f"{label} must be timezone-aware UTC")
    if value.utcoffset() != timezone.utc.utcoffset(value):
        raise ValueError(f"{label} must use UTC")


def _canonical_id(instrument: _InstrumentLike) -> str:
    canonical_id = getattr(instrument, "canonical_id", None)
    if isinstance(canonical_id, str) and canonical_id:
        return canonical_id
    kind = getattr(instrument.instrument_type, "value", instrument.instrument_type)
    return f"{kind}:{instrument.symbol}"


def _provider_value(provider: Any) -> str:
    return str(getattr(provider, "value", provider))


@dataclass(frozen=True)
class MarketSnapshot:
    """Immutable market fact batch. Does not perform provider fallback."""

    schema_version: int
    snapshot_id: str
    as_of: datetime
    received_at: datetime
    quotes: tuple[Any, ...]
    provider_states: tuple[Any, ...]
    source_batch_ids: tuple[str, ...]

    def validate(self) -> None:
        if self.schema_version < 1:
            raise ValueError("schema_version must be >= 1")
        if not self.snapshot_id.strip():
            raise ValueError("snapshot_id is required")
        _require_aware_utc("as_of", self.as_of)
        _require_aware_utc("received_at", self.received_at)
        seen: set[tuple[str, str, datetime]] = set()
        for quote in self.quotes:
            key = (
                _canonical_id(quote.instrument),
                _provider_value(quote.provider),
                quote.received_at,
            )
            if key in seen:
                raise ValueError(f"duplicate quote key: {key}")
            seen.add(key)

    def quotes_for(self, instrument_id: str) -> tuple[Any, ...]:
        needle = instrument_id.strip().upper()
        return tuple(
            quote
            for quote in self.quotes
            if _canonical_id(quote.instrument).upper() == needle
            or quote.instrument.symbol.upper() == needle
        )

    def options(self, underlier: str, expiry: str | None = None) -> tuple[Any, ...]:
        underlier_key = underlier.strip().upper()
        selected: list[Any] = []
        for quote in self.quotes:
            instrument = quote.instrument
            kind = str(getattr(instrument.instrument_type, "value", "")).lower()
            if kind != "option":
                continue
            if str(instrument.underlier or "").upper() != underlier_key:
                continue
            if expiry is not None and str(instrument.expiry or "") != expiry:
                continue
            selected.append(quote)
        return tuple(selected)


def dedupe_quotes(quotes: Iterable[Any]) -> tuple[Any, ...]:
    """Keep the last quote for each (canonical_id, provider, received_at)."""
    ordered: dict[tuple[str, str, datetime], Any] = {}
    for quote in quotes:
        key = (
            _canonical_id(quote.instrument),
            _provider_value(quote.provider),
            quote.received_at,
        )
        ordered[key] = quote
    return tuple(ordered.values())


def ensure_quote_sequence(quotes: Sequence[Any]) -> tuple[Any, ...]:
    return tuple(quotes)
