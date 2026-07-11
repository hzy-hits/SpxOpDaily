"""Schwab symbol mapping backed by the documented runtime table."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

from spx_spark.runtime_config import runtime_instrument_rows


@dataclass(frozen=True)
class SchwabInstrumentConfig:
    canonical_symbol: str
    instrument_type: str
    quote_symbol: str
    option_chain_symbol: str | None
    option_trading_classes: tuple[str, ...]
    collect_quote: bool
    collect_option_chain: bool
    description: str


@lru_cache(maxsize=1)
def schwab_instruments() -> tuple[SchwabInstrumentConfig, ...]:
    instruments: list[SchwabInstrumentConfig] = []
    for row in runtime_instrument_rows():
        quote_symbol = str(row.get("quote_symbol", "")).strip().upper()
        chain_raw = str(row.get("option_chain_symbol", "")).strip().upper()
        trading_classes_raw = row.get("option_trading_classes", [])
        if not quote_symbol:
            raise ValueError(f"Missing Schwab quote symbol for {row['canonical_symbol']}")
        if not isinstance(trading_classes_raw, list):
            raise TypeError("option_trading_classes must be a list")
        instruments.append(
            SchwabInstrumentConfig(
                canonical_symbol=row["canonical_symbol"],
                instrument_type=str(row.get("instrument_type", "")).strip().lower(),
                quote_symbol=quote_symbol,
                option_chain_symbol=chain_raw or None,
                option_trading_classes=tuple(
                    str(item).strip().upper() for item in trading_classes_raw if str(item).strip()
                ),
                collect_quote=bool(row.get("collect_quote", False)),
                collect_option_chain=bool(row.get("collect_option_chain", False)),
                description=str(row["description"]).strip(),
            )
        )
    return tuple(instruments)


def find_schwab_instrument(symbol: str) -> SchwabInstrumentConfig | None:
    normalized = symbol.strip().upper()
    for instrument in schwab_instruments():
        aliases = {
            instrument.canonical_symbol,
            instrument.quote_symbol,
            *instrument.option_trading_classes,
        }
        if instrument.option_chain_symbol:
            aliases.add(instrument.option_chain_symbol)
        if normalized in aliases:
            return instrument
    return None


def option_chain_symbol_for_schwab(symbol: str) -> str:
    instrument = find_schwab_instrument(symbol)
    if instrument is None:
        return symbol.strip().upper()
    return instrument.option_chain_symbol or instrument.quote_symbol


def canonical_underlier_for_schwab(symbol: str) -> str:
    instrument = find_schwab_instrument(symbol)
    return instrument.canonical_symbol if instrument is not None else symbol.strip().lstrip("$").upper()


def schwab_quote_symbols() -> list[str]:
    return [item.quote_symbol for item in schwab_instruments() if item.collect_quote]


def schwab_option_chain_underliers() -> list[str]:
    return [item.canonical_symbol for item in schwab_instruments() if item.collect_option_chain]


def schwab_symbols_by_type(instrument_type: str) -> list[str]:
    normalized = instrument_type.strip().lower()
    return [
        item.quote_symbol
        for item in schwab_instruments()
        if item.collect_quote and item.instrument_type == normalized
    ]
