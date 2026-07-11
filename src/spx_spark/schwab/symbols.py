"""Schwab symbol mapping backed by the documented runtime table."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from functools import lru_cache
from zoneinfo import ZoneInfo

from spx_spark.runtime_config import runtime_instrument_rows, runtime_value


@dataclass(frozen=True)
class SchwabInstrumentConfig:
    canonical_symbol: str
    instrument_type: str
    quote_symbol: str
    option_chain_symbol: str | None
    option_trading_classes: tuple[str, ...]
    quote_symbol_mode: str
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
                quote_symbol_mode=str(row.get("quote_symbol_mode", "static")).strip().lower(),
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
        if _is_concrete_future_symbol(normalized, instrument):
            return instrument
    return None


def _quarterly_month_codes() -> dict[int, str]:
    raw = runtime_value("schwab.future_contract_resolution.quarterly_month_codes")
    if not isinstance(raw, dict) or not raw:
        raise TypeError("Schwab quarterly month codes must be a non-empty mapping")
    resolved: dict[int, str] = {}
    for month, code in raw.items():
        numeric_month = int(month)
        normalized_code = str(code).strip().upper()
        if numeric_month < 1 or numeric_month > 12 or len(normalized_code) != 1:
            raise ValueError("Invalid Schwab quarterly month-code mapping")
        resolved[numeric_month] = normalized_code
    return dict(sorted(resolved.items()))


def _is_concrete_future_symbol(
    symbol: str,
    instrument: SchwabInstrumentConfig,
) -> bool:
    if instrument.instrument_type != "future" or instrument.quote_symbol_mode != "front_quarterly":
        return False
    month_codes = "".join(re.escape(code) for code in _quarterly_month_codes().values())
    pattern = rf"^{re.escape(instrument.quote_symbol)}[{month_codes}]\d{{2}}$"
    return re.fullmatch(pattern, symbol) is not None


def _third_friday(year: int, month: int) -> date:
    first = date(year, month, 1)
    first_friday = first + timedelta(days=(4 - first.weekday()) % 7)
    return first_friday + timedelta(days=14)


def _exchange_datetime(now: datetime | date | None) -> datetime:
    timezone = ZoneInfo(str(runtime_value("schwab.future_contract_resolution.calendar_timezone")))
    if isinstance(now, date) and not isinstance(now, datetime):
        return datetime.combine(now, time.min, tzinfo=timezone)
    value = now or datetime.now(tz=timezone)
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone)
    return value.astimezone(timezone)


def active_quarterly_contract_month(now: datetime | date | None = None) -> tuple[int, int]:
    """Return the active CME equity-index futures contract as ``(year, month)``."""

    exchange_now = _exchange_datetime(now)
    roll_days = int(
        runtime_value("schwab.future_contract_resolution.roll_calendar_days_before_expiry")
    )
    if roll_days < 0:
        raise ValueError("Schwab futures roll days cannot be negative")
    try:
        roll_session_start = time.fromisoformat(
            str(runtime_value("schwab.future_contract_resolution.roll_session_start_time"))
        )
    except ValueError as exc:
        raise ValueError("Invalid Schwab futures roll session start time") from exc
    for year in (exchange_now.year, exchange_now.year + 1):
        for month in _quarterly_month_codes():
            expiry = _third_friday(year, month)
            roll_date = expiry - timedelta(days=roll_days)
            roll_session_at = datetime.combine(
                roll_date - timedelta(days=1),
                roll_session_start,
                tzinfo=exchange_now.tzinfo,
            )
            if exchange_now < roll_session_at:
                return year, month
    raise RuntimeError("Unable to resolve a Schwab quarterly futures contract")


def resolved_schwab_quote_symbol(
    symbol: str,
    *,
    now: datetime | date | None = None,
) -> str:
    """Expand configured logical futures roots while preserving explicit contracts."""

    normalized = symbol.strip().upper()
    instrument = find_schwab_instrument(normalized)
    if instrument is None or instrument.quote_symbol_mode != "front_quarterly":
        return normalized
    if _is_concrete_future_symbol(normalized, instrument):
        return normalized
    year, month = active_quarterly_contract_month(now)
    month_code = _quarterly_month_codes()[month]
    return f"{instrument.quote_symbol}{month_code}{year % 100:02d}"


def resolved_schwab_quote_symbols(
    symbols: list[str],
    *,
    now: datetime | date | None = None,
) -> list[str]:
    resolved: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        provider_symbol = resolved_schwab_quote_symbol(symbol, now=now)
        if provider_symbol in seen:
            continue
        seen.add(provider_symbol)
        resolved.append(provider_symbol)
    return resolved


def option_chain_symbol_for_schwab(symbol: str) -> str:
    instrument = find_schwab_instrument(symbol)
    if instrument is None:
        return symbol.strip().upper()
    return instrument.option_chain_symbol or instrument.quote_symbol


def canonical_underlier_for_schwab(symbol: str) -> str:
    instrument = find_schwab_instrument(symbol)
    return (
        instrument.canonical_symbol
        if instrument is not None
        else symbol.strip().lstrip("$").upper()
    )


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
