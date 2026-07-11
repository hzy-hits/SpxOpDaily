from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

from spx_spark.schwab.symbols import (
    active_quarterly_contract_month,
    find_schwab_instrument,
    resolved_schwab_quote_symbol,
    resolved_schwab_quote_symbols,
    resolved_schwab_canonical_quote_symbols,
)


def test_front_quarterly_future_resolves_to_concrete_schwab_symbol() -> None:
    assert active_quarterly_contract_month(date(2026, 7, 11)) == (2026, 9)
    assert resolved_schwab_quote_symbol("/ES", now=date(2026, 7, 11)) == "/ESU26"
    assert resolved_schwab_quote_symbol("MES", now=date(2026, 7, 11)) == "/MESU26"


def test_canonical_stream_universe_maps_provider_symbols_before_future_resolution() -> None:
    assert resolved_schwab_canonical_quote_symbols(
        ("SPX", "SPY", "ES"),
        now=date(2026, 7, 11),
    ) == ["$SPX", "SPY", "/ESU26"]


def test_front_future_rolls_on_monday_before_third_friday() -> None:
    assert resolved_schwab_quote_symbol("/ES", now=date(2026, 9, 13)) == "/ESU26"
    assert resolved_schwab_quote_symbol("/ES", now=date(2026, 9, 14)) == "/ESZ26"


def test_front_future_rolls_at_sunday_globex_session_open() -> None:
    new_york = ZoneInfo("America/New_York")
    before_roll_session = datetime(2026, 9, 13, 17, 59, tzinfo=new_york)
    at_roll_session = datetime(2026, 9, 13, 18, 0, tzinfo=new_york)

    assert resolved_schwab_quote_symbol("/ES", now=before_roll_session) == "/ESU26"
    assert resolved_schwab_quote_symbol("/ES", now=at_roll_session) == "/ESZ26"


def test_explicit_future_contract_is_preserved_and_maps_to_logical_instrument() -> None:
    assert resolved_schwab_quote_symbol("/ESU26", now=date(2026, 9, 14)) == "/ESU26"
    instrument = find_schwab_instrument("/ESU26")
    assert instrument is not None
    assert instrument.canonical_symbol == "ES"


def test_resolved_quote_symbols_preserve_order_and_remove_duplicates() -> None:
    assert resolved_schwab_quote_symbols(
        ["SPY", "/ES", "SPY", "/MES"],
        now=date(2026, 7, 11),
    ) == ["SPY", "/ESU26", "/MESU26"]
