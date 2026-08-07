from __future__ import annotations

from typing import Any

from spx_spark.schwab.symbols import (
    canonical_underlier_for_schwab,
    find_schwab_instrument,
    option_chain_symbol_for_schwab,
    schwab_instruments,
    schwab_option_chain_underliers,
    schwab_quote_symbols,
)
from spx_spark.settings import current_app_settings, settings_value


def test_every_runtime_setting_has_a_description() -> None:
    settings_found = 0

    def visit(node: Any, path: str) -> None:
        nonlocal settings_found
        if isinstance(node, dict) and "value" in node:
            settings_found += 1
            assert isinstance(node.get("description"), str), path
            assert node["description"].strip(), path
            return
        if isinstance(node, dict):
            for key, value in node.items():
                visit(value, f"{path}.{key}" if path else str(key))
        elif isinstance(node, list):
            for index, value in enumerate(node):
                visit(value, f"{path}[{index}]")

    visit(current_app_settings().raw, "")
    assert settings_found > 0


def test_schwab_instrument_table_owns_index_and_trading_class_aliases() -> None:
    rows = schwab_instruments()
    assert all(row.description for row in rows)
    assert option_chain_symbol_for_schwab("SPX") == "$SPX"
    assert option_chain_symbol_for_schwab("SPXW") == "$SPX"
    assert option_chain_symbol_for_schwab("XSP") == "$XSP"
    assert canonical_underlier_for_schwab("SPXW") == "SPX"
    assert schwab_option_chain_underliers() == ["SPX", "XSP", "SPY", "QQQ", "IWM"]
    by_symbol = {row.canonical_symbol: row for row in rows}
    assert by_symbol["SPX"].chain_interval_seconds == 5
    assert by_symbol["SPX"].option_chain_strike_count == 80
    assert by_symbol["SPY"].chain_interval_seconds == 15
    assert by_symbol["SPY"].option_chain_strike_count is None
    assert settings_value("ibkr_stream.max_option_lines") == 84
    assert settings_value("market_features.hot_option_limit") == 84
    assert settings_value("sampling.hot_window_points") == 55
    assert settings_value("schwab.collection.request_budget_warning_per_minute") == 84
    assert settings_value("schwab.quote_symbol_capacity") == 500
    assert settings_value("schwab.quote_batch_size") == 80


def test_runtime_provider_priority_makes_schwab_primary_with_ibkr_fallback() -> None:
    priority = settings_value("market_data.provider_priority")
    assert priority[:2] == ["schwab", "ibkr"]


def test_schwab_spx_reference_universe_is_configured_without_obsolete_splg() -> None:
    hot_symbols = set(schwab_quote_symbols())
    assert {
        "SPY",
        "RSP",
        "XLB",
        "XLC",
        "XLE",
        "XLF",
        "XLI",
        "XLK",
        "XLP",
        "XLRE",
        "XLU",
        "XLV",
        "XLY",
    } <= hot_symbols
    assert "SPLG" not in hot_symbols
    spym = find_schwab_instrument("SPYM")
    assert spym is not None
    assert spym.collect_quote is False
