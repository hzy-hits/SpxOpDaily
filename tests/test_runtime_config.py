from __future__ import annotations

from typing import Any

from spx_spark.runtime_config import runtime_config, runtime_instrument_rows, runtime_value
from spx_spark.schwab.symbols import (
    canonical_underlier_for_schwab,
    option_chain_symbol_for_schwab,
    schwab_option_chain_underliers,
)


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

    visit(runtime_config(), "")
    assert settings_found > 0


def test_schwab_instrument_table_owns_index_and_trading_class_aliases() -> None:
    rows = runtime_instrument_rows()
    assert all(str(row["description"]).strip() for row in rows)
    assert option_chain_symbol_for_schwab("SPX") == "$SPX"
    assert option_chain_symbol_for_schwab("SPXW") == "$SPX"
    assert option_chain_symbol_for_schwab("XSP") == "$XSP"
    assert canonical_underlier_for_schwab("SPXW") == "SPX"
    assert schwab_option_chain_underliers() == ["SPX", "XSP", "SPY", "QQQ", "IWM"]


def test_runtime_provider_priority_makes_schwab_primary_with_ibkr_fallback() -> None:
    priority = runtime_value("market_data.provider_priority")
    assert priority[:2] == ["schwab", "ibkr"]
