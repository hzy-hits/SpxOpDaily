from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from spx_spark.runtime_config import (
    _load_runtime_config,
    runtime_config,
    runtime_instrument_rows,
    runtime_overrides_path,
    runtime_value,
)
from spx_spark.schwab.symbols import (
    canonical_underlier_for_schwab,
    find_schwab_instrument,
    option_chain_symbol_for_schwab,
    schwab_option_chain_underliers,
    schwab_quote_symbols,
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
    by_symbol = {str(row["canonical_symbol"]): row for row in rows}
    assert by_symbol["SPX"]["chain_interval_seconds"] == 5
    assert by_symbol["SPX"]["option_chain_strike_count"] == 40
    assert by_symbol["SPY"]["chain_interval_seconds"] == 15
    assert "option_chain_strike_count" not in by_symbol["SPY"]
    assert runtime_value("ibkr_stream.max_option_lines") == 68
    assert runtime_value("sampling.hot_window_points") == 55
    assert runtime_value("schwab.collection.request_budget_warning_per_minute") == 100


def test_runtime_provider_priority_makes_schwab_primary_with_ibkr_fallback() -> None:
    priority = runtime_value("market_data.provider_priority")
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


def test_runtime_local_overrides_replace_values_without_repeating_descriptions(
    tmp_path: Path,
) -> None:
    base = tmp_path / "base.yaml"
    overrides = tmp_path / "overrides.yaml"
    base.write_text(
        "feature:\n  enabled:\n    value: false\n    description: Feature gate.\n",
        encoding="utf-8",
    )
    overrides.write_text("feature:\n  enabled:\n    value: true\n", encoding="utf-8")

    merged = _load_runtime_config(str(base), str(overrides))

    assert merged["feature"]["enabled"] == {
        "value": True,
        "description": "Feature gate.",
    }


def test_runtime_local_overrides_reject_unknown_paths(tmp_path: Path) -> None:
    base = tmp_path / "base.yaml"
    overrides = tmp_path / "overrides.yaml"
    base.write_text(
        "feature:\n  enabled:\n    value: false\n    description: Feature gate.\n",
        encoding="utf-8",
    )
    overrides.write_text("feature:\n  typo:\n    value: true\n", encoding="utf-8")

    with pytest.raises(KeyError, match="Unknown runtime override keys"):
        _load_runtime_config(str(base), str(overrides))


def test_runtime_local_overrides_cannot_replace_descriptions(tmp_path: Path) -> None:
    base = tmp_path / "base.yaml"
    overrides = tmp_path / "overrides.yaml"
    base.write_text(
        "feature:\n  enabled:\n    value: false\n    description: Feature gate.\n",
        encoding="utf-8",
    )
    overrides.write_text(
        "feature:\n  enabled:\n    value: true\n    description: Local text.\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="must contain only a value field"):
        _load_runtime_config(str(base), str(overrides))


def test_runtime_local_overrides_reject_value_type_changes(tmp_path: Path) -> None:
    base = tmp_path / "base.yaml"
    overrides = tmp_path / "overrides.yaml"
    base.write_text(
        "feature:\n  enabled:\n    value: false\n    description: Feature gate.\n",
        encoding="utf-8",
    )
    overrides.write_text("feature:\n  enabled:\n    value: 'true'\n", encoding="utf-8")

    with pytest.raises(TypeError, match="must match bool"):
        _load_runtime_config(str(base), str(overrides))


def test_explicit_runtime_override_path_must_exist(monkeypatch, tmp_path: Path) -> None:
    missing = tmp_path / "missing.yaml"
    monkeypatch.setenv("SPX_SPARK_RUNTIME_OVERRIDES", str(missing))

    with pytest.raises(FileNotFoundError, match="Runtime overrides not found"):
        runtime_overrides_path()
