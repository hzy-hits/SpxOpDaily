import json
from dataclasses import replace
from datetime import date

import pytest

from spx_spark.config import SchwabSettings
from spx_spark.schwab.verifier import (
    MAX_QUOTE_BATCH_SIZE,
    build_schwab_client,
    count_chain_contracts,
    load_access_token,
    quote_batches,
    safe_settings_dict,
    validate_gateway_url,
    verify_option_chains,
    verify_quotes,
)


def make_settings(token_file: str) -> SchwabSettings:
    return SchwabSettings(
        api_base_url="https://api.schwabapi.com",
        access_token="",
        token_file=token_file,
        verify_indexes=["$SPX", "$VIX"],
        verify_equities=["SPY"],
        verify_futures=["/ES"],
        verify_option_chains=["SPX"],
        option_chain_strike_count=10,
        quote_fields="quote,reference",
        request_timeout_seconds=12.0,
    )


def test_load_access_token_supports_nested_schwab_py_shape(tmp_path):
    token_file = tmp_path / "token.json"
    token_file.write_text(
        json.dumps({"token": {"access_token": "abc123"}}),
        encoding="utf-8",
    )

    assert load_access_token(make_settings(str(token_file))) == "abc123"


def test_count_chain_contracts():
    chain_map = {
        "2026-07-06:2": {
            "7500.0": [{"symbol": "A"}, {"symbol": "B"}],
            "7505.0": [{"symbol": "C"}],
        }
    }

    assert count_chain_contracts(chain_map) == 3


def test_gateway_client_does_not_require_or_send_access_token(tmp_path):
    settings = replace(
        make_settings(str(tmp_path / "missing-token.json")),
        gateway_url="http://127.0.0.1:8184",
    )

    client = build_schwab_client(settings)

    assert client is not None
    assert client.access_token == ""
    assert client.api_base_url == "http://127.0.0.1:8184"


def test_safe_settings_redacts_all_schwab_credentials(tmp_path):
    settings = replace(
        make_settings(str(tmp_path / "token.json")),
        access_token="access",
        app_key="key",
        app_secret="secret",
    )

    safe = safe_settings_dict(settings)

    assert safe["access_token"] == "***"
    assert safe["app_key"] == "***"
    assert safe["app_secret"] == "***"


def test_gateway_url_must_remain_loopback_only():
    validate_gateway_url("http://127.0.0.1:8184")
    validate_gateway_url("http://localhost:8184")
    with pytest.raises(ValueError, match="loopback"):
        validate_gateway_url("http://gateway.example.com")
    with pytest.raises(ValueError, match="IPv4 loopback"):
        validate_gateway_url("http://[::1]:8184")


def test_quote_batches_default_to_500_symbols():
    symbols = [f"SYMBOL{index}" for index in range(1_001)]

    batches = quote_batches(symbols)

    assert MAX_QUOTE_BATCH_SIZE == 500
    assert [len(batch) for batch in batches] == [500, 500, 1]
    assert [symbol for batch in batches for symbol in batch] == symbols
    with pytest.raises(ValueError, match="between 1 and 500"):
        quote_batches(symbols, 501)


def test_quote_verifier_uses_500_symbol_batches(tmp_path):
    settings = replace(
        make_settings(str(tmp_path / "token.json")),
        verify_indexes=[f"SYMBOL{index}" for index in range(1_001)],
        verify_equities=[],
        verify_futures=[],
    )

    class RecordingClient:
        def __init__(self) -> None:
            self.batch_sizes: list[int] = []

        def get_json(self, path: str, params: dict[str, object]) -> tuple[int, object]:
            assert path == "/marketdata/v1/quotes"
            self.batch_sizes.append(len(str(params["symbols"]).split(",")))
            return 200, {}

    client = RecordingClient()

    results = verify_quotes(client, settings)  # type: ignore[arg-type]

    assert client.batch_sizes == [500, 500, 1]
    assert len(results) == 3
    assert all(result.ok for result in results)


def test_option_chain_verifier_maps_spx_to_provider_index_symbol(tmp_path):
    settings = replace(
        make_settings(str(tmp_path / "token.json")),
        verify_option_chains=["SPX", "SPXW", "XSP", "SPY"],
    )

    class RecordingClient:
        def __init__(self) -> None:
            self.symbols: list[str] = []

        def get_json(self, path: str, params: dict[str, object]) -> tuple[int, object]:
            assert path == "/marketdata/v1/chains"
            self.symbols.append(str(params["symbol"]))
            return 200, {"status": "SUCCESS"}

    client = RecordingClient()

    results = verify_option_chains(client, settings)  # type: ignore[arg-type]

    assert client.symbols == ["$SPX", "$SPX", "$XSP", "SPY"]
    assert [result.label for result in results] == ["SPX", "SPXW", "XSP", "SPY"]
    assert all(result.ok for result in results)


def test_quote_verifier_resolves_logical_future_root(tmp_path):
    settings = replace(
        make_settings(str(tmp_path / "token.json")),
        verify_indexes=[],
        verify_equities=[],
        verify_futures=["/ES", "/MES"],
    )

    class RecordingClient:
        def __init__(self) -> None:
            self.symbols = ""

        def get_json(self, path: str, params: dict[str, object]) -> tuple[int, object]:
            assert path == "/marketdata/v1/quotes"
            self.symbols = str(params["symbols"])
            return 200, {}

    client = RecordingClient()
    results = verify_quotes(  # type: ignore[arg-type]
        client,
        settings,
        now=date(2026, 7, 11),
    )

    assert client.symbols == "/ESU26,/MESU26"
    assert len(results) == 1
    assert results[0].ok is True
