import json
from dataclasses import replace

import pytest

from spx_spark.config import SchwabSettings
from spx_spark.schwab.verifier import (
    build_schwab_client,
    count_chain_contracts,
    load_access_token,
    safe_settings_dict,
    validate_gateway_url,
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
