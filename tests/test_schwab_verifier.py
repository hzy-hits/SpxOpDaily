import json

from spx_spark.config import SchwabSettings
from spx_spark.schwab.verifier import count_chain_contracts, load_access_token


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
