from __future__ import annotations

from datetime import datetime, timedelta, timezone

from spx_spark.marketdata import MarketDataQuality, Provider, ProviderStatus
from spx_spark.schwab.adapter import (
    option_quotes_from_chain_payload,
    quote_from_schwab_payload,
    snapshot_from_quote_payload,
)
from spx_spark.schwab.symbols import option_chain_symbol_for_schwab


def test_option_chain_symbol_uses_schwab_index_format():
    assert option_chain_symbol_for_schwab("SPX") == "$SPX"
    assert option_chain_symbol_for_schwab("SPXW") == "$SPX"
    assert option_chain_symbol_for_schwab("$SPX") == "$SPX"
    assert option_chain_symbol_for_schwab("XSP") == "$XSP"
    assert option_chain_symbol_for_schwab("SPY") == "SPY"


def test_schwab_quote_payload_builds_provider_snapshot():
    received_at = datetime(2026, 7, 6, 13, 30, 5, tzinfo=timezone.utc)
    quote_time = int((received_at - timedelta(seconds=1)).timestamp() * 1000)
    payload = {
        "$SPX": {
            "assetMainType": "INDEX",
            "quote": {
                "bidPrice": 7500.0,
                "askPrice": 7501.0,
                "lastPrice": 7500.5,
                "quoteTime": quote_time,
            },
            "reference": {"description": "S&P 500 Index"},
        }
    }

    snapshot = snapshot_from_quote_payload(
        payload,
        ["$SPX"],
        received_at=received_at,
    )

    assert snapshot.provider == Provider.SCHWAB
    assert snapshot.quote_count == 1
    assert snapshot.provider_state is not None
    assert snapshot.provider_state.status == ProviderStatus.AVAILABLE
    assert snapshot.quotes[0].instrument.canonical_id == "index:SPX"
    assert snapshot.quotes[0].quality == MarketDataQuality.LIVE


def test_schwab_option_chain_payload_flattens_contracts():
    received_at = datetime(2026, 7, 6, 13, 30, 5, tzinfo=timezone.utc)
    quote_time = int((received_at - timedelta(seconds=1)).timestamp() * 1000)
    payload = {
        "callExpDateMap": {
            "2026-07-06:0": {
                "7500.0": [
                    {
                        "symbol": "SPXW  260706C07500000",
                        "putCall": "CALL",
                        "expirationDate": "2026-07-06T20:00:00+00:00",
                        "strikePrice": 7500.0,
                        "bid": 12.0,
                        "ask": 12.5,
                        "mark": 12.25,
                        "quoteTimeInLong": quote_time,
                    }
                ]
            }
        },
        "putExpDateMap": {},
    }

    quotes = option_quotes_from_chain_payload(payload, underlier="SPX", received_at=received_at)

    assert len(quotes) == 1
    assert quotes[0].provider == Provider.SCHWAB
    assert quotes[0].instrument.canonical_id == "option:SPX:SPXW:20260706:7500:C"
    assert quotes[0].effective_price == 12.25
    assert quotes[0].last_update_at == received_at


def test_schwab_quote_endpoint_parses_spxw_occ_identity() -> None:
    received_at = datetime(2026, 7, 10, 14, 0, 1, tzinfo=timezone.utc)
    quote = quote_from_schwab_payload(
        "SPXW  260710C06300000",
        {
            "assetMainType": "OPTION",
            "quote": {
                "bidPrice": 4.0,
                "askPrice": 4.4,
                "mark": 4.2,
                "quoteTime": int((received_at - timedelta(seconds=1)).timestamp() * 1000),
            },
        },
        received_at=received_at,
    )

    assert quote.instrument.canonical_id == "option:SPX:SPXW:20260710:6300:C"
    assert quote.instrument.provider_symbol == "SPXW  260710C06300000"
    assert quote.last_update_at == received_at
    assert quote.quality is MarketDataQuality.LIVE


def test_schwab_quote_endpoint_parses_monthly_spx_occ_identity() -> None:
    received_at = datetime(2026, 7, 17, 14, 0, 1, tzinfo=timezone.utc)
    quote = quote_from_schwab_payload(
        "SPX   260717P06300000",
        {
            "assetMainType": "OPTION",
            "quote": {
                "lastPrice": 3.5,
                "quoteTime": int((received_at - timedelta(seconds=1)).timestamp() * 1000),
            },
        },
        received_at=received_at,
    )

    assert quote.instrument.canonical_id == "option:SPX:SPX:20260717:6300:P"


def test_schwab_sparse_quote_without_source_timestamp_is_not_actionable() -> None:
    received_at = datetime(2026, 7, 10, 14, 0, 1, tzinfo=timezone.utc)
    quote = quote_from_schwab_payload(
        "$SPX",
        {"assetMainType": "INDEX", "quote": {"mark": 6300.0}},
        received_at=received_at,
    )

    assert quote.quality is MarketDataQuality.UNKNOWN
    assert quote.last_update_at == received_at


def test_configured_schwab_etf_uses_stable_equity_namespace() -> None:
    received_at = datetime(2026, 7, 10, 14, 0, 1, tzinfo=timezone.utc)
    quote = quote_from_schwab_payload(
        "SPY",
        {
            "assetMainType": "EQUITY",
            "assetSubType": "ETF",
            "quote": {
                "lastPrice": 750.0,
                "quoteTime": int((received_at - timedelta(seconds=1)).timestamp() * 1000),
            },
        },
        received_at=received_at,
    )

    assert quote.instrument.canonical_id == "equity:SPY"
    assert quote.instrument.provider_symbol == "SPY"


def test_concrete_schwab_future_uses_stable_logical_namespace() -> None:
    received_at = datetime(2026, 7, 10, 14, 0, 1, tzinfo=timezone.utc)
    quote = quote_from_schwab_payload(
        "/ESU26",
        {
            "assetMainType": "FUTURE",
            "quote": {
                "lastPrice": 7525.0,
                "quoteTime": int((received_at - timedelta(seconds=1)).timestamp() * 1000),
            },
        },
        received_at=received_at,
    )

    assert quote.instrument.canonical_id == "future:ES"
    assert quote.instrument.provider_symbol == "/ESU26"
