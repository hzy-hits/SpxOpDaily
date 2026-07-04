from __future__ import annotations

from datetime import datetime, timedelta, timezone

from spx_spark.marketdata import MarketDataQuality, Provider, ProviderStatus
from spx_spark.schwab.adapter import (
    option_quotes_from_chain_payload,
    snapshot_from_quote_payload,
)


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
