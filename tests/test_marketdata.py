from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    Provider,
    Quote,
    choose_best_quote,
    quote_from_ibkr_row,
    quote_from_schwab_option_contract,
    quote_from_schwab_payload,
)


def test_option_instrument_canonical_id():
    instrument = InstrumentId.option(
        "SPX",
        expiry="20260706",
        strike=7500,
        right="C",
        trading_class="SPXW",
    )

    assert instrument.canonical_id == "option:SPX:SPXW:20260706:7500:C"


def test_schwab_payload_normalizes_index_quote():
    received_at = datetime(2026, 7, 6, 13, 30, 5, tzinfo=timezone.utc)
    quote_time = int((received_at - timedelta(seconds=1)).timestamp() * 1000)
    payload = {
        "assetMainType": "INDEX",
        "quote": {
            "bidPrice": 7500.0,
            "askPrice": 7501.0,
            "lastPrice": 7500.5,
            "quoteTime": quote_time,
        },
        "reference": {"description": "S&P 500 Index"},
    }

    quote = quote_from_schwab_payload("$SPX", payload, received_at=received_at)

    assert quote.instrument.canonical_id == "index:SPX"
    assert quote.provider == Provider.SCHWAB
    assert quote.quality == MarketDataQuality.LIVE
    assert quote.mid == 7500.5
    assert quote.spread_bps == pytest.approx(1.3332, rel=1e-4)
    assert quote.source_latency_ms == pytest.approx(1000.0)


def test_ibkr_row_normalizes_option_quote_and_greeks():
    received_at = datetime(2026, 7, 6, 13, 30, 5, tzinfo=timezone.utc)
    row = SimpleNamespace(
        label="option:SPXW:20260706:7500:C",
        kind="option",
        symbol="SPX",
        market_data_type=1,
        bid=10.0,
        ask=10.5,
        last=10.2,
        market_price=10.25,
        close=None,
        bid_size=3,
        ask_size=4,
        last_size=1,
        ticker_time=(received_at - timedelta(seconds=1)).isoformat(),
        stale=False,
        model_iv=0.18,
        delta=0.51,
        gamma=0.004,
        theta=-1.2,
        vega=0.4,
        und_price=7501.0,
        error=None,
    )

    quote = quote_from_ibkr_row(row, received_at=received_at)

    assert quote.instrument.canonical_id == "option:SPX:SPXW:20260706:7500:C"
    assert quote.provider == Provider.IBKR
    assert quote.quality == MarketDataQuality.LIVE
    assert quote.mid == 10.25
    assert quote.effective_price == 10.25
    assert quote.greeks is not None
    assert quote.greeks.delta == 0.51


def test_schwab_option_contract_normalizes_chain_fields():
    received_at = datetime(2026, 7, 6, 13, 30, 5, tzinfo=timezone.utc)
    quote_time = int((received_at - timedelta(seconds=2)).timestamp() * 1000)
    contract = {
        "symbol": "SPXW  260706C07500000",
        "putCall": "CALL",
        "expirationDate": "2026-07-06T20:00:00+00:00",
        "strikePrice": 7500.0,
        "bid": 12.0,
        "ask": 12.6,
        "mark": 12.3,
        "quoteTimeInLong": quote_time,
        "delta": 0.54,
        "gamma": 0.003,
        "theta": -1.8,
        "vega": 0.42,
        "volatility": 17.2,
    }

    quote = quote_from_schwab_option_contract("SPX", contract, received_at=received_at)

    assert quote.instrument.canonical_id == "option:SPX:SPXW:20260706:7500:C"
    assert quote.quality == MarketDataQuality.LIVE
    assert quote.greeks is not None
    assert quote.greeks.implied_vol == pytest.approx(0.172)
    assert quote.effective_price == 12.3


def test_choose_best_quote_prefers_live_provider_over_stale_priority_provider():
    now = datetime(2026, 7, 6, 13, 30, 5, tzinfo=timezone.utc)
    instrument = InstrumentId.index("SPX")
    ibkr = Quote(
        instrument=instrument,
        provider=Provider.IBKR,
        received_at=now,
        quality=MarketDataQuality.STALE,
        mark=7500.0,
        quote_time=now - timedelta(minutes=2),
    )
    schwab = Quote(
        instrument=instrument,
        provider=Provider.SCHWAB,
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7501.0,
        quote_time=now - timedelta(seconds=1),
    )

    assert choose_best_quote([ibkr, schwab], as_of=now) == schwab


def test_choose_best_quote_uses_provider_priority_when_quality_matches():
    now = datetime(2026, 7, 6, 13, 30, 5, tzinfo=timezone.utc)
    instrument = InstrumentId.index("SPX")
    ibkr = Quote(
        instrument=instrument,
        provider=Provider.IBKR,
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7500.0,
        quote_time=now - timedelta(seconds=3),
    )
    schwab = Quote(
        instrument=instrument,
        provider=Provider.SCHWAB,
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7501.0,
        quote_time=now - timedelta(seconds=1),
    )

    assert choose_best_quote([schwab, ibkr], as_of=now) == ibkr
