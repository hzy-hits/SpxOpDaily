from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from spx_spark.ibkr.adapter import quote_from_ibkr_row
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    Provider,
    Quote,
    QuoteFreshness,
    choose_best_quote,
    greeks_from_dict,
    normalize_implied_vol,
    normalize_implied_vol_percent,
    quote_from_dict,
    quote_use_decision,
)
from spx_spark.schwab.adapter import (
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
        volume=1250,
        open_interest=4321,
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
    assert quote.volume == 1250
    assert quote.open_interest == 4321


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


def test_option_mid_allows_zero_bid_when_ask_positive() -> None:
    quote = Quote(
        instrument=InstrumentId.option(
            "SPX",
            expiry="20260707",
            strike=7500,
            right="C",
            trading_class="SPXW",
        ),
        provider=Provider.IBKR,
        received_at=datetime(2026, 7, 7, 14, 0, tzinfo=timezone.utc),
        quality=MarketDataQuality.LIVE,
        bid=0.0,
        ask=0.1,
    )

    assert quote.mid == pytest.approx(0.05)


def test_index_mid_rejects_zero_bid() -> None:
    quote = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        received_at=datetime(2026, 7, 7, 14, 0, tzinfo=timezone.utc),
        quality=MarketDataQuality.LIVE,
        bid=0.0,
        ask=0.1,
    )

    assert quote.mid is None


def test_ibkr_model_iv_keeps_decimal_values_above_three() -> None:
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
        ticker_time=received_at.isoformat(),
        stale=False,
        model_iv=3.5,
        delta=0.51,
        gamma=0.004,
        theta=-1.2,
        vega=0.4,
        und_price=7501.0,
        volume=1250,
        open_interest=4321,
        error=None,
    )

    quote = quote_from_ibkr_row(row, received_at=received_at)

    assert quote.greeks is not None
    assert quote.greeks.implied_vol == pytest.approx(3.5)


def test_schwab_percent_iv_normalization_and_missing_sentinel() -> None:
    assert normalize_implied_vol_percent(350) == pytest.approx(3.5)
    assert normalize_implied_vol_percent(-999) is None


def test_quote_from_dict_preserves_normalized_implied_vol() -> None:
    payload = {
        "instrument": {
            "symbol": "SPX",
            "instrument_type": "option",
            "expiry": "20260707",
            "strike": 7500,
            "right": "C",
            "trading_class": "SPXW",
        },
        "provider": "ibkr",
        "received_at": "2026-07-07T14:00:00+00:00",
        "quality": "live",
        "greeks": {"implied_vol": 3.5},
    }

    quote = quote_from_dict(payload)

    assert quote.greeks is not None
    assert quote.greeks.implied_vol == pytest.approx(3.5)
    assert normalize_implied_vol(3.5) == pytest.approx(3.5)
    assert greeks_from_dict({"implied_vol": 3.5}) is not None
    assert greeks_from_dict({"implied_vol": 3.5}).implied_vol == pytest.approx(3.5)


def test_delayed_quote_uses_transport_update_age_for_research_freshness() -> None:
    now = datetime(2026, 7, 7, 14, 0, tzinfo=timezone.utc)
    quote = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        received_at=now,
        quality=MarketDataQuality.DELAYED,
        market_data_type=3,
        mark=7500.0,
        quote_time=now - timedelta(minutes=15),
        last_update_at=now - timedelta(seconds=2),
    )

    decision = quote_use_decision(quote, as_of=now)

    assert decision.freshness == QuoteFreshness.FRESH
    assert decision.research_usable is True
    assert decision.alert_allowed is False
    assert decision.pricing_allowed is False


def test_fresh_live_quote_is_actionable() -> None:
    now = datetime(2026, 7, 7, 14, 0, tzinfo=timezone.utc)
    quote = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7500.0,
        last_update_at=now - timedelta(seconds=1),
    )

    decision = quote_use_decision(quote, as_of=now)

    assert decision.freshness == QuoteFreshness.FRESH
    assert decision.research_usable is True
    assert decision.alert_allowed is True
    assert decision.pricing_allowed is True


def test_frozen_quote_requires_explicit_actionability_permission() -> None:
    now = datetime(2026, 7, 7, 14, 0, tzinfo=timezone.utc)
    quote = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        received_at=now,
        quality=MarketDataQuality.FROZEN,
        mark=7500.0,
        last_update_at=now,
    )

    blocked = quote_use_decision(quote, as_of=now)
    allowed = quote_use_decision(quote, as_of=now, allow_frozen=True)

    assert blocked.research_usable is True
    assert blocked.pricing_allowed is False
    assert allowed.alert_allowed is True
    assert allowed.pricing_allowed is True


def test_delayed_quote_becomes_stale_when_transport_stops_advancing() -> None:
    now = datetime(2026, 7, 7, 14, 0, tzinfo=timezone.utc)
    quote = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        received_at=now,
        quality=MarketDataQuality.DELAYED,
        market_data_type=3,
        mark=7500.0,
        last_update_at=now - timedelta(seconds=61),
    )

    decision = quote_use_decision(quote, as_of=now)

    assert decision.freshness == QuoteFreshness.STALE
    assert decision.research_usable is False
    assert decision.alert_allowed is False


def test_transport_freshness_threshold_is_inclusive() -> None:
    now = datetime(2026, 7, 7, 14, 0, tzinfo=timezone.utc)
    at_boundary = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7500.0,
        last_update_at=now - timedelta(seconds=15),
    )
    over_boundary = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7500.0,
        last_update_at=now - timedelta(seconds=15, microseconds=1),
    )

    assert quote_use_decision(at_boundary, as_of=now).freshness == QuoteFreshness.FRESH
    assert quote_use_decision(over_boundary, as_of=now).freshness == QuoteFreshness.STALE


def test_legacy_delayed_quote_is_unknown_research_only() -> None:
    now = datetime(2026, 7, 7, 14, 0, tzinfo=timezone.utc)
    quote = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        received_at=now,
        quality=MarketDataQuality.DELAYED,
        market_data_type=3,
        mark=7500.0,
        quote_time=now - timedelta(minutes=15),
    )

    decision = quote_use_decision(quote, as_of=now)

    assert decision.freshness == QuoteFreshness.UNKNOWN
    assert decision.research_usable is True
    assert decision.pricing_allowed is False


def test_future_transport_timestamp_fails_closed() -> None:
    now = datetime(2026, 7, 7, 14, 0, tzinfo=timezone.utc)
    quote = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7500.0,
        last_update_at=now + timedelta(seconds=6),
    )

    decision = quote_use_decision(quote, as_of=now)

    assert decision.freshness == QuoteFreshness.UNKNOWN
    assert decision.research_usable is False
    assert decision.alert_allowed is False
    assert decision.pricing_allowed is False


def test_synthetic_quote_is_fresh_research_only() -> None:
    now = datetime(2026, 7, 7, 14, 0, tzinfo=timezone.utc)
    quote = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.INTERNAL,
        received_at=now,
        quality=MarketDataQuality.SYNTHETIC,
        mark=7500.0,
        quote_time=now - timedelta(seconds=1),
    )

    decision = quote_use_decision(quote, as_of=now)

    assert decision.freshness == QuoteFreshness.FRESH
    assert decision.research_usable is True
    assert decision.alert_allowed is False
    assert decision.pricing_allowed is False


def test_legacy_stale_quality_reports_stale_freshness() -> None:
    now = datetime(2026, 7, 7, 14, 0, tzinfo=timezone.utc)
    quote = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.SCHWAB,
        received_at=now,
        quality=MarketDataQuality.STALE,
        mark=7500.0,
    )

    decision = quote_use_decision(quote, as_of=now)

    assert decision.freshness == QuoteFreshness.STALE
    assert decision.research_usable is False
    assert decision.alert_allowed is False


def test_quote_last_update_at_round_trips_through_json_payload() -> None:
    last_update_at = datetime(2026, 7, 7, 14, 0, tzinfo=timezone.utc)
    quote = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        received_at=last_update_at,
        quality=MarketDataQuality.LIVE,
        mark=7500.0,
        last_update_at=last_update_at,
    )

    restored = quote_from_dict(quote.to_dict())

    assert restored.last_update_at == last_update_at
