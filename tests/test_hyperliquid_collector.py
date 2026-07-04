from __future__ import annotations

from datetime import datetime, timezone

import pytest

from spx_spark.hyperliquid.collector import (
    build_asset_context,
    find_asset_context,
    infer_symbol_warning,
    parse_levels,
    quote_from_context,
    recent_trade_stats,
)
from spx_spark.marketdata import InstrumentType, MarketDataQuality, Provider


def test_find_asset_context_matches_universe_index():
    payload = [
        {"universe": [{"name": "BTC"}, {"name": "SPX"}]},
        [{"markPx": "65000"}, {"markPx": "7500"}],
    ]

    asset, context = find_asset_context(payload, "SPX")

    assert asset is not None
    assert asset["name"] == "SPX"
    assert context is not None
    assert context["markPx"] == "7500"


def test_parse_levels_returns_bbo_and_imbalance():
    book = {
        "levels": [
            [{"px": "7499.5", "sz": "2"}, {"px": "7498.0", "sz": "3"}],
            [{"px": "7500.5", "sz": "1"}, {"px": "7501.0", "sz": "1"}],
        ]
    }

    best_bid, best_ask, bid_size, ask_size, imbalance = parse_levels(book, depth=2)

    assert best_bid == 7499.5
    assert best_ask == 7500.5
    assert bid_size == 2
    assert ask_size == 1
    assert imbalance == pytest.approx((5 - 2) / 7)


def test_recent_trade_stats_counts_large_trades():
    trades = [
        {"px": "7500", "sz": "5", "side": "B", "time": 1783344600000},
        {"px": "7510", "sz": "20", "side": "A", "time": 1783344601000},
    ]

    stats = recent_trade_stats(trades, large_trade_notional_threshold=100_000)

    assert stats.trade_count == 2
    assert stats.last_price == 7510
    assert stats.buy_notional == 37_500
    assert stats.sell_notional == 150_200
    assert stats.large_trade_count == 1
    assert stats.large_trade_notional == 150_200


def test_build_asset_context_and_quote():
    received_at = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)
    all_mids = {"SPX": "7500.25"}
    meta_and_contexts = [
        {"universe": [{"name": "SPX"}]},
        [
            {
                "markPx": "7500.5",
                "oraclePx": "7500.0",
                "funding": "0.000012",
                "openInterest": "123.45",
                "dayNtlVlm": "4567890.0",
            }
        ],
    ]
    book = {
        "levels": [
            [{"px": "7500.0", "sz": "3"}],
            [{"px": "7501.0", "sz": "4"}],
        ]
    }
    trades = [{"px": "7500.75", "sz": "2", "side": "B", "time": 1783344600000}]

    context = build_asset_context(
        coin="SPX",
        all_mids=all_mids,
        meta_and_contexts=meta_and_contexts,
        book=book,
        trades=trades,
        received_at=received_at,
        book_depth_levels=5,
        large_trade_notional_threshold=100_000,
    )
    quote = quote_from_context(context)

    assert context.mark_px == 7500.5
    assert context.oracle_px == 7500.0
    assert context.funding == 0.000012
    assert context.open_interest == 123.45
    assert context.premium == 0.5
    assert context.premium_bps == pytest.approx(0.666666, rel=1e-4)
    assert quote.instrument.instrument_type == InstrumentType.CRYPTO_PERP
    assert quote.instrument.canonical_id == "crypto_perp:SPX"
    assert quote.provider == Provider.HYPERLIQUID
    assert quote.quality == MarketDataQuality.LIVE
    assert quote.mark == 7500.5
    assert quote.open_interest == 123.45


def test_infer_symbol_warning_for_low_priced_hyperliquid_spx():
    warning = infer_symbol_warning("SPX", 0.43)

    assert warning is not None
    assert "not official Cboe SPX" in warning
