from __future__ import annotations

from datetime import datetime, timedelta, timezone

from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    OptionGreeks,
    Provider,
    Quote,
)
from spx_spark.storage import latest_by_provider


def test_new_hot_price_does_not_clear_chain_structure() -> None:
    base = datetime(2026, 7, 13, 14, 0, tzinfo=timezone.utc)
    instrument = InstrumentId.option(
        "SPX",
        expiry="20260713",
        strike=7500,
        right="C",
        trading_class="SPXW",
    )
    chain = Quote(
        instrument=instrument,
        provider=Provider.SCHWAB,
        received_at=base,
        quote_time=base,
        structure_time=base,
        quality=MarketDataQuality.LIVE,
        bid=10,
        ask=12,
        open_interest=1234,
        greeks=OptionGreeks(delta=0.5, gamma=0.02, model="schwab_chain"),
    )
    hot = Quote(
        instrument=instrument,
        provider=Provider.SCHWAB,
        received_at=base + timedelta(seconds=2),
        quote_time=base + timedelta(seconds=2),
        quality=MarketDataQuality.LIVE,
        bid=11,
        ask=13,
    )

    merged = latest_by_provider((chain, hot))[0]
    assert (merged.bid, merged.ask) == (11, 13)
    assert merged.open_interest == 1234
    assert merged.greeks is not None and merged.greeks.gamma == 0.02
    assert merged.structure_time == base


def test_new_quote_open_interest_does_not_clear_chain_greeks() -> None:
    base = datetime(2026, 7, 13, 14, 0, tzinfo=timezone.utc)
    instrument = InstrumentId.option(
        "SPX", expiry="20260713", strike=7500, right="P", trading_class="SPXW"
    )
    chain = Quote(
        instrument=instrument,
        provider=Provider.SCHWAB,
        received_at=base,
        structure_time=base,
        quality=MarketDataQuality.LIVE,
        bid=10,
        ask=12,
        open_interest=100,
        greeks=OptionGreeks(delta=-0.5, gamma=0.02),
    )
    hot = Quote(
        instrument=instrument,
        provider=Provider.SCHWAB,
        received_at=base + timedelta(seconds=2),
        quote_time=base + timedelta(seconds=2),
        quality=MarketDataQuality.LIVE,
        bid=11,
        ask=13,
        open_interest=101,
    )

    merged = latest_by_provider((chain, hot))[0]
    assert merged.open_interest == 101
    assert merged.greeks is not None and merged.greeks.gamma == 0.02
