from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from spx_spark.market_context import build_market_context
from spx_spark.marketdata import InstrumentId, MarketDataQuality, Provider, Quote
from spx_spark.storage import LatestState


BJ_TZ = ZoneInfo("Asia/Shanghai")


def make_quote(instrument_id: InstrumentId, price: float, close: float, now: datetime) -> Quote:
    return Quote(
        instrument=instrument_id,
        provider=Provider.IBKR,
        provider_symbol=instrument_id.canonical_id,
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=price,
        close=close,
        quote_time=now,
    )


def test_market_context_includes_vol_and_cross_asset_ratios() -> None:
    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)
    state = LatestState(
        created_at=now,
        as_of=now,
        quotes=(),
        best_quotes=(
            make_quote(InstrumentId.index("VIX1D"), 14.0, 13.0, now),
            make_quote(InstrumentId.index("VIX9D"), 16.0, 15.5, now),
            make_quote(InstrumentId.index("VIX"), 18.0, 17.0, now),
            make_quote(InstrumentId.index("VIX3M"), 20.0, 19.5, now),
            make_quote(InstrumentId.equity("SPY"), 750.0, 745.0, now),
            make_quote(InstrumentId.equity("QQQ"), 725.0, 720.0, now),
            make_quote(InstrumentId.equity("HYG"), 80.0, 79.5, now),
            make_quote(InstrumentId.equity("LQD"), 108.0, 108.5, now),
        ),
    )

    context = build_market_context(state)

    entries = {entry["instrument_id"]: entry for entry in context["entries"]}
    assert entries["index:VIX"]["quality"] == "live"
    assert entries["index:NDX"]["quality"] == "missing"
    assert context["derived"]["vix1d_vix9d"] == 14.0 / 16.0
    assert context["derived"]["vix9d_vix"] == 16.0 / 18.0
    assert context["derived"]["vix_vix3m"] == 18.0 / 20.0
    assert context["derived"]["qqq_spy"] == 725.0 / 750.0
    assert context["derived"]["hyg_lqd"] == 80.0 / 108.0
