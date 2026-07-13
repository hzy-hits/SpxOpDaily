from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

from spx_spark.analytics.options import build_max_pain, enrich_open_interest, pair_by_strike
from spx_spark.marketdata import InstrumentId, MarketDataQuality, Provider, Quote


NOW = datetime(2026, 7, 13, 14, 0, tzinfo=timezone.utc)


def option(strike: float, right: str, open_interest: float | None) -> Quote:
    return Quote(
        instrument=InstrumentId.option(
            "SPX",
            expiry="20260713",
            strike=strike,
            right=right,
            trading_class="SPXW",
        ),
        provider=Provider.IBKR,
        received_at=NOW,
        quality=MarketDataQuality.LIVE,
        bid=1.0,
        ask=1.2,
        open_interest=open_interest,
    )


def test_build_max_pain_combines_call_and_put_open_interest() -> None:
    pairs = pair_by_strike(
        [
            option(90, "C", 10),
            option(90, "P", 1),
            option(100, "C", 5),
            option(100, "P", 5),
            option(110, "C", 1),
            option(110, "P", 10),
        ]
    )

    result = build_max_pain(pairs, underlier=101)

    assert result is not None
    assert result.settlement_strike == 100
    assert result.payout_points == 200
    assert result.call_oi_peak_strike == 90
    assert result.call_oi_peak == 10
    assert result.put_oi_peak_strike == 110
    assert result.put_oi_peak == 10
    assert result.quality == "partial_window"


def test_build_max_pain_returns_none_without_open_interest() -> None:
    pairs = pair_by_strike([option(100, "C", None), option(100, "P", 0)])

    assert build_max_pain(pairs, underlier=100) is None


def test_open_interest_is_enriched_without_replacing_selected_price() -> None:
    schwab = option(100, "C", 0)
    schwab = replace(schwab, provider=Provider.SCHWAB, bid=4.0, ask=4.2)
    ibkr = option(100, "C", 1234)
    ibkr = replace(ibkr, bid=3.0, ask=3.2)

    enriched = enrich_open_interest((schwab,), (schwab, ibkr))[0]

    assert enriched.provider is Provider.SCHWAB
    assert enriched.bid == 4.0
    assert enriched.ask == 4.2
    assert enriched.open_interest == 1234
    assert enriched.raw is not None
    assert enriched.raw["open_interest_provider"] == "ibkr"
