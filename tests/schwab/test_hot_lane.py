from __future__ import annotations

from datetime import datetime, timezone

from spx_spark.marketdata import InstrumentId, MarketDataQuality, OptionRight, Provider, Quote
from spx_spark.schwab.hot_lane import hot_plan_is_fresh, option_symbol_budget, select_hot_lane


def quote(strike: float, right: OptionRight) -> Quote:
    symbol = f"SPXW  260713{right.value}{int(strike * 1000):08d}"
    return Quote(
        instrument=InstrumentId.option(
            "SPX",
            expiry="20260713",
            strike=strike,
            right=right,
            trading_class="SPXW",
            provider_symbol=symbol,
        ),
        provider=Provider.SCHWAB,
        provider_symbol=symbol,
        received_at=datetime(2026, 7, 13, 14, 0, tzinfo=timezone.utc),
        quality=MarketDataQuality.LIVE,
        bid=1,
        ask=2,
    )


def test_hot_lane_uses_all_available_complete_pairs_nearest_atm() -> None:
    quotes = tuple(
        quote(strike, right)
        for strike in (7490.0, 7500.0, 7510.0)
        for right in OptionRight
    )
    plan = select_hot_lane(
        quotes,
        expiry="20260713",
        spot=7501.0,
        symbol_budget=4,
    )

    assert plan.pair_count == 2
    assert len(plan.symbols) == 4
    assert any("07500000" in symbol for symbol in plan.symbols)
    assert option_symbol_budget(context_symbol_count=37, reserve=10) == 453


def test_hot_plan_expires_when_structure_discovery_is_stale() -> None:
    now = datetime(2026, 7, 13, 14, 0, 31, tzinfo=timezone.utc)
    assert not hot_plan_is_fresh(
        hot_expiry="20260713",
        expected_expiry="20260713",
        planned_at=datetime(2026, 7, 13, 14, 0, tzinfo=timezone.utc),
        now=now,
        max_age_seconds=30,
    )
