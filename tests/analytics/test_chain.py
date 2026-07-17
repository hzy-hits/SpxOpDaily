"""chain_implied_spot robustness: median of the five tightest parity pairs."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from spx_spark.analytics.options import chain_implied_spot, pair_by_strike
from spx_spark.marketdata import InstrumentId, MarketDataQuality, Provider, Quote

NOW = datetime(2026, 7, 13, 14, 0, tzinfo=timezone.utc)


def option(strike: float, right: str, mid: float) -> Quote:
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
        bid=mid - 0.05,
        ask=mid + 0.05,
    )


def build_pairs(rows: list[tuple[float, float, float]]) -> dict:
    quotes = []
    for strike, call_mid, put_mid in rows:
        quotes.append(option(strike, "C", call_mid))
        quotes.append(option(strike, "P", put_mid))
    return pair_by_strike(quotes)


def test_chain_implied_spot_median_rejects_tightest_bad_pair() -> None:
    # The 7498 pair has the tightest |C - P| but is a bad tick ~4 points off
    # the consensus; the single-pair pick would have followed it.
    pairs = build_pairs(
        [
            (7490.0, 14.0, 2.0),  # implied 7502.0
            (7495.0, 9.0, 2.1),  # implied 7501.9
            (7498.0, 4.0, 3.9),  # bad tick: tightest |C-P|, implied 7498.1
            (7500.0, 5.4, 3.3),  # implied 7502.1
            (7505.0, 2.2, 5.3),  # implied 7501.9
            (7510.0, 1.1, 9.2),  # implied 7501.9
        ]
    )

    implied = chain_implied_spot(pairs)

    assert implied == pytest.approx(7501.9)


def test_chain_implied_spot_uses_all_pairs_when_fewer_than_five() -> None:
    pairs = build_pairs(
        [
            (7515.0, 12.0, 10.0),  # implied 7517.0, tightest
            (7550.0, 2.0, 34.0),  # implied 7518.0
        ]
    )

    # Both pairs enter the sample; the median of two is their average.
    assert chain_implied_spot(pairs) == pytest.approx(7517.5)


def test_chain_implied_spot_returns_none_without_paired_mids() -> None:
    assert chain_implied_spot({}) is None
    one_sided = pair_by_strike([option(7500.0, "C", 5.0)])
    assert chain_implied_spot(one_sided) is None
