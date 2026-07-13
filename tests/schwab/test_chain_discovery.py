from __future__ import annotations

from datetime import datetime, timezone

from spx_spark.marketdata import InstrumentId, MarketDataQuality, OptionRight, Provider, Quote
from spx_spark.schwab.chain_discovery import (
    ChainWidthPolicy,
    coverage_sufficient,
    measure_chain_coverage,
    next_strike_count,
)


NOW = datetime(2026, 7, 13, 14, 0, tzinfo=timezone.utc)


def option(strike: float, right: OptionRight) -> Quote:
    return Quote(
        instrument=InstrumentId.option(
            "SPX",
            expiry="20260713",
            strike=strike,
            right=right,
            trading_class="SPXW",
        ),
        provider=Provider.SCHWAB,
        received_at=NOW,
        quality=MarketDataQuality.LIVE,
        bid=1.0,
        ask=1.2,
    )


def chain(strikes: range) -> tuple[Quote, ...]:
    return tuple(option(float(strike), right) for strike in strikes for right in OptionRight)


def test_adaptive_width_stays_at_smallest_sufficient_candidate() -> None:
    policy = ChainWidthPolicy()
    observation = measure_chain_coverage(chain(range(7300, 7705, 5)), spot=7500.0)

    assert coverage_sufficient(observation, policy)
    assert next_strike_count(80, observation, policy) == 80


def test_adaptive_width_escalates_but_is_bounded() -> None:
    policy = ChainWidthPolicy()
    observation = measure_chain_coverage(chain(range(7450, 7555, 5)), spot=7500.0)

    assert not coverage_sufficient(observation, policy)
    assert next_strike_count(80, observation, policy) == 100
    assert next_strike_count(100, observation, policy) == 120
    assert next_strike_count(120, observation, policy) == 120


def test_large_grid_gap_blocks_otherwise_wide_chain() -> None:
    policy = ChainWidthPolicy(min_usable_strikes=10)
    strikes = list(range(7300, 7440, 5)) + list(range(7560, 7705, 5))
    observation = measure_chain_coverage(
        tuple(option(float(strike), right) for strike in strikes for right in OptionRight),
        spot=7500.0,
    )

    assert observation.max_gap == 125.0
    assert observation.median_step == 5.0
    assert not coverage_sufficient(observation, policy)


def test_stale_chain_preserves_geometry_but_reports_no_fresh_coverage() -> None:
    stale = tuple(
        Quote(
            **{
                **option(float(strike), right).__dict__,
                "quality": MarketDataQuality.STALE,
                "quote_time": NOW.replace(day=10),
            }
        )
        for strike in range(7450, 7555, 5)
        for right in OptionRight
    )

    observation = measure_chain_coverage(stale, spot=7500.0)

    assert observation.usable_strikes == 21
    assert observation.fresh_usable_strikes == 0
    assert observation.fresh_two_sided_ratio == 0.0
    assert observation.latest_quote_age_seconds == 3 * 24 * 60 * 60
