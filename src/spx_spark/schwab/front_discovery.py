"""Pure SPX front-chain discovery projection."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from spx_spark.marketdata import Quote
from spx_spark.schwab.chain_discovery import (
    ChainCoverageObservation,
    ChainWidthPolicy,
    measure_chain_coverage,
    next_strike_count,
)
from spx_spark.schwab.hot_lane import HotLanePlan, option_symbol_budget, select_hot_lane


@dataclass(frozen=True)
class FrontDiscovery:
    spot: float
    next_strike_count: int
    hot_plan: HotLanePlan
    coverage: ChainCoverageObservation
    coverage_summary: dict[str, Any]


def build_front_discovery(
    quotes: tuple[Quote, ...],
    *,
    spot: float,
    expiry: str,
    requested_strike_count: int,
    context_symbol_count: int,
    typed_settings: Any,
) -> FrontDiscovery:
    front_quotes = tuple(
        quote
        for quote in quotes
        if quote.instrument.expiry == expiry
        and (quote.instrument.trading_class or "").upper() == "SPXW"
    )
    policy = ChainWidthPolicy(
        candidates=typed_settings.wide_chain.strike_count_candidates,
        min_usable_strikes=typed_settings.wide_chain.min_usable_strikes,
        min_two_sided_ratio=typed_settings.wide_chain.min_two_sided_ratio,
        expected_move_multiple=typed_settings.wide_chain.expected_move_multiple,
        min_width_points=typed_settings.wide_chain.min_width_points,
        max_gap_multiple=typed_settings.wide_chain.max_gap_multiple,
    )
    coverage = measure_chain_coverage(front_quotes, spot=spot)
    next_count = next_strike_count(requested_strike_count, coverage, policy)
    symbol_budget = option_symbol_budget(
        context_symbol_count=context_symbol_count,
        max_batch_size=typed_settings.capacity.max_symbols_per_quote_request,
        reserve=typed_settings.hot_lane.minimum_dynamic_symbol_reserve,
    )
    hot_plan = select_hot_lane(
        front_quotes,
        expiry=expiry,
        spot=spot,
        symbol_budget=symbol_budget,
    )
    gap_multiple = (
        coverage.max_gap / coverage.median_step
        if coverage.max_gap is not None
        and coverage.median_step is not None
        and coverage.median_step > 0
        else None
    )
    return FrontDiscovery(
        spot=spot,
        next_strike_count=next_count,
        hot_plan=hot_plan,
        coverage=coverage,
        coverage_summary={
            "requested_strike_count": requested_strike_count,
            "next_strike_count": next_count,
            "distinct_strikes": coverage.distinct_strikes,
            "usable_strikes": coverage.usable_strikes,
            "two_sided_ratio": coverage.two_sided_ratio,
            "fresh_usable_strikes": coverage.fresh_usable_strikes,
            "fresh_two_sided_ratio": coverage.fresh_two_sided_ratio,
            "positive_oi_strikes": coverage.positive_oi_strikes,
            "market_quote_as_of": (
                coverage.market_quote_as_of.isoformat()
                if coverage.market_quote_as_of is not None
                else None
            ),
            "latest_quote_age_seconds": coverage.latest_quote_age_seconds,
            "market_status": (
                "current"
                if coverage.fresh_usable_strikes > 0
                else "stale"
                if coverage.usable_strikes > 0
                else "missing"
            ),
            "lower_width_points": coverage.lower_width_points,
            "upper_width_points": coverage.upper_width_points,
            "median_step": coverage.median_step,
            "max_gap": coverage.max_gap,
            "max_gap_multiple": gap_multiple,
            "hot_symbol_count": len(hot_plan.symbols),
        },
    )
