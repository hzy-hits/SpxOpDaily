"""Typed policy for the Growth Dislocation LEAPS scanner."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class GrowthDislocationSettings:
    enabled: bool
    universe_path: str
    max_price_location_52w: float
    max_dividend_yield: float
    min_market_cap: float
    max_ivp_13w: float
    max_ivp_26w: float
    ibkr_history_timeout_seconds: float
    ibkr_history_concurrency: int
    min_leaps_dte: int
    preferred_leaps_dte: int
    max_leaps_dte: int
    target_delta_min: float
    target_delta_max: float
    max_leaps_spread_mid: float
    hard_max_leaps_spread_mid: float
    rsi_oversold_threshold: float
    rsi_recovery_min: float
    rsi_recovery_optimal_low: float
    rsi_recovery_optimal_high: float
    chain_strike_count: int
    rth_request_budget: int
    daily_request_budget: int
    quote_max_age_seconds: float
    option_quote_max_age_seconds: float
    max_names_per_crowding_group: int
    top_count: int

    def __post_init__(self) -> None:
        if not self.universe_path.strip():
            raise ValueError("growth-dislocation universe_path cannot be empty")
        for label, value in (
            ("max_price_location_52w", self.max_price_location_52w),
            ("max_dividend_yield", self.max_dividend_yield),
            ("max_ivp_13w", self.max_ivp_13w),
            ("max_ivp_26w", self.max_ivp_26w),
            ("target_delta_min", self.target_delta_min),
            ("target_delta_max", self.target_delta_max),
            ("max_leaps_spread_mid", self.max_leaps_spread_mid),
            ("hard_max_leaps_spread_mid", self.hard_max_leaps_spread_mid),
        ):
            if not 0.0 < value <= 1.0:
                raise ValueError(f"growth-dislocation {label} must be in (0, 1]")
        if self.target_delta_min >= self.target_delta_max:
            raise ValueError("growth-dislocation target delta range is invalid")
        if self.max_leaps_spread_mid > self.hard_max_leaps_spread_mid:
            raise ValueError("growth-dislocation spread thresholds are not ordered")
        if self.min_market_cap <= 0.0:
            raise ValueError("growth-dislocation min_market_cap must be positive")
        if self.ibkr_history_timeout_seconds <= 0.0:
            raise ValueError("growth-dislocation IBKR history timeout must be positive")
        if self.ibkr_history_concurrency <= 0:
            raise ValueError("growth-dislocation IBKR history concurrency must be positive")
        if not 0 < self.min_leaps_dte <= self.preferred_leaps_dte <= self.max_leaps_dte:
            raise ValueError("growth-dislocation LEAPS DTE thresholds are not ordered")
        if not (
            0.0
            < self.rsi_oversold_threshold
            < self.rsi_recovery_min
            <= self.rsi_recovery_optimal_low
            < self.rsi_recovery_optimal_high
            < 100.0
        ):
            raise ValueError("growth-dislocation RSI thresholds are not ordered")
        for label, value in (
            ("chain_strike_count", self.chain_strike_count),
            ("rth_request_budget", self.rth_request_budget),
            ("daily_request_budget", self.daily_request_budget),
            ("max_names_per_crowding_group", self.max_names_per_crowding_group),
            ("top_count", self.top_count),
        ):
            if value <= 0:
                raise ValueError(f"growth-dislocation {label} must be positive")
        if self.rth_request_budget > self.daily_request_budget:
            raise ValueError("growth-dislocation request budgets are not ordered")
        if self.quote_max_age_seconds <= 0.0 or self.option_quote_max_age_seconds <= 0.0:
            raise ValueError("growth-dislocation quote ages must be positive")


def load_growth_dislocation_settings(
    get: Callable[[str], Any],
) -> GrowthDislocationSettings:
    """Build this settings slice at the settings composition boundary."""

    prefix = "growth_dislocation."

    def value(name: str) -> Any:
        return get(f"{prefix}{name}")

    return GrowthDislocationSettings(
        enabled=bool(value("enabled")),
        universe_path=str(value("universe_path")),
        max_price_location_52w=float(value("max_price_location_52w")),
        max_dividend_yield=float(value("max_dividend_yield")),
        min_market_cap=float(value("min_market_cap")),
        max_ivp_13w=float(value("max_ivp_13w")),
        max_ivp_26w=float(value("max_ivp_26w")),
        ibkr_history_timeout_seconds=float(value("ibkr_history_timeout_seconds")),
        ibkr_history_concurrency=int(value("ibkr_history_concurrency")),
        min_leaps_dte=int(value("min_leaps_dte")),
        preferred_leaps_dte=int(value("preferred_leaps_dte")),
        max_leaps_dte=int(value("max_leaps_dte")),
        target_delta_min=float(value("target_delta_min")),
        target_delta_max=float(value("target_delta_max")),
        max_leaps_spread_mid=float(value("max_leaps_spread_mid")),
        hard_max_leaps_spread_mid=float(value("hard_max_leaps_spread_mid")),
        rsi_oversold_threshold=float(value("rsi_oversold_threshold")),
        rsi_recovery_min=float(value("rsi_recovery_min")),
        rsi_recovery_optimal_low=float(value("rsi_recovery_optimal_low")),
        rsi_recovery_optimal_high=float(value("rsi_recovery_optimal_high")),
        chain_strike_count=int(value("chain_strike_count")),
        rth_request_budget=int(value("rth_request_budget")),
        daily_request_budget=int(value("daily_request_budget")),
        quote_max_age_seconds=float(value("quote_max_age_seconds")),
        option_quote_max_age_seconds=float(value("option_quote_max_age_seconds")),
        max_names_per_crowding_group=int(value("max_names_per_crowding_group")),
        top_count=int(value("top_count")),
    )
