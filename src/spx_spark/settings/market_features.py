"""Typed policy for unified market and option feature frames."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import time


@dataclass(frozen=True)
class MarketFeatureSettings:
    enabled: bool = True
    interval_seconds: int = 60
    sample_interval_seconds: int = 60
    max_quote_age_seconds: float = 90.0
    retention_hours: int = 18
    option_history_minutes: int = 180
    volume_baseline_sessions: int = 20
    hot_option_limit: int = 64
    provider_sync_tolerance_seconds: float = 5.0
    asia_end_et: str = "03:00"
    europe_end_et: str = "08:00"
    premarket_end_et: str = "09:30"
    rth_end_et: str = "16:00"
    curb_end_et: str = "17:00"
    min_l1_liquidity_score: float = 40.0

    def __post_init__(self) -> None:
        positive = (
            self.interval_seconds,
            self.sample_interval_seconds,
            self.max_quote_age_seconds,
            self.retention_hours,
            self.option_history_minutes,
            self.volume_baseline_sessions,
            self.hot_option_limit,
            self.provider_sync_tolerance_seconds,
            self.min_l1_liquidity_score,
        )
        if any(value <= 0 for value in positive):
            raise ValueError("market feature settings must be positive")
        boundaries = tuple(
            _parse_clock(value)
            for value in (
                self.asia_end_et,
                self.europe_end_et,
                self.premarket_end_et,
                self.rth_end_et,
                self.curb_end_et,
            )
        )
        if tuple(sorted(boundaries)) != boundaries:
            raise ValueError("market feature session boundaries must be increasing")
        if self.min_l1_liquidity_score > 100:
            raise ValueError("min_l1_liquidity_score cannot exceed 100")


def _parse_clock(value: str) -> time:
    try:
        return time.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"invalid ET clock: {value}") from exc
