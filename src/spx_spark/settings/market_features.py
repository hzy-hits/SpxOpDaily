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
    trend_efficiency_high: float = 0.45
    trend_efficiency_low: float = 0.25
    flat_vwap_slope_points: float = 1.0
    trend_min_score: float = 65.0
    mean_reversion_min_score: float = 60.0
    regime_score_margin: float = 10.0
    breakout_min_impulse_score: float = 50.0
    breakout_score_margin: float = 12.0
    breakout_local_gex_band_points: float = 10.0
    breakout_near_wall_points: float = 10.0

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
            self.trend_efficiency_high,
            self.trend_efficiency_low,
            self.flat_vwap_slope_points,
            self.trend_min_score,
            self.mean_reversion_min_score,
            self.regime_score_margin,
            self.breakout_min_impulse_score,
            self.breakout_score_margin,
            self.breakout_local_gex_band_points,
            self.breakout_near_wall_points,
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
        if not 0 < self.trend_efficiency_low < self.trend_efficiency_high <= 1:
            raise ValueError("trend efficiency thresholds must be ordered within (0, 1]")
        score_fields = (
            self.trend_min_score,
            self.mean_reversion_min_score,
            self.breakout_min_impulse_score,
        )
        if any(value > 100 for value in score_fields):
            raise ValueError("market feature score thresholds cannot exceed 100")


def _parse_clock(value: str) -> time:
    try:
        return time.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"invalid ET clock: {value}") from exc
