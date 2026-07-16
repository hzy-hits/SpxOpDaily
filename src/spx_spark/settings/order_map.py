"""Typed calibration policy for order-map pricing and ES volume context."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class OrderMapPolicy:
    touch_time_fraction_coefficient: float = 0.6
    touch_time_fraction_maximum: float = 0.90
    vol_slope_beta: float = 1.2
    minimum_tau_at_touch_hours: float = 0.25
    conservative_limit_multiplier: float = 0.85
    risk_free_rate: float = 0.05
    early_touch_fraction_multiplier: float = 0.50
    late_touch_fraction_multiplier: float = 1.50
    execution_max_spread_points: float = 3.00
    execution_max_spread_bps: float = 3000.0
    execution_max_spread_percentile: float = 0.90
    execution_max_quote_age_seconds: float = 15.0
    execution_max_source_age_seconds: float = 15.0
    execution_max_provider_mid_divergence_bps: float = 800.0
    execution_max_provider_underlier_divergence_points: float = 3.0
    frontrun_fraction: float = 0.30
    frontrun_min_points: float = 2.0
    frontrun_max_points: float = 8.0
    es_volume_min_window_minutes: float = 3.0
    es_volume_max_window_minutes: float = 120.0
    es_volume_elevated_ratio: float = 1.5
    es_volume_quiet_ratio: float = 0.5
    es_volume_max_samples: int = 16
    es_volume_max_quote_age_seconds: float = 900.0
    es_volume_flat_points: float = 3.0
    es_volume_level_band_points: float = 8.0
    es_volume_reclaim_min_minutes: float = 10.0
    es_volume_reclaim_max_minutes: float = 90.0

    def __post_init__(self) -> None:
        if any(value <= 0 for value in self.__dict__.values()):
            raise ValueError("order-map policy thresholds must be positive")
        if not 0 < self.touch_time_fraction_maximum <= 1:
            raise ValueError("touch-time maximum must be in (0, 1]")
        if not 0 < self.conservative_limit_multiplier <= 1:
            raise ValueError("conservative limit multiplier must be in (0, 1]")
        if not 0 < self.early_touch_fraction_multiplier <= 1:
            raise ValueError("early-touch multiplier must be in (0, 1]")
        if self.late_touch_fraction_multiplier < 1:
            raise ValueError("late-touch multiplier must be at least 1")
        if not 0 < self.execution_max_spread_percentile <= 1:
            raise ValueError("spread percentile limit must be in (0, 1]")
        if self.frontrun_min_points > self.frontrun_max_points:
            raise ValueError("frontrun minimum cannot exceed maximum")
        if self.es_volume_min_window_minutes >= self.es_volume_max_window_minutes:
            raise ValueError("ES volume window bounds are invalid")
        if self.es_volume_reclaim_min_minutes >= self.es_volume_reclaim_max_minutes:
            raise ValueError("ES reclaim window bounds are invalid")


DEFAULT_ORDER_MAP_POLICY = OrderMapPolicy()
