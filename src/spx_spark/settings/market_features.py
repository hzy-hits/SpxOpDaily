"""Typed policy for unified market and option feature frames."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import time


@dataclass(frozen=True)
class MarketFeatureSettings:
    enabled: bool = True
    interval_seconds: int = 5
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
    l1_spread_p50_limit_bps: float = 500.0
    l1_spread_p90_limit_bps: float = 1500.0
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
    trade_follow_through_seconds: float = 30.0
    trade_follow_through_min_points: float = 2.0
    trade_follow_through_em_fraction: float = 0.05
    trade_repricing_max_age_seconds: float = 90.0
    trade_quote_max_age_seconds: float = 5.0
    trade_market_anchor_max_age_seconds: float = 20.0
    trade_structure_drift_points: float = 2.5
    trade_entry_spread_fraction: float = 0.35
    trade_intent_ttl_seconds: float = 90.0
    trade_entry_window_seconds: float = 20.0
    trade_invalidation_buffer_points: float = 3.0
    trade_target_em_fraction: float = 0.15
    trade_min_target_room_points: float = 3.0
    trade_min_reward_risk: float = 0.25
    trade_time_stop_minutes: int = 15
    session_episode_enabled: bool = True
    session_break_buffer_points: float = 2.0
    session_extreme_extension_points: float = 5.0
    session_reclaim_hold_seconds: float = 60.0
    session_recovery_ratio: float = 0.50
    greek_decision_min_coverage: float = 0.60
    greek_target_delta_min: float = 0.35
    greek_target_delta_max: float = 0.70
    greek_max_theta_15m_loss_fraction: float = 0.35
    greek_max_iv_crush_loss_fraction: float = 0.45
    greek_delta_saturation: float = 0.85
    virtual_strategy_enabled: bool = True
    virtual_profit_take_fraction: float = 0.30
    virtual_gamma_retention_fraction: float = 0.60
    virtual_iv_drop_vol_points: float = 1.0
    virtual_wall_touch_points: float = 5.0
    virtual_gth_time_stop_minutes: int = 360

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
            self.l1_spread_p50_limit_bps,
            self.l1_spread_p90_limit_bps,
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
            self.trade_follow_through_seconds,
            self.trade_follow_through_min_points,
            self.trade_follow_through_em_fraction,
            self.trade_repricing_max_age_seconds,
            self.trade_quote_max_age_seconds,
            self.trade_market_anchor_max_age_seconds,
            self.trade_structure_drift_points,
            self.trade_entry_spread_fraction,
            self.trade_intent_ttl_seconds,
            self.trade_entry_window_seconds,
            self.trade_invalidation_buffer_points,
            self.trade_target_em_fraction,
            self.trade_min_target_room_points,
            self.trade_min_reward_risk,
            self.trade_time_stop_minutes,
            self.session_break_buffer_points,
            self.session_extreme_extension_points,
            self.session_reclaim_hold_seconds,
            self.session_recovery_ratio,
            self.greek_decision_min_coverage,
            self.greek_target_delta_min,
            self.greek_target_delta_max,
            self.greek_max_theta_15m_loss_fraction,
            self.greek_max_iv_crush_loss_fraction,
            self.greek_delta_saturation,
            self.virtual_profit_take_fraction,
            self.virtual_gamma_retention_fraction,
            self.virtual_iv_drop_vol_points,
            self.virtual_wall_touch_points,
            self.virtual_gth_time_stop_minutes,
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
        fractions = (
            self.trade_follow_through_em_fraction,
            self.trade_entry_spread_fraction,
            self.trade_target_em_fraction,
            self.session_recovery_ratio,
            self.greek_decision_min_coverage,
            self.greek_max_theta_15m_loss_fraction,
            self.greek_max_iv_crush_loss_fraction,
            self.greek_delta_saturation,
            self.virtual_profit_take_fraction,
            self.virtual_gamma_retention_fraction,
        )
        if any(value > 1 for value in fractions):
            raise ValueError("market feature trade fractions cannot exceed 1")
        if not 0 < self.greek_target_delta_min < self.greek_target_delta_max < 1:
            raise ValueError("Greek target delta band must be ordered within (0, 1)")


def _parse_clock(value: str) -> time:
    try:
        return time.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"invalid ET clock: {value}") from exc
