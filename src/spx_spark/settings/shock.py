"""Intraday shock monitor policy settings slice."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import time


@dataclass(frozen=True)
class ShockSettings:
    """Typed intraday-shock policy (defaults match config/runtime.yaml)."""

    anchor_provider_priority: tuple[str, ...] = ("schwab", "ibkr")
    require_schwab_streaming_anchors: bool = True
    provider_switch_reset_seconds: int = 30
    one_minute_seconds: int = 60
    three_minute_seconds: int = 180
    one_minute_threshold_bps: float = 20.0
    three_minute_threshold_bps: float = 35.0
    es_confirm_ratio: float = 0.5
    max_spx_age_seconds: float = 15.0
    max_es_age_seconds: float = 10.0
    max_anchor_skew_seconds: float = 5.0
    reclaim_window_seconds: int = 300
    event_expiry_seconds: int = 600
    reclaim_fraction: float = 0.6
    es_reclaim_fraction: float = 0.4
    reclaim_hold_fraction: float = 0.55
    es_reclaim_hold_fraction: float = 0.35
    reclaim_confirm_samples: int = 2
    completion_hold_seconds: int = 60
    rearm_recovery_fraction: float = 0.4
    rearm_neutral_seconds: int = 300
    retry_seconds: int = 30
    gth_dip_reclaim_enabled: bool = True
    gth_short_horizon_seconds: int = 900
    gth_long_horizon_seconds: int = 3600
    gth_short_min_drawdown_points: float = 10.0
    gth_long_min_drawdown_points: float = 14.0
    gth_short_min_descent_seconds: int = 300
    gth_long_min_descent_seconds: int = 1200
    gth_expected_move_fraction: float = 0.10
    gth_reclaim_fraction: float = 0.40
    gth_min_reclaim_points: float = 5.0
    gth_confirm_samples: int = 2
    gth_confirm_hold_seconds: int = 60
    gth_session_warmup_seconds: int = 3600
    gth_max_signals_per_session: int = 3
    gth_cooldown_seconds: int = 3600
    gth_spread_min_width_points: float = 15.0
    gth_spread_max_width_points: float = 75.0
    gth_spread_default_width_points: float = 50.0
    gth_structure_max_age_seconds: float = 90.0
    gth_exit_clock_et: str = "09:45"
    data_root: str = "data"

    def __post_init__(self) -> None:
        validate_gth_spread_policy(
            min_width_points=self.gth_spread_min_width_points,
            max_width_points=self.gth_spread_max_width_points,
            default_width_points=self.gth_spread_default_width_points,
            structure_max_age_seconds=self.gth_structure_max_age_seconds,
            exit_clock_et=self.gth_exit_clock_et,
        )


def validate_gth_spread_policy(
    *,
    min_width_points: float,
    max_width_points: float,
    default_width_points: float,
    structure_max_age_seconds: float,
    exit_clock_et: str,
) -> None:
    """Validate one coherent, listed-strike GTH spread policy."""

    widths = (min_width_points, default_width_points, max_width_points)
    if any(value <= 0 for value in widths):
        raise ValueError("GTH spread widths must be positive")
    if not min_width_points <= default_width_points <= max_width_points:
        raise ValueError("GTH spread widths must satisfy min <= default <= max")
    if any(not (float(value) / 5.0).is_integer() for value in widths):
        raise ValueError("GTH spread widths must use five-point strike increments")
    if structure_max_age_seconds <= 0:
        raise ValueError("GTH structure max age must be positive")
    if parse_et_clock(exit_clock_et) <= time(4, 30):
        raise ValueError("GTH exit clock must be after the 04:30 ET window start")


def parse_et_clock(value: str) -> time:
    """Parse an America/New_York wall clock without an embedded offset."""

    try:
        parsed = time.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid ET clock: {value}") from exc
    if parsed.tzinfo is not None or parsed.second or parsed.microsecond:
        raise ValueError(f"invalid ET clock: {value}")
    return parsed


DEFAULT_SHOCK_SETTINGS = ShockSettings()
