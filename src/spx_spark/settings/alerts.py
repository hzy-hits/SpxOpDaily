"""Alert / notification policy settings slice."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AlertSettings:
    """Typed alert policy injected into alert_engine / shock / service loop.

    Defaults match ``config/runtime.yaml`` so rules can use this object instead
    of calling ``runtime_value()``. Environment overrides still apply via
    ``env_float`` / ``env_bool`` at call sites when present.
    """

    steven_enabled: bool = False
    steven_alert_context_enabled: bool = False
    require_schwab_streaming_anchors: bool = True
    move_quiet_floor_bps: float = 15.0
    move_high_severity_em_fraction: float = 0.35
    min_option_live_ratio: float = 0.5
    max_option_quote_age_ms: float = 20000.0
    require_option_quote_timestamps: bool = False
    gamma_regime_hysteresis_seconds: float = 600.0
    max_iv_surface_age_seconds: float = 420.0
    broker_state_max_age_seconds: float = 900.0
    system_events_enabled: bool = True
    allow_broker_unavailable_proxy_watch: bool = True
    iv_surface_shift_1h_threshold: float = 0.05
    iv_atm_change_1h_threshold: float = 0.04
    skew_25d_threshold: float = 0.02
    min_known_option_timestamp_ratio: float = 0.75
    wall_proximity_min_points: float = 10.0
    wall_proximity_underlier_fraction: float = 0.002
    degraded_threshold_multiplier: float = 1.5
    atm_iv_jump_threshold: float = 0.03
    skew_steepening_threshold: float = 0.08
    surface_shift_threshold: float = 0.03
    term_gap_threshold: float = 0.05
    wall_dedup_band_points: float = 25.0
    # Used by system session/interrupt gates (ibkr_broker.execution_mode).
    ibkr_execution_mode: str = "manual"


# Frozen YAML-aligned defaults for rules that have not yet received injection.
DEFAULT_ALERT_SETTINGS = AlertSettings()
