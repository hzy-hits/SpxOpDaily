"""Intraday shock monitor policy settings slice."""

from __future__ import annotations

from dataclasses import dataclass


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
    data_root: str = "data"


DEFAULT_SHOCK_SETTINGS = ShockSettings()
