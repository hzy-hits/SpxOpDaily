"""Runtime / service-loop settings slice (RuntimePolicy)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RuntimeSettingsSlice:
    """Typed runtime / service-loop policy for composition roots."""

    control_ibkr_stream_enabled: bool = False
    provider_failover_enabled: bool = True
    provider_failover_interval_seconds: int = 15
    hyperliquid_enabled: bool = True
    polymarket_enabled: bool = False
    ibkr_enabled: bool = False
    iv_surface_enabled: bool = True
    intraday_shock_enabled: bool = False
    alerts_enabled: bool = True
    realtime_engine_enabled: bool = True
    realtime_engine_interval_seconds: int = 15
    hyperliquid_interval_seconds: int = 30
    polymarket_interval_seconds: int = 60
    ibkr_interval_seconds: int = 60
    iv_surface_interval_seconds: int = 300
    intraday_shock_interval_seconds: int = 5
    alert_interval_seconds: int = 30
    heartbeat_seconds: int = 60
    ibkr_skip_options: bool = False
    ibkr_connect_retry_seconds: int = 60
    ibkr_conflict_probe_seconds: int = 60
    max_concurrent_tasks: int = 4
    greek_shadow_enabled: bool = False
    greek_shadow_interval_seconds: int = 60
    task_timeout_seconds: int = 120
    output_tail_characters: int = 1200
    ibkr_positions_poll_interval_seconds: int = 60
