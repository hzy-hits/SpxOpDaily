"""Runtime / service-loop settings slice (RuntimePolicy)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RuntimeSettingsSlice:
    """Typed runtime / service-loop policy for composition roots."""

    control_ibkr_stream_enabled: bool = False
    provider_failover_enabled: bool = True
    provider_failover_state_path: str = ""
    provider_failover_required_instruments: tuple[str, ...] = (
        "index:SPX",
        "future:ES",
    )
    provider_failover_globex_required_instruments: tuple[str, ...] = ("future:ES",)
    provider_failover_state_max_age_seconds: float = 45.0
    provider_failover_quote_max_age_seconds: float = 45.0
    provider_failover_control_state_max_age_seconds: float = 60.0
    provider_failover_transition_alert_max_age_seconds: float = 900.0
    provider_failover_monitor_rth_only: bool = False
    provider_failover_gth_min_live_option_contracts: int = 20
    provider_failover_gth_option_quote_max_age_seconds: float = 90.0
    provider_failover_schwab_unhealthy_observations: int = 2
    provider_failover_schwab_recovery_observations: int = 3
    provider_failover_ibkr_unhealthy_observations: int = 4
    provider_failover_ibkr_recovery_observations: int = 3
    provider_failover_interval_seconds: int = 15
    hyperliquid_enabled: bool = True
    ibkr_enabled: bool = False
    iv_surface_enabled: bool = True
    intraday_shock_enabled: bool = False
    alerts_enabled: bool = True
    realtime_engine_enabled: bool = True
    realtime_engine_interval_seconds: int = 15
    hyperliquid_interval_seconds: int = 30
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
