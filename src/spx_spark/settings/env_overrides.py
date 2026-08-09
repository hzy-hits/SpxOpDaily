"""Documented environment overrides for typed AppSettings paths."""

from __future__ import annotations

from typing import Any, Mapping


def env_override(dotted_path: str, environ: Mapping[str, str]) -> Any | None:
    """Map a small set of documented environment overrides into settings paths."""

    env_map = {
        "market_data.provider_priority": "MARKET_DATA_PROVIDER_PRIORITY",
        "market_data.standardized_minute_max_age_seconds": (
            "SPX_STANDARDIZED_MINUTE_MAX_AGE_SECONDS"
        ),
        "ibkr_broker.account_read_enabled": "IBKR_BROKER_ACCOUNT_READ_ENABLED",
        "steven.enabled": "SPX_STEVEN_ENABLED",
        "steven.alert_context_enabled": "SPX_STEVEN_ALERT_CONTEXT_ENABLED",
        "alerts.move_quiet_floor_bps": "ALERT_MOVE_QUIET_FLOOR_BPS",
        "alerts.move_high_severity_em_fraction": (
            "ALERT_MOVE_HIGH_SEVERITY_EM_FRACTION"
        ),
        "alerts.min_option_live_ratio": "ALERT_MIN_OPTION_LIVE_RATIO",
        "alerts.max_option_quote_age_ms": "ALERT_MAX_OPTION_QUOTE_AGE_MS",
        "alerts.require_option_quote_timestamps": (
            "ALERT_REQUIRE_OPTION_QUOTE_TIMESTAMPS"
        ),
        "alerts.gamma_regime_hysteresis_seconds": (
            "ALERT_GAMMA_REGIME_HYSTERESIS_SECONDS"
        ),
        "alerts.max_iv_surface_age_seconds": "ALERT_MAX_IV_SURFACE_AGE_SECONDS",
        "alerts.broker_state_max_age_seconds": "ALERT_BROKER_STATE_MAX_AGE_SECONDS",
        "alerts.system_events_enabled": "ALERT_SYSTEM_EVENTS_ENABLED",
        "alerts.allow_broker_unavailable_proxy_watch": (
            "ALERT_ALLOW_BROKER_UNAVAILABLE_PROXY_WATCH"
        ),
        "alerts.iv_surface_shift_1h_threshold": (
            "ALERT_IV_SURFACE_SHIFT_1H_THRESHOLD"
        ),
        "alerts.iv_atm_change_1h_threshold": "ALERT_IV_ATM_CHANGE_1H_THRESHOLD",
        "alerts.skew_25d_threshold": "ALERT_SKEW_25D_THRESHOLD",
        "position_alerts.enabled": "ALERT_POSITIONS_ENABLED",
        "position_alerts.structural_enabled": "ALERT_POSITION_STRUCTURAL_ENABLED",
        "position_alerts.pnl_enabled": "ALERT_POSITION_PNL_ENABLED",
        "position_alerts.pnl_change_usd": "ALERT_POSITION_PNL_CHANGE_USD",
        "position_alerts.pnl_loss_usd": "ALERT_POSITION_PNL_LOSS_USD",
        "position_alerts.pnl_critical_loss_usd": (
            "ALERT_POSITION_PNL_CRITICAL_LOSS_USD"
        ),
        "position_alerts.pnl_bucket_usd": "ALERT_POSITION_PNL_DEDUP_BUCKET_USD",
        "hyperliquid.proxy_basis_warn_bps": "HYPERLIQUID_PROXY_BASIS_WARN_BPS",
        "hyperliquid.proxy_basis_block_bps": "HYPERLIQUID_PROXY_BASIS_BLOCK_BPS",
        "hyperliquid.proxy_futures_basis_warn_bps": (
            "HYPERLIQUID_PROXY_FUTURES_BASIS_WARN_BPS"
        ),
        "hyperliquid.proxy_futures_basis_block_bps": (
            "HYPERLIQUID_PROXY_FUTURES_BASIS_BLOCK_BPS"
        ),
        "hyperliquid.es_carry_annual_rate": "HYPERLIQUID_ES_CARRY_ANNUAL_RATE",
        "human_focus.event_tags": "MICOPEDIA_EVENT_TAGS",
        "provider_failover.enabled": "PROVIDER_FAILOVER_ENABLED",
        "provider_failover.control_ibkr_stream_enabled": (
            "PROVIDER_FAILOVER_CONTROL_IBKR_STREAM_ENABLED"
        ),
        "provider_failover.state_path": "PROVIDER_FAILOVER_STATE_PATH",
        "provider_failover.provider_state_max_age_seconds": (
            "PROVIDER_FAILOVER_STATE_MAX_AGE_SECONDS"
        ),
        "provider_failover.quote_max_age_seconds": (
            "PROVIDER_FAILOVER_QUOTE_MAX_AGE_SECONDS"
        ),
        "provider_failover.control_state_max_age_seconds": (
            "PROVIDER_FAILOVER_CONTROL_STATE_MAX_AGE_SECONDS"
        ),
        "provider_failover.transition_alert_max_age_seconds": (
            "PROVIDER_FAILOVER_TRANSITION_ALERT_MAX_AGE_SECONDS"
        ),
        "provider_failover.monitor_rth_only": "PROVIDER_FAILOVER_RTH_ONLY",
        "provider_failover.gth_min_live_option_contracts": (
            "PROVIDER_FAILOVER_GTH_MIN_LIVE_OPTION_CONTRACTS"
        ),
        "provider_failover.gth_option_quote_max_age_seconds": (
            "PROVIDER_FAILOVER_GTH_OPTION_QUOTE_MAX_AGE_SECONDS"
        ),
        "provider_failover.schwab_unhealthy_observations": (
            "PROVIDER_FAILOVER_SCHWAB_UNHEALTHY_OBSERVATIONS"
        ),
        "provider_failover.schwab_recovery_observations": (
            "PROVIDER_FAILOVER_SCHWAB_RECOVERY_OBSERVATIONS"
        ),
        "provider_failover.ibkr_unhealthy_observations": (
            "PROVIDER_FAILOVER_IBKR_UNHEALTHY_OBSERVATIONS"
        ),
        "provider_failover.ibkr_recovery_observations": (
            "PROVIDER_FAILOVER_IBKR_RECOVERY_OBSERVATIONS"
        ),
        "intraday_shock.anchor_provider_priority": (
            "ALERT_INTRADAY_ANCHOR_PROVIDER_PRIORITY"
        ),
        "intraday_shock.require_schwab_streaming_anchors": (
            "ALERT_INTRADAY_REQUIRE_SCHWAB_STREAMING_ANCHORS"
        ),
        "intraday_shock.provider_switch_reset_seconds": (
            "ALERT_INTRADAY_PROVIDER_SWITCH_RESET_SECONDS"
        ),
        "intraday_shock.one_minute_seconds": "ALERT_INTRADAY_SHOCK_1M_SECONDS",
        "intraday_shock.three_minute_seconds": "ALERT_INTRADAY_SHOCK_3M_SECONDS",
        "intraday_shock.one_minute_threshold_bps": "ALERT_INTRADAY_SHOCK_1M_BPS",
        "intraday_shock.three_minute_threshold_bps": "ALERT_INTRADAY_SHOCK_3M_BPS",
        "intraday_shock.es_confirm_ratio": "ALERT_INTRADAY_SHOCK_ES_CONFIRM_RATIO",
        "intraday_shock.max_spx_age_seconds": (
            "ALERT_INTRADAY_SHOCK_SPX_MAX_AGE_SECONDS"
        ),
        "intraday_shock.max_es_age_seconds": (
            "ALERT_INTRADAY_SHOCK_ES_MAX_AGE_SECONDS"
        ),
        "intraday_shock.max_anchor_skew_seconds": (
            "ALERT_INTRADAY_SHOCK_MAX_ANCHOR_SKEW_SECONDS"
        ),
        "intraday_shock.reclaim_window_seconds": (
            "ALERT_INTRADAY_RECLAIM_WINDOW_SECONDS"
        ),
        "intraday_shock.event_expiry_seconds": (
            "ALERT_INTRADAY_EVENT_EXPIRY_SECONDS"
        ),
        "intraday_shock.reclaim_fraction": "ALERT_INTRADAY_RECLAIM_FRACTION",
        "intraday_shock.es_reclaim_fraction": (
            "ALERT_INTRADAY_RECLAIM_ES_FRACTION"
        ),
        "intraday_shock.reclaim_hold_fraction": (
            "ALERT_INTRADAY_RECLAIM_HOLD_FRACTION"
        ),
        "intraday_shock.es_reclaim_hold_fraction": (
            "ALERT_INTRADAY_RECLAIM_ES_HOLD_FRACTION"
        ),
        "intraday_shock.reclaim_confirm_samples": (
            "ALERT_INTRADAY_RECLAIM_CONFIRM_SAMPLES"
        ),
        "intraday_shock.completion_hold_seconds": (
            "ALERT_INTRADAY_COMPLETION_HOLD_SECONDS"
        ),
        "intraday_shock.rearm_recovery_fraction": (
            "ALERT_INTRADAY_REARM_RECOVERY_FRACTION"
        ),
        "intraday_shock.rearm_neutral_seconds": (
            "ALERT_INTRADAY_REARM_NEUTRAL_SECONDS"
        ),
        "intraday_shock.retry_seconds": "ALERT_INTRADAY_DELIVERY_RETRY_SECONDS",
        "ibkr_stream.max_option_lines": "IBKR_STREAM_MAX_OPTION_LINES",
        "schwab.streaming.mode": "SCHWAB_STREAMING_MODE",
        "level_decision_shadow.gth_phase_timeout_seconds": (
            "SPX_LEVEL_DECISION_GTH_PHASE_TIMEOUT_SECONDS"
        ),
    }
    env_name = env_map.get(dotted_path)
    if env_name is None:
        return None
    raw = environ.get(env_name)
    if raw is None or not str(raw).strip():
        return None
    text = str(raw).strip()
    if dotted_path == "human_focus.event_tags":
        return tuple(part.strip().lower() for part in text.split(",") if part.strip())
    if dotted_path.endswith("provider_priority") or dotted_path.endswith(
        "anchor_provider_priority"
    ):
        return tuple(part.strip().lower() for part in text.split(",") if part.strip())
    lowered = text.lower()
    if lowered in {"1", "true", "yes", "y", "on"}:
        return True
    if lowered in {"0", "false", "no", "n", "off"}:
        return False
    if text.isdigit():
        return int(text)
    try:
        return float(text)
    except ValueError:
        return text
