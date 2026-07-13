"""Load AppSettings from defaults YAML, optional deployment YAML, and environ.

Priority is fixed: defaults < deployment < environment.
Secret values must never be written into SettingSource or logs.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping

import yaml

from spx_spark.settings.alerts import AlertSettings
from spx_spark.settings.analytics import AnalyticsSettings
from spx_spark.settings.globex_trend import GlobexTrendSettings
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.settings.ibkr import IbkrSettingsSlice
from spx_spark.settings.level_decision import LevelDecisionPolicy
from spx_spark.settings.market_data import MarketDataSettings
from spx_spark.settings.order_map import OrderMapPolicy
from spx_spark.settings.runtime import RuntimeSettingsSlice
from spx_spark.settings.schema import AppSettings, SettingSource
from spx_spark.settings.schwab import (
    SchwabCadenceSettings,
    SchwabCapacitySettings,
    SchwabHotLaneSettings,
    SchwabSettingsSlice,
    SchwabWideChainSettings,
)
from spx_spark.settings.shock import ShockSettings
from spx_spark.settings.storage import StorageSettingsSlice

_SECRET_KEY_FRAGMENTS = (
    "secret",
    "token",
    "password",
    "api_key",
    "app_key",
    "app_secret",
    "refresh",
)


_CONFIG_ENV_VAR = "SPX_SPARK_RUNTIME_CONFIG"
_OVERRIDES_ENV_VAR = "SPX_SPARK_RUNTIME_OVERRIDES"
_DEFAULT_CONFIG_RELATIVE_PATH = Path("config/runtime.yaml")
_DEFAULT_OVERRIDES_RELATIVE_PATH = Path("config/runtime.local.yaml")


def default_defaults_path() -> Path:
    """Repository ``config/runtime.yaml`` (settings package is composition-safe)."""

    return Path(__file__).resolve().parents[3] / "config" / "runtime.yaml"


def _runtime_config_path() -> Path:
    """Mirror ``runtime_config.runtime_config_path`` without importing L0 sibling."""

    override = os.getenv(_CONFIG_ENV_VAR, "").strip()
    if override:
        return Path(override).expanduser().resolve()
    cwd_candidate = (Path.cwd() / _DEFAULT_CONFIG_RELATIVE_PATH).resolve()
    if cwd_candidate.is_file():
        return cwd_candidate
    return default_defaults_path().resolve()


def _runtime_overrides_path() -> Path | None:
    """Mirror ``runtime_config.runtime_overrides_path`` without importing L0 sibling."""

    explicit = os.getenv(_OVERRIDES_ENV_VAR, "").strip()
    if explicit:
        path = Path(explicit).expanduser().resolve()
        if not path.is_file():
            raise FileNotFoundError(f"Runtime overrides not found at {path}")
        return path
    disabled = os.getenv("SPX_SPARK_DISABLE_RUNTIME_OVERRIDES", "").strip().lower()
    if disabled in {"1", "true", "yes", "y", "on"}:
        return None
    cwd_candidate = (Path.cwd() / _DEFAULT_OVERRIDES_RELATIVE_PATH).resolve()
    if cwd_candidate.is_file():
        return cwd_candidate
    repository_candidate = Path(__file__).resolve().parents[3] / _DEFAULT_OVERRIDES_RELATIVE_PATH
    if repository_candidate.is_file():
        return repository_candidate.resolve()
    return None


def _is_secret_path(dotted_path: str) -> bool:
    lowered = dotted_path.lower()
    return any(fragment in lowered for fragment in _SECRET_KEY_FRAGMENTS)


def _read_yaml_mapping(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"Settings file not found: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Settings root must be a mapping: {path}")
    return payload


def _setting_value(node: Any, dotted_path: str) -> Any:
    cursor: Any = node
    for part in dotted_path.split("."):
        if not isinstance(cursor, dict) or part not in cursor:
            raise KeyError(f"Missing settings path: {dotted_path}")
        cursor = cursor[part]
    if isinstance(cursor, dict) and "value" in cursor:
        description = cursor.get("description")
        if not isinstance(description, str) or not description.strip():
            raise ValueError(f"Runtime setting has no description: {dotted_path}")
        return cursor["value"]
    return cursor


def _merge_maps(
    base: dict[str, Any],
    overlay: dict[str, Any],
    *,
    dotted_path: str = "",
) -> dict[str, Any]:
    unknown = sorted(set(overlay) - set(base))
    if unknown:
        raise KeyError(f"Unknown deployment settings at {dotted_path or '<root>'}: {unknown}")
    merged = dict(base)
    for key, value in overlay.items():
        child_path = f"{dotted_path}.{key}" if dotted_path else str(key)
        base_value = base[key]
        if isinstance(base_value, dict) and "value" in base_value:
            if not isinstance(value, dict) or set(value) != {"value"}:
                raise ValueError(
                    f"Deployment override for {child_path} must contain only a value field"
                )
            merged[key] = {**base_value, "value": value["value"]}
        elif isinstance(base_value, dict) and isinstance(value, dict):
            merged[key] = _merge_maps(base_value, value, dotted_path=child_path)
        elif isinstance(base_value, list) and isinstance(value, list):
            merged[key] = list(value)
        else:
            raise TypeError(f"Deployment override type mismatch at {child_path}")
    return merged


def _has_path(node: dict[str, Any], dotted_path: str) -> bool:
    cursor: Any = node
    for part in dotted_path.split("."):
        if not isinstance(cursor, dict) or part not in cursor:
            return False
        cursor = cursor[part]
    return True


def _env_override(dotted_path: str, environ: Mapping[str, str]) -> Any | None:
    """Map a small set of documented environment overrides into settings paths."""
    env_map = {
        "market_data.provider_priority": "MARKET_DATA_PROVIDER_PRIORITY",
        "ibkr_broker.account_read_enabled": "IBKR_BROKER_ACCOUNT_READ_ENABLED",
        "steven.enabled": "SPX_STEVEN_ENABLED",
        "steven.alert_context_enabled": "SPX_STEVEN_ALERT_CONTEXT_ENABLED",
        "provider_failover.enabled": "PROVIDER_FAILOVER_ENABLED",
        "provider_failover.control_ibkr_stream_enabled": (
            "PROVIDER_FAILOVER_CONTROL_IBKR_STREAM_ENABLED"
        ),
        "intraday_shock.require_schwab_streaming_anchors": (
            "ALERT_INTRADAY_REQUIRE_SCHWAB_STREAMING_ANCHORS"
        ),
        "ibkr_stream.max_option_lines": "IBKR_STREAM_MAX_OPTION_LINES",
        "schwab.streaming.mode": "SCHWAB_STREAMING_MODE",
    }
    env_name = env_map.get(dotted_path)
    if env_name is None:
        return None
    raw = environ.get(env_name)
    if raw is None or not str(raw).strip():
        return None
    text = str(raw).strip()
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


def _resolve(
    *,
    defaults: dict[str, Any],
    merged: dict[str, Any],
    deployment: dict[str, Any] | None,
    environ: Mapping[str, str],
    dotted_path: str,
    sources: dict[str, SettingSource],
) -> Any:
    _setting_value(defaults, dotted_path)
    value = _setting_value(merged, dotted_path)
    origin = (
        "deployment"
        if deployment is not None and _has_path(deployment, dotted_path)
        else "defaults"
    )
    env_value = _env_override(dotted_path, environ)
    if env_value is not None:
        value = env_value
        origin = "environment"
    if not _is_secret_path(dotted_path):
        sources[dotted_path] = SettingSource(path=dotted_path, origin=origin)
    return value


def _as_str_tuple(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        return tuple(part.strip().lower() for part in value.split(",") if part.strip())
    if isinstance(value, (list, tuple)):
        return tuple(str(item).lower() for item in value)
    raise TypeError(f"Expected list/tuple/str, got {type(value)!r}")


def load_settings(
    *,
    defaults_path: Path,
    deployment_path: Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> AppSettings:
    env = environ if environ is not None else os.environ
    defaults = _read_yaml_mapping(defaults_path)
    deployment = _read_yaml_mapping(deployment_path) if deployment_path is not None else None
    merged = _merge_maps(defaults, deployment) if deployment is not None else defaults
    sources: dict[str, SettingSource] = {}

    def get(path: str) -> Any:
        return _resolve(
            defaults=defaults,
            merged=merged,
            deployment=deployment,
            environ=env,
            dotted_path=path,
            sources=sources,
        )

    market_data = MarketDataSettings(
        known_providers=_as_str_tuple(get("market_data.known_providers")),
        provider_priority=_as_str_tuple(get("market_data.provider_priority")),
        latest_stale_after_seconds=float(get("market_data.latest_stale_after_seconds")),
        delayed_stale_after_seconds=float(get("market_data.delayed_stale_after_seconds")),
    )
    if not market_data.provider_priority:
        raise ValueError("market_data.provider_priority cannot be empty")
    if len(set(market_data.provider_priority)) != len(market_data.provider_priority):
        raise ValueError("market_data.provider_priority cannot contain duplicates")
    analytics = AnalyticsSettings(
        max_chain_age_seconds=float(get("analytics.max_chain_age_seconds")),
        min_usable_strikes=int(get("analytics.min_usable_strikes")),
        min_two_sided_ratio=float(get("analytics.min_two_sided_ratio")),
        min_wing_strikes_each_side=int(get("analytics.min_wing_strikes_each_side")),
        provider_priority=_as_str_tuple(get("analytics.provider_priority")),
        underlier_reference_tolerance_fraction=float(
            get("analytics.underlier_reference_tolerance_fraction")
        ),
    )
    globex_trend = GlobexTrendSettings(
        enabled=bool(get("globex_trend.enabled")),
        interval_seconds=int(get("globex_trend.interval_seconds")),
        sample_interval_seconds=int(get("globex_trend.sample_interval_seconds")),
        short_horizon_minutes=int(get("globex_trend.short_horizon_minutes")),
        medium_horizon_minutes=int(get("globex_trend.medium_horizon_minutes")),
        long_horizon_minutes=int(get("globex_trend.long_horizon_minutes")),
        short_move_points=float(get("globex_trend.short_move_points")),
        medium_move_points=float(get("globex_trend.medium_move_points")),
        long_move_points=float(get("globex_trend.long_move_points")),
        reversal_points=float(get("globex_trend.reversal_points")),
        confirmation_observations=int(get("globex_trend.confirmation_observations")),
        max_quote_age_seconds=float(get("globex_trend.max_quote_age_seconds")),
        retention_hours=int(get("globex_trend.retention_hours")),
        pending_event_ttl_seconds=int(get("globex_trend.pending_event_ttl_seconds")),
    )
    market_features = MarketFeatureSettings(
        enabled=bool(get("market_features.enabled")),
        interval_seconds=int(get("market_features.interval_seconds")),
        sample_interval_seconds=int(get("market_features.sample_interval_seconds")),
        max_quote_age_seconds=float(get("market_features.max_quote_age_seconds")),
        retention_hours=int(get("market_features.retention_hours")),
        option_history_minutes=int(get("market_features.option_history_minutes")),
        volume_baseline_sessions=int(get("market_features.volume_baseline_sessions")),
        hot_option_limit=int(get("market_features.hot_option_limit")),
        provider_sync_tolerance_seconds=float(
            get("market_features.provider_sync_tolerance_seconds")
        ),
        asia_end_et=str(get("market_features.asia_end_et")),
        europe_end_et=str(get("market_features.europe_end_et")),
        premarket_end_et=str(get("market_features.premarket_end_et")),
        rth_end_et=str(get("market_features.rth_end_et")),
        curb_end_et=str(get("market_features.curb_end_et")),
        min_l1_liquidity_score=float(get("market_features.min_l1_liquidity_score")),
    )

    ibkr = IbkrSettingsSlice(
        max_option_lines=int(get("ibkr_stream.max_option_lines")),
        account_read_enabled=bool(get("ibkr_broker.account_read_enabled")),
        position_shadow_enabled=bool(get("ibkr_broker.position_shadow_enabled")),
        legacy_position_poller_enabled=bool(get("ibkr_broker.legacy_position_poller_enabled")),
        execution_mode=str(get("ibkr_broker.execution_mode")).lower(),
    )
    schwab = SchwabSettingsSlice(
        streaming_mode=str(get("schwab.streaming.mode")).lower(),
        request_budget_warning_per_minute=int(
            get("schwab.collection.request_budget_warning_per_minute")
        ),
        collection_enabled=bool(get("schwab.collection.enabled")),
        service_loop_enabled=bool(get("schwab.collection.service_loop_enabled")),
        collection_interval_seconds=int(get("schwab.collection.interval_seconds")),
        capacity=SchwabCapacitySettings(
            nominal_requests_per_minute=int(get("schwab.request_policy.requests_per_minute")),
            planned_requests_per_minute=int(get("schwab.collection.planned_requests_per_minute")),
            max_symbols_per_quote_request=int(get("schwab.quote_symbol_capacity")),
            operational_quote_batch_size=int(get("schwab.quote_batch_size")),
        ),
        cadence=SchwabCadenceSettings(
            off_hours_quote_seconds=float(get("schwab.collection.cadence.off_hours_quote_seconds")),
            off_hours_front_chain_seconds=float(
                get("schwab.collection.cadence.off_hours_front_chain_seconds")
            ),
            off_hours_next_chain_seconds=float(
                get("schwab.collection.cadence.off_hours_next_chain_seconds")
            ),
            off_hours_confirmation_chain_seconds=float(
                get("schwab.collection.cadence.off_hours_confirmation_chain_seconds")
            ),
            gth_quote_seconds=float(get("schwab.collection.cadence.gth_quote_seconds")),
            gth_front_chain_seconds=float(
                get("schwab.collection.cadence.gth_front_chain_seconds")
            ),
            gth_next_chain_seconds=float(
                get("schwab.collection.cadence.gth_next_chain_seconds")
            ),
            gth_confirmation_chain_seconds=float(
                get("schwab.collection.cadence.gth_confirmation_chain_seconds")
            ),
            normal_quote_seconds=float(get("schwab.collection.cadence.normal_quote_seconds")),
            normal_front_chain_seconds=float(
                get("schwab.collection.cadence.normal_front_chain_seconds")
            ),
            active_quote_seconds=float(get("schwab.collection.cadence.active_quote_seconds")),
            active_front_chain_seconds=float(
                get("schwab.collection.cadence.active_front_chain_seconds")
            ),
            burst_quote_seconds=float(get("schwab.collection.cadence.burst_quote_seconds")),
            burst_front_chain_seconds=float(
                get("schwab.collection.cadence.burst_front_chain_seconds")
            ),
            next_chain_seconds=float(get("schwab.collection.cadence.next_chain_seconds")),
            spy_xsp_chain_seconds=float(get("schwab.collection.cadence.spy_xsp_chain_seconds")),
            qqq_iwm_chain_seconds=float(get("schwab.collection.cadence.qqq_iwm_chain_seconds")),
        ),
        wide_chain=SchwabWideChainSettings(
            strike_count_candidates=tuple(
                int(item) for item in get("schwab.collection.wide_chain.strike_count_candidates")
            ),
            next_expiry_strike_count=int(
                get("schwab.collection.wide_chain.next_expiry_strike_count")
            ),
            min_usable_strikes=int(get("schwab.collection.wide_chain.min_usable_strikes")),
            min_two_sided_ratio=float(get("schwab.collection.wide_chain.min_two_sided_ratio")),
            expected_move_multiple=float(
                get("schwab.collection.wide_chain.expected_move_multiple")
            ),
            min_width_points=float(get("schwab.collection.wide_chain.min_width_points")),
            max_gap_multiple=float(get("schwab.collection.wide_chain.max_gap_multiple")),
        ),
        hot_lane=SchwabHotLaneSettings(
            minimum_dynamic_symbol_reserve=int(
                get("schwab.collection.hot_lane.dynamic_symbol_reserve")
            ),
            max_plan_age_seconds=float(get("schwab.collection.hot_lane.max_plan_age_seconds")),
            recenter_drift_points=float(get("schwab.collection.hot_lane.recenter_drift_points")),
        ),
    )
    alerts = AlertSettings(
        steven_enabled=bool(get("steven.enabled")),
        steven_alert_context_enabled=bool(get("steven.alert_context_enabled")),
        require_schwab_streaming_anchors=bool(
            get("intraday_shock.require_schwab_streaming_anchors")
        ),
        move_quiet_floor_bps=float(get("alerts.move_quiet_floor_bps")),
        move_high_severity_em_fraction=float(get("alerts.move_high_severity_em_fraction")),
        min_option_live_ratio=float(get("alerts.min_option_live_ratio")),
        max_option_quote_age_ms=float(get("alerts.max_option_quote_age_ms")),
        require_option_quote_timestamps=bool(get("alerts.require_option_quote_timestamps")),
        gamma_regime_hysteresis_seconds=float(get("alerts.gamma_regime_hysteresis_seconds")),
        max_iv_surface_age_seconds=float(get("alerts.max_iv_surface_age_seconds")),
        broker_state_max_age_seconds=float(get("alerts.broker_state_max_age_seconds")),
        system_events_enabled=bool(get("alerts.system_events_enabled")),
        allow_broker_unavailable_proxy_watch=bool(
            get("alerts.allow_broker_unavailable_proxy_watch")
        ),
        iv_surface_shift_1h_threshold=float(get("alerts.iv_surface_shift_1h_threshold")),
        iv_atm_change_1h_threshold=float(get("alerts.iv_atm_change_1h_threshold")),
        skew_25d_threshold=float(get("alerts.skew_25d_threshold")),
        min_known_option_timestamp_ratio=float(get("alerts.min_known_option_timestamp_ratio")),
        wall_proximity_min_points=float(get("alerts.wall_proximity_min_points")),
        wall_proximity_underlier_fraction=float(get("alerts.wall_proximity_underlier_fraction")),
        degraded_threshold_multiplier=float(get("alerts.degraded_threshold_multiplier")),
        atm_iv_jump_threshold=float(get("alerts.atm_iv_jump_threshold")),
        skew_steepening_threshold=float(get("alerts.skew_steepening_threshold")),
        surface_shift_threshold=float(get("alerts.surface_shift_threshold")),
        term_gap_threshold=float(get("alerts.term_gap_threshold")),
        wall_dedup_band_points=float(get("alerts.wall_dedup_band_points")),
        ibkr_execution_mode=str(get("ibkr_broker.execution_mode")).lower(),
    )
    runtime = RuntimeSettingsSlice(
        control_ibkr_stream_enabled=bool(get("provider_failover.control_ibkr_stream_enabled")),
        provider_failover_enabled=bool(get("provider_failover.enabled")),
        provider_failover_interval_seconds=int(get("provider_failover.interval_seconds")),
        hyperliquid_enabled=bool(get("service_loop.hyperliquid_enabled")),
        polymarket_enabled=bool(get("service_loop.polymarket_enabled")),
        ibkr_enabled=bool(get("service_loop.ibkr_enabled")),
        iv_surface_enabled=bool(get("service_loop.iv_surface_enabled")),
        intraday_shock_enabled=bool(get("service_loop.intraday_shock_enabled")),
        alerts_enabled=bool(get("service_loop.alerts_enabled")),
        realtime_engine_enabled=bool(get("service_loop.realtime_engine_enabled")),
        realtime_engine_interval_seconds=int(get("service_loop.realtime_engine_interval_seconds")),
        hyperliquid_interval_seconds=int(get("service_loop.hyperliquid_interval_seconds")),
        polymarket_interval_seconds=int(get("service_loop.polymarket_interval_seconds")),
        ibkr_interval_seconds=int(get("service_loop.ibkr_interval_seconds")),
        iv_surface_interval_seconds=int(get("service_loop.iv_surface_interval_seconds")),
        intraday_shock_interval_seconds=int(get("service_loop.intraday_shock_interval_seconds")),
        alert_interval_seconds=int(get("service_loop.alert_interval_seconds")),
        heartbeat_seconds=int(get("service_loop.heartbeat_seconds")),
        ibkr_skip_options=bool(get("service_loop.ibkr_skip_options")),
        ibkr_connect_retry_seconds=int(get("service_loop.ibkr_connect_retry_seconds")),
        ibkr_conflict_probe_seconds=int(get("service_loop.ibkr_conflict_probe_seconds")),
        max_concurrent_tasks=int(get("service_loop.max_concurrent_tasks")),
        greek_shadow_enabled=bool(get("service_loop.greek_shadow_enabled")),
        greek_shadow_interval_seconds=int(get("service_loop.greek_shadow_interval_seconds")),
        task_timeout_seconds=int(get("service_loop.task_timeout_seconds")),
        output_tail_characters=int(get("service_loop.output_tail_characters")),
        ibkr_positions_poll_interval_seconds=int(get("ibkr_positions.poll_interval_seconds")),
    )
    data_root = str(
        env.get("MARKET_DATA_DATA_ROOT")
        or env.get("MAINTENANCE_DATA_ROOT")
        or get("maintenance.data_root")
    )
    shock = ShockSettings(
        anchor_provider_priority=_as_str_tuple(get("intraday_shock.anchor_provider_priority")),
        require_schwab_streaming_anchors=bool(
            get("intraday_shock.require_schwab_streaming_anchors")
        ),
        provider_switch_reset_seconds=int(get("intraday_shock.provider_switch_reset_seconds")),
        one_minute_seconds=int(get("intraday_shock.one_minute_seconds")),
        three_minute_seconds=int(get("intraday_shock.three_minute_seconds")),
        one_minute_threshold_bps=float(get("intraday_shock.one_minute_threshold_bps")),
        three_minute_threshold_bps=float(get("intraday_shock.three_minute_threshold_bps")),
        es_confirm_ratio=float(get("intraday_shock.es_confirm_ratio")),
        max_spx_age_seconds=float(get("intraday_shock.max_spx_age_seconds")),
        max_es_age_seconds=float(get("intraday_shock.max_es_age_seconds")),
        max_anchor_skew_seconds=float(get("intraday_shock.max_anchor_skew_seconds")),
        reclaim_window_seconds=int(get("intraday_shock.reclaim_window_seconds")),
        event_expiry_seconds=int(get("intraday_shock.event_expiry_seconds")),
        reclaim_fraction=float(get("intraday_shock.reclaim_fraction")),
        es_reclaim_fraction=float(get("intraday_shock.es_reclaim_fraction")),
        reclaim_hold_fraction=float(get("intraday_shock.reclaim_hold_fraction")),
        es_reclaim_hold_fraction=float(get("intraday_shock.es_reclaim_hold_fraction")),
        reclaim_confirm_samples=int(get("intraday_shock.reclaim_confirm_samples")),
        completion_hold_seconds=int(get("intraday_shock.completion_hold_seconds")),
        rearm_recovery_fraction=float(get("intraday_shock.rearm_recovery_fraction")),
        rearm_neutral_seconds=int(get("intraday_shock.rearm_neutral_seconds")),
        retry_seconds=int(get("intraday_shock.retry_seconds")),
        data_root=data_root,
    )
    level_decision = LevelDecisionPolicy(
        enabled=bool(get("level_decision_shadow.enabled")),
        notify_transitions=bool(get("level_decision_shadow.notify_transitions")),
        formal_signal_enabled=bool(get("level_decision_shadow.formal_signal_enabled")),
        approach_points=float(get("level_decision_shadow.approach_points")),
        test_points=float(get("level_decision_shadow.test_points")),
        break_buffer_points=float(get("level_decision_shadow.break_buffer_points")),
        reject_points=float(get("level_decision_shadow.reject_points")),
        accept_hold_seconds=float(get("level_decision_shadow.accept_hold_seconds")),
        retest_points=float(get("level_decision_shadow.retest_points")),
        confirm_move_points=float(get("level_decision_shadow.confirm_move_points")),
        confirm_hold_seconds=float(get("level_decision_shadow.confirm_hold_seconds")),
        phase_timeout_seconds=float(get("level_decision_shadow.phase_timeout_seconds")),
        event_ttl_seconds=float(get("level_decision_shadow.event_ttl_seconds")),
        data_grace_seconds=float(get("level_decision_shadow.data_grace_seconds")),
        structure_drift_points=float(get("level_decision_shadow.structure_drift_points")),
        es_confirm_ratio=float(get("level_decision_shadow.es_confirm_ratio")),
        terminal_rearm_seconds=float(get("level_decision_shadow.terminal_rearm_seconds")),
        max_frozen_structure_age_sessions=int(
            get("level_decision_shadow.max_frozen_structure_age_sessions")
        ),
        outcome_horizons_seconds=tuple(
            int(item) for item in get("level_decision_shadow.outcome_horizons_seconds")
        ),
        outcome_sample_tolerance_seconds=float(
            get("level_decision_shadow.outcome_sample_tolerance_seconds")
        ),
        outcome_no_follow_through_mfe_bps=float(
            get("level_decision_shadow.outcome_no_follow_through_mfe_bps")
        ),
        outcome_false_confirmation_mae_bps=float(
            get("level_decision_shadow.outcome_false_confirmation_mae_bps")
        ),
        outcome_follow_through_end_bps=float(
            get("level_decision_shadow.outcome_follow_through_end_bps")
        ),
        outcome_retention_seconds=float(get("level_decision_shadow.outcome_retention_seconds")),
        acceptance_min_events=int(get("level_decision_shadow.acceptance_min_events")),
        acceptance_min_sessions=int(get("level_decision_shadow.acceptance_min_sessions")),
        acceptance_min_complete_rth_sessions=int(
            get("level_decision_shadow.acceptance_min_complete_rth_sessions")
        ),
        acceptance_min_rth_sample_ratio=float(
            get("level_decision_shadow.acceptance_min_rth_sample_ratio")
        ),
        acceptance_max_rth_gap_seconds=float(
            get("level_decision_shadow.acceptance_max_rth_gap_seconds")
        ),
        acceptance_expected_sample_seconds=float(
            get("level_decision_shadow.acceptance_expected_sample_seconds")
        ),
    )
    storage = StorageSettingsSlice(
        data_root=data_root,
        latest_stale_after_seconds=market_data.latest_stale_after_seconds,
    )
    order_map = OrderMapPolicy(
        touch_time_fraction_coefficient=float(
            get("order_map_policy.touch_time_fraction_coefficient")
        ),
        touch_time_fraction_maximum=float(get("order_map_policy.touch_time_fraction_maximum")),
        vol_slope_beta=float(get("order_map_policy.vol_slope_beta")),
        minimum_tau_at_touch_hours=float(get("order_map_policy.minimum_tau_at_touch_hours")),
        conservative_limit_multiplier=float(get("order_map_policy.conservative_limit_multiplier")),
        risk_free_rate=float(get("order_map_policy.risk_free_rate")),
        early_touch_fraction_multiplier=float(
            get("order_map_policy.early_touch_fraction_multiplier")
        ),
        late_touch_fraction_multiplier=float(
            get("order_map_policy.late_touch_fraction_multiplier")
        ),
        execution_max_spread_points=float(get("order_map_policy.execution_max_spread_points")),
        execution_max_spread_bps=float(get("order_map_policy.execution_max_spread_bps")),
        execution_max_spread_percentile=float(
            get("order_map_policy.execution_max_spread_percentile")
        ),
        execution_max_quote_age_seconds=float(
            get("order_map_policy.execution_max_quote_age_seconds")
        ),
        execution_max_source_age_seconds=float(
            get("order_map_policy.execution_max_source_age_seconds")
        ),
        execution_max_provider_mid_divergence_bps=float(
            get("order_map_policy.execution_max_provider_mid_divergence_bps")
        ),
        frontrun_fraction=float(get("order_map_policy.frontrun_fraction")),
        frontrun_min_points=float(get("order_map_policy.frontrun_min_points")),
        frontrun_max_points=float(get("order_map_policy.frontrun_max_points")),
        es_volume_min_window_minutes=float(get("order_map_policy.es_volume_min_window_minutes")),
        es_volume_max_window_minutes=float(get("order_map_policy.es_volume_max_window_minutes")),
        es_volume_elevated_ratio=float(get("order_map_policy.es_volume_elevated_ratio")),
        es_volume_quiet_ratio=float(get("order_map_policy.es_volume_quiet_ratio")),
        es_volume_max_samples=int(get("order_map_policy.es_volume_max_samples")),
        es_volume_max_quote_age_seconds=float(
            get("order_map_policy.es_volume_max_quote_age_seconds")
        ),
        es_volume_flat_points=float(get("order_map_policy.es_volume_flat_points")),
        es_volume_level_band_points=float(get("order_map_policy.es_volume_level_band_points")),
        es_volume_reclaim_min_minutes=float(get("order_map_policy.es_volume_reclaim_min_minutes")),
        es_volume_reclaim_max_minutes=float(get("order_map_policy.es_volume_reclaim_max_minutes")),
    )
    if env.get("MARKET_DATA_DATA_ROOT") or env.get("MAINTENANCE_DATA_ROOT"):
        sources["storage.data_root"] = SettingSource(
            path="storage.data_root",
            origin="environment",
        )

    return AppSettings(
        market_data=market_data,
        ibkr=ibkr,
        schwab=schwab,
        analytics=analytics,
        globex_trend=globex_trend,
        market_features=market_features,
        alerts=alerts,
        runtime=runtime,
        shock=shock,
        level_decision=level_decision,
        order_map=order_map,
        storage=storage,
        defaults_path=defaults_path.resolve(),
        deployment_path=deployment_path.resolve() if deployment_path is not None else None,
        sources=sources,
        raw=merged,
    )


def load_app_settings(
    *,
    defaults_path: Path | None = None,
    deployment_path: Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> AppSettings:
    """Composition-root convenience: load documented runtime YAML (+ local overlay).

    Path discovery mirrors ``runtime_config`` so ``SPX_SPARK_RUNTIME_CONFIG`` and
    ``SPX_SPARK_RUNTIME_OVERRIDES`` / ``runtime.local.yaml`` keep working.
    """

    if defaults_path is None:
        defaults_path = _runtime_config_path()
    if deployment_path is None:
        deployment_path = _runtime_overrides_path()
    return load_settings(
        defaults_path=defaults_path,
        deployment_path=deployment_path,
        environ=environ,
    )


@lru_cache(maxsize=8)
def _cached_app_settings(defaults_text: str, deployment_text: str) -> AppSettings:
    return load_settings(
        defaults_path=Path(defaults_text),
        deployment_path=Path(deployment_text) if deployment_text else None,
    )


def clear_settings_cache() -> None:
    """Drop cached AppSettings (tests that retarget runtime YAML must call this)."""

    _cached_app_settings.cache_clear()


def current_app_settings() -> AppSettings:
    """Cached AppSettings for factory ``from_env`` helpers (settings-loader owned)."""

    defaults = _runtime_config_path()
    overrides = _runtime_overrides_path()
    return _cached_app_settings(
        str(defaults.resolve()),
        str(overrides.resolve()) if overrides is not None else "",
    )


def settings_value(dotted_path: str, *, app: AppSettings | None = None) -> Any:
    """Read a documented setting via AppSettings.raw — preferred over runtime_value()."""

    settings = app if app is not None else current_app_settings()
    node: Any = settings.raw
    for part in dotted_path.split("."):
        if not isinstance(node, dict) or part not in node:
            raise KeyError(f"Missing runtime configuration value: {dotted_path}")
        node = node[part]
    if not isinstance(node, dict) or "value" not in node:
        raise ValueError(f"Runtime setting must contain value and description: {dotted_path}")
    description = node.get("description")
    if not isinstance(description, str) or not description.strip():
        raise ValueError(f"Runtime setting has no description: {dotted_path}")
    return node["value"]


def settings_csv(dotted_path: str, *, app: AppSettings | None = None) -> str:
    value = settings_value(dotted_path, app=app)
    if not isinstance(value, list):
        raise TypeError(f"Runtime setting must be a list: {dotted_path}")
    return ",".join(str(item) for item in value)
