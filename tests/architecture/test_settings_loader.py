from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from spx_spark.application.shock import models as shock_models
from spx_spark.application.shock.models import IntradayShockSettings
from spx_spark.settings import (
    AppSettings,
    SpringGammaV3Settings,
    StrategyDistributionSettings,
    load_settings,
)
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.settings.shock import ShockSettings


FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "runtime.defaults.toml"


def test_intraday_shock_reuses_explicit_app_settings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = load_settings(defaults_path=FIXTURE, environ={})

    def unexpected_reload() -> AppSettings:
        raise AssertionError("shock tick must not reload runtime TOML")

    monkeypatch.setattr(shock_models, "current_app_settings", unexpected_reload)

    settings = IntradayShockSettings.from_env(app_settings=app)

    assert settings.gth_exit_clock_et == app.shock.gth_exit_clock_et


def test_load_settings_from_fixture_is_stable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("MARKET_DATA_PROVIDER_PRIORITY", raising=False)
    monkeypatch.delenv("IBKR_BROKER_ACCOUNT_READ_ENABLED", raising=False)
    monkeypatch.delenv("SPX_STEVEN_ENABLED", raising=False)

    settings = load_settings(defaults_path=FIXTURE, environ={})

    assert isinstance(settings, AppSettings)
    assert settings.market_data.provider_priority[:2] == ("schwab", "ibkr")
    assert settings.market_data.standardized_minute_max_age_seconds == 90.0
    assert settings.market_context.sector_breadth_min_usable == 8
    assert settings.market_context.hyperliquid_proxy_basis_warn_bps == 50.0
    assert settings.ibkr.account_read_enabled is False
    assert settings.alerts.steven_enabled is False
    assert settings.runtime.control_ibkr_stream_enabled is False
    assert settings.schwab.streaming_mode == "live"
    assert settings.schwab.service_loop_enabled is False
    assert settings.schwab.capacity.planned_requests_per_minute == 84
    assert settings.schwab.wide_chain.strike_count_candidates == (80, 100, 120)
    assert settings.schwab.wide_chain.next_expiry_strike_count == 40
    assert settings.market_features.enabled is True
    assert settings.market_features.volume_baseline_sessions == 20
    assert isinstance(settings.spring_gamma_v3, SpringGammaV3Settings)
    assert settings.spring_gamma_v3.authority == "shadow"
    assert settings.spring_gamma_v3.prediction_interval_seconds == 60
    assert settings.spring_gamma_v3.horizons_minutes == (15, 30, 60)
    assert settings.spring_gamma_v3.rth_greek_max_age_seconds == 15.0
    assert settings.spring_gamma_v3.gth_greek_max_age_seconds == 90.0
    assert settings.sources["spring_gamma_v3.enabled"].origin == "defaults"
    assert settings.sources["market_data.provider_priority"].origin == "defaults"
    assert isinstance(settings.strategy_distribution, StrategyDistributionSettings)
    assert settings.strategy_distribution.horizon_seconds == 300
    assert settings.strategy_distribution.refresh_seconds == 60.0
    assert settings.strategy_distribution.action_authority == "none"
    assert settings.strategy_distribution.automatic_ordering is False
    assert settings.level_decision.phase_timeout_seconds == 90.0
    assert settings.level_decision.gth_phase_timeout_seconds == 300.0


def test_environment_overrides_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = load_settings(
        defaults_path=FIXTURE,
        environ={
            "SPX_STEVEN_ENABLED": "true",
            "MARKET_DATA_PROVIDER_PRIORITY": "ibkr,schwab",
            "SPX_STANDARDIZED_MINUTE_MAX_AGE_SECONDS": "120",
            "ALERT_INTRADAY_ANCHOR_PROVIDER_PRIORITY": "ibkr,schwab",
            "ALERT_INTRADAY_REQUIRE_SCHWAB_STREAMING_ANCHORS": "false",
            "ALERT_INTRADAY_PROVIDER_SWITCH_RESET_SECONDS": "45",
            "ALERT_INTRADAY_SHOCK_SPX_MAX_AGE_SECONDS": "21.5",
            "ALERT_GAMMA_REGIME_HYSTERESIS_SECONDS": "720",
            "ALERT_IV_SURFACE_SHIFT_1H_THRESHOLD": "0.06",
            "ALERT_IV_ATM_CHANGE_1H_THRESHOLD": "0.045",
            "ALERT_SYSTEM_EVENTS_ENABLED": "false",
            "ALERT_POSITIONS_ENABLED": "true",
            "ALERT_POSITION_PNL_CHANGE_USD": "250",
            "HYPERLIQUID_PROXY_BASIS_WARN_BPS": "55",
            "HYPERLIQUID_ES_CARRY_ANNUAL_RATE": "0.04",
            "MICOPEDIA_EVENT_TAGS": "FOMC,cpi",
        },
    )
    assert settings.alerts.steven_enabled is True
    assert settings.market_data.provider_priority == ("ibkr", "schwab")
    assert settings.market_data.standardized_minute_max_age_seconds == 120.0
    assert settings.shock.anchor_provider_priority == ("ibkr", "schwab")
    assert settings.shock.require_schwab_streaming_anchors is False
    assert settings.shock.provider_switch_reset_seconds == 45
    assert settings.shock.max_spx_age_seconds == 21.5
    assert settings.alerts.gamma_regime_hysteresis_seconds == 720.0
    assert settings.alerts.iv_surface_shift_1h_threshold == 0.06
    assert settings.alerts.iv_atm_change_1h_threshold == 0.045
    assert settings.alerts.system_events_enabled is False
    assert settings.alerts.positions_enabled is True
    assert settings.alerts.position_pnl_change_usd == 250.0
    assert settings.market_context.hyperliquid_proxy_basis_warn_bps == 55.0
    assert settings.market_context.hyperliquid_es_carry_annual_rate == 0.04
    assert settings.market_context.human_focus_event_tags == ("fomc", "cpi")
    assert settings.sources["steven.enabled"].origin == "environment"
    assert settings.sources["market_data.provider_priority"].origin == "environment"
    assert settings.sources["intraday_shock.max_spx_age_seconds"].origin == "environment"
    assert settings.sources["alerts.gamma_regime_hysteresis_seconds"].origin == "environment"
    assert settings.sources["alerts.system_events_enabled"].origin == "environment"
    assert settings.sources["position_alerts.enabled"].origin == "environment"
    assert settings.sources["hyperliquid.proxy_basis_warn_bps"].origin == "environment"
    assert settings.sources["human_focus.event_tags"].origin == "environment"


def test_provider_failover_environment_overrides_resolve_in_typed_policy() -> None:
    settings = load_settings(
        defaults_path=FIXTURE,
        environ={
            "PROVIDER_FAILOVER_ENABLED": "false",
            "PROVIDER_FAILOVER_CONTROL_IBKR_STREAM_ENABLED": "true",
            "PROVIDER_FAILOVER_STATE_PATH": "/tmp/failover-state.json",
            "PROVIDER_FAILOVER_STATE_MAX_AGE_SECONDS": "51.5",
            "PROVIDER_FAILOVER_QUOTE_MAX_AGE_SECONDS": "52.5",
            "PROVIDER_FAILOVER_CONTROL_STATE_MAX_AGE_SECONDS": "61.5",
            "PROVIDER_FAILOVER_TRANSITION_ALERT_MAX_AGE_SECONDS": "901.5",
            "PROVIDER_FAILOVER_RTH_ONLY": "true",
            "PROVIDER_FAILOVER_GTH_MIN_LIVE_OPTION_CONTRACTS": "24",
            "PROVIDER_FAILOVER_GTH_OPTION_QUOTE_MAX_AGE_SECONDS": "91.5",
            "PROVIDER_FAILOVER_SCHWAB_UNHEALTHY_OBSERVATIONS": "5",
            "PROVIDER_FAILOVER_SCHWAB_RECOVERY_OBSERVATIONS": "6",
            "PROVIDER_FAILOVER_IBKR_UNHEALTHY_OBSERVATIONS": "7",
            "PROVIDER_FAILOVER_IBKR_RECOVERY_OBSERVATIONS": "8",
        },
    )

    policy = settings.runtime
    assert policy.provider_failover_enabled is False
    assert policy.control_ibkr_stream_enabled is True
    assert policy.provider_failover_state_path == "/tmp/failover-state.json"
    assert policy.provider_failover_state_max_age_seconds == 51.5
    assert policy.provider_failover_quote_max_age_seconds == 52.5
    assert policy.provider_failover_control_state_max_age_seconds == 61.5
    assert policy.provider_failover_transition_alert_max_age_seconds == 901.5
    assert policy.provider_failover_monitor_rth_only is True
    assert policy.provider_failover_gth_min_live_option_contracts == 24
    assert policy.provider_failover_gth_option_quote_max_age_seconds == 91.5
    assert policy.provider_failover_schwab_unhealthy_observations == 5
    assert policy.provider_failover_schwab_recovery_observations == 6
    assert policy.provider_failover_ibkr_unhealthy_observations == 7
    assert policy.provider_failover_ibkr_recovery_observations == 8
    assert settings.sources["provider_failover.state_path"].origin == "environment"
    assert (
        settings.sources["provider_failover.ibkr_recovery_observations"].origin
        == "environment"
    )


def test_deployment_overlay_beats_defaults(tmp_path: Path) -> None:
    deployment = tmp_path / "deployment.toml"
    deployment.write_text("[steven.enabled]\nvalue = true\n", encoding="utf-8")

    settings = load_settings(
        defaults_path=FIXTURE,
        deployment_path=deployment,
        environ={},
    )
    assert settings.alerts.steven_enabled is True
    assert settings.sources["steven.enabled"].origin == "deployment"


def test_level_decision_gth_timeout_follows_typed_precedence(tmp_path: Path) -> None:
    deployment = tmp_path / "deployment.toml"
    deployment.write_text(
        "[level_decision_shadow.gth_phase_timeout_seconds]\nvalue = 240.0\n",
        encoding="utf-8",
    )

    deployed = load_settings(
        defaults_path=FIXTURE,
        deployment_path=deployment,
        environ={},
    )
    assert deployed.level_decision.phase_timeout_seconds == 90.0
    assert deployed.level_decision.gth_phase_timeout_seconds == 240.0
    assert (
        deployed.sources["level_decision_shadow.gth_phase_timeout_seconds"].origin
        == "deployment"
    )

    environment = load_settings(
        defaults_path=FIXTURE,
        deployment_path=deployment,
        environ={"SPX_LEVEL_DECISION_GTH_PHASE_TIMEOUT_SECONDS": "275"},
    )
    assert environment.level_decision.phase_timeout_seconds == 90.0
    assert environment.level_decision.gth_phase_timeout_seconds == 275.0
    assert (
        environment.sources["level_decision_shadow.gth_phase_timeout_seconds"].origin
        == "environment"
    )


@pytest.mark.parametrize("value", ("true", "nan", "inf", "-inf"))
def test_level_decision_gth_timeout_rejects_non_finite_or_boolean_environment(
    value: str,
) -> None:
    with pytest.raises((TypeError, ValueError), match="finite number|must be finite"):
        load_settings(
            defaults_path=FIXTURE,
            environ={"SPX_LEVEL_DECISION_GTH_PHASE_TIMEOUT_SECONDS": value},
        )


def test_spring_gamma_v3_deployment_overlay_remains_shadow_only(tmp_path: Path) -> None:
    deployment = tmp_path / "deployment.toml"
    deployment.write_text(
        "[spring_gamma_v3.report_enabled]\nvalue = false\n"
        "[spring_gamma_v3.min_probability]\nvalue = 0.65\n",
        encoding="utf-8",
    )

    settings = load_settings(
        defaults_path=FIXTURE,
        deployment_path=deployment,
        environ={},
    )

    assert settings.spring_gamma_v3.report_enabled is False
    assert settings.spring_gamma_v3.min_probability == 0.65
    assert settings.spring_gamma_v3.authority == "shadow"
    assert "authority" not in settings.raw["spring_gamma_v3"]
    assert settings.sources["spring_gamma_v3.min_probability"].origin == "deployment"


def test_deployment_overlay_rejects_unknown_paths(tmp_path: Path) -> None:
    deployment = tmp_path / "deployment.toml"
    deployment.write_text("[steven.typo]\nvalue = true\n", encoding="utf-8")

    with pytest.raises(KeyError, match="Unknown deployment settings"):
        load_settings(defaults_path=FIXTURE, deployment_path=deployment, environ={})


def test_deployment_overlay_cannot_replace_descriptions(tmp_path: Path) -> None:
    deployment = tmp_path / "deployment.toml"
    deployment.write_text(
        "[steven.enabled]\n"
        "value = true\n"
        'description = "Local description must not replace the tracked one."\n',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="must contain only a value field"):
        load_settings(defaults_path=FIXTURE, deployment_path=deployment, environ={})


def test_deployment_overlay_rejects_value_type_changes(tmp_path: Path) -> None:
    deployment = tmp_path / "deployment.toml"
    deployment.write_text('[steven.enabled]\nvalue = "true"\n', encoding="utf-8")

    with pytest.raises(TypeError, match="must match bool"):
        load_settings(defaults_path=FIXTURE, deployment_path=deployment, environ={})


def test_missing_required_path_fails_fast(tmp_path: Path) -> None:
    broken = tmp_path / "broken.toml"
    broken.write_text(
        '[schema_version]\nvalue = 1\ndescription = "x"\n',
        encoding="utf-8",
    )
    with pytest.raises(KeyError, match="market_data.known_providers"):
        load_settings(defaults_path=broken, environ={})


def test_cwd_does_not_change_fixture_settings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    first = load_settings(defaults_path=FIXTURE, environ={})
    second = load_settings(defaults_path=FIXTURE, environ={})
    assert first.market_data.provider_priority == second.market_data.provider_priority
    assert first.alerts.steven_enabled == second.alerts.steven_enabled


def test_gth_spread_and_exit_clock_settings_load() -> None:
    settings = load_settings(defaults_path=FIXTURE, environ={})

    assert settings.shock.gth_spread_min_width_points == 15.0
    assert settings.shock.gth_spread_max_width_points == 75.0
    assert settings.shock.gth_spread_default_width_points == 50.0
    assert settings.shock.gth_structure_max_age_seconds == 90.0
    assert settings.shock.gth_exit_clock_et == "09:45"
    assert settings.market_features.virtual_gth_time_stop_minutes == 810
    assert settings.market_features.virtual_gth_exit_clock_et == "09:45"
    assert settings.market_features.virtual_gth_spread_saturation_fraction == 0.85
    assert settings.market_features.gth_manual_candidate_enabled is True
    assert (
        settings.market_features.gth_manual_candidate_quote_max_age_seconds
        == 15.0
    )
    assert settings.market_features.gth_manual_candidate_ttl_seconds == 300.0
    assert settings.market_features.gth_manual_candidate_min_parity_pairs == 3
    assert settings.market_features.virtual_gth_exit_clock_et == settings.shock.gth_exit_clock_et


def test_intraday_shock_settings_carry_gth_spread_policy() -> None:
    settings = load_settings(defaults_path=FIXTURE, environ={})
    derived = IntradayShockSettings.from_policy(settings.shock)

    assert derived.gth_spread_min_width_points == 15.0
    assert derived.gth_spread_max_width_points == 75.0
    assert derived.gth_spread_default_width_points == 50.0
    assert derived.gth_structure_max_age_seconds == 90.0
    assert derived.gth_exit_clock_et == "09:45"


@pytest.mark.parametrize(
    ("overrides", "message"),
    (
        ({"gth_spread_min_width_points": 55.0}, "min <= default <= max"),
        ({"gth_spread_default_width_points": 52.0}, "five-point"),
        ({"gth_structure_max_age_seconds": 0.0}, "max age"),
        ({"gth_exit_clock_et": "13:45 UTC"}, "invalid ET clock"),
        ({"gth_exit_clock_et": "04:30"}, "after the 04:30"),
    ),
)
def test_shock_rejects_invalid_gth_spread_policy(
    overrides: dict[str, object], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        replace(ShockSettings(), **overrides)


def test_virtual_gth_exit_clock_rejects_non_wall_clock() -> None:
    with pytest.raises(ValueError, match="invalid ET clock"):
        replace(MarketFeatureSettings(), virtual_gth_exit_clock_et="09:45:30")


@pytest.mark.parametrize(
    "overrides",
    (
        {"trade_quote_max_age_seconds": 9.9},
        {"trade_quote_max_age_seconds": 15.1},
        {"gth_manual_candidate_quote_max_age_seconds": 9.9},
        {"gth_manual_candidate_quote_max_age_seconds": 15.1},
    ),
)
def test_market_feature_settings_bound_execution_quote_freshness(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="10 to 15 seconds"):
        replace(MarketFeatureSettings(), **overrides)


@pytest.mark.parametrize(
    "overrides",
    (
        {"trade_intent_ttl_seconds": 299.9},
        {"trade_intent_ttl_seconds": 600.1},
        {"trade_entry_window_seconds": 299.9},
        {"trade_entry_window_seconds": 600.1},
        {"gth_manual_candidate_ttl_seconds": 299.9},
        {"gth_manual_candidate_ttl_seconds": 600.1},
    ),
)
def test_market_feature_settings_bound_manual_opportunity_windows(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="5 to 10 minutes"):
        replace(MarketFeatureSettings(), **overrides)


@pytest.mark.parametrize(
    "overrides",
    (
        {"play_stats_window_days": 0},
        {"play_stats_min_samples": 0},
        {"play_stats_refresh_seconds": -1.0},
        {"play_stats_horizon": "0"},
        {"play_stats_horizon": "300.0"},
        {"play_stats_horizon": "0300"},
    ),
)
def test_market_feature_settings_reject_invalid_play_stats(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        replace(MarketFeatureSettings(), **overrides)


@pytest.mark.parametrize(
    "overrides",
    (
        {"prediction_interval_seconds": 0},
        {"horizons_minutes": ()},
        {"horizons_minutes": (30, 15)},
        {"rth_greek_max_age_seconds": 0.0},
        {"gth_iv_max_age_seconds": -1.0},
        {"min_pair_ratio": 0.0},
        {"min_iv": 1.01},
        {"min_delta": -0.1},
        {"min_oi": 1.1},
        {"min_paired_strikes": 0},
        {"min_probability": 0.5},
        {"min_margin": 0.0},
    ),
)
def test_spring_gamma_v3_rejects_invalid_policy(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        replace(SpringGammaV3Settings(), **overrides)


def test_strategy_distribution_refresh_must_stay_inside_projection_ttl() -> None:
    with pytest.raises(ValueError, match="shorter than projection TTL"):
        replace(
            StrategyDistributionSettings(),
            refresh_seconds=90.0,
            projection_ttl_seconds=90.0,
        )
