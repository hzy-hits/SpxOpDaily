from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest
import yaml

from spx_spark.application.shock.models import IntradayShockSettings
from spx_spark.settings import AppSettings, load_settings
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.settings.shock import ShockSettings


FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "runtime.defaults.yaml"


def test_load_settings_from_fixture_is_stable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("MARKET_DATA_PROVIDER_PRIORITY", raising=False)
    monkeypatch.delenv("IBKR_BROKER_ACCOUNT_READ_ENABLED", raising=False)
    monkeypatch.delenv("SPX_STEVEN_ENABLED", raising=False)

    settings = load_settings(defaults_path=FIXTURE, environ={})

    assert isinstance(settings, AppSettings)
    assert settings.market_data.provider_priority[:2] == ("schwab", "ibkr")
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
    assert settings.sources["market_data.provider_priority"].origin == "defaults"


def test_environment_overrides_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = load_settings(
        defaults_path=FIXTURE,
        environ={
            "SPX_STEVEN_ENABLED": "true",
            "MARKET_DATA_PROVIDER_PRIORITY": "ibkr,schwab",
        },
    )
    assert settings.alerts.steven_enabled is True
    assert settings.market_data.provider_priority == ("ibkr", "schwab")
    assert settings.sources["steven.enabled"].origin == "environment"
    assert settings.sources["market_data.provider_priority"].origin == "environment"


def test_deployment_overlay_beats_defaults(tmp_path: Path) -> None:
    overlay = {
        "steven": {
            "enabled": {
                "value": True,
            }
        }
    }
    deployment = tmp_path / "deployment.yaml"
    deployment.write_text(yaml.safe_dump(overlay), encoding="utf-8")

    settings = load_settings(
        defaults_path=FIXTURE,
        deployment_path=deployment,
        environ={},
    )
    assert settings.alerts.steven_enabled is True
    assert settings.sources["steven.enabled"].origin == "deployment"


def test_deployment_overlay_rejects_unknown_paths(tmp_path: Path) -> None:
    deployment = tmp_path / "deployment.yaml"
    deployment.write_text(
        yaml.safe_dump({"steven": {"typo": {"value": True}}}),
        encoding="utf-8",
    )

    with pytest.raises(KeyError, match="Unknown deployment settings"):
        load_settings(defaults_path=FIXTURE, deployment_path=deployment, environ={})


def test_deployment_overlay_cannot_replace_descriptions(tmp_path: Path) -> None:
    deployment = tmp_path / "deployment.yaml"
    deployment.write_text(
        yaml.safe_dump(
            {
                "steven": {
                    "enabled": {
                        "value": True,
                        "description": "Local description must not replace the tracked one.",
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="must contain only a value field"):
        load_settings(defaults_path=FIXTURE, deployment_path=deployment, environ={})


def test_missing_required_path_fails_fast(tmp_path: Path) -> None:
    broken = tmp_path / "broken.yaml"
    broken.write_text("schema_version:\n  value: 1\n  description: x\n", encoding="utf-8")
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
