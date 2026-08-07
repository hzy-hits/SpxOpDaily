"""Assert unit tests do not observe workspace .env deployment overrides."""

from __future__ import annotations

import os
from pathlib import Path

from spx_spark.config import IbkrBrokerSettings, load_dotenv
from spx_spark.settings import current_app_settings, load_settings, settings_value


def test_app_settings_points_at_test_fixture() -> None:
    path = current_app_settings().defaults_path
    assert path.name == "runtime.defaults.toml"
    assert "tests/fixtures" in str(path).replace("\\", "/")


def test_load_dotenv_is_disabled_under_pytest() -> None:
    assert os.environ.get("SPX_SPARK_DISABLE_DOTENV", "").lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }
    before = dict(os.environ)
    load_dotenv()  # would otherwise setdefault from workspace .env
    assert os.environ == before


def test_runtime_local_overrides_are_disabled_under_pytest() -> None:
    assert os.environ.get("SPX_SPARK_DISABLE_RUNTIME_OVERRIDES", "").lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }
    assert current_app_settings().deployment_path is None


def test_broker_account_read_defaults_match_fixture_not_workspace_env(
    monkeypatch,
) -> None:
    monkeypatch.delenv("IBKR_BROKER_ACCOUNT_READ_ENABLED", raising=False)
    monkeypatch.delenv("IBKR_POSITIONS_ENABLED", raising=False)
    settings = IbkrBrokerSettings.from_env()
    assert settings.account_read_enabled is False
    assert settings_value("ibkr_broker.account_read_enabled") is False


def test_app_settings_match_fixture_defaults() -> None:
    fixture = Path(__file__).resolve().parents[1] / "fixtures" / "runtime.defaults.toml"
    settings = load_settings(defaults_path=fixture, environ={})
    assert settings.runtime.control_ibkr_stream_enabled is False
    assert settings.alerts.steven_enabled is False
    assert settings.alerts.require_schwab_streaming_anchors is True
