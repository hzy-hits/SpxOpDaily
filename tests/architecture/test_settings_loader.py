from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from spx_spark.settings import AppSettings, load_settings


FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "runtime.defaults.yaml"


def test_load_settings_from_fixture_is_stable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MARKET_DATA_PROVIDER_PRIORITY", raising=False)
    monkeypatch.delenv("IBKR_BROKER_ACCOUNT_READ_ENABLED", raising=False)
    monkeypatch.delenv("SPX_STEVEN_ENABLED", raising=False)

    settings = load_settings(defaults_path=FIXTURE, environ={})

    assert isinstance(settings, AppSettings)
    assert settings.market_data.provider_priority[:2] == ("schwab", "ibkr")
    assert settings.ibkr.account_read_enabled is False
    assert settings.alerts.steven_enabled is False
    assert settings.runtime.control_ibkr_stream_enabled is False
    assert settings.schwab.streaming_mode == "off"
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


def test_cwd_does_not_change_fixture_settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    first = load_settings(defaults_path=FIXTURE, environ={})
    second = load_settings(defaults_path=FIXTURE, environ={})
    assert first.market_data.provider_priority == second.market_data.provider_priority
    assert first.alerts.steven_enabled == second.alerts.steven_enabled
