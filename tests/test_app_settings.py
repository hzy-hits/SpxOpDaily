from pathlib import Path

import pytest
from pydantic import ValidationError
from typer.testing import CliRunner

from spx_spark.app_settings import AppSettings
from spx_spark.cli import app


@pytest.fixture
def settings_cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    config = tmp_path / "config"
    config.mkdir()
    (config / "defaults.toml").write_text(
        'data_root = "/defaults"\nlog_level = "INFO"\n', encoding="utf-8"
    )
    (config / "production.toml").write_text(
        'data_root = "/production"\nlog_level = "WARNING"\n', encoding="utf-8"
    )
    monkeypatch.chdir(tmp_path)
    return config


def test_settings_priority_is_toml_then_env_then_init(
    settings_cwd: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert AppSettings().data_root == Path("/production")

    monkeypatch.setenv("SPX_DATA_ROOT", "/environment")
    assert AppSettings().data_root == Path("/environment")
    assert AppSettings(data_root=Path("/init")).data_root == Path("/init")


def test_unknown_toml_key_is_rejected(settings_cwd: Path) -> None:
    (settings_cwd / "production.toml").write_text('unexpected = "value"\n', encoding="utf-8")

    with pytest.raises(ValidationError, match="unexpected"):
        AppSettings()


def test_core_runtime_paths_are_typed_and_cli_is_registered(
    settings_cwd: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SPX_CORE_SOCKET_PATH", "/run/user/1001/spx-core.sock")
    monkeypatch.setenv("SPX_CORE_LOCK_ROOT", "/run/user/1001")
    settings = AppSettings()

    assert settings.core_socket_path == Path("/run/user/1001/spx-core.sock")
    assert settings.core_lock_root == Path("/run/user/1001")
    result = CliRunner().invoke(app, ["core", "--help"])
    assert result.exit_code == 0
    assert "run" in result.stdout
