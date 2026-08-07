from pathlib import Path

import pytest
from pydantic import ValidationError

from spx_spark.app_settings import AppSettings


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
