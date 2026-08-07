"""Pytest bootstrap: isolate unit tests from workspace deployment config."""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
import subprocess
import sys

import pytest

_TESTS_ROOT = Path(__file__).resolve().parent
_FIXTURE_RUNTIME_CONFIG = _TESTS_ROOT / "fixtures" / "runtime.defaults.yaml"

# Pin before any spx_spark import evaluates runtime_value() dataclass defaults.
os.environ["SPX_SPARK_DISABLE_DOTENV"] = "1"
os.environ["SPX_SPARK_DISABLE_RUNTIME_OVERRIDES"] = "1"
os.environ["SPX_SPARK_RUNTIME_CONFIG"] = str(_FIXTURE_RUNTIME_CONFIG)


def _dotenv_keys(path: Path) -> list[str]:
    if not path.is_file():
        return []
    keys: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key = stripped.split("=", 1)[0].strip()
        if key:
            keys.append(key)
    return keys


def pytest_configure() -> None:
    """Drop keys that a local .env may have already injected into the process."""
    for key in _dotenv_keys(Path.cwd() / ".env"):
        os.environ.pop(key, None)
    # Ensure the fixture path wins even if a parent conftest changed it.
    os.environ.pop("SPX_SPARK_RUNTIME_OVERRIDES", None)
    os.environ["SPX_SPARK_DISABLE_DOTENV"] = "1"
    os.environ["SPX_SPARK_DISABLE_RUNTIME_OVERRIDES"] = "1"
    os.environ["SPX_SPARK_RUNTIME_CONFIG"] = str(_FIXTURE_RUNTIME_CONFIG)
    try:
        from spx_spark.runtime_config import _load_runtime_config
        from spx_spark.settings import clear_settings_cache

        _load_runtime_config.cache_clear()
        clear_settings_cache()
    except Exception:
        # Package may not be importable yet during very early collection failures.
        pass


@pytest.fixture
def migrate_operational_database(
    monkeypatch: pytest.MonkeyPatch,
) -> Callable[[Path], Path]:
    """Create the single Alembic-managed operational database for a test."""

    def migrate(root: Path) -> Path:
        from spx_spark.app_settings import get_settings

        monkeypatch.setenv("SPX_DATA_ROOT", str(root))
        get_settings.cache_clear()
        result = subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            check=False,
            capture_output=True,
            text=True,
            cwd=_TESTS_ROOT.parent,
            env=os.environ | {"SPX_DATA_ROOT": str(root)},
        )
        assert result.returncode == 0, result.stderr
        return root / "spx.sqlite"

    yield migrate
    from spx_spark.app_settings import get_settings

    get_settings.cache_clear()
