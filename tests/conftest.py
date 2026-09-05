"""Pytest bootstrap: isolate unit tests from workspace deployment config."""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
import subprocess
import sys

import pytest

_TESTS_ROOT = Path(__file__).resolve().parent
_FIXTURE_SETTINGS = _TESTS_ROOT / "fixtures" / "runtime.defaults.toml"

# Pin before any spx_spark import loads typed application defaults.
os.environ["SPX_SPARK_DISABLE_DOTENV"] = "1"
os.environ["SPX_SPARK_DISABLE_RUNTIME_OVERRIDES"] = "1"
os.environ["SPX_SPARK_RUNTIME_CONFIG"] = str(_FIXTURE_SETTINGS)


def pytest_configure() -> None:
    """Isolate inherited application overrides without reading local credentials."""
    for key in tuple(os.environ):
        if key.startswith(("SPX_", "IBKR_", "SCHWAB_", "BARK_", "FEISHU_")):
            os.environ.pop(key, None)
    # Ensure the fixture path wins even if a parent conftest changed it.
    os.environ.pop("SPX_SPARK_RUNTIME_OVERRIDES", None)
    os.environ["SPX_SPARK_DISABLE_DOTENV"] = "1"
    os.environ["SPX_SPARK_DISABLE_RUNTIME_OVERRIDES"] = "1"
    os.environ["SPX_SPARK_RUNTIME_CONFIG"] = str(_FIXTURE_SETTINGS)
    try:
        from spx_spark.settings import clear_settings_cache

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
