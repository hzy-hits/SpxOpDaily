from __future__ import annotations

from pathlib import Path


SRC_ROOT = Path(__file__).resolve().parents[2] / "src" / "spx_spark"
MAX_PRODUCTION_MODULE_LINES = 1000


def test_production_python_modules_stay_below_size_budget() -> None:
    oversized = {
        str(path.relative_to(SRC_ROOT)): len(path.read_text(encoding="utf-8").splitlines())
        for path in SRC_ROOT.rglob("*.py")
        if len(path.read_text(encoding="utf-8").splitlines()) > MAX_PRODUCTION_MODULE_LINES
    }
    assert not oversized, f"Production modules exceed {MAX_PRODUCTION_MODULE_LINES} lines: {oversized}"
