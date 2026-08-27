from __future__ import annotations

from pathlib import Path


SRC_ROOT = Path(__file__).resolve().parents[2] / "src" / "spx_spark"
MAX_PRODUCTION_MODULE_LINES = 1000
# Monotonic debt baseline: values may only decrease; remove an entry once it reaches the limit.
OVERSIZED_MODULE_LINE_BASELINE = {
    "application/order_map/candidate_factory.py": 1451,
    "application/order_map/operator_status.py": 1214,
    "application/order_map/rth_daily_acceptance.py": 1046,
    "application/order_map/service.py": 1041,
    "application/order_map/strategy_facts.py": 1309,
    "application/order_map/strategy_ranker.py": 2107,
    "application/order_map/strategy_regime.py": 1032,
    "application/shock/gth_dip.py": 1001,
    "data_platform/research/regime_hmm_calibration.py": 2060,
    "infrastructure/growth_dislocation.py": 1414,
    "options_map/render.py": 1358,
}


def test_production_python_modules_stay_below_size_budget() -> None:
    line_counts = {
        str(path.relative_to(SRC_ROOT)): len(path.read_text(encoding="utf-8").splitlines())
        for path in SRC_ROOT.rglob("*.py")
    }
    new_oversized = {
        path: lines
        for path, lines in line_counts.items()
        if lines > MAX_PRODUCTION_MODULE_LINES and path not in OVERSIZED_MODULE_LINE_BASELINE
    }
    baseline_growth = {
        path: {"baseline": baseline, "current": line_counts.get(path, 0)}
        for path, baseline in OVERSIZED_MODULE_LINE_BASELINE.items()
        if line_counts.get(path, 0) > baseline
    }
    stale_baseline = {
        path: line_counts.get(path, 0)
        for path in OVERSIZED_MODULE_LINE_BASELINE
        if line_counts.get(path, 0) <= MAX_PRODUCTION_MODULE_LINES
    }

    assert not (new_oversized or baseline_growth or stale_baseline), (
        f"Production module size budget failed: new_oversized={new_oversized}, "
        f"baseline_growth={baseline_growth}, stale_baseline={stale_baseline}"
    )
