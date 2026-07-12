"""Per-file decreasing budget for residual runtime_value() call sites.

Import-time / module-scope runtime_value reads must stay at zero. Function-body
calls are frozen per file and may only decrease as composition roots inject
AppSettings / typed policies instead.

Factories should prefer ``settings_value`` / ``settings_csv`` (AppSettings.raw)
over direct ``runtime_value`` calls.
"""

from __future__ import annotations

import ast
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[2] / "src" / "spx_spark"

# Only settings loader / composition roots should read runtime config long-term.
# Cleared this session (P1-C fold into settings_value / AppSettings):
#   config.py, strategy/steven.py, post_close_review.py, position_alerts.py,
#   provider_failover_controller.py, and remaining non-loader call sites.
# Sole residual: runtime_config.runtime_value itself (definition body).
RUNTIME_VALUE_CALL_BUDGET: dict[str, int] = {
    "runtime_config.py": 1,
    # Zeroed — kept at 0 so regressions fail closed if reintroduced.
    "config.py": 0,
    "strategy/steven.py": 0,
    "post_close_review.py": 0,
    "position_alerts.py": 0,
    "provider_failover_controller.py": 0,
    "intraday_strategy.py": 0,
    "market_context.py": 0,
    "data_platform/settings.py": 0,
    "notifier/llm_writer.py": 0,
    "schwab/gateway.py": 0,
    "schwab/symbols.py": 0,
    "intraday_event_outcomes.py": 0,
    "features/exposure_map.py": 0,
    "ibkr/stream/session_ops.py": 0,
    "steven_validation.py": 0,
    "strategy/steven_replay.py": 0,
    "application/runtime/settings.py": 0,
    "application/shock/models.py": 0,
    "application/shock/service.py": 0,
    "alert_engine/rules_options.py": 0,
    "alert_engine/rules_system.py": 0,
    "alert_engine/rules_price.py": 0,
    "alert_engine/evaluator.py": 0,
}


def _count_runtime_value_calls(path: Path) -> int:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    count = 0
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Name) and func.id == "runtime_value":
            count += 1
        elif isinstance(func, ast.Attribute) and func.attr == "runtime_value":
            count += 1
    return count


def _import_time_runtime_value_sites(path: Path) -> list[int]:
    """Line numbers of runtime_value calls at module / class body scope."""

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    sites: list[int] = []

    def scan_body(body: list[ast.stmt]) -> None:
        for node in body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if isinstance(node, ast.ClassDef):
                scan_body(node.body)
                continue
            for child in ast.walk(node):
                if not isinstance(child, ast.Call):
                    continue
                func = child.func
                if isinstance(func, ast.Name) and func.id == "runtime_value":
                    sites.append(child.lineno)
                elif isinstance(func, ast.Attribute) and func.attr == "runtime_value":
                    sites.append(child.lineno)

    scan_body(tree.body)
    return sites


def test_no_import_time_runtime_value_calls() -> None:
    offenders: list[str] = []
    for path in sorted(SRC_ROOT.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        sites = _import_time_runtime_value_sites(path)
        if not sites:
            continue
        rel = str(path.relative_to(SRC_ROOT)).replace("\\", "/")
        offenders.append(f"{rel}: lines {sites}")
    assert not offenders, (
        "Import-time runtime_value() is forbidden; use literals + from_env/load_settings:\n"
        + "\n".join(offenders)
    )


def test_runtime_value_call_budgets_only_decrease() -> None:
    observed: dict[str, int] = {}
    for path in sorted(SRC_ROOT.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        count = _count_runtime_value_calls(path)
        if count == 0:
            continue
        rel = str(path.relative_to(SRC_ROOT)).replace("\\", "/")
        observed[rel] = count

    unknown = sorted(set(observed) - set(RUNTIME_VALUE_CALL_BUDGET))
    assert not unknown, (
        "New runtime_value() files are forbidden; inject AppSettings instead:\n"
        + "\n".join(unknown)
    )

    regressions: list[str] = []
    for rel, budget in sorted(RUNTIME_VALUE_CALL_BUDGET.items()):
        actual = observed.get(rel, 0)
        if actual > budget:
            regressions.append(f"{rel}: {actual} > budget {budget}")
    assert not regressions, (
        "runtime_value() call counts may only decrease:\n" + "\n".join(regressions)
    )
