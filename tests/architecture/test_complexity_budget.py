from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path


SRC_ROOT = Path(__file__).resolve().parents[2] / "src" / "spx_spark"
CONTROL_FLOW = (ast.If, ast.Match, ast.Try, ast.For, ast.While)


@dataclass(frozen=True)
class FunctionBudget:
    module: str
    function: str
    max_lines: int
    max_control_flow: int


CRITICAL_FUNCTION_BUDGETS = (
    FunctionBudget("schwab/collector.py", "run", 260, 8),
    FunctionBudget("strategy/steven_machine.py", "advance_state", 50, 6),
    FunctionBudget(
        "post_close_completeness.py",
        "evaluate_review_completeness",
        25,
        1,
    ),
    FunctionBudget("greek_reference.py", "calculate_contract_reference", 50, 1),
    FunctionBudget("greek_reference.py", "build_zero_dte_greeks_reference", 90, 6),
    FunctionBudget("data_platform/lake/compact.py", "_maybe_delete_raw", 50, 6),
)


def _find_function(path: Path, name: str) -> ast.FunctionDef | ast.AsyncFunctionDef:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    matches = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name == name
    ]
    assert len(matches) == 1, f"Expected exactly one {name} in {path}, found {len(matches)}"
    return matches[0]


def test_critical_orchestrators_stay_within_complexity_budgets() -> None:
    violations: list[str] = []
    for budget in CRITICAL_FUNCTION_BUDGETS:
        node = _find_function(SRC_ROOT / budget.module, budget.function)
        line_count = node.end_lineno - node.lineno + 1
        control_flow = sum(isinstance(child, CONTROL_FLOW) for child in ast.walk(node))
        if line_count > budget.max_lines or control_flow > budget.max_control_flow:
            violations.append(
                f"{budget.module}:{budget.function} lines={line_count}/{budget.max_lines} "
                f"control_flow={control_flow}/{budget.max_control_flow}"
            )
    assert not violations, "Critical function complexity regressions:\n" + "\n".join(violations)
