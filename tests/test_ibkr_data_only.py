from __future__ import annotations

import ast
from pathlib import Path


FORBIDDEN_IBKR_METHODS = {
    "bracketOrder",
    "cancelOrder",
    "exerciseOptions",
    "oneCancelsAll",
    "placeOrder",
    "reqAccountSummary",
    "reqAccountUpdates",
    "reqAccountUpdatesMulti",
    "reqAllOpenOrders",
    "reqAutoOpenOrders",
    "reqCompletedOrders",
    "reqExecutions",
    "reqGlobalCancel",
    "reqOpenOrders",
    "reqPnL",
    "reqPnLSingle",
    "reqPositions",
    "reqPositionsMulti",
    "whatIfOrder",
}


def iter_python_files(root: Path) -> list[Path]:
    return sorted(
        path
        for path in root.rglob("*.py")
        if "__pycache__" not in path.parts
    )


def test_ibkr_package_stays_data_only() -> None:
    root = Path(__file__).resolve().parents[1] / "src" / "spx_spark" / "ibkr"
    violations: list[str] = []

    for path in iter_python_files(root):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr in FORBIDDEN_IBKR_METHODS:
                violations.append(f"{path.relative_to(root.parent.parent.parent)}:{node.lineno}: {node.attr}")

    assert not violations, "IBKR package must stay market-data only:\n" + "\n".join(violations)
