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


def test_ibkr_connect_calls_disable_startup_account_fetches() -> None:
    root = Path(__file__).resolve().parents[1] / "src" / "spx_spark" / "ibkr"
    violations: list[str] = []

    for path in iter_python_files(root):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not isinstance(node.func, ast.Attribute) or node.func.attr != "connect":
                continue

            keywords = {keyword.arg: keyword.value for keyword in node.keywords}
            readonly = keywords.get("readonly")
            fetch_fields = keywords.get("fetchFields")
            if not (isinstance(readonly, ast.Constant) and readonly.value is True):
                violations.append(f"{path.relative_to(root.parent.parent.parent)}:{node.lineno}: missing readonly=True")
                continue
            if not (
                isinstance(fetch_fields, ast.Call)
                and isinstance(fetch_fields.func, ast.Name)
                and fetch_fields.func.id == "StartupFetch"
                and len(fetch_fields.args) == 1
                and isinstance(fetch_fields.args[0], ast.Constant)
                and fetch_fields.args[0].value == 0
            ):
                violations.append(
                    f"{path.relative_to(root.parent.parent.parent)}:{node.lineno}: "
                    "missing fetchFields=StartupFetch(0)"
                )

    assert not violations, "IBKR connect must not fetch account/order/position startup state:\n" + "\n".join(
        violations
    )
