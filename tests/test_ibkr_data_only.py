from __future__ import annotations

import ast
import asyncio
from pathlib import Path

from spx_spark.config import IbkrSettings
from spx_spark.ibkr.verifier import connect_market_data_only, qualify_and_subscribe


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


def test_ibkr_connect_overrides_library_startup_positions_fetch() -> None:
    class FakeIB:
        def __init__(self) -> None:
            self.connect_kwargs = {}

        async def reqPositionsAsync(self) -> list[str]:
            return ["should not be called"]

        def connect(self, *args, **kwargs) -> None:
            self.connect_args = args
            self.connect_kwargs = kwargs

    settings = IbkrSettings(
        host="127.0.0.1",
        port=4001,
        client_id=171,
        market_data_type=1,
        es_expiry="202609",
        mes_expiry="202609",
        verify_indexes=[],
        verify_stocks=[],
        verify_futures=[],
        option_expiry="20260706",
        option_strike_window_points=50,
        option_strike_step=5,
        max_option_lines=40,
        quote_wait_seconds=1.0,
        stale_after_seconds=10.0,
        qualify_contracts=False,
    )
    ib = FakeIB()

    connect_market_data_only(ib, settings)

    assert asyncio.run(ib.reqPositionsAsync()) == []
    assert ib.connect_args == ("127.0.0.1", 4001)
    assert ib.connect_kwargs["readonly"] is True
    assert ib.connect_kwargs["fetchFields"].value == 0


def test_ibkr_subscribe_can_skip_contract_qualification() -> None:
    class Contract:
        symbol = "SPX"
        exchange = "CBOE"

    class FakeIB:
        qualify_called = False

        def qualifyContracts(self, contract):
            self.qualify_called = True
            return [contract]

        def reqMktData(self, contract, generic_tick_list, snapshot, regulatory_snapshot):
            return object()

    ib = FakeIB()

    rows = qualify_and_subscribe(
        ib,
        [("index:SPX", "index", Contract())],
        qualify=False,
    )

    assert ib.qualify_called is False
    ticker, row = rows["index:SPX"]
    assert ticker is not None
    assert row.subscribed is True
    assert row.qualified is False
