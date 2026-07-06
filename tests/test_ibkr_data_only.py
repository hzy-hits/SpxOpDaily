from __future__ import annotations

import ast
import asyncio
from pathlib import Path

from spx_spark.config import IbkrSettings
from spx_spark.ibkr.verifier import (
    VerifyRow,
    connect_market_data_only,
    generic_ticks_for_contract,
    option_open_interest_from_ticker,
    qualify_and_subscribe,
    resolve_contract_for_market_data,
    snapshot_rows,
)


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
    position_only_files = {"position_watcher.py"}

    for path in iter_python_files(root):
        if path.name in position_only_files:
            continue
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
        slow_index_stale_after_seconds=300.0,
        slow_index_labels=frozenset(),
        qualify_contracts=False,
        request_timeout_seconds=30.0,
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
        conId = 416904

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


def test_ibkr_subscribe_qualifies_on_missing_conid_hash_error() -> None:
    class Contract:
        symbol = "SPX"
        exchange = "CBOE"

    class FakeIB:
        qualify_called = False
        req_count = 0

        def qualifyContracts(self, contract):
            self.qualify_called = True
            contract.conId = 416904
            return [contract]

        def reqMktData(self, contract, generic_tick_list, snapshot, regulatory_snapshot):
            self.req_count += 1
            if self.req_count == 1:
                raise ValueError("can't be hashed because no 'conId' value exists")
            return object()

    ib = FakeIB()

    rows = qualify_and_subscribe(
        ib,
        [("index:SPX", "index", Contract())],
        qualify=False,
    )

    assert ib.qualify_called is True
    assert ib.req_count == 2
    ticker, row = rows["index:SPX"]
    assert ticker is not None
    assert row.subscribed is True
    assert row.qualified is True
    assert row.error is None


def test_option_subscriptions_request_open_interest_generic_ticks() -> None:
    class OptionContract:
        secType = "OPT"
        symbol = "SPX"
        conId = 12345

    class IndexContract:
        secType = "IND"
        symbol = "SPX"
        conId = 416904

    assert generic_ticks_for_contract(OptionContract()) == "100,101"
    assert generic_ticks_for_contract(IndexContract()) == ""

    seen: list[str] = []

    class FakeIB:
        def reqMktData(self, contract, generic_tick_list, snapshot, regulatory_snapshot):
            seen.append(generic_tick_list)
            return object()

    qualify_and_subscribe(
        FakeIB(),
        [
            ("option:SPXW:20260706:7500:C", "option", OptionContract()),
            ("index:SPX", "index", IndexContract()),
        ],
        qualify=False,
    )
    assert seen == ["100,101", ""]


def test_snapshot_rows_collects_option_open_interest_by_right() -> None:
    from types import SimpleNamespace

    call_ticker = SimpleNamespace(
        contract=SimpleNamespace(right="C"),
        marketDataType=1,
        bid=10.0,
        ask=10.5,
        last=10.2,
        close=9.8,
        bidSize=1,
        askSize=2,
        lastSize=1,
        volume=1500.0,
        callOpenInterest=4321.0,
        putOpenInterest=float("nan"),
        time=None,
        modelGreeks=None,
        marketPrice=lambda: 10.25,
    )
    put_ticker = SimpleNamespace(
        contract=SimpleNamespace(right="P"),
        marketDataType=1,
        bid=8.0,
        ask=8.5,
        last=8.2,
        close=8.1,
        bidSize=1,
        askSize=2,
        lastSize=1,
        volume=900.0,
        callOpenInterest=float("nan"),
        putOpenInterest=1234.0,
        time=None,
        modelGreeks=None,
        marketPrice=lambda: 8.25,
    )
    subscriptions = {
        "option:SPXW:20260706:7500:C": (
            call_ticker,
            VerifyRow(label="option:SPXW:20260706:7500:C", kind="option", symbol="SPX"),
        ),
        "option:SPXW:20260706:7500:P": (
            put_ticker,
            VerifyRow(label="option:SPXW:20260706:7500:P", kind="option", symbol="SPX"),
        ),
    }

    rows = snapshot_rows(subscriptions, 10.0)
    by_label = {row.label: row for row in rows}

    assert by_label["option:SPXW:20260706:7500:C"].open_interest == 4321.0
    assert by_label["option:SPXW:20260706:7500:P"].open_interest == 1234.0
    assert by_label["option:SPXW:20260706:7500:C"].volume == 1500.0


def test_option_open_interest_ignores_missing_right() -> None:
    from types import SimpleNamespace

    ticker = SimpleNamespace(contract=SimpleNamespace(right=""), callOpenInterest=5.0, putOpenInterest=6.0)
    assert option_open_interest_from_ticker(ticker) is None


def test_resolve_contract_uses_known_index_conid_when_qualify_fails() -> None:
    class Contract:
        secType = "IND"
        symbol = "SPX"
        exchange = "CBOE"
        conId = 0

    class FakeIB:
        def qualifyContracts(self, contract):
            raise TimeoutError("sec-def farm unavailable")

    row = VerifyRow(label="index:SPX", kind="index", symbol="SPX", exchange="CBOE")
    resolved = resolve_contract_for_market_data(FakeIB(), Contract(), row)

    assert resolved is not None
    assert resolved.conId == 416904
    assert row.qualified is True
    assert row.error is None
