"""Optional IBKR SPXW position polling.

This module is intentionally isolated from the market-data-only collectors.
It uses a separate API client id and StartupFetch.POSITIONS so stream/collector
sessions stay read-only and position-free.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from spx_spark.config import IbkrPositionSettings, IbkrSettings, StorageSettings
from spx_spark.ibkr.verifier import prepare_ib_client
from spx_spark.marketdata import InstrumentId, normalize_option_right
from spx_spark.storage import LatestStateStore


def default_positions_path(storage_settings: StorageSettings) -> str:
    return str(Path(storage_settings.data_root) / "latest" / "ibkr_positions.json")


def position_state_path() -> str:
    import os

    data_root = os.getenv("MARKET_DATA_DATA_ROOT") or os.getenv("MAINTENANCE_DATA_ROOT") or "data"
    return os.getenv("IBKR_POSITIONS_STATE_PATH", f"{data_root.rstrip('/')}/latest/ibkr_position_state.json")


@dataclass(frozen=True)
class SpxwPosition:
    account: str
    symbol: str
    expiry: str
    strike: float
    right: str
    qty: float
    avg_cost: float
    con_id: int
    trading_class: str | None
    local_symbol: str | None
    canonical_id: str
    market_price: float | None = None
    unrealized_pnl: float | None = None
    unrealized_pnl_pct: float | None = None
    distance_from_spx_points: float | None = None

    @property
    def label(self) -> str:
        return f"SPXW {self.expiry} {self.strike:g}{self.right}"

    @property
    def position_key(self) -> str:
        return f"{self.account}|{self.canonical_id}"


@dataclass(frozen=True)
class PositionSnapshot:
    fetched_at: str
    account_count: int
    positions: tuple[SpxwPosition, ...]
    spx_reference_price: float | None
    spx_reference_source: str | None

    @property
    def total_contracts(self) -> int:
        return sum(1 for item in self.positions if item.qty != 0)


def is_spxw_contract(contract: Any) -> bool:
    symbol = str(getattr(contract, "symbol", "")).upper()
    sec_type = str(getattr(contract, "secType", "")).upper()
    if symbol != "SPX" or sec_type not in {"OPT", "FOP"}:
        return False
    trading_class = str(getattr(contract, "tradingClass", "") or "").upper()
    local_symbol = str(getattr(contract, "localSymbol", "") or "").upper()
    return trading_class == "SPXW" or local_symbol.startswith("SPXW")


def normalize_expiry(raw: Any) -> str:
    value = str(raw or "").strip()
    if len(value) == 8 and value.isdigit():
        return value
    if len(value) >= 8 and value[:8].isdigit():
        return value[:8]
    return value


def build_canonical_id(expiry: str, strike: float, right: str) -> str:
    normalized_right = normalize_option_right(right).value
    return InstrumentId.option(
        "SPX",
        expiry=expiry,
        strike=strike,
        right=normalized_right,
        trading_class="SPXW",
        provider_symbol=f"option:SPX:SPXW:{expiry}:{strike:g}:{normalized_right}",
    ).canonical_id


def spx_reference_from_state(store: LatestStateStore) -> tuple[float | None, str | None]:
    state = store.load()
    for instrument_id, source in (
        ("index:SPX", "index:SPX"),
        ("future:ES", "future:ES"),
        ("equity:SPY", "equity:SPY"),
    ):
        quote = state.best_quote(instrument_id)
        if quote is None:
            continue
        price = quote.effective_price
        if price is None:
            continue
        if instrument_id == "equity:SPY":
            return price * 10.0, "SPY*10"
        return price, source
    return None, None


def option_market_price(store: LatestStateStore, canonical_id: str) -> float | None:
    quote = store.load().best_quote(canonical_id)
    if quote is None:
        return None
    return quote.effective_price


def position_from_ib(
    position: Any,
    *,
    store: LatestStateStore,
    spx_price: float | None,
) -> SpxwPosition | None:
    contract = position.contract
    if not is_spxw_contract(contract):
        return None
    expiry = normalize_expiry(getattr(contract, "lastTradeDateOrContractMonth", ""))
    strike = float(getattr(contract, "strike", 0.0) or 0.0)
    right = str(getattr(contract, "right", "") or "").upper()
    qty = float(position.position)
    avg_cost = float(position.avgCost)
    canonical_id = build_canonical_id(expiry, strike, right)
    market_price = option_market_price(store, canonical_id)
    unrealized_pnl = None
    unrealized_pnl_pct = None
    if market_price is not None and qty != 0:
        multiplier = 100.0
        if qty > 0:
            market_value = market_price * qty * multiplier
            unrealized_pnl = market_value - avg_cost
        else:
            liability = market_price * abs(qty) * multiplier
            unrealized_pnl = avg_cost - liability
        if avg_cost:
            unrealized_pnl_pct = (unrealized_pnl / abs(avg_cost)) * 100.0
    distance = None
    if spx_price is not None:
        distance = spx_price - strike
    return SpxwPosition(
        account=str(position.account),
        symbol=str(getattr(contract, "symbol", "SPX")),
        expiry=expiry,
        strike=strike,
        right=right,
        qty=qty,
        avg_cost=avg_cost,
        con_id=int(getattr(contract, "conId", 0) or 0),
        trading_class=getattr(contract, "tradingClass", None),
        local_symbol=getattr(contract, "localSymbol", None),
        canonical_id=canonical_id,
        market_price=market_price,
        unrealized_pnl=unrealized_pnl,
        unrealized_pnl_pct=unrealized_pnl_pct,
        distance_from_spx_points=distance,
    )


def fetch_positions(
    ib: Any,
    *,
    storage_settings: StorageSettings,
) -> PositionSnapshot:
    from ib_async.ib import StartupFetch

    store = LatestStateStore(storage_settings)
    spx_price, spx_source = spx_reference_from_state(store)
    rows = [
        item
        for item in (
            position_from_ib(position, store=store, spx_price=spx_price)
            for position in ib.positions()
        )
        if item is not None and item.qty != 0
    ]
    accounts = {item.account for item in rows}
    fetched_at = datetime.now(tz=timezone.utc).isoformat()
    return PositionSnapshot(
        fetched_at=fetched_at,
        account_count=len(accounts),
        positions=tuple(sorted(rows, key=lambda item: (item.expiry, item.strike, item.right, item.qty))),
        spx_reference_price=spx_price,
        spx_reference_source=spx_source,
    )


def connect_positions_client(ib: Any, settings: IbkrSettings, position_settings: IbkrPositionSettings) -> None:
    from ib_async.ib import StartupFetch

    prepare_ib_client(ib, request_timeout_seconds=settings.request_timeout_seconds)
    ib.connect(
        settings.host,
        settings.port,
        clientId=position_settings.client_id,
        readonly=True,
        fetchFields=StartupFetch.POSITIONS,
    )


def write_snapshot(snapshot: PositionSnapshot, path: str) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = output_path.with_suffix(f"{output_path.suffix}.tmp")
    temp_path.write_text(json.dumps(asdict(snapshot), indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(output_path)
    return output_path


def load_snapshot(path: str) -> PositionSnapshot | None:
    snapshot_path = Path(path)
    if not snapshot_path.exists():
        return None
    try:
        raw = json.loads(snapshot_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    positions = tuple(SpxwPosition(**item) for item in raw.get("positions", []))
    return PositionSnapshot(
        fetched_at=str(raw.get("fetched_at") or ""),
        account_count=int(raw.get("account_count") or 0),
        positions=positions,
        spx_reference_price=raw.get("spx_reference_price"),
        spx_reference_source=raw.get("spx_reference_source"),
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Poll IBKR SPXW positions and write a snapshot.")
    parser.add_argument("--json", action="store_true", help="Print snapshot JSON to stdout.")
    parser.add_argument("--print-config", action="store_true", help="Print settings and exit.")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    ibkr_settings = IbkrSettings.from_env()
    position_settings = IbkrPositionSettings.from_env()
    storage_settings = StorageSettings.from_env()

    if args.print_config:
        print(
            json.dumps(
                {
                    "ibkr": asdict(ibkr_settings),
                    "positions": asdict(position_settings),
                    "storage": asdict(storage_settings),
                },
                indent=2,
                sort_keys=True,
                default=str,
            )
        )
        return 0

    if not position_settings.enabled:
        print("IBKR positions polling disabled (IBKR_POSITIONS_ENABLED=false).", file=sys.stderr)
        return 0

    try:
        from ib_async import IB
    except ImportError as exc:
        print("Missing dependency: ib_async. Run `uv sync` first.", file=sys.stderr)
        raise SystemExit(2) from exc

    ib = IB()
    try:
        connect_positions_client(ib, ibkr_settings, position_settings)
        snapshot = fetch_positions(ib, storage_settings=storage_settings)
    except Exception as exc:  # noqa: BLE001
        print(f"Failed to fetch IBKR positions: {exc}", file=sys.stderr)
        return 1
    finally:
        if ib.isConnected():
            ib.disconnect()

    output_path = position_settings.snapshot_path or default_positions_path(storage_settings)
    write_snapshot(snapshot, output_path)
    if args.json:
        print(json.dumps(asdict(snapshot), indent=2, sort_keys=True))
    else:
        print(f"SPXW positions: {snapshot.total_contracts}")
        for item in snapshot.positions:
            pnl = "-" if item.unrealized_pnl_pct is None else f"{item.unrealized_pnl_pct:.1f}%"
            print(f"- {item.label} qty={item.qty:g} pnl={pnl}")
        print(f"Wrote snapshot: {output_path}")
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
