"""Optional IBKR SPXW position polling.

This module is intentionally isolated from the market-data-only collectors.
It uses a separate API client id and StartupFetch.POSITIONS so stream/collector
sessions stay read-only and position-free.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from spx_spark.config import IbkrPositionSettings, IbkrSettings, StorageSettings
from spx_spark.ibkr.verifier import prepare_ib_client
from spx_spark.marketdata import (
    InstrumentId,
    clean_float,
    normalize_option_right,
)
from spx_spark.state_io import atomic_write_json_secure
from spx_spark.storage import (
    LatestState,
    LatestStateStore,
    configured_quote_use_decision,
)


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
    multiplier: float = 100.0
    market_price: float | None = None
    mark_source: str | None = None
    mark_quality: str | None = None
    mark_freshness: str | None = None
    source_quote_time: str | None = None
    last_observed_update_time: str | None = None
    mark_age_seconds: float | None = None
    mark_pricing_allowed: bool = True
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
    book_unrealized_pnl: float | None = None
    book_cost_basis: float | None = None
    book_unrealized_pnl_pct: float | None = None
    schema_version: int = 2
    snapshot_id: str = ""
    fetch_complete: bool = True
    managed_account_count: int = 0
    raw_position_count: int = 0
    filtered_spxw_count: int = 0
    priced_leg_count: int = 0
    total_leg_count: int = 0
    book_pnl_complete: bool = True

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
    return spx_reference_from_latest_state(store.load())


def spx_reference_from_latest_state(state: LatestState) -> tuple[float | None, str | None]:
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


def resolved_contract_multiplier(contract: Any) -> float | None:
    multiplier = clean_float(getattr(contract, "multiplier", None))
    if multiplier is not None and multiplier > 0:
        return multiplier
    if is_spxw_contract(contract):
        return 100.0
    return None


def position_unrealized_metrics(
    *,
    qty: float,
    avg_cost: float,
    mark: float,
    multiplier: float,
) -> tuple[float, float | None]:
    unrealized_pnl = qty * (mark * multiplier - avg_cost)
    cost_basis = abs(qty * avg_cost)
    unrealized_pnl_pct = unrealized_pnl / cost_basis * 100.0 if cost_basis else None
    return unrealized_pnl, unrealized_pnl_pct


def position_from_ib(
    position: Any,
    *,
    store: LatestStateStore | None = None,
    latest_state: LatestState | None = None,
    quote_settings: StorageSettings | None = None,
    spx_price: float | None,
    as_of: datetime | None = None,
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
    as_of = as_of or datetime.now(tz=timezone.utc)
    if latest_state is None:
        if store is None:
            raise ValueError("store or latest_state is required")
        latest_state = store.load(now=as_of)
    quote = latest_state.best_quote(canonical_id)
    market_price = quote.effective_price if quote is not None else None
    multiplier = resolved_contract_multiplier(contract)
    mark_source = quote.provider.value if quote is not None else None
    mark_quality = quote.quality.value if quote is not None else None
    mark_freshness = None
    source_quote_time = quote.quote_time.isoformat() if quote and quote.quote_time else None
    last_observed_update_time = (
        quote.last_update_at.isoformat() if quote and quote.last_update_at else None
    )
    mark_age_seconds = None
    mark_pricing_allowed = False
    if quote is not None:
        decision = configured_quote_use_decision(
            quote,
            as_of=as_of,
            settings=quote_settings or (store.settings if store is not None else None),
        )
        mark_freshness = decision.freshness.value
        mark_pricing_allowed = decision.pricing_allowed
        observed_at = quote.last_update_at or quote.quote_time or quote.trade_time
        if observed_at is not None:
            mark_age_seconds = max((as_of - observed_at).total_seconds(), 0.0)
    unrealized_pnl = None
    unrealized_pnl_pct = None
    if market_price is not None and multiplier is not None and qty != 0:
        unrealized_pnl, unrealized_pnl_pct = position_unrealized_metrics(
            qty=qty,
            avg_cost=avg_cost,
            mark=market_price,
            multiplier=multiplier,
        )
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
        multiplier=multiplier or 100.0,
        market_price=market_price,
        mark_source=mark_source,
        mark_quality=mark_quality,
        mark_freshness=mark_freshness,
        source_quote_time=source_quote_time,
        last_observed_update_time=last_observed_update_time,
        mark_age_seconds=mark_age_seconds,
        mark_pricing_allowed=mark_pricing_allowed,
        unrealized_pnl=unrealized_pnl,
        unrealized_pnl_pct=unrealized_pnl_pct,
        distance_from_spx_points=distance,
    )


def snapshot_book_metrics(
    positions: tuple[SpxwPosition, ...],
) -> tuple[float | None, float | None, float | None]:
    priced_positions = [
        item
        for item in positions
        if item.unrealized_pnl is not None and item.mark_pricing_allowed
    ]
    pnls = [item.unrealized_pnl for item in priced_positions]
    if not pnls:
        return None, None, None
    book_pnl = sum(pnls)
    book_cost = sum(abs(item.qty * item.avg_cost) for item in priced_positions)
    book_pnl_pct = (book_pnl / book_cost * 100.0) if book_cost else None
    return book_pnl, book_cost, book_pnl_pct


def build_snapshot_id(
    *,
    fetched_at: str,
    fetch_complete: bool,
    positions: tuple[SpxwPosition, ...],
) -> str:
    payload = {
        "fetch_complete": fetch_complete,
        "fetched_at": fetched_at,
        "positions": [asdict(position) for position in positions],
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def managed_account_ids(ib: Any, raw_positions: tuple[Any, ...]) -> tuple[str, ...]:
    try:
        accounts = tuple(str(account) for account in ib.managedAccounts() if account)
    except (AttributeError, TypeError):
        accounts = ()
    if accounts:
        return tuple(sorted(set(accounts)))
    return tuple(
        sorted(
            {
                str(getattr(position, "account", ""))
                for position in raw_positions
                if getattr(position, "account", "")
            }
        )
    )


def fetch_positions(
    ib: Any,
    *,
    storage_settings: StorageSettings,
) -> PositionSnapshot:

    as_of = datetime.now(tz=timezone.utc)
    store = LatestStateStore(storage_settings)
    latest_state = store.load(now=as_of)
    spx_price, spx_source = spx_reference_from_latest_state(latest_state)
    raw_positions = tuple(ib.positions())
    rows = [
        item
        for item in (
            position_from_ib(
                position,
                latest_state=latest_state,
                quote_settings=storage_settings,
                spx_price=spx_price,
                as_of=as_of,
            )
            for position in raw_positions
        )
        if item is not None and item.qty != 0
    ]
    positions = tuple(sorted(rows, key=lambda item: (item.expiry, item.strike, item.right, item.qty)))
    managed_accounts = managed_account_ids(ib, raw_positions)
    fetched_at = as_of.isoformat()
    book_pnl, book_cost, book_pnl_pct = snapshot_book_metrics(positions)
    priced_leg_count = sum(
        1
        for item in positions
        if item.mark_pricing_allowed and item.unrealized_pnl is not None
    )
    total_leg_count = len(positions)
    return PositionSnapshot(
        fetched_at=fetched_at,
        account_count=len(managed_accounts),
        positions=positions,
        spx_reference_price=spx_price,
        spx_reference_source=spx_source,
        book_unrealized_pnl=book_pnl,
        book_cost_basis=book_cost,
        book_unrealized_pnl_pct=book_pnl_pct,
        snapshot_id=build_snapshot_id(
            fetched_at=fetched_at,
            fetch_complete=True,
            positions=positions,
        ),
        fetch_complete=True,
        managed_account_count=len(managed_accounts),
        raw_position_count=len(raw_positions),
        filtered_spxw_count=len(positions),
        priced_leg_count=priced_leg_count,
        total_leg_count=total_leg_count,
        book_pnl_complete=priced_leg_count == total_leg_count,
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


def connect_broker_readonly_with_positions(
    ib: Any,
    settings: IbkrSettings,
    *,
    client_id: int,
) -> None:
    """Open one read-only broker socket with a complete startup position snapshot."""

    from ib_async.ib import StartupFetch

    ib.connect(
        settings.host,
        settings.port,
        clientId=client_id,
        readonly=True,
        fetchFields=StartupFetch.POSITIONS,
    )


def write_snapshot(snapshot: PositionSnapshot, path: str) -> Path:
    output_path = Path(path)
    atomic_write_json_secure(output_path, asdict(snapshot))
    return output_path


def load_snapshot(path: str) -> PositionSnapshot | None:
    snapshot_path = Path(path)
    if not snapshot_path.exists():
        return None
    try:
        raw = json.loads(snapshot_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    try:
        schema_version = int(raw.get("schema_version") or 1)
    except (TypeError, ValueError):
        return None
    if schema_version not in {1, 2}:
        return None
    raw_positions_payload = raw.get("positions", [])
    if not isinstance(raw_positions_payload, list):
        return None
    positions_payload = []
    for item in raw_positions_payload:
        if not isinstance(item, dict):
            continue
        normalized = dict(item)
        if "mark_pricing_allowed" not in normalized:
            normalized["mark_pricing_allowed"] = False
        positions_payload.append(normalized)
    try:
        positions = tuple(SpxwPosition(**item) for item in positions_payload)
    except (TypeError, ValueError):
        return None
    book_pnl = raw.get("book_unrealized_pnl")
    book_cost = raw.get("book_cost_basis")
    book_pnl_pct = raw.get("book_unrealized_pnl_pct")
    if book_pnl is None and positions:
        book_pnl, book_cost, book_pnl_pct = snapshot_book_metrics(positions)
    fetched_at = str(raw.get("fetched_at") or "")
    fetch_complete = bool(raw.get("fetch_complete", False))
    priced_leg_count = _safe_nonnegative_int(
        raw.get("priced_leg_count")
        if raw.get("priced_leg_count") is not None
        else sum(
            1
            for item in positions
            if item.mark_pricing_allowed and item.unrealized_pnl is not None
        ),
        default=0,
    )
    total_leg_count = _safe_nonnegative_int(
        raw.get("total_leg_count"),
        default=len(positions),
    )
    return PositionSnapshot(
        fetched_at=fetched_at,
        account_count=_safe_nonnegative_int(raw.get("account_count"), default=0),
        positions=positions,
        spx_reference_price=raw.get("spx_reference_price"),
        spx_reference_source=raw.get("spx_reference_source"),
        book_unrealized_pnl=float(book_pnl) if isinstance(book_pnl, int | float) else book_pnl,
        book_cost_basis=float(book_cost) if isinstance(book_cost, int | float) else book_cost,
        book_unrealized_pnl_pct=float(book_pnl_pct) if isinstance(book_pnl_pct, int | float) else book_pnl_pct,
        schema_version=schema_version,
        snapshot_id=str(raw.get("snapshot_id") or build_snapshot_id(
            fetched_at=fetched_at,
            fetch_complete=fetch_complete,
            positions=positions,
        )),
        fetch_complete=fetch_complete,
        managed_account_count=_safe_nonnegative_int(
            raw.get("managed_account_count"),
            default=_safe_nonnegative_int(raw.get("account_count"), default=0),
        ),
        raw_position_count=_safe_nonnegative_int(
            raw.get("raw_position_count"),
            default=len(positions),
        ),
        filtered_spxw_count=_safe_nonnegative_int(
            raw.get("filtered_spxw_count"),
            default=len(positions),
        ),
        priced_leg_count=priced_leg_count,
        total_leg_count=total_leg_count,
        book_pnl_complete=bool(
            raw.get("book_pnl_complete")
            if raw.get("book_pnl_complete") is not None
            else priced_leg_count == total_leg_count
        ),
    )


def _safe_nonnegative_int(value: object, *, default: int) -> int:
    if value is None:
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed >= 0 else default


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
    output_path = position_settings.snapshot_path or default_positions_path(storage_settings)

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
        print(
            "IBKR account reads disabled (IBKR_BROKER_ACCOUNT_READ_ENABLED=false).",
            file=sys.stderr,
        )
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
        failed_at = datetime.now(tz=timezone.utc).isoformat()
        incomplete_snapshot = PositionSnapshot(
            fetched_at=failed_at,
            account_count=0,
            positions=(),
            spx_reference_price=None,
            spx_reference_source=None,
            snapshot_id=build_snapshot_id(
                fetched_at=failed_at,
                fetch_complete=False,
                positions=(),
            ),
            fetch_complete=False,
            book_pnl_complete=False,
        )
        try:
            write_snapshot(incomplete_snapshot, output_path)
        except OSError as write_exc:
            print(f"Failed to write incomplete position snapshot: {write_exc}", file=sys.stderr)
        return 1
    finally:
        if ib.isConnected():
            ib.disconnect()

    write_snapshot(snapshot, output_path)
    if args.json:
        print(json.dumps(asdict(snapshot), indent=2, sort_keys=True))
    else:
        print(f"SPXW positions: {snapshot.total_contracts}")
        if snapshot.book_unrealized_pnl is not None:
            pct = (
                f" ({snapshot.book_unrealized_pnl_pct:+.1f}%)"
                if snapshot.book_unrealized_pnl_pct is not None
                else ""
            )
            print(f"Book unrealized: ${snapshot.book_unrealized_pnl:+,.0f}{pct}")
        for item in snapshot.positions:
            pnl = "-" if item.unrealized_pnl_pct is None else f"{item.unrealized_pnl_pct:+.1f}%"
            print(f"- {item.label} qty={item.qty:g} pnl={pnl}")
        print(f"Wrote snapshot: {output_path}")
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
