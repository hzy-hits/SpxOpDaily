from __future__ import annotations

import argparse
import json
import math
import sys
from copy import copy
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from spx_spark.config import IbkrSettings, NY_TZ
from spx_spark.ibkr.adapter import quote_from_ibkr_row


DEFAULT_INDEX_EXCHANGES: dict[str, str] = {
    "SPX": "CBOE",
    "VIX": "CBOE",
    "VIX1D": "CBOE",
    "VIX9D": "CBOE",
    "VIX3M": "CBOE",
    "VVIX": "CBOE",
    "SKEW": "CBOE",
    "NDX": "NASDAQ",
    "RUT": "RUSSELL",
    "DJX": "CBOE",
    "DJU": "CBOE",
}

# Stable IB conIds for cash indexes. Used only when qualifyContracts times out
# but Gateway still accepts streaming subscriptions for the resolved symbol.
KNOWN_INDEX_CONIDS: dict[str, int] = {
    "SPX": 416904,
    "VIX": 13455763,
    "VIX1D": 627990891,
    "VIX9D": 322592334,
    "VIX3M": 47511905,
    "VVIX": 105068053,
    "SKEW": 84597750,
    "NDX": 416843,
}


def clean_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(result) or math.isinf(result):
        return None
    return result


def money(value: float | None) -> str:
    if value is None:
        return "-"
    if abs(value) >= 100:
        return f"{value:.2f}"
    return f"{value:.4f}"


@dataclass
class VerifyRow:
    label: str
    kind: str
    symbol: str
    exchange: str | None = None
    qualified: bool = False
    subscribed: bool = False
    market_data_type: int | None = None
    bid: float | None = None
    ask: float | None = None
    last: float | None = None
    close: float | None = None
    market_price: float | None = None
    bid_size: float | None = None
    ask_size: float | None = None
    last_size: float | None = None
    volume: float | None = None
    open_interest: float | None = None
    model_iv: float | None = None
    delta: float | None = None
    gamma: float | None = None
    theta: float | None = None
    vega: float | None = None
    und_price: float | None = None
    ticker_time: str | None = None
    last_update_at: str | None = None
    request_id: int | None = None
    stale: bool | None = None
    error: str | None = None

    @property
    def live_state(self) -> str:
        mapping = {
            1: "live",
            2: "frozen",
            3: "delayed",
            4: "delayed-frozen",
        }
        if self.market_data_type is None:
            return "unknown"
        return mapping.get(self.market_data_type, str(self.market_data_type))


@dataclass
class IbkrError:
    req_id: int
    error_code: int
    message: str
    contract: str | None
    ts: str


def parse_index_spec(spec: str) -> tuple[str, str]:
    raw = spec.strip()
    if not raw:
        raise ValueError("empty index spec")

    if "@" in raw:
        symbol, exchange = raw.split("@", 1)
    elif ":" in raw:
        symbol, exchange = raw.split(":", 1)
    else:
        symbol, exchange = raw, DEFAULT_INDEX_EXCHANGES.get(raw.upper(), "CBOE")

    symbol = symbol.strip().upper()
    exchange = exchange.strip().upper()
    if not symbol or not exchange:
        raise ValueError(f"invalid index spec: {spec!r}")
    return symbol, exchange


def build_base_contracts(settings: IbkrSettings) -> list[tuple[str, str, Any]]:
    from ib_async import CFD, Future, Index, Stock

    contracts: list[tuple[str, str, Any]] = []

    for spec in settings.verify_indexes:
        symbol, exchange = parse_index_spec(spec)
        contracts.append((f"index:{symbol}", "index", Index(symbol, exchange, "USD")))

    for symbol in settings.verify_stocks:
        contracts.append((f"stock:{symbol}", "stock", Stock(symbol, "SMART", "USD")))

    for symbol in settings.verify_futures:
        expiry = settings.mes_expiry if symbol == "MES" else settings.es_expiry
        contracts.append((f"future:{symbol}", "future", Future(symbol, expiry, "CME", currency="USD")))

    for symbol in settings.verify_cfds:
        contracts.append((f"cfd:{symbol}", "cfd", CFD(symbol, "SMART", "USD")))

    return contracts


def build_spxw_option_contracts(
    settings: IbkrSettings,
    atm_reference: float,
) -> list[tuple[str, str, Any]]:
    from ib_async import Option

    step = settings.option_strike_step
    atm_strike = round(atm_reference / step) * step
    strikes: list[int] = []
    for strike in range(
        int(atm_strike - settings.option_strike_window_points),
        int(atm_strike + settings.option_strike_window_points) + step,
        step,
    ):
        if strike > 0:
            strikes.append(strike)

    contracts: list[tuple[str, str, Any]] = []
    for strike in strikes:
        for right in ("C", "P"):
            label = f"option:SPXW:{settings.option_expiry}:{strike}:{right}"
            contract = Option(
                "SPX",
                settings.option_expiry,
                float(strike),
                right,
                "SMART",
                multiplier="100",
                currency="USD",
                tradingClass="SPXW",
            )
            contracts.append((label, "option", contract))

    return contracts[: settings.max_option_lines]


def prepare_ib_client(ib: Any, *, request_timeout_seconds: float) -> None:
    """Bound blocking API calls without installing a Jupyter nested loop."""

    ib.RequestTimeout = request_timeout_seconds


def contract_has_con_id(contract: Any) -> bool:
    con_id = getattr(contract, "conId", 0)
    return bool(con_id)


def apply_known_index_conid(contract: Any) -> Any | None:
    sec_type = getattr(contract, "secType", "") or "IND"
    if sec_type != "IND":
        return None
    symbol = str(getattr(contract, "symbol", "")).upper()
    con_id = KNOWN_INDEX_CONIDS.get(symbol)
    if con_id is None:
        return None
    fallback = copy(contract)
    fallback.conId = con_id
    return fallback


def resolve_contract_for_market_data(ib: Any, contract: Any, row: VerifyRow) -> Any | None:
    if contract_has_con_id(contract):
        return contract

    try:
        qualified = ib.qualifyContracts(contract)
        if qualified:
            resolved = qualified[0]
            row.exchange = getattr(resolved, "exchange", row.exchange)
            row.qualified = True
            return resolved
        row.error = "qualify returned no contracts"
    except Exception as exc:  # noqa: BLE001
        row.error = f"qualify failed: {exc}"

    fallback = apply_known_index_conid(contract)
    if fallback is not None:
        row.qualified = True
        row.error = None
        return fallback
    return None


def connect_market_data_only(ib: Any, settings: IbkrSettings) -> None:
    from ib_async.ib import StartupFetch

    disable_startup_positions_fetch(ib)
    ib.connect(
        settings.host,
        settings.port,
        clientId=settings.client_id,
        readonly=True,
        fetchFields=StartupFetch(0),
    )


def disable_startup_positions_fetch(ib: Any) -> None:
    async def no_startup_positions() -> list[Any]:
        return []

    ib.reqPositionsAsync = no_startup_positions


def qualify_and_subscribe(
    ib: Any,
    contracts: list[tuple[str, str, Any]],
    *,
    qualify: bool = False,
    on_progress: Any | None = None,
) -> dict[str, tuple[Any, VerifyRow]]:
    result: dict[str, tuple[Any, VerifyRow]] = {}
    for index, (label, kind, contract) in enumerate(contracts, start=1):
        row = VerifyRow(
            label=label,
            kind=kind,
            symbol=getattr(contract, "symbol", label),
            exchange=getattr(contract, "exchange", None),
        )
        if on_progress is not None:
            on_progress(label=label, index=index, total=len(contracts), phase="start")

        if qualify or not contract_has_con_id(contract):
            resolved = resolve_contract_for_market_data(ib, contract, row)
            if resolved is None:
                result[label] = (None, row)
                if on_progress is not None:
                    on_progress(label=label, index=index, total=len(contracts), phase="failed", error=row.error)
                continue
            contract = resolved
        elif qualify:
            row.qualified = True

        ticker = subscribe_contract(ib, contract, row, allow_qualify_fallback=not qualify)
        result[label] = (ticker, row)
        if on_progress is not None:
            on_progress(
                label=label,
                index=index,
                total=len(contracts),
                phase="subscribed" if row.subscribed else "failed",
                error=row.error,
            )
    return result


def subscribe_contract(
    ib: Any,
    contract: Any,
    row: VerifyRow,
    *,
    allow_qualify_fallback: bool,
) -> Any | None:
    if not contract_has_con_id(contract):
        resolved = resolve_contract_for_market_data(ib, contract, row)
        if resolved is None:
            return None
        contract = resolved

    try:
        ticker = ib.reqMktData(contract, generic_ticks_for_contract(contract), False, False)
        row.subscribed = True
        row.request_id = ticker_request_id(ib, ticker)
        return ticker
    except Exception as exc:  # noqa: BLE001
        if not allow_qualify_fallback or not needs_contract_qualification(exc):
            row.error = f"subscribe failed: {exc}"
            return None

        try:
            qualified = ib.qualifyContracts(contract)
            if not qualified:
                row.error = f"subscribe failed: {exc}; qualify fallback returned no contracts"
                return None
            contract = qualified[0]
            row.exchange = getattr(contract, "exchange", row.exchange)
            row.qualified = True
        except Exception as qualify_exc:  # noqa: BLE001
            row.error = f"subscribe failed: {exc}; qualify fallback failed: {qualify_exc}"
            return None

        try:
            ticker = ib.reqMktData(contract, generic_ticks_for_contract(contract), False, False)
            row.subscribed = True
            row.request_id = ticker_request_id(ib, ticker)
            return ticker
        except Exception as retry_exc:  # noqa: BLE001
            row.error = f"subscribe failed after qualify fallback: {retry_exc}"
            return None


def ticker_request_id(ib: Any, ticker: Any) -> int | None:
    wrapper = getattr(ib, "wrapper", None)
    ticker_to_req_id = getattr(wrapper, "ticker2ReqId", None)
    if ticker_to_req_id is None:
        return None
    try:
        market_data_ids = ticker_to_req_id.get("mktData", ticker_to_req_id)
        request_id = market_data_ids.get(ticker)
    except (AttributeError, TypeError):
        return None
    return int(request_id) if isinstance(request_id, int) else None


def needs_contract_qualification(exc: Exception) -> bool:
    message = str(exc).lower()
    return "conid" in message or "can't be hashed" in message or "cannot be hashed" in message


# Generic tick 100 = option volume, 101 = option open interest. Without 101
# IBKR never sends OI, which leaves GEX/call-put walls/zero-gamma unavailable
# (`gex_quality=no_open_interest_gex`).
OPTION_GENERIC_TICKS = "100,101"


def generic_ticks_for_contract(contract: Any) -> str:
    if str(getattr(contract, "secType", "")).upper() == "OPT":
        return OPTION_GENERIC_TICKS
    return ""


def option_open_interest_from_ticker(ticker: Any) -> float | None:
    right = str(getattr(getattr(ticker, "contract", None), "right", "") or "").upper()
    if right.startswith("C"):
        return clean_float(getattr(ticker, "callOpenInterest", None))
    if right.startswith("P"):
        return clean_float(getattr(ticker, "putOpenInterest", None))
    return None


def snapshot_rows(
    subscriptions: dict[str, tuple[Any, VerifyRow]],
    stale_after_seconds: float,
    *,
    slow_index_stale_after_seconds: float | None = None,
    slow_index_labels: frozenset[str] | None = None,
    now: datetime | None = None,
) -> list[VerifyRow]:
    now = now or datetime.now(tz=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    else:
        now = now.astimezone(timezone.utc)
    rows: list[VerifyRow] = []
    slow_labels = slow_index_labels or frozenset()
    for label, (ticker, row) in subscriptions.items():
        row_label = str(getattr(row, "label", "") or label)
        row_stale_after = (
            slow_index_stale_after_seconds
            if slow_index_stale_after_seconds is not None and row_label in slow_labels
            else stale_after_seconds
        )
        if ticker is None:
            rows.append(row)
            continue

        previous_fingerprint = normalized_row_fingerprint(row)

        row.market_data_type = getattr(ticker, "marketDataType", None)
        row.bid = clean_float(getattr(ticker, "bid", None))
        row.ask = clean_float(getattr(ticker, "ask", None))
        row.last = clean_float(getattr(ticker, "last", None))
        row.close = clean_float(getattr(ticker, "close", None))
        row.bid_size = clean_float(getattr(ticker, "bidSize", None))
        row.ask_size = clean_float(getattr(ticker, "askSize", None))
        row.last_size = clean_float(getattr(ticker, "lastSize", None))
        row.volume = clean_float(getattr(ticker, "volume", None))
        if row.kind == "option":
            row.open_interest = option_open_interest_from_ticker(ticker)

        try:
            row.market_price = clean_float(ticker.marketPrice())
        except Exception:  # noqa: BLE001
            row.market_price = first_present(row.last, midpoint(row.bid, row.ask), row.close)

        ticker_time = getattr(ticker, "time", None)
        if ticker_time is not None:
            if ticker_time.tzinfo is None:
                ticker_time = ticker_time.replace(tzinfo=timezone.utc)
            row.ticker_time = ticker_time.astimezone(timezone.utc).isoformat()
            row.stale = (now - ticker_time.astimezone(timezone.utc)).total_seconds() > row_stale_after

        greeks = getattr(ticker, "modelGreeks", None)
        if greeks is not None:
            row.model_iv = clean_float(getattr(greeks, "impliedVol", None))
            row.delta = clean_float(getattr(greeks, "delta", None))
            row.gamma = clean_float(getattr(greeks, "gamma", None))
            row.theta = clean_float(getattr(greeks, "theta", None))
            row.vega = clean_float(getattr(greeks, "vega", None))
            row.und_price = clean_float(getattr(greeks, "undPrice", None))

        current_fingerprint = normalized_row_fingerprint(row)
        if (
            row.last_update_at is None or current_fingerprint != previous_fingerprint
        ) and any(value is not None for value in current_fingerprint):
            row.last_update_at = now.isoformat()

        rows.append(row)
    return rows


def normalized_row_fingerprint(row: VerifyRow) -> tuple[object, ...]:
    """Fields whose advancement proves that the persistent ticker changed."""

    return (
        row.ticker_time,
        row.bid,
        row.ask,
        row.last,
        row.market_price,
        row.close,
        row.bid_size,
        row.ask_size,
        row.last_size,
        row.volume,
        row.open_interest,
        row.model_iv,
        row.delta,
        row.gamma,
        row.und_price,
    )


def first_present(*values: float | None) -> float | None:
    for value in values:
        if value is not None and value > 0:
            return value
    return None


def midpoint(bid: float | None, ask: float | None) -> float | None:
    if bid is not None and ask is not None and bid > 0 and ask > 0 and ask >= bid:
        return (bid + ask) / 2
    return None


def estimate_atm_reference(rows: list[VerifyRow]) -> tuple[float | None, str]:
    """Conservative stateless ATM reference for one-shot diagnostics.

    Persistent streaming uses AtmReferenceController. This compatibility
    helper intentionally excludes raw ES (which needs qualified RTH basis)
    and all stale fallbacks.
    """
    by_label = {row.label: row for row in rows}
    candidates = (
        ("index:SPX", 1.0, "SPX"),
        ("cfd:IBUS500", 1.0, "IBUS500"),
        ("stock:SPY", 10.0, "SPY*10"),
    )
    for label, multiplier, name in candidates:
        row = by_label.get(label)
        if row is None or row.stale is not False or row.market_data_type in {3, 4}:
            continue
        price = first_present(
            row.market_price, row.last, midpoint(row.bid, row.ask), row.close
        )
        if price:
            return price * multiplier, name

    return None, "none"


def cancel_subscriptions(ib: Any, subscriptions: dict[str, tuple[Any, VerifyRow]]) -> bool:
    success = True
    for ticker, row in subscriptions.values():
        # An asynchronous IBKR rejection means there is no live ticker to
        # cancel. Sending cancelMktData anyway produces a misleading code 300.
        if ticker is None:
            continue
        if not row.subscribed:
            # IBKR already rejected the server-side request. Remove only the
            # local ib_async ticker registration to avoid a bogus code 300.
            end_ticker = getattr(getattr(ib, "wrapper", None), "endTicker", None)
            if callable(end_ticker):
                try:
                    end_ticker(ticker, "mktData")
                except Exception:  # noqa: BLE001
                    success = False
            continue
        try:
            if ib.cancelMktData(ticker.contract) is False:
                success = False
        except Exception:  # noqa: BLE001
            success = False
    return success


def print_rows(rows: list[VerifyRow]) -> None:
    headers = [
        "label",
        "state",
        "bid",
        "ask",
        "last",
        "mkt",
        "iv",
        "delta",
        "gamma",
        "stale",
        "error",
    ]
    table: list[list[str]] = []
    for row in rows:
        table.append(
            [
                row.label,
                row.live_state,
                money(row.bid),
                money(row.ask),
                money(row.last),
                money(row.market_price),
                money(row.model_iv),
                money(row.delta),
                money(row.gamma),
                "-" if row.stale is None else str(row.stale).lower(),
                row.error or "",
            ]
        )

    widths = [
        max(len(headers[index]), *(len(line[index]) for line in table)) for index in range(len(headers))
    ]
    print(" | ".join(header.ljust(widths[index]) for index, header in enumerate(headers)))
    print("-+-".join("-" * width for width in widths))
    for line in table:
        print(" | ".join(value.ljust(widths[index]) for index, value in enumerate(line)))


def write_snapshot(settings: IbkrSettings, rows: list[VerifyRow], errors: list[IbkrError]) -> Path:
    output_dir = Path("logs")
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(tz=NY_TZ).strftime("%Y%m%d-%H%M%S")
    output_path = output_dir / f"ibkr-verifier-{timestamp}.json"
    payload = {
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
        "settings": asdict(settings),
        "rows": [asdict(row) | {"live_state": row.live_state} for row in rows],
        "normalized_quotes": [quote_from_ibkr_row(row).to_dict() for row in rows],
        "ibkr_errors": [asdict(error) for error in errors],
    }
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return output_path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify IBKR market data availability.")
    parser.add_argument(
        "--print-config",
        action="store_true",
        help="Print resolved verifier settings and exit without connecting to IBKR.",
    )
    parser.add_argument(
        "--skip-options",
        action="store_true",
        help="Verify indexes/stocks/futures only and skip SPXW option subscriptions.",
    )
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = IbkrSettings.from_env()
    if args.print_config:
        print(json.dumps(asdict(settings), indent=2, sort_keys=True))
        return 0

    try:
        from ib_async import IB
    except ImportError as exc:
        print("Missing dependency: ib_async. Run `uv sync` first.", file=sys.stderr)
        raise SystemExit(2) from exc

    ib = IB()
    prepare_ib_client(ib, request_timeout_seconds=settings.request_timeout_seconds)
    base_subs: dict[str, tuple[Any, VerifyRow]] = {}
    option_subs: dict[str, tuple[Any, VerifyRow]] = {}
    ibkr_errors: list[IbkrError] = []

    def on_error(req_id: int, error_code: int, message: str, contract: Any) -> None:
        ibkr_errors.append(
            IbkrError(
                req_id=req_id,
                error_code=error_code,
                message=message,
                contract=str(contract) if contract is not None else None,
                ts=datetime.now(tz=timezone.utc).isoformat(),
            )
        )

    try:
        print(f"Connecting to IBKR at {settings.host}:{settings.port} clientId={settings.client_id}")
        try:
            connect_market_data_only(ib, settings)
        except Exception as exc:  # noqa: BLE001
            print(
                "Failed to connect to IBKR. Confirm TWS/IB Gateway is running, "
                "API socket is enabled, and IBKR_HOST/IBKR_PORT are correct.",
                file=sys.stderr,
            )
            print(f"Connection error: {exc}", file=sys.stderr)
            return 1
        ib.errorEvent += on_error
        ib.reqMarketDataType(settings.market_data_type)

        base_contracts = build_base_contracts(settings)
        base_subs = qualify_and_subscribe(ib, base_contracts, qualify=settings.qualify_contracts)
        ib.sleep(settings.quote_wait_seconds)
        base_rows = snapshot_rows(base_subs, settings.stale_after_seconds)

        atm_reference, atm_source = estimate_atm_reference(base_rows)
        if args.skip_options:
            rows = base_rows
        elif atm_reference is None:
            print("Could not estimate SPX ATM reference; skipping SPXW option checks.", file=sys.stderr)
            rows = base_rows
        else:
            print(f"Estimated SPX ATM reference {atm_reference:.2f} from {atm_source}")
            option_contracts = build_spxw_option_contracts(settings, atm_reference)
            option_subs = qualify_and_subscribe(
                ib,
                option_contracts,
                qualify=settings.qualify_contracts,
            )
            ib.sleep(settings.quote_wait_seconds)
            rows = base_rows + snapshot_rows(option_subs, settings.stale_after_seconds)

        print_rows(rows)
        if ibkr_errors:
            print("\nIBKR errors:")
            for error in ibkr_errors:
                print(f"- reqId={error.req_id} code={error.error_code}: {error.message}")
        output_path = write_snapshot(settings, rows, ibkr_errors)
        print(f"\nWrote JSON snapshot: {output_path}")
        return 0
    finally:
        cancel_subscriptions(ib, option_subs)
        cancel_subscriptions(ib, base_subs)
        if ib.isConnected():
            ib.disconnect()


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
