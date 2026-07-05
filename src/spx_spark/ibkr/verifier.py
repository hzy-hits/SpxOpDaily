from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from spx_spark.config import IbkrSettings, NY_TZ
from spx_spark.marketdata import quote_from_ibkr_row


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
    model_iv: float | None = None
    delta: float | None = None
    gamma: float | None = None
    theta: float | None = None
    vega: float | None = None
    und_price: float | None = None
    ticker_time: str | None = None
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
    from ib_async import Future, Index, Stock

    contracts: list[tuple[str, str, Any]] = []

    for spec in settings.verify_indexes:
        symbol, exchange = parse_index_spec(spec)
        contracts.append((f"index:{symbol}", "index", Index(symbol, exchange, "USD")))

    for symbol in settings.verify_stocks:
        contracts.append((f"stock:{symbol}", "stock", Stock(symbol, "SMART", "USD")))

    for symbol in settings.verify_futures:
        expiry = settings.mes_expiry if symbol == "MES" else settings.es_expiry
        contracts.append((f"future:{symbol}", "future", Future(symbol, expiry, "CME", currency="USD")))

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
) -> dict[str, tuple[Any, VerifyRow]]:
    result: dict[str, tuple[Any, VerifyRow]] = {}
    for label, kind, contract in contracts:
        row = VerifyRow(
            label=label,
            kind=kind,
            symbol=getattr(contract, "symbol", label),
            exchange=getattr(contract, "exchange", None),
        )
        if qualify:
            try:
                qualified = ib.qualifyContracts(contract)
                if qualified:
                    contract = qualified[0]
                    row.exchange = getattr(contract, "exchange", row.exchange)
                row.qualified = True
            except Exception as exc:  # noqa: BLE001
                row.error = f"qualify failed: {exc}"
                result[label] = (None, row)
                continue

        try:
            ticker = ib.reqMktData(contract, "", False, False)
            row.subscribed = True
            result[label] = (ticker, row)
        except Exception as exc:  # noqa: BLE001
            row.error = f"subscribe failed: {exc}"
            result[label] = (None, row)
    return result


def snapshot_rows(
    subscriptions: dict[str, tuple[Any, VerifyRow]],
    stale_after_seconds: float,
) -> list[VerifyRow]:
    now = datetime.now(tz=timezone.utc)
    rows: list[VerifyRow] = []
    for _, (ticker, row) in subscriptions.items():
        if ticker is None:
            rows.append(row)
            continue

        row.market_data_type = getattr(ticker, "marketDataType", None)
        row.bid = clean_float(getattr(ticker, "bid", None))
        row.ask = clean_float(getattr(ticker, "ask", None))
        row.last = clean_float(getattr(ticker, "last", None))
        row.close = clean_float(getattr(ticker, "close", None))
        row.bid_size = clean_float(getattr(ticker, "bidSize", None))
        row.ask_size = clean_float(getattr(ticker, "askSize", None))
        row.last_size = clean_float(getattr(ticker, "lastSize", None))

        try:
            row.market_price = clean_float(ticker.marketPrice())
        except Exception:  # noqa: BLE001
            row.market_price = first_present(row.last, midpoint(row.bid, row.ask), row.close)

        ticker_time = getattr(ticker, "time", None)
        if ticker_time is not None:
            if ticker_time.tzinfo is None:
                ticker_time = ticker_time.replace(tzinfo=timezone.utc)
            row.ticker_time = ticker_time.astimezone(timezone.utc).isoformat()
            row.stale = (now - ticker_time.astimezone(timezone.utc)).total_seconds() > stale_after_seconds

        greeks = getattr(ticker, "modelGreeks", None)
        if greeks is not None:
            row.model_iv = clean_float(getattr(greeks, "impliedVol", None))
            row.delta = clean_float(getattr(greeks, "delta", None))
            row.gamma = clean_float(getattr(greeks, "gamma", None))
            row.theta = clean_float(getattr(greeks, "theta", None))
            row.vega = clean_float(getattr(greeks, "vega", None))
            row.und_price = clean_float(getattr(greeks, "undPrice", None))

        rows.append(row)
    return rows


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
    by_label = {row.label: row for row in rows}

    spx = by_label.get("index:SPX")
    if spx:
        price = first_present(spx.market_price, spx.last, midpoint(spx.bid, spx.ask), spx.close)
        if price:
            return price, "SPX"

    es = by_label.get("future:ES")
    if es:
        price = first_present(es.market_price, es.last, midpoint(es.bid, es.ask), es.close)
        if price:
            return price, "ES"

    spy = by_label.get("stock:SPY")
    if spy:
        price = first_present(spy.market_price, spy.last, midpoint(spy.bid, spy.ask), spy.close)
        if price:
            return price * 10.0, "SPY*10"

    return None, "none"


def cancel_subscriptions(ib: Any, subscriptions: dict[str, tuple[Any, VerifyRow]]) -> None:
    for ticker, _ in subscriptions.values():
        if ticker is None:
            continue
        try:
            ib.cancelMktData(ticker.contract)
        except Exception:  # noqa: BLE001
            pass


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
