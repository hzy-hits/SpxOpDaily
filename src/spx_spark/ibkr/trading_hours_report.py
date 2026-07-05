from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, time as datetime_time, timezone
from pathlib import Path
from typing import Any

from spx_spark.config import IbkrSettings, NY_TZ
from spx_spark.ibkr.verifier import (
    IbkrError,
    VerifyRow,
    build_base_contracts,
    build_spxw_option_contracts,
    cancel_subscriptions,
    connect_market_data_only,
    estimate_atm_reference,
    first_present,
    midpoint,
    qualify_and_subscribe,
    snapshot_rows,
)


P0_INDEX_LABELS = (
    "index:SPX",
    "index:VIX",
    "index:VIX1D",
    "index:VIX9D",
    "index:VIX3M",
    "index:VVIX",
    "index:SKEW",
)

ENTITLEMENT_ERROR_CODES = {354, 10090, 10186, 10197}
OK_STATUSES = {"ok"}
DEGRADED_STATUSES = {
    "delayed",
    "delayed_frozen",
    "frozen",
    "missing_bid_ask",
    "missing_greeks",
    "missing_price",
    "stale",
    "unknown",
}


@dataclass(frozen=True)
class RowCheck:
    label: str
    kind: str
    symbol: str
    exchange: str | None
    group: str
    status: str
    live_state: str
    qualified: bool
    subscribed: bool
    has_price: bool
    has_bid_ask: bool
    has_greeks: bool
    stale: bool | None
    bid: float | None
    ask: float | None
    last: float | None
    market_price: float | None
    close: float | None
    ticker_time: str | None
    error: str | None


@dataclass(frozen=True)
class GroupCheck:
    name: str
    total: int
    counts: dict[str, int]
    ok: int
    degraded: int
    failed: int
    missing_required_labels: list[str]
    bad_required_labels: list[str]
    status: str


def has_price(row: VerifyRow) -> bool:
    return first_present(row.market_price, row.last, midpoint(row.bid, row.ask), row.close) is not None


def has_bid_ask(row: VerifyRow) -> bool:
    return midpoint(row.bid, row.ask) is not None


def has_greeks(row: VerifyRow) -> bool:
    values = (row.model_iv, row.delta, row.gamma, row.theta, row.vega)
    return all(value is not None for value in values)


def row_group(row: VerifyRow) -> str:
    if row.kind == "option":
        return "spxw_options"
    if row.kind == "future":
        return "futures"
    if row.kind == "stock":
        return "etf_proxies"
    if row.label in P0_INDEX_LABELS:
        return "p0_indexes"
    if row.kind == "index":
        return "secondary_indexes"
    return "other"


def classify_row(row: VerifyRow, *, require_option_greeks: bool = True) -> str:
    price = has_price(row)
    bid_ask = has_bid_ask(row)
    greeks = has_greeks(row)

    if row.error:
        return "error"
    if not row.qualified or not row.subscribed:
        return "missing"
    if row.stale is True:
        return "stale"
    if row.live_state == "delayed":
        return "delayed"
    if row.live_state == "delayed-frozen":
        return "delayed_frozen"
    if row.live_state == "frozen":
        return "frozen"
    if row.live_state == "unknown":
        return "unknown"
    if not price:
        return "missing_price"
    if row.kind == "option" and not bid_ask:
        return "missing_bid_ask"
    if row.kind == "option" and require_option_greeks and not greeks:
        return "missing_greeks"
    return "ok"


def check_row(row: VerifyRow, *, require_option_greeks: bool = True) -> RowCheck:
    return RowCheck(
        label=row.label,
        kind=row.kind,
        symbol=row.symbol,
        exchange=row.exchange,
        group=row_group(row),
        status=classify_row(row, require_option_greeks=require_option_greeks),
        live_state=row.live_state,
        qualified=row.qualified,
        subscribed=row.subscribed,
        has_price=has_price(row),
        has_bid_ask=has_bid_ask(row),
        has_greeks=has_greeks(row),
        stale=row.stale,
        bid=row.bid,
        ask=row.ask,
        last=row.last,
        market_price=row.market_price,
        close=row.close,
        ticker_time=row.ticker_time,
        error=row.error,
    )


def summarize_group(
    name: str,
    checks: list[RowCheck],
    *,
    required_labels: tuple[str, ...] = (),
) -> GroupCheck:
    group_rows = [row for row in checks if row.group == name]
    counts = Counter(row.status for row in group_rows)
    by_label = {row.label: row for row in group_rows}
    missing_required = [label for label in required_labels if label not in by_label]
    bad_required = [
        label
        for label in required_labels
        if label in by_label and by_label[label].status not in OK_STATUSES
    ]
    failed = sum(counts[status] for status in ("error", "missing"))
    degraded = sum(counts[status] for status in DEGRADED_STATUSES)

    if missing_required or bad_required or failed:
        status = "failed"
    elif degraded:
        status = "degraded"
    else:
        status = "ok"

    return GroupCheck(
        name=name,
        total=len(group_rows),
        counts=dict(sorted(counts.items())),
        ok=counts["ok"],
        degraded=degraded,
        failed=failed,
        missing_required_labels=missing_required,
        bad_required_labels=bad_required,
        status=status,
    )


def is_regular_trading_hours(now: datetime) -> bool:
    now_ny = now.astimezone(NY_TZ)
    return now_ny.weekday() < 5 and datetime_time(9, 30) <= now_ny.time() < datetime_time(16, 0)


def trading_window(now: datetime) -> dict[str, Any]:
    now_ny = now.astimezone(NY_TZ)
    return {
        "timezone": str(NY_TZ),
        "now": now_ny.isoformat(),
        "weekday": now_ny.strftime("%A"),
        "regular_trading_hours": is_regular_trading_hours(now_ny),
        "regular_session": "09:30-16:00 America/New_York",
    }


def summarize_errors(errors: list[IbkrError]) -> dict[str, Any]:
    counts = Counter(error.error_code for error in errors)
    entitlement = [error for error in errors if error.error_code in ENTITLEMENT_ERROR_CODES]
    return {
        "total": len(errors),
        "by_code": {str(code): count for code, count in sorted(counts.items())},
        "entitlement_error_count": len(entitlement),
        "entitlement_errors": [asdict(error) for error in entitlement],
        "all": [asdict(error) for error in errors],
    }


def determine_overall_status(
    *,
    connected: bool,
    rth_now: bool,
    groups: dict[str, GroupCheck],
    errors: list[IbkrError],
    skip_options: bool,
    allow_outside_rth: bool,
) -> str:
    if not connected:
        return "unavailable"
    if not rth_now and not allow_outside_rth:
        return "not_rth"
    if any(error.error_code in ENTITLEMENT_ERROR_CODES for error in errors):
        return "degraded"
    required_groups = ["p0_indexes", "etf_proxies", "futures"]
    if not skip_options:
        required_groups.append("spxw_options")
    for name in required_groups:
        group = groups.get(name)
        if group is None or group.status != "ok":
            return "degraded"
    return "available"


def report_payload(
    *,
    settings: IbkrSettings,
    rows: list[VerifyRow],
    errors: list[IbkrError],
    connected: bool,
    authenticated: bool | None,
    latency_ms: float | None,
    skip_options: bool,
    allow_outside_rth: bool,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated_at = generated_at or datetime.now(tz=timezone.utc)
    checks = [check_row(row) for row in rows]
    groups = {
        "p0_indexes": summarize_group("p0_indexes", checks, required_labels=P0_INDEX_LABELS),
        "secondary_indexes": summarize_group("secondary_indexes", checks),
        "etf_proxies": summarize_group("etf_proxies", checks),
        "futures": summarize_group("futures", checks),
        "spxw_options": summarize_group("spxw_options", checks),
        "other": summarize_group("other", checks),
    }
    rth_now = is_regular_trading_hours(generated_at)
    overall_status = determine_overall_status(
        connected=connected,
        rth_now=rth_now,
        groups=groups,
        errors=errors,
        skip_options=skip_options,
        allow_outside_rth=allow_outside_rth,
    )
    option_rows = [row for row in checks if row.group == "spxw_options"]

    return {
        "created_at": generated_at.isoformat(),
        "overall_status": overall_status,
        "connection": {
            "host": settings.host,
            "port": settings.port,
            "client_id": settings.client_id,
            "connected": connected,
            "authenticated": authenticated,
            "latency_ms": latency_ms,
            "market_data_type_requested": settings.market_data_type,
        },
        "trading_window": trading_window(generated_at),
        "settings": asdict(settings),
        "summary": {
            "row_count": len(checks),
            "option_count": len(option_rows),
            "option_with_bid_ask": sum(1 for row in option_rows if row.has_bid_ask),
            "option_with_greeks": sum(1 for row in option_rows if row.has_greeks),
            "skip_options": skip_options,
            "allow_outside_rth": allow_outside_rth,
        },
        "groups": {name: asdict(group) for name, group in groups.items()},
        "rows": [asdict(row) for row in checks],
        "ibkr_errors": summarize_errors(errors),
    }


def write_report(payload: dict[str, Any], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(tz=NY_TZ).strftime("%Y%m%d-%H%M%S")
    output_path = output_dir / f"ibkr-trading-hours-report-{timestamp}.json"
    output_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return output_path


def print_report_summary(payload: dict[str, Any], output_path: Path | None = None) -> None:
    print(f"IBKR trading-hours report: {payload['overall_status']}")
    window = payload["trading_window"]
    print(f"NY time: {window['now']} ({window['regular_session']})")
    print(f"Connected: {payload['connection']['connected']} port={payload['connection']['port']}")
    print(f"Rows: {payload['summary']['row_count']} options={payload['summary']['option_count']}")
    print("Groups:")
    for name, group in payload["groups"].items():
        if group["total"] == 0 and name in {"secondary_indexes", "other"}:
            continue
        counts = ", ".join(f"{status}={count}" for status, count in group["counts"].items())
        print(f"- {name}: {group['status']} total={group['total']} {counts}".rstrip())
        if group["missing_required_labels"]:
            print(f"  missing required: {', '.join(group['missing_required_labels'])}")
        if group["bad_required_labels"]:
            print(f"  bad required: {', '.join(group['bad_required_labels'])}")
    errors = payload["ibkr_errors"]
    if errors["total"]:
        print(f"IBKR errors: {errors['total']} by_code={errors['by_code']}")
    if output_path is not None:
        print(f"Wrote JSON report: {output_path}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write an IBKR trading-hours market-data report.")
    parser.add_argument(
        "--print-config",
        action="store_true",
        help="Print resolved settings and exit without connecting.",
    )
    parser.add_argument(
        "--skip-options",
        action="store_true",
        help="Verify indexes/stocks/futures only and skip SPXW option subscriptions.",
    )
    parser.add_argument(
        "--allow-outside-rth",
        action="store_true",
        help="Allow an available status outside the regular 09:30-16:00 ET session.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero unless overall_status is available.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print full JSON report to stdout.",
    )
    parser.add_argument(
        "--output-dir",
        default="logs",
        help="Directory for the JSON report.",
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
    connected = False
    authenticated: bool | None = None
    latency_ms: float | None = None

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

    start = time.perf_counter()
    try:
        try:
            connect_market_data_only(ib, settings)
            connected = True
            authenticated = True
            latency_ms = (time.perf_counter() - start) * 1000.0
        except Exception as exc:  # noqa: BLE001
            latency_ms = (time.perf_counter() - start) * 1000.0
            ibkr_errors.append(
                IbkrError(
                    req_id=-1,
                    error_code=-1,
                    message=f"connect failed: {exc}",
                    contract=None,
                    ts=datetime.now(tz=timezone.utc).isoformat(),
                )
            )
            payload = report_payload(
                settings=settings,
                rows=[],
                errors=ibkr_errors,
                connected=False,
                authenticated=False,
                latency_ms=latency_ms,
                skip_options=args.skip_options,
                allow_outside_rth=args.allow_outside_rth,
            )
            output_path = write_report(payload, Path(args.output_dir))
            if args.json:
                print(json.dumps(payload, indent=2, sort_keys=True))
            else:
                print_report_summary(payload, output_path)
            return 1

        ib.errorEvent += on_error
        ib.reqMarketDataType(settings.market_data_type)

        base_subs = qualify_and_subscribe(
            ib,
            build_base_contracts(settings),
            qualify=settings.qualify_contracts,
        )
        ib.sleep(settings.quote_wait_seconds)
        base_rows = snapshot_rows(base_subs, settings.stale_after_seconds)

        atm_reference, _ = estimate_atm_reference(base_rows)
        if args.skip_options:
            rows = base_rows
        elif atm_reference is None:
            rows = base_rows
            ibkr_errors.append(
                IbkrError(
                    req_id=-2,
                    error_code=-2,
                    message="could not estimate SPX ATM reference; skipped SPXW option checks",
                    contract=None,
                    ts=datetime.now(tz=timezone.utc).isoformat(),
                )
            )
        else:
            option_subs = qualify_and_subscribe(
                ib,
                build_spxw_option_contracts(settings, atm_reference),
                qualify=settings.qualify_contracts,
            )
            ib.sleep(settings.quote_wait_seconds)
            rows = base_rows + snapshot_rows(option_subs, settings.stale_after_seconds)

        payload = report_payload(
            settings=settings,
            rows=rows,
            errors=ibkr_errors,
            connected=connected,
            authenticated=authenticated,
            latency_ms=latency_ms,
            skip_options=args.skip_options,
            allow_outside_rth=args.allow_outside_rth,
        )
        output_path = write_report(payload, Path(args.output_dir))
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print_report_summary(payload, output_path)
        if args.strict and payload["overall_status"] != "available":
            return 1
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
