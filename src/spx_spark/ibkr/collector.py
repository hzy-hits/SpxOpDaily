from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

from spx_spark.config import IbkrSettings, RuntimePolicySettings, StorageSettings
from spx_spark.ibkr.adapter import snapshot_from_rows
from spx_spark.ibkr.verifier import (
    IbkrError,
    VerifyRow,
    build_base_contracts,
    build_spxw_option_contracts,
    cancel_subscriptions,
    connect_market_data_only,
    estimate_atm_reference,
    print_rows,
    qualify_and_subscribe,
    snapshot_rows,
)
from spx_spark.marketdata import (
    Provider,
    ProviderState,
    ProviderStatus,
)
from spx_spark.provider_adapter import ProviderSnapshot, persist_provider_snapshot
from spx_spark.runtime_mode import ibkr_allowed, load_override
from spx_spark.storage import LatestStateStore


NON_DEGRADING_ERROR_CODES = {
    2104,  # market data farm connection is OK
    2106,  # historical market data farm connection is OK
    2119,  # market data farm is connecting
    2158,  # sec-def data farm connection is OK
}


def write_empty_provider_state(
    state: ProviderState,
    *,
    storage_settings: StorageSettings,
    now: datetime,
) -> None:
    persist_provider_snapshot(
        ProviderSnapshot.from_state(Provider.IBKR, state, received_at=now),
        storage_settings,
    )


def print_collector_summary(
    *,
    raw_paths: dict[str, int],
    latest_path: str,
    provider_state: ProviderState,
    quote_count: int,
    best_quote_count: int,
) -> None:
    print(f"Provider: {provider_state.provider.value}")
    print(f"Status: {provider_state.status.value}")
    if provider_state.reason:
        print(f"Reason: {provider_state.reason}")
    print(f"Quotes collected: {quote_count}")
    print(f"Latest state: {latest_path}")
    print(f"Best quotes: {best_quote_count}")
    if raw_paths:
        print("Raw files:")
        for path, count in sorted(raw_paths.items()):
            print(f"- {path}: {count}")


def has_competing_session_error(errors: list[IbkrError]) -> bool:
    return any(error.error_code == 10197 for error in errors)


def provider_error_count(errors: list[IbkrError]) -> int:
    return sum(1 for error in errors if error.error_code not in NON_DEGRADING_ERROR_CODES)


def error_payload(errors: list[IbkrError]) -> list[dict[str, object]]:
    return [asdict(error) for error in errors]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Collect one IBKR market-data snapshot.")
    parser.add_argument(
        "--print-config",
        action="store_true",
        help="Print resolved collector settings and exit without connecting.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print runtime policy and exit without connecting.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore runtime mode gate and try connecting to IBKR.",
    )
    parser.add_argument(
        "--skip-options",
        action="store_true",
        help="Collect indexes/stocks/futures only and skip SPXW option subscriptions.",
    )
    parser.add_argument(
        "--no-table",
        action="store_true",
        help="Do not print the verifier-style quote table.",
    )
    parser.add_argument("--json", action="store_true", help="Print a JSON summary.")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    ibkr_settings = IbkrSettings.from_env()
    runtime_policy = RuntimePolicySettings.from_env()
    storage_settings = StorageSettings.from_env()
    override = load_override(runtime_policy.runtime_mode_path)
    allowed = args.force or ibkr_allowed(runtime_policy, override=override)

    config_summary = {
        "ibkr": asdict(ibkr_settings),
        "runtime": asdict(runtime_policy),
        "runtime_override": override.to_dict() if override else None,
        "storage": asdict(storage_settings),
        "allowed": allowed,
        "force": args.force,
    }
    if args.print_config or args.dry_run:
        print(json.dumps(config_summary, indent=2, sort_keys=True, default=str))
        return 0

    now = datetime.now(tz=timezone.utc)
    if not allowed:
        state = ProviderState(
            provider=Provider.IBKR,
            status=ProviderStatus.UNAVAILABLE,
            checked_at=now,
            reason="runtime policy blocks IBKR collection",
            connected=False,
            authenticated=None,
            priority=0,
        )
        write_empty_provider_state(state, storage_settings=storage_settings, now=now)
        if args.json:
            print(
                json.dumps(
                    {
                        "status": state.status.value,
                        "reason": state.reason,
                        "quotes_collected": 0,
                        "latest_state": storage_settings.latest_state_path,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
        else:
            print_collector_summary(
                raw_paths={},
                latest_path=storage_settings.latest_state_path,
                provider_state=state,
                quote_count=0,
                best_quote_count=len(LatestStateStore(storage_settings).load().best_quotes),
            )
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

    start = time.perf_counter()
    try:
        try:
            connect_market_data_only(ib, ibkr_settings)
        except Exception as exc:  # noqa: BLE001
            checked_at = datetime.now(tz=timezone.utc)
            state = ProviderState(
                provider=Provider.IBKR,
                status=ProviderStatus.UNAVAILABLE,
                checked_at=checked_at,
                reason=f"connect failed: {exc}",
                connected=False,
                authenticated=False,
                latency_ms=(time.perf_counter() - start) * 1000.0,
                priority=0,
            )
            write_empty_provider_state(state, storage_settings=storage_settings, now=checked_at)
            print_collector_summary(
                raw_paths={},
                latest_path=storage_settings.latest_state_path,
                provider_state=state,
                quote_count=0,
                best_quote_count=len(LatestStateStore(storage_settings).load().best_quotes),
            )
            return 1

        ib.errorEvent += on_error
        ib.reqMarketDataType(ibkr_settings.market_data_type)

        base_subs = qualify_and_subscribe(ib, build_base_contracts(ibkr_settings))
        ib.sleep(ibkr_settings.quote_wait_seconds)
        base_rows = snapshot_rows(base_subs, ibkr_settings.stale_after_seconds)

        atm_reference, atm_source = estimate_atm_reference(base_rows)
        if args.skip_options:
            rows = base_rows
        elif atm_reference is None:
            if not args.json:
                print("Could not estimate SPX ATM reference; skipping SPXW options.", file=sys.stderr)
            rows = base_rows
        else:
            if not args.json:
                print(f"Estimated SPX ATM reference {atm_reference:.2f} from {atm_source}")
            option_contracts = build_spxw_option_contracts(ibkr_settings, atm_reference)
            option_subs = qualify_and_subscribe(ib, option_contracts)
            ib.sleep(ibkr_settings.quote_wait_seconds)
            rows = base_rows + snapshot_rows(option_subs, ibkr_settings.stale_after_seconds)

        received_at = datetime.now(tz=timezone.utc)
        snapshot = snapshot_from_rows(
            rows,
            received_at=received_at,
            stale_after_seconds=ibkr_settings.stale_after_seconds,
            connected=True,
            authenticated=True,
            latency_ms=(time.perf_counter() - start) * 1000.0,
            error_count=provider_error_count(ibkr_errors),
        )
        if has_competing_session_error(ibkr_errors):
            snapshot = ProviderSnapshot(
                provider=Provider.IBKR,
                received_at=received_at,
                quotes=snapshot.quotes,
                provider_states=(
                    ProviderState(
                        provider=Provider.IBKR,
                        status=ProviderStatus.UNAVAILABLE,
                        checked_at=received_at,
                        reason="competing session blocks live market data (IBKR 10197)",
                        connected=True,
                        authenticated=True,
                        latency_ms=(time.perf_counter() - start) * 1000.0,
                        priority=0,
                    ),
                ),
            )
        state = snapshot.provider_state
        if state is None:
            raise RuntimeError("IBKR snapshot missing provider state")
        write_result = persist_provider_snapshot(snapshot, storage_settings)

        if not args.json and not args.no_table:
            print_rows(rows)
        if ibkr_errors and not args.json:
            print("\nIBKR errors:")
            for error in ibkr_errors:
                print(f"- reqId={error.req_id} code={error.error_code}: {error.message}")

        summary = {
            "provider_state": state.to_dict(),
            "quotes_collected": snapshot.quote_count,
            "error_count": len(ibkr_errors),
            "provider_error_count": provider_error_count(ibkr_errors),
            "errors": error_payload(ibkr_errors),
            "competing_session": has_competing_session_error(ibkr_errors),
            "raw_paths": write_result.raw_paths,
            "latest_state": write_result.latest_state,
            "best_quote_count": write_result.best_quote_count,
            "provider_quote_count": write_result.provider_quote_count,
        }
        if args.json:
            print(json.dumps(summary, indent=2, sort_keys=True))
        else:
            print_collector_summary(
                raw_paths=write_result.raw_paths,
                latest_path=write_result.latest_state,
                provider_state=state,
                quote_count=snapshot.quote_count,
                best_quote_count=write_result.best_quote_count,
            )
        return 0 if state.status != ProviderStatus.UNAVAILABLE else 1
    finally:
        cancel_subscriptions(ib, option_subs)
        cancel_subscriptions(ib, base_subs)
        if ib.isConnected():
            ib.disconnect()


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
