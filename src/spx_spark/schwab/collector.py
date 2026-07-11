"""Schwab option-chain collector: fetch chains and persist normalized quotes."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from urllib.error import HTTPError, URLError

from spx_spark.config import SchwabSettings, StorageSettings, env_csv
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET
from spx_spark.provider_adapter import persist_provider_snapshot
from spx_spark.schwab.adapter import snapshot_from_chain_payload
from spx_spark.schwab.verifier import SchwabClient, build_schwab_client


def fetch_chain(
    client: SchwabClient,
    symbol: str,
    settings: SchwabSettings,
    *,
    now: datetime | None = None,
) -> Any:
    current_expiry, next_expiry = DEFAULT_MARKET_CALENDAR.research_expiries(
        now or datetime.now(tz=ET)
    )
    _status, payload = client.get_json(
        "/marketdata/v1/chains",
        {
            "symbol": symbol,
            "contractType": "ALL",
            "strategy": "SINGLE",
            "strikeCount": settings.option_chain_strike_count,
            "includeUnderlyingQuote": "true",
            "fromDate": current_expiry.isoformat(),
            "toDate": next_expiry.isoformat(),
        },
    )
    return payload


def run(argv: list[str] | None = None) -> int:
    del argv
    settings = SchwabSettings.from_env()
    storage_settings = StorageSettings.from_env()
    client = build_schwab_client(settings)
    if client is None:
        print(json.dumps({"ok": False, "skipped": True, "reason": "missing_schwab_auth"}))
        return 0

    symbols = env_csv("SCHWAB_COLLECT_CHAINS", "SPY")
    quote_counts: dict[str, int] = {}
    errors: list[str] = []
    for symbol in symbols:
        try:
            payload = fetch_chain(client, symbol, settings)
            snapshot = snapshot_from_chain_payload(payload, underlier=symbol)
            persist_provider_snapshot(snapshot, storage_settings)
            quote_counts[symbol] = snapshot.quote_count
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, ValueError) as exc:
            errors.append(f"{symbol}: {exc}")

    ok = bool(quote_counts)
    summary = {
        "ok": ok,
        "symbols": list(quote_counts.keys()),
        "quote_counts": quote_counts,
        "errors": errors,
    }
    print(json.dumps(summary, sort_keys=True))
    return 0 if ok or not symbols else 1


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
