"""Schwab option-chain collector: fetch chains and persist normalized quotes."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from urllib.error import HTTPError, URLError

from spx_spark.config import SchwabSettings, SchwabStreamSettings, StorageSettings, env_csv
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET
from spx_spark.provider_adapter import persist_provider_snapshot
from spx_spark.runtime_config import runtime_value
from spx_spark.schwab.adapter import snapshot_from_chain_payload, snapshot_from_quote_payload
from spx_spark.schwab.symbols import (
    canonical_underlier_for_schwab,
    option_chain_symbol_for_schwab,
    resolved_schwab_canonical_quote_symbols,
    resolved_schwab_quote_symbols,
    schwab_option_chain_underliers,
    schwab_quote_symbols,
)
from spx_spark.schwab.verifier import SchwabClient, build_schwab_client, quote_batches


SCHWAB_QUOTE_PATH = str(runtime_value("schwab.quote_path"))
SCHWAB_OPTION_CHAIN_PATH = str(runtime_value("schwab.option_chain_path"))


def fetch_quotes(client: SchwabClient, symbols: list[str], settings: SchwabSettings) -> Any:
    _status, payload = client.get_json(
        SCHWAB_QUOTE_PATH,
        {
            "symbols": ",".join(symbols),
            "fields": settings.quote_fields,
            "indicative": "false",
        },
    )
    return payload


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
    provider_symbol = option_chain_symbol_for_schwab(symbol)
    _status, payload = client.get_json(
        SCHWAB_OPTION_CHAIN_PATH,
        {
            "symbol": provider_symbol,
            "contractType": "ALL",
            "strategy": "SINGLE",
            "strikeCount": settings.option_chain_strike_count,
            "includeUnderlyingQuote": "true",
            "fromDate": current_expiry.isoformat(),
            "toDate": next_expiry.isoformat(),
        },
    )
    return payload


def run(argv: list[str] | None = None, *, now: datetime | None = None) -> int:
    del argv
    evaluation_now = now or datetime.now(tz=ET)
    settings = SchwabSettings.from_env()
    storage_settings = StorageSettings.from_env()
    stream_settings = SchwabStreamSettings.from_env(data_root=storage_settings.data_root)
    client = build_schwab_client(settings)
    if client is None:
        print(json.dumps({"ok": False, "skipped": True, "reason": "missing_schwab_auth"}))
        return 0

    configured_quote_symbols = env_csv(
        "SCHWAB_COLLECT_QUOTES",
        ",".join(schwab_quote_symbols()),
    )
    quote_symbols = resolved_schwab_quote_symbols(
        configured_quote_symbols,
        now=evaluation_now,
    )
    if stream_settings.mode == "live":
        streaming_symbols = set(
            resolved_schwab_canonical_quote_symbols(
                stream_settings.canonical_symbols,
                now=evaluation_now,
            )
        )
        quote_symbols = [symbol for symbol in quote_symbols if symbol not in streaming_symbols]
    chain_symbols = env_csv(
        "SCHWAB_COLLECT_CHAINS",
        ",".join(schwab_option_chain_underliers()),
    )
    quote_counts: dict[str, int] = {}
    errors: list[str] = []
    for batch in quote_batches(quote_symbols):
        label = ",".join(batch)
        try:
            payload = fetch_quotes(client, batch, settings)
            snapshot = snapshot_from_quote_payload(payload, batch)
            persist_provider_snapshot(snapshot, storage_settings)
            quote_counts[f"quotes:{label}"] = snapshot.quote_count
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, ValueError) as exc:
            errors.append(f"quotes:{label}: {exc}")

    for symbol in chain_symbols:
        try:
            payload = fetch_chain(client, symbol, settings, now=evaluation_now)
            snapshot = snapshot_from_chain_payload(
                payload,
                underlier=canonical_underlier_for_schwab(symbol),
            )
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
    return 0 if ok or not quote_symbols and not chain_symbols else 1


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
