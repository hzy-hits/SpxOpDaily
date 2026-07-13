"""Schwab collector transport and response helpers."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime
from typing import Any
from urllib.error import HTTPError, URLError

from spx_spark.config import SchwabSettings, StorageSettings
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET
from spx_spark.provider_adapter import persist_provider_snapshot
from spx_spark.schwab.adapter import snapshot_from_quote_payload
from spx_spark.schwab.chain_discovery import chain_params
from spx_spark.schwab.request_models import RequestWindow
from spx_spark.schwab.symbols import (
    option_chain_strike_count_for,
    option_chain_symbol_for_schwab,
)
from spx_spark.schwab.verifier import SchwabClient, quote_batches


SCHWAB_QUOTE_PATH = "/marketdata/v1/quotes"
SCHWAB_OPTION_CHAIN_PATH = "/marketdata/v1/chains"


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
    strike_count: int | None = None,
    expiry: Any | None = None,
) -> Any:
    current_expiry, next_expiry = DEFAULT_MARKET_CALENDAR.research_expiries(
        now or datetime.now(tz=ET)
    )
    provider_symbol = option_chain_symbol_for_schwab(symbol)
    resolved_strike_count = (
        int(strike_count)
        if strike_count is not None
        else option_chain_strike_count_for(symbol, settings.option_chain_strike_count)
    )
    params = (
        chain_params(symbol=provider_symbol, expiry=expiry, strike_count=resolved_strike_count)
        if expiry is not None
        else {
            "symbol": provider_symbol,
            "contractType": "ALL",
            "strategy": "SINGLE",
            "strikeCount": resolved_strike_count,
            "includeUnderlyingQuote": "true",
            "fromDate": current_expiry.isoformat(),
            "toDate": next_expiry.isoformat(),
        }
    )
    _status, payload = client.get_json(SCHWAB_OPTION_CHAIN_PATH, params)
    return payload


def chain_spot(payload: Any, quotes: tuple[Any, ...]) -> float | None:
    if isinstance(payload, Mapping):
        for value in (
            payload.get("underlyingPrice"),
            payload.get("underlierPrice"),
        ):
            parsed = float_or_none(value)
            if parsed is not None and parsed > 0:
                return parsed
        underlying = payload.get("underlying")
        if isinstance(underlying, Mapping):
            for key in ("mark", "last", "lastPrice", "close"):
                parsed = float_or_none(underlying.get(key))
                if parsed is not None and parsed > 0:
                    return parsed
    for quote in quotes:
        greeks = getattr(quote, "greeks", None)
        value = getattr(greeks, "underlier_price", None) if greeks else None
        if value is not None and value > 0:
            return float(value)
    return None


def collect_quote_batches(
    client: SchwabClient,
    symbols: list[str],
    *,
    settings: SchwabSettings,
    storage_settings: StorageSettings,
    received_at: datetime,
    batch_size: int,
    priority_symbol_count: int,
    available_requests: int,
    hot_lane: bool,
    persist_snapshot: Any = persist_provider_snapshot,
) -> tuple[int, dict[str, int], list[str], bool]:
    request_count = 0
    quote_counts: dict[str, int] = {}
    errors: list[str] = []
    complete = True
    priority_end = min(max(priority_symbol_count, 0), len(symbols))
    batches = [
        *quote_batches(symbols[:priority_end], batch_size=batch_size),
        *quote_batches(symbols[priority_end:], batch_size=batch_size),
    ]
    for batch in batches:
        if request_count >= available_requests:
            errors.append("quotes:hot_context: planned_request_ceiling")
            complete = False
            break
        label = ",".join(batch)
        try:
            payload = fetch_quotes(client, batch, settings)
            request_count += 1
            snapshot = snapshot_from_quote_payload(payload, batch, received_at=received_at)
            persist_snapshot(snapshot, storage_settings)
            key = "quotes:hot_context" if hot_lane else f"quotes:{label}"
            quote_counts[key] = quote_counts.get(key, 0) + snapshot.quote_count
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, ValueError) as exc:
            errors.append(f"quotes:{label}: {exc}")
            complete = False
    return request_count, quote_counts, errors, complete and bool(symbols)


def gateway_request_window(client: Any) -> RequestWindow:
    health_reader = getattr(client, "get_gateway_health", None)
    if not callable(health_reader):
        return RequestWindow()
    try:
        health = health_reader()
    except (HTTPError, URLError, TimeoutError, ValueError, json.JSONDecodeError):
        return RequestWindow(failures=1)
    if not isinstance(health, Mapping):
        return RequestWindow()
    payload = health.get("request_window")
    if not isinstance(payload, Mapping):
        return RequestWindow()
    return RequestWindow(
        attempts=max(int(payload.get("attempts", 0)), 0),
        retries=max(int(payload.get("retries", 0)), 0),
        throttled=max(int(payload.get("throttled", 0)), 0),
        failures=max(int(payload.get("failures", 0)), 0),
        response_bytes=max(int(payload.get("response_bytes", 0)), 0),
    )


def float_or_none(value: Any) -> float | None:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None
