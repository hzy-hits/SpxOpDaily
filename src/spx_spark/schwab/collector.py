"""Schwab option-chain collector: fetch chains and persist normalized quotes."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError

from spx_spark.config import SchwabSettings, SchwabStreamSettings, StorageSettings, env_csv
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET
from spx_spark.marketdata import as_utc
from spx_spark.provider_adapter import persist_provider_snapshot
from spx_spark.schwab.adapter import snapshot_from_chain_payload, snapshot_from_quote_payload
from spx_spark.schwab.symbols import (
    canonical_underlier_for_schwab,
    chain_interval_seconds_for,
    option_chain_strike_count_for,
    option_chain_symbol_for_schwab,
    resolved_schwab_canonical_quote_symbols,
    resolved_schwab_quote_symbols,
    schwab_option_chain_underliers,
    schwab_quote_symbols,
)
from spx_spark.schwab.verifier import SchwabClient, build_schwab_client, quote_batches


SCHWAB_QUOTE_PATH = '/marketdata/v1/quotes'
SCHWAB_OPTION_CHAIN_PATH = '/marketdata/v1/chains'
COLLECTOR_STATE_FILE_NAME = "schwab_collector_state.json"
REQUEST_BUDGET_WARNING_PER_MINUTE = int(
    100
)
LOGGER = logging.getLogger(__name__)


@dataclass
class CollectorBudgetState:
    """Disk-backed cadence and rolling request timestamps across collector subprocesses."""

    chain_last_fetched_at: dict[str, datetime] = field(default_factory=dict)
    request_timestamps: list[float] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "chain_last_fetched_at": {
                symbol: stamp.isoformat()
                for symbol, stamp in sorted(self.chain_last_fetched_at.items())
            },
            "request_timestamps": list(self.request_timestamps),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "CollectorBudgetState":
        chain_raw = payload.get("chain_last_fetched_at", {})
        timestamps_raw = payload.get("request_timestamps", [])
        chain_last: dict[str, datetime] = {}
        if isinstance(chain_raw, dict):
            for symbol, value in chain_raw.items():
                stamp = _parse_iso_datetime(value)
                if stamp is not None:
                    chain_last[str(symbol).strip().upper()] = stamp
        timestamps: list[float] = []
        if isinstance(timestamps_raw, list):
            for item in timestamps_raw:
                try:
                    timestamps.append(float(item))
                except (TypeError, ValueError):
                    continue
        return cls(chain_last_fetched_at=chain_last, request_timestamps=timestamps)


def collector_state_path(storage_settings: StorageSettings) -> Path:
    return Path(storage_settings.data_root).expanduser() / "latest" / COLLECTOR_STATE_FILE_NAME


def load_collector_budget_state(path: Path) -> CollectorBudgetState:
    if not path.is_file():
        return CollectorBudgetState()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return CollectorBudgetState()
    if not isinstance(payload, dict):
        return CollectorBudgetState()
    return CollectorBudgetState.from_dict(payload)


def save_collector_budget_state(path: Path, state: CollectorBudgetState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state.to_dict(), sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _parse_iso_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return as_utc(stamp)


def chain_is_due(
    *,
    last_fetched_at: datetime | None,
    now: datetime,
    interval_seconds: int,
) -> bool:
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    if last_fetched_at is None:
        return True
    elapsed = (as_utc(now) - as_utc(last_fetched_at)).total_seconds()
    return elapsed >= float(interval_seconds)


def prune_request_timestamps(
    timestamps: list[float],
    *,
    now_epoch: float,
    window_seconds: float = 60.0,
) -> list[float]:
    cutoff = now_epoch - window_seconds
    return [stamp for stamp in timestamps if stamp >= cutoff]


def record_requests(
    state: CollectorBudgetState,
    *,
    count: int,
    now: datetime,
) -> int:
    """Append ``count`` request markers and return the trailing-60s total."""

    if count < 0:
        raise ValueError("request count cannot be negative")
    now_epoch = as_utc(now).timestamp()
    state.request_timestamps = prune_request_timestamps(
        state.request_timestamps,
        now_epoch=now_epoch,
    )
    if count:
        state.request_timestamps.extend([now_epoch] * count)
    state.request_timestamps = prune_request_timestamps(
        state.request_timestamps,
        now_epoch=now_epoch,
    )
    return len(state.request_timestamps)


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
    _status, payload = client.get_json(
        SCHWAB_OPTION_CHAIN_PATH,
        {
            "symbol": provider_symbol,
            "contractType": "ALL",
            "strategy": "SINGLE",
            "strikeCount": resolved_strike_count,
            "includeUnderlyingQuote": "true",
            "fromDate": current_expiry.isoformat(),
            "toDate": next_expiry.isoformat(),
        },
    )
    return payload


def run(argv: list[str] | None = None, *, now: datetime | None = None) -> int:
    del argv
    evaluation_now = as_utc(now or datetime.now(tz=ET))
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
    chain_canonicals = [canonical_underlier_for_schwab(symbol) for symbol in chain_symbols]

    state_path = collector_state_path(storage_settings)
    budget_state = load_collector_budget_state(state_path)

    quote_counts: dict[str, int] = {}
    errors: list[str] = []
    request_count = 0
    chains_fetched: list[str] = []
    chains_skipped: list[str] = []
    chain_as_of: dict[str, str | None] = {
        canonical: (
            budget_state.chain_last_fetched_at[canonical].isoformat()
            if canonical in budget_state.chain_last_fetched_at
            else None
        )
        for canonical in chain_canonicals
    }

    for batch in quote_batches(quote_symbols):
        label = ",".join(batch)
        try:
            payload = fetch_quotes(client, batch, settings)
            request_count += 1
            snapshot = snapshot_from_quote_payload(payload, batch, received_at=evaluation_now)
            persist_provider_snapshot(snapshot, storage_settings)
            quote_counts[f"quotes:{label}"] = snapshot.quote_count
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, ValueError) as exc:
            errors.append(f"quotes:{label}: {exc}")

    for symbol in chain_symbols:
        canonical = canonical_underlier_for_schwab(symbol)
        interval_seconds = chain_interval_seconds_for(canonical)
        last_fetched = budget_state.chain_last_fetched_at.get(canonical)
        if not chain_is_due(
            last_fetched_at=last_fetched,
            now=evaluation_now,
            interval_seconds=interval_seconds,
        ):
            chains_skipped.append(canonical)
            chain_as_of[canonical] = last_fetched.isoformat() if last_fetched is not None else None
            continue
        try:
            strike_count = option_chain_strike_count_for(
                canonical,
                settings.option_chain_strike_count,
            )
            payload = fetch_chain(
                client,
                symbol,
                settings,
                now=evaluation_now,
                strike_count=strike_count,
            )
            request_count += 1
            snapshot = snapshot_from_chain_payload(
                payload,
                underlier=canonical,
                received_at=evaluation_now,
            )
            persist_provider_snapshot(snapshot, storage_settings)
            quote_counts[canonical] = snapshot.quote_count
            budget_state.chain_last_fetched_at[canonical] = evaluation_now
            chains_fetched.append(canonical)
            chain_as_of[canonical] = evaluation_now.isoformat()
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError, ValueError) as exc:
            errors.append(f"{canonical}: {exc}")

    requests_last_minute = record_requests(
        budget_state,
        count=request_count,
        now=evaluation_now,
    )
    save_collector_budget_state(state_path, budget_state)

    if requests_last_minute > REQUEST_BUDGET_WARNING_PER_MINUTE:
        LOGGER.warning(
            "Schwab collector request budget soft guardrail exceeded: "
            "%s requests in trailing 60s (warning threshold %s/min, gateway cap 120/min)",
            requests_last_minute,
            REQUEST_BUDGET_WARNING_PER_MINUTE,
        )

    ok = bool(quote_counts) or bool(chains_skipped)
    summary = {
        "ok": ok,
        "symbols": list(quote_counts.keys()),
        "quote_counts": quote_counts,
        "errors": errors,
        "request_count": request_count,
        "requests_last_minute": requests_last_minute,
        "chains_fetched": chains_fetched,
        "chains_skipped": chains_skipped,
        "chain_as_of": chain_as_of,
    }
    print(json.dumps(summary, sort_keys=True))
    return 0 if ok or not quote_symbols and not chain_symbols else 1


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
