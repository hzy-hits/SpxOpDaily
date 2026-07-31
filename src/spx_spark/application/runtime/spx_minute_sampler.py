"""Independent RTH official-SPX minute sampler with explicit loss telemetry."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import signal
import threading
import time
import uuid
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from pathlib import Path

from spx_spark.application.market_features.market import (
    freshest_quote,
    normalized_quote,
    quote_source_at,
)
from spx_spark.application.market_features.spx_standardized import (
    canonical_spx_minute_state_path,
)
from spx_spark.config import StorageSettings
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import (
    FUTURE_TIMESTAMP_TOLERANCE_SECONDS,
    MarketDataQuality,
    Provider,
    Quote,
    as_utc,
    instrument_matches_id,
)
from spx_spark.settings.loader import load_app_settings
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock
from spx_spark.storage import LatestState, LatestStateStore


TASK_NAME = "spx_minute_sampler"
SCHEMA_VERSION = 1
DEFAULT_INTERVAL_SECONDS = 5.0


def canonical_state_path(storage: StorageSettings) -> Path:
    return canonical_spx_minute_state_path(storage)


def lease_path(storage: StorageSettings) -> Path:
    return Path(storage.data_root) / "latest" / "spx_minute_sampler_lease.json"


def sample_spx_minute_once(
    *,
    storage: StorageSettings,
    policy: MarketFeatureSettings,
    now: datetime,
    latest_state: LatestState | None = None,
    writer_instance_id: str | None = None,
) -> dict[str, object]:
    """Persist the current RTH minute even when no official SPX quote survives."""

    observed_at = as_utc(now)
    writer_instance_id = writer_instance_id or f"direct-{os.getpid()}"
    if not DEFAULT_MARKET_CALENDAR.is_rth_open(observed_at):
        result = {
            "ok": True,
            "at": observed_at.isoformat(),
            "accepted": False,
            "rejection": "official_spx_not_expected_outside_rth",
            "official_spx_expected": False,
            "writer_instance_id": writer_instance_id,
        }
        atomic_write_json_secure(lease_path(storage), result)
        return result

    state = latest_state or LatestStateStore(storage).load(now=observed_at)
    exchange_date = DEFAULT_MARKET_CALENDAR.research_expiry(observed_at)
    session = DEFAULT_MARKET_CALENDAR.session(exchange_date)
    if session is None or not (session.open_at <= observed_at < session.close_at):
        raise RuntimeError("RTH calendar invariant failed")
    minute = observed_at.replace(second=0, microsecond=0)
    diagnostics = [
        _provider_diagnostic(
            state.quotes,
            provider=provider,
            now=observed_at,
            policy=policy,
            snapshot_generation=state.created_at,
        )
        for provider in (Provider.SCHWAB, Provider.IBKR)
    ]
    selected_quote = freshest_quote(
        state.quotes,
        instrument_id="index:SPX",
        now=observed_at,
        policy=policy,
        provider=Provider.SCHWAB,
    ) or freshest_quote(
        state.quotes,
        instrument_id="index:SPX",
        now=observed_at,
        policy=policy,
        provider=Provider.IBKR,
    )
    selected_provider = selected_quote.provider.value if selected_quote is not None else None
    diagnostics = [
        {
            **item,
            "selected": item.get("provider") == selected_provider,
            "selection_outcome": (
                "selected"
                if item.get("provider") == selected_provider
                else "not_selected_provider_priority"
                if item.get("selected_eligible") is True
                else "rejected"
            ),
        }
        for item in diagnostics
    ]
    selected = normalized_quote(selected_quote) if selected_quote is not None else None
    drop_reasons = sorted(
        {
            str(item["drop_reason"])
            for item in diagnostics
            if item.get("selected_eligible") is not True and item.get("drop_reason")
        }
    )
    row: dict[str, object] = {
        "minute": minute.isoformat(),
        "observed_at": observed_at.isoformat(),
        "session_date": exchange_date.isoformat(),
        "official_spx_expected": True,
        "status": "selected" if selected is not None else "missing",
        "selected": selected,
        "selected_provider": selected.get("provider") if selected else None,
        "provider_diagnostics": diagnostics,
        "drop_reasons": drop_reasons,
        "snapshot_generation": state.created_at.isoformat(),
        "snapshot_as_of": state.as_of.isoformat(),
        "writer_instance_id": writer_instance_id,
        "synthetic_price": False,
    }
    path = canonical_state_path(storage)
    with exclusive_state_lock(path):
        payload = _load_state(path)
        rows = [dict(item) for item in payload.get("rows") or [] if isinstance(item, Mapping)]
        rows = _upsert_and_mark_gaps(rows, row, session_open=session.open_at)
        cutoff = observed_at - timedelta(days=5)
        rows = [item for item in rows if (_parse_at(item.get("minute")) or observed_at) >= cutoff]
        coverage = _coverage(rows, session_date=exchange_date.isoformat(), now=observed_at)
        payload = {
            "schema_version": SCHEMA_VERSION,
            "kind": "standardized_official_spx_minutes",
            "updated_at": observed_at.isoformat(),
            "rth_only": True,
            "gth_contract": "ibkr_spxw_parity_then_es_basis_no_cash_spx_required",
            "rows": rows,
            "coverage": coverage,
        }
        atomic_write_json_secure(path, payload)
    _append_audit(storage, observed_at, row)
    result = {
        "ok": True,
        "at": observed_at.isoformat(),
        "accepted": selected is not None,
        "source_at": selected.get("source_at") if selected else None,
        "rejection": None if selected is not None else "official_spx_quote_unavailable",
        "official_spx_expected": True,
        "minute": minute.isoformat(),
        "selected_provider": selected.get("provider") if selected else None,
        "coverage": coverage,
        "writer_instance_id": writer_instance_id,
    }
    atomic_write_json_secure(lease_path(storage), result)
    return result


def _provider_diagnostic(
    quotes: tuple[Quote, ...],
    *,
    provider: Provider,
    now: datetime,
    policy: MarketFeatureSettings,
    snapshot_generation: datetime,
) -> dict[str, object]:
    candidates = [
        quote
        for quote in quotes
        if quote.provider is provider and instrument_matches_id(quote.instrument, "index:SPX")
    ]
    if not candidates:
        return {
            "provider": provider.value,
            "raw_present": False,
            "selected_eligible": False,
            "drop_reason": "raw_quote_absent",
            "source_age_seconds": None,
            "transport_age_seconds": None,
            "quote_quality": None,
            "snapshot_generation": snapshot_generation.isoformat(),
        }
    quote = max(candidates, key=lambda item: quote_source_at(item))
    source_at = quote.quote_time or quote.trade_time
    transport_at = quote.last_update_at or quote.received_at
    source_age = (now - as_utc(source_at)).total_seconds() if source_at is not None else None
    transport_age = (now - as_utc(transport_at)).total_seconds()
    reason = _drop_reason(
        quote,
        source_age=source_age,
        transport_age=transport_age,
        max_age_seconds=policy.max_quote_age_seconds,
    )
    return {
        "provider": provider.value,
        "raw_present": True,
        "selected_eligible": reason is None,
        "drop_reason": reason,
        "source_age_seconds": source_age,
        "transport_age_seconds": transport_age,
        "quote_quality": quote.quality.value,
        "market_data_type": quote.market_data_type,
        "bid": quote.bid,
        "ask": quote.ask,
        "last": quote.last,
        "quote_time": quote.quote_time.isoformat() if quote.quote_time else None,
        "trade_time": quote.trade_time.isoformat() if quote.trade_time else None,
        "received_at": quote.received_at.isoformat(),
        "snapshot_generation": snapshot_generation.isoformat(),
    }


def _drop_reason(
    quote: Quote,
    *,
    source_age: float | None,
    transport_age: float,
    max_age_seconds: float,
) -> str | None:
    if quote.quality is not MarketDataQuality.LIVE:
        return f"quote_quality:{quote.quality.value}"
    if quote.mid is None and not (quote.last is not None and quote.last > 0):
        return "price_unavailable_or_crossed"
    if source_age is None:
        return "source_timestamp_unavailable"
    if min(source_age, transport_age) < -FUTURE_TIMESTAMP_TOLERANCE_SECONDS:
        return "timestamp_in_future"
    if source_age > max_age_seconds:
        return "source_stale"
    if transport_age > max_age_seconds:
        return "transport_stale"
    return None


def _upsert_and_mark_gaps(
    rows: list[dict[str, object]],
    row: dict[str, object],
    *,
    session_open: datetime,
) -> list[dict[str, object]]:
    current = _parse_at(row.get("minute"))
    assert current is not None
    same_session = [item for item in rows if item.get("session_date") == row.get("session_date")]
    prior_times = [
        parsed
        for item in same_session
        if (parsed := _parse_at(item.get("minute"))) is not None and parsed < current
    ]
    prior = max(prior_times) if prior_times else session_open - timedelta(minutes=1)
    next_minute = max(prior + timedelta(minutes=1), session_open)
    while next_minute < current:
        rows.append(
            {
                "minute": next_minute.isoformat(),
                "observed_at": row["observed_at"],
                "session_date": row["session_date"],
                "official_spx_expected": True,
                "status": "missing",
                "selected": None,
                "selected_provider": None,
                "provider_diagnostics": [],
                "drop_reasons": ["sampler_process_gap"],
                "snapshot_generation": row["snapshot_generation"],
                "writer_instance_id": row["writer_instance_id"],
                "synthetic_price": False,
            }
        )
        next_minute += timedelta(minutes=1)
    rows = [item for item in rows if item.get("minute") != row.get("minute")]
    rows.append(row)
    rows.sort(key=lambda item: str(item.get("minute") or ""))
    return rows


def _coverage(
    rows: list[dict[str, object]],
    *,
    session_date: str,
    now: datetime,
) -> dict[str, object]:
    session_rows = [row for row in rows if row.get("session_date") == session_date]
    persisted = len({row.get("minute") for row in session_rows})
    selected = sum(isinstance(row.get("selected"), Mapping) for row in session_rows)
    raw = sum(
        any(
            isinstance(item, Mapping) and item.get("raw_present") is True
            for item in row.get("provider_diagnostics") or ()
        )
        for row in session_rows
    )
    session = DEFAULT_MARKET_CALENDAR.session(datetime.fromisoformat(session_date).date())
    expected = 390
    if session is not None and now < session.close_at:
        expected = max(int((now - session.open_at).total_seconds() // 60) + 1, 1)
    return {
        "session_date": session_date,
        "expected_minutes": expected,
        "raw_latest_state_minutes": raw,
        "selected_minutes": selected,
        "persisted_minutes": persisted,
        "raw_coverage": round(min(raw / expected, 1.0), 6),
        "selected_coverage": round(min(selected / expected, 1.0), 6),
        "persisted_coverage": round(min(persisted / expected, 1.0), 6),
        "semantics": {
            "raw": "latest_state_contains_any_provider_spx",
            "selected": "fresh_live_official_spx_survived_normalization",
            "persisted": "minute_row_written_even_when_price_missing",
        },
    }


def _append_audit(storage: StorageSettings, now: datetime, row: Mapping[str, object]) -> None:
    path = (
        Path(storage.data_root)
        / "features"
        / "spx_standardized_samples"
        / f"date={now.date().isoformat()}"
        / "events.jsonl"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        os.write(fd, (json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n").encode())
        os.fsync(fd)
    finally:
        os.close(fd)


def _load_state(path: Path) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(value) if isinstance(value, Mapping) else {}


def _parse_at(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return as_utc(datetime.fromisoformat(value))
    except ValueError:
        return None


def run(
    *,
    interval_seconds: float = DEFAULT_INTERVAL_SECONDS,
    lock_path: str | None = None,
    max_cycles: int | None = None,
) -> int:
    settings = load_app_settings()
    storage = StorageSettings.from_env()
    policy = settings.market_features
    instance_id = str(uuid.uuid4())
    stop = threading.Event()
    for signal_number in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signal_number, lambda *_args: stop.set())
    lock_file = Path(lock_path or "/tmp/spx-spark-spx-minute-sampler.lock")
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    with lock_file.open("a", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 73
        cycles = 0
        while not stop.is_set():
            started = time.monotonic()
            result = sample_spx_minute_once(
                storage=storage,
                policy=policy,
                now=datetime.now(tz=timezone.utc),
                writer_instance_id=instance_id,
            )
            print(json.dumps({"task": TASK_NAME, **result}, sort_keys=True), flush=True)
            cycles += 1
            if max_cycles is not None and cycles >= max_cycles:
                break
            stop.wait(max(interval_seconds - (time.monotonic() - started), 0.0))
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval-seconds", type=float, default=DEFAULT_INTERVAL_SECONDS)
    parser.add_argument("--lock-path")
    parser.add_argument("--max-cycles", type=int)
    args = parser.parse_args()
    raise SystemExit(
        run(
            interval_seconds=args.interval_seconds,
            lock_path=args.lock_path,
            max_cycles=args.max_cycles,
        )
    )


if __name__ == "__main__":
    main()
