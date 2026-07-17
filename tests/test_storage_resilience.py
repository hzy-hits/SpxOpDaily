from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

from spx_spark.config import StorageSettings
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    Provider,
    Quote,
    QuoteFreshness,
    quote_use_decision,
)
from spx_spark.storage import LatestStateStore, latest_by_provider


def make_storage_settings(tmp_path) -> StorageSettings:
    return StorageSettings(
        data_root=str(tmp_path / "data"),
        latest_state_path=str(tmp_path / "data" / "latest" / "state.json"),
        raw_file_name="quotes.jsonl",
        include_raw_payload=False,
        latest_stale_after_seconds=15.0,
        slow_index_stale_after_seconds=300.0,
        slow_index_labels=frozenset(),
        provider_priority=("ibkr", "schwab"),
    )


def make_index_quote(*, received_at: datetime) -> Quote:
    return Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.SCHWAB,
        provider_symbol="$SPX",
        received_at=received_at,
        quality=MarketDataQuality.LIVE,
        bid=7499.5,
        ask=7500.5,
        last=7500.0,
        mark=7500.0,
        quote_time=received_at,
    )


def make_schwab_option_quote(*, received_at: datetime, quote_time: datetime) -> Quote:
    return Quote(
        instrument=InstrumentId.option(
            "SPX",
            expiry="20260713",
            strike=7500.0,
            right="C",
            trading_class="SPXW",
        ),
        provider=Provider.SCHWAB,
        provider_symbol="SPXW  260713C07500000",
        received_at=received_at,
        quality=MarketDataQuality.LIVE,
        bid=10.0,
        ask=12.0,
        last=11.0,
        quote_time=quote_time,
    )


def make_missing_quote(priced: Quote, *, received_at: datetime) -> Quote:
    # Mirrors schwab/adapter.py's placeholder for a symbol absent from a
    # partial batch response: no prices and no source timestamps.
    return Quote(
        instrument=priced.instrument,
        provider=priced.provider,
        provider_symbol=priced.provider_symbol,
        received_at=received_at,
        quality=MarketDataQuality.MISSING,
        error="symbol missing from Schwab payload",
    )


def test_load_quarantines_corrupt_state_and_returns_empty(tmp_path, caplog) -> None:
    settings = make_storage_settings(tmp_path)
    store = LatestStateStore(settings)
    store.path.parent.mkdir(parents=True, exist_ok=True)
    store.path.write_text('{"quotes": [broken', encoding="utf-8")
    now = datetime(2026, 7, 13, 14, 0, tzinfo=timezone.utc)

    with caplog.at_level(logging.WARNING, logger="spx_spark.storage"):
        state = store.load(now=now)

    assert state.quotes == ()
    assert state.best_quotes == ()
    quarantine = store.path.with_name(f"{store.path.name}.corrupt-{int(now.timestamp())}")
    assert not store.path.exists()
    assert quarantine.read_text(encoding="utf-8") == '{"quotes": [broken'
    assert "latest state unreadable" in caplog.text


def test_update_recovers_from_corrupt_state(tmp_path) -> None:
    settings = make_storage_settings(tmp_path)
    store = LatestStateStore(settings)
    store.path.parent.mkdir(parents=True, exist_ok=True)
    store.path.write_text("not json at all", encoding="utf-8")
    now = datetime(2026, 7, 13, 14, 0, tzinfo=timezone.utc)

    result = store.update([make_index_quote(received_at=now)], now=now)

    assert result.provider_quote_count == 1
    state = LatestStateStore(settings).load(now=now)
    assert [quote.instrument.canonical_id for quote in state.quotes] == ["index:SPX"]
    quarantine = store.path.with_name(f"{store.path.name}.corrupt-{int(now.timestamp())}")
    assert quarantine.exists()


def test_write_fsyncs_file_and_directory(tmp_path, monkeypatch) -> None:
    settings = make_storage_settings(tmp_path)
    store = LatestStateStore(settings)
    fsync_calls: list[int] = []
    real_fsync = os.fsync

    def spy_fsync(fd: int) -> None:
        fsync_calls.append(fd)
        real_fsync(fd)

    monkeypatch.setattr("spx_spark.storage.os.fsync", spy_fsync)
    now = datetime(2026, 7, 13, 14, 0, tzinfo=timezone.utc)

    store.update([make_index_quote(received_at=now)], now=now)

    assert len(fsync_calls) == 2  # temp file before rename, then parent directory


def test_merge_keeps_priced_row_over_newer_missing_placeholder() -> None:
    base = datetime(2026, 7, 13, 14, 0, tzinfo=timezone.utc)
    priced = make_schwab_option_quote(received_at=base, quote_time=base)
    missing = make_missing_quote(priced, received_at=base + timedelta(seconds=5))

    merged = latest_by_provider((priced, missing))[0]

    assert (merged.bid, merged.ask, merged.last) == (10.0, 12.0, 11.0)
    assert merged.received_at == base + timedelta(seconds=5)
    # The preserved price keeps its old source timestamp so downstream
    # staleness checks still fail closed instead of freshening it.
    assert merged.quote_time == base


def test_missing_partial_response_marks_strike_stale_not_priceless(tmp_path) -> None:
    settings = make_storage_settings(tmp_path)
    store = LatestStateStore(settings)
    base = datetime(2026, 7, 13, 14, 0, tzinfo=timezone.utc)
    priced = make_schwab_option_quote(received_at=base, quote_time=base)
    store.update([priced], now=base)

    missing_at = base + timedelta(seconds=30)
    store.update([make_missing_quote(priced, received_at=missing_at)], now=missing_at)

    state = store.load(now=missing_at)
    assert len(state.quotes) == 1
    merged = state.quotes[0]
    assert (merged.bid, merged.ask, merged.last) == (10.0, 12.0, 11.0)
    assert merged.quality == MarketDataQuality.STALE
    assert merged.quote_time == base
    assert merged.received_at == missing_at
    decision = quote_use_decision(
        merged,
        as_of=missing_at,
        stale_after_seconds=settings.latest_stale_after_seconds,
    )
    assert decision.freshness == QuoteFreshness.STALE
    assert not decision.pricing_allowed
