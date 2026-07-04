from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from spx_spark.config import StorageSettings
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    Provider,
    Quote,
)
from spx_spark.storage import JsonlQuoteWriter, LatestStateStore


def make_storage_settings(tmp_path) -> StorageSettings:
    return StorageSettings(
        data_root=str(tmp_path / "data"),
        latest_state_path=str(tmp_path / "data" / "latest" / "state.json"),
        raw_file_name="quotes.jsonl",
        include_raw_payload=False,
        latest_stale_after_seconds=15.0,
    )


def make_quote(
    *,
    provider: Provider,
    quality: MarketDataQuality,
    mark: float,
    received_at: datetime,
    quote_time: datetime | None = None,
) -> Quote:
    return Quote(
        instrument=InstrumentId.index("SPX"),
        provider=provider,
        provider_symbol=f"{provider.value}:SPX",
        received_at=received_at,
        quality=quality,
        bid=mark - 0.5,
        ask=mark + 0.5,
        last=mark,
        mark=mark,
        quote_time=quote_time or received_at,
    )


def test_jsonl_writer_partitions_by_provider_date_and_hour(tmp_path):
    settings = make_storage_settings(tmp_path)
    writer = JsonlQuoteWriter(settings)
    now = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)
    quote = make_quote(
        provider=Provider.SCHWAB,
        quality=MarketDataQuality.LIVE,
        mark=7500,
        received_at=now,
    )

    result = writer.write_quotes([quote])

    assert result.row_count == 1
    assert len(result.paths) == 1
    path = tmp_path / "data" / "raw" / "provider=schwab" / "date=2026-07-06"
    path = path / "hour=13" / "quotes.jsonl"
    assert result.paths[0] == str(path)
    record = json.loads(path.read_text(encoding="utf-8").strip())
    assert record["instrument_id"] == "index:SPX"
    assert record["provider"] == "schwab"


def test_latest_state_falls_back_from_stale_ibkr_to_live_schwab(tmp_path):
    settings = make_storage_settings(tmp_path)
    store = LatestStateStore(settings)
    now = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)
    ibkr = make_quote(
        provider=Provider.IBKR,
        quality=MarketDataQuality.LIVE,
        mark=7500,
        received_at=now - timedelta(minutes=2),
        quote_time=now - timedelta(minutes=2),
    )
    schwab = make_quote(
        provider=Provider.SCHWAB,
        quality=MarketDataQuality.LIVE,
        mark=7501,
        received_at=now,
        quote_time=now,
    )

    result = store.update([ibkr, schwab], now=now)
    state = LatestStateStore(settings).load()
    best = state.best_quote("index:SPX")

    assert result.provider_quote_count == 2
    assert result.best_quote_count == 1
    assert best is not None
    assert best.provider == Provider.SCHWAB
    assert best.effective_price == 7501
    ibkr_state = [quote for quote in state.quotes if quote.provider == Provider.IBKR][0]
    assert ibkr_state.quality == MarketDataQuality.STALE


def test_latest_state_keeps_provider_latest_across_updates(tmp_path):
    settings = make_storage_settings(tmp_path)
    store = LatestStateStore(settings)
    now = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)
    store.update(
        [
            make_quote(
                provider=Provider.SCHWAB,
                quality=MarketDataQuality.LIVE,
                mark=7501,
                received_at=now,
            )
        ],
        now=now,
    )
    store.update(
        [
            make_quote(
                provider=Provider.IBKR,
                quality=MarketDataQuality.LIVE,
                mark=7502,
                received_at=now + timedelta(seconds=1),
            )
        ],
        now=now + timedelta(seconds=1),
    )

    state = store.load()
    assert len(state.quotes) == 2
    assert state.best_quote("index:SPX").provider == Provider.IBKR
