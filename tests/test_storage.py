from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from spx_spark.config import StorageSettings
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    Provider,
    ProviderState,
    ProviderStatus,
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
        slow_index_stale_after_seconds=300.0,
        slow_index_labels=frozenset({"index:SKEW", "index:VVIX"}),
    )


def make_quote(
    *,
    provider: Provider,
    quality: MarketDataQuality,
    mark: float,
    received_at: datetime,
    symbol: str = "SPX",
    quote_time: datetime | None = None,
) -> Quote:
    return Quote(
        instrument=InstrumentId.index(symbol),
        provider=provider,
        provider_symbol=f"{provider.value}:{symbol}",
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
    state = LatestStateStore(settings).load(now=now)
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


def test_latest_state_can_replace_a_dynamic_provider_quote_set(tmp_path):
    settings = make_storage_settings(tmp_path)
    store = LatestStateStore(settings)
    now = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)
    store.update(
        [
            make_quote(
                provider=Provider.POLYMARKET,
                quality=MarketDataQuality.LIVE,
                mark=0.6,
                received_at=now,
                symbol="POLY-OLD",
            ),
            make_quote(
                provider=Provider.IBKR,
                quality=MarketDataQuality.LIVE,
                mark=7500,
                received_at=now,
            ),
        ],
        now=now,
    )

    store.update(
        [
            make_quote(
                provider=Provider.POLYMARKET,
                quality=MarketDataQuality.LIVE,
                mark=0.7,
                received_at=now + timedelta(seconds=30),
                symbol="POLY-NEW",
            )
        ],
        now=now + timedelta(seconds=30),
        replace_providers=(Provider.POLYMARKET,),
    )
    state = store.load(now=now + timedelta(seconds=30))

    quote_ids = {quote.instrument.canonical_id for quote in state.quotes}
    assert "index:POLY-OLD" not in quote_ids
    assert "index:POLY-NEW" in quote_ids
    assert "index:SPX" in quote_ids


def test_latest_state_concurrent_updates_do_not_lose_quotes(tmp_path):
    settings = make_storage_settings(tmp_path)
    now = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)
    symbols = [f"SYM{i}" for i in range(20)]

    def update_one(symbol: str) -> None:
        LatestStateStore(settings).update(
            [
                make_quote(
                    provider=Provider.IBKR,
                    quality=MarketDataQuality.LIVE,
                    mark=7500,
                    received_at=now,
                    symbol=symbol,
                )
            ],
            now=now,
        )

    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(update_one, symbols))

    state = LatestStateStore(settings).load(now=now)
    quote_ids = {quote.instrument.canonical_id for quote in state.quotes}
    assert quote_ids == {f"index:{symbol}" for symbol in symbols}


def test_latest_state_round_trips_provider_state(tmp_path):
    settings = make_storage_settings(tmp_path)
    store = LatestStateStore(settings)
    now = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)
    provider_state = ProviderState(
        provider=Provider.IBKR,
        status=ProviderStatus.UNAVAILABLE,
        checked_at=now,
        reason="runtime policy blocks IBKR collection",
        connected=False,
        authenticated=None,
        latency_ms=None,
        priority=0,
    )

    store.update([], now=now, provider_states=[provider_state])
    state = LatestStateStore(settings).load(now=now)

    assert len(state.provider_states) == 1
    assert state.provider_states[0].provider == Provider.IBKR
    assert state.provider_states[0].status == ProviderStatus.UNAVAILABLE
    assert state.provider_states[0].connected is False


def test_latest_state_merges_provider_states_across_provider_updates(tmp_path):
    settings = make_storage_settings(tmp_path)
    store = LatestStateStore(settings)
    now = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)
    ibkr_state = ProviderState(
        provider=Provider.IBKR,
        status=ProviderStatus.UNAVAILABLE,
        checked_at=now,
        reason="competing session",
        connected=False,
        authenticated=False,
        priority=0,
    )
    hyperliquid_state = ProviderState(
        provider=Provider.HYPERLIQUID,
        status=ProviderStatus.AVAILABLE,
        checked_at=now + timedelta(seconds=30),
        connected=True,
        authenticated=None,
        priority=0,
    )

    store.update([], now=now, provider_states=[ibkr_state])
    store.update(
        [
            make_quote(
                provider=Provider.HYPERLIQUID,
                quality=MarketDataQuality.LIVE,
                mark=7505,
                received_at=now + timedelta(seconds=30),
            )
        ],
        now=now + timedelta(seconds=30),
        provider_states=[hyperliquid_state],
    )
    state = LatestStateStore(settings).load(now=now + timedelta(seconds=30))

    states_by_provider = {item.provider: item for item in state.provider_states}
    assert states_by_provider[Provider.IBKR].status == ProviderStatus.UNAVAILABLE
    assert states_by_provider[Provider.HYPERLIQUID].status == ProviderStatus.AVAILABLE
