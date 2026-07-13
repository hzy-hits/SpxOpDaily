from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from spx_spark.config import StorageSettings
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    Provider,
    ProviderState,
    ProviderStatus,
    Quote,
)
from spx_spark.provider_adapter import (
    ProviderSnapshot,
    merge_provider_snapshots,
    persist_provider_snapshot,
    provider_state_from_quote_health,
)
from spx_spark.storage import LatestStateStore


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
    quote_time: datetime | None = None,
) -> Quote:
    return Quote(
        instrument=InstrumentId.index("SPX"),
        provider=provider,
        received_at=received_at,
        quality=quality,
        provider_symbol=f"{provider.value}:SPX",
        mark=mark,
        quote_time=quote_time or received_at,
    )


def make_state(provider: Provider, *, checked_at: datetime) -> ProviderState:
    return ProviderState(
        provider=provider,
        status=ProviderStatus.AVAILABLE,
        checked_at=checked_at,
        connected=True,
        authenticated=True,
        priority=0,
    )


def test_provider_snapshot_rejects_mismatched_provider_quote():
    now = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)
    quote = make_quote(
        provider=Provider.SCHWAB,
        quality=MarketDataQuality.LIVE,
        mark=7500,
        received_at=now,
    )

    with pytest.raises(ValueError, match="mismatched provider"):
        ProviderSnapshot(provider=Provider.IBKR, received_at=now, quotes=(quote,))


def test_provider_state_is_degraded_when_all_priced_quotes_are_stale() -> None:
    now = datetime(2026, 7, 13, 5, 30, tzinfo=timezone.utc)
    quote = make_quote(
        provider=Provider.SCHWAB,
        quality=MarketDataQuality.STALE,
        mark=7500,
        received_at=now,
        quote_time=now - timedelta(days=3),
    )

    state = provider_state_from_quote_health(
        Provider.SCHWAB,
        (quote,),
        checked_at=now,
        connected=True,
        authenticated=True,
        latency_ms=None,
        priority=1,
    )

    assert state.status is ProviderStatus.DEGRADED
    assert state.reason == "connected but all priced quotes are stale"


def test_merge_provider_snapshots_feeds_normalized_fallback():
    now = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)
    ibkr_quote = make_quote(
        provider=Provider.IBKR,
        quality=MarketDataQuality.STALE,
        mark=7500,
        received_at=now - timedelta(minutes=2),
        quote_time=now - timedelta(minutes=2),
    )
    schwab_quote = make_quote(
        provider=Provider.SCHWAB,
        quality=MarketDataQuality.LIVE,
        mark=7501,
        received_at=now,
    )
    ibkr_snapshot = ProviderSnapshot(
        provider=Provider.IBKR,
        received_at=ibkr_quote.received_at,
        quotes=(ibkr_quote,),
        provider_states=(make_state(Provider.IBKR, checked_at=ibkr_quote.received_at),),
    )
    schwab_snapshot = ProviderSnapshot(
        provider=Provider.SCHWAB,
        received_at=now,
        quotes=(schwab_quote,),
        provider_states=(make_state(Provider.SCHWAB, checked_at=now),),
    )

    merged = merge_provider_snapshots([ibkr_snapshot, schwab_snapshot], created_at=now)
    best = merged.best_quote("index:SPX")

    assert len(merged.quotes) == 2
    assert len(merged.provider_states) == 2
    assert best is not None
    assert best.provider == Provider.SCHWAB


def test_persist_provider_snapshot_writes_raw_and_latest(tmp_path):
    settings = make_storage_settings(tmp_path)
    now = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)
    quote = make_quote(
        provider=Provider.HYPERLIQUID,
        quality=MarketDataQuality.LIVE,
        mark=7493.5,
        received_at=now,
    )
    snapshot = ProviderSnapshot(
        provider=Provider.HYPERLIQUID,
        received_at=now,
        quotes=(quote,),
        provider_states=(make_state(Provider.HYPERLIQUID, checked_at=now),),
    )

    result = persist_provider_snapshot(snapshot, settings)
    state = LatestStateStore(settings).load(now=now)

    assert result.updated_quote_count == 1
    assert result.best_quote_count == 1
    assert len(result.raw_paths) == 1
    raw_path = next(iter(result.raw_paths))
    record = json.loads(Path(raw_path).read_text(encoding="utf-8").splitlines()[0])
    assert record["provider"] == "hyperliquid"
    assert state.best_quote("index:SPX").provider == Provider.HYPERLIQUID


def test_persist_provider_snapshot_can_replace_ibkr_quotes(tmp_path):
    settings = make_storage_settings(tmp_path)
    store = LatestStateStore(settings)
    now = datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc)
    old_option = Quote(
        instrument=InstrumentId.option("SPX", expiry="20260706", strike=7300, right="C", trading_class="SPXW"),
        provider=Provider.IBKR,
        received_at=now,
        quality=MarketDataQuality.STALE,
        provider_symbol="option:SPXW:20260706:7300:C",
        mark=1.0,
        quote_time=now - timedelta(days=2),
    )
    other_provider = make_quote(
        provider=Provider.HYPERLIQUID,
        quality=MarketDataQuality.LIVE,
        mark=7493.5,
        received_at=now,
    )
    store.update([old_option, other_provider], now=now)

    new_option = Quote(
        instrument=InstrumentId.option("SPX", expiry="20260706", strike=7500, right="C", trading_class="SPXW"),
        provider=Provider.IBKR,
        received_at=now + timedelta(seconds=12),
        quality=MarketDataQuality.LIVE,
        provider_symbol="option:SPXW:20260706:7500:C",
        mark=20.0,
        quote_time=now + timedelta(seconds=12),
    )
    index_quote = make_quote(
        provider=Provider.IBKR,
        quality=MarketDataQuality.LIVE,
        mark=7524.0,
        received_at=now + timedelta(seconds=12),
    )
    snapshot = ProviderSnapshot(
        provider=Provider.IBKR,
        received_at=now + timedelta(seconds=12),
        quotes=(index_quote, new_option),
        provider_states=(make_state(Provider.IBKR, checked_at=now + timedelta(seconds=12)),),
        metadata={"replace_provider_quotes": True},
    )

    persist_provider_snapshot(snapshot, settings)
    state = store.load(now=now + timedelta(seconds=12))
    quote_ids = {quote.instrument.canonical_id for quote in state.quotes}

    assert "option:SPX:SPXW:20260706:7300:C" not in quote_ids
    assert "option:SPX:SPXW:20260706:7500:C" in quote_ids
    assert "index:SPX" in quote_ids
    assert any(quote.provider == Provider.HYPERLIQUID for quote in state.quotes)
