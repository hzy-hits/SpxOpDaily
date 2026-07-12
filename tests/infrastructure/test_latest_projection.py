"""LatestMarketProjectionStore boundary tests."""

from __future__ import annotations

from datetime import datetime, timezone

from spx_spark.config import StorageSettings
from spx_spark.infrastructure.market_data.latest_projection import (
    LatestMarketProjectionStore,
)
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    Provider,
    ProviderState,
    ProviderStatus,
    Quote,
)
from spx_spark.storage import LatestMarketProjectionStore as StorageProjection


NOW = datetime(2026, 7, 11, 18, 0, tzinfo=timezone.utc)


def _settings(tmp_path) -> StorageSettings:
    return StorageSettings(
        data_root=str(tmp_path / "data"),
        latest_state_path=str(tmp_path / "data" / "latest" / "state.json"),
        raw_file_name="quotes.jsonl",
        include_raw_payload=False,
        latest_stale_after_seconds=15.0,
        slow_index_stale_after_seconds=300.0,
        slow_index_labels=frozenset(),
        provider_priority=("schwab", "ibkr"),
    )


def test_projection_store_update_and_load_roundtrip(tmp_path) -> None:
    assert LatestMarketProjectionStore is StorageProjection
    store = LatestMarketProjectionStore(_settings(tmp_path))
    quote = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.SCHWAB,
        provider_symbol="schwab:SPX",
        received_at=NOW,
        quality=MarketDataQuality.LIVE,
        bid=5000.0,
        ask=5001.0,
        last=5000.5,
        mark=5000.5,
        quote_time=NOW,
    )
    result = store.update(
        [quote],
        now=NOW,
        provider_states=[
            ProviderState(
                provider=Provider.SCHWAB,
                status=ProviderStatus.AVAILABLE,
                checked_at=NOW,
            )
        ],
    )
    assert result.updated_quote_count == 1
    state = store.load(now=NOW)
    symbols = {q.instrument.symbol for q in state.quotes}
    assert "SPX" in symbols
