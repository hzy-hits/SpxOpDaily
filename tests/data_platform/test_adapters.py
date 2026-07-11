from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from spx_spark.config import StorageSettings
from spx_spark.data_platform.adapters.jsonl_landing import JsonlQuoteLandingWriter
from spx_spark.data_platform.adapters.parquet_lake import ParquetHistoricalLake
from spx_spark.data_platform.contracts import LakePartition
from spx_spark.data_platform.lake.compact import QuoteLakeCompactor
from spx_spark.marketdata import InstrumentId, InstrumentType, MarketDataQuality, Provider, Quote


UTC = timezone.utc


def storage_settings(tmp_path) -> StorageSettings:
    return StorageSettings(
        data_root=str(tmp_path),
        latest_state_path=str(tmp_path / "latest" / "state.json"),
        raw_file_name="quotes.jsonl",
        include_raw_payload=False,
        latest_stale_after_seconds=15,
        slow_index_stale_after_seconds=300,
        slow_index_labels=frozenset(),
    )


def test_jsonl_landing_adapter_preserves_existing_partition_writer(tmp_path) -> None:
    writer = JsonlQuoteLandingWriter(storage_settings(tmp_path))
    at = datetime(2026, 7, 10, 14, 0, tzinfo=UTC)
    quote = Quote(
        instrument=InstrumentId("SPX", InstrumentType.INDEX, "SPX", "CBOE", "USD"),
        provider=Provider.IBKR,
        provider_symbol="SPX",
        received_at=at,
        quality=MarketDataQuality.LIVE,
        last=6300.0,
    )

    receipt = writer.append_quotes((quote,))

    assert receipt.row_count == 1
    assert sum(receipt.path_counts.values()) == 1
    assert next(iter(receipt.path_counts)).endswith(
        "raw/provider=ibkr/date=2026-07-10/hour=14/quotes.jsonl"
    )


def test_parquet_lake_adapter_rejects_partition_mismatch(tmp_path) -> None:
    path = tmp_path / "raw/provider=ibkr/date=2026-07-10/hour=14/quotes.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text("{}\n", encoding="utf-8")
    lake = ParquetHistoricalLake(QuoteLakeCompactor(tmp_path, settle_seconds=0))
    logical = LakePartition(
        dataset="quotes",
        schema_version="v1",
        session_date=date(2026, 7, 10),
        provider="ibkr",
        hour=15,
    )

    with pytest.raises(ValueError, match="does not match"):
        lake.publish_partition(
            logical,
            path,
            as_of=datetime(2026, 7, 10, 16, tzinfo=UTC) + timedelta(hours=1),
        )
