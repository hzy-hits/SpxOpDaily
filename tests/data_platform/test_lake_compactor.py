from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

from spx_spark.data_platform.lake.compact import QuoteLakeCompactor
from spx_spark.data_platform.lake.layout import discover_raw_quote_partitions
from spx_spark.data_platform.lake.manifest import load_manifest


NOW = datetime(2026, 7, 10, 13, 10, tzinfo=timezone.utc)


def quote_payload(
    *,
    received_at: str = "2026-07-10T10:05:00+00:00",
    provider: str = "ibkr",
    instrument_id: str = "option:SPX:SPXW:20260710:6200:C",
) -> dict[str, object]:
    return {
        "instrument": {
            "symbol": "SPX",
            "instrument_type": "option",
            "provider_symbol": "SPXW 260710C06200000",
            "exchange": "SMART",
            "currency": "USD",
            "expiry": "20260710",
            "strike": 6200.0,
            "right": "C",
            "multiplier": "100",
            "underlier": "SPX",
            "trading_class": "SPXW",
            "canonical_id": instrument_id,
        },
        "instrument_id": instrument_id,
        "provider": provider,
        "provider_symbol": "SPXW 260710C06200000",
        "received_at": received_at,
        "quality": "live",
        "bid": 10.0,
        "ask": 10.4,
        "last": 10.2,
        "mark": 10.2,
        "bid_size": 3.0,
        "ask_size": 4.0,
        "quote_time": "2026-07-10T10:04:59+00:00",
        "source_latency_ms": 1000.0,
        "market_data_type": 1,
        "greeks": {
            "implied_vol": 0.2,
            "delta": 0.51,
            "gamma": 0.004,
            "theta": -1.2,
            "vega": 0.4,
            "rho": None,
            "underlier_price": 6201.0,
            "model": "ibkr",
        },
        "sampling_mode": "execution_monitor",
        "sampling_group": 0,
        "mid": 10.2,
        "spread": 0.4,
        "spread_bps": 392.1568627,
        "effective_price": 10.2,
        "raw": {"ignored": "the lake schema is intentionally bounded"},
    }


def source_path(data_root: Path, *, hour: int = 10) -> Path:
    return (
        data_root
        / "raw"
        / "provider=ibkr"
        / "date=2026-07-10"
        / f"hour={hour:02d}"
        / "quotes.jsonl"
    )


def write_source(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )
    settled = (NOW - timedelta(minutes=10)).timestamp()
    os.utime(path, (settled, settled))


def test_compacts_closed_hour_to_typed_zstd_parquet_and_manifest(tmp_path: Path) -> None:
    raw = source_path(tmp_path)
    write_source(raw, [quote_payload(), quote_payload(instrument_id="index:SPX")])
    partition = discover_raw_quote_partitions(tmp_path)[0]

    summary = QuoteLakeCompactor(tmp_path).run(now=NOW)

    assert summary.status_counts == {"compacted": 1}
    assert raw.exists(), "compaction must never delete landing JSONL"
    assert partition.parquet_path.exists()
    connection = duckdb.connect()
    try:
        rows = connection.execute(
            """
            SELECT
                schema_version,
                provider,
                instrument_id,
                bid,
                delta,
                source_sha256,
                typeof(received_at),
                expiry,
                typeof(expiry),
                compacted_at
            FROM read_parquet(?)
            ORDER BY instrument_id
            """,
            [str(partition.parquet_path)],
        ).fetchall()
        compression = connection.execute(
            "SELECT DISTINCT compression FROM parquet_metadata(?)",
            [str(partition.parquet_path)],
        ).fetchall()
    finally:
        connection.close()
    assert len(rows) == 2
    assert rows[0][0:6] == ("v1", "ibkr", "index:SPX", 10.0, 0.51, rows[0][5])
    assert len(rows[0][5]) == 64
    assert rows[0][6] == "TIMESTAMP WITH TIME ZONE"
    assert str(rows[0][7]) == "2026-07-10"
    assert rows[0][8] == "DATE"
    assert rows[0][9] is None
    assert compression == [("ZSTD",)]

    manifest = load_manifest(partition.manifest_path)
    assert manifest is not None
    assert manifest.status == "verified"
    assert manifest.row_count == 2
    assert manifest.source_sha256 == rows[0][5]
    assert manifest.output_sha256
    assert manifest.min_received_at == "2026-07-10T10:05:00.000000Z"


def test_same_source_checksum_is_idempotent(tmp_path: Path) -> None:
    write_source(source_path(tmp_path), [quote_payload()])
    compactor = QuoteLakeCompactor(tmp_path)
    partition = discover_raw_quote_partitions(tmp_path)[0]

    first = compactor.run(now=NOW)
    first_mtime = partition.parquet_path.stat().st_mtime_ns
    second = compactor.run(now=NOW)

    assert first.status_counts == {"compacted": 1}
    assert second.status_counts == {"up_to_date": 1}
    assert partition.parquet_path.stat().st_mtime_ns == first_mtime


def test_missing_output_rebuild_is_byte_reproducible_and_manifest_tracks_wall_clock(
    tmp_path: Path,
) -> None:
    write_source(source_path(tmp_path), [quote_payload()])
    compactor = QuoteLakeCompactor(tmp_path)
    partition = discover_raw_quote_partitions(tmp_path)[0]

    assert compactor.run(now=NOW).status_counts == {"compacted": 1}
    first_manifest = load_manifest(partition.manifest_path)
    first_bytes = partition.parquet_path.read_bytes()
    assert first_manifest is not None

    partition.parquet_path.unlink()
    later = NOW + timedelta(minutes=5)
    assert compactor.run(now=later).status_counts == {"compacted": 1}
    rebuilt_manifest = load_manifest(partition.manifest_path)

    assert rebuilt_manifest is not None
    assert rebuilt_manifest.completed_at == later.isoformat()
    assert rebuilt_manifest.completed_at != first_manifest.completed_at
    assert rebuilt_manifest.output_sha256 == first_manifest.output_sha256
    assert partition.parquet_path.read_bytes() == first_bytes


def test_same_size_corrupt_output_is_detected_and_rebuilt(tmp_path: Path) -> None:
    write_source(source_path(tmp_path), [quote_payload()])
    compactor = QuoteLakeCompactor(tmp_path)
    partition = discover_raw_quote_partitions(tmp_path)[0]
    assert compactor.run(now=NOW).status_counts == {"compacted": 1}
    original = partition.parquet_path.read_bytes()

    corrupted = bytearray(original)
    corrupted[len(corrupted) // 2] ^= 0x01
    partition.parquet_path.write_bytes(corrupted)
    assert partition.parquet_path.stat().st_size == len(original)

    assert compactor.run(now=NOW + timedelta(minutes=5)).status_counts == {"compacted": 1}
    assert partition.parquet_path.read_bytes() == original


def test_expiry_normalization_accepts_compact_iso_and_null_values(tmp_path: Path) -> None:
    compact = quote_payload(instrument_id="compact")
    iso = quote_payload(instrument_id="iso")
    missing = quote_payload(instrument_id="missing")
    iso["instrument"]["expiry"] = "2026-07-10"  # type: ignore[index]
    missing["instrument"]["expiry"] = None  # type: ignore[index]
    write_source(source_path(tmp_path), [compact, iso, missing])
    partition = discover_raw_quote_partitions(tmp_path)[0]

    assert QuoteLakeCompactor(tmp_path).run(now=NOW).status_counts == {"compacted": 1}
    connection = duckdb.connect()
    try:
        rows = connection.execute(
            "SELECT instrument_id, expiry FROM read_parquet(?) ORDER BY instrument_id",
            [str(partition.parquet_path)],
        ).fetchall()
    finally:
        connection.close()

    assert [(instrument_id, str(expiry) if expiry else None) for instrument_id, expiry in rows] == [
        ("compact", "2026-07-10"),
        ("iso", "2026-07-10"),
        ("missing", None),
    ]


def test_limit_counts_work_not_old_up_to_date_partitions(tmp_path: Path) -> None:
    for hour in (9, 10, 11):
        write_source(
            source_path(tmp_path, hour=hour),
            [quote_payload(received_at=f"2026-07-10T{hour:02d}:05:00+00:00")],
        )
    compactor = QuoteLakeCompactor(tmp_path)

    first = compactor.run(now=NOW, limit=1)
    second = compactor.run(now=NOW, limit=1)
    third = compactor.run(now=NOW, limit=1)

    assert first.status_counts == {"compacted": 1}
    assert second.status_counts == {"compacted": 1, "up_to_date": 1}
    assert third.status_counts == {"compacted": 1, "up_to_date": 2}
    assert len(tuple((tmp_path / "lake").glob("**/*.parquet"))) == 3


def test_failed_partition_does_not_starve_later_valid_work(tmp_path: Path) -> None:
    malformed = source_path(tmp_path, hour=9)
    malformed.parent.mkdir(parents=True, exist_ok=True)
    malformed.write_text('{"not": "closed"\n', encoding="utf-8")
    settled = (NOW - timedelta(minutes=10)).timestamp()
    os.utime(malformed, (settled, settled))
    write_source(
        source_path(tmp_path, hour=10),
        [quote_payload(received_at="2026-07-10T10:05:00+00:00")],
    )

    summary = QuoteLakeCompactor(tmp_path).run(now=NOW, limit=1)

    assert summary.status_counts == {"compacted": 1, "failed": 1}
    assert len(tuple((tmp_path / "lake").glob("**/*.parquet"))) == 1


def test_changed_closed_source_atomically_replaces_one_partition_file(tmp_path: Path) -> None:
    raw = source_path(tmp_path)
    write_source(raw, [quote_payload()])
    compactor = QuoteLakeCompactor(tmp_path, settle_seconds=0)
    partition = discover_raw_quote_partitions(tmp_path)[0]
    assert compactor.run(now=NOW).status_counts == {"compacted": 1}
    first_manifest = load_manifest(partition.manifest_path)

    write_source(raw, [quote_payload(), quote_payload(instrument_id="index:SPX")])
    assert compactor.run(now=NOW).status_counts == {"compacted": 1}

    second_manifest = load_manifest(partition.manifest_path)
    assert first_manifest is not None and second_manifest is not None
    assert second_manifest.source_sha256 != first_manifest.source_sha256
    assert second_manifest.row_count == 2
    assert list(partition.parquet_path.parent.glob("*.parquet")) == [partition.parquet_path]


def test_dry_run_writes_no_lake_or_manifest(tmp_path: Path) -> None:
    write_source(source_path(tmp_path), [quote_payload()])
    partition = discover_raw_quote_partitions(tmp_path)[0]

    summary = QuoteLakeCompactor(tmp_path).run(now=NOW, dry_run=True)

    assert summary.status_counts == {"would_compact": 1}
    assert not partition.parquet_path.exists()
    assert not partition.manifest_path.exists()
    assert not (tmp_path / "manifests").exists()


def test_active_and_recent_files_are_not_read(tmp_path: Path) -> None:
    active = source_path(tmp_path, hour=13)
    write_source(active, [quote_payload(received_at="2026-07-10T13:05:00+00:00")])
    settling = source_path(tmp_path, hour=12)
    write_source(settling, [quote_payload(received_at="2026-07-10T12:05:00+00:00")])
    recent = (NOW - timedelta(seconds=30)).timestamp()
    os.utime(settling, (recent, recent))

    summary = QuoteLakeCompactor(tmp_path, settle_seconds=120).run(now=NOW)

    assert summary.status_counts == {"active_hour": 1, "settling": 1}
    assert not (tmp_path / "lake").exists()


def test_bad_json_and_wrong_partition_publish_nothing(tmp_path: Path) -> None:
    malformed = source_path(tmp_path)
    malformed.parent.mkdir(parents=True, exist_ok=True)
    malformed.write_text('{"not": "closed"\n', encoding="utf-8")
    old = (NOW - timedelta(minutes=10)).timestamp()
    os.utime(malformed, (old, old))

    first = QuoteLakeCompactor(tmp_path).run(now=NOW)

    assert first.status_counts == {"failed": 1}
    partition = discover_raw_quote_partitions(tmp_path)[0]
    assert not partition.parquet_path.exists()
    assert not partition.manifest_path.exists()

    write_source(
        malformed,
        [quote_payload(received_at="2026-07-10T11:05:00+00:00")],
    )
    second = QuoteLakeCompactor(tmp_path).run(now=NOW)
    assert second.status_counts == {"failed": 1}
    assert "outside_partition=1" in (second.results[0].detail or "")
    assert not partition.parquet_path.exists()


def test_empty_closed_file_gets_empty_manifest_but_no_parquet(tmp_path: Path) -> None:
    raw = source_path(tmp_path)
    write_source(raw, [])
    partition = discover_raw_quote_partitions(tmp_path)[0]

    summary = QuoteLakeCompactor(tmp_path).run(now=NOW)

    assert summary.status_counts == {"empty": 1}
    manifest = load_manifest(partition.manifest_path)
    assert manifest is not None
    assert manifest.status == "empty"
    assert manifest.row_count == 0
    assert not partition.parquet_path.exists()

    second = QuoteLakeCompactor(tmp_path).run(now=NOW)
    assert second.status_counts == {"empty_up_to_date": 1}


def test_empty_truncation_preserves_previous_verified_partition(tmp_path: Path) -> None:
    raw = source_path(tmp_path)
    write_source(raw, [quote_payload()])
    compactor = QuoteLakeCompactor(tmp_path)
    partition = discover_raw_quote_partitions(tmp_path)[0]
    assert compactor.run(now=NOW).status_counts == {"compacted": 1}
    original_parquet = partition.parquet_path.read_bytes()
    original_manifest = partition.manifest_path.read_bytes()

    write_source(raw, [])
    summary = compactor.run(now=NOW)

    assert summary.status_counts == {"failed": 1}
    assert "preserved previous verified" in (summary.results[0].detail or "")
    assert partition.parquet_path.read_bytes() == original_parquet
    assert partition.manifest_path.read_bytes() == original_manifest
