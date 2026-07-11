from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

from spx_spark.data_platform.lake.compact import QuoteLakeCompactor, main
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
    malformed = source_path(tmp_path, hour=10)
    malformed.parent.mkdir(parents=True, exist_ok=True)
    malformed.write_text('{"not": "closed"\n', encoding="utf-8")
    settled = (NOW - timedelta(minutes=10)).timestamp()
    os.utime(malformed, (settled, settled))
    write_source(
        source_path(tmp_path, hour=9),
        [quote_payload(received_at="2026-07-10T09:05:00+00:00")],
    )

    summary = QuoteLakeCompactor(tmp_path).run(now=NOW, limit=1)

    assert summary.status_counts == {"compacted": 1, "failed": 1}
    assert len(tuple((tmp_path / "lake").glob("**/*.parquet"))) == 1


def test_provider_filter_is_bounded_to_requested_landing_source(tmp_path: Path) -> None:
    write_source(source_path(tmp_path), [quote_payload()])
    other = (
        tmp_path
        / "raw/provider=hyperliquid/date=2026-07-10/hour=10/quotes.jsonl"
    )
    write_source(other, [quote_payload(provider="hyperliquid")])

    summary = QuoteLakeCompactor(tmp_path).run(now=NOW, provider="ibkr")

    assert summary.status_counts == {"compacted": 1}
    outputs = tuple((tmp_path / "lake").glob("**/*.parquet"))
    assert len(outputs) == 1
    assert "provider=ibkr" in outputs[0].as_posix()


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


def test_summary_only_json_omits_per_partition_success_rows(
    tmp_path: Path,
    capsys: object,
) -> None:
    write_source(source_path(tmp_path), [quote_payload()])

    assert (
        main(
            [
                "--data-root",
                str(tmp_path),
                "--settle-seconds",
                "0",
                "--json",
                "--summary-only",
            ]
        )
        == 0
    )

    output = capsys.readouterr().out  # type: ignore[attr-defined]
    payload = json.loads(output)
    assert payload["result_count"] == 1
    assert payload["status_counts"] == {"compacted": 1}
    assert payload["failed"] is False
    assert payload["failures"] == []
    assert "results" not in payload


def test_raw_delete_disabled_by_default_keeps_verified_source(tmp_path: Path) -> None:
    raw = source_path(tmp_path)
    write_source(raw, [quote_payload()])
    compactor = QuoteLakeCompactor(tmp_path, settle_seconds=0)
    assert compactor.run(now=NOW).status_counts == {"compacted": 1}
    later = NOW + timedelta(hours=100)
    summary = compactor.run(now=later)
    assert summary.status_counts == {"up_to_date": 1}
    assert raw.exists()


def test_raw_delete_after_grace_removes_only_verified_source(tmp_path: Path) -> None:
    raw = source_path(tmp_path)
    write_source(raw, [quote_payload(), quote_payload(instrument_id="index:SPX")])
    latest = tmp_path / "latest" / "state.json"
    latest.parent.mkdir(parents=True)
    latest.write_text("{}", encoding="utf-8")
    runtime = tmp_path / "runtime" / "mode.json"
    runtime.parent.mkdir(parents=True)
    runtime.write_text("{}", encoding="utf-8")
    lock = tmp_path / "manifests" / "compaction" / ".compact.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    lock.write_text("lock", encoding="utf-8")

    compactor = QuoteLakeCompactor(
        tmp_path,
        settle_seconds=0,
        raw_delete_enabled=True,
        raw_delete_grace_hours=24,
    )
    partition = discover_raw_quote_partitions(tmp_path)[0]
    assert compactor.run(now=NOW).status_counts == {"compacted": 1}
    assert raw.exists()

    before_grace = NOW + timedelta(hours=23)
    waiting = compactor.run(now=before_grace)
    assert waiting.status_counts == {"up_to_date": 1}
    assert raw.exists()

    after_grace = NOW + timedelta(hours=25)
    deleted = compactor.run(now=after_grace)
    assert deleted.status_counts == {"raw_deleted": 1}
    assert deleted.failed is False
    assert deleted.results[0].detail
    assert "deleted verified" in deleted.results[0].detail
    assert not raw.exists()
    assert partition.parquet_path.exists()
    assert partition.manifest_path.exists()
    assert latest.exists()
    assert runtime.exists()
    assert lock.exists()

    audit_path = tmp_path / "manifests" / "compaction" / "raw_deletion_audit.jsonl"
    assert audit_path.exists()
    assert audit_path.stat().st_mode & 0o777 == 0o600
    entries = [json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines()]
    assert [entry["event"] for entry in entries] == ["authorize", "final"]
    assert entries[0]["status"] == "authorized"
    assert entries[1]["status"] == "raw_deleted"
    for entry in entries:
        assert entry["source_path"] == partition.source_relative_path
        assert entry["output_path"] == partition.parquet_path.relative_to(tmp_path).as_posix()
        assert entry["source_sha256"]
        assert entry["output_sha256"]
        assert entry["row_count"] == 2
        assert "payload" not in entry
        assert "secret" not in entry
        assert "bid" not in entry
        assert "ask" not in entry


def test_raw_delete_blocked_when_source_checksum_mismatches(tmp_path: Path) -> None:
    from spx_spark.data_platform.lake.compact import CompactionResult

    raw = source_path(tmp_path)
    write_source(raw, [quote_payload()])
    compactor = QuoteLakeCompactor(
        tmp_path,
        settle_seconds=0,
        raw_delete_enabled=True,
        raw_delete_grace_hours=24,
    )
    partition = discover_raw_quote_partitions(tmp_path)[0]
    assert compactor.run(now=NOW).status_counts == {"compacted": 1}
    manifest = load_manifest(partition.manifest_path)
    assert manifest is not None
    raw.write_bytes(raw.read_bytes() + b"\n")
    settled = (NOW - timedelta(minutes=10)).timestamp()
    os.utime(raw, (settled, settled))

    result = compactor._maybe_delete_raw(
        partition,
        CompactionResult(
            partition.source_relative_path,
            manifest.output_path,
            "up_to_date",
            row_count=manifest.row_count,
            source_sha256=manifest.source_sha256,
        ),
        manifest=manifest,
        now=NOW + timedelta(hours=30),
        dry_run=False,
    )
    assert result.status == "raw_delete_blocked"
    assert "source checksum or size no longer matches" in (result.detail or "")
    assert raw.exists()


def test_raw_delete_blocked_when_parquet_checksum_mismatches(tmp_path: Path) -> None:
    from spx_spark.data_platform.lake.compact import CompactionResult

    raw = source_path(tmp_path)
    write_source(raw, [quote_payload()])
    compactor = QuoteLakeCompactor(
        tmp_path,
        settle_seconds=0,
        raw_delete_enabled=True,
        raw_delete_grace_hours=24,
    )
    partition = discover_raw_quote_partitions(tmp_path)[0]
    assert compactor.run(now=NOW).status_counts == {"compacted": 1}
    manifest = load_manifest(partition.manifest_path)
    assert manifest is not None
    corrupted = bytearray(partition.parquet_path.read_bytes())
    corrupted[len(corrupted) // 2] ^= 0x01
    partition.parquet_path.write_bytes(corrupted)

    result = compactor._maybe_delete_raw(
        partition,
        CompactionResult(
            partition.source_relative_path,
            manifest.output_path,
            "up_to_date",
            row_count=manifest.row_count,
            source_sha256=manifest.source_sha256,
        ),
        manifest=manifest,
        now=NOW + timedelta(hours=30),
        dry_run=False,
    )
    assert result.status == "raw_delete_blocked"
    assert "parquet checksum or size does not match" in (result.detail or "")
    assert raw.exists()


def test_raw_delete_blocked_when_manifest_incomplete(tmp_path: Path) -> None:
    from spx_spark.data_platform.lake.compact import CompactionResult
    from spx_spark.data_platform.lake.manifest import CompactionManifest

    raw = source_path(tmp_path)
    write_source(raw, [quote_payload()])
    compactor = QuoteLakeCompactor(
        tmp_path,
        settle_seconds=0,
        raw_delete_enabled=True,
        raw_delete_grace_hours=24,
    )
    partition = discover_raw_quote_partitions(tmp_path)[0]
    assert compactor.run(now=NOW).status_counts == {"compacted": 1}
    manifest = load_manifest(partition.manifest_path)
    assert manifest is not None
    incomplete = CompactionManifest(
        manifest_version=manifest.manifest_version,
        dataset=manifest.dataset,
        schema_version=manifest.schema_version,
        writer_version=manifest.writer_version,
        status="verified",
        source_path=manifest.source_path,
        source_size=manifest.source_size,
        source_mtime_ns=manifest.source_mtime_ns,
        source_sha256=manifest.source_sha256,
        output_path=manifest.output_path,
        output_size=None,
        output_sha256=None,
        row_count=manifest.row_count,
        min_received_at=manifest.min_received_at,
        max_received_at=manifest.max_received_at,
        min_source_at=manifest.min_source_at,
        max_source_at=manifest.max_source_at,
        completed_at=manifest.completed_at,
    )

    result = compactor._maybe_delete_raw(
        partition,
        CompactionResult(
            partition.source_relative_path,
            incomplete.output_path,
            "up_to_date",
            row_count=incomplete.row_count,
            source_sha256=incomplete.source_sha256,
        ),
        manifest=incomplete,
        now=NOW + timedelta(hours=30),
        dry_run=False,
    )
    assert result.status == "raw_delete_blocked"
    assert "missing parquet output metadata" in (result.detail or "")
    assert raw.exists()


def test_raw_delete_blocked_when_row_counts_diverge(tmp_path: Path) -> None:
    from spx_spark.data_platform.lake.compact import CompactionResult
    from spx_spark.data_platform.lake.manifest import CompactionManifest

    raw = source_path(tmp_path)
    write_source(raw, [quote_payload()])
    compactor = QuoteLakeCompactor(
        tmp_path,
        settle_seconds=0,
        raw_delete_enabled=True,
        raw_delete_grace_hours=24,
    )
    partition = discover_raw_quote_partitions(tmp_path)[0]
    assert compactor.run(now=NOW).status_counts == {"compacted": 1}
    manifest = load_manifest(partition.manifest_path)
    assert manifest is not None
    mismatched = CompactionManifest(
        manifest_version=manifest.manifest_version,
        dataset=manifest.dataset,
        schema_version=manifest.schema_version,
        writer_version=manifest.writer_version,
        status=manifest.status,
        source_path=manifest.source_path,
        source_size=manifest.source_size,
        source_mtime_ns=manifest.source_mtime_ns,
        source_sha256=manifest.source_sha256,
        output_path=manifest.output_path,
        output_size=manifest.output_size,
        output_sha256=manifest.output_sha256,
        row_count=manifest.row_count + 1,
        min_received_at=manifest.min_received_at,
        max_received_at=manifest.max_received_at,
        min_source_at=manifest.min_source_at,
        max_source_at=manifest.max_source_at,
        completed_at=manifest.completed_at,
    )

    result = compactor._maybe_delete_raw(
        partition,
        CompactionResult(
            partition.source_relative_path,
            mismatched.output_path,
            "up_to_date",
            row_count=mismatched.row_count,
            source_sha256=mismatched.source_sha256,
        ),
        manifest=mismatched,
        now=NOW + timedelta(hours=30),
        dry_run=False,
    )
    assert result.status == "raw_delete_blocked"
    assert "row count mismatch" in (result.detail or "")
    assert raw.exists()


def test_raw_delete_dry_run_reports_without_removing_source(tmp_path: Path) -> None:
    raw = source_path(tmp_path)
    write_source(raw, [quote_payload()])
    compactor = QuoteLakeCompactor(
        tmp_path,
        settle_seconds=0,
        raw_delete_enabled=True,
        raw_delete_grace_hours=24,
    )
    assert compactor.run(now=NOW).status_counts == {"compacted": 1}
    summary = compactor.run(now=NOW + timedelta(hours=30), dry_run=True)
    assert summary.status_counts == {"would_delete_raw": 1}
    assert raw.exists()
    assert not (tmp_path / "manifests" / "compaction" / "raw_deletion_audit.jsonl").exists()


def test_raw_delete_toctou_swap_preserves_quarantine(tmp_path: Path, monkeypatch) -> None:
    from spx_spark.data_platform.lake.compact import CompactionResult

    raw = source_path(tmp_path)
    write_source(raw, [quote_payload()])
    compactor = QuoteLakeCompactor(
        tmp_path,
        settle_seconds=0,
        raw_delete_enabled=True,
        raw_delete_grace_hours=24,
    )
    partition = discover_raw_quote_partitions(tmp_path)[0]
    assert compactor.run(now=NOW).status_counts == {"compacted": 1}
    manifest = load_manifest(partition.manifest_path)
    assert manifest is not None
    original_bytes = raw.read_bytes()

    real_rename = os.rename

    def swap_then_rename(src: str | os.PathLike[str], dst: str | os.PathLike[str]) -> None:
        src_path = Path(src)
        if src_path.name == "quotes.jsonl":
            attacker = src_path.with_name("attacker-swap.jsonl")
            attacker.write_bytes(b'{"evil":true}\n')
            real_rename(attacker, src_path)
        real_rename(src, dst)

    monkeypatch.setattr(os, "rename", swap_then_rename)

    result = compactor._maybe_delete_raw(
        partition,
        CompactionResult(
            partition.source_relative_path,
            manifest.output_path,
            "up_to_date",
            row_count=manifest.row_count,
            source_sha256=manifest.source_sha256,
        ),
        manifest=manifest,
        now=NOW + timedelta(hours=30),
        dry_run=False,
    )
    assert result.status == "raw_delete_failed"
    assert "quarantined raw preserved" in (result.detail or "")
    assert "post-rename mismatch" in (result.detail or "")
    assert not raw.exists()
    quarantines = list((raw.parent).glob(".quotes.jsonl.raw-delete-quarantine.*"))
    assert len(quarantines) == 1
    assert quarantines[0].read_bytes() == b'{"evil":true}\n'
    assert quarantines[0].read_bytes() != original_bytes


def test_raw_delete_rejects_symlink_source(tmp_path: Path) -> None:
    from spx_spark.data_platform.lake.compact import CompactionResult
    from spx_spark.data_platform.lake.layout import RawQuotePartition
    from spx_spark.data_platform.lake.manifest import CompactionManifest

    raw = source_path(tmp_path)
    write_source(raw, [quote_payload()])
    compactor = QuoteLakeCompactor(
        tmp_path,
        settle_seconds=0,
        raw_delete_enabled=True,
        raw_delete_grace_hours=24,
    )
    partition = discover_raw_quote_partitions(tmp_path)[0]
    assert compactor.run(now=NOW).status_counts == {"compacted": 1}
    manifest = load_manifest(partition.manifest_path)
    assert manifest is not None

    outside = tmp_path / "outside" / "quotes.jsonl"
    outside.parent.mkdir(parents=True)
    outside.write_bytes(raw.read_bytes())
    raw.unlink()
    raw.symlink_to(outside)

    symlink_partition = RawQuotePartition(
        data_root=tmp_path,
        source_path=raw,
        provider="ibkr",
        session_date="2026-07-10",
        hour=10,
    )
    result = compactor._maybe_delete_raw(
        symlink_partition,
        CompactionResult(
            symlink_partition.source_relative_path,
            manifest.output_path,
            "up_to_date",
            row_count=manifest.row_count,
            source_sha256=manifest.source_sha256,
        ),
        manifest=CompactionManifest(
            manifest_version=manifest.manifest_version,
            dataset=manifest.dataset,
            schema_version=manifest.schema_version,
            writer_version=manifest.writer_version,
            status="verified",
            source_path=symlink_partition.source_relative_path,
            source_size=manifest.source_size,
            source_mtime_ns=manifest.source_mtime_ns,
            source_sha256=manifest.source_sha256,
            output_path=manifest.output_path,
            output_size=manifest.output_size,
            output_sha256=manifest.output_sha256,
            row_count=manifest.row_count,
            min_received_at=manifest.min_received_at,
            max_received_at=manifest.max_received_at,
            min_source_at=manifest.min_source_at,
            max_source_at=manifest.max_source_at,
            completed_at=manifest.completed_at,
        ),
        now=NOW + timedelta(hours=30),
        dry_run=False,
    )
    assert result.status == "raw_delete_blocked"
    assert "symlink" in (result.detail or "")
    assert raw.is_symlink()
    assert outside.exists()


def test_raw_delete_rejects_malformed_provider_date_hour_path(tmp_path: Path) -> None:
    from spx_spark.data_platform.lake.layout import RawQuotePartition

    bad = tmp_path / "raw" / "ibkr" / "2026-07-10" / "10" / "quotes.jsonl"
    write_source(bad, [quote_payload()])
    partition = RawQuotePartition(
        data_root=tmp_path,
        source_path=bad,
        provider="ibkr",
        session_date="2026-07-10",
        hour=10,
    )
    compactor = QuoteLakeCompactor(
        tmp_path,
        settle_seconds=0,
        raw_delete_enabled=True,
        raw_delete_grace_hours=24,
    )
    reason = compactor._unsafe_raw_delete_target_reason(partition)
    assert reason is not None
    assert "malformed provider" in reason


def test_raw_delete_blocked_when_manifest_source_path_mismatches(tmp_path: Path) -> None:
    from spx_spark.data_platform.lake.compact import CompactionResult
    from spx_spark.data_platform.lake.manifest import CompactionManifest

    raw = source_path(tmp_path)
    write_source(raw, [quote_payload()])
    compactor = QuoteLakeCompactor(
        tmp_path,
        settle_seconds=0,
        raw_delete_enabled=True,
        raw_delete_grace_hours=24,
    )
    partition = discover_raw_quote_partitions(tmp_path)[0]
    assert compactor.run(now=NOW).status_counts == {"compacted": 1}
    manifest = load_manifest(partition.manifest_path)
    assert manifest is not None
    mismatched = CompactionManifest(
        manifest_version=manifest.manifest_version,
        dataset=manifest.dataset,
        schema_version=manifest.schema_version,
        writer_version=manifest.writer_version,
        status=manifest.status,
        source_path="raw/provider=other/date=2026-07-10/hour=10/quotes.jsonl",
        source_size=manifest.source_size,
        source_mtime_ns=manifest.source_mtime_ns,
        source_sha256=manifest.source_sha256,
        output_path=manifest.output_path,
        output_size=manifest.output_size,
        output_sha256=manifest.output_sha256,
        row_count=manifest.row_count,
        min_received_at=manifest.min_received_at,
        max_received_at=manifest.max_received_at,
        min_source_at=manifest.min_source_at,
        max_source_at=manifest.max_source_at,
        completed_at=manifest.completed_at,
    )
    result = compactor._maybe_delete_raw(
        partition,
        CompactionResult(
            partition.source_relative_path,
            mismatched.output_path,
            "up_to_date",
            row_count=mismatched.row_count,
            source_sha256=mismatched.source_sha256,
        ),
        manifest=mismatched,
        now=NOW + timedelta(hours=30),
        dry_run=False,
    )
    assert result.status == "raw_delete_blocked"
    assert "manifest source_path does not match" in (result.detail or "")
    assert raw.exists()


def test_raw_delete_audit_write_failure_blocks_deletion(tmp_path: Path, monkeypatch) -> None:
    from spx_spark.data_platform.lake.compact import CompactionResult

    raw = source_path(tmp_path)
    write_source(raw, [quote_payload()])
    compactor = QuoteLakeCompactor(
        tmp_path,
        settle_seconds=0,
        raw_delete_enabled=True,
        raw_delete_grace_hours=24,
    )
    partition = discover_raw_quote_partitions(tmp_path)[0]
    assert compactor.run(now=NOW).status_counts == {"compacted": 1}
    manifest = load_manifest(partition.manifest_path)
    assert manifest is not None

    def boom(*_args, **_kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(compactor, "_append_raw_deletion_audit", boom)

    result = compactor._maybe_delete_raw(
        partition,
        CompactionResult(
            partition.source_relative_path,
            manifest.output_path,
            "up_to_date",
            row_count=manifest.row_count,
            source_sha256=manifest.source_sha256,
        ),
        manifest=manifest,
        now=NOW + timedelta(hours=30),
        dry_run=False,
    )
    assert result.status == "raw_delete_audit_failed"
    assert "pre-delete audit" in (result.detail or "")
    assert raw.exists()


def test_raw_delete_blocked_is_summary_failure(tmp_path: Path, capsys, monkeypatch) -> None:
    from spx_spark.data_platform.lake.compact import CompactionResult, CompactionSummary

    summary = CompactionSummary(
        dry_run=False,
        data_root=str(tmp_path),
        started_at=NOW.isoformat(),
        finished_at=NOW.isoformat(),
        results=(
            CompactionResult(
                "raw/provider=ibkr/date=2026-07-10/hour=10/quotes.jsonl",
                "lake/quotes/schema=v1/date=2026-07-10/provider=ibkr/hour=10/quotes.parquet",
                "raw_delete_blocked",
                row_count=1,
                detail="source checksum or size no longer matches manifest",
            ),
        ),
    )
    assert summary.failed is True

    monkeypatch.setattr(
        "spx_spark.data_platform.lake.compact.QuoteLakeCompactor.run",
        lambda self, **_kwargs: summary,
    )
    monkeypatch.setattr(
        "spx_spark.data_platform.lake.compact.StorageSettings.from_env",
        lambda: type("S", (), {"raw_file_name": "quotes.jsonl"})(),
    )
    monkeypatch.setattr(
        "spx_spark.data_platform.lake.compact.DataPlatformSettings.from_env",
        lambda: type(
            "P",
            (),
            {
                "data_root": str(tmp_path),
                "compaction_min_age_seconds": 0,
                "raw_delete_enabled": True,
                "raw_delete_grace_hours": 24,
            },
        )(),
    )
    code = main(["--data-root", str(tmp_path), "--summary-only", "--json"])
    assert code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["failed"] is True
    assert len(payload["failures"]) == 1
    assert payload["failures"][0]["status"] == "raw_delete_blocked"