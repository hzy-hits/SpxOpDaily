from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from spx_spark.data_platform.cli import run
from spx_spark.data_platform.lake.compact import QuoteLakeCompactor
from spx_spark.data_platform.lake.layout import discover_raw_quote_partitions
from spx_spark.data_platform.lake.manifest import CompactionManifest, write_manifest
from spx_spark.data_platform.research import ResearchCatalog


def configure(monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MARKET_DATA_DATA_ROOT", str(tmp_path / "data"))
    monkeypatch.setenv(
        "DATA_PLATFORM_LEDGER_PATH",
        str(tmp_path / "data/runtime/research-ledger.sqlite3"),
    )


def test_status_initializes_private_ledger_and_reports_counts(
    tmp_path, monkeypatch, capsys
) -> None:
    configure(monkeypatch, tmp_path)
    assert run(["status"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["ledger"]["counts"]["events"] == 0
    assert payload["lake"]["parquet_files"] == 0


def test_sync_manifests_is_idempotent(tmp_path, monkeypatch, capsys) -> None:
    configure(monkeypatch, tmp_path)
    path = tmp_path / "data/manifests/compaction/date=2026-07-10/quotes.json"
    now = datetime(2026, 7, 10, 15, tzinfo=timezone.utc).isoformat()
    write_manifest(
        path,
        CompactionManifest(
            manifest_version=1,
            dataset="quotes",
            schema_version="v1",
            writer_version="test-v1",
            status="verified",
            source_path="raw/provider=ibkr/date=2026-07-10/hour=14/quotes.jsonl",
            source_size=10,
            source_mtime_ns=1,
            source_sha256="a" * 64,
            output_path="lake/quotes/date=2026-07-10/hour=14/quotes.parquet",
            output_size=4,
            output_sha256="b" * 64,
            row_count=1,
            min_received_at=now,
            max_received_at=now,
            min_source_at=now,
            max_source_at=now,
            completed_at=now,
        ),
    )
    assert run(["sync-manifests"]) == 0
    first = json.loads(capsys.readouterr().out)
    assert first["synced"] == 1
    assert run(["sync-manifests"]) == 0
    second = json.loads(capsys.readouterr().out)
    assert second["skipped"] == 1

    changed = CompactionManifest.from_dict(
        {
            **json.loads(path.read_text(encoding="utf-8")),
            "output_sha256": "c" * 64,
            "completed_at": datetime(2026, 7, 10, 15, 5, tzinfo=timezone.utc).isoformat(),
        }
    )
    write_manifest(path, changed)
    assert run(["sync-manifests"]) == 0
    third = json.loads(capsys.readouterr().out)
    assert third["updated"] == 1
    assert third["failed"] == 0


def test_missing_parquet_rebuild_resyncs_and_quality_view_deduplicates_lineage(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    configure(monkeypatch, tmp_path)
    data_root = tmp_path / "data"
    raw = (
        data_root
        / "raw/provider=ibkr/date=2026-07-10/hour=10/quotes.jsonl"
    )
    raw.parent.mkdir(parents=True)
    raw.write_text(
        json.dumps(
            {
                "instrument": {
                    "symbol": "SPX",
                    "instrument_type": "option",
                    "expiry": "20260710",
                    "canonical_id": "opaque-option",
                },
                "instrument_id": "opaque-option",
                "provider": "ibkr",
                "received_at": "2026-07-10T10:05:00+00:00",
                "quote_time": "2026-07-10T10:04:59+00:00",
                "quality": "live",
                "bid": 1.0,
                "ask": 1.2,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    first_now = datetime(2026, 7, 10, 13, 10, tzinfo=timezone.utc)
    settled = (first_now - timedelta(minutes=10)).timestamp()
    os.utime(raw, (settled, settled))
    compactor = QuoteLakeCompactor(data_root)

    assert compactor.run(now=first_now).status_counts == {"compacted": 1}
    partition = discover_raw_quote_partitions(data_root)[0]
    first_bytes = partition.parquet_path.read_bytes()
    first_manifest = json.loads(partition.manifest_path.read_text(encoding="utf-8"))
    assert first_manifest["manifest_id"].startswith("compact_")
    assert run(["sync-manifests"]) == 0
    assert json.loads(capsys.readouterr().out)["synced"] == 1

    partition.parquet_path.unlink()
    assert compactor.run(now=first_now + timedelta(minutes=5)).status_counts == {
        "compacted": 1
    }
    assert partition.parquet_path.read_bytes() == first_bytes
    assert run(["sync-manifests"]) == 0
    rebuild_sync = json.loads(capsys.readouterr().out)
    assert rebuild_sync["updated"] == 1
    assert rebuild_sync["failed"] == 0
    assert run(["sync-manifests"]) == 0
    assert json.loads(capsys.readouterr().out)["skipped"] == 1

    ledger = data_root / "runtime/research-ledger.sqlite3"
    with ResearchCatalog.in_memory(data_root, sqlite_ledger=ledger) as catalog:
        quality = catalog.reader().session_data_quality()
        quotes = catalog.reader().quotes()

    assert len(quality) == 1
    assert quality[0]["session_date"] == date(2026, 7, 10)
    assert quality[0]["provider"] == "ibkr"
    assert quality[0]["dataset"] == "quotes"
    assert quality[0]["partition_count"] == 1
    assert quality[0]["row_count"] == 1
    assert quotes[0]["schema_version"] == "v1"
    assert quotes[0]["expiry"] == date(2026, 7, 10)
