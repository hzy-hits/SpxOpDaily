"""Operational CLI for the local SPX Spark data platform."""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Any

from spx_spark.data_platform.adapters.sqlite_ledger import SQLiteDecisionLedger
from spx_spark.data_platform.contracts import CompactionManifestRecord
from spx_spark.data_platform.lake.manifest import load_manifest
from spx_spark.data_platform.research import build_research_catalog
from spx_spark.data_platform.settings import DataPlatformSettings
from spx_spark.data_platform.telemetry import FallbackSpool, OperationalTelemetry


LEDGER_TABLES = (
    "sessions",
    "strategy_versions",
    "events",
    "feature_snapshots",
    "decisions",
    "decision_legs",
    "alert_deliveries",
    "outcomes",
    "compaction_manifests",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SPX Spark local data-platform operations")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init", help="Initialize or migrate the SQLite ledger")
    subparsers.add_parser("status", help="Show counts and storage sizes without identifiers")
    subparsers.add_parser("replay-spool", help="Replay fallback telemetry into SQLite")
    subparsers.add_parser(
        "sync-manifests",
        help="Copy verified JSON compaction lineage into the SQLite ledger",
    )

    query = subparsers.add_parser("query", help="Query an allowlisted DuckDB research view")
    query.add_argument("view", choices=("strategy", "bias", "quality", "quotes"))
    query.add_argument("--start")
    query.add_argument("--end")
    query.add_argument("--strategy")
    query.add_argument("--side")
    query.add_argument("--provider")
    query.add_argument("--dataset")
    query.add_argument("--limit", type=int, default=1000)
    return parser


def run(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = DataPlatformSettings.from_env()
    ledger = SQLiteDecisionLedger(
        settings.ledger_path,
        busy_timeout_ms=settings.sqlite_busy_timeout_ms,
    )
    if args.command == "init":
        _print_json(
            {
                "status": "initialized",
                "ledger_path": settings.ledger_path,
                "raw_delete_enabled": settings.raw_delete_enabled,
            }
        )
        return 0
    if args.command == "status":
        _print_json(_status(settings))
        return 0
    if args.command == "replay-spool":
        result = OperationalTelemetry(
            ledger,
            FallbackSpool(
                settings.fallback_spool_path,
                max_bytes=settings.fallback_spool_max_bytes,
            ),
        ).replay_fallback()
        _print_json(
            {
                "status": "ok" if result.retained == 0 else "partial",
                "replayed": result.replayed,
                "retained": result.retained,
                "invalid": result.invalid,
            }
        )
        return 0 if result.retained == 0 else 1
    if args.command == "sync-manifests":
        result = _sync_manifests(settings, ledger)
        _print_json(result)
        return 0 if result["failed"] == 0 else 1
    if args.command == "query":
        rows = _query(args, settings)
        _print_json({"view": args.view, "row_count": len(rows), "rows": rows})
        return 0
    raise AssertionError(f"unhandled command: {args.command}")


def _status(settings: DataPlatformSettings) -> dict[str, object]:
    ledger_path = Path(settings.ledger_path)
    connection = sqlite3.connect(f"file:{ledger_path}?mode=ro", uri=True)
    try:
        counts = {
            table: int(connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0])
            for table in LEDGER_TABLES
        }
    finally:
        connection.close()
    data_root = Path(settings.data_root)
    parquet_files = tuple((data_root / "lake").glob("**/*.parquet"))
    manifest_files = tuple((data_root / "manifests").glob("**/*.json"))
    spool_path = Path(settings.fallback_spool_path)
    return {
        "enabled": settings.enabled,
        "raw_delete_enabled": settings.raw_delete_enabled,
        "ledger": {
            "path": str(ledger_path),
            "size_bytes": ledger_path.stat().st_size if ledger_path.exists() else 0,
            "counts": counts,
        },
        "fallback_spool": {
            "path": str(spool_path),
            "size_bytes": spool_path.stat().st_size if spool_path.exists() else 0,
            "max_bytes": settings.fallback_spool_max_bytes,
        },
        "lake": {
            "parquet_files": len(parquet_files),
            "parquet_bytes": sum(path.stat().st_size for path in parquet_files),
            "manifest_files": len(manifest_files),
        },
    }


def _sync_manifests(
    settings: DataPlatformSettings,
    ledger: SQLiteDecisionLedger,
) -> dict[str, int | str]:
    synced = 0
    updated = 0
    skipped = 0
    failed = 0
    root = Path(settings.manifest_root) / "compaction"
    for path in sorted(root.glob("**/*.json")) if root.exists() else ():
        manifest = load_manifest(path)
        if manifest is None:
            failed += 1
            continue
        try:
            record = CompactionManifestRecord(
                source_path=manifest.source_path,
                source_sha256=manifest.source_sha256,
                source_size=manifest.source_size,
                source_mtime_ns=manifest.source_mtime_ns,
                output_path=manifest.output_path,
                output_sha256=manifest.output_sha256,
                row_count=manifest.row_count,
                min_received_at=_optional_datetime(manifest.min_received_at),
                max_received_at=_optional_datetime(manifest.max_received_at),
                schema_version=manifest.schema_version,
                writer_version=manifest.writer_version,
                completed_at=_required_datetime(manifest.completed_at),
                status=manifest.status,
                dataset=manifest.dataset,
            )
            existing = ledger.get_compaction_manifest(record.source_path, record.source_sha256)
            if existing is None:
                ledger.record_compaction_manifest(record)
                synced += 1
            elif existing == record:
                skipped += 1
            else:
                ledger.record_compaction_manifest(record)
                updated += 1
        except (OSError, ValueError, RuntimeError):
            failed += 1
    return {
        "status": "ok" if failed == 0 else "partial",
        "synced": synced,
        "updated": updated,
        "skipped": skipped,
        "failed": failed,
    }


def _query(args: argparse.Namespace, settings: DataPlatformSettings) -> list[dict[str, object]]:
    start = date.fromisoformat(args.start) if args.start else None
    end = date.fromisoformat(args.end) if args.end else None
    ledger = Path(settings.ledger_path)
    with build_research_catalog(
        settings.data_root,
        database=":memory:",
        sqlite_ledger=ledger if ledger.exists() else None,
    ) as catalog:
        reader = catalog.reader()
        if args.view == "strategy":
            return reader.strategy_outcomes(
                start_date=start,
                end_date=end,
                strategy_name=args.strategy,
                side=args.side,
                limit=args.limit,
            )
        if args.view == "bias":
            return reader.put_call_bias(start_date=start, end_date=end, limit=args.limit)
        if args.view == "quality":
            return reader.session_data_quality(
                start_date=start,
                end_date=end,
                provider=args.provider,
                dataset=args.dataset,
                limit=args.limit,
            )
        return reader.quotes(
            start_date=start,
            end_date=end,
            provider=args.provider,
            limit=args.limit,
        )


def _optional_datetime(value: str | None) -> datetime | None:
    return _required_datetime(value) if value is not None else None


def _required_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("manifest timestamp must be timezone-aware")
    return parsed


def _print_json(payload: object) -> None:
    print(
        json.dumps(
            payload,
            default=_json_default,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )


def _json_default(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value)


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
