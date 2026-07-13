"""CLI for the quote-lake compactor."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import asdict

from spx_spark.config import StorageSettings
from spx_spark.data_platform.settings import DataPlatformSettings
from spx_spark.data_platform.lake.compact import QuoteLakeCompactor, SUMMARY_FAILURE_STATUSES


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Compact closed-hour SPX Spark JSONL into Parquet")
    parser.add_argument("--data-root", help="Data root; defaults to MARKET_DATA_DATA_ROOT")
    parser.add_argument("--raw-file-name", help="Collector JSONL file name")
    parser.add_argument("--provider", help="Only compact one provider (for validation/backfill)")
    parser.add_argument(
        "--settle-seconds",
        type=float,
        help="Minimum source mtime age; defaults to DATA_PLATFORM_COMPACTION_MIN_AGE_SECONDS",
    )
    parser.add_argument("--compression-level", type=int, default=3)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--json", action="store_true", dest="as_json")
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Print aggregate counts and failures without one row per partition.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    storage_settings = StorageSettings.from_env()
    platform_settings = DataPlatformSettings.from_env()
    compactor = QuoteLakeCompactor(
        args.data_root or platform_settings.data_root,
        raw_file_name=args.raw_file_name or storage_settings.raw_file_name,
        settle_seconds=(
            args.settle_seconds
            if args.settle_seconds is not None
            else platform_settings.compaction_min_age_seconds
        ),
        compression_level=args.compression_level,
        raw_delete_enabled=platform_settings.raw_delete_enabled,
        raw_delete_grace_hours=platform_settings.raw_delete_grace_hours,
    )
    summary = compactor.run(
        dry_run=args.dry_run,
        limit=args.limit,
        provider=args.provider,
    )
    if args.summary_only:
        failures = [
            asdict(result)
            for result in summary.results
            if result.status in SUMMARY_FAILURE_STATUSES
        ]
        deletions = [
            asdict(result)
            for result in summary.results
            if result.status in {"raw_deleted", "would_delete_raw"}
        ]
        payload = {
            "dry_run": summary.dry_run,
            "data_root": summary.data_root,
            "started_at": summary.started_at,
            "finished_at": summary.finished_at,
            "result_count": len(summary.results),
            "status_counts": summary.status_counts,
            "failed": summary.failed,
            "failures": failures,
            "raw_delete_enabled": platform_settings.raw_delete_enabled,
            "deletions": deletions,
        }
        if args.as_json:
            print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
        else:
            print(
                "quote compaction "
                f"dry_run={summary.dry_run} results={len(summary.results)} "
                f"counts={json.dumps(summary.status_counts, sort_keys=True)}"
            )
            for failure in failures:
                print(
                    f"{failure['status']}: {failure['source_path']} "
                    f"detail={failure.get('detail') or 'unknown'}"
                )
            for deletion in deletions:
                print(
                    f"{deletion['status']}: {deletion['source_path']} "
                    f"detail={deletion.get('detail') or ''}"
                )
    elif args.as_json:
        print(json.dumps(summary.to_dict(), sort_keys=True, separators=(",", ":")))
    else:
        print(
            "quote compaction "
            f"dry_run={summary.dry_run} counts={json.dumps(summary.status_counts, sort_keys=True)}"
        )
        for result in summary.results:
            detail = f" detail={result.detail}" if result.detail else ""
            print(f"{result.status}: {result.source_path}{detail}")
    return 1 if summary.failed else 0
