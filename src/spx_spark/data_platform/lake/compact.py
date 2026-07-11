from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import sys
from collections import Counter
from contextlib import contextmanager, nullcontext
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, Sequence

import duckdb

from spx_spark.config import StorageSettings
from spx_spark.data_platform.settings import DataPlatformSettings
from spx_spark.data_platform.lake.layout import (
    QUOTE_SCHEMA_VERSION,
    QUOTE_WRITER_VERSION,
    RawQuotePartition,
    discover_raw_quote_partitions,
)
from spx_spark.data_platform.lake.manifest import (
    MANIFEST_VERSION,
    CompactionManifest,
    load_manifest,
    write_manifest,
)
from spx_spark.data_platform.lake.normalize import (
    NormalizedQuoteStats,
    create_normalized_quotes,
    verify_parquet,
    write_normalized_parquet,
)


LIMITED_WORK_STATUSES = frozenset(
    {
        "compacted",
        "empty",
        "would_compact",
        "would_mark_empty",
    }
)


@dataclass(frozen=True)
class SourceSnapshot:
    size: int
    mtime_ns: int
    sha256: str


@dataclass(frozen=True)
class CompactionResult:
    source_path: str
    output_path: str | None
    status: str
    row_count: int = 0
    source_sha256: str | None = None
    detail: str | None = None


@dataclass(frozen=True)
class CompactionSummary:
    dry_run: bool
    data_root: str
    started_at: str
    finished_at: str
    results: tuple[CompactionResult, ...]

    @property
    def status_counts(self) -> dict[str, int]:
        return dict(sorted(Counter(result.status for result in self.results).items()))

    @property
    def failed(self) -> bool:
        return any(result.status == "failed" for result in self.results)

    def to_dict(self) -> dict[str, object]:
        return {
            "dry_run": self.dry_run,
            "data_root": self.data_root,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "status_counts": self.status_counts,
            "results": [asdict(result) for result in self.results],
        }


class QuoteLakeCompactor:
    def __init__(
        self,
        data_root: str | Path,
        *,
        raw_file_name: str = "quotes.jsonl",
        settle_seconds: float = 120.0,
        compression_level: int = 3,
    ) -> None:
        if settle_seconds < 0:
            raise ValueError("settle_seconds must be >= 0")
        if not 1 <= compression_level <= 22:
            raise ValueError("compression_level must be between 1 and 22")
        self.data_root = Path(data_root)
        self.raw_file_name = raw_file_name
        self.settle_seconds = settle_seconds
        self.compression_level = compression_level

    def run(
        self,
        *,
        now: datetime | None = None,
        dry_run: bool = False,
        limit: int | None = None,
        provider: str | None = None,
    ) -> CompactionSummary:
        if limit is not None and limit <= 0:
            raise ValueError("limit must be positive")
        started = _as_utc(now or datetime.now(tz=timezone.utc))
        results: list[CompactionResult] = []
        work_count = 0
        partitions = discover_raw_quote_partitions(
            self.data_root,
            raw_file_name=self.raw_file_name,
        )
        if provider is not None:
            partitions = tuple(row for row in partitions if row.provider == provider)
        if limit is not None:
            # Keep current research fresh while a bounded initial backfill
            # drains older history in the remaining slots.
            partitions = tuple(reversed(partitions))
        for partition in partitions:
            if limit is not None and work_count >= limit:
                break
            result = self.compact_one(partition, now=started, dry_run=dry_run)
            results.append(result)
            if result.status in LIMITED_WORK_STATUSES:
                work_count += 1
        finished = datetime.now(tz=timezone.utc)
        return CompactionSummary(
            dry_run=dry_run,
            data_root=str(self.data_root),
            started_at=started.isoformat(),
            finished_at=finished.isoformat(),
            results=tuple(results),
        )

    def compact_one(
        self,
        partition: RawQuotePartition,
        *,
        now: datetime,
        dry_run: bool = False,
    ) -> CompactionResult:
        """Publish one partition safely, including callers using the lake adapter directly."""

        lock = nullcontext() if dry_run else self._exclusive_lock()
        with lock:
            return self._compact_one_unlocked(partition, now=now, dry_run=dry_run)

    def _compact_one_unlocked(
        self,
        partition: RawQuotePartition,
        *,
        now: datetime,
        dry_run: bool,
    ) -> CompactionResult:
        now = _as_utc(now)
        source_label = partition.source_relative_path
        output_label = partition.parquet_path.relative_to(self.data_root).as_posix()
        if partition.end_at > now:
            return CompactionResult(source_label, None, "active_hour", detail="hour is not closed")

        try:
            stat = partition.source_path.stat()
        except FileNotFoundError:
            return CompactionResult(source_label, None, "vanished")
        except OSError as exc:
            return CompactionResult(source_label, None, "failed", detail=str(exc))
        settle_cutoff = now - timedelta(seconds=self.settle_seconds)
        mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
        if mtime > settle_cutoff:
            return CompactionResult(source_label, None, "settling", detail="source mtime is recent")

        existing = load_manifest(partition.manifest_path)
        if self._manifest_matches_metadata(partition, existing, stat):
            assert existing is not None
            status = "empty_up_to_date" if existing.status == "empty" else "up_to_date"
            return CompactionResult(
                source_label,
                existing.output_path,
                status,
                row_count=existing.row_count,
                source_sha256=existing.source_sha256,
            )

        try:
            snapshot = snapshot_source(partition.source_path)
        except (OSError, RuntimeError) as exc:
            return CompactionResult(
                source_label,
                None,
                "failed",
                detail=f"{type(exc).__name__}: {exc}",
            )

        if snapshot.size == 0:
            if (
                existing is not None
                and existing.status == "empty"
                and existing.source_path == source_label
                and existing.source_sha256 == snapshot.sha256
            ):
                return CompactionResult(
                    source_label,
                    None,
                    "empty_up_to_date",
                    source_sha256=snapshot.sha256,
                )
            if existing is not None and existing.status == "verified":
                return CompactionResult(
                    source_label,
                    output_label,
                    "failed",
                    row_count=existing.row_count,
                    source_sha256=snapshot.sha256,
                    detail="closed source became empty; preserved previous verified partition",
                )
            if dry_run:
                return CompactionResult(
                    source_label,
                    None,
                    "would_mark_empty",
                    source_sha256=snapshot.sha256,
                )
            manifest = self._empty_manifest(partition, snapshot, now=now)
            try:
                write_manifest(partition.manifest_path, manifest)
            except OSError as exc:
                return CompactionResult(
                    source_label,
                    None,
                    "failed",
                    source_sha256=snapshot.sha256,
                    detail=f"{type(exc).__name__}: {exc}",
                )
            return CompactionResult(
                source_label,
                None,
                "empty",
                source_sha256=snapshot.sha256,
            )

        if self._verified_manifest_matches(partition, existing, snapshot):
            return CompactionResult(
                source_label,
                output_label,
                "up_to_date",
                row_count=existing.row_count if existing else 0,
                source_sha256=snapshot.sha256,
            )
        if dry_run:
            return CompactionResult(
                source_label,
                output_label,
                "would_compact",
                source_sha256=snapshot.sha256,
            )

        # A stable name lets the next locked run clean up after SIGTERM/power loss.
        temp_path = partition.parquet_path.with_name(f".{partition.parquet_path.name}.tmp")
        connection: duckdb.DuckDBPyConnection | None = None
        try:
            partition.parquet_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path.unlink(missing_ok=True)
            connection = duckdb.connect()
            stats = create_normalized_quotes(
                connection,
                source_path=partition.source_path,
                expected_provider=partition.provider,
                partition_start=partition.start_at,
                partition_end=partition.end_at,
                source_relative_path=source_label,
                source_sha256=snapshot.sha256,
                schema_version=QUOTE_SCHEMA_VERSION,
                writer_version=QUOTE_WRITER_VERSION,
            )
            write_normalized_parquet(
                connection,
                temp_path,
                compression_level=self.compression_level,
            )
            verify_parquet(connection, temp_path, stats)
            after = snapshot_source(partition.source_path)
            if after != snapshot:
                raise RuntimeError("source changed while compaction was running")
            os.chmod(temp_path, 0o600)
            output_snapshot = snapshot_source(temp_path)
            os.replace(temp_path, partition.parquet_path)
            manifest = self._verified_manifest(
                partition,
                source=snapshot,
                output=output_snapshot,
                stats=stats,
                now=now,
            )
            write_manifest(partition.manifest_path, manifest)
        except Exception as exc:
            return CompactionResult(
                source_label,
                None,
                "failed",
                source_sha256=snapshot.sha256,
                detail=f"{type(exc).__name__}: {exc}",
            )
        finally:
            if connection is not None:
                connection.close()
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass

        return CompactionResult(
            source_label,
            output_label,
            "compacted",
            row_count=stats.row_count,
            source_sha256=snapshot.sha256,
        )

    def _manifest_matches_metadata(
        self,
        partition: RawQuotePartition,
        manifest: CompactionManifest | None,
        source_stat: os.stat_result,
    ) -> bool:
        """Fast idempotence path; avoids re-hashing every retained raw hour."""

        if manifest is None or manifest.status not in {"verified", "empty"}:
            return False
        if manifest.source_path != partition.source_relative_path:
            return False
        if manifest.schema_version != QUOTE_SCHEMA_VERSION:
            return False
        if manifest.writer_version != QUOTE_WRITER_VERSION:
            return False
        if manifest.source_size != source_stat.st_size:
            return False
        if manifest.source_mtime_ns != source_stat.st_mtime_ns:
            return False
        if manifest.status == "empty":
            return source_stat.st_size == 0 and manifest.output_path is None
        if manifest.output_path != partition.parquet_path.relative_to(self.data_root).as_posix():
            return False
        if (
            manifest.output_size is None
            or not manifest.output_sha256
            or not partition.parquet_path.is_file()
        ):
            return False
        try:
            output = snapshot_source(partition.parquet_path)
            return output.size == manifest.output_size and output.sha256 == manifest.output_sha256
        except (OSError, RuntimeError):
            return False

    def _verified_manifest_matches(
        self,
        partition: RawQuotePartition,
        manifest: CompactionManifest | None,
        source: SourceSnapshot,
    ) -> bool:
        if manifest is None or manifest.status != "verified":
            return False
        if manifest.source_path != partition.source_relative_path:
            return False
        if manifest.source_sha256 != source.sha256 or manifest.source_size != source.size:
            return False
        if manifest.schema_version != QUOTE_SCHEMA_VERSION:
            return False
        if manifest.writer_version != QUOTE_WRITER_VERSION:
            return False
        if not partition.parquet_path.is_file() or not manifest.output_sha256:
            return False
        try:
            return snapshot_source(partition.parquet_path).sha256 == manifest.output_sha256
        except (OSError, RuntimeError):
            return False

    def _verified_manifest(
        self,
        partition: RawQuotePartition,
        *,
        source: SourceSnapshot,
        output: SourceSnapshot,
        stats: NormalizedQuoteStats,
        now: datetime,
    ) -> CompactionManifest:
        return CompactionManifest(
            manifest_version=MANIFEST_VERSION,
            dataset="quotes",
            schema_version=QUOTE_SCHEMA_VERSION,
            writer_version=QUOTE_WRITER_VERSION,
            status="verified",
            source_path=partition.source_relative_path,
            source_size=source.size,
            source_mtime_ns=source.mtime_ns,
            source_sha256=source.sha256,
            output_path=partition.parquet_path.relative_to(self.data_root).as_posix(),
            output_size=output.size,
            output_sha256=output.sha256,
            row_count=stats.row_count,
            min_received_at=stats.min_received_at,
            max_received_at=stats.max_received_at,
            min_source_at=stats.min_source_at,
            max_source_at=stats.max_source_at,
            completed_at=now.isoformat(),
        )

    def _empty_manifest(
        self,
        partition: RawQuotePartition,
        source: SourceSnapshot,
        *,
        now: datetime,
    ) -> CompactionManifest:
        return CompactionManifest(
            manifest_version=MANIFEST_VERSION,
            dataset="quotes",
            schema_version=QUOTE_SCHEMA_VERSION,
            writer_version=QUOTE_WRITER_VERSION,
            status="empty",
            source_path=partition.source_relative_path,
            source_size=source.size,
            source_mtime_ns=source.mtime_ns,
            source_sha256=source.sha256,
            output_path=None,
            output_size=None,
            output_sha256=None,
            row_count=0,
            min_received_at=None,
            max_received_at=None,
            min_source_at=None,
            max_source_at=None,
            completed_at=now.isoformat(),
        )

    @contextmanager
    def _exclusive_lock(self) -> Iterator[None]:
        lock_path = self.data_root / "manifests" / "compaction" / ".compact.lock"
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def snapshot_source(path: str | Path) -> SourceSnapshot:
    source_path = Path(path)
    before = source_path.stat()
    digest = hashlib.sha256()
    with source_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    after = source_path.stat()
    if before.st_size != after.st_size or before.st_mtime_ns != after.st_mtime_ns:
        raise RuntimeError(f"source changed while hashing: {source_path}")
    return SourceSnapshot(size=after.st_size, mtime_ns=after.st_mtime_ns, sha256=digest.hexdigest())


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
    )
    summary = compactor.run(
        dry_run=args.dry_run,
        limit=args.limit,
        provider=args.provider,
    )
    if args.summary_only:
        failures = [asdict(result) for result in summary.results if result.status == "failed"]
        payload = {
            "dry_run": summary.dry_run,
            "data_root": summary.data_root,
            "started_at": summary.started_at,
            "finished_at": summary.finished_at,
            "result_count": len(summary.results),
            "status_counts": summary.status_counts,
            "failed": summary.failed,
            "failures": failures,
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
                    f"failed: {failure['source_path']} "
                    f"detail={failure.get('detail') or 'unknown'}"
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


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(timezone.utc)


if __name__ == "__main__":
    sys.exit(main())
