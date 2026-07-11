from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import stat
import sys
from collections import Counter
from contextlib import contextmanager, nullcontext
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, Sequence
from uuid import uuid4

import duckdb

from spx_spark.config import StorageSettings
from spx_spark.data_platform.settings import DataPlatformSettings
from spx_spark.data_platform.lake.layout import (
    QUOTE_SCHEMA_VERSION,
    QUOTE_WRITER_VERSION,
    RawQuotePartition,
    discover_raw_quote_partitions,
    parse_raw_quote_partition,
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

RAW_DELETE_FAILURE_STATUSES = frozenset(
    {
        "raw_delete_blocked",
        "raw_delete_failed",
        "raw_delete_audit_failed",
    }
)
SUMMARY_FAILURE_STATUSES = frozenset({"failed"}) | RAW_DELETE_FAILURE_STATUSES
RAW_DELETION_AUDIT_NAME = "raw_deletion_audit.jsonl"


LIMITED_WORK_STATUSES = frozenset(
    {
        "compacted",
        "empty",
        "would_compact",
        "would_mark_empty",
        "raw_deleted",
        "would_delete_raw",
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
        return any(result.status in SUMMARY_FAILURE_STATUSES for result in self.results)

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
        raw_delete_enabled: bool = False,
        raw_delete_grace_hours: int = 72,
    ) -> None:
        if settle_seconds < 0:
            raise ValueError("settle_seconds must be >= 0")
        if not 1 <= compression_level <= 22:
            raise ValueError("compression_level must be between 1 and 22")
        if raw_delete_grace_hours < 24:
            raise ValueError("raw_delete_grace_hours must be at least 24")
        self.data_root = Path(data_root)
        self.raw_file_name = raw_file_name
        self.settle_seconds = settle_seconds
        self.compression_level = compression_level
        self.raw_delete_enabled = raw_delete_enabled
        self.raw_delete_grace_hours = raw_delete_grace_hours

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
            result = CompactionResult(
                source_label,
                existing.output_path,
                status,
                row_count=existing.row_count,
                source_sha256=existing.source_sha256,
            )
            return self._maybe_delete_raw(
                partition,
                result,
                manifest=existing,
                now=now,
                dry_run=dry_run,
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
            assert existing is not None
            result = CompactionResult(
                source_label,
                output_label,
                "up_to_date",
                row_count=existing.row_count,
                source_sha256=snapshot.sha256,
            )
            return self._maybe_delete_raw(
                partition,
                result,
                manifest=existing,
                now=now,
                dry_run=dry_run,
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

        result = CompactionResult(
            source_label,
            output_label,
            "compacted",
            row_count=stats.row_count,
            source_sha256=snapshot.sha256,
        )
        return self._maybe_delete_raw(
            partition,
            result,
            manifest=manifest,
            now=now,
            dry_run=False,
        )

    def _maybe_delete_raw(
        self,
        partition: RawQuotePartition,
        result: CompactionResult,
        *,
        manifest: CompactionManifest,
        now: datetime,
        dry_run: bool,
    ) -> CompactionResult:
        """Delete a verified closed-hour JSONL only when every safety gate passes."""

        if not self.raw_delete_enabled:
            return result
        if result.status not in {"compacted", "up_to_date"}:
            return result
        if manifest.status != "verified":
            return result
        if partition.end_at > now:
            return result
        try:
            completed_at = datetime.fromisoformat(manifest.completed_at)
        except ValueError:
            return self._raw_delete_blocked(
                result,
                "manifest completed_at is invalid",
            )
        if completed_at.tzinfo is None:
            return self._raw_delete_blocked(
                result,
                "manifest completed_at must be timezone-aware",
            )
        grace_deadline = completed_at.astimezone(timezone.utc) + timedelta(
            hours=self.raw_delete_grace_hours
        )
        if now < grace_deadline:
            return result

        unsafe_reason = self._unsafe_raw_delete_target_reason(partition)
        if unsafe_reason is not None:
            return self._raw_delete_blocked(result, unsafe_reason)
        if manifest.source_path != partition.source_relative_path:
            return self._raw_delete_blocked(
                result,
                "manifest source_path does not match partition source path",
            )
        if not manifest.output_path or not manifest.output_sha256 or manifest.output_size is None:
            return self._raw_delete_blocked(
                result,
                "verified manifest is missing parquet output metadata",
            )
        expected_output = partition.parquet_path.relative_to(self.data_root).as_posix()
        if manifest.output_path != expected_output:
            return self._raw_delete_blocked(
                result,
                "manifest output_path does not match partition parquet path",
            )
        if partition.parquet_path.is_symlink() or not partition.parquet_path.is_file():
            return self._raw_delete_blocked(result, "parquet output is missing")
        try:
            output = snapshot_source(partition.parquet_path)
        except (OSError, RuntimeError) as exc:
            return self._raw_delete_blocked(
                result,
                f"parquet verification failed: {type(exc).__name__}: {exc}",
            )
        if output.sha256 != manifest.output_sha256 or output.size != manifest.output_size:
            return self._raw_delete_blocked(
                result,
                "parquet checksum or size does not match manifest",
            )
        try:
            source = snapshot_source(partition.source_path)
        except (OSError, RuntimeError) as exc:
            return self._raw_delete_blocked(
                result,
                f"source verification failed: {type(exc).__name__}: {exc}",
            )
        if source.sha256 != manifest.source_sha256 or source.size != manifest.source_size:
            return self._raw_delete_blocked(
                result,
                "source checksum or size no longer matches manifest",
                source_sha256=source.sha256,
            )
        try:
            source_rows = count_jsonl_rows(partition.source_path)
            parquet_rows = count_parquet_rows(partition.parquet_path)
        except (OSError, RuntimeError) as exc:
            return self._raw_delete_blocked(
                result,
                f"row count verification failed: {type(exc).__name__}: {exc}",
                source_sha256=source.sha256,
            )
        if (
            source_rows != manifest.row_count
            or parquet_rows != manifest.row_count
            or source_rows != parquet_rows
        ):
            return self._raw_delete_blocked(
                result,
                (
                    "row count mismatch "
                    f"(source={source_rows} parquet={parquet_rows} "
                    f"manifest={manifest.row_count})"
                ),
                source_sha256=source.sha256,
            )
        if dry_run:
            return CompactionResult(
                result.source_path,
                result.output_path,
                "would_delete_raw",
                row_count=result.row_count,
                source_sha256=source.sha256,
                detail="raw JSONL eligible for verified deletion",
            )

        audit_base = {
            "source_path": partition.source_relative_path,
            "output_path": manifest.output_path,
            "source_sha256": source.sha256,
            "output_sha256": output.sha256,
            "source_size": source.size,
            "output_size": output.size,
            "row_count": manifest.row_count,
        }
        try:
            self._append_raw_deletion_audit(
                {
                    **audit_base,
                    "event": "authorize",
                    "status": "authorized",
                    "detail": "pre-delete verification passed; quarantine unlink authorized",
                },
                now=now,
            )
        except OSError as exc:
            return CompactionResult(
                result.source_path,
                result.output_path,
                "raw_delete_audit_failed",
                row_count=result.row_count,
                source_sha256=source.sha256,
                detail=f"pre-delete audit fsync failed: {type(exc).__name__}: {exc}",
            )

        quarantine_path = partition.source_path.with_name(
            f".{partition.source_path.name}.raw-delete-quarantine.{uuid4().hex}"
        )
        try:
            os.rename(partition.source_path, quarantine_path)
        except OSError as exc:
            failure = CompactionResult(
                result.source_path,
                result.output_path,
                "raw_delete_failed",
                row_count=result.row_count,
                source_sha256=source.sha256,
                detail=f"quarantine rename failed: {type(exc).__name__}: {exc}",
            )
            return self._finalize_raw_deletion_audit(
                failure,
                audit_base=audit_base,
                now=now,
                quarantine_path=None,
            )

        try:
            quarantined = snapshot_source(quarantine_path)
            quarantined_rows = count_jsonl_rows(quarantine_path)
        except (OSError, RuntimeError) as exc:
            failure = CompactionResult(
                result.source_path,
                result.output_path,
                "raw_delete_failed",
                row_count=result.row_count,
                source_sha256=source.sha256,
                detail=(
                    "quarantined raw preserved after re-verification error: "
                    f"{type(exc).__name__}: {exc}; "
                    f"quarantine_path={quarantine_path.relative_to(self.data_root).as_posix()}"
                ),
            )
            return self._finalize_raw_deletion_audit(
                failure,
                audit_base=audit_base,
                now=now,
                quarantine_path=quarantine_path,
            )
        if (
            quarantined.sha256 != manifest.source_sha256
            or quarantined.size != manifest.source_size
            or quarantined_rows != manifest.row_count
        ):
            failure = CompactionResult(
                result.source_path,
                result.output_path,
                "raw_delete_failed",
                row_count=result.row_count,
                source_sha256=quarantined.sha256,
                detail=(
                    "quarantined raw preserved after post-rename mismatch "
                    f"(sha256/size/rows); "
                    f"quarantine_path={quarantine_path.relative_to(self.data_root).as_posix()}"
                ),
            )
            return self._finalize_raw_deletion_audit(
                failure,
                audit_base={
                    **audit_base,
                    "source_sha256": quarantined.sha256,
                    "source_size": quarantined.size,
                    "row_count": quarantined_rows,
                },
                now=now,
                quarantine_path=quarantine_path,
            )

        try:
            quarantine_path.unlink()
        except OSError as exc:
            failure = CompactionResult(
                result.source_path,
                result.output_path,
                "raw_delete_failed",
                row_count=result.row_count,
                source_sha256=source.sha256,
                detail=(
                    "quarantined raw preserved after unlink failure: "
                    f"{type(exc).__name__}: {exc}; "
                    f"quarantine_path={quarantine_path.relative_to(self.data_root).as_posix()}"
                ),
            )
            return self._finalize_raw_deletion_audit(
                failure,
                audit_base=audit_base,
                now=now,
                quarantine_path=quarantine_path,
            )

        success = CompactionResult(
            result.source_path,
            result.output_path,
            "raw_deleted",
            row_count=result.row_count,
            source_sha256=source.sha256,
            detail="deleted verified closed-hour raw JSONL after grace period",
        )
        return self._finalize_raw_deletion_audit(
            success,
            audit_base=audit_base,
            now=now,
            quarantine_path=None,
        )

    def _raw_delete_blocked(
        self,
        result: CompactionResult,
        detail: str,
        *,
        source_sha256: str | None = None,
    ) -> CompactionResult:
        return CompactionResult(
            result.source_path,
            result.output_path,
            "raw_delete_blocked",
            row_count=result.row_count,
            source_sha256=source_sha256 if source_sha256 is not None else result.source_sha256,
            detail=detail,
        )

    def _raw_deletion_audit_path(self) -> Path:
        return self.data_root / "manifests" / "compaction" / RAW_DELETION_AUDIT_NAME

    def _append_raw_deletion_audit(
        self,
        entry: dict[str, object],
        *,
        now: datetime,
    ) -> None:
        """Append one owner-readable audit line; raises if durable write/fsync fails."""

        payload = {
            "at": _as_utc(now).isoformat(),
            **entry,
        }
        # Never persist secrets or raw quote payloads — only paths/hashes/metadata.
        encoded = (
            json.dumps(
                payload,
                allow_nan=False,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")
        audit_path = self._raw_deletion_audit_path()
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(audit_path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
        try:
            os.fchmod(descriptor, 0o600)
            written = 0
            while written < len(encoded):
                written += os.write(descriptor, encoded[written:])
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _finalize_raw_deletion_audit(
        self,
        result: CompactionResult,
        *,
        audit_base: dict[str, object],
        now: datetime,
        quarantine_path: Path | None,
    ) -> CompactionResult:
        quarantine_label = None
        if quarantine_path is not None:
            try:
                quarantine_label = quarantine_path.relative_to(self.data_root).as_posix()
            except ValueError:
                quarantine_label = str(quarantine_path)
        try:
            self._append_raw_deletion_audit(
                {
                    **audit_base,
                    "event": "final",
                    "status": result.status,
                    "detail": result.detail,
                    "quarantine_path": quarantine_label,
                },
                now=now,
            )
        except OSError as exc:
            return CompactionResult(
                result.source_path,
                result.output_path,
                "raw_delete_audit_failed",
                row_count=result.row_count,
                source_sha256=result.source_sha256,
                detail=(
                    f"final audit fsync failed after status={result.status}: "
                    f"{type(exc).__name__}: {exc}"
                    + (f"; prior_detail={result.detail}" if result.detail else "")
                ),
            )
        return result

    def _unsafe_raw_delete_target_reason(self, partition: RawQuotePartition) -> str | None:
        """Reject anything outside a closed-hour raw landing file."""

        source = partition.source_path
        try:
            if source.is_symlink():
                return "refusing to delete via symlink"
        except OSError as exc:
            return f"source path lstat failed: {type(exc).__name__}: {exc}"

        try:
            root = self.data_root.resolve(strict=True)
        except OSError:
            return "data root is not resolvable"

        relative = Path(partition.source_relative_path)
        parts = relative.parts
        # raw / provider=... / date=... / hour=... / quotes.jsonl
        if len(parts) != 5 or parts[0] != "raw":
            return "source path is not a raw hourly landing file"
        if not parts[1].startswith("provider="):
            return "malformed provider path segment"
        if not parts[2].startswith("date="):
            return "malformed date path segment"
        if not parts[3].startswith("hour="):
            return "malformed hour path segment"
        if any(part in {"latest", "runtime", "manifests", "lake", "analytics"} for part in parts):
            return "refusing to touch latest/runtime/manifest/lake paths"
        if source.name.startswith(".") or source.name.endswith(".lock"):
            return "refusing to delete lock or hidden runtime files"
        if source.name != self.raw_file_name or parts[4] != self.raw_file_name:
            return "source file name does not match configured raw landing name"

        # Reject symlinks in any path component under data_root.
        cursor = self.data_root
        for part in parts:
            cursor = cursor / part
            try:
                if cursor.is_symlink():
                    return "refusing to delete via symlink"
            except OSError as exc:
                return f"source path lstat failed: {type(exc).__name__}: {exc}"
        try:
            resolved = source.resolve(strict=True)
            resolved.relative_to(root)
        except (OSError, ValueError):
            return "source path is outside the data root"
        try:
            mode = source.lstat().st_mode
        except OSError as exc:
            return f"source path lstat failed: {type(exc).__name__}: {exc}"
        if not stat.S_ISREG(mode):
            return "source path is not a regular file"
        if parse_raw_quote_partition(self.data_root, source) is None:
            return "source path failed strict raw partition parse"
        return None

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


def count_jsonl_rows(path: str | Path) -> int:
    count = 0
    with Path(path).open("rb") as handle:
        for line in handle:
            if line.strip():
                count += 1
    return count


def count_parquet_rows(path: str | Path) -> int:
    connection = duckdb.connect()
    try:
        row = connection.execute(
            "SELECT count(*)::BIGINT FROM read_parquet(?)",
            [str(path)],
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        raise RuntimeError(f"unable to count parquet rows: {path}")
    return int(row[0])


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


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return value.astimezone(timezone.utc)


if __name__ == "__main__":
    sys.exit(main())
