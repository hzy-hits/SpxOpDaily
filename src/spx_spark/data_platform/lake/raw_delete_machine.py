from __future__ import annotations

import fcntl
import os
import stat
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Iterator

from spx_spark.data_platform.lake.compact_support import SourceSnapshot
from spx_spark.data_platform.lake.layout import RawQuotePartition, parse_raw_quote_partition
from spx_spark.data_platform.lake.manifest import CompactionManifest, load_manifest

if TYPE_CHECKING:
    from spx_spark.data_platform.lake.compact import CompactionResult, QuoteLakeCompactor


class RawDeletePhase(StrEnum):
    RETAINED = "retained"
    METADATA = "metadata"
    CONTENT = "content"
    AUTHORIZED = "authorized"
    QUARANTINED = "quarantined"
    DELETED = "deleted"
    BLOCKED = "blocked"
    FAILED = "failed"


@dataclass(frozen=True)
class RawDeleteGate:
    phase: RawDeletePhase
    reason: str | None = None


@dataclass(frozen=True)
class RawDeleteEvidence:
    source: SourceSnapshot
    output: SourceSnapshot
    source_rows: int
    parquet_rows: int


def delete_raw_if_manifest_matches(
    compactor: QuoteLakeCompactor,
    partition: RawQuotePartition,
    *,
    expected_manifest: CompactionManifest,
    now: datetime,
    dry_run: bool = False,
) -> CompactionResult:
    """Delete only the exact source lineage already authorized by a caller."""
    from spx_spark.data_platform.lake.compact import CompactionResult, _as_utc

    lock = nullcontext() if dry_run else compactor._exclusive_lock()
    with lock:
        with _exclusive_raw_path_lock(partition.source_path):
            current = load_manifest(partition.manifest_path)
            if current != expected_manifest:
                return CompactionResult(
                    partition.source_relative_path,
                    expected_manifest.output_path,
                    "raw_delete_blocked",
                    row_count=expected_manifest.row_count,
                    source_sha256=expected_manifest.source_sha256,
                    detail="compaction manifest changed after replay authorization",
                )
            status = "empty_up_to_date" if current.status == "empty" else "up_to_date"
            result = CompactionResult(
                partition.source_relative_path,
                current.output_path,
                status,
                row_count=current.row_count,
                source_sha256=current.source_sha256,
            )
            return compactor._maybe_delete_raw(
                partition,
                result,
                manifest=current,
                now=_as_utc(now),
                dry_run=dry_run,
            )


@contextmanager
def _exclusive_raw_path_lock(source_path: Path) -> Iterator[None]:
    lock_path = source_path.with_name(f".{source_path.name}.lock")
    with lock_path.open("a", encoding="utf-8") as handle:
        os.fchmod(handle.fileno(), 0o600)
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def raw_delete_gate(
    *,
    enabled: bool,
    result_status: str,
    manifest: CompactionManifest,
    partition_end: datetime,
    now: datetime,
    grace_hours: int,
) -> RawDeleteGate:
    if not enabled or result_status not in {"compacted", "up_to_date"}:
        return RawDeleteGate(RawDeletePhase.RETAINED)
    if manifest.status != "verified" or partition_end > now:
        return RawDeleteGate(RawDeletePhase.RETAINED)
    try:
        completed_at = datetime.fromisoformat(manifest.completed_at)
    except ValueError:
        return RawDeleteGate(RawDeletePhase.BLOCKED, "manifest completed_at is invalid")
    if completed_at.tzinfo is None:
        return RawDeleteGate(
            RawDeletePhase.BLOCKED,
            "manifest completed_at must be timezone-aware",
        )
    grace_deadline = completed_at.astimezone(timezone.utc) + timedelta(hours=grace_hours)
    phase = RawDeletePhase.METADATA if now >= grace_deadline else RawDeletePhase.RETAINED
    return RawDeleteGate(phase)


def evidence_mismatch_reason(
    evidence: RawDeleteEvidence,
    manifest: CompactionManifest,
) -> str | None:
    if (
        evidence.output.sha256 != manifest.output_sha256
        or evidence.output.size != manifest.output_size
    ):
        return "parquet checksum or size does not match manifest"
    if (
        evidence.source.sha256 != manifest.source_sha256
        or evidence.source.size != manifest.source_size
    ):
        return "source checksum or size no longer matches manifest"
    if (
        evidence.source_rows != manifest.row_count
        or evidence.parquet_rows != manifest.row_count
        or evidence.source_rows != evidence.parquet_rows
    ):
        return (
            "row count mismatch "
            f"(source={evidence.source_rows} parquet={evidence.parquet_rows} "
            f"manifest={manifest.row_count})"
        )
    return None


def quarantined_evidence_matches(
    *,
    snapshot: SourceSnapshot,
    row_count: int,
    manifest: CompactionManifest,
) -> bool:
    return (
        snapshot.sha256 == manifest.source_sha256
        and snapshot.size == manifest.source_size
        and row_count == manifest.row_count
    )


def unsafe_raw_delete_target_reason(
    *,
    data_root: Path,
    raw_file_name: str,
    partition: RawQuotePartition,
) -> str | None:
    source = partition.source_path
    try:
        if source.is_symlink():
            return "refusing to delete via symlink"
    except OSError as exc:
        return f"source path lstat failed: {type(exc).__name__}: {exc}"
    try:
        root = data_root.resolve(strict=True)
    except OSError:
        return "data root is not resolvable"
    relative = Path(partition.source_relative_path)
    parts = relative.parts
    if len(parts) != 5 or parts[0] != "raw":
        return "source path is not a raw hourly landing file"
    expected_prefixes = ((1, "provider="), (2, "date="), (3, "hour="))
    malformed = next((label for index, label in expected_prefixes if not parts[index].startswith(label)), None)
    if malformed is not None:
        return f"malformed {malformed.removesuffix('=')} path segment"
    protected = {"latest", "runtime", "manifests", "lake", "analytics"}
    if any(part in protected for part in parts):
        return "refusing to touch latest/runtime/manifest/lake paths"
    if source.name.startswith(".") or source.name.endswith(".lock"):
        return "refusing to delete lock or hidden runtime files"
    if source.name != raw_file_name or parts[4] != raw_file_name:
        return "source file name does not match configured raw landing name"
    for cursor in _path_components(data_root, parts):
        try:
            if cursor.is_symlink():
                return "refusing to delete via symlink"
        except OSError as exc:
            return f"source path lstat failed: {type(exc).__name__}: {exc}"
    try:
        source.resolve(strict=True).relative_to(root)
    except (OSError, ValueError):
        return "source path is outside the data root"
    try:
        mode = source.lstat().st_mode
    except OSError as exc:
        return f"source path lstat failed: {type(exc).__name__}: {exc}"
    if not stat.S_ISREG(mode):
        return "source path is not a regular file"
    if parse_raw_quote_partition(data_root, source) is None:
        return "source path failed strict raw partition parse"
    return None


def _path_components(data_root: Path, parts: tuple[str, ...]) -> tuple[Path, ...]:
    paths = []
    cursor = data_root
    for part in parts:
        cursor = cursor / part
        paths.append(cursor)
    return tuple(paths)
