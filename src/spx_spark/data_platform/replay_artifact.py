"""Immutable session replay artifacts and artifact-authorized raw cleanup.

This module is deliberately content-agnostic: the L5 finalizer supplies the
deterministic review bytes.  L4 owns quote lineage verification, durable
publication, and exact-partition deletion authorization.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
from collections import Counter, defaultdict
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator, Sequence
from uuid import uuid4

from spx_spark.data_platform.lake.compact import CompactionResult, QuoteLakeCompactor
from spx_spark.data_platform.lake.compact_support import (
    count_jsonl_rows,
    count_parquet_rows,
    snapshot_source,
)
from spx_spark.data_platform.lake.layout import (
    RawQuotePartition,
    discover_raw_quote_partitions,
    parse_raw_quote_partition,
)
from spx_spark.data_platform.lake.manifest import CompactionManifest, load_manifest
from spx_spark.data_platform.replay_artifact_contract import (
    FINALIZER_LOCK,
    REPLAY_DATASET,
    REPLAY_MANIFEST_NAME,
    REPLAY_MANIFEST_VERSION,
    REPLAY_ROOT,
    REPLAY_SCHEMA_VERSION,
    REPLAY_STATUS,
    REPLAY_WRITER_VERSION,
    REVIEW_JSON_NAME,
    REVIEW_MARKDOWN_NAME,
    ArtifactPublishResult,
    ReplayArtifactError,
    ReplayArtifactManifest,
    ReplayCleanupSummary,
    ReplayDeleteResult,
    ReplayFileEvidence,
    ReplayFinalizerBusy,
    ReplaySourceEvidence,
    StoragePressure,
)


@dataclass(frozen=True, slots=True)
class PartitionPreparation:
    status: str
    partitions: tuple[RawQuotePartition, ...]
    sources: tuple[ReplaySourceEvidence, ...]
    results: tuple[CompactionResult, ...]
    errors: tuple[str, ...] = ()

    @property
    def ready(self) -> bool:
        return self.status == "verified"


def discover_partitions_for_window(
    data_root: str | Path,
    *,
    start: datetime,
    end: datetime,
    raw_file_name: str = "quotes.jsonl",
) -> tuple[RawQuotePartition, ...]:
    start_utc = _as_utc(start)
    end_utc = _as_utc(end)
    if end_utc <= start_utc:
        raise ValueError("replay window end must be after start")
    return tuple(
        partition
        for partition in discover_raw_quote_partitions(
            data_root,
            raw_file_name=raw_file_name,
        )
        if partition.start_at < end_utc and partition.end_at > start_utc
    )


def prepare_replay_sources(
    partitions: Sequence[RawQuotePartition],
    *,
    compactor: QuoteLakeCompactor,
    now: datetime,
    dry_run: bool = False,
) -> PartitionPreparation:
    ordered = tuple(sorted(partitions, key=lambda item: item.source_relative_path))
    if not ordered:
        return PartitionPreparation("no_sources", (), (), ())
    results: list[CompactionResult] = []
    sources: list[ReplaySourceEvidence] = []
    errors: list[str] = []
    for partition in ordered:
        result = compactor.compact_one(partition, now=_as_utc(now), dry_run=dry_run)
        results.append(result)
        if result.status in {"failed", "raw_delete_blocked", "raw_delete_failed"}:
            errors.append(f"{partition.source_relative_path}: {result.status}: {result.detail or ''}")
            continue
        if result.status in {"active_hour", "settling", "would_compact", "would_mark_empty"}:
            errors.append(f"{partition.source_relative_path}: {result.status}")
            continue
        try:
            sources.append(verify_compacted_partition(partition))
        except ReplayArtifactError as exc:
            errors.append(f"{partition.source_relative_path}: {exc}")
    if errors:
        status = "would_prepare" if dry_run and all(
            result.status in {"would_compact", "would_mark_empty", "up_to_date", "empty_up_to_date"}
            for result in results
        ) else "blocked"
        return PartitionPreparation(status, ordered, tuple(sources), tuple(results), tuple(errors))
    return PartitionPreparation("verified", ordered, tuple(sources), tuple(results))


def verify_compacted_partition(partition: RawQuotePartition) -> ReplaySourceEvidence:
    manifest = load_manifest(partition.manifest_path)
    if manifest is None:
        raise ReplayArtifactError("verified compaction manifest is missing or invalid")
    if manifest.source_path != partition.source_relative_path:
        raise ReplayArtifactError("compaction manifest source path mismatch")
    try:
        source = snapshot_source(partition.source_path)
        source_rows = count_jsonl_rows(partition.source_path)
    except (OSError, RuntimeError) as exc:
        raise ReplayArtifactError(f"source verification failed: {type(exc).__name__}: {exc}") from exc
    if (
        source.sha256 != manifest.source_sha256
        or source.size != manifest.source_size
        or source.mtime_ns != manifest.source_mtime_ns
    ):
        raise ReplayArtifactError("source sha256/size/mtime does not match compaction manifest")
    manifest_relative = partition.manifest_path.relative_to(partition.data_root).as_posix()
    if source.size == 0:
        if manifest.status != "empty" or source_rows != 0 or manifest.row_count != 0:
            raise ReplayArtifactError("empty source does not have an empty compaction manifest")
        if manifest.output_path is not None:
            raise ReplayArtifactError("empty source unexpectedly references a parquet output")
        return ReplaySourceEvidence(
            provider=partition.provider,
            utc_date=partition.session_date,
            hour=partition.hour,
            source_path=partition.source_relative_path,
            source_size=source.size,
            source_mtime_ns=source.mtime_ns,
            source_sha256=source.sha256,
            source_row_count=source_rows,
            compaction_manifest_path=manifest_relative,
            compaction_manifest_id=manifest.manifest_id,
            compaction_completed_at=manifest.completed_at,
            output_path=None,
            output_size=None,
            output_sha256=None,
            output_row_count=0,
            compaction_status=manifest.status,
        )
    if manifest.status != "verified":
        raise ReplayArtifactError("non-empty source lacks a verified compaction manifest")
    expected_output = partition.parquet_path.relative_to(partition.data_root).as_posix()
    if manifest.output_path != expected_output:
        raise ReplayArtifactError("compaction output path mismatch")
    if partition.parquet_path.is_symlink() or not partition.parquet_path.is_file():
        raise ReplayArtifactError("verified parquet output is missing or unsafe")
    try:
        output = snapshot_source(partition.parquet_path)
        output_rows = count_parquet_rows(partition.parquet_path)
    except (OSError, RuntimeError) as exc:
        raise ReplayArtifactError(f"parquet verification failed: {type(exc).__name__}: {exc}") from exc
    if output.sha256 != manifest.output_sha256 or output.size != manifest.output_size:
        raise ReplayArtifactError("parquet sha256/size does not match compaction manifest")
    if source_rows != output_rows or source_rows != manifest.row_count:
        raise ReplayArtifactError(
            "source/parquet/manifest row counts differ "
            f"({source_rows}/{output_rows}/{manifest.row_count})"
        )
    return ReplaySourceEvidence(
        provider=partition.provider,
        utc_date=partition.session_date,
        hour=partition.hour,
        source_path=partition.source_relative_path,
        source_size=source.size,
        source_mtime_ns=source.mtime_ns,
        source_sha256=source.sha256,
        source_row_count=source_rows,
        compaction_manifest_path=manifest_relative,
        compaction_manifest_id=manifest.manifest_id,
        compaction_completed_at=manifest.completed_at,
        output_path=manifest.output_path,
        output_size=output.size,
        output_sha256=output.sha256,
        output_row_count=output_rows,
        compaction_status=manifest.status,
    )


def replay_source_digest(sources: Sequence[ReplaySourceEvidence]) -> str:
    payload = [
        source.lineage_dict()
        for source in sorted(sources, key=lambda item: item.source_path)
    ]
    return hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()


def publish_replay_artifact(
    data_root: str | Path,
    *,
    trading_date: date,
    window_start: datetime,
    window_end: datetime,
    sources: Sequence[ReplaySourceEvidence],
    review_json: bytes,
    review_markdown: bytes,
    generated_at: datetime,
    dry_run: bool = False,
) -> ArtifactPublishResult:
    root = Path(data_root)
    ordered_sources = tuple(sorted(sources, key=lambda item: item.source_path))
    if not ordered_sources:
        raise ReplayArtifactError("cannot publish a replay artifact without quote sources")
    _validate_review_bytes(review_json, review_markdown, trading_date=trading_date)
    source_digest = replay_source_digest(ordered_sources)
    window_start_iso = _as_utc(window_start).isoformat()
    window_end_iso = _as_utc(window_end).isoformat()
    review_json_sha256 = hashlib.sha256(review_json).hexdigest()
    review_markdown_sha256 = hashlib.sha256(review_markdown).hexdigest()
    revision = replay_revision(
        source_digest=source_digest,
        window_start=window_start_iso,
        window_end=window_end_iso,
        review_json_size=len(review_json),
        review_json_sha256=review_json_sha256,
        review_markdown_size=len(review_markdown),
        review_markdown_sha256=review_markdown_sha256,
        schema_version=REPLAY_SCHEMA_VERSION,
        writer_version=REPLAY_WRITER_VERSION,
    )
    artifact_id = _artifact_id(trading_date.isoformat(), revision)
    final_dir = _artifact_dir(root, trading_date.isoformat(), revision)
    manifest_path = final_dir / REPLAY_MANIFEST_NAME
    review_json_relative = (final_dir / REVIEW_JSON_NAME).relative_to(root).as_posix()
    review_markdown_relative = (final_dir / REVIEW_MARKDOWN_NAME).relative_to(root).as_posix()
    requested_json = _bytes_evidence(review_json_relative, review_json)
    requested_markdown = _bytes_evidence(review_markdown_relative, review_markdown)
    if final_dir.exists():
        existing = verify_replay_artifact(
            manifest_path,
            data_root=root,
            verify_source_files=False,
        )
        if existing.sources != ordered_sources:
            raise ReplayArtifactError("existing replay revision source evidence changed")
        return ArtifactPublishResult(
            "already_published",
            artifact_id,
            revision,
            manifest_path.relative_to(root).as_posix(),
            len(ordered_sources),
        )

    manifest = ReplayArtifactManifest(
        manifest_version=REPLAY_MANIFEST_VERSION,
        dataset=REPLAY_DATASET,
        schema_version=REPLAY_SCHEMA_VERSION,
        writer_version=REPLAY_WRITER_VERSION,
        status=REPLAY_STATUS,
        artifact_id=artifact_id,
        trading_date=trading_date.isoformat(),
        window_start=window_start_iso,
        window_end=window_end_iso,
        source_digest=source_digest,
        revision=revision,
        generated_at=_as_utc(generated_at).isoformat(),
        sources=ordered_sources,
        review_json=requested_json,
        review_markdown=requested_markdown,
    )
    if dry_run:
        return ArtifactPublishResult(
            "would_publish",
            artifact_id,
            revision,
            manifest_path.relative_to(root).as_posix(),
            len(ordered_sources),
        )

    parent = final_dir.parent
    parent.mkdir(parents=True, exist_ok=True)
    staging = parent / f".{revision}.{uuid4().hex}.tmp"
    try:
        staging.mkdir(mode=0o700)
        _write_durable(staging / REVIEW_JSON_NAME, review_json)
        _write_durable(staging / REVIEW_MARKDOWN_NAME, review_markdown)
        _write_durable(
            staging / REPLAY_MANIFEST_NAME,
            _canonical_json_bytes(manifest.to_dict(), pretty=True),
        )
        _fsync_directory(staging)
        try:
            os.rename(staging, final_dir)
        except OSError:
            if not final_dir.exists():
                raise
        _fsync_directory(parent)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    # Source/parquet bytes were fully verified immediately before publication;
    # avoid another multi-gigabyte pass merely to validate our atomic write.
    verified = verify_replay_artifact(
        manifest_path,
        data_root=root,
        verify_source_files=False,
    )
    if verified.sources != ordered_sources:
        raise ReplayArtifactError("published replay source evidence changed after rename")
    if verified.artifact_id != artifact_id:
        raise ReplayArtifactError("published replay artifact identity changed after rename")
    return ArtifactPublishResult(
        "published",
        artifact_id,
        revision,
        manifest_path.relative_to(root).as_posix(),
        len(ordered_sources),
    )


def load_replay_manifest(path: str | Path) -> ReplayArtifactManifest:
    manifest_path = Path(path)
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("manifest root must be an object")
        manifest = ReplayArtifactManifest.from_dict(payload)
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
        raise ReplayArtifactError(f"invalid replay manifest {manifest_path}: {exc}") from exc
    return manifest


def verify_replay_artifact(
    manifest_path: str | Path,
    *,
    data_root: str | Path,
    verify_source_files: bool = True,
) -> ReplayArtifactManifest:
    root = Path(data_root)
    path = Path(manifest_path)
    manifest = load_replay_manifest(path)
    if (
        manifest.manifest_version != REPLAY_MANIFEST_VERSION
        or manifest.dataset != REPLAY_DATASET
        or manifest.schema_version != REPLAY_SCHEMA_VERSION
        or manifest.writer_version != REPLAY_WRITER_VERSION
        or manifest.status != REPLAY_STATUS
    ):
        raise ReplayArtifactError("replay manifest contract/version/status mismatch")
    if manifest.source_digest != replay_source_digest(manifest.sources):
        raise ReplayArtifactError("replay source digest does not match source lineage")
    expected_revision = replay_revision(
        source_digest=manifest.source_digest,
        window_start=manifest.window_start,
        window_end=manifest.window_end,
        review_json_size=manifest.review_json.size,
        review_json_sha256=manifest.review_json.sha256,
        review_markdown_size=manifest.review_markdown.size,
        review_markdown_sha256=manifest.review_markdown.sha256,
        schema_version=manifest.schema_version,
        writer_version=manifest.writer_version,
    )
    if manifest.revision != expected_revision:
        raise ReplayArtifactError("replay revision does not match complete artifact lineage")
    if manifest.artifact_id != _artifact_id(manifest.trading_date, manifest.revision):
        raise ReplayArtifactError("replay artifact id does not match date/source lineage")
    expected = _artifact_dir(root, manifest.trading_date, manifest.revision) / REPLAY_MANIFEST_NAME
    try:
        if path.resolve(strict=True) != expected.resolve(strict=True):
            raise ReplayArtifactError("replay manifest path does not match its identity")
    except OSError as exc:
        raise ReplayArtifactError(f"replay manifest path is not resolvable: {exc}") from exc
    _verify_artifact_file(root, manifest.review_json)
    _verify_artifact_file(root, manifest.review_markdown)
    if verify_source_files:
        deleted = _raw_deleted_evidence(root)
        for source in manifest.sources:
            _verify_artifact_source(root, source, deleted=deleted)
    return manifest


def latest_verified_replay_artifact(
    data_root: str | Path,
    *,
    trading_date: date,
    verify_source_files: bool = True,
) -> ReplayArtifactManifest:
    root = Path(data_root)
    grouped = _artifact_manifests_by_date(root)
    paths = grouped.get(trading_date.isoformat(), ())
    return _latest_verified_manifest(
        root,
        paths,
        verify_source_files=verify_source_files,
    )


def measure_storage_pressure(
    data_root: str | Path,
    *,
    action_free_bytes: int,
    warning_free_bytes: int,
    critical_free_bytes: int,
    free_bytes_override: int | None = None,
) -> StoragePressure:
    if not action_free_bytes > warning_free_bytes >= critical_free_bytes:
        raise ValueError("storage pressure thresholds must satisfy action > warning >= critical")
    usage = shutil.disk_usage(Path(data_root))
    free = usage.free if free_bytes_override is None else free_bytes_override
    if free <= critical_free_bytes:
        level = "critical"
    elif free <= warning_free_bytes:
        level = "warning"
    elif free <= action_free_bytes:
        level = "action"
    else:
        level = "normal"
    return StoragePressure(
        total_bytes=usage.total,
        used_bytes=max(0, usage.total - free),
        free_bytes=free,
        level=level,
        action_required=free <= action_free_bytes,
    )


def cleanup_authorized_replay_sources(
    data_root: str | Path,
    *,
    now: datetime,
    pressure: StoragePressure,
    raw_file_name: str = "quotes.jsonl",
    grace_hours: int = 24,
    max_backlog_days: int = 7,
    dry_run: bool = False,
) -> ReplayCleanupSummary:
    if grace_hours < 24:
        raise ValueError("replay raw deletion grace must be at least 24 hours")
    if max_backlog_days <= 0:
        raise ValueError("max_backlog_days must be positive")
    if not pressure.action_required:
        return _cleanup_summary("pressure_normal", dry_run=dry_run, pressure=pressure)

    root = Path(data_root)
    grouped = _artifact_manifests_by_date(root)
    dates = _cleanup_candidate_dates(
        root,
        grouped,
        raw_file_name=raw_file_name,
        limit=max_backlog_days,
    )
    results: list[ReplayDeleteResult] = []
    authorized_dates: list[str] = []
    blocked_dates: list[tuple[str, str]] = []
    deleter = QuoteLakeCompactor(
        root,
        raw_file_name=raw_file_name,
        settle_seconds=0,
        raw_delete_enabled=True,
        raw_delete_grace_hours=grace_hours,
    )
    utc_now = _as_utc(now)
    for trading_date in dates:
        expected_manifests: dict[str, CompactionManifest] = {}
        try:
            manifest = _latest_verified_manifest(root, grouped[trading_date])
            current = discover_partitions_for_window(
                root,
                start=_parse_aware(manifest.window_start),
                end=_parse_aware(manifest.window_end),
                raw_file_name=raw_file_name,
            )
            current_paths = {partition.source_relative_path for partition in current}
            authorized_paths = {source.source_path for source in manifest.sources}
            extra = sorted(current_paths - authorized_paths)
            if extra:
                raise ReplayArtifactError(
                    "published replay does not authorize late/current source(s): "
                    + ", ".join(extra)
                )
            present = [
                source
                for source in manifest.sources
                if source.source_size > 0 and (root / source.source_path).exists()
            ]
            for source in present:
                expected = load_manifest(root / source.compaction_manifest_path)
                if expected is None:
                    raise ReplayArtifactError(
                        f"compaction manifest missing: {source.compaction_manifest_path}"
                    )
                _compare_compaction_manifest(source, expected)
                expected_manifests[source.source_path] = expected
            immature = [
                source.source_path
                for source in present
                if not _source_grace_elapsed(source, now=utc_now, grace_hours=grace_hours)
            ]
            if immature:
                blocked_dates.append(
                    (trading_date, "grace_not_elapsed: " + ", ".join(immature))
                )
                continue
        except ReplayArtifactError as exc:
            blocked_dates.append((trading_date, str(exc)))
            continue

        if not present:
            continue
        authorized_dates.append(trading_date)
        # All target-day validation and grace checks complete before the first unlink.
        for source in present:
            partition = parse_raw_quote_partition(root, root / source.source_path)
            if partition is None:
                result = ReplayDeleteResult(
                    trading_date,
                    source.source_path,
                    "blocked",
                    source.source_size,
                    "artifact source is not a strict raw quote partition",
                )
            else:
                compacted = deleter.delete_raw_if_manifest_matches(
                    partition,
                    expected_manifest=expected_manifests[source.source_path],
                    now=utc_now,
                    dry_run=dry_run,
                )
                result = ReplayDeleteResult(
                    trading_date,
                    source.source_path,
                    compacted.status,
                    source.source_size,
                    compacted.detail,
                )
            results.append(result)

    deleted = [result for result in results if result.status == "raw_deleted"]
    planned = [result for result in results if result.status == "would_delete_raw"]
    failures = [
        result
        for result in results
        if result.status not in {"raw_deleted", "would_delete_raw"}
    ]
    if failures:
        status = "delete_failed"
    elif blocked_dates:
        status = "blocked"
    elif deleted:
        status = "deleted"
    elif planned:
        status = "would_delete"
    else:
        status = "no_authorized_sources"
    return ReplayCleanupSummary(
        status=status,
        dry_run=dry_run,
        pressure=pressure,
        artifact_dates_scanned=dates,
        authorized_dates=tuple(authorized_dates),
        blocked_dates=tuple(blocked_dates),
        results=tuple(results),
        deleted_files=len(deleted),
        deleted_bytes=sum(result.source_size for result in deleted),
        would_delete_files=len(planned),
        would_delete_bytes=sum(result.source_size for result in planned),
    )


@contextmanager
def session_finalizer_lock(
    data_root: str | Path,
    *,
    dry_run: bool = False,
) -> Iterator[None]:
    if dry_run:
        with nullcontext():
            yield
        return
    lock_path = Path(data_root) / "manifests" / "session-replay" / FINALIZER_LOCK
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ReplayFinalizerBusy("session finalizer is already running") from exc
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _latest_verified_manifest(
    data_root: Path,
    paths: Sequence[Path],
    *,
    verify_source_files: bool = True,
) -> ReplayArtifactManifest:
    loaded: list[tuple[datetime, Path, ReplayArtifactManifest]] = []
    for path in paths:
        manifest = load_replay_manifest(path)
        loaded.append((_parse_aware(manifest.generated_at), path, manifest))
    if not loaded:
        raise ReplayArtifactError("no replay artifact exists for trading date")
    _generated, path, _manifest = max(
        loaded,
        key=lambda item: (item[0], len(item[2].sources), str(item[1])),
    )
    return verify_replay_artifact(
        path,
        data_root=data_root,
        verify_source_files=verify_source_files,
    )


def _artifact_manifests_by_date(data_root: Path) -> dict[str, tuple[Path, ...]]:
    grouped: dict[str, list[Path]] = defaultdict(list)
    root = data_root / REPLAY_ROOT / f"schema={REPLAY_SCHEMA_VERSION}"
    for path in root.glob(f"date=*/revision=*/{REPLAY_MANIFEST_NAME}"):
        date_part = path.parent.parent.name
        if date_part.startswith("date="):
            grouped[date_part.removeprefix("date=")].append(path)
    return {key: tuple(sorted(value)) for key, value in grouped.items()}


def _cleanup_candidate_dates(
    data_root: Path,
    grouped: dict[str, tuple[Path, ...]],
    *,
    raw_file_name: str,
    limit: int,
) -> tuple[str, ...]:
    """Select the oldest artifact dates that still have raw cleanup work.

    Already-clean dates do not consume the bounded batch forever. Invalid
    artifact metadata remains selected so corruption is visible and cannot be
    skipped in favor of deleting newer evidence.
    """

    raw_partitions = discover_raw_quote_partitions(
        data_root,
        raw_file_name=raw_file_name,
    )
    selected: list[str] = []
    for trading_date in sorted(grouped):
        try:
            loaded = [load_replay_manifest(path) for path in grouped[trading_date]]
            latest = max(
                loaded,
                key=lambda item: (
                    _parse_aware(item.generated_at),
                    len(item.sources),
                    item.revision,
                ),
            )
            start = _parse_aware(latest.window_start)
            end = _parse_aware(latest.window_end)
            has_raw_work = any(
                partition.start_at < end and partition.end_at > start
                for partition in raw_partitions
            )
        except (ReplayArtifactError, ValueError):
            has_raw_work = True
        if has_raw_work:
            selected.append(trading_date)
            if len(selected) >= limit:
                break
    return tuple(selected)


def _verify_artifact_source(
    data_root: Path,
    source: ReplaySourceEvidence,
    *,
    deleted: set[tuple[object, ...]],
) -> None:
    source_path = data_root / source.source_path
    manifest_path = data_root / source.compaction_manifest_path
    compaction = load_manifest(manifest_path)
    if compaction is None:
        raise ReplayArtifactError(f"compaction manifest missing: {source.compaction_manifest_path}")
    _compare_compaction_manifest(source, compaction)
    if source.output_path is not None:
        output_path = data_root / source.output_path
        if output_path.is_symlink() or not output_path.is_file():
            raise ReplayArtifactError(f"replay parquet output missing: {source.output_path}")
        output = snapshot_source(output_path)
        output_rows = count_parquet_rows(output_path)
        if (
            output.sha256 != source.output_sha256
            or output.size != source.output_size
            or output_rows != source.output_row_count
        ):
            raise ReplayArtifactError(f"replay parquet evidence changed: {source.output_path}")
    if source_path.exists():
        if source_path.is_symlink() or not source_path.is_file():
            raise ReplayArtifactError(f"replay raw source is unsafe: {source.source_path}")
        snapshot = snapshot_source(source_path)
        rows = count_jsonl_rows(source_path)
        if (
            snapshot.sha256 != source.source_sha256
            or snapshot.size != source.source_size
            or snapshot.mtime_ns != source.source_mtime_ns
            or rows != source.source_row_count
        ):
            raise ReplayArtifactError(f"replay raw source evidence changed: {source.source_path}")
        return
    audit_key = (
        source.source_path,
        source.source_sha256,
        source.source_size,
        source.output_path,
        source.output_sha256,
        source.output_size,
        source.source_row_count,
    )
    if audit_key not in deleted:
        raise ReplayArtifactError(
            f"replay raw source is missing without matching deletion audit: {source.source_path}"
        )


def _compare_compaction_manifest(
    source: ReplaySourceEvidence,
    manifest: CompactionManifest,
) -> None:
    actual = (
        manifest.manifest_id,
        manifest.status,
        manifest.source_path,
        manifest.source_size,
        manifest.source_mtime_ns,
        manifest.source_sha256,
        manifest.output_path,
        manifest.output_size,
        manifest.output_sha256,
        manifest.row_count,
        manifest.completed_at,
    )
    expected = (
        source.compaction_manifest_id,
        source.compaction_status,
        source.source_path,
        source.source_size,
        source.source_mtime_ns,
        source.source_sha256,
        source.output_path,
        source.output_size,
        source.output_sha256,
        source.source_row_count,
        source.compaction_completed_at,
    )
    if actual != expected:
        raise ReplayArtifactError(
            f"compaction lineage changed for replay source: {source.source_path}"
        )


def _raw_deleted_evidence(data_root: Path) -> set[tuple[object, ...]]:
    audit = data_root / "manifests" / "compaction" / "raw_deletion_audit.jsonl"
    if not audit.is_file():
        return set()
    evidence: set[tuple[object, ...]] = set()
    try:
        with audit.open(encoding="utf-8") as handle:
            for raw in handle:
                if not raw.strip():
                    continue
                payload = json.loads(raw)
                if payload.get("event") != "final" or payload.get("status") != "raw_deleted":
                    continue
                evidence.add(
                    (
                        payload.get("source_path"),
                        payload.get("source_sha256"),
                        payload.get("source_size"),
                        payload.get("output_path"),
                        payload.get("output_sha256"),
                        payload.get("output_size"),
                        payload.get("row_count"),
                    )
                )
    except (OSError, json.JSONDecodeError) as exc:
        raise ReplayArtifactError(f"raw deletion audit is unreadable: {exc}") from exc
    return evidence


def _source_grace_elapsed(
    source: ReplaySourceEvidence,
    *,
    now: datetime,
    grace_hours: int,
) -> bool:
    grace = timedelta(hours=grace_hours)
    source_mtime = datetime.fromtimestamp(source.source_mtime_ns / 1_000_000_000, tz=timezone.utc)
    completed_at = _parse_aware(source.compaction_completed_at)
    return now >= source_mtime + grace and now >= completed_at + grace


def _verify_artifact_file(data_root: Path, evidence: ReplayFileEvidence) -> None:
    path = data_root / evidence.path
    if path.is_symlink() or not path.is_file():
        raise ReplayArtifactError(f"replay artifact file is missing or unsafe: {evidence.path}")
    snapshot = snapshot_source(path)
    if snapshot.sha256 != evidence.sha256 or snapshot.size != evidence.size:
        raise ReplayArtifactError(f"replay artifact file evidence changed: {evidence.path}")


def _artifact_dir(data_root: Path, trading_date: str, revision: str) -> Path:
    return (
        data_root
        / REPLAY_ROOT
        / f"schema={REPLAY_SCHEMA_VERSION}"
        / f"date={trading_date}"
        / f"revision={revision}"
    )


def replay_revision(
    *,
    source_digest: str,
    window_start: str,
    window_end: str,
    review_json_size: int,
    review_json_sha256: str,
    review_markdown_size: int,
    review_markdown_sha256: str,
    schema_version: str,
    writer_version: str,
) -> str:
    lineage = {
        "source_digest": source_digest,
        "window_start": window_start,
        "window_end": window_end,
        "review_json": {"size": review_json_size, "sha256": review_json_sha256},
        "review_markdown": {
            "size": review_markdown_size,
            "sha256": review_markdown_sha256,
        },
        "schema_version": schema_version,
        "writer_version": writer_version,
    }
    return hashlib.sha256(_canonical_json_bytes(lineage)).hexdigest()


def _artifact_id(trading_date: str, revision: str) -> str:
    return f"spx-session-replay:{trading_date}:{revision}"


def _validate_review_bytes(
    review_json: bytes,
    review_markdown: bytes,
    *,
    trading_date: date,
) -> None:
    try:
        payload = json.loads(review_json)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ReplayArtifactError(f"deterministic review JSON is invalid: {exc}") from exc
    if not isinstance(payload, dict) or payload.get("trading_date") != trading_date.isoformat():
        raise ReplayArtifactError("deterministic review JSON trading_date mismatch")
    try:
        markdown = review_markdown.decode("utf-8").strip()
    except UnicodeDecodeError as exc:
        raise ReplayArtifactError("deterministic review Markdown is not UTF-8") from exc
    if not markdown:
        raise ReplayArtifactError("deterministic review Markdown is empty")


def _bytes_evidence(path: str, payload: bytes) -> ReplayFileEvidence:
    return ReplayFileEvidence(path=path, size=len(payload), sha256=hashlib.sha256(payload).hexdigest())


def _canonical_json_bytes(payload: object, *, pretty: bool = False) -> bytes:
    if pretty:
        encoded = json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    else:
        encoded = json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    return (encoded + "\n").encode("utf-8")


def _write_durable(path: Path, payload: bytes) -> None:
    with path.open("xb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(path, 0o600)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _parse_aware(raw: str) -> datetime:
    try:
        value = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ReplayArtifactError(f"invalid timezone-aware datetime: {raw}") from exc
    return _as_utc(value)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("replay datetimes must be timezone-aware")
    return value.astimezone(timezone.utc)


def _cleanup_summary(
    status: str,
    *,
    dry_run: bool,
    pressure: StoragePressure,
) -> ReplayCleanupSummary:
    return ReplayCleanupSummary(
        status=status,
        dry_run=dry_run,
        pressure=pressure,
        artifact_dates_scanned=(),
        authorized_dates=(),
        blocked_dates=(),
        results=(),
        deleted_files=0,
        deleted_bytes=0,
        would_delete_files=0,
        would_delete_bytes=0,
    )


def compaction_status_counts(preparation: PartitionPreparation) -> dict[str, int]:
    return dict(sorted(Counter(result.status for result in preparation.results).items()))
