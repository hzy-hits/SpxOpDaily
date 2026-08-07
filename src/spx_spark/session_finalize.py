"""Finalize one SPX trading session into an immutable replay artifact.

The replay artifact always contains the deterministic post-close review.  An
optional human-facing LLM/write/push phase reuses the same in-memory payload;
notification failures never revoke an already-published artifact.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Sequence

from spx_spark.config import StorageSettings
from spx_spark.data_platform.lake.compact import QuoteLakeCompactor
from spx_spark.data_platform.replay_artifact import (
    ArtifactPublishResult,
    PartitionPreparation,
    ReplayArtifactError,
    ReplayCleanupSummary,
    StoragePressure,
    cleanup_authorized_replay_sources,
    compaction_status_counts,
    discover_partitions_for_window,
    measure_storage_pressure,
    latest_verified_replay_artifact,
    prepare_replay_sources,
    publish_replay_artifact,
    session_finalizer_lock,
)
from spx_spark.data_platform.settings import DataPlatformSettings
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET, MarketCalendar
from spx_spark.post_close_render import render_markdown
from spx_spark.post_close_review import build_review_payload
from spx_spark.post_close_runtime import (
    ReviewLlmSettings,
    default_hermes_export_dir,
    default_latest_markdown_path,
    maybe_write_llm_review,
    push_review,
    review_paths,
    write_outputs,
)


@dataclass(frozen=True, slots=True)
class SessionFinalizeResult:
    status: str
    dry_run: bool
    trading_date: str | None
    window_start: str | None
    window_end: str | None
    discovered_partitions: int
    compaction_status_counts: dict[str, int]
    preparation_status: str | None
    preparation_errors: tuple[str, ...]
    artifact: ArtifactPublishResult | None
    cleanup: ReplayCleanupSummary
    human_review: dict[str, object] | None

    @property
    def failed(self) -> bool:
        return self.status in {
            "blocked",
            "failed",
            "busy",
            "cleanup_failed",
            "cleanup_blocked",
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "dry_run": self.dry_run,
            "trading_date": self.trading_date,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "discovered_partitions": self.discovered_partitions,
            "compaction_status_counts": self.compaction_status_counts,
            "preparation_status": self.preparation_status,
            "preparation_errors": list(self.preparation_errors),
            "artifact": self.artifact.to_dict() if self.artifact else None,
            "cleanup": self.cleanup.to_dict(),
            "human_review": self.human_review,
        }


@dataclass(frozen=True, slots=True)
class _FinalizedCore:
    result: SessionFinalizeResult
    payload: dict[str, Any] | None
    deterministic_markdown: str | None


def resolve_finalize_date(
    raw: str,
    *,
    now: datetime,
    calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR,
) -> date:
    if raw.lower() == "auto":
        selected = calendar.completed_review_date(now)
    else:
        selected = date.fromisoformat(raw)
    if calendar.session(selected) is None:
        raise ValueError(f"{selected.isoformat()} is not a trading day")
    return selected


def session_research_window(
    trading_date: date,
    *,
    calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR,
) -> tuple[datetime, datetime]:
    if calendar.session(trading_date) is None:
        raise ValueError(f"{trading_date.isoformat()} is not a trading day")
    return (
        datetime.combine(trading_date - timedelta(days=1), time(20, 15), ET),
        datetime.combine(trading_date, time(16, 15), ET),
    )


def run_session_finalize(
    *,
    selected_date: date,
    now: datetime,
    storage_settings: StorageSettings,
    platform_settings: DataPlatformSettings,
    dry_run: bool,
    max_backlog_days: int,
    no_llm: bool,
    no_push: bool,
) -> SessionFinalizeResult:
    core = _finalize_core(
        selected_date=selected_date,
        now=now,
        storage_settings=storage_settings,
        platform_settings=platform_settings,
        dry_run=dry_run,
        max_backlog_days=max_backlog_days,
    )
    if core.result.artifact is not None and core.result.artifact.status == "already_published":
        return replace(
            core.result,
            human_review={
                "status": "skipped",
                "reason": "artifact_already_published",
            },
        )
    if (
        core.result.status in {"blocked", "failed", "busy"}
        or dry_run
        or core.payload is None
        or core.deterministic_markdown is None
    ):
        return core.result
    human_review = _run_human_review(
        payload=core.payload,
        deterministic_markdown=core.deterministic_markdown,
        trading_date=selected_date,
        storage_settings=storage_settings,
        no_llm=no_llm,
        no_push=no_push,
    )
    return replace(core.result, human_review=human_review)


def run_pressure_check(
    *,
    now: datetime,
    storage_settings: StorageSettings,
    platform_settings: DataPlatformSettings,
    dry_run: bool,
    max_backlog_days: int,
) -> SessionFinalizeResult:
    pressure = _measure_pressure(platform_settings)
    with session_finalizer_lock(platform_settings.data_root, dry_run=dry_run):
        cleanup = cleanup_authorized_replay_sources(
            platform_settings.data_root,
            now=now,
            pressure=pressure,
            raw_file_name=storage_settings.raw_file_name,
            grace_hours=platform_settings.replay_raw_delete_grace_hours,
            max_backlog_days=max_backlog_days,
            dry_run=dry_run,
        )
    status = _status_after_cleanup(
        cleanup,
        success="pressure_checked",
        failure="failed",
    )
    return SessionFinalizeResult(
        status=status,
        dry_run=dry_run,
        trading_date=None,
        window_start=None,
        window_end=None,
        discovered_partitions=0,
        compaction_status_counts={},
        preparation_status=None,
        preparation_errors=(),
        artifact=None,
        cleanup=cleanup,
        human_review=None,
    )


def _finalize_core(
    *,
    selected_date: date,
    now: datetime,
    storage_settings: StorageSettings,
    platform_settings: DataPlatformSettings,
    dry_run: bool,
    max_backlog_days: int,
) -> _FinalizedCore:
    start, end = session_research_window(selected_date)
    pressure = _measure_pressure(platform_settings)
    cleanup = _not_run_cleanup(pressure, dry_run=dry_run)
    preparation: PartitionPreparation | None = None
    artifact: ArtifactPublishResult | None = None
    payload: dict[str, Any] | None = None
    deterministic_markdown: str | None = None
    with session_finalizer_lock(platform_settings.data_root, dry_run=dry_run):
        partitions = discover_partitions_for_window(
            platform_settings.data_root,
            start=start,
            end=end,
            raw_file_name=storage_settings.raw_file_name,
        )
        existing = (
            _unchanged_existing_artifact(
                platform_settings.data_root,
                selected_date=selected_date,
                partitions=partitions,
            )
            if partitions
            else None
        )
        if existing is not None:
            cleanup = cleanup_authorized_replay_sources(
                platform_settings.data_root,
                now=now,
                pressure=pressure,
                raw_file_name=storage_settings.raw_file_name,
                grace_hours=platform_settings.replay_raw_delete_grace_hours,
                max_backlog_days=max_backlog_days,
                dry_run=dry_run,
            )
            result = SessionFinalizeResult(
                status=(
                    "dry_run"
                    if dry_run
                    else _status_after_cleanup(
                        cleanup,
                        success="complete",
                        failure="cleanup_failed",
                    )
                ),
                dry_run=dry_run,
                trading_date=selected_date.isoformat(),
                window_start=start.isoformat(),
                window_end=end.isoformat(),
                discovered_partitions=len(partitions),
                compaction_status_counts={},
                preparation_status="artifact_verified_sources_unchanged",
                preparation_errors=(),
                artifact=ArtifactPublishResult(
                    status="already_published",
                    artifact_id=existing.artifact_id,
                    revision=existing.revision,
                    manifest_path=str(
                        Path(existing.review_json.path).parent / "manifest.json"
                    ),
                    source_count=len(existing.sources),
                ),
                cleanup=cleanup,
                human_review=None,
            )
            return _FinalizedCore(result, None, None)
        if not partitions:
            try:
                existing = latest_verified_replay_artifact(
                    platform_settings.data_root,
                    trading_date=selected_date,
                )
            except ReplayArtifactError as exc:
                result = SessionFinalizeResult(
                    status="blocked",
                    dry_run=dry_run,
                    trading_date=selected_date.isoformat(),
                    window_start=start.isoformat(),
                    window_end=end.isoformat(),
                    discovered_partitions=0,
                    compaction_status_counts={},
                    preparation_status="no_sources",
                    preparation_errors=(str(exc),),
                    artifact=None,
                    cleanup=cleanup,
                    human_review=None,
                )
                return _FinalizedCore(result, None, None)
            cleanup = cleanup_authorized_replay_sources(
                platform_settings.data_root,
                now=now,
                pressure=pressure,
                raw_file_name=storage_settings.raw_file_name,
                grace_hours=platform_settings.replay_raw_delete_grace_hours,
                max_backlog_days=max_backlog_days,
                dry_run=dry_run,
            )
            result = SessionFinalizeResult(
                status=(
                    "dry_run"
                    if dry_run
                    else _status_after_cleanup(
                        cleanup,
                        success="complete",
                        failure="cleanup_failed",
                    )
                ),
                dry_run=dry_run,
                trading_date=selected_date.isoformat(),
                window_start=start.isoformat(),
                window_end=end.isoformat(),
                discovered_partitions=0,
                compaction_status_counts={},
                preparation_status="artifact_verified_without_raw",
                preparation_errors=(),
                artifact=ArtifactPublishResult(
                    status="already_published",
                    artifact_id=existing.artifact_id,
                    revision=existing.revision,
                    manifest_path=str(
                        Path(existing.review_json.path).parent / "manifest.json"
                    ),
                    source_count=len(existing.sources),
                ),
                cleanup=cleanup,
                human_review=None,
            )
            return _FinalizedCore(result, None, None)
        compactor = QuoteLakeCompactor(
            platform_settings.data_root,
            raw_file_name=storage_settings.raw_file_name,
            settle_seconds=platform_settings.compaction_min_age_seconds,
            raw_delete_enabled=False,
            raw_delete_grace_hours=platform_settings.replay_raw_delete_grace_hours,
        )
        preparation = prepare_replay_sources(
            partitions,
            compactor=compactor,
            now=now,
            dry_run=dry_run,
        )
        if not preparation.ready:
            result = SessionFinalizeResult(
                status="blocked" if preparation.status != "would_prepare" else "dry_run_pending",
                dry_run=dry_run,
                trading_date=selected_date.isoformat(),
                window_start=start.isoformat(),
                window_end=end.isoformat(),
                discovered_partitions=len(partitions),
                compaction_status_counts=compaction_status_counts(preparation),
                preparation_status=preparation.status,
                preparation_errors=preparation.errors,
                artifact=None,
                cleanup=cleanup,
                human_review=None,
            )
            return _FinalizedCore(result, None, None)

        session = DEFAULT_MARKET_CALENDAR.session(selected_date)
        if session is None:
            raise ValueError(f"{selected_date.isoformat()} is not a trading day")
        deterministic_at = session.review_ready_at.astimezone(timezone.utc)
        # This is the only payload construction in the finalizer. The same
        # object is reused by artifact publication and the human review phase.
        payload = build_review_payload(
            trading_date=selected_date,
            settings=storage_settings,
            now=deterministic_at,
        )
        deterministic_markdown = render_markdown(payload)
        review_json = _deterministic_json(payload)
        review_markdown = _deterministic_markdown_bytes(deterministic_markdown)
        artifact = publish_replay_artifact(
            platform_settings.data_root,
            trading_date=selected_date,
            window_start=start,
            window_end=end,
            sources=preparation.sources,
            review_json=review_json,
            review_markdown=review_markdown,
            generated_at=now,
            dry_run=dry_run,
        )
        cleanup = cleanup_authorized_replay_sources(
            platform_settings.data_root,
            now=now,
            pressure=pressure,
            raw_file_name=storage_settings.raw_file_name,
            grace_hours=platform_settings.replay_raw_delete_grace_hours,
            max_backlog_days=max_backlog_days,
            dry_run=dry_run,
        )

    result = SessionFinalizeResult(
        status=(
            "dry_run"
            if dry_run
            else _status_after_cleanup(
                cleanup,
                success="complete",
                failure="cleanup_failed",
            )
        ),
        dry_run=dry_run,
        trading_date=selected_date.isoformat(),
        window_start=start.isoformat(),
        window_end=end.isoformat(),
        discovered_partitions=len(preparation.partitions),
        compaction_status_counts=compaction_status_counts(preparation),
        preparation_status=preparation.status,
        preparation_errors=preparation.errors,
        artifact=artifact,
        cleanup=cleanup,
        human_review=None,
    )
    return _FinalizedCore(result, payload, deterministic_markdown)


def _unchanged_existing_artifact(
    data_root: str | Path,
    *,
    selected_date: date,
    partitions: Sequence[object],
) -> Any | None:
    """Return the verified artifact when all still-present raw sources match by stat."""

    try:
        existing = latest_verified_replay_artifact(
            data_root,
            trading_date=selected_date,
            verify_source_files=False,
        )
    except ReplayArtifactError:
        return None
    if not partitions:
        return existing
    by_path = {source.source_path: source for source in existing.sources}
    current_paths = {partition.source_relative_path for partition in partitions}
    if current_paths != set(by_path):
        return None
    for partition in partitions:
        evidence = by_path[partition.source_relative_path]
        try:
            stat = partition.source_path.stat()
        except OSError:
            return None
        if stat.st_size != evidence.source_size or stat.st_mtime_ns != evidence.source_mtime_ns:
            return None
    return existing


def _run_human_review(
    *,
    payload: dict[str, Any],
    deterministic_markdown: str,
    trading_date: date,
    storage_settings: StorageSettings,
    no_llm: bool,
    no_push: bool,
) -> dict[str, object]:
    outcome: dict[str, object] = {"status": "complete", "errors": []}
    markdown = deterministic_markdown
    llm_settings = ReviewLlmSettings.from_env()
    if no_llm:
        llm_settings = replace(llm_settings, enabled=False)
    try:
        markdown = maybe_write_llm_review(payload, deterministic_markdown, llm_settings)
        outcome["llm_writer"] = dict(payload.get("llm_writer") or {})
    except Exception as exc:  # The immutable deterministic artifact already exists.
        outcome["status"] = "degraded"
        outcome["errors"].append(f"llm: {type(exc).__name__}: {exc}")

    paths_payload: dict[str, str] | None = None
    try:
        paths = review_paths(
            trading_date=trading_date,
            settings=storage_settings,
            hermes_export_dir=default_hermes_export_dir(),
        )
        paths_payload = write_outputs(payload, markdown, paths)
        outcome["paths"] = paths_payload
    except Exception as exc:
        outcome["status"] = "degraded"
        outcome["errors"].append(f"write: {type(exc).__name__}: {exc}")

    if no_push:
        outcome["push"] = {"skipped": True, "reason": "cli_no_push"}
        return outcome
    latest_path = (
        paths_payload["latest_markdown_path"]
        if paths_payload is not None
        else str(default_latest_markdown_path(storage_settings))
    )
    try:
        push = push_review(
            payload,
            latest_markdown_path=latest_path,
            full_markdown=markdown,
        )
        outcome["push"] = push
        if not push.get("skipped") and not push.get("delivered_ok"):
            outcome["status"] = "degraded"
            outcome["errors"].append("push: no target reported delivered_ok")
    except Exception as exc:
        outcome["status"] = "degraded"
        outcome["errors"].append(f"push: {type(exc).__name__}: {exc}")
    return outcome


def _measure_pressure(settings: DataPlatformSettings) -> StoragePressure:
    return measure_storage_pressure(
        settings.data_root,
        action_free_bytes=settings.storage_pressure_action_free_bytes,
        warning_free_bytes=settings.storage_pressure_warning_free_bytes,
        critical_free_bytes=settings.storage_pressure_critical_free_bytes,
    )


def _status_after_cleanup(
    cleanup: ReplayCleanupSummary,
    *,
    success: str,
    failure: str,
) -> str:
    if cleanup.status == "delete_failed":
        return failure
    if cleanup.status == "blocked" and cleanup.pressure.level in {"warning", "critical"}:
        return "cleanup_blocked"
    return success


def _not_run_cleanup(pressure: StoragePressure, *, dry_run: bool) -> ReplayCleanupSummary:
    return ReplayCleanupSummary(
        status="not_run",
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


def _deterministic_json(payload: dict[str, Any]) -> bytes:
    return (
        json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _deterministic_markdown_bytes(markdown: str) -> bytes:
    return (markdown.rstrip() + "\n").encode("utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Finalize one SPX session into an immutable replay artifact."
    )
    parser.add_argument("--date", default="auto", help="NY trading date, YYYY-MM-DD, or auto")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--pressure-check", action="store_true")
    parser.add_argument("--max-backlog-days", type=int)
    parser.add_argument("--no-llm", action="store_true")
    parser.add_argument("--no-push", action="store_true")
    parser.add_argument("--json", action="store_true", dest="as_json")
    return parser


def run(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    storage_settings = StorageSettings.from_env()
    platform_settings = DataPlatformSettings.from_env()
    max_backlog_days = (
        args.max_backlog_days
        if args.max_backlog_days is not None
        else platform_settings.replay_finalize_backlog_days
    )
    if max_backlog_days <= 0:
        raise ValueError("max-backlog-days must be positive")
    now = datetime.now(tz=timezone.utc)
    try:
        if args.pressure_check:
            result = run_pressure_check(
                now=now,
                storage_settings=storage_settings,
                platform_settings=platform_settings,
                dry_run=args.dry_run,
                max_backlog_days=max_backlog_days,
            )
        else:
            selected = resolve_finalize_date(args.date, now=now)
            result = run_session_finalize(
                selected_date=selected,
                now=now,
                storage_settings=storage_settings,
                platform_settings=platform_settings,
                dry_run=args.dry_run,
                max_backlog_days=max_backlog_days,
                no_llm=args.no_llm,
                no_push=args.no_push,
            )
    except ReplayArtifactError as exc:
        payload = {"status": "failed", "error": f"{type(exc).__name__}: {exc}"}
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 1
    payload = result.to_dict()
    if args.as_json:
        print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        artifact_status = result.artifact.status if result.artifact else "none"
        print(
            "session finalizer "
            f"status={result.status} date={result.trading_date or '-'} "
            f"artifact={artifact_status} cleanup={result.cleanup.status} "
            f"deleted_files={result.cleanup.deleted_files} "
            f"deleted_bytes={result.cleanup.deleted_bytes}"
        )
        for error in result.preparation_errors:
            print(f"blocked: {error}")
    return 1 if result.failed else 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
