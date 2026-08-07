"""Typed wire contract for immutable session replay artifacts."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


REPLAY_MANIFEST_VERSION = 1
REPLAY_SCHEMA_VERSION = "v1"
REPLAY_WRITER_VERSION = "spx-spark-session-replay-v1"
REPLAY_DATASET = "spx_session_replay"
REPLAY_STATUS = "verified"
REPLAY_ROOT = Path("published") / "session-replay"
REPLAY_MANIFEST_NAME = "manifest.json"
REVIEW_JSON_NAME = "review.json"
REVIEW_MARKDOWN_NAME = "review.md"
FINALIZER_LOCK = ".session-finalize.lock"


class ReplayArtifactError(RuntimeError):
    """Raised when replay lineage cannot be proven safe."""


class ReplayFinalizerBusy(ReplayArtifactError):
    """Raised when another non-dry-run finalizer owns the process lock."""


@dataclass(frozen=True, slots=True)
class ReplaySourceEvidence:
    provider: str
    utc_date: str
    hour: int
    source_path: str
    source_size: int
    source_mtime_ns: int
    source_sha256: str
    source_row_count: int
    compaction_manifest_path: str
    compaction_manifest_id: str
    compaction_completed_at: str
    output_path: str | None
    output_size: int | None
    output_sha256: str | None
    output_row_count: int
    compaction_status: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    def lineage_dict(self) -> dict[str, object]:
        """Return revision-bearing content, excluding wall-clock metadata."""

        return {
            "provider": self.provider,
            "utc_date": self.utc_date,
            "hour": self.hour,
            "source_path": self.source_path,
            "source_size": self.source_size,
            "source_sha256": self.source_sha256,
            "source_row_count": self.source_row_count,
            "compaction_manifest_id": self.compaction_manifest_id,
            "output_path": self.output_path,
            "output_size": self.output_size,
            "output_sha256": self.output_sha256,
            "output_row_count": self.output_row_count,
            "compaction_status": self.compaction_status,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ReplaySourceEvidence":
        return cls(
            provider=str(payload["provider"]),
            utc_date=str(payload["utc_date"]),
            hour=int(payload["hour"]),
            source_path=str(payload["source_path"]),
            source_size=int(payload["source_size"]),
            source_mtime_ns=int(payload["source_mtime_ns"]),
            source_sha256=str(payload["source_sha256"]),
            source_row_count=int(payload["source_row_count"]),
            compaction_manifest_path=str(payload["compaction_manifest_path"]),
            compaction_manifest_id=str(payload["compaction_manifest_id"]),
            compaction_completed_at=str(payload["compaction_completed_at"]),
            output_path=(str(payload["output_path"]) if payload.get("output_path") else None),
            output_size=(
                int(payload["output_size"]) if payload.get("output_size") is not None else None
            ),
            output_sha256=(
                str(payload["output_sha256"]) if payload.get("output_sha256") else None
            ),
            output_row_count=int(payload["output_row_count"]),
            compaction_status=str(payload["compaction_status"]),
        )


@dataclass(frozen=True, slots=True)
class ReplayFileEvidence:
    path: str
    size: int
    sha256: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ReplayFileEvidence":
        return cls(
            path=str(payload["path"]),
            size=int(payload["size"]),
            sha256=str(payload["sha256"]),
        )


@dataclass(frozen=True, slots=True)
class ReplayArtifactManifest:
    manifest_version: int
    dataset: str
    schema_version: str
    writer_version: str
    status: str
    artifact_id: str
    trading_date: str
    window_start: str
    window_end: str
    source_digest: str
    revision: str
    generated_at: str
    sources: tuple[ReplaySourceEvidence, ...]
    review_json: ReplayFileEvidence
    review_markdown: ReplayFileEvidence

    def to_dict(self) -> dict[str, object]:
        return {
            "manifest_version": self.manifest_version,
            "dataset": self.dataset,
            "schema_version": self.schema_version,
            "writer_version": self.writer_version,
            "status": self.status,
            "artifact_id": self.artifact_id,
            "trading_date": self.trading_date,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "source_digest": self.source_digest,
            "revision": self.revision,
            "generated_at": self.generated_at,
            "sources": [source.to_dict() for source in self.sources],
            "review_json": self.review_json.to_dict(),
            "review_markdown": self.review_markdown.to_dict(),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ReplayArtifactManifest":
        raw_sources = payload.get("sources")
        if not isinstance(raw_sources, list):
            raise ValueError("replay sources must be a list")
        review_json = payload.get("review_json")
        review_markdown = payload.get("review_markdown")
        if not isinstance(review_json, dict) or not isinstance(review_markdown, dict):
            raise ValueError("replay review file evidence is missing")
        return cls(
            manifest_version=int(payload["manifest_version"]),
            dataset=str(payload["dataset"]),
            schema_version=str(payload["schema_version"]),
            writer_version=str(payload["writer_version"]),
            status=str(payload["status"]),
            artifact_id=str(payload["artifact_id"]),
            trading_date=str(payload["trading_date"]),
            window_start=str(payload["window_start"]),
            window_end=str(payload["window_end"]),
            source_digest=str(payload["source_digest"]),
            revision=str(payload["revision"]),
            generated_at=str(payload["generated_at"]),
            sources=tuple(ReplaySourceEvidence.from_dict(item) for item in raw_sources),
            review_json=ReplayFileEvidence.from_dict(review_json),
            review_markdown=ReplayFileEvidence.from_dict(review_markdown),
        )


@dataclass(frozen=True, slots=True)
class ArtifactPublishResult:
    status: str
    artifact_id: str
    revision: str
    manifest_path: str
    source_count: int

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class StoragePressure:
    total_bytes: int
    used_bytes: int
    free_bytes: int
    level: str
    action_required: bool

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ReplayDeleteResult:
    trading_date: str
    source_path: str
    status: str
    source_size: int
    detail: str | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ReplayCleanupSummary:
    status: str
    dry_run: bool
    pressure: StoragePressure
    artifact_dates_scanned: tuple[str, ...]
    authorized_dates: tuple[str, ...]
    blocked_dates: tuple[tuple[str, str], ...]
    results: tuple[ReplayDeleteResult, ...]
    deleted_files: int
    deleted_bytes: int
    would_delete_files: int
    would_delete_bytes: int

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "dry_run": self.dry_run,
            "pressure": self.pressure.to_dict(),
            "artifact_dates_scanned": list(self.artifact_dates_scanned),
            "authorized_dates": list(self.authorized_dates),
            "blocked_dates": [
                {"trading_date": trading_date, "reason": reason}
                for trading_date, reason in self.blocked_dates
            ],
            "results": [result.to_dict() for result in self.results],
            "deleted_files": self.deleted_files,
            "deleted_bytes": self.deleted_bytes,
            "would_delete_files": self.would_delete_files,
            "would_delete_bytes": self.would_delete_bytes,
        }
