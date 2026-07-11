from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from spx_spark.data_platform.ids import make_compaction_manifest_id


MANIFEST_VERSION = 1


@dataclass(frozen=True)
class CompactionManifest:
    manifest_version: int
    dataset: str
    schema_version: str
    writer_version: str
    status: str
    source_path: str
    source_size: int
    source_mtime_ns: int
    source_sha256: str
    output_path: str | None
    output_size: int | None
    output_sha256: str | None
    row_count: int
    min_received_at: str | None
    max_received_at: str | None
    min_source_at: str | None
    max_source_at: str | None
    completed_at: str

    @property
    def manifest_id(self) -> str:
        return make_compaction_manifest_id(self.source_path, self.source_sha256)

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["manifest_id"] = self.manifest_id
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "CompactionManifest":
        manifest = cls(
            manifest_version=int(payload["manifest_version"]),
            dataset=str(payload["dataset"]),
            schema_version=str(payload["schema_version"]),
            writer_version=str(payload["writer_version"]),
            status=str(payload["status"]),
            source_path=str(payload["source_path"]),
            source_size=int(payload["source_size"]),
            source_mtime_ns=int(payload["source_mtime_ns"]),
            source_sha256=str(payload["source_sha256"]),
            output_path=(str(payload["output_path"]) if payload.get("output_path") else None),
            output_size=(
                int(payload["output_size"]) if payload.get("output_size") is not None else None
            ),
            output_sha256=(str(payload["output_sha256"]) if payload.get("output_sha256") else None),
            row_count=int(payload["row_count"]),
            min_received_at=_optional_str(payload.get("min_received_at")),
            max_received_at=_optional_str(payload.get("max_received_at")),
            min_source_at=_optional_str(payload.get("min_source_at")),
            max_source_at=_optional_str(payload.get("max_source_at")),
            completed_at=str(payload["completed_at"]),
        )
        supplied_id = payload.get("manifest_id")
        if supplied_id is not None and str(supplied_id) != manifest.manifest_id:
            raise ValueError("manifest_id does not match source lineage")
        return manifest


def load_manifest(path: str | Path) -> CompactionManifest | None:
    manifest_path = Path(path)
    if not manifest_path.exists():
        return None
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            return None
        manifest = CompactionManifest.from_dict(payload)
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
        return None
    if manifest.manifest_version != MANIFEST_VERSION:
        return None
    return manifest


def write_manifest(path: str | Path, manifest: CompactionManifest) -> None:
    """Atomically replace a manifest without exposing a partially written JSON file."""

    manifest_path = Path(path)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = manifest_path.with_name(f".{manifest_path.name}.{uuid4().hex}.tmp")
    try:
        with temp_path.open("x", encoding="utf-8") as handle:
            json.dump(manifest.to_dict(), handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temp_path, 0o600)
        os.replace(temp_path, manifest_path)
    finally:
        temp_path.unlink(missing_ok=True)


def _optional_str(value: object) -> str | None:
    return str(value) if value is not None else None
