"""Crash-safe persistence for the live SPXW session accumulator."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date, datetime
from pathlib import Path
from typing import Any

from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock
from spx_spark.surface_live_session_models import (
    LIVE_SESSION_STATE_SCHEMA_VERSION,
    MAX_LIVE_STATE_BYTES,
    LiveStateError,
    signed_payload,
    verify_artifact,
)


class LiveSessionStateStore:
    """Own immutable manifests/boundaries and one atomic runtime checkpoint."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).expanduser()
        self.owner_path = self.root / "owner"

    def owner_lock(self):  # type annotation would expose private contextmanager type
        return exclusive_state_lock(self.owner_path, timeout_seconds=0.0)

    def session_dir(self, session_date: date | str) -> Path:
        value = session_date.isoformat() if isinstance(session_date, date) else str(session_date)
        try:
            parsed = date.fromisoformat(value)
        except ValueError as exc:
            raise LiveStateError("live_session_date_invalid") from exc
        if parsed.isoformat() != value:
            raise LiveStateError("live_session_date_invalid")
        return self.root / f"session={value}"

    def manifest_path(self, session_date: date | str) -> Path:
        return self.session_dir(session_date) / "manifest.json"

    def runtime_path(self, session_date: date | str) -> Path:
        return self.session_dir(session_date) / "runtime.json"

    def boundaries_dir(self, session_date: date | str) -> Path:
        return self.session_dir(session_date) / "boundaries"

    def boundary_path(self, session_date: date | str, end_at: datetime) -> Path:
        return self.boundaries_dir(session_date) / f"end={end_at.strftime('%H%M%SZ')}.json"

    def ensure_root(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.root.chmod(0o700)

    def _read(self, path: Path, *, expected_kind: str) -> dict[str, Any] | None:
        try:
            stat = path.stat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise LiveStateError(f"live_state_stat_failed:{path.name}") from exc
        if stat.st_size <= 0 or stat.st_size > MAX_LIVE_STATE_BYTES:
            raise LiveStateError(f"live_state_size_invalid:{path.name}")
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise LiveStateError(f"live_state_unreadable:{path.name}") from exc
        if not isinstance(payload, dict):
            raise LiveStateError(f"live_state_contract_invalid:{path.name}")
        if (
            payload.get("schema_version") != LIVE_SESSION_STATE_SCHEMA_VERSION
            or payload.get("kind") != expected_kind
        ):
            raise LiveStateError(f"live_state_identity_invalid:{path.name}")
        try:
            verify_artifact(payload, code="live_state")
        except Exception as exc:
            raise LiveStateError(f"live_state_hash_invalid:{path.name}") from exc
        return payload

    def load_manifest(self, session_date: date | str) -> dict[str, Any] | None:
        return self._read(
            self.manifest_path(session_date),
            expected_kind="spxw_live_session_manifest",
        )

    def load_runtime(self, session_date: date | str) -> dict[str, Any] | None:
        return self._read(
            self.runtime_path(session_date),
            expected_kind="spxw_live_session_runtime",
        )

    def load_boundaries(self, session_date: date | str) -> tuple[dict[str, Any], ...]:
        directory = self.boundaries_dir(session_date)
        if not directory.exists():
            return ()
        rows: list[dict[str, Any]] = []
        for path in sorted(directory.glob("end=*.json")):
            row = self._read(path, expected_kind="spxw_live_session_boundary")
            if row is not None:
                rows.append(row)
        return tuple(rows)

    def write_manifest(self, session_date: date | str, payload: Mapping[str, object]) -> None:
        self._write_immutable(
            self.manifest_path(session_date),
            payload,
            expected_kind="spxw_live_session_manifest",
        )

    def write_boundary(
        self,
        session_date: date | str,
        end_at: datetime,
        payload: Mapping[str, object],
    ) -> None:
        self._write_immutable(
            self.boundary_path(session_date, end_at),
            payload,
            expected_kind="spxw_live_session_boundary",
        )

    def write_runtime(self, session_date: date | str, payload: Mapping[str, object]) -> None:
        path = self.runtime_path(session_date)
        signed = signed_payload(payload)
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        path.parent.chmod(0o700)
        atomic_write_json_secure(path, signed)

    def _write_immutable(
        self,
        path: Path,
        payload: Mapping[str, object],
        *,
        expected_kind: str,
    ) -> None:
        signed = signed_payload(payload)
        if signed.get("kind") != expected_kind:
            raise LiveStateError("live_immutable_kind_invalid")
        existing = self._read(path, expected_kind=expected_kind)
        if existing is not None:
            if existing.get("artifact_sha256") != signed.get("artifact_sha256"):
                raise LiveStateError(f"live_frozen_state_conflict:{path.name}")
            return
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        path.parent.chmod(0o700)
        atomic_write_json_secure(path, signed)


def state_payload(
    *,
    kind: str,
    session_date: str,
    values: Mapping[str, object],
) -> dict[str, object]:
    return {
        "schema_version": LIVE_SESSION_STATE_SCHEMA_VERSION,
        "kind": kind,
        "session_date": session_date,
        **dict(values),
    }


__all__ = ("LiveSessionStateStore", "state_payload")
