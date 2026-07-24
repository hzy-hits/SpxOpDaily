"""Fail-closed persistence for Spring Gamma v3 shadow predictions."""

from __future__ import annotations

import fcntl
import json
import os
import tempfile
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA = "spring_gamma_v3_shadow.v1"
STATUSES = frozenset({"ready", "abstain", "failed", "disabled"})


class SpringGammaV3ShadowContractError(ValueError):
    """Raised when a shadow prediction could imply production authority."""


def latest_spring_gamma_v3_shadow_path(data_root: str | Path) -> Path:
    return Path(data_root).expanduser() / "latest" / "spring_gamma_v3_shadow.json"


def validate_spring_gamma_v3_shadow(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Return a validated copy of a Spring Gamma v3 shadow prediction."""

    record = dict(payload)
    if record.get("schema_version") != SCHEMA:
        raise SpringGammaV3ShadowContractError(
            f"schema_version must be {SCHEMA!r}"
        )
    if record.get("status") not in STATUSES:
        raise SpringGammaV3ShadowContractError(
            "status must be ready, abstain, failed, or disabled"
        )

    for field in ("direction_authority", "action_authority"):
        if record.get(field) != "none":
            raise SpringGammaV3ShadowContractError(f"{field} must be 'none'")
    if record.get("actionable") is not False:
        raise SpringGammaV3ShadowContractError("actionable must be false")
    if record.get("automatic_ordering") is not False:
        raise SpringGammaV3ShadowContractError(
            "automatic_ordering must be false"
        )
    _validate_no_authority(record)

    _parse_aware_iso(record.get("as_of"), field="as_of")
    for field in ("session_id", "prediction_id", "input_fingerprint"):
        value = record.get(field)
        if not isinstance(value, str) or not value.strip():
            raise SpringGammaV3ShadowContractError(
                f"{field} must be a non-empty string"
            )

    try:
        json.dumps(
            record,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise SpringGammaV3ShadowContractError(
            "shadow prediction must be finite JSON"
        ) from exc
    return record


def _validate_no_authority(value: object, *, path: str = "shadow") -> None:
    """Reject authority grants anywhere in the persisted shadow tree."""

    if isinstance(value, Mapping):
        for raw_key, child in value.items():
            key = str(raw_key)
            child_path = f"{path}.{key}"
            if (
                key == "authority" or key.endswith("_authority")
            ) and child != "none":
                raise SpringGammaV3ShadowContractError(
                    f"{child_path} must not grant authority"
                )
            if key in {"actionable", "automatic_ordering"} and child is not False:
                raise SpringGammaV3ShadowContractError(
                    f"{child_path} must be false"
                )
            _validate_no_authority(child, path=child_path)
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            _validate_no_authority(child, path=f"{path}[{index}]")


def persist_spring_gamma_v3_shadow(
    payload: Mapping[str, Any],
    *,
    data_root: str | Path,
    prediction_interval_seconds: int,
) -> dict[str, object]:
    """Persist one prediction without granting freshness to stale input.

    The raw log stores at most one record per session and prediction-interval
    bucket.  A newer observation in the same bucket may still replace the
    latest projection, preserving a current view without inflating samples.
    """

    if (
        isinstance(prediction_interval_seconds, bool)
        or not isinstance(prediction_interval_seconds, int)
        or prediction_interval_seconds <= 0
    ):
        raise ValueError("prediction_interval_seconds must be a positive integer")

    record = validate_spring_gamma_v3_shadow(payload)
    incoming_as_of = _parse_aware_iso(record["as_of"], field="as_of")
    bucket_start = _bucket_start(incoming_as_of, prediction_interval_seconds)
    date_label = incoming_as_of.astimezone(timezone.utc).date().isoformat()

    root = Path(data_root).expanduser()
    raw_path = (
        root
        / "features"
        / "spring_gamma_v3"
        / f"date={date_label}"
        / "predictions.jsonl"
    )
    latest_path = latest_spring_gamma_v3_shadow_path(root)
    lock_path = root / "latest" / "spring_gamma_v3_shadow.lock"
    serialized = json.dumps(
        record,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )

    raw_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    appended = False
    latest_updated = False
    with lock_path.open("a+", encoding="utf-8") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            if not _bucket_exists(
                raw_path,
                session_id=str(record["session_id"]),
                bucket_start=bucket_start,
                prediction_interval_seconds=prediction_interval_seconds,
            ):
                _append_line(raw_path, serialized)
                appended = True

            current_as_of = _latest_as_of(latest_path)
            if current_as_of is None or incoming_as_of > current_as_of:
                _atomic_write(latest_path, serialized)
                latest_updated = True
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    return {
        "raw_path": str(raw_path),
        "latest_path": str(latest_path),
        "bucket_start": bucket_start.isoformat(),
        "appended": appended,
        "latest_updated": latest_updated,
    }


def spring_gamma_v3_prediction_due(
    latest: Mapping[str, Any] | None,
    *,
    now: datetime,
    session_id: str,
    prediction_interval_seconds: int,
) -> bool:
    """Return whether a new durable shadow bucket should be evaluated."""

    if prediction_interval_seconds <= 0:
        raise ValueError("prediction_interval_seconds must be positive")
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    if not isinstance(latest, Mapping):
        return True
    if latest.get("schema_version") != SCHEMA or latest.get("session_id") != session_id:
        return True
    try:
        latest_as_of = _parse_aware_iso(latest.get("as_of"), field="as_of")
    except SpringGammaV3ShadowContractError:
        return True
    return _bucket_start(
        now.astimezone(timezone.utc), prediction_interval_seconds
    ) > _bucket_start(latest_as_of, prediction_interval_seconds)


def _parse_aware_iso(value: object, *, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise SpringGammaV3ShadowContractError(
            f"{field} must be a timezone-aware ISO timestamp"
        )
    text = value.strip()
    try:
        parsed = datetime.fromisoformat(
            f"{text[:-1]}+00:00" if text.endswith(("Z", "z")) else text
        )
    except ValueError as exc:
        raise SpringGammaV3ShadowContractError(
            f"{field} must be a timezone-aware ISO timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise SpringGammaV3ShadowContractError(
            f"{field} must be a timezone-aware ISO timestamp"
        )
    return parsed.astimezone(timezone.utc)


def _bucket_start(value: datetime, interval_seconds: int) -> datetime:
    epoch_seconds = int(value.timestamp())
    start_seconds = epoch_seconds - epoch_seconds % interval_seconds
    return datetime.fromtimestamp(start_seconds, tz=timezone.utc)


def _bucket_exists(
    path: Path,
    *,
    session_id: str,
    bucket_start: datetime,
    prediction_interval_seconds: int,
) -> bool:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return False
    except OSError:
        return False
    for line in lines:
        try:
            existing = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(existing, dict):
            continue
        if (
            existing.get("schema_version") != SCHEMA
            or existing.get("session_id") != session_id
        ):
            continue
        try:
            existing_as_of = _parse_aware_iso(existing.get("as_of"), field="as_of")
        except SpringGammaV3ShadowContractError:
            continue
        if _bucket_start(existing_as_of, prediction_interval_seconds) == bucket_start:
            return True
    return False


def _latest_as_of(path: Path) -> datetime | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    try:
        return _parse_aware_iso(payload.get("as_of"), field="as_of")
    except SpringGammaV3ShadowContractError:
        return None


def _append_line(path: Path, serialized: str) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(serialized)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def _atomic_write(path: Path, serialized: str) -> None:
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
