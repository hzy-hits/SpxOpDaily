"""Fail-closed persistence for Spring Gamma v3 shadow predictions."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import tempfile
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from spx_spark.application.market_features.spring_gamma_v3 import (
    MODEL_VERSION,
    SCHEMA_VERSION,
    build_spring_gamma_v3_shadow,
)
from spx_spark.application.market_features.state import load_json
from spx_spark.application.market_features.wall_probability import (
    build_wall_probability_tenor_shadow,
)
from spx_spark.config import StorageSettings
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import as_utc
from spx_spark.options_map import group_spxw_option_quotes

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


def process_spring_gamma_v3_shadow(
    *,
    storage: StorageSettings,
    latest_state: object,
    options_map: object,
    market_frame: object,
    option_frame: object,
    greek_reference: dict[str, Any],
    exposure_map: object,
    level_decision: dict[str, object],
    now: datetime,
    settings: object,
) -> dict[str, object]:
    """Evaluate and persist the isolated research shadow without failing hot data."""

    enabled = (
        settings.get("enabled", True)
        if isinstance(settings, Mapping)
        else getattr(settings, "enabled", True)
    )
    if enabled is False:
        return {
            "evaluated": False,
            "status": "disabled",
            "prediction_id": None,
            "reason": "shadow_disabled",
        }

    interval = 900
    session_id = "unknown"
    expected_expiry = DEFAULT_MARKET_CALENDAR.research_expiry(now).strftime("%Y%m%d")
    try:
        configured_interval = getattr(settings, "prediction_interval_seconds", interval)
        if isinstance(configured_interval, bool):
            raise ValueError("prediction_interval_seconds must be a positive integer")
        parsed_interval = int(configured_interval)
        if parsed_interval <= 0:
            raise ValueError("prediction_interval_seconds must be a positive integer")
        interval = parsed_interval
        market_payload = market_frame.to_dict()
        if not isinstance(market_payload, dict):
            raise TypeError("market_frame.to_dict() must return a mapping")
        session_id = str(market_payload.get("session_id") or "unknown")
        latest_shadow = reusable_spring_gamma_v3_shadow(
            load_json(latest_spring_gamma_v3_shadow_path(storage.data_root)),
            now=now,
            session_id=session_id,
            expected_expiry=expected_expiry,
        )
        if not spring_gamma_v3_prediction_due(
            latest_shadow,
            now=now,
            session_id=session_id,
            prediction_interval_seconds=interval,
        ):
            return {
                "evaluated": False,
                "status": str(latest_shadow.get("status") or "unknown"),
                "prediction_id": latest_shadow.get("prediction_id"),
            }

        shadow = build_spring_gamma_v3_shadow(
            market_frame=market_frame,
            option_frame=option_frame,
            greek_reference=greek_reference,
            exposure_map=exposure_map,
            now=now,
            expected_expiry=expected_expiry,
            settings=settings,
            level_decision=level_decision,
        )
        direction = shadow.get("direction")
        direction_decision = (
            str(direction.get("decision") or "abstain")
            if isinstance(direction, dict)
            else "abstain"
        )
        wall_probability = build_wall_probability_tenor_shadow(
            options_map=options_map,
            grouped_quotes=group_spxw_option_quotes(
                latest_state,
                storage_settings=storage,
            ),
            option_frame=option_frame,
            direction=direction_decision,
            now=now,
            horizons=getattr(settings, "horizons_minutes", (15, 30, 60)),
        )
        shadow = validate_spring_gamma_v3_shadow(
            attach_wall_probability_shadow(shadow, wall_probability)
        )
    except Exception as exc:  # A research calculation must never stop production frames.
        shadow = _failed_spring_gamma_v3_shadow(
            now=now,
            session_id=session_id,
            expected_expiry=expected_expiry,
            error=exc,
        )

    try:
        persisted = persist_spring_gamma_v3_shadow(
            shadow,
            data_root=storage.data_root,
            prediction_interval_seconds=interval,
        )
    except Exception as exc:  # Preserve the production hot loop on research I/O failure.
        return {
            "evaluated": True,
            "status": "failed",
            "prediction_id": shadow.get("prediction_id"),
            "error": f"{type(exc).__name__}:{exc}",
        }
    return {
        "evaluated": True,
        "status": shadow.get("status"),
        "prediction_id": shadow.get("prediction_id"),
        **persisted,
    }


def reusable_spring_gamma_v3_shadow(
    payload: dict[str, Any],
    *,
    now: datetime,
    session_id: str,
    expected_expiry: str,
) -> dict[str, Any]:
    """Return only a current-session shadow that may suppress this bucket."""

    try:
        record = validate_spring_gamma_v3_shadow(payload)
        text = str(record["as_of"]).strip()
        as_of = datetime.fromisoformat(
            f"{text[:-1]}+00:00" if text.endswith(("Z", "z")) else text
        )
    except (TypeError, ValueError):
        return {}
    if (
        record.get("session_id") != session_id
        or record.get("expiry") != expected_expiry
        or as_utc(as_of) > as_utc(now)
    ):
        return {}
    return record


def attach_wall_probability_shadow(
    shadow: dict[str, object],
    wall_probability: dict[str, object],
) -> dict[str, object]:
    """Attach expression research without erasing an independent ES direction."""

    combined = dict(shadow)
    combined["wall_probability"] = wall_probability

    direction_fingerprint = str(combined.get("input_fingerprint") or "")
    combined["direction_input_fingerprint"] = direction_fingerprint
    encoded = json.dumps(
        {
            "direction_input_fingerprint": direction_fingerprint,
            "wall_probability": wall_probability,
        },
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode()
    fingerprint = hashlib.sha256(encoded).hexdigest()
    combined["input_fingerprint"] = fingerprint
    combined["prediction_id"] = (
        f"spring-gamma-v3:{combined.get('session_id') or 'unknown'}:"
        f"{combined.get('expiry') or 'unknown'}:{fingerprint[:16]}"
    )
    return combined


def _failed_spring_gamma_v3_shadow(
    *,
    now: datetime,
    session_id: str,
    expected_expiry: str,
    error: Exception,
) -> dict[str, object]:
    error_code = f"{type(error).__name__}:{error}"
    fingerprint = hashlib.sha256(
        f"{now.isoformat()}|{session_id}|{expected_expiry}|{error_code}".encode()
    ).hexdigest()
    return {
        "schema_version": SCHEMA_VERSION,
        "model_version": MODEL_VERSION,
        "prediction_id": f"spring-gamma-v3:{session_id}:{expected_expiry}:{fingerprint[:16]}",
        "input_fingerprint": fingerprint,
        "as_of": now.isoformat(),
        "session_id": session_id,
        "session": "unknown",
        "expiry": expected_expiry,
        "status": "failed",
        "mode": "shadow",
        "direction_authority": "none",
        "action_authority": "none",
        "actionable": False,
        "automatic_ordering": False,
        "calibration_status": "uncalibrated_shadow",
        "direction": {"decision": "abstain"},
        "regime": "abstain",
        "opportunity": "abstain",
        "abstain": True,
        "abstain_reasons": ["shadow_runtime_failure"],
        "error": error_code,
    }


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
