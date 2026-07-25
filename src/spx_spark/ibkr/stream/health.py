"""Durable data-plane health projection for the persistent IBKR stream."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping

from spx_spark.config import StorageSettings
from spx_spark.state_io import atomic_write_json_secure


HEALTH_SCHEMA_VERSION = 1
HEALTH_FILE_NAME = "ibkr_stream_health.json"
DEFAULT_HEALTH_MAX_AGE_SECONDS = 90.0


def stream_health_path(storage_settings: StorageSettings) -> Path:
    return Path(storage_settings.data_root) / "latest" / HEALTH_FILE_NAME


def persist_stream_health(
    storage_settings: StorageSettings,
    *,
    data_plane_healthy: bool,
    policy_blocked: bool,
    reason: str,
    connected: bool,
    circuit_state: str,
    conflict_count: int,
    retry_in_seconds: float | None = None,
    connection_generation: int | None = None,
    observed_at: datetime | None = None,
    max_age_seconds: float = DEFAULT_HEALTH_MAX_AGE_SECONDS,
) -> None:
    """Publish process and data-plane health as separate, explicit facts."""

    now = observed_at or datetime.now(tz=timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    else:
        now = now.astimezone(timezone.utc)
    retry_seconds = (
        max(float(retry_in_seconds), 0.0) if retry_in_seconds is not None else None
    )
    health_max_age_seconds = max(float(max_age_seconds), 1.0)
    retry_at = (
        (now + timedelta(seconds=retry_seconds)).isoformat()
        if retry_seconds is not None
        else None
    )
    atomic_write_json_secure(
        stream_health_path(storage_settings),
        {
            "schema_version": HEALTH_SCHEMA_VERSION,
            "service": "ibkr_stream",
            "observed_at": now.isoformat(),
            "expires_at": (
                now + timedelta(seconds=health_max_age_seconds)
            ).isoformat(),
            "max_age_seconds": health_max_age_seconds,
            # A live process is not proof that the market-data plane works.
            "process_active": True,
            "data_plane_healthy": bool(data_plane_healthy),
            "policy_blocked": bool(policy_blocked),
            "connected": bool(connected),
            "circuit_state": circuit_state,
            "conflict_count": max(int(conflict_count), 0),
            "retry_at": retry_at,
            "retry_in_seconds": retry_seconds,
            "connection_generation": connection_generation,
            "reason": reason,
        },
    )


def stream_health_is_fresh(
    payload: Mapping[str, object],
    *,
    now: datetime | None = None,
) -> bool:
    """Return whether a health heartbeat is still machine-valid."""

    observed_raw = payload.get("observed_at")
    max_age_raw = payload.get("max_age_seconds")
    if not isinstance(observed_raw, str):
        return False
    try:
        observed_at = datetime.fromisoformat(observed_raw)
        max_age_seconds = float(max_age_raw)
    except (TypeError, ValueError):
        return False
    if observed_at.tzinfo is None or max_age_seconds <= 0:
        return False
    checked_at = now or datetime.now(tz=timezone.utc)
    if checked_at.tzinfo is None:
        checked_at = checked_at.replace(tzinfo=timezone.utc)
    else:
        checked_at = checked_at.astimezone(timezone.utc)
    age_seconds = (
        checked_at - observed_at.astimezone(timezone.utc)
    ).total_seconds()
    return 0.0 <= age_seconds <= max_age_seconds
