"""Fail-closed consumer fence for canonical ES five-minute bars."""

from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import datetime

from spx_spark.application.market_features.es_bar_state import (
    INTERVAL_SECONDS,
    SCHEMA_VERSION as BAR_STATE_SCHEMA_VERSION,
    completed_es_bars,
)
from spx_spark.application.runtime.es_bar_sampler import (
    DEFAULT_MAX_READY_AGE_SECONDS,
    LEASE_SCHEMA_VERSION,
    TASK_NAME,
    canonical_state_path,
    lease_path,
)
from spx_spark.config import StorageSettings
from spx_spark.marketdata import as_utc
from spx_spark.state_io import read_json_object


SCHEMA_VERSION = "es_bar_consumer_readiness.v1"
FUTURE_TOLERANCE_SECONDS = 5.0
_STALE_REASONS = frozenset(
    {
        "lease_stale",
        "last_accept_stale",
        "accepted_source_stale",
        "state_source_stale",
    }
)


def load_consumable_es_bars(
    storage: StorageSettings,
    *,
    now: datetime,
    max_age_seconds: float = DEFAULT_MAX_READY_AGE_SECONDS,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    """Read lease before state and expose bars only when their fence is ready.

    The sampler publishes canonical state before its lease. Reading in this
    order makes a matching last-accepted marker a safe publication fence. A
    concurrent newer state can only cause a transient fail-closed mismatch.
    """

    checked_at = as_utc(now)
    lease = read_json_object(lease_path(storage))
    state = read_json_object(canonical_state_path(storage))
    readiness = evaluate_es_bar_consumer_readiness(
        lease=lease,
        state=state,
        now=checked_at,
        max_age_seconds=max_age_seconds,
    )
    bars = completed_es_bars(state) if readiness["ready"] is True else []
    return bars, readiness


def evaluate_es_bar_consumer_readiness(
    *,
    lease: Mapping[str, object],
    state: Mapping[str, object],
    now: datetime,
    max_age_seconds: float = DEFAULT_MAX_READY_AGE_SECONDS,
) -> dict[str, object]:
    """Validate v2 lease health, freshness, and exact writer/source fencing."""

    if not math.isfinite(max_age_seconds) or max_age_seconds <= 0:
        raise ValueError("consumer max age must be positive and finite")
    checked_at = as_utc(now)
    reasons: list[str] = []

    if lease.get("schema_version") != LEASE_SCHEMA_VERSION:
        reasons.append("lease_schema_invalid")
    if lease.get("task") != TASK_NAME or lease.get("event") != "cycle_finished":
        reasons.append("lease_identity_invalid")
    if lease.get("ok") is not True:
        reasons.append("lease_not_healthy")
    if lease.get("liveness_ok") is not True:
        reasons.append("lease_liveness_failed")
    if lease.get("data_healthy") is not True:
        reasons.append("lease_data_not_healthy")
    if lease.get("sla_ok") is not True:
        reasons.append("lease_cycle_sla_failed")
    if lease.get("writer_has_accepted") is not True:
        reasons.append("writer_has_not_accepted")

    if (
        state.get("schema_version") != BAR_STATE_SCHEMA_VERSION
        or state.get("interval_seconds") != INTERVAL_SECONDS
    ):
        reasons.append("bar_state_schema_invalid")
    lease_writer = _text(lease.get("writer_instance_id"))
    state_writer = _text(state.get("writer_instance_id"))
    state_diagnostics = _mapping(state.get("diagnostics"))
    if lease_writer is None:
        reasons.append("lease_writer_instance_missing")
    if state_writer is None:
        reasons.append("state_writer_instance_missing")
    if lease_writer is not None and state_writer is not None and lease_writer != state_writer:
        reasons.append("writer_instance_mismatch")
    if state_diagnostics.get("canonical_writer") != TASK_NAME:
        reasons.append("canonical_writer_invalid")
    if state_diagnostics.get("writer_instance_id") != state_writer:
        reasons.append("state_writer_fence_invalid")

    finished_at = _timestamp(lease.get("finished_at"))
    last_accepted_at = _timestamp(lease.get("last_accepted_at"))
    accepted_source_at = _timestamp(lease.get("last_accepted_source_at"))
    state_source_at = _timestamp(state.get("last_source_at"))
    lease_age = _age(checked_at, finished_at)
    last_accept_age = _age(checked_at, last_accepted_at)
    accepted_source_age = _age(checked_at, accepted_source_at)
    state_source_age = _age(checked_at, state_source_at)

    if finished_at is None:
        reasons.append("lease_finished_at_missing")
    elif not _fresh(lease_age, max_age_seconds=max_age_seconds):
        reasons.append("lease_stale")
    if last_accepted_at is None:
        reasons.append("last_accepted_at_missing")
    elif not _fresh(
        last_accept_age,
        max_age_seconds=max_age_seconds,
        future_tolerance_seconds=0.0,
    ):
        reasons.append("last_accept_stale")
    if accepted_source_at is None:
        reasons.append("accepted_source_at_missing")
    elif not _fresh(accepted_source_age, max_age_seconds=max_age_seconds):
        reasons.append("accepted_source_stale")
    if state_source_at is None:
        reasons.append("state_source_at_missing")
    elif not _fresh(state_source_age, max_age_seconds=max_age_seconds):
        reasons.append("state_source_stale")
    if accepted_source_at is None or state_source_at != accepted_source_at:
        reasons.append("accepted_source_marker_mismatch")

    unique_reasons = list(dict.fromkeys(reasons))
    ready = not unique_reasons
    status = (
        "ready"
        if ready
        else "stale"
        if any(reason in _STALE_REASONS for reason in unique_reasons)
        else "unavailable"
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "lease_schema_version": lease.get("schema_version"),
        "checked_at": checked_at.isoformat(),
        "ready": ready,
        "status": status,
        "max_age_seconds": max_age_seconds,
        "lease_ok": lease.get("ok") is True,
        "data_healthy": lease.get("data_healthy") is True,
        "sla_ok": lease.get("sla_ok") is True,
        "writer_instance_id": lease_writer,
        "state_writer_instance_id": state_writer,
        "lease_age_seconds": lease_age,
        "last_accepted_age_seconds": last_accept_age,
        "accepted_source_age_seconds": accepted_source_age,
        "state_source_age_seconds": state_source_age,
        "last_accepted_source_at": (
            accepted_source_at.isoformat() if accepted_source_at is not None else None
        ),
        "state_source_at": (state_source_at.isoformat() if state_source_at is not None else None),
        "reasons": unique_reasons,
    }


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _timestamp(value: object) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        return None
    return as_utc(parsed)


def _age(now: datetime, then: datetime | None) -> float | None:
    return (as_utc(now) - then).total_seconds() if then is not None else None


def _fresh(
    age_seconds: float | None,
    *,
    max_age_seconds: float,
    future_tolerance_seconds: float = FUTURE_TOLERANCE_SECONDS,
) -> bool:
    return bool(
        age_seconds is not None and -future_tolerance_seconds <= age_seconds <= max_age_seconds
    )
