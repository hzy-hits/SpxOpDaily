"""Provider-incident telemetry at the manual entry boundary."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from pathlib import Path

from spx_spark.ibkr.stream.health import stream_health_is_fresh
from spx_spark.provider_failover import new_entry_control_decision
from spx_spark.provider_failover_controller import (
    ProviderFailoverSettings,
    load_failover_control,
)
from spx_spark.state_io import read_json_object


def provider_entry_control(
    settings: ProviderFailoverSettings,
    *,
    now: datetime,
) -> dict[str, object]:
    if settings.enabled:
        return new_entry_control_decision(
            load_failover_control(settings.state_path),
            now=now,
            max_age_seconds=settings.control_state_max_age_seconds,
        )
    return {
        "allowed": True,
        "reason": "provider_failover_disabled",
        "mode": None,
        "updated_at": None,
        "age_seconds": None,
        "max_age_seconds": settings.control_state_max_age_seconds,
    }


def apply_provider_entry_control(
    trade_intent: Mapping[str, object],
    control: Mapping[str, object],
) -> dict[str, object]:
    """Attach incident telemetry without vetoing a valid manual card."""

    result = {**trade_intent, "provider_failover_control": dict(control)}
    if control.get("allowed") is not True and result.get("status") == "trade_ready":
        result["provider_incident_warning"] = str(control.get("reason") or "provider_incident")
    return result


def gth_ibkr_entry_control(
    data_root: str | Path,
    *,
    now: datetime,
) -> dict[str, object]:
    """Fail closed on the IBKR data-plane health required by GTH SPXW.

    The general provider controller is intentionally Schwab-first and can remain
    healthy while the independent IBKR SPXW lane is in a 10197 circuit.  A GTH
    option candidate must therefore consume the stream owner's immediate health
    projection in addition to the general underlier/provider decision.
    """

    payload = read_json_object(Path(data_root) / "latest" / "ibkr_stream_health.json")
    reason = str(payload.get("reason") or "")
    circuit_state = str(payload.get("circuit_state") or "unknown")
    conflict_count = payload.get("conflict_count")
    if not payload:
        block_reason = "ibkr_stream_health_missing"
    elif not stream_health_is_fresh(payload, now=now):
        block_reason = "ibkr_stream_health_stale"
    elif (
        payload.get("policy_blocked") is True
        or circuit_state != "closed"
        or not isinstance(conflict_count, int)
        or isinstance(conflict_count, bool)
        or conflict_count != 0
        or "10197" in reason
        or "competing" in reason.lower()
    ):
        block_reason = "ibkr_competing_session"
    elif payload.get("connected") is not True:
        block_reason = "ibkr_stream_disconnected"
    elif payload.get("data_plane_healthy") is not True:
        block_reason = "ibkr_data_plane_unhealthy"
    else:
        block_reason = "allowed"
    return {
        "allowed": block_reason == "allowed",
        "reason": block_reason,
        "service": payload.get("service"),
        "observed_at": payload.get("observed_at"),
        "connected": payload.get("connected"),
        "data_plane_healthy": payload.get("data_plane_healthy"),
        "policy_blocked": payload.get("policy_blocked"),
        "circuit_state": payload.get("circuit_state"),
        "conflict_count": conflict_count,
        "source_reason": payload.get("reason"),
    }


__all__ = [
    "apply_provider_entry_control",
    "gth_ibkr_entry_control",
    "provider_entry_control",
]
