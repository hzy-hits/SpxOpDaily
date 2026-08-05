"""Provider-incident telemetry at the manual entry boundary."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

from spx_spark.provider_failover import new_entry_control_decision
from spx_spark.provider_failover_controller import (
    ProviderFailoverSettings,
    load_failover_control,
)


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


__all__ = ["apply_provider_entry_control", "provider_entry_control"]
