"""Pure EngineHealth evaluation for RealtimeEngine readiness."""

from __future__ import annotations

from datetime import datetime

from spx_spark.domain.health import EngineHealth, EngineMode, HealthFactor


def evaluate_engine_health(
    *,
    tradfi_anchor_usable: bool,
    front_chain_fresh: bool,
    analytics_succeeded: bool,
    outbox_writable: bool,
    critical_tasks_healthy: bool,
    checked_at: datetime,
    engine_failed: bool = False,
    warmed_up: bool = True,
    any_critical_success: bool = True,
) -> EngineHealth:
    """Map observation flags to EngineMode per acceptance plan §7.3.

    READY requires every factor AND warmed_up (all critical tasks have
    succeeded at least once). Before warm-up the mode is STARTING (no
    critical success yet) or WARMING (partial progress / observations),
    never READY.

    BLOCKED when pricing/executable output is impossible after warm-up
    (missing TradFi anchor, analytics failure, outbox down, or critical
    task failure). DEGRADED when research can continue but a non-blocking
    capability (e.g. front chain freshness) is impaired. FAILED is reserved
    for unrecoverable engine faults.
    """

    factors = {
        HealthFactor.TRADFI_ANCHOR.value: tradfi_anchor_usable,
        HealthFactor.FRONT_CHAIN_FRESH.value: front_chain_fresh,
        HealthFactor.ANALYTICS_OK.value: analytics_succeeded,
        HealthFactor.OUTBOX_WRITABLE.value: outbox_writable,
        HealthFactor.CRITICAL_TASKS_OK.value: critical_tasks_healthy,
    }
    reasons: list[str] = [f"{name}_failed" for name, ok in factors.items() if not ok]

    if engine_failed:
        return EngineHealth(
            mode=EngineMode.FAILED,
            factors=factors,
            reasons=tuple(["engine_failed", *reasons]),
            checked_at=checked_at,
        )

    if not warmed_up or not any_critical_success:
        if not any_critical_success:
            reasons = tuple(dict.fromkeys([*reasons, "critical_tasks_not_warmed"]))
            mode = EngineMode.STARTING
        else:
            reasons = tuple(dict.fromkeys([*reasons, "engine_warming"]))
            mode = EngineMode.WARMING
        return EngineHealth(
            mode=mode,
            factors=factors,
            reasons=tuple(reasons) if reasons else (mode.value,),
            checked_at=checked_at,
        )

    if all(factors.values()):
        return EngineHealth(
            mode=EngineMode.READY,
            factors=factors,
            reasons=(),
            checked_at=checked_at,
        )

    blocking = (
        not tradfi_anchor_usable
        or not analytics_succeeded
        or not outbox_writable
        or not critical_tasks_healthy
    )
    mode = EngineMode.BLOCKED if blocking else EngineMode.DEGRADED
    return EngineHealth(
        mode=mode,
        factors=factors,
        reasons=tuple(reasons),
        checked_at=checked_at,
    )
