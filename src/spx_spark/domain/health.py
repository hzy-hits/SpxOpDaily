"""Engine and provider health mode enums (stdlib-only)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Mapping


class EngineMode(str, Enum):
    STARTING = "starting"
    WARMING = "warming"
    READY = "ready"
    DEGRADED = "degraded"
    GLOBEX_CONTEXT = "globex_context"
    BLOCKED = "blocked"
    STOPPING = "stopping"
    FAILED = "failed"


class ProviderRuntimeMode(str, Enum):
    STOPPED = "stopped"
    STARTING = "starting"
    LIVE = "live"
    DEGRADED = "degraded"
    BACKOFF = "backoff"
    POLICY_BLOCKED = "policy_blocked"
    CONFLICT_WAIT = "conflict_wait"
    STOPPING = "stopping"
    FAILED = "failed"


class TaskCriticality(str, Enum):
    CRITICAL = "critical"
    IMPORTANT = "important"
    OPTIONAL = "optional"


class TaskMode(str, Enum):
    DISABLED = "disabled"
    IDLE = "idle"
    RUNNING = "running"
    BACKOFF = "backoff"
    UNHEALTHY = "unhealthy"


class HealthFactor(str, Enum):
    """Factors that must all pass for EngineMode.READY."""

    TRADFI_ANCHOR = "tradfi_anchor"
    FRONT_CHAIN_FRESH = "front_chain_fresh"
    ANALYTICS_OK = "analytics_ok"
    OUTBOX_WRITABLE = "outbox_writable"
    CRITICAL_TASKS_OK = "critical_tasks_ok"
    CASH_SESSION_OPEN = "cash_session_open"
    GLOBEX_CONTEXT_USABLE = "globex_context_usable"
    GTH_OPTION_SESSION_OPEN = "gth_option_session_open"


@dataclass(frozen=True)
class EngineHealth:
    mode: EngineMode
    factors: Mapping[str, bool]
    reasons: tuple[str, ...]
    checked_at: datetime

    @property
    def ok(self) -> bool:
        """True when the engine is operational in its declared session mode."""
        return self.mode in {
            EngineMode.READY,
            EngineMode.DEGRADED,
            EngineMode.GLOBEX_CONTEXT,
        }

    @property
    def actionable(self) -> bool:
        """True when the active session has authoritative executable pricing."""
        if self.mode is EngineMode.READY:
            return True
        if self.mode is not EngineMode.GLOBEX_CONTEXT:
            return False
        return all(
            self.factors.get(factor, False)
            for factor in (
                HealthFactor.TRADFI_ANCHOR.value,
                HealthFactor.FRONT_CHAIN_FRESH.value,
                HealthFactor.ANALYTICS_OK.value,
                HealthFactor.OUTBOX_WRITABLE.value,
                HealthFactor.CRITICAL_TASKS_OK.value,
                HealthFactor.GLOBEX_CONTEXT_USABLE.value,
                HealthFactor.GTH_OPTION_SESSION_OPEN.value,
            )
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "mode": self.mode.value,
            "ok": self.ok,
            "actionable": self.actionable,
            "factors": dict(self.factors),
            "reasons": list(self.reasons),
            "checked_at": self.checked_at.isoformat(),
        }
