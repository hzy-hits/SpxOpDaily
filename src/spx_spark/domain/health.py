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


@dataclass(frozen=True)
class EngineHealth:
    mode: EngineMode
    factors: Mapping[str, bool]
    reasons: tuple[str, ...]
    checked_at: datetime

    @property
    def ok(self) -> bool:
        """True only when the engine can produce pricing/executable output."""
        return self.mode is EngineMode.READY

    def to_dict(self) -> dict[str, object]:
        return {
            "mode": self.mode.value,
            "ok": self.ok,
            "factors": dict(self.factors),
            "reasons": list(self.reasons),
            "checked_at": self.checked_at.isoformat(),
        }
