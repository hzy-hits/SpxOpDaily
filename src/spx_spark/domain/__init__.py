"""Domain contracts for market snapshots, events, and health (Phase 2)."""

from __future__ import annotations

from spx_spark.domain.events import DomainEvent, EventKind
from spx_spark.domain.health import EngineHealth, EngineMode, HealthFactor, TaskCriticality
from spx_spark.domain.market import MarketSnapshot

__all__ = [
    "DomainEvent",
    "EngineHealth",
    "EngineMode",
    "EventKind",
    "HealthFactor",
    "MarketSnapshot",
    "TaskCriticality",
]
