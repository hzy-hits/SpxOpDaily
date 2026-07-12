"""Realtime engine contracts and ports."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, Sequence

from spx_spark.domain.analytics import AnalyticsResult
from spx_spark.domain.events import AppendResult, DomainEvent
from spx_spark.domain.health import EngineHealth
from spx_spark.domain.market import MarketSnapshot

__all__ = [
    "AlertEvaluator",
    "AnalyticsKernel",
    "AppendResult",
    "EngineTick",
    "EventOutbox",
    "ProjectionStore",
    "SnapshotSource",
]


@dataclass(frozen=True)
class EngineTick:
    tick_id: str
    started_at: datetime
    source_snapshot_id: str
    analytics: AnalyticsResult | None
    events: tuple[DomainEvent, ...]
    health: EngineHealth
    duration_ms: float

    def to_dict(self) -> dict[str, object]:
        return {
            "tick_id": self.tick_id,
            "started_at": self.started_at.isoformat(),
            "source_snapshot_id": self.source_snapshot_id,
            "analytics_result_id": (
                None if self.analytics is None else self.analytics.result_id
            ),
            "event_count": len(self.events),
            "health": self.health.to_dict(),
            "duration_ms": self.duration_ms,
        }


class SnapshotSource(Protocol):
    def read(self) -> MarketSnapshot: ...


class EventOutbox(Protocol):
    def append(self, events: Sequence[DomainEvent]) -> AppendResult: ...

    def writable(self) -> bool: ...


class ProjectionStore(Protocol):
    def publish(self, tick: EngineTick) -> None: ...


class AnalyticsKernel(Protocol):
    def compute(self, snapshot: MarketSnapshot, *, now: datetime) -> AnalyticsResult: ...


class AlertEvaluator(Protocol):
    def evaluate(
        self,
        snapshot: MarketSnapshot,
        analytics: AnalyticsResult | None,
        *,
        now: datetime,
    ) -> tuple[DomainEvent, ...]: ...
