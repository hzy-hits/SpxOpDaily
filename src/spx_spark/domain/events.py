"""Domain event contracts (stdlib-only)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Mapping


class EventKind(str, Enum):
    PROVIDER_TRANSITION = "provider_transition"
    DATA_QUALITY_TRANSITION = "data_quality_transition"
    PRICE_SHOCK = "price_shock"
    OPTION_STRUCTURE_TRANSITION = "option_structure_transition"
    POSITION_TRANSITION = "position_transition"
    ALERT_CANDIDATE = "alert_candidate"
    DELIVERY_RESULT = "delivery_result"


@dataclass(frozen=True)
class AppendResult:
    """Result of appending domain events to an outbox (stdlib value object)."""

    accepted: int
    duplicate: int = 0
    writable: bool = True


@dataclass(frozen=True)
class DomainEvent:
    schema_version: int
    event_id: str
    kind: EventKind
    source_at: datetime
    available_at: datetime
    aggregate_id: str
    sequence: int
    payload: Mapping[str, Any]

    def validate(self) -> None:
        if self.schema_version < 1:
            raise ValueError("schema_version must be >= 1")
        if not self.event_id.strip():
            raise ValueError("event_id is required")
        if not self.aggregate_id.strip():
            raise ValueError("aggregate_id is required")
        if self.sequence < 0:
            raise ValueError("sequence must be >= 0")
        if self.source_at.tzinfo is None or self.available_at.tzinfo is None:
            raise ValueError("event timestamps must be timezone-aware")
