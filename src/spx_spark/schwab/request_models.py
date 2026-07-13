"""Typed Schwab request planning and telemetry contracts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class SchwabLane(str, Enum):
    HOT_AND_CONTEXT_QUOTES = "hot_and_context_quotes"
    FRONT_CHAIN = "front_chain"
    NEXT_CHAIN = "next_chain"
    CONFIRMATION_CHAIN = "confirmation_chain"
    RECOVERY_PROBE = "recovery_probe"


class CollectionProfile(str, Enum):
    OFF_HOURS = "off_hours"
    GTH = "gth"
    NORMAL = "normal"
    ACTIVE = "active"
    BURST = "burst"


class QuotaMode(str, Enum):
    NORMAL = "normal"
    PRESSURE = "pressure"
    THROTTLED = "throttled"
    COOLDOWN = "cooldown"
    RECOVERING = "recovering"


@dataclass(frozen=True)
class SchwabRequestSpec:
    request_id: str
    lane: SchwabLane
    path: str
    params: tuple[tuple[str, str], ...]
    symbol_count: int
    priority: int
    due_at: datetime
    deadline_at: datetime


@dataclass(frozen=True)
class SchwabRequestObservation:
    path: str
    attempted_at_epoch: float
    completed_at_epoch: float
    retry_index: int
    status_code: int | None
    response_bytes: int
    latency_ms: float
    retry_after_seconds: float | None
    outcome: str


@dataclass(frozen=True)
class RequestWindow:
    attempts: int = 0
    retries: int = 0
    throttled: int = 0
    failures: int = 0
    response_bytes: int = 0
