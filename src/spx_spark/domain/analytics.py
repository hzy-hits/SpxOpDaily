"""Analytics domain value objects (stdlib-only)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Mapping


class AnalyticsStatus(str, Enum):
    """Front-month / overall compute outcome for readiness gates.

    ``analytics_ok`` requires ``SUCCESS`` explicitly — a non-throwing compute
    that returns empty or degraded front-month output must not count as success.
    """

    SUCCESS = "success"
    DEGRADED = "degraded"
    FAILED = "failed"


@dataclass(frozen=True)
class AnalyticsDiagnostics:
    input_legs: int
    usable_legs: int
    duration_ms: float
    warnings: tuple[str, ...]
    model_versions: Mapping[str, str]


@dataclass(frozen=True)
class AnalyticsResult:
    schema_version: int
    result_id: str
    input_snapshot_id: str
    computed_at: datetime
    underlier: Any
    expiries: tuple[Any, ...]
    diagnostics: AnalyticsDiagnostics
    status: AnalyticsStatus = AnalyticsStatus.FAILED

    @property
    def analytics_ok(self) -> bool:
        return self.status is AnalyticsStatus.SUCCESS
