"""Service-loop task registry models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Callable

from spx_spark.domain.health import TaskCriticality, TaskMode

TaskFn = Callable[[], int]


@dataclass(frozen=True)
class TaskSpec:
    name: str
    command: tuple[str, ...]
    interval_seconds: float
    timeout_seconds: float
    criticality: TaskCriticality
    max_consecutive_failures: int


@dataclass
class TaskRuntimeState:
    name: str
    criticality: TaskCriticality = TaskCriticality.IMPORTANT
    mode: TaskMode = TaskMode.IDLE
    max_consecutive_failures: int = 5
    consecutive_failures: int = 0
    last_success_at: datetime | None = None
    last_finished_at: datetime | None = None
    last_error: str | None = None
    in_flight_since_monotonic: float | None = None
    last_engine_health: dict[str, object] | None = None

    def record_success(self, *, finished_at: datetime) -> None:
        self.consecutive_failures = 0
        self.last_success_at = finished_at
        self.last_finished_at = finished_at
        self.last_error = None
        self.mode = TaskMode.IDLE
        self.in_flight_since_monotonic = None

    def record_failure(self, *, finished_at: datetime, error: str | None = None) -> None:
        self.consecutive_failures += 1
        self.last_finished_at = finished_at
        self.last_error = error
        self.in_flight_since_monotonic = None
        if self.consecutive_failures >= self.max_consecutive_failures:
            self.mode = TaskMode.UNHEALTHY
        else:
            self.mode = TaskMode.BACKOFF

    def mark_running(self, *, now_monotonic: float) -> None:
        self.mode = TaskMode.RUNNING
        self.in_flight_since_monotonic = now_monotonic

    @property
    def healthy(self) -> bool:
        return self.mode is not TaskMode.UNHEALTHY

    def to_dict(self, *, now_monotonic: float | None = None) -> dict[str, object]:
        in_flight_age = None
        if (
            self.in_flight_since_monotonic is not None
            and now_monotonic is not None
        ):
            in_flight_age = max(now_monotonic - self.in_flight_since_monotonic, 0.0)
        return {
            "name": self.name,
            "criticality": self.criticality.value,
            "mode": self.mode.value,
            "consecutive_failures": self.consecutive_failures,
            "max_consecutive_failures": self.max_consecutive_failures,
            "last_success_at": (
                None if self.last_success_at is None else self.last_success_at.isoformat()
            ),
            "last_finished_at": (
                None if self.last_finished_at is None else self.last_finished_at.isoformat()
            ),
            "last_error": self.last_error,
            "in_flight_age_seconds": in_flight_age,
            "healthy": self.healthy,
            "last_engine_health": self.last_engine_health,
        }


# Default criticality by known service task name.
DEFAULT_TASK_CRITICALITY: dict[str, TaskCriticality] = {
    "provider_failover": TaskCriticality.IMPORTANT,
    "intraday_shock": TaskCriticality.IMPORTANT,
    "hyperliquid": TaskCriticality.OPTIONAL,
    "polymarket": TaskCriticality.OPTIONAL,
    "ibkr": TaskCriticality.CRITICAL,
    "schwab_chains": TaskCriticality.CRITICAL,
    "iv_surface": TaskCriticality.OPTIONAL,
    "alert_engine": TaskCriticality.IMPORTANT,
    "realtime_engine": TaskCriticality.IMPORTANT,
    "greek_shadow": TaskCriticality.OPTIONAL,
    "steven": TaskCriticality.OPTIONAL,
    "ibkr_positions": TaskCriticality.OPTIONAL,
}
