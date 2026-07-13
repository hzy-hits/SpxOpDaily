"""Application runtime (service-loop) package."""

from __future__ import annotations

from spx_spark.application.runtime.health import (
    CRITICAL_FAILURE_EXIT_CODE,
    aggregate_runtime_health,
    build_heartbeat_event,
    critical_tasks_healthy,
)
from spx_spark.application.runtime.tasks import (
    DEFAULT_TASK_CRITICALITY,
    TaskRuntimeState,
    TaskSpec,
)

__all__ = [
    "CRITICAL_FAILURE_EXIT_CODE",
    "DEFAULT_TASK_CRITICALITY",
    "TaskRuntimeState",
    "TaskSpec",
    "aggregate_runtime_health",
    "build_heartbeat_event",
    "critical_tasks_healthy",
]
