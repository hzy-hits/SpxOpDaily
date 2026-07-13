"""Runtime supervisor helpers (critical-exit policy)."""

from __future__ import annotations

from spx_spark.application.runtime.health import (
    CRITICAL_FAILURE_EXIT_CODE,
    any_critical_unrecoverable,
)
from spx_spark.application.runtime.tasks import TaskRuntimeState


def should_exit_process(states: list[TaskRuntimeState]) -> int | None:
    """Return a process exit code when the supervisor must stop, else None."""

    if any_critical_unrecoverable(states):
        return CRITICAL_FAILURE_EXIT_CODE
    return None
