from __future__ import annotations

import subprocess
from collections.abc import Callable
from dataclasses import asdict, dataclass


CommandRunner = Callable[[list[str], float], subprocess.CompletedProcess[str]]


@dataclass(frozen=True)
class SinkResult:
    sink: str
    attempted: bool
    ok: bool
    dry_run: bool = False
    exit_code: int | None = None
    error: str | None = None
    alert_keys: tuple[str, ...] = ()
    verdict: str | None = None
    # Deterministic failure (e.g. HTTP 4xx except 429): retrying the identical
    # payload cannot succeed, so the outbox dead-letters on the first attempt.
    permanent: bool = False

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class NotificationResult:
    enabled: bool
    selected_count: int
    sent_count: int
    skipped_reason: str | None
    sinks: tuple[SinkResult, ...]
    acknowledged_event_ids: tuple[str, ...] = ()
    selected_alert_keys: tuple[str, ...] = ()
    outcome: str = "unknown"

    def to_dict(self) -> dict[str, object]:
        return {
            "enabled": self.enabled,
            "selected_count": self.selected_count,
            "sent_count": self.sent_count,
            "skipped_reason": self.skipped_reason,
            "sinks": [sink.to_dict() for sink in self.sinks],
            "acknowledged_event_ids": list(self.acknowledged_event_ids),
            "selected_alert_keys": list(self.selected_alert_keys),
            "outcome": self.outcome,
        }


def default_runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )
