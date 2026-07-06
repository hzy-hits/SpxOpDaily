"""Shared alert value objects.

Kept dependency-free (only stdlib) so both alert producers (alert_engine,
ibkr.position_alerts) and consumers can import it without layering cycles.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class Alert:
    severity: str
    kind: str
    instrument_id: str | None
    title: str
    detail: str
    provider: str | None = None
    quality: str | None = None
    value: float | None = None
    threshold: float | None = None
    research_only: bool = False
    source_gate: str | None = None
    dedup_group: str | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def severity_for_priority(priority: str) -> str:
    return {
        "critical": "critical",
        "high": "high",
        "elevated": "medium",
        "normal": "medium",
        "low": "low",
        "off": "info",
    }.get(priority, "medium")
