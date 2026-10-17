"""Strategy plug-in protocol.

A strategy may emit candidates and gate results, but never authorize: the Core
still owns the manual authorization boundary. Shadow strategies are recorded for
research/replay only.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class StrategyCandidate:
    strategy_id: str
    version: str
    structure: str
    direction: str
    legs: tuple[Mapping[str, Any], ...] = ()
    evidence: Mapping[str, Any] = field(default_factory=dict)
    invalidation: Mapping[str, Any] | None = None
    expires_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "version": self.version,
            "structure": self.structure,
            "direction": self.direction,
            "legs": [dict(leg) for leg in self.legs],
            "evidence": dict(self.evidence),
            "invalidation": dict(self.invalidation) if self.invalidation else None,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "shadow_only": True,
            "authority": "none",
        }


@dataclass(frozen=True, slots=True)
class GateResult:
    gate: str
    passed: bool
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {"gate": self.gate, "passed": self.passed, "reason": self.reason}


class Strategy(Protocol):
    strategy_id: str
    version: str

    def evaluate(self, facts: Mapping[str, Any]) -> Sequence[StrategyCandidate]: ...

    def gates(
        self, candidate: StrategyCandidate, facts: Mapping[str, Any]
    ) -> Sequence[GateResult]: ...

    def manage(self, candidate: StrategyCandidate) -> Mapping[str, Any]: ...

    def explain(self, candidate: StrategyCandidate) -> Mapping[str, Any]: ...
