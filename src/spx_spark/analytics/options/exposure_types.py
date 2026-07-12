"""GEX strike and wall ladder value objects."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class StrikeGex:
    strike: float
    call_gex: float
    put_gex: float
    net_gex: float
    abs_gex: float
    call_open_interest: float
    put_open_interest: float
    call_volume: float = 0.0
    put_volume: float = 0.0


@dataclass(frozen=True)
class WallLevel:
    """One rung of the wall ladder: a strike with concentrated dealer gamma."""

    strike: float
    side: str  # "call" | "put"
    gex: float
    open_interest: float
    volume: float
    distance_points: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
