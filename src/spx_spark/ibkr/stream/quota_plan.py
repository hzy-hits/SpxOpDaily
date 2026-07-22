"""Pure IBKR option-line allocation for Schwab validation and fallback."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class IbkrQuotaMode(str, Enum):
    VALIDATION = "validation"
    FALLBACK = "fallback"


@dataclass(frozen=True)
class IbkrOptionAllocation:
    mode: IbkrQuotaMode
    discovered_capacity: int
    base_lines: int
    temporary_lines: int
    hot_option_lines: int
    rotation_option_lines: int
    reserve_lines: int

    @property
    def option_lines(self) -> int:
        return self.hot_option_lines + self.rotation_option_lines

    @property
    def hot_lane_share(self) -> float:
        return self.hot_option_lines / self.option_lines if self.option_lines else 0.0


def plan_ibkr_option_allocation(
    *,
    discovered_capacity: int = 100,
    fallback: bool,
    base_lines: int = 4,
    temporary_lines: int = 6,
) -> IbkrOptionAllocation:
    if discovered_capacity <= 0:
        raise ValueError("IBKR discovered line capacity must be positive")
    mode = IbkrQuotaMode.FALLBACK if fallback else IbkrQuotaMode.VALIDATION
    target_hot, target_rotation, minimum_reserve = (46, 38, 6) if fallback else (44, 20, 20)
    usable = max(discovered_capacity - base_lines - temporary_lines - minimum_reserve, 0)
    hot = min(target_hot, usable)
    hot -= hot % 2
    rotation = min(target_rotation, max(usable - hot, 0))
    rotation -= rotation % 2
    reserve = discovered_capacity - base_lines - temporary_lines - hot - rotation
    return IbkrOptionAllocation(
        mode=mode,
        discovered_capacity=discovered_capacity,
        base_lines=base_lines,
        temporary_lines=temporary_lines,
        hot_option_lines=hot,
        rotation_option_lines=rotation,
        reserve_lines=reserve,
    )
