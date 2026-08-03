"""Market-data settings slice."""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class MarketDataSettings:
    known_providers: tuple[str, ...]
    provider_priority: tuple[str, ...]
    latest_stale_after_seconds: float
    standardized_minute_max_age_seconds: float
    delayed_stale_after_seconds: float

    def __post_init__(self) -> None:
        if (
            not math.isfinite(self.standardized_minute_max_age_seconds)
            or self.standardized_minute_max_age_seconds <= 0.0
        ):
            raise ValueError("standardized_minute_max_age_seconds must be finite and positive")
