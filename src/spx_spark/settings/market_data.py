"""Market-data settings slice."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MarketDataSettings:
    known_providers: tuple[str, ...]
    provider_priority: tuple[str, ...]
    latest_stale_after_seconds: float
    delayed_stale_after_seconds: float
