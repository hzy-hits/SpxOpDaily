"""Schwab settings slice."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SchwabSettingsSlice:
    streaming_mode: str
    request_budget_warning_per_minute: int
    collection_enabled: bool = True
    collection_interval_seconds: int = 5
