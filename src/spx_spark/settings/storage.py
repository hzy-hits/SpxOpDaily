"""Storage settings slice."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StorageSettingsSlice:
    data_root: str
    latest_stale_after_seconds: float
