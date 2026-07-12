"""IBKR settings slice (typed view over runtime YAML)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class IbkrSettingsSlice:
    max_option_lines: int
    account_read_enabled: bool
    position_shadow_enabled: bool
    legacy_position_poller_enabled: bool
    execution_mode: str
