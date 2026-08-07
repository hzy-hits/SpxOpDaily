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


@dataclass(frozen=True)
class MarketContextSettings:
    """Typed policy for derived cross-market context and human-focus tags."""

    spx_sector_instrument_ids: tuple[str, ...]
    sector_breadth_min_usable: int
    sector_quote_max_age_seconds: float
    sector_unchanged_band_bps: float
    sector_directional_bias_score: float
    direction_confirmation_move_bps: float
    hyperliquid_proxy_basis_warn_bps: float
    hyperliquid_proxy_basis_block_bps: float
    hyperliquid_proxy_futures_basis_warn_bps: float
    hyperliquid_proxy_futures_basis_block_bps: float
    hyperliquid_es_carry_annual_rate: float
    human_focus_event_tags: tuple[str, ...]
    polymarket_latest_context_path: str

    def __post_init__(self) -> None:
        if not self.spx_sector_instrument_ids:
            raise ValueError("spx_sector_instrument_ids cannot be empty")
        if not 0 < self.sector_breadth_min_usable <= len(self.spx_sector_instrument_ids):
            raise ValueError("sector_breadth_min_usable must fit the configured universe")
        if self.sector_quote_max_age_seconds <= 0.0:
            raise ValueError("sector_quote_max_age_seconds must be positive")
        if self.sector_unchanged_band_bps < 0.0:
            raise ValueError("sector_unchanged_band_bps cannot be negative")
        if not 0.0 <= self.sector_directional_bias_score <= 1.0:
            raise ValueError("sector_directional_bias_score must be between zero and one")
        if self.direction_confirmation_move_bps < 0.0:
            raise ValueError("direction_confirmation_move_bps cannot be negative")
        if not (
            0.0 < self.hyperliquid_proxy_basis_warn_bps
            < self.hyperliquid_proxy_basis_block_bps
        ):
            raise ValueError("cash proxy basis thresholds must be positive and ordered")
        if not (
            0.0 < self.hyperliquid_proxy_futures_basis_warn_bps
            < self.hyperliquid_proxy_futures_basis_block_bps
        ):
            raise ValueError("futures proxy basis thresholds must be positive and ordered")
        if not math.isfinite(self.hyperliquid_es_carry_annual_rate):
            raise ValueError("hyperliquid_es_carry_annual_rate must be finite")
        if not self.polymarket_latest_context_path.strip():
            raise ValueError("polymarket_latest_context_path cannot be empty")
