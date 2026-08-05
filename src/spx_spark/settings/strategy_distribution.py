"""Typed settings for the causal strategy-distribution research lane."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class StrategyDistributionSettings:
    """Runtime policy for an advisory-only P-vs-Q forecast artifact."""

    enabled: bool = True
    horizon_seconds: int = 300
    window_days: int = 35
    refresh_seconds: float = 60.0
    projection_ttl_seconds: float = 90.0
    append_interval_seconds: float = 60.0
    minimum_physical_samples: int = 30
    beta_prior_alpha: float = 1.0
    beta_prior_beta: float = 1.0

    def __post_init__(self) -> None:
        if isinstance(self.horizon_seconds, bool) or self.horizon_seconds <= 0:
            raise ValueError("strategy distribution horizon_seconds must be positive")
        if isinstance(self.window_days, bool) or self.window_days <= 0:
            raise ValueError("strategy distribution window_days must be positive")
        for value, name in (
            (self.refresh_seconds, "refresh_seconds"),
            (self.projection_ttl_seconds, "projection_ttl_seconds"),
            (self.append_interval_seconds, "append_interval_seconds"),
            (self.beta_prior_alpha, "beta_prior_alpha"),
            (self.beta_prior_beta, "beta_prior_beta"),
        ):
            if isinstance(value, bool) or value <= 0:
                raise ValueError(f"strategy distribution {name} must be positive")
        if self.refresh_seconds >= self.projection_ttl_seconds:
            raise ValueError(
                "strategy distribution refresh_seconds must be shorter than projection TTL"
            )
        if isinstance(self.minimum_physical_samples, bool) or self.minimum_physical_samples <= 0:
            raise ValueError("strategy distribution minimum_physical_samples must be positive")

    @property
    def action_authority(self) -> str:
        return "none"

    @property
    def automatic_ordering(self) -> bool:
        return False
