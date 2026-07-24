"""Typed settings for the Spring Gamma v3 research shadow."""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, Literal


@dataclass(frozen=True)
class SpringGammaV3Settings:
    """Independent, fail-closed policy for shadow direction predictions.

    ``authority`` is deliberately a class variable instead of a runtime
    setting.  This slice can enable shadow computation and report rendering,
    but it cannot acquire production decision authority through configuration.
    """

    authority: ClassVar[Literal["shadow"]] = "shadow"

    enabled: bool = True
    report_enabled: bool = True
    prediction_interval_seconds: int = 60
    horizons_minutes: tuple[int, ...] = (15, 30, 60)
    rth_greek_max_age_seconds: float = 15.0
    rth_iv_max_age_seconds: float = 15.0
    gth_greek_max_age_seconds: float = 90.0
    gth_iv_max_age_seconds: float = 90.0
    min_pair_ratio: float = 0.80
    min_iv: float = 0.60
    min_delta: float = 0.60
    min_oi: float = 0.60
    min_paired_strikes: int = 3
    min_probability: float = 0.60
    min_margin: float = 0.10

    def __post_init__(self) -> None:
        if not isinstance(self.enabled, bool) or not isinstance(self.report_enabled, bool):
            raise ValueError("Spring Gamma v3 enable flags must be booleans")
        if self.prediction_interval_seconds <= 0:
            raise ValueError("Spring Gamma v3 prediction interval must be positive")
        if (
            not self.horizons_minutes
            or tuple(sorted(set(self.horizons_minutes))) != self.horizons_minutes
            or any(value <= 0 for value in self.horizons_minutes)
        ):
            raise ValueError(
                "Spring Gamma v3 horizons must be unique, positive, and ascending"
            )
        freshness_values = (
            self.rth_greek_max_age_seconds,
            self.rth_iv_max_age_seconds,
            self.gth_greek_max_age_seconds,
            self.gth_iv_max_age_seconds,
        )
        if any(value <= 0 for value in freshness_values):
            raise ValueError("Spring Gamma v3 freshness limits must be positive")
        coverage_values = (
            self.min_pair_ratio,
            self.min_iv,
            self.min_delta,
            self.min_oi,
        )
        if any(not 0 < value <= 1 for value in coverage_values):
            raise ValueError("Spring Gamma v3 coverage gates must be in (0, 1]")
        if self.min_paired_strikes <= 0:
            raise ValueError("Spring Gamma v3 paired-strike minimum must be positive")
        if not 0.5 < self.min_probability <= 1:
            raise ValueError(
                "Spring Gamma v3 minimum probability must be in (0.5, 1]"
            )
        if not 0 < self.min_margin <= 1:
            raise ValueError("Spring Gamma v3 minimum margin must be in (0, 1]")
