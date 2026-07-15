"""Analytics settings / policy slice."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AnalyticsSettings:
    """Typed analytics policy injected at composition roots.

    Chain freshness thresholds align with acceptance plan §9.2 RTH initial gates.
    ``passthrough_shadow_mode`` keeps PassthroughAnalytics for differential /
    unit shadow only — production defaults to the real OptionsAnalyticsKernel.
    """

    schema_version: int = 1
    passthrough_shadow_mode: bool = False
    # Front SPXW chain freshness (§9.2 / P1-B)
    max_chain_age_seconds: float = 15.0
    gth_max_chain_age_seconds: float = 90.0
    min_usable_strikes: int = 21
    min_two_sided_ratio: float = 0.80
    min_wing_strikes_each_side: int = 8
    provider_priority: tuple[str, ...] = ("schwab", "ibkr")
    underlier_reference_tolerance_fraction: float = 0.02
