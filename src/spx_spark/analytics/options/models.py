"""Options analytics value objects."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum
from typing import Any

from spx_spark.analytics.options.exposure_types import StrikeGex, WallLevel


class DensityQuality(str, Enum):
    """Risk-neutral density publishability (§9.1).

    Wire values keep the historical labels (``ok``, ``noisy_quotes``, …) so
    golden payloads and notifier prompts stay stable. Plan vocabulary names
    (READY / DEGRADED_NOISY / BLOCKED_*) are Enum aliases of those values until
    RTH §9.2 calibration promotes the new strings in fixtures.
    """

    OK = "ok"
    READY = "ok"  # §9.1 alias
    NOISY_QUOTES = "noisy_quotes"
    DEGRADED_NOISY = "noisy_quotes"  # §9.1 alias
    NARROW_RANGE = "narrow_range"
    INSUFFICIENT_STRIKES = "insufficient_strikes"
    BLOCKED_COVERAGE = "insufficient_strikes"  # §9.1 alias until taxonomy expands
    BLOCKED_STALE = "blocked_stale"  # reserved; unused until age gates land
    BLOCKED_ARBITRAGE = "blocked_arbitrage"  # reserved


@dataclass(frozen=True)
class DensityDiagnostics:
    """Partial §9.1 diagnostics; remaining fields await RTH §9.2 calibration."""

    usable_strikes: int = 0
    clipped_mass_fraction: float | None = None
    lower_width_points: float | None = None
    upper_width_points: float | None = None
    two_sided_ratio: float | None = None
    max_gap_multiple: float | None = None
    monotonic_violation_fraction: float | None = None
    negative_mass_fraction: float | None = None
    normalized_mass: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class UnderlierReference:
    price: float | None
    source: str | None


@dataclass(frozen=True)
class OptionCoverage:
    total: int
    live: int
    stale: int
    delayed: int
    unknown_age: int
    max_age_ms: float | None
    with_bid_ask: int
    with_mid: int
    with_iv: int
    with_delta: int
    with_gamma: int
    with_theta: int
    with_vega: int
    with_open_interest: int
    avg_spread_bps: float | None


@dataclass(frozen=True)
class LevelProbability:
    level_name: str
    level: float
    prob_close_beyond: float | None
    prob_touch: float | None
    source_strike: float | None
    source_delta: float | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RnDensity:
    """Breeden-Litzenberger risk-neutral close distribution for one expiry.

    f(K) = d²C/dK² (r≈0 for 0DTE/1DTE). The call curve is synthesized from
    OTM options (puts below spot via parity, calls above) because OTM quotes
    are tighter than deep ITM ones. Percentiles are conditional on the
    observed strike range; mass outside the sampled window is not priced.
    """

    quality: DensityQuality
    median: float | None = None
    p10: float | None = None
    p25: float | None = None
    p75: float | None = None
    p90: float | None = None
    prob_below_put_wall: float | None = None
    prob_above_call_wall: float | None = None
    clipped_mass_fraction: float | None = None
    strike_range: tuple[float, float] | None = None
    diagnostics: DensityDiagnostics | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["quality"] = self.quality.value
        return payload


@dataclass(frozen=True)
class WallConfluence:
    spy_underlier: float | None
    spy_front_expiry: str | None
    spy_call_wall_spx: float | None
    spy_put_wall_spx: float | None
    call_wall_confluent: bool | None
    put_wall_confluent: bool | None
    tolerance_points: float
    spy_option_count: int
    quality: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class MaxPain:
    """OI-derived settlement pain and the strongest call/put concentrations."""

    settlement_strike: float
    payout_points: float
    call_oi_peak_strike: float
    call_oi_peak: float
    put_oi_peak_strike: float
    put_oi_peak: float
    call_open_interest: float
    put_open_interest: float
    oi_strike_count: int
    strike_range: tuple[float, float]
    quality: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExpiryOptionsMap:
    expiry: str
    option_count: int
    strike_count: int
    atm_strike: float | None
    atm_call_mid: float | None
    atm_put_mid: float | None
    atm_straddle_mid: float | None
    expected_move_points: float | None
    expected_move_pct: float | None
    atm_iv: float | None
    put_wing_iv: float | None
    call_wing_iv: float | None
    put_skew_ratio: float | None
    call_skew_ratio: float | None
    net_gex: float | None
    abs_gex: float | None
    net_gamma_ratio: float | None
    zero_gamma: float | None
    zero_gamma_distance_points: float | None
    call_wall: float | None
    put_wall: float | None
    nearest_wall: float | None
    nearest_wall_distance_points: float | None
    gamma_state: str
    gex_quality: str
    coverage: OptionCoverage
    top_gex_strikes: tuple[StrikeGex, ...]
    warnings: tuple[str, ...]
    level_probabilities: tuple[LevelProbability, ...] = ()
    gamma_flip_zone: tuple[float, float] | None = None
    gex_weighting: str = "oi"
    zero_gamma_method: str = "strike_profile_fallback_no_flip"
    put_skew_25d: float | None = None
    call_skew_25d: float | None = None
    skew_method: str = "moneyness_fallback"
    # Wall ladder: top-4 call walls at/above spot and put walls at/below spot,
    # from OI-weighted GEX (positioning), not intraday volume.
    call_walls: tuple[WallLevel, ...] = ()
    put_walls: tuple[WallLevel, ...] = ()
    wall_method: str = "oi_gex"
    rn_density: RnDensity | None = None
    max_pain: MaxPain | None = None


@dataclass(frozen=True)
class OptionsMap:
    created_at: datetime
    as_of: datetime
    underlier: UnderlierReference
    expiries: tuple[ExpiryOptionsMap, ...]
    warnings: tuple[str, ...]
    spy_confluence: WallConfluence | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["created_at"] = self.created_at.isoformat()
        payload["as_of"] = self.as_of.isoformat()
        for index, expiry in enumerate(self.expiries):
            if expiry.rn_density is not None:
                payload["expiries"][index]["rn_density"] = expiry.rn_density.to_dict()
        return payload
