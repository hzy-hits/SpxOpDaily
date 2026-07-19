"""Contracts and serialization metadata for SPXW exposure surfaces."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Mapping
from zoneinfo import ZoneInfo

SCHEMA_VERSION = "spxw_exposure_surface.v1"
MODEL = "bs_r0_q0"
SIGN_CONVENTION = "calls_positive_puts_negative"
DEALER_POSITION_SIGN = "unknown"
YEAR_SECONDS = 365.0 * 24.0 * 3600.0
WEIGHTINGS = ("oi_weighted", "volume_weighted")
GREEK_KERNELS_PER_CONTRACT_CELL = 3
EXPIRY_TIMEZONE = ZoneInfo("America/New_York")
METRIC_UNITS = {
    "signed_gamma": "proxy_delta_dollars_per_1pct_underlier_move",
    "gross_gamma": "gross_delta_dollars_per_1pct_underlier_move",
    "charm": "proxy_1pct_notional_delta_change_per_calendar_minute",
    "vanna": "proxy_1pct_notional_delta_change_per_1_vol_point",
}
WEIGHTING_SEMANTICS = {
    "oi_weighted": (
        "reported_open_interest_unsigned_structural_proxy; "
        "zero_is_valid; missing_is_excluded"
    ),
    "volume_weighted": (
        "cumulative_contract_volume_unsigned_activity_proxy_not_buy_sell_flow; "
        "zero_is_valid; missing_is_excluded"
    ),
}
STRIKE_LADDER_BASIS = "observed_contract_strike_revalued_at_reference_spot_minutes_forward_0"


@dataclass(frozen=True)
class SurfaceContract:
    expiry: str
    strike: float
    right: str
    iv: float | None
    open_interest: float | None = 0.0
    volume: float | None = 0.0


@dataclass(frozen=True)
class SurfaceGridConfig:
    spot_step_points: float = 5.0
    spot_steps_each_side: int = 20
    default_time_offsets_minutes: tuple[float, ...] = (0.0, 5.0, 15.0, 30.0, 60.0)
    max_spot_points: int = 81
    max_time_points: int = 24
    max_cells: int = 1_944
    max_contracts: int = 2_000
    max_contract_cell_evaluations: int = 1_500_000
    min_tau_seconds: float = 300.0
    min_usable_contracts: int = 4
    min_coverage_ratio: float = 0.60
    min_iv: float = 0.0001
    max_iv: float = 10.0
    max_weight: float = 10_000_000.0

    def __post_init__(self) -> None:
        if not math.isfinite(self.spot_step_points) or self.spot_step_points <= 0:
            raise ValueError("spot_step_points must be positive and finite")
        if self.spot_steps_each_side < 0:
            raise ValueError("spot_steps_each_side must be non-negative")
        for name, value in (
            ("max_spot_points", self.max_spot_points),
            ("max_time_points", self.max_time_points),
            ("max_cells", self.max_cells),
            ("max_contracts", self.max_contracts),
            ("max_contract_cell_evaluations", self.max_contract_cell_evaluations),
        ):
            if value <= 0:
                raise ValueError(f"{name} must be positive")
        if self.min_tau_seconds < 0 or not math.isfinite(self.min_tau_seconds):
            raise ValueError("min_tau_seconds must be non-negative and finite")
        if self.min_usable_contracts <= 0:
            raise ValueError("min_usable_contracts must be positive")
        if not 0.0 <= self.min_coverage_ratio <= 1.0:
            raise ValueError("min_coverage_ratio must be between zero and one")
        if not math.isfinite(self.min_iv) or self.min_iv <= 0:
            raise ValueError("min_iv must be positive and finite")
        if not math.isfinite(self.max_iv) or self.max_iv < self.min_iv:
            raise ValueError("max_iv must be finite and no smaller than min_iv")
        if not math.isfinite(self.max_weight) or self.max_weight <= 0:
            raise ValueError("max_weight must be positive and finite")


@dataclass(frozen=True)
class SurfaceMetrics:
    signed_gamma: tuple[float | None, ...]
    gross_gamma: tuple[float | None, ...]
    charm: tuple[float | None, ...]
    vanna: tuple[float | None, ...]

    @classmethod
    def empty(cls, size: int) -> SurfaceMetrics:
        values = (None,) * size
        return cls(
            signed_gamma=values,
            gross_gamma=values,
            charm=values,
            vanna=values,
        )

    def to_dict(self) -> dict[str, tuple[float | None, ...]]:
        return {
            "signed_gamma": self.signed_gamma,
            "gross_gamma": self.gross_gamma,
            "charm": self.charm,
            "vanna": self.vanna,
        }


@dataclass(frozen=True)
class SurfaceMetricPoint:
    signed_gamma: float | None
    gross_gamma: float | None
    charm: float | None
    vanna: float | None

    @classmethod
    def empty(cls) -> SurfaceMetricPoint:
        return cls(
            signed_gamma=None,
            gross_gamma=None,
            charm=None,
            vanna=None,
        )


@dataclass(frozen=True)
class SurfaceExtremum:
    spot: float
    value: float


@dataclass(frozen=True)
class SurfaceCoverage:
    total_contracts: int
    usable_contracts: int
    ratio: float


@dataclass(frozen=True)
class SurfaceWeightingSlice:
    metrics: SurfaceMetrics
    zero_ridge_spot: float | None
    positive_peak: SurfaceExtremum | None
    negative_trough: SurfaceExtremum | None
    coverage: SurfaceCoverage
    quality: str
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class SurfaceTimeSlice:
    minutes_forward: float
    tau_seconds: float
    weightings: Mapping[str, SurfaceWeightingSlice]
    quality: str
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class SurfaceStrikeLeg:
    iv: float | None
    open_interest: float | None
    volume: float | None


@dataclass(frozen=True)
class SurfaceStrikeWeighting:
    metrics: SurfaceMetricPoint
    quality: str
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class SurfaceStrikeRow:
    strike: float
    call: SurfaceStrikeLeg | None
    put: SurfaceStrikeLeg | None
    weightings: Mapping[str, SurfaceStrikeWeighting]
    quality: str
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class ExposureSurface:
    created_at: datetime
    as_of: datetime
    expiry: str
    expiry_close: datetime
    reference_spot: float
    spot_grid: tuple[float, ...]
    time_offsets_minutes: tuple[float, ...]
    contract_count: int
    time_slices: tuple[SurfaceTimeSlice, ...]
    strike_ladder: tuple[SurfaceStrikeRow, ...]
    quality: str
    warnings: tuple[str, ...]
    schema_version: str = SCHEMA_VERSION
    model: str = MODEL
    sign_convention: str = SIGN_CONVENTION
    dealer_position_sign: str = DEALER_POSITION_SIGN
    metric_units: Mapping[str, str] = field(default_factory=lambda: dict(METRIC_UNITS))
    weighting_semantics: Mapping[str, str] = field(
        default_factory=lambda: dict(WEIGHTING_SEMANTICS)
    )
    strike_ladder_basis: str = STRIKE_LADDER_BASIS

    def to_dict(self) -> dict[str, Any]:
        return _json_safe(asdict(self))


@dataclass(frozen=True)
class _PreparedContract:
    strike: float
    right: str
    iv: float
    oi_weight: float | None
    volume_weight: float | None

    def weight(self, weighting: str) -> float | None:
        return self.oi_weight if weighting == "oi_weighted" else self.volume_weight


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, tuple | list):
        return [_json_safe(item) for item in value]
    return value


__all__ = (
    "DEALER_POSITION_SIGN",
    "ExposureSurface",
    "METRIC_UNITS",
    "MODEL",
    "SCHEMA_VERSION",
    "SIGN_CONVENTION",
    "STRIKE_LADDER_BASIS",
    "SurfaceContract",
    "SurfaceCoverage",
    "SurfaceExtremum",
    "SurfaceGridConfig",
    "SurfaceMetricPoint",
    "SurfaceMetrics",
    "SurfaceStrikeLeg",
    "SurfaceStrikeRow",
    "SurfaceStrikeWeighting",
    "SurfaceTimeSlice",
    "SurfaceWeightingSlice",
    "WEIGHTING_SEMANTICS",
)
