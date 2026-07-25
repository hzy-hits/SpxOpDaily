"""Stable exposure-map data contracts and JSON serialization."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from spx_spark.analytics.options.exposure_types import WallLevel
from spx_spark.state_io import atomic_write_json_secure

SIGN_CONVENTION = "calls_positive_puts_negative"
DEALER_POSITION_SIGN = "unknown"
DIRECTION = "unknown"
MODEL = "bs_r0_q0"
METHOD = "call_positive_put_negative_oi_proxy_not_dealer_position"
PROXY_DISCLAIMER = (
    "all *_proxy metrics are house-defined; not comparable to any vendor metric of similar name"
)


@dataclass(frozen=True)
class ExposureInputRow:
    contract_id: str
    expiry: str
    strike: float
    right: str
    provider: str
    quality: str
    bid: float | None
    ask: float | None
    mid: float | None
    iv: float | None
    delta: float | None
    gamma: float | None
    open_interest: float
    volume: float
    quote_age_seconds: float | None
    observation_age_seconds: float | None
    structure_age_seconds: float | None
    pricing_provider: str
    greeks_provider: str | None
    open_interest_provider: str
    pricing_lane: str
    greeks_lane: str
    open_interest_lane: str
    open_interest_observation_age_seconds: float | None
    analytical_max_age_seconds: float
    pricing_allowed: bool
    analytical_allowed: bool
    analytical_reason: str | None
    delta_source: str
    gamma_source: str


@dataclass(frozen=True)
class StrikeExposureValues:
    call_gex: float | None
    put_gex: float | None
    net_gex: float | None
    abs_gex: float | None
    net_dex_proxy: float | None
    vex_proxy: float | None
    cex_proxy: float | None
    abs_dex_proxy: float | None = None


@dataclass(frozen=True)
class StrikeExposure:
    strike: float
    call_open_interest: float
    put_open_interest: float
    call_volume: float
    put_volume: float
    call_iv: float | None
    put_iv: float | None
    call_delta: float | None
    put_delta: float | None
    call_gamma: float | None
    put_gamma: float | None
    call_vanna_per_vol_point: float | None
    put_vanna_per_vol_point: float | None
    call_charm_per_minute: float | None
    put_charm_per_minute: float | None
    oi_weighted: StrikeExposureValues
    volume_weighted: StrikeExposureValues
    leg_metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ExposureAggregates:
    net_gex: float | None
    abs_gex: float | None
    net_gamma_ratio: float | None
    net_dex_proxy: float | None
    net_dex_ratio_proxy: float | None
    dagex_proxy: float | None
    vex_proxy: float | None
    cex_proxy: float | None
    abs_dex_proxy: float | None = None


@dataclass(frozen=True)
class WallSet:
    call_walls: tuple[WallLevel, ...]
    put_walls: tuple[WallLevel, ...]
    wall_method: str
    pin_candidate: float | None


@dataclass(frozen=True)
class ExpiryExposure:
    expiry: str
    row_count: int
    strike_count: int
    quality: str
    oi_quality: str
    iv_source: str
    snapshot_age_seconds: float | None
    delta_coverage_ratio: float
    iv_coverage_ratio: float
    strikes: tuple[StrikeExposure, ...]
    oi_weighted: ExposureAggregates
    volume_weighted: ExposureAggregates
    gex_weighting_divergence: float | None
    walls: WallSet
    zero_gamma: float | None
    gamma_flip_zone: tuple[float, float] | None
    zero_gamma_method: str
    sign_convention: str
    dealer_position_sign: str
    direction: str
    model: str
    warnings: tuple[str, ...]
    freshness: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ExposureMap:
    created_at: datetime
    as_of: datetime
    underlier: Any
    expiries: tuple[ExpiryExposure, ...]
    warnings: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return exposure_map_to_dict(self)


def exposure_map_to_dict(exposure: ExposureMap) -> dict[str, Any]:
    expiries_payload = []
    for expiry in exposure.expiries:
        strikes_payload = [
            {
                "strike": strike.strike,
                "call_open_interest": strike.call_open_interest,
                "put_open_interest": strike.put_open_interest,
                "call_volume": strike.call_volume,
                "put_volume": strike.put_volume,
                "call_iv": strike.call_iv,
                "put_iv": strike.put_iv,
                "call_delta": strike.call_delta,
                "put_delta": strike.put_delta,
                "call_gamma": strike.call_gamma,
                "put_gamma": strike.put_gamma,
                "call_vanna_per_vol_point": strike.call_vanna_per_vol_point,
                "put_vanna_per_vol_point": strike.put_vanna_per_vol_point,
                "call_charm_per_minute": strike.call_charm_per_minute,
                "put_charm_per_minute": strike.put_charm_per_minute,
                "oi_weighted": asdict(strike.oi_weighted),
                "volume_weighted": asdict(strike.volume_weighted),
                "leg_metadata": strike.leg_metadata,
            }
            for strike in expiry.strikes
        ]
        expiries_payload.append(
            {
                "expiry": expiry.expiry,
                "row_count": expiry.row_count,
                "strike_count": expiry.strike_count,
                "quality": expiry.quality,
                "oi_quality": expiry.oi_quality,
                "iv_source": expiry.iv_source,
                "snapshot_age_seconds": expiry.snapshot_age_seconds,
                "freshness": expiry.freshness,
                "delta_coverage_ratio": expiry.delta_coverage_ratio,
                "iv_coverage_ratio": expiry.iv_coverage_ratio,
                "strikes": strikes_payload,
                "oi_weighted": asdict(expiry.oi_weighted),
                "volume_weighted": asdict(expiry.volume_weighted),
                "gex_weighting_divergence": expiry.gex_weighting_divergence,
                "walls": {
                    "call_walls": [wall.to_dict() for wall in expiry.walls.call_walls],
                    "put_walls": [wall.to_dict() for wall in expiry.walls.put_walls],
                    "wall_method": expiry.walls.wall_method,
                    "pin_candidate": expiry.walls.pin_candidate,
                },
                "zero_gamma": expiry.zero_gamma,
                "gamma_flip_zone": expiry.gamma_flip_zone,
                "zero_gamma_method": expiry.zero_gamma_method,
                "sign_convention": SIGN_CONVENTION,
                "dealer_position_sign": DEALER_POSITION_SIGN,
                "direction": DIRECTION,
                "model": MODEL,
                "method": METHOD,
                "proxy_disclaimer": PROXY_DISCLAIMER,
                "warnings": list(expiry.warnings),
            }
        )
    return {
        "created_at": exposure.created_at.isoformat(),
        "as_of": exposure.as_of.isoformat(),
        "underlier": asdict(exposure.underlier),
        "expiries": expiries_payload,
        "warnings": list(exposure.warnings),
    }


def net_dex_proxy_by_expiry(exposure: ExposureMap, *, weighting: str) -> dict[str, float | None]:
    if weighting not in {"oi_weighted", "volume_weighted"}:
        raise ValueError(f"unsupported weighting: {weighting}")
    result: dict[str, float | None] = {}
    for expiry in exposure.expiries:
        aggregates = expiry.oi_weighted if weighting == "oi_weighted" else expiry.volume_weighted
        result[expiry.expiry] = aggregates.net_dex_proxy
    return result


def persist_exposure_map(exposure: ExposureMap, data_root: Path | str) -> Path:
    """Atomically write exposure_map.json under {data_root}/latest/."""
    path = Path(data_root) / "latest" / "exposure_map.json"
    atomic_write_json_secure(path, exposure_map_to_dict(exposure))
    return path
