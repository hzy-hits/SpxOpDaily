"""Versioned contracts for unified market, option and decision projections."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

from spx_spark.marketdata import Quote


# The canonical provider-neutral quote already exists in marketdata. Re-export
# the contract name used by the feature pipeline instead of duplicating it.
NormalizedQuote = Quote


class FrameQuality(str, Enum):
    READY = "ready"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"


class MarketSessionSegment(str, Enum):
    ASIA = "asia"
    EUROPE = "europe"
    US_PREMARKET = "us_premarket"
    RTH = "rth"
    CURB = "curb"
    MAINTENANCE = "maintenance"


@dataclass(frozen=True)
class MinuteMarketFrame:
    schema_version: int
    frame_id: str
    session_id: str
    as_of: datetime
    quality: FrameQuality
    es: dict[str, Any]
    session_ranges: dict[str, Any]
    volume: dict[str, Any]
    cross_asset: dict[str, Any]
    volatility: dict[str, Any]
    diagnostics: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["as_of"] = self.as_of.isoformat()
        payload["quality"] = self.quality.value
        return payload


@dataclass(frozen=True)
class L1MicrostructureFrame:
    quality: FrameQuality
    expiry: str | None
    contract_count: int
    metrics: dict[str, Any]
    diagnostics: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["quality"] = self.quality.value
        return payload


@dataclass(frozen=True)
class OptionStructureFrame:
    schema_version: int
    frame_id: str
    as_of: datetime
    quality: FrameQuality
    front_expiry: str | None
    next_expiry: str | None
    structure: dict[str, Any]
    volatility: dict[str, Any]
    concentration: dict[str, Any]
    density: dict[str, Any]
    l1: L1MicrostructureFrame
    diagnostics: dict[str, Any]
    exposure: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["as_of"] = self.as_of.isoformat()
        payload["quality"] = self.quality.value
        payload["l1"] = self.l1.to_dict()
        return payload


@dataclass(frozen=True)
class DecisionContext:
    schema_version: int
    context_id: str
    as_of: datetime
    session_id: str
    market_frame_id: str
    option_frame_id: str
    trend: dict[str, Any]
    level_decision: dict[str, Any]
    confirmations: dict[str, Any]
    invalidations: tuple[str, ...]
    data_quality: dict[str, Any]
    regime_decision: dict[str, Any] = field(default_factory=dict)
    breakout_filter: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["as_of"] = self.as_of.isoformat()
        return payload


@dataclass(frozen=True)
class DecisionAudit:
    schema_version: int
    audit_id: str
    context_id: str
    observed_at: datetime
    trigger: str
    decision_mid: float | None
    order_limit: float | None
    fill_price: float | None
    slippage: float | None
    outcome_status: str
    outcome_reference: str | None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["observed_at"] = self.observed_at.isoformat()
        return payload
