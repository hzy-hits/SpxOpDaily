"""Option-chain snapshot contracts (stdlib-only)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any


class ChainReadiness(str, Enum):
    READY = "ready"
    DEGRADED = "degraded"
    BLOCKED = "blocked"


class ChainIssue(str, Enum):
    STALE = "stale"
    INSUFFICIENT_STRIKES = "insufficient_strikes"
    INSUFFICIENT_WIDTH = "insufficient_width"
    GAPPED_GRID = "gapped_grid"
    INSUFFICIENT_TWO_SIDED = "insufficient_two_sided"
    MISSING_IV = "missing_iv"
    MISSING_OI = "missing_oi"
    WIDE_MARKET = "wide_market"
    NON_MONOTONE_CALL_CURVE = "non_monotone_call_curve"
    NON_CONVEX_CALL_CURVE = "non_convex_call_curve"


@dataclass(frozen=True)
class ChainCoverage:
    total_legs: int
    distinct_strikes: int
    usable_strikes: int
    two_sided_strikes: int
    iv_legs: int
    oi_legs: int
    min_strike: float | None
    max_strike: float | None
    median_step: float | None
    max_gap: float | None
    lower_width_points: float | None
    upper_width_points: float | None
    max_quote_age_seconds: float | None
    max_cross_row_skew_seconds: float | None


@dataclass(frozen=True)
class OptionChainSnapshot:
    snapshot_id: str
    underlier: str
    trading_class: str
    expiry: str
    as_of: datetime
    spot: float | None
    quotes: tuple[Any, ...]
    coverage: ChainCoverage
    readiness: ChainReadiness
    issues: tuple[ChainIssue, ...]
