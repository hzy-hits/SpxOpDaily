"""Order-map value objects and shared play constants."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import time
from zoneinfo import ZoneInfo

from spx_spark.domain.state_machines import SignalMode
from spx_spark.intraday_strategy import (
    CALL_WALL_BREAKOUT_CALL_KIND,
    FLIP_RECLAIM_CALL_KIND,
)

__all__ = [
    "BJ_WINDOW_END",
    "BJ_WINDOW_START",
    "FRONTRUN_FRACTION",
    "FRONTRUN_MAX_POINTS",
    "FRONTRUN_MIN_POINTS",
    "HL_SP500_PROXY_ID",
    "OrderCandidate",
    "PLAY_ORDER",
    "PLAY_TEMPLATE_LINES",
    "SHANGHAI_TZ",
    "SignalMode",
    "SpotResolution",
]


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
BJ_WINDOW_START = time(13, 30)
BJ_WINDOW_END = time(21, 25)

PLAY_ORDER = (
    "put_wall_bounce_call",
    FLIP_RECLAIM_CALL_KIND,
    "flip_breakdown_put",
    CALL_WALL_BREAKOUT_CALL_KIND,
    "call_wall_fade_put",
)

PLAY_TEMPLATE_LINES = {
    "put_wall_bounce_call": "{level_label} 反弹买 call → SPXW {strike}{right}",
    FLIP_RECLAIM_CALL_KIND: "{level_label} 收复回踩买 call → SPXW {strike}{right}",
    "flip_breakdown_put": "{level_label} 跌破买 put → SPXW {strike}{right}",
    CALL_WALL_BREAKOUT_CALL_KIND: "{level_label} 突破回踩买 call → SPXW {strike}{right}",
    "call_wall_fade_put": "{level_label} 冲墙买 put → SPXW {strike}{right}",
}


@dataclass(frozen=True)
class OrderCandidate:
    play: str
    level: float
    level_label: str
    contract_id: str
    strike: int
    right: str
    current_mid: float
    projected_mid: float
    limit_aggressive: float
    limit_conservative: float
    prob_touch: float | None
    prob_close_beyond: float | None
    delta: float
    gamma: float
    frontrun_level: float | None = None
    frontrun_projected_mid: float | None = None
    frontrun_limit: float | None = None
    frontrun_prob_touch: float | None = None
    order_style: str = "resting_limit"
    projection_model: str = "taylor_fallback"
    touch_eta_minutes: float | None = None


FRONTRUN_FRACTION = 0.30
FRONTRUN_MIN_POINTS = 2.0
FRONTRUN_MAX_POINTS = 8.0

HL_SP500_PROXY_ID = "crypto_perp:xyz:SP500"


@dataclass(frozen=True)
class SpotResolution:
    research_price: float | None
    research_source: str | None
    pricing_price: float | None
    pricing_source: str | None
    pricing_allowed: bool
    gate_state: str
    reason: str
    divergence_bps: float | None = None

    @property
    def research_only(self) -> bool:
        return not self.pricing_allowed
