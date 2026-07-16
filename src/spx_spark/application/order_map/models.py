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
    "LEVEL_DECISION_PLAYS",
    "OrderCandidate",
    "PLAY_ORDER",
    "PLAY_TEMPLATE_LINES",
    "SHANGHAI_TZ",
    "SignalMode",
    "SpotResolution",
    "level_decision_play",
]


SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
BJ_WINDOW_START = time(13, 30)
BJ_WINDOW_END = time(21, 25)

LEVEL_BREAKOUT_CALL_KIND = "level_breakout_call"
LEVEL_BREAKOUT_PUT_KIND = "level_breakout_put"
LEVEL_FADE_CALL_KIND = "level_fade_call"
LEVEL_FADE_PUT_KIND = "level_fade_put"
LEVEL_DECISION_PLAYS = frozenset(
    {
        LEVEL_BREAKOUT_CALL_KIND,
        LEVEL_BREAKOUT_PUT_KIND,
        LEVEL_FADE_CALL_KIND,
        LEVEL_FADE_PUT_KIND,
    }
)


def level_decision_play(thesis: str, direction: str) -> str | None:
    return {
        ("breakout", "up"): LEVEL_BREAKOUT_CALL_KIND,
        ("breakout", "down"): LEVEL_BREAKOUT_PUT_KIND,
        ("fade", "up"): LEVEL_FADE_CALL_KIND,
        ("fade", "down"): LEVEL_FADE_PUT_KIND,
    }.get((thesis, direction))


PLAY_ORDER = (
    LEVEL_BREAKOUT_CALL_KIND,
    LEVEL_BREAKOUT_PUT_KIND,
    LEVEL_FADE_CALL_KIND,
    LEVEL_FADE_PUT_KIND,
    "put_wall_bounce_call",
    FLIP_RECLAIM_CALL_KIND,
    "flip_breakdown_put",
    CALL_WALL_BREAKOUT_CALL_KIND,
    "call_wall_fade_put",
)

PLAY_TEMPLATE_LINES = {
    LEVEL_BREAKOUT_CALL_KIND: "正式突破 {level_label}，确认买 call → SPXW {strike}{right}",
    LEVEL_BREAKOUT_PUT_KIND: "正式跌破 {level_label}，确认买 put → SPXW {strike}{right}",
    LEVEL_FADE_CALL_KIND: "正式拒绝下破 {level_label}，确认买 call → SPXW {strike}{right}",
    LEVEL_FADE_PUT_KIND: "正式拒绝上破 {level_label}，确认买 put → SPXW {strike}{right}",
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
    projected_mid: float | None
    limit_aggressive: float | None
    limit_conservative: float | None
    prob_touch: float | None
    prob_close_beyond: float | None
    delta: float
    gamma: float
    frontrun_level: float | None = None
    frontrun_projected_mid: float | None = None
    frontrun_limit: float | None = None
    frontrun_prob_touch: float | None = None
    order_style: str = "underlier_triggered_limit"
    projection_model: str = "taylor_fallback"
    touch_eta_minutes: float | None = None
    projection_iv_now: float | None = None
    projection_iv_at_touch: float | None = None
    projection_tau_now_minutes: float | None = None
    projection_tau_at_touch_minutes: float | None = None
    projection_touch_time_fraction: float | None = None
    projection_model_anchor_price: float | None = None
    projection_model_target_price: float | None = None
    projection_early_mid: float | None = None
    projection_late_mid: float | None = None
    projection_range_low: float | None = None
    projection_range_high: float | None = None
    projection_forward_now: float | None = None
    projection_forward_at_touch: float | None = None
    projection_pricing_kernel: str | None = None
    execution_quote_status: str = "range_only"
    execution_quote_reasons: tuple[str, ...] = ()
    execution_quote_spread_bps: float | None = None
    execution_quote_spread_percentile: float | None = None
    execution_quote_source_age_seconds: float | None = None
    execution_quote_provider_divergence_bps: float | None = None
    execution_quote_excluded_providers: tuple[str, ...] = ()
    touch_time_model_source: str = "brownian_heuristic"


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
