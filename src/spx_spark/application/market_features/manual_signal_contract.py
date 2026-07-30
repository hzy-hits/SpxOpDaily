"""Approved RTH manual-signal lanes and their supported level paths."""

from __future__ import annotations

from datetime import time


ENTRY_WINDOW_START_ET = time(9, 30)
ENTRY_WINDOW_END_ET = time(15, 30)
HARD_EXIT_ET = time(15, 45)
CALL_BREAKOUT_MANUAL_LANE = "long_0dte_rth_upper_breakout_call_manual"
LOWER_REJECTION_CALL_MANUAL_LANE = "long_0dte_rth_lower_rejection_call_manual"
FLIP_LOW_BREAKDOWN_PUT_MANUAL_LANE = "long_0dte_rth_flip_low_breakdown_put_manual"
UPPER_REJECTION_PUT_MANUAL_LANE = "long_0dte_rth_upper_rejection_put_manual"
PUT_WALL_BREAKDOWN_PUT_MANUAL_LANE = "long_0dte_rth_put_wall_breakdown_put_manual"

# Compatibility aliases keep persisted cards revalidatable after lane renames.
CALL_BREAKOUT_PILOT_LANE = CALL_BREAKOUT_MANUAL_LANE
PUT_WALL_BREAKDOWN_DISABLED_LANE = PUT_WALL_BREAKDOWN_PUT_MANUAL_LANE
LEGACY_PUT_SHADOW_LANES = frozenset(
    {
        "long_0dte_rth_flip_low_breakdown_put_shadow",
        "long_0dte_rth_upper_rejection_put_shadow",
        "long_0dte_rth_put_wall_breakdown_disabled",
    }
)
APPROVED_MANUAL_LANE_CONTRACTS = {
    CALL_BREAKOUT_MANUAL_LANE: ("up", "C"),
    LOWER_REJECTION_CALL_MANUAL_LANE: ("up", "C"),
    FLIP_LOW_BREAKDOWN_PUT_MANUAL_LANE: ("down", "P"),
    UPPER_REJECTION_PUT_MANUAL_LANE: ("down", "P"),
    PUT_WALL_BREAKDOWN_PUT_MANUAL_LANE: ("down", "P"),
    "rth_confirmed_level": ("up", "C"),
    "long_0dte_rth_upside_breakout_pilot": ("up", "C"),
}


def manual_lane_scope(
    *,
    enabled: bool,
    thesis: str,
    direction: str,
    level_kind: str,
) -> tuple[str, bool, str, str | None]:
    """Map one confirmed level lifecycle to its manual notification lane."""

    supported = {
        ("breakout", "down", "flip_low"): (
            FLIP_LOW_BREAKDOWN_PUT_MANUAL_LANE,
            "normal",
        ),
        ("breakout", "down", "put_wall"): (
            PUT_WALL_BREAKDOWN_PUT_MANUAL_LANE,
            "normal",
        ),
        ("breakout", "up", "flip_high"): (CALL_BREAKOUT_MANUAL_LANE, "high"),
        ("breakout", "up", "call_wall"): (CALL_BREAKOUT_MANUAL_LANE, "high"),
        ("fade", "down", "call_wall"): (UPPER_REJECTION_PUT_MANUAL_LANE, "normal"),
        ("fade", "down", "flip_high"): (UPPER_REJECTION_PUT_MANUAL_LANE, "normal"),
        ("fade", "up", "put_wall"): (LOWER_REJECTION_CALL_MANUAL_LANE, "normal"),
        ("fade", "up", "flip_low"): (LOWER_REJECTION_CALL_MANUAL_LANE, "normal"),
    }
    lane_priority = supported.get((thesis, direction, level_kind))
    if lane_priority is None:
        return "rth_confirmed_level", False, "disabled", "unsupported_level_path"
    lane, priority = lane_priority
    return (
        lane,
        False,
        priority if enabled else "disabled",
        None if enabled else "manual_signals_disabled",
    )
