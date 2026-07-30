"""Frozen compatibility contract for the retired Put shadow collector."""

from __future__ import annotations

from datetime import time


PUT_SHADOW_LANES = frozenset(
    {
        "long_0dte_rth_flip_low_breakdown_put_shadow",
        "long_0dte_rth_upper_rejection_put_shadow",
    }
)
PUT_SHADOW_EXACT_QUOTE_POLICY_VERSION = "put_shadow_exact_quote.v1"
PUT_SHADOW_EXACT_QUOTE_MAX_AGE_SECONDS = 15.0
PUT_SHADOW_CANDIDATE_CONTRACT_VERSION = "put_shadow_candidate_lifecycle.v2"
PUT_SHADOW_STATE_SCHEMA_VERSION = 2
PUT_SHADOW_ENTRY_WINDOW_START_ET = time(9, 45)
PUT_SHADOW_HARD_EXIT_ET = time(13, 0)
LEGACY_PUT_SHADOW_SOURCE_CONTRACT_VERSION = "rth_lanes_0945_1300_put_shadow.v1"
PUT_SHADOW_CONSUMED_IDENTITY_LIMIT = 1_000
PUT_SHADOW_LANE_CONTRACTS: dict[str, tuple[str, str, frozenset[str]]] = {
    "long_0dte_rth_flip_low_breakdown_put_shadow": (
        "level_breakout_put",
        "breakout",
        frozenset({"flip_low"}),
    ),
    "long_0dte_rth_upper_rejection_put_shadow": (
        "level_fade_put",
        "fade",
        frozenset({"call_wall", "flip_high"}),
    ),
}
