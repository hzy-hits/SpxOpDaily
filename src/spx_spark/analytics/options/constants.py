"""Shared constants for options analytics."""

from __future__ import annotations

from spx_spark.marketdata import MarketDataQuality

UNDERLIER_CANDIDATES = (
    ("index:SPX", 1.0),
    ("future:ES", 1.0),
    ("future:MES", 1.0),
    ("equity:SPY", 10.0),
)

UNDERLIER_MISMATCH_SOURCES = frozenset(
    {
        "future:ES",
        "future:MES",
        "equity:SPY",
    }
)

BAD_QUALITIES = {
    MarketDataQuality.MISSING,
    MarketDataQuality.ERROR,
    MarketDataQuality.STALE,
    MarketDataQuality.UNKNOWN,
    MarketDataQuality.DELAYED,
    MarketDataQuality.DELAYED_FROZEN,
}

# Structural features tolerate rotation-stale samples (see options_map history).
STRUCTURE_MAX_AGE_SECONDS = 900.0

_HARD_BAD_QUALITIES = {
    MarketDataQuality.MISSING,
    MarketDataQuality.ERROR,
    MarketDataQuality.UNKNOWN,
}

_MIN_TIME_TO_EXPIRY_YEARS = 15.0 / (60.0 * 24.0 * 365.0)

RN_DENSITY_MIN_STRIKES = 6
RN_DENSITY_NOISY_CLIP_FRACTION = 0.4
