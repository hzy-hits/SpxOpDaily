"""Domain states for the ES Globex path machine."""

from __future__ import annotations

from enum import Enum


class GlobexTrendRegime(str, Enum):
    NEUTRAL = "neutral"
    BEARISH = "bearish"
    BULLISH = "bullish"


REGIME_LABELS_CN = {
    GlobexTrendRegime.NEUTRAL.value: "中性/未确认",
    GlobexTrendRegime.BEARISH.value: "空头趋势",
    GlobexTrendRegime.BULLISH.value: "多头趋势",
}
