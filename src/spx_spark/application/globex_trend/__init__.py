"""ES Globex trend and reversal detection."""

from spx_spark.application.globex_trend.machine import advance_trend_state, initial_state
from spx_spark.application.globex_trend.models import GlobexTrendRegime
from spx_spark.application.globex_trend.state import load_trend_state

__all__ = [
    "GlobexTrendRegime",
    "advance_trend_state",
    "initial_state",
    "load_trend_state",
]
