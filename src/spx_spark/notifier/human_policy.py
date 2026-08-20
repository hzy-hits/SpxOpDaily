"""Final human-notification session policy.

Producers continue evaluating and persisting during the blackout.  This
module only decides whether a completed notification may reach a person.
"""

from __future__ import annotations

from datetime import datetime

from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.notifier.model import NotificationEnvelope


QUIET_WINDOW_EXCEPTION_LANES = frozenset(
    {
        "position_safety",
        "execution_safety",
        "trade_ready",
        "gth_manual_candidate",
        "gth_level_manual_candidate",
        "growth_dislocation",
    }
)


def quiet_window_suppresses(
    envelope: NotificationEnvelope,
    *,
    now: datetime,
) -> bool:
    """Block ordinary human delivery from RTH close to the next GTH open."""

    if envelope.lane in QUIET_WINDOW_EXCEPTION_LANES:
        return False
    return DEFAULT_MARKET_CALENDAR.is_post_rth_pre_gth_quiet(now)
