"""Intraday call-bias loading for conditional order-map plays."""

from __future__ import annotations

from datetime import datetime

from spx_spark.intraday_shock import IntradayShockSettings, load_monitor_state, rth_session_date
from spx_spark.intraday_strategy import confirmed_call_bias


def load_intraday_call_bias(*, now: datetime) -> dict[str, object] | None:
    """Read the short-lived 5-second path confirmation without mutating it."""

    session_date = rth_session_date(now)
    if session_date is None:
        return None
    settings = IntradayShockSettings.from_env()
    monitor_state = load_monitor_state(settings.state_path, session_date=session_date)
    return confirmed_call_bias(monitor_state, now=now)
