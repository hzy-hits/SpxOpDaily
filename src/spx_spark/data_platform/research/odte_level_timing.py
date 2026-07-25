"""Shared causal quote and RTH clock lookup helpers for 0DTE research."""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from datetime import datetime
from typing import Sequence

from .odte_level_signals import (
    RTH_ANALYSIS_START_ET_HHMM,
    RTH_EXIT_CLOCK_ET_HHMM,
    OptionTick,
    Signal,
    next_exit_clock,
)


def option_tick_mid(tick: OptionTick) -> float | None:
    if tick.mid is not None:
        return tick.mid
    if tick.bid is not None and tick.ask is not None:
        return (tick.bid + tick.ask) / 2.0
    return None


def first_tick_at_or_after(
    series: Sequence[OptionTick],
    times: Sequence[datetime],
    at: datetime,
) -> OptionTick | None:
    index = bisect_left(times, at)
    return None if index >= len(series) else series[index]


def tick_at_or_before(
    series: Sequence,
    times: Sequence[datetime],
    at: datetime,
    *,
    fallback_first: bool,
):
    index = bisect_right(times, at) - 1
    if index < 0:
        return series[0] if fallback_first and series else None
    return series[index]


def in_rth_1300_entry_window(signal: Signal) -> bool:
    """Whether an entry is in the expiry date's [09:45, 13:00) ET window."""

    if signal.expiry is None:
        return False
    opened_at = next_exit_clock(
        signal.entry_at,
        signal.expiry,
        hhmm=RTH_ANALYSIS_START_ET_HHMM,
    )
    exits_at = next_exit_clock(
        signal.entry_at,
        signal.expiry,
        hhmm=RTH_EXIT_CLOCK_ET_HHMM,
    )
    return opened_at <= signal.entry_at < exits_at
