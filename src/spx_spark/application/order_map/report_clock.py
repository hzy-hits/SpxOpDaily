"""Shared exchange-local clock contract for scheduled desk reports."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta

from spx_spark.market_calendar import (
    DEFAULT_MARKET_CALENDAR,
    ET,
    MarketCalendar,
    MarketSession,
)


RTH_REPORT_CADENCE = timedelta(minutes=15)
# Shared with the Rust scheduled_report lane and the GTH desk-map source_slot.
REPORT_SLOT_CADENCE = RTH_REPORT_CADENCE
RTH_REPORT_START_GRACE_SECONDS = 120.0


def floor_report_slot_et(now: datetime) -> datetime:
    """Floor an aware timestamp to the ET quarter-hour report boundary."""

    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("report slot flooring requires a timezone-aware timestamp")
    ny = now.astimezone(ET)
    cadence_minutes = int(REPORT_SLOT_CADENCE.total_seconds() // 60)
    minute = (ny.minute // cadence_minutes) * cadence_minutes
    return ny.replace(minute=minute, second=0, microsecond=0)


@dataclass(frozen=True, slots=True)
class RthReportSlot:
    """One exchange-local RTH heartbeat boundary."""

    trading_date: str
    slot_at: datetime
    index: int
    delay_seconds: float

    @property
    def key(self) -> str:
        return f"{self.trading_date}:{self.slot_at.strftime('%H:%M')}"


def rth_report_schedule(
    trading_date: date,
    *,
    calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR,
) -> tuple[datetime, ...]:
    """Return every report boundary from RTH open through the last pre-close slot."""

    session = calendar.session(trading_date)
    if session is None:
        return ()
    return rth_report_schedule_for_session(session)


def rth_report_schedule_for_session(session: MarketSession) -> tuple[datetime, ...]:
    """Return report boundaries for an already-resolved calendar session."""

    slots: list[datetime] = []
    slot_at = session.open_at
    while slot_at < session.close_at:
        slots.append(slot_at)
        slot_at += RTH_REPORT_CADENCE
    return tuple(slots)


def rth_report_slot(
    now: datetime,
    *,
    start_grace_seconds: float = RTH_REPORT_START_GRACE_SECONDS,
    calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR,
) -> RthReportSlot | None:
    """Resolve a timer invocation to its ET quarter-hour RTH slot.

    Calendar timers are not guaranteed to start on second zero.  The bounded
    grace accepts ordinary service-manager scheduling jitter without turning
    an arbitrary in-session invocation into another scheduled report.
    """

    if now.tzinfo is None or now.utcoffset() is None:
        return None
    ny = now.astimezone(ET)
    session = calendar.session(ny.date())
    if session is None:
        return None
    return rth_report_slot_for_session(
        ny,
        session=session,
        start_grace_seconds=start_grace_seconds,
    )


def rth_report_slot_for_session(
    now: datetime,
    *,
    session: MarketSession,
    start_grace_seconds: float = RTH_REPORT_START_GRACE_SECONDS,
) -> RthReportSlot | None:
    """Resolve an invocation against one preselected session."""

    if now.tzinfo is None or now.utcoffset() is None:
        return None
    ny = now.astimezone(ET)
    schedule = rth_report_schedule_for_session(session)
    if not schedule or ny < schedule[0]:
        return None
    elapsed_seconds = (ny - schedule[0]).total_seconds()
    slot_index = int(elapsed_seconds // RTH_REPORT_CADENCE.total_seconds())
    if slot_index >= len(schedule):
        return None
    slot_at = schedule[slot_index]
    delay_seconds = (ny - slot_at).total_seconds()
    if delay_seconds < 0 or delay_seconds > max(float(start_grace_seconds), 0.0):
        return None
    return RthReportSlot(
        trading_date=session.trading_date.isoformat(),
        slot_at=slot_at,
        index=slot_index,
        delay_seconds=delay_seconds,
    )
