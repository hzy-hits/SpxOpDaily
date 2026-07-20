from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo


ET = ZoneInfo("America/New_York")

_RTH_OPEN = time(9, 30)
_RTH_CLOSE = time(16, 0)
_EARLY_CLOSE = time(13, 0)
_REVIEW_READY = time(17, 0)
_NEXT_EXPIRY_PREFETCH_BEFORE_CLOSE = timedelta(minutes=30)
_GLOBEX_OPEN = time(18, 0)
_GLOBEX_MAINTENANCE = time(17, 0)
_SPX_GTH_OPEN = time(20, 15)
_SPX_GTH_CLOSE = time(9, 25)
_FIVE_MINUTES = 5 * 60


@dataclass(frozen=True, slots=True)
class MarketSession:
    trading_date: date
    open_at: datetime
    close_at: datetime
    review_ready_at: datetime
    early_close: bool

    @property
    def expected_five_minute_buckets(self) -> int:
        """Return the number of non-overlapping five-minute RTH intervals."""
        return int((self.close_at - self.open_at).total_seconds() // _FIVE_MINUTES)


@dataclass(frozen=True, slots=True)
class SpxSessionWindow:
    """Full SPXW surface session for one trading date.

    The canvas runs from the evening GTH open (20:15 ET on the preceding
    calendar day) through the RTH close, split into gth / closed_gap / rth
    segments.  Segment boundaries follow the replay surface contract:
    gth [session_start, gth_end), closed_gap [gth_end, rth_open),
    rth [rth_open, session_end].
    """

    trading_date: date
    session_start: datetime
    gth_end: datetime
    rth_open: datetime
    session_end: datetime

    def segment_at(self, moment: datetime) -> str | None:
        if self.session_start <= moment < self.gth_end:
            return "gth"
        if self.gth_end <= moment < self.rth_open:
            return "closed_gap"
        if self.rth_open <= moment <= self.session_end:
            return "rth"
        return None


class MarketCalendar:
    """Deterministic US equity calendar using America/New_York wall time.

    Exceptional one-off closures and early closes can be supplied explicitly
    without introducing a runtime market-calendar dependency.
    """

    def __init__(
        self,
        *,
        full_day_closures: Iterable[date] = (),
        early_closes: Mapping[date, time] | None = None,
    ) -> None:
        self._full_day_closures = frozenset(full_day_closures)
        self._early_closes = dict(early_closes or {})

    def is_trading_day(self, day: date) -> bool:
        if day.weekday() >= 5 or day in self._full_day_closures:
            return False
        return day not in self._holidays_around(day.year)

    def next_trading_day(self, day: date) -> date:
        candidate = day + timedelta(days=1)
        while not self.is_trading_day(candidate):
            candidate += timedelta(days=1)
        return candidate

    def previous_trading_day(self, day: date) -> date:
        candidate = day - timedelta(days=1)
        while not self.is_trading_day(candidate):
            candidate -= timedelta(days=1)
        return candidate

    def trading_days_elapsed(self, start: date, end: date) -> int | None:
        """Count trading-day transitions from start to end, inclusive age 0."""

        if end < start:
            return None
        elapsed = 0
        current = start
        while current < end:
            candidate = self.next_trading_day(current)
            if candidate > end:
                break
            current = candidate
            elapsed += 1
        return elapsed

    def session(self, day: date) -> MarketSession | None:
        if not self.is_trading_day(day):
            return None

        close_time = self._early_closes.get(day)
        if close_time is None and self._is_scheduled_early_close(day):
            close_time = _EARLY_CLOSE
        close_time = close_time or _RTH_CLOSE

        return MarketSession(
            trading_date=day,
            open_at=datetime.combine(day, _RTH_OPEN, tzinfo=ET),
            close_at=datetime.combine(day, close_time, tzinfo=ET),
            review_ready_at=datetime.combine(day, _REVIEW_READY, tzinfo=ET),
            early_close=close_time != _RTH_CLOSE,
        )

    def is_rth_open(self, now: datetime) -> bool:
        current = _as_et(now)
        current_session = self.session(current.date())
        if current_session is None:
            return False
        return current_session.open_at <= current < current_session.close_at

    def is_globex_open(self, now: datetime) -> bool:
        """Return the regular CME equity-index futures session state.

        ES trades Sunday 18:00 ET through Friday 17:00 ET with a daily
        17:00-18:00 maintenance break. Exchange holiday exceptions remain a
        provider-health concern: a missing live ES quote fails the caller's
        data gate even when this regular schedule says the session is open.
        """

        current = _as_et(now)
        weekday = current.weekday()
        current_time = current.time()
        if weekday == 5:
            return False
        if weekday == 6:
            return current_time >= _GLOBEX_OPEN
        if weekday == 4:
            return current_time < _GLOBEX_MAINTENANCE
        return current_time < _GLOBEX_MAINTENANCE or current_time >= _GLOBEX_OPEN

    def is_spx_gth_open(self, now: datetime) -> bool:
        """Return the SPX global trading-hours session state.

        Each trading day's GTH session runs 20:15 ET on the preceding
        calendar day through 09:25 ET on the trading day. Using the equity
        calendar here prevents Friday-night and holiday-eve false sessions.
        """

        current = _as_et(now)
        if current.time() < _SPX_GTH_CLOSE:
            return self.is_trading_day(current.date())
        if current.time() >= _SPX_GTH_OPEN:
            return self.is_trading_day(current.date() + timedelta(days=1))
        return False

    def spx_session_window(self, day: date) -> SpxSessionWindow | None:
        """Return the full GTH→closed_gap→RTH surface window for a trading date."""

        session = self.session(day)
        if session is None:
            return None
        return SpxSessionWindow(
            trading_date=day,
            session_start=datetime.combine(day - timedelta(days=1), _SPX_GTH_OPEN, tzinfo=ET),
            gth_end=datetime.combine(day, _SPX_GTH_CLOSE, tzinfo=ET),
            rth_open=session.open_at,
            session_end=session.close_at,
        )

    def spx_session_date_for(self, now: datetime, *, retain_completed: bool = False) -> date | None:
        """Resolve the trading date whose surface window contains ``now``.

        Evening GTH (>= 20:15 ET) belongs to the next trading day; morning
        GTH, the closed gap, and RTH belong to the current trading day.
        Returns None outside every session window.  With
        ``retain_completed=True`` a completed session stays current until the
        next session's GTH open (post-close, weekends, and holidays resolve
        to the most recent trading date whose window has already started).
        """

        current = _as_et(now)
        if current.time() >= _SPX_GTH_OPEN:
            candidate = self.next_trading_day(current.date())
            window = self.spx_session_window(candidate)
            if window is not None and window.session_start <= current < window.gth_end:
                return candidate
            if not retain_completed:
                return None
        else:
            window = self.spx_session_window(current.date())
            if window is not None and window.session_start <= current <= window.session_end:
                return window.trading_date
            if not retain_completed:
                return None
        candidate_day = current.date()
        for _ in range(15):
            if self.is_trading_day(candidate_day):
                retained = self.spx_session_window(candidate_day)
                if retained is not None and current >= retained.session_start:
                    return retained.trading_date
            candidate_day -= timedelta(days=1)
        return None

    def research_expiry(self, now: datetime) -> date:
        current = _as_et(now)
        day = current.date()
        if self.is_trading_day(day) and current.time() < _REVIEW_READY:
            return day
        if self.is_trading_day(day):
            return self.next_trading_day(day)

        candidate = day
        while not self.is_trading_day(candidate):
            candidate += timedelta(days=1)
        return candidate

    def research_expiries(self, now: datetime) -> tuple[date, date]:
        current = self.research_expiry(now)
        return current, self.next_trading_day(current)

    def option_collection_expiry(self, now: datetime) -> date:
        """Roll acquisition to the next expiry 30 minutes before the RTH close."""

        current = _as_et(now)
        session = self.session(current.date())
        if session is not None:
            prefetch_at = session.close_at - _NEXT_EXPIRY_PREFETCH_BEFORE_CLOSE
            if prefetch_at <= current < session.review_ready_at:
                return self.next_trading_day(session.trading_date)
        return self.research_expiry(current)

    def option_collection_expiries(self, now: datetime) -> tuple[date, date]:
        current = self.option_collection_expiry(now)
        return current, self.next_trading_day(current)

    def is_next_expiry_prefetch_window(self, now: datetime) -> bool:
        current = _as_et(now)
        session = self.session(current.date())
        if session is None:
            return False
        return (
            session.close_at - _NEXT_EXPIRY_PREFETCH_BEFORE_CLOSE
            <= current
            < session.review_ready_at
        )

    def completed_review_date(self, now: datetime) -> date:
        current = _as_et(now)
        day = current.date()
        if self.is_trading_day(day) and current.time() >= _REVIEW_READY:
            return day
        return self.previous_trading_day(day)

    @staticmethod
    def _holidays_around(year: int) -> frozenset[date]:
        holidays: set[date] = set()
        for nominal_year in (year - 1, year, year + 1):
            holidays.update(_holidays_for_nominal_year(nominal_year))
        return frozenset(holidays)

    def _is_scheduled_early_close(self, day: date) -> bool:
        thanksgiving = _nth_weekday(day.year, 11, weekday=3, occurrence=4)
        if day == thanksgiving + timedelta(days=1):
            return True

        # NYSE closes early on July 3 when it is a trading day. If July 4 is
        # Saturday, July 3 is instead the observed full-day holiday.
        if day.month == 7 and day.day == 3:
            return True

        return day.month == 12 and day.day == 24


DEFAULT_MARKET_CALENDAR = MarketCalendar()


def default_spxw_expiry(
    today: date | None = None,
    *,
    now: datetime | None = None,
) -> str:
    """Compatibility wrapper for the calendar's 17:00 ET research expiry."""

    if today is not None:
        candidate = today
        while not DEFAULT_MARKET_CALENDAR.is_trading_day(candidate):
            candidate += timedelta(days=1)
        return candidate.strftime("%Y%m%d")
    current = now or datetime.now(tz=ET)
    return DEFAULT_MARKET_CALENDAR.research_expiry(current).strftime("%Y%m%d")


def _as_et(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("market calendar datetimes must be timezone-aware")
    return value.astimezone(ET)


def _holidays_for_nominal_year(year: int) -> set[date]:
    holidays = {
        _observed_new_year(year),
        _nth_weekday(year, 1, weekday=0, occurrence=3),
        _nth_weekday(year, 2, weekday=0, occurrence=3),
        _easter_sunday(year) - timedelta(days=2),
        _last_weekday(year, 5, weekday=0),
        _observed_fixed_holiday(date(year, 7, 4)),
        _nth_weekday(year, 9, weekday=0, occurrence=1),
        _nth_weekday(year, 11, weekday=3, occurrence=4),
        _observed_fixed_holiday(date(year, 12, 25)),
    }
    if year >= 2022:
        holidays.add(_observed_fixed_holiday(date(year, 6, 19)))
    return holidays


def _observed_new_year(year: int) -> date:
    new_year = date(year, 1, 1)
    # NYSE does not carry a Saturday New Year's closure back into the prior
    # calendar year (for example, January 1, 2028 has no observed closure).
    if new_year.weekday() == 6:
        return new_year + timedelta(days=1)
    return new_year


def _observed_fixed_holiday(day: date) -> date:
    if day.weekday() == 5:
        return day - timedelta(days=1)
    if day.weekday() == 6:
        return day + timedelta(days=1)
    return day


def _nth_weekday(year: int, month: int, *, weekday: int, occurrence: int) -> date:
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + (occurrence - 1) * 7)


def _last_weekday(year: int, month: int, *, weekday: int) -> date:
    if month == 12:
        first_next_month = date(year + 1, 1, 1)
    else:
        first_next_month = date(year, month + 1, 1)
    last = first_next_month - timedelta(days=1)
    return last - timedelta(days=(last.weekday() - weekday) % 7)


def _easter_sunday(year: int) -> date:
    """Return Gregorian Easter Sunday using the Meeus/Jones/Butcher rule."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    ell = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * ell) // 451
    month = (h + ell - 7 * m + 114) // 31
    day = (h + ell - 7 * m + 114) % 31 + 1
    return date(year, month, day)
