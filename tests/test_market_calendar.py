from __future__ import annotations

from datetime import date, datetime, time, timezone

import pytest

from spx_spark.market_calendar import ET, MarketCalendar


CALENDAR = MarketCalendar()


def et_datetime(day: date, hour: int, minute: int = 0) -> datetime:
    return datetime(day.year, day.month, day.day, hour, minute, tzinfo=ET)


def test_normal_session_has_expected_window_and_bucket_count() -> None:
    session = CALENDAR.session(date(2026, 7, 2))

    assert session is not None
    assert session.open_at == et_datetime(date(2026, 7, 2), 9, 30)
    assert session.close_at == et_datetime(date(2026, 7, 2), 16)
    assert session.review_ready_at == et_datetime(date(2026, 7, 2), 17)
    assert session.early_close is False
    assert session.expected_five_minute_buckets == 78


@pytest.mark.parametrize("day", [date(2026, 11, 27), date(2026, 12, 24)])
def test_approved_2026_early_closes_keep_review_ready_at_1700(day: date) -> None:
    session = CALENDAR.session(day)

    assert session is not None
    assert session.close_at == et_datetime(day, 13)
    assert session.review_ready_at == et_datetime(day, 17)
    assert session.early_close is True
    assert session.expected_five_minute_buckets == 42


@pytest.mark.parametrize(
    "day",
    [
        date(2026, 1, 1),  # New Year's Day
        date(2026, 1, 19),  # Martin Luther King Jr. Day
        date(2026, 2, 16),  # Washington's Birthday
        date(2026, 4, 3),  # Good Friday
        date(2026, 5, 25),  # Memorial Day
        date(2026, 6, 19),  # Juneteenth
        date(2026, 7, 3),  # Independence Day observed
        date(2026, 9, 7),  # Labor Day
        date(2026, 11, 26),  # Thanksgiving
        date(2026, 12, 25),  # Christmas Day
        date(2027, 6, 18),  # Juneteenth observed
        date(2027, 12, 24),  # Christmas observed
    ],
)
def test_regular_and_observed_full_day_holidays_are_closed(day: date) -> None:
    assert CALENDAR.is_trading_day(day) is False
    assert CALENDAR.session(day) is None


def test_research_expiry_rolls_at_exactly_1700_et() -> None:
    thursday = date(2026, 7, 9)

    assert CALENDAR.research_expiry(et_datetime(thursday, 16, 59)) == thursday
    assert CALENDAR.research_expiry(et_datetime(thursday, 17)) == date(2026, 7, 10)
    assert CALENDAR.research_expiry(et_datetime(date(2026, 7, 10), 17)) == date(2026, 7, 13)


def test_july_2_roll_skips_observed_holiday_and_weekend() -> None:
    now = et_datetime(date(2026, 7, 2), 17)

    assert CALENDAR.research_expiry(now) == date(2026, 7, 6)
    assert CALENDAR.research_expiries(now) == (date(2026, 7, 6), date(2026, 7, 7))


def test_cross_year_roll_skips_new_year_and_weekend() -> None:
    now = et_datetime(date(2026, 12, 31), 17)

    assert CALENDAR.research_expiry(now) == date(2027, 1, 4)


def test_trading_days_elapsed_skips_holidays_and_weekends() -> None:
    assert CALENDAR.trading_days_elapsed(date(2026, 7, 2), date(2026, 7, 2)) == 0
    assert CALENDAR.trading_days_elapsed(date(2026, 7, 2), date(2026, 7, 6)) == 1
    assert CALENDAR.trading_days_elapsed(date(2026, 7, 6), date(2026, 7, 2)) is None
    assert CALENDAR.trading_days_elapsed(date(2026, 7, 10), date(2026, 7, 11)) == 0
    assert CALENDAR.trading_days_elapsed(date(2026, 7, 7), date(2026, 7, 12)) == 3
    assert CALENDAR.trading_days_elapsed(date(2026, 7, 2), date(2026, 7, 5)) == 0


def test_globex_schedule_includes_sunday_reopen_and_daily_break() -> None:
    sunday = date(2026, 7, 12)
    monday = date(2026, 7, 13)
    friday = date(2026, 7, 17)
    saturday = date(2026, 7, 18)

    assert CALENDAR.is_globex_open(et_datetime(sunday, 17, 59)) is False
    assert CALENDAR.is_globex_open(et_datetime(sunday, 18)) is True
    assert CALENDAR.is_globex_open(et_datetime(monday, 16, 59)) is True
    assert CALENDAR.is_globex_open(et_datetime(monday, 17, 30)) is False
    assert CALENDAR.is_globex_open(et_datetime(monday, 18)) is True
    assert CALENDAR.is_globex_open(et_datetime(friday, 17)) is False
    assert CALENDAR.is_globex_open(et_datetime(saturday, 12)) is False


def test_spx_gth_tracks_trading_date_weekends_and_holidays() -> None:
    sunday = date(2026, 7, 12)
    monday = date(2026, 7, 13)
    friday = date(2026, 7, 17)

    assert CALENDAR.is_spx_gth_open(et_datetime(sunday, 20, 14)) is False
    assert CALENDAR.is_spx_gth_open(et_datetime(sunday, 20, 15)) is True
    assert CALENDAR.is_spx_gth_open(et_datetime(monday, 9, 24)) is True
    assert CALENDAR.is_spx_gth_open(et_datetime(monday, 9, 25)) is False
    assert CALENDAR.is_spx_gth_open(et_datetime(friday, 20, 15)) is False

    labor_day_eve = date(2026, 9, 6)
    labor_day = date(2026, 9, 7)
    assert CALENDAR.is_spx_gth_open(et_datetime(labor_day_eve, 20, 15)) is False
    assert CALENDAR.is_spx_gth_open(et_datetime(labor_day, 20, 15)) is True


def test_utc_rollover_tracks_et_across_daylight_saving_change() -> None:
    before_dst = date(2026, 3, 6)
    after_dst = date(2026, 3, 9)

    assert CALENDAR.research_expiry(datetime(2026, 3, 6, 21, 59, tzinfo=timezone.utc)) == before_dst
    assert CALENDAR.research_expiry(datetime(2026, 3, 6, 22, 0, tzinfo=timezone.utc)) == after_dst
    assert CALENDAR.research_expiry(datetime(2026, 3, 9, 20, 59, tzinfo=timezone.utc)) == after_dst
    assert CALENDAR.research_expiry(datetime(2026, 3, 9, 21, 0, tzinfo=timezone.utc)) == date(
        2026, 3, 10
    )


def test_saturday_new_year_has_no_prior_friday_observance() -> None:
    assert CALENDAR.is_trading_day(date(2027, 12, 31)) is True
    assert CALENDAR.is_trading_day(date(2028, 1, 1)) is False


def test_rth_open_uses_actual_close_and_accepts_other_aware_timezones() -> None:
    regular_day = date(2026, 7, 2)
    early_day = date(2026, 11, 27)

    assert CALENDAR.is_rth_open(et_datetime(regular_day, 9, 29)) is False
    assert CALENDAR.is_rth_open(et_datetime(regular_day, 9, 30)) is True
    assert CALENDAR.is_rth_open(et_datetime(regular_day, 16)) is False
    assert CALENDAR.is_rth_open(et_datetime(early_day, 12, 59)) is True
    assert CALENDAR.is_rth_open(et_datetime(early_day, 13)) is False
    assert CALENDAR.is_rth_open(datetime(2026, 7, 2, 14, 0, tzinfo=timezone.utc)) is True


def test_completed_review_date_waits_until_1700_even_on_early_close() -> None:
    early_day = date(2026, 11, 27)

    assert CALENDAR.completed_review_date(et_datetime(early_day, 16, 59)) == date(2026, 11, 25)
    assert CALENDAR.completed_review_date(et_datetime(early_day, 17)) == early_day
    assert CALENDAR.completed_review_date(et_datetime(date(2026, 11, 28), 12)) == early_day


def test_explicit_exception_overrides_are_supported() -> None:
    closed = date(2026, 7, 8)
    shortened = date(2026, 7, 9)
    calendar = MarketCalendar(
        full_day_closures={closed},
        early_closes={shortened: time(14)},
    )

    assert calendar.session(closed) is None
    shortened_session = calendar.session(shortened)
    assert shortened_session is not None
    assert shortened_session.close_at == et_datetime(shortened, 14)
    assert shortened_session.early_close is True
    assert shortened_session.expected_five_minute_buckets == 54


def test_naive_datetime_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        CALENDAR.research_expiry(datetime(2026, 7, 2, 17))
