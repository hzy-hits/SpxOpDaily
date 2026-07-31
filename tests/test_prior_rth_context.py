from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from spx_spark.application.market_features.prior_rth_context import (
    build_prior_rth_context,
    gth_position_fraction,
    prior_session_operator_line,
    prior_session_signal_view,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR


UTC = timezone.utc
NOW = datetime(2026, 7, 30, 3, 0, tzinfo=UTC)


def test_prior_rth_context_carries_shock_close_location_and_tail() -> None:
    session = DEFAULT_MARKET_CALENDAR.session(
        DEFAULT_MARKET_CALENDAR.previous_trading_day(
            DEFAULT_MARKET_CALENDAR.research_expiry(NOW)
        )
    )
    assert session is not None
    samples = []
    cursor = session.open_at
    minute = 0
    while cursor <= session.close_at:
        progress = minute / 390.0
        price = 7420.0 - 106.0 * progress
        samples.append(
            {
                "at": cursor.isoformat(),
                "instruments": {
                    "index:SPX": {
                        "price": price,
                        "reference_close": 7428.78,
                    }
                },
            }
        )
        cursor += timedelta(minutes=1)
        minute += 1

    context = build_prior_rth_context(
        samples,
        now=NOW,
        official_close=7316.15,
    )

    assert context["status"] == "ready"
    assert context["session_date"] == "2026-07-29"
    assert context["return_points"] == pytest.approx(-112.63)
    assert context["return_fraction"] == pytest.approx(-0.01516131)
    assert context["shock_direction"] == "down"
    assert context["close_zone"] == "lower"
    assert context["path_class"] == "shock_down_close_low"
    assert context["minute_coverage"] == 1.0
    assert context["execution_gate"] is False


def test_prior_down_shock_marks_only_lower_extreme_put_as_chase_risk() -> None:
    context = {
        "status": "ready",
        "session_date": "2026-07-29",
        "return_fraction": -0.015,
        "return_points": -112.0,
        "close_location_fraction": 0.02,
        "tail_return_fraction": -0.004,
        "shock_direction": "down",
        "close_zone": "lower",
        "path_class": "shock_down_close_low",
    }

    floor_put = prior_session_signal_view(
        context,
        direction="down",
        gth_position_fraction=0.02,
    )
    rebound_call = prior_session_signal_view(
        context,
        direction="up",
        gth_position_fraction=0.02,
    )

    assert floor_put["chase_risk"] == "high"
    assert floor_put["execution_gate"] is False
    assert rebound_call["chase_risk"] == "normal"
    assert "本票同向追单风险高" in prior_session_operator_line(floor_put)


def test_seventy_seven_percent_spx_path_is_partial_not_ready() -> None:
    session = DEFAULT_MARKET_CALENDAR.session(
        DEFAULT_MARKET_CALENDAR.previous_trading_day(
            DEFAULT_MARKET_CALENDAR.research_expiry(NOW)
        )
    )
    assert session is not None
    minutes = [session.open_at + timedelta(minutes=index) for index in range(300)]
    minutes.append(session.close_at)
    samples = [
        {
            "at": at.isoformat(),
            "instruments": {
                "index:SPX": {
                    "price": 7400.0 + index / 10.0,
                    "reference_close": 7390.0,
                }
            },
        }
        for index, at in enumerate(minutes)
    ]

    context = build_prior_rth_context(samples, now=NOW)

    assert context["minute_coverage"] == pytest.approx(301 / 390)
    assert context["status"] == "partial"
    assert "prior_rth_minute_coverage_low" in context["reasons"]
    assert context["execution_gate"] is False


def test_gth_position_fraction_is_bounded_and_requires_a_range() -> None:
    assert gth_position_fraction(
        {"price": 7355.25, "session_low": 7354.5, "session_high": 7392.5}
    ) == pytest.approx(0.01973684)
    assert gth_position_fraction(
        {"price": 7400.0, "session_low": 7400.0, "session_high": 7400.0}
    ) is None
