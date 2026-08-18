from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from spx_spark.data_platform.research.es_volume_momentum_backtest import (
    FactObservation,
    Geometry,
    apply_direction_stick,
    build_funnel,
    label_geometries,
)
from spx_spark.data_platform.research.odte_level_signals import OptionTick


SESSION = date(2026, 8, 17)


def _fact(
    minute: int,
    second: int = 0,
    *,
    label: str = "elevated",
    direction: str = "up",
    one: float | None = 1.0,
    five: float | None = 2.0,
    atr: float | None = 2.0,
) -> FactObservation:
    return FactObservation(
        session_date=SESSION.isoformat(),
        decision_at=datetime(2026, 8, 17, 14, minute, second, tzinfo=timezone.utc),
        label=label,
        direction=direction,
        pace_ratio=3.0,
        return_1m_points=one,
        return_5m_points=five,
        atr_5m=atr,
        spx=6003.0,
    )


def _tick(at: datetime, bid: float, ask: float) -> OptionTick:
    return OptionTick(
        at=at,
        bid=bid,
        ask=ask,
        mid=(bid + ask) / 2,
        source_at=at,
        delta=None,
        implied_vol=None,
    )


def test_funnel_uses_first_fully_qualifying_print_in_each_minute() -> None:
    observations = [
        _fact(0, label="normal"),
        _fact(1, 1, one=None),
        _fact(1, 20),
        _fact(2, direction="flat"),
    ]

    funnel, openings, quality = build_funnel(observations)

    assert funnel == {
        "facts_rth_minutes": 3,
        "elevated_minutes": 2,
        "directional_minutes": 1,
        "aligned_1m_5m_minutes": 1,
        "strong_momentum_minutes": 1,
        "atr_available_minutes": 1,
        "not_too_late_minutes": 1,
    }
    assert [row.decision_at.second for row in openings] == [20]
    assert quality["minutes_with_intraminute_fact_variation"] == 1


def test_direction_stick_is_independent_by_direction_and_reopens_at_900_seconds() -> None:
    first = _fact(0)
    within = _fact(14, 59)
    reopen = _fact(15)
    opposite = _fact(1, direction="down", one=-1.0, five=-2.0)

    accepted = apply_direction_stick([first, opposite, within, reopen])

    assert accepted == [first, opposite, reopen]


class FakeQuoteStore:
    def __init__(self, series: dict[tuple[str, float, str], list[OptionTick]]) -> None:
        self.series = series

    def load_option_window(self, **_kwargs) -> int:
        return sum(len(rows) for rows in self.series.values())

    def option_snapshot(self, *, as_of: datetime, strikes, **_kwargs):
        result = {}
        for (provider, strike, right), ticks in self.series.items():
            if strike not in strikes:
                continue
            eligible = [tick for tick in ticks if tick.at <= as_of]
            if eligible:
                result[(provider, strike, right)] = eligible[-1]
        return result

    def option_series(self, *, provider: str, strike: float, right: str, start, end, **_kwargs):
        return [
            tick
            for tick in self.series[(provider, strike, right)]
            if start <= tick.at <= end
        ]


def test_label_geometry_uses_conservative_ask_and_production_premium_stop() -> None:
    entry = datetime(2026, 8, 17, 14, 0, tzinfo=timezone.utc)
    stop = entry + timedelta(minutes=1)
    store = FakeQuoteStore(
        {
            ("schwab", 6000.0, "C"): [
                _tick(entry, 5.0, 6.0),
                _tick(stop, 4.0, 5.0),
            ],
            ("schwab", 6010.0, "C"): [
                _tick(entry, 2.0, 3.0),
                _tick(stop, 2.0, 2.0),
            ],
        }
    )
    geometry = Geometry(
        cohort="factor_signal",
        row_id="one",
        session_date=SESSION,
        decision_at=entry,
        direction="UP",
        right="C",
        long_strike=6000.0,
        short_strike=6010.0,
        fixed_provider=None,
        metadata={},
    )

    row = label_geometries(store, [geometry])[0]

    assert row["entry_combo_ask"] == 4.0
    assert row["exit_combo_bid"] == 2.0
    assert row["exit_reason"] == "premium_stop"
    assert row["pnl_gross_points"] == -2.0
    assert row["pnl_net_fees_points"] == pytest.approx(-2.0528)


def test_label_geometry_drops_marks_exhausted_without_a_close_bbo() -> None:
    entry = datetime(2026, 8, 17, 14, 0, tzinfo=timezone.utc)
    later = entry + timedelta(minutes=1)
    store = FakeQuoteStore(
        {
            ("schwab", 6000.0, "P"): [
                _tick(entry, 5.5, 6.0),
                _tick(later, 6.0, 7.0),
            ],
            ("schwab", 5990.0, "P"): [
                _tick(entry, 2.0, 3.0),
                _tick(later, 2.0, 3.0),
            ],
        }
    )
    geometry = Geometry(
        cohort="factor_signal",
        row_id="one",
        session_date=SESSION,
        decision_at=entry,
        direction="DOWN",
        right="P",
        long_strike=6000.0,
        short_strike=5990.0,
        fixed_provider=None,
        metadata={},
    )

    row = label_geometries(store, [geometry])[0]

    assert row["label_status"] == "dropped"
    assert row["drop_reason"] == "exit_bbo_unavailable"
    assert row["provisional_exit_reason"] == "marks_exhausted"
