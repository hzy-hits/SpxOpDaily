from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from spx_spark.data_platform.research.odte_level_signals import OptionTick
from spx_spark.data_platform.research.odte_signal_universe import (
    EXITS,
    MODES,
    RULE_SPECS,
    STRUCTURES,
    StructureGeometry,
    _entry_quote,
    _geometry,
    _label_exits,
    _combo_mark_path,
    explore,
    fit_thresholds,
    metrics,
    rth_sample_times,
    select_rows,
)
from spx_spark.market_calendar import ET


SESSION_DATE = date(2026, 8, 17)


def _tick(at: datetime, bid: float, ask: float) -> OptionTick:
    return OptionTick(
        at=at,
        bid=bid,
        ask=ask,
        mid=(bid + ask) / 2.0,
        source_at=at,
        delta=None,
        implied_vol=None,
    )


def _row(
    day: str,
    at: int,
    structure: str,
    pnl: float,
    *,
    premium: float = 0.4,
    spot_ret: float = 0.01,
    straddle: float = 20.0,
) -> dict[str, object]:
    return {
        "session_date": day,
        "decision_at": f"{day}T{at:02d}:00:00+00:00",
        "structure": structure,
        "premium_fraction_of_width": premium,
        "atm_straddle_ask": straddle,
        "spot_ret_15m": spot_ret,
        "hour_et": at,
        "pnl_hold_1545": pnl,
        "pnl_giveback_50": pnl,
    }


def test_rule_registry_is_preregistered_to_96_execution_hypotheses() -> None:
    assert len(RULE_SPECS) == 48
    assert len({rule.name for rule in RULE_SPECS}) == 48
    assert len(RULE_SPECS) * len(EXITS) == 96
    assert MODES == ("sampling_points", "session_first", "session_mean")


def test_rth_sampling_is_fifteen_minutes_and_ends_at_1545() -> None:
    samples = rth_sample_times(SESSION_DATE)

    assert len(samples) == 26
    assert samples[0].astimezone(ET).strftime("%H:%M") == "09:30"
    assert samples[-1].astimezone(ET).strftime("%H:%M") == "15:45"
    assert all(right - left == timedelta(minutes=15) for left, right in zip(samples, samples[1:]))


def test_credit_vertical_uses_short_bid_minus_long_ask_and_close_inverse() -> None:
    at = datetime(2026, 8, 17, 14, 0, tzinfo=timezone.utc)
    geometry = _geometry("call_credit_vertical", 6000.0)
    snapshot = {
        ("schwab", 6000.0, "C"): _tick(at, 6.0, 6.2),
        ("schwab", 6010.0, "C"): _tick(at, 2.0, 2.2),
    }

    entry, failure = _entry_quote(snapshot, geometry, decision_at=at)

    assert failure is None
    assert entry is not None
    assert entry["entry_value"] == pytest.approx(3.8)
    assert entry["entry_close_value"] == pytest.approx(4.2)
    assert entry["provider"] == "schwab"


def test_entry_rejects_cross_provider_legs_and_reports_missing_leg() -> None:
    at = datetime(2026, 8, 17, 14, 0, tzinfo=timezone.utc)
    geometry = _geometry("put_credit_vertical", 6000.0)
    snapshot = {
        ("schwab", 6000.0, "P"): _tick(at, 6.0, 6.2),
        ("ibkr", 5990.0, "P"): _tick(at, 2.0, 2.2),
    }

    entry, failure = _entry_quote(snapshot, geometry, decision_at=at)

    assert entry is None
    assert failure == "entry_missing_leg"


def test_butterfly_and_condor_entries_use_conservative_synthetic_sides() -> None:
    at = datetime(2026, 8, 17, 14, 0, tzinfo=timezone.utc)
    snapshot = {
        ("schwab", 5990.0, "C"): _tick(at, 11.8, 12.0),
        ("schwab", 6000.0, "C"): _tick(at, 5.0, 5.2),
        ("schwab", 6010.0, "C"): _tick(at, 1.0, 1.2),
        ("schwab", 5975.0, "P"): _tick(at, 0.5, 0.6),
        ("schwab", 5985.0, "P"): _tick(at, 1.0, 1.1),
        ("schwab", 6015.0, "C"): _tick(at, 1.2, 1.3),
        ("schwab", 6025.0, "C"): _tick(at, 0.4, 0.5),
    }

    fly, _ = _entry_quote(
        snapshot, _geometry("call_butterfly", 6000.0), decision_at=at
    )
    condor, _ = _entry_quote(snapshot, _geometry("iron_condor", 6000.0), decision_at=at)

    assert fly is not None and condor is not None
    assert fly["entry_value"] == pytest.approx(3.2)
    assert fly["entry_close_value"] == pytest.approx(2.4)
    assert condor["entry_value"] == pytest.approx(1.1)
    assert condor["entry_close_value"] == pytest.approx(1.5)


def test_credit_giveback_exits_when_close_debit_reaches_half_credit() -> None:
    at = datetime(2026, 8, 17, 14, 0, tzinfo=timezone.utc)
    geometry = _geometry("call_credit_vertical", 6000.0)
    entry = {"entry_value": 4.0, "entry_close_value": 4.2}
    path = [
        (at + timedelta(minutes=15), 3.0),
        (at + timedelta(minutes=30), 1.9),
        (at + timedelta(hours=1, minutes=45), 1.0),
    ]

    labels = _label_exits(
        geometry,
        entry,
        path,
        decision_at=at,
        session_date=SESSION_DATE,
    )

    assert labels is not None
    assert labels["pnl_hold_1545"] == pytest.approx(3.0)
    assert labels["pnl_giveback_50"] == pytest.approx(2.1)
    assert labels["giveback_50_exit_reason"] == "credit_close_debit_at_or_below_half_entry"


def test_combo_mark_path_tracks_every_leg_stream() -> None:
    start = datetime(2026, 8, 17, 13, 30, tzinfo=timezone.utc)
    geometry = _geometry("call_credit_vertical", 6000.0)

    class FakeStore:
        def option_series(self, *, strike: float, **_kwargs):
            if strike == 6000.0:
                return [
                    _tick(start, 6.0, 6.2),
                    _tick(start + timedelta(minutes=1), 5.0, 5.2),
                ]
            return [
                _tick(start, 2.0, 2.2),
                _tick(start + timedelta(minutes=1), 1.0, 1.2),
            ]

    path = _combo_mark_path(
        FakeStore(),  # type: ignore[arg-type]
        geometry,
        provider="schwab",
        start=start,
        end=start + timedelta(minutes=1),
    )

    assert path == [(start, 4.2), (start + timedelta(minutes=1), 4.2)]


def test_fly_giveback_reuses_management_v2_premium_stop() -> None:
    at = datetime(2026, 8, 17, 14, 0, tzinfo=timezone.utc)
    geometry = StructureGeometry(
        "call_butterfly",
        "debit",
        ((5990.0, "C"), (6000.0, "C"), (6010.0, "C")),
    )
    entry = {"entry_value": 4.0, "entry_close_value": 3.0}
    path = [
        (at + timedelta(minutes=15), 2.5),
        (at + timedelta(minutes=30), 2.0),
        (at + timedelta(hours=1, minutes=45), 5.0),
    ]

    labels = _label_exits(
        geometry,
        entry,
        path,
        decision_at=at,
        session_date=SESSION_DATE,
    )

    assert labels is not None
    assert labels["giveback_50_exit_reason"] == "premium_stop"
    assert labels["giveback_50_exit_value"] == 2.0
    assert labels["pnl_giveback_50"] == -2.0


def test_side_rules_choose_expensive_and_spot_same_credit() -> None:
    rows = [
        _row("2026-07-06", 10, "call_credit_vertical", 1.0, premium=0.6),
        _row("2026-07-06", 10, "put_credit_vertical", 1.0, premium=0.4),
    ]
    thresholds = fit_thresholds(rows)
    expensive = next(rule for rule in RULE_SPECS if rule.name == "credit_expensive_side")
    same = next(rule for rule in RULE_SPECS if rule.name == "credit_spot15_same")

    assert [row["structure"] for row in select_rows(rows, expensive, thresholds)] == [
        "call_credit_vertical"
    ]
    assert [row["structure"] for row in select_rows(rows, same, thresholds)] == [
        "put_credit_vertical"
    ]


def test_session_metrics_limit_overlap_to_one_daily_observation() -> None:
    rows = [
        _row("2026-07-06", 10, "call_credit_vertical", 4.0),
        _row("2026-07-06", 11, "call_credit_vertical", -2.0),
        _row("2026-07-07", 10, "call_credit_vertical", 2.0),
    ]

    first = metrics(rows, "session_first", "hold_1545")
    daily_mean = metrics(rows, "session_mean", "hold_1545")

    assert first["n"] == first["sessions"] == 2
    assert first["mean"] == 3.0
    assert daily_mean["mean"] == pytest.approx(1.5)


def test_holdout_values_do_not_change_train_thresholds_or_manifest() -> None:
    train_days = [date(2026, 7, 6) + timedelta(days=index) for index in range(10)]
    train = [
        _row(
            day.isoformat(),
            10,
            structure,
            1.0,
            premium=0.1 + index / 100,
            straddle=10.0 + index,
        )
        for index, day in enumerate(train_days)
        for structure in STRUCTURES
    ]
    holdout = [
        _row("2026-08-03", 10, structure, 1.0, premium=99.0, straddle=999.0)
        for structure in STRUCTURES
    ]

    report, records = explore(train + holdout)

    assert report["train_thresholds"] == fit_thresholds(train)
    assert report["base_entry_rules"] == 48
    assert report["hypotheses_tested"] == 96
    assert len(records) == 96
