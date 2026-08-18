from __future__ import annotations

from datetime import date, timedelta

import pytest

from spx_spark.data_platform.research.odte_ev_explore import (
    MODES,
    RULE_SPECS,
    explore,
    fit_thresholds,
    metrics,
    select_rows,
)


def _row(day: str, at: int, direction: str, pnl: float, **changes):
    row = {
        "session_date": day,
        "decision_at": f"{day}T{at:02d}:00:00+00:00",
        "session_mode": "rth",
        "direction": direction,
        "pnl_hold_to_1545": pnl,
        "debit_fraction_of_width": 0.4 if direction == "call" else 0.6,
        "atm_straddle_mid": 20.0,
        "iv_skew": 0.0,
        "quote_spread_fraction": 0.04,
        "spot_ret_5m": 0.01,
        "spot_ret_15m": 0.01,
        "spot_ret_60m": 0.01,
        "minutes_to_close": 100.0,
        "hour_et": at,
    }
    row.update(changes)
    return row


def test_rule_registry_is_frozen_to_114_execution_hypotheses() -> None:
    assert len(RULE_SPECS) == 38
    assert len({rule["name"] for rule in RULE_SPECS}) == 38
    assert len(RULE_SPECS) * len(MODES) == 114


def test_one_side_rules_choose_cheaper_and_return_direction() -> None:
    rows = [_row("2026-07-06", 10, "call", 2.0), _row("2026-07-06", 10, "put", -2.0)]
    thresholds = fit_thresholds(rows)
    cheaper = next(rule for rule in RULE_SPECS if rule["name"] == "cheaper")
    reverse = next(rule for rule in RULE_SPECS if rule["name"] == "ret_5m_reverse")

    assert [row["direction"] for row in select_rows(rows, cheaper, thresholds)] == ["call"]
    assert [row["direction"] for row in select_rows(rows, reverse, thresholds)] == ["put"]


def test_session_metrics_do_not_treat_overlapping_bars_as_independent() -> None:
    rows = [
        _row("2026-07-06", 10, "call", 4.0),
        _row("2026-07-06", 11, "call", -2.0),
        _row("2026-07-07", 10, "call", 2.0),
    ]

    first = metrics(rows, "session_first")
    daily_mean = metrics(rows, "session_mean")

    assert first["n"] == first["sessions"] == 2
    assert first["mean"] == 3.0
    assert daily_mean["mean"] == pytest.approx(1.5)


def test_holdout_values_do_not_change_train_quantile_boundaries() -> None:
    train_days = [date(2026, 7, 6) + timedelta(days=index) for index in range(20)]
    train = [
        _row(day.isoformat(), 10, direction, 1.0, debit_fraction_of_width=0.01 * index)
        for index, day in enumerate(train_days)
        for direction in ("call", "put")
    ]
    holdout = [
        _row("2026-08-03", 10, direction, 1.0, debit_fraction_of_width=999.0)
        for direction in ("call", "put")
    ]

    report, records = explore(train + holdout)

    assert report["train_thresholds"] == fit_thresholds(train)
    assert len(records) == 114
    assert all(record["train_thresholds"] == fit_thresholds(train) for record in records)
