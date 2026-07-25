from __future__ import annotations

import runpy
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any

from spx_spark.config import NY_TZ


SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "build_market_state_5m_ibkr_history.py"
)


def _script() -> dict[str, Any]:
    return runpy.run_path(str(SCRIPT))


def _rth_rows(
    trading_day: date,
    *,
    first_close: float,
    contract_identity: str = "future:ES:20260918",
) -> list[dict[str, object]]:
    start = datetime.combine(trading_day, time(9, 30), tzinfo=NY_TZ)
    rows: list[dict[str, object]] = []
    for index in range(78):
        bar_start = start + timedelta(minutes=5 * index)
        close = first_close + index * 0.2
        rows.append(
            {
                "bar_start": bar_start.isoformat(),
                "bar_end": (bar_start + timedelta(minutes=5)).isoformat(),
                "interval_seconds": 300,
                "open": close - 0.1,
                "high": close + 0.5,
                "low": close - 0.5,
                "close": close,
                "volume": 1000.0,
                "wap": close,
                "quality": "ok",
                "gap_before": False,
                "segment": "rth",
                "trading_date_et": trading_day.isoformat(),
                "contract_identity": contract_identity,
            }
        )
    return rows


def test_score_day_ma_history_is_cross_session_and_causal() -> None:
    namespace = _script()
    score_day = namespace["_score_day"]
    days = [date(2026, 7, day) for day in (20, 21, 22, 23)]
    by_day = {
        day: _rth_rows(day, first_close=7400.0 + offset * 15.6)
        for offset, day in enumerate(days)
    }
    history = [row for day in days for row in by_day[day]]
    current = by_day[days[-1]]

    observations = score_day(
        days[-1],
        es_rows=current,
        es_history_rows=history,
        sectors={},
        ranges={},
    )
    at_ten = next(
        row for row in observations if str(row["as_of_et"])[11:16] == "10:05"
    )
    moving = at_ten["moving_average_diagnostics"]

    assert moving["status"] == "ready"
    assert moving["regime_state"] == "TREND_EXTENDED"
    assert moving["regime_direction"] == "up"
    assert moving["rth_bar_count"] == 241
    assert moving["atr_5m_source"] == "shared_session_aware_rth_atr"
    assert moving["atr_5m_overnight_gap_included"] is False
    assert "excludes_overnight_gap" in str(moving["atr_5m_method"])
    assert datetime.fromisoformat(str(moving["latest_bar_end"])) == datetime.fromisoformat(
        str(at_ten["as_of"])
    )

    changed_history = [dict(row) for row in history]
    cutoff = datetime.fromisoformat(str(at_ten["as_of"]))
    for row in changed_history:
        if datetime.fromisoformat(str(row["bar_end"])) > cutoff:
            row["close"] = float(row["close"]) + 1000.0
    changed_current = changed_history[-78:]
    changed = score_day(
        days[-1],
        es_rows=changed_current,
        es_history_rows=changed_history,
        sectors={},
        ranges={},
    )
    changed_at_ten = next(
        row for row in changed if str(row["as_of_et"])[11:16] == "10:05"
    )

    assert (
        changed_at_ten["moving_average_diagnostics"]
        == at_ten["moving_average_diagnostics"]
    )


def test_ma_summaries_group_regime_and_orient_directional_returns() -> None:
    namespace = _script()
    rows = [
        {
            "moving_average_diagnostics": {
                "regime_state": "TREND_ALIGNED",
                "regime_direction": "up",
            },
            "forward_es": {
                "15m": {"endpoint_points": 2.0},
                "30m": {"endpoint_points": 3.0},
                "60m": {"endpoint_points": 4.0},
            },
        },
        {
            "moving_average_diagnostics": {
                "regime_state": "REGIME_TRANSITION",
                "regime_direction": "down",
            },
            "forward_es": {
                "15m": {"endpoint_points": -1.0},
                "30m": {"endpoint_points": 2.0},
                "60m": {"endpoint_points": -3.0},
            },
        },
        {
            "moving_average_diagnostics": {
                "regime_state": "MIXED",
                "regime_direction": None,
            },
            "forward_es": {
                "15m": {"endpoint_points": 1.0},
                "30m": {"endpoint_points": 1.0},
                "60m": {"endpoint_points": 1.0},
            },
        },
    ]

    counts = namespace["_ma_regime_direction_counts"](rows)
    summary = namespace["_ma_forward_summary"](rows)
    down_15m = next(
        row
        for row in summary
        if row["regime_state"] == "REGIME_TRANSITION"
        and row["horizon_minutes"] == 15
    )
    mixed_15m = next(
        row
        for row in summary
        if row["regime_state"] == "MIXED" and row["horizon_minutes"] == 15
    )

    assert counts == [
        {
            "regime_state": "MIXED",
            "regime_direction": "none",
            "count": 1,
        },
        {
            "regime_state": "REGIME_TRANSITION",
            "regime_direction": "down",
            "count": 1,
        },
        {
            "regime_state": "TREND_ALIGNED",
            "regime_direction": "up",
            "count": 1,
        },
    ]
    assert down_15m["mean_endpoint_points"] == -1.0
    assert down_15m["mean_directional_points"] == 1.0
    assert down_15m["directional_hit_rate"] == 1.0
    assert mixed_15m["mean_directional_points"] is None
    assert mixed_15m["directional_hit_rate"] is None
