from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from spx_spark.ibkr.adapter import iv_percentile_snapshot_from_bars


def _bars(count: int = 252, *, span_days: int = 365) -> list[SimpleNamespace]:
    start = date(2025, 8, 20)
    return [
        SimpleNamespace(
            date=start + timedelta(days=index * span_days / max(count - 1, 1)),
            close=0.15 + index / 10_000,
        )
        for index in range(count)
    ]


def test_daily_iv_history_calculates_rolling_percentile_and_rank() -> None:
    bars = _bars()

    snapshot = iv_percentile_snapshot_from_bars(
        bars,
        provider_symbol="AAPL",
        observed_at=datetime(2026, 8, 20, tzinfo=timezone.utc),
    )

    assert snapshot.as_of_date == bars[-1].date
    assert snapshot.ivp_13w is not None and snapshot.ivp_13w > 0.95
    assert snapshot.ivp_26w is not None and snapshot.ivp_26w > 0.95
    assert snapshot.ivp_52w is not None and snapshot.ivp_52w > 0.95
    assert snapshot.iv_rank_13w == pytest.approx(1.0)
    assert snapshot.iv_rank_26w == pytest.approx(1.0)
    assert snapshot.iv_rank_52w == pytest.approx(1.0)


def test_daily_iv_history_fails_long_lookbacks_closed_when_history_is_short() -> None:
    snapshot = iv_percentile_snapshot_from_bars(
        _bars(60, span_days=90),
        provider_symbol="NEW",
        observed_at=datetime(2026, 8, 20, tzinfo=timezone.utc),
    )

    assert snapshot.ivp_13w is not None
    assert snapshot.ivp_26w is None
    assert snapshot.ivp_52w is None


def test_daily_iv_history_ignores_invalid_values_and_accepts_ib_date_format() -> None:
    bars = _bars()
    payload = [
        {"date": row.date.strftime("%Y%m%d"), "close": row.close}
        for row in bars
    ]
    payload.extend(
        [
            {"date": "bad", "close": 0.2},
            {"date": "20260820", "close": float("nan")},
        ]
    )

    snapshot = iv_percentile_snapshot_from_bars(
        payload,
        provider_symbol="AAPL",
        observed_at=datetime(2026, 8, 20, tzinfo=timezone.utc),
    )

    assert snapshot.as_of_date == bars[-1].date
    assert snapshot.ivp_52w is not None
