from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from spx_spark.features.bar_builder import SpxBarBuilder, bar_hold

UTC = timezone.utc


def _ts(hour: int, minute: int, second: int = 0) -> datetime:
    return datetime(2026, 7, 13, hour, minute, second, tzinfo=UTC)


def test_bar_boundary_epoch_alignment() -> None:
    builder = SpxBarBuilder()
    builder.ingest(_ts(14, 0, 2), 7500.0, "ibkr")
    builder.ingest(_ts(14, 0, 57), 7501.0, "ibkr")
    assert builder._open_1m is not None
    assert builder._open_1m.bar_start == _ts(14, 0, 0)
    closed = builder.ingest(_ts(14, 1, 3), 7502.0, "ibkr")
    assert len(closed) == 1
    assert closed[0].bar_start == _ts(14, 0, 0)
    assert builder._open_1m.bar_start == _ts(14, 1, 0)


def test_bar_ohlc_and_sample_count() -> None:
    builder = SpxBarBuilder()
    prices = [7500.0, 7502.0, 7504.0, 7506.0, 7508.0, 7510.0, 7495.0, 7497.0, 7499.0, 7501.0, 7503.0, 7505.0]
    start = _ts(14, 0, 0)
    for index, price in enumerate(prices):
        builder.ingest(start + timedelta(seconds=5 * index + 2), price, "ibkr")
    builder.ingest(_ts(14, 1, 3), 7506.0, "ibkr")
    bar = builder.closed_bars_1m()[-1]
    assert bar.open == 7500.0
    assert bar.high == 7510.0
    assert bar.low == 7495.0
    assert bar.close == 7505.0
    assert bar.sample_count == 12
    assert bar.quality == "ok"


def test_partial_bar_flagged() -> None:
    builder = SpxBarBuilder()
    start = _ts(14, 0, 0)
    for index in range(4):
        builder.ingest(start + timedelta(seconds=5 * index), 7500.0 + index, "ibkr")
    builder.ingest(_ts(14, 1, 1), 7505.0, "ibkr")
    bar = builder.closed_bars_1m()[-1]
    assert bar.quality == "partial"
    assert bar_hold(builder.closed_bars_1m(), 7490.0, "above", 1) is False


def test_empty_minute_creates_gap_not_bar() -> None:
    builder = SpxBarBuilder()
    start = _ts(14, 0, 0)
    for index in range(6):
        builder.ingest(start + timedelta(seconds=5 * index), 7500.0 + index, "ibkr")
    builder.ingest(_ts(14, 2, 1), 7507.0, "ibkr")
    builder.ingest(_ts(14, 3, 1), 7508.0, "ibkr")
    bars = builder.closed_bars_1m()
    assert [bar.bar_start for bar in bars] == [_ts(14, 0, 0), _ts(14, 2, 0)]
    assert bars[1].gap_before is True
    assert bar_hold(bars, 7490.0, "above", 2) is False


def test_five_minute_bars_from_closed_one_minute_bars() -> None:
    builder = SpxBarBuilder()
    for minute in range(6):
        start = _ts(14, minute, 0)
        for sample in range(6):
            builder.ingest(start + timedelta(seconds=5 * sample), 7500.0 + minute + sample * 0.1, "ibkr")
        builder.ingest(_ts(14, minute + 1, 1), 7500.0 + minute + 1.0, "ibkr")
    five = builder.closed_bars_5m()[-1]
    one_minute = builder.closed_bars_1m()
    assert five.bar_start == _ts(14, 0, 0)
    assert five.open == one_minute[0].open
    assert five.high == max(bar.high for bar in one_minute[:5])
    assert five.low == min(bar.low for bar in one_minute[:5])
    assert five.close == one_minute[4].close
    assert five.quality == "ok"

    partial_builder = SpxBarBuilder()
    for minute in range(5):
        start = _ts(15, minute, 0)
        for sample in range(4):
            partial_builder.ingest(
                start + timedelta(seconds=5 * sample), 7500.0 + minute, "ibkr"
            )
        partial_builder.ingest(_ts(15, minute + 1, 1), 7500.0 + minute, "ibkr")
    partial_builder.ingest(_ts(15, 6, 1), 7506.0, "ibkr")
    partial_five = partial_builder.closed_bars_5m()[-1]
    assert partial_five.quality == "partial"


def test_latest_bars_persisted_atomically(tmp_path: Path) -> None:
    builder = SpxBarBuilder()
    start = _ts(14, 0, 0)
    for index in range(6):
        builder.ingest(start + timedelta(seconds=5 * index), 7500.0 + index, "ibkr")
    closed = builder.ingest(_ts(14, 1, 1), 7506.0, "ibkr")
    as_of = _ts(14, 1, 1)
    builder.persist(tmp_path, as_of=as_of, trading_date="2026-07-13")

    latest = json.loads((tmp_path / "latest" / "spx_bars_1m.json").read_text(encoding="utf-8"))
    assert latest["schema_version"] == "spx_bars.v0.1"
    assert latest["interval_seconds"] == 60
    assert len(latest["bars"]) == len(builder.closed_bars_1m())

    lake_lines = (tmp_path / "lake" / "steven" / "bars" / "date=2026-07-13" / "spx_bars_1m.jsonl").read_text(
        encoding="utf-8"
    ).strip().splitlines()
    assert len(lake_lines) == len(closed)
