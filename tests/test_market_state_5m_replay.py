from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

from scripts.build_market_state_5m_replay import (
    SECTOR_INSTRUMENTS,
    QuotePoint,
    _bars_from_samples,
    _market_samples,
    build_completed_baseline_state,
    build_prior_session_baselines,
    build_replay_report,
    discover_quote_files,
    load_live_quote_points,
    parse_args,
    write_report,
)


ET = ZoneInfo("America/New_York")
UTC = timezone.utc


def _bar(day: date, *, end_minute: int, low: float, high: float) -> dict[str, object]:
    end = datetime.combine(day, time(9, end_minute), tzinfo=ET).astimezone(UTC)
    start = end - timedelta(minutes=5)
    return {
        "bar_start": start.isoformat(),
        "bar_end": end.isoformat(),
        "interval_seconds": 300,
        "open": low,
        "high": high,
        "low": low,
        "close": high,
        "sample_count": 20,
        "max_sample_gap_seconds": 15.0,
        "quality": "ok",
        "gap_before": False,
        "provider": "schwab",
        "segment": "rth",
        "trading_date_et": day.isoformat(),
    }


def _weekdays(start: date, count: int) -> list[date]:
    result: list[date] = []
    candidate = start
    while len(result) < count:
        if candidate.weekday() < 5:
            result.append(candidate)
        candidate += timedelta(days=1)
    return result


def test_prior_session_baseline_excludes_current_and_future_sessions() -> None:
    days = _weekdays(date(2026, 6, 1), 12)
    bars = {
        day: [_bar(day, end_minute=35, low=100.0, high=101.0 + index)]
        for index, day in enumerate(days)
    }

    snapshots = build_prior_session_baselines(bars, session_days=days)
    day_eleven_rows = snapshots[days[10]]["slots"]["09:35"]

    assert len(day_eleven_rows) == 10
    assert {row["trading_date_et"] for row in day_eleven_rows} == {
        day.isoformat() for day in days[:10]
    }
    assert all(
        date.fromisoformat(row["trading_date_et"]) < days[10]
        for row in day_eleven_rows
    )
    assert days[10].isoformat() not in {
        row["trading_date_et"] for row in day_eleven_rows
    }
    assert days[11].isoformat() not in {
        row["trading_date_et"] for row in day_eleven_rows
    }


def test_quote_lake_reader_supports_hour_partitions_and_filters_live_rows(
    tmp_path: Path,
) -> None:
    partition = (
        tmp_path
        / "schema=v1/date=2026-07-23/provider=schwab/hour=13"
    )
    partition.mkdir(parents=True)
    parquet = partition / "quotes.parquet"
    connection = duckdb.connect()
    try:
        connection.execute(
            """
            CREATE TABLE quotes AS
            SELECT *
            FROM (
                VALUES
                    (
                        'schwab', 'future:ES', 'live',
                        TIMESTAMPTZ '2026-07-23 13:30:01+00',
                        TIMESTAMPTZ '2026-07-23 13:30:02+00',
                        7450.25, NULL, NULL, NULL, NULL, NULL, NULL, 1000.0
                    ),
                    (
                        'schwab', 'equity:XLB', 'live',
                        TIMESTAMPTZ '2026-07-23 13:30:03+00',
                        TIMESTAMPTZ '2026-07-23 13:30:04+00',
                        50.25, NULL, NULL, NULL, NULL, NULL, NULL, 2000.0
                    ),
                    (
                        'schwab', 'equity:XLC', 'stale',
                        TIMESTAMPTZ '2026-07-23 13:30:03+00',
                        TIMESTAMPTZ '2026-07-23 13:30:04+00',
                        90.0, NULL, NULL, NULL, NULL, NULL, NULL, 3000.0
                    ),
                    (
                        'schwab', 'equity:SPY', 'live',
                        TIMESTAMPTZ '2026-07-23 13:30:03+00',
                        TIMESTAMPTZ '2026-07-23 13:30:04+00',
                        700.0, NULL, NULL, NULL, NULL, NULL, NULL, 4000.0
                    )
            ) AS rows(
                provider, instrument_id, quality, source_at, received_at,
                effective_price, mark, mid, last, close, bid, ask, volume
            )
            """
        )
        connection.execute(
            "COPY quotes TO ? (FORMAT PARQUET)",
            [str(parquet)],
        )
    finally:
        connection.close()

    files = discover_quote_files(tmp_path)
    points = load_live_quote_points(files)

    assert files == [parquet]
    assert [(point.instrument_id, point.price) for point in points] == [
        ("future:ES", 7450.25),
        ("equity:XLB", 50.25),
    ]


def _session_points(
    day: date,
    *,
    include_sectors: bool,
    future_jump: float = 0.0,
) -> list[QuotePoint]:
    points: list[QuotePoint] = []
    start = datetime.combine(day, time(8), tzinfo=ET)
    end = datetime.combine(day, time(16), tzinfo=ET)
    step = timedelta(seconds=15)
    current = start
    index = 0
    while current < end:
        elapsed = (current - start).total_seconds()
        jump = future_jump if current.time() >= time(10, 5) else 0.0
        source_at = current.astimezone(UTC)
        points.append(
            QuotePoint(
                provider="schwab",
                instrument_id="future:ES",
                source_at=source_at,
                received_at=source_at + timedelta(milliseconds=200),
                price=7400.0 + elapsed / 750.0 + jump,
                volume=100_000.0 + index * 25.0,
            )
        )
        current += step
        index += 1

    if include_sectors:
        sector_start = datetime.combine(day, time(9, 30), tzinfo=ET)
        current = sector_start
        sector_index = 0
        while current < end:
            source_at = current.astimezone(UTC)
            for instrument_offset, instrument_id in enumerate(SECTOR_INSTRUMENTS):
                points.append(
                    QuotePoint(
                        provider="schwab",
                        instrument_id=instrument_id,
                        source_at=source_at,
                        received_at=source_at + timedelta(milliseconds=250),
                        price=50.0 + instrument_offset + sector_index * 0.01,
                        volume=10_000.0 + sector_index * 100.0,
                    )
                )
            current += timedelta(seconds=30)
            sector_index += 1
    return points


def _observation_at(
    report: dict[str, object],
    *,
    local_clock: str,
) -> dict[str, object]:
    return next(
        row
        for row in report["observations"]
        if datetime.fromisoformat(row["as_of_et"]).strftime("%H:%M") == local_clock
    )


def test_replay_is_causal_and_attaches_forward_paths_only_after_scoring() -> None:
    days = _weekdays(date(2026, 6, 1), 11)
    history = [
        point
        for day in days[:10]
        for point in _session_points(day, include_sectors=False)
    ]
    ordinary = [
        *history,
        *_session_points(days[10], include_sectors=True),
    ]
    jumped = [
        *history,
        *_session_points(days[10], include_sectors=True, future_jump=100.0),
    ]

    ordinary_report = build_replay_report(
        ordinary,
        start_date=days[10],
        end_date=days[10],
    )
    jumped_report = build_replay_report(
        jumped,
        start_date=days[10],
        end_date=days[10],
    )
    ordinary_1000 = _observation_at(ordinary_report, local_clock="10:00")
    jumped_1000 = _observation_at(jumped_report, local_clock="10:00")

    assert ordinary_1000["input_status"] == "ready"
    assert ordinary_1000["state"] == "TREND_UP"
    assert ordinary_1000["inputs"] == jumped_1000["inputs"]
    assert ordinary_1000["state"] == jumped_1000["state"]
    assert ordinary_1000["forward_es"]["15m"]["evaluation_only"] is True
    assert (
        ordinary_1000["forward_es"]["15m"]["endpoint_points"]
        != jumped_1000["forward_es"]["15m"]["endpoint_points"]
    )
    assert ordinary_report["model"]["production_thresholds_overridden"] is False
    assert ordinary_report["methodology"]["production_state_written"] is False
    assert ordinary_report["state_counts"]["TREND_UP"] > 0
    assert ordinary_report["daily_coverage"][0]["sector_instruments_present_count"] == 11
    assert len(ordinary_report["observations"]) == 75
    assert all(
        datetime.fromisoformat(row["as_of_et"]).time() < time(16)
        for row in ordinary_report["observations"]
    )


def test_bar_edge_gap_is_partial_and_sector_source_times_are_not_aligned() -> None:
    day = date(2026, 7, 23)
    bar_start = datetime.combine(day, time(9, 30), tzinfo=ET).astimezone(UTC)
    late_samples: list[tuple[datetime, QuotePoint]] = []
    for index in range(20):
        source_at = bar_start + timedelta(seconds=31 + index * 10)
        point = QuotePoint(
            provider="schwab",
            instrument_id="future:ES",
            source_at=source_at,
            received_at=source_at + timedelta(milliseconds=100),
            price=7450.0 + index,
            volume=1000.0 + index,
        )
        late_samples.append((point.received_at, point))

    bars = _bars_from_samples(late_samples)

    assert bars[0]["quality"] == "partial"
    assert bars[0]["leading_edge_gap_seconds"] == 31.0
    assert bars[0]["contract_identity"] == "replay:future:ES:2026-07-23"

    tick = datetime.combine(day, time(9, 31), tzinfo=ET).astimezone(UTC)
    sector_points = [
        QuotePoint(
            provider="schwab",
            instrument_id="equity:XLB",
            source_at=tick - timedelta(seconds=3),
            received_at=tick - timedelta(seconds=2),
            price=50.0,
            volume=1000.0,
        ),
        QuotePoint(
            provider="schwab",
            instrument_id="equity:XLC",
            source_at=tick - timedelta(seconds=37),
            received_at=tick - timedelta(seconds=36),
            price=90.0,
            volume=2000.0,
        ),
    ]
    first_sample = _market_samples(sector_points, trading_day=day)[0]

    assert (
        first_sample["instruments"]["equity:XLB"]["source_at"]
        == sector_points[0].source_at.isoformat()
    )
    assert (
        first_sample["instruments"]["equity:XLC"]["source_at"]
        == sector_points[1].source_at.isoformat()
    )


def test_completed_baseline_is_production_compatible_and_has_no_future_rows() -> None:
    days = _weekdays(date(2026, 5, 1), 23)
    bars = {
        day: [_bar(day, end_minute=35, low=100.0, high=101.0 + index)]
        for index, day in enumerate(days)
    }

    completed = build_completed_baseline_state(
        bars,
        through_date=days[21],
        session_days=days,
    )
    rows = completed["slots"]["09:35"]

    assert completed["schema_version"] == "market_state_5m_range_baselines.v1"
    assert completed["target_sessions"] == 20
    assert len(rows) == 21
    assert rows[-1]["trading_date_et"] == days[21].isoformat()
    assert all(
        date.fromisoformat(row["trading_date_et"]) <= days[21]
        for row in rows
    )
    assert days[22].isoformat() not in {
        row["trading_date_et"] for row in rows
    }


def test_cli_accepts_optional_baseline_output(tmp_path: Path) -> None:
    args = parse_args(
        [
            "--output",
            str(tmp_path / "replay.json"),
            "--baseline-output",
            str(tmp_path / "baseline.json"),
        ]
    )

    assert args.baseline_output == tmp_path / "baseline.json"


def test_write_report_uses_the_requested_path(tmp_path: Path) -> None:
    output = tmp_path / "chosen/report.json"
    payload = {"schema_version": "test", "value": 1.23}

    write_report(payload, output)

    assert json.loads(output.read_text(encoding="utf-8")) == payload
