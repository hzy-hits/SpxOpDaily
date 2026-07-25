from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from spx_spark.application.market_features.market_state_5m import (
    TREND_UP,
    score_market_state_5m,
)
from spx_spark.application.market_features.market_state_5m_inputs import (
    SECTOR_INSTRUMENTS,
    _bar_vwaps,
    _es_vwap_series,
    _provider_vwap_series,
    build_market_state_5m_inputs,
    update_same_time_range_baselines,
)
from spx_spark.market_calendar import ET


DAY = datetime(2026, 7, 24, 10, 0, tzinfo=ET)


def bar(
    start: datetime,
    *,
    open_: float,
    high: float,
    low: float,
    close: float,
    segment: str,
    quality: str = "ok",
) -> dict[str, object]:
    return {
        "bar_start": start.isoformat(),
        "bar_end": (start + timedelta(minutes=5)).isoformat(),
        "interval_seconds": 300,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "sample_count": 60,
        "max_sample_gap_seconds": 5.0,
        "quality": quality,
        "gap_before": False,
        "provider": "schwab",
        "segment": segment,
        "trading_date_et": start.date().isoformat(),
    }


def trending_bars() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    premarket = DAY.replace(hour=8, minute=15)
    price = 7380.0
    for index in range(15):
        start = premarket + timedelta(minutes=5 * index)
        rows.append(
            bar(
                start,
                open_=price,
                high=price + 2.0,
                low=price - 1.0,
                close=price + 1.0,
                segment="us_premarket",
            )
        )
        price += 1.0
    rth = DAY.replace(hour=9, minute=30)
    for index in range(6):
        start = rth + timedelta(minutes=5 * index)
        base = 7400.0 + index * 5.0
        rows.append(
            bar(
                start,
                open_=base,
                high=base + 5.0,
                low=base - 1.0,
                close=base + 4.0,
                segment="rth",
            )
        )
    return rows


def market_samples(
    *,
    include_sectors: bool = True,
    es_provider: str = "schwab",
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    start = DAY.replace(hour=9, minute=30)
    for index in range(31):
        at = start + timedelta(minutes=index)
        es_price = 7400.0 + index
        instruments: dict[str, object] = {
            "future:ES": {
                "price": es_price,
                "volume": 100_000.0 + index * 100.0,
                "provider": es_provider,
                "source_at": at.isoformat(),
                "quality": "live",
            }
        }
        if include_sectors:
            for sector_index, instrument_id in enumerate(SECTOR_INSTRUMENTS):
                instruments[instrument_id] = {
                    "price": 100.0 + sector_index + index * 0.1,
                    "volume": 10_000.0 + index * 50.0,
                    "provider": "schwab",
                    "source_at": at.isoformat(),
                    "quality": "live",
                }
        rows.append(
            {
                "at": at.isoformat(),
                "session_id": "2026-07-24",
                "segment": "rth",
                "instruments": instruments,
                "es_by_provider": {es_provider: instruments["future:ES"]},
            }
        )
    return rows


def baselines(value: float = 31.0, count: int = 20) -> dict[str, object]:
    return {
        "schema_version": "market_state_5m_range_baselines.v1",
        "slots": {
            "10:00": [
                {
                    "trading_date_et": f"2026-06-{index + 1:02d}",
                    "range_points": value,
                }
                for index in range(count)
            ]
        },
    }


def test_derives_all_eight_inputs_and_scores_clean_trend_up() -> None:
    derived = build_market_state_5m_inputs(
        bars=trending_bars(),
        market_samples=market_samples(),
        range_baselines=baselines(),
        now=DAY,
    )

    assert derived["status"] == "ready"
    assert derived["available_count"] == 8
    values = derived["values"]
    assert values["price_vs_vwap"] == "ABOVE_CONFIRMED"
    assert values["vwap_slope"] > 0.30
    assert values["opening_range_state"] == "ABOVE_ORH_CONFIRMED"
    assert values["market_structure"] == "HH_HL"
    assert values["efficiency_ratio"] == pytest.approx(1.0)
    assert values["vwap_cross_count"] == 0
    assert values["same_time_range_ratio"] == pytest.approx(1.0)
    assert values["breadth_above_vwap"] == pytest.approx(1.0)

    scored = score_market_state_5m(now=DAY, **values)
    assert scored["D"] == 10
    assert scored["state"] == TREND_UP
    assert scored["action_authority"] == "none"
    assert scored["actionable"] is False


def test_missing_breadth_and_range_history_are_not_filled_as_neutral() -> None:
    derived = build_market_state_5m_inputs(
        bars=trending_bars(),
        market_samples=market_samples(include_sectors=False),
        range_baselines=baselines(count=9),
        now=DAY,
    )

    assert derived["status"] == "incomplete"
    assert "breadth_above_vwap" in derived["missing"]
    assert "same_time_range_ratio" in derived["missing"]
    assert derived["diagnostics"]["same_time_range"]["status"] == "warming"
    assert derived["diagnostics"]["breadth"]["usable_count"] == 0


def test_provider_that_joins_late_cannot_fake_session_vwap() -> None:
    samples = market_samples()
    for row in samples[16:]:
        quote = dict(row["instruments"]["future:ES"])
        quote["provider"] = "ibkr"
        row["es_by_provider"]["ibkr"] = quote

    derived = build_market_state_5m_inputs(
        bars=trending_bars(),
        market_samples=samples,
        range_baselines=baselines(),
        now=DAY,
    )

    assert derived["diagnostics"]["vwap"]["provider"] == "schwab"
    assert derived["diagnostics"]["vwap"]["first_at"].startswith("2026-07-24T09:30")


def test_fresh_provider_beats_a_denser_but_stale_provider() -> None:
    start = DAY.replace(hour=9, minute=30)
    samples: list[dict[str, object]] = []
    for index in range(31):
        at = start + timedelta(minutes=index)
        providers: dict[str, object] = {}
        if index <= 20:
            providers["schwab"] = {
                "price": 7400.0 + index,
                "volume": 100_000.0 + index * 100.0,
                "source_at": at.isoformat(),
            }
        if index % 2 == 0:
            providers["ibkr"] = {
                "price": 7400.0 + index,
                "volume": 100_000.0 + index * 100.0,
                "source_at": at.isoformat(),
            }
        samples.append(
            {
                "at": at.isoformat(),
                "segment": "rth",
                "es_by_provider": providers,
            }
        )

    series, diagnostics = _es_vwap_series(
        samples,
        trading_date=DAY.date(),
        now=DAY,
    )

    assert series
    assert diagnostics["provider"] == "ibkr"
    assert datetime.fromisoformat(str(diagnostics["last_at"])) == DAY


def test_opening_range_rejects_intrabar_spike_that_closes_inside() -> None:
    bars = trending_bars()
    rth = [row for row in bars if row["segment"] == "rth"]
    rth[-2]["high"] = 7500.0
    rth[-2]["close"] = 7410.0
    rth[-1]["close"] = 7412.0

    derived = build_market_state_5m_inputs(
        bars=bars,
        market_samples=market_samples(),
        range_baselines=baselines(),
        now=DAY,
    )

    assert derived["values"]["opening_range_state"] == "INSIDE"


def test_same_time_baseline_updates_current_session_once_per_slot() -> None:
    first = update_same_time_range_baselines(
        baselines(),
        bars=trending_bars(),
        now=DAY,
    )
    second = update_same_time_range_baselines(
        first,
        bars=trending_bars(),
        now=DAY,
    )

    rows = second["slots"]["10:00"]
    assert len(rows) == 21
    current = [row for row in rows if row["trading_date_et"] == "2026-07-24"]
    assert len(current) == 1
    assert current[0]["source"] == "live_es_5m_ohlc"


def test_future_dated_quotes_and_range_rows_cannot_leak_into_state() -> None:
    samples = market_samples()
    for row in samples:
        future_at = DAY + timedelta(days=1)
        for quote in row["instruments"].values():
            quote["source_at"] = future_at.isoformat()
        row["es_by_provider"]["schwab"]["source_at"] = future_at.isoformat()
    future_rows = [
        {
            "trading_date_et": f"2026-08-{index + 1:02d}",
            "range_points": 1.0,
        }
        for index in range(20)
    ]
    prior_rows = [
        {
            "trading_date_et": f"2026-06-{index + 1:02d}",
            "range_points": 31.0,
        }
        for index in range(9)
    ]

    derived = build_market_state_5m_inputs(
        bars=trending_bars(),
        market_samples=samples,
        range_baselines={
            "schema_version": "market_state_5m_range_baselines.v1",
            "slots": {"10:00": [*prior_rows, *future_rows]},
        },
        now=DAY,
    )

    assert derived["values"]["price_vs_vwap"] is None
    assert derived["values"]["breadth_above_vwap"] is None
    assert derived["values"]["same_time_range_ratio"] is None
    assert derived["diagnostics"]["same_time_range"]["sample_count"] == 9


def test_quote_exactly_at_bar_end_belongs_to_next_bar() -> None:
    target = trending_bars()[-1]
    end = datetime.fromisoformat(str(target["bar_end"]))
    mapped = _bar_vwaps(
        [target],
        [
            (end - timedelta(seconds=1), 7400.0),
            (end, 9999.0),
        ],
    )

    assert mapped[str(target["bar_start"])] == 7400.0


def test_missing_middle_bar_is_not_compressed_into_a_thirty_minute_window() -> None:
    bars = [
        row
        for row in trending_bars()
        if not (
            row["segment"] == "rth"
            and datetime.fromisoformat(str(row["bar_start"])).time()
            == datetime.strptime("09:45", "%H:%M").time()
        )
    ]

    derived = build_market_state_5m_inputs(
        bars=bars,
        market_samples=market_samples(),
        range_baselines=baselines(),
        now=DAY,
    )

    assert derived["values"]["market_structure"] is None
    assert derived["values"]["efficiency_ratio"] is None
    assert derived["values"]["vwap_cross_count"] is None
    assert derived["values"]["same_time_range_ratio"] is None
    assert (
        derived["diagnostics"]["same_time_range"]["reason"]
        == "rth_bars_not_continuous_from_open"
    )


def test_sector_breadth_rejects_cross_section_timestamp_skew() -> None:
    samples = market_samples()
    for instrument_id in SECTOR_INSTRUMENTS[:8]:
        samples[-1]["instruments"][instrument_id]["source_at"] = (
            DAY - timedelta(seconds=50)
        ).isoformat()

    derived = build_market_state_5m_inputs(
        bars=trending_bars(),
        market_samples=samples,
        range_baselines=baselines(),
        now=DAY,
    )

    assert derived["values"]["breadth_above_vwap"] is None
    assert (
        derived["diagnostics"]["breadth"]["reason"]
        == "sector_cross_section_timestamp_skew"
    )
    assert derived["diagnostics"]["breadth"]["cross_section_skew_seconds"] == 50.0


def test_vwap_accepts_two_minute_snapshot_gap_with_source_jitter() -> None:
    start = DAY.replace(hour=9, minute=30)
    points = [
        (start, 10_000.0, 100.0),
        (start + timedelta(seconds=60), 10_100.0, 101.0),
        (start + timedelta(seconds=187.8), 10_300.0, 103.0),
    ]

    series, diagnostics = _provider_vwap_series(
        points,
        trading_date=DAY.date(),
    )

    assert len(series) == 2
    assert diagnostics["max_gap_seconds"] == pytest.approx(127.8)


def test_vwap_sampling_gap_is_unavailable_then_recovers_from_fresh_delta() -> None:
    start = DAY.replace(hour=9, minute=30)
    before_gap = start + timedelta(minutes=1)
    gap_at = start + timedelta(minutes=4)
    recovered_at = start + timedelta(minutes=5)
    points = [
        (start, 10_000.0, 100.0),
        (before_gap, 11_000.0, 101.0),
        (gap_at, 11_100.0, 104.0),
        (recovered_at, 11_200.0, 105.0),
    ]

    series, diagnostics = _provider_vwap_series(
        points,
        trading_date=DAY.date(),
    )
    gap_bar_vwap = _bar_vwaps(
        [
            {
                "bar_start": (gap_at - timedelta(minutes=5)).isoformat(),
                "bar_end": (gap_at + timedelta(seconds=1)).isoformat(),
            }
        ],
        series,
    )
    recovered_bar_vwap = _bar_vwaps(
        [
            {
                "bar_start": gap_at.isoformat(),
                "bar_end": (recovered_at + timedelta(seconds=1)).isoformat(),
            }
        ],
        series,
    )

    assert [at for at, _ in series] == [before_gap, recovered_at]
    assert gap_bar_vwap == {}
    assert recovered_bar_vwap[gap_at.isoformat()] == pytest.approx(101.363636)
    assert diagnostics["gap_count"] == 1
    assert diagnostics["reset_count"] == 0
    assert diagnostics["observed_volume"] == pytest.approx(1_100.0)
    assert diagnostics["skipped_cross_gap_volume"] == pytest.approx(100.0)
    assert diagnostics["observed_volume_ratio"] == pytest.approx(11 / 12)
    assert diagnostics["minimum_observed_volume_ratio"] == 0.80
    assert diagnostics["partial_observed_volume"] is True
    assert diagnostics["volume_coverage"] == "partial_observed_deltas"


def test_vwap_sampling_gap_stays_unavailable_below_observed_volume_threshold() -> None:
    start = DAY.replace(hour=9, minute=30)
    before_gap = start + timedelta(minutes=1)
    gap_at = start + timedelta(minutes=4)
    after_gap_one = start + timedelta(minutes=5)
    after_gap_two = start + timedelta(minutes=6)
    points = [
        (start, 10_000.0, 100.0),
        (before_gap, 10_100.0, 101.0),
        (gap_at, 11_000.0, 104.0),
        (after_gap_one, 11_100.0, 105.0),
        (after_gap_two, 11_200.0, 106.0),
    ]

    series, diagnostics = _provider_vwap_series(
        points,
        trading_date=DAY.date(),
    )
    post_gap_bar_vwap = _bar_vwaps(
        [
            {
                "bar_start": gap_at.isoformat(),
                "bar_end": (after_gap_two + timedelta(seconds=1)).isoformat(),
            }
        ],
        series,
    )

    assert [at for at, _ in series] == [before_gap]
    assert post_gap_bar_vwap == {}
    assert diagnostics["observed_volume"] == pytest.approx(300.0)
    assert diagnostics["skipped_cross_gap_volume"] == pytest.approx(900.0)
    assert diagnostics["observed_volume_ratio"] == pytest.approx(0.25)
    assert diagnostics["minimum_observed_volume_ratio"] == 0.80


def test_vwap_volume_reset_skips_reset_delta_then_recovers() -> None:
    start = DAY.replace(hour=9, minute=30)
    before_reset = start + timedelta(minutes=1)
    reset_at = start + timedelta(minutes=2)
    recovered_at = start + timedelta(minutes=3)
    points = [
        (start, 10_000.0, 100.0),
        (before_reset, 10_100.0, 101.0),
        (reset_at, 5.0, 102.0),
        (recovered_at, 105.0, 103.0),
    ]

    series, diagnostics = _provider_vwap_series(
        points,
        trading_date=DAY.date(),
    )

    assert [at for at, _ in series] == [before_reset, recovered_at]
    assert series[-1][1] == pytest.approx(102.0)
    assert diagnostics["gap_count"] == 0
    assert diagnostics["reset_count"] == 1
    assert diagnostics["observed_volume"] == pytest.approx(200.0)
    assert diagnostics["skipped_cross_gap_volume"] == 0.0
    assert diagnostics["observed_volume_ratio"] == 1.0
    assert diagnostics["minimum_observed_volume_ratio"] == 0.80
    assert diagnostics["partial_observed_volume"] is True
    assert diagnostics["volume_coverage"] == "partial_observed_deltas"
