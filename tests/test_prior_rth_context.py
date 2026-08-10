from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from spx_spark.application.market_features.prior_rth_context import (
    SCHEMA_VERSION,
    build_prior_rth_context,
    gth_position_fraction,
    prior_session_operator_line,
    prior_session_signal_view,
    process_prior_rth_context,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.storage import LatestState


UTC = timezone.utc
NOW = datetime(2026, 7, 30, 3, 0, tzinfo=UTC)


def test_prior_rth_context_carries_shock_close_location_and_tail() -> None:
    session = DEFAULT_MARKET_CALENDAR.session(
        DEFAULT_MARKET_CALENDAR.previous_trading_day(DEFAULT_MARKET_CALENDAR.research_expiry(NOW))
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
                        "provider": "schwab",
                        "price_kind": "last",
                        "source_at": cursor.isoformat(),
                    },
                    "index:NDX": {
                        "price": 23_000.0 - 150.0 * progress,
                        "reference_close": 23_050.0,
                        "provider": "schwab",
                        "price_kind": "last",
                        "source_at": cursor.isoformat(),
                    },
                    "index:DJI": {
                        "price": 44_000.0 - 100.0 * progress,
                        "reference_close": 44_020.0,
                        "provider": "schwab",
                        "price_kind": "last",
                        "source_at": cursor.isoformat(),
                    },
                    "index:RUT": {
                        "price": 2_250.0 - 30.0 * progress,
                        "reference_close": 2_260.0,
                        "provider": "schwab",
                        "price_kind": "last",
                        "source_at": cursor.isoformat(),
                    },
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
    assert context["schema_version"] == "prior_rth_context.v2"
    assert set(context["indices"]) == {
        "index:SPX",
        "index:NDX",
        "index:DJI",
        "index:RUT",
    }
    assert context["cross_index"]["status"] == "ready"
    assert context["cross_index"]["missing_instruments"] == []
    assert context["cross_index"]["return_dispersion_bps"] > 0.0
    assert context["cross_index"]["semantics"] == (
        "observed_prior_rth_cash_index_regime_not_market_maker_behavior"
    )


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
        DEFAULT_MARKET_CALENDAR.previous_trading_day(DEFAULT_MARKET_CALENDAR.research_expiry(NOW))
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
    assert "prior_rth_minute_coverage_low:index:SPX" in context["reasons"]
    assert context["cross_index"]["missing_instruments"] == [
        "index:NDX",
        "index:DJI",
        "index:RUT",
    ]
    assert context["execution_gate"] is False


def test_process_invalidates_v1_cache_and_preserves_cross_indices_under_spx_overlay(
    tmp_path: Path,
) -> None:
    session = DEFAULT_MARKET_CALENDAR.session(
        DEFAULT_MARKET_CALENDAR.previous_trading_day(DEFAULT_MARKET_CALENDAR.research_expiry(NOW))
    )
    assert session is not None
    samples = []
    canonical_rows = []
    for minute in range(391):
        at = session.open_at + timedelta(minutes=minute)
        spx = 7_400.0 + minute / 10.0
        samples.append(
            {
                "at": at.isoformat(),
                "instruments": {
                    "index:SPX": {"price": spx - 50.0, "reference_close": 7_390.0},
                    "index:NDX": {"price": 23_000.0 + minute, "reference_close": 22_900.0},
                    "index:DJI": {"price": 44_000.0 + minute, "reference_close": 43_900.0},
                    "index:RUT": {"price": 2_200.0 + minute / 10.0, "reference_close": 2_190.0},
                },
            }
        )
        canonical_rows.append(
            {
                "minute": at.isoformat(),
                "session_date": session.trading_date.isoformat(),
                "official_spx_expected": True,
                "selected": {
                    "price": spx,
                    "reference_close": 7_390.0,
                    "provider": "schwab",
                    "price_kind": "last",
                    "source_at": at.isoformat(),
                },
            }
        )
    latest_dir = tmp_path / "latest"
    latest_dir.mkdir(parents=True)
    (latest_dir / "prior_rth_context.json").write_text(
        json.dumps(
            {
                "schema_version": "prior_rth_context.v1",
                "session_date": session.trading_date.isoformat(),
                "status": "ready",
                "sentinel": "stale-v1",
            }
        ),
        encoding="utf-8",
    )
    (latest_dir / "spx_standardized_minutes.json").write_text(
        json.dumps({"rows": canonical_rows}),
        encoding="utf-8",
    )
    latest = LatestState(NOW, NOW, (), ())

    context = process_prior_rth_context(tmp_path, samples, latest, now=NOW)

    assert context["schema_version"] == SCHEMA_VERSION
    assert "sentinel" not in context
    assert context["status"] == "ready"
    assert context["indices"]["index:SPX"]["open"] == 7_400.0
    assert context["indices"]["index:NDX"]["sample_count"] == 391
    assert context["cross_index"]["missing_instruments"] == []


def test_process_backfills_prior_session_from_quote_lake_after_weekend(
    tmp_path: Path,
) -> None:
    """After retention eviction the lake still restores a ready prior context."""

    duckdb = pytest.importorskip("duckdb")
    now = datetime(2026, 8, 10, 1, 0, tzinfo=UTC)  # Sunday 21:00 ET, GTH for Monday
    prior_date = DEFAULT_MARKET_CALENDAR.previous_trading_day(
        DEFAULT_MARKET_CALENDAR.research_expiry(now)
    )
    session = DEFAULT_MARKET_CALENDAR.session(prior_date)
    assert session is not None
    rows = []
    cursor = session.open_at
    minute = 0
    while cursor <= session.close_at:
        progress = minute / 390.0
        for instrument_id, base, reference in (
            ("index:SPX", 7700.0, 7690.0),
            ("index:NDX", 29_500.0, 29_400.0),
            ("index:DJI", 54_000.0, 53_900.0),
            ("index:RUT", 3_020.0, 3_000.0),
        ):
            rows.append(
                {
                    "instrument_id": instrument_id,
                    "last": base + 10.0 * progress,
                    "close": reference,
                    "source_at": cursor.isoformat(),
                }
            )
        cursor += timedelta(minutes=1)
        minute += 1
    partition = (
        tmp_path
        / "lake"
        / "quotes"
        / "schema=v1"
        / f"date={prior_date.isoformat()}"
        / "provider=schwab"
        / "hour=14"
    )
    partition.mkdir(parents=True)
    connection = duckdb.connect()
    try:
        connection.execute(
            "COPY (SELECT instrument_id, last, close,"
            " CAST(source_at AS TIMESTAMPTZ) AS source_at"
            " FROM (SELECT unnest(?::STRUCT(instrument_id VARCHAR, last DOUBLE,"
            " close DOUBLE, source_at VARCHAR)[], recursive := true)))"
            f" TO '{partition / 'quotes.parquet'}' (FORMAT PARQUET)",
            [rows],
        )
    finally:
        connection.close()
    latest = LatestState(now, now, (), ())
    context = process_prior_rth_context(tmp_path, [], latest, now=now)
    assert context["status"] == "ready"
    assert context["session_date"] == prior_date.isoformat()
    assert context["source"] == "quote_lake_minute_backfill"
    cross_index = context["cross_index"]
    assert cross_index["status"] == "ready"
    assert cross_index["missing_instruments"] == []
    assert all(
        value is not None for value in cross_index["return_bps"].values()
    )


def test_process_without_samples_or_lake_stays_unavailable(tmp_path: Path) -> None:
    now = datetime(2026, 8, 10, 1, 0, tzinfo=UTC)
    latest = LatestState(now, now, (), ())
    context = process_prior_rth_context(tmp_path, [], latest, now=now)
    assert context["status"] == "unavailable"


def test_gth_position_fraction_is_bounded_and_requires_a_range() -> None:
    assert gth_position_fraction(
        {"price": 7355.25, "session_low": 7354.5, "session_high": 7392.5}
    ) == pytest.approx(0.01973684)
    assert (
        gth_position_fraction({"price": 7400.0, "session_low": 7400.0, "session_high": 7400.0})
        is None
    )
