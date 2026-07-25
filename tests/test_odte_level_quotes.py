from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb

from spx_spark.data_platform.research.odte_level_quotes import QuoteStore


EXPIRY = date(2026, 7, 15)
DECISION_AT = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc)


def _write_quote_partition(
    root: Path,
    *,
    provider: str,
    rows: list[tuple[object, ...]],
) -> None:
    partition = (
        root
        / "lake/quotes/schema=v1"
        / f"date={DECISION_AT.date().isoformat()}"
        / f"provider={provider}"
        / f"hour={DECISION_AT:%H}"
    )
    partition.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    try:
        connection.execute(
            """
            CREATE TABLE quotes (
                received_at TIMESTAMPTZ,
                source_at TIMESTAMPTZ,
                quote_time TIMESTAMPTZ,
                instrument_id VARCHAR,
                trading_class VARCHAR,
                expiry DATE,
                strike DOUBLE,
                "right" VARCHAR,
                bid DOUBLE,
                ask DOUBLE,
                mid DOUBLE,
                last DOUBLE,
                effective_price DOUBLE,
                delta DOUBLE,
                quality VARCHAR,
                market_data_type VARCHAR
            )
            """
        )
        connection.executemany(
            """
            INSERT INTO quotes
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        connection.execute(
            "COPY quotes TO ? (FORMAT PARQUET)",
            [str(partition / "quotes.parquet")],
        )
    finally:
        connection.close()


def _option_row(
    *,
    received_at: datetime,
    source_at: datetime,
    strike: float,
    delta: float,
) -> tuple[object, ...]:
    return (
        received_at,
        source_at,
        None,
        f"option:SPX:SPXW:20260715:{strike:.0f}:C",
        "SPXW",
        EXPIRY,
        strike,
        "C",
        9.8,
        10.0,
        9.9,
        None,
        None,
        delta,
        "live",
        "live",
    )


def test_quote_store_uses_received_at_and_rejects_non_live_or_stale_source(
    tmp_path: Path,
) -> None:
    late_received = DECISION_AT + timedelta(seconds=10)
    invalid_future_source = DECISION_AT + timedelta(seconds=20)
    _write_quote_partition(
        tmp_path,
        provider="schwab",
        rows=[
            _option_row(
                received_at=late_received,
                source_at=DECISION_AT,
                strike=7550.0,
                delta=0.5,
            ),
            _option_row(
                received_at=DECISION_AT + timedelta(seconds=1),
                source_at=invalid_future_source,
                strike=7550.0,
                delta=0.5,
            ),
            (
                DECISION_AT + timedelta(seconds=3),
                DECISION_AT,
                DECISION_AT,
                "option:SPX:SPXW:20260715:7550:C",
                "SPXW",
                EXPIRY,
                7550.0,
                "C",
                1.0,
                1.1,
                1.05,
                None,
                None,
                0.5,
                "stale",
                "live",
            ),
            (
                DECISION_AT + timedelta(seconds=4),
                DECISION_AT - timedelta(seconds=31),
                DECISION_AT - timedelta(seconds=31),
                "option:SPX:SPXW:20260715:7550:C",
                "SPXW",
                EXPIRY,
                7550.0,
                "C",
                2.0,
                2.1,
                2.05,
                None,
                None,
                0.5,
                "live",
                "1",
            ),
            (
                DECISION_AT + timedelta(seconds=2),
                DECISION_AT,
                invalid_future_source,
                "option:SPX:SPXW:20260715:7550:C",
                "SPXW",
                EXPIRY,
                7550.0,
                "C",
                9.8,
                10.0,
                9.9,
                None,
                None,
                0.5,
                "live",
                "1",
            ),
            (
                DECISION_AT + timedelta(seconds=3),
                DECISION_AT,
                DECISION_AT,
                "index:SPX",
                None,
                None,
                None,
                None,
                None,
                None,
                7000.0,
                None,
                None,
                None,
                "stale",
                "live",
            ),
            (
                late_received,
                DECISION_AT,
                None,
                "index:SPX",
                None,
                None,
                None,
                None,
                None,
                None,
                7552.0,
                None,
                None,
                None,
                "live",
                "1",
            ),
        ],
    )
    store = QuoteStore(tmp_path)
    try:
        assert (
            store.option_series(
                provider="schwab",
                expiry=EXPIRY,
                strike=7550.0,
                right="C",
                start=DECISION_AT,
                end=DECISION_AT + timedelta(seconds=5),
            )
            == []
        )
        option_ticks = store.option_series(
            provider="schwab",
            expiry=EXPIRY,
            strike=7550.0,
            right="C",
            start=DECISION_AT,
            end=DECISION_AT + timedelta(seconds=15),
        )
        assert [tick.at for tick in option_ticks] == [late_received]

        underlier_ticks = store.underlier_series(
            instrument_id="index:SPX",
            start=DECISION_AT,
            end=DECISION_AT + timedelta(seconds=15),
        )
        assert [tick.at for tick in underlier_ticks] == [late_received]
        assert underlier_ticks[0].price == 7552.0
    finally:
        store.close()


def test_delta_selection_excludes_quotes_received_after_decision(
    tmp_path: Path,
) -> None:
    _write_quote_partition(
        tmp_path,
        provider="schwab",
        rows=[
            _option_row(
                received_at=DECISION_AT - timedelta(seconds=5),
                source_at=DECISION_AT - timedelta(seconds=10),
                strike=7545.0,
                delta=0.45,
            ),
            _option_row(
                received_at=DECISION_AT + timedelta(seconds=10),
                source_at=DECISION_AT - timedelta(seconds=1),
                strike=7550.0,
                delta=0.5,
            ),
            (
                DECISION_AT - timedelta(seconds=1),
                DECISION_AT - timedelta(seconds=2),
                DECISION_AT - timedelta(seconds=2),
                "option:SPX:SPXW:20260715:7550:C",
                "SPXW",
                EXPIRY,
                7550.0,
                "C",
                9.8,
                10.0,
                9.9,
                None,
                None,
                0.5,
                "stale",
                "live",
            ),
        ],
    )
    store = QuoteStore(tmp_path)
    try:
        assert store.select_delta_strike(
            expiry=EXPIRY,
            right="C",
            t0=DECISION_AT,
        ) == 7545.0
    finally:
        store.close()
