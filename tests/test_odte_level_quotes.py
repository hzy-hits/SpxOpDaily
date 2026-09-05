from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import json
import hashlib
from zoneinfo import ZoneInfo

import pytest

import duckdb

from spx_spark.data_platform.research.odte_level_quotes import QuoteStore


EXPIRY = date(2026, 7, 15)
DECISION_AT = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc)


def test_dated_es_is_loaded_without_provider_or_roll_price_splicing(tmp_path):
    def tick(contract, seconds, price):
        at = DECISION_AT+timedelta(seconds=seconds)
        return (at, at, at, contract, None, None, None, None, None, None,
                price, None, None, None, "live", "1")

    _write_quote_partition(tmp_path, provider="ibkr", rows=[
        tick("future:ES:20260918", 0, 7500),
        tick("future:ES:20261218", 1, 7600),
        tick("future:ES:20260918", 2, 7501),
    ])
    _write_quote_partition(tmp_path, provider="schwab", rows=[tick("future:ES", 1, 7530)])
    store = QuoteStore(tmp_path)
    try:
        result = store.underlier_series(instrument_id="future:ES", start=DECISION_AT,
                                       end=DECISION_AT+timedelta(seconds=10))
        assert [t.price for t in result] == [7500, 7501]
    finally:
        store.close()


def test_quote_windows_with_eastern_time_read_the_utc_partitions(tmp_path):
    _write_quote_partition(tmp_path, provider="schwab", rows=[_option_row(
        received_at=DECISION_AT, source_at=DECISION_AT, strike=7550, delta=.5,
    )])
    store = QuoteStore(tmp_path)
    try:
        start = DECISION_AT.astimezone(ZoneInfo("America/New_York"))
        result = store.option_series(provider="schwab", expiry=EXPIRY, strike=7550, right="C",
                                     start=start, end=start+timedelta(seconds=10))
        assert [t.at for t in result] == [DECISION_AT]
    finally:
        store.close()


def test_captured_quote_replay_never_consults_changed_lake(tmp_path):
    _write_quote_partition(tmp_path, provider="schwab", rows=[_option_row(
        received_at=DECISION_AT, source_at=DECISION_AT, strike=7550, delta=.5,
    )])
    query = dict(provider="schwab", expiry=EXPIRY, strike=7550, right="C",
                 start=DECISION_AT, end=DECISION_AT + timedelta(seconds=60))
    store = QuoteStore(tmp_path)
    expected = store.option_series(**query)
    snapshot = tmp_path / "snapshot.jsonl"
    digest = store.write_snapshot(snapshot)
    store.close()
    assert hashlib.sha256(snapshot.read_bytes()).hexdigest() == digest
    assert json.loads(snapshot.read_text())["ticks"][0]["source_at"]
    for path in (tmp_path / "lake").rglob("*.parquet"):
        path.write_bytes(b"changed after snapshot")
    restored = QuoteStore(tmp_path)
    try:
        restored.load_snapshot(snapshot)
        assert restored.option_series(**query) == expected
        assert restored.option_series(**{**query, "end": DECISION_AT + timedelta(seconds=5)}) == expected
        with pytest.raises(ValueError, match="quote_snapshot_series_unavailable"):
            restored.option_series(**{**query, "end": DECISION_AT + timedelta(seconds=61)})
        with pytest.raises(ValueError, match="quote_snapshot_series_unavailable"):
            restored.option_series(**{**query, "strike": 7560})
    finally:
        restored.close()


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
        source_at,
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


def test_recent_trade_or_receipt_does_not_certify_option_bbo_age(tmp_path):
    row = list(_option_row(received_at=DECISION_AT, source_at=DECISION_AT, strike=7550, delta=.5))
    row[2] = None
    _write_quote_partition(tmp_path, provider="schwab", rows=[tuple(row)])
    store = QuoteStore(tmp_path)
    try:
        ticks = store.option_series(provider="schwab", expiry=EXPIRY, strike=7550, right="C",
                                   start=DECISION_AT, end=DECISION_AT+timedelta(seconds=10))
        assert len(ticks) == 1
        assert ticks[0].bid is None and ticks[0].ask is None and ticks[0].source_at is None
    finally:
        store.close()


def test_frozen_arrival_invalidates_carried_bbo_until_that_leg_recovers(tmp_path):
    from spx_spark.data_platform.research.strategy_policy_backfill import _combo_bid_marks

    rows = []
    for seconds, strike in ((0, 7550), (0, 7565), (1, 7550), (1, 7565), (2, 7565), (3, 7550), (3, 7565)):
        at = DECISION_AT+timedelta(seconds=seconds)
        row = list(_option_row(received_at=at, source_at=at, strike=strike, delta=.5))
        if seconds==1 and strike==7550:
            row[-2] = "frozen"
        rows.append(tuple(row))
    rest = list(_option_row(received_at=DECISION_AT+timedelta(seconds=3.5),
                            source_at=DECISION_AT+timedelta(seconds=4), strike=7550, delta=.5))
    rest[-1] = None
    rows.append(tuple(rest))
    _write_quote_partition(tmp_path, provider="schwab", rows=rows)
    store = QuoteStore(tmp_path)
    try:
        marks = _combo_bid_marks(store, legs=[
            {"expiry": str(EXPIRY), "strike": 7550, "right": "C", "quantity": 1},
            {"expiry": str(EXPIRY), "strike": 7565, "right": "C", "quantity": -1},
        ], provider="schwab", start=DECISION_AT, end=DECISION_AT+timedelta(seconds=4))
        assert [m.at for m in marks] == [DECISION_AT, DECISION_AT+timedelta(seconds=3)]
    finally:
        store.close()


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
        assert all(tick.bid is None and tick.ask is None for tick in store.option_series(
                provider="schwab",
                expiry=EXPIRY,
                strike=7550.0,
                right="C",
                start=DECISION_AT,
                end=DECISION_AT + timedelta(seconds=5),
            ))
        option_ticks = store.option_series(
            provider="schwab",
            expiry=EXPIRY,
            strike=7550.0,
            right="C",
            start=DECISION_AT,
            end=DECISION_AT + timedelta(seconds=15),
        )
        assert [tick.at for tick in option_ticks if tick.ask is not None] == [late_received]

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
