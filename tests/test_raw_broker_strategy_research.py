"""Cash and observation boundaries of the executable raw-broker research entrypoint."""

from datetime import date, datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path

import duckdb
import pytest


DAY = date(2026, 8, 5)
ENTRY = datetime(2026, 8, 5, 14, 0, 15, tzinfo=timezone.utc)


@pytest.fixture(scope="module")
def research():
    path = Path(__file__).parents[1] / "docs/notebooks/spx-one-month-strategy-edge-2026-08-29.py"
    spec = importlib.util.spec_from_file_location("raw_broker_research", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def leg(i, price, at=ENTRY, **extra):
    return {
        "instrument_id": f"option:{i}",
        "strike": 7500 + i * 5,
        "right": "C",
        "provider": "schwab",
        "received_at": at,
        "quote_time": at,
        "bid": price,
        "ask": price,
        "bid_size": 10,
        "ask_size": 10,
        "quality": "live",
        "market_data_type": "live",
        "greeks_model": "schwab_stream",
        **extra,
    }


@pytest.mark.parametrize(
    "buyback,expected,reason",
    [
        (1.25, 114.44, "profit_take"),
        (7.5, -510.56, "stop_loss"),
    ],
)
def test_credit_cash_profit_and_three_credit_stop(research, buyback, expected, reason):
    row = {
        "family": "condor",
        "entry_at": ENTRY,
        "entry_price": 2.5,
        "width": 10,
        "quantities": [1, -1, -1, 1],
        "legs": [leg(i, p) for i, p in enumerate((0.5, 1.75, 1.75, 0.5))],
    }
    at = ENTRY + timedelta(seconds=5)
    close = [leg(i, p, at) for i, p in enumerate((0.5, (buyback + 1) / 2, (buyback + 1) / 2, 0.5))]
    result = research._label(
        row, {q["instrument_id"]: [q] for q in close}, {}, day=DAY, mode="rth", age=15, skew=2
    )
    assert result["pnl_usd"] == pytest.approx(expected)
    assert result["cash_exit_points"] == buyback
    assert result["exit_reason"] == reason


def test_frozen_leg_cannot_be_refreshed_by_other_legs(research):
    row = {
        "family": "vertical",
        "entry_at": ENTRY,
        "entry_price": 4,
        "width": 15,
        "quantities": [1, -1],
        "legs": [leg(0, 6), leg(1, 2)],
    }
    end = ENTRY + timedelta(seconds=125)
    events = {
        "option:0": [leg(0, 3, end)],
        "option:1": [leg(1, 2, ENTRY + timedelta(seconds=i)) for i in range(5, 126, 5)],
    }
    result = research._label(row, events, {}, day=DAY, mode="rth", age=15, skew=2)
    assert result["status"] == "QUOTE_GAP"
    assert result["pnl_usd"] is None


@pytest.mark.parametrize(
    "bad",
    [
        {"quote_time": None},
        {"quality": "frozen"},
        {"received_at": ENTRY + timedelta(seconds=1)},
        {"quote_time": ENTRY + timedelta(seconds=1)},
        {"quote_time": ENTRY - timedelta(seconds=16)},
    ],
)
def test_invalid_leg_never_becomes_an_executable_package(research, bad):
    assert research._cash_quote([leg(0, 6, **bad), leg(1, 2)], [1, -1], ENTRY, 15, 2) is None


def _write_lake(root, rows):
    fields = {
        "received_at": "TIMESTAMPTZ",
        "source_at": "TIMESTAMPTZ",
        "quote_time": "TIMESTAMPTZ",
        "trade_time": "TIMESTAMPTZ",
        "instrument_id": "VARCHAR",
        "trading_class": "VARCHAR",
        "expiry": "DATE",
        "strike": "DOUBLE",
        "right": "VARCHAR",
        "bid": "DOUBLE",
        "ask": "DOUBLE",
        "bid_size": "DOUBLE",
        "ask_size": "DOUBLE",
        "last": "DOUBLE",
        "effective_price": "DOUBLE",
        "delta": "DOUBLE",
        "implied_vol": "DOUBLE",
        "greeks_model": "VARCHAR",
        "quality": "VARCHAR",
        "market_data_type": "VARCHAR",
    }
    with duckdb.connect() as con:
        con.execute("CREATE TABLE q(" + ",".join(f'"{k}" {v}' for k, v in fields.items()) + ")")
        con.executemany(
            "INSERT INTO q VALUES (" + ",".join("?" for _ in fields) + ")",
            [[r.get(k) for k in fields] for r in rows],
        )
        for hour in sorted(
            {r["received_at"].replace(minute=0, second=0, microsecond=0) for r in rows}
        ):
            path = (
                root
                / f"lake/quotes/schema=v1/date={hour:%Y-%m-%d}/provider=schwab/hour={hour:%H}/quotes.parquet"
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            con.execute(
                "CREATE OR REPLACE TEMP TABLE one_hour AS SELECT * FROM q WHERE received_at>=? AND received_at<?",
                [hour, hour + timedelta(hours=1)],
            )
            con.execute("COPY one_hour TO ? (FORMAT PARQUET)", [str(path)])


def test_raw_lake_to_frozen_butterfly_cash_exit_and_missing_provider_denominator(
    research, tmp_path
):
    signal = ENTRY.replace(hour=19, minute=0, second=0)
    deadline = signal.replace(minute=55)
    rows = []
    # Long 7485 C, short two 7500 C, long 7515 C: debit 4, terminal bid 6.
    for at, prices in (
        (signal, (8, 3, 2)),
        (signal + timedelta(seconds=10), (8, 3, 2)),
        (deadline, (10, 3, 2)),
    ):
        for i, (strike, price) in enumerate(zip((7485, 7500, 7515), prices)):
            rows.append(
                leg(i, price, at, strike=strike, trading_class="SPXW", expiry=DAY, source_at=at)
            )
        rows.append(
            leg(3, 3, at, strike=7500, right="P", trading_class="SPXW", expiry=DAY, source_at=at)
        )
    # A legacy REST request clock is later than the good stream receipt, but
    # its response contains quotes not yet available at the action time.
    # It must not overwrite the stream lane's current book.
    rows.extend([
        {**q, "received_at": signal+timedelta(seconds=14),
         "quote_time": signal+timedelta(seconds=16), "greeks_model": "schwab_chain",
         "market_data_type": None}
        for q in list(rows) if q["received_at"]==signal+timedelta(seconds=10)
    ])
    _write_lake(tmp_path, rows)
    output = tmp_path / "result"
    report = research.run(tmp_path, output, start=DAY, end=DAY)
    outcomes = [json.loads(line) for line in (output / "rows.jsonl").read_text().splitlines()]
    butterfly = next(
        r for r in outcomes if r["provider"] == "schwab" and r["setup"] == "clock_butterfly"
    )
    assert butterfly["pnl_usd"] == pytest.approx(189.44)
    assert butterfly["contract_count"] == 4
    assert butterfly["exit_at"] == str(deadline)
    assert len(outcomes) == 18  # every setup/provider/session remains in the denominator
    assert all(r["status"] == "PARTITION_MISSING" for r in outcomes if r["provider"] == "ibkr")
    assert len(report["coverage"]) == 3
    # Two short contracts require two displayed contracts to buy them back.
    closing = {
        (q["strike"], q["right"]): {**q, "ask_size": 1}
        for q in rows
        if q["received_at"] == deadline
    }
    row = {
        **butterfly,
        "entry_at": signal + timedelta(seconds=15),
        "legs": [
            q
            for q in rows
            if q["received_at"] == signal + timedelta(seconds=10) and q["right"] == "C"
        ],
    }
    assert research._label(row, {}, closing, day=DAY, mode="rth", age=15, skew=2)["pnl_usd"] is None


def test_dated_future_and_no_roll_are_used_for_raw_signal_path(research, tmp_path):
    rows = []
    for minute, contract, price in (
        (0, "future:ES:20260918", 7500),
        (1, "future:ES:20261218", 7600),
        (2, "future:ES:20260918", 7501),
    ):
        at = ENTRY.replace(second=59) + timedelta(minutes=minute)
        rows.append(leg(minute, price, at, instrument_id=contract, source_at=at))
    _write_lake(tmp_path, rows)
    files = list(map(str, (tmp_path / "lake").rglob("*.parquet")))
    with duckdb.connect() as con:
        result = research._underlier_minutes(
            con, files, "future:ES", ENTRY, ENTRY + timedelta(minutes=4)
        )
    assert list(result.values()) == [7500, 7501]
