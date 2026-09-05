"""Cash and observation boundaries of the executable raw-broker research entrypoint."""

from datetime import date, datetime, timedelta, timezone
import importlib.util
import json
from dataclasses import replace
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


@pytest.mark.parametrize('target,width', [(0.10, 5), (0.15, 10), (0.20, 20)])
def test_fresh_price_implied_delta_condor_has_exact_wings_and_cash(research, target, width):
    at = research._at(DAY, 10, 0)
    tau = 6 / (365*24)
    chain = {}
    for strike in range(7350, 7651, 5):
        for right in ('C', 'P'):
            price = research.bs_price(7500, strike, .20, tau, right)
            chain[strike, right] = leg(strike, price, at, strike=strike, right=right,
                                       delta=None, instrument_id=f'{strike}:{right}')
    row, reason = research._delta_condor(chain, at, target, width, 'bbo_implied')
    assert reason is None
    long_put, short_put, short_call, long_call = row['legs']
    assert short_put['strike']-long_put['strike'] == width
    assert long_call['strike']-short_call['strike'] == width
    assert short_put['strike'] < 7500 < short_call['strike']
    assert all(0 < d <= target for d in row['selected_abs_deltas'])
    credit = short_put['bid']+short_call['bid']-long_put['ask']-long_call['ask']
    assert row['signal_package_price'] == pytest.approx(credit)
    assert 0 < credit < width


def test_removing_twenty_minute_stop_keeps_later_cash_path(research):
    deadline = research._at(DAY, 10, 31)
    row = dict(family='vertical', entry_at=ENTRY, entry_price=4, quantities=[1,-1],
               legs=[leg(0,6), leg(1,2)])
    marks = [research.PolicyMark(ENTRY+timedelta(seconds=s), 3.8 if s<=1200 else 5.5)
             for s in range(0, 1801, 30)]
    book = {(7500,'C'):leg(0,7.5,deadline), (7505,'C'):leg(1,2,deadline)}
    policy = replace(research.DEFAULT_MANAGEMENT_POLICY, hard_exit_et='10:31')
    unrestricted = research._managed_research_exit(row, marks, book, deadline, policy)
    twenty = research._managed_research_exit(row, marks, book, deadline,
                                             replace(policy, time_stop_minutes=20))
    assert unrestricted['exit_at'] == deadline
    assert unrestricted['pnl_usd'] == pytest.approx(144.72)
    assert twenty['pnl_usd'] == pytest.approx(-25.28)
    assert twenty['exit_at'] == ENTRY+timedelta(minutes=20)


def test_managed_butterfly_can_exit_before_1555_and_charges_four_contracts(research):
    row = dict(family='butterfly', entry_at=ENTRY, entry_price=4, quantities=[1,-2,1],
               legs=[leg(0,3),leg(1,1),leg(2,3)])
    at = ENTRY+timedelta(seconds=30)
    result = research._managed_research_exit(row, [research.PolicyMark(at,1.5)], {},
               research._at(DAY,15,59),
               replace(research.DEFAULT_MANAGEMENT_POLICY, hard_exit_et='15:59'))
    assert result['exit_at'] == at
    assert result['exit_reason'] == 'premium_stop'
    assert result['fees_points'] == pytest.approx(.1056)
    assert result['pnl_usd'] == pytest.approx(-260.56)


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
        "source_latency_ms": "DOUBLE",
        "last_update_at": "TIMESTAMPTZ",
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


def test_environment_context_rejects_future_source_and_preserves_invalidations(research, tmp_path):
    at = ENTRY.replace(second=0) + timedelta(minutes=1)
    source = at - timedelta(seconds=10)
    rows = [leg(0, 20, source, instrument_id='index:VIX', market_data_type=None)]
    rows += [
        leg(0, 7500, source, instrument_id='index:SPX'),
        leg(0, 7600, source + timedelta(seconds=1), instrument_id='index:SPX', quality='frozen'),
        leg(0, 10, source, instrument_id='index:VIX1D', quote_time=source+timedelta(seconds=2)),
        leg(0, 999, at+timedelta(seconds=1), instrument_id='index:VIX'),
    ]
    _write_lake(tmp_path, rows)
    files = research._files(tmp_path/'lake/quotes/schema=v1', 'schwab', at-timedelta(minutes=1), at)
    with duckdb.connect() as con:
        paths, modes = research._context_minutes(con, files, at-timedelta(minutes=1), at)
    assert paths['index:VIX'] == {at: 20}
    assert 'index:SPX' not in paths
    assert 'index:VIX1D' not in paths
    assert modes['index:VIX:None'] == 1


def test_environment_prefix_cannot_use_future_prices_or_fill_missing_minutes(research):
    path = {ENTRY+timedelta(minutes=i): 7500+i for i in range(16)}
    at = ENTRY+timedelta(minutes=15)
    before = research._path_context(path, at, 15)
    path[at+timedelta(minutes=1)] = 8000
    assert research._path_context(path, at, 15) == before
    assert before['net'] == 15
    del path[ENTRY+timedelta(minutes=7)]
    gapped = research._path_context(path, at, 15)
    assert gapped['net'] == 15
    assert gapped['rv_points'] is None
    assert not gapped['complete_path']
    del path[ENTRY]
    assert research._path_context(path, at, 15) is None


def test_stop_ablation_uses_same_scheduled_exit_as_hold(research, tmp_path):
    entry = ENTRY.replace(hour=19, minute=54, second=0)
    deadline = entry + timedelta(minutes=1)
    initial = [leg(i, p, entry) for i, p in enumerate((8, 3, 2))]
    closing = [leg(i, p, deadline-timedelta(seconds=1)) for i, p in enumerate((8, 3, 2))]
    later = [leg(i, p, deadline+timedelta(seconds=1)) for i, p in enumerate((5, 3, 2))]
    row = {'family': 'butterfly', 'entry_at': entry, 'signal_at': entry,
           'entry_price': 4, 'width': 15, 'quantities': [1, -2, 1], 'legs': initial}
    events = {q['instrument_id']: [q, later[i]] for i, q in enumerate(closing)}
    chain = {(q['strike'], q['right']): q for q in closing}
    row.update(research._label(row, events, chain, day=DAY, mode='rth', age=15, skew=2))
    _write_lake(tmp_path, initial)
    files = research._files(tmp_path/'lake/quotes/schema=v1', 'schwab', entry, deadline)
    with duckdb.connect() as con:
        research._attribute(con, files, [row], events, {}, DAY, 'rth', 15, 2, chain)
    assert row['attribution']['alternative_exit_at'] == deadline
    assert row['attribution']['alternative_exit_reason'] == 'hard_close'
    assert row['attribution']['alternative_pnl_usd'] == pytest.approx(row['pnl_usd'])
    assert row['attribution']['paired_difference_usd'] == pytest.approx(0)


def test_up_only_policy_observes_first_up_after_down_and_rejects_missing_prefix(research):
    opening = research._at(DAY, 9, 30)
    path = {opening+timedelta(minutes=i): 7500 for i in range(1, 16)}
    path.update({opening+timedelta(minutes=i): 7495 for i in range(16, 19)})
    path.update({opening+timedelta(minutes=i): 7505 for i in range(19, 22)})
    up = research._first_directional_range_signal(DAY, path, 'UP')
    down = research._first_directional_range_signal(DAY, path, 'DOWN')
    assert up['signal_at'] == opening+timedelta(minutes=21)
    assert down['signal_at'] == opening+timedelta(minutes=18)
    path[opening+timedelta(minutes=22)] = 7000
    assert research._first_directional_range_signal(DAY, path, 'UP') == up
    del path[opening+timedelta(minutes=17)]
    assert research._first_directional_range_signal(DAY, path, 'UP')['status'] == 'UNDERLIER_GAP'


def test_full_raw_directional_policy_uses_scheduled_twenty_minute_exit(research, tmp_path):
    opening = research._at(DAY, 9, 30)
    signal = opening+timedelta(minutes=18)
    entry = signal+timedelta(seconds=15)
    rows = []
    for minute in range(1, 19):
        source = opening+timedelta(minutes=minute, seconds=-5)
        rows.append(leg(0, 7500 if minute<=15 else 7505, source, instrument_id='index:SPX'))
    times = [signal, entry]+[entry+timedelta(seconds=i) for i in range(10, 1861, 10)]
    for at in times:
        long_price = 6 if at<entry+timedelta(minutes=15) else 8
        if at>entry+timedelta(minutes=20):
            long_price = 3
        for i, strike, right, price in [(0, 7505, 'C', long_price), (1, 7520, 'C', 2), (2, 7505, 'P', 6)]:
            rows.append(leg(i, price, at, strike=strike, right=right, trading_class='SPXW', expiry=DAY))
    _write_lake(tmp_path/'raw', rows)
    research.validate_directional_signal(tmp_path/'raw', tmp_path/'out', DAY, DAY, ['schwab'])
    result = [json.loads(x) for x in (tmp_path/'out/rows.jsonl').read_text().splitlines()]
    primary = next(r for r in result if r['setup']=='or15_up_20m')
    assert primary['status'] == 'COMPLETE_EXIT'
    assert primary['pnl_usd'] == pytest.approx(194.72)
    assert datetime.fromisoformat(primary['exit_at']) == entry+timedelta(minutes=20)
    assert primary['exit_reason'] == 'time_stop'
    assert len(result) == 6


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
    rows.extend(
        [
            {
                **q,
                "received_at": signal + timedelta(seconds=14),
                "quote_time": signal + timedelta(seconds=16),
                "greeks_model": "schwab_chain",
                "market_data_type": None,
            }
            for q in list(rows)
            if q["received_at"] == signal + timedelta(seconds=10)
        ]
    )
    _write_lake(tmp_path, rows)
    output = tmp_path / "result"
    report = research.run(tmp_path, output, start=DAY, end=DAY, attribution=True)
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
    dedup = tmp_path / "deduplicated"
    research.deduplicate_lake(tmp_path, dedup, DAY, DAY, ["schwab"])
    replay = tmp_path / "dedup-replay"
    research.run(dedup, replay, start=DAY, end=DAY, attribution=True)
    restored = [json.loads(line) for line in (replay / "rows.jsonl").read_text().splitlines()]
    assert restored == outcomes
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


def test_dedup_preserves_receipts_invalidations_depth_and_source_age(research, tmp_path):
    source = tmp_path / "raw.parquet"
    target = tmp_path / "dedup.parquet"
    with duckdb.connect() as con:
        con.execute("""CREATE TABLE q(provider VARCHAR,instrument_id VARCHAR,
            received_at TIMESTAMPTZ,quote_time TIMESTAMPTZ,source_latency_ms DOUBLE,
            bid DOUBLE,ask DOUBLE,bid_size DOUBLE,quality VARCHAR,greeks_model VARCHAR)""")
        # A repeated state reappears after a freeze: recovery must survive;
        # same prices with a new source clock or size are distinct states.
        values = [
            (
                "schwab",
                "x",
                ENTRY + timedelta(seconds=i),
                source_at,
                1000.0 * i,
                1.0,
                2.0,
                size,
                quality,
                lane,
            )
            for i, source_at, size, quality, lane in [
                (0, ENTRY, 5, "live", "schwab_stream"),
                (1, ENTRY, 5, "live", "schwab_stream"),
                (2, ENTRY, 5, "frozen", "schwab_stream"),
                (3, ENTRY, 5, "live", "schwab_stream"),
                (4, ENTRY, 6, "live", "schwab_stream"),
                (5, ENTRY + timedelta(seconds=5), 6, "live", "schwab_stream"),
                (6, ENTRY, 5, "live", "schwab_chain"),
            ]
        ]
        con.executemany("INSERT INTO q VALUES (?,?,?,?,?,?,?,?,?,?)", values)
        con.execute("ALTER TABLE q ADD COLUMN last_update_at TIMESTAMPTZ")
        con.execute("UPDATE q SET last_update_at=received_at")
        con.execute("COPY q TO ? (FORMAT PARQUET)", [str(source)])
        result = research._deduplicate_partition(con, source, target)
        assert result["input_rows"] == 7
        assert result["unique_snapshots"] == 5
        research._read_quotes(con, [str(target)])
        actual = con.execute("SELECT * FROM broker_quotes ORDER BY received_at").fetchall()
        names = [d[0] for d in con.description]
        recovered = [dict(zip(names, r)) for r in actual]
        assert [r["quality"] for r in recovered] == [v[8] for v in values]
        assert [r["received_at"] for r in recovered] == [v[2] for v in values]
        assert recovered[3]["quote_time"] == ENTRY
        assert recovered[5]["quote_time"] == ENTRY + timedelta(seconds=5)
        assert con.execute("SELECT count(*) FROM read_parquet(?)", [str(source)]).fetchone()[0] == 7


def test_expanding_model_never_trains_on_current_or_future_return(research):
    import copy

    rows = [
        {
            "provider": "schwab",
            "mode": "rth",
            "setup": "clock_condor",
            "session_date": str(DAY + timedelta(days=i)),
            "width": 10,
            "atm_straddle_points": 30,
            "pnl_usd": -20.0,
            "attribution": {
                "entry_cost_fraction": 0.25,
                "signal_net_15m": 1.0,
                "signal_efficiency_15m": 0.2,
            },
        }
        for i in range(17)
    ]
    changed = copy.deepcopy(rows)
    changed[-2]["pnl_usd"] = 1_000_000.0
    changed[-1]["pnl_usd"] = 2_000_000.0
    research._model_check(rows)
    research._model_check(changed)
    assert rows[-2]["model_check"] == changed[-2]["model_check"]
    assert rows[-2]["model_check"]["trained_through"] < rows[-2]["session_date"]
    assert rows[-2]["model_check"]["ridge_expected_usd"] == pytest.approx(-20.0)
    assert "model_check" not in rows[14]
