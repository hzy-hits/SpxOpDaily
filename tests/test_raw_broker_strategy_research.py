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


def test_reclaim_requires_observed_break_and_recovery_without_future_filter(research):
    opening = research._at(DAY, 9, 31)
    path = {opening + timedelta(minutes=i): 100.0 for i in range(15)}
    for minute, price in [(46, 98), (47, 97), (48, 102), (49, 103)]:
        path[research._at(DAY, 9, minute)] = price
    signal = research._range_reclaim_signal(DAY, path, "UP")
    assert signal["signal_at"] == research._at(DAY, 9, 49)
    changed = {**path, research._at(DAY, 10, 0): 1.0}
    assert research._range_reclaim_signal(DAY, changed, "UP") == signal
    del changed[research._at(DAY, 9, 48)]
    assert research._range_reclaim_signal(DAY, changed, "UP")["status"] == "UNDERLIER_GAP"


def test_price_exit_can_hold_longer_than_twenty_minutes_but_cannot_skip_missing_close(research):
    start = research._at(DAY, 9, 31)
    path = {start + timedelta(minutes=i): 100.0 + i for i in range(90)}
    deadline = research._at(DAY, 10, 45)
    row = dict(entry_at=ENTRY, family="vertical", direction="UP")
    assert research._price_exit_intent(row, path, deadline, ema_span=10)["at"] == deadline
    gap = research._at(DAY, 10, 25)
    del path[gap]
    intent = research._price_exit_intent(row, path, deadline, ema_span=10)
    assert intent == {"at": gap, "reason": "UNDERLIER_EXIT_GAP", "censored": True}


def test_only_observations_needed_by_exit_contract_can_censor_cash(research):
    row = dict(entry_at=ENTRY, entry_price=4.0, family="vertical", quantities=[1, -1])
    trigger = ENTRY + timedelta(minutes=10)
    action = trigger + timedelta(seconds=15)
    marks = [research.PolicyMark(ENTRY, 4.0), research.PolicyMark(action, 6.0)]
    intent = {"at": trigger, "reason": "ema10_reversal", "censored": False}
    deadline = research._at(DAY, 15, 45)
    price_only = research._action_exit_label(row, marks, intent, deadline)
    assert price_only["exit_at"] == action
    assert price_only["pnl_usd"] == pytest.approx(194.72)
    with_quote_stop = research._action_exit_label(
        row, marks, intent, deadline, quote_management=True
    )
    assert with_quote_stop["status"] == "QUOTE_GAP"
    assert with_quote_stop["pnl_usd"] is None


@pytest.mark.parametrize("latency", [15, 30, 60])
def test_exit_latency_uses_first_valid_book_and_never_best_future_price(research, latency):
    row = dict(entry_at=ENTRY, entry_price=4.0, family="vertical", quantities=[1, -1])
    trigger = ENTRY + timedelta(minutes=3)
    marks = [
        research.PolicyMark(trigger + timedelta(seconds=s), bid)
        for s, bid in [(1, 9.0), (latency, 5.0), (latency + 5, 10.0)]
    ]
    result = research._action_exit_label(
        row,
        marks,
        {"at": trigger, "reason": "ema10_reversal", "censored": False},
        research._at(DAY, 15, 45),
        latency_seconds=latency,
    )
    assert result["exit_at"] == trigger + timedelta(seconds=latency)
    assert result["pnl_usd"] == pytest.approx(94.72)


def test_two_price_closes_exit_cannot_be_reversed_by_later_recovery(research):
    start = research._at(DAY, 9, 31)
    path = {start + timedelta(minutes=i): 100.0 + i for i in range(30)}
    row = dict(entry_at=research._at(DAY, 9, 50), family="vertical", direction="UP")
    first, second = research._at(DAY, 10, 1), research._at(DAY, 10, 2)
    path[first], path[second] = 100.0, 99.0
    intent = research._price_exit_intent(row, path, research._at(DAY, 15, 45), ema_span=10)
    assert intent == {"at": second, "reason": "ema10_reversal", "censored": False}
    path[research._at(DAY, 10, 3)] = 1000.0
    assert research._price_exit_intent(row, path, research._at(DAY, 15, 45), ema_span=10) == intent


@pytest.mark.parametrize(
    "family,quantities,entry,policy_mark,expected",
    [
        ("condor", [1, -1, -1, 1], 2.5, -2.5, -510.56),
        ("condor", [1, -1, -1, 1], 2.5, 3.75, 114.44),
        ("butterfly", [1, -2, 1], 2.5, 3.75, 114.44),
    ],
)
def test_price_exit_cash_preserves_credit_losses_and_contract_multiplicity(
    research, family, quantities, entry, policy_mark, expected
):
    row = dict(entry_at=ENTRY, entry_price=entry, family=family, quantities=quantities)
    trigger = ENTRY + timedelta(minutes=5)
    marks = [research.PolicyMark(trigger + timedelta(seconds=15), policy_mark)]
    result = research._action_exit_label(
        row,
        marks,
        {"at": trigger, "reason": "structure_breach", "censored": False},
        research._at(DAY, 15, 45),
    )
    assert result["status"] == "COMPLETE_EXIT"
    assert result["pnl_usd"] == pytest.approx(expected)


def test_gamma_position_requires_coverage_and_never_uses_future_receipts(research):
    import copy

    at = research._at(DAY, 10, 0)
    tau = 6 / (365 * 24)
    chain = {}
    for strike in range(7400, 7601, 5):
        for right in ("C", "P"):
            price = research.bs_price(7500, strike, .20, tau, right)
            chain[strike, right] = leg(
                strike, price, at, strike=strike, right=right,
                open_interest=1000 if strike == 7510 else 1,
            )
    result = research._gamma_position(chain, at)
    assert result["status"] == "available"
    assert result["pin_center"] == 7510
    assert result["dealer_sign"] == "UNKNOWN"
    changed = copy.deepcopy(chain)
    changed[7515, "C"].update(open_interest=1e10, received_at=at+timedelta(seconds=1))
    assert research._gamma_position(changed, at)["pin_center"] == 7510
    sparse = {key: value for key, value in chain.items() if abs(key[0]-7500) <= 10}
    assert research._gamma_position(sparse, at)["status"] == "GAMMA_COVERAGE_UNAVAILABLE"


@pytest.mark.parametrize("fault", ["frozen", "missing_source", "future_source"])
def test_raw_ict_bars_cannot_hide_a_stale_final_tick(research, tmp_path, fault):
    at = research._at(DAY, 9, 30)
    rows = [leg(i, 7500+i, at+timedelta(seconds=s), instrument_id="index:SPX")
            for i,s in enumerate([5, 55, 65, 115])]
    if fault == "frozen":
        rows[-1].update(quality="frozen")
    elif fault == "missing_source":
        rows[-1].update(quote_time=None)
    else:
        rows[-1].update(quote_time=at+timedelta(seconds=116))
    _write_lake(tmp_path, rows)
    files = list(map(str, (tmp_path/"lake").rglob("*.parquet")))
    with duckdb.connect() as con:
        bars = research._raw_spx_bars(con, files, DAY)
    assert len(bars) == 1
    assert bars[0]["available_at"] == at+timedelta(minutes=1)
    assert bars[0]["high"] == 7501
    assert bars[0]["low"] == 7500


def test_ict_first_confirmation_is_causal_and_cannot_bridge_missing_minutes(research):
    first = research._at(DAY, 9, 31)
    bars = [dict(available_at=first+timedelta(minutes=i), open=100., high=101., low=99., close=100.)
            for i in range(15)]
    bars += [
        dict(available_at=research._at(DAY,9,46),open=100.,high=100.,low=98.5,close=99.5),
        dict(available_at=research._at(DAY,9,47),open=99.5,high=102.,low=99.5,close=101.5),
    ]
    result = research._ict_first_signals(DAY,bars)[0]
    assert result['signal_at'] == research._at(DAY,9,47)
    assert result['ict']['stage'] == 'MSS_DISPLACEMENT_CONFIRMED'
    later = bars+[dict(available_at=research._at(DAY,9,48),open=101.,high=150.,low=90.,close=140.)]
    assert research._ict_first_signals(DAY,later)[0] == result
    assert research._ict_first_signals(DAY,later[:15]+later[16:])[0]['status'] == 'UNDERLIER_GAP'


def test_factor_selection_cannot_use_later_features_or_outcomes(research):
    import copy

    rows=[]
    for i in range(20):
        training=i<12
        rows.append(dict(setup='clock_condor',family='condor',session_date=f'2026-07-{i+10:02}' if training else f'2026-08-{i-11:02}',
            entry_at='observed',defined_risk_usd=1000.,quantities=[1,-1,-1,1],
            pnl_usd=100. if i<6 else -100.,factors={'rv5':float(i)}))
    selected=research.evaluate_option_factor_rules(rows)['selected']['condor']['winner']
    assert selected is not None
    changed=copy.deepcopy(rows)
    for r in changed[12:]:
        r['pnl_usd']=1e9
        r['factors']['rv5']=-1e12
    again=research.evaluate_option_factor_rules(changed)['selected']['condor']['winner']
    assert selected==again
    assert selected['conditions'][0][0]=='rv5'


def test_entered_missing_factor_label_is_never_zero_profit(research):
    row=dict(entry_at='observed',defined_risk_usd=500.,quantities=[1,-2,1],pnl_usd=None)
    assert research._factor_return(row)==pytest.approx(-1.02112)
    assert research._factor_return({**row,'entry_at':None})==0


def test_factor_cash_uses_signal_book_and_absolute_contract_count(research):
    import copy

    at=research._at(DAY,10,0)
    tau=6/(365*24)
    chain={}
    for strike in range(7400,7601,5):
        for right in ('C','P'):
            price=research.bs_price(7500,strike,.20,tau,right)
            chain[strike,right]=leg(strike,price,at,strike=strike,right=right)
    row=dict(family='butterfly',width=15,quantities=[1,-2,1],
             legs=[chain[k,'C'] for k in (7485,7500,7515)],entry_price=1.)
    path={research._at(DAY,9,31)+timedelta(minutes=i):7497.+i*.1 for i in range(30)}
    first=research._raw_option_factors(row,path,chain,{}, {},at)
    altered=copy.deepcopy(row)
    altered['entry_price']=10000.
    later={**path,at+timedelta(minutes=1):1e9}
    second=research._raw_option_factors(altered,later,chain,{}, {},at)
    assert first==second
    premium=research._cash_quote(row['legs'],row['quantities'],at,15,2)[0]
    assert first['fee_fraction']==pytest.approx(10.56/(100*(15-premium)))
    assert first['rv30']==pytest.approx(.01)


def test_factor_input_cannot_accept_source_after_receipt_before_decision(research):
    quote=leg(0,4.,ENTRY-timedelta(seconds=10),quote_time=ENTRY-timedelta(seconds=5))
    assert research._cash_quote([quote],[1],ENTRY,15,2) is None


@pytest.mark.parametrize('sign',[1,-1])
def test_failed_expansion_is_first_causal_excursion_with_frozen_target(research,sign):
    start=research._at(DAY,9,31)
    values=[7500,7510]*7+[7505,7512,7513,7508,7507]
    path={start+timedelta(minutes=i):7505+sign*(p-7505) for i,p in enumerate(values)}
    direction='UP' if sign==1 else 'DOWN'
    result=research._failed_expansion_signal(DAY,path,direction)
    assert result['signal_at']==research._at(DAY,9,49)
    assert result['target_level']==7505
    assert result['invalidation_level']==7505+9*sign
    future={**path,research._at(DAY,9,50):9000}
    assert research._failed_expansion_signal(DAY,future,direction)==result
    del future[research._at(DAY,9,48)]
    assert research._failed_expansion_signal(DAY,future,direction)['status']=='UNDERLIER_GAP'


def test_failed_first_expansion_cannot_be_replaced_by_a_later_winning_reclaim(research):
    start=research._at(DAY,9,31)
    values=[7500,7510]*7+[7505]+[7515]*19+[7507]*5
    path={start+timedelta(minutes=i):p for i,p in enumerate(values)}
    assert research._failed_expansion_signal(DAY,path,'UP')['status']=='FIRST_EXPANSION_DID_NOT_FAIL_IN_15M'


@pytest.mark.parametrize('direction,sign',[('UP',1),('DOWN',-1)])
def test_destination_exit_uses_first_observed_hit_and_delayed_cash(research,direction,sign):
    row=dict(family='butterfly',direction=direction,entry_at=ENTRY,entry_price=2.,
        target_level=7500+15*sign,invalidation_level=7500-5*sign,quantities=[1,-2,1])
    first=ENTRY.replace(second=0)+timedelta(minutes=1)
    path={first:7500+10*sign,first+timedelta(minutes=1):7500+16*sign}
    deadline=research._at(DAY,15,59)
    intent=research._mechanism_exit_intent(row,path,deadline)
    assert intent['reason']=='price_destination_reached'
    assert intent['at']==first+timedelta(minutes=1)
    marks=[research.PolicyMark(intent['at'],10.),research.PolicyMark(intent['at']+timedelta(seconds=15),3.)]
    result=research._action_exit_label(row,marks,intent,deadline)
    assert result['pnl_usd']==pytest.approx(89.44)
    path.pop(first)
    assert research._mechanism_exit_intent(row,path,deadline)['censored']


def test_condor_does_not_exit_just_because_midpoint_is_reached(research):
    row=dict(family='condor',direction='DOWN',entry_at=ENTRY,target_level=7500,invalidation_level=7515)
    first=ENTRY.replace(second=0)+timedelta(minutes=1)
    path={first:7499,first+timedelta(minutes=1):7516,first+timedelta(minutes=2):7517}
    intent=research._mechanism_exit_intent(row,path,research._at(DAY,15,45))
    assert intent['reason']=='price_hypothesis_failed'
    assert intent['at']==first+timedelta(minutes=2)


def test_primary_mechanism_policy_cannot_replace_unpriceable_first_signal(research):
    early=dict(session_date='2026-08-05',mechanism='failed_expansion',
        setup='failure_up_ic20_w20_price_quote',signal_name='failure_up_ic20_w20',
        signal_at=ENTRY,status='ENTRY_BBO_UNAVAILABLE')
    late={**early,'signal_at':ENTRY+timedelta(hours=1),'signal_name':'failure_down_ic20_w20',
        'setup':'failure_down_ic20_w20_price_quote','entry_at':ENTRY+timedelta(hours=1),
        'status':'COMPLETE_EXIT','pnl_usd':1000.,'entry_price':2.,'defined_risk_usd':1800.,
        'quantities':[1,-1,-1,1],'exit_reason':'take_profit'}
    result=research._evaluate_mechanism_replay([early,late])['primary']['failed_expansion']
    assert result['all']['entered']==0
    assert result['decisions'][0]['status']=='ENTRY_BBO_UNAVAILABLE'
    missing={k:v for k,v in early.items() if k!='signal_at'}
    missing['status']='UNDERLIER_GAP'
    result=research._evaluate_mechanism_replay([missing])['primary']['failed_expansion']
    assert result['decisions'][0]['status']=='UNDERLIER_GAP'


def test_regime_transition_is_causal_confirmed_and_not_an_entry_mismatch(research):
    at=ENTRY.replace(second=0)
    path={at+timedelta(minutes=i):7500+(i%2)*.2 for i in range(-20,1)}
    path.update({at+timedelta(minutes=i):7500+2*i for i in range(1,21)})
    row=dict(entry_at=ENTRY,family='condor',direction='NEUTRAL')
    first=research._regime_transition_trace(row,path,at+timedelta(minutes=20))
    assert first['status']=='TRANSITION'
    assert not first['entry_already_adverse']
    assert first['transition_at']>=at+timedelta(minutes=2)
    prefix={t:p for t,p in path.items() if t<=first['transition_at']}
    assert research._regime_transition_trace(row,prefix,at+timedelta(minutes=20))==first
    del prefix[at+timedelta(minutes=1)]
    assert research._regime_transition_trace(row,prefix,at+timedelta(minutes=20))['status']=='REGIME_PATH_GAP'
    trending={at+timedelta(minutes=i):7500+2*i for i in range(-20,21)}
    early=research._regime_transition_trace(row,trending,at+timedelta(minutes=20))
    assert early['entry_already_adverse']
    assert early['transition_at'] is None


def test_butterfly_favorable_trend_is_not_a_regime_failure(research):
    at=ENTRY.replace(second=0)
    path={at+timedelta(minutes=i):7500+(i%2)*.2 for i in range(-20,1)}
    path.update({at+timedelta(minutes=i):7500+2*i for i in range(1,21)})
    row=dict(entry_at=ENTRY,family='butterfly',direction='UP')
    result=research._regime_transition_trace(row,path,at+timedelta(minutes=20))
    assert result['transition_at'] is None
    assert research._regime_transition_trace({**row,'direction':'DOWN'},path,at+timedelta(minutes=20))['transition_at']


def test_zero_variance_does_not_fabricate_infinite_expansion(research):
    at=ENTRY.replace(second=0)
    path={at+timedelta(minutes=i):7500+2*i for i in range(-15,1)}
    fact=research._causal_regime(path,at,0.)
    assert fact['state']=='TREND_UP'
    assert fact['expansion_ratio'] is None
