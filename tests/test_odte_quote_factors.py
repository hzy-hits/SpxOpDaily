from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from spx_spark.data_platform.research.odte_level_signals import OptionTick, UnderlierTick
from spx_spark.data_platform.research.odte_quote_factors import (
    ES_INSTRUMENT_ID,
    SPX_INSTRUMENT_ID,
    _best_vertical,
    _factor_correlations,
    _spot_returns,
    _top_factors,
    mine_session,
    session_sample_times,
)
from spx_spark.market_calendar import ET


SESSION_DATE = date(2026, 8, 17)


def _option_tick(
    at: datetime,
    *,
    bid: float,
    ask: float,
    delta: float,
) -> OptionTick:
    return OptionTick(
        at=at,
        bid=bid,
        ask=ask,
        mid=(bid + ask) / 2.0,
        source_at=at,
        delta=delta,
        implied_vol=0.20,
    )


class FakeQuoteStore:
    def __init__(self) -> None:
        gth = datetime(2026, 8, 16, 20, 15, tzinfo=ET).astimezone(timezone.utc)
        gth_later = datetime(2026, 8, 16, 20, 20, tzinfo=ET).astimezone(timezone.utc)
        prior_rth = datetime(2026, 8, 16, 15, 0, tzinfo=ET).astimezone(timezone.utc)
        rth = datetime(2026, 8, 17, 9, 30, tzinfo=ET).astimezone(timezone.utc)
        self.spx = [UnderlierTick(rth, 6001.0)]
        self.es = [
            UnderlierTick(prior_rth, 5000.0),
            UnderlierTick(gth, 6001.0),
            UnderlierTick(gth_later, 6013.0),
        ]

    def option_expiry_providers(self, **_kwargs) -> tuple[str, ...]:
        return ("schwab",)

    def underlier_series(self, *, instrument_id: str, **_kwargs) -> list[UnderlierTick]:
        if instrument_id == ES_INSTRUMENT_ID:
            return list(self.es)
        return list(self.spx)

    def load_option_window(self, **_kwargs) -> int:
        return 12

    def option_snapshot(self, *, as_of: datetime, **_kwargs):
        return {
            ("schwab", 5990.0, "P"): _option_tick(as_of, bid=1.0, ask=2.0, delta=-0.30),
            ("schwab", 6000.0, "P"): _option_tick(as_of, bid=4.0, ask=5.0, delta=-0.50),
            ("schwab", 6000.0, "C"): _option_tick(as_of, bid=4.0, ask=5.0, delta=0.50),
            ("schwab", 6010.0, "C"): _option_tick(as_of, bid=1.0, ask=2.0, delta=0.30),
            ("schwab", 6005.0, "P"): _option_tick(as_of, bid=1.0, ask=2.0, delta=-0.30),
            ("schwab", 6015.0, "P"): _option_tick(as_of, bid=4.0, ask=5.0, delta=-0.50),
            ("schwab", 6015.0, "C"): _option_tick(as_of, bid=4.0, ask=5.0, delta=0.50),
            ("schwab", 6025.0, "C"): _option_tick(as_of, bid=1.0, ask=2.0, delta=0.30),
        }

    def option_series(self, *, strike: float, start: datetime, end: datetime, **_kwargs):
        is_long = strike in {6000.0, 6015.0}
        return [
            _option_tick(
                start,
                bid=4.0 if is_long else 1.0,
                ask=5.0 if is_long else 2.0,
                delta=0.50 if is_long else 0.30,
            ),
            _option_tick(
                start + timedelta(hours=1),
                bid=0.0 if is_long else 0.5,
                ask=1.0,
                delta=0.50 if is_long else 0.30,
            ),
            _option_tick(
                end,
                bid=6.0 if is_long else 1.0,
                ask=7.0 if is_long else 2.0,
                delta=0.50 if is_long else 0.30,
            ),
        ]


def test_quote_only_miner_labels_both_ten_wide_sides_at_close_without_stop() -> None:
    result = mine_session(FakeQuoteStore(), session_date=SESSION_DATE)

    assert len(result.rows) == 6
    assert {(row["session_mode"], row["direction"]) for row in result.rows} == {
        ("gth", "call"),
        ("gth", "put"),
        ("rth", "call"),
        ("rth", "put"),
    }
    assert {row["width"] for row in result.rows} == {10.0}
    assert {row["entry_combo_ask"] for row in result.rows} == {4.0}
    assert {row["exit_combo_bid"] for row in result.rows} == {4.0}
    assert {row["pnl_hold_to_1545"] for row in result.rows} == {0.0}
    assert {row["label_policy"] for row in result.rows} == {
        "hold_to_1545_no_stop_no_trail"
    }
    assert {row["spot_source"] for row in result.rows if row["session_mode"] == "gth"} == {
        ES_INSTRUMENT_ID
    }
    assert {row["spot_source"] for row in result.rows if row["session_mode"] == "rth"} == {
        SPX_INSTRUMENT_ID
    }
    assert all(str(row["exit_at"]).endswith("19:45:00+00:00") for row in result.rows)


def test_gth_returns_use_same_session_es_change_and_ignore_prior_rth() -> None:
    result = mine_session(FakeQuoteStore(), session_date=SESSION_DATE)
    gth_later = [
        row
        for row in result.rows
        if row["session_mode"] == "gth"
        and str(row["decision_at"]).endswith("00:20:00+00:00")
    ]
    gth_open = [
        row
        for row in result.rows
        if row["session_mode"] == "gth"
        and str(row["decision_at"]).endswith("00:15:00+00:00")
    ]

    assert gth_later
    assert gth_open
    assert gth_later[0]["spot"] == 6013.0
    assert gth_later[0]["spot_ret_5m"] == pytest.approx(6013.0 / 6001.0 - 1.0)
    assert gth_later[0]["spot_ret_60m"] is None
    assert gth_open[0]["spot_ret_5m"] is None
    assert gth_open[0]["spot_ret_60m"] is None


def test_spot_returns_do_not_look_before_session_floor() -> None:
    decision_at = datetime(2026, 8, 17, 0, 20, tzinfo=timezone.utc)
    session_start = datetime(2026, 8, 17, 0, 15, tzinfo=timezone.utc)
    ticks = [
        UnderlierTick(datetime(2026, 8, 16, 23, 20, tzinfo=timezone.utc), 5000.0),
        UnderlierTick(session_start, 6000.0),
        UnderlierTick(decision_at, 6060.0),
    ]

    returns = _spot_returns(
        ticks,
        ticks[-1],
        decision_at,
        not_before=session_start,
    )

    assert returns["spot_ret_5m"] == pytest.approx(0.01)
    assert returns["spot_ret_60m"] is None


def test_five_minute_samples_split_exchange_open_gth_and_rth() -> None:
    samples = session_sample_times(SESSION_DATE)

    assert sum(mode == "gth" for _, mode in samples) == 158
    assert sum(mode == "rth" for _, mode in samples) == 76
    assert samples[0][0].astimezone(ET).strftime("%Y-%m-%d %H:%M") == "2026-08-16 20:15"
    assert samples[-1][0].astimezone(ET).strftime("%Y-%m-%d %H:%M") == "2026-08-17 15:45"


def test_vertical_selection_rejects_ticks_received_after_decision() -> None:
    decision_at = datetime(2026, 8, 17, 14, 0, tzinfo=timezone.utc)
    snapshot = {
        ("schwab", 6000.0, "C"): _option_tick(
            decision_at + timedelta(seconds=1), bid=4.0, ask=5.0, delta=0.50
        ),
        ("schwab", 6010.0, "C"): _option_tick(
            decision_at, bid=1.0, ask=2.0, delta=0.30
        ),
        ("ibkr", 6000.0, "C"): _option_tick(
            decision_at, bid=3.9, ask=5.1, delta=0.50
        ),
        ("ibkr", 6010.0, "C"): _option_tick(
            decision_at, bid=0.9, ask=2.1, delta=0.30
        ),
    }

    selected = _best_vertical(
        snapshot,
        long_strike=6000.0,
        short_strike=6010.0,
        right="C",
        as_of=decision_at,
    )

    assert selected is not None
    assert selected["provider"] == "ibkr"


def test_boolean_factors_are_correlated_and_top_lists_keep_their_sign() -> None:
    rows = [
        {
            "session_mode": "gth" if index < 2 else "rth",
            "session_gth": index < 2,
            "session_rth": index >= 2,
            "direction_call": index % 2 == 1,
            "pnl_hold_to_1545": float(index),
        }
        for index in range(4)
    ]

    correlations = _factor_correlations(rows)
    top = _top_factors(correlations)

    assert correlations["overall"]["session_gth"]["n"] == 4
    assert correlations["overall"]["direction_call"]["n"] == 4
    assert all(item["spearman_rho"] > 0 for item in top["positive"])
    assert all(item["spearman_rho"] < 0 for item in top["negative"])
