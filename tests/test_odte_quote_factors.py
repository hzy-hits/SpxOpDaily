from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from spx_spark.data_platform.research.odte_level_signals import OptionTick, UnderlierTick
from spx_spark.data_platform.research.odte_quote_factors import (
    _best_vertical,
    _factor_correlations,
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
        rth = datetime(2026, 8, 17, 9, 30, tzinfo=ET).astimezone(timezone.utc)
        self.underlier = [UnderlierTick(gth, 6001.0), UnderlierTick(rth, 6001.0)]

    def option_expiry_providers(self, **_kwargs) -> tuple[str, ...]:
        return ("schwab",)

    def underlier_series(self, **_kwargs) -> list[UnderlierTick]:
        return self.underlier

    def load_option_window(self, **_kwargs) -> int:
        return 12

    def option_snapshot(self, *, as_of: datetime, **_kwargs):
        return {
            ("schwab", 5990.0, "P"): _option_tick(
                as_of, bid=1.0, ask=2.0, delta=-0.30
            ),
            ("schwab", 6000.0, "P"): _option_tick(
                as_of, bid=4.0, ask=5.0, delta=-0.50
            ),
            ("schwab", 6000.0, "C"): _option_tick(
                as_of, bid=4.0, ask=5.0, delta=0.50
            ),
            ("schwab", 6010.0, "C"): _option_tick(
                as_of, bid=1.0, ask=2.0, delta=0.30
            ),
        }

    def option_series(self, *, strike: float, start: datetime, end: datetime, **_kwargs):
        is_long = strike == 6000.0
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

    assert len(result.rows) == 4
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
    assert all(str(row["exit_at"]).endswith("19:45:00+00:00") for row in result.rows)


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
