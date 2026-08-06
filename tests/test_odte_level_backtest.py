from __future__ import annotations

import json
import runpy
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from spx_spark.data_platform.research.odte_level_backtest import (
    _readiness_session_cohorts,
    _stats,
    _uses_put_session_cohort,
    aggregate,
    evaluate_signal,
    follow_through_pass,
    run,
    simulate_trade,
)
from spx_spark.data_platform.research.odte_level_quotes import QuoteStore, pick_provider
from spx_spark.data_platform.research.odte_level_signals import (
    PROFILES,
    SET_GTH_LEVEL_CANDIDATE,
    SET_TRADE_READY,
    OptionTick,
    Signal,
    Skip,
    Trade,
    UnderlierTick,
    expiry_close_at,
    formula_target,
    hour_bucket,
    load_confirmed_signals,
    load_gth_dip_signals,
    load_gth_level_candidate_signals,
    load_prefill_signals,
    load_trade_ready_signals,
    nearest_wall,
    next_exit_clock,
    right_for,
    round_strike,
    spread_strikes,
    trade_intent_coverage,
    wall_spread_structure,
)


def _profile(name: str):
    return next(profile for profile in PROFILES if profile.name == name)


T0 = datetime(2026, 7, 15, 14, 0, 0, tzinfo=timezone.utc)
ENTRY = T0 + timedelta(seconds=15)


def _signal(**overrides) -> Signal:
    base = dict(
        set_name="confirmed",
        key="k1",
        at=T0,
        direction="up",
        level=7550.0,
        strike=7550.0,
        expiry=date(2026, 7, 15),
        entry_at=ENTRY,
        walls=(7560.0, 7540.0),
    )
    return Signal(**{**base, **overrides})


def _ready_signal(**overrides) -> Signal:
    base = dict(
        set_name=SET_TRADE_READY,
        key="intent:ready",
        at=T0,
        direction="up",
        level=7550.0,
        strike=7550.0,
        expiry=date(2026, 7, 15),
        entry_at=T0,
        thesis="level_breakout_call",
        entry_limit=10.0,
        entry_expires_at=T0 + timedelta(seconds=20),
        entry_provider="schwab",
        decision_spot=7552.0,
        invalidation_level=7547.0,
        invalidation_buffer=0.0,
        target_mode="recorded",
        target_level=7560.0,
        recorded_time_stop_at=T0 + timedelta(minutes=15),
        contract_id="option:SPX:SPXW:20260715:7550:C",
    )
    return Signal(**{**base, **overrides})


def _tick(at: datetime, bid: float, ask: float, mid: float | None = None) -> OptionTick:
    return OptionTick(at=at, bid=bid, ask=ask, mid=(bid + ask) / 2 if mid is None else mid)


def _flat_series(start: datetime, seconds: int, step: int = 30, bid=9.8, ask=10.0):
    return [_tick(start + timedelta(seconds=s), bid, ask) for s in range(0, seconds + 1, step)]


def _flat_underlier(price: float, start: datetime = T0, seconds: int = 2400, step: int = 5):
    return [
        UnderlierTick(at=start + timedelta(seconds=s), price=price)
        for s in range(0, seconds + 1, step)
    ]


class _MemoryQuoteStore:
    def __init__(
        self,
        option_ticks: dict[float, list[OptionTick]],
        underlier: list[UnderlierTick],
        *,
        delta_strike: float = 7550.0,
    ) -> None:
        self.option_ticks = option_ticks
        self.underlier = underlier
        self.delta_strike = delta_strike
        self.delta_select_calls = 0

    def option_series(self, *, strike: float, start: datetime, end: datetime, **_kwargs):
        return [tick for tick in self.option_ticks.get(strike, []) if start <= tick.at <= end]

    def underlier_series(self, *, start: datetime, end: datetime, **_kwargs):
        return [tick for tick in self.underlier if start <= tick.at <= end]

    def select_delta_strike(self, **_kwargs):
        self.delta_select_calls += 1
        return self.delta_strike


def test_pick_provider_uses_earliest_executable_entry_not_future_coverage() -> None:
    class Store:
        def option_series(self, *, provider: str, start: datetime, end: datetime, **_kwargs):
            rows = {
                "schwab": [
                    _tick(ENTRY + timedelta(seconds=5), 9.8, 10.0),
                    *[
                        _tick(ENTRY + timedelta(minutes=minute), 9.8, 10.0)
                        for minute in range(1, 30)
                    ],
                ],
                "ibkr": [_tick(ENTRY + timedelta(seconds=1), 9.9, 10.1)],
            }[provider]
            return [tick for tick in rows if start <= tick.at <= end]

    selected = pick_provider(
        Store(),
        expiry=date(2026, 7, 15),
        strike=7550.0,
        right="C",
        t0=ENTRY,
        quote_side="ask",
    )
    assert selected == "ibkr"


def test_pick_provider_requires_executable_side_for_short_leg() -> None:
    class Store:
        def option_series(self, *, provider: str, start: datetime, end: datetime, **_kwargs):
            rows = {
                "schwab": [OptionTick(ENTRY + timedelta(seconds=1), None, 5.0, None)],
                "ibkr": [_tick(ENTRY + timedelta(seconds=2), 4.9, 5.0)],
            }[provider]
            return [tick for tick in rows if start <= tick.at <= end]

    selected = pick_provider(
        Store(),
        expiry=date(2026, 7, 15),
        strike=7555.0,
        right="C",
        t0=ENTRY,
        quote_side="bid",
    )
    assert selected == "ibkr"


def test_round_strike_ties_away_from_zero() -> None:
    assert round_strike(7552.5) == 7555.0
    assert round_strike(7557.5) == 7560.0
    assert round_strike(7552.4) == 7550.0
    assert round_strike(7557.6) == 7560.0
    assert round_strike(7555.0) == 7555.0


def test_spread_and_right_construction() -> None:
    assert right_for("up") == "C"
    assert right_for("down") == "P"
    assert spread_strikes("up", 7550.0, 5.0) == (7550.0, 7555.0)
    assert spread_strikes("down", 7550.0, 10.0) == (7550.0, 7540.0)


def test_nearest_wall_requires_profitable_side() -> None:
    assert nearest_wall(7550.0, (7560.0, 7555.0, 7540.0), 1) == 7555.0
    assert nearest_wall(7550.0, (7560.0, 7555.0, 7540.0), -1) == 7540.0
    assert nearest_wall(7550.0, (7550.0,), 1) is None  # strictly profitable side
    assert nearest_wall(7550.0, (), 1) is None


def test_formula_target_uses_expected_move_floor() -> None:
    assert formula_target(7550.0, 1, None) is None
    assert formula_target(7550.0, 1, 20.0) == 7555.0  # max(5, 20*0.15) = 5
    assert formula_target(7550.0, -1, 40.0) == 7544.0


def test_hour_bucket_is_new_york_dst_aware() -> None:
    assert hour_bucket(datetime(2026, 7, 15, 13, 45, tzinfo=timezone.utc)) == "rth_open"
    assert hour_bucket(datetime(2026, 7, 15, 16, 0, tzinfo=timezone.utc)) == "rth_midday"
    assert hour_bucket(datetime(2026, 7, 15, 19, 30, tzinfo=timezone.utc)) == "rth_close"
    assert hour_bucket(datetime(2026, 7, 15, 7, 30, tzinfo=timezone.utc)) == "gth"
    # Winter RTH opens one UTC hour later.
    assert hour_bucket(datetime(2026, 12, 15, 14, 45, tzinfo=timezone.utc)) == "rth_open"
    assert hour_bucket(datetime(2026, 12, 15, 13, 45, tzinfo=timezone.utc)) == "gth"


def test_put_session_cohort_requires_rth_trade_intent_window() -> None:
    expiry = date(2026, 7, 15)

    def put_signal(at: datetime, *, set_name: str = SET_TRADE_READY) -> Signal:
        return _ready_signal(
            set_name=set_name,
            at=at,
            entry_at=at,
            direction="down",
            expiry=expiry,
            contract_id="option:SPX:SPXW:20260715:7550:P",
        )

    assert _uses_put_session_cohort(put_signal(datetime(2026, 7, 15, 13, 45, tzinfo=timezone.utc)))
    assert not _uses_put_session_cohort(
        put_signal(datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc))
    )
    assert not _uses_put_session_cohort(
        put_signal(datetime(2026, 7, 15, 17, 0, tzinfo=timezone.utc))
    )
    assert not _uses_put_session_cohort(
        put_signal(
            datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc),
            set_name="gth_dip",
        )
    )


def test_invalidation_exits_at_bid() -> None:
    long_series = _flat_series(ENTRY, 1800)
    underlier = _flat_underlier(7555.0)
    for i in range(60, 80):  # sustained dip below 7550 - 3
        underlier[i] = UnderlierTick(at=underlier[i].at, price=7546.9)
    result = simulate_trade(_signal(), "naked", long_series, None, underlier)
    assert isinstance(result, Trade)
    assert result.exit_reason == "invalidation"
    assert result.exit_px == 9.8  # long bid
    assert result.pnl_points == -0.2
    assert result.pnl_usd == -20.0
    assert result.entry_price_source == "lake_ask"


def test_stale_underlier_cannot_trigger_invalidation_or_target() -> None:
    long_series = _flat_series(ENTRY, 1800)
    stale = [
        UnderlierTick(at=ENTRY - timedelta(seconds=31), price=7540.0),
    ]
    result = simulate_trade(_signal(), "naked", long_series, None, stale)
    assert isinstance(result, Skip)
    assert result.reason == "pre_entry_underlier_unavailable"


def test_target_wall_exit_for_up_signal() -> None:
    long_series = _flat_series(ENTRY, 1800)
    underlier = _flat_underlier(7555.0)
    for i in range(30, 40):  # sustained touch of the 7560 wall
        underlier[i] = UnderlierTick(at=underlier[i].at, price=7560.0)
    result = simulate_trade(_signal(), "naked", long_series, None, underlier)
    assert result.exit_reason == "target_wall"
    assert result.exit_time == (ENTRY + timedelta(seconds=150)).isoformat()


def test_invalidation_at_first_ask_skips_entry_instead_of_booking_the_spread() -> None:
    long_series = [_tick(ENTRY, 12.9, 13.1)]  # mid 13.0 >= 1.3 * entry would trigger
    underlier = [UnderlierTick(at=ENTRY, price=7540.0)]  # invalidates first
    result = simulate_trade(_signal(decision_spot=7555.0), "naked", long_series, None, underlier)
    assert isinstance(result, Skip)
    assert result.reason == "invalidation_before_entry"


def test_nonproduction_target_at_first_ask_skips_entry() -> None:
    result = simulate_trade(
        _signal(decision_spot=7555.0),
        "naked",
        [_tick(ENTRY, 9.8, 10.0)],
        None,
        [UnderlierTick(at=ENTRY, price=7560.0)],
    )
    assert isinstance(result, Skip)
    assert result.reason == "target_before_entry"


def test_nonproduction_mid_path_underlier_gap_fails_closed() -> None:
    entry_at = T0 + timedelta(seconds=45)
    result = simulate_trade(
        _signal(entry_at=entry_at, decision_spot=7555.0),
        "naked",
        [_tick(entry_at, 9.8, 10.0)],
        None,
        [
            UnderlierTick(at=T0 + timedelta(seconds=5), price=7555.0),
            UnderlierTick(at=T0 + timedelta(seconds=40), price=7555.0),
            UnderlierTick(at=entry_at, price=7555.0),
        ],
    )
    assert isinstance(result, Skip)
    assert result.reason == "pre_entry_underlier_unavailable"


def test_pre_entry_boundary_before_later_path_gap_keeps_causal_reason() -> None:
    entry_at = T0 + timedelta(seconds=45)
    result = simulate_trade(
        _signal(entry_at=entry_at, decision_spot=7555.0),
        "naked",
        [_tick(entry_at, 9.8, 10.0)],
        None,
        [
            UnderlierTick(at=T0 + timedelta(seconds=5), price=7560.0),
            UnderlierTick(at=T0 + timedelta(seconds=40), price=7555.0),
            UnderlierTick(at=entry_at, price=7555.0),
        ],
    )
    assert isinstance(result, Skip)
    assert result.reason == "target_before_entry"


def test_profit_target_triggers_at_mid_and_exits_at_bid() -> None:
    long_series = _flat_series(ENTRY, 300, step=30)  # entry ask 10.0, mid 9.9
    long_series.append(_tick(ENTRY + timedelta(seconds=301), 13.4, 13.6))  # mid 13.5
    result = simulate_trade(
        _signal(), "naked", sorted(long_series, key=lambda t: t.at), None, _flat_underlier(7555.0)
    )
    assert result.exit_reason == "profit_target"
    assert result.exit_px == 13.4
    assert result.pnl_points == 3.4
    assert result.mfe_points == 3.5
    assert result.mae_points == -0.1


def test_time_stop_exits_at_bid_after_15_minutes() -> None:
    long_series = _flat_series(ENTRY, 1800)
    result = simulate_trade(_signal(), "naked", long_series, None, _flat_underlier(7555.0))
    assert result.exit_reason == "time_stop"
    assert datetime.fromisoformat(result.exit_time) >= ENTRY + timedelta(minutes=15)
    assert result.exit_px == 9.8


def test_time_stop_precedes_profit_rule_on_first_quote_after_deadline() -> None:
    long_series = [
        _tick(ENTRY, 9.8, 10.0),
        _tick(ENTRY + timedelta(minutes=15, seconds=1), 13.4, 13.6),
    ]
    result = simulate_trade(_signal(), "naked", long_series, None, _flat_underlier(7555.0))

    assert isinstance(result, Trade)
    assert result.exit_reason == "time_stop"
    assert result.exit_px == 13.4


def test_end_of_data_fallback_when_series_ends_early() -> None:
    long_series = _flat_series(ENTRY, 300)  # ends 5 minutes after entry
    result = simulate_trade(_signal(), "naked", long_series, None, _flat_underlier(7555.0))
    assert isinstance(result, Skip)
    assert result.reason == "no_fresh_exit_quote"


def test_end_of_data_fallback_requires_mark_near_scheduled_exit() -> None:
    long_series = [
        _tick(ENTRY, 9.8, 10.0),
        _tick(ENTRY + timedelta(minutes=15, seconds=-15), 9.8, 10.0),
    ]
    result = simulate_trade(_signal(), "naked", long_series, None, _flat_underlier(7555.0))
    assert isinstance(result, Trade)
    assert result.exit_reason == "end_of_data"
    assert result.exit_time == long_series[-1].at.isoformat()


def test_no_quote_skip_when_first_quote_too_old() -> None:
    long_series = _flat_series(ENTRY + timedelta(seconds=31), 600)
    result = simulate_trade(_signal(), "naked", long_series, None, _flat_underlier(7555.0))
    assert isinstance(result, Skip)
    assert result.reason == "no_quote"


def test_no_path_skip_for_recorded_entry_without_quotes() -> None:
    signal = _signal(entry_px=10.6, target_mode="formula")
    result = simulate_trade(signal, "naked", [], None, _flat_underlier(7555.0))
    assert isinstance(result, Skip)
    assert result.reason == "no_path"


def test_recorded_external_entry_skips_quote_lookup() -> None:
    signal = _signal(entry_px=10.6, target_mode="formula")
    # first quote is 45s after entry: would be no_quote for a lake entry, but the
    # an externally recorded non-S2 ask needs no entry quote
    long_series = _flat_series(ENTRY + timedelta(seconds=45), 1800)
    result = simulate_trade(signal, "naked", long_series, None, _flat_underlier(7555.0))
    assert isinstance(result, Trade)
    assert result.entry_px == 10.6
    assert result.entry_price_source == "recorded_ask"


def test_trade_ready_fills_first_ask_at_or_below_recorded_limit() -> None:
    long_series = [
        _tick(T0, 10.0, 10.2),
        _tick(T0 + timedelta(seconds=10), 9.8, 9.9),
        *_flat_series(T0 + timedelta(seconds=30), 15 * 60, bid=9.8, ask=10.2),
    ]
    result = simulate_trade(
        _ready_signal(),
        "naked",
        sorted(long_series, key=lambda tick: tick.at),
        None,
        _flat_underlier(7552.0, start=T0),
    )
    assert isinstance(result, Trade)
    assert result.entry_time == (T0 + timedelta(seconds=10)).isoformat()
    assert result.entry_px == 9.9
    assert result.entry_price_source == "lake_ask_at_or_below_recorded_limit"
    assert result.exit_time == (T0 + timedelta(minutes=15)).isoformat()


def test_trade_ready_does_not_assume_fill_when_limit_is_not_reached() -> None:
    result = simulate_trade(
        _ready_signal(),
        "naked",
        [_tick(T0, 10.0, 10.2), _tick(T0 + timedelta(seconds=19), 10.0, 10.1)],
        None,
        _flat_underlier(7552.0, start=T0, seconds=30),
    )
    assert isinstance(result, Skip)
    assert result.reason == "entry_limit_not_reached"


def test_trade_ready_entry_window_end_is_exclusive() -> None:
    result = simulate_trade(
        _ready_signal(),
        "naked",
        [_tick(T0, 10.0, 10.2), _tick(T0 + timedelta(seconds=20), 9.8, 9.9)],
        None,
        _flat_underlier(7552.0, start=T0, seconds=30),
    )
    assert isinstance(result, Skip)
    assert result.reason == "entry_limit_not_reached"


def test_trade_ready_latency_sensitivity_respects_exclusive_ttl_and_records_sides() -> None:
    long_series = [
        _tick(T0, 10.0, 10.2),
        _tick(T0 + timedelta(seconds=10), 9.8, 9.9),
        *_flat_series(T0 + timedelta(seconds=30), 15 * 60, bid=10.8, ask=11.0),
    ]
    reached = simulate_trade(
        _ready_signal(),
        "naked",
        sorted(long_series, key=lambda tick: tick.at),
        None,
        _flat_underlier(7552.0, start=T0),
        entry_latency_seconds=5,
    )
    expired = simulate_trade(
        _ready_signal(),
        "naked",
        sorted(long_series, key=lambda tick: tick.at),
        None,
        _flat_underlier(7552.0, start=T0),
        entry_latency_seconds=20,
    )
    assert isinstance(reached, Trade)
    assert reached.entry_latency_seconds == 5
    assert reached.entry_time == (T0 + timedelta(seconds=10)).isoformat()
    assert reached.executable_sides == (9.9, None, 10.8, None)
    assert isinstance(expired, Skip)
    assert expired.reason == "entry_window_expired"


def test_trade_ready_target_before_fill_skips_without_pnl() -> None:
    result = simulate_trade(
        _ready_signal(),
        "naked",
        [_tick(T0, 10.0, 10.2), _tick(T0 + timedelta(seconds=10), 9.8, 9.9)],
        None,
        [
            UnderlierTick(at=T0, price=7552.0),
            UnderlierTick(at=T0 + timedelta(seconds=5), price=7560.0),
        ],
    )
    assert isinstance(result, Skip)
    assert result.reason == "target_before_entry"


def test_trade_ready_invalidation_before_fill_skips_without_pnl() -> None:
    result = simulate_trade(
        _ready_signal(),
        "naked",
        [_tick(T0, 10.0, 10.2), _tick(T0 + timedelta(seconds=10), 9.8, 9.9)],
        None,
        [
            UnderlierTick(at=T0, price=7552.0),
            UnderlierTick(at=T0 + timedelta(seconds=5), price=7547.0),
        ],
    )
    assert isinstance(result, Skip)
    assert result.reason == "invalidation_before_entry"


def test_trade_ready_evaluate_uses_recorded_provider_and_is_naked_only() -> None:
    class Store(_MemoryQuoteStore):
        def __init__(self) -> None:
            ticks = [
                _tick(T0, 10.0, 10.2),
                _tick(T0 + timedelta(seconds=5), 9.8, 9.9),
                *_flat_series(T0 + timedelta(seconds=30), 15 * 60, bid=9.8, ask=10.2),
            ]
            super().__init__({7550.0: ticks}, _flat_underlier(7552.0, start=T0))
            self.providers: list[str] = []

        def option_series(self, *, provider: str, **kwargs):
            self.providers.append(provider)
            return super().option_series(**kwargs)

    store = Store()
    trades, skips = evaluate_signal(store, _ready_signal(), profiles=[_profile("baseline")])
    assert [trade.variant for trade in trades] == ["naked"]
    assert store.providers == ["schwab"]
    assert {(skip.variant, skip.reason) for skip in skips} == {
        ("spread5", "not_applicable"),
        ("spread10", "not_applicable"),
        ("spread_wall", "not_applicable"),
    }


def test_spread_entry_and_exit_economics() -> None:
    long_series = _flat_series(ENTRY, 1800, bid=9.8, ask=10.0)
    short_series = _flat_series(ENTRY, 1800, bid=5.0, ask=5.1)
    result = simulate_trade(
        _signal(), "spread5", long_series, short_series, _flat_underlier(7555.0)
    )
    assert result.exit_reason == "time_stop"
    assert result.entry_px == 5.0  # long ask - short bid
    assert result.exit_px == 4.7  # long bid - short ask
    assert result.pnl_usd == -30.0


def test_spread_skipped_when_short_leg_missing() -> None:
    long_series = _flat_series(ENTRY, 1800)
    result = simulate_trade(_signal(), "spread10", long_series, [], _flat_underlier(7555.0))
    assert isinstance(result, Skip)
    assert result.reason == "no_short_leg"


def test_evaluate_signal_never_builds_cross_provider_spread() -> None:
    class Store(_MemoryQuoteStore):
        def __init__(self) -> None:
            super().__init__({}, _flat_underlier(7555.0, start=ENTRY))
            self.requests: list[tuple[str, float]] = []

        def option_series(self, *, provider: str, strike: float, start, end, **_kwargs):
            self.requests.append((provider, strike))
            rows = {
                ("schwab", 7550.0): _flat_series(ENTRY, 1800, bid=9.8, ask=10.0),
                ("ibkr", 7560.0): _flat_series(ENTRY, 1800, bid=5.0, ask=5.1),
            }.get((provider, strike), [])
            return [tick for tick in rows if start <= tick.at <= end]

    store = Store()
    trades, skips = evaluate_signal(store, _signal(), profiles=[_profile("baseline")])

    assert not any(trade.variant == "spread10" for trade in trades)
    assert any(
        skip.variant == "spread10" and skip.reason == "no_short_leg"
        for skip in skips
    )
    assert ("ibkr", 7560.0) not in store.requests


def test_spread_entry_waits_for_both_legs_without_using_future_short_mark() -> None:
    long_series = [
        _tick(ENTRY, 9.8, 10.0),
        _tick(ENTRY + timedelta(seconds=6), 9.8, 10.0),
    ]
    short_series = [
        _tick(ENTRY + timedelta(seconds=4), 5.0, 5.1),
        _tick(ENTRY + timedelta(seconds=6), 5.0, 5.1),
        _tick(ENTRY + timedelta(minutes=15, seconds=4), 5.0, 5.1),
    ]
    long_series.append(_tick(ENTRY + timedelta(minutes=15, seconds=4), 9.8, 10.0))
    result = simulate_trade(
        _signal(), "spread5", long_series, short_series, _flat_underlier(7555.0)
    )
    assert isinstance(result, Trade)
    assert result.entry_time == (ENTRY + timedelta(seconds=4)).isoformat()
    assert datetime.fromisoformat(result.exit_time) >= datetime.fromisoformat(result.entry_time)


def test_spread_entry_rejects_leg_timestamp_skew() -> None:
    long_series = [_tick(ENTRY, 9.8, 10.0)]
    short_series = [_tick(ENTRY + timedelta(seconds=6), 4.9, 5.0)]
    result = simulate_trade(
        _signal(), "spread5", long_series, short_series, _flat_underlier(7555.0)
    )
    assert isinstance(result, Skip)
    assert result.reason == "entry_leg_skew"


def test_spread_entry_rejects_non_debit_or_above_width_quote() -> None:
    long_series = [_tick(ENTRY, 3.8, 4.0)]
    short_series = [_tick(ENTRY, 4.9, 5.0)]
    result = simulate_trade(
        _signal(), "spread5", long_series, short_series, _flat_underlier(7555.0)
    )
    assert isinstance(result, Skip)
    assert result.reason == "invalid_spread_debit"


def test_spread_does_not_ffill_stale_short_to_time_stop() -> None:
    long_series = _flat_series(ENTRY, 16 * 60, step=6, bid=9.8, ask=10.0)
    short_series = [_tick(ENTRY, 5.0, 5.1)]
    result = simulate_trade(
        _signal(), "spread5", long_series, short_series, _flat_underlier(7555.0)
    )
    assert isinstance(result, Skip)
    assert result.reason == "no_fresh_exit_quote"


def test_gth_dip_trough_invalidation_uses_zero_buffer() -> None:
    signal = _signal(
        set_name="gth_dip",
        level=7600.5,
        walls=(),
        target_mode="formula",
        invalidation_level=7594.0,
        invalidation_buffer=0.0,
        underlier_instrument="future:ES",
        expected_move_points=None,
    )
    long_series = _flat_series(ENTRY, 600)
    underlier = _flat_underlier(7600.0)
    for i in range(10, 20):  # sustained break of the trough
        underlier[i] = UnderlierTick(at=underlier[i].at, price=7593.9)
    result = simulate_trade(signal, "naked", long_series, None, underlier)
    assert result.exit_reason == "invalidation"


def test_follow_through_gate() -> None:
    underlier = [
        UnderlierTick(at=T0, price=7550.0),
        UnderlierTick(at=T0 + timedelta(seconds=15), price=7552.5),
        UnderlierTick(at=T0 + timedelta(seconds=30), price=7551.0),
    ]
    assert follow_through_pass(underlier, T0, 1) is True
    assert follow_through_pass(underlier, T0, -1) is False
    assert follow_through_pass(underlier, None, 1) is None
    assert follow_through_pass([], T0, 1) is None


def test_follow_through_gate_uses_trigger_and_em_scaled_threshold() -> None:
    underlier = [
        UnderlierTick(at=T0, price=7552.0),
        UnderlierTick(at=T0 + timedelta(seconds=15), price=7552.1),
    ]
    # Production compares 7552.1 against the 7550 trigger, not against the
    # already-elevated 7552.0 touch print.
    assert (
        follow_through_pass(
            underlier,
            T0,
            1,
            trigger_level=7550.0,
            expected_move_points=40.0,
        )
        is True
    )
    assert (
        follow_through_pass(
            underlier,
            T0,
            1,
            trigger_level=7550.0,
            expected_move_points=None,
        )
        is None
    )


def test_s2_evaluate_enters_after_gate_with_new_lake_ask() -> None:
    underlier = _flat_underlier(7552.1, start=T0, seconds=20 * 60, step=5)
    long_series = _flat_series(ENTRY, 16 * 60, step=30, bid=11.8, ask=12.0)
    store = _MemoryQuoteStore({7550.0: long_series}, underlier)
    signal = _signal(
        set_name="prefill",
        at=T0,
        entry_at=T0,
        first_touch_at=T0,
        entry_px=8.0,  # historical prefill: must not become the fill
        expected_move_points=40.0,
        target_mode="formula",
    )
    trades, skips = evaluate_signal(store, signal, profiles=[_profile("baseline")])
    naked = next(row for row in trades if row.variant == "naked")
    assert naked.ft_pass_15s2p is True
    assert naked.entry_time == ENTRY.isoformat()
    assert naked.entry_px == 12.0
    assert naked.entry_price_source == "lake_ask"
    assert ("spread_wall", "not_applicable") in {(skip.variant, skip.reason) for skip in skips}


def test_s2_evaluate_does_not_trade_failed_em_scaled_gate() -> None:
    underlier = _flat_underlier(7553.0, start=T0, seconds=60, step=5)
    store = _MemoryQuoteStore(
        {7550.0: _flat_series(ENTRY, 60, step=5, bid=11.8, ask=12.0)},
        underlier,
    )
    signal = _signal(
        set_name="prefill",
        at=T0,
        entry_at=T0,
        first_touch_at=T0,
        entry_px=8.0,
        expected_move_points=100.0,  # production threshold is 5 points
        target_mode="formula",
    )
    trades, skips = evaluate_signal(store, signal, profiles=[_profile("baseline")])
    assert trades == []
    assert ("naked", "follow_through_failed") in {(skip.variant, skip.reason) for skip in skips}
    # EM=100 raises the threshold from 2 to 5 points.
    assert (
        follow_through_pass(
            underlier,
            T0,
            1,
            trigger_level=7550.0,
            expected_move_points=100.0,
        )
        is False
    )


def _trade(pnl: float, **overrides) -> Trade:
    base = dict(
        set_name="confirmed",
        profile="baseline",
        key="k",
        at=T0.isoformat(),
        play="breakout",
        direction="up",
        level=7550.0,
        level_kind="flip_high",
        contract_id="option:SPX:SPXW:20260715:7550:C",
        short_contract_id=None,
        variant="naked",
        entry_time=ENTRY.isoformat(),
        entry_px=10.0,
        exit_time=(ENTRY + timedelta(minutes=15)).isoformat(),
        exit_px=10.0 + pnl / 100,
        exit_reason="time_stop",
        pnl_points=pnl / 100,
        pnl_usd=pnl,
        mfe_points=None,
        mae_points=None,
        underlier_source="index:SPX",
        trend_regime=None,
        session_bucket=None,
        ft_pass_15s2p=None,
        entry_price_source="lake_ask",
        h60_ret=None,
        h300_ret=None,
        h900_ret=None,
    )
    return Trade(**{**base, **overrides})


def test_stats_winrate_profit_factor_and_median() -> None:
    rows = [_trade(100.0), _trade(200.0), _trade(-50.0), _trade(-150.0), _trade(0.0)]
    stats = _stats(rows)
    assert stats["n"] == 5
    assert stats["winrate"] == 0.4
    assert stats["profit_factor"] == 1.5
    assert stats["median_pnl_usd"] == 0.0
    assert stats["total_pnl_usd"] == 100.0
    assert _stats([])["winrate"] is None


def test_aggregate_groups_by_profile_set_variant_and_slices() -> None:
    trades = [
        _trade(100.0, play="breakout", ft_pass_15s2p=None),
        _trade(-50.0, play="fade", variant="spread5", set_name="prefill", ft_pass_15s2p=True),
        _trade(80.0, play="fade", variant="spread5", set_name="prefill", ft_pass_15s2p=False),
    ]
    skips = [
        Skip(set_name="prefill", profile="baseline", key="k2", variant="naked", reason="no_quote")
    ]
    profiles = aggregate(trades, skips, {"confirmed": 1, "prefill": 2, "gth_dip": 0})
    sets = profiles["baseline"]
    naked = sets["confirmed"]["variants"]["naked"]
    assert naked["n"] == 1
    assert naked["slices"]["by_thesis"]["breakout"]["n"] == 1
    prefill_naked = sets["prefill"]["variants"]["naked"]
    assert prefill_naked["skipped"] == {"no_quote": 1}
    gate = sets["prefill"]["variants"]["spread5"]["ft_gate"]
    assert gate["gated"]["n"] == 1
    assert gate["ungated"]["avg_pnl_usd"] == 80.0
    # every configured profile is present in the aggregate
    assert set(profiles) == {profile.name for profile in PROFILES}


def test_aggregate_session_slices_prefer_spxw_expiry_and_fallback_to_entry_date() -> None:
    gth_entry = datetime(2026, 7, 14, 23, 30, tzinfo=timezone.utc)
    fallback_entry = datetime(2026, 7, 16, 14, 30, tzinfo=timezone.utc)
    trades = [
        _trade(
            100.0,
            entry_time=gth_entry.isoformat(),
            exit_time=(gth_entry + timedelta(minutes=15)).isoformat(),
            contract_id="option:SPX:SPXW:20260715:7550:C",
        ),
        _trade(
            -50.0,
            entry_time=fallback_entry.isoformat(),
            exit_time=(fallback_entry + timedelta(minutes=15)).isoformat(),
            contract_id="future:ES",
        ),
    ]

    profiles = aggregate(trades, [], {"confirmed": 2})
    slices = profiles["baseline"]["confirmed"]["variants"]["naked"]["slices"]

    assert slices["by_session_date"] == {
        "2026-07-15": {
            "n": 1,
            "winrate": 1.0,
            "avg_pnl_usd": 100.0,
            "total_pnl_usd": 100.0,
        },
        "2026-07-16": {
            "n": 1,
            "winrate": 0.0,
            "avg_pnl_usd": -50.0,
            "total_pnl_usd": -50.0,
        },
    }
    assert slices["by_weekday"]["Wednesday"]["n"] == 1
    assert slices["by_weekday"]["Thursday"]["n"] == 1


def test_wide_invalidation_scales_buffer_with_expected_move() -> None:
    signal = _signal(expected_move_points=40.0)  # buffer max(3, 0.15*40) = 6
    long_series = _flat_series(ENTRY, 1800)
    underlier = _flat_underlier(7555.0)
    for i in range(60, 80):  # dip to 7546: beyond 3.0 buffer, inside 6.0 buffer
        underlier[i] = UnderlierTick(at=underlier[i].at, price=7546.0)
    baseline = simulate_trade(signal, "naked", long_series, None, underlier, _profile("baseline"))
    wide = simulate_trade(
        signal, "naked", long_series, None, underlier, _profile("wide_invalidation")
    )
    assert baseline.exit_reason == "invalidation"
    assert wide.exit_reason == "time_stop"


def test_wide_invalidation_falls_back_to_fixed_buffer_without_em() -> None:
    signal = _signal(expected_move_points=None)
    long_series = _flat_series(ENTRY, 1800)
    underlier = _flat_underlier(7555.0)
    for i in range(60, 80):
        underlier[i] = UnderlierTick(at=underlier[i].at, price=7546.9)  # <= 7550 - 3
    wide = simulate_trade(
        signal, "naked", long_series, None, underlier, _profile("wide_invalidation")
    )
    assert wide.exit_reason == "invalidation"


def _trailing_ticks() -> list[OptionTick]:
    return [
        _tick(ENTRY, 9.8, 10.0),  # entry 10.0, mid 9.9
        _tick(ENTRY + timedelta(seconds=60), 11.5, 11.7),  # mid 11.6: arms at +15%
        _tick(ENTRY + timedelta(seconds=120), 10.9, 11.1),  # mid 11.0: giveback > 1/3
    ]


def test_trailing_tp_exits_after_giveback() -> None:
    result = simulate_trade(
        _signal(),
        "naked",
        _trailing_ticks(),
        None,
        _flat_underlier(7555.0),
        _profile("trailing_tp"),
    )
    assert result.exit_reason == "trailing_tp"
    assert result.exit_px == 10.9  # bid at the giveback tick
    assert result.pnl_usd == 90.0
    assert result.mfe_points == 1.6


def test_trailing_tp_floor_rises_with_new_peak() -> None:
    ticks = [
        _tick(ENTRY, 9.8, 10.0),
        _tick(ENTRY + timedelta(seconds=60), 11.5, 11.7),  # mid 11.6 arms
        _tick(ENTRY + timedelta(seconds=90), 12.3, 12.5),  # mid 12.4 new peak
        _tick(ENTRY + timedelta(seconds=120), 11.9, 12.1),  # mid 12.0 > 12.4-0.8: holds
        _tick(ENTRY + timedelta(seconds=150), 11.5, 11.7),  # mid 11.6 <= 11.6: exits
    ]
    result = simulate_trade(
        _signal(), "naked", ticks, None, _flat_underlier(7555.0), _profile("trailing_tp")
    )
    assert result.exit_reason == "trailing_tp"
    assert result.exit_time == (ENTRY + timedelta(seconds=150)).isoformat()
    assert result.exit_px == 11.5


def test_trailing_tp_not_armed_below_activation() -> None:
    ticks = [
        _tick(ENTRY, 9.8, 10.0),
        _tick(ENTRY + timedelta(seconds=60), 11.3, 11.5),  # mid 11.4 < 11.5: no arm
        _tick(ENTRY + timedelta(seconds=120), 9.0, 9.2),  # would give back, but never armed
        _tick(ENTRY + timedelta(minutes=15), 9.0, 9.2),
    ]
    result = simulate_trade(
        _signal(), "naked", ticks, None, _flat_underlier(7555.0), _profile("trailing_tp")
    )
    assert result.exit_reason == "time_stop"


def test_baseline_fixed_target_not_fooled_by_trailing_path() -> None:
    ticks = [*_trailing_ticks(), _tick(ENTRY + timedelta(minutes=15), 9.8, 10.0)]
    result = simulate_trade(
        _signal(), "naked", ticks, None, _flat_underlier(7555.0), _profile("baseline")
    )
    assert result.exit_reason == "time_stop"  # 11.6 < 1.3 * 10.0; no fixed trigger


def test_gth_360_extends_time_stop_only_for_gth_signals() -> None:
    long_series = _flat_series(ENTRY, 365 * 60, step=300)
    underlier = _flat_underlier(7555.0, seconds=370 * 60, step=300)
    expiry_close = expiry_close_at(date(2026, 7, 15))
    long_series.append(_tick(expiry_close, 9.8, 10.0))
    long_series.sort(key=lambda tick: tick.at)
    underlier.append(UnderlierTick(at=expiry_close, price=7555.0))
    underlier.sort(key=lambda tick: tick.at)
    gth_signal = _signal(underlier_instrument="future:ES", basis_points=45.0)
    gth_360 = simulate_trade(gth_signal, "naked", long_series, None, underlier, _profile("gth_360"))
    assert gth_360.exit_reason == "time_stop"
    assert datetime.fromisoformat(gth_360.exit_time) == expiry_close
    baseline = simulate_trade(
        gth_signal, "naked", long_series, None, underlier, _profile("baseline")
    )
    assert datetime.fromisoformat(baseline.exit_time) == ENTRY + timedelta(minutes=15)
    # RTH signals keep the 15-minute stop under gth_360
    rth = simulate_trade(_signal(), "naked", long_series, None, underlier, _profile("gth_360"))
    assert datetime.fromisoformat(rth.exit_time) == ENTRY + timedelta(minutes=15)


def test_rth_1300_profile_loads_exact_contract_through_clock_exit() -> None:
    rth_exit_tick = datetime(2026, 7, 15, 17, 0, 15, tzinfo=timezone.utc)
    long_series = _flat_series(
        ENTRY,
        int((rth_exit_tick - ENTRY).total_seconds()),
        step=30,
        bid=9.8,
        ask=10.0,
    )
    underlier = _flat_underlier(
        7555.0,
        seconds=int((rth_exit_tick - T0).total_seconds()) + 30,
        step=5,
    )
    store = _MemoryQuoteStore({7550.0: long_series}, underlier)

    trades, _ = evaluate_signal(
        store,
        _signal(contract_id="option:SPX:SPXW:20260715:7550:C"),
        profiles=[_profile("rth_1300")],
    )

    naked = next(row for row in trades if row.variant == "naked")
    assert naked.contract_id == "option:SPX:SPXW:20260715:7550:C"
    assert naked.entry_px == 10.0  # exact-contract ask
    assert naked.exit_px == 9.8  # first fresh exact-contract bid after 13:00 ET
    assert naked.exit_reason == "time_stop"
    assert datetime.fromisoformat(naked.exit_time) == rth_exit_tick


def test_rth_1300_profile_overrides_recorded_15_minute_time_stop() -> None:
    rth_exit_tick = datetime(2026, 7, 15, 17, 0, 0, tzinfo=timezone.utc)
    long_series = _flat_series(
        T0,
        int((rth_exit_tick - T0).total_seconds()),
        step=30,
        bid=9.8,
        ask=10.0,
    )
    underlier = _flat_underlier(
        7555.0,
        seconds=int((rth_exit_tick - T0).total_seconds()) + 30,
        step=5,
    )

    result = simulate_trade(
        _ready_signal(),
        "naked",
        long_series,
        None,
        underlier,
        _profile("rth_1300"),
    )

    assert result.exit_reason == "time_stop"
    assert datetime.fromisoformat(result.exit_time) == rth_exit_tick


def test_rth_1300_profile_requires_a_fresh_quote_at_or_after_exit() -> None:
    last_pre_exit = datetime(2026, 7, 15, 16, 59, 45, tzinfo=timezone.utc)
    long_series = _flat_series(
        ENTRY,
        int((last_pre_exit - ENTRY).total_seconds()),
        step=30,
        bid=9.8,
        ask=10.0,
    )
    if long_series[-1].at != last_pre_exit:
        long_series.append(_tick(last_pre_exit, 9.8, 10.0))
    underlier = _flat_underlier(
        7555.0,
        seconds=int((last_pre_exit - T0).total_seconds()),
        step=5,
    )

    result = simulate_trade(
        _signal(),
        "naked",
        long_series,
        None,
        underlier,
        _profile("rth_1300"),
    )

    assert isinstance(result, Skip)
    assert result.reason == "no_fresh_exit_quote_after_rth_exit"


def test_rth_1300_profile_rejects_entries_outside_rth_window() -> None:
    profile = _profile("rth_1300")
    premarket_entry = datetime(2026, 7, 15, 13, 0, 15, tzinfo=timezone.utc)
    premarket = simulate_trade(
        _signal(
            at=datetime(2026, 7, 15, 13, 0, tzinfo=timezone.utc),
            entry_at=premarket_entry,
        ),
        "naked",
        _flat_series(premarket_entry, 60),
        None,
        _flat_underlier(
            7555.0,
            start=datetime(2026, 7, 15, 13, 0, tzinfo=timezone.utc),
        ),
        profile,
    )
    after_cutoff_entry = datetime(2026, 7, 15, 17, 0, 15, tzinfo=timezone.utc)
    after_cutoff = simulate_trade(
        _signal(
            at=datetime(2026, 7, 15, 17, 0, tzinfo=timezone.utc),
            entry_at=after_cutoff_entry,
        ),
        "naked",
        _flat_series(after_cutoff_entry, 60),
        None,
        _flat_underlier(
            7555.0,
            start=datetime(2026, 7, 15, 17, 0, tzinfo=timezone.utc),
        ),
        profile,
    )
    before_analysis_entry = datetime(2026, 7, 15, 13, 40, 15, tzinfo=timezone.utc)
    before_analysis = simulate_trade(
        _signal(
            at=datetime(2026, 7, 15, 13, 40, tzinfo=timezone.utc),
            entry_at=before_analysis_entry,
        ),
        "naked",
        _flat_series(before_analysis_entry, 60),
        None,
        _flat_underlier(
            7555.0,
            start=datetime(2026, 7, 15, 13, 40, tzinfo=timezone.utc),
        ),
        profile,
    )

    assert isinstance(premarket, Skip)
    assert premarket.reason == "entry_before_rth_window"
    assert isinstance(before_analysis, Skip)
    assert before_analysis.reason == "entry_before_rth_window"
    assert isinstance(after_cutoff, Skip)
    assert after_cutoff.reason == "entry_after_exit_clock"


def _write_jsonl(path: Path, records: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(record if isinstance(record, str) else json.dumps(record))
            handle.write("\n")


def _trade_ready_record(**overrides) -> dict:
    base = {
        "status": "trade_ready",
        "intent_id": "intent:call",
        "event_id": "level:call",
        "evaluated_at": T0.isoformat(),
        "expires_at": (T0 + timedelta(seconds=20)).isoformat(),
        "time_stop_at": (T0 + timedelta(minutes=15)).isoformat(),
        "direction": "up",
        "contract_id": "option:SPX:SPXW:20260715:7550:C",
        "provider": "schwab",
        "entry_limit": 10.0,
        "trigger_level": 7550.0,
        "invalidation_spx": 7547.0,
        "target_spx": 7560.0,
        "spx_spot": 7552.0,
        "play": "level_breakout_call",
        "horizons": {"900": {"return_fraction": 99.0}},
    }
    return {**base, **overrides}


def test_load_trade_ready_uses_only_persisted_complete_terminal_decisions(
    tmp_path: Path,
) -> None:
    root = tmp_path / "features"
    _write_jsonl(
        root / "trade_intents/date=2026-07-15/events.jsonl",
        [
            {"status": "observing", "evaluated_at": T0.isoformat()},
            _trade_ready_record(),
            _trade_ready_record(
                evaluated_at=(T0 + timedelta(seconds=5)).isoformat(),
                provider="ibkr",
            ),  # duplicate intent: later row cannot replace the original
            _trade_ready_record(
                intent_id="intent:wrong-right",
                event_id="level:wrong-right",
                contract_id="option:SPX:SPXW:20260715:7550:P",
            ),
            _trade_ready_record(
                intent_id="intent:put",
                event_id="level:put",
                direction="down",
                contract_id="option:SPX:SPXW:20260715:7550:P",
                trigger_level=7550.0,
                invalidation_spx=7553.0,
                target_spx=7540.0,
                spx_spot=7548.0,
                play="level_breakout_put",
            ),
        ],
    )
    signals = load_trade_ready_signals(root)
    assert [signal.key for signal in signals] == ["intent:call", "intent:put"]
    call, put = signals
    assert call.entry_provider == "schwab"
    assert call.entry_limit == 10.0
    assert call.entry_expires_at == T0 + timedelta(seconds=20)
    assert call.target_level == 7560.0
    assert call.invalidation_level == 7547.0
    assert call.horizons == {}  # future outcome labels are not entry gates
    assert put.direction == "down"  # direction remains a cohort slice, not a ban


def test_trade_intent_coverage_keeps_observing_separate(tmp_path: Path) -> None:
    root = tmp_path / "features"
    _write_jsonl(
        root / "trade_intents/date=2026-07-15/events.jsonl",
        [
            {"status": "observing", "evaluated_at": T0.isoformat()},
            {
                "status": "blocked",
                "event_id": "level:block",
                "evaluated_at": T0.isoformat(),
            },
            {
                "status": "shadow_ready",
                "intent_id": "intent:put-shadow",
                "event_id": "level:put-shadow",
                "evaluated_at": T0.isoformat(),
            },
            _trade_ready_record(),
            _trade_ready_record(evaluated_at="2026-07-16T00:00:00+00:00"),
        ],
    )
    coverage = trade_intent_coverage(
        root,
        cutoff_at=datetime(2026, 7, 16, tzinfo=timezone.utc),
        last_complete_date=date(2026, 7, 15),
    )
    assert coverage["records_by_status"] == {
        "observing": 1,
        "blocked": 1,
        "shadow_ready": 1,
        "trade_ready": 1,
    }
    assert coverage["evaluation_records"] == 4
    assert coverage["distinct_shadow_ready_intent_ids"] == 1
    assert "excluded from pass/block" in coverage["observing_semantics"]


def test_load_confirmed_signals_dedupes_and_normalizes(tmp_path: Path) -> None:
    root = tmp_path / "features"
    confirmed = {
        "at": "2026-07-15T14:54:33+00:00",
        "current_phase": "confirmed",
        "previous_phase": "retest",
        "event_id": "level:aaa",
        "direction": "down",
        "thesis": "breakout",
        "level": 7560.0,
        "spx_level": 7560.0,
        "level_kind": "flip_low",
        "levels": {
            "call_wall": 7600.0,
            "flip_high": 7565.0,
            "flip_low": 7560.0,
            "put_wall": 7550.0,
        },
        "trigger_coordinate_kind": "official_spx",
        "trigger_basis_points": 44.46,
        "spot": 7554.0,
    }
    gth = {
        **confirmed,
        "at": "2026-07-15T03:28:21+00:00",
        "event_id": "level:bbb",
        "direction": "up",
        "level": 7600.43,
        "spx_level": 7555.0,
        "levels": {
            "call_wall": 7645.43,
            "flip_high": 7600.43,
            "flip_low": 7595.43,
            "put_wall": 7595.43,
        },
        "trigger_coordinate_kind": "es_equivalent",
        "trigger_basis_points": 45.43,
        "spot": 7601.43,
    }
    _write_jsonl(
        root / "level_decision_audit/date=2026-07-15/transitions.jsonl",
        [
            confirmed,
            {**confirmed, "previous_phase": "confirmed"},  # re-entry: skip
            {**confirmed, "current_phase": "testing"},  # not confirmed: skip
            "{not json",  # malformed: skip
            gth,
            gth,  # duplicate event_id: skip
        ],
    )
    signals = load_confirmed_signals(root)
    assert len(signals) == 2
    by_key = {signal.key: signal for signal in signals}
    first = by_key["level:aaa"]
    assert first.direction == "down"
    assert first.entry_at == first.at + timedelta(seconds=15)
    assert first.underlier_instrument == "index:SPX"
    assert first.basis_points == 0.0
    assert first.strike == 7560.0
    assert first.contract_id == "option:SPX:SPXW:20260715:7560:P"
    assert first.decision_spot == 7554.0
    second = by_key["level:bbb"]
    assert second.underlier_instrument == "future:ES"
    assert second.basis_points == 45.43
    assert second.level == 7555.0
    assert second.decision_spot == 7556.0
    assert second.walls == (7550.0, 7550.0, 7555.0, 7600.0)  # es coords minus basis


def test_load_confirmed_signals_cools_down_legacy_duplicate_economic_events(
    tmp_path: Path,
) -> None:
    root = tmp_path / "features"

    def record(at: str, event_id: str, *, level: float = 7550.0) -> dict:
        return {
            "at": at,
            "current_phase": "confirmed",
            "previous_phase": "retest",
            "event_id": event_id,
            "direction": "up",
            "level": level,
            "spx_level": level,
            "level_kind": "flip_high",
            "levels": {"call_wall": 7600.0},
            "spot": 7555.0,
            "trigger_coordinate_kind": "official_spx",
        }

    _write_jsonl(
        root / "level_decision_audit/date=2026-07-15/transitions.jsonl",
        [
            record("2026-07-15T14:00:00+00:00", "level:first"),
            record("2026-07-15T14:09:59+00:00", "level:duplicate"),
            record("2026-07-15T14:10:00+00:00", "level:after-cooldown"),
            record("2026-07-15T14:11:00+00:00", "level:different-level", level=7555.0),
        ],
    )
    signals = load_confirmed_signals(root)
    assert [signal.key for signal in signals] == [
        "level:first",
        "level:after-cooldown",
        "level:different-level",
    ]


def test_load_confirmed_signals_cooldown_uses_expiry_across_gth_midnight(
    tmp_path: Path,
) -> None:
    root = tmp_path / "features"

    def record(at: str, event_id: str) -> dict:
        return {
            "at": at,
            "current_phase": "confirmed",
            "previous_phase": "retest",
            "event_id": event_id,
            "direction": "up",
            "level": 7550.0,
            "spx_level": 7550.0,
            "level_kind": "flip_high",
            "levels": {"call_wall": 7600.0},
            "spot": 7555.0,
            "trigger_coordinate_kind": "official_spx",
        }

    _write_jsonl(
        root / "level_decision_audit/date=2026-07-15/transitions.jsonl",
        [
            record("2026-07-15T03:59:00+00:00", "level:before-et-midnight"),
            record("2026-07-15T04:05:00+00:00", "level:after-et-midnight"),
        ],
    )

    signals = load_confirmed_signals(root)

    assert [signal.key for signal in signals] == ["level:before-et-midnight"]
    assert signals[0].expiry == date(2026, 7, 15)


def test_load_confirmed_signals_first_incomplete_event_id_cannot_be_repaired(
    tmp_path: Path,
) -> None:
    root = tmp_path / "features"
    valid = {
        "at": "2026-07-15T14:00:01+00:00",
        "current_phase": "confirmed",
        "previous_phase": "retest",
        "event_id": "level:incomplete-first",
        "direction": "up",
        "level": 7550.0,
        "spx_level": 7550.0,
        "level_kind": "flip_high",
        "levels": {"call_wall": 7600.0},
        "spot": 7555.0,
        "trigger_coordinate_kind": "official_spx",
    }
    _write_jsonl(
        root / "level_decision_audit/date=2026-07-15/transitions.jsonl",
        [
            {**valid, "at": "2026-07-15T14:00:00+00:00", "spot": None},
            valid,
        ],
    )

    assert load_confirmed_signals(root) == []


def test_load_prefill_signals_filters_and_parses(tmp_path: Path) -> None:
    root = tmp_path / "features"
    base = {
        "key": "level:x|level_breakout_call|option:SPX:SPXW:20260715:7550:C",
        "contract_id": "option:SPX:SPXW:20260715:7550:C",
        "play": "level_breakout_call",
        "touched": True,
        "spx_level": 7550.0,
        "level_kind": "call_wall",
        "prefill_ask": 10.6,
        "prefill_at": "2026-07-15T14:36:05+00:00",
        "first_touch_at": "2026-07-15T14:36:05+00:00",
        "trend_regime": "bullish",
        "session_bucket": "rth_open",
        "expected_move_points": 18.66,
        "trigger_coordinate_kind": "official_spx",
        "horizons": {"300": {"return_fraction": 0.16}},
    }
    _write_jsonl(
        root / "pricing_outcomes/date=2026-07-15/outcomes.jsonl",
        [
            base,
            {
                **base,
                "first_touch_at": "2026-07-15T14:50:05+00:00",
            },  # a later economic touch remains independent
            {
                **base,
                "key": "level:regenerated|level_breakout_call|option:SPX:SPXW:20260715:7550:C",
            },  # regenerated event id for the same economic touch: collapse
            {**base, "touched": False, "key": "skip:untouched"},
            {
                **base,
                "key": "level:y|level_fade_put|option:SPX:SPXW:20260715:7550:P",
                "contract_id": "option:SPX:SPXW:20260715:7550:P",
                "play": "level_fade_put",
                "prefill_ask": None,
                "prefill_at": None,
            },
            {**base, "key": "bad", "contract_id": "bogus"},
        ],
    )
    signals = load_prefill_signals(root)
    assert len(signals) == 3
    early = next(
        signal for signal in signals if signal.direction == "up" and signal.at.minute == 36
    )
    assert early.entry_px is None
    assert early.entry_at == datetime(2026, 7, 15, 14, 36, 20, tzinfo=timezone.utc)
    assert early.strike == 7550.0
    assert early.expiry == date(2026, 7, 15)
    late = next(signal for signal in signals if signal.direction == "up" and signal.at.minute == 50)
    assert late.entry_at == datetime(2026, 7, 15, 14, 50, 20, tzinfo=timezone.utc)
    fallback = next(signal for signal in signals if signal.direction == "down")
    assert fallback.entry_px is None
    assert fallback.entry_at == datetime(2026, 7, 15, 14, 36, 20, tzinfo=timezone.utc)


def test_prefill_coordinate_kind_uses_native_underlier_without_fixed_basis(
    tmp_path: Path,
) -> None:
    root = tmp_path / "features"
    base = {
        "key": "chain",
        "contract_id": "option:SPX:SPXW:20260715:7550:C",
        "play": "level_breakout_call",
        "touched": True,
        "spx_level": 7550.0,
        "trigger_target": 7550.0,
        "first_touch_at": "2026-07-15T14:36:05+00:00",
        "expected_move_points": 20.0,
        "trigger_coordinate_kind": "chain_implied_spx",
        "trigger_instrument_id": "synthetic:SPXW_PARITY",
    }
    _write_jsonl(
        root / "pricing_outcomes/date=2026-07-15/outcomes.jsonl",
        [
            base,
            {
                **base,
                "key": "chain:wrong-path",
                "trigger_instrument_id": "index:SPX",
            },
            {
                **base,
                "key": "es",
                "contract_id": "option:SPX:SPXW:20260715:7550:P",
                "play": "level_breakout_put",
                "spx_level": 7550.0,
                "trigger_target": 7595.43,
                "trigger_coordinate_kind": "es_equivalent",
                "trigger_instrument_id": "future:ES",
            },
        ],
    )
    signals = {signal.key: signal for signal in load_prefill_signals(root)}
    assert set(signals) == {"chain", "es"}
    assert signals["chain"].underlier_instrument == "synthetic:SPXW_PARITY"
    assert signals["chain"].level == 7550.0
    assert signals["chain"].basis_points == 0.0
    assert signals["es"].underlier_instrument == "future:ES"
    assert signals["es"].level == 7595.43
    assert signals["es"].basis_points == 0.0


def test_load_gth_dip_signals_uses_trough_invalidation(tmp_path: Path) -> None:
    root = tmp_path / "features"
    _write_jsonl(
        root / "gth_dip_reclaim/date=2026-07-16/events.jsonl",
        [
            {
                "confirmed_at": "2026-07-16T06:09:55.427+00:00",
                "direction": "up",
                "es": 7621.75,
                "event_id": "gth-dip:35c0",
                "expected_move_points": 25.4575,
                "kind": "gth_dip_reclaim_call",
                "session_date": "2026-07-16",
                "trough": 7613.5,
            }
        ],
    )
    signals = load_gth_dip_signals(root)
    assert len(signals) == 1
    signal = signals[0]
    assert signal.strike is None
    assert signal.expiry == date(2026, 7, 16)
    assert signal.invalidation_level == 7613.5
    assert signal.invalidation_buffer == 0.0
    assert signal.underlier_instrument == "future:ES"
    assert signal.level == 7621.75


def test_current_gth_candidate_replay_freezes_runtime_spread_and_decision_ask(
    tmp_path: Path,
) -> None:
    root = tmp_path / "features"
    evaluated_at = datetime(2026, 7, 16, 6, 9, 55, tzinfo=timezone.utc)
    long_id = "option:SPX:SPXW:20260716:7575:C"
    short_id = "option:SPX:SPXW:20260716:7615:C"
    snapshot = {
        "at": evaluated_at.isoformat(),
        "bid": 7.7,
        "mid": 7.85,
        "ask": 8.0,
        "quality": {"status": "ok"},
        "long": {
            "bid": 9.8,
            "mid": 9.9,
            "ask": 10.0,
            "provider": "ibkr",
            "source_at": evaluated_at.isoformat(),
            "transport_at": evaluated_at.isoformat(),
            "quality": {"status": "ok"},
        },
        "short": {
            "bid": 2.0,
            "mid": 2.05,
            "ask": 2.1,
            "provider": "ibkr",
            "source_at": evaluated_at.isoformat(),
            "transport_at": evaluated_at.isoformat(),
            "quality": {"status": "ok"},
        },
    }
    candidate = {
        "schema_version": 3,
        "policy_version": "gth_level_manual_candidate.v1+sha256:test",
        "valid_until": (evaluated_at + timedelta(seconds=20)).isoformat(),
        "coordinate": {
            "kind": "chain_implied_spx",
            "instrument_id": "synthetic:SPXW_PARITY",
            "observed_value": 7608.25,
            "target_value": 7640.0,
            "spx_observed_value": 7608.25,
            "basis_points": 0.0,
            "as_of": evaluated_at.isoformat(),
        },
        "block_reasons": [],
        "event": "gth_level_manual_candidate_evaluated",
        "kind": "gth_spxw_level_manual_spread_candidate",
        "candidate_id": "gth-level-manual:recorded",
        "source_signal_id": "level:recorded",
        "strategy_id": "gth_level_manual_candidate",
        "strategy_lane": "gth_level_manual_candidate",
        "lifecycle_status": "legacy_production",
        "runtime_status": "production_runtime",
        "status": "manual_ready",
        "manual_action_eligible": True,
        "execution_eligible": False,
        "broker_submission_allowed": False,
        "automatic_ordering": False,
        "session_date": "2026-07-16",
        "evaluated_at": evaluated_at.isoformat(),
        "direction": "up",
        "position_type": "call_debit_spread",
        "path_kind": "upper_acceptance_call",
        "long_contract_id": long_id,
        "short_contract_id": short_id,
        "contract_id": f"{long_id}|-{short_id}",
        "spread_width_points": 40.0,
        "decision_bid": 7.7,
        "decision_mid": 7.85,
        "decision_ask": 8.0,
        "entry_limit": 8.0,
        "trigger_level": 7621.75,
        "target_spx": 7640.0,
        "invalidation_spx": 7600.0,
        "invalidation_es": 7613.5,
        "exit_at": (evaluated_at + timedelta(minutes=15)).isoformat(),
        "exact_spread_snapshot": snapshot,
    }
    expired = {
        **candidate,
        "candidate_id": "gth-level-manual:expired",
        "valid_until": evaluated_at.isoformat(),
    }
    wrong_provider = {
        **candidate,
        "candidate_id": "gth-level-manual:wrong-provider",
        "exact_spread_snapshot": {
            **snapshot,
            "long": {**snapshot["long"], "provider": "schwab"},
        },
    }
    blocked = {
        **candidate,
        "candidate_id": "gth-level-manual:blocked",
        "status": "blocked",
        "manual_action_eligible": False,
        "block_reasons": ["spread_reward_risk_insufficient"],
    }
    suppressed = {
        **candidate,
        "candidate_id": "gth-level-manual:suppressed",
        "status": "suppressed",
        "manual_action_eligible": False,
    }
    _write_jsonl(
        root / "gth_level_manual_candidates/date=2026-07-16/events.jsonl",
        [candidate, expired, wrong_provider, blocked, suppressed],
    )

    loaded = load_gth_level_candidate_signals(root)
    assert [signal.key for signal in loaded] == ["gth-level-manual:recorded"]
    signal = loaded[0]
    assert signal.set_name == SET_GTH_LEVEL_CANDIDATE
    assert signal.contract_id == long_id
    assert signal.recorded_short_strike == 7615.0
    assert signal.entry_provider == "ibkr"
    assert signal.entry_px == signal.decision_ask == 8.0
    assert signal.entry_expires_at == evaluated_at + timedelta(seconds=20)
    long_series = _flat_series(evaluated_at, 16 * 60, step=30, bid=9.8, ask=10.0)
    short_series = _flat_series(evaluated_at, 16 * 60, step=30, bid=1.9, ask=2.0)
    underlier = _flat_underlier(7621.75, start=evaluated_at, seconds=20 * 60, step=5)
    store = _MemoryQuoteStore(
        {7575.0: long_series, 7615.0: short_series},
        underlier,
        delta_strike=7550.0,
    )

    trades, skips = evaluate_signal(store, signal, profiles=[_profile("baseline")])

    spread_trade = next(row for row in trades if row.variant == "spread_wall")
    assert store.delta_select_calls == 0
    assert spread_trade.contract_id == long_id
    assert spread_trade.short_contract_id == short_id
    assert spread_trade.entry_px == 8.0
    assert {skip.reason for skip in skips} == {"not_applicable"}


def test_current_gth_candidate_latency_reprices_all_four_executable_sides() -> None:
    evaluated_at = datetime(2026, 7, 16, 6, 9, 55, tzinfo=timezone.utc)
    signal = Signal(
        set_name=SET_GTH_LEVEL_CANDIDATE,
        key="gth-level-manual:latency",
        at=evaluated_at,
        direction="up",
        level=7621.75,
        strike=7575.0,
        expiry=date(2026, 7, 16),
        entry_at=evaluated_at,
        thesis="upper_acceptance_call",
        entry_px=8.0,
        entry_limit=8.0,
        entry_expires_at=evaluated_at + timedelta(seconds=20),
        entry_provider="ibkr",
        decision_bid=7.7,
        decision_ask=8.0,
        decision_leg_sides=(9.8, 10.0, 2.0, 2.1),
        target_level=7640.0,
        invalidation_level=7600.0,
        invalidation_buffer=0.0,
        recorded_time_stop_at=evaluated_at + timedelta(minutes=15),
        underlier_instrument="future:ES",
        contract_id="option:SPX:SPXW:20260716:7575:C",
        recorded_short_strike=7615.0,
        recorded_spread_width=40.0,
    )
    long_series = _flat_series(evaluated_at, 16 * 60, step=30, bid=9.8, ask=10.0)
    short_series = _flat_series(evaluated_at, 16 * 60, step=30, bid=2.0, ask=2.1)
    result = simulate_trade(
        signal,
        "spread_wall",
        long_series,
        short_series,
        _flat_underlier(7621.75, start=evaluated_at, seconds=20 * 60, step=5),
        _profile("baseline"),
        spread_width=40.0,
        short_contract_id="option:SPX:SPXW:20260716:7615:C",
        long_provider="ibkr",
        short_provider="ibkr",
        entry_latency_seconds=5,
    )
    assert isinstance(result, Trade)
    assert result.entry_time == (evaluated_at + timedelta(seconds=5)).isoformat()
    assert result.entry_px == 8.0
    assert result.executable_sides == (10.0, 2.0, 9.8, 2.1)
    assert result.entry_price_source == "lake_ibkr_long_ask_short_bid_after_latency"


def test_legacy_gth_dip_is_excluded_from_current_runtime_spread_replay(tmp_path: Path) -> None:
    signal = _gth_signal(
        strike=None,
        contract_id=None,
        recorded_short_strike=None,
        recorded_spread_width=None,
    )
    long_series = _flat_series(ENTRY, 16 * 60, step=30)
    underlier = _flat_underlier(7555.0, seconds=20 * 60, step=5)
    store = _MemoryQuoteStore({7550.0: long_series}, underlier)
    _, skips = evaluate_signal(store, signal, profiles=[_profile("baseline")])
    assert ("spread_wall", "not_applicable") in {
        (skip.variant, skip.reason) for skip in skips
    }


# ---------------------------------------------------------------------------
# spread_wall structure + GTH clock profiles
# ---------------------------------------------------------------------------


def test_wall_spread_structure_anchors_on_nearest_wall() -> None:
    short, width, anchor = wall_spread_structure(
        direction="up",
        long_strike=7555.0,
        wall_map={"flip_high": 7562.0, "call_wall": 7600.0},
        expected_move_points=None,
    )
    # flip_high rounds to 7560 (5 < 15 min width, skipped); call_wall anchors
    assert (short, width, anchor) == (7600.0, 45.0, "structure_wall")


def test_wall_spread_structure_caps_width_and_mirrors_down() -> None:
    short, width, _ = wall_spread_structure(
        direction="up",
        long_strike=7500.0,
        wall_map={"call_wall": 7700.0},
        expected_move_points=None,
    )
    assert (short, width) == (7575.0, 75.0)  # capped at max width
    short, width, anchor = wall_spread_structure(
        direction="down",
        long_strike=7550.0,
        wall_map={"flip_low": 7546.0, "put_wall": 7490.0},
        expected_move_points=None,
    )
    assert (short, width, anchor) == (7490.0, 60.0, "structure_wall")


def test_wall_spread_structure_em_then_default_fallback() -> None:
    short, width, anchor = wall_spread_structure(
        direction="up", long_strike=7550.0, wall_map={}, expected_move_points=40.0
    )
    assert (short, width, anchor) == (7570.0, 20.0, "expected_move")
    short, width, anchor = wall_spread_structure(
        direction="down", long_strike=7550.0, wall_map={}, expected_move_points=None
    )
    assert (short, width, anchor) == (7500.0, 50.0, "default")


def test_exit_clock_is_expiry_anchored_dst_aware_and_never_rolls() -> None:
    morning = datetime(2026, 7, 15, 6, 0, tzinfo=timezone.utc)
    expiry = date(2026, 7, 15)
    assert next_exit_clock(morning, expiry) == datetime(2026, 7, 15, 13, 45, tzinfo=timezone.utc)
    evening = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc)
    assert next_exit_clock(evening, expiry) == datetime(2026, 7, 15, 13, 45, tzinfo=timezone.utc)
    assert next_exit_clock(
        datetime(2026, 12, 15, 6, 0, tzinfo=timezone.utc), date(2026, 12, 15)
    ) == datetime(2026, 12, 15, 14, 45, tzinfo=timezone.utc)
    assert next_exit_clock(morning, expiry, hhmm=(13, 0)) == datetime(
        2026, 7, 15, 17, 0, tzinfo=timezone.utc
    )
    assert next_exit_clock(
        datetime(2026, 12, 15, 6, 0, tzinfo=timezone.utc),
        date(2026, 12, 15),
        hhmm=(13, 0),
    ) == datetime(2026, 12, 15, 18, 0, tzinfo=timezone.utc)
    assert expiry_close_at(date(2026, 12, 15)) == datetime(2026, 12, 15, 21, 0, tzinfo=timezone.utc)


def _gth_signal(**overrides) -> Signal:
    base = dict(
        set_name="gth_dip",
        underlier_instrument="future:ES",
        basis_points=45.0,
        invalidation_level=7540.0,
        invalidation_buffer=0.0,
        walls=(),
        target_mode="formula",
    )
    return _signal(**{**base, **overrides})


GTH_AT = datetime(2026, 7, 15, 6, 0, tzinfo=timezone.utc)
GTH_ENTRY = GTH_AT + timedelta(seconds=15)


def test_sat85_exits_at_saturation() -> None:
    long_series = [
        _tick(GTH_ENTRY, 9.8, 10.0),
        _tick(GTH_ENTRY + timedelta(seconds=60), 19.8, 20.0),
    ]
    short_series = _flat_series(GTH_ENTRY, 120, step=60, bid=1.0, ask=1.2)
    result = simulate_trade(
        _gth_signal(at=GTH_AT, entry_at=GTH_ENTRY),
        "spread_wall",
        long_series,
        short_series,
        _flat_underlier(7555.0, start=GTH_AT),
        _profile("sat85"),
        spread_width=20.0,
    )
    assert result.exit_reason == "saturation"
    assert result.exit_px == 18.6  # long bid 19.8 - short ask 1.2
    assert result.entry_px == 9.0
    assert result.pnl_usd == 960.0


def test_sat85_not_triggered_falls_to_clock_stop() -> None:
    morning = datetime(2026, 7, 15, 6, 0, 15, tzinfo=timezone.utc)
    signal = _gth_signal(at=datetime(2026, 7, 15, 6, 0, tzinfo=timezone.utc), entry_at=morning)
    long_series = _flat_series(morning, 8 * 3600, step=900)
    short_series = _flat_series(morning, 8 * 3600, step=900, bid=1.0, ask=1.2)
    result = simulate_trade(
        signal,
        "spread_wall",
        long_series,
        short_series,
        _flat_underlier(7555.0, start=signal.at, seconds=8 * 3600, step=900),
        _profile("sat85"),
        spread_width=20.0,
    )
    assert result.exit_reason == "time_stop"
    assert datetime.fromisoformat(result.exit_time) == datetime(
        2026, 7, 15, 13, 45, 15, tzinfo=timezone.utc
    )


def test_trail33_arms_at_half_width_and_exits_on_giveback() -> None:
    long_series = [
        _tick(GTH_ENTRY, 9.8, 10.0),  # value 8.8
        _tick(GTH_ENTRY + timedelta(seconds=60), 11.8, 12.0),  # value 10.9 >= 10
        _tick(GTH_ENTRY + timedelta(seconds=120), 10.3, 10.5),  # giveback
    ]
    short_series = _flat_series(GTH_ENTRY, 120, step=60, bid=1.0, ask=1.2)
    result = simulate_trade(
        _gth_signal(at=GTH_AT, entry_at=GTH_ENTRY),
        "spread_wall",
        long_series,
        short_series,
        _flat_underlier(7555.0, start=GTH_AT),
        _profile("trail33"),
        spread_width=20.0,
    )
    assert result.exit_reason == "trailing_tp"
    assert result.exit_px == 9.1  # long bid 10.3 - short ask 1.2


def test_clock_profile_ignores_profit_but_keeps_invalidation() -> None:
    long_series = [
        _tick(GTH_ENTRY, 9.8, 10.0),
        _tick(GTH_ENTRY + timedelta(seconds=60), 19.8, 20.0),  # would trigger sat85
    ]
    short_series = _flat_series(GTH_ENTRY, 120, step=60, bid=1.0, ask=1.2)
    underlier = _flat_underlier(7555.0, start=GTH_AT)
    signal = _gth_signal(at=GTH_AT, entry_at=GTH_ENTRY)
    result = simulate_trade(
        signal,
        "spread_wall",
        long_series,
        short_series,
        underlier,
        _profile("clock"),
        spread_width=20.0,
    )
    assert isinstance(result, Skip)
    assert result.reason == "no_fresh_exit_quote"  # no profit rule fired
    dipping = [UnderlierTick(at=t.at, price=7539.0) for t in underlier]
    stopped = simulate_trade(
        signal,
        "spread_wall",
        long_series,
        short_series,
        dipping,
        _profile("clock"),
        spread_width=20.0,
    )
    assert isinstance(stopped, Skip)
    assert stopped.reason == "invalidation_before_entry"


def test_clock_profile_rejects_entry_after_same_day_exit_clock() -> None:
    result = simulate_trade(
        _gth_signal(),
        "spread_wall",
        _flat_series(ENTRY, 60),
        _flat_series(ENTRY, 60, bid=1.0, ask=1.2),
        _flat_underlier(7555.0),
        _profile("clock"),
        spread_width=20.0,
    )
    assert isinstance(result, Skip)
    assert result.reason == "entry_after_exit_clock"


def test_prefill_spread_wall_is_not_applicable(tmp_path: Path) -> None:
    signal = _signal(
        set_name="prefill",
        entry_px=10.6,
        target_mode="formula",
        expiry=date(2026, 7, 15),
    )
    store = QuoteStore(tmp_path)  # empty lake: every quote lookup misses
    try:
        trades, skips = evaluate_signal(store, signal, profiles=[_profile("baseline")])
    finally:
        store.close()
    assert trades == []
    reasons = {(skip.variant, skip.reason) for skip in skips}
    assert ("spread_wall", "not_applicable") in reasons
    assert ("naked", "follow_through_unavailable") in reasons


def test_gth_only_profiles_skip_rth_signals(tmp_path: Path) -> None:
    store = QuoteStore(tmp_path)
    try:
        trades, skips = evaluate_signal(
            store,
            _signal(),
            profiles=[_profile("sat85")],  # RTH signal (index:SPX)
        )
        prefill_trades, prefill_skips = evaluate_signal(
            store,
            _signal(set_name="prefill", entry_px=10.6, target_mode="formula"),
            profiles=[_profile("sat85")],  # prefill not in the profile's set_names
        )
        synthetic_trades, synthetic_skips = evaluate_signal(
            store,
            _signal(underlier_instrument="synthetic:SPXW_PARITY"),
            profiles=[_profile("sat85")],
        )
    finally:
        store.close()
    assert (trades, skips) == ([], [])
    assert (prefill_trades, prefill_skips) == ([], [])
    assert (synthetic_trades, synthetic_skips) == ([], [])


def test_run_as_of_uses_readiness_complete_sessions_and_keeps_partitions(
    tmp_path: Path,
    monkeypatch,
) -> None:
    features = tmp_path / "features"
    readiness = {
        "schema_version": 1,
        "generated_at": "2026-07-17T00:00:00+00:00",
        "cutoff_at": "2026-07-17T00:00:00+00:00",
        "mode": "forward_shadow_readiness",
        "status": "collecting",
        "automatic_promotion": False,
        "policy_version": "test-policy-v3",
        "thresholds": {
            "complete_sessions": 20,
            "gth_exact_entries": 20,
            "put_exact_entries": 20,
            "exact_spread_exits": 20,
            "minute_coverage_ratio": 0.9,
            "contract_coverage_ratio": 1.0,
        },
        "sessions": {
            "observed": 2,
            "health_complete": 1,
            "contract_consistent_complete": 1,
            "target": 20,
            "dates": ["2026-07-15"],
            "details": [
                {"session_date": "2026-07-15", "complete": True},
                {"session_date": "2026-07-16", "complete": False},
            ],
        },
        "cohort_sessions": {
            "put_exact_entry": {
                "observed": 2,
                "rth_complete": 2,
                "contract_consistent_complete": 2,
                "target": 20,
                "dates": ["2026-07-15", "2026-07-16"],
            }
        },
        "cohorts": {
            "gth_exact_entry": {"count": 0, "target": 20, "status": "collecting"},
            "put_exact_entry": {"count": 0, "target": 20, "status": "collecting"},
            "exact_spread_complete_exit": {
                "count": 0,
                "target": 20,
                "status": "collecting",
            },
        },
        "blockers": ["contract_consistent_complete_sessions_below_20"],
    }
    readiness_call = {}

    def fake_readiness(root: Path, *, cutoff_at: datetime, generated_at: datetime) -> dict:
        readiness_call.update({"root": root, "cutoff_at": cutoff_at, "generated_at": generated_at})
        return readiness

    monkeypatch.setattr(
        "spx_spark.data_platform.research.odte_level_backtest.build_strategy_readiness",
        fake_readiness,
    )

    def transition(at: str, event_id: str, *, direction: str = "up") -> dict:
        return {
            "at": at,
            "current_phase": "confirmed",
            "previous_phase": "retest",
            "event_id": event_id,
            "direction": direction,
            "thesis": "breakout",
            "level": 7550.0,
            "spx_level": 7550.0,
            "level_kind": "flip_high",
            "levels": {"call_wall": 7600.0},
            "trigger_coordinate_kind": "official_spx",
            "spot": 7555.0,
        }

    _write_jsonl(
        features / "level_decision_audit/date=2026-07-15/transitions.jsonl",
        [transition("2026-07-15T14:00:00+00:00", "level:15")],
    )
    _write_jsonl(
        features / "level_decision_audit/date=2026-07-16/transitions.jsonl",
        [
            transition(
                "2026-07-16T14:00:00+00:00",
                "level:16",
                direction="down",
            )
        ],
    )
    put_at = datetime(2026, 7, 16, 14, 0, tzinfo=timezone.utc)
    _write_jsonl(
        features / "trade_intents/date=2026-07-16/events.jsonl",
        [
            _trade_ready_record(
                intent_id="intent:put-16",
                event_id="level:put-16",
                evaluated_at=put_at.isoformat(),
                expires_at=(put_at + timedelta(seconds=20)).isoformat(),
                time_stop_at=(put_at + timedelta(minutes=15)).isoformat(),
                direction="down",
                contract_id="option:SPX:SPXW:20260716:7550:P",
                trigger_level=7550.0,
                invalidation_spx=7553.0,
                target_spx=7540.0,
                spx_spot=7548.0,
                play="level_breakout_put",
            )
        ],
    )
    target = run(
        features,
        tmp_path / "data",
        tmp_path / "report",
        as_of=date(2026, 7, 16),
    )
    artifact = json.loads((target / "artifact.json").read_text(encoding="utf-8"))
    persisted_readiness = json.loads((target / "readiness.json").read_text(encoding="utf-8"))
    report = (target / "report.md").read_text(encoding="utf-8")
    assert artifact["schema_version"] == 7
    assert artifact["signal_counts"]["confirmed"] == 1
    assert artifact["window"]["complete_sessions"] == ["2026-07-15"]
    assert artifact["window"]["trading_days"] == 1
    assert artifact["window"]["put_complete_sessions"] == [
        "2026-07-15",
        "2026-07-16",
    ]
    assert artifact["window"]["put_trading_days"] == 2
    assert artifact["window"]["observed_partitions"] == ["2026-07-15", "2026-07-16"]
    assert artifact["window"]["observed_partition_count"] == 2
    assert artifact["window"]["cutoff_at"] == "2026-07-17T00:00:00+00:00"
    assert artifact["strategy_readiness"] == readiness
    assert persisted_readiness == readiness
    assert readiness_call["root"] == features.resolve()
    assert readiness_call["cutoff_at"] == datetime(2026, 7, 17, tzinfo=timezone.utc)
    assert readiness_call["generated_at"].tzinfo is not None
    assert artifact["trade_intent_coverage"]["scope"] == {
        "kind": "observed_feature_partitions",
        "dates": ["2026-07-15", "2026-07-16"],
        "note": (
            "telemetry scope; executable backtest signals use their readiness-complete "
            "GTH/global or RTH Put session cohort"
        ),
    }
    assert artifact["signal_counts"]["trade_ready"] == 1
    assert artifact["production_strategy_total"]["excluded_sets"] == [
        "confirmed",
        "prefill",
        "gth_level_manual_candidate",
    ]
    assert [row["intent_id"] for row in artifact["trade_ready_decisions"]] == ["intent:put-16"]
    assert artifact["trade_ready_decisions"][0]["execution_result"] == "not_reached"
    assert [row["opportunity_id"] for row in artifact["opportunities"]] == ["intent:put-16"]
    opportunity_rows = [
        json.loads(line)
        for line in (target / "opportunities.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert opportunity_rows == artifact["opportunities"]
    assert [
        row["latency_seconds"]
        for row in artifact["opportunities"][0]["latency_sensitivity"]
    ] == [0, 5, 10, 20, 30]
    expected_slippage_grid = {
        "unit": "SPX_option_premium_points_per_contract_leg_side",
        "unit_definition": "1.00 option point = 100 USD per contract",
        "per_leg_side_points": [0.0, 0.05, 0.1, 0.2],
        "reference_per_leg_side_points": 0.05,
        "single_total_slippage_formula": "2 * per_leg_side_points",
        "vertical_total_slippage_formula": "4 * per_leg_side_points",
    }
    assert artifact["opportunity_summary"]["slippage_sensitivity"] == (
        expected_slippage_grid
    )
    assert artifact["method"]["opportunity_replay"]["costs"][
        "slippage_sensitivity"
    ] == expected_slippage_grid
    assert (
        "opportunity_costs_use_fixed_commission_and_versioned_slippage_sensitivity_grid"
        in artifact["limitations"]
    )
    assert "filled" not in json.dumps(artifact["opportunities"])
    assert "expected_confirmed_signals" not in artifact
    assert "five_trading_days_small_sample" not in artifact["limitations"]
    assert "## 裁决冻结/样本就绪度" in report
    assert "| Put RTH contract-consistent sessions | 2 | 20 | collecting |" in report
    assert "| GTH exact entries | 0 | 20 | collecting |" in report
    assert "| Put exact entries | 0 | 20 | collecting |" in report
    assert "| exact spread complete exits | 0 | 20 | collecting |" in report
    assert "| shadow_ready | 0 | 0 |" in report
    assert "滑点采用每腿每侧 0/0.05/0.10/0.20 个期权点的版本化网格" in report
    assert "单腿总滑点=2×s，vertical=4×s" in report
    assert "0s 参考滑点(0.05/腿/侧)净 PnL$" in report
    assert "`automatic_promotion=false`" in report


def test_readiness_session_cohort_excludes_contract_violating_complete_day() -> None:
    readiness = {
        "sessions": {
            "details": [
                {
                    "session_date": "2026-07-15",
                    "complete": True,
                    "contract_consistent": False,
                },
                {
                    "session_date": "2026-07-16",
                    "complete": True,
                    "contract_consistent": True,
                },
            ]
        },
        "cohort_sessions": {
            "put_exact_entry": {
                "dates": ["2026-07-15"],
            }
        },
    }

    global_dates, put_dates = _readiness_session_cohorts(
        readiness,
        last_complete_date=date(2026, 7, 16),
    )

    assert global_dates == {date(2026, 7, 16)}
    assert put_dates == {date(2026, 7, 15)}


def test_backtest_cli_exposes_and_parses_as_of() -> None:
    script = Path(__file__).resolve().parents[1] / "scripts/backtest-0dte-levels.py"
    completed = subprocess.run(
        [sys.executable, str(script), "--help"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert "--as-of AS_OF" in completed.stdout
    namespace = runpy.run_path(str(script))
    parse_as_of = namespace["_parse_as_of"]
    assert parse_as_of("2026-07-17") == date(2026, 7, 17)
    assert parse_as_of("2026-07-17T12:30:00Z") == datetime(2026, 7, 17, 12, 30, tzinfo=timezone.utc)
