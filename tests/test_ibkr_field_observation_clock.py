from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

from ib_async import IB, Option

from spx_spark.analytics.options.quote_policy import (
    option_field_age_seconds,
    option_field_observed_at,
)
from spx_spark.ibkr.adapter import quote_from_ibkr_row
from spx_spark.application.order_map.execution_quote import nearest_abs_delta_strike
from spx_spark.application.order_map.strategy_regime import StrategyPolicy
from spx_spark.ibkr.verifier import VerifyRow, qualify_and_subscribe, snapshot_rows
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    OptionGreeks,
    Provider,
    Quote,
)
from spx_spark.storage import LatestState, merge_option_observations


UTC = timezone.utc
START = datetime(2026, 7, 24, 14, 30, tzinfo=UTC)


def option_subscription():
    ib = IB()
    contract = Option("SPX", "20260724", 7500, "C", "SMART", conId=123)
    ticker = ib.wrapper.startTicker(1, contract, "mktData")
    ib.reqMktData = lambda *_args: ticker  # local API boundary, no connection
    subscriptions = qualify_and_subscribe(
        ib, [("option:SPXW:20260724:7500:C", "option", contract)]
    )
    ticker.marketDataType = 1
    ib.wrapper.lastTime = START
    ib.wrapper.priceSizeTick(1, 1, 9.9, 10)
    ib.wrapper.priceSizeTick(1, 2, 10.1, 12)
    ib.wrapper.tickSize(1, 27, 321)
    model_callback(ib, START)
    return ib, ticker, subscriptions


def model_callback(ib, at, *, tick_type=13, gamma=0.001):
    ib.wrapper.lastTime = at
    ib.wrapper.tickOptionComputation(
        1, tick_type, 0, 0.2, 0.19, 10.0, 0.0, gamma, 0.2, -0.1, 7500.0
    )
    ib.wrapper.tcpDataProcessed()


def option_row() -> VerifyRow:
    return VerifyRow(
        label="option:SPXW:20260724:7500:C",
        kind="option",
        symbol="SPX",
        subscribed=True,
        sampling_mode="ibkr_stream_core",
    )


def test_identical_ibkr_fields_keep_their_original_observation_clocks() -> None:
    ib, ticker, subscriptions = option_subscription()
    row = next(iter(subscriptions.values()))[1]

    snapshot_rows(subscriptions, 15.0, option_stale_after_seconds=90.0, now=START)
    first = quote_from_ibkr_row(row, received_at=START)
    assert option_field_observed_at(first, field="greeks") == START
    assert option_field_observed_at(first, field="open_interest") == START

    later = START + timedelta(seconds=60)
    ib.wrapper.lastTime = later
    ib.wrapper.priceSizeTick(1, 1, ticker.bid, ticker.bidSize + 1)
    ib.wrapper.tcpDataProcessed()
    snapshot_rows(subscriptions, 15.0, option_stale_after_seconds=90.0, now=later)
    second = quote_from_ibkr_row(row, received_at=later)

    assert option_field_observed_at(second, field="pricing") == later
    assert option_field_observed_at(second, field="greeks") == START
    assert option_field_observed_at(second, field="open_interest") == START
    assert option_field_age_seconds(second, as_of=later, field="greeks") == 60.0
    assert option_field_age_seconds(second, as_of=later, field="open_interest") == 60.0


def test_ibkr_field_changes_advance_only_the_changed_field_clocks() -> None:
    ib, _ticker, subscriptions = option_subscription()
    row = next(iter(subscriptions.values()))[1]
    snapshot_rows(subscriptions, 15.0, option_stale_after_seconds=90.0, now=START)

    greek_change_at = START + timedelta(seconds=30)
    model_callback(ib, greek_change_at, gamma=0.0012)
    snapshot_rows(
        subscriptions,
        15.0,
        option_stale_after_seconds=90.0,
        now=greek_change_at,
    )
    greek_changed = quote_from_ibkr_row(row, received_at=greek_change_at)
    assert option_field_observed_at(greek_changed, field="greeks") == greek_change_at
    assert option_field_observed_at(greek_changed, field="open_interest") == START

    oi_change_at = START + timedelta(seconds=60)
    ib.wrapper.lastTime = oi_change_at
    ib.wrapper.tickSize(1, 27, 322)
    ib.wrapper.tcpDataProcessed()
    snapshot_rows(
        subscriptions,
        15.0,
        option_stale_after_seconds=90.0,
        now=oi_change_at,
    )
    oi_changed = quote_from_ibkr_row(row, received_at=oi_change_at)
    assert option_field_observed_at(oi_changed, field="greeks") == greek_change_at
    assert option_field_observed_at(oi_changed, field="open_interest") == oi_change_at


def test_equal_model_callback_restores_delta_but_other_ticks_cannot_renew_it() -> None:
    ib, ticker, subscriptions = option_subscription()
    row = next(iter(subscriptions.values()))[1]
    snapshot_rows(subscriptions, 90.0, now=START)
    original = quote_from_ibkr_row(row, received_at=START)

    def selected_at(now):
        snapshot_rows(subscriptions, 90.0, now=now)
        quote = quote_from_ibkr_row(row, received_at=now)
        latest = LatestState(START, now, (quote,), (quote,))
        selected = nearest_abs_delta_strike(
            latest, "20260724", "C", target_abs_delta=0.2, now=now,
            policy=StrategyPolicy(quote_max_age_seconds=30), providers=(Provider.IBKR,),
            max_abs_delta=0.2, max_greeks_age_seconds=30,
        )
        return quote, selected

    at = START + timedelta(seconds=40)
    ib.wrapper.lastTime = at
    ib.wrapper.priceSizeTick(1, 1, 9.95, 10)
    ib.wrapper.tcpDataProcessed()
    assert selected_at(at)[1] is None

    model_callback(ib, at)  # exactly the same six Greek/model values
    fresh, selected = selected_at(at + timedelta(seconds=2))
    assert fresh.greeks == original.greeks
    assert option_field_observed_at(fresh, field="greeks") == at
    assert selected == 7500

    # Snapshot delay is not added to the clock. Price, OI and other computation
    # ticks do not make the expired model Delta executable again.
    later = at + timedelta(seconds=31)
    ib.wrapper.lastTime = later
    ib.wrapper.priceSizeTick(1, 1, 10.0, 11)
    ib.wrapper.tickSize(1, 27, 322)
    for tick_type in (10, 11, 12):
        model_callback(ib, later, tick_type=tick_type)
    stale, selected = selected_at(later)
    assert option_field_observed_at(stale, field="greeks") == at
    assert selected is None

    # Reusing a cached Ticker on resubscription is not a new model observation.
    subscriptions = qualify_and_subscribe(
        ib, [(row.label, "option", ticker.contract)]
    )
    row = next(iter(subscriptions.values()))[1]
    assert selected_at(later)[1] is None
    model_callback(ib, later)
    assert selected_at(later)[1] == 7500


def test_missing_cross_provider_structure_clock_fails_closed_during_merge() -> None:
    instrument = InstrumentId.option(
        "SPX",
        expiry="20260724",
        strike=7500.0,
        right="C",
        trading_class="SPXW",
    )
    pricing = Quote(
        instrument=instrument,
        provider=Provider.SCHWAB,
        received_at=START,
        quality=MarketDataQuality.LIVE,
        bid=9.9,
        ask=10.1,
        quote_time=START,
        last_update_at=START,
        greeks=OptionGreeks(implied_vol=0.2, delta=0.5, gamma=0.001),
        raw={"greeks_provider": Provider.IBKR.value},
    )
    quiet = replace(
        pricing,
        received_at=START + timedelta(seconds=60),
        last_update_at=START + timedelta(seconds=60),
        greeks=None,
        raw=None,
    )

    merged = merge_option_observations(pricing, quiet)

    assert merged.raw["greeks_observed_at"] == "1970-01-01T00:00:00+00:00"
    assert option_field_age_seconds(merged, as_of=START, field="greeks") > 1_000_000


def test_current_ibkr_stream_field_without_independent_clock_fails_closed() -> None:
    row = option_row()
    row.market_data_type = 1
    row.bid = 9.9
    row.ask = 10.1
    row.model_iv = 0.2
    row.delta = 0.5
    row.gamma = 0.001
    row.open_interest = 321

    quote = quote_from_ibkr_row(row, received_at=START)

    assert option_field_observed_at(
        quote,
        field="greeks",
    ) == datetime(1970, 1, 1, tzinfo=UTC)
    assert option_field_observed_at(
        quote,
        field="open_interest",
    ) == datetime(1970, 1, 1, tzinfo=UTC)
