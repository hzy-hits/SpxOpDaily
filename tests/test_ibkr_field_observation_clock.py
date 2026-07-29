from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from spx_spark.analytics.options.quote_policy import (
    option_field_age_seconds,
    option_field_observed_at,
)
from spx_spark.ibkr.adapter import quote_from_ibkr_row
from spx_spark.ibkr.verifier import VerifyRow, snapshot_rows
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    OptionGreeks,
    Provider,
    Quote,
)
from spx_spark.storage import merge_option_observations


UTC = timezone.utc
START = datetime(2026, 7, 24, 14, 30, tzinfo=UTC)


class OptionTicker:
    def __init__(self) -> None:
        self.contract = SimpleNamespace(right="C")
        self.marketDataType = 1
        self.bid = 9.9
        self.ask = 10.1
        self.last = 10.0
        self.close = 9.5
        self.bidSize = 10
        self.askSize = 12
        self.lastSize = 1
        self.volume = 100
        self.callOpenInterest = 321
        self.putOpenInterest = None
        self.time = START
        self.modelGreeks = SimpleNamespace(
            impliedVol=0.2,
            delta=0.5,
            gamma=0.001,
            theta=-0.1,
            vega=0.2,
            undPrice=7500.0,
        )

    def marketPrice(self) -> float:
        return 10.0


def option_row() -> VerifyRow:
    return VerifyRow(
        label="option:SPXW:20260724:7500:C",
        kind="option",
        symbol="SPX",
        subscribed=True,
        sampling_mode="ibkr_stream_core",
    )


def test_identical_ibkr_fields_keep_their_original_observation_clocks() -> None:
    ticker = OptionTicker()
    row = option_row()
    subscriptions = {row.label: (ticker, row)}

    snapshot_rows(subscriptions, 15.0, option_stale_after_seconds=90.0, now=START)
    first = quote_from_ibkr_row(row, received_at=START)
    assert option_field_observed_at(first, field="greeks") == START
    assert option_field_observed_at(first, field="open_interest") == START

    later = START + timedelta(seconds=60)
    ticker.time = later  # a new quiet pricing observation, not new structure
    snapshot_rows(subscriptions, 15.0, option_stale_after_seconds=90.0, now=later)
    second = quote_from_ibkr_row(row, received_at=later)

    assert option_field_observed_at(second, field="pricing") == START
    assert option_field_observed_at(second, field="greeks") == START
    assert option_field_observed_at(second, field="open_interest") == START
    assert option_field_age_seconds(second, as_of=later, field="greeks") == 60.0
    assert option_field_age_seconds(second, as_of=later, field="open_interest") == 60.0


def test_ibkr_field_changes_advance_only_the_changed_field_clocks() -> None:
    ticker = OptionTicker()
    row = option_row()
    subscriptions = {row.label: (ticker, row)}
    snapshot_rows(subscriptions, 15.0, option_stale_after_seconds=90.0, now=START)

    greek_change_at = START + timedelta(seconds=30)
    ticker.time = greek_change_at
    ticker.modelGreeks.gamma = 0.0012
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
    ticker.time = oi_change_at
    ticker.callOpenInterest = 322
    snapshot_rows(
        subscriptions,
        15.0,
        option_stale_after_seconds=90.0,
        now=oi_change_at,
    )
    oi_changed = quote_from_ibkr_row(row, received_at=oi_change_at)
    assert option_field_observed_at(oi_changed, field="greeks") == greek_change_at
    assert option_field_observed_at(oi_changed, field="open_interest") == oi_change_at


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
