from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from spx_spark.analytics.options.chain import (
    chain_implied_spot,
    enrich_open_interest,
    enrich_option_greeks,
    pair_by_strike,
)
from spx_spark.analytics.options.quote_policy import (
    analytical_option_quote,
    option_analytical_iv_allowed,
    option_analytical_pricing_allowed,
    option_field_live_entitlement,
    option_field_live_entitlement_source,
    option_field_market_data_type,
    option_field_provider,
    option_field_sampling_mode,
)
from spx_spark.greek_reference import inputs_from_quote
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    OptionGreeks,
    Provider,
    Quote,
    quote_from_dict,
)
from spx_spark.options_map import build_options_map
from spx_spark.storage import LatestState, merge_option_observations


UTC = timezone.utc
NOW = datetime(2026, 7, 24, 15, 0, tzinfo=UTC)
EXPIRY = "20260724"


def option_quote(
    *,
    provider: Provider = Provider.SCHWAB,
    strike: float = 7500.0,
    right: str = "C",
    quality: MarketDataQuality = MarketDataQuality.LIVE,
    market_data_type: int | str | None = 1,
    observed_at: datetime = NOW,
    sampling_mode: str = "core",
    greeks: OptionGreeks | None = None,
    open_interest: float | None = 100.0,
    raw: dict[str, object] | None = None,
) -> Quote:
    return Quote(
        instrument=InstrumentId.option(
            "SPX",
            expiry=EXPIRY,
            strike=strike,
            right=right,
            trading_class="SPXW",
        ),
        provider=provider,
        received_at=observed_at,
        quality=quality,
        bid=9.9,
        ask=10.1,
        quote_time=observed_at,
        last_update_at=observed_at,
        structure_time=observed_at,
        market_data_type=market_data_type,
        sampling_mode=sampling_mode,
        greeks=greeks,
        open_interest=open_interest,
        raw=raw,
    )


def vendor_greeks() -> OptionGreeks:
    return OptionGreeks(
        implied_vol=0.2,
        delta=0.5,
        gamma=0.001,
        underlier_price=7500.0,
        model="field_contract_fixture",
    )


@pytest.mark.parametrize(
    ("market_data_type", "source"),
    (
        (2, "market_data_type_frozen"),
        (3, "market_data_type_delayed"),
        (4, "market_data_type_delayed_frozen"),
    ),
)
def test_negative_feed_type_overrides_live_quality(
    market_data_type: int,
    source: str,
) -> None:
    quote = option_quote(
        quality=MarketDataQuality.LIVE,
        market_data_type=market_data_type,
        raw={"isDelayed": False},
    )

    normalized = analytical_option_quote(
        quote,
        as_of=NOW,
        core_max_age_seconds=15.0,
        rotation_max_age_seconds=90.0,
    )

    assert option_field_live_entitlement(normalized, field="pricing") is False
    assert option_field_live_entitlement_source(normalized, field="pricing") == source
    assert normalized.raw["analytical_rejection_reason"] == "pricing_feed_not_live"
    assert option_analytical_pricing_allowed(normalized) is False


def test_cross_provider_field_helpers_do_not_borrow_top_level_live_feed() -> None:
    quote = option_quote(
        greeks=vendor_greeks(),
        raw={
            "greeks_provider": "ibkr",
            "greeks_sampling_mode": "ibkr_stream_rotation",
            "greeks_market_data_type": 3,
            "greeks_live_entitlement": True,
            "greeks_live_entitlement_source": "bad_prior_value",
        },
    )

    assert option_field_provider(quote, field="greeks") is Provider.IBKR
    assert option_field_sampling_mode(quote, field="greeks") == "ibkr_stream_rotation"
    assert option_field_market_data_type(quote, field="greeks") == 3
    assert option_field_live_entitlement(quote, field="greeks") is False
    assert (
        option_field_live_entitlement_source(quote, field="greeks")
        == "market_data_type_delayed"
    )


def test_multi_round_merge_preserves_each_fields_true_provenance() -> None:
    greek_at = NOW - timedelta(seconds=60)
    oi_at = NOW - timedelta(hours=2)
    enriched = option_quote(
        observed_at=greek_at,
        greeks=vendor_greeks(),
        open_interest=321.0,
        raw={
            "greeks_provider": "ibkr",
            "greeks_sampling_mode": "ibkr_stream_rotation",
            "greeks_market_data_type": 1,
            "greeks_explicit_delayed": False,
            "greeks_live_entitlement": True,
            "greeks_live_entitlement_source": "market_data_type_live",
            "greeks_observed_at": greek_at.isoformat(),
            "open_interest_provider": "ibkr",
            "open_interest_sampling_mode": "ibkr_daily_structure",
            "open_interest_market_data_type": 1,
            "open_interest_live_entitlement": True,
            "open_interest_live_entitlement_source": "market_data_type_live",
            "open_interest_observed_at": oi_at.isoformat(),
        },
    )
    quiet_pricing = option_quote(
        observed_at=NOW - timedelta(seconds=2),
        greeks=None,
        open_interest=None,
    )
    newest_pricing = replace(
        quiet_pricing,
        received_at=NOW - timedelta(seconds=1),
        last_update_at=NOW - timedelta(seconds=1),
    )

    merged = merge_option_observations(enriched, quiet_pricing)
    merged = merge_option_observations(merged, newest_pricing)

    assert merged.raw["pricing_provider"] == "schwab"
    assert merged.raw["greeks_provider"] == "ibkr"
    assert merged.raw["greeks_sampling_mode"] == "ibkr_stream_rotation"
    assert merged.raw["greeks_market_data_type"] == 1
    assert merged.raw["greeks_observed_at"] == greek_at.isoformat()
    assert merged.raw["open_interest_provider"] == "ibkr"
    assert merged.raw["open_interest_sampling_mode"] == "ibkr_daily_structure"
    assert merged.raw["open_interest_observed_at"] == oi_at.isoformat()


def test_oi_and_greek_enrichment_use_field_source_contract() -> None:
    pricing = option_quote(greeks=None, open_interest=None)
    oi_at = NOW - timedelta(hours=1)
    oi_source = option_quote(
        open_interest=456.0,
        raw={
            "open_interest_provider": "ibkr",
            "open_interest_sampling_mode": "ibkr_daily_structure",
            "open_interest_observed_at": oi_at.isoformat(),
        },
    )
    enriched_oi = enrich_open_interest((pricing,), (oi_source,))[0]

    assert enriched_oi.open_interest == 456.0
    assert enriched_oi.raw["open_interest_provider"] == "ibkr"
    assert enriched_oi.raw["open_interest_observed_at"] == oi_at.isoformat()

    delayed_greek_source = option_quote(
        provider=Provider.IBKR,
        quality=MarketDataQuality.LIVE,
        market_data_type=3,
        greeks=vendor_greeks(),
        sampling_mode="ibkr_stream_rotation",
    )
    enriched_greeks = enrich_option_greeks(
        (pricing,),
        (delayed_greek_source,),
        as_of=NOW,
        core_max_age_seconds=15.0,
        rotation_max_age_seconds=90.0,
    )[0]

    assert enriched_greeks.greeks is None
    assert enriched_greeks.raw["greeks_analytical_allowed"] is False
    assert (
        "greeks_feed_not_live:market_data_type_delayed"
        in enriched_greeks.raw["greeks_rejection_reasons"]
    )
    assert option_analytical_iv_allowed(enriched_greeks) is False


def test_greek_reference_rejects_delayed_field_on_live_pricing_quote() -> None:
    quote = option_quote(
        greeks=vendor_greeks(),
        raw={
            "greeks_provider": "ibkr",
            "greeks_market_data_type": 3,
            "greeks_observed_at": NOW.isoformat(),
        },
    )

    inputs, quality = inputs_from_quote(quote, as_of=NOW)

    assert inputs is None
    assert quality.status == "blocked"
    assert quality.reasons == (
        "greeks_field_not_live:market_data_type_delayed",
    )


def test_projection_round_trip_preserves_bounded_feed_contract() -> None:
    quote = option_quote(
        greeks=vendor_greeks(),
        raw={
            "pricing_provider": "schwab",
            "pricing_sampling_mode": "schwab_chain",
            "pricing_market_data_type": "live",
            "pricing_explicit_delayed": False,
            "pricing_live_entitlement": True,
            "pricing_live_entitlement_source": "schwab_explicit_not_delayed",
            "pricing_observed_at": NOW.isoformat(),
            "greeks_provider": "ibkr",
            "greeks_sampling_mode": "ibkr_stream_rotation",
            "greeks_market_data_type": 1,
            "greeks_explicit_delayed": False,
            "greeks_live_entitlement": True,
            "greeks_live_entitlement_source": "market_data_type_live",
            "greeks_observed_at": NOW.isoformat(),
            "provider_secret": "do_not_serialize",
        },
    )

    payload = quote.to_dict()
    restored = quote_from_dict(payload)

    assert payload["field_provenance"]["greeks_market_data_type"] == 1
    assert payload["field_provenance"]["greeks_live_entitlement"] is True
    assert "provider_secret" not in payload["field_provenance"]
    assert restored.raw["pricing_explicit_delayed"] is False
    assert restored.raw["greeks_provider"] == "ibkr"


def test_rejected_analytical_leg_cannot_enter_parity_or_wall_map() -> None:
    rejected_call = analytical_option_quote(
        option_quote(
            right="C",
            quality=MarketDataQuality.LIVE,
            market_data_type=3,
            greeks=vendor_greeks(),
        ),
        as_of=NOW,
        core_max_age_seconds=15.0,
        rotation_max_age_seconds=90.0,
    )
    live_put = analytical_option_quote(
        option_quote(right="P", greeks=vendor_greeks()),
        as_of=NOW,
        core_max_age_seconds=15.0,
        rotation_max_age_seconds=90.0,
    )
    live_put = replace(
        live_put,
        raw={
            **dict(live_put.raw or {}),
            "greeks_analytical_allowed": True,
            "greeks_provider": "schwab",
            "greeks_market_data_type": 1,
            "greeks_live_entitlement": True,
        },
    )

    assert chain_implied_spot(pair_by_strike([rejected_call, live_put])) is None

    underlier = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.SCHWAB,
        received_at=NOW,
        quality=MarketDataQuality.LIVE,
        mark=7500.0,
        quote_time=NOW,
        last_update_at=NOW,
        market_data_type=1,
    )
    state = LatestState(
        created_at=NOW,
        as_of=NOW,
        quotes=(underlier, rejected_call, live_put),
        best_quotes=(underlier, rejected_call, live_put),
    )
    expiry = build_options_map(state).expiries[0]

    assert expiry.atm_call_mid is None
    assert expiry.call_wall is None
    assert expiry.put_wall is None
    assert expiry.wall_method == "unavailable"
