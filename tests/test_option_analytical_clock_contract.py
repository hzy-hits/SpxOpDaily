from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from spx_spark.analytics.options.quote_policy import (
    analytical_option_quote,
    option_field_age_seconds,
)
from spx_spark.application.market_features.spring_gamma_v3 import _strike_coverage
from spx_spark.config import StorageSettings
from spx_spark.features.exposure_map import build_exposure_map
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    OptionGreeks,
    Provider,
    Quote,
    quote_from_dict,
)
from spx_spark.options_map import group_spxw_option_quotes
from spx_spark.storage import (
    LatestState,
    configured_quote_use_decision,
    merge_option_observations,
)


UTC = timezone.utc
NOW = datetime(2026, 7, 24, 14, 0, tzinfo=UTC)
EXPIRY = "20260724"


def settings() -> StorageSettings:
    return StorageSettings(
        data_root="data",
        latest_state_path="data/latest/state.json",
        raw_file_name="quotes.jsonl",
        include_raw_payload=False,
        latest_stale_after_seconds=15.0,
        delayed_stale_after_seconds=60.0,
        rotation_stale_after_seconds=90.0,
        slow_index_stale_after_seconds=90.0,
        slow_index_labels=frozenset(),
    )


def option_quote(
    *,
    provider: Provider,
    strike: float,
    right: str,
    observed_age: float,
    sampling_mode: str,
    greeks: OptionGreeks | None,
    quality: MarketDataQuality = MarketDataQuality.LIVE,
    bid: float = 9.9,
    ask: float = 10.1,
    market_data_type: int | str | None = 1,
    raw: dict[str, object] | None = None,
) -> Quote:
    observed_at = NOW - timedelta(seconds=observed_age)
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
        bid=bid,
        ask=ask,
        quote_time=observed_at,
        last_update_at=observed_at,
        structure_time=observed_at if greeks is not None else None,
        market_data_type=market_data_type,
        sampling_mode=sampling_mode,
        greeks=greeks,
        open_interest=100.0,
        raw=raw,
    )


def model_fields() -> OptionGreeks:
    return OptionGreeks(
        implied_vol=0.20,
        delta=None,
        gamma=None,
        underlier_price=7500.0,
        model="fixture_iv_only",
    )


def test_rotation_quote_is_analytical_at_60s_but_not_execution_pricing() -> None:
    quote = option_quote(
        provider=Provider.IBKR,
        strike=7500.0,
        right="C",
        observed_age=60.0,
        sampling_mode="ibkr_stream_rotation",
        greeks=model_fields(),
        quality=MarketDataQuality.STALE,
    )

    analytical = analytical_option_quote(
        quote,
        as_of=NOW,
        core_max_age_seconds=15.0,
        rotation_max_age_seconds=90.0,
    )

    assert analytical.quality is MarketDataQuality.LIVE
    assert analytical.raw["analytical_lane"] == "rotation"
    assert analytical.raw["analytical_max_age_seconds"] == 90.0
    assert analytical.raw["nbbo_interpolated"] is False
    assert configured_quote_use_decision(
        analytical,
        as_of=NOW,
        settings=settings(),
    ).pricing_allowed is False


@pytest.mark.parametrize("delay_key", ("isDelayed", "is_delayed", "is-delayed"))
def test_schwab_explicit_not_delayed_is_live_entitlement(delay_key: str) -> None:
    quote = option_quote(
        provider=Provider.SCHWAB,
        strike=7500.0,
        right="C",
        observed_age=2.0,
        sampling_mode="schwab_chain",
        greeks=model_fields(),
        quality=MarketDataQuality.STALE,
        market_data_type=None,
        raw={delay_key: False},
    )

    analytical = analytical_option_quote(
        quote,
        as_of=NOW,
        core_max_age_seconds=15.0,
        rotation_max_age_seconds=90.0,
    )

    assert analytical.quality is MarketDataQuality.LIVE
    assert (
        analytical.raw["pricing_live_entitlement_source"]
        == "schwab_explicit_not_delayed"
    )
    decision = configured_quote_use_decision(
        analytical,
        as_of=NOW,
        settings=settings(),
    )
    assert decision.research_usable is True
    assert decision.alert_allowed is False
    assert decision.pricing_allowed is False
    assert decision.reason == "analytical_only_non_executable"


def test_unknown_schwab_feed_is_not_promoted_to_live() -> None:
    quote = option_quote(
        provider=Provider.SCHWAB,
        strike=7500.0,
        right="P",
        observed_age=2.0,
        sampling_mode="schwab_chain",
        greeks=model_fields(),
        quality=MarketDataQuality.STALE,
        market_data_type=None,
        raw={},
    )

    analytical = analytical_option_quote(
        quote,
        as_of=NOW,
        core_max_age_seconds=15.0,
        rotation_max_age_seconds=90.0,
    )

    assert analytical.quality is MarketDataQuality.STALE
    assert analytical.raw["analytical_rejection_reason"] == "pricing_feed_not_live"


def test_cross_provider_greek_merge_keeps_schwab_nbbo_and_field_clock() -> None:
    schwab = option_quote(
        provider=Provider.SCHWAB,
        strike=7500.0,
        right="C",
        observed_age=2.0,
        sampling_mode="schwab_stream",
        greeks=None,
        bid=12.0,
        ask=12.4,
    )
    rotation = option_quote(
        provider=Provider.IBKR,
        strike=7500.0,
        right="C",
        observed_age=60.0,
        sampling_mode="ibkr_stream_rotation",
        greeks=model_fields(),
        quality=MarketDataQuality.STALE,
        bid=11.0,
        ask=13.0,
    )
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
        quotes=(underlier, schwab, rotation),
        best_quotes=(underlier, schwab),
    )

    selected = group_spxw_option_quotes(state, storage_settings=settings())[EXPIRY][0]

    assert selected.provider is Provider.SCHWAB
    assert (selected.bid, selected.ask) == (12.0, 12.4)
    assert selected.greeks == rotation.greeks
    assert selected.raw["greeks_provider"] == "ibkr"
    assert selected.raw["greeks_analytical_lane"] == "rotation"
    assert selected.raw["greeks_observed_age_seconds"] == 60.0
    assert selected.raw["nbbo_interpolated"] is False
    assert build_exposure_map(state).expiries[0].iv_source == "vendor_ibkr"


def test_rotation_greeks_over_90s_are_rejected_with_reason() -> None:
    schwab = option_quote(
        provider=Provider.SCHWAB,
        strike=7500.0,
        right="P",
        observed_age=2.0,
        sampling_mode="schwab_stream",
        greeks=None,
    )
    rotation = option_quote(
        provider=Provider.IBKR,
        strike=7500.0,
        right="P",
        observed_age=91.0,
        sampling_mode="ibkr_stream_rotation",
        greeks=model_fields(),
        quality=MarketDataQuality.STALE,
    )
    state = LatestState(
        created_at=NOW,
        as_of=NOW,
        quotes=(schwab, rotation),
        best_quotes=(schwab,),
    )

    selected = group_spxw_option_quotes(state, storage_settings=settings())[EXPIRY][0]

    assert selected.greeks is None
    assert selected.raw["greeks_analytical_allowed"] is False
    assert "rotation_greeks_stale" in selected.raw["greeks_rejection_reasons"]


def test_merge_does_not_refresh_old_greeks_with_newer_open_interest_clock() -> None:
    older_greeks = option_quote(
        provider=Provider.SCHWAB,
        strike=7500.0,
        right="C",
        observed_age=30.0,
        sampling_mode="schwab_stream",
        greeks=model_fields(),
    )
    newer_oi = option_quote(
        provider=Provider.SCHWAB,
        strike=7500.0,
        right="C",
        observed_age=1.0,
        sampling_mode="schwab_stream",
        greeks=None,
    )

    merged = merge_option_observations(older_greeks, newer_oi)

    assert option_field_age_seconds(merged, as_of=NOW, field="greeks") == 30.0
    assert option_field_age_seconds(merged, as_of=NOW, field="open_interest") == 1.0
    assert merged.raw["greeks_observed_at"] != merged.raw["open_interest_observed_at"]


def test_merge_refreshes_quiet_pricing_from_provider_observation_clock() -> None:
    older = option_quote(
        provider=Provider.SCHWAB,
        strike=7500.0,
        right="C",
        observed_age=30.0,
        sampling_mode="schwab_chain",
        greeks=None,
    )
    newer = option_quote(
        provider=Provider.SCHWAB,
        strike=7500.0,
        right="C",
        observed_age=1.0,
        sampling_mode="schwab_chain",
        greeks=None,
    )
    unchanged_source_at = NOW - timedelta(seconds=60)
    older = replace(older, quote_time=unchanged_source_at)
    newer = replace(newer, quote_time=unchanged_source_at)

    merged = merge_option_observations(older, newer)

    assert option_field_age_seconds(merged, as_of=NOW, field="pricing") == 1.0
    assert merged.raw["pricing_source_at"] == unchanged_source_at.isoformat()


def test_latest_projection_round_trip_preserves_only_bounded_field_provenance() -> None:
    quote = option_quote(
        provider=Provider.SCHWAB,
        strike=7500.0,
        right="C",
        observed_age=2.0,
        sampling_mode="schwab_chain",
        greeks=model_fields(),
        raw={
            "pricing_provider": "schwab",
            "pricing_observed_at": NOW.isoformat(),
            "greeks_provider": "ibkr",
            "greeks_sampling_mode": "ibkr_stream_rotation",
            "greeks_observed_at": (NOW - timedelta(seconds=60)).isoformat(),
            "open_interest_provider": "schwab",
            "open_interest_observed_at": NOW.isoformat(),
            "sensitive_provider_payload": "must-not-persist",
        },
    )
    payload = LatestState(
        created_at=NOW,
        as_of=NOW,
        quotes=(quote,),
        best_quotes=(quote,),
    ).to_dict()["quotes"][0]

    assert "raw" not in payload
    assert payload["field_provenance"]["greeks_provider"] == "ibkr"
    assert "sensitive_provider_payload" not in payload["field_provenance"]

    restored = quote_from_dict(payload)
    assert restored.raw["greeks_provider"] == "ibkr"
    assert restored.raw["greeks_sampling_mode"] == "ibkr_stream_rotation"
    assert restored.raw["pricing_observed_at"] == NOW.isoformat()
    assert "sensitive_provider_payload" not in restored.raw


def test_exposure_reports_core_and_rotation_ages_and_derives_model_greeks() -> None:
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
    rows = (
        option_quote(
            provider=Provider.SCHWAB,
            strike=7500.0,
            right="C",
            observed_age=2.0,
            sampling_mode="schwab_stream",
            greeks=model_fields(),
        ),
        option_quote(
            provider=Provider.SCHWAB,
            strike=7500.0,
            right="P",
            observed_age=2.0,
            sampling_mode="schwab_stream",
            greeks=model_fields(),
        ),
        option_quote(
            provider=Provider.IBKR,
            strike=7505.0,
            right="C",
            observed_age=60.0,
            sampling_mode="ibkr_stream_rotation",
            greeks=model_fields(),
            quality=MarketDataQuality.STALE,
        ),
        option_quote(
            provider=Provider.IBKR,
            strike=7505.0,
            right="P",
            observed_age=60.0,
            sampling_mode="ibkr_stream_rotation",
            greeks=model_fields(),
            quality=MarketDataQuality.STALE,
        ),
    )
    state = LatestState(
        created_at=NOW,
        as_of=NOW,
        quotes=(underlier, *rows),
        best_quotes=(underlier, *rows),
    )

    expiry = build_exposure_map(state).expiries[0]

    assert expiry.snapshot_age_seconds == 2.0
    assert expiry.freshness["core"]["max_seconds"] == 2.0
    assert expiry.freshness["rotation"]["max_seconds"] == 60.0
    assert expiry.freshness["nbbo_interpolated"] is False
    assert expiry.strikes[0].leg_metadata["call"]["gamma_source"] == "bs_from_observed_iv"
    assert expiry.strikes[1].leg_metadata["put"]["greeks_lane"] == "rotation"


def test_spring_coverage_uses_nearest_61_not_unbounded_sparse_perimeter() -> None:
    strikes = []
    for index in range(-40, 41):
        complete = -30 <= index <= 30
        row = {"strike": 7500.0 + index * 5.0}
        for side, delta in (("call", 0.5), ("put", -0.5)):
            row.update(
                {
                    f"{side}_iv": 0.20 if complete else None,
                    f"{side}_delta": delta if complete else None,
                    f"{side}_gamma": 0.001 if complete else None,
                    f"{side}_vanna_per_vol_point": 0.01 if complete else None,
                    f"{side}_charm_per_minute": 0.01 if complete else None,
                    f"{side}_open_interest": 100.0,
                }
            )
        strikes.append(row)

    coverage = _strike_coverage(
        {
            "strikes": strikes,
            "iv_coverage_ratio": 61 / 81,
            "delta_coverage_ratio": 61 / 81,
        },
        {"underlier": {"price": 7500.0}},
    )

    assert coverage["available_strike_count"] == 81
    assert coverage["strike_count"] == 61
    assert coverage["complete_pair_ratio"] == 1.0
    assert coverage["left_wing_complete_pair_ratio"] == 1.0
    assert coverage["right_wing_complete_pair_ratio"] == 1.0
    assert coverage["nbbo_interpolated"] is False
