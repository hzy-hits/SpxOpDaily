from __future__ import annotations

from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone

import pytest

from spx_spark.application.market_features.spring_gamma_coverage import strike_coverage
from spx_spark.features.exposure_map import (
    _build_expiry_exposure,
    exposure_input_row_from_quote,
)
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    OptionGreeks,
    Provider,
    Quote,
)

UTC = timezone.utc
NOW = datetime(2026, 7, 24, 14, 0, tzinfo=UTC)
EXPIRY = "20260724"
SPOT = 7500.0


def _quote(
    *,
    right: str,
    age_seconds: float,
    analytical_rejection_reason: str | None = None,
) -> Quote:
    observed_at = NOW - timedelta(seconds=age_seconds)
    raw: dict[str, object] = {
        "pricing_provider": "ibkr",
        "pricing_sampling_mode": "ibkr_stream_core",
        "pricing_observed_at": observed_at.isoformat(),
        "greeks_provider": "ibkr",
        "greeks_sampling_mode": "ibkr_stream_core",
        "greeks_observed_at": observed_at.isoformat(),
        # Prove that OI provenance is independent of the pricing provider.
        "open_interest_provider": "schwab",
        "open_interest_sampling_mode": "schwab_chain_rotation",
        "open_interest_observed_at": observed_at.isoformat(),
    }
    if analytical_rejection_reason is not None:
        raw.update(
            {
                "analytical_only": True,
                "analytical_rejection_reason": analytical_rejection_reason,
                "greeks_analytical_allowed": False,
                "nbbo_interpolated": False,
            }
        )
    return Quote(
        instrument=InstrumentId.option(
            "SPX",
            expiry=EXPIRY,
            strike=SPOT,
            right=right,
            trading_class="SPXW",
        ),
        provider=Provider.IBKR,
        received_at=observed_at,
        quality=MarketDataQuality.LIVE,
        bid=9.9,
        ask=10.1,
        quote_time=observed_at,
        last_update_at=observed_at,
        structure_time=observed_at,
        market_data_type=1,
        sampling_mode="ibkr_stream_core",
        greeks=OptionGreeks(
            implied_vol=0.20,
            delta=0.50 if right == "C" else -0.50,
            gamma=0.001,
            underlier_price=SPOT,
            model="fixture",
        ),
        open_interest=100.0,
        volume=25.0,
        raw=raw,
    )


def test_rejected_leg_keeps_oi_but_cannot_leak_greeks_or_complete_pair() -> None:
    expiry = _build_expiry_exposure(
        EXPIRY,
        [
            _quote(right="C", age_seconds=2.0),
            _quote(
                right="P",
                age_seconds=60.0,
                analytical_rejection_reason="greeks_not_accepted",
            ),
        ],
        spot=SPOT,
        as_of=NOW,
    )

    strike = expiry.strikes[0]
    assert strike.call_iv == pytest.approx(0.20)
    assert strike.put_open_interest == 100.0
    assert strike.put_volume == 25.0
    assert strike.put_iv is None
    assert strike.put_delta is None
    assert strike.put_gamma is None
    assert strike.put_vanna_per_vol_point is None
    assert strike.put_charm_per_minute is None
    assert strike.leg_metadata["put"]["analytical_allowed"] is False
    assert strike.leg_metadata["put"]["open_interest_provider"] == "schwab"
    assert strike.leg_metadata["put"]["open_interest_lane"] == "rotation"
    assert strike.leg_metadata["put"]["open_interest_observation_age_seconds"] == 60.0

    assert expiry.iv_source == "vendor_ibkr"
    assert expiry.oi_quality == "schwab_unverified"
    assert expiry.freshness["open_interest_provider_counts"] == {"schwab": 2}
    assert expiry.freshness["open_interest"]["rotation"]["max_seconds"] == 60.0

    coverage = strike_coverage(
        {
            "strikes": [asdict(strike)],
            "iv_coverage_ratio": expiry.iv_coverage_ratio,
            "delta_coverage_ratio": expiry.delta_coverage_ratio,
            "freshness": expiry.freshness,
        },
        {"underlier": {"price": SPOT}},
    )
    assert coverage["paired_strikes"] == 0
    assert coverage["complete_pair_ratio"] == 0.0
    assert coverage["iv_coverage_ratio"] == 0.5
    assert coverage["delta_coverage_ratio"] == 0.5
    assert coverage["greek_coverage_ratio"] == 0.5
    assert coverage["nonzero_oi_leg_ratio"] == 1.0
    assert coverage["density_state"] == "missing"
    assert coverage["density_target_pair_count"] == 61


def test_canonical_quote_cannot_borrow_live_pricing_for_delayed_greeks() -> None:
    quote = _quote(right="C", age_seconds=2.0)
    quote = replace(
        quote,
        raw={
            **dict(quote.raw or {}),
            "greeks_market_data_type": 3,
        },
    )

    row = exposure_input_row_from_quote(quote, as_of=NOW)

    assert row is not None
    assert row.open_interest == 100.0
    assert row.analytical_allowed is False
    assert row.iv is None
    assert row.delta is None
    assert row.gamma is None
    assert row.analytical_reason == (
        "greeks_field_not_live:market_data_type_delayed"
    )


def _complete_strikes(count: int) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index in range(count):
        row: dict[str, object] = {
            "strike": 7350.0 + index * 5.0,
            "leg_metadata": {
                "call": {"analytical_allowed": True},
                "put": {"analytical_allowed": True},
            },
        }
        for side, delta in (("call", 0.50), ("put", -0.50)):
            row.update(
                {
                    f"{side}_iv": 0.20,
                    f"{side}_delta": delta,
                    f"{side}_gamma": 0.001,
                    f"{side}_vanna_per_vol_point": 0.01,
                    f"{side}_charm_per_minute": 0.01,
                    f"{side}_open_interest": 100.0,
                }
            )
        rows.append(row)
    return rows


@pytest.mark.parametrize(
    ("paired_strikes", "expected"),
    ((3, "sparse"), (13, "core_covered"), (49, "dense"), (61, "full_61")),
)
def test_density_state_discloses_progress_against_61_pair_target(
    paired_strikes: int,
    expected: str,
) -> None:
    coverage = strike_coverage(
        {"strikes": _complete_strikes(paired_strikes)},
        {"underlier": {"price": SPOT}},
    )

    assert coverage["paired_strikes"] == paired_strikes
    assert coverage["density_state"] == expected
    assert coverage["density_target_pair_count"] == 61
    assert coverage["density_complete_pair_ratio"] == round(paired_strikes / 61, 6)
