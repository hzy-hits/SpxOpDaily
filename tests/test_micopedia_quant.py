from __future__ import annotations

from datetime import datetime, timezone

from spx_spark.strategy.micopedia import (
    MicopediaInputs,
    build_micopedia_signal,
    classify_dip_context,
    classify_regime,
)


def test_vix_ratio_event_pricing_forces_high_vol_event() -> None:
    inputs = MicopediaInputs(
        created_at=datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc),
        vix1d=19.0,
        vix=20.0,
    )
    assert inputs.vix_ratio == 0.95
    assert classify_regime(inputs) == "high_vol_event"


def test_dip_context_matrix() -> None:
    base = dict(
        created_at=datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc),
        vix1d=12.0,
        vix=20.0,
    )
    assert (
        classify_dip_context(
            MicopediaInputs(**base, skew_index=150.0, gamma_state="negative")
        )
        == "dip_acceleration_risk"
    )
    assert (
        classify_dip_context(
            MicopediaInputs(**base, put_skew_ratio=1.20, gamma_state="transition")
        )
        == "dip_acceleration_risk"
    )
    assert (
        classify_dip_context(MicopediaInputs(**base, skew_index=155.0, gamma_state="positive"))
        == "expensive_tail_protection"
    )
    assert (
        classify_dip_context(
            MicopediaInputs(**base, skew_index=120.0, put_skew_ratio=1.0)
        )
        == "dip_buy_friendly"
    )
    assert (
        classify_dip_context(MicopediaInputs(created_at=base["created_at"], vix1d=14.0, vix=20.0, skew_index=130.0))
        == "neutral"
    )


def test_signal_carries_dip_context_and_trigger_line() -> None:
    inputs = MicopediaInputs(
        created_at=datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc),
        vix1d=12.0,
        vix=20.0,
        skew_index=120.0,
        put_skew_ratio=1.0,
        gamma_state="positive",
        has_option_chain=True,
    )
    signal = build_micopedia_signal(inputs)
    assert signal.dip_context == "dip_buy_friendly"
    assert any("Dip context" in line for line in signal.trigger_watchlist)
