from __future__ import annotations

from datetime import datetime, timezone

from spx_spark.config import SamplingSettings
from spx_spark.marketdata import Provider
from spx_spark.mock_collector import build_mock_quotes


def make_sampling_settings() -> SamplingSettings:
    return SamplingSettings(
        strike_step=5,
        window_points=200,
        hot_window_points=50,
        group_count=4,
        group_interval_seconds=4,
        degraded_group_count=20,
        degraded_group_interval_seconds=3,
        group_strategy="interleaved",
        hot_human_cadence_seconds=8,
        hot_execution_cadence_seconds=2,
        include_next_expiry=True,
        default_mode="human_alert",
    )


def test_build_mock_quotes_contains_context_and_options():
    quotes, summary = build_mock_quotes(
        underlier=7500,
        expiry="20260706",
        next_expiry="20260707",
        mode="human_alert",
        sampling_settings=make_sampling_settings(),
        rolling_group_index=0,
        received_at=datetime(2026, 7, 6, 13, 30, tzinfo=timezone.utc),
    )

    instrument_ids = {quote.instrument.canonical_id for quote in quotes}
    assert "index:SPX" in instrument_ids
    assert "index:VIX" in instrument_ids
    assert "option:SPX:SPXW:20260706:7500:C" in instrument_ids
    assert all(quote.provider == Provider.MOCK for quote in quotes)
    assert summary["group_strategy"] == "interleaved"
