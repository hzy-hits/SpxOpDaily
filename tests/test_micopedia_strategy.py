from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from spx_spark.marketdata import InstrumentId, InstrumentType, MarketDataQuality, Provider, Quote
from spx_spark.storage import LatestState
from spx_spark.strategy.micopedia import (
    MicopediaInputs,
    build_micopedia_signal,
    inputs_from_latest_state,
)


def test_opex_pin_signal_uses_mixed_tactical_framework():
    inputs = MicopediaInputs(
        created_at=datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc),
        underlier_price=7502.0,
        vix1d=12.5,
        gamma_state="pin",
        directional_bias="mixed_tactical",
        time_phase="late",
        event_tags=("opex", "jpm"),
        key_levels=(7500, 7525),
        has_option_chain=True,
        has_es_data=True,
    )

    signal = build_micopedia_signal(inputs)

    assert signal.regime == "opex_gamma_pin"
    assert signal.directional_bias == "mixed_tactical"
    assert signal.suggested_sampling_mode == "execution_monitor"
    assert signal.nearest_key_level is not None
    assert signal.nearest_key_level.level == 7500
    assert "bounded spread" in signal.candidate_expression


def test_missing_market_data_keeps_signal_low_confidence_and_warned():
    inputs = MicopediaInputs(
        created_at=datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc),
        directional_bias="bullish",
    )

    signal = build_micopedia_signal(inputs)

    assert signal.confidence == "low_observational"
    assert signal.suggested_sampling_mode == "degraded"
    assert any("Missing SPX underlier" in warning for warning in signal.data_warnings)
    assert any("Missing SPXW option-chain" in warning for warning in signal.data_warnings)


def test_latest_state_prefers_official_spx_over_hyperliquid_context():
    now = datetime(2026, 7, 6, 14, 0, tzinfo=timezone.utc)
    spx = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7501.25,
    )
    hyperliquid = Quote(
        instrument=InstrumentId(
            symbol="xyz:SP500",
            instrument_type=InstrumentType.CRYPTO_PERP,
            provider_symbol="xyz:SP500",
        ),
        provider=Provider.HYPERLIQUID,
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7499.0,
    )
    state = LatestState(
        created_at=now,
        as_of=now,
        quotes=(spx, hyperliquid),
        best_quotes=(hyperliquid, spx),
    )

    inputs = inputs_from_latest_state(state)

    assert inputs.underlier_price == 7501.25
    assert "Underlier from latest state index:SPX." in inputs.source_notes


def test_signal_schema_contains_required_guardrail_fields():
    schema_path = Path(__file__).resolve().parents[1] / "docs" / "micopedia-signal-schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    required = set(schema["required"])

    assert "risk_policy" in required
    assert "data_warnings" in required
    assert "candidate_expression" in required
