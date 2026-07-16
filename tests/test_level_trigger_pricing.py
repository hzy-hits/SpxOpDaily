from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from spx_spark.application.order_map.execution_quote import evaluate_execution_quote
from spx_spark.application.order_map.pricing import (
    YEAR_SECONDS,
    build_option_price_bs_projection,
    parity_forward,
)
from spx_spark.application.order_map.pricing_outcomes import advance_pricing_outcomes
from spx_spark.application.order_map.touch_time_model import estimate_touch_time
from spx_spark.application.order_map.trigger_coordinates import (
    TriggerCoordinateKind,
    resolve_trigger_coordinate,
)
from spx_spark.config import StorageSettings
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    OptionGreeks,
    Provider,
    Quote,
)
from spx_spark.options_map import pair_by_strike
from spx_spark.storage import LatestState, LatestStateStore


NOW = datetime(2026, 7, 13, 14, 30, tzinfo=timezone.utc)


def _option(
    *,
    strike: float = 6000.0,
    right: str = "C",
    bid: float = 4.0,
    ask: float = 4.2,
    provider: Provider = Provider.IBKR,
    now: datetime = NOW,
) -> Quote:
    return Quote(
        instrument=InstrumentId.option(
            "SPX", expiry="20260713", strike=strike, right=right, trading_class="SPXW"
        ),
        provider=provider,
        provider_symbol=f"{provider.value}:{strike}:{right}",
        received_at=now,
        last_update_at=now,
        quote_time=now,
        quality=MarketDataQuality.LIVE,
        bid=bid,
        ask=ask,
        greeks=OptionGreeks(implied_vol=0.22, delta=0.5, gamma=0.01),
    )


def _state(*quotes: Quote, now: datetime = NOW) -> LatestState:
    return LatestState(
        created_at=now,
        as_of=now,
        quotes=tuple(quotes),
        best_quotes=tuple(quotes),
    )


def _storage(tmp_path: Path) -> StorageSettings:
    root = tmp_path / "data"
    return StorageSettings(
        data_root=str(root),
        latest_state_path=str(root / "latest" / "state.json"),
        raw_file_name="quotes.jsonl",
        include_raw_payload=False,
        latest_stale_after_seconds=15.0,
        slow_index_stale_after_seconds=300.0,
        slow_index_labels=frozenset(),
    )


def test_execution_quote_gate_blocks_wide_and_cross_provider_mid() -> None:
    quote = _option(bid=2.0, ask=6.0)
    schwab = _option(bid=8.0, ask=8.2, provider=Provider.SCHWAB)

    gate = evaluate_execution_quote(quote, (quote, schwab), as_of=NOW)

    assert gate.executable is False
    assert "spread_points_exceeded" in gate.reasons
    assert "provider_mid_divergence_exceeded" in gate.reasons


def test_execution_quote_gate_ignores_provider_with_stale_source_timestamp() -> None:
    schwab = _option(bid=8.0, ask=8.2, provider=Provider.SCHWAB)
    stale_ibkr = _option(bid=2.0, ask=2.2)
    stale_ibkr = replace(stale_ibkr, quote_time=NOW - timedelta(minutes=5))

    gate = evaluate_execution_quote(schwab, (schwab, stale_ibkr), as_of=NOW)

    assert gate.executable is True
    assert gate.providers == ("schwab",)
    assert gate.provider_mid_divergence_bps is None


def test_execution_quote_gate_excludes_provider_with_stale_model_underlier() -> None:
    schwab = _option(bid=12.2, ask=12.4, provider=Provider.SCHWAB)
    schwab = replace(
        schwab,
        greeks=replace(schwab.greeks, underlier_price=7544.7),
    )
    ibkr = _option(bid=6.3, ask=6.4)
    ibkr = replace(
        ibkr,
        greeks=replace(ibkr.greeks, underlier_price=7563.8),
    )

    gate = evaluate_execution_quote(schwab, (schwab, ibkr), as_of=NOW)

    assert gate.executable is True
    assert gate.providers == ("schwab",)
    assert gate.provider_mid_divergence_bps is None
    assert gate.excluded_providers == ("ibkr:model_underlier_divergence",)


def test_parity_forward_and_black76_projection_expose_scenario_range() -> None:
    call = _option(strike=6000, right="C", bid=19.9, ask=20.1)
    put = _option(strike=6000, right="P", bid=9.9, ask=10.1)
    forward = parity_forward(pair_by_strike([call, put]))
    projection = build_option_price_bs_projection(
        mid=20.0,
        iv=0.22,
        strike=6000.0,
        right="C",
        spot=6010.0,
        target=6020.0,
        tau_now_years=180 * 60 / YEAR_SECONDS,
        em_points=30.0,
        slope_per_point=None,
        forward_now=forward,
    )

    assert forward == pytest.approx(6010.0)
    assert projection is not None
    assert projection.pricing_kernel == "black76_parity_forward"
    assert projection.price_range_low <= projection.projected_mid <= projection.price_range_high
    assert projection.forward_at_touch == pytest.approx(6020.0)


def test_trigger_coordinate_uses_official_rth_chain_gth_then_es(monkeypatch) -> None:
    spx = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.SCHWAB,
        provider_symbol="$SPX",
        received_at=NOW,
        last_update_at=NOW,
        quote_time=NOW,
        quality=MarketDataQuality.LIVE,
        bid=5999.0,
        ask=6001.0,
    )
    es = Quote(
        instrument=InstrumentId.future("ES"),
        provider=Provider.IBKR,
        provider_symbol="ESU6",
        received_at=NOW,
        last_update_at=NOW,
        quote_time=NOW,
        quality=MarketDataQuality.LIVE,
        bid=6049.75,
        ask=6050.25,
    )
    monkeypatch.setattr(
        "spx_spark.application.order_map.trigger_coordinates.DEFAULT_MARKET_CALENDAR.is_rth_open",
        lambda _now: True,
    )
    official = resolve_trigger_coordinate(_state(spx, es), None, now=NOW, qualified_es_basis=50)
    assert official.kind is TriggerCoordinateKind.OFFICIAL_SPX
    assert official.trigger_level(6010) == 6010

    monkeypatch.setattr(
        "spx_spark.application.order_map.trigger_coordinates.DEFAULT_MARKET_CALENDAR.is_rth_open",
        lambda _now: False,
    )
    monkeypatch.setattr(
        "spx_spark.application.order_map.trigger_coordinates.actionable_chain_implied_spot",
        lambda *_args, **_kwargs: 6002.0,
    )
    chain = resolve_trigger_coordinate(
        _state(es),
        SimpleNamespace(expiries=[SimpleNamespace(expiry="20260713")]),
        now=NOW,
        qualified_es_basis=50,
    )
    assert chain.kind is TriggerCoordinateKind.CHAIN_IMPLIED_SPX

    monkeypatch.setattr(
        "spx_spark.application.order_map.trigger_coordinates.actionable_chain_implied_spot",
        lambda *_args, **_kwargs: None,
    )
    equivalent = resolve_trigger_coordinate(
        _state(es),
        SimpleNamespace(expiries=[SimpleNamespace(expiry="20260713")]),
        now=NOW,
        qualified_es_basis=50,
    )
    assert equivalent.kind is TriggerCoordinateKind.ES_EQUIVALENT
    assert equivalent.observed_value == pytest.approx(6050.0)
    assert equivalent.trigger_level(6000.0) == pytest.approx(6050.0)


def test_pricing_outcome_auto_fills_touch_prefill_and_horizons(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    store = LatestStateStore(storage)
    quote = _option(bid=3.7, ask=3.9)
    store.update([quote], now=NOW)
    coordinate = {
        "kind": "official_spx",
        "instrument_id": "index:SPX",
        "observed_value": 99.0,
        "target_value": 100.0,
    }
    repricing = {
        "status": "repriced",
        "event_id": "level:test",
        "level_kind": "put_wall",
        "spx_level": 100.0,
        "trigger_coordinate": coordinate,
        "expected_move_points": 20.0,
        "trend_regime": "downtrend",
        "volatility_regime": "normal",
        "candidates": [
            {
                "play": "level_fade_call",
                "contract_id": quote.instrument.canonical_id,
                "right": "C",
                "limit_aggressive": 4.0,
                "projection_range_low": 3.8,
                "projection_range_high": 4.5,
                "projection_tau_now_minutes": 180.0,
            }
        ],
    }
    decision = {"trigger_coordinate": coordinate}
    first = advance_pricing_outcomes(storage, repricing, decision, now=NOW)
    assert first["open_count"] == 1

    for seconds, mid in ((5, 5.0), (65, 5.5), (305, 4.5), (905, 6.0)):
        at = NOW + timedelta(seconds=seconds)
        updated = _option(bid=mid - 0.1, ask=mid + 0.1, now=at)
        store.update([updated], now=at)
        current = dict(coordinate, observed_value=100.0)
        result = advance_pricing_outcomes(
            storage,
            {"status": "idle"},
            {"trigger_coordinate": current},
            now=at,
        )
    assert result["completed_count"] == 1
    outcome_path = next(
        (Path(storage.data_root) / "features" / "pricing_outcomes").glob("date=*/outcomes.jsonl")
    )
    outcome = json.loads(outcome_path.read_text().splitlines()[0])
    assert outcome["touched"] is True
    assert outcome["prefill_before_touch"] is True
    assert outcome["touch_mid"] == pytest.approx(5.0)
    assert set(outcome["horizons"]) == {"60", "300", "900"}
    assert outcome["horizons"]["900"]["mfe_fraction"] == pytest.approx(0.2)


def test_touch_time_model_waits_for_five_sessions_then_calibrates(tmp_path: Path) -> None:
    root = tmp_path / "features" / "pricing_outcomes" / "date=2026-07-13"
    root.mkdir(parents=True)
    rows = []
    for session in range(5):
        for sample in range(4):
            rows.append(
                {
                    "session_date": f"2026-07-{13 + session:02d}",
                    "distance_over_em": 0.4,
                    "session_bucket": "rth_open",
                    "volatility_regime": "normal",
                    "trend_regime": "downtrend",
                    "actual_touch_fraction": 0.20 + sample * 0.10,
                }
            )
    (root / "outcomes.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
    )

    estimate = estimate_touch_time(
        str(tmp_path),
        distance_over_em=0.4,
        session_bucket="rth_open",
        volatility_regime="normal",
        trend_regime="downtrend",
    )

    assert estimate.calibrated is True
    assert estimate.sample_count == 20
    assert estimate.session_count == 5
    assert estimate.early_fraction < estimate.base_fraction < estimate.late_fraction
