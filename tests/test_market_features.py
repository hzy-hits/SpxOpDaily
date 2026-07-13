from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from spx_spark.application.globex_trend.service import globex_session_id
from spx_spark.application.market_features.composition import (
    build_decision_audit,
    build_decision_context,
)
from spx_spark.application.market_features.market import (
    build_minute_market_frame,
    session_segment,
)
from spx_spark.application.market_features.models import (
    FrameQuality,
    L1MicrostructureFrame,
    OptionStructureFrame,
)
from spx_spark.application.market_features.options import (
    imbalance,
    provider_mid_divergences,
)
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    Provider,
    Quote,
)
from spx_spark.settings.market_features import MarketFeatureSettings


UTC = timezone.utc


def test_minute_frame_calculates_path_volume_cross_asset_and_volatility() -> None:
    start = datetime(2026, 7, 13, 6, 0, tzinfo=UTC)
    now = start + timedelta(minutes=180)
    session_id = globex_session_id(now)
    samples = [
        _market_sample(
            start + timedelta(minutes=minute),
            session_id=session_id,
            es=7500.0 + minute,
            volume=1_000_000.0 + 100 * minute,
            spy=500.0 + minute / 10,
            qqq=600.0 + minute / 8,
            rsp=200.0 + minute / 25,
            spx=7450.0 + minute,
            vix=15.0 + minute / 100,
            vvix=80.0 + minute / 10,
        )
        for minute in range(181)
    ]
    slot = now.astimezone().strftime("%H:%M")
    # Frame code keys same-clock history in New York, so use the frame output
    # only for readiness assertions and keep this baseline intentionally empty.
    _ = slot
    frame = build_minute_market_frame(
        samples,
        now=now,
        expected_move_points=100.0,
        atm_iv=0.20,
        structural_levels={"put_wall": 7650.0, "call_wall": 7700.0},
        volume_baselines={},
        policy=MarketFeatureSettings(),
    )

    assert frame.quality is FrameQuality.READY
    assert frame.es["return_1m_points"] == pytest.approx(1.0)
    assert frame.es["return_15m_points"] == pytest.approx(15.0)
    assert frame.es["return_60m_points"] == pytest.approx(60.0)
    assert frame.es["return_180m_points"] == pytest.approx(180.0)
    assert frame.es["trend_efficiency_60m"] == pytest.approx(1.0)
    assert frame.volume["volume_delta_5m"] == pytest.approx(500.0)
    assert frame.volume["pace_5m_per_minute"] == pytest.approx(100.0)
    assert frame.volume["session_vwap"] is not None
    assert frame.volume["overnight_vwap"] is not None
    assert frame.cross_asset["es_spx_basis_points"] == pytest.approx(50.0)
    assert frame.cross_asset["es_spx_basis_deviation_points"] == pytest.approx(0.0)
    assert frame.cross_asset["es_spy_direction_confirmation_15m"] == "confirmed"
    assert frame.volatility["vix1d_vix_ratio"] is None
    assert frame.volatility["es_realized_vol_60m_annualized"] is not None
    assert frame.volatility["atm_iv_minus_es_realized_vol"] is not None


def test_volume_reset_and_insufficient_history_do_not_publish_percentile() -> None:
    now = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
    session_id = globex_session_id(now)
    samples = [
        _market_sample(
            now - timedelta(minutes=5),
            session_id=session_id,
            es=7600.0,
            volume=1_000_000.0,
        ),
        _market_sample(now, session_id=session_id, es=7601.0, volume=10.0),
    ]

    frame = build_minute_market_frame(
        samples,
        now=now,
        expected_move_points=None,
        atm_iv=None,
        structural_levels={},
        volume_baselines={},
        policy=MarketFeatureSettings(),
    )

    assert frame.volume["session_reset_detected"] is True
    assert frame.volume["volume_delta_5m"] is None
    assert frame.volume["pace_percentile_20_sessions"] is None
    assert frame.volume["pace_baseline_ready"] is False


def test_volume_alignment_does_not_mix_incomplete_price_and_volume_windows() -> None:
    now = datetime(2026, 7, 13, 15, 0, tzinfo=UTC)
    session_id = globex_session_id(now)
    samples = [
        _market_sample(
            now - timedelta(minutes=7),
            session_id=session_id,
            es=7590.0,
            volume=100_000.0,
        ),
        _market_sample(now, session_id=session_id, es=7600.0, volume=110_000.0),
    ]

    frame = build_minute_market_frame(
        samples,
        now=now,
        expected_move_points=None,
        atm_iv=None,
        structural_levels={},
        volume_baselines={},
        policy=MarketFeatureSettings(),
    )

    assert frame.volume["volume_delta_5m"] is None
    assert frame.volume["price_volume_alignment_5m"] == "unavailable"
    assert (
        frame.volume["price_volume_alignment_reason_5m"]
        == "insufficient_synchronized_window"
    )


def test_l1_helpers_measure_imbalance_and_only_compare_synchronized_providers() -> None:
    now = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
    instrument = InstrumentId.option(
        "SPX",
        expiry="20260713",
        strike=7550,
        right="C",
        trading_class="SPXW",
    )
    schwab = _option_quote(instrument, Provider.SCHWAB, now, bid=10.0, ask=10.4)
    ibkr = _option_quote(instrument, Provider.IBKR, now - timedelta(seconds=2), bid=10.2, ask=10.4)

    assert imbalance(30, 10) == pytest.approx(0.5)
    assert provider_mid_divergences(
        [schwab, ibkr], policy=MarketFeatureSettings()
    ) == pytest.approx([0.1])

    stale_ibkr = _option_quote(
        instrument,
        Provider.IBKR,
        now - timedelta(seconds=10),
        bid=10.2,
        ask=10.4,
    )
    assert provider_mid_divergences(
        [schwab, stale_ibkr], policy=MarketFeatureSettings()
    ) == []


def test_decision_context_audit_only_changes_on_meaningful_state() -> None:
    now = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
    market = build_minute_market_frame(
        [_market_sample(now, session_id=globex_session_id(now), es=7600, volume=1000)],
        now=now,
        expected_move_points=None,
        atm_iv=None,
        structural_levels={},
        volume_baselines={},
        policy=MarketFeatureSettings(),
    )
    options = OptionStructureFrame(
        schema_version=1,
        frame_id="options:test",
        as_of=now,
        quality=FrameQuality.READY,
        front_expiry="20260713",
        next_expiry="20260714",
        structure={},
        volatility={},
        concentration={},
        density={},
        l1=L1MicrostructureFrame(
            quality=FrameQuality.READY,
            expiry="20260713",
            contract_count=10,
            metrics={"liquidity_score": 80.0},
            diagnostics={},
        ),
        diagnostics={},
    )
    context = build_decision_context(
        market,
        options,
        now=now,
        trend={"regime": "bearish"},
        level_decision={"event_id": "level:1", "phase": "testing"},
    )

    first = build_decision_audit(context, previous=None)
    duplicate = build_decision_audit(context, previous=context.to_dict())

    assert first is not None
    assert first.outcome_reference == "level:1"
    assert duplicate is None


def _market_sample(
    at: datetime,
    *,
    session_id: str,
    es: float,
    volume: float,
    spy: float | None = None,
    qqq: float | None = None,
    rsp: float | None = None,
    spx: float | None = None,
    vix: float | None = None,
    vvix: float | None = None,
) -> dict[str, object]:
    instruments: dict[str, dict[str, object]] = {
        "future:ES": _sample_quote(es, volume, at),
    }
    for instrument_id, value in (
        ("equity:SPY", spy),
        ("equity:QQQ", qqq),
        ("equity:RSP", rsp),
        ("index:SPX", spx),
        ("index:VIX", vix),
        ("index:VVIX", vvix),
    ):
        if value is not None:
            instruments[instrument_id] = _sample_quote(value, None, at)
    return {
        "at": at.isoformat(),
        "session_id": session_id,
        "segment": session_segment(at),
        "instruments": instruments,
        "es_by_provider": {},
    }


def _sample_quote(price: float, volume: float | None, at: datetime) -> dict[str, object]:
    return {
        "price": price,
        "provider": "schwab",
        "source_at": at.isoformat(),
        "volume": volume,
        "quality": "live",
    }


def _option_quote(
    instrument: InstrumentId,
    provider: Provider,
    at: datetime,
    *,
    bid: float,
    ask: float,
) -> Quote:
    return Quote(
        instrument=instrument,
        provider=provider,
        received_at=at,
        quote_time=at,
        last_update_at=at,
        quality=MarketDataQuality.LIVE,
        bid=bid,
        ask=ask,
        bid_size=10,
        ask_size=10,
    )
