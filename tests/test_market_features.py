from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from spx_spark.application.globex_trend.service import globex_session_id
from spx_spark.application.market_features.composition import (
    build_decision_audit,
    build_decision_context,
)
from spx_spark.application.market_features.decision_filters import (
    build_breakout_filter,
    build_regime_decision,
)
from spx_spark.application.market_features.market import (
    build_minute_market_frame,
    session_segment,
)
from spx_spark.application.market_features.models import (
    FrameQuality,
    L1MicrostructureFrame,
    MinuteMarketFrame,
    OptionStructureFrame,
)
from spx_spark.application.market_features.options import (
    _fresh_front_quotes,
    _wall_rank_persistence,
    build_option_structure_frame,
    imbalance,
    provider_mid_divergences,
)
from spx_spark.analytics.options.models import OptionsMap, UnderlierReference
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    Provider,
    Quote,
)
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.storage import LatestState


UTC = timezone.utc


def test_wall_rank_persistence_tracks_primary_rank_and_confidence() -> None:
    history = [
        {
            "front_expiry": "20260722",
            "structure": {
                "call_walls": [{"strike": strike} for strike in strikes],
            },
        }
        for strikes in (
            (7600.0, 7610.0),
            (7610.0, 7600.0),
            (7610.0, 7620.0),
            (7600.0, 7620.0),
        )
    ]

    result = _wall_rank_persistence(
        "20260722",
        [7600.0, 7610.0],
        history,
        side="call",
    )

    assert result["observations"] == 4
    assert result["top4_presence_ratio"] == 0.75
    assert result["same_rank_ratio"] == 0.5
    assert result["mean_rank_score"] == 0.6875
    assert result["top4_presence_confidence_95"] == [0.3006, 0.9544]


def test_missing_live_chain_retains_same_expiry_structure_without_pricing() -> None:
    now = datetime(2026, 7, 16, 1, 0, tzinfo=UTC)
    state = LatestState(created_at=now, as_of=now, quotes=(), best_quotes=())
    empty_map = OptionsMap(
        created_at=now,
        as_of=now,
        underlier=UnderlierReference(price=None, source=None),
        expiries=(),
        warnings=("missing live SPXW",),
    )
    last_usable = {
        "as_of": "2026-07-16T00:30:00+00:00",
        "front_expiry": "20260716",
        "structure": {
            "put_wall": 7525.0,
            "call_wall": 7625.0,
            "zero_gamma": 7572.0,
            "flip_zone": [7570.0, 7575.0],
            "gamma_state": "zero_gamma_transition",
            "put_walls": [{"strike": 7525.0, "open_interest": 3622.0, "gex": -1.3e9}],
            "call_walls": [{"strike": 7625.0, "open_interest": 1562.0, "gex": 9.3e8}],
        },
    }

    frame, contracts = build_option_structure_frame(
        state,
        empty_map,
        now=now,
        history=[],
        previous_contracts={},
        policy=MarketFeatureSettings(),
        last_usable_frame=last_usable,
    )

    assert frame.quality is FrameQuality.UNAVAILABLE
    assert frame.front_expiry == "20260716"
    assert frame.structure["frozen"] is True
    assert frame.structure["put_wall"] == 7525.0
    assert frame.structure["underlier"] is None
    assert frame.volatility["atm_iv_0dte"] is None
    assert frame.l1.quality is FrameQuality.UNAVAILABLE
    assert contracts == {}


def test_missing_chain_does_not_carry_prior_expiry_structure() -> None:
    now = datetime(2026, 7, 16, 1, 0, tzinfo=UTC)
    state = LatestState(created_at=now, as_of=now, quotes=(), best_quotes=())
    empty_map = OptionsMap(
        created_at=now,
        as_of=now,
        underlier=UnderlierReference(price=None, source=None),
        expiries=(),
        warnings=(),
    )
    frame, _ = build_option_structure_frame(
        state,
        empty_map,
        now=now,
        history=[],
        previous_contracts={},
        policy=MarketFeatureSettings(),
        last_usable_frame={
            "front_expiry": "20260715",
            "structure": {"put_wall": 7500.0},
        },
    )

    assert frame.front_expiry is None
    assert frame.structure.get("frozen") is not True
    assert frame.structure["put_wall"] is None


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
    assert frame.es["observed_at"] == now.isoformat()
    assert frame.es["source_at"] == now.isoformat()
    assert frame.es["transport_at"] == now.isoformat()
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


def test_gth_expected_move_starts_at_2015_et_and_resets_with_session() -> None:
    gth_open = datetime(2026, 7, 14, 0, 15, tzinfo=UTC)
    session_id = globex_session_id(gth_open)
    samples = [
        _market_sample(
            gth_open - timedelta(minutes=1),
            session_id=session_id,
            es=7495.0,
            volume=1_000.0,
        ),
        _market_sample(
            gth_open,
            session_id=session_id,
            es=7500.0,
            volume=1_100.0,
        ),
        _market_sample(
            gth_open + timedelta(minutes=15),
            session_id=session_id,
            es=7510.0,
            volume=1_200.0,
        ),
    ]

    frame = build_minute_market_frame(
        samples,
        now=gth_open + timedelta(minutes=15),
        expected_move_points=50.0,
        atm_iv=None,
        structural_levels={},
        volume_baselines={},
        policy=MarketFeatureSettings(),
    )

    assert frame.es["gth_open_price"] == 7500.0
    assert frame.es["gth_move_points"] == 10.0
    assert frame.es["gth_expected_move_used"] == pytest.approx(0.20)

    next_open = gth_open + timedelta(days=1)
    next_session = globex_session_id(next_open)
    reset = build_minute_market_frame(
        [
            *samples,
            _market_sample(
                next_open,
                session_id=next_session,
                es=7480.0,
                volume=100.0,
            ),
        ],
        now=next_open,
        expected_move_points=40.0,
        atm_iv=None,
        structural_levels={},
        volume_baselines={},
        policy=MarketFeatureSettings(),
    )

    assert reset.es["gth_open_price"] == 7480.0
    assert reset.es["gth_move_points"] == 0.0
    assert reset.es["gth_expected_move_used"] == 0.0


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
    assert frame.volume["price_volume_alignment_reason_5m"] == "insufficient_synchronized_window"


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
    assert provider_mid_divergences([schwab, stale_ibkr], policy=MarketFeatureSettings()) == []


def test_gth_hot_option_quotes_use_ibkr_even_when_schwab_is_newer() -> None:
    now = datetime(2026, 7, 14, 0, 30, tzinfo=UTC)
    instrument = InstrumentId.option(
        "SPX",
        expiry="20260714",
        strike=7550,
        right="C",
        trading_class="SPXW",
    )
    schwab = _option_quote(instrument, Provider.SCHWAB, now, bid=10.0, ask=10.2)
    ibkr = _option_quote(
        instrument,
        Provider.IBKR,
        now - timedelta(seconds=2),
        bid=10.1,
        ask=10.3,
    )
    state = LatestState(
        created_at=now,
        as_of=now,
        quotes=(schwab, ibkr),
        best_quotes=(schwab,),
    )

    selected = _fresh_front_quotes(
        state,
        expiry="20260714",
        now=now,
        policy=MarketFeatureSettings(),
    )

    assert len(selected) == 1
    assert selected[0].provider is Provider.IBKR


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


def test_dense_gex_and_dex_divergence_block_weak_breakout() -> None:
    market = _decision_market_frame(
        return_15=2.0,
        return_60=-3.0,
        efficiency=0.12,
        vwap_distance=-2.0,
        vwap_slope=-1.0,
        volume_alignment="price_volume_divergent",
        cross_confirmation="divergent",
    )
    options = _decision_option_frame(
        abs_gex=100.0,
        wall_gex=35.0,
        next_wall_distance=5.0,
        gamma_top_share=0.80,
        oi_dex_ratio=0.35,
        volume_dex_ratio=-0.40,
    )
    level = {
        "event_id": "level:blocked",
        "phase": "confirmed",
        "thesis": "breakout",
        "direction": "up",
        "level_kind": "call_wall",
        "level": 7550.0,
    }
    policy = MarketFeatureSettings()
    regime = build_regime_decision(
        market,
        options,
        trend={"regime": "neutral"},
        level_decision=level,
        policy=policy,
    )
    result = build_breakout_filter(
        market,
        options,
        level_decision=level,
        regime_decision=regime,
        policy=policy,
    )

    assert result["verdict"] == "blocked"
    assert result["actionable"] is False
    assert result["barrier_score"] > result["impulse_score"]
    assert result["oi_volume_dex_divergent"] is True
    assert "dense_local_gex_ahead" in result["evidence"]


def test_aligned_path_and_dex_allow_confirmed_breakout() -> None:
    market = _decision_market_frame(
        return_15=12.0,
        return_60=28.0,
        efficiency=0.80,
        vwap_distance=8.0,
        vwap_slope=3.0,
        volume_alignment="price_volume_aligned",
        cross_confirmation="confirmed",
    )
    options = _decision_option_frame(
        abs_gex=1000.0,
        wall_gex=10.0,
        next_wall_distance=30.0,
        gamma_top_share=0.10,
        oi_dex_ratio=0.30,
        volume_dex_ratio=0.45,
    )
    level = {
        "event_id": "level:supported",
        "phase": "confirmed",
        "thesis": "breakout",
        "direction": "up",
        "level_kind": "call_wall",
        "level": 7550.0,
    }
    policy = MarketFeatureSettings()
    regime = build_regime_decision(
        market,
        options,
        trend={"regime": "bullish"},
        level_decision=level,
        policy=policy,
    )
    result = build_breakout_filter(
        market,
        options,
        level_decision=level,
        regime_decision=regime,
        policy=policy,
    )

    assert regime["mode"] == "trending"
    assert result["verdict"] == "supported"
    assert result["actionable"] is True
    assert result["impulse_score"] > result["barrier_score"]


def test_opposite_signed_confirmation_cannot_support_upward_breakout() -> None:
    market = _decision_market_frame(
        return_15=-12.0,
        return_60=-28.0,
        efficiency=0.80,
        vwap_distance=-8.0,
        vwap_slope=-3.0,
        volume_alignment="price_volume_aligned",
        cross_confirmation="confirmed",
    )
    options = _decision_option_frame(
        abs_gex=1000.0,
        wall_gex=10.0,
        next_wall_distance=30.0,
        gamma_top_share=0.10,
        oi_dex_ratio=0.30,
        volume_dex_ratio=0.45,
    )
    level = {
        "event_id": "level:opposed",
        "phase": "confirmed",
        "thesis": "breakout",
        "direction": "up",
        "level_kind": "call_wall",
        "level": 7550.0,
    }
    policy = MarketFeatureSettings()
    regime = build_regime_decision(
        market,
        options,
        trend={"regime": "bearish"},
        level_decision=level,
        policy=policy,
    )
    result = build_breakout_filter(
        market,
        options,
        level_decision=level,
        regime_decision=regime,
        policy=policy,
    )

    assert regime["mode"] == "trending"
    assert regime["direction"] == "down"
    assert result["verdict"] == "blocked"
    assert result["actionable"] is False
    assert "price_volume_supports_breakout" not in result["evidence"]
    assert "es_spy_supports_breakout" not in result["evidence"]
    assert "price_volume_opposes_breakout" in result["evidence"]
    assert "es_spy_opposes_breakout" in result["evidence"]
    assert "trend_efficiency_opposes_breakout" in result["evidence"]
    assert "trending_regime_opposes_breakout" in result["evidence"]
    assert "regime_direction_opposes_breakout" in result["invalidations"]


def test_recent_volume_opposition_is_barrier_not_breakout_support() -> None:
    market = _decision_market_frame(
        return_15=12.0,
        return_60=28.0,
        efficiency=0.80,
        vwap_distance=8.0,
        vwap_slope=3.0,
        volume_alignment="price_volume_aligned",
        cross_confirmation="confirmed",
        return_5=-4.0,
    )
    options = _decision_option_frame(
        abs_gex=1000.0,
        wall_gex=10.0,
        next_wall_distance=30.0,
        gamma_top_share=0.10,
        oi_dex_ratio=0.30,
        volume_dex_ratio=0.45,
    )
    level = {
        "event_id": "level:volume-opposed",
        "phase": "confirmed",
        "thesis": "breakout",
        "direction": "up",
        "level_kind": "call_wall",
        "level": 7550.0,
    }
    policy = MarketFeatureSettings()
    regime = build_regime_decision(
        market,
        options,
        trend={"regime": "bullish"},
        level_decision=level,
        policy=policy,
    )
    result = build_breakout_filter(
        market,
        options,
        level_decision=level,
        regime_decision=regime,
        policy=policy,
    )

    assert "price_volume_supports_breakout" not in result["evidence"]
    assert "price_volume_opposes_breakout" in result["evidence"]
    assert "es_spy_supports_breakout" in result["evidence"]


def test_vix_confirmation_strengthens_trend_and_breakout() -> None:
    confirming = _decision_market_frame(
        return_15=12.0,
        return_60=28.0,
        efficiency=0.35,
        vwap_distance=8.0,
        vwap_slope=3.0,
        volume_alignment="price_volume_aligned",
        cross_confirmation="confirmed",
        volatility={
            "vix1d_vix_ratio": 1.05,
            "vix_return_15m_pct": -2.0,
            "vix_vvix_direction_confirmation_15m": "confirmed",
        },
    )
    opposing = replace(
        confirming,
        volatility={
            "vix1d_vix_ratio": 1.05,
            "vix_return_15m_pct": 2.0,
            "vix_vvix_direction_confirmation_15m": "confirmed",
        },
    )
    options = _decision_option_frame(
        abs_gex=1000.0,
        wall_gex=10.0,
        next_wall_distance=30.0,
        gamma_top_share=0.10,
        oi_dex_ratio=0.30,
        volume_dex_ratio=0.45,
    )
    level = {
        "event_id": "level:vix",
        "phase": "confirmed",
        "thesis": "breakout",
        "direction": "up",
        "level_kind": "call_wall",
        "level": 7550.0,
    }
    policy = MarketFeatureSettings()

    confirming_regime = build_regime_decision(
        confirming,
        options,
        trend={"regime": "bullish"},
        level_decision=level,
        policy=policy,
    )
    opposing_regime = build_regime_decision(
        opposing,
        options,
        trend={"regime": "bullish"},
        level_decision=level,
        policy=policy,
    )
    confirming_filter = build_breakout_filter(
        confirming,
        options,
        level_decision=level,
        regime_decision=confirming_regime,
        policy=policy,
    )
    opposing_filter = build_breakout_filter(
        opposing,
        options,
        level_decision=level,
        regime_decision=opposing_regime,
        policy=policy,
    )

    assert confirming_regime["volatility_regime"] == "stressed"
    assert confirming_regime["trend_score"] > opposing_regime["trend_score"]
    assert confirming_filter["impulse_score"] > opposing_filter["impulse_score"]
    assert confirming_filter["barrier_score"] < opposing_filter["barrier_score"]
    assert "vix_vvix_support_breakout" in confirming_filter["evidence"]
    assert "vix_vvix_oppose_breakout" in opposing_filter["evidence"]


def _decision_market_frame(
    *,
    return_15: float,
    return_60: float,
    efficiency: float,
    vwap_distance: float,
    vwap_slope: float,
    volume_alignment: str,
    cross_confirmation: str,
    volatility: dict[str, object] | None = None,
    return_5: float | None = None,
) -> MinuteMarketFrame:
    now = datetime(2026, 7, 13, 15, 0, tzinfo=UTC)
    return MinuteMarketFrame(
        schema_version=1,
        frame_id="market:decision",
        session_id="2026-07-13",
        as_of=now,
        quality=FrameQuality.READY,
        es={
            "return_5m_points": return_15 if return_5 is None else return_5,
            "return_15m_points": return_15,
            "return_60m_points": return_60,
            "trend_efficiency_60m": efficiency,
            "vwap_distance_points": vwap_distance,
            "vwap_slope_15m_points": vwap_slope,
            "higher_low_60m": return_60 > 0,
            "lower_high_60m": return_60 < 0,
        },
        session_ranges={},
        volume={"price_volume_alignment_5m": volume_alignment},
        cross_asset={"es_spy_direction_confirmation_15m": cross_confirmation},
        volatility=volatility or {},
        diagnostics={},
    )


def _decision_option_frame(
    *,
    abs_gex: float,
    wall_gex: float,
    next_wall_distance: float,
    gamma_top_share: float,
    oi_dex_ratio: float,
    volume_dex_ratio: float,
) -> OptionStructureFrame:
    now = datetime(2026, 7, 13, 15, 0, tzinfo=UTC)
    return OptionStructureFrame(
        schema_version=1,
        frame_id="options:decision",
        as_of=now,
        quality=FrameQuality.READY,
        front_expiry="20260713",
        next_expiry="20260714",
        structure={
            "underlier": 7555.0,
            "put_wall": 7500.0,
            "call_wall": 7550.0,
            "abs_gex": abs_gex,
            "call_walls": [
                {"strike": 7550.0, "gex": wall_gex},
                {"strike": 7550.0 + next_wall_distance, "gex": wall_gex},
            ],
            "put_walls": [],
        },
        volatility={},
        concentration={"gamma_top_share": gamma_top_share},
        density={},
        l1=L1MicrostructureFrame(
            quality=FrameQuality.READY,
            expiry="20260713",
            contract_count=10,
            metrics={"liquidity_score": 90.0},
            diagnostics={},
        ),
        diagnostics={},
        exposure={
            "quality": "ok",
            "oi_weighted": {"net_dex_ratio_proxy": oi_dex_ratio},
            "volume_weighted": {"net_dex_ratio_proxy": volume_dex_ratio},
            "gex_weighting_divergence": 0.0,
        },
    )


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
        "transport_at": at.isoformat(),
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
