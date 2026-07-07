from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from spx_spark.alert_engine import (
    effective_move_threshold_bps,
    evaluate_alerts,
    evaluate_payload,
    iv_surface_freshness_alert,
    iv_surface_alerts,
    movement_alerts,
    system_event_alerts,
)
from spx_spark.alert_profile import active_window
from spx_spark.iv_surface import IvSurfaceExpiry, IvSurfaceSnapshot
from spx_spark.market_context import build_market_context
from spx_spark.marketdata import (
    InstrumentId,
    InstrumentType,
    MarketDataQuality,
    OptionGreeks,
    Provider,
    ProviderState,
    ProviderStatus,
    Quote,
)
from spx_spark.options_map import build_options_map
from spx_spark.storage import LatestState


BJ_TZ = ZoneInfo("Asia/Shanghai")


def make_quote(
    instrument: InstrumentId,
    *,
    mark: float,
    close: float | None = None,
    quality: MarketDataQuality = MarketDataQuality.LIVE,
    now: datetime,
) -> Quote:
    return Quote(
        instrument=instrument,
        provider=Provider.IBKR,
        provider_symbol=instrument.canonical_id,
        received_at=now,
        quality=quality,
        mark=mark,
        close=close,
        quote_time=now,
    )


def make_state(
    *quotes: Quote,
    now: datetime,
    provider_states: tuple[ProviderState, ...] = (),
) -> LatestState:
    return LatestState(
        created_at=now,
        as_of=now,
        quotes=tuple(quotes),
        best_quotes=tuple(quotes),
        provider_states=provider_states,
    )


def make_option(
    *,
    expiry: str,
    strike: float,
    right: str,
    mark: float,
    now: datetime,
    quality: MarketDataQuality = MarketDataQuality.LIVE,
) -> Quote:
    return Quote(
        instrument=InstrumentId.option(
            "SPX",
            expiry=expiry,
            strike=strike,
            right=right,
            trading_class="SPXW",
        ),
        provider=Provider.IBKR,
        provider_symbol=f"SPXW:{expiry}:{strike}:{right}",
        received_at=now,
        quality=quality,
        bid=mark - 0.1,
        ask=mark + 0.1,
        mark=mark,
        open_interest=1000,
        quote_time=now,
        greeks=OptionGreeks(
            implied_vol=0.22,
            delta=0.5 if right == "C" else -0.5,
            gamma=0.003,
            theta=-1.0,
            vega=0.3,
            model="test",
        ),
    )


def make_surface(*, as_of: datetime) -> IvSurfaceSnapshot:
    return IvSurfaceSnapshot(
        created_at=as_of,
        as_of=as_of,
        underlier_price=7500.0,
        underlier_source="index:SPX",
        front_expiry="20260707",
        next_expiry="20260708",
        front_vs_next_atm_iv_gap=0.08,
        expiries=(
            IvSurfaceExpiry(
                expiry="20260707",
                atm_iv=0.28,
                atm_straddle_mid=30.0,
                expected_move_points=28.0,
                expected_move_pct=0.0037,
                put_skew_ratio=1.25,
                call_skew_ratio=1.0,
                smile_slope=-0.02,
                smile_curvature=0.01,
                iv_surface_level=0.29,
                iv_surface_shift_5m=0.04,
                atm_iv_jump_5m=0.04,
                put_skew_steepening_5m=0.10,
                call_wing_bid=False,
                smile_curvature_change_5m=0.01,
                surface_fit_quality="raw_grid",
                wide_quote_surface_degraded=False,
                gamma_state="mixed_gamma",
                zero_gamma=7500.0,
                put_wall=7450.0,
                call_wall=7550.0,
                option_count=20,
                iv_coverage_ratio=0.9,
                gamma_coverage_ratio=0.9,
                avg_spread_bps=100.0,
                warnings=(),
            ),
        ),
        warnings=(),
    )


def test_alert_engine_flags_missing_required_data_for_current_window() -> None:
    now = datetime(2026, 7, 6, 17, 0, tzinfo=BJ_TZ)
    state = make_state(now=now)

    payload = evaluate_payload(state, now=now)

    kinds = {alert["kind"] for alert in payload["alerts"]}
    assert payload["window"]["name"] == "early_premarket_dip_watch"
    assert payload["market_context"]["quality_summary"]["total_count"] >= 20
    assert "required_data_missing" in kinds
    assert payload["alert_count"] >= 4


def test_alert_engine_flags_large_move_from_close(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("ALERT_MOVEMENT_STATE_PATH", str(tmp_path / "movement-state.json"))
    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)
    spy = make_quote(
        InstrumentId.equity("SPY"),
        mark=754.0,
        close=750.0,
        now=now,
    )
    state = make_state(spy, now=now)

    payload = evaluate_payload(state, now=now)

    move_alerts = [
        alert for alert in payload["alerts"] if alert["kind"] == "price_move_from_close"
    ]
    assert payload["window"]["name"] == "close_one_hour"
    assert move_alerts
    assert move_alerts[0]["instrument_id"] == "equity:SPY"
    assert move_alerts[0]["dedup_group"] == "up:2"


def test_alert_engine_does_not_warn_for_optional_missing_at_critical_level() -> None:
    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)
    state = make_state(now=now)

    payload = evaluate_payload(state, now=now)

    optional_missing = [
        alert for alert in payload["alerts"] if alert["kind"] == "optional_data_missing"
    ]
    assert optional_missing
    assert all(alert["severity"] == "low" for alert in optional_missing)


def test_alert_engine_suppresses_option_wall_alert_when_0dte_quotes_are_stale() -> None:
    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)
    stale_time = now - timedelta(seconds=30)
    state = make_state(
        make_quote(InstrumentId.index("SPX"), mark=7500.0, close=7490.0, now=now),
        replace(
            make_option(expiry="20260707", strike=7500, right="C", mark=10.0, now=stale_time),
            quality=MarketDataQuality.STALE,
        ),
        make_option(expiry="20260707", strike=7500, right="P", mark=11.0, now=now),
        now=now,
    )
    window = active_window(now)

    alerts = evaluate_alerts(
        state,
        window=window,
        options_map=build_options_map(state),
        market_context=build_market_context(state),
    )
    kinds = {alert.kind for alert in alerts}

    assert "option_quote_freshness_degraded" in kinds
    assert "option_wall_proximity" not in kinds
    assert "option_gamma_regime" not in kinds


def test_iv_surface_stale_alert_suppresses_surface_alerts() -> None:
    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)
    surface = make_surface(as_of=now - timedelta(minutes=10))

    freshness = iv_surface_freshness_alert(surface, now=now)
    active_alerts = [] if freshness is not None else iv_surface_alerts(surface, window=active_window(now))

    assert freshness is not None
    assert freshness.kind == "iv_surface_stale"
    assert active_alerts == []


def test_hyperliquid_proxy_is_context_only_without_tradfi_anchor() -> None:
    now = datetime(2026, 7, 7, 7, 15, tzinfo=BJ_TZ)
    hl = Quote(
        instrument=InstrumentId(
            symbol="xyz:SP500",
            instrument_type=InstrumentType.CRYPTO_PERP,
        ),
        provider=Provider.HYPERLIQUID,
        provider_symbol="xyz:SP500",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7600.0,
        close=7500.0,
        quote_time=now,
    )
    state = make_state(hl, now=now)
    context = build_market_context(state)

    alerts = evaluate_alerts(state, window=active_window(now), market_context=context)
    kinds = {alert.kind for alert in alerts}
    proxy_alerts = [alert for alert in alerts if alert.kind == "hyperliquid_proxy_quality_gate"]

    assert "price_move_from_close" not in kinds
    assert proxy_alerts
    assert proxy_alerts[0].research_only is True


def test_hyperliquid_proxy_can_trigger_degraded_watch_when_ibkr_feed_is_unavailable() -> None:
    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)
    hl = Quote(
        instrument=InstrumentId(
            symbol="xyz:SP500",
            instrument_type=InstrumentType.CRYPTO_PERP,
        ),
        provider=Provider.HYPERLIQUID,
        provider_symbol="xyz:SP500",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7600.0,
        close=7500.0,
        quote_time=now,
    )
    ibkr_state = ProviderState(
        provider=Provider.IBKR,
        status=ProviderStatus.UNAVAILABLE,
        checked_at=now,
        reason="competing session; phone owns trading session",
        connected=False,
        authenticated=False,
        priority=0,
    )
    state = make_state(hl, now=now, provider_states=(ibkr_state,))
    context = build_market_context(state)

    alerts = evaluate_alerts(state, window=active_window(now), market_context=context)
    fallback_alerts = [alert for alert in alerts if alert.kind == "broker_unavailable_proxy_watch"]

    assert fallback_alerts
    assert fallback_alerts[0].instrument_id == "index:SPX"
    assert fallback_alerts[0].quality == "degraded"
    assert fallback_alerts[0].research_only is False


def test_ibkr_session_transition_alerts_are_edge_triggered(tmp_path, monkeypatch) -> None:
    state_path = tmp_path / "system-event-state.json"
    monkeypatch.setenv("ALERT_SYSTEM_EVENT_STATE_PATH", str(state_path))
    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)
    interrupted = ProviderState(
        provider=Provider.IBKR,
        status=ProviderStatus.UNAVAILABLE,
        checked_at=now,
        reason="competing session blocks live market data (IBKR 10197)",
        connected=False,
        authenticated=False,
        priority=0,
    )
    state = make_state(now=now, provider_states=(interrupted,))

    first_alerts = system_event_alerts(state)
    repeated_alerts = system_event_alerts(state)
    restored = replace(
        interrupted,
        status=ProviderStatus.AVAILABLE,
        checked_at=now + timedelta(minutes=5),
        reason=None,
        connected=True,
        authenticated=True,
    )
    restored_alerts = system_event_alerts(
        make_state(now=now + timedelta(minutes=5), provider_states=(restored,))
    )

    assert [alert.kind for alert in first_alerts] == ["ibkr_session_interrupted"]
    assert first_alerts[0].instrument_id == "index:SPX"
    assert first_alerts[0].source_gate == "ibkr_session_state"
    assert repeated_alerts == []
    assert [alert.kind for alert in restored_alerts] == ["ibkr_session_restored"]


def test_ibkr_unknown_state_preserves_interrupted_status_for_restore(tmp_path, monkeypatch) -> None:
    state_path = tmp_path / "system-event-state.json"
    monkeypatch.setenv("ALERT_SYSTEM_EVENT_STATE_PATH", str(state_path))
    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)
    interrupted = ProviderState(
        provider=Provider.IBKR,
        status=ProviderStatus.UNAVAILABLE,
        checked_at=now,
        reason="competing session blocks live market data (IBKR 10197)",
        connected=False,
        authenticated=False,
        priority=0,
    )
    stale = replace(interrupted, checked_at=now - timedelta(hours=1))
    restored = replace(
        interrupted,
        status=ProviderStatus.AVAILABLE,
        checked_at=now + timedelta(minutes=1),
        reason=None,
        connected=True,
        authenticated=True,
    )

    assert system_event_alerts(make_state(now=now, provider_states=(interrupted,)))
    assert system_event_alerts(make_state(now=now, provider_states=(stale,))) == []
    restored_alerts = system_event_alerts(
        make_state(now=now + timedelta(minutes=1), provider_states=(restored,))
    )

    assert [alert.kind for alert in restored_alerts] == ["ibkr_session_restored"]


def test_ibkr_degraded_reconnect_state_does_not_swallow_restored_alert(
    tmp_path, monkeypatch
) -> None:
    state_path = tmp_path / "system-event-state.json"
    monkeypatch.setenv("ALERT_SYSTEM_EVENT_STATE_PATH", str(state_path))
    now = datetime(2026, 7, 7, 11, 9, tzinfo=BJ_TZ)
    interrupted = ProviderState(
        provider=Provider.IBKR,
        status=ProviderStatus.UNAVAILABLE,
        checked_at=now,
        reason="disconnected",
        connected=False,
        authenticated=None,
        priority=0,
    )
    reconnecting = ProviderState(
        provider=Provider.IBKR,
        status=ProviderStatus.DEGRADED,
        checked_at=now + timedelta(minutes=1),
        reason="connected; awaiting first flush",
        connected=True,
        authenticated=True,
        priority=0,
    )
    restored = replace(
        interrupted,
        status=ProviderStatus.AVAILABLE,
        checked_at=now + timedelta(minutes=2),
        reason=None,
        connected=True,
        authenticated=True,
    )

    assert [
        alert.kind
        for alert in system_event_alerts(make_state(now=now, provider_states=(interrupted,)))
    ] == ["ibkr_session_interrupted"]
    assert (
        system_event_alerts(
            make_state(now=now + timedelta(minutes=1), provider_states=(reconnecting,))
        )
        == []
    )
    restored_alerts = system_event_alerts(
        make_state(now=now + timedelta(minutes=2), provider_states=(restored,))
    )

    assert [alert.kind for alert in restored_alerts] == ["ibkr_session_restored"]


def test_evaluate_payload_can_detect_system_event_without_persisting(tmp_path, monkeypatch) -> None:
    state_path = tmp_path / "system-event-state.json"
    monkeypatch.setenv("ALERT_SYSTEM_EVENT_STATE_PATH", str(state_path))
    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)
    interrupted = ProviderState(
        provider=Provider.IBKR,
        status=ProviderStatus.UNAVAILABLE,
        checked_at=now,
        reason="competing session blocks live market data (IBKR 10197)",
        connected=False,
        authenticated=False,
        priority=0,
    )

    payload = evaluate_payload(
        make_state(now=now, provider_states=(interrupted,)),
        now=now,
        persist_system_events=False,
    )

    assert any(
        alert["kind"] == "ibkr_session_interrupted"
        for alert in payload["alerts"]
        if isinstance(alert, dict)
    )
    assert not state_path.exists()


def test_movement_alerts_are_edge_triggered(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("ALERT_MOVEMENT_STATE_PATH", str(tmp_path / "movement-state.json"))
    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)
    window = active_window(now)

    def spy_state(mark: float) -> LatestState:
        return make_state(
            make_quote(InstrumentId.equity("SPY"), mark=mark, close=750.0, now=now),
            now=now,
        )

    def spy_alerts(mark: float, *, persist: bool) -> list:
        return movement_alerts(
            spy_state(mark),
            window=window,
            market_context=build_market_context(spy_state(mark)),
            persist=persist,
        )

    first = spy_alerts(752.25, persist=True)
    assert len(first) == 1
    assert first[0].dedup_group == "up:1"

    repeated = spy_alerts(752.25, persist=True)
    assert repeated == []

    upgraded = spy_alerts(754.5, persist=True)
    assert len(upgraded) == 1
    assert upgraded[0].dedup_group == "up:3"

    flipped = spy_alerts(747.75, persist=True)
    assert len(flipped) == 1
    assert flipped[0].dedup_group == "down:1"

    below = spy_alerts(750.0, persist=True)
    assert below == []

    recross = spy_alerts(752.25, persist=True)
    assert len(recross) == 1
    assert recross[0].dedup_group == "up:1"


def test_overnight_dip_escalates_to_high_severity(tmp_path, monkeypatch) -> None:
    import spx_spark.alert_engine as ae

    monkeypatch.setenv("ALERT_MOVEMENT_STATE_PATH", str(tmp_path / "movement-state.json"))
    # Day EM = 41 bps (low-vol regime); quiet Asia-session window.
    monkeypatch.setattr(ae, "front_expected_move_pct", lambda *_args, **_kwargs: 0.0041)
    now = datetime(2026, 7, 7, 12, 30, tzinfo=BJ_TZ)
    window = active_window(now)
    assert window.priority == "low"

    state = make_state(
        make_quote(InstrumentId.equity("SPY"), mark=748.0, close=750.0, now=now),
        now=now,
    )
    alerts = movement_alerts(
        state,
        window=window,
        market_context=build_market_context(state),
        persist=False,
        options_map=object(),
    )

    moves = [alert for alert in alerts if alert.kind == "price_move_from_close"]
    assert len(moves) == 1
    # -26.7 bps in a 41 bps EM day: clears the 24.6 bps quiet bar and gets
    # escalated to high so it passes the notify severity gate at night.
    assert moves[0].severity == "high"
    assert moves[0].dedup_group == "down:1"
    assert "em_consumed" in moves[0].detail


def test_iv_surface_degraded_expiry_still_emits_movement_alerts() -> None:
    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)
    surface = IvSurfaceSnapshot(
        created_at=now,
        as_of=now,
        underlier_price=7500.0,
        underlier_source="index:SPX",
        front_expiry="20260707",
        next_expiry="20260708",
        front_vs_next_atm_iv_gap=0.0,
        expiries=(
            IvSurfaceExpiry(
                expiry="20260707",
                atm_iv=0.28,
                atm_straddle_mid=30.0,
                expected_move_points=28.0,
                expected_move_pct=0.0037,
                put_skew_ratio=1.25,
                call_skew_ratio=1.0,
                smile_slope=-0.02,
                smile_curvature=0.01,
                iv_surface_level=0.29,
                iv_surface_shift_5m=0.04,
                atm_iv_jump_5m=0.04,
                put_skew_steepening_5m=0.10,
                call_wing_bid=False,
                smile_curvature_change_5m=0.01,
                surface_fit_quality="low_iv_coverage",
                wide_quote_surface_degraded=False,
                gamma_state="mixed_gamma",
                zero_gamma=7500.0,
                put_wall=7450.0,
                call_wall=7550.0,
                option_count=20,
                iv_coverage_ratio=0.9,
                gamma_coverage_ratio=0.9,
                avg_spread_bps=100.0,
                warnings=(),
            ),
        ),
        warnings=(),
    )

    alerts = iv_surface_alerts(surface, window=active_window(now))
    kinds = {alert.kind for alert in alerts}

    assert "iv_surface_degraded" in kinds
    assert "atm_iv_jump_5m" in kinds
    assert "put_skew_steepening_5m" in kinds
    assert "iv_surface_shift_5m" in kinds
    degraded_jump = next(alert for alert in alerts if alert.kind == "atm_iv_jump_5m")
    assert "[degraded IV coverage]" in degraded_jump.detail


def test_iv_surface_history_emits_1h_shift_alert() -> None:
    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)
    surface = make_surface(as_of=now)
    history = {
        "expiries": [
            {
                "expiry": "20260707",
                "iv_surface_level_change_1h": 0.09,
                "atm_iv_change_1h": 0.02,
                "surface_fit_quality": "low_iv_coverage",
            }
        ]
    }
    alerts = iv_surface_alerts(surface, window=active_window(now), history_1h=history)
    kinds = {alert.kind for alert in alerts}
    assert "iv_surface_shift_1h" in kinds
    assert "atm_iv_change_1h" not in kinds


def test_run_persists_system_events_when_notifications_disabled(tmp_path, monkeypatch) -> None:
    from spx_spark.alert_engine import run

    persist_calls: list[LatestState] = []
    monkeypatch.setattr(
        "spx_spark.alert_engine.persist_system_event_state",
        lambda state: persist_calls.append(state),
    )
    monkeypatch.setenv("ALERT_NOTIFY_ENABLED", "false")
    monkeypatch.setenv("ALERT_SYSTEM_EVENT_STATE_PATH", str(tmp_path / "system-event-state.json"))
    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)
    interrupted = ProviderState(
        provider=Provider.IBKR,
        status=ProviderStatus.UNAVAILABLE,
        checked_at=now,
        reason="competing session blocks live market data (IBKR 10197)",
        connected=False,
        authenticated=False,
        priority=0,
    )
    state = make_state(now=now, provider_states=(interrupted,))

    class FakeStore:
        def __init__(self, settings) -> None:
            pass

        def load(self, *, now=None, refresh_quality=True) -> LatestState:
            return state

    monkeypatch.setattr("spx_spark.alert_engine.LatestStateStore", FakeStore)

    run(["--no-notify"])

    assert len(persist_calls) == 1


def test_effective_move_threshold_bps_em_normalized_when_em_above_static() -> None:
    threshold, source = effective_move_threshold_bps("high", 0.015)
    assert threshold == pytest.approx(45.0)
    assert source == "em_normalized"


def test_effective_move_threshold_bps_static_when_expected_move_missing() -> None:
    threshold, source = effective_move_threshold_bps("high", None)
    assert threshold == 30.0
    assert source == "static"


def test_effective_move_threshold_bps_static_floor_when_em_too_low() -> None:
    threshold, source = effective_move_threshold_bps("high", 0.002)
    assert threshold == 30.0
    assert source == "static"


def test_effective_move_threshold_quiet_window_scales_down_to_em() -> None:
    # Low-vol regime: day EM 60 bps, quiet window. Static 85 bps would never
    # fire overnight; the bar should drop to 0.35 x EM instead.
    threshold, source = effective_move_threshold_bps("low", 0.006)
    assert threshold == pytest.approx(21.0)
    assert source == "em_normalized_quiet"


def test_effective_move_threshold_quiet_window_keeps_floor() -> None:
    threshold, source = effective_move_threshold_bps("low", 0.0005)
    assert threshold == 15.0
    assert source == "em_normalized_quiet"


def test_effective_move_threshold_quiet_window_high_vol_scales_up() -> None:
    # Day EM 300 bps: quiet bar rises to 0.35 x EM above the static 85.
    threshold, source = effective_move_threshold_bps("low", 0.03)
    assert threshold == pytest.approx(105.0)
    assert source == "em_normalized"
