from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from spx_spark.alert_engine import (
    effective_move_threshold_bps,
    evaluate_alerts,
    evaluate_payload,
    front_expected_move_pct,
    ibkr_session_status,
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


def test_account_standby_connected_counts_as_broker_session_available() -> None:
    now = datetime(2026, 7, 13, 22, 0, tzinfo=BJ_TZ)
    state = ProviderState(
        provider=Provider.IBKR,
        status=ProviderStatus.DEGRADED,
        checked_at=now,
        reason="account standby connected; market data inactive",
        connected=True,
    )

    assert ibkr_session_status(state, now=now) == "available"


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


def test_hyperliquid_proxy_watch_stays_research_only_when_ibkr_is_unavailable() -> None:
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
    assert fallback_alerts[0].instrument_id == "crypto_perp:xyz:SP500"
    assert fallback_alerts[0].quality == "degraded"
    assert fallback_alerts[0].research_only is True


def test_wall_dedup_band_groups_nearby_walls() -> None:
    from spx_spark.alert_engine import wall_dedup_band

    # 7425-7449 share a band; 7450 starts the next one. A wall drifting a
    # strike or two no longer opens a fresh cooldown slot.
    assert wall_dedup_band(7425.0) == wall_dedup_band(7430.0) == wall_dedup_band(7435.0)
    assert wall_dedup_band(7449.0) == "band:7425"
    assert wall_dedup_band(7450.0) == "band:7450"
    assert wall_dedup_band(7450.0) != wall_dedup_band(7425.0)


def test_gamma_regime_hysteresis_requires_state_to_hold(tmp_path, monkeypatch) -> None:
    from spx_spark.alert_engine import (
        gamma_regime_observation_stable,
        persist_gamma_regime_observations,
    )

    monkeypatch.setenv(
        "ALERT_GAMMA_REGIME_STATE_PATH", str(tmp_path / "gamma-regime-state.json")
    )
    now = datetime(2026, 7, 8, 22, 17, tzinfo=BJ_TZ)

    spx = make_quote(InstrumentId.index("SPX"), mark=7500.0, close=7490.0, now=now)
    call = make_option(expiry="20260708", strike=7500, right="C", mark=10.0, now=now)
    put = make_option(expiry="20260708", strike=7500, right="P", mark=11.0, now=now)
    options_map = build_options_map(make_state(spx, call, put, now=now))
    expiry_key = options_map.expiries[0].expiry
    observed_state = options_map.expiries[0].gamma_state

    # First observation starts the clock: not stable yet.
    persist_gamma_regime_observations(options_map, as_of=now)
    assert gamma_regime_observation_stable(expiry_key, observed_state, as_of=now) is False
    assert (
        gamma_regime_observation_stable(
            expiry_key, observed_state, as_of=now + timedelta(minutes=5)
        )
        is False
    )
    # Same state still observed 10+ minutes later: stable.
    assert (
        gamma_regime_observation_stable(
            expiry_key, observed_state, as_of=now + timedelta(minutes=10)
        )
        is True
    )
    # A different state resets the clock (flip-flop never clears hysteresis).
    assert (
        gamma_regime_observation_stable(
            expiry_key, "some_other_state", as_of=now + timedelta(minutes=10)
        )
        is False
    )


def test_hyperliquid_proxy_watch_is_research_only_when_anchor_is_closed() -> None:
    """During the ES maintenance break (or any anchor-dead stretch) the SP500
    perp is the only live monitor; its moves must alert even when the broker
    session itself is fine."""
    now = datetime(2026, 7, 7, 5, 30, tzinfo=BJ_TZ)  # 17:30 ET: ES maintenance break
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
    healthy = ProviderState(
        provider=Provider.IBKR,
        status=ProviderStatus.AVAILABLE,
        checked_at=now,
        reason=None,
        connected=True,
        authenticated=True,
        priority=0,
    )
    state = make_state(hl, now=now, provider_states=(healthy,))
    context = build_market_context(state)

    alerts = evaluate_alerts(state, window=active_window(now), market_context=context)
    fallback_alerts = [alert for alert in alerts if alert.kind == "broker_unavailable_proxy_watch"]

    assert fallback_alerts
    assert fallback_alerts[0].instrument_id == "crypto_perp:xyz:SP500"
    assert fallback_alerts[0].research_only is True
    assert "No live SPX/ES anchor" in fallback_alerts[0].detail


def test_ibkr_session_transition_alerts_are_edge_triggered(tmp_path, monkeypatch) -> None:
    state_path = tmp_path / "system-event-state.json"
    monkeypatch.setenv("ALERT_SYSTEM_EVENT_STATE_PATH", str(state_path))
    monkeypatch.setenv("IBKR_EXECUTION_MODE", "live")
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
    monkeypatch.setenv("IBKR_EXECUTION_MODE", "live")
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
    monkeypatch.setenv("IBKR_EXECUTION_MODE", "live")
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
    monkeypatch.setenv("IBKR_EXECUTION_MODE", "live")
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


def _write_failover_transition(
    path,
    *,
    now: datetime,
    sequence: int,
    previous_mode: str,
    mode: str,
) -> None:
    path.write_text(
        json.dumps(
            {
                "mode": mode,
                "updated_at": now.isoformat(),
                "sequence": sequence,
                "schwab_unhealthy_streak": 0,
                "schwab_recovery_streak": 0,
                "ibkr_unhealthy_streak": 0,
                "monitoring_active": True,
                "ibkr_market_data_required": mode != "schwab_primary",
                "transition": {
                    "transition_id": f"provider-failover:{sequence}:{mode}",
                    "sequence": sequence,
                    "previous_mode": previous_mode,
                    "mode": mode,
                    "occurred_at": now.isoformat(),
                    "reason": "test transition",
                },
            }
        ),
        encoding="utf-8",
    )


def test_provider_failover_transitions_are_edge_triggered(tmp_path, monkeypatch) -> None:
    event_path = tmp_path / "system-event-state.json"
    failover_path = tmp_path / "provider-failover.json"
    monkeypatch.setenv("ALERT_SYSTEM_EVENT_STATE_PATH", str(event_path))
    monkeypatch.setenv("PROVIDER_FAILOVER_STATE_PATH", str(failover_path))
    now = datetime(2026, 7, 13, 22, 0, tzinfo=BJ_TZ)
    _write_failover_transition(
        failover_path,
        now=now,
        sequence=2,
        previous_mode="recovery_pending",
        mode="ibkr_fallback",
    )

    first = system_event_alerts(make_state(now=now))
    repeated = system_event_alerts(make_state(now=now))

    assert [alert.kind for alert in first] == ["market_data_ibkr_fallback_activated"]
    assert first[0].source_gate == "provider_failover_state"
    assert repeated == []

    restored_at = now + timedelta(minutes=5)
    _write_failover_transition(
        failover_path,
        now=restored_at,
        sequence=3,
        previous_mode="ibkr_fallback",
        mode="schwab_primary",
    )
    restored = system_event_alerts(make_state(now=restored_at))

    assert [alert.kind for alert in restored] == ["market_data_schwab_restored"]


def test_both_direct_providers_unavailable_is_critical(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("ALERT_SYSTEM_EVENT_STATE_PATH", str(tmp_path / "events.json"))
    failover_path = tmp_path / "provider-failover.json"
    monkeypatch.setenv("PROVIDER_FAILOVER_STATE_PATH", str(failover_path))
    now = datetime(2026, 7, 13, 22, 0, tzinfo=BJ_TZ)
    _write_failover_transition(
        failover_path,
        now=now,
        sequence=2,
        previous_mode="recovery_pending",
        mode="both_unavailable",
    )

    alerts = system_event_alerts(make_state(now=now))

    assert [alert.kind for alert in alerts] == ["market_data_all_providers_unavailable"]
    assert alerts[0].severity == "critical"
    assert "禁止新开仓" in alerts[0].detail


def test_schwab_recovery_before_takeover_does_not_claim_ibkr_was_active(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("ALERT_SYSTEM_EVENT_STATE_PATH", str(tmp_path / "events.json"))
    failover_path = tmp_path / "provider-failover.json"
    monkeypatch.setenv("PROVIDER_FAILOVER_STATE_PATH", str(failover_path))
    now = datetime(2026, 7, 13, 22, 0, tzinfo=BJ_TZ)
    _write_failover_transition(
        failover_path,
        now=now,
        sequence=2,
        previous_mode="recovery_pending",
        mode="schwab_primary",
    )

    alerts = system_event_alerts(make_state(now=now))

    assert [alert.kind for alert in alerts] == ["market_data_schwab_restored"]
    assert "备用接管已取消" in alerts[0].title
    assert "退出 IBKR" not in alerts[0].detail


def test_ibkr_standby_disconnect_is_silent_without_position_or_live_execution(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("ALERT_SYSTEM_EVENT_STATE_PATH", str(tmp_path / "events.json"))
    monkeypatch.setenv("IBKR_EXECUTION_MODE", "manual")
    monkeypatch.setenv("IBKR_BROKER_ACCOUNT_READ_ENABLED", "false")
    monkeypatch.setenv("IBKR_POSITIONS_SNAPSHOT_PATH", str(tmp_path / "missing-positions.json"))
    now = datetime(2026, 7, 13, 22, 0, tzinfo=BJ_TZ)
    interrupted = ProviderState(
        provider=Provider.IBKR,
        status=ProviderStatus.UNAVAILABLE,
        checked_at=now,
        reason="standby disconnected",
        connected=False,
    )

    assert system_event_alerts(make_state(now=now, provider_states=(interrupted,))) == []


def test_ibkr_standby_reconnect_pages_ops_login_without_position(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("ALERT_SYSTEM_EVENT_STATE_PATH", str(tmp_path / "events.json"))
    monkeypatch.setenv("IBKR_EXECUTION_MODE", "manual")
    monkeypatch.setenv("IBKR_BROKER_ACCOUNT_READ_ENABLED", "false")
    monkeypatch.setenv("IBKR_POSITIONS_SNAPSHOT_PATH", str(tmp_path / "missing-positions.json"))
    now = datetime(2026, 7, 13, 22, 0, tzinfo=BJ_TZ)
    interrupted = ProviderState(
        provider=Provider.IBKR,
        status=ProviderStatus.UNAVAILABLE,
        checked_at=now,
        reason="IBKR account standby disconnected",
        connected=False,
    )
    standby = ProviderState(
        provider=Provider.IBKR,
        status=ProviderStatus.DEGRADED,
        checked_at=now + timedelta(minutes=1),
        reason="account standby connected; market data inactive",
        connected=True,
        authenticated=True,
    )

    assert system_event_alerts(make_state(now=now, provider_states=(interrupted,))) == []
    login_alerts = system_event_alerts(
        make_state(now=now + timedelta(minutes=1), provider_states=(standby,))
    )
    assert [alert.kind for alert in login_alerts] == ["ibkr_session_login"]
    assert login_alerts[0].severity == "high"
    assert "account standby" in login_alerts[0].detail
    assert system_event_alerts(
        make_state(now=now + timedelta(minutes=2), provider_states=(standby,))
    ) == []


def test_old_or_inactive_failover_transition_never_pages_after_restart(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("ALERT_SYSTEM_EVENT_STATE_PATH", str(tmp_path / "events.json"))
    failover_path = tmp_path / "provider-failover.json"
    monkeypatch.setenv("PROVIDER_FAILOVER_STATE_PATH", str(failover_path))
    now = datetime(2026, 7, 13, 22, 0, tzinfo=BJ_TZ)
    old = now - timedelta(minutes=10)
    _write_failover_transition(
        failover_path,
        now=old,
        sequence=2,
        previous_mode="recovery_pending",
        mode="ibkr_fallback",
    )
    raw = json.loads(failover_path.read_text(encoding="utf-8"))
    raw["updated_at"] = now.isoformat()
    failover_path.write_text(json.dumps(raw), encoding="utf-8")

    assert system_event_alerts(make_state(now=now)) == []

    raw["transition"]["occurred_at"] = now.isoformat()
    raw["monitoring_active"] = False
    failover_path.write_text(json.dumps(raw), encoding="utf-8")

    assert system_event_alerts(make_state(now=now)) == []


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


def test_movement_snapshot_uses_same_dynamic_threshold_as_evaluation(
    tmp_path, monkeypatch
) -> None:
    import spx_spark.alert_engine as ae

    state_path = tmp_path / "movement-state.json"
    monkeypatch.setenv("ALERT_MOVEMENT_STATE_PATH", str(state_path))
    monkeypatch.setattr(ae, "front_expected_move_pct", lambda *_args, **_kwargs: 0.015)
    now = datetime(2026, 7, 7, 23, 0, tzinfo=BJ_TZ)
    window = active_window(now)
    assert window.priority == "high"
    # Dynamic high-window threshold is 45 bps; the old static snapshot used
    # 30 bps and prematurely occupied bucket 1 before evaluation could alert.
    state = make_state(
        make_quote(InstrumentId.index("SPX"), mark=7526.25, close=7500.0, now=now),
        now=now,
    )

    ae.persist_movement_state_snapshot(state, window=window, options_map=object())

    stored = json.loads(state_path.read_text(encoding="utf-8"))
    assert stored["instruments"] == {}


def test_delayed_quote_cannot_trigger_movement_alert(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("ALERT_MOVEMENT_STATE_PATH", str(tmp_path / "movement.json"))
    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)
    delayed = replace(
        make_quote(
            InstrumentId.equity("SPY"),
            mark=760.0,
            close=750.0,
            quality=MarketDataQuality.DELAYED,
            now=now,
        ),
        market_data_type=3,
        last_update_at=now,
    )
    state = make_state(delayed, now=now)

    alerts = movement_alerts(
        state,
        window=active_window(now),
        market_context=build_market_context(state),
        persist=False,
    )

    assert not any(alert.kind == "price_move_from_close" for alert in alerts)


def test_overnight_dip_escalates_to_high_severity(tmp_path, monkeypatch) -> None:
    import spx_spark.alert_engine as ae

    monkeypatch.setenv("ALERT_MOVEMENT_STATE_PATH", str(tmp_path / "movement-state.json"))
    # Day EM = 41 bps (low-vol regime); quiet Asia-session window.
    monkeypatch.setattr(ae, "front_expected_move_pct", lambda *_args, **_kwargs: 0.0041)
    # Beijing 07:00 = ET 19:00: before the reader's day starts, still the
    # quiet_futures_context window (now high priority for off-hours parity).
    now = datetime(2026, 7, 7, 7, 0, tzinfo=BJ_TZ)
    window = active_window(now)
    assert window.priority == "high"

    # -40 bps clears the high-window 30 bps bar and consumes ~98% of the day EM.
    state = make_state(
        make_quote(InstrumentId.equity("SPY"), mark=747.0, close=750.0, now=now),
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
    # Window priority is already high, so the alert clears the notify gate
    # without needing the old low→high EM escalation path.
    assert moves[0].severity == "high"
    assert moves[0].dedup_group == "down:1"
    assert moves[0].threshold == 30.0


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


def test_run_reconciles_exact_position_event_acknowledgements(monkeypatch) -> None:
    from spx_spark.alert_engine import run
    from spx_spark.notifier import NotificationResult

    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)
    state = make_state(now=now)
    reconciled: list[tuple[str, ...]] = []

    class FakeStore:
        def __init__(self, settings) -> None:
            pass

        def load(self, *, now=None, refresh_quality=True) -> LatestState:
            return state

    monkeypatch.setattr("spx_spark.alert_engine.LatestStateStore", FakeStore)
    monkeypatch.setattr(
        "spx_spark.alert_engine.evaluate_payload",
        lambda *args, **kwargs: {
            "alerts": [],
            "window": {"name": "test", "priority": "high"},
            "as_of": now.isoformat(),
            "alert_count": 0,
        },
    )
    monkeypatch.setattr(
        "spx_spark.alert_engine.notify_payload",
        lambda *args, **kwargs: NotificationResult(
            enabled=True,
            selected_count=1,
            sent_count=1,
            skipped_reason=None,
            sinks=(),
            acknowledged_event_ids=("event-1", "event-2"),
        ),
    )
    monkeypatch.setattr(
        "spx_spark.alert_engine.reconcile_position_event_acknowledgements",
        lambda event_ids: reconciled.append(event_ids) or True,
    )
    monkeypatch.setattr("spx_spark.alert_engine.persist_system_event_state", lambda state: None)
    monkeypatch.setattr("spx_spark.alert_engine.persist_movement_state_snapshot", lambda state: None)

    run(["--notify", "--json"])

    assert reconciled == [("event-1", "event-2")]


def test_effective_move_threshold_bps_em_normalized_when_em_above_static() -> None:
    threshold, source = effective_move_threshold_bps("high", 0.015)
    assert threshold == pytest.approx(45.0)
    assert source == "em_normalized"


def test_effective_move_threshold_bps_static_when_expected_move_missing() -> None:
    threshold, source = effective_move_threshold_bps("high", None)
    assert threshold == 30.0
    assert source == "static"


def test_front_expected_move_expires_at_research_rollover() -> None:
    options_map = SimpleNamespace(
        expiries=[SimpleNamespace(expiry="20260709", expected_move_pct=0.01)]
    )

    assert (
        front_expected_move_pct(
            options_map,
            as_of=datetime.fromisoformat("2026-07-09T20:59:00+00:00"),
        )
        == 0.01
    )
    assert (
        front_expected_move_pct(
            options_map,
            as_of=datetime.fromisoformat("2026-07-09T21:00:00+00:00"),
        )
        is None
    )


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


def test_preemption_session_alert_sequence(tmp_path, monkeypatch) -> None:
    state_path = tmp_path / "system-event-state.json"
    monkeypatch.setenv("ALERT_SYSTEM_EVENT_STATE_PATH", str(state_path))
    monkeypatch.setenv("IBKR_EXECUTION_MODE", "live")
    now = datetime(2026, 7, 11, 11, 0, tzinfo=BJ_TZ)

    unavailable = ProviderState(
        provider=Provider.IBKR,
        status=ProviderStatus.UNAVAILABLE,
        checked_at=now,
        reason="IBKR disconnected mid-session",
        connected=False,
        authenticated=None,
        priority=0,
    )
    degraded = ProviderState(
        provider=Provider.IBKR,
        status=ProviderStatus.DEGRADED,
        checked_at=now + timedelta(minutes=1),
        reason="connected; awaiting first flush",
        connected=True,
        authenticated=True,
        priority=0,
    )
    available = ProviderState(
        provider=Provider.IBKR,
        status=ProviderStatus.AVAILABLE,
        checked_at=now + timedelta(minutes=2),
        reason=None,
        connected=True,
        authenticated=True,
        priority=0,
    )

    interrupted = system_event_alerts(make_state(now=now, provider_states=(unavailable,)))
    transitional = system_event_alerts(
        make_state(now=now + timedelta(minutes=1), provider_states=(degraded,))
    )
    restored = system_event_alerts(
        make_state(now=now + timedelta(minutes=2), provider_states=(available,))
    )
    repeated = system_event_alerts(
        make_state(now=now + timedelta(minutes=3), provider_states=(available,))
    )

    assert [alert.kind for alert in interrupted] == ["ibkr_session_interrupted"]
    assert transitional == []
    assert [alert.kind for alert in restored] == ["ibkr_session_restored"]
    assert repeated == []
