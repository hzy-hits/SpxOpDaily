from datetime import datetime, timedelta, timezone

from spx_spark.application.shock.gth_dip import advance_gth_dip


NOW = datetime(2026, 7, 14, 3, 0, tzinfo=timezone.utc)


def advance(state, minute: int, es: float, *, allowed: bool = True):
    return advance_gth_dip(
        state,
        session_date="2026-07-14",
        at=NOW + timedelta(minutes=minute),
        es=es,
        provider="schwab",
        expected_move_points=80,
        short_horizon_seconds=900,
        long_horizon_seconds=3600,
        short_min_drawdown_points=8,
        long_min_drawdown_points=12,
        short_min_descent_seconds=0,
        long_min_descent_seconds=0,
        expected_move_fraction=0.10,
        reclaim_fraction=0.35,
        min_reclaim_points=4,
        confirm_samples=2,
        confirm_hold_seconds=0,
        session_warmup_seconds=0,
        max_signals_per_session=3,
        cooldown_seconds=900,
        entry_allowed=allowed,
    )


def test_slow_es_dip_reclaim_confirms_without_spx() -> None:
    state = None
    for minute, es in ((0, 7560), (5, 7554), (10, 7546), (12, 7551)):
        state, alert, signal = advance(state, minute, es)
    assert alert is None
    state, alert, signal = advance(state, 13, 7552)
    assert alert is not None
    assert alert.kind == "gth_dip_reclaim_call"
    assert alert.title == "SPX 0DTE | CALL RECLAIM (60m)"
    assert "Desk View" in alert.detail
    assert "Execution" in alert.detail
    assert "Risk" in alert.detail
    assert signal["direction"] == "up"
    assert signal["drawdown_points"] == 14


def test_macro_pre_event_suppresses_confirmation_but_keeps_observation() -> None:
    state = None
    for minute, es in ((0, 7560), (5, 7554), (10, 7546), (12, 7551), (13, 7552)):
        state, alert, signal = advance(state, minute, es, allowed=False)
    assert alert is None
    assert signal is None
    assert state["status"] == "suppressed_pre_event"
