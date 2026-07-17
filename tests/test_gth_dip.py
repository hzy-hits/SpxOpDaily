from datetime import datetime, timedelta, timezone

from spx_spark.application.shock.gth_dip import advance_gth_dip, mark_gth_delivery


NOW = datetime(2026, 7, 14, 3, 0, tzinfo=timezone.utc)


def advance(
    state,
    minute: int,
    es: float,
    *,
    allowed: bool = True,
    seconds: int = 0,
    retry_seconds: int = 30,
    expiry_seconds: int = 600,
):
    return advance_gth_dip(
        state,
        session_date="2026-07-14",
        at=NOW + timedelta(minutes=minute, seconds=seconds),
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
        delivery_retry_seconds=retry_seconds,
        signal_expiry_seconds=expiry_seconds,
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


def confirmed_signal_state():
    state = None
    for minute, es in ((0, 7560), (5, 7554), (10, 7546), (12, 7551), (13, 7552)):
        state, alert, signal = advance(state, minute, es)
    assert alert is not None
    return state, alert


def test_undelivered_signal_redelivers_after_retry_interval() -> None:
    state, alert = confirmed_signal_state()

    state, early, early_signal = advance(state, 13, 7552, seconds=29)
    assert early is None
    assert early_signal is None

    state, retry, retry_signal = advance(state, 13, 7552, seconds=31)
    assert retry is not None
    assert retry.event_id == alert.event_id
    assert retry.dedup_group == alert.dedup_group
    assert retry.title == alert.title
    assert retry.detail == alert.detail
    assert retry.source_at == alert.source_at
    assert retry_signal["delivery_retry"] is True
    assert state["status"] == "delivery_retry"
    assert state["last_signal"]["last_delivery_attempt_at"] == (
        NOW + timedelta(minutes=13, seconds=31)
    ).isoformat()


def test_delivery_ack_stops_redelivery() -> None:
    state, alert = confirmed_signal_state()
    state = mark_gth_delivery(
        state,
        event_id=str(alert.event_id),
        at=NOW + timedelta(minutes=13),
    )
    state, retry, retry_signal = advance(state, 13, 7552, seconds=45)
    assert retry is None
    assert retry_signal is None


def test_redelivery_stops_after_signal_expiry() -> None:
    state = None
    for minute, es in ((0, 7560), (5, 7554), (10, 7546), (12, 7551), (13, 7552)):
        state, alert, signal = advance(state, minute, es, expiry_seconds=60)
    assert alert is not None

    state, retry, _ = advance(state, 13, 7552, seconds=45, expiry_seconds=60)
    assert retry is not None

    # 75s after confirmation the signal is too old to retry, even when due.
    state, late, late_signal = advance(state, 14, 7553, seconds=15, expiry_seconds=60)
    assert late is None
    assert late_signal is None


def test_confirm_count_requires_fresh_samples() -> None:
    state = None
    for minute, es in ((0, 7560), (5, 7554), (10, 7546), (12, 7551)):
        state, alert, signal = advance(state, minute, es)
    assert state["pending"]["confirm_count"] == 1

    # A repeated poll with the same timestamp enqueues no new sample.
    state, alert, signal = advance(state, 12, 7551)
    assert alert is None
    assert state["pending"]["confirm_count"] == 1

    state, alert, signal = advance(state, 13, 7552)
    assert alert is not None
