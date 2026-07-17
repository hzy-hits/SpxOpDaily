from __future__ import annotations

import time

from spx_spark.ibkr.farm_health import (
    FarmHealthTracker,
    FarmLinkStatus,
    classify_farm_error,
    parse_farm_name,
)


def test_classify_farm_error_detects_broken_and_ok_messages():
    assert classify_farm_error(2157, "Sec-def data farm connection is broken:secdefhk") == (
        FarmLinkStatus.BROKEN,
        "secdefhk",
    )
    assert classify_farm_error(2104, "Market data farm connection is OK:hfarm") == (
        FarmLinkStatus.OK,
        "hfarm",
    )
    assert classify_farm_error(2119, "Market data farm is connecting:usfarm.nj") == (
        FarmLinkStatus.CONNECTING,
        "usfarm",
    )
    assert classify_farm_error(
        2110, "Connectivity between Trader Workstation and server is broken."
    ) == (
        FarmLinkStatus.BROKEN,
        "tws-server",
    )
    assert classify_farm_error(1100, "Connectivity between IBKR and TWS has been lost") == (
        FarmLinkStatus.BROKEN,
        "tws-server",
    )
    assert classify_farm_error(1102, "Connectivity restored; data maintained") == (
        FarmLinkStatus.OK,
        "tws-server",
    )


def test_parse_farm_name_handles_tws_connectivity_message():
    assert (
        parse_farm_name("Connectivity between Trader Workstation and server is broken.")
        == "tws-server"
    )


def test_farm_health_tracker_emits_only_on_status_transition():
    tracker = FarmHealthTracker(broken_restart_seconds=60.0)
    t0 = 1000.0

    first = tracker.observe(2157, "Sec-def data farm connection is broken:secdefhk", now=t0)
    assert first is not None
    assert first.status is FarmLinkStatus.BROKEN

    second = tracker.observe(2157, "Sec-def data farm connection is broken:secdefhk", now=t0 + 5)
    assert second is None

    recovered = tracker.observe(2158, "Sec-def data farm connection is OK:secdefhk", now=t0 + 10)
    assert recovered is not None
    assert recovered.status is FarmLinkStatus.OK
    assert tracker.broken_since is None


def test_farm_health_tracker_restarts_after_sustained_broken_state():
    tracker = FarmHealthTracker(broken_restart_seconds=30.0)
    t0 = time.monotonic()
    tracker.observe(2110, "Connectivity between Trader Workstation and server is broken.", now=t0)
    assert not tracker.should_restart_gateway(now=t0 + 10)
    assert tracker.should_restart_gateway(now=t0 + 31)


def test_unrelated_farm_ok_does_not_clear_futures_farm_outage() -> None:
    tracker = FarmHealthTracker(broken_restart_seconds=30.0)
    t0 = 3000.0

    tracker.observe(2103, "Market data farm connection is broken:hfarm", now=t0)
    event = tracker.observe(
        2104,
        "Market data farm connection is OK:usopt",
        now=t0 + 1.0,
    )

    assert event is None
    assert tracker.status is FarmLinkStatus.BROKEN
    assert tracker.farms["hfarm"] is FarmLinkStatus.BROKEN
    assert tracker.farms["usopt"] is FarmLinkStatus.OK
    assert tracker.broken_since == t0
    assert tracker.oldest_broken_farm() == "hfarm"
    assert tracker.should_restart_gateway(now=t0 + 31.0)

    recovered = tracker.observe(
        2104,
        "Market data farm connection is OK:hfarm",
        now=t0 + 32.0,
    )
    assert recovered is not None
    assert recovered.status is FarmLinkStatus.OK
    assert tracker.broken_since is None


def test_recovering_oldest_broken_farm_keeps_other_outage_active() -> None:
    tracker = FarmHealthTracker(broken_restart_seconds=30.0)
    t0 = 4000.0
    tracker.observe(2103, "Market data farm connection is broken:hfarm", now=t0)
    tracker.observe(2103, "Market data farm connection is broken:usopt", now=t0 + 5.0)

    tracker.observe(2104, "Market data farm connection is OK:hfarm", now=t0 + 10.0)

    assert tracker.status is FarmLinkStatus.BROKEN
    assert tracker.broken_since == t0 + 5.0
    assert tracker.oldest_broken_farm() == "usopt"
    assert not tracker.should_restart_gateway(now=t0 + 34.0)
    assert tracker.should_restart_gateway(now=t0 + 36.0)


def test_tws_1100_outage_starts_recovery_timer_until_1102() -> None:
    tracker = FarmHealthTracker(broken_restart_seconds=30.0)
    t0 = 2000.0

    lost = tracker.observe(1100, "Connectivity between IBKR and TWS has been lost", now=t0)

    assert lost is not None
    assert lost.status is FarmLinkStatus.BROKEN
    assert not tracker.should_restart_gateway(now=t0 + 29.0)
    assert tracker.should_restart_gateway(now=t0 + 31.0)

    restored = tracker.observe(1102, "Connectivity restored; data maintained", now=t0 + 32.0)

    assert restored is not None
    assert restored.status is FarmLinkStatus.OK
    assert tracker.broken_since is None


def test_mark_probe_failed_starts_broken_timer():
    from spx_spark.ibkr.farm_health import DataPlaneProbeResult

    tracker = FarmHealthTracker(broken_restart_seconds=10.0)
    t0 = 500.0
    probe = DataPlaneProbeResult(
        ok=False,
        current_time_ok=True,
        qualify_ok=False,
        error="qualifyContracts failed: timeout",
    )
    event = tracker.mark_probe_failed(probe, now=t0)
    assert event.status is FarmLinkStatus.BROKEN
    assert tracker.should_restart_gateway(now=t0 + 10)

    tracker.mark_probe_succeeded()
    assert tracker.status is FarmLinkStatus.OK
    assert tracker.broken_since is None


def test_farm_tracker_market_data_ready_transitions() -> None:
    tracker = FarmHealthTracker(broken_restart_seconds=60.0)
    assert tracker.market_data_ready() is True

    tracker.observe(2119, "Market data farm is connecting:usfarm.nj", now=1000.0)
    assert tracker.market_data_ready() is False

    tracker.observe(2104, "Market data farm connection is OK:usfarm", now=1001.0)
    assert tracker.market_data_ready() is True

    tracker.observe(1100, "Connectivity between IB and TWS has been lost", now=1002.0)
    assert tracker.market_data_ready() is False


def test_data_flow_silence_marks_farm_broken_and_live_clears() -> None:
    tracker = FarmHealthTracker()
    now = time.monotonic()

    event = tracker.mark_data_flow_silent("no ES ticks for 150s", now=now)
    assert event is not None
    assert tracker.status is FarmLinkStatus.BROKEN
    assert tracker.market_data_ready() is False

    # Repeated detections do not spam events, and duration accumulates.
    assert tracker.mark_data_flow_silent("no ES ticks for 160s", now=now + 10) is None
    assert tracker.broken_duration(now=now + 181) == 181
    assert tracker.should_restart_gateway(now=now + 181) is True

    tracker.mark_data_flow_live()
    assert tracker.status is FarmLinkStatus.OK
    assert tracker.market_data_ready() is True
    assert tracker.should_restart_gateway(now=now + 400) is False


def test_data_flow_silence_breach_requires_open_session_window() -> None:
    from datetime import datetime, timezone

    from spx_spark.ibkr.farm_health import data_flow_silence_breached

    # 2026-07-17 06:00 UTC = 02:00 ET Friday: Globex open.
    now = datetime(2026, 7, 17, 6, 0, tzinfo=timezone.utc)
    frozen = datetime(2026, 7, 17, 5, 56, tzinfo=timezone.utc)
    assert (
        data_flow_silence_breached(ticker_time=frozen, now=now, silence_seconds=120.0) is True
    )
    # Fresh ticks never breach.
    fresh = datetime(2026, 7, 17, 5, 59, 30, tzinfo=timezone.utc)
    assert (
        data_flow_silence_breached(ticker_time=fresh, now=now, silence_seconds=120.0) is False
    )
    # Weekend silence is expected, not a breach.
    weekend_now = datetime(2026, 7, 18, 12, 0, tzinfo=timezone.utc)
    weekend_frozen = datetime(2026, 7, 18, 11, 0, tzinfo=timezone.utc)
    assert (
        data_flow_silence_breached(
            ticker_time=weekend_frozen, now=weekend_now, silence_seconds=120.0
        )
        is False
    )
    # Right after the Sunday Globex open the window was closed moments ago.
    reopen = datetime(2026, 7, 19, 22, 1, tzinfo=timezone.utc)
    friday_close_tick = datetime(2026, 7, 17, 20, 59, tzinfo=timezone.utc)
    assert (
        data_flow_silence_breached(
            ticker_time=friday_close_tick, now=reopen, silence_seconds=120.0
        )
        is False
    )
    # No tick timestamp yet (warmup) never breaches.
    assert data_flow_silence_breached(ticker_time=None, now=now, silence_seconds=120.0) is False
