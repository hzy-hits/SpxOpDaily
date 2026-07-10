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
        None,
    )
    assert classify_farm_error(2110, "Connectivity between Trader Workstation and server is broken.") == (
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
    assert tracker.should_restart_gateway(now=t0 + 11)
