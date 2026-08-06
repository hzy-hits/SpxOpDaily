from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from spx_spark.application.market_features.provider_entry_control import (
    gth_ibkr_entry_control,
)
from spx_spark.state_io import atomic_write_json_secure


NOW = datetime(2026, 8, 6, 10, 52, tzinfo=timezone.utc)


def _write_health(tmp_path, **overrides) -> None:
    payload = {
        "schema_version": 1,
        "service": "ibkr_stream",
        "observed_at": NOW.isoformat(),
        "expires_at": (NOW + timedelta(seconds=30)).isoformat(),
        "max_age_seconds": 30.0,
        "process_active": True,
        "data_plane_healthy": True,
        "policy_blocked": False,
        "connected": True,
        "circuit_state": "closed",
        "conflict_count": 0,
        "retry_at": None,
        "retry_in_seconds": None,
        "connection_generation": 7,
        "reason": "fresh live SPXW flush",
    }
    payload.update(overrides)
    atomic_write_json_secure(
        tmp_path / "latest" / "ibkr_stream_health.json",
        payload,
    )


def test_gth_ibkr_entry_control_accepts_only_fresh_healthy_data_plane(tmp_path) -> None:
    _write_health(tmp_path)

    observed = gth_ibkr_entry_control(tmp_path, now=NOW + timedelta(seconds=1))

    assert observed["allowed"] is True
    assert observed["reason"] == "allowed"
    assert observed["data_plane_healthy"] is True


@pytest.mark.parametrize(
    ("overrides", "reason"),
    (
        (
            {
                "data_plane_healthy": False,
                "policy_blocked": True,
                "circuit_state": "open",
                "conflict_count": 1,
                "reason": "competing session blocks live market data (IBKR 10197)",
            },
            "ibkr_competing_session",
        ),
        (
            {
                "circuit_state": "half_open",
                "conflict_count": 1,
                "reason": "healthy probe pending",
            },
            "ibkr_competing_session",
        ),
        (
            {
                "circuit_state": "recovering",
                "conflict_count": 1,
                "reason": "healthy SPXW recovery flush",
            },
            "ibkr_competing_session",
        ),
        (
            {
                "circuit_state": "closed",
                "conflict_count": 1,
                "reason": "conflict history not cleared",
            },
            "ibkr_competing_session",
        ),
        ({"connected": False}, "ibkr_stream_disconnected"),
        ({"data_plane_healthy": False}, "ibkr_data_plane_unhealthy"),
    ),
)
def test_gth_ibkr_entry_control_fails_closed_on_runtime_incident(
    tmp_path,
    overrides,
    reason,
) -> None:
    _write_health(tmp_path, **overrides)

    observed = gth_ibkr_entry_control(tmp_path, now=NOW + timedelta(seconds=1))

    assert observed["allowed"] is False
    assert observed["reason"] == reason


def test_gth_ibkr_entry_control_fails_closed_on_missing_or_stale_health(tmp_path) -> None:
    missing = gth_ibkr_entry_control(tmp_path, now=NOW)
    assert missing["allowed"] is False
    assert missing["reason"] == "ibkr_stream_health_missing"

    _write_health(
        tmp_path,
        observed_at=(NOW - timedelta(seconds=31)).isoformat(),
    )
    stale = gth_ibkr_entry_control(tmp_path, now=NOW)
    assert stale["allowed"] is False
    assert stale["reason"] == "ibkr_stream_health_stale"
