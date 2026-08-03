from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from spx_spark.ibkr.stream.health import (
    persist_stream_health,
    stream_health_is_fresh,
    stream_health_path,
)
from spx_spark.ibkr.stream.models import CompetingSessionCircuit


def test_competing_session_circuit_exponentially_backs_off_and_closes() -> None:
    circuit = CompetingSessionCircuit(
        min_seconds=5.0,
        max_seconds=20.0,
        recovery_seconds=30.0,
    )

    assert circuit.open(now_monotonic=100.0) == 5.0
    assert circuit.state(now_monotonic=104.0) == "open"
    assert circuit.remaining_seconds(now_monotonic=104.0) == 1.0
    assert circuit.state(now_monotonic=105.0) == "half_open"

    assert circuit.open(now_monotonic=105.0) == 10.0
    assert circuit.open(now_monotonic=115.0) == 20.0
    assert circuit.open(now_monotonic=135.0) == 20.0
    assert circuit.failures == 4

    circuit.close()

    assert circuit.failures == 0
    assert circuit.state(now_monotonic=155.0) == "closed"
    assert circuit.remaining_seconds(now_monotonic=155.0) == 0.0
    assert circuit.open(now_monotonic=155.0) == 5.0


def test_competing_session_circuit_stays_bounded_after_many_failures() -> None:
    circuit = CompetingSessionCircuit(
        min_seconds=5.0,
        max_seconds=300.0,
        recovery_seconds=300.0,
        failures=10_000,
    )

    assert circuit.open(now_monotonic=100.0) == 300.0
    assert circuit.failures == 10_001


def test_stream_health_projection_separates_process_and_data_plane(tmp_path) -> None:
    storage = SimpleNamespace(data_root=str(tmp_path))
    observed_at = datetime(2026, 7, 25, 6, 0, tzinfo=timezone.utc)

    persist_stream_health(
        storage,  # type: ignore[arg-type]
        data_plane_healthy=False,
        policy_blocked=True,
        reason="competing session blocks live market data (IBKR 10197)",
        connected=False,
        circuit_state="open",
        conflict_count=3,
        retry_in_seconds=20.0,
        connection_generation=1102,
        observed_at=observed_at,
    )

    path = stream_health_path(storage)  # type: ignore[arg-type]
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload == {
        "schema_version": 1,
        "service": "ibkr_stream",
        "observed_at": "2026-07-25T06:00:00+00:00",
        "expires_at": "2026-07-25T06:01:30+00:00",
        "max_age_seconds": 90.0,
        "process_active": True,
        "data_plane_healthy": False,
        "policy_blocked": True,
        "connected": False,
        "circuit_state": "open",
        "conflict_count": 3,
        "retry_at": "2026-07-25T06:00:20+00:00",
        "retry_in_seconds": 20.0,
        "connection_generation": 1102,
        "reason": "competing session blocks live market data (IBKR 10197)",
    }
    assert path.stat().st_mode & 0o777 == 0o600


def test_stream_health_consumer_rejects_expired_or_future_heartbeat() -> None:
    observed_at = datetime(2026, 7, 25, 6, 0, tzinfo=timezone.utc)
    payload: dict[str, object] = {
        "observed_at": observed_at.isoformat(),
        "max_age_seconds": 90.0,
    }

    assert stream_health_is_fresh(
        payload,
        now=datetime(2026, 7, 25, 6, 1, 30, tzinfo=timezone.utc),
    )
    assert not stream_health_is_fresh(
        payload,
        now=datetime(2026, 7, 25, 6, 1, 30, 1, tzinfo=timezone.utc),
    )
    assert not stream_health_is_fresh(
        payload,
        now=datetime(2026, 7, 25, 5, 59, 59, tzinfo=timezone.utc),
    )
    assert not stream_health_is_fresh(
        {"observed_at": "invalid", "max_age_seconds": 90.0},
        now=observed_at,
    )


@pytest.mark.parametrize(
    ("minimum", "maximum"),
    [(0.0, 10.0), (10.0, 5.0)],
)
def test_competing_session_circuit_rejects_invalid_bounds(
    minimum: float,
    maximum: float,
) -> None:
    with pytest.raises(ValueError):
        CompetingSessionCircuit(min_seconds=minimum, max_seconds=maximum)


def test_competing_session_circuit_preserves_backoff_during_brief_recovery() -> None:
    circuit = CompetingSessionCircuit(
        min_seconds=5.0,
        max_seconds=20.0,
        recovery_seconds=30.0,
    )

    assert circuit.open(now_monotonic=0.0) == 5.0
    assert circuit.observe_healthy(now_monotonic=6.0) is False
    assert circuit.state(now_monotonic=6.0) == "recovering"

    assert circuit.open(now_monotonic=20.0) == 10.0
    assert circuit.failures == 2
    assert circuit.observe_healthy(now_monotonic=31.0) is False
    assert circuit.observe_healthy(now_monotonic=60.9) is False
    assert circuit.observe_healthy(now_monotonic=61.0) is True
    assert circuit.state(now_monotonic=61.0) == "closed"


def test_competing_session_circuit_requires_continuous_recovery() -> None:
    circuit = CompetingSessionCircuit(
        min_seconds=5.0,
        max_seconds=20.0,
        recovery_seconds=30.0,
    )
    circuit.open(now_monotonic=0.0)
    circuit.observe_healthy(now_monotonic=6.0)

    circuit.interrupt_recovery()

    assert circuit.observe_healthy(now_monotonic=40.0) is False
    assert circuit.observe_healthy(now_monotonic=69.9) is False
    assert circuit.observe_healthy(now_monotonic=70.0) is True
