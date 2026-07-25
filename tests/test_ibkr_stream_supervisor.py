from __future__ import annotations

from types import SimpleNamespace

import pytest

import spx_spark.ibkr.stream.supervisor as supervisor_module
from spx_spark.ibkr.stream.supervisor import StreamRuntime
from spx_spark.ibkr.verifier import IbkrError


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now


class FakeIb:
    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.sleep_calls: list[float] = []
        self.connected = True

    def sleep(self, seconds: float) -> None:
        self.sleep_calls.append(seconds)
        self.clock.now += seconds

    def isConnected(self) -> bool:  # noqa: N802 - mirrors ib_async
        return self.connected


class FakeCollector:
    def __init__(self, clock: FakeClock, *, disconnect_after_flushes: int = 1) -> None:
        self.clock = clock
        self.ib = FakeIb(clock)
        self.subscription_health_failed = False
        self.flush_times: list[float] = []
        self.demand_times: list[float] = []
        self.disconnect_after_flushes = disconnect_after_flushes

    def reconcile_exact_leg_demand(self) -> dict[str, object]:
        self.demand_times.append(self.clock.now)
        return {"task": "ibkr_stream", "event": "exact_leg_demand_polled"}

    def flush(self) -> dict[str, object]:
        self.flush_times.append(self.clock.now)
        if len(self.flush_times) >= self.disconnect_after_flushes:
            self.ib.connected = False
        return {"task": "ibkr_stream", "event": "flush"}

    def flush_position_shadow_if_due(self, *, now_monotonic: float) -> None:
        del now_monotonic

    def drain_new_errors(self) -> list[IbkrError]:
        return []

    def market_data_allowed(self) -> bool:
        return True


def stream_settings(**overrides: object) -> SimpleNamespace:
    values: dict[str, object] = {
        "reconnect_min_seconds": 1.0,
        "reconnect_max_seconds": 2.0,
        "flush_interval_seconds": 1.0,
        "exact_leg_pin_enabled": True,
        "quote_demand_poll_seconds": 0.05,
        "auto_restart_gateway_on_farm_broken": False,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def make_runtime(
    monkeypatch: pytest.MonkeyPatch,
    collector: FakeCollector,
    **setting_overrides: object,
) -> tuple[StreamRuntime, list[dict[str, object]]]:
    events: list[dict[str, object]] = []
    monkeypatch.setattr(
        supervisor_module,
        "time",
        SimpleNamespace(monotonic=collector.clock.monotonic),
    )
    monkeypatch.setattr(supervisor_module, "log_event", events.append)
    monkeypatch.setattr(supervisor_module, "persist_state_only", lambda *_args: None)
    runtime = StreamRuntime(
        collector=collector,  # type: ignore[arg-type]
        stream_settings=stream_settings(**setting_overrides),  # type: ignore[arg-type]
        storage_settings=object(),  # type: ignore[arg-type]
        runtime_policy=object(),  # type: ignore[arg-type]
    )
    return runtime, events


def test_session_loop_polls_demand_with_bounded_slices_and_keeps_flush_cadence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    collector = FakeCollector(clock, disconnect_after_flushes=2)
    runtime, events = make_runtime(
        monkeypatch,
        collector,
        flush_interval_seconds=0.2,
    )

    assert runtime.session_loop() is True

    assert collector.flush_times == pytest.approx([0.2, 0.4])
    assert collector.demand_times == pytest.approx(
        [0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4]
    )
    assert collector.ib.sleep_calls == pytest.approx([0.05] * 8)
    assert all(0.0 < delay <= 0.05 for delay in collector.ib.sleep_calls)
    assert sum(event["event"] == "flush" for event in events) == 2
    assert sum(event["event"] == "exact_leg_demand_polled" for event in events) == 8


def test_session_loop_disabled_demand_uses_one_flush_sleep_and_never_reconciles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    collector = FakeCollector(clock)

    def unexpected_reconcile() -> None:
        raise AssertionError("disabled demand reconciliation was called")

    collector.reconcile_exact_leg_demand = unexpected_reconcile  # type: ignore[method-assign]
    runtime, _events = make_runtime(
        monkeypatch,
        collector,
        exact_leg_pin_enabled=False,
    )

    assert runtime.session_loop() is True
    assert collector.ib.sleep_calls == pytest.approx([1.0])
    assert collector.flush_times == pytest.approx([1.0])


def test_zero_poll_setting_is_clamped_to_positive_sleep(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    collector = FakeCollector(clock)
    runtime, _events = make_runtime(
        monkeypatch,
        collector,
        flush_interval_seconds=0.025,
        quote_demand_poll_seconds=0.0,
    )

    assert runtime.session_loop() is True
    assert collector.flush_times == pytest.approx([0.025])
    assert collector.ib.sleep_calls == pytest.approx([0.01, 0.01, 0.005])
    assert all(delay > 0.0 for delay in collector.ib.sleep_calls)


def test_competing_error_precedes_generic_subscription_health_reconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    collector = FakeCollector(clock)
    collector.subscription_health_failed = True
    conflict = IbkrError(
        req_id=-1,
        error_code=10197,
        message="competing live session",
        contract=None,
        ts="2026-07-19T00:00:00+00:00",
    )
    collector.drain_new_errors = lambda: [conflict]  # type: ignore[method-assign]
    deferred: list[float] = []
    collector.defer_market_data_after_conflict = (  # type: ignore[attr-defined]
        lambda *, seconds: deferred.append(seconds)
    )
    collector.broker_settings = SimpleNamespace(account_read_enabled=False)  # type: ignore[attr-defined]
    runtime, events = make_runtime(
        monkeypatch,
        collector,
        flush_interval_seconds=0.0,
    )
    runtime.runtime_policy = SimpleNamespace(ibkr_conflict_probe_seconds=5.0)

    assert runtime.session_loop() is False
    assert deferred == [5.0]
    assert any(event.get("event") == "competing_session" for event in events)
    assert not any(event.get("event") == "subscription_health_reconnect" for event in events)


def test_repeated_competing_sessions_use_bounded_exponential_cooldown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    collector = FakeCollector(clock)
    collector.broker_settings = SimpleNamespace(account_read_enabled=False)  # type: ignore[attr-defined]
    deferred: list[float] = []
    collector.defer_market_data_after_conflict = (  # type: ignore[attr-defined]
        lambda *, seconds: deferred.append(seconds)
    )
    runtime, events = make_runtime(monkeypatch, collector)
    runtime.runtime_policy = SimpleNamespace(
        ibkr_conflict_probe_seconds=5.0,
        ibkr_conflict_probe_max_seconds=20.0,
    )
    runtime.__post_init__()

    for expected_clock in (0.0, 5.0, 15.0, 35.0):
        clock.now = expected_clock
        runtime._defer_competing_session(phase="active_session")

    assert deferred == [5.0, 10.0, 20.0, 20.0]
    conflicts = [event for event in events if event.get("event") == "competing_session"]
    assert [event["conflict_count"] for event in conflicts] == [1, 2, 3, 4]
    assert [event["probe_in_seconds"] for event in conflicts] == deferred
    assert runtime.competing_session_circuit.state(now_monotonic=54.0) == "open"
    assert runtime.competing_session_circuit.state(now_monotonic=55.0) == "half_open"


def test_healthy_data_flush_closes_competing_session_circuit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    collector = FakeCollector(clock, disconnect_after_flushes=2)
    original_flush = collector.flush
    clear_calls: list[float] = []
    collector.clear_market_data_conflict = lambda: clear_calls.append(clock.now)  # type: ignore[attr-defined]

    def available_flush() -> dict[str, object]:
        original_flush()
        return {
            "task": "ibkr_stream",
            "event": "flush",
            "quotes": 10,
            "provider_status": "available",
            "provider_reason": None,
            "farm_status": "ok",
        }

    collector.flush = available_flush  # type: ignore[method-assign]
    runtime, _events = make_runtime(monkeypatch, collector)
    runtime.competing_session_circuit.open(now_monotonic=0.0)
    runtime.competing_session_circuit.open(now_monotonic=5.0)

    assert runtime.session_loop() is True

    assert clear_calls == [1.0]
    assert runtime.competing_session_circuit.failures == 0
    assert runtime.session_had_healthy_flush is True


def test_non_competing_subscription_failure_does_not_open_conflict_circuit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    collector = FakeCollector(clock)
    collector.subscription_health_failed = True
    runtime, events = make_runtime(monkeypatch, collector)

    assert runtime.session_loop() is True

    assert runtime.competing_session_circuit.failures == 0
    assert any(event.get("event") == "subscription_health_reconnect" for event in events)
    assert not any(event.get("event") == "competing_session" for event in events)


def test_health_projection_validity_tracks_policy_heartbeat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    collector = FakeCollector(clock)
    runtime, _events = make_runtime(
        monkeypatch,
        collector,
        policy_check_seconds=40.0,
    )
    runtime.storage_settings = SimpleNamespace(data_root="/tmp")  # type: ignore[assignment]
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        supervisor_module,
        "persist_stream_health",
        lambda _storage, **kwargs: captured.update(kwargs),
    )

    runtime._publish_health(
        data_plane_healthy=False,
        policy_blocked=True,
        reason="test heartbeat",
    )

    assert captured["max_age_seconds"] == 120.0


def test_competing_session_standby_heartbeats_keep_retry_visible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    collector = FakeCollector(clock)
    collector.tws_connectivity_lost = False  # type: ignore[attr-defined]
    collector.connection_required = lambda: True  # type: ignore[attr-defined]
    collector.market_data_block_reason = (  # type: ignore[attr-defined]
        lambda: "competing live session owns shared market data (IBKR 10197)"
    )
    runtime, _events = make_runtime(
        monkeypatch,
        collector,
        policy_check_seconds=30.0,
    )
    runtime.storage_settings = SimpleNamespace(data_root="/tmp")  # type: ignore[assignment]
    for _ in range(7):
        runtime.competing_session_circuit.open(now_monotonic=clock.now)

    captured: list[dict[str, object]] = []
    monkeypatch.setattr(
        supervisor_module,
        "persist_stream_health",
        lambda _storage, **kwargs: captured.append(kwargs),
    )

    assert runtime.account_standby_loop() is False

    assert captured
    heartbeat = captured[-1]
    assert heartbeat["policy_blocked"] is True
    assert heartbeat["circuit_state"] == "open"
    assert heartbeat["retry_in_seconds"] == pytest.approx(270.0)
    assert heartbeat["reason"] == "competing live session cooldown (IBKR 10197)"
