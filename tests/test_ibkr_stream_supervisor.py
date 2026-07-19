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
        "quote_demand_poll_seconds": 0.25,
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
    runtime, events = make_runtime(monkeypatch, collector)

    assert runtime.session_loop() is True

    assert collector.flush_times == pytest.approx([1.0, 2.0])
    assert collector.demand_times == pytest.approx(
        [0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75, 2.0]
    )
    assert collector.ib.sleep_calls == pytest.approx([0.25] * 8)
    assert all(0.0 < delay <= 0.25 for delay in collector.ib.sleep_calls)
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
