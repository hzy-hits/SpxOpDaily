from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

import spx_spark.ibkr.stream.supervisor as supervisor_module
from spx_spark.application.market_features.provider_entry_control import (
    gth_ibkr_entry_control,
)
from spx_spark.ibkr.stream.health import persist_stream_health
from spx_spark.ibkr.stream.session_ops import SessionOps
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


class FakeCollector(SessionOps):
    def __init__(self, clock: FakeClock, *, disconnect_after_flushes: int = 1) -> None:
        self.clock = clock
        self.ib = FakeIb(clock)
        self.errors: list[IbkrError] = []
        self.subscription_rejection_sequence = 0
        self.subscription_rejection_log: list[tuple[int, IbkrError]] = []
        self.subscription_rows_by_req_id = {}
        self.subscription_lane_by_req_id = {}
        self.subscription_lane_history_by_req_id = {}
        self._subscription_request_lane = None
        self.farm_health = SimpleNamespace(observe=lambda *_args: None)
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
        errors, self.errors = self.errors, []
        return errors

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


def _seed_healthy_stream_health(tmp_path) -> tuple[SimpleNamespace, datetime]:
    storage = SimpleNamespace(data_root=str(tmp_path))
    observed_at = datetime.now(tz=timezone.utc)
    persist_stream_health(
        storage,  # type: ignore[arg-type]
        data_plane_healthy=True,
        policy_blocked=False,
        reason="healthy market-data flush",
        connected=True,
        circuit_state="closed",
        conflict_count=0,
        observed_at=observed_at,
    )
    return storage, observed_at


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


def test_session_loop_disabled_demand_uses_bounded_health_slices_and_never_reconciles(
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
    assert collector.ib.sleep_calls == pytest.approx([0.05] * 20)
    assert all(0.0 < delay <= 0.05 for delay in collector.ib.sleep_calls)
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


def test_large_poll_cannot_delay_callback_durable_competing_gate(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    collector = FakeCollector(clock, disconnect_after_flushes=99)
    runtime, _events = make_runtime(
        monkeypatch,
        collector,
        flush_interval_seconds=1.0,
        quote_demand_poll_seconds=600.0,
    )
    storage, healthy_at = _seed_healthy_stream_health(tmp_path)
    runtime.storage_settings = storage  # type: ignore[assignment]
    runtime.runtime_policy = SimpleNamespace(ibkr_conflict_probe_seconds=5.0)
    collector.broker_settings = SimpleNamespace(account_read_enabled=False)  # type: ignore[attr-defined]
    collector.defer_market_data_after_conflict = lambda *, seconds: None  # type: ignore[method-assign]
    original_sleep = collector.ib.sleep
    callback_gate_results: list[dict[str, object]] = []

    def sleep_and_receive_conflict(seconds: float) -> None:
        collector._on_error(
            -1,
            10197,
            "No market data during competing live session",
            None,
        )
        callback_gate_results.append(
            gth_ibkr_entry_control(
                tmp_path,
                now=datetime.now(tz=timezone.utc),
            )
        )
        original_sleep(seconds)

    collector.ib.sleep = sleep_and_receive_conflict  # type: ignore[method-assign]

    assert gth_ibkr_entry_control(
        tmp_path,
        now=healthy_at,
    )["allowed"] is True
    assert runtime.session_loop() is False

    assert collector.ib.sleep_calls == pytest.approx([0.05])
    assert callback_gate_results[0]["allowed"] is False
    assert callback_gate_results[0]["reason"] == "ibkr_competing_session"


def test_callback_during_flush_latches_over_stale_healthy_publish(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    collector = FakeCollector(clock, disconnect_after_flushes=99)
    runtime, _events = make_runtime(
        monkeypatch,
        collector,
        flush_interval_seconds=0.0,
    )
    storage, healthy_at = _seed_healthy_stream_health(tmp_path)
    runtime.storage_settings = storage  # type: ignore[assignment]
    runtime.runtime_policy = SimpleNamespace(ibkr_conflict_probe_seconds=5.0)
    collector.broker_settings = SimpleNamespace(account_read_enabled=False)  # type: ignore[attr-defined]
    collector.defer_market_data_after_conflict = lambda *, seconds: None  # type: ignore[method-assign]
    callback_gate_results: list[dict[str, object]] = []

    def flush_with_competing_callback() -> dict[str, object]:
        collector.flush_times.append(clock.now)
        collector._on_error(
            -1,
            10197,
            "No market data during competing live session",
            None,
        )
        callback_gate_results.append(
            gth_ibkr_entry_control(
                tmp_path,
                now=datetime.now(tz=timezone.utc),
            )
        )
        # Simulate an already-running flush trying to publish its stale
        # pre-callback healthy result. The incident latch must win.
        runtime._publish_health(
            data_plane_healthy=True,
            policy_blocked=False,
            reason="stale in-flight healthy flush",
        )
        callback_gate_results.append(
            gth_ibkr_entry_control(
                tmp_path,
                now=datetime.now(tz=timezone.utc),
            )
        )
        return {
            "task": "ibkr_stream",
            "event": "flush",
            "fresh_quotes": 10,
            "fresh_spxw_quotes": 10,
            "data_plane_healthy": True,
            "provider_status": "available",
        }

    collector.flush = flush_with_competing_callback  # type: ignore[method-assign]

    assert gth_ibkr_entry_control(
        tmp_path,
        now=healthy_at,
    )["allowed"] is True
    assert runtime.session_loop() is False

    assert [row["allowed"] for row in callback_gate_results] == [False, False]
    assert all(
        row["reason"] == "ibkr_competing_session"
        for row in callback_gate_results
    )


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


def test_subscription_failure_during_wait_publishes_unhealthy_before_next_flush(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    collector = FakeCollector(clock, disconnect_after_flushes=99)
    original_sleep = collector.ib.sleep
    conflict = IbkrError(
        req_id=-1,
        error_code=10197,
        message="competing live session",
        contract=None,
        ts="2026-08-06T10:52:00+00:00",
    )
    errors_drained = False

    def sleep_and_receive_conflict(seconds: float) -> None:
        original_sleep(seconds)
        collector.subscription_health_failed = True

    def drain_errors() -> list[IbkrError]:
        nonlocal errors_drained
        if errors_drained:
            return []
        errors_drained = True
        return [conflict]

    collector.ib.sleep = sleep_and_receive_conflict  # type: ignore[method-assign]
    collector.drain_new_errors = drain_errors  # type: ignore[method-assign]
    deferred: list[float] = []
    collector.defer_market_data_after_conflict = (  # type: ignore[attr-defined]
        lambda *, seconds: deferred.append(seconds)
    )
    collector.broker_settings = SimpleNamespace(  # type: ignore[attr-defined]
        account_read_enabled=False
    )
    runtime, _events = make_runtime(monkeypatch, collector)
    runtime.runtime_policy = SimpleNamespace(ibkr_conflict_probe_seconds=5.0)
    published: list[tuple[float, dict[str, object]]] = []
    runtime._publish_health = (  # type: ignore[method-assign]
        lambda **kwargs: published.append((clock.now, kwargs))
    )

    assert runtime.session_loop() is False

    # The callback is classified after one bounded event-loop slice, not after
    # the ordinary one-second flush (or a multi-second conflict probe window).
    assert collector.ib.sleep_calls == pytest.approx([0.05])
    assert collector.flush_times == pytest.approx([0.05])
    assert published[0] == (
        pytest.approx(0.05),
        {
            "data_plane_healthy": False,
            "policy_blocked": False,
            "reason": "subscription health failed; awaiting error classification",
        },
    )
    assert published[-1][1]["data_plane_healthy"] is False
    assert published[-1][1]["policy_blocked"] is True
    assert "10197" in str(published[-1][1]["reason"])
    assert deferred == [5.0]


def test_rotation_competing_error_keeps_healthy_session_running(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    collector = FakeCollector(clock, disconnect_after_flushes=2)
    rotation_conflict = IbkrError(
        req_id=2262,
        error_code=10197,
        message="competing live session",
        contract=None,
        ts="2026-08-04T02:00:00+00:00",
        subscription_lane="rotation",
    )
    drain_count = 0

    def drain_errors() -> list[IbkrError]:
        nonlocal drain_count
        drain_count += 1
        return [rotation_conflict] if drain_count == 1 else []

    collector.drain_new_errors = drain_errors  # type: ignore[method-assign]
    deferred: list[float] = []
    collector.defer_market_data_after_conflict = (  # type: ignore[attr-defined]
        lambda *, seconds: deferred.append(seconds)
    )
    collector.broker_settings = SimpleNamespace(  # type: ignore[attr-defined]
        account_read_enabled=False
    )
    runtime, events = make_runtime(monkeypatch, collector)

    assert runtime.session_loop() is True

    assert len(collector.flush_times) == 2
    assert deferred == []
    assert runtime.competing_session_circuit.failures == 0
    assert not any(event.get("event") == "competing_session" for event in events)


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
        ibkr_conflict_recovery_seconds=60.0,
    )
    runtime.__post_init__()

    for expected_clock in (0.0, 5.0, 15.0, 35.0):
        clock.now = expected_clock
        runtime._defer_competing_session(phase="active_session")

    assert deferred == [5.0, 10.0, 20.0, 20.0]
    conflicts = [event for event in events if event.get("event") == "competing_session"]
    assert [event["conflict_count"] for event in conflicts] == [1, 2, 3, 4]
    assert [event["probe_in_seconds"] for event in conflicts] == deferred
    assert runtime.competing_session_circuit.recovery_seconds == 60.0
    assert runtime.competing_session_circuit.state(now_monotonic=54.0) == "open"
    assert runtime.competing_session_circuit.state(now_monotonic=55.0) == "half_open"


def test_brief_healthy_flush_preserves_competing_session_backoff_history(
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
    assert runtime.competing_session_circuit.failures == 2
    assert runtime.session_had_healthy_flush is True


def test_fresh_spxw_releases_10197_latch_even_when_farm_flag_is_broken(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    collector = FakeCollector(clock, disconnect_after_flushes=4)

    def farm_broken_but_live_spxw() -> dict[str, object]:
        collector.flush_times.append(clock.now)
        if len(collector.flush_times) >= collector.disconnect_after_flushes:
            collector.ib.connected = False
        return {
            "task": "ibkr_stream",
            "event": "flush",
            "quotes": 48,
            "fresh_quotes": 47,
            "fresh_spxw_quotes": 46,
            "data_plane_healthy": False,
            "farm_status": "broken",
            "provider_status": "degraded",
            "provider_reason": "IBKR market data farms not ready",
        }

    collector.flush = farm_broken_but_live_spxw  # type: ignore[method-assign]
    runtime, events = make_runtime(
        monkeypatch,
        collector,
        exact_leg_pin_enabled=False,
        flush_interval_seconds=1.0,
    )
    storage, _healthy_at = _seed_healthy_stream_health(tmp_path)
    runtime.storage_settings = storage  # type: ignore[assignment]
    runtime.competing_session_circuit.recovery_seconds = 1.0
    runtime.competing_session_circuit.open(now_monotonic=0.0)
    runtime._invalidate_competing_session_health(
        error_code=10197,
        message="No market data during competing live session",
    )

    latched = gth_ibkr_entry_control(tmp_path, now=datetime.now(tz=timezone.utc))
    assert latched["allowed"] is False
    assert latched["reason"] == "ibkr_competing_session"

    assert runtime.session_loop() is True

    assert runtime.competing_session_circuit.failures == 0
    assert runtime._competing_health_latched_sequence is None
    assert any(event.get("event") == "competing_session_recovered" for event in events)
    recovered = gth_ibkr_entry_control(tmp_path, now=datetime.now(tz=timezone.utc))
    assert recovered["source_reason"] != "competing live session callback latched (IBKR 10197)"
    assert recovered["circuit_state"] == "closed"
    assert recovered["conflict_count"] == 0


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


def test_open_conflict_circuit_blocks_gateway_restart(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    collector = FakeCollector(clock)
    collector.force = False  # type: ignore[attr-defined]
    collector.farm_health = SimpleNamespace(  # type: ignore[attr-defined]
        should_restart_gateway=lambda: True,
    )
    runtime, _events = make_runtime(
        monkeypatch,
        collector,
        auto_restart_gateway_on_farm_broken=True,
    )
    monkeypatch.setattr(
        supervisor_module,
        "runtime_blocks_gateway_restart",
        lambda *_args, **_kwargs: False,
    )
    runtime.competing_session_circuit.open(now_monotonic=clock.now)

    assert runtime._should_restart_gateway() is False


@pytest.mark.parametrize(
    ("globex_open", "expected"),
    [(False, False), (True, True)],
)
def test_gateway_restart_requires_open_es_session(
    monkeypatch: pytest.MonkeyPatch,
    globex_open: bool,
    expected: bool,
) -> None:
    clock = FakeClock()
    collector = FakeCollector(clock)
    collector.force = False  # type: ignore[attr-defined]
    collector.market_calendar = SimpleNamespace(  # type: ignore[attr-defined]
        is_globex_open=lambda _now: globex_open,
    )
    collector.farm_health = SimpleNamespace(  # type: ignore[attr-defined]
        should_restart_gateway=lambda: True,
    )
    runtime, _events = make_runtime(
        monkeypatch,
        collector,
        auto_restart_gateway_on_farm_broken=True,
    )
    monkeypatch.setattr(
        supervisor_module,
        "runtime_blocks_gateway_restart",
        lambda *_args, **_kwargs: False,
    )

    assert runtime._should_restart_gateway() is expected


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
    runtime.runtime_policy = SimpleNamespace(
        ibkr_conflict_probe_seconds=5.0,
        ibkr_conflict_probe_max_seconds=300.0,
        ibkr_conflict_recovery_seconds=60.0,
    )
    runtime.__post_init__()
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
    assert runtime.competing_session_circuit.recovery_seconds == 60.0
