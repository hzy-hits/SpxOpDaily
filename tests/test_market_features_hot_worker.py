from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from spx_spark.application.runtime import market_features_hot_worker as hot_worker


class FakeClock:
    def __init__(self) -> None:
        self.value = 0.0
        self.base = datetime(2026, 7, 19, tzinfo=timezone.utc)

    def monotonic(self) -> float:
        return self.value

    def utcnow(self) -> datetime:
        return self.base + timedelta(seconds=self.value)

    def advance(self, seconds: float) -> None:
        self.value += seconds


class FakeStopEvent:
    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.stopped = False
        self.waits: list[float] = []

    def is_set(self) -> bool:
        return self.stopped

    def set(self) -> None:
        self.stopped = True

    def wait(self, timeout: float | None = None) -> bool:
        delay = float(timeout or 0.0)
        self.waits.append(delay)
        self.clock.advance(delay)
        return self.stopped


def test_resolve_market_features_interval_slows_outside_spx_sessions() -> None:
    rth = datetime(2026, 8, 7, 15, 0, tzinfo=timezone.utc)  # Friday 11:00 ET
    weekend = datetime(2026, 8, 9, 15, 0, tzinfo=timezone.utc)  # Sunday

    assert (
        hot_worker.resolve_market_features_interval(
            now=rth,
            hot_interval_seconds=5.0,
            sample_interval_seconds=60.0,
        )
        == 5.0
    )
    assert (
        hot_worker.resolve_market_features_interval(
            now=weekend,
            hot_interval_seconds=5.0,
            sample_interval_seconds=60.0,
        )
        == 60.0
    )


def test_worker_loop_accepts_dynamic_interval_callable() -> None:
    clock = FakeClock()
    stop = FakeStopEvent(clock)
    events: list[dict[str, object]] = []
    intervals = iter((5.0, 60.0))

    result = hot_worker.run_worker_loop(
        lambda: 0,
        interval_seconds=lambda: next(intervals),
        stop_event=stop,
        max_cycles=2,
        monotonic=clock.monotonic,
        utcnow=clock.utcnow,
        emit=events.append,
    )

    assert result == 0
    assert events[0]["interval_seconds"] == 5.0
    assert events[1]["interval_seconds"] == 60.0
    # max_cycles stops before sleeping after the final cycle.
    assert stop.waits == [5.0]


def test_worker_reuses_one_process_without_overlapping_slow_cycles() -> None:
    clock = FakeClock()
    stop = FakeStopEvent(clock)
    starts: list[float] = []
    durations = iter((2.0, 6.0))
    events: list[dict[str, object]] = []

    def cycle() -> int:
        starts.append(clock.monotonic())
        clock.advance(next(durations))
        return 0

    result = hot_worker.run_worker_loop(
        cycle,
        interval_seconds=5.0,
        stop_event=stop,
        max_cycles=2,
        monotonic=clock.monotonic,
        utcnow=clock.utcnow,
        emit=events.append,
    )

    assert result == 0
    assert starts == [0.0, 5.0]
    assert stop.waits == [3.0]
    assert events[0]["duration_ms"] == 2000.0
    assert events[1]["duration_ms"] == 6000.0
    assert events[1]["overrun_ms"] == 1000.0


def test_worker_exits_after_repeated_cycle_failures() -> None:
    clock = FakeClock()
    stop = FakeStopEvent(clock)
    events: list[dict[str, object]] = []

    def fail() -> int:
        raise RuntimeError("broken cycle")

    result = hot_worker.run_worker_loop(
        fail,
        interval_seconds=1.0,
        stop_event=stop,
        max_consecutive_failures=2,
        monotonic=clock.monotonic,
        utcnow=clock.utcnow,
        emit=events.append,
    )

    assert result == 1
    assert len(events) == 2
    assert events[-1]["consecutive_failures"] == 2
    assert events[-1]["error"] == "RuntimeError:broken cycle"


def test_worker_writes_atomic_readiness_lease(tmp_path: Path) -> None:
    clock = FakeClock()
    stop = FakeStopEvent(clock)
    lease_path = tmp_path / "worker.lease.json"

    result = hot_worker.run_worker_loop(
        lambda: 0,
        interval_seconds=1.0,
        stop_event=stop,
        max_cycles=1,
        monotonic=clock.monotonic,
        utcnow=clock.utcnow,
        emit=lambda _event: None,
        lease_path=lease_path,
    )

    lease = json.loads(lease_path.read_text(encoding="utf-8"))
    assert result == 0
    assert lease["ok"] is True
    assert lease["finished_at"] == clock.base.isoformat()
    assert lease["consecutive_failures"] == 0


def test_process_lock_rejects_a_second_owner(tmp_path: Path) -> None:
    lock_path = tmp_path / "hot-worker.lock"
    first = hot_worker.ProcessLock(lock_path)
    second = hot_worker.ProcessLock(lock_path)

    first.acquire()
    try:
        assert lock_path.read_text(encoding="utf-8") == f"{hot_worker.os.getpid()}\n"
        with pytest.raises(hot_worker.ProcessLockUnavailable, match="Lock is held"):
            second.acquire()
    finally:
        first.release()

    second.acquire()
    second.release()


def test_default_lock_inode_lives_outside_a_unit_owned_runtime_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))

    assert hot_worker.default_lock_path() == tmp_path / hot_worker.LOCK_FILE_NAME


def test_direct_one_shot_refuses_to_overlap_the_hot_owner(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    lock_path = tmp_path / "shared-owner.lock"
    calls: list[str] = []

    with hot_worker.ProcessLock(lock_path):
        result = hot_worker.run_locked_market_features_once(
            lambda: calls.append("ran") or 0,
            lock_path=lock_path,
        )

    assert result == 75
    assert calls == []
    output = json.loads(capsys.readouterr().out)
    assert output["event"] == "owner_lock_unavailable"
    assert output["lock_path"] == str(lock_path)


def test_core_worker_holds_the_embedded_shock_owner_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    feature_lock = tmp_path / "feature.lock"
    shock_lock = tmp_path / "shock.lock"

    monkeypatch.setattr(
        hot_worker,
        "load_app_settings",
            lambda: SimpleNamespace(
                market_features=SimpleNamespace(
                    interval_seconds=5,
                    sample_interval_seconds=60,
                )
            ),
    )
    monkeypatch.setattr(
        hot_worker,
        "StorageSettings",
        SimpleNamespace(
            from_env=lambda: SimpleNamespace(data_root=str(tmp_path)),
        ),
    )

    def cycle(*_args, **_kwargs) -> int:
        contender = hot_worker.ProcessLock(shock_lock)
        with pytest.raises(hot_worker.ProcessLockUnavailable):
            contender.acquire()
        return 0

    monkeypatch.setattr(hot_worker, "run_market_features_cycle", cycle)

    assert hot_worker.run_with_stop(
        stop_event=FakeStopEvent(FakeClock()),
        lock_path=feature_lock,
        additional_lock_path=shock_lock,
        max_cycles=1,
    ) == 0


def test_direct_market_features_main_uses_the_shared_owner_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from spx_spark.application.market_features import service

    calls: list[str] = []

    def locked(cycle) -> int:
        calls.append("locked")
        return cycle()

    monkeypatch.setattr(hot_worker, "run_locked_market_features_once", locked)
    monkeypatch.setattr(service, "run", lambda: calls.append("cycle") or 0)

    with pytest.raises(SystemExit) as exited:
        service.main()

    assert exited.value.code == 0
    assert calls == ["locked", "cycle"]


def test_cycle_forwards_frames_to_the_core_callback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from spx_spark.application.market_features import service

    observed: list[tuple[dict[str, object], dict[str, object]]] = []
    snapshots: list[tuple[object, object]] = []

    def run(argv, *, on_frames, on_analytical_snapshot, state_cache) -> int:
        assert argv == []
        assert state_cache == {"sentinel": True}
        on_analytical_snapshot("latest-state", "options-map")
        on_frames({"frame_id": "market:1"}, {"frame_id": "options:1"})
        return 0

    monkeypatch.setattr(service, "run", run)

    result = hot_worker.run_market_features_cycle(
        lambda market, options: observed.append((dict(market), dict(options))),
        on_analytical_snapshot=lambda latest, options: snapshots.append((latest, options)),
        emit_json=False,
        state_cache={"sentinel": True},
    )

    assert result == 0
    assert snapshots == [("latest-state", "options-map")]
    assert observed == [({"frame_id": "market:1"}, {"frame_id": "options:1"})]


def test_cli_once_uses_configured_cadence_and_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        hot_worker,
        "load_app_settings",
            lambda: SimpleNamespace(
                market_features=SimpleNamespace(
                    interval_seconds=7,
                    sample_interval_seconds=60,
                )
            ),
    )
    monkeypatch.setattr(
        hot_worker,
        "run_market_features_cycle",
        lambda *_args, **_kwargs: calls.append("cycle") or 0,
    )
    monkeypatch.setattr(hot_worker, "install_stop_handlers", lambda stop_event: None)

    result = hot_worker.run(["--once", "--lock-path", str(tmp_path / "worker.lock")])

    assert result == 0
    assert calls == ["cycle"]
    output = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [event["event"] for event in output] == ["started", "cycle_finished", "stopped"]
    assert output[0]["hot_interval_seconds"] == 7.0
    assert output[0]["closed_interval_seconds"] == 60.0
    assert output[0]["interval_seconds"] == hot_worker.resolve_market_features_interval(
        now=datetime.now(tz=timezone.utc),
        hot_interval_seconds=7.0,
        sample_interval_seconds=60.0,
    )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    (
        ({"interval_seconds": 0.0}, "interval"),
        ({"interval_seconds": 1.0, "max_consecutive_failures": 0}, "failures"),
        ({"interval_seconds": 1.0, "max_cycles": 0}, "cycles"),
    ),
)
def test_worker_rejects_invalid_loop_settings(kwargs: dict[str, object], message: str) -> None:
    clock = FakeClock()
    stop = FakeStopEvent(clock)

    with pytest.raises(ValueError, match=message):
        hot_worker.run_worker_loop(
            lambda: 0,
            stop_event=stop,
            monotonic=clock.monotonic,
            utcnow=clock.utcnow,
            emit=lambda event: None,
            **kwargs,
        )
