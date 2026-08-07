from __future__ import annotations

import json
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from spx_spark.application.runtime import intraday_shock_hot_worker as hot_worker
from spx_spark.application.runtime import market_features_hot_worker as shared_hot_worker


def test_worker_telemetry_identifies_the_intraday_shock_owner() -> None:
    events: list[dict[str, object]] = []

    result = shared_hot_worker.run_worker_loop(
        lambda: 0,
        interval_seconds=5.0,
        stop_event=threading.Event(),
        max_cycles=1,
        emit=events.append,
        task_name="intraday_shock_hot_worker",
    )

    assert result == 0
    assert events[0]["task"] == "intraday_shock_hot_worker"


def test_default_lock_inode_lives_in_the_stable_user_runtime_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))

    assert hot_worker.default_lock_path() == tmp_path / hot_worker.LOCK_FILE_NAME


def test_direct_one_shot_refuses_to_overlap_the_hot_owner(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    lock_path = tmp_path / "intraday-shock-owner.lock"
    calls: list[str] = []

    with shared_hot_worker.ProcessLock(lock_path):
        result = hot_worker.run_locked_intraday_shock_once(
            lambda: calls.append("ran") or 0,
            lock_path=lock_path,
        )

    assert result == 75
    assert calls == []
    output = json.loads(capsys.readouterr().out)
    assert output["task"] == "intraday_shock"
    assert output["event"] == "owner_lock_unavailable"
    assert output["lock_path"] == str(lock_path)


def test_direct_intraday_shock_main_uses_the_shared_owner_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from spx_spark.application.shock import service

    calls: list[str] = []

    def locked(cycle) -> int:
        calls.append("locked")
        return cycle()

    monkeypatch.setattr(hot_worker, "run_locked_intraday_shock_once", locked)
    monkeypatch.setattr(service, "run", lambda: calls.append("cycle") or 0)

    with pytest.raises(SystemExit) as exited:
        service.main()

    assert exited.value.code == 0
    assert calls == ["locked", "cycle"]


def test_core_cycle_suppresses_legacy_full_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from spx_spark.application.shock import service

    calls: list[list[str]] = []
    monkeypatch.setattr(service, "run", lambda argv: calls.append(argv) or 0)

    assert hot_worker.run_intraday_shock_cycle(emit_json=False) == 0
    assert calls == [[]]


def test_embedded_cycle_reuses_snapshot_and_keeps_a_separate_lease(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    latest = object()
    options = object()
    app = object()
    storage = SimpleNamespace(data_root=str(tmp_path))
    calls: list[dict[str, object]] = []
    monotonic = iter((100.0, 100.25))

    def cycle(**kwargs) -> int:
        calls.append(kwargs)
        return 0

    monkeypatch.setattr(hot_worker, "run_intraday_shock_cycle", cycle)
    monkeypatch.setattr(hot_worker.time, "monotonic", lambda: next(monotonic))

    hot_worker.run_embedded_intraday_shock_cycle(
        latest,
        options,
        app_settings=app,
        storage_settings=storage,
        emit_json=False,
    )

    assert calls == [
        {
            "emit_json": False,
            "latest_state": latest,
            "options_map": options,
            "app_settings": app,
            "storage_settings": storage,
        }
    ]
    lease = json.loads(
        (tmp_path / "latest" / "intraday_shock_hot_worker.lease.json").read_text(
            encoding="utf-8"
        )
    )
    assert lease["ok"] is True
    assert lease["duration_ms"] == 250.0
    assert lease["execution_mode"] == "embedded_direct_call"
    emitted = json.loads(capsys.readouterr().out)
    assert emitted == lease


def test_cli_once_uses_the_typed_runtime_shock_cadence_and_one_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    app = SimpleNamespace(runtime=SimpleNamespace(intraday_shock_interval_seconds=7))
    calls: list[str] = []
    monkeypatch.setattr(hot_worker, "load_app_settings", lambda: app)
    monkeypatch.setattr(
        hot_worker,
        "run_intraday_shock_cycle",
        lambda **kwargs: calls.append("cycle") or 0,
    )
    monkeypatch.setattr(hot_worker, "install_stop_handlers", lambda stop_event: None)

    result = hot_worker.run(["--once", "--lock-path", str(tmp_path / "worker.lock")])

    assert result == 0
    assert calls == ["cycle"]
    output = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [event["event"] for event in output] == ["started", "cycle_finished", "stopped"]
    assert output[0]["interval_seconds"] == 7.0
    assert output[1]["task"] == "intraday_shock_hot_worker"
