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


def test_cli_once_uses_the_service_loop_shock_cadence_and_one_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    app = object()
    calls: list[str] = []
    monkeypatch.setattr(hot_worker, "load_app_settings", lambda: app)
    monkeypatch.setattr(
        hot_worker,
        "ServiceLoopSettings",
        SimpleNamespace(
            from_app_settings=lambda loaded: SimpleNamespace(
                intraday_shock_interval_seconds=7
            )
        ),
    )
    monkeypatch.setattr(
        hot_worker,
        "run_intraday_shock_cycle",
        lambda: calls.append("cycle") or 0,
    )
    monkeypatch.setattr(hot_worker, "install_stop_handlers", lambda stop_event: None)

    result = hot_worker.run(["--once", "--lock-path", str(tmp_path / "worker.lock")])

    assert result == 0
    assert calls == ["cycle"]
    output = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [event["event"] for event in output] == ["started", "cycle_finished", "stopped"]
    assert output[0]["interval_seconds"] == 7.0
    assert output[1]["task"] == "intraday_shock_hot_worker"
