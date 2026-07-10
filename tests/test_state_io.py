from __future__ import annotations

import json
import os
import stat
from pathlib import Path

from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock


def file_mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


def test_atomic_write_json_secure_sets_temp_and_final_mode_0600(tmp_path, monkeypatch) -> None:
    state_path = tmp_path / "state.json"
    state_path.write_text("{}", encoding="utf-8")
    state_path.chmod(0o644)
    real_replace = os.replace
    observed_temp_modes: list[int] = []

    def recording_replace(source: str | Path, destination: str | Path) -> None:
        observed_temp_modes.append(file_mode(Path(source)))
        real_replace(source, destination)

    monkeypatch.setattr("spx_spark.state_io.os.replace", recording_replace)

    atomic_write_json_secure(state_path, {"schema_version": 2, "pending_events": []})

    assert observed_temp_modes == [0o600]
    assert file_mode(state_path) == 0o600
    assert json.loads(state_path.read_text(encoding="utf-8")) == {
        "pending_events": [],
        "schema_version": 2,
    }
    assert list(tmp_path.glob(".state.json.*.tmp")) == []


def test_exclusive_state_lock_uses_owner_only_lock_file(tmp_path) -> None:
    state_path = tmp_path / "state.json"

    with exclusive_state_lock(state_path):
        lock_path = tmp_path / "state.json.lock"
        assert lock_path.exists()
        assert file_mode(lock_path) == 0o600
