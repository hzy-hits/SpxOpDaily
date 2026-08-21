from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def test_core_keeps_resource_bounds_without_the_retired_surface_socket() -> None:
    unit = read("systemd/spx-core.service")

    assert ".venv/bin/spx core run" in unit
    assert "runtime/core-api.sock" not in unit
    assert "ExecStartPre=/usr/bin/install -d -m 0700" not in unit
    assert "MemoryMax=2G" in unit
    assert "ProtectSystem=strict" in unit
    assert "PrivateNetwork=true" in unit
    assert "RestrictAddressFamilies=AF_UNIX" in unit
    assert "ReadWritePaths=/srv/data/spx-spark/data %t" in unit


def test_replay_transport_remains_internal_tooling_only() -> None:
    project = read("pyproject.toml")
    api = read("src/spx_spark/web/replay_api.py")

    assert "def create_app(" in api
    assert "spx-spark-surface-replay-service" not in project
    assert not (ROOT / "scripts" / "warm-spxw-surface-replay-catalog.sh").exists()
    assert not (ROOT / "systemd" / "spx-spark-surface-replay-warm.service").exists()
    assert not (ROOT / "systemd" / "spx-spark-surface-replay-warm.timer").exists()
