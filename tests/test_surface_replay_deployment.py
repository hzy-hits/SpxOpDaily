from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def test_replay_service_is_unix_socket_only_and_resource_bounded() -> None:
    unit = read("systemd/spx-spark-surface-replay.service")
    runner = read("scripts/run-spxw-surface-replay-service.sh")

    assert "--unix-socket \"$SOCKET_PATH\"" in runner
    assert "--bind-host" not in runner
    assert "Nice=10" in unit
    assert "IOSchedulingClass=idle" in unit
    assert "MemoryMax=2G" in unit
    assert "ProtectSystem=strict" in unit
    assert "EnvironmentFile=" not in unit
    assert "SPX_SPARK_DISABLE_DOTENV=1" in unit
    assert "PrivateNetwork=true" in unit
    assert "RestrictAddressFamilies=AF_UNIX" in unit
    assert "ReadWritePaths=/srv/data/spx-spark/data/published/spxw-surface" in unit


def test_post_close_timer_warms_catalog_without_materializing_frames() -> None:
    timer = read("systemd/spx-spark-surface-replay-warm.timer")
    warmer = read("scripts/warm-spxw-surface-replay-catalog.sh")

    assert "21:20:00 UTC" in timer
    assert "22:20:00 UTC" in timer
    assert "23:20:00 UTC" in timer
    assert "/timeline?step_minutes=5" in warmer
    assert "/frame?" not in warmer
    warm_unit = read("systemd/spx-spark-surface-replay-warm.service")
    assert "EnvironmentFile=" not in warm_unit
    assert "PrivateNetwork=true" in warm_unit
    assert "RestrictAddressFamilies=AF_UNIX" in warm_unit


def test_replay_shell_entrypoints_parse() -> None:
    for relative in (
        "scripts/run-spxw-surface-replay-service.sh",
        "scripts/warm-spxw-surface-replay-catalog.sh",
    ):
        subprocess.run(
            ["bash", "-n", str(ROOT / relative)],
            check=True,
            capture_output=True,
            text=True,
        )


def test_only_canonical_service_module_is_directly_executable() -> None:
    transport = read("src/spx_spark/surface_replay_http.py")
    service = read("src/spx_spark/surface_replay_service.py")

    assert 'if __name__ == "__main__"' not in transport
    assert 'if __name__ == "__main__"' in service
