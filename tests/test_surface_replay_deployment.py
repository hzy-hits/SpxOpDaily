from __future__ import annotations

import os
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


def test_post_close_timer_warms_catalog_and_default_session_surfaces() -> None:
    timer = read("systemd/spx-spark-surface-replay-warm.timer")
    warmer = read("scripts/warm-spxw-surface-replay-catalog.sh")

    assert "21:20:00 UTC" in timer
    assert "22:20:00 UTC" in timer
    assert "23:20:00 UTC" in timer
    assert 'for session_date in "${session_dates[@]}"' in warmer
    assert 'latest_session="${session_dates[0]}"' in warmer
    assert 'surface_times=("${frame_times[-1]}")' in warmer
    assert 'latest_frame_times=("${frame_times[@]}")' in warmer
    assert "Land every catalog date first" in warmer
    assert "/timeline?step_minutes=5" in warmer
    assert 'payload.get("surface_frames") or payload.get("frames", [])' in warmer
    assert 'row["at"]' in warmer
    assert '"role=front"' in warmer
    assert '"weighting=oi_weighted"' in warmer
    assert '"weighting=volume_weighted"' in warmer
    assert '"bucket_minutes=5"' in warmer
    assert '"price_step=5"' in warmer
    assert '"price_step=2.5"' in warmer
    assert "seeds the replay worker's causal" in warmer
    assert "datetime.timedelta(seconds=3)" in warmer
    assert "/session-surface" in warmer
    assert "/trend?" not in warmer
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


def test_catalog_warmer_lands_every_session_before_latest_full_playback(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_curl = fake_bin / "curl"
    fake_curl.write_text(
        """#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys

args = sys.argv[1:]
url = next(value for value in args if value.startswith("http://"))
log = Path(os.environ["WARM_TEST_LOG"])
if url.endswith("/api/v1/replay/sessions"):
    print(json.dumps({"sessions": [
        {"session_date": "2026-07-17"},
        {"session_date": "2026-07-16"},
    ]}))
elif "/timeline?" in url:
    session = url.split("/sessions/", 1)[1].split("/", 1)[0]
    surface_frames = (
        ["2026-07-17T00:20:00Z", "2026-07-17T14:35:00Z"]
        if session == "2026-07-17"
        else ["2026-07-16T00:20:00Z", "2026-07-16T14:30:00Z"]
    )
    with log.open("a", encoding="utf-8") as stream:
        stream.write(f"timeline:{session}\\n")
    print(json.dumps({
        "frames": [{"at": "2099-01-01T00:00:00Z"}],
        "surface_frames": [{"at": value} for value in surface_frames],
    }))
else:
    session = url.split("/sessions/", 1)[1].split("/", 1)[0]
    encoded = [
        args[index + 1]
        for index, value in enumerate(args)
        if value == "--data-urlencode"
    ]
    at = next(value.removeprefix("at=") for value in encoded if value.startswith("at="))
    with log.open("a", encoding="utf-8") as stream:
        stream.write(f"surface:{session}:{at}\\n")
    print("{}")
""",
        encoding="utf-8",
    )
    fake_curl.chmod(0o755)
    log = tmp_path / "warm.log"
    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{fake_bin}:{env['PATH']}",
            "WARM_TEST_LOG": str(log),
            "MARKET_DATA_DATA_ROOT": str(tmp_path / "data"),
        }
    )

    completed = subprocess.run(
        ["bash", str(ROOT / "scripts/warm-spxw-surface-replay-catalog.sh")],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    assert log.read_text(encoding="utf-8").splitlines() == [
        "timeline:2026-07-17",
        "surface:2026-07-17:2026-07-17T14:35:00Z",
        "timeline:2026-07-16",
        "surface:2026-07-16:2026-07-16T14:30:00Z",
        "surface:2026-07-17:2026-07-17T14:34:57Z",
        "surface:2026-07-17:2026-07-17T00:20:00Z",
    ]
    assert "warmed 2 replay timelines and 4 session surface requests" in completed.stdout


def test_only_canonical_service_module_is_directly_executable() -> None:
    transport = read("src/spx_spark/surface_replay_http.py")
    service = read("src/spx_spark/surface_replay_service.py")

    assert 'if __name__ == "__main__"' not in transport
    assert 'if __name__ == "__main__"' in service
