from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def test_core_no_longer_owns_a_surface_api_socket() -> None:
    unit = read("systemd/spx-core.service")
    core = read("src/spx_spark/core_main.py")

    assert "ProtectSystem=strict" in unit
    assert "ProtectHome=read-only" in unit
    assert "PrivateTmp=true" in unit
    assert "NoNewPrivileges=true" in unit
    assert "RestrictAddressFamilies=AF_UNIX" in unit
    assert "PrivateNetwork=true" in unit
    assert "runtime/core-api.sock" not in unit
    assert "SPX_CORE_SOCKET_PATH" not in unit
    assert "spx_spark.web.live_api" not in core
    assert "surface_dashboard.run_loop" not in core


def test_only_notification_images_remain_served() -> None:
    entry = read("site/spxw-surface/entry-nginx.conf")
    compose = read("site/spxw-surface/compose.yaml")

    assert "\n  spxw-surface:\n" not in compose
    assert "container_name: spxw-surface-entry" in compose
    assert "network_mode: container:code-server" not in compose
    assert "location = /oi/latest.png" in entry
    assert "location = /strategy-risk/latest.png" in entry
    assert entry.count("return 410;") == 7


def test_surface_website_and_projection_code_are_deleted() -> None:
    retired_modules = (
        "surface_artifact.py",
        "surface_dashboard.py",
        "surface_dashboard_replay.py",
        "surface_live_session_input.py",
        "surface_live_session_models.py",
        "surface_live_session_projection.py",
        "surface_live_session_store.py",
        "surface_live_session_worker.py",
        "surface_replay_catalog_payload.py",
        "surface_replay_service.py",
        "surface_replay_session.py",
        "surface_replay_session_data.py",
        "surface_replay_session_frames.py",
        "surface_replay_session_models.py",
        "surface_replay_session_reference.py",
        "surface_replay_trend.py",
    )
    for module in retired_modules:
        assert not (ROOT / "src" / "spx_spark" / module).exists()
    assert not (ROOT / "src" / "spx_spark" / "web" / "live_api.py").exists()
    assert not (ROOT / "src" / "spx_spark" / "web" / "replay_api.py").exists()
    public = ROOT / "site" / "spxw-surface" / "public"
    assert not public.exists() or not any(public.iterdir())
    assert not (ROOT / "site" / "spxw-surface" / "nginx.conf").exists()

    cli = read("src/spx_spark/cli.py")
    maintenance = read("src/spx_spark/maintenance.py")
    jobs = read("src/spx_spark/infrastructure/jobs.py")
    runtime = read("config/runtime.toml")
    assert "surface_dashboard_replay" not in cli
    assert "cache-prune" not in maintenance
    assert "cache-prune" not in jobs
    assert "surface_cache_retention_days" not in runtime


def test_live_deployment_shell_entrypoints_parse() -> None:
    for relative in (
        "scripts/install-spx-spark-services.sh",
        "scripts/run-session-finalize.sh",
    ):
        subprocess.run(
            ["bash", "-n", str(ROOT / relative)],
            check=True,
            capture_output=True,
            text=True,
        )
