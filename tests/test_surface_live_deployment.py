from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def test_live_routes_are_owned_by_the_single_core_unix_socket() -> None:
    unit = read("systemd/spx-core.service")
    core = read("src/spx_spark/core_main.py")

    assert "ProtectSystem=strict" in unit
    assert "ProtectHome=read-only" in unit
    assert "PrivateTmp=true" in unit
    assert "NoNewPrivileges=true" in unit
    assert "RestrictAddressFamilies=AF_UNIX" in unit
    assert "PrivateNetwork=true" in unit
    assert "runtime/core-api.sock" in unit
    assert "from spx_spark.web.live_api import create_default_app" in core


def test_nginx_proxies_only_read_only_live_surface_and_independent_health() -> None:
    nginx = read("site/spxw-surface/nginx.conf")
    compose = read("site/spxw-surface/compose.yaml")

    assert 'location = /api/v1/live/healthz {' in nginx
    assert 'location = /api/v1/live/session-surface {' in nginx
    assert nginx.count("limit_except GET") >= 8
    assert (
        "proxy_pass http://unix:/usr/share/nginx/replay-runtime/core-api.sock:/healthz;"
        in nginx
    )
    assert (
        "proxy_pass http://unix:/usr/share/nginx/replay-runtime/core-api.sock:"
        "/api/v1/live/session-surface;" in nginx
    )
    assert "proxy_read_timeout 15s;" in nginx
    assert '~^/api/v1/live/ "private, no-store, max-age=0";' in nginx
    assert "replay-api.sock" not in nginx
    assert "CMD-SHELL" in compose
    assert "http://127.0.0.1:18082/healthz" in compose
    assert "http://127.0.0.1:18082/api/v1/live/healthz" in compose
    assert ":/usr/share/nginx/replay-runtime:ro" in compose


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
