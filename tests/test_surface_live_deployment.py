from __future__ import annotations

import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def test_live_service_is_unix_socket_only_and_has_narrow_write_paths() -> None:
    unit = read("systemd/spx-spark-surface-live.service")
    runner = read("scripts/run-spxw-surface-live-service.sh")

    assert "After=spx-spark-surface-dashboard.service" in unit
    assert "Wants=spx-spark-surface-dashboard.service" in unit
    assert "SPX_SPARK_DISABLE_DOTENV=1" in unit
    assert "EnvironmentFile=" not in unit
    assert "ProtectSystem=strict" in unit
    assert "ProtectHome=read-only" in unit
    assert "PrivateTmp=true" in unit
    assert "NoNewPrivileges=true" in unit
    assert "RestrictAddressFamilies=AF_UNIX" in unit
    assert "PrivateNetwork" not in unit
    assert (
        "ReadWritePaths=/srv/data/spx-spark/data/published/spxw-surface/live"
        in unit
    )
    assert (
        "ReadWritePaths=/srv/data/spx-spark/data/published/spxw-surface/runtime/live"
        in unit
    )
    assert 'runtime/live}' in runner
    assert 'live/policy=live-v2}' in runner
    assert 'live-api.sock}' in runner
    assert '--input-path "$LIVE_INPUT_PATH"' in runner
    assert '--state-root "$LIVE_STATE_ROOT"' in runner
    assert '--unix-socket "$LIVE_SOCKET_PATH"' in runner
    assert "spx_spark.surface_live_session_http" in runner
    assert "mkdir" not in runner


def test_live_installer_prepares_and_validates_private_runtime() -> None:
    installer = read("scripts/install-spxw-surface-live-service.sh")
    main_installer = read("scripts/install-spx-spark-services.sh")

    assert 'mkdir -p "$LIVE_USER_UNIT_DIR"' in installer
    assert 'install -d -m 0700 "$LIVE_STATE_ROOT" "$LIVE_RUNTIME_ROOT"' in installer
    assert 'live/policy=live-v2}' in installer
    assert "SPXW_SURFACE_UID" in installer
    assert "SPXW_SURFACE_GID" in installer
    assert "docker inspect --format '{{.Config.User}}' spxw-surface" in installer
    assert "stat -c '%u:%g'" in installer
    assert "stat -c '%a'" in installer
    assert '"$mode" != "700"' in installer
    assert '"$mode" != "660"' in installer
    assert "--unix-socket \"$LIVE_SOCKET_PATH\"" in installer
    assert "http://localhost/healthz" in installer
    assert "enable spx-spark-surface-live.service" in installer
    assert "restart spx-spark-surface-live.service" in installer
    assert "rm " not in installer
    assert "unlink" not in installer
    assert '"$ROOT/scripts/install-spxw-surface-live-service.sh"' in main_installer
    assert main_installer.index("restart spx-spark-surface-dashboard.service") < (
        main_installer.index('"$ROOT/scripts/install-spxw-surface-live-service.sh" --now')
    )


def test_nginx_proxies_only_read_only_live_surface_and_independent_health() -> None:
    nginx = read("site/spxw-surface/nginx.conf")
    compose = read("site/spxw-surface/compose.yaml")

    assert 'location = /api/v1/live/healthz {' in nginx
    assert 'location = /api/v1/live/session-surface {' in nginx
    assert nginx.count("limit_except GET") >= 8
    assert (
        "proxy_pass http://unix:/usr/share/nginx/replay-runtime/live/live-api.sock:/healthz;"
        in nginx
    )
    assert (
        "proxy_pass http://unix:/usr/share/nginx/replay-runtime/live/live-api.sock:"
        "/api/v1/live/session-surface;" in nginx
    )
    assert "proxy_read_timeout 15s;" in nginx
    assert '~^/api/v1/live/ "private, no-store, max-age=0";' in nginx
    assert "replay-api.sock" in nginx
    assert "CMD-SHELL" in compose
    assert "http://127.0.0.1:18082/healthz" in compose
    assert "http://127.0.0.1:18082/api/v1/live/healthz" in compose
    assert ":/usr/share/nginx/replay-runtime:ro" in compose


def test_live_deployment_shell_entrypoints_parse() -> None:
    for relative in (
        "scripts/run-spxw-surface-live-service.sh",
        "scripts/install-spxw-surface-live-service.sh",
        "scripts/install-spx-spark-services.sh",
    ):
        subprocess.run(
            ["bash", "-n", str(ROOT / relative)],
            check=True,
            capture_output=True,
            text=True,
        )
