from pathlib import Path


ROOT = Path(__file__).parents[1]


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def test_weekend_bulk_compaction_is_bounded_low_priority_and_persistent() -> None:
    service = read("systemd/spx-spark-data-compact-weekend.service")
    timer = read("systemd/spx-spark-data-compact-weekend.timer")

    assert "--limit 512" in service
    assert "--summary-only" in service
    assert "IOSchedulingClass=idle" in service
    assert "CPUQuota=150%" in service
    assert "MemoryMax=4G" in service
    assert "TimeoutStartSec=4h" in service
    assert "OnCalendar=Sat,Sun *-*-* 08:30:00 Asia/Shanghai" in timer
    assert "Persistent=true" in timer


def test_weekly_prune_is_threshold_gated() -> None:
    weekly = read("systemd/spx-spark-maintenance-weekly.timer")
    weekly_script = read("scripts/run-maintenance-weekly.sh")

    assert "OnCalendar=Sun *-*-* 13:00:00 Asia/Shanghai" in weekly
    # Deletion only fires when the dry-run report crosses the prune watermark;
    # below it the weekly pass stays audit-only.
    assert "spx ops maintenance dry-run --json --no-write" in weekly_script
    assert "action_level" in weekly_script
    assert "prune|critical_stop_raw)" in weekly_script
    assert "spx ops maintenance prune --execute" in weekly_script
    assert "spx ops maintenance prune\n" in weekly_script
    # Notification retention is now owned by the unified operational database.
    assert "purge-outbox" not in weekly_script
    assert "spx ops maintenance trim-review-audit" in weekly_script


def test_installer_enables_weekend_bulk_timer() -> None:
    installer = read("scripts/install-spx-spark-services.sh")

    assert "spx-spark-data-compact-weekend.service" in installer
    assert "spx-spark-data-compact-weekend.timer" in installer
    assert "enable --now spx-spark-data-compact-weekend.timer" in installer


def test_rth_daily_acceptance_runs_on_new_york_session_clock() -> None:
    service = read("systemd/spx-spark-rth-daily-acceptance.service")
    timer = read("systemd/spx-spark-rth-daily-acceptance.timer")
    installer = read("scripts/install-spx-spark-services.sh")

    assert ".venv/bin/spx job rth-daily-acceptance --date auto --strict" in service
    assert "OnCalendar=Mon..Fri *-*-* 17:30:00 America/New_York" in timer
    assert "Persistent=true" in timer
    assert "disable --now spx-spark-post-close-review.timer" in installer
    assert "enable --now spx-spark-post-close-review.timer" not in installer
    assert "spx-spark-rth-daily-acceptance.timer" in installer
    assert "enable --now spx-spark-rth-daily-acceptance.timer" in installer
    assert "restart spx-spark-rth-daily-acceptance.timer" in installer


def test_session_finalizer_owns_daily_review_order_on_new_york_clock() -> None:
    service = read("systemd/spx-spark-session-finalize.service")
    timer = read("systemd/spx-spark-session-finalize.timer")
    runner = read("scripts/run-session-finalize.sh")
    installer = read("scripts/install-spx-spark-services.sh")

    assert "OnCalendar=*-*-* 18:00:00 America/New_York" in timer
    assert "Mon..Fri" not in timer  # weekends/holidays are idempotent repair runs
    assert " UTC" not in timer  # named exchange timezone owns EDT/EST conversion
    assert timer.count("OnCalendar=") == 1
    assert "Persistent=true" in timer
    assert "AccuracySec=1min" in timer
    assert "spx-spark-session-finalize.service" in timer
    assert "scripts/run-session-finalize.sh --date auto --json" in service
    assert "TimeoutStartSec=45min" in service
    assert "NoNewPrivileges=true" in service
    assert "ProtectSystem=strict" in service
    assert "ProtectHome=read-only" in service
    assert "ReadWritePaths=/srv/data/spx-spark/data" in service
    assert "ReadWritePaths=-/home/ubuntu/research/finance/daily/spx-options-review" in service
    assert "MemoryMax=" not in service
    assert "/srv/data/spx-spark/rust-core-shadow/frames" not in service

    assert "spx-spark-session-finalize.lock" in runner
    assert "command -v flock" in runner
    assert "flock_command_unavailable" in runner
    assert "flock -n 9" in runner
    assert "python -m spx_spark.session_finalize" in runner
    assert "run-post-close-review.sh" not in runner

    for unit in (
        "spx-spark-session-finalize.service",
        "spx-spark-session-finalize.timer",
    ):
        assert unit in installer
    assert "enable --now spx-spark-session-finalize.timer" in installer
    assert "disable --now spx-spark-post-close-review.timer" in installer


def test_session_finalize_watermarks_stay_in_typed_config_not_systemd() -> None:
    service = read("systemd/spx-spark-session-finalize.service")
    runner = read("scripts/run-session-finalize.sh")
    wiring = service + runner

    assert "30064771072" not in wiring  # 28 GiB action watermark
    assert "25769803776" not in wiring  # 24 GiB warning watermark
    assert "21474836480" not in wiring  # 20 GiB critical/reserve watermark
    assert "--pressure-check" not in wiring


def test_compactors_cannot_bypass_replay_artifact_deletion_gate() -> None:
    hourly = read("systemd/spx-spark-data-compact.service")
    weekend = read("systemd/spx-spark-data-compact-weekend.service")

    assert "Environment=DATA_PLATFORM_RAW_DELETE_ENABLED=false" in hourly
    assert "Environment=DATA_PLATFORM_RAW_DELETE_ENABLED=false" in weekend


def test_main_installer_refuses_non_master_dirty_or_unpushed_deployments() -> None:
    installer = read("scripts/install-spx-spark-services.sh")

    assert "fetch --quiet origin master" in installer
    assert '[[ "$DEPLOY_BRANCH" != "master" ]]' in installer
    assert "status --porcelain=v1" in installer
    assert '[[ "$DEPLOY_HEAD" != "$DEPLOY_ORIGIN_MASTER" ]]' in installer
    assert "uv sync --frozen" in installer
    assert installer.index("systemd unit drift") < installer.index(
        'for unit in "${retired_units[@]}"'
    )


def test_main_installer_stops_each_retired_owner_independently() -> None:
    installer = read("scripts/install-spx-spark-services.sh")

    loop = installer[installer.index('for unit in "${retired_units[@]}"') :]
    assert 'systemctl --user stop "$unit" || true' in loop
    assert 'systemctl --user disable "$unit" || true' in loop
    assert 'disable --now "${retired_units[@]}"' not in installer


def test_main_installer_owns_the_persistent_order_map_timer() -> None:
    timer = read("systemd/spx-spark-order-map.timer")
    installer = read("scripts/install-spx-spark-services.sh")

    assert "Persistent=true" in timer
    assert 'ln -sfn "$ROOT/systemd/spx-spark-order-map.service"' in installer
    assert 'ln -sfn "$ROOT/systemd/spx-spark-order-map.timer"' in installer
    assert "enable --now spx-spark-order-map.timer" in installer
    assert "restart spx-spark-order-map.timer" in installer
    assert 'for tracked_unit in "$ROOT"/systemd/spx-spark-*' in installer
    assert "systemd unit drift" in installer


def test_order_map_status_timer_covers_full_exchange_local_rth() -> None:
    timer = read("systemd/spx-spark-order-map-status.timer")
    installer = read("scripts/install-spx-spark-services.sh")

    assert "OnCalendar=Mon..Fri *-*-* 09:00,15,30,45:00 America/New_York" in timer
    assert "OnCalendar=Mon..Fri *-*-* 10..15:00,15,30,45:00 America/New_York" in timer
    assert "AccuracySec=1s" in timer
    assert "Asia/Shanghai" not in timer
    assert 'ln -sfn "$ROOT/systemd/spx-spark-order-map-status.service"' in installer
    assert 'ln -sfn "$ROOT/systemd/spx-spark-order-map-status.timer"' in installer
    assert "enable --now spx-spark-order-map-status.timer" in installer
    assert "restart spx-spark-order-map-status.timer" in installer


def test_core_cutover_retires_all_legacy_realtime_units() -> None:
    core = read("systemd/spx-core.service")
    installer = read("scripts/install-spx-spark-services.sh")

    assert ".venv/bin/spx core run" in core
    assert "runtime/core-api.sock" in core
    assert "enable --now spx-core.service" in installer
    for unit in (
        "spx-spark-24h.service",
        "spx-spark-es-bar-sampler.service",
        "spx-spark-spx-minute-sampler.service",
        "spx-spark-market-features-hot.service",
        "spx-spark-market-regime-signal.service",
        "spx-spark-intraday-shock-hot.service",
        "spx-spark-surface-dashboard.service",
        "spx-spark-surface-live.service",
        "spx-spark-surface-replay.service",
    ):
        assert unit in installer
        assert not (ROOT / "systemd" / unit).exists()


def test_notification_delivery_uses_the_single_huey_worker() -> None:
    service = read("systemd/spx-worker.service")
    installer = read("scripts/install-spx-spark-services.sh")

    assert "huey_consumer spx_spark.infrastructure.jobs.huey -w 1 -k thread" in service
    assert "alembic upgrade head" in service
    assert "Restart=on-failure" in service
    assert "enable spx-worker.service" in installer
    for unit in (
        "spx-spark-notification-delivery.service",
        "spx-spark-maintenance-daily.service",
        "spx-spark-maintenance-daily.timer",
        "spx-spark-storage-pressure.service",
        "spx-spark-storage-pressure.timer",
        "spx-spark-schwab-reauth-reminder.service",
        "spx-spark-schwab-reauth-reminder.timer",
    ):
        assert unit in installer
        assert not (ROOT / "systemd" / unit).exists()
    assert not (ROOT / "scripts/run-maintenance-daily.sh").exists()
    assert not (ROOT / "scripts/run-schwab-reauth-reminder.sh").exists()


def test_core_publishes_the_surface_projection_for_its_live_api() -> None:
    core = read("src/spx_spark/core_main.py")

    assert "surface_dashboard.run_loop" in core
    assert "published/spxw-surface/snapshot.json" in core
    assert "from spx_spark.web.live_api import create_default_app" in core


def test_compaction_runner_has_a_non_blocking_whole_run_lock() -> None:
    runner = read("scripts/run-data-compact.sh")

    assert "spx-spark-data-compact.lock" in runner
    assert "flock -n 9" in runner
    assert "compaction_already_running" in runner
    assert "spx data compact" in runner
    assert "spx data replay-spool" in runner
    assert "spx data sync-manifests" in runner


def test_schwab_oauth_service_is_loopback_only_and_private_by_default() -> None:
    service = read("systemd/spx-spark-schwab-oauth.service")
    installer = read("scripts/install-schwab-oauth-service.sh")
    env_writer = read("scripts/set-schwab-env.sh")

    assert ".venv/bin/spx schwab oauth serve" in service
    assert "UMask=0077" in service
    assert "NoNewPrivileges=true" in service
    assert "TasksMax=32" in service
    assert "MemoryMax=512M" in service
    assert "LimitCORE=0" in service
    assert "spx schwab oauth status" in installer
    assert "enable --now spx-spark-schwab-oauth.service" in installer
    assert "Unsupported Schwab environment key" in env_writer
    assert "umask 077" in env_writer
    assert "chmod 600" in env_writer


def test_core_service_is_installed_disabled_until_staging_cutover() -> None:
    service = read("systemd/spx-core.service")
    installer = read("scripts/install-spx-spark-services.sh")

    assert ".venv/bin/spx core run" in service
    assert "Restart=on-failure" in service
    assert "PrivateNetwork=true" in service
    assert "RestrictAddressFamilies=AF_UNIX" in service
    assert 'ln -sfn "$ROOT/systemd/spx-core.service"' in installer
    assert "enable spx-core.service" not in installer
    assert "restart spx-core.service" not in installer


def test_schwab_reauth_reminder_is_owned_only_by_huey() -> None:
    installer = read("scripts/install-schwab-oauth-service.sh")

    assert not (ROOT / "systemd/spx-spark-schwab-reauth-reminder.service").exists()
    assert not (ROOT / "systemd/spx-spark-schwab-reauth-reminder.timer").exists()
    assert not (ROOT / "scripts/run-schwab-reauth-reminder.sh").exists()
    assert "spx-spark-schwab-reauth-reminder" not in installer
