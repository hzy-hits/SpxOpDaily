from pathlib import Path


ROOT = Path(__file__).parents[1]


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def test_realtime_regime_signal_is_installed_as_an_independent_advisory_worker() -> None:
    service = read("systemd/spx-spark-market-regime-signal.service")
    installer = read("scripts/install-spx-spark-services.sh")

    assert "After=spx-spark-market-features-hot.service" in service
    assert ".venv/bin/spx-spark-market-regime-signal --interval-seconds=5" in service
    assert "Restart=always" in service
    assert "spx-spark-market-regime-signal.service" in installer


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


def test_retention_audits_run_after_market_and_weekly_prune_is_threshold_gated() -> None:
    daily = read("systemd/spx-spark-maintenance-daily.timer")
    weekly = read("systemd/spx-spark-maintenance-weekly.timer")
    weekly_script = read("scripts/run-maintenance-weekly.sh")

    assert "OnCalendar=*-*-* 07:30:00 Asia/Shanghai" in daily
    assert "OnCalendar=Sun *-*-* 13:00:00 Asia/Shanghai" in weekly
    # Deletion only fires when the dry-run report crosses the prune watermark;
    # below it the weekly pass stays audit-only.
    assert "spx-spark-maintenance dry-run --json --no-write" in weekly_script
    assert "action_level" in weekly_script
    assert "prune|critical_stop_raw)" in weekly_script
    assert "spx-spark-maintenance prune --execute" in weekly_script
    assert "spx-spark-maintenance prune\n" in weekly_script
    # Notification retention is now owned by the unified operational database.
    assert "purge-outbox" not in weekly_script
    assert "spx-spark-maintenance trim-review-audit" in weekly_script


def test_installer_enables_weekend_bulk_timer() -> None:
    installer = read("scripts/install-spx-spark-services.sh")

    assert "spx-spark-data-compact-weekend.service" in installer
    assert "spx-spark-data-compact-weekend.timer" in installer
    assert "enable --now spx-spark-data-compact-weekend.timer" in installer


def test_rth_daily_acceptance_runs_on_new_york_session_clock() -> None:
    service = read("systemd/spx-spark-rth-daily-acceptance.service")
    timer = read("systemd/spx-spark-rth-daily-acceptance.timer")
    runner = read("scripts/run-rth-daily-acceptance.sh")
    installer = read("scripts/install-spx-spark-services.sh")

    assert "--date auto --strict" in service
    assert "OnCalendar=Mon..Fri *-*-* 17:30:00 America/New_York" in timer
    assert "Persistent=true" in timer
    assert "spx_spark.application.order_map.rth_daily_acceptance" in runner
    assert "disable --now spx-spark-post-close-review.timer" in installer
    assert "enable --now spx-spark-post-close-review.timer" not in installer
    assert "spx-spark-rth-daily-acceptance.timer" in installer
    assert "enable --now spx-spark-rth-daily-acceptance.timer" in installer
    assert "restart spx-spark-rth-daily-acceptance.timer" in installer


def test_session_finalizer_owns_daily_review_order_on_new_york_clock() -> None:
    service = read("systemd/spx-spark-session-finalize.service")
    timer = read("systemd/spx-spark-session-finalize.timer")
    pressure_service = read("systemd/spx-spark-storage-pressure.service")
    pressure_timer = read("systemd/spx-spark-storage-pressure.timer")
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

    assert "--date auto --json --pressure-check" in pressure_service
    assert "MemoryMax=" not in pressure_service
    assert "/srv/data/spx-spark/rust-core-shadow/frames" not in pressure_service
    assert "OnCalendar=*-*-* *:20:00" in pressure_timer
    assert "Persistent=true" in pressure_timer
    assert "AccuracySec=1min" in pressure_timer
    assert "spx-spark-storage-pressure.service" in pressure_timer

    for unit in (
        "spx-spark-session-finalize.service",
        "spx-spark-session-finalize.timer",
        "spx-spark-storage-pressure.service",
        "spx-spark-storage-pressure.timer",
    ):
        assert unit in installer
    assert "enable --now spx-spark-session-finalize.timer" in installer
    assert "enable --now spx-spark-storage-pressure.timer" in installer
    assert "disable --now spx-spark-post-close-review.timer" in installer


def test_session_finalize_watermarks_stay_in_typed_config_not_systemd() -> None:
    service = read("systemd/spx-spark-session-finalize.service")
    pressure_service = read("systemd/spx-spark-storage-pressure.service")
    runner = read("scripts/run-session-finalize.sh")
    wiring = service + pressure_service + runner

    assert "30064771072" not in wiring  # 28 GiB action watermark
    assert "25769803776" not in wiring  # 24 GiB warning watermark
    assert "21474836480" not in wiring  # 20 GiB critical/reserve watermark
    assert "--pressure-check" in pressure_service


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


def test_market_features_hot_worker_is_a_dedicated_single_owner_service() -> None:
    hot_service = read("systemd/spx-spark-market-features-hot.service")
    shared_service = read("systemd/spx-spark-24h.service")
    runner = read("scripts/run-market-features-hot-worker.sh")
    installer = read("scripts/install-spx-spark-services.sh")

    assert "scripts/run-market-features-hot-worker.sh" in hot_service
    assert "RuntimeDirectory=" not in hot_service
    assert "--lock-path=%t/spx-spark-market-features-hot-worker.lock" in hot_service
    assert "RestartPreventExitStatus" not in hot_service
    assert "RestartSec=10" in hot_service
    assert "KillSignal=SIGTERM" in hot_service
    assert "SPX_SERVICE_ENABLE_MARKET_FEATURES=false" in shared_service
    assert "--exclude-task market_features" in shared_service
    assert 'exec "$ENTRYPOINT" "$@"' in runner
    assert "spx_spark.application.runtime.market_features_hot_worker" in runner
    assert "enable spx-spark-market-features-hot.service" in installer
    assert installer.index("restart spx-spark-24h.service") < installer.index(
        "restart spx-spark-market-features-hot.service"
    )


def test_es_bar_sampler_is_the_canonical_writer_with_safe_deploy_order() -> None:
    sampler_service = read("systemd/spx-spark-es-bar-sampler.service")
    feature_service = read("systemd/spx-spark-market-features-hot.service")
    runner = read("scripts/run-es-bar-sampler.sh")
    installer = read("scripts/install-spx-spark-services.sh")

    assert "scripts/run-es-bar-sampler.sh" in sampler_service
    assert "--mark-starting" in sampler_service
    assert "--interval-seconds=5" in sampler_service
    assert "--lock-path=%t/spx-spark-es-bar-sampler.lock" in sampler_service
    assert "Restart=always" in sampler_service
    assert "RestartSec=2" in sampler_service
    assert "spx-spark-es-bar-sampler.service" in feature_service
    assert 'exec "$ENTRYPOINT" "$@"' in runner
    assert "spx_spark.application.runtime.es_bar_sampler" in runner
    assert "enable spx-spark-es-bar-sampler.service" in installer

    stop_writer = installer.index("stop spx-spark-market-features-hot.service")
    start_sampler = installer.index("restart spx-spark-es-bar-sampler.service")
    start_reader = installer.index("restart spx-spark-market-features-hot.service")
    assert stop_writer < start_sampler < start_reader
    assert "is-active --quiet spx-spark-es-bar-sampler.service" in installer
    assert "--check-ready" in installer


def test_official_spx_sampler_is_independent_and_rth_only() -> None:
    sampler_service = read("systemd/spx-spark-spx-minute-sampler.service")
    feature_service = read("systemd/spx-spark-market-features-hot.service")
    runner = read("scripts/run-spx-minute-sampler.sh")
    installer = read("scripts/install-spx-spark-services.sh")

    assert "scripts/run-spx-minute-sampler.sh" in sampler_service
    assert "spx_spark.application.runtime.spx_minute_sampler" in runner
    assert "--lock-path=%t/spx-spark-spx-minute-sampler.lock" in sampler_service
    assert "spx-spark-spx-minute-sampler.service" in feature_service
    assert "enable spx-spark-spx-minute-sampler.service" in installer


def test_intraday_shock_hot_worker_is_a_dedicated_single_owner_service() -> None:
    hot_service = read("systemd/spx-spark-intraday-shock-hot.service")
    shared_service = read("systemd/spx-spark-24h.service")
    runner = read("scripts/run-intraday-shock-hot-worker.sh")
    installer = read("scripts/install-spx-spark-services.sh")

    assert "scripts/run-intraday-shock-hot-worker.sh" in hot_service
    assert "RuntimeDirectory=" not in hot_service
    assert "--lock-path=%t/spx-spark-intraday-shock-hot-worker.lock" in hot_service
    assert "RestartPreventExitStatus" not in hot_service
    assert "RestartSec=10" in hot_service
    assert "KillSignal=SIGTERM" in hot_service
    assert "SPX_SERVICE_ENABLE_INTRADAY_SHOCK=false" in shared_service
    assert "--exclude-task intraday_shock" in shared_service
    assert 'exec "$ENTRYPOINT" "$@"' in runner
    assert "spx_spark.application.runtime.intraday_shock_hot_worker" in runner
    assert "enable spx-spark-intraday-shock-hot.service" in installer
    assert installer.index("restart spx-spark-24h.service") < installer.index(
        "restart spx-spark-intraday-shock-hot.service"
    )


def test_notification_delivery_uses_the_single_huey_worker() -> None:
    service = read("systemd/spx-worker.service")
    installer = read("scripts/install-spx-spark-services.sh")

    assert "huey_consumer spx_spark.infrastructure.jobs.huey -w 1 -k thread" in service
    assert "alembic upgrade head" in service
    assert "Restart=on-failure" in service
    assert "enable spx-worker.service" in installer


def test_surface_dashboard_worker_publishes_to_an_isolated_read_only_feed() -> None:
    service = read("systemd/spx-spark-surface-dashboard.service")
    runner = read("scripts/run-spxw-surface-dashboard.sh")
    installer = read("scripts/install-spx-spark-services.sh")

    assert "scripts/run-spxw-surface-dashboard.sh --interval-seconds 5" in service
    assert "Restart=always" in service
    assert "SuccessExitStatus=143 SIGTERM" in service
    assert "/published/spxw-surface/snapshot.json" in runner
    assert "--output-path" in runner
    assert "spx_spark.surface_dashboard" in runner
    assert "enable spx-spark-surface-dashboard.service" in installer


def test_compaction_runner_has_a_non_blocking_whole_run_lock() -> None:
    runner = read("scripts/run-data-compact.sh")

    assert "spx-spark-data-compact.lock" in runner
    assert "flock -n 9" in runner
    assert "compaction_already_running" in runner


def test_schwab_oauth_service_is_loopback_only_and_private_by_default() -> None:
    service = read("systemd/spx-spark-schwab-oauth.service")
    runner = read("scripts/run-schwab-oauth.sh")
    installer = read("scripts/install-schwab-oauth-service.sh")
    env_writer = read("scripts/set-schwab-env.sh")

    assert "scripts/run-schwab-oauth.sh serve" in service
    assert "UMask=0077" in service
    assert "NoNewPrivileges=true" in service
    assert "TasksMax=32" in service
    assert "MemoryMax=512M" in service
    assert "LimitCORE=0" in service
    assert "uv run --frozen" in runner
    assert "spx-spark-schwab-oauth status" in installer
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


def test_schwab_reauth_reminder_runs_each_sunday_in_beijing() -> None:
    service = read("systemd/spx-spark-schwab-reauth-reminder.service")
    timer = read("systemd/spx-spark-schwab-reauth-reminder.timer")
    runner = read("scripts/run-schwab-reauth-reminder.sh")
    installer = read("scripts/install-schwab-oauth-service.sh")

    assert "scripts/run-schwab-reauth-reminder.sh" in service
    assert "UMask=0077" in service
    assert "OnCalendar=Sun *-*-* 20:00:00 Asia/Shanghai" in timer
    assert "Persistent=true" in timer
    assert "spx_spark.application.schwab_reauth_reminder" in runner
    assert "enable --now spx-spark-schwab-reauth-reminder.timer" in installer
