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
    # Ledger retention rides the same weekly off-market window.
    assert "spx-spark-maintenance purge-outbox --vacuum" in weekly_script
    assert "spx-spark-maintenance trim-review-audit" in weekly_script


def test_installer_enables_weekend_bulk_timer() -> None:
    installer = read("scripts/install-spx-spark-services.sh")

    assert "spx-spark-data-compact-weekend.service" in installer
    assert "spx-spark-data-compact-weekend.timer" in installer
    assert "enable --now spx-spark-data-compact-weekend.timer" in installer


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


def test_notification_delivery_has_a_persistent_subsecond_worker() -> None:
    service = read("systemd/spx-spark-notification-delivery.service")
    installer = read("scripts/install-spx-spark-services.sh")

    assert "spx_spark.notifier.delivery_worker --poll-seconds 0.5" in service
    assert "Restart=always" in service
    assert "SuccessExitStatus=143 SIGTERM" in service
    assert "enable spx-spark-notification-delivery.service" in installer


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
