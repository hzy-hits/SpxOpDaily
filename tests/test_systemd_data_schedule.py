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
