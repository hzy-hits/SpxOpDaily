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


def test_retention_audits_run_after_market_and_never_execute_deletion() -> None:
    daily = read("systemd/spx-spark-maintenance-daily.timer")
    weekly = read("systemd/spx-spark-maintenance-weekly.timer")
    weekly_script = read("scripts/run-maintenance-weekly.sh")

    assert "OnCalendar=*-*-* 07:30:00 Asia/Shanghai" in daily
    assert "OnCalendar=Sun *-*-* 13:00:00 Asia/Shanghai" in weekly
    assert "--execute" not in weekly_script
    assert "spx-spark-maintenance prune" in weekly_script
    assert "spx-spark-maintenance prune --json" not in weekly_script


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
