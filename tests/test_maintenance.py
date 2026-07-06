import os
from datetime import datetime, timedelta, timezone

from spx_spark.config import MaintenanceSettings
from spx_spark.maintenance import build_report, execute_prune, is_protected_path


def make_settings(tmp_path) -> MaintenanceSettings:
    return MaintenanceSettings(
        data_root=str(tmp_path / "data"),
        logs_root=str(tmp_path / "logs"),
        output_root=str(tmp_path / "logs"),
        data_budget_gb=80.0,
        raw_retention_days=10,
        alert_window_retention_days=60,
        feature_1s_retention_days=30,
        feature_5s_retention_days=90,
        log_retention_days=14,
        trash_retention_days=7,
        warn_pct=70.0,
        compact_pct=80.0,
        degraded_pct=85.0,
        prune_pct=90.0,
        critical_pct=95.0,
    )


def touch_old(path, *, now: datetime, days: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"12345")
    old_mtime = (now - timedelta(days=days)).timestamp()
    os.utime(path, (old_mtime, old_mtime))


def test_maintenance_dry_run_finds_old_raw_file(tmp_path):
    settings = make_settings(tmp_path)
    raw_dir = tmp_path / "data" / "raw" / "provider=schwab" / "date=2026-01-01"
    old_file = raw_dir / "old.parquet"
    now = datetime(2026, 7, 4, tzinfo=timezone.utc)
    touch_old(old_file, now=now, days=20)

    report = build_report(settings, now=now)

    assert len(report.prune_candidates) == 1
    assert report.prune_candidates[0].path.endswith("old.parquet")
    assert "raw older" in (report.prune_candidates[0].reason or "")


def test_latest_state_is_never_a_prune_candidate(tmp_path):
    settings = make_settings(tmp_path)
    now = datetime(2026, 7, 4, tzinfo=timezone.utc)
    latest = tmp_path / "data" / "latest" / "state.json"
    touch_old(latest, now=now, days=90)

    report = build_report(settings, now=now)

    assert report.prune_candidates == []
    assert is_protected_path(latest, tmp_path / "data")


def test_prune_dry_run_does_not_delete_files(tmp_path):
    settings = make_settings(tmp_path)
    now = datetime(2026, 7, 4, tzinfo=timezone.utc)
    old_file = tmp_path / "data" / "context" / "provider=ibkr" / "date=2026-01-01" / "quotes.jsonl"
    touch_old(old_file, now=now, days=20)

    report = build_report(settings, now=now)
    result = execute_prune(report, settings, execute=False)

    assert old_file.exists()
    assert result.deleted_files == 0
    assert result.executed is False
    assert len(report.prune_candidates) == 1
    assert "context older" in (report.prune_candidates[0].reason or "")


def test_prune_execute_deletes_expired_raw_and_empty_dirs(tmp_path):
    settings = make_settings(tmp_path)
    now = datetime(2026, 7, 4, tzinfo=timezone.utc)
    old_file = tmp_path / "data" / "raw" / "provider=mock" / "date=2026-01-01" / "quotes.jsonl"
    fresh_file = tmp_path / "data" / "raw" / "provider=ibkr" / "date=2026-07-04" / "quotes.jsonl"
    touch_old(old_file, now=now, days=20)
    touch_old(fresh_file, now=now, days=1)

    report = build_report(settings, now=now)
    result = execute_prune(report, settings, execute=True)

    assert result.deleted_files == 1
    assert not old_file.exists()
    assert fresh_file.exists()
    assert result.removed_empty_dirs >= 1


def test_prune_execute_deletes_old_logs(tmp_path):
    settings = make_settings(tmp_path)
    now = datetime(2026, 7, 4, tzinfo=timezone.utc)
    old_log = tmp_path / "logs" / "maintenance-dry-run-old.json"
    touch_old(old_log, now=now, days=20)

    report = build_report(settings, now=now)
    result = execute_prune(report, settings, execute=True)

    assert result.deleted_files == 1
    assert not old_log.exists()
