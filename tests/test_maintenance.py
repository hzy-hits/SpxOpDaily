import os
from datetime import datetime, timedelta, timezone

from spx_spark.config import MaintenanceSettings
from spx_spark.maintenance import build_report


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


def test_maintenance_dry_run_finds_old_raw_file(tmp_path):
    settings = make_settings(tmp_path)
    raw_dir = tmp_path / "data" / "raw" / "provider=schwab" / "date=2026-01-01"
    raw_dir.mkdir(parents=True)
    old_file = raw_dir / "old.parquet"
    old_file.write_bytes(b"12345")

    now = datetime(2026, 7, 4, tzinfo=timezone.utc)
    old_mtime = (now - timedelta(days=20)).timestamp()
    old_file.touch()
    os.utime(old_file, (old_mtime, old_mtime))

    report = build_report(settings, now=now)

    assert len(report.prune_candidates) == 1
    assert report.prune_candidates[0].path.endswith("old.parquet")
    assert "raw older" in (report.prune_candidates[0].reason or "")
