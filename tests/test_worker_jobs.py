from __future__ import annotations

import importlib
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType

import pytest

from spx_spark.app_settings import get_settings


@pytest.fixture(autouse=True)
def reset_worker_import_state() -> None:
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()
    sys.modules.pop("spx_spark.infrastructure.jobs", None)


def load_jobs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    monkeypatch.setenv("SPX_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    sys.modules.pop("spx_spark.infrastructure.jobs", None)
    return importlib.import_module("spx_spark.infrastructure.jobs")


def test_jobs_use_one_sqlite_queue_and_delegate_to_existing_entries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    jobs = load_jobs(tmp_path, monkeypatch)
    calls: list[tuple[str, object]] = []

    monkeypatch.setattr(
        "spx_spark.maintenance.run",
        lambda argv: calls.append(("maintenance", argv)) or 0,
    )
    monkeypatch.setattr(
        "spx_spark.session_finalize.run",
        lambda argv: calls.append(("storage", argv)) or 0,
    )
    monkeypatch.setattr(
        "spx_spark.application.schwab_reauth_reminder.run",
        lambda: calls.append(("reauth", None)) or 0,
    )

    jobs.maintenance_daily.call_local()
    jobs.storage_pressure.call_local()
    jobs.schwab_reauth_reminder.call_local()

    assert jobs.huey.storage.filename == str(tmp_path / "huey.sqlite")
    assert calls == [
        ("maintenance", ["dry-run"]),
        ("storage", ["--date", "auto", "--json", "--pressure-check"]),
        ("reauth", None),
    ]


def test_worker_schedules_match_the_documented_utc_crontabs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    jobs = load_jobs(tmp_path, monkeypatch)
    utc = timezone.utc

    assert jobs.maintenance_daily.task_class().validate_datetime(
        datetime(2026, 8, 7, 23, 30, tzinfo=utc)
    )
    assert jobs.storage_pressure.task_class().validate_datetime(
        datetime(2026, 8, 7, 11, 20, tzinfo=utc)
    )
    assert jobs.schwab_reauth_reminder.task_class().validate_datetime(
        datetime(2026, 8, 9, 12, 0, tzinfo=utc)
    )
    assert not jobs.schwab_reauth_reminder.task_class().validate_datetime(
        datetime(2026, 8, 10, 12, 0, tzinfo=utc)
    )


def test_nonzero_existing_entry_fails_the_huey_task(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    jobs = load_jobs(tmp_path, monkeypatch)
    monkeypatch.setattr("spx_spark.maintenance.run", lambda argv: 1)

    with pytest.raises(RuntimeError, match="maintenance daily failed"):
        jobs.maintenance_daily.call_local()


def test_initial_migration_builds_only_two_business_tables_and_checks_status(
    tmp_path: Path,
) -> None:
    environment = os.environ | {"SPX_DATA_ROOT": str(tmp_path)}
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        check=False,
        capture_output=True,
        text=True,
        env=environment,
    )
    assert result.returncode == 0, result.stderr

    database = tmp_path / "spx.sqlite"
    with sqlite3.connect(database) as connection:
        tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert tables == {
            "alembic_version",
            "notification_attempts",
            "notification_events",
        }
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO notification_events "
                "(idempotency_key, channel, payload_json, status) VALUES (?, ?, ?, ?)",
                ("bad", "test", "{}", "typo"),
            )
