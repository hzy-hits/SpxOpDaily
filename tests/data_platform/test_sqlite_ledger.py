from __future__ import annotations

from collections.abc import Callable
import sqlite3
import stat
from pathlib import Path

import pytest

from spx_spark.data_platform.adapters.sqlite_ledger import SQLiteDecisionLedger
from spx_spark.data_platform.ports import MigrationError


def test_sqlite_uses_alembic_schema_wal_foreign_keys_and_private_files(
    tmp_path: Path,
    migrate_operational_database: Callable[[Path], Path],
) -> None:
    path = migrate_operational_database(tmp_path)
    ledger = SQLiteDecisionLedger(path, busy_timeout_ms=250)

    with ledger.engine.begin() as connection:
        assert connection.exec_driver_sql("PRAGMA journal_mode").scalar_one() == "wal"
        assert connection.exec_driver_sql("PRAGMA foreign_keys").scalar_one() == 1
        assert connection.exec_driver_sql("PRAGMA synchronous").scalar_one() == 1
        assert connection.exec_driver_sql("PRAGMA busy_timeout").scalar_one() == 250

    for candidate in path.parent.glob(f"{path.name}*"):
        assert stat.S_IMODE(candidate.stat().st_mode) == 0o600

    with sqlite3.connect(path) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    assert {
        "alembic_version",
        "sessions",
        "events",
        "decisions",
        "decision_legs",
        "outcomes",
        "provider_incidents",
        "compaction_manifests",
        "notification_events",
        "notification_attempts",
    } == tables


def test_unmigrated_database_is_rejected_without_creating_tables(tmp_path: Path) -> None:
    database = tmp_path / "spx.sqlite"

    with pytest.raises(MigrationError, match="alembic upgrade head"):
        SQLiteDecisionLedger(database)

    with sqlite3.connect(database) as connection:
        assert connection.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table'"
        ).fetchone()[0] == 0
