from __future__ import annotations

import shutil
import sqlite3
import stat
from pathlib import Path

import pytest

from spx_spark.data_platform.adapters.sqlite_ledger import SQLiteDecisionLedger
from spx_spark.data_platform.ports import MigrationError


def test_sqlite_uses_wal_foreign_keys_and_private_files(tmp_path: Path) -> None:
    path = tmp_path / "private" / "ledger.sqlite3"
    ledger = SQLiteDecisionLedger(path)

    connection = ledger._connect()
    try:
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert connection.execute("PRAGMA synchronous").fetchone()[0] == 1
    finally:
        connection.close()

    for candidate in path.parent.glob(f"{path.name}*"):
        assert stat.S_IMODE(candidate.stat().st_mode) == 0o600

    connection = sqlite3.connect(path)
    try:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert {
            "schema_migrations",
            "sessions",
            "strategy_versions",
            "events",
            "feature_snapshots",
            "decisions",
            "decision_legs",
            "alert_deliveries",
            "outcomes",
            "compaction_manifests",
        } <= tables
        migration = connection.execute("SELECT version, checksum FROM schema_migrations").fetchone()
        assert migration[0] == 1
        assert len(migration[1]) == 64
    finally:
        connection.close()


def test_applied_migration_checksum_is_forward_only(tmp_path: Path) -> None:
    source_migrations = (
        Path(__file__).resolve().parents[2] / "src" / "spx_spark" / "data_platform" / "migrations"
    )
    migrations = tmp_path / "migrations"
    shutil.copytree(source_migrations, migrations)
    database = tmp_path / "ledger.sqlite3"
    SQLiteDecisionLedger(database, migrations_path=migrations)

    migration = migrations / "0001_initial.sql"
    migration.write_text(migration.read_text(encoding="utf-8") + "\n-- changed\n", encoding="utf-8")

    with pytest.raises(MigrationError, match="checksum"):
        SQLiteDecisionLedger(database, migrations_path=migrations)


def test_unknown_applied_migration_is_rejected(tmp_path: Path) -> None:
    database = tmp_path / "ledger.sqlite3"
    SQLiteDecisionLedger(database)
    connection = sqlite3.connect(database)
    try:
        connection.execute(
            "INSERT INTO schema_migrations(version, name, checksum, applied_at) "
            "VALUES (999, 'future', ?, '2026-07-10T00:00:00+00:00')",
            ("f" * 64,),
        )
        connection.commit()
    finally:
        connection.close()

    with pytest.raises(MigrationError, match="unknown"):
        SQLiteDecisionLedger(database)


def test_retroactive_migration_is_rejected(tmp_path: Path) -> None:
    source_migrations = (
        Path(__file__).resolve().parents[2] / "src" / "spx_spark" / "data_platform" / "migrations"
    )
    migrations = tmp_path / "migrations"
    shutil.copytree(source_migrations, migrations)
    database = tmp_path / "ledger.sqlite3"
    SQLiteDecisionLedger(database, migrations_path=migrations)
    (migrations / "0000_retroactive.sql").write_text(
        "CREATE TABLE should_not_exist(value TEXT);\n",
        encoding="utf-8",
    )

    with pytest.raises(MigrationError, match="retroactive"):
        SQLiteDecisionLedger(database, migrations_path=migrations)
