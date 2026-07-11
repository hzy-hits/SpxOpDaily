"""SQLite WAL implementation of the operational decision ledger."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, timezone
from pathlib import Path
from typing import TypeVar

from spx_spark.data_platform.contracts import (
    CompactionManifestRecord,
    DecisionLegRecord,
    DecisionRecord,
    DeliveryRecord,
    EventRecord,
    FeatureSnapshotRecord,
    JsonValue,
    OutcomeRecord,
    SessionRecord,
    StrategyVersionRecord,
)
from spx_spark.data_platform.ids import make_compaction_manifest_id
from spx_spark.data_platform.ports import (
    LedgerConflictError,
    LedgerReferenceError,
    LookaheadViolationError,
    MigrationError,
)


_T = TypeVar("_T")
_MIGRATION_RE = re.compile(r"^(?P<version>\d+)_(?P<name>[a-zA-Z0-9_\-]+)\.sql$")


def _utc_text(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _parse_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(str(value)).astimezone(timezone.utc)


def _json_text(value: Mapping[str, JsonValue]) -> str:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("record metadata must be valid JSON") from exc


def _json_mapping(value: object) -> dict[str, JsonValue]:
    parsed = json.loads(str(value))
    if not isinstance(parsed, dict):
        raise ValueError("stored metadata is not a JSON object")
    return parsed


def _now_text() -> str:
    return datetime.now(tz=timezone.utc).isoformat(timespec="microseconds")


class SQLiteDecisionLedger:
    """Retry-safe SQLite ledger using one short transaction per fact/aggregate."""

    def __init__(
        self,
        path: str | Path,
        *,
        migrations_path: str | Path | None = None,
        busy_timeout_ms: int = 250,
    ) -> None:
        if busy_timeout_ms < 0:
            raise ValueError("busy_timeout_ms cannot be negative")
        self.path = Path(path)
        self.migrations_path = (
            Path(migrations_path)
            if migrations_path
            else (Path(__file__).resolve().parents[1] / "migrations")
        )
        self.busy_timeout_ms = busy_timeout_ms
        self._prepare_database_file()
        self._initialize()

    def _prepare_database_file(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        os.close(descriptor)
        os.chmod(self.path, 0o600)

    def _secure_database_files(self) -> None:
        for candidate in (self.path, Path(f"{self.path}-wal"), Path(f"{self.path}-shm")):
            if candidate.exists():
                os.chmod(candidate, 0o600)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=self.busy_timeout_ms / 1000,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute(f"PRAGMA busy_timeout={self.busy_timeout_ms}")
        connection.execute("PRAGMA synchronous=NORMAL")
        return connection

    def _initialize(self) -> None:
        connection = self._connect()
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    checksum TEXT NOT NULL,
                    applied_at TEXT NOT NULL
                )
                """
            )
            connection.commit()
            self._apply_migrations(connection)
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()
            self._secure_database_files()

    def _discover_migrations(self) -> list[tuple[int, str, str, str]]:
        if not self.migrations_path.is_dir():
            raise MigrationError(f"migration directory does not exist: {self.migrations_path}")
        migrations: list[tuple[int, str, str, str]] = []
        versions: set[int] = set()
        for path in sorted(self.migrations_path.glob("*.sql")):
            match = _MIGRATION_RE.fullmatch(path.name)
            if match is None:
                raise MigrationError(f"invalid migration filename: {path.name}")
            version = int(match.group("version"))
            if version in versions:
                raise MigrationError(f"duplicate migration version: {version}")
            versions.add(version)
            sql = path.read_text(encoding="utf-8")
            checksum = hashlib.sha256(sql.encode("utf-8")).hexdigest()
            migrations.append((version, match.group("name"), checksum, sql))
        if not migrations:
            raise MigrationError("at least one migration is required")
        return sorted(migrations)

    def _apply_migrations(self, connection: sqlite3.Connection) -> None:
        migrations = self._discover_migrations()
        available = {version: (name, checksum) for version, name, checksum, _ in migrations}
        applied_rows = connection.execute(
            "SELECT version, name, checksum FROM schema_migrations ORDER BY version"
        ).fetchall()
        for row in applied_rows:
            version = int(row["version"])
            expected = available.get(version)
            if expected is None:
                raise MigrationError(f"database contains unknown migration version {version}")
            if expected != (str(row["name"]), str(row["checksum"])):
                raise MigrationError(f"migration {version} checksum or name changed")

        applied = {int(row["version"]) for row in applied_rows}
        highest_applied = max(applied, default=0)
        retroactive = [
            version for version in available if version < highest_applied and version not in applied
        ]
        if retroactive:
            raise MigrationError(
                f"cannot apply retroactive migration version {min(retroactive)} after {highest_applied}"
            )
        for version, name, checksum, sql in migrations:
            if version in applied:
                continue
            connection.execute("BEGIN IMMEDIATE")
            try:
                self._execute_script_statements(connection, sql)
                connection.execute(
                    "INSERT INTO schema_migrations(version, name, checksum, applied_at) "
                    "VALUES (?, ?, ?, ?)",
                    (version, name, checksum, _now_text()),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise

    @staticmethod
    def _execute_script_statements(connection: sqlite3.Connection, sql: str) -> None:
        pending = ""
        for line in sql.splitlines(keepends=True):
            pending += line
            if sqlite3.complete_statement(pending):
                statement = pending.strip()
                pending = ""
                if statement:
                    connection.execute(statement)
        if pending.strip():
            raise MigrationError("migration ends with an incomplete SQL statement")

    def _write(self, callback: Callable[[sqlite3.Connection], _T]) -> _T:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            result = callback(connection)
            connection.commit()
            return result
        except sqlite3.IntegrityError as exc:
            connection.rollback()
            raise LedgerReferenceError(str(exc)) from exc
        except Exception:
            if connection.in_transaction:
                connection.rollback()
            raise
        finally:
            connection.close()
            self._secure_database_files()

    def _read(self, callback: Callable[[sqlite3.Connection], _T]) -> _T:
        connection = self._connect()
        try:
            return callback(connection)
        finally:
            connection.close()
            self._secure_database_files()

    @staticmethod
    def _insert_immutable(
        connection: sqlite3.Connection,
        *,
        table: str,
        key_where: str,
        key_values: tuple[object, ...],
        values: Mapping[str, object],
    ) -> bool:
        columns = tuple(values)
        placeholders = ", ".join("?" for _ in columns)
        cursor = connection.execute(
            f"INSERT OR IGNORE INTO {table} ({', '.join(columns)}, created_at) "
            f"VALUES ({placeholders}, ?)",
            (*[values[column] for column in columns], _now_text()),
        )
        if cursor.rowcount == 1:
            return True
        existing = connection.execute(
            f"SELECT {', '.join(columns)} FROM {table} WHERE {key_where}",
            key_values,
        ).fetchone()
        if existing is None or any(existing[column] != values[column] for column in columns):
            raise LedgerConflictError(f"conflicting immutable {table} record")
        return False

    def record_session(self, session: SessionRecord) -> None:
        values = (
            session.session_date.isoformat(),
            session.market,
            session.status,
            _utc_text(session.opened_at),
            _utc_text(session.closed_at),
            session.data_quality,
            _json_text(session.metadata),
            _now_text(),
            _now_text(),
        )

        def write(connection: sqlite3.Connection) -> None:
            connection.execute(
                """
                INSERT INTO sessions(
                    session_date, market, status, opened_at, closed_at,
                    data_quality, metadata_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_date) DO UPDATE SET
                    market=excluded.market,
                    status=excluded.status,
                    opened_at=excluded.opened_at,
                    closed_at=excluded.closed_at,
                    data_quality=excluded.data_quality,
                    metadata_json=excluded.metadata_json,
                    updated_at=excluded.updated_at
                """,
                values,
            )

        self._write(write)

    def record_strategy_version(self, version: StrategyVersionRecord) -> None:
        values = {
            "strategy_name": version.strategy_name,
            "strategy_version": version.strategy_version,
            "activated_at": _utc_text(version.activated_at),
            "git_commit": version.git_commit,
            "config_sha256": version.config_sha256,
            "metadata_json": _json_text(version.metadata),
        }
        self._write(
            lambda connection: self._insert_immutable(
                connection,
                table="strategy_versions",
                key_where="strategy_name=? AND strategy_version=?",
                key_values=(version.strategy_name, version.strategy_version),
                values=values,
            )
        )

    def record_event(self, event: EventRecord) -> None:
        values = {
            "event_key": event.event_key,
            "event_type": event.event_type,
            "session_date": event.session_date.isoformat(),
            "source_at": _utc_text(event.source_at),
            "available_at": _utc_text(event.available_at),
            "received_at": _utc_text(event.received_at),
            "phase": event.phase,
            "direction": event.direction,
            "data_quality": event.data_quality,
            "schema_version": event.schema_version,
            "attributes_json": _json_text(event.attributes),
        }
        self._write(
            lambda connection: self._insert_immutable(
                connection,
                table="events",
                key_where="event_key=?",
                key_values=(event.event_key,),
                values=values,
            )
        )

    def record_feature_snapshot(self, snapshot: FeatureSnapshotRecord) -> None:
        values = {
            "snapshot_id": snapshot.snapshot_id,
            "event_key": snapshot.event_key,
            "captured_at": _utc_text(snapshot.captured_at),
            "available_at": _utc_text(snapshot.available_at),
            "gamma_regime": snapshot.gamma_regime,
            "schema_version": snapshot.schema_version,
            "payload_json": _json_text(snapshot.payload),
        }
        self._write(
            lambda connection: self._insert_immutable(
                connection,
                table="feature_snapshots",
                key_where="snapshot_id=?",
                key_values=(snapshot.snapshot_id,),
                values=values,
            )
        )

    @staticmethod
    def _leg_values(leg: DecisionLegRecord) -> dict[str, object]:
        return {
            "decision_id": leg.decision_id,
            "leg_index": leg.leg_index,
            "instrument_id": leg.instrument_id,
            "right_code": leg.right,
            "expiry": leg.expiry.isoformat() if leg.expiry else None,
            "strike": leg.strike,
            "quantity": leg.quantity,
            "bid": leg.bid,
            "ask": leg.ask,
            "delta": leg.delta,
            "gamma": leg.gamma,
            "theta": leg.theta,
            "vega": leg.vega,
            "quote_source_at": _utc_text(leg.quote_source_at),
            "quote_available_at": _utc_text(leg.quote_available_at),
            "attributes_json": _json_text(leg.attributes),
        }

    def record_decision(
        self,
        decision: DecisionRecord,
        legs: Sequence[DecisionLegRecord] = (),
    ) -> None:
        normalized_legs = tuple(legs)
        if len({leg.leg_index for leg in normalized_legs}) != len(normalized_legs):
            raise ValueError("decision leg indexes must be unique")
        for leg in normalized_legs:
            if leg.decision_id != decision.decision_id:
                raise ValueError("every leg must reference the decision being recorded")
            if leg.quote_available_at > decision.decision_at:
                raise LookaheadViolationError("decision leg quote was unavailable at decision time")

        decision_values = {
            "decision_id": decision.decision_id,
            "event_key": decision.event_key,
            "feature_snapshot_id": decision.feature_snapshot_id,
            "strategy_name": decision.strategy_name,
            "strategy_version": decision.strategy_version,
            "decision_at": _utc_text(decision.decision_at),
            "available_at": _utc_text(decision.available_at),
            "status": decision.status,
            "action": decision.action,
            "side": decision.side,
            "reason": decision.reason,
            "gamma_regime": decision.gamma_regime,
            "attributes_json": _json_text(decision.attributes),
        }

        def write(connection: sqlite3.Connection) -> None:
            if decision.event_key:
                event = connection.execute(
                    "SELECT available_at FROM events WHERE event_key=?", (decision.event_key,)
                ).fetchone()
                if event is None:
                    raise LedgerReferenceError("decision event has not been recorded")
                if str(event["available_at"]) > str(decision_values["decision_at"]):
                    raise LookaheadViolationError("event was unavailable at decision time")
            if decision.feature_snapshot_id:
                snapshot = connection.execute(
                    "SELECT available_at FROM feature_snapshots WHERE snapshot_id=?",
                    (decision.feature_snapshot_id,),
                ).fetchone()
                if snapshot is None:
                    raise LedgerReferenceError("decision feature snapshot has not been recorded")
                if str(snapshot["available_at"]) > str(decision_values["decision_at"]):
                    raise LookaheadViolationError(
                        "feature snapshot was unavailable at decision time"
                    )

            inserted = self._insert_immutable(
                connection,
                table="decisions",
                key_where="decision_id=?",
                key_values=(decision.decision_id,),
                values=decision_values,
            )
            if not inserted:
                existing_rows = connection.execute(
                    "SELECT * FROM decision_legs WHERE decision_id=? ORDER BY leg_index",
                    (decision.decision_id,),
                ).fetchall()
                expected_values = [
                    self._leg_values(leg)
                    for leg in sorted(normalized_legs, key=lambda x: x.leg_index)
                ]
                if len(existing_rows) != len(expected_values):
                    raise LedgerConflictError("conflicting immutable decision leg set")
                for row, expected in zip(existing_rows, expected_values, strict=True):
                    if any(row[column] != value for column, value in expected.items()):
                        raise LedgerConflictError("conflicting immutable decision leg")
                return

            for leg in sorted(normalized_legs, key=lambda item: item.leg_index):
                self._insert_immutable(
                    connection,
                    table="decision_legs",
                    key_where="decision_id=? AND leg_index=?",
                    key_values=(leg.decision_id, leg.leg_index),
                    values=self._leg_values(leg),
                )

        self._write(write)

    def record_delivery(self, delivery: DeliveryRecord) -> None:
        values = {
            "delivery_id": delivery.delivery_id,
            "decision_id": delivery.decision_id,
            "channel": delivery.channel,
            "provider": delivery.provider,
            "status": delivery.status,
            "attempted_at": _utc_text(delivery.attempted_at),
            "sent_at": _utc_text(delivery.sent_at),
            "veto_reason": delivery.veto_reason,
            "error_code": delivery.error_code,
            "message_fingerprint": delivery.message_fingerprint,
            "attributes_json": _json_text(delivery.attributes),
        }
        self._write(
            lambda connection: self._insert_immutable(
                connection,
                table="alert_deliveries",
                key_where="delivery_id=?",
                key_values=(delivery.delivery_id,),
                values=values,
            )
        )

    def record_outcome(self, outcome: OutcomeRecord) -> None:
        values = {
            "outcome_id": outcome.outcome_id,
            "event_key": outcome.event_key,
            "decision_id": outcome.decision_id,
            "horizon_minutes": outcome.horizon_minutes,
            "status": outcome.status,
            "target_at": _utc_text(outcome.target_at),
            "sampled_at": _utc_text(outcome.sampled_at),
            "hypothesis_direction": outcome.hypothesis_direction,
            "spx_return_bps": outcome.spx_return_bps,
            "spx_mfe_bps": outcome.spx_mfe_bps,
            "spx_mae_bps": outcome.spx_mae_bps,
            "option_return_bps": outcome.option_return_bps,
            "option_pnl": outcome.option_pnl,
            "attributes_json": _json_text(outcome.attributes),
        }
        self._write(
            lambda connection: self._insert_immutable(
                connection,
                table="outcomes",
                key_where="outcome_id=?",
                key_values=(outcome.outcome_id,),
                values=values,
            )
        )

    def record_compaction_manifest(self, manifest: CompactionManifestRecord) -> None:
        manifest_id = make_compaction_manifest_id(manifest.source_path, manifest.source_sha256)
        values = {
            "manifest_id": manifest_id,
            "source_path": manifest.source_path,
            "source_sha256": manifest.source_sha256,
            "source_size": manifest.source_size,
            "source_mtime_ns": manifest.source_mtime_ns,
            "output_path": manifest.output_path,
            "output_sha256": manifest.output_sha256,
            "row_count": manifest.row_count,
            "min_received_at": _utc_text(manifest.min_received_at),
            "max_received_at": _utc_text(manifest.max_received_at),
            "schema_version": manifest.schema_version,
            "writer_version": manifest.writer_version,
            "dataset": manifest.dataset,
            "completed_at": _utc_text(manifest.completed_at),
            "status": manifest.status,
        }

        def write(connection: sqlite3.Connection) -> None:
            columns = tuple(values)
            placeholders = ", ".join("?" for _ in columns)
            mutable = tuple(
                column for column in columns if column not in {"source_path", "source_sha256"}
            )
            updates = ", ".join(f"{column}=excluded.{column}" for column in mutable)
            connection.execute(
                f"INSERT INTO compaction_manifests ({', '.join(columns)}, created_at) "
                f"VALUES ({placeholders}, ?) "
                f"ON CONFLICT(source_path, source_sha256) DO UPDATE SET {updates}",
                (*[values[column] for column in columns], _now_text()),
            )

        self._write(write)

    def get_event(self, event_key: str) -> EventRecord | None:
        def read(connection: sqlite3.Connection) -> EventRecord | None:
            row = connection.execute(
                "SELECT * FROM events WHERE event_key=?", (event_key,)
            ).fetchone()
            if row is None:
                return None
            return EventRecord(
                event_key=row["event_key"],
                event_type=row["event_type"],
                session_date=date.fromisoformat(row["session_date"]),
                source_at=_parse_datetime(row["source_at"]),  # type: ignore[arg-type]
                available_at=_parse_datetime(row["available_at"]),  # type: ignore[arg-type]
                received_at=_parse_datetime(row["received_at"]),
                phase=row["phase"],
                direction=row["direction"],
                data_quality=row["data_quality"],
                schema_version=row["schema_version"],
                attributes=_json_mapping(row["attributes_json"]),
            )

        return self._read(read)

    def get_decision(self, decision_id: str) -> DecisionRecord | None:
        def read(connection: sqlite3.Connection) -> DecisionRecord | None:
            row = connection.execute(
                "SELECT * FROM decisions WHERE decision_id=?", (decision_id,)
            ).fetchone()
            return self._decision_from_row(row) if row is not None else None

        return self._read(read)

    @staticmethod
    def _decision_from_row(row: sqlite3.Row) -> DecisionRecord:
        return DecisionRecord(
            decision_id=row["decision_id"],
            event_key=row["event_key"],
            feature_snapshot_id=row["feature_snapshot_id"],
            strategy_name=row["strategy_name"],
            strategy_version=row["strategy_version"],
            decision_at=_parse_datetime(row["decision_at"]),  # type: ignore[arg-type]
            available_at=_parse_datetime(row["available_at"]),  # type: ignore[arg-type]
            status=row["status"],
            action=row["action"],
            side=row["side"],
            reason=row["reason"],
            gamma_regime=row["gamma_regime"],
            attributes=_json_mapping(row["attributes_json"]),
        )

    @staticmethod
    def _leg_from_row(row: sqlite3.Row) -> DecisionLegRecord:
        return DecisionLegRecord(
            decision_id=row["decision_id"],
            leg_index=row["leg_index"],
            instrument_id=row["instrument_id"],
            right=row["right_code"],
            expiry=date.fromisoformat(row["expiry"]) if row["expiry"] else None,
            strike=row["strike"],
            quantity=row["quantity"],
            bid=row["bid"],
            ask=row["ask"],
            delta=row["delta"],
            gamma=row["gamma"],
            theta=row["theta"],
            vega=row["vega"],
            quote_source_at=_parse_datetime(row["quote_source_at"]),  # type: ignore[arg-type]
            quote_available_at=_parse_datetime(row["quote_available_at"]),  # type: ignore[arg-type]
            attributes=_json_mapping(row["attributes_json"]),
        )

    def list_decision_legs(self, decision_id: str) -> tuple[DecisionLegRecord, ...]:
        return self._read(
            lambda connection: tuple(
                self._leg_from_row(row)
                for row in connection.execute(
                    "SELECT * FROM decision_legs WHERE decision_id=? ORDER BY leg_index",
                    (decision_id,),
                ).fetchall()
            )
        )

    @staticmethod
    def _delivery_from_row(row: sqlite3.Row) -> DeliveryRecord:
        return DeliveryRecord(
            delivery_id=row["delivery_id"],
            decision_id=row["decision_id"],
            channel=row["channel"],
            provider=row["provider"],
            status=row["status"],
            attempted_at=_parse_datetime(row["attempted_at"]),  # type: ignore[arg-type]
            sent_at=_parse_datetime(row["sent_at"]),
            veto_reason=row["veto_reason"],
            error_code=row["error_code"],
            message_fingerprint=row["message_fingerprint"],
            attributes=_json_mapping(row["attributes_json"]),
        )

    def list_deliveries(self, decision_id: str) -> tuple[DeliveryRecord, ...]:
        return self._read(
            lambda connection: tuple(
                self._delivery_from_row(row)
                for row in connection.execute(
                    "SELECT * FROM alert_deliveries WHERE decision_id=? ORDER BY attempted_at",
                    (decision_id,),
                ).fetchall()
            )
        )

    @staticmethod
    def _outcome_from_row(row: sqlite3.Row) -> OutcomeRecord:
        return OutcomeRecord(
            outcome_id=row["outcome_id"],
            event_key=row["event_key"],
            decision_id=row["decision_id"],
            horizon_minutes=row["horizon_minutes"],
            status=row["status"],
            target_at=_parse_datetime(row["target_at"]),  # type: ignore[arg-type]
            sampled_at=_parse_datetime(row["sampled_at"]),
            hypothesis_direction=row["hypothesis_direction"],
            spx_return_bps=row["spx_return_bps"],
            spx_mfe_bps=row["spx_mfe_bps"],
            spx_mae_bps=row["spx_mae_bps"],
            option_return_bps=row["option_return_bps"],
            option_pnl=row["option_pnl"],
            attributes=_json_mapping(row["attributes_json"]),
        )

    def list_outcomes(self, decision_id: str) -> tuple[OutcomeRecord, ...]:
        return self._read(
            lambda connection: tuple(
                self._outcome_from_row(row)
                for row in connection.execute(
                    "SELECT * FROM outcomes WHERE decision_id=? ORDER BY horizon_minutes",
                    (decision_id,),
                ).fetchall()
            )
        )

    @staticmethod
    def _manifest_from_row(row: sqlite3.Row) -> CompactionManifestRecord:
        return CompactionManifestRecord(
            source_path=row["source_path"],
            source_sha256=row["source_sha256"],
            source_size=row["source_size"],
            source_mtime_ns=row["source_mtime_ns"],
            output_path=row["output_path"],
            output_sha256=row["output_sha256"],
            row_count=row["row_count"],
            min_received_at=_parse_datetime(row["min_received_at"]),
            max_received_at=_parse_datetime(row["max_received_at"]),
            schema_version=row["schema_version"],
            writer_version=row["writer_version"],
            completed_at=_parse_datetime(row["completed_at"]),  # type: ignore[arg-type]
            status=row["status"],
            dataset=row["dataset"],
        )

    def get_compaction_manifest(
        self,
        source_path: str,
        source_sha256: str,
    ) -> CompactionManifestRecord | None:
        def read(connection: sqlite3.Connection) -> CompactionManifestRecord | None:
            row = connection.execute(
                "SELECT * FROM compaction_manifests WHERE source_path=? AND source_sha256=?",
                (source_path, source_sha256),
            ).fetchone()
            return self._manifest_from_row(row) if row is not None else None

        return self._read(read)


SQLiteLedger = SQLiteDecisionLedger
