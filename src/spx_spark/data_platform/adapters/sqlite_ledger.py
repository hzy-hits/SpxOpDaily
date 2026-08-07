"""SQLAlchemy Core adapter for the unified Alembic-managed operational DB."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path

import sqlalchemy as sa
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

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
from spx_spark.infrastructure.notifications import create_database_engine
from spx_spark.infrastructure.operational_db import (
    compaction_manifests,
    decision_legs,
    decisions,
    events,
    outcomes,
    sessions,
)


def _utc_text(value: datetime | None) -> str | None:
    return (
        value.astimezone(timezone.utc).isoformat(timespec="microseconds")
        if value is not None
        else None
    )


def _parse_datetime(value: object) -> datetime | None:
    return datetime.fromisoformat(str(value)).astimezone(timezone.utc) if value else None


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


def _json_safe(value):
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def _json_mapping(value: object) -> dict[str, JsonValue]:
    parsed = json.loads(str(value))
    if not isinstance(parsed, dict):
        raise ValueError("stored metadata is not a JSON object")
    return parsed


def _now_text() -> str:
    return _utc_text(datetime.now(tz=timezone.utc)) or ""


class SQLiteDecisionLedger:
    """Retry-safe operational facts in the single Alembic-managed database."""

    def __init__(
        self,
        path: str | Path,
        *,
        busy_timeout_ms: int = 250,
    ) -> None:
        if busy_timeout_ms < 0:
            raise ValueError("busy_timeout_ms cannot be negative")
        self.path = Path(path)
        self.busy_timeout_ms = busy_timeout_ms
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            descriptor = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
            os.close(descriptor)
        os.chmod(self.path, 0o600)
        self.engine = create_database_engine(
            self.path,
            timeout_seconds=busy_timeout_ms / 1_000,
        )
        if not sa.inspect(self.engine).has_table("decisions"):
            raise MigrationError("operational database is not migrated; run alembic upgrade head")
        with self.engine.begin() as connection:
            connection.exec_driver_sql("PRAGMA journal_mode=WAL")

    def _write(self, callback) -> None:
        try:
            with self.engine.begin() as connection:
                callback(connection)
        except sa.exc.IntegrityError as exc:
            raise LedgerReferenceError(str(exc)) from exc
        self._secure_files()

    def _secure_files(self) -> None:
        for candidate in (self.path, Path(f"{self.path}-wal"), Path(f"{self.path}-shm")):
            if candidate.exists():
                os.chmod(candidate, 0o600)

    @staticmethod
    def _insert_immutable(connection, table, values: Mapping[str, object], *keys) -> bool:
        connection.execute(sqlite_insert(table).values(values).on_conflict_do_nothing())
        predicates = [table.c[name] == value for name, value in keys]
        stored = connection.execute(sa.select(table).where(*predicates)).mappings().one_or_none()
        if stored is None:
            raise LedgerConflictError(f"conflicting immutable {table.name} record")
        if any(stored[name] != value for name, value in values.items() if name != "created_at"):
            raise LedgerConflictError(f"conflicting immutable {table.name} record")
        return stored["created_at"] == values.get("created_at")

    def record_session(self, session: SessionRecord) -> None:
        now = _now_text()
        values = {
            "session_date": session.session_date.isoformat(),
            "market": session.market,
            "status": session.status,
            "opened_at": _utc_text(session.opened_at),
            "closed_at": _utc_text(session.closed_at),
            "data_quality": session.data_quality,
            "metadata_json": _json_text(session.metadata),
            "created_at": now,
            "updated_at": now,
        }
        statement = sqlite_insert(sessions).values(values).on_conflict_do_update(
            index_elements=[sessions.c.session_date],
            set_={name: value for name, value in values.items() if name != "created_at"},
        )
        self._write(lambda connection: connection.execute(statement))

    def record_strategy_version(self, version: StrategyVersionRecord) -> None:
        digest = hashlib.sha256(
            f"{version.strategy_name}\0{version.strategy_version}".encode()
        ).hexdigest()[:32]
        self.record_event(
            EventRecord(
                event_key=f"strategy-version:{digest}",
                event_type="strategy_version",
                session_date=version.activated_at.date(),
                source_at=version.activated_at,
                available_at=version.activated_at,
                data_quality="metadata",
                attributes={
                    "strategy_name": version.strategy_name,
                    "strategy_version": version.strategy_version,
                    "git_commit": version.git_commit,
                    "config_sha256": version.config_sha256,
                    "metadata": dict(version.metadata),
                },
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
            "created_at": _now_text(),
        }
        self._write(
            lambda connection: self._insert_immutable(
                connection, events, values, ("event_key", event.event_key)
            )
        )

    def record_feature_snapshot(self, snapshot: FeatureSnapshotRecord) -> None:
        self.record_event(
            EventRecord(
                event_key=snapshot.snapshot_id,
                event_type="feature_snapshot",
                session_date=snapshot.captured_at.date(),
                source_at=snapshot.captured_at,
                available_at=snapshot.available_at,
                data_quality="snapshot",
                schema_version=snapshot.schema_version,
                attributes={
                    "parent_event_key": snapshot.event_key,
                    "gamma_regime": snapshot.gamma_regime,
                    "payload": dict(snapshot.payload),
                },
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
            "created_at": _now_text(),
        }

    def record_decision(
        self,
        decision: DecisionRecord,
        legs: Sequence[DecisionLegRecord] = (),
    ) -> None:
        normalized = tuple(legs)
        if len({leg.leg_index for leg in normalized}) != len(normalized):
            raise ValueError("decision leg indexes must be unique")
        for leg in normalized:
            if leg.decision_id != decision.decision_id:
                raise ValueError("every leg must reference the decision being recorded")
            if leg.quote_available_at > decision.decision_at:
                raise LookaheadViolationError("decision leg quote was unavailable at decision time")
        values = {
            "decision_id": decision.decision_id,
            "event_key": decision.event_key,
            "feature_snapshot_id": decision.feature_snapshot_id,
            "session_date": None,
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
            "created_at": _now_text(),
        }

        def write(connection) -> None:
            for reference in (decision.event_key, decision.feature_snapshot_id):
                if not reference:
                    continue
                row = connection.execute(
                    sa.select(events.c.available_at, events.c.session_date).where(
                        events.c.event_key == reference
                    )
                ).one_or_none()
                if row is None:
                    raise LedgerReferenceError("decision reference has not been recorded")
                if str(row[0]) > str(values["decision_at"]):
                    raise LookaheadViolationError("decision reference was unavailable at decision time")
                if values["session_date"] is None:
                    values["session_date"] = row[1]
            inserted = self._insert_immutable(
                connection, decisions, values, ("decision_id", decision.decision_id)
            )
            expected = [self._leg_values(leg) for leg in sorted(normalized, key=lambda row: row.leg_index)]
            if inserted:
                for leg_values in expected:
                    self._insert_immutable(
                        connection,
                        decision_legs,
                        leg_values,
                        ("decision_id", leg_values["decision_id"]),
                        ("leg_index", leg_values["leg_index"]),
                    )
            stored = connection.execute(
                sa.select(decision_legs).where(
                    decision_legs.c.decision_id == decision.decision_id
                ).order_by(decision_legs.c.leg_index)
            ).mappings().all()
            if len(stored) != len(expected) or any(
                any(row[name] != value for name, value in values.items() if name != "created_at")
                for row, values in zip(stored, expected, strict=True)
            ):
                raise LedgerConflictError("conflicting immutable decision leg set")

        self._write(write)

    def record_delivery(self, delivery: DeliveryRecord) -> None:
        self.record_event(
            EventRecord(
                event_key=delivery.delivery_id,
                event_type="notification_delivery",
                session_date=delivery.attempted_at.date(),
                source_at=delivery.attempted_at,
                available_at=delivery.attempted_at,
                data_quality=delivery.status,
                attributes=_json_safe(asdict(delivery)),
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
            "created_at": _now_text(),
        }
        self._write(
            lambda connection: self._insert_immutable(
                connection, outcomes, values, ("outcome_id", outcome.outcome_id)
            )
        )

    def record_compaction_manifest(self, manifest: CompactionManifestRecord) -> None:
        values = {
            "manifest_id": make_compaction_manifest_id(
                manifest.source_path, manifest.source_sha256
            ),
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
            "created_at": _now_text(),
        }
        statement = sqlite_insert(compaction_manifests).values(values).on_conflict_do_update(
            index_elements=[
                compaction_manifests.c.source_path,
                compaction_manifests.c.source_sha256,
            ],
            set_={
                name: value
                for name, value in values.items()
                if name not in {"source_path", "source_sha256", "created_at"}
            },
        )
        self._write(lambda connection: connection.execute(statement))

    def get_event(self, event_key: str) -> EventRecord | None:
        with self.engine.begin() as connection:
            row = connection.execute(
                sa.select(events).where(events.c.event_key == event_key)
            ).mappings().one_or_none()
        return self._event_from_row(row) if row else None

    @staticmethod
    def _event_from_row(row: Mapping[str, object]) -> EventRecord:
        return EventRecord(
            event_key=str(row["event_key"]),
            event_type=str(row["event_type"]),
            session_date=date.fromisoformat(str(row["session_date"])),
            source_at=_parse_datetime(row["source_at"]),  # type: ignore[arg-type]
            available_at=_parse_datetime(row["available_at"]),  # type: ignore[arg-type]
            received_at=_parse_datetime(row["received_at"]),
            phase=row["phase"],  # type: ignore[arg-type]
            direction=row["direction"],  # type: ignore[arg-type]
            data_quality=str(row["data_quality"]),
            schema_version=int(row["schema_version"]),
            attributes=_json_mapping(row["attributes_json"]),
        )

    def get_decision(self, decision_id: str) -> DecisionRecord | None:
        with self.engine.begin() as connection:
            row = connection.execute(
                sa.select(decisions).where(decisions.c.decision_id == decision_id)
            ).mappings().one_or_none()
        if row is None:
            return None
        return DecisionRecord(
            decision_id=str(row["decision_id"]),
            event_key=row["event_key"],  # type: ignore[arg-type]
            feature_snapshot_id=(
                str(row["feature_snapshot_id"])
                if row["feature_snapshot_id"] is not None
                else None
            ),
            strategy_name=str(row["strategy_name"]),
            strategy_version=str(row["strategy_version"]),
            decision_at=_parse_datetime(row["decision_at"]),  # type: ignore[arg-type]
            available_at=_parse_datetime(row["available_at"]),  # type: ignore[arg-type]
            status=str(row["status"]),
            action=str(row["action"]),
            side=str(row["side"]),
            reason=row["reason"],  # type: ignore[arg-type]
            gamma_regime=row["gamma_regime"],  # type: ignore[arg-type]
            attributes=_json_mapping(row["attributes_json"]),
        )

    @staticmethod
    def _leg_from_row(row: Mapping[str, object]) -> DecisionLegRecord:
        return DecisionLegRecord(
            decision_id=str(row["decision_id"]),
            leg_index=int(row["leg_index"]),
            instrument_id=str(row["instrument_id"]),
            right=row["right_code"],  # type: ignore[arg-type]
            expiry=date.fromisoformat(str(row["expiry"])) if row["expiry"] else None,
            strike=row["strike"],  # type: ignore[arg-type]
            quantity=row["quantity"],  # type: ignore[arg-type]
            bid=row["bid"],  # type: ignore[arg-type]
            ask=row["ask"],  # type: ignore[arg-type]
            delta=row["delta"],  # type: ignore[arg-type]
            gamma=row["gamma"],  # type: ignore[arg-type]
            theta=row["theta"],  # type: ignore[arg-type]
            vega=row["vega"],  # type: ignore[arg-type]
            quote_source_at=_parse_datetime(row["quote_source_at"]),  # type: ignore[arg-type]
            quote_available_at=_parse_datetime(row["quote_available_at"]),  # type: ignore[arg-type]
            attributes=_json_mapping(row["attributes_json"]),
        )

    def list_decision_legs(self, decision_id: str) -> tuple[DecisionLegRecord, ...]:
        with self.engine.begin() as connection:
            rows = connection.execute(
                sa.select(decision_legs).where(
                    decision_legs.c.decision_id == decision_id
                ).order_by(decision_legs.c.leg_index)
            ).mappings().all()
        return tuple(self._leg_from_row(row) for row in rows)

    def list_deliveries(self, decision_id: str) -> tuple[DeliveryRecord, ...]:
        with self.engine.begin() as connection:
            rows = connection.execute(
                sa.select(events.c.attributes_json).where(
                    events.c.event_type == "notification_delivery"
                ).order_by(events.c.source_at)
            ).scalars().all()
        result = []
        for value in rows:
            payload = _json_mapping(value)
            if payload.get("decision_id") != decision_id:
                continue
            result.append(
                DeliveryRecord(
                    **{
                        **payload,
                        "attempted_at": _parse_datetime(payload["attempted_at"]),
                        "sent_at": _parse_datetime(payload.get("sent_at")),
                    }
                )
            )
        return tuple(result)

    @staticmethod
    def _outcome_from_row(row: Mapping[str, object]) -> OutcomeRecord:
        return OutcomeRecord(
            outcome_id=str(row["outcome_id"]),
            event_key=str(row["event_key"]),
            decision_id=row["decision_id"],  # type: ignore[arg-type]
            horizon_minutes=int(row["horizon_minutes"]),
            status=str(row["status"]),
            target_at=_parse_datetime(row["target_at"]),  # type: ignore[arg-type]
            sampled_at=_parse_datetime(row["sampled_at"]),
            hypothesis_direction=row["hypothesis_direction"],  # type: ignore[arg-type]
            spx_return_bps=row["spx_return_bps"],  # type: ignore[arg-type]
            spx_mfe_bps=row["spx_mfe_bps"],  # type: ignore[arg-type]
            spx_mae_bps=row["spx_mae_bps"],  # type: ignore[arg-type]
            option_return_bps=row["option_return_bps"],  # type: ignore[arg-type]
            option_pnl=row["option_pnl"],  # type: ignore[arg-type]
            attributes=_json_mapping(row["attributes_json"]),
        )

    def list_outcomes(self, decision_id: str) -> tuple[OutcomeRecord, ...]:
        with self.engine.begin() as connection:
            rows = connection.execute(
                sa.select(outcomes).where(outcomes.c.decision_id == decision_id).order_by(
                    outcomes.c.horizon_minutes
                )
            ).mappings().all()
        return tuple(self._outcome_from_row(row) for row in rows)

    def get_compaction_manifest(
        self,
        source_path: str,
        source_sha256: str,
    ) -> CompactionManifestRecord | None:
        with self.engine.begin() as connection:
            row = connection.execute(
                sa.select(compaction_manifests).where(
                    compaction_manifests.c.source_path == source_path,
                    compaction_manifests.c.source_sha256 == source_sha256,
                )
            ).mappings().one_or_none()
        if row is None:
            return None
        return CompactionManifestRecord(
            source_path=str(row["source_path"]),
            source_sha256=str(row["source_sha256"]),
            source_size=int(row["source_size"]),
            source_mtime_ns=int(row["source_mtime_ns"]),
            output_path=row["output_path"],  # type: ignore[arg-type]
            output_sha256=row["output_sha256"],  # type: ignore[arg-type]
            row_count=int(row["row_count"]),
            min_received_at=_parse_datetime(row["min_received_at"]),
            max_received_at=_parse_datetime(row["max_received_at"]),
            schema_version=str(row["schema_version"]),
            writer_version=str(row["writer_version"]),
            completed_at=_parse_datetime(row["completed_at"]),  # type: ignore[arg-type]
            status=str(row["status"]),
            dataset=str(row["dataset"]),
        )


SQLiteLedger = SQLiteDecisionLedger
