"""Fail-open operational telemetry with a replayable local spool."""

from __future__ import annotations

import fcntl
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any
from uuid import uuid4

from spx_spark.data_platform.adapters.sqlite_ledger import SQLiteDecisionLedger
from spx_spark.data_platform.contracts import (
    DecisionLegRecord,
    DecisionRecord,
    DeliveryRecord,
    EventRecord,
    FeatureSnapshotRecord,
    OutcomeRecord,
    StrategyVersionRecord,
)
from spx_spark.data_platform.ports import DecisionLedger
from spx_spark.data_platform.settings import DataPlatformSettings


SPOOL_SCHEMA_VERSION = 1
DEFAULT_SPOOL_MAX_BYTES = 67_108_864


class FallbackSpoolCapacityError(RuntimeError):
    """Raised before a fallback append could grow the spool without bound."""


@dataclass(frozen=True)
class TelemetryWriteResult:
    status: str
    operation: str
    error: str | None = None


@dataclass(frozen=True)
class SpoolReplayResult:
    replayed: int
    retained: int
    invalid: int


class FallbackSpool:
    """Owner-only append journal used only when the SQLite ledger is unavailable."""

    def __init__(
        self,
        path: str | Path,
        *,
        max_bytes: int = DEFAULT_SPOOL_MAX_BYTES,
    ) -> None:
        if max_bytes <= 0:
            raise ValueError("fallback spool maximum must be positive")
        self.path = Path(path)
        self.lock_path = self.path.with_name(f"{self.path.name}.lock")
        self.max_bytes = max_bytes

    def append(self, operation: str, payload: Mapping[str, object]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        encoded = (
            json.dumps(
                {
                    "schema_version": SPOOL_SCHEMA_VERSION,
                    "operation": operation,
                    "payload": payload,
                },
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        ).encode("utf-8")
        with self._lock():
            current_size = self.path.stat().st_size if self.path.exists() else 0
            if current_size + len(encoded) > self.max_bytes:
                raise FallbackSpoolCapacityError(
                    f"fallback spool capacity exceeded: {current_size + len(encoded)}"
                )
            descriptor = os.open(self.path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
            try:
                os.fchmod(descriptor, 0o600)
                _write_all(descriptor, encoded)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)

    def replay(self, ledger: DecisionLedger) -> SpoolReplayResult:
        if not self.path.exists():
            return SpoolReplayResult(replayed=0, retained=0, invalid=0)
        replayed = 0
        invalid = 0
        retained: list[str] = []
        with self._lock():
            try:
                lines = self.path.read_text(encoding="utf-8").splitlines()
            except OSError:
                return SpoolReplayResult(replayed=0, retained=0, invalid=1)
            for line in lines:
                if not line.strip():
                    continue
                try:
                    envelope = json.loads(line)
                    if (
                        not isinstance(envelope, dict)
                        or envelope.get("schema_version") != SPOOL_SCHEMA_VERSION
                        or not isinstance(envelope.get("operation"), str)
                        or not isinstance(envelope.get("payload"), dict)
                    ):
                        raise ValueError("invalid fallback envelope")
                    _apply_operation(
                        ledger,
                        str(envelope["operation"]),
                        envelope["payload"],
                    )
                    replayed += 1
                except (TypeError, ValueError, KeyError, RuntimeError):
                    retained.append(line)
                    invalid += 1
            self._replace_lines(retained)
        return SpoolReplayResult(
            replayed=replayed,
            retained=len(retained),
            invalid=invalid,
        )

    def _replace_lines(self, lines: Sequence[str]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_name(f".{self.path.name}.{uuid4().hex}.tmp")
        try:
            with temporary.open("x", encoding="utf-8") as handle:
                for line in lines:
                    handle.write(line)
                    handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temporary, 0o600)
            os.replace(temporary, self.path)
        finally:
            temporary.unlink(missing_ok=True)

    def _lock(self):  # type: ignore[no-untyped-def]
        return _FileLock(self.lock_path)


class _FileLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.handle = None

    def __enter__(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a", encoding="utf-8")
        os.chmod(self.path, 0o600)
        fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX)

    def __exit__(self, *_: object) -> None:
        assert self.handle is not None
        fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        self.handle.close()


class OperationalTelemetry:
    """Best-effort facade that never raises into a realtime caller."""

    def __init__(self, ledger: DecisionLedger, spool: FallbackSpool) -> None:
        self.ledger = ledger
        self.spool = spool

    def record_decision_bundle(
        self,
        *,
        event: EventRecord,
        decision: DecisionRecord,
        strategy_version: StrategyVersionRecord | None = None,
        feature_snapshot: FeatureSnapshotRecord | None = None,
        legs: Sequence[DecisionLegRecord] = (),
    ) -> TelemetryWriteResult:
        payload = {
            "strategy_version": _encode_record(strategy_version),
            "event": _encode_record(event),
            "feature_snapshot": _encode_record(feature_snapshot),
            "decision": _encode_record(decision),
            "legs": [_encode_record(leg) for leg in legs],
        }
        return self._record("decision_bundle", payload)

    def record_delivery(self, delivery: DeliveryRecord) -> TelemetryWriteResult:
        return self._record("delivery", {"delivery": _encode_record(delivery)})

    def record_event(self, event: EventRecord) -> TelemetryWriteResult:
        return self._record("event", {"event": _encode_record(event)})

    def record_outcome(self, outcome: OutcomeRecord) -> TelemetryWriteResult:
        return self._record("outcome", {"outcome": _encode_record(outcome)})

    def replay_fallback(self) -> SpoolReplayResult:
        return self.spool.replay(self.ledger)

    def _record(self, operation: str, payload: Mapping[str, object]) -> TelemetryWriteResult:
        try:
            _apply_operation(self.ledger, operation, payload)
            return TelemetryWriteResult(status="recorded", operation=operation)
        except Exception as exc:  # Telemetry must not suppress a realtime alert.
            error = f"{type(exc).__name__}:{exc}"
            try:
                self.spool.append(operation, payload)
            except Exception as spool_exc:
                return TelemetryWriteResult(
                    status="error",
                    operation=operation,
                    error=f"{error};spool={type(spool_exc).__name__}:{spool_exc}",
                )
            return TelemetryWriteResult(status="spooled", operation=operation, error=error)


@lru_cache(maxsize=8)
def _cached_telemetry(
    ledger_path: str,
    spool_path: str,
    busy_timeout_ms: int,
    spool_max_bytes: int,
) -> OperationalTelemetry:
    return OperationalTelemetry(
        SQLiteDecisionLedger(ledger_path, busy_timeout_ms=busy_timeout_ms),
        FallbackSpool(spool_path, max_bytes=spool_max_bytes),
    )


def telemetry_from_settings(settings: DataPlatformSettings) -> OperationalTelemetry | None:
    if not settings.enabled:
        return None
    return _cached_telemetry(
        settings.ledger_path,
        settings.fallback_spool_path,
        settings.sqlite_busy_timeout_ms,
        settings.fallback_spool_max_bytes,
    )


def clear_telemetry_cache() -> None:
    _cached_telemetry.cache_clear()


def _apply_operation(
    ledger: DecisionLedger,
    operation: str,
    payload: Mapping[str, object],
) -> None:
    if operation == "decision_bundle":
        raw_version = payload.get("strategy_version")
        raw_snapshot = payload.get("feature_snapshot")
        if raw_version is not None:
            ledger.record_strategy_version(_decode_strategy_version(_mapping(raw_version)))
        event = _decode_event(_mapping(payload["event"]))
        ledger.record_event(event)
        snapshot = None
        if raw_snapshot is not None:
            snapshot = _decode_feature_snapshot(_mapping(raw_snapshot))
            ledger.record_feature_snapshot(snapshot)
        decision = _decode_decision(_mapping(payload["decision"]))
        legs = tuple(_decode_leg(_mapping(row)) for row in _sequence(payload.get("legs", ())))
        ledger.record_decision(decision, legs)
        return
    if operation == "delivery":
        ledger.record_delivery(_decode_delivery(_mapping(payload["delivery"])))
        return
    if operation == "event":
        ledger.record_event(_decode_event(_mapping(payload["event"])))
        return
    if operation == "outcome":
        ledger.record_outcome(_decode_outcome(_mapping(payload["outcome"])))
        return
    raise ValueError(f"unsupported telemetry operation: {operation}")


def _encode_record(record: object | None) -> dict[str, object] | None:
    if record is None:
        return None
    return _json_value(asdict(record))


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError("telemetry record must be an object")
    return value


def _sequence(value: object) -> Sequence[object]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ValueError("telemetry record list is invalid")
    return value


def _datetime(value: object | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("telemetry timestamps must be timezone-aware")
    return parsed


def _date(value: object | None) -> date | None:
    return date.fromisoformat(str(value)) if value is not None else None


def _decode_strategy_version(row: Mapping[str, object]) -> StrategyVersionRecord:
    return StrategyVersionRecord(
        strategy_name=str(row["strategy_name"]),
        strategy_version=str(row["strategy_version"]),
        activated_at=_required_datetime(row.get("activated_at")),
        git_commit=str(row["git_commit"]) if row.get("git_commit") is not None else None,
        config_sha256=(
            str(row["config_sha256"]) if row.get("config_sha256") is not None else None
        ),
        metadata=_metadata(row.get("metadata")),
    )


def _decode_event(row: Mapping[str, object]) -> EventRecord:
    return EventRecord(
        event_key=str(row["event_key"]),
        event_type=str(row["event_type"]),
        session_date=_required_date(row.get("session_date")),
        source_at=_required_datetime(row.get("source_at")),
        available_at=_required_datetime(row.get("available_at")),
        received_at=_datetime(row.get("received_at")),
        phase=str(row["phase"]) if row.get("phase") is not None else None,
        direction=str(row["direction"]) if row.get("direction") is not None else None,
        data_quality=str(row.get("data_quality") or "unknown"),
        schema_version=int(row.get("schema_version") or 1),
        attributes=_metadata(row.get("attributes")),
    )


def _decode_feature_snapshot(row: Mapping[str, object]) -> FeatureSnapshotRecord:
    return FeatureSnapshotRecord(
        snapshot_id=str(row["snapshot_id"]),
        captured_at=_required_datetime(row.get("captured_at")),
        available_at=_required_datetime(row.get("available_at")),
        payload=_metadata(row.get("payload")),
        event_key=str(row["event_key"]) if row.get("event_key") is not None else None,
        gamma_regime=(
            str(row["gamma_regime"]) if row.get("gamma_regime") is not None else None
        ),
        schema_version=int(row.get("schema_version") or 1),
    )


def _decode_decision(row: Mapping[str, object]) -> DecisionRecord:
    return DecisionRecord(
        decision_id=str(row["decision_id"]),
        strategy_name=str(row["strategy_name"]),
        strategy_version=str(row["strategy_version"]),
        decision_at=_required_datetime(row.get("decision_at")),
        available_at=_required_datetime(row.get("available_at")),
        status=str(row["status"]),
        action=str(row["action"]),
        side=str(row["side"]),
        event_key=str(row["event_key"]) if row.get("event_key") is not None else None,
        feature_snapshot_id=(
            str(row["feature_snapshot_id"])
            if row.get("feature_snapshot_id") is not None
            else None
        ),
        reason=str(row["reason"]) if row.get("reason") is not None else None,
        gamma_regime=(
            str(row["gamma_regime"]) if row.get("gamma_regime") is not None else None
        ),
        attributes=_metadata(row.get("attributes")),
    )


def _decode_leg(row: Mapping[str, object]) -> DecisionLegRecord:
    return DecisionLegRecord(
        decision_id=str(row["decision_id"]),
        leg_index=int(row["leg_index"]),
        instrument_id=str(row["instrument_id"]),
        quote_source_at=_required_datetime(row.get("quote_source_at")),
        quote_available_at=_required_datetime(row.get("quote_available_at")),
        right=str(row["right"]) if row.get("right") is not None else None,
        expiry=_date(row.get("expiry")),
        strike=_float(row.get("strike")),
        quantity=_float(row.get("quantity")),
        bid=_float(row.get("bid")),
        ask=_float(row.get("ask")),
        delta=_float(row.get("delta")),
        gamma=_float(row.get("gamma")),
        theta=_float(row.get("theta")),
        vega=_float(row.get("vega")),
        attributes=_metadata(row.get("attributes")),
    )


def _decode_delivery(row: Mapping[str, object]) -> DeliveryRecord:
    return DeliveryRecord(
        delivery_id=str(row["delivery_id"]),
        decision_id=str(row["decision_id"]),
        channel=str(row["channel"]),
        status=str(row["status"]),
        attempted_at=_required_datetime(row.get("attempted_at")),
        sent_at=_datetime(row.get("sent_at")),
        provider=str(row["provider"]) if row.get("provider") is not None else None,
        veto_reason=(
            str(row["veto_reason"]) if row.get("veto_reason") is not None else None
        ),
        error_code=str(row["error_code"]) if row.get("error_code") is not None else None,
        message_fingerprint=(
            str(row["message_fingerprint"])
            if row.get("message_fingerprint") is not None
            else None
        ),
        attributes=_metadata(row.get("attributes")),
    )


def _decode_outcome(row: Mapping[str, object]) -> OutcomeRecord:
    return OutcomeRecord(
        outcome_id=str(row["outcome_id"]),
        event_key=str(row["event_key"]),
        horizon_minutes=int(row["horizon_minutes"]),
        status=str(row["status"]),
        target_at=_required_datetime(row.get("target_at")),
        sampled_at=_datetime(row.get("sampled_at")),
        decision_id=str(row["decision_id"]) if row.get("decision_id") is not None else None,
        hypothesis_direction=(
            str(row["hypothesis_direction"])
            if row.get("hypothesis_direction") is not None
            else None
        ),
        spx_return_bps=_float(row.get("spx_return_bps")),
        spx_mfe_bps=_float(row.get("spx_mfe_bps")),
        spx_mae_bps=_float(row.get("spx_mae_bps")),
        option_return_bps=_float(row.get("option_return_bps")),
        option_pnl=_float(row.get("option_pnl")),
        attributes=_metadata(row.get("attributes")),
    )


def _metadata(value: object) -> Mapping[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError("telemetry metadata must be an object")
    return {str(key): item for key, item in value.items()}


def _required_datetime(value: object | None) -> datetime:
    parsed = _datetime(value)
    if parsed is None:
        raise ValueError("required telemetry timestamp is missing")
    return parsed


def _required_date(value: object | None) -> date:
    parsed = _date(value)
    if parsed is None:
        raise ValueError("required telemetry date is missing")
    return parsed


def _float(value: object | None) -> float | None:
    return float(value) if value is not None else None


def _write_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(descriptor, payload[offset:])
        if written <= 0:
            raise OSError("short write to telemetry fallback spool")
        offset += written
