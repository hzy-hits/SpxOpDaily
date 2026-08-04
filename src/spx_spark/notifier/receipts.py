"""Durable, content-free receipts for all human notification attempts."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

from spx_spark.notifier.model import SinkResult


_SCHEMA = """
CREATE TABLE IF NOT EXISTS notification_delivery_receipts (
    attempt_id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    source TEXT NOT NULL,
    kind TEXT NOT NULL,
    lane TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    attempted_at TEXT NOT NULL,
    outcome TEXT NOT NULL,
    queued_for_recovery INTEGER NOT NULL,
    sinks_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_notification_receipts_event
    ON notification_delivery_receipts(event_id, attempted_at);
CREATE INDEX IF NOT EXISTS idx_notification_receipts_outcome
    ON notification_delivery_receipts(outcome, attempted_at);

CREATE TABLE IF NOT EXISTS notification_delivery_receipt_mirrors (
    mirror_id TEXT PRIMARY KEY,
    attempt_id TEXT NOT NULL,
    mirrored_at TEXT NOT NULL,
    FOREIGN KEY (attempt_id) REFERENCES notification_delivery_receipts(attempt_id)
        ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_notification_receipt_mirrors_attempt
    ON notification_delivery_receipt_mirrors(attempt_id);
"""

_RECEIPT_COLUMNS = frozenset(
    {
        "attempt_id",
        "event_id",
        "source",
        "kind",
        "lane",
        "occurred_at",
        "attempted_at",
        "outcome",
        "queued_for_recovery",
        "sinks_json",
    }
)
_MIRROR_COLUMNS = frozenset({"mirror_id", "attempt_id", "mirrored_at"})
_SQLITE_PARAMETER_BATCH = 400


@dataclass(frozen=True)
class NotificationEnvelope:
    event_id: str
    source: str
    kind: str
    lane: str
    occurred_at: datetime
    expires_at: datetime | None = None
    # Frozen Rust fan-out targets. These travel with the immutable outbox event
    # so a retry cannot silently change its payload when environment settings
    # are edited after enqueue.
    operator_targets: tuple[tuple[str, str], ...] = ()
    operator_opportunity_id: str | None = None
    operator_generation: int = 0

    def validate(self) -> None:
        for label, value in (
            ("event_id", self.event_id),
            ("source", self.source),
            ("kind", self.kind),
            ("lane", self.lane),
        ):
            if not value.strip():
                raise ValueError(f"{label} is required")
        if self.occurred_at.tzinfo is None:
            raise ValueError("occurred_at must be timezone-aware")
        if self.expires_at is not None:
            if self.expires_at.tzinfo is None:
                raise ValueError("expires_at must be timezone-aware")
            if self.expires_at <= self.occurred_at:
                raise ValueError("expires_at must be after occurred_at")
        seen_keys: set[str] = set()
        for target in self.operator_targets:
            if len(target) != 2:
                raise ValueError("operator target must contain key and channel")
            key, channel = target
            if not key.strip():
                raise ValueError("operator target key is required")
            if key in seen_keys:
                raise ValueError(f"duplicate operator target key: {key}")
            if channel not in {"bark", "feishu", "webhook"}:
                raise ValueError(f"unsupported operator target channel: {channel}")
            seen_keys.add(key)
        if self.operator_opportunity_id is not None:
            if not self.operator_opportunity_id.strip():
                raise ValueError("operator_opportunity_id cannot be blank")
            if "\0" in self.operator_opportunity_id:
                raise ValueError("operator_opportunity_id contains NUL")
            if len(self.operator_opportunity_id.encode("utf-8")) > 4_096:
                raise ValueError("operator_opportunity_id exceeds 4096 UTF-8 bytes")
        if (
            isinstance(self.operator_generation, bool)
            or not isinstance(self.operator_generation, int)
            or not 0 <= self.operator_generation <= 4_294_967_295
        ):
            raise ValueError("operator_generation must be a u32 integer")


@dataclass(frozen=True)
class ReceiptStoreInspection:
    """Read-only receipt-ledger durability and mirror reconciliation result."""

    ok: bool
    exists: bool
    quick_check: str
    journal_mode: str
    synchronous: str
    schema_present: bool
    required_mirror_ids: int
    missing_mirror_ids: tuple[str, ...]
    error: str | None = None


def notification_event_id(
    kind: str,
    *,
    source: str,
    occurred_at: datetime,
    identity: str,
) -> str:
    """Stable semantic delivery id; message text is deliberately excluded."""

    occurred = occurred_at.astimezone(timezone.utc).isoformat(timespec="seconds")
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:20]
    return f"notify:{source}:{kind}:{occurred}:{digest}"


def inspect_delivery_receipt_store(
    path: str | Path,
    *,
    required_mirror_ids: Sequence[str] = (),
) -> ReceiptStoreInspection:
    """Inspect the ledger without creating, migrating, or mutating it."""

    database = Path(path) if path else Path()
    required = tuple(dict.fromkeys(str(value) for value in required_mirror_ids if value))
    if not path or not database.is_file():
        return ReceiptStoreInspection(
            ok=False,
            exists=False,
            quick_check="missing",
            journal_mode="missing",
            synchronous="missing",
            schema_present=False,
            required_mirror_ids=len(required),
            missing_mirror_ids=required,
            error="receipt_store_missing",
        )
    quick_check = "unreadable"
    journal_mode = "unknown"
    synchronous = "unknown"
    schema_present = False
    try:
        with sqlite3.connect(
            f"file:{database}?mode=ro",
            uri=True,
            timeout=1.0,
        ) as connection:
            connection.execute("PRAGMA query_only=ON")
            connection.execute("PRAGMA busy_timeout=1000")
            quick_check = str(connection.execute("PRAGMA quick_check(1)").fetchone()[0]).lower()
            journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower()
            synchronous = _synchronous_name(connection.execute("PRAGMA synchronous").fetchone()[0])
            receipt_columns = {
                str(row[1])
                for row in connection.execute("PRAGMA table_info(notification_delivery_receipts)")
            }
            mirror_columns = {
                str(row[1])
                for row in connection.execute(
                    "PRAGMA table_info(notification_delivery_receipt_mirrors)"
                )
            }
            schema_present = (
                _RECEIPT_COLUMNS <= receipt_columns and _MIRROR_COLUMNS <= mirror_columns
            )
            present: set[str] = set()
            if schema_present:
                for offset in range(0, len(required), _SQLITE_PARAMETER_BATCH):
                    batch = required[offset : offset + _SQLITE_PARAMETER_BATCH]
                    placeholders = ",".join("?" for _ in batch)
                    present.update(
                        str(row[0])
                        for row in connection.execute(
                            f"""
                            SELECT m.mirror_id
                            FROM notification_delivery_receipt_mirrors AS m
                            JOIN notification_delivery_receipts AS r
                              ON r.attempt_id = m.attempt_id
                            WHERE m.mirror_id IN ({placeholders})
                            """,
                            batch,
                        )
                    )
            missing = tuple(value for value in required if value not in present)
    except (OSError, sqlite3.Error, ValueError) as exc:
        return ReceiptStoreInspection(
            ok=False,
            exists=True,
            quick_check=quick_check,
            journal_mode=journal_mode,
            synchronous=synchronous,
            schema_present=schema_present,
            required_mirror_ids=len(required),
            missing_mirror_ids=required,
            error=f"{type(exc).__name__}: {str(exc)[:300]}",
        )
    ok = (
        quick_check == "ok"
        and journal_mode == "delete"
        and synchronous == "full"
        and schema_present
        and not missing
    )
    return ReceiptStoreInspection(
        ok=ok,
        exists=True,
        quick_check=quick_check,
        journal_mode=journal_mode,
        synchronous=synchronous,
        schema_present=schema_present,
        required_mirror_ids=len(required),
        missing_mirror_ids=missing,
        error=None if ok else "receipt_store_contract_failed",
    )


def prepare_delivery_receipt_store(path: str | Path) -> bool:
    """Create/migrate a healthy receipt ledger without deleting bad data."""

    if not path:
        return False
    database = Path(path)
    try:
        database.parent.mkdir(parents=True, exist_ok=True)
        descriptor = os.open(database, os.O_RDWR | os.O_CREAT, 0o600)
        os.close(descriptor)
        os.chmod(database, 0o600)
        with sqlite3.connect(
            database,
            timeout=1.0,
            isolation_level=None,
        ) as connection:
            connection.execute("PRAGMA busy_timeout=1000")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("PRAGMA foreign_keys=ON")
            journal_mode = str(
                connection.execute("PRAGMA journal_mode=DELETE").fetchone()[0]
            ).lower()
            if journal_mode != "delete":
                return False
            connection.executescript(_SCHEMA)
            quick_check = str(connection.execute("PRAGMA quick_check(1)").fetchone()[0]).lower()
            if quick_check != "ok":
                return False
    except (OSError, sqlite3.Error, ValueError):
        return False
    return inspect_delivery_receipt_store(database).ok


def record_delivery_receipt(
    path: str,
    envelope: NotificationEnvelope,
    *,
    sinks: Sequence[SinkResult],
    outcome: str,
    queued_for_recovery: bool,
    attempted_at: datetime | None = None,
    idempotency_key: str | None = None,
    mirror_ids: Sequence[str] = (),
) -> bool:
    """Persist one delivery outcome without storing message bodies or secrets."""

    if not path:
        return False
    envelope.validate()
    attempted_at = attempted_at or datetime.now(tz=timezone.utc)
    if attempted_at.tzinfo is None:
        attempted_at = attempted_at.replace(tzinfo=timezone.utc)
    attempted = attempted_at.astimezone(timezone.utc).isoformat(timespec="microseconds")
    identity = f"{envelope.event_id}|{attempted}"
    if idempotency_key is not None:
        identity = f"{identity}|{idempotency_key}"
    attempt_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    normalized_mirror_ids = tuple(dict.fromkeys(str(value) for value in mirror_ids if value))
    sink_rows = [
        {
            "sink": sink.sink,
            "attempted": sink.attempted,
            "ok": sink.ok,
            "error": (sink.error or "")[:500] or None,
            "verdict": sink.verdict,
        }
        for sink in sinks
    ]
    database = Path(path)
    try:
        if not prepare_delivery_receipt_store(database):
            return False
        with sqlite3.connect(
            database,
            timeout=1.0,
            isolation_level=None,
        ) as connection:
            connection.execute("PRAGMA busy_timeout=1000")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute("PRAGMA foreign_keys=ON")
            if (
                str(connection.execute("PRAGMA journal_mode=DELETE").fetchone()[0]).lower()
                != "delete"
            ):
                return False
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                INSERT OR IGNORE INTO notification_delivery_receipts (
                    attempt_id, event_id, source, kind, lane, occurred_at,
                    attempted_at, outcome, queued_for_recovery, sinks_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    attempt_id,
                    envelope.event_id,
                    envelope.source,
                    envelope.kind,
                    envelope.lane,
                    envelope.occurred_at.astimezone(timezone.utc).isoformat(),
                    attempted,
                    outcome,
                    int(queued_for_recovery),
                    json.dumps(sink_rows, ensure_ascii=False, separators=(",", ":")),
                ),
            )
            for mirror_id in normalized_mirror_ids:
                existing = connection.execute(
                    """
                    SELECT attempt_id
                    FROM notification_delivery_receipt_mirrors
                    WHERE mirror_id = ?
                    """,
                    (mirror_id,),
                ).fetchone()
                if existing is not None and str(existing[0]) != attempt_id:
                    raise sqlite3.IntegrityError(f"receipt mirror_id collision: {mirror_id}")
                connection.execute(
                    """
                    INSERT OR IGNORE INTO notification_delivery_receipt_mirrors (
                        mirror_id, attempt_id, mirrored_at
                    ) VALUES (?, ?, ?)
                    """,
                    (mirror_id, attempt_id, attempted),
                )
            connection.execute("COMMIT")
        inspection = inspect_delivery_receipt_store(
            database,
            required_mirror_ids=normalized_mirror_ids,
        )
        return inspection.ok
    except (OSError, sqlite3.Error, ValueError):
        # Receipt telemetry must never change the authoritative delivery result.
        return False


def _synchronous_name(value: object) -> str:
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return str(value).lower()
    return {
        0: "off",
        1: "normal",
        2: "full",
        3: "extra",
    }.get(normalized, str(normalized))
