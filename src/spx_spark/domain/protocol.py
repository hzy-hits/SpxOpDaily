"""Cross-process decision envelope and correlation identifiers.

Phase 1 of the decision protocol: a stdlib-only envelope shared by producers and
consumers. Payload schemas are still dicts; the envelope gives every message a
type/version, a UTC production clock and one correlation id that can be followed
from the decision into notification records.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from spx_spark.clock import Clock, SystemClock


def new_correlation_id() -> str:
    return f"corr:{uuid4().hex}"


def new_message_id() -> str:
    return f"msg:{uuid4().hex}"


def ensure_correlation_id(payload: Mapping[str, Any]) -> str:
    """Reuse a producer correlation id or create one at the decision boundary."""

    text = str(payload.get("correlation_id") or "").strip()
    return text or new_correlation_id()


def _require_utc(label: str, value: datetime) -> None:
    if value.tzinfo is None:
        raise ValueError(f"{label} must be timezone-aware")
    if value.utcoffset() != timezone.utc.utcoffset(value):
        raise ValueError(f"{label} must use UTC")


@dataclass(frozen=True, slots=True)
class Envelope:
    message_id: str
    type: str
    version: int
    produced_at: datetime
    as_of: datetime
    source: str
    correlation_id: str
    payload: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for label, value in (
            ("message_id", self.message_id),
            ("type", self.type),
            ("source", self.source),
            ("correlation_id", self.correlation_id),
        ):
            if not value or not value.strip():
                raise ValueError(f"{label} is required")
        if self.version < 1:
            raise ValueError("version must be >= 1")
        _require_utc("produced_at", self.produced_at)
        _require_utc("as_of", self.as_of)

    def validated_payload(self) -> Any:
        from spx_spark.domain.payload_schemas import validate_payload

        return validate_payload(self.type, self.version, self.payload)

    def to_dict(self) -> dict[str, Any]:
        return {
            "message_id": self.message_id,
            "type": self.type,
            "version": self.version,
            "produced_at": self.produced_at.isoformat(),
            "as_of": self.as_of.isoformat(),
            "source": self.source,
            "correlation_id": self.correlation_id,
            "payload": dict(self.payload),
        }


def envelope_for(
    *,
    message_type: str,
    version: int,
    as_of: datetime,
    source: str,
    correlation_id: str,
    payload: Mapping[str, Any] | None = None,
    produced_at: datetime | None = None,
    clock: Clock | None = None,
) -> Envelope:
    return Envelope(
        message_id=new_message_id(),
        type=message_type,
        version=version,
        produced_at=produced_at or (clock or SystemClock()).now(),
        as_of=as_of,
        source=source,
        correlation_id=correlation_id,
        payload=payload or {},
    )
