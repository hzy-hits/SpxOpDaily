from __future__ import annotations

import subprocess
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import datetime


CommandRunner = Callable[[list[str], float], subprocess.CompletedProcess[str]]


@dataclass(frozen=True)
class NotificationEnvelope:
    event_id: str
    source: str
    kind: str
    lane: str
    occurred_at: datetime
    expires_at: datetime | None = None
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
class DeliveryJob:
    envelope: NotificationEnvelope
    title: str
    text: str
    feishu_text: str | None
    friend: bool
    targets: tuple[str, ...]


@dataclass(frozen=True)
class DeliveryEventInspection:
    event_id: str
    exists: bool
    cancelled: bool
    payload_matches: bool
    targets_match: bool
    event_status: str | None
    target_statuses: tuple[tuple[str, str], ...]
    reason: str

    @property
    def acceptable(self) -> bool:
        return self.reason == "accepted"


@dataclass(frozen=True)
class ExternalDeliveryReceipt:
    """Earliest durable Bark/Feishu receipt for one immutable event."""

    event_id: str
    receipt_id: str
    delivered_at: datetime
    sink: str
    channel: str
    ledger: str


@dataclass(frozen=True)
class ExternalDeliveryReceiptLookup:
    """Separate a healthy no-receipt result from an unreadable ledger."""

    observable: bool
    receipt: ExternalDeliveryReceipt | None
    error: str | None = None


@dataclass(frozen=True)
class SinkResult:
    sink: str
    attempted: bool
    ok: bool
    dry_run: bool = False
    exit_code: int | None = None
    error: str | None = None
    alert_keys: tuple[str, ...] = ()
    verdict: str | None = None
    # Deterministic failure (e.g. HTTP 4xx except 429): retrying the identical
    # payload cannot succeed, so the outbox dead-letters on the first attempt.
    permanent: bool = False

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class NotificationResult:
    enabled: bool
    selected_count: int
    sent_count: int
    skipped_reason: str | None
    sinks: tuple[SinkResult, ...]
    acknowledged_event_ids: tuple[str, ...] = ()
    selected_alert_keys: tuple[str, ...] = ()
    outcome: str = "unknown"

    def to_dict(self) -> dict[str, object]:
        return {
            "enabled": self.enabled,
            "selected_count": self.selected_count,
            "sent_count": self.sent_count,
            "skipped_reason": self.skipped_reason,
            "sinks": [sink.to_dict() for sink in self.sinks],
            "acknowledged_event_ids": list(self.acknowledged_event_ids),
            "selected_alert_keys": list(self.selected_alert_keys),
            "outcome": self.outcome,
        }


def default_runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )
