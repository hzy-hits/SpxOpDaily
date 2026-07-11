"""Versioned records shared by operational storage adapters.

The contracts deliberately describe business facts rather than database CRUD.
All timestamps are timezone-aware and the ``available_at`` clock is kept
separate from source/receive clocks so historical replay can prevent lookahead.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Mapping, TypeAlias


JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]
Metadata: TypeAlias = Mapping[str, JsonValue]


def _require_text(name: str, value: str) -> None:
    if not value or not value.strip():
        raise ValueError(f"{name} is required")


def _require_aware(name: str, value: datetime | None) -> None:
    if value is not None and (value.tzinfo is None or value.utcoffset() is None):
        raise ValueError(f"{name} must be timezone-aware")


def _require_finite(name: str, value: float | None) -> None:
    if value is not None and not math.isfinite(value):
        raise ValueError(f"{name} must be finite")


def _require_sha256(name: str, value: str | None) -> None:
    if value is None:
        return
    if len(value) != 64 or any(character not in "0123456789abcdefABCDEF" for character in value):
        raise ValueError(f"{name} must be a 64-character SHA-256 hex digest")


@dataclass(frozen=True, slots=True)
class SessionRecord:
    """One market session and its final data-quality disposition."""

    session_date: date
    market: str = "SPX"
    status: str = "open"
    opened_at: datetime | None = None
    closed_at: datetime | None = None
    data_quality: str = "unknown"
    metadata: Metadata = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_text("market", self.market)
        _require_text("status", self.status)
        _require_text("data_quality", self.data_quality)
        _require_aware("opened_at", self.opened_at)
        _require_aware("closed_at", self.closed_at)
        if self.opened_at and self.closed_at and self.closed_at < self.opened_at:
            raise ValueError("closed_at cannot precede opened_at")


@dataclass(frozen=True, slots=True)
class StrategyVersionRecord:
    """An immutable strategy/configuration version used by decisions."""

    strategy_name: str
    strategy_version: str
    activated_at: datetime
    git_commit: str | None = None
    config_sha256: str | None = None
    metadata: Metadata = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_text("strategy_name", self.strategy_name)
        _require_text("strategy_version", self.strategy_version)
        _require_aware("activated_at", self.activated_at)


@dataclass(frozen=True, slots=True)
class EventRecord:
    """A deterministic market event such as shock, reclaim, or wall break."""

    event_key: str
    event_type: str
    session_date: date
    source_at: datetime
    available_at: datetime
    received_at: datetime | None = None
    phase: str | None = None
    direction: str | None = None
    data_quality: str = "unknown"
    schema_version: int = 1
    attributes: Metadata = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_text("event_key", self.event_key)
        _require_text("event_type", self.event_type)
        _require_text("data_quality", self.data_quality)
        _require_aware("source_at", self.source_at)
        _require_aware("available_at", self.available_at)
        _require_aware("received_at", self.received_at)
        if self.available_at < self.source_at:
            raise ValueError("available_at cannot precede source_at")
        if self.schema_version <= 0:
            raise ValueError("schema_version must be positive")


@dataclass(frozen=True, slots=True)
class FeatureSnapshotRecord:
    """The exact feature payload available to one decision or event."""

    snapshot_id: str
    captured_at: datetime
    available_at: datetime
    payload: Metadata
    event_key: str | None = None
    gamma_regime: str | None = None
    schema_version: int = 1

    def __post_init__(self) -> None:
        _require_text("snapshot_id", self.snapshot_id)
        _require_aware("captured_at", self.captured_at)
        _require_aware("available_at", self.available_at)
        if self.available_at < self.captured_at:
            raise ValueError("available_at cannot precede captured_at")
        if self.schema_version <= 0:
            raise ValueError("schema_version must be positive")


@dataclass(frozen=True, slots=True)
class DecisionRecord:
    """One strategy decision, including rejected and vetoed candidates."""

    decision_id: str
    strategy_name: str
    strategy_version: str
    decision_at: datetime
    available_at: datetime
    status: str
    action: str
    side: str
    event_key: str | None = None
    feature_snapshot_id: str | None = None
    reason: str | None = None
    gamma_regime: str | None = None
    attributes: Metadata = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name, value in (
            ("decision_id", self.decision_id),
            ("strategy_name", self.strategy_name),
            ("strategy_version", self.strategy_version),
            ("status", self.status),
            ("action", self.action),
            ("side", self.side),
        ):
            _require_text(name, value)
        _require_aware("decision_at", self.decision_at)
        _require_aware("available_at", self.available_at)
        if self.available_at > self.decision_at:
            raise ValueError("decision cannot use features that were not yet available")


@dataclass(frozen=True, slots=True)
class DecisionLegRecord:
    """The option/underlying quote frozen for one decision leg."""

    decision_id: str
    leg_index: int
    instrument_id: str
    quote_source_at: datetime
    quote_available_at: datetime
    right: str | None = None
    expiry: date | None = None
    strike: float | None = None
    quantity: float | None = None
    bid: float | None = None
    ask: float | None = None
    delta: float | None = None
    gamma: float | None = None
    theta: float | None = None
    vega: float | None = None
    attributes: Metadata = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_text("decision_id", self.decision_id)
        _require_text("instrument_id", self.instrument_id)
        if self.leg_index < 0:
            raise ValueError("leg_index cannot be negative")
        _require_aware("quote_source_at", self.quote_source_at)
        _require_aware("quote_available_at", self.quote_available_at)
        if self.quote_available_at < self.quote_source_at:
            raise ValueError("quote_available_at cannot precede quote_source_at")
        for name in ("strike", "quantity", "bid", "ask", "delta", "gamma", "theta", "vega"):
            _require_finite(name, getattr(self, name))


@dataclass(frozen=True, slots=True)
class DeliveryRecord:
    """One concrete notification attempt, including vetoes and failures."""

    delivery_id: str
    decision_id: str
    channel: str
    status: str
    attempted_at: datetime
    sent_at: datetime | None = None
    provider: str | None = None
    veto_reason: str | None = None
    error_code: str | None = None
    message_fingerprint: str | None = None
    attributes: Metadata = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name, value in (
            ("delivery_id", self.delivery_id),
            ("decision_id", self.decision_id),
            ("channel", self.channel),
            ("status", self.status),
        ):
            _require_text(name, value)
        _require_aware("attempted_at", self.attempted_at)
        _require_aware("sent_at", self.sent_at)
        if self.sent_at and self.sent_at < self.attempted_at:
            raise ValueError("sent_at cannot precede attempted_at")


@dataclass(frozen=True, slots=True)
class OutcomeRecord:
    """A fixed-horizon event/decision result used by research views."""

    outcome_id: str
    event_key: str
    horizon_minutes: int
    status: str
    target_at: datetime
    sampled_at: datetime | None = None
    decision_id: str | None = None
    hypothesis_direction: str | None = None
    spx_return_bps: float | None = None
    spx_mfe_bps: float | None = None
    spx_mae_bps: float | None = None
    option_return_bps: float | None = None
    option_pnl: float | None = None
    attributes: Metadata = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_text("outcome_id", self.outcome_id)
        _require_text("event_key", self.event_key)
        _require_text("status", self.status)
        if self.horizon_minutes <= 0:
            raise ValueError("horizon_minutes must be positive")
        _require_aware("target_at", self.target_at)
        _require_aware("sampled_at", self.sampled_at)
        for name in (
            "spx_return_bps",
            "spx_mfe_bps",
            "spx_mae_bps",
            "option_return_bps",
            "option_pnl",
        ):
            _require_finite(name, getattr(self, name))


@dataclass(frozen=True, slots=True)
class CompactionManifestRecord:
    """Current verified lake lineage for one immutable landing-file checksum."""

    source_path: str
    source_sha256: str
    source_size: int
    source_mtime_ns: int
    output_path: str | None
    output_sha256: str | None
    row_count: int
    min_received_at: datetime | None
    max_received_at: datetime | None
    schema_version: str
    writer_version: str
    completed_at: datetime
    status: str = "verified"
    dataset: str = "quotes"

    def __post_init__(self) -> None:
        for name, value in (
            ("source_path", self.source_path),
            ("source_sha256", self.source_sha256),
            ("writer_version", self.writer_version),
            ("status", self.status),
            ("dataset", self.dataset),
        ):
            _require_text(name, value)
        _require_sha256("source_sha256", self.source_sha256)
        _require_sha256("output_sha256", self.output_sha256)
        if (self.output_path is None) != (self.output_sha256 is None):
            raise ValueError(
                "output_path and output_sha256 must either both be set or both be absent"
            )
        if self.output_path is not None:
            _require_text("output_path", self.output_path)
        if self.source_size < 0 or self.source_mtime_ns < 0 or self.row_count < 0:
            raise ValueError("manifest sizes and row_count cannot be negative")
        _require_text("schema_version", self.schema_version)
        if self.status == "verified" and self.row_count > 0 and self.output_path is None:
            raise ValueError("a non-empty verified manifest requires an output")
        _require_aware("min_received_at", self.min_received_at)
        _require_aware("max_received_at", self.max_received_at)
        _require_aware("completed_at", self.completed_at)
        if (
            self.min_received_at
            and self.max_received_at
            and self.max_received_at < self.min_received_at
        ):
            raise ValueError("max_received_at cannot precede min_received_at")


@dataclass(frozen=True, slots=True)
class LandingWriteReceipt:
    """Result of appending one quote batch to the short-lived landing zone."""

    row_count: int
    path_counts: Mapping[str, int]

    def __post_init__(self) -> None:
        if self.row_count < 0 or any(count < 0 for count in self.path_counts.values()):
            raise ValueError("landing row counts cannot be negative")
        if self.row_count != sum(self.path_counts.values()):
            raise ValueError("landing row_count must equal the sum of path_counts")


@dataclass(frozen=True, slots=True)
class LakePartition:
    """Logical immutable lake partition independent of a filesystem layout."""

    dataset: str
    schema_version: str
    session_date: date
    provider: str | None = None
    hour: int | None = None
    attributes: Metadata = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_text("dataset", self.dataset)
        _require_text("schema_version", self.schema_version)
        if self.hour is not None and not 0 <= self.hour <= 23:
            raise ValueError("partition hour must be between 0 and 23")


@dataclass(frozen=True, slots=True)
class LakePublishReceipt:
    """Outcome of publishing and verifying an immutable lake partition."""

    partition: LakePartition
    source_path: str
    output_path: str | None
    status: str
    row_count: int
    source_sha256: str | None = None

    def __post_init__(self) -> None:
        _require_text("source_path", self.source_path)
        _require_text("status", self.status)
        if self.row_count < 0:
            raise ValueError("published row_count cannot be negative")
