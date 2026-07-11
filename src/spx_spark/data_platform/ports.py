"""Capability-oriented storage ports used by the data platform."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime
from pathlib import Path
from typing import Mapping, Protocol, Sequence, TypeVar, runtime_checkable

from spx_spark.data_platform.contracts import (
    CompactionManifestRecord,
    DecisionLegRecord,
    DecisionRecord,
    DeliveryRecord,
    EventRecord,
    FeatureSnapshotRecord,
    LakePartition,
    LakePublishReceipt,
    LandingWriteReceipt,
    OutcomeRecord,
    SessionRecord,
    StrategyVersionRecord,
)


QuoteT = TypeVar("QuoteT", contravariant=True)


class LedgerError(RuntimeError):
    """Base class for operational-ledger failures."""


class LedgerConflictError(LedgerError):
    """The same immutable business key was retried with different content."""


class LedgerReferenceError(LedgerError):
    """A record references a parent that has not been persisted."""


class LookaheadViolationError(LedgerError):
    """A decision attempted to consume data unavailable at decision time."""


class MigrationError(LedgerError):
    """The forward-only migration history cannot be trusted."""


@runtime_checkable
class DecisionLedger(Protocol):
    """Transactional operational ledger; implementations must be retry-safe."""

    def record_session(self, session: SessionRecord) -> None: ...

    def record_strategy_version(self, version: StrategyVersionRecord) -> None: ...

    def record_event(self, event: EventRecord) -> None: ...

    def record_feature_snapshot(self, snapshot: FeatureSnapshotRecord) -> None: ...

    def record_decision(
        self,
        decision: DecisionRecord,
        legs: Sequence[DecisionLegRecord] = (),
    ) -> None: ...

    def record_delivery(self, delivery: DeliveryRecord) -> None: ...

    def record_outcome(self, outcome: OutcomeRecord) -> None: ...

    def record_compaction_manifest(self, manifest: CompactionManifestRecord) -> None: ...

    def get_event(self, event_key: str) -> EventRecord | None: ...

    def get_decision(self, decision_id: str) -> DecisionRecord | None: ...

    def list_decision_legs(self, decision_id: str) -> Sequence[DecisionLegRecord]: ...

    def list_deliveries(self, decision_id: str) -> Sequence[DeliveryRecord]: ...

    def list_outcomes(self, decision_id: str) -> Sequence[OutcomeRecord]: ...

    def get_compaction_manifest(
        self,
        source_path: str,
        source_sha256: str,
    ) -> CompactionManifestRecord | None: ...


OperationalLedger = DecisionLedger


@runtime_checkable
class QuoteLandingWriter(Protocol[QuoteT]):
    """Append live quote batches without exposing the landing file format."""

    def append_quotes(self, quotes: Iterable[QuoteT]) -> LandingWriteReceipt: ...


@runtime_checkable
class HistoricalLake(Protocol):
    """Publish one closed source file as an immutable verified partition."""

    def publish_partition(
        self,
        partition: LakePartition,
        source_path: str | Path,
        *,
        as_of: datetime,
        dry_run: bool = False,
    ) -> LakePublishReceipt: ...


class ResearchReader(Protocol):
    """Read-only analytical surface; the lake remains the source of truth."""

    def strategy_outcomes(
        self,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        strategy_name: str | None = None,
        side: str | None = None,
        limit: int | None = None,
    ) -> Sequence[Mapping[str, object]]: ...
