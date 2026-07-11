"""In-memory operational ledger for fast contract and strategy tests."""

from __future__ import annotations

from collections.abc import Sequence
from threading import RLock

from spx_spark.data_platform.contracts import (
    CompactionManifestRecord,
    DecisionLegRecord,
    DecisionRecord,
    DeliveryRecord,
    EventRecord,
    FeatureSnapshotRecord,
    OutcomeRecord,
    SessionRecord,
    StrategyVersionRecord,
)
from spx_spark.data_platform.ports import (
    LedgerConflictError,
    LedgerReferenceError,
    LookaheadViolationError,
)


def _put_immutable[K, V](target: dict[K, V], key: K, value: V, label: str) -> None:
    existing = target.get(key)
    if existing is not None and existing != value:
        raise LedgerConflictError(f"conflicting immutable {label} record")
    target[key] = value


class InMemoryDecisionLedger:
    """Behavioral twin of ``SQLiteDecisionLedger`` without persistence."""

    def __init__(self) -> None:
        self._lock = RLock()
        self.sessions: dict[object, SessionRecord] = {}
        self.strategy_versions: dict[tuple[str, str], StrategyVersionRecord] = {}
        self.events: dict[str, EventRecord] = {}
        self.feature_snapshots: dict[str, FeatureSnapshotRecord] = {}
        self.decisions: dict[str, DecisionRecord] = {}
        self.decision_legs: dict[str, tuple[DecisionLegRecord, ...]] = {}
        self.deliveries: dict[str, DeliveryRecord] = {}
        self.outcomes: dict[str, OutcomeRecord] = {}
        self.compaction_manifests: dict[tuple[str, str], CompactionManifestRecord] = {}

    def record_session(self, session: SessionRecord) -> None:
        with self._lock:
            self.sessions[session.session_date] = session

    def record_strategy_version(self, version: StrategyVersionRecord) -> None:
        with self._lock:
            _put_immutable(
                self.strategy_versions,
                (version.strategy_name, version.strategy_version),
                version,
                "strategy version",
            )

    def record_event(self, event: EventRecord) -> None:
        with self._lock:
            _put_immutable(self.events, event.event_key, event, "event")

    def record_feature_snapshot(self, snapshot: FeatureSnapshotRecord) -> None:
        with self._lock:
            if snapshot.event_key and snapshot.event_key not in self.events:
                raise LedgerReferenceError("feature snapshot event has not been recorded")
            _put_immutable(
                self.feature_snapshots, snapshot.snapshot_id, snapshot, "feature snapshot"
            )

    def record_decision(
        self,
        decision: DecisionRecord,
        legs: Sequence[DecisionLegRecord] = (),
    ) -> None:
        normalized_legs = tuple(sorted(legs, key=lambda leg: leg.leg_index))
        if len({leg.leg_index for leg in normalized_legs}) != len(normalized_legs):
            raise ValueError("decision leg indexes must be unique")
        for leg in normalized_legs:
            if leg.decision_id != decision.decision_id:
                raise ValueError("every leg must reference the decision being recorded")
            if leg.quote_available_at > decision.decision_at:
                raise LookaheadViolationError("decision leg quote was unavailable at decision time")

        with self._lock:
            if decision.event_key:
                event = self.events.get(decision.event_key)
                if event is None:
                    raise LedgerReferenceError("decision event has not been recorded")
                if event.available_at > decision.decision_at:
                    raise LookaheadViolationError("event was unavailable at decision time")
            if decision.feature_snapshot_id:
                snapshot = self.feature_snapshots.get(decision.feature_snapshot_id)
                if snapshot is None:
                    raise LedgerReferenceError("decision feature snapshot has not been recorded")
                if snapshot.available_at > decision.decision_at:
                    raise LookaheadViolationError(
                        "feature snapshot was unavailable at decision time"
                    )

            existing = self.decisions.get(decision.decision_id)
            if existing is not None:
                if (
                    existing != decision
                    or self.decision_legs.get(decision.decision_id, ()) != normalized_legs
                ):
                    raise LedgerConflictError("conflicting immutable decision aggregate")
                return
            self.decisions[decision.decision_id] = decision
            self.decision_legs[decision.decision_id] = normalized_legs

    def record_delivery(self, delivery: DeliveryRecord) -> None:
        with self._lock:
            if delivery.decision_id not in self.decisions:
                raise LedgerReferenceError("delivery decision has not been recorded")
            _put_immutable(self.deliveries, delivery.delivery_id, delivery, "delivery")

    def record_outcome(self, outcome: OutcomeRecord) -> None:
        with self._lock:
            if outcome.event_key not in self.events:
                raise LedgerReferenceError("outcome event has not been recorded")
            if outcome.decision_id and outcome.decision_id not in self.decisions:
                raise LedgerReferenceError("outcome decision has not been recorded")
            for existing in self.outcomes.values():
                if (
                    existing.event_key,
                    existing.decision_id,
                    existing.horizon_minutes,
                ) == (outcome.event_key, outcome.decision_id, outcome.horizon_minutes) and (
                    existing.outcome_id != outcome.outcome_id
                ):
                    raise LedgerConflictError("conflicting outcome natural key")
            _put_immutable(self.outcomes, outcome.outcome_id, outcome, "outcome")

    def record_compaction_manifest(self, manifest: CompactionManifestRecord) -> None:
        with self._lock:
            self.compaction_manifests[(manifest.source_path, manifest.source_sha256)] = manifest

    def get_event(self, event_key: str) -> EventRecord | None:
        with self._lock:
            return self.events.get(event_key)

    def get_decision(self, decision_id: str) -> DecisionRecord | None:
        with self._lock:
            return self.decisions.get(decision_id)

    def list_decision_legs(self, decision_id: str) -> tuple[DecisionLegRecord, ...]:
        with self._lock:
            return self.decision_legs.get(decision_id, ())

    def list_deliveries(self, decision_id: str) -> tuple[DeliveryRecord, ...]:
        with self._lock:
            return tuple(
                sorted(
                    (item for item in self.deliveries.values() if item.decision_id == decision_id),
                    key=lambda item: item.attempted_at,
                )
            )

    def list_outcomes(self, decision_id: str) -> tuple[OutcomeRecord, ...]:
        with self._lock:
            return tuple(
                sorted(
                    (item for item in self.outcomes.values() if item.decision_id == decision_id),
                    key=lambda item: item.horizon_minutes,
                )
            )

    def get_compaction_manifest(
        self,
        source_path: str,
        source_sha256: str,
    ) -> CompactionManifestRecord | None:
        with self._lock:
            return self.compaction_manifests.get((source_path, source_sha256))


InMemoryLedger = InMemoryDecisionLedger
