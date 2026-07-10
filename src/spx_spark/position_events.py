from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path

from spx_spark.state_io import atomic_write_json_secure, exclusive_state_lock


BOOK_PNL_EVENT_KIND = "spxw_position_book_pnl"


class PositionEventStoreCorrupt(RuntimeError):
    pass


@dataclass(frozen=True)
class ObservedPosition:
    key: str
    instrument_id: str
    label: str
    qty: float

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> ObservedPosition:
        key = str(payload.get("key") or "")
        instrument_id = str(payload.get("instrument_id") or "")
        label = str(payload.get("label") or "")
        qty = _optional_float(payload.get("qty"))
        if not key or not instrument_id or not label or qty is None:
            raise ValueError("invalid observed position")
        return cls(
            key=key,
            instrument_id=instrument_id,
            label=label,
            qty=qty,
        )


@dataclass(frozen=True)
class PositionObservation:
    snapshot_id: str
    observed_at: str
    fetch_complete: bool
    positions: tuple[ObservedPosition, ...]
    book_pnl: float | None
    book_pnl_pct: float | None
    book_pnl_complete: bool
    book_detail: str


@dataclass(frozen=True)
class PendingPositionEvent:
    event_id: str
    snapshot_id: str
    kind: str
    instrument_id: str
    label: str
    created_at: str
    old_qty: float | None = None
    new_qty: float | None = None
    book_pnl: float | None = None
    book_pnl_pct: float | None = None
    book_detail: str | None = None
    pnl_bucket: str | None = None
    severity: str | None = None
    threshold: float | None = None

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> PendingPositionEvent:
        event_id = str(payload.get("event_id") or "")
        snapshot_id = str(payload.get("snapshot_id") or "")
        kind = str(payload.get("kind") or "")
        instrument_id = str(payload.get("instrument_id") or "")
        label = str(payload.get("label") or "")
        created_at = str(payload.get("created_at") or "")
        if not all((event_id, snapshot_id, kind, instrument_id, label, created_at)):
            raise ValueError("invalid pending position event")
        return cls(
            event_id=event_id,
            snapshot_id=snapshot_id,
            kind=kind,
            instrument_id=instrument_id,
            label=label,
            created_at=created_at,
            old_qty=_optional_float(payload.get("old_qty")),
            new_qty=_optional_float(payload.get("new_qty")),
            book_pnl=_optional_float(payload.get("book_pnl")),
            book_pnl_pct=_optional_float(payload.get("book_pnl_pct")),
            book_detail=str(payload.get("book_detail"))
            if payload.get("book_detail") is not None
            else None,
            pnl_bucket=str(payload.get("pnl_bucket"))
            if payload.get("pnl_bucket") is not None
            else None,
            severity=str(payload.get("severity"))
            if payload.get("severity") is not None
            else None,
            threshold=_optional_float(payload.get("threshold")),
        )


@dataclass(frozen=True)
class PositionEventState:
    observed_snapshot_id: str | None = None
    observed_at: str | None = None
    observed_positions: tuple[ObservedPosition, ...] = ()
    pending_events: tuple[PendingPositionEvent, ...] = ()
    last_acknowledged_book_pnl: float | None = None
    updated_at: str | None = None
    schema_version: int = 2

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "observed_snapshot_id": self.observed_snapshot_id,
            "observed_at": self.observed_at,
            "observed_positions": [asdict(position) for position in self.observed_positions],
            "pending_events": [asdict(event) for event in self.pending_events],
            "last_acknowledged_book_pnl": self.last_acknowledged_book_pnl,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True)
class PositionEventBatch:
    pending_events: tuple[PendingPositionEvent, ...]
    accepted_snapshot: bool
    rejection_reason: str | None
    state: PositionEventState


class PositionEventStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def prepare(
        self,
        observation: PositionObservation | None,
        *,
        acknowledged_event_ids: tuple[str, ...] = (),
        as_of: datetime | None = None,
        max_snapshot_age_seconds: float = 180.0,
        pnl_change_usd: float = 200.0,
        pnl_loss_usd: float = 400.0,
        pnl_critical_loss_usd: float = 1000.0,
        pnl_bucket_usd: float = 100.0,
        structural_enabled: bool = True,
        pnl_enabled: bool = True,
    ) -> PositionEventBatch:
        as_of = _as_utc(as_of or datetime.now(tz=timezone.utc))
        with exclusive_state_lock(self.path):
            state = self._load_unlocked()
            state = _acknowledge_state(state, set(acknowledged_event_ids))
            state = _apply_event_class_policy(
                state,
                structural_enabled=structural_enabled,
                pnl_enabled=pnl_enabled,
            )
            accepted = False
            rejection_reason = None
            if observation is not None:
                rejection_reason = _observation_rejection_reason(
                    observation,
                    state=state,
                    as_of=as_of,
                    max_snapshot_age_seconds=max_snapshot_age_seconds,
                )
                if rejection_reason is None:
                    state = _derive_events(
                        state,
                        observation,
                        pnl_change_usd=pnl_change_usd,
                        pnl_loss_usd=pnl_loss_usd,
                        pnl_critical_loss_usd=pnl_critical_loss_usd,
                        pnl_bucket_usd=pnl_bucket_usd,
                        structural_enabled=structural_enabled,
                        pnl_enabled=pnl_enabled,
                    )
                    accepted = True
            state = replace(state, updated_at=as_of.isoformat())
            atomic_write_json_secure(self.path, state.to_dict())
            return PositionEventBatch(
                pending_events=state.pending_events,
                accepted_snapshot=accepted,
                rejection_reason=rejection_reason,
                state=state,
            )

    def acknowledge(
        self,
        event_ids: tuple[str, ...],
        *,
        as_of: datetime | None = None,
    ) -> PositionEventState:
        batch = self.prepare(
            None,
            acknowledged_event_ids=event_ids,
            as_of=as_of,
        )
        return batch.state

    def load(self) -> PositionEventState:
        with exclusive_state_lock(self.path):
            return self._load_unlocked()

    def _load_unlocked(self) -> PositionEventState:
        if not self.path.exists():
            return PositionEventState()
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise PositionEventStoreCorrupt(
                f"position event state is unreadable: {self.path}"
            ) from exc
        if not isinstance(payload, dict):
            raise PositionEventStoreCorrupt(
                f"position event state is not a JSON object: {self.path}"
            )
        try:
            schema_version = int(payload.get("schema_version") or 1)
        except (TypeError, ValueError) as exc:
            raise PositionEventStoreCorrupt(
                f"invalid position event state schema: {self.path}"
            ) from exc
        if schema_version == 1:
            return _migrate_v1(payload)
        if schema_version != 2:
            raise PositionEventStoreCorrupt(
                f"unsupported position event state schema {schema_version}: {self.path}"
            )
        observed_payload = payload.get("observed_positions") or []
        pending_payload = payload.get("pending_events") or []
        if not isinstance(observed_payload, list) or not isinstance(pending_payload, list):
            raise PositionEventStoreCorrupt(
                f"invalid position event state collections: {self.path}"
            )
        if not all(isinstance(item, dict) for item in observed_payload):
            raise PositionEventStoreCorrupt(
                f"invalid observed position entries: {self.path}"
            )
        if not all(isinstance(item, dict) for item in pending_payload):
            raise PositionEventStoreCorrupt(
                f"invalid pending position event entries: {self.path}"
            )
        try:
            return PositionEventState(
                observed_snapshot_id=str(payload.get("observed_snapshot_id"))
                if payload.get("observed_snapshot_id")
                else None,
                observed_at=str(payload.get("observed_at"))
                if payload.get("observed_at")
                else None,
                observed_positions=tuple(
                    ObservedPosition.from_dict(item)
                    for item in observed_payload
                ),
                pending_events=tuple(
                    PendingPositionEvent.from_dict(item)
                    for item in pending_payload
                ),
                last_acknowledged_book_pnl=_optional_float(
                    payload.get("last_acknowledged_book_pnl")
                ),
                updated_at=str(payload.get("updated_at"))
                if payload.get("updated_at")
                else None,
            )
        except (TypeError, ValueError, OverflowError) as exc:
            raise PositionEventStoreCorrupt(
                f"invalid position event state fields: {self.path}"
            ) from exc


def _optional_float(value: object) -> float | None:
    if not isinstance(value, int | float):
        return None
    result = float(value)
    return result if math.isfinite(result) else None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return _as_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        return None


def _label_from_position_key(key: str) -> tuple[str, str]:
    _account, separator, instrument_id = key.partition("|")
    if not separator:
        instrument_id = key
    parts = instrument_id.split(":")
    if len(parts) >= 6 and parts[0] == "option":
        return instrument_id, f"SPXW {parts[3]} {parts[4]}{parts[5]}"
    return instrument_id, instrument_id or "SPXW"


def _migrate_v1(payload: dict[str, object]) -> PositionEventState:
    previous_qty = payload.get("previous_qty") or {}
    if not isinstance(previous_qty, dict):
        raise PositionEventStoreCorrupt("invalid version-1 position quantities")
    positions = []
    for key, qty in sorted(previous_qty.items(), key=lambda item: str(item[0])):
        numeric_qty = _optional_float(qty)
        if numeric_qty is None:
            continue
        instrument_id, label = _label_from_position_key(str(key))
        positions.append(
            ObservedPosition(
                key=str(key),
                instrument_id=instrument_id,
                label=label,
                qty=numeric_qty,
            )
        )
    return PositionEventState(
        observed_at=str(payload.get("fetched_at") or payload.get("updated_at") or "") or None,
        observed_positions=tuple(positions),
        last_acknowledged_book_pnl=_optional_float(payload.get("book_pnl")),
    )


def _acknowledge_state(
    state: PositionEventState,
    event_ids: set[str],
) -> PositionEventState:
    if not event_ids:
        return state
    kept = []
    last_acknowledged_book_pnl = state.last_acknowledged_book_pnl
    for event in state.pending_events:
        if event.event_id not in event_ids:
            kept.append(event)
            continue
        if event.kind == BOOK_PNL_EVENT_KIND and event.book_pnl is not None:
            last_acknowledged_book_pnl = event.book_pnl
    return replace(
        state,
        pending_events=tuple(kept),
        last_acknowledged_book_pnl=last_acknowledged_book_pnl,
    )


def _apply_event_class_policy(
    state: PositionEventState,
    *,
    structural_enabled: bool,
    pnl_enabled: bool,
) -> PositionEventState:
    pending_events = list(state.pending_events)
    last_acknowledged_book_pnl = state.last_acknowledged_book_pnl
    if not pnl_enabled:
        for event in pending_events:
            if event.kind == BOOK_PNL_EVENT_KIND and event.book_pnl is not None:
                last_acknowledged_book_pnl = event.book_pnl
        pending_events = [
            event for event in pending_events if event.kind != BOOK_PNL_EVENT_KIND
        ]
    if not structural_enabled:
        pending_events = [
            event for event in pending_events if event.kind == BOOK_PNL_EVENT_KIND
        ]
    return replace(
        state,
        pending_events=tuple(pending_events),
        last_acknowledged_book_pnl=last_acknowledged_book_pnl,
    )


def _observation_rejection_reason(
    observation: PositionObservation,
    *,
    state: PositionEventState,
    as_of: datetime,
    max_snapshot_age_seconds: float,
) -> str | None:
    if not observation.fetch_complete:
        return "snapshot_incomplete"
    if not observation.snapshot_id:
        return "snapshot_id_missing"
    if observation.snapshot_id == state.observed_snapshot_id:
        return "snapshot_duplicate"
    observed_at = _parse_utc(observation.observed_at)
    if observed_at is None:
        return "snapshot_time_invalid"
    age_seconds = (as_of - observed_at).total_seconds()
    if age_seconds < -5.0:
        return "snapshot_from_future"
    if age_seconds > max_snapshot_age_seconds:
        return "snapshot_stale"
    previous_at = _parse_utc(state.observed_at)
    if previous_at is not None and observed_at <= previous_at:
        return "snapshot_non_monotonic"
    return None


def _event_id(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _structural_event(
    *,
    observation: PositionObservation,
    kind: str,
    position: ObservedPosition,
    old_qty: float | None,
    new_qty: float | None,
) -> PendingPositionEvent:
    event_id = _event_id(
        {
            "snapshot_id": observation.snapshot_id,
            "kind": kind,
            "position_key": position.key,
            "old_qty": old_qty,
            "new_qty": new_qty,
        }
    )
    return PendingPositionEvent(
        event_id=event_id,
        snapshot_id=observation.snapshot_id,
        kind=kind,
        instrument_id=position.instrument_id,
        label=position.label,
        created_at=observation.observed_at,
        old_qty=old_qty,
        new_qty=new_qty,
    )


def _pnl_bucket(value: float, step_usd: float) -> str:
    if step_usd <= 0:
        return f"{value:.0f}"
    bucket = math.floor(value / step_usd)
    return f"{bucket * int(step_usd)}"


def _pnl_severity(value: float, *, loss_usd: float, critical_loss_usd: float) -> str:
    if value <= -critical_loss_usd:
        return "critical"
    if value <= -loss_usd:
        return "high"
    return "medium"


def _pending_pnl_event(
    events: tuple[PendingPositionEvent, ...],
) -> PendingPositionEvent | None:
    return next((event for event in events if event.kind == BOOK_PNL_EVENT_KIND), None)


def _derive_events(
    state: PositionEventState,
    observation: PositionObservation,
    *,
    pnl_change_usd: float,
    pnl_loss_usd: float,
    pnl_critical_loss_usd: float,
    pnl_bucket_usd: float,
    structural_enabled: bool,
    pnl_enabled: bool,
) -> PositionEventState:
    previous = {position.key: position for position in state.observed_positions}
    current = {position.key: position for position in observation.positions}
    pending = list(state.pending_events)
    for key in sorted(set(previous) | set(current)) if structural_enabled else ():
        old = previous.get(key)
        new = current.get(key)
        if old is None and new is not None and new.qty != 0:
            pending.append(
                _structural_event(
                    observation=observation,
                    kind="spxw_position_opened",
                    position=new,
                    old_qty=None,
                    new_qty=new.qty,
                )
            )
        elif old is not None and new is None and old.qty != 0:
            pending.append(
                _structural_event(
                    observation=observation,
                    kind="spxw_position_closed",
                    position=old,
                    old_qty=old.qty,
                    new_qty=0.0,
                )
            )
        elif old is not None and new is not None and old.qty != new.qty:
            pending.append(
                _structural_event(
                    observation=observation,
                    kind="spxw_position_qty_changed",
                    position=new,
                    old_qty=old.qty,
                    new_qty=new.qty,
                )
            )

    last_acknowledged_book_pnl = state.last_acknowledged_book_pnl
    if (
        pnl_enabled
        and observation.positions
        and observation.book_pnl_complete
        and observation.book_pnl is not None
    ):
        book_pnl = observation.book_pnl
        baseline = state.last_acknowledged_book_pnl
        bucket = _pnl_bucket(book_pnl, pnl_bucket_usd)
        severity = _pnl_severity(
            book_pnl,
            loss_usd=pnl_loss_usd,
            critical_loss_usd=pnl_critical_loss_usd,
        )
        qualifies = False
        if baseline is None:
            qualifies = book_pnl <= -pnl_loss_usd
        else:
            baseline_bucket = _pnl_bucket(baseline, pnl_bucket_usd)
            qualifies = (
                abs(book_pnl - baseline) >= pnl_change_usd
                or bucket != baseline_bucket
                or (baseline > -pnl_loss_usd and book_pnl <= -pnl_loss_usd)
            )
        existing_pnl = _pending_pnl_event(tuple(pending))
        should_replace = existing_pnl is None or (
            existing_pnl.pnl_bucket != bucket or existing_pnl.severity != severity
        )
        if qualifies and should_replace:
            pending = [event for event in pending if event.kind != BOOK_PNL_EVENT_KIND]
            event_payload = {
                "snapshot_id": observation.snapshot_id,
                "kind": BOOK_PNL_EVENT_KIND,
                "pnl_bucket": bucket,
                "severity": severity,
            }
            pending.append(
                PendingPositionEvent(
                    event_id=_event_id(event_payload),
                    snapshot_id=observation.snapshot_id,
                    kind=BOOK_PNL_EVENT_KIND,
                    instrument_id="option_map:SPXW",
                    label="SPXW book",
                    created_at=observation.observed_at,
                    book_pnl=book_pnl,
                    book_pnl_pct=observation.book_pnl_pct,
                    book_detail=observation.book_detail,
                    pnl_bucket=bucket,
                    severity=severity,
                    threshold=-pnl_loss_usd if book_pnl < 0 else pnl_loss_usd,
                )
            )

    if (
        not pnl_enabled
        and observation.book_pnl_complete
        and observation.book_pnl is not None
    ):
        last_acknowledged_book_pnl = observation.book_pnl

    return replace(
        state,
        observed_snapshot_id=observation.snapshot_id,
        observed_at=observation.observed_at,
        observed_positions=tuple(sorted(observation.positions, key=lambda item: item.key)),
        pending_events=tuple(pending),
        last_acknowledged_book_pnl=last_acknowledged_book_pnl,
    )
