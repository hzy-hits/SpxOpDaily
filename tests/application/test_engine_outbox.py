"""RealtimeEngine + outbox integration: neutral ticks do not grow the outbox."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
from pathlib import Path

from spx_spark.application.realtime.contracts import EngineTick
from spx_spark.application.realtime.engine import RealtimeEngine
from spx_spark.domain.analytics import AnalyticsDiagnostics, AnalyticsResult, AnalyticsStatus
from spx_spark.domain.events import DomainEvent, EventKind
from spx_spark.domain.market import MarketSnapshot
from spx_spark.infrastructure.notifications import (
    NotificationEventQueue,
    create_engine,
    event_rows,
    metadata,
)


NOW = datetime(2026, 7, 11, 17, 0, tzinfo=timezone.utc)


def _quote() -> SimpleNamespace:
    return SimpleNamespace(
        instrument=SimpleNamespace(
            symbol="SPX",
            instrument_type=SimpleNamespace(value="index"),
            underlier=None,
            expiry=None,
        ),
        provider=SimpleNamespace(value="schwab"),
        received_at=NOW,
        quality=SimpleNamespace(value="live"),
        effective_price=6500.0,
    )


def _snapshot() -> MarketSnapshot:
    return MarketSnapshot(
        schema_version=1,
        snapshot_id="snap-neutral",
        as_of=NOW,
        received_at=NOW,
        quotes=(_quote(),),
        provider_states=(),
        source_batch_ids=("b1",),
    )


@dataclass
class Snap:
    def read(self) -> MarketSnapshot:
        return _snapshot()


@dataclass
class Analytics:
    def compute(self, snapshot: MarketSnapshot, *, now: datetime) -> AnalyticsResult:
        return AnalyticsResult(
            schema_version=1,
            result_id="an",
            input_snapshot_id=snapshot.snapshot_id,
            computed_at=now,
            underlier=None,
            expiries=(),
            diagnostics=AnalyticsDiagnostics(0, 0, 1.0, (), {}),
            status=AnalyticsStatus.SUCCESS,
        )


@dataclass
class SilentAlerts:
    def evaluate(self, snapshot, analytics, *, now):  # noqa: ANN001
        return ()


@dataclass
class Proj:
    def __init__(self) -> None:
        self.ticks: list[EngineTick] = []

    def publish(self, tick: EngineTick) -> None:
        self.ticks.append(tick)


def test_neutral_tick_does_not_grow_outbox(tmp_path) -> None:
    store = create_engine(Path(tmp_path))
    metadata.create_all(store)
    outbox = NotificationEventQueue(store, schedule=lambda _event_id: None)
    engine = RealtimeEngine(
        snapshots=Snap(),
        analytics=Analytics(),
        alerts=SilentAlerts(),
        projections=Proj(),
        outbox=outbox,
        front_chain_fresh=True,
    )
    tick = engine.tick(now=NOW)
    assert tick.events == ()
    assert event_rows(store, "missing") == ()


def test_alert_candidate_tick_appends_outbox(tmp_path) -> None:
    store = create_engine(Path(tmp_path))
    metadata.create_all(store)
    outbox = NotificationEventQueue(store, schedule=lambda _event_id: None)
    event = DomainEvent(
        schema_version=1,
        event_id="cand-1",
        kind=EventKind.ALERT_CANDIDATE,
        source_at=NOW,
        available_at=NOW,
        aggregate_id="spx",
        sequence=1,
        payload={"x": 1},
    )

    class Alerts:
        def evaluate(self, snapshot, analytics, *, now):  # noqa: ANN001
            return (event,)

    engine = RealtimeEngine(
        snapshots=Snap(),
        analytics=Analytics(),
        alerts=Alerts(),
        projections=Proj(),
        outbox=outbox,
        front_chain_fresh=True,
    )
    engine.tick(now=NOW)
    assert [(row["channel"], row["status"]) for row in event_rows(store, "cand-1")] == [
        ("alert_pipeline", "pending")
    ]
