"""RealtimeEngine tick orchestration tests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from spx_spark.application.realtime.contracts import EngineTick
from spx_spark.application.realtime.engine import (
    RealtimeEngine,
    snapshot_has_fresh_spxw_chain,
    snapshot_has_tradfi_anchor,
)
from spx_spark.domain.analytics import AnalyticsDiagnostics, AnalyticsResult, AnalyticsStatus
from spx_spark.domain.events import AppendResult, DomainEvent, EventKind
from spx_spark.domain.health import EngineMode
from spx_spark.domain.market import MarketSnapshot
from spx_spark.marketdata import InstrumentId, MarketDataQuality, Provider, Quote

NOW = datetime(2026, 7, 13, 15, 0, tzinfo=timezone.utc)


def _quote(symbol: str = "SPX", kind: str = "index") -> SimpleNamespace:
    return SimpleNamespace(
        instrument=SimpleNamespace(
            canonical_id=f"future:{symbol}" if kind == "future" else f"index:{symbol}",
            symbol=symbol,
            instrument_type=SimpleNamespace(value=kind),
            underlier=None,
            expiry=None,
        ),
        provider=SimpleNamespace(value="schwab"),
        received_at=NOW,
        quality=SimpleNamespace(value="live"),
        effective_price=6500.0,
    )


def _snapshot(*, quotes=()) -> MarketSnapshot:
    return MarketSnapshot(
        schema_version=1,
        snapshot_id="snap-1",
        as_of=NOW,
        received_at=NOW,
        quotes=tuple(quotes),
        provider_states=(),
        source_batch_ids=("batch-1",),
    )


@dataclass
class FakeSnapshotSource:
    snapshot: MarketSnapshot

    def read(self) -> MarketSnapshot:
        return self.snapshot


@dataclass
class FakeAnalytics:
    fail: bool = False
    status: AnalyticsStatus = AnalyticsStatus.SUCCESS

    def compute(self, snapshot: MarketSnapshot, *, now: datetime) -> AnalyticsResult:
        if self.fail:
            raise RuntimeError("analytics boom")
        return AnalyticsResult(
            schema_version=1,
            result_id="an-1",
            input_snapshot_id=snapshot.snapshot_id,
            computed_at=now,
            underlier=None,
            expiries=(),
            diagnostics=AnalyticsDiagnostics(
                input_legs=0,
                usable_legs=0,
                duration_ms=1.0,
                warnings=(),
                model_versions={},
            ),
            status=self.status,
        )


@dataclass
class FakeAlerts:
    events: tuple[DomainEvent, ...] = ()

    def evaluate(self, snapshot, analytics, *, now):  # noqa: ANN001
        return self.events


@dataclass
class FakeOutbox:
    writable_flag: bool = True
    appended: list = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.appended is None:
            self.appended = []

    def writable(self) -> bool:
        return self.writable_flag

    def append(self, events):  # noqa: ANN001
        self.appended.extend(events)
        return AppendResult(accepted=len(events), writable=self.writable_flag)


@dataclass
class FakeProjection:
    ticks: list[EngineTick]

    def __init__(self) -> None:
        self.ticks = []

    def publish(self, tick: EngineTick) -> None:
        self.ticks.append(tick)


def test_snapshot_has_tradfi_anchor() -> None:
    assert snapshot_has_tradfi_anchor(_snapshot(quotes=[_quote()])) is True
    assert snapshot_has_tradfi_anchor(_snapshot(quotes=[])) is False


def test_realtime_engine_uses_globex_context_when_cash_analytics_are_unavailable() -> None:
    globex_now = datetime(2026, 7, 13, 1, 30, tzinfo=timezone.utc)
    es = _quote(symbol="ES", kind="future")
    engine = RealtimeEngine(
        snapshots=FakeSnapshotSource(_snapshot(quotes=[es])),
        analytics=FakeAnalytics(status=AnalyticsStatus.DEGRADED),
        alerts=FakeAlerts(),
        projections=FakeProjection(),
        outbox=FakeOutbox(),
        front_chain_fresh=False,
    )

    tick = engine.tick(now=globex_now)

    assert tick.health.mode is EngineMode.GLOBEX_CONTEXT
    assert tick.health.ok is True
    assert tick.health.actionable is False
    assert tick.health.factors["globex_context_usable"] is True


def test_realtime_engine_is_ready_with_live_gth_option_analytics() -> None:
    globex_now = datetime(2026, 7, 13, 1, 30, tzinfo=timezone.utc)
    es = _quote(symbol="ES", kind="future")
    engine = RealtimeEngine(
        snapshots=FakeSnapshotSource(_snapshot(quotes=[es])),
        analytics=FakeAnalytics(status=AnalyticsStatus.SUCCESS),
        alerts=FakeAlerts(),
        projections=FakeProjection(),
        outbox=FakeOutbox(),
        front_chain_fresh=True,
    )

    tick = engine.tick(now=globex_now)

    assert tick.health.mode is EngineMode.READY
    assert tick.health.actionable is True
    assert tick.health.reasons == ("cash_session_closed_live_option_chain",)


def test_realtime_engine_tick_ready() -> None:
    event = DomainEvent(
        schema_version=1,
        event_id="e1",
        kind=EventKind.ALERT_CANDIDATE,
        source_at=NOW,
        available_at=NOW,
        aggregate_id="spx",
        sequence=1,
        payload={"play": "test"},
    )
    outbox = FakeOutbox()
    projections = FakeProjection()
    engine = RealtimeEngine(
        snapshots=FakeSnapshotSource(_snapshot(quotes=[_quote()])),
        analytics=FakeAnalytics(),
        alerts=FakeAlerts(events=(event,)),
        projections=projections,
        outbox=outbox,
        critical_tasks_healthy=True,
        front_chain_fresh=True,
    )
    tick = engine.tick(now=NOW)
    assert tick.health.mode is EngineMode.READY
    assert tick.health.ok is True
    assert tick.analytics is not None
    assert outbox.appended == [event]
    assert projections.ticks == [tick]


def test_realtime_engine_blocks_on_outbox_failure() -> None:
    engine = RealtimeEngine(
        snapshots=FakeSnapshotSource(_snapshot(quotes=[_quote()])),
        analytics=FakeAnalytics(),
        alerts=FakeAlerts(),
        projections=FakeProjection(),
        outbox=FakeOutbox(writable_flag=False),
        front_chain_fresh=True,
    )
    tick = engine.tick(now=NOW)
    assert tick.health.mode is EngineMode.BLOCKED
    assert tick.health.ok is False
    assert "outbox_writable_failed" in tick.health.reasons


def test_realtime_engine_failed_on_analytics_exception() -> None:
    engine = RealtimeEngine(
        snapshots=FakeSnapshotSource(_snapshot(quotes=[_quote()])),
        analytics=FakeAnalytics(fail=True),
        alerts=FakeAlerts(),
        projections=FakeProjection(),
        outbox=FakeOutbox(),
    )
    tick = engine.tick(now=NOW)
    assert tick.health.mode is EngineMode.FAILED
    assert tick.analytics is None
    assert tick.health.factors["outbox_writable"] is True


def test_realtime_engine_blocks_when_anchor_is_stale() -> None:
    stale = _quote()
    stale.quality = SimpleNamespace(value="stale")
    engine = RealtimeEngine(
        snapshots=FakeSnapshotSource(_snapshot(quotes=[stale])),
        analytics=FakeAnalytics(),
        alerts=FakeAlerts(),
        projections=FakeProjection(),
        outbox=FakeOutbox(),
        front_chain_fresh=True,
    )

    tick = engine.tick(now=NOW)

    assert tick.health.mode is EngineMode.BLOCKED
    assert tick.health.factors["tradfi_anchor"] is False


def test_front_chain_freshness_rejects_single_option_without_structure() -> None:
    option = _quote(symbol="SPX", kind="option")
    option.instrument.underlier = "SPX"
    option.instrument.trading_class = "SPXW"
    option.instrument.expiry = "20260713"
    option.instrument.strike = 6500.0
    option.instrument.right = SimpleNamespace(value="C")
    option.quote_age_ms = lambda _now: 1000.0
    option.quality = SimpleNamespace(value="live")
    option.mid = 10.0
    # A lone option cannot satisfy usable-strikes / two-sided / wing gates.
    assert snapshot_has_fresh_spxw_chain(_snapshot(quotes=[option]), now=NOW) is False


def test_front_chain_freshness_ignores_stale_provider_rows_when_live_fallback_exists() -> None:
    quotes: list[Quote] = [
        Quote(
            instrument=InstrumentId.index("SPX"),
            provider=Provider.IBKR,
            received_at=NOW,
            quality=MarketDataQuality.LIVE,
            bid=7499.0,
            ask=7501.0,
            quote_time=NOW,
        )
    ]
    for strike in range(7440, 7565, 5):
        for right in ("C", "P"):
            instrument = InstrumentId.option(
                "SPX",
                expiry="20260713",
                strike=strike,
                right=right,
                trading_class="SPXW",
            )
            quotes.extend(
                (
                    Quote(
                        instrument=instrument,
                        provider=Provider.SCHWAB,
                        received_at=NOW - timedelta(hours=1),
                        quality=MarketDataQuality.STALE,
                        bid=1.0,
                        ask=1.2,
                        quote_time=NOW - timedelta(hours=1),
                    ),
                    Quote(
                        instrument=instrument,
                        provider=Provider.IBKR,
                        received_at=NOW,
                        quality=MarketDataQuality.LIVE,
                        bid=1.0,
                        ask=1.2,
                        quote_time=NOW,
                    ),
                )
            )

    assert snapshot_has_fresh_spxw_chain(_snapshot(quotes=quotes), now=NOW) is True


def test_analytics_ok_requires_explicit_success_status() -> None:
    engine = RealtimeEngine(
        snapshots=FakeSnapshotSource(_snapshot(quotes=[_quote()])),
        analytics=FakeAnalytics(status=AnalyticsStatus.FAILED),
        alerts=FakeAlerts(),
        projections=FakeProjection(),
        outbox=FakeOutbox(),
        front_chain_fresh=True,
    )
    tick = engine.tick(now=NOW)
    assert tick.analytics is not None
    assert tick.health.factors["analytics_ok"] is False
    assert tick.health.mode is EngineMode.BLOCKED


def test_engine_starting_when_not_warmed_up() -> None:
    engine = RealtimeEngine(
        snapshots=FakeSnapshotSource(_snapshot(quotes=[_quote()])),
        analytics=FakeAnalytics(),
        alerts=FakeAlerts(),
        projections=FakeProjection(),
        outbox=FakeOutbox(),
        front_chain_fresh=True,
        warmed_up=False,
    )
    tick = engine.tick(now=NOW)
    assert tick.health.mode is EngineMode.STARTING
    assert tick.health.ok is False
