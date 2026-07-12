"""RealtimeEngine: snapshot → analytics → alerts → projection/outbox."""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone

from spx_spark.application.realtime.contracts import (
    AlertEvaluator,
    AnalyticsKernel,
    EngineTick,
    EventOutbox,
    ProjectionStore,
    SnapshotSource,
)
from spx_spark.application.realtime.health import evaluate_engine_health
from spx_spark.application.realtime.options_kernel import (
    ChainFreshnessThresholds,
    evaluate_front_chain_fresh,
)
from spx_spark.domain.analytics import AnalyticsResult, AnalyticsStatus
from spx_spark.domain.events import DomainEvent
from spx_spark.domain.health import EngineHealth, EngineMode
from spx_spark.domain.market import MarketSnapshot


def _default_tick_id(now: datetime) -> str:
    return f"tick:{now.strftime('%Y%m%dT%H%M%S')}:{uuid.uuid4().hex[:8]}"


def snapshot_has_tradfi_anchor(snapshot: MarketSnapshot) -> bool:
    """True when at least one direct TradFi SPX-style underlier quote is present."""

    for quote in snapshot.quotes:
        quality = str(getattr(quote.quality, "value", quote.quality)).lower()
        price = getattr(quote, "effective_price", None)
        if quality != "live" or price is None or price <= 0:
            continue
        instrument = quote.instrument
        symbol = str(getattr(instrument, "symbol", "")).upper()
        underlier = str(getattr(instrument, "underlier", "") or "").upper()
        kind = str(getattr(instrument.instrument_type, "value", instrument.instrument_type)).lower()
        if kind in {"index", "stock", "etf", "future"} and (
            symbol in {"SPX", "SPXW", "ES", "$SPX"} or underlier in {"SPX", "$SPX"}
        ):
            return True
        if symbol in {"INDEX:SPX", "FUTURE:ES"} or symbol.endswith(":SPX"):
            return True
    return False


def snapshot_has_fresh_spxw_chain(
    snapshot: MarketSnapshot,
    *,
    now: datetime | None = None,
    thresholds: ChainFreshnessThresholds | None = None,
) -> bool:
    """True when front-month SPXW passes age / strikes / two-sided / wing gates."""

    return evaluate_front_chain_fresh(snapshot, now=now, thresholds=thresholds)


def analytics_result_ok(result: AnalyticsResult | None) -> bool:
    """Require explicit SUCCESS status — non-None / non-throwing is not enough."""

    return result is not None and result.status is AnalyticsStatus.SUCCESS


class RealtimeEngine:
    """Orchestrates one analytics tick without side-channel I/O beyond ports."""

    def __init__(
        self,
        *,
        snapshots: SnapshotSource,
        analytics: AnalyticsKernel,
        alerts: AlertEvaluator,
        projections: ProjectionStore,
        outbox: EventOutbox,
        critical_tasks_healthy: bool = True,
        front_chain_fresh: bool | None = None,
        chain_thresholds: ChainFreshnessThresholds | None = None,
        warmed_up: bool = True,
    ) -> None:
        self.snapshots = snapshots
        self.analytics = analytics
        self.alerts = alerts
        self.projections = projections
        self.outbox = outbox
        self.critical_tasks_healthy = critical_tasks_healthy
        self.front_chain_fresh = front_chain_fresh
        self.chain_thresholds = chain_thresholds or ChainFreshnessThresholds()
        self.warmed_up = warmed_up
        self._mode = EngineMode.STARTING

    @property
    def mode(self) -> EngineMode:
        return self._mode

    def tick(self, *, now: datetime | None = None) -> EngineTick:
        started = time.perf_counter()
        now = now or datetime.now(tz=timezone.utc)
        analytics_result: AnalyticsResult | None = None
        events: tuple[DomainEvent, ...] = ()
        engine_failed = False
        source_snapshot_id = ""
        try:
            snapshot = self.snapshots.read()
            snapshot.validate()
            source_snapshot_id = snapshot.snapshot_id
            analytics_result = self.analytics.compute(snapshot, now=now)
            events = self.alerts.evaluate(snapshot, analytics_result, now=now)
            if events:
                self.outbox.append(events)
            health = self._health(
                snapshot=snapshot,
                analytics_result=analytics_result,
                now=now,
                engine_failed=False,
            )
            tick = EngineTick(
                tick_id=_default_tick_id(now),
                started_at=now,
                source_snapshot_id=source_snapshot_id,
                analytics=analytics_result,
                events=events,
                health=health,
                duration_ms=(time.perf_counter() - started) * 1000.0,
            )
            self.projections.publish(tick)
            self._mode = health.mode
            return tick
        except Exception:
            engine_failed = True
            try:
                outbox_writable = self.outbox.writable()
            except Exception:  # noqa: BLE001
                outbox_writable = False
            health = evaluate_engine_health(
                tradfi_anchor_usable=False,
                front_chain_fresh=False,
                analytics_succeeded=False,
                outbox_writable=outbox_writable,
                critical_tasks_healthy=self.critical_tasks_healthy,
                checked_at=now,
                engine_failed=True,
                warmed_up=self.warmed_up,
                any_critical_success=self.warmed_up,
            )
            self._mode = health.mode
            tick = EngineTick(
                tick_id=_default_tick_id(now),
                started_at=now,
                source_snapshot_id=source_snapshot_id or "unavailable",
                analytics=None,
                events=(),
                health=health,
                duration_ms=(time.perf_counter() - started) * 1000.0,
            )
            if engine_failed:
                try:
                    self.projections.publish(tick)
                except Exception:  # noqa: BLE001
                    pass
            return tick

    def _health(
        self,
        *,
        snapshot: MarketSnapshot,
        analytics_result: AnalyticsResult | None,
        now: datetime,
        engine_failed: bool,
    ) -> EngineHealth:
        try:
            outbox_writable = self.outbox.writable()
        except Exception:  # noqa: BLE001
            outbox_writable = False
        return evaluate_engine_health(
            tradfi_anchor_usable=snapshot_has_tradfi_anchor(snapshot),
            front_chain_fresh=(
                snapshot_has_fresh_spxw_chain(
                    snapshot, now=now, thresholds=self.chain_thresholds
                )
                if self.front_chain_fresh is None
                else self.front_chain_fresh
            ),
            analytics_succeeded=analytics_result_ok(analytics_result),
            outbox_writable=outbox_writable,
            critical_tasks_healthy=self.critical_tasks_healthy,
            checked_at=now,
            engine_failed=engine_failed,
            warmed_up=self.warmed_up,
            any_critical_success=self.warmed_up,
        )
