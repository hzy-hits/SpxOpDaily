"""24h composition root for RealtimeEngine + outbox + idempotent consumer."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from spx_spark.application.notifications.deliver import make_deliver_alert_candidate
from spx_spark.application.notifications.outbox_consumer import (
    ConsumeResult,
    IdempotentOutboxConsumer,
)
from spx_spark.application.order_map.level_decision_shadow import (
    run_level_decision_shadow,
)
from spx_spark.application.order_map.level_trigger_repricing import (
    run_level_trigger_repricing,
)
from spx_spark.application.order_map.pricing_outcomes import advance_pricing_outcomes
from spx_spark.application.realtime.alert_evaluator import AlertEngineEvaluator
from spx_spark.application.realtime.contracts import AnalyticsKernel, EngineTick
from spx_spark.application.realtime.engine import RealtimeEngine
from spx_spark.application.realtime.options_kernel import (
    ChainFreshnessThresholds,
    OptionsAnalyticsKernel,
)
from spx_spark.config import (
    NotificationSettings,
    StorageSettings,
    direct_alert_delivery_enabled,
    outbox_alert_evaluation_enabled,
    outbox_delivery_enabled,
)
from spx_spark.domain.analytics import (
    AnalyticsDiagnostics,
    AnalyticsResult,
    AnalyticsStatus,
)
from spx_spark.domain.events import DomainEvent, EventKind
from spx_spark.domain.health import EngineMode
from spx_spark.domain.market import MarketSnapshot
from spx_spark.infrastructure.ledger.outbox import SqliteEventOutbox
from spx_spark.infrastructure.ledger.processed_ids import DurableProcessedIdSet
from spx_spark.settings import AppSettings, load_app_settings
from spx_spark.settings.alerts import AlertSettings
from spx_spark.settings.analytics import AnalyticsSettings
from spx_spark.storage import LatestMarketProjectionStore


DeliverFn = Callable[[DomainEvent], bool]


def default_runtime_defaults_path() -> Path:
    """Resolve config/runtime.yaml relative to the repository root."""

    from spx_spark.settings.loader import default_defaults_path

    return default_defaults_path()


def load_production_settings(
    *,
    defaults_path: Path | None = None,
    deployment_path: Path | None = None,
) -> AppSettings:
    """Composition-root settings load — single call per process entry."""

    return load_app_settings(
        defaults_path=defaults_path,
        deployment_path=deployment_path,
    )


def default_outbox_path(storage: StorageSettings) -> Path:
    return Path(storage.data_root) / "ledger" / "domain_event_outbox.sqlite"


def default_processed_ids_path(storage: StorageSettings) -> Path:
    return Path(storage.data_root) / "ledger" / "outbox_processed_ids.json"


def market_snapshot_from_projection(
    store: LatestMarketProjectionStore,
    *,
    now: datetime | None = None,
) -> MarketSnapshot:
    """Build a domain MarketSnapshot from the latest projection store."""

    now = now or datetime.now(tz=timezone.utc)
    state = store.load(now=now)
    snapshot_id = f"proj:{state.as_of.strftime('%Y%m%dT%H%M%S')}:{uuid.uuid4().hex[:8]}"
    return MarketSnapshot(
        schema_version=1,
        snapshot_id=snapshot_id,
        as_of=state.as_of,
        received_at=state.created_at,
        quotes=tuple(state.quotes),
        provider_states=tuple(state.provider_states),
        source_batch_ids=(f"latest:{store.path}",),
    )


@dataclass
class ProjectionSnapshotSource:
    store: LatestMarketProjectionStore

    def read(self) -> MarketSnapshot:
        return market_snapshot_from_projection(self.store)


@dataclass
class PassthroughAnalytics:
    """Shadow / unit-test kernel only — not for production composition.

    Returns explicit SUCCESS so differential shadow can compare structure
    without implying a real front-month options compute.
    """

    def compute(self, snapshot: MarketSnapshot, *, now: datetime) -> AnalyticsResult:
        usable = len(snapshot.quotes)
        return AnalyticsResult(
            schema_version=1,
            result_id=f"an:{snapshot.snapshot_id}",
            input_snapshot_id=snapshot.snapshot_id,
            computed_at=now,
            underlier=None,
            expiries=(),
            diagnostics=AnalyticsDiagnostics(
                input_legs=usable,
                usable_legs=usable,
                duration_ms=0.0,
                warnings=("passthrough_shadow",),
                model_versions={"passthrough": "1"},
            ),
            status=AnalyticsStatus.SUCCESS,
        )


def resolve_analytics_kernel(
    analytics_settings: AnalyticsSettings | None = None,
    *,
    analytics: AnalyticsKernel | None = None,
) -> AnalyticsKernel:
    """Production default is OptionsAnalyticsKernel; passthrough only via flag."""

    if analytics is not None:
        return analytics
    settings = analytics_settings or AnalyticsSettings()
    if settings.passthrough_shadow_mode:
        return PassthroughAnalytics()
    return OptionsAnalyticsKernel(policy=settings)


@dataclass
class SilentAlertEvaluator:
    """No-op evaluator used when outbox alert evaluation is disabled."""

    def evaluate(self, snapshot, analytics, *, now):  # noqa: ANN001
        return ()


@dataclass
class TickProjectionSink:
    """Records the last engine tick for telemetry (no external side effects)."""

    last_tick: EngineTick | None = None
    ticks: list[EngineTick] = field(default_factory=list)

    def publish(self, tick: EngineTick) -> None:
        self.last_tick = tick
        self.ticks.append(tick)


def log_only_deliver(event: DomainEvent) -> bool:
    """Safe sink: acknowledge without external notification IO (shadow / disabled)."""

    _ = event
    return True


def resolve_alert_evaluator(
    store: LatestMarketProjectionStore,
    *,
    evaluation_enabled: bool | None = None,
    alert_settings: AlertSettings | None = None,
):
    if evaluation_enabled is None:
        evaluation_enabled = outbox_alert_evaluation_enabled()
    if evaluation_enabled:
        return AlertEngineEvaluator(store, alert_settings=alert_settings)
    return SilentAlertEvaluator()


def resolve_deliver_fn(
    *,
    delivery_enabled: bool | None = None,
    notification_settings: NotificationSettings | None = None,
    deliver: DeliverFn | None = None,
) -> DeliverFn:
    """Pick outbox deliver sink.

    Explicit ``deliver`` wins. Otherwise outbox_delivery_enabled selects
    notify_payload vs log-only. When direct_delivery_enabled is also true,
    outbox stays log-only so alert_engine owns human notifications (no double-send).
    """

    if deliver is not None:
        return deliver
    if delivery_enabled is None:
        delivery_enabled = outbox_delivery_enabled()
    if not delivery_enabled:
        return log_only_deliver
    if direct_alert_delivery_enabled():
        # Dual-path cutover: evaluation may still fill the outbox, but live
        # notify stays on alert_engine until direct_delivery is flipped off.
        return log_only_deliver
    return make_deliver_alert_candidate(notification_settings)


@dataclass
class RealtimeRuntime:
    """Wired RealtimeEngine + outbox consumer for one service-loop cycle."""

    engine: RealtimeEngine
    consumer: IdempotentOutboxConsumer
    outbox: SqliteEventOutbox
    projections: TickProjectionSink
    processed_ids: DurableProcessedIdSet
    storage: StorageSettings

    def run_cycle(self, *, now: datetime | None = None, consume_limit: int = 20) -> "CycleResult":
        now = now or datetime.now(tz=timezone.utc)
        tick = self.engine.tick(now=now)
        consume = self.consumer.consume(
            limit=consume_limit,
            kinds=(EventKind.ALERT_CANDIDATE,),
            now=now,
        )
        # BLOCKED/DEGRADED/STARTING/WARMING are valid observations, not process
        # failures. The service-loop heartbeat carries readiness separately.
        ok = tick.health.mode is not EngineMode.FAILED
        return CycleResult(tick=tick, consume=consume, ok=ok)


@dataclass(frozen=True)
class CycleResult:
    tick: EngineTick
    consume: ConsumeResult
    ok: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "mode": self.tick.health.mode.value,
            "tick": self.tick.to_dict(),
            "consume": {
                "claimed": self.consume.claimed,
                "delivered": self.consume.delivered,
                "duplicate_skipped": self.consume.duplicate_skipped,
                "failed": self.consume.failed,
                "dead_lettered": self.consume.dead_lettered,
                "acked_ids": list(self.consume.acked_ids),
            },
            "outbox_writable": self.tick.health.factors.get("outbox_writable"),
        }


def build_realtime_runtime(
    storage: StorageSettings | None = None,
    *,
    deliver: DeliverFn | None = None,
    consumer_id: str = "notifier-24h",
    critical_tasks_healthy: bool = True,
    front_chain_fresh: bool | None = None,
    outbox_path: Path | None = None,
    processed_ids_path: Path | None = None,
    evaluation_enabled: bool | None = None,
    delivery_enabled: bool | None = None,
    notification_settings: NotificationSettings | None = None,
    app_settings: AppSettings | None = None,
    analytics_settings: AnalyticsSettings | None = None,
    analytics: AnalyticsKernel | None = None,
    warmed_up: bool = True,
) -> RealtimeRuntime:
    storage = storage or StorageSettings.from_env()
    analytics_policy = analytics_settings
    if analytics_policy is None and app_settings is not None:
        analytics_policy = app_settings.analytics
    if analytics_policy is None:
        analytics_policy = AnalyticsSettings()
    alert_policy: AlertSettings | None = app_settings.alerts if app_settings is not None else None
    projection = LatestMarketProjectionStore(storage)
    outbox = SqliteEventOutbox(outbox_path or default_outbox_path(storage))
    processed = DurableProcessedIdSet(processed_ids_path or default_processed_ids_path(storage))
    sink = TickProjectionSink()
    engine = RealtimeEngine(
        snapshots=ProjectionSnapshotSource(projection),
        analytics=resolve_analytics_kernel(analytics_policy, analytics=analytics),
        alerts=resolve_alert_evaluator(
            projection,
            evaluation_enabled=evaluation_enabled,
            alert_settings=alert_policy,
        ),
        projections=sink,
        outbox=outbox,
        critical_tasks_healthy=critical_tasks_healthy,
        front_chain_fresh=front_chain_fresh,
        chain_thresholds=ChainFreshnessThresholds.from_settings(analytics_policy),
        warmed_up=warmed_up,
    )
    consumer = IdempotentOutboxConsumer(
        outbox,
        consumer_id=consumer_id,
        deliver=resolve_deliver_fn(
            delivery_enabled=delivery_enabled,
            notification_settings=notification_settings,
            deliver=deliver,
        ),
        processed_ids=processed,
    )
    return RealtimeRuntime(
        engine=engine,
        consumer=consumer,
        outbox=outbox,
        projections=sink,
        processed_ids=processed,
        storage=storage,
    )


def run_realtime_engine_cycle(
    *,
    app_settings: AppSettings | None = None,
) -> int:
    """CLI/service-loop entry: one tick + outbox consume, JSON summary on stdout."""

    settings = app_settings or load_production_settings()
    storage = StorageSettings.from_env()
    runtime = build_realtime_runtime(storage, app_settings=settings)
    now = datetime.now(tz=timezone.utc)
    result = runtime.run_cycle(now=now)
    try:
        level_shadow = run_level_decision_shadow(
            storage,
            result.tick,
            now=now,
            policy=settings.level_decision,
        )
    except Exception as exc:  # noqa: BLE001 - shadow audit must not break realtime
        level_shadow = {
            "status": "failed",
            "actionable": False,
            "error_type": type(exc).__name__,
        }
    try:
        level_repricing = run_level_trigger_repricing(
            storage,
            level_shadow,
            now=now,
            policy=settings.order_map,
        )
    except Exception as exc:  # noqa: BLE001 - expose failure without stopping collection
        level_repricing = {
            "status": "failed",
            "error_type": type(exc).__name__,
        }
    try:
        pricing_outcomes = advance_pricing_outcomes(
            storage,
            level_repricing,
            level_shadow,
            now=now,
        )
    except Exception as exc:  # noqa: BLE001 - outcome audit is non-critical IO
        pricing_outcomes = {
            "status": "failed",
            "error_type": type(exc).__name__,
        }
    payload = result.to_dict()
    payload["level_decision_shadow"] = level_shadow
    payload["level_trigger_repricing"] = level_repricing
    payload["pricing_outcomes"] = pricing_outcomes
    print(json.dumps(payload, sort_keys=True))
    return 0 if result.ok else 1


def main() -> int:
    return run_realtime_engine_cycle()


if __name__ == "__main__":
    raise SystemExit(main())
