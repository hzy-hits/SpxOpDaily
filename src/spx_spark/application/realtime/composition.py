"""24h composition root for RealtimeEngine + unified notification queue."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
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
from spx_spark.domain.health import EngineMode
from spx_spark.domain.market import MarketSnapshot
from spx_spark.infrastructure.notifications import NotificationEventQueue, create_engine
from spx_spark.notifier.unified_delivery import engine_for_settings
from spx_spark.provider_failover_controller import ProviderFailoverSettings
from spx_spark.settings import AppSettings, load_app_settings
from spx_spark.settings.alerts import AlertSettings
from spx_spark.settings.analytics import AnalyticsSettings
from spx_spark.storage import LatestMarketProjectionStore


def default_runtime_defaults_path() -> Path:
    """Resolve config/runtime.toml relative to the repository root."""

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
    """Deprecated location hint retained for one caller release."""

    return Path(storage.data_root) / "spx.sqlite"


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
        failover_mode=state.failover_mode,
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


def resolve_alert_evaluator(
    store: LatestMarketProjectionStore,
    *,
    evaluation_enabled: bool | None = None,
    alert_settings: AlertSettings | None = None,
    provider_failover_settings: ProviderFailoverSettings | None = None,
    app_settings: AppSettings | None = None,
    event_bucket_seconds: int = 300,
):
    if evaluation_enabled is None:
        evaluation_enabled = outbox_alert_evaluation_enabled()
    if evaluation_enabled:
        return AlertEngineEvaluator(
            store,
            alert_settings=alert_settings,
            provider_failover_settings=provider_failover_settings,
            app_settings=app_settings,
            event_bucket_seconds=event_bucket_seconds,
        )
    return SilentAlertEvaluator()


@dataclass
class RealtimeRuntime:
    """Wired RealtimeEngine with Huey-owned alert candidate delivery."""

    engine: RealtimeEngine
    outbox: NotificationEventQueue
    projections: TickProjectionSink
    storage: StorageSettings

    def run_cycle(self, *, now: datetime | None = None, consume_limit: int = 20) -> "CycleResult":
        now = now or datetime.now(tz=timezone.utc)
        tick = self.engine.tick(now=now)
        del consume_limit
        # BLOCKED/DEGRADED/STARTING/WARMING are valid observations, not process
        # failures. The service-loop heartbeat carries readiness separately.
        ok = tick.health.mode is not EngineMode.FAILED
        return CycleResult(tick=tick, ok=ok)


@dataclass(frozen=True)
class CycleResult:
    tick: EngineTick
    ok: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "mode": self.tick.health.mode.value,
            "tick": self.tick.to_dict(),
            "delivery": "huey_async",
            "outbox_writable": self.tick.health.factors.get("outbox_writable"),
        }


def build_realtime_runtime(
    storage: StorageSettings | None = None,
    *,
    deliver=None,
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
    del consumer_id, deliver, processed_ids_path
    storage = storage or StorageSettings.from_env()
    analytics_policy = analytics_settings
    if analytics_policy is None and app_settings is not None:
        analytics_policy = app_settings.analytics
    if analytics_policy is None:
        analytics_policy = AnalyticsSettings()
    alert_policy: AlertSettings | None = app_settings.alerts if app_settings is not None else None
    failover_policy = (
        ProviderFailoverSettings.from_policy(
            app_settings.runtime,
            data_root=storage.data_root,
        )
        if app_settings is not None
        else None
    )
    notification_settings = notification_settings or NotificationSettings.from_env()
    projection = LatestMarketProjectionStore(storage)
    notification_engine = (
        create_engine((outbox_path or default_outbox_path(storage)).parent)
        if outbox_path is not None
        else engine_for_settings(notification_settings)
    )
    if delivery_enabled is None:
        delivery_enabled = outbox_delivery_enabled()
    schedule_enabled = delivery_enabled and not direct_alert_delivery_enabled()

    def schedule(event_id: int) -> None:
        if not schedule_enabled:
            return
        from spx_spark.infrastructure.jobs import deliver_notification_event

        deliver_notification_event(event_id, priority=-10)

    outbox = NotificationEventQueue(notification_engine, schedule=schedule)
    sink = TickProjectionSink()
    engine = RealtimeEngine(
        snapshots=ProjectionSnapshotSource(projection),
        analytics=resolve_analytics_kernel(analytics_policy, analytics=analytics),
        alerts=resolve_alert_evaluator(
            projection,
            evaluation_enabled=evaluation_enabled,
            alert_settings=alert_policy,
            provider_failover_settings=failover_policy,
            app_settings=app_settings,
            event_bucket_seconds=notification_settings.cooldown_seconds,
        ),
        projections=sink,
        outbox=outbox,
        critical_tasks_healthy=critical_tasks_healthy,
        front_chain_fresh=front_chain_fresh,
        chain_thresholds=ChainFreshnessThresholds.from_settings(analytics_policy),
        warmed_up=warmed_up,
    )
    return RealtimeRuntime(
        engine=engine,
        outbox=outbox,
        projections=sink,
        storage=storage,
    )


def run_realtime_engine_cycle(
    *,
    app_settings: AppSettings | None = None,
    storage_settings: StorageSettings | None = None,
) -> int:
    """CLI/service-loop entry: one tick + outbox consume, JSON summary on stdout."""

    settings = app_settings or load_production_settings()
    storage = storage_settings or StorageSettings.from_env()
    runtime = build_realtime_runtime(storage, app_settings=settings)
    tick_started_at = datetime.now(tz=timezone.utc)
    result = runtime.run_cycle(now=tick_started_at)
    # Analytics can exceed the quote freshness threshold. Projection-side
    # health checks must use wall-clock time after the tick, not the timestamp
    # captured before the analytics work started.
    projection_now = datetime.now(tz=timezone.utc)
    try:
        level_shadow = run_level_decision_shadow(
            storage,
            result.tick,
            now=projection_now,
            policy=settings.level_decision,
            notifications_enabled=True,
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
            now=projection_now,
            policy=settings.order_map,
            feature_policy=settings.market_features,
            level_policy=settings.level_decision,
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
            now=projection_now,
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
    tick = payload.get("tick") if isinstance(payload.get("tick"), dict) else {}
    print(
        json.dumps(
            {
                "task": "realtime_engine",
                "event": "cycle_summary",
                "ok": payload.get("ok"),
                "mode": payload.get("mode"),
                "tick_id": tick.get("tick_id"),
                "duration_ms": tick.get("duration_ms"),
                "event_count": tick.get("event_count"),
                "level_status": level_shadow.get("status"),
                "level_actionable": level_shadow.get("actionable"),
                "repricing_status": level_repricing.get("status"),
                "pricing_outcomes_status": pricing_outcomes.get("status"),
            },
            sort_keys=True,
        )
    )
    return 0 if result.ok else 1


def main() -> int:
    return run_realtime_engine_cycle()


if __name__ == "__main__":
    raise SystemExit(main())
