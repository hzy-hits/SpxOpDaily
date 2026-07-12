"""Composition root: RealtimeEngine + durable processed_ids + outbox consumer."""

from __future__ import annotations

from datetime import datetime, timezone

from spx_spark.application.realtime.composition import (
    PassthroughAnalytics,
    build_realtime_runtime,
    default_outbox_path,
    default_processed_ids_path,
    resolve_analytics_kernel,
)
from spx_spark.application.realtime.options_kernel import OptionsAnalyticsKernel
from spx_spark.config import StorageSettings
from spx_spark.domain.analytics import AnalyticsStatus
from spx_spark.domain.events import DomainEvent, EventKind
from spx_spark.domain.health import EngineMode
from spx_spark.infrastructure.ledger.processed_ids import DurableProcessedIdSet
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    Provider,
    ProviderState,
    ProviderStatus,
    Quote,
)
from spx_spark.settings.analytics import AnalyticsSettings
from spx_spark.storage import LatestMarketProjectionStore


NOW = datetime(2026, 7, 11, 20, 0, tzinfo=timezone.utc)


def _storage(tmp_path) -> StorageSettings:
    return StorageSettings(
        data_root=str(tmp_path / "data"),
        latest_state_path=str(tmp_path / "data" / "latest" / "state.json"),
        raw_file_name="quotes.jsonl",
        include_raw_payload=False,
        latest_stale_after_seconds=15.0,
        slow_index_stale_after_seconds=300.0,
        slow_index_labels=frozenset(),
        provider_priority=("schwab", "ibkr"),
    )


def _seed_spx(storage: StorageSettings, *, now: datetime) -> None:
    quote = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.SCHWAB,
        provider_symbol="schwab:SPX",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        bid=5000.0,
        ask=5001.0,
        last=5000.5,
        mark=5000.5,
        quote_time=now,
    )
    LatestMarketProjectionStore(storage).update(
        [quote],
        now=now,
        provider_states=[
            ProviderState(
                provider=Provider.SCHWAB,
                status=ProviderStatus.AVAILABLE,
                checked_at=now,
            )
        ],
    )


def test_production_composition_uses_options_analytics_kernel() -> None:
    kernel = resolve_analytics_kernel(AnalyticsSettings())
    assert isinstance(kernel, OptionsAnalyticsKernel)
    shadow = resolve_analytics_kernel(AnalyticsSettings(passthrough_shadow_mode=True))
    assert isinstance(shadow, PassthroughAnalytics)


def test_build_realtime_runtime_tick_and_persist_processed_ids(tmp_path) -> None:
    storage = _storage(tmp_path)
    now = datetime.now(tz=timezone.utc)
    _seed_spx(storage, now=now)
    sent: list[str] = []

    def deliver(event: DomainEvent) -> bool:
        sent.append(event.event_id)
        return True

    runtime = build_realtime_runtime(
        storage,
        deliver=deliver,
        outbox_path=default_outbox_path(storage),
        processed_ids_path=default_processed_ids_path(storage),
        evaluation_enabled=False,
        front_chain_fresh=True,
        # Explicit passthrough for outbox wiring test; production default is real kernel.
        analytics=PassthroughAnalytics(),
    )
    event = DomainEvent(
        schema_version=1,
        event_id="wired-1",
        kind=EventKind.ALERT_CANDIDATE,
        source_at=now,
        available_at=now,
        aggregate_id="spx",
        sequence=1,
        payload={"k": 1},
    )
    runtime.outbox.append([event])
    result = runtime.run_cycle(now=now)
    assert result.ok is True
    assert result.tick.health.mode is EngineMode.READY
    assert result.tick.analytics is not None
    assert result.tick.analytics.status is AnalyticsStatus.SUCCESS
    assert result.consume.delivered == 1
    assert sent == ["wired-1"]
    assert "wired-1" in runtime.processed_ids
    reloaded = DurableProcessedIdSet(default_processed_ids_path(storage))
    assert "wired-1" in reloaded


def test_real_kernel_marks_analytics_ok_false_without_front_month(tmp_path) -> None:
    storage = _storage(tmp_path)
    _seed_spx(storage, now=NOW)
    runtime = build_realtime_runtime(
        storage,
        outbox_path=default_outbox_path(storage),
        processed_ids_path=default_processed_ids_path(storage),
        evaluation_enabled=False,
        front_chain_fresh=True,
    )
    assert isinstance(runtime.engine.analytics, OptionsAnalyticsKernel)
    result = runtime.run_cycle(now=NOW)
    assert result.ok is True
    assert result.tick.analytics is not None
    assert result.tick.analytics.status is not AnalyticsStatus.SUCCESS
    assert result.tick.health.factors["analytics_ok"] is False
    assert result.tick.health.mode is EngineMode.BLOCKED


def test_durable_processed_ids_survives_restart(tmp_path) -> None:
    path = tmp_path / "ids.json"
    store = DurableProcessedIdSet(path)
    store.add("a")
    store.add("b")
    again = DurableProcessedIdSet(path)
    assert "a" in again and "b" in again
    assert len(again) == 2


def test_blocked_readiness_is_a_successful_runtime_observation(tmp_path) -> None:
    storage = _storage(tmp_path)
    runtime = build_realtime_runtime(
        storage,
        outbox_path=default_outbox_path(storage),
        processed_ids_path=default_processed_ids_path(storage),
        evaluation_enabled=False,
    )

    result = runtime.run_cycle(now=NOW)

    assert result.tick.health.mode is EngineMode.BLOCKED
    assert result.ok is True
