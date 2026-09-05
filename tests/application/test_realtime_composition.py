"""Realtime composition using the unified notification queue."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from spx_spark.application.realtime.composition import (
    PassthroughAnalytics,
    build_realtime_runtime,
    default_outbox_path,
    resolve_analytics_kernel,
)
from spx_spark.application.realtime.options_kernel import OptionsAnalyticsKernel
from spx_spark.config import StorageSettings
from spx_spark.domain.analytics import AnalyticsStatus
from spx_spark.domain.events import DomainEvent, EventKind
from spx_spark.domain.health import EngineMode
from spx_spark.infrastructure.notifications import create_engine, event_rows, metadata
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    Provider,
    ProviderState,
    ProviderStatus,
    Quote,
)
from spx_spark.settings.analytics import AnalyticsSettings
from spx_spark.storage import LatestStateStore


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


def _store(storage: StorageSettings):
    engine = create_engine(Path(storage.data_root))
    metadata.create_all(engine)
    return engine


def _seed_spx(storage: StorageSettings, *, now: datetime) -> None:
    LatestStateStore(storage).update(
        [
            Quote(
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
        ],
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
    assert isinstance(resolve_analytics_kernel(AnalyticsSettings()), OptionsAnalyticsKernel)
    assert isinstance(
        resolve_analytics_kernel(AnalyticsSettings(passthrough_shadow_mode=True)),
        PassthroughAnalytics,
    )


def test_realtime_event_is_durably_queued_once(tmp_path, monkeypatch) -> None:
    storage = _storage(tmp_path)
    store = _store(storage)
    now = datetime.now(tz=timezone.utc)
    monkeypatch.setattr(
        "spx_spark.application.realtime.engine.DEFAULT_MARKET_CALENDAR.is_rth_open",
        lambda _now: True,
    )
    _seed_spx(storage, now=now)
    runtime = build_realtime_runtime(
        storage,
        outbox_path=default_outbox_path(storage),
        evaluation_enabled=False,
        delivery_enabled=False,
        front_chain_fresh=True,
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
        payload={"alerts": []},
    )

    first = runtime.outbox.append([event])
    duplicate = runtime.outbox.append([event])
    result = runtime.run_cycle(now=now)

    assert result.ok is True
    assert result.tick.health.mode is EngineMode.READY
    assert result.tick.analytics is not None
    assert result.tick.analytics.status is AnalyticsStatus.SUCCESS
    assert first.accepted == 1 and duplicate.duplicate == 1
    assert [(row["channel"], row["status"]) for row in event_rows(store, "wired-1")] == [
        ("alert_pipeline", "pending")
    ]
    store.dispose()


def test_real_kernel_marks_analytics_blocked_without_front_month(tmp_path) -> None:
    storage = _storage(tmp_path)
    store = _store(storage)
    _seed_spx(storage, now=NOW)
    runtime = build_realtime_runtime(
        storage,
        outbox_path=default_outbox_path(storage),
        evaluation_enabled=False,
        delivery_enabled=False,
        front_chain_fresh=True,
    )

    result = runtime.run_cycle(now=NOW)

    assert result.ok is True
    assert result.tick.analytics is not None
    assert result.tick.analytics.status is not AnalyticsStatus.SUCCESS
    assert result.tick.health.factors["analytics_ok"] is False
    assert result.tick.health.mode is EngineMode.BLOCKED
    store.dispose()
