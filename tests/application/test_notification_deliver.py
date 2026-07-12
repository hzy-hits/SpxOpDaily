"""Outbox deliver + alert evaluator bridge tests."""

from __future__ import annotations

from datetime import datetime, timezone
from dataclasses import replace

from spx_spark.application.notifications.deliver import (
    deliver_alert_candidate,
    notification_settled,
)
from spx_spark.application.realtime.alert_evaluator import (
    alert_batch_event_id,
    domain_events_from_payload,
)
from spx_spark.application.realtime.composition import (
    build_realtime_runtime,
    resolve_deliver_fn,
    log_only_deliver,
)
from spx_spark.config import NotificationSettings, StorageSettings
from spx_spark.domain.events import DomainEvent, EventKind
from spx_spark.notifier.model import NotificationResult


NOW = datetime(2026, 7, 11, 21, 0, tzinfo=timezone.utc)


def _payload(*kinds: str) -> dict[str, object]:
    alerts = [
        {
            "severity": "high",
            "kind": kind,
            "instrument_id": "index:SPX",
            "title": kind,
            "detail": "test",
            "dedup_group": kind,
        }
        for kind in kinds
    ]
    return {
        "as_of": NOW.isoformat(),
        "window": {"name": "rth", "priority": "high"},
        "market_context": {},
        "human_focus_context": {},
        "alerts": alerts,
        "alert_count": len(alerts),
    }


def test_domain_events_from_payload_empty() -> None:
    assert domain_events_from_payload({"alerts": []}, now=NOW) == ()


def test_domain_events_from_payload_deterministic_id() -> None:
    payload = _payload("price_move_from_close")
    first = domain_events_from_payload(payload, now=NOW)
    second = domain_events_from_payload(payload, now=NOW)
    assert len(first) == 1
    assert first[0].event_id == second[0].event_id
    assert first[0].kind is EventKind.ALERT_CANDIDATE
    assert first[0].payload["alert_count"] == 1
    assert alert_batch_event_id(payload, now=NOW) == first[0].event_id


def test_notification_settled_semantics() -> None:
    disabled = NotificationResult(
        enabled=False, selected_count=0, sent_count=0, skipped_reason="disabled", sinks=()
    )
    assert notification_settled(disabled) is True
    skipped = NotificationResult(
        enabled=True,
        selected_count=0,
        sent_count=0,
        skipped_reason="no_alerts_after_severity_or_cooldown",
        sinks=(),
    )
    assert notification_settled(skipped) is True
    sent = NotificationResult(
        enabled=True, selected_count=1, sent_count=1, skipped_reason=None, sinks=()
    )
    assert notification_settled(sent) is True
    retry = NotificationResult(
        enabled=True, selected_count=2, sent_count=0, skipped_reason=None, sinks=()
    )
    assert notification_settled(retry) is False


def test_deliver_alert_candidate_calls_notify_payload(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_notify(payload, *, settings=None, now=None, **kwargs):  # noqa: ANN001
        calls.append(dict(payload))
        return NotificationResult(
            enabled=True,
            selected_count=1,
            sent_count=1,
            skipped_reason=None,
            sinks=(),
            acknowledged_event_ids=("pos-1",),
        )

    acks: list[tuple[str, ...]] = []

    def fake_reconcile(ids):  # noqa: ANN001
        acks.append(tuple(ids))

    monkeypatch.setattr(
        "spx_spark.application.notifications.deliver.notify_payload",
        fake_notify,
    )
    monkeypatch.setattr(
        "spx_spark.application.notifications.deliver.reconcile_position_event_acknowledgements",
        fake_reconcile,
    )
    event = domain_events_from_payload(_payload("price_move_from_close"), now=NOW)[0]
    settings = replace(NotificationSettings.from_env(), enabled=True)
    assert deliver_alert_candidate(event, settings=settings) is True
    assert len(calls) == 1
    assert calls[0]["alert_count"] == 1
    assert acks == [("pos-1",)]


def test_resolve_deliver_fn_mutual_exclusion(monkeypatch) -> None:
    monkeypatch.setattr(
        "spx_spark.application.realtime.composition.outbox_delivery_enabled",
        lambda: True,
    )
    monkeypatch.setattr(
        "spx_spark.application.realtime.composition.direct_alert_delivery_enabled",
        lambda: True,
    )
    assert resolve_deliver_fn() is log_only_deliver

    monkeypatch.setattr(
        "spx_spark.application.realtime.composition.direct_alert_delivery_enabled",
        lambda: False,
    )
    fn = resolve_deliver_fn()
    assert fn is not log_only_deliver


def test_resolve_shock_notify_independent_of_alert_engine_direct(monkeypatch) -> None:
    from spx_spark.config import resolve_shock_notify_enabled

    settings = replace(NotificationSettings.from_env(), enabled=True)
    monkeypatch.setattr(
        "spx_spark.config.shock_direct_delivery_enabled",
        lambda: True,
    )
    assert resolve_shock_notify_enabled(settings=settings) is True
    assert resolve_shock_notify_enabled(no_notify=True, settings=settings) is False

    monkeypatch.setattr(
        "spx_spark.config.shock_direct_delivery_enabled",
        lambda: False,
    )
    assert resolve_shock_notify_enabled(settings=settings) is False
    disabled = replace(settings, enabled=False)
    monkeypatch.setattr(
        "spx_spark.config.shock_direct_delivery_enabled",
        lambda: True,
    )
    assert resolve_shock_notify_enabled(settings=disabled) is False


def test_build_runtime_evaluator_emits_and_delivers(tmp_path, monkeypatch) -> None:
    storage = StorageSettings(
        data_root=str(tmp_path / "data"),
        latest_state_path=str(tmp_path / "data" / "latest" / "state.json"),
        raw_file_name="quotes.jsonl",
        include_raw_payload=False,
        latest_stale_after_seconds=15.0,
        slow_index_stale_after_seconds=300.0,
        slow_index_labels=frozenset(),
        provider_priority=("schwab", "ibkr"),
    )
    # Seed TradFi anchor so engine health is READY.
    from spx_spark.marketdata import (
        InstrumentId,
        MarketDataQuality,
        Provider,
        ProviderState,
        ProviderStatus,
        Quote,
    )
    from spx_spark.storage import LatestMarketProjectionStore

    LatestMarketProjectionStore(storage).update(
        [
            Quote(
                instrument=InstrumentId.index("SPX"),
                provider=Provider.SCHWAB,
                provider_symbol="schwab:SPX",
                received_at=NOW,
                quality=MarketDataQuality.LIVE,
                bid=5000.0,
                ask=5001.0,
                last=5000.5,
                mark=5000.5,
                quote_time=NOW,
            )
        ],
        now=NOW,
        provider_states=[
            ProviderState(
                provider=Provider.SCHWAB,
                status=ProviderStatus.AVAILABLE,
                checked_at=NOW,
            )
        ],
    )

    delivered: list[str] = []

    def capture(event: DomainEvent) -> bool:
        delivered.append(event.event_id)
        return True

    from spx_spark.application.realtime import alert_evaluator as ae

    def fake_evaluate(self, snapshot, analytics, *, now):  # noqa: ANN001
        return domain_events_from_payload(_payload("quote_health"), now=now)

    monkeypatch.setattr(ae.AlertEngineEvaluator, "evaluate", fake_evaluate)

    runtime = build_realtime_runtime(
        storage,
        deliver=capture,
        evaluation_enabled=True,
        delivery_enabled=True,
    )
    result = runtime.run_cycle(now=NOW)
    assert result.ok is True
    assert result.consume.delivered == 1
    assert delivered
