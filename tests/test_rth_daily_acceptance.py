"""RTH notification acceptance against unified event/attempt rows."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from spx_spark.application.order_map.rth_daily_acceptance_support import (
    explicitly_terminal_event_ids,
    fully_delivered_event_ids,
    receipt_store_check,
    timely_delivered_event_ids,
)
from spx_spark.infrastructure.notifications import (
    NotificationDraft,
    NotificationStatus,
    begin_attempt,
    cancel,
    create_engine,
    enqueue,
    mark_transport_started,
    metadata,
    settle,
)


NOW = datetime(2026, 8, 7, 12, 0, tzinfo=timezone.utc)


def _store(tmp_path: Path):
    engine = create_engine(tmp_path)
    metadata.create_all(engine)
    return engine


def _enqueue(engine, event_id: str, *, expires_at: datetime | None = None) -> tuple[int, ...]:
    return enqueue(
        engine,
        NotificationDraft(
            logical_event_id=event_id,
            source="test",
            kind="trade_intent",
            lane="trade_ready",
            payload={"body": "immutable"},
            channels=("bark", "feishu"),
            expires_at=expires_at or NOW + timedelta(minutes=5),
        ),
        now=NOW,
    ).event_ids


def _deliver(engine, event_ids: tuple[int, ...], *, at: datetime) -> None:
    for event_id in event_ids:
        attempt = begin_attempt(engine, event_id, now=at)
        assert attempt is not None
        mark_transport_started(engine, attempt.attempt_id)
        settle(
            engine,
            attempt.attempt_id,
            status=NotificationStatus.DELIVERED,
            outcome="delivered",
            ok=True,
            now=at,
        )


def test_all_targets_require_real_success_attempts(tmp_path: Path) -> None:
    engine = _store(tmp_path)
    ids = _enqueue(engine, "ready")
    _deliver(engine, ids, at=NOW + timedelta(seconds=1))

    assert fully_delivered_event_ids(tmp_path / "spx.sqlite", ("ready",)) == {"ready"}
    accepted, diagnostics = timely_delivered_event_ids(
        tmp_path / "spx.sqlite", ("ready",)
    )
    assert accepted == {"ready"}
    assert diagnostics["events"]["ready"]["first_delivery_seconds"] == 1.0


def test_first_delivery_after_five_seconds_is_rejected(tmp_path: Path) -> None:
    engine = _store(tmp_path)
    ids = _enqueue(engine, "late")
    _deliver(engine, ids, at=NOW + timedelta(seconds=6))

    accepted, diagnostics = timely_delivered_event_ids(tmp_path / "spx.sqlite", ("late",))

    assert accepted == frozenset()
    assert diagnostics["events"]["late"]["reasons"] == ["first_delivery_slo_breached"]


def test_cancellation_fence_is_an_explicit_terminal_result(tmp_path: Path) -> None:
    engine = _store(tmp_path)
    _enqueue(engine, "cancelled")
    assert cancel(engine, "cancelled", reason="source_invalidated", now=NOW) == 2

    accepted, diagnostics = explicitly_terminal_event_ids(
        tmp_path / "spx.sqlite", ("cancelled",)
    )

    assert accepted == {"cancelled"}
    assert diagnostics["events"]["cancelled"]["reasons"] == []


def test_attempt_store_integrity_uses_spx_sqlite_only(tmp_path: Path) -> None:
    engine = _store(tmp_path)
    engine.dispose()

    check = receipt_store_check(tmp_path / "spx.sqlite", tmp_path / "legacy.sqlite")

    assert check.passed is True
    assert check.measured["schema_present"] is True
