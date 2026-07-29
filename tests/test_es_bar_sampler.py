from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from spx_spark.application.runtime import es_bar_sampler
from spx_spark.config import StorageSettings
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    Provider,
    Quote,
)
from spx_spark.settings.market_features import MarketFeatureSettings
from spx_spark.storage import LatestState


UTC = timezone.utc


def _storage(tmp_path: Path) -> StorageSettings:
    return StorageSettings(
        data_root=str(tmp_path),
        latest_state_path=str(tmp_path / "latest" / "state.json"),
        raw_file_name="quotes.jsonl",
        include_raw_payload=False,
        latest_stale_after_seconds=90.0,
        slow_index_stale_after_seconds=180.0,
        slow_index_labels=frozenset(),
    )


def _latest(at: datetime, price: float) -> LatestState:
    quote = Quote(
        instrument=InstrumentId.future(
            "ES",
            expiry="202609",
            provider_symbol="/ESU26",
        ),
        provider=Provider.IBKR,
        provider_symbol="/ESU26",
        received_at=at,
        last_update_at=at,
        quality=MarketDataQuality.LIVE,
        bid=price - 0.25,
        ask=price + 0.25,
        quote_time=at,
    )
    return LatestState(
        created_at=at,
        as_of=at,
        quotes=(quote,),
        best_quotes=(quote,),
    )


def _empty_latest(at: datetime) -> LatestState:
    return LatestState(
        created_at=at,
        as_of=at,
        quotes=(),
        best_quotes=(),
    )


def _state(storage: StorageSettings) -> dict[str, object]:
    return json.loads(es_bar_sampler.canonical_state_path(storage).read_text(encoding="utf-8"))


def test_sampler_accepts_only_fresh_source_timestamps(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    policy = MarketFeatureSettings()
    start = datetime(2026, 7, 30, 13, 30, tzinfo=UTC)

    first = es_bar_sampler.sample_es_bar_once(
        storage=storage,
        policy=policy,
        now=start,
        latest_state=_latest(start, 7400.0),
    )
    duplicate = es_bar_sampler.sample_es_bar_once(
        storage=storage,
        policy=policy,
        now=start + timedelta(seconds=5),
        latest_state=_latest(start, 7500.0),
    )
    second = es_bar_sampler.sample_es_bar_once(
        storage=storage,
        policy=policy,
        now=start + timedelta(seconds=10),
        latest_state=_latest(start + timedelta(seconds=10), 7401.0),
    )
    state = _state(storage)

    assert first["accepted"] is True
    assert duplicate["accepted"] is False
    assert duplicate["rejection"] == "es_source_timestamp_duplicate_or_out_of_order"
    assert second["accepted"] is True
    assert state["current_bar"]["sample_count"] == 2
    assert state["current_bar"]["high"] == 7401.0
    assert state["diagnostics"]["canonical_writer"] == "es_bar_sampler"
    assert state["writer_instance_id"] == f"direct-{es_bar_sampler.os.getpid()}"


def test_duplicate_does_not_rewrite_the_large_canonical_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = _storage(tmp_path)
    at = datetime(2026, 7, 30, 13, 30, tzinfo=UTC)
    es_bar_sampler.sample_es_bar_once(
        storage=storage,
        policy=MarketFeatureSettings(),
        now=at,
        latest_state=_latest(at, 7400.0),
        writer_instance_id="writer-a",
    )
    original = es_bar_sampler.canonical_state_path(storage).read_bytes()
    writes: list[Path] = []
    real_write = es_bar_sampler.atomic_write_json_secure

    def record_write(path: Path, payload: dict[str, object]) -> None:
        writes.append(path)
        real_write(path, payload)

    monkeypatch.setattr(es_bar_sampler, "atomic_write_json_secure", record_write)
    duplicate = es_bar_sampler.sample_es_bar_once(
        storage=storage,
        policy=MarketFeatureSettings(),
        now=at + timedelta(seconds=5),
        latest_state=_latest(at, 7500.0),
        writer_instance_id="writer-a",
    )

    assert duplicate["accepted"] is False
    assert duplicate["canonical_write_performed"] is False
    assert writes == []
    assert es_bar_sampler.canonical_state_path(storage).read_bytes() == original


def test_missing_es_is_reported_without_creating_canonical_state(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    at = datetime(2026, 7, 30, 13, 30, tzinfo=UTC)

    result = es_bar_sampler.sample_es_bar_once(
        storage=storage,
        policy=MarketFeatureSettings(),
        now=at,
        latest_state=_empty_latest(at),
        writer_instance_id="writer-a",
    )

    assert result["accepted"] is False
    assert result["rejection"] == "es_source_timestamp_missing"
    assert result["canonical_write_performed"] is False
    assert not es_bar_sampler.canonical_state_path(storage).exists()


def test_sampler_never_synthesizes_missed_buckets(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    policy = MarketFeatureSettings()
    start = datetime(2026, 7, 30, 13, 30, tzinfo=UTC)

    es_bar_sampler.sample_es_bar_once(
        storage=storage,
        policy=policy,
        now=start,
        latest_state=_latest(start, 7400.0),
    )
    later = start + timedelta(minutes=10)
    es_bar_sampler.sample_es_bar_once(
        storage=storage,
        policy=policy,
        now=later,
        latest_state=_latest(later, 7410.0),
    )
    state = _state(storage)

    assert len(state["closed_bars"]) == 1
    assert state["closed_bars"][0]["quality"] == "partial"
    assert state["current_bar"]["bar_start"] == later.isoformat()
    assert state["current_bar"]["gap_before"] is True


def test_sampler_fails_closed_without_overwriting_corrupt_state(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    path = es_bar_sampler.canonical_state_path(storage)
    path.parent.mkdir(parents=True)
    original = b"{malformed"
    path.write_bytes(original)
    at = datetime(2026, 7, 30, 13, 30, tzinfo=UTC)

    with pytest.raises(
        es_bar_sampler.CanonicalEsBarStateError,
        match="canonical_es_bar_state_unreadable",
    ):
        es_bar_sampler.sample_es_bar_once(
            storage=storage,
            policy=MarketFeatureSettings(),
            now=at,
            latest_state=_latest(at, 7400.0),
        )

    assert path.read_bytes() == original


def test_sampler_worker_publishes_a_failure_heartbeat_and_exits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = _storage(tmp_path)
    path = es_bar_sampler.canonical_state_path(storage)
    path.parent.mkdir(parents=True)
    path.write_text("{malformed", encoding="utf-8")
    monkeypatch.setattr(
        es_bar_sampler,
        "load_app_settings",
        lambda: SimpleNamespace(market_features=MarketFeatureSettings()),
    )
    monkeypatch.setattr(
        es_bar_sampler.StorageSettings,
        "from_env",
        classmethod(lambda _cls: storage),
    )
    monkeypatch.setattr(es_bar_sampler, "install_stop_handlers", lambda _event: None)

    result = es_bar_sampler.run(
        [
            "--once",
            "--max-consecutive-failures=1",
            "--lock-path",
            str(tmp_path / "sampler.lock"),
        ]
    )
    lease = json.loads(es_bar_sampler.lease_path(storage).read_text(encoding="utf-8"))

    assert result == 1
    assert lease["task"] == "es_bar_sampler"
    assert lease["ok"] is False
    assert "CanonicalEsBarStateError" in str(lease["error"])
    assert path.read_text(encoding="utf-8") == "{malformed"


def test_sampler_lease_fails_closed_for_no_es_data(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    at = datetime(2026, 7, 30, 13, 30, tzinfo=UTC)
    observation = es_bar_sampler.sample_es_bar_once(
        storage=storage,
        policy=MarketFeatureSettings(),
        now=at,
        latest_state=_empty_latest(at),
        writer_instance_id="writer-a",
    )
    emitted: list[dict[str, object]] = []
    times = iter((at, at + timedelta(milliseconds=10)))
    ticks = iter((10.0, 10.01))

    result = es_bar_sampler.run_es_bar_sampler_loop(
        lambda: observation,
        interval_seconds=5.0,
        stop_event=threading.Event(),
        writer_instance_id="writer-a",
        max_cycles=1,
        monotonic=lambda: next(ticks),
        utcnow=lambda: next(times),
        emit=emitted.append,
        output_lease_path=es_bar_sampler.lease_path(storage),
    )
    lease = emitted[0]

    assert result == 0
    assert lease["liveness_ok"] is True
    assert lease["data_healthy"] is False
    assert lease["ok"] is False
    assert lease["rejection"] == "es_source_timestamp_missing"
    assert lease["consecutive_data_failures"] == 1
    assert lease["writer_has_accepted"] is False


def test_one_missing_timestamp_keeps_a_fresh_accepted_fence_healthy() -> None:
    at = datetime(2026, 7, 30, 13, 30, tzinfo=UTC)
    emitted: list[dict[str, object]] = []
    observations = iter(
        (
            {
                "accepted": True,
                "source_at": at.isoformat(),
                "rejection": None,
                "writer_instance_id": "writer-a",
            },
            {
                "accepted": False,
                "source_at": at.isoformat(),
                "rejection": "es_source_timestamp_missing",
                "writer_instance_id": "writer-a",
            },
        )
    )
    times = iter(
        (
            at,
            at + timedelta(milliseconds=10),
            at + timedelta(seconds=5),
            at + timedelta(seconds=5, milliseconds=10),
        )
    )
    ticks = iter((10.0, 10.01, 15.0, 15.01))

    es_bar_sampler.run_es_bar_sampler_loop(
        lambda: next(observations),
        interval_seconds=0.1,
        stop_event=threading.Event(),
        writer_instance_id="writer-a",
        max_source_age_seconds=15.0,
        max_cycles=2,
        monotonic=lambda: next(ticks),
        utcnow=lambda: next(times),
        emit=emitted.append,
    )

    assert emitted[0]["data_healthy"] is True
    assert emitted[1]["accepted"] is False
    assert emitted[1]["rejection"] == "es_source_timestamp_missing"
    assert emitted[1]["last_accepted_age_seconds"] == 5.0
    assert emitted[1]["last_accepted_source_age_seconds"] == 5.01
    assert emitted[1]["data_healthy"] is True
    assert emitted[1]["ok"] is True


def test_sampler_lease_rejects_stale_source_and_slow_cycle(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    source_at = datetime(2026, 7, 30, 13, 29, 40, tzinfo=UTC)
    started_at = datetime(2026, 7, 30, 13, 30, tzinfo=UTC)
    finished_at = started_at + timedelta(seconds=6)
    emitted: list[dict[str, object]] = []
    times = iter((started_at, finished_at))
    ticks = iter((10.0, 16.0))

    es_bar_sampler.run_es_bar_sampler_loop(
        lambda: {
            "accepted": True,
            "source_at": source_at.isoformat(),
            "rejection": None,
        },
        interval_seconds=5.0,
        stop_event=threading.Event(),
        writer_instance_id="writer-a",
        max_source_age_seconds=15.0,
        max_cycles=1,
        monotonic=lambda: next(ticks),
        utcnow=lambda: next(times),
        emit=emitted.append,
        output_lease_path=es_bar_sampler.lease_path(storage),
    )
    lease = emitted[0]

    assert lease["liveness_ok"] is True
    assert lease["data_healthy"] is False
    assert lease["sla_ok"] is False
    assert lease["ok"] is False
    assert lease["source_age_seconds"] == 26.0
    assert lease["last_accepted_source_age_seconds"] == 26.0
    assert lease["duration_ms"] == 6000.0
    assert lease["overrun_ms"] == 1000.0


def test_contract_conflict_is_unhealthy_even_while_last_accept_is_fresh() -> None:
    at = datetime(2026, 7, 30, 13, 30, tzinfo=UTC)
    emitted: list[dict[str, object]] = []
    observations = iter(
        (
            {
                "accepted": True,
                "source_at": at.isoformat(),
                "rejection": None,
                "writer_instance_id": "writer-a",
            },
            {
                "accepted": False,
                "source_at": at.isoformat(),
                "rejection": "es_contract_identity_provider_conflict",
                "writer_instance_id": "writer-a",
            },
        )
    )
    times = iter(
        (
            at,
            at + timedelta(milliseconds=10),
            at + timedelta(seconds=5),
            at + timedelta(seconds=5, milliseconds=10),
        )
    )
    ticks = iter((10.0, 10.01, 15.0, 15.01))

    es_bar_sampler.run_es_bar_sampler_loop(
        lambda: next(observations),
        interval_seconds=0.1,
        stop_event=threading.Event(),
        writer_instance_id="writer-a",
        max_cycles=2,
        monotonic=lambda: next(ticks),
        utcnow=lambda: next(times),
        emit=emitted.append,
    )

    assert emitted[0]["data_healthy"] is True
    assert emitted[1]["last_accepted_source_age_seconds"] == 5.01
    assert emitted[1]["data_healthy"] is False
    assert emitted[1]["ok"] is False
    assert emitted[1]["consecutive_data_failures"] == 1


def test_readiness_requires_fresh_acceptance_and_matching_writer_marker(
    tmp_path: Path,
) -> None:
    storage = _storage(tmp_path)
    at = datetime(2026, 7, 30, 13, 30, tzinfo=UTC)
    writer_instance_id = "writer-current"
    emitted: list[dict[str, object]] = []
    times = iter((at, at + timedelta(milliseconds=10)))
    ticks = iter((10.0, 10.01))

    def cycle() -> dict[str, object]:
        return es_bar_sampler.sample_es_bar_once(
            storage=storage,
            policy=MarketFeatureSettings(),
            now=at,
            latest_state=_latest(at, 7400.0),
            writer_instance_id=writer_instance_id,
        )

    es_bar_sampler.run_es_bar_sampler_loop(
        cycle,
        interval_seconds=5.0,
        stop_event=threading.Event(),
        writer_instance_id=writer_instance_id,
        max_cycles=1,
        monotonic=lambda: next(ticks),
        utcnow=lambda: next(times),
        emit=emitted.append,
        output_lease_path=es_bar_sampler.lease_path(storage),
    )

    ready = es_bar_sampler.sampler_readiness(
        storage=storage,
        now=at + timedelta(seconds=1),
        max_age_seconds=15.0,
    )
    assert ready["ready"] is True
    assert ready["reasons"] == []

    lease = json.loads(es_bar_sampler.lease_path(storage).read_text(encoding="utf-8"))
    lease["writer_instance_id"] = "writer-stale"
    es_bar_sampler.atomic_write_json_secure(es_bar_sampler.lease_path(storage), lease)
    mismatched = es_bar_sampler.sampler_readiness(
        storage=storage,
        now=at + timedelta(seconds=1),
        max_age_seconds=15.0,
    )
    assert mismatched["ready"] is False
    assert "writer_instance_mismatch" in mismatched["reasons"]


def test_readiness_rejects_stale_accepted_marker(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    at = datetime(2026, 7, 30, 13, 30, tzinfo=UTC)
    writer_instance_id = "writer-old"
    es_bar_sampler.sample_es_bar_once(
        storage=storage,
        policy=MarketFeatureSettings(),
        now=at,
        latest_state=_latest(at, 7400.0),
        writer_instance_id=writer_instance_id,
    )
    lease = {
        "schema_version": es_bar_sampler.LEASE_SCHEMA_VERSION,
        "task": es_bar_sampler.TASK_NAME,
        "event": "cycle_finished",
        "ok": True,
        "data_healthy": True,
        "sla_ok": True,
        "writer_has_accepted": True,
        "writer_instance_id": writer_instance_id,
        "finished_at": at.isoformat(),
        "last_accepted_at": at.isoformat(),
        "last_accepted_source_at": at.isoformat(),
    }
    es_bar_sampler.atomic_write_json_secure(es_bar_sampler.lease_path(storage), lease)

    readiness = es_bar_sampler.sampler_readiness(
        storage=storage,
        now=at + timedelta(seconds=16),
        max_age_seconds=15.0,
    )

    assert readiness["ready"] is False
    assert "lease_stale" in readiness["reasons"]
    assert "last_accept_stale" in readiness["reasons"]
    assert "accepted_source_stale" in readiness["reasons"]


def test_starting_marker_invalidates_a_previous_ready_lease(tmp_path: Path) -> None:
    storage = _storage(tmp_path)
    at = datetime(2026, 7, 30, 13, 30, tzinfo=UTC)
    es_bar_sampler.sample_es_bar_once(
        storage=storage,
        policy=MarketFeatureSettings(),
        now=at,
        latest_state=_latest(at, 7400.0),
        writer_instance_id="writer-old",
    )
    es_bar_sampler.atomic_write_json_secure(
        es_bar_sampler.lease_path(storage),
        {
            "schema_version": es_bar_sampler.LEASE_SCHEMA_VERSION,
            "task": es_bar_sampler.TASK_NAME,
            "event": "cycle_finished",
            "ok": True,
            "data_healthy": True,
            "sla_ok": True,
            "writer_has_accepted": True,
            "writer_instance_id": "writer-old",
            "finished_at": at.isoformat(),
            "last_accepted_at": at.isoformat(),
            "last_accepted_source_at": at.isoformat(),
        },
    )

    es_bar_sampler.mark_sampler_starting(
        storage=storage,
        writer_instance_id="writer-new",
        now=at + timedelta(seconds=1),
    )
    readiness = es_bar_sampler.sampler_readiness(
        storage=storage,
        now=at + timedelta(seconds=1),
    )

    assert readiness["ready"] is False
    assert "lease_identity_invalid" in readiness["reasons"]
    assert "writer_has_not_accepted" in readiness["reasons"]


def test_heavy_feature_cycle_is_a_read_only_es_bar_consumer() -> None:
    source = (
        Path(__file__).parents[1]
        / "src"
        / "spx_spark"
        / "application"
        / "market_features"
        / "service.py"
    ).read_text(encoding="utf-8")

    assert "advance_es_bar_state" not in source
    assert "exclusive_state_lock(es_bar_path)" not in source
    assert "es_bars, es_bar_consumer = load_consumable_es_bars(" in source
