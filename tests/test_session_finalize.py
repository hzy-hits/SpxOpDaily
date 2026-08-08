from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from spx_spark.config import StorageSettings
from spx_spark.data_platform.replay_artifact import (
    ReplayCleanupSummary,
    StoragePressure,
    cleanup_authorized_replay_sources,
    verify_replay_artifact,
)
from spx_spark.data_platform.settings import DataPlatformSettings
from spx_spark.market_calendar import ET
from spx_spark.post_close_runtime import ReviewLlmSettings
from spx_spark.session_finalize import (
    SessionFinalizeResult,
    _run_human_review,
    resolve_finalize_date,
    run_pressure_check,
    run_session_finalize,
    session_research_window,
)


TRADING_DATE = date(2026, 7, 10)
RUN_NOW = datetime(2026, 7, 10, 18, 0, tzinfo=ET).astimezone(timezone.utc)


def storage_settings(tmp_path: Path) -> StorageSettings:
    return StorageSettings(
        data_root=str(tmp_path),
        latest_state_path=str(tmp_path / "latest" / "state.json"),
        raw_file_name="quotes.jsonl",
        include_raw_payload=False,
        latest_stale_after_seconds=15.0,
        slow_index_stale_after_seconds=300.0,
        slow_index_labels=frozenset({"index:SKEW"}),
    )


def platform_settings(tmp_path: Path) -> DataPlatformSettings:
    return DataPlatformSettings(
        enabled=True,
        data_root=str(tmp_path),
        ledger_path=str(tmp_path / "runtime" / "ledger.sqlite3"),
        fallback_spool_path=str(tmp_path / "runtime" / "fallback.jsonl"),
        fallback_spool_max_bytes=67_108_864,
        lake_root=str(tmp_path / "lake"),
        manifest_root=str(tmp_path / "manifests"),
        research_catalog_path=str(tmp_path / "analytics" / "research.duckdb"),
        sqlite_busy_timeout_ms=250,
        compaction_min_age_seconds=0,
        raw_delete_enabled=False,
        raw_delete_grace_hours=48,
        writer_version="test-v1",
    )


def write_quote_source(tmp_path: Path) -> Path:
    path = (
        tmp_path
        / "raw"
        / "provider=ibkr"
        / "date=2026-07-10"
        / "hour=10"
        / "quotes.jsonl"
    )
    payload = {
        "instrument": {
            "symbol": "SPX",
            "instrument_type": "index",
            "provider_symbol": "SPX",
            "exchange": "CBOE",
            "currency": "USD",
            "expiry": None,
            "strike": None,
            "right": None,
            "multiplier": None,
            "underlier": None,
            "trading_class": None,
            "canonical_id": "index:SPX",
        },
        "instrument_id": "index:SPX",
        "provider": "ibkr",
        "provider_symbol": "SPX",
        "received_at": "2026-07-10T10:05:00+00:00",
        "quality": "live",
        "bid": 6200.0,
        "ask": 6200.5,
        "last": 6200.25,
        "mark": 6200.25,
        "bid_size": 1.0,
        "ask_size": 1.0,
        "quote_time": "2026-07-10T10:05:00+00:00",
        "source_latency_ms": 0.0,
        "market_data_type": 1,
        "greeks": None,
        "sampling_mode": "human_alert",
        "sampling_group": 0,
        "mid": 6200.25,
        "spread": 0.5,
        "spread_bps": 0.8,
        "effective_price": 6200.25,
    }
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    settled = (RUN_NOW - timedelta(hours=1)).timestamp()
    os.utime(path, (settled, settled))
    return path


def normal_pressure() -> StoragePressure:
    return StoragePressure(
        total_bytes=100 * 1024**3,
        used_bytes=60 * 1024**3,
        free_bytes=40 * 1024**3,
        level="normal",
        action_required=False,
    )


def empty_cleanup(*, status: str = "pressure_normal") -> ReplayCleanupSummary:
    return ReplayCleanupSummary(
        status=status,
        dry_run=False,
        pressure=normal_pressure(),
        artifact_dates_scanned=(),
        authorized_dates=(),
        blocked_dates=(),
        results=(),
        deleted_files=0,
        deleted_bytes=0,
        would_delete_files=0,
        would_delete_bytes=0,
    )


def test_auto_at_18_et_resolves_latest_completed_trading_day() -> None:
    assert resolve_finalize_date("auto", now=RUN_NOW) == TRADING_DATE
    assert resolve_finalize_date(
        "auto",
        now=datetime(2026, 7, 10, 16, 59, tzinfo=ET),
    ) == date(2026, 7, 9)
    assert resolve_finalize_date("2026-07-09", now=RUN_NOW) == date(2026, 7, 9)
    with pytest.raises(ValueError, match="not a trading day"):
        resolve_finalize_date("2026-07-11", now=RUN_NOW)


def test_research_window_is_previous_2015_through_trading_day_1615_et() -> None:
    start, end = session_research_window(TRADING_DATE)

    assert start == datetime(2026, 7, 9, 20, 15, tzinfo=ET)
    assert end == datetime(2026, 7, 10, 16, 15, tzinfo=ET)


def test_finalizer_builds_payload_once_and_reuses_same_object_for_human_phase(
    tmp_path: Path,
    monkeypatch,
) -> None:
    write_quote_source(tmp_path)
    calls: list[tuple[date, datetime]] = []
    payload_ids: list[int] = []

    def fake_build_review_payload(*, trading_date, settings, now):
        assert settings.data_root == str(tmp_path)
        calls.append((trading_date, now))
        return {
            "created_at": now.isoformat(),
            "trading_date": trading_date.isoformat(),
            "coverage": {"raw_quote_rows": 1, "iv_surface_snapshots": 0},
        }

    def fake_human_review(*, payload, **_kwargs):
        payload_ids.append(id(payload))
        return {"status": "complete", "push": {"skipped": True}}

    monkeypatch.setattr("spx_spark.session_finalize.build_review_payload", fake_build_review_payload)
    monkeypatch.setattr(
        "spx_spark.session_finalize.render_markdown",
        lambda payload: f"# SPX/SPXW Post-Close Review - {payload['trading_date']}\n",
    )
    monkeypatch.setattr("spx_spark.session_finalize._run_human_review", fake_human_review)
    monkeypatch.setattr("spx_spark.session_finalize._measure_pressure", lambda _settings: normal_pressure())

    result = run_session_finalize(
        selected_date=TRADING_DATE,
        now=RUN_NOW,
        storage_settings=storage_settings(tmp_path),
        platform_settings=platform_settings(tmp_path),
        dry_run=False,
        max_backlog_days=7,
        no_llm=True,
        no_push=True,
    )

    assert result.status == "complete"
    assert result.artifact is not None and result.artifact.status == "published"
    assert len(calls) == 1
    assert calls[0] == (
        TRADING_DATE,
        datetime(2026, 7, 10, 17, 0, tzinfo=ET).astimezone(timezone.utc),
    )
    assert len(payload_ids) == 1
    manifest = verify_replay_artifact(tmp_path / result.artifact.manifest_path, data_root=tmp_path)
    artifact_payload = json.loads((tmp_path / manifest.review_json.path).read_text())
    assert artifact_payload["created_at"] == "2026-07-10T21:00:00+00:00"


def test_idempotent_existing_artifact_does_not_repeat_human_push(
    tmp_path: Path,
    monkeypatch,
) -> None:
    write_quote_source(tmp_path)
    payload_calls = 0

    def fake_build_payload(*, trading_date, settings, now):
        nonlocal payload_calls
        payload_calls += 1
        return {
            "created_at": now.isoformat(),
            "trading_date": trading_date.isoformat(),
            "coverage": {"raw_quote_rows": 1, "iv_surface_snapshots": 0},
        }

    monkeypatch.setattr(
        "spx_spark.session_finalize.build_review_payload",
        fake_build_payload,
    )
    monkeypatch.setattr(
        "spx_spark.session_finalize.render_markdown",
        lambda payload: f"# SPX/SPXW Post-Close Review - {payload['trading_date']}\n",
    )
    monkeypatch.setattr(
        "spx_spark.session_finalize._measure_pressure",
        lambda _settings: normal_pressure(),
    )
    human_calls: list[object] = []
    monkeypatch.setattr(
        "spx_spark.session_finalize._run_human_review",
        lambda **kwargs: human_calls.append(kwargs) or {"status": "complete"},
    )
    arguments = {
        "selected_date": TRADING_DATE,
        "now": RUN_NOW,
        "storage_settings": storage_settings(tmp_path),
        "platform_settings": platform_settings(tmp_path),
        "dry_run": False,
        "max_backlog_days": 7,
        "no_llm": True,
        "no_push": True,
    }

    first = run_session_finalize(**arguments)
    second = run_session_finalize(**arguments)

    assert first.artifact is not None and first.artifact.status == "published"
    assert second.artifact is not None and second.artifact.status == "already_published"
    assert second.human_review == {
        "status": "skipped",
        "reason": "artifact_already_published",
    }
    assert len(human_calls) == 1
    assert payload_calls == 1


def test_existing_artifact_remains_idempotent_after_authorized_raw_cleanup(
    tmp_path: Path,
    monkeypatch,
) -> None:
    write_quote_source(tmp_path)
    monkeypatch.setattr(
        "spx_spark.session_finalize.build_review_payload",
        lambda *, trading_date, settings, now: {
            "created_at": now.isoformat(),
            "trading_date": trading_date.isoformat(),
            "coverage": {"raw_quote_rows": 1, "iv_surface_snapshots": 0},
        },
    )
    monkeypatch.setattr(
        "spx_spark.session_finalize.render_markdown",
        lambda payload: f"# SPX/SPXW Post-Close Review - {payload['trading_date']}\n",
    )
    monkeypatch.setattr(
        "spx_spark.session_finalize._run_human_review",
        lambda **_kwargs: {"status": "complete"},
    )
    monkeypatch.setattr(
        "spx_spark.session_finalize._measure_pressure",
        lambda _settings: normal_pressure(),
    )
    common = {
        "selected_date": TRADING_DATE,
        "storage_settings": storage_settings(tmp_path),
        "platform_settings": platform_settings(tmp_path),
        "dry_run": False,
        "max_backlog_days": 7,
        "no_llm": True,
        "no_push": True,
    }
    first = run_session_finalize(now=RUN_NOW, **common)
    assert first.artifact is not None and first.artifact.status == "published"
    cleanup = cleanup_authorized_replay_sources(
        tmp_path,
        now=RUN_NOW + timedelta(hours=25),
        pressure=StoragePressure(
            total_bytes=100 * 1024**3,
            used_bytes=81 * 1024**3,
            free_bytes=19 * 1024**3,
            level="critical",
            action_required=True,
        ),
    )
    assert cleanup.status == "deleted"

    second = run_session_finalize(now=RUN_NOW + timedelta(hours=26), **common)

    assert second.status == "complete"
    assert second.preparation_status == "artifact_verified_without_raw"
    assert second.artifact is not None and second.artifact.status == "already_published"


def test_human_phase_preserves_config_disabled_llm_and_records_push_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    settings = ReviewLlmSettings(
        enabled=False,
        provider="deepseek",
        model="deepseek-v4-flash",
        url="https://example.invalid",
        env_file="",
        timeout_seconds=1.0,
        max_tokens=100,
    )
    observed: list[bool] = []
    monkeypatch.setattr(ReviewLlmSettings, "from_env", staticmethod(lambda: settings))

    def fake_llm(payload, markdown, llm_settings):
        observed.append(llm_settings.enabled)
        payload["llm_writer"] = {"status": "disabled", "enabled": llm_settings.enabled}
        return markdown

    monkeypatch.setattr("spx_spark.session_finalize.maybe_write_llm_review", fake_llm)
    monkeypatch.setattr("spx_spark.session_finalize.default_hermes_export_dir", lambda: tmp_path)
    monkeypatch.setattr("spx_spark.session_finalize.review_paths", lambda **_kwargs: object())
    monkeypatch.setattr(
        "spx_spark.session_finalize.write_outputs",
        lambda *_args: {"latest_markdown_path": str(tmp_path / "latest.md")},
    )
    monkeypatch.setattr(
        "spx_spark.session_finalize.push_review",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("sink down")),
    )

    outcome = _run_human_review(
        payload={"trading_date": TRADING_DATE.isoformat()},
        deterministic_markdown="# deterministic\n",
        trading_date=TRADING_DATE,
        storage_settings=storage_settings(tmp_path),
        no_llm=False,
        no_push=False,
    )

    assert observed == [False]
    assert outcome["status"] == "degraded"
    assert any("push: RuntimeError: sink down" in item for item in outcome["errors"])


def test_pressure_check_never_builds_review_or_notifies(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("spx_spark.session_finalize._measure_pressure", lambda _settings: normal_pressure())
    monkeypatch.setattr(
        "spx_spark.session_finalize.build_review_payload",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not build review")),
    )
    monkeypatch.setattr(
        "spx_spark.session_finalize.push_review",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must not push")),
    )

    result = run_pressure_check(
        now=RUN_NOW,
        storage_settings=storage_settings(tmp_path),
        platform_settings=platform_settings(tmp_path),
        dry_run=True,
        max_backlog_days=7,
    )

    assert result.status == "pressure_checked"
    assert result.cleanup.status == "no_verified_sources"
    assert result.artifact is None
    assert result.human_review is None


def test_cleanup_failure_is_nonzero_semantics_without_revoking_artifact() -> None:
    result = SessionFinalizeResult(
        status="cleanup_failed",
        dry_run=False,
        trading_date=TRADING_DATE.isoformat(),
        window_start=None,
        window_end=None,
        discovered_partitions=1,
        compaction_status_counts={"up_to_date": 1},
        preparation_status="verified",
        preparation_errors=(),
        artifact=None,
        cleanup=empty_cleanup(status="delete_failed"),
        human_review={"status": "complete"},
    )

    assert result.failed is True
    assert result.to_dict()["cleanup"]["status"] == "delete_failed"
