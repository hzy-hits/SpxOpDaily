import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone

from spx_spark.config import MaintenanceSettings, NotificationSettings
from spx_spark.domain.events import DomainEvent, EventKind
from spx_spark.infrastructure.ledger.outbox import OutboxStatus, SqliteEventOutbox
from spx_spark.maintenance import (
    MaintenanceReport,
    action_level,
    build_report,
    disk_alert_state_path,
    execute_prune,
    is_protected_path,
    maybe_send_disk_alert,
    purge_outbox,
    trim_review_audit_file,
)


def make_settings(tmp_path) -> MaintenanceSettings:
    return MaintenanceSettings(
        data_root=str(tmp_path / "data"),
        logs_root=str(tmp_path / "logs"),
        output_root=str(tmp_path / "logs"),
        data_budget_gb=80.0,
        raw_retention_days=10,
        alert_window_retention_days=60,
        feature_1s_retention_days=30,
        feature_5s_retention_days=90,
        log_retention_days=14,
        trash_retention_days=7,
        outbox_retention_days=30,
        review_audit_retention_days=30,
        alert_cooldown_hours=24.0,
        warn_pct=70.0,
        compact_pct=80.0,
        degraded_pct=85.0,
        prune_pct=90.0,
        critical_pct=95.0,
    )


def touch_old(path, *, now: datetime, days: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"12345")
    old_mtime = (now - timedelta(days=days)).timestamp()
    os.utime(path, (old_mtime, old_mtime))


def test_maintenance_dry_run_finds_old_raw_file(tmp_path):
    settings = make_settings(tmp_path)
    raw_dir = tmp_path / "data" / "raw" / "provider=schwab" / "date=2026-01-01"
    old_file = raw_dir / "old.parquet"
    now = datetime(2026, 7, 4, tzinfo=timezone.utc)
    touch_old(old_file, now=now, days=20)

    report = build_report(settings, now=now)

    assert len(report.prune_candidates) == 1
    assert report.prune_candidates[0].path.endswith("old.parquet")
    assert "raw older" in (report.prune_candidates[0].reason or "")


def test_latest_state_is_never_a_prune_candidate(tmp_path):
    settings = make_settings(tmp_path)
    now = datetime(2026, 7, 4, tzinfo=timezone.utc)
    latest = tmp_path / "data" / "latest" / "state.json"
    touch_old(latest, now=now, days=90)

    report = build_report(settings, now=now)

    assert report.prune_candidates == []
    assert is_protected_path(latest, tmp_path / "data")


def test_prune_dry_run_does_not_delete_files(tmp_path):
    settings = make_settings(tmp_path)
    now = datetime(2026, 7, 4, tzinfo=timezone.utc)
    old_file = tmp_path / "data" / "context" / "provider=ibkr" / "date=2026-01-01" / "quotes.jsonl"
    touch_old(old_file, now=now, days=20)

    report = build_report(settings, now=now)
    result = execute_prune(report, settings, execute=False)

    assert old_file.exists()
    assert result.deleted_files == 0
    assert result.executed is False
    assert len(report.prune_candidates) == 1
    assert "context older" in (report.prune_candidates[0].reason or "")


def test_prune_execute_deletes_expired_raw_and_empty_dirs(tmp_path):
    settings = make_settings(tmp_path)
    now = datetime(2026, 7, 4, tzinfo=timezone.utc)
    old_file = tmp_path / "data" / "raw" / "provider=mock" / "date=2026-01-01" / "quotes.jsonl"
    fresh_file = tmp_path / "data" / "raw" / "provider=ibkr" / "date=2026-07-04" / "quotes.jsonl"
    touch_old(old_file, now=now, days=20)
    touch_old(fresh_file, now=now, days=1)

    report = build_report(settings, now=now)
    result = execute_prune(report, settings, execute=True)

    assert result.deleted_files == 1
    assert not old_file.exists()
    assert fresh_file.exists()
    assert result.removed_empty_dirs >= 1


def test_prune_execute_deletes_old_logs(tmp_path):
    settings = make_settings(tmp_path)
    now = datetime(2026, 7, 4, tzinfo=timezone.utc)
    old_log = tmp_path / "logs" / "maintenance-dry-run-old.json"
    touch_old(old_log, now=now, days=20)

    report = build_report(settings, now=now)
    result = execute_prune(report, settings, execute=True)

    assert result.deleted_files == 1
    assert not old_log.exists()


NOW = datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc)


def make_report(settings: MaintenanceSettings, *, level: str) -> MaintenanceReport:
    return MaintenanceReport(
        created_at=NOW.isoformat(),
        disk_total_bytes=100 * 1024**3,
        disk_used_bytes=87 * 1024**3,
        disk_free_bytes=13 * 1024**3,
        disk_used_pct=87.0,
        data_budget_bytes=80 * 1024**3,
        data_bytes=40 * 1024**3,
        data_budget_used_pct=50.0,
        action_level=level,
        settings={},
        summaries=[],
        prune_candidates=[],
    )


def make_notification_settings(tmp_path, *, bark_enabled: bool = True) -> NotificationSettings:
    return NotificationSettings(
        enabled=True,
        min_severity="high",
        cooldown_seconds=300,
        state_path=str(tmp_path / "notify_state.json"),
        openclaw_enabled=False,
        openclaw_command="openclaw",
        openclaw_channel="",
        openclaw_account="",
        openclaw_target="",
        openclaw_dry_run=True,
        openclaw_timeout_seconds=20.0,
        openclaw_agent_enabled=False,
        openclaw_agent_deliver=False,
        openclaw_agent_name="main",
        openclaw_agent_model="gpt-5.3-codex-spark",
        openclaw_agent_session_key="spx-spark-alerts",
        openclaw_agent_thinking="high",
        openclaw_agent_timeout_seconds=180.0,
        codex_enabled=False,
        codex_deliver=True,
        codex_command="codex",
        codex_model="gpt-5.3-codex-spark",
        codex_reasoning_effort="high",
        codex_cwd="/tmp",
        codex_sandbox="read-only",
        codex_timeout_seconds=120.0,
        codex_output_max_chars=4000,
        codex_require_delivery_cue=True,
        bark_enabled=bark_enabled,
        bark_url="https://example.com/bark" if bark_enabled else "",
    )


def patch_bark(monkeypatch, *, ok: bool) -> list[dict[str, object]]:
    calls: list[dict[str, object]] = []

    def poster(url: str, payload: dict[str, object], timeout_seconds: float) -> dict[str, object]:
        calls.append(payload)
        if not ok:
            raise RuntimeError("sink unavailable")
        return {"code": 200, "message": "success"}

    monkeypatch.setattr("spx_spark.notifier.sinks.post_bark", poster)
    return calls


def test_action_level_thresholds_drive_weekly_gating(tmp_path) -> None:
    settings = make_settings(tmp_path)

    assert action_level(69.9, settings) == "ok"
    assert action_level(70.0, settings) == "warn"
    assert action_level(80.0, settings) == "compact"
    assert action_level(85.0, settings) == "degraded"
    assert action_level(90.0, settings) == "prune"
    assert action_level(95.0, settings) == "critical_stop_raw"


def test_disk_alert_below_threshold_skips(tmp_path, monkeypatch) -> None:
    settings = make_settings(tmp_path)
    calls = patch_bark(monkeypatch, ok=True)

    result = maybe_send_disk_alert(
        make_report(settings, level="warn"),
        settings,
        now=NOW,
        notification=make_notification_settings(tmp_path),
    )

    assert result["sent"] is False
    assert result["reason"] == "below_degraded_threshold"
    assert calls == []


def test_disk_alert_sends_then_cools_down_per_level(tmp_path, monkeypatch) -> None:
    settings = make_settings(tmp_path)
    notification = make_notification_settings(tmp_path)
    calls = patch_bark(monkeypatch, ok=True)

    first = maybe_send_disk_alert(
        make_report(settings, level="degraded"),
        settings,
        now=NOW,
        notification=notification,
    )
    assert first["sent"] is True
    assert first["delivered"] is True
    assert len(calls) == 1
    state = json.loads(disk_alert_state_path(settings).read_text(encoding="utf-8"))
    assert state["levels"]["degraded"]

    repeat = maybe_send_disk_alert(
        make_report(settings, level="degraded"),
        settings,
        now=NOW + timedelta(hours=1),
        notification=notification,
    )
    assert repeat["sent"] is False
    assert repeat["reason"] == "cooldown"
    assert len(calls) == 1

    escalated = maybe_send_disk_alert(
        make_report(settings, level="prune"),
        settings,
        now=NOW + timedelta(hours=2),
        notification=notification,
    )
    assert escalated["sent"] is True
    assert len(calls) == 2

    after_cooldown = maybe_send_disk_alert(
        make_report(settings, level="degraded"),
        settings,
        now=NOW + timedelta(hours=25),
        notification=notification,
    )
    assert after_cooldown["sent"] is True
    assert len(calls) == 3


def test_disk_alert_undelivered_does_not_raise_or_burn_cooldown(tmp_path, monkeypatch) -> None:
    settings = make_settings(tmp_path)
    notification = make_notification_settings(tmp_path)
    patch_bark(monkeypatch, ok=False)

    failed = maybe_send_disk_alert(
        make_report(settings, level="critical_stop_raw"),
        settings,
        now=NOW,
        notification=notification,
    )
    assert failed["sent"] is False
    assert failed["reason"] == "delivery_not_confirmed:failed"
    assert not disk_alert_state_path(settings).exists()

    calls = patch_bark(monkeypatch, ok=True)
    retried = maybe_send_disk_alert(
        make_report(settings, level="critical_stop_raw"),
        settings,
        now=NOW + timedelta(minutes=5),
        notification=notification,
    )
    assert retried["sent"] is True
    assert len(calls) == 1


def _outbox_event(event_id: str, *, at: datetime) -> DomainEvent:
    return DomainEvent(
        schema_version=1,
        event_id=event_id,
        kind=EventKind.ALERT_CANDIDATE,
        source_at=at,
        available_at=at,
        aggregate_id="spx",
        sequence=1,
        payload={"alert": event_id},
    )


def _ack_with_updated_at(outbox: SqliteEventOutbox, event_id: str, *, at: datetime) -> None:
    outbox.append([_outbox_event(event_id, at=at)])
    outbox.claim(consumer_id="test", now=at)
    assert outbox.ack([event_id], consumer_id="test", outcome="consumed") == 1
    with sqlite3.connect(outbox.path) as connection:
        connection.execute(
            "UPDATE domain_event_outbox SET updated_at = ? WHERE event_id = ?",
            (at.isoformat(), event_id),
        )


def test_purge_outbox_deletes_only_expired_acked_rows(tmp_path) -> None:
    settings = make_settings(tmp_path)
    outbox_path = tmp_path / "data" / "ledger" / "domain_event_outbox.sqlite"
    outbox = SqliteEventOutbox(outbox_path)
    old = NOW - timedelta(days=40)
    _ack_with_updated_at(outbox, "old-acked", at=old)
    _ack_with_updated_at(outbox, "recent-acked", at=NOW - timedelta(days=2))
    outbox.append([_outbox_event("still-pending", at=old)])

    payload = purge_outbox(settings)

    assert payload["exists"] is True
    assert payload["retention_days"] == 30
    assert payload["deleted"] == 1
    counts = outbox.count_by_status()
    assert counts.get(OutboxStatus.ACKED.value) == 1
    assert counts.get(OutboxStatus.PENDING.value) == 1


def test_purge_outbox_missing_file_is_a_noop(tmp_path) -> None:
    settings = make_settings(tmp_path)

    payload = purge_outbox(settings)

    assert payload["exists"] is False
    assert payload["deleted"] == 0


def test_trim_review_audit_file_drops_only_expired_entries(tmp_path) -> None:
    audit_path = tmp_path / "alert_review_audit.jsonl"
    recent = json.dumps({"at": (NOW - timedelta(days=2)).isoformat(), "outcome": "ok"})
    expired = json.dumps({"at": (NOW - timedelta(days=45)).isoformat(), "outcome": "ok"})
    malformed = "not-json-at-all"
    no_timestamp = json.dumps({"outcome": "missing-at"})
    audit_path.write_text(
        "\n".join([recent, expired, malformed, no_timestamp]) + "\n",
        encoding="utf-8",
    )

    payload = trim_review_audit_file(audit_path, retention_days=30, now=NOW)

    assert payload["exists"] is True
    assert payload["dropped"] == 1
    assert payload["kept"] == 3
    remaining = audit_path.read_text(encoding="utf-8")
    assert expired not in remaining
    assert recent in remaining
    assert malformed in remaining
    assert no_timestamp in remaining


def test_trim_review_audit_file_missing_is_a_noop(tmp_path) -> None:
    payload = trim_review_audit_file(tmp_path / "absent.jsonl", retention_days=30, now=NOW)

    assert payload["exists"] is False
    assert payload["dropped"] == 0
