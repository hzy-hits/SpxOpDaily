from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import json

from spx_spark.maintenance import desk_pipeline_faults, monitor_desk_pipeline

NOW = datetime(2026, 9, 21, 14, 6, tzinfo=timezone.utc)


def healthy(now=NOW):
    return dict(now=now, bridge={"updated_at": now.isoformat(), "phase": "connected"},
                report={"updated_at": now.isoformat(), "last_persisted_at": now.replace(minute=0).isoformat()},
                source={"available_at": now.isoformat()}, mirrored={"available_at": now.isoformat()})


def test_live_processes_do_not_mask_missing_report_or_stopped_bridge():
    data = healthy()
    assert not desk_pipeline_faults(**data)
    data["report"]["last_persisted_at"] = (NOW - timedelta(days=3)).isoformat()
    data["bridge"]["phase"] = "halted"
    assert desk_pipeline_faults(**data) == ["bridge_halted", "scheduled_report_missing"]


def test_slot_grace_weekend_and_stale_or_future_heartbeat():
    data = healthy(NOW.replace(minute=3))
    data["report"]["last_persisted_at"] = NOW.replace(hour=13, minute=30).isoformat()
    assert not desk_pipeline_faults(**data)
    data = healthy(datetime(2026, 9, 20, 14, 6, tzinfo=timezone.utc))
    data["report"]["last_persisted_at"] = None
    assert not desk_pipeline_faults(**data)
    data["bridge"]["updated_at"] = (data["now"] + timedelta(seconds=1)).isoformat()
    assert desk_pipeline_faults(**data) == ["bridge_heartbeat_missing"]


def test_projection_lag_is_separate_from_report_completion():
    data = healthy()
    data["source"]["available_at"] = (NOW - timedelta(minutes=4)).isoformat()
    data["mirrored"] = {}
    assert desk_pipeline_faults(**data) == ["desk_projection_not_forwarded"]


def test_independent_alert_retries_cooldown_and_recovery(tmp_path):
    settings = SimpleNamespace(data_root=str(tmp_path))
    root = tmp_path / "health"
    data = healthy()
    files = {root / "spx-spark-bridge-shadow/health.json": data["bridge"],
             root / "spx-spark-report-shadow/health.json": data["report"],
             tmp_path / "latest/desk_map_projection.json": data["source"],
             root / "spx-spark-core-shadow/latest/desk-map.json": {"projection": data["mirrored"]}}
    for path, value in files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value))
    bridge_path = root / "spx-spark-bridge-shadow/health.json"
    bridge_path.write_text(json.dumps({"updated_at": NOW.isoformat(), "phase": "halted"}))
    state = tmp_path / "ledger/maintenance_disk_alert_state.json"
    state.parent.mkdir(parents=True)
    state.write_text(json.dumps({"levels": {"degraded": "preserve"}}))
    calls = []
    def send(settings, card):
        calls.append(card)
        return SimpleNamespace(ok=len(calls) > 1)
    kwargs = dict(now=NOW, settings=settings, notification=object(), sender=send, health_root=root, delivery_active=True)
    assert not monitor_desk_pipeline(**kwargs)["sent"]
    assert monitor_desk_pipeline(**kwargs)["sent"]
    assert not monitor_desk_pipeline(**kwargs)["sent"]
    bridge_path.write_text(json.dumps(data["bridge"]))
    assert monitor_desk_pipeline(**kwargs)["reason"] == "recovered"
    assert json.loads(state.read_text())["levels"] == {"degraded": "preserve"}
    assert len(calls) == 3


def test_stopped_delivery_alerts_even_with_fresh_report():
    assert desk_pipeline_faults(**healthy(), delivery_active=False) == ["delivery_service_unavailable"]


def test_heartbeat_published_during_reads_is_not_future(monkeypatch, tmp_path):
    import spx_spark.maintenance as module
    clock = [NOW]
    later = NOW + timedelta(seconds=1)
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0]
    def read(path):
        clock[0] = later
        if path.name == "health.json":
            return {"updated_at": later.isoformat(), "last_persisted_at": NOW.replace(minute=0).isoformat()}
        return {}
    monkeypatch.setattr(module, "datetime", Clock)
    monkeypatch.setattr(module, "read_json_object", read)
    result = monitor_desk_pipeline(settings=SimpleNamespace(data_root=str(tmp_path)),
                                   health_root=tmp_path, delivery_active=True)
    assert result["faults"] == []
    assert not result["sent"]
