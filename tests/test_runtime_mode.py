from datetime import UTC, datetime, time

from spx_spark.config import RuntimePolicySettings
from spx_spark.runtime_mode import ibkr_allowed, load_override, write_override


def make_policy() -> RuntimePolicySettings:
    return RuntimePolicySettings(
        ibkr_schedule_enabled=True,
        ibkr_schedule_timezone="Asia/Shanghai",
        ibkr_schedule_start=time(1, 5),
        ibkr_schedule_stop=time(8, 0),
        ibkr_connect_retry_seconds=300,
        ibkr_conflict_retry_minutes=0,
        ibkr_conflict_probe_seconds=300,
        ibkr_fallback_provider="schwab",
        strict_no_session_fight=True,
        weekend_maintenance_mode=False,
        runtime_mode_path="runtime/mode.json",
        agent_override_default_ttl_minutes=120,
    )


def make_weekend_policy() -> RuntimePolicySettings:
    return RuntimePolicySettings(
        ibkr_schedule_enabled=True,
        ibkr_schedule_timezone="Asia/Shanghai",
        ibkr_schedule_start=time(0, 0),
        ibkr_schedule_stop=time(0, 0),
        ibkr_connect_retry_seconds=300,
        ibkr_conflict_retry_minutes=0,
        ibkr_conflict_probe_seconds=300,
        ibkr_fallback_provider="schwab",
        strict_no_session_fight=True,
        weekend_maintenance_mode=True,
        runtime_mode_path="runtime/mode.json",
        agent_override_default_ttl_minutes=120,
    )


def test_ibkr_on_override_allows_outside_schedule(tmp_path):
    now = datetime(2026, 7, 4, 18, 0, tzinfo=UTC)
    path = tmp_path / "mode.json"
    override = write_override(path, "ibkr-on", ttl_minutes=60, reason="test", now=now)

    assert ibkr_allowed(make_policy(), now=now, override=override)


def test_protected_override_blocks_inside_schedule(tmp_path):
    now = datetime(2026, 7, 3, 18, 0, tzinfo=UTC)
    path = tmp_path / "mode.json"
    override = write_override(path, "protected", ttl_minutes=60, reason="test", now=now)

    assert not ibkr_allowed(make_policy(), now=now, override=override)


def test_expired_override_is_ignored(tmp_path):
    now = datetime(2026, 7, 4, 18, 0, tzinfo=UTC)
    path = tmp_path / "mode.json"
    write_override(path, "ibkr-on", ttl_minutes=1, reason="test", now=now)

    assert load_override(path, now=datetime(2026, 7, 4, 18, 2, tzinfo=UTC)) is None


def test_ibkr_on_override_can_allow_weekend(tmp_path):
    now = datetime(2026, 7, 4, 18, 0, tzinfo=UTC)
    path = tmp_path / "mode.json"
    override = write_override(path, "ibkr-on", ttl_minutes=60, reason="test", now=now)

    assert ibkr_allowed(make_weekend_policy(), now=now, override=None) is False
    assert ibkr_allowed(make_weekend_policy(), now=now, override=override)
