from datetime import datetime, time
from zoneinfo import ZoneInfo

from spx_spark.config import (
    RuntimePolicySettings,
    StorageSettings,
    default_spxw_expiry,
    is_time_in_window,
    next_equity_futures_month,
    parse_hhmm,
)


def test_next_equity_futures_month_returns_yyyymm():
    value = next_equity_futures_month()
    assert len(value) == 6
    assert value.isdigit()


def test_default_spxw_expiry_returns_yyyymmdd():
    value = default_spxw_expiry()
    assert len(value) == 8
    assert value.isdigit()


def test_parse_hhmm():
    assert parse_hhmm("01:05") == time(hour=1, minute=5)


def test_is_time_in_window_same_day():
    assert is_time_in_window(time(1, 5), time(1, 5), time(4, 20))
    assert is_time_in_window(time(3, 0), time(1, 5), time(4, 20))
    assert not is_time_in_window(time(4, 20), time(1, 5), time(4, 20))


def test_is_time_in_window_cross_midnight():
    assert is_time_in_window(time(23, 0), time(22, 0), time(2, 0))
    assert is_time_in_window(time(1, 30), time(22, 0), time(2, 0))
    assert not is_time_in_window(time(3, 0), time(22, 0), time(2, 0))


def test_is_time_in_window_same_start_stop_means_always_open():
    assert is_time_in_window(time(0, 0), time(0, 0), time(0, 0))
    assert is_time_in_window(time(13, 30), time(0, 0), time(0, 0))


def test_runtime_policy_uses_beijing_window():
    policy = RuntimePolicySettings(
        ibkr_schedule_enabled=True,
        ibkr_schedule_timezone="Asia/Shanghai",
        ibkr_schedule_start=time(1, 5),
        ibkr_schedule_stop=time(4, 20),
        ibkr_connect_retry_seconds=300,
        ibkr_conflict_retry_minutes=0,
        ibkr_conflict_probe_seconds=300,
        ibkr_fallback_provider="schwab",
        strict_no_session_fight=True,
        weekend_maintenance_mode=True,
        runtime_mode_path="runtime/mode.json",
        agent_override_default_ttl_minutes=120,
    )
    timezone = ZoneInfo("Asia/Shanghai")
    assert policy.ibkr_window_is_open(datetime(2026, 7, 4, 1, 5, tzinfo=timezone))
    assert policy.ibkr_window_is_open(datetime(2026, 7, 4, 3, 0, tzinfo=timezone))
    assert not policy.ibkr_window_is_open(datetime(2026, 7, 4, 4, 20, tzinfo=timezone))
    assert not policy.should_retry_after_conflict
    assert policy.should_probe_after_conflict


def test_runtime_policy_blocks_weekend_collection_in_auto_mode():
    policy = RuntimePolicySettings(
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
    timezone = ZoneInfo("Asia/Shanghai")
    saturday = datetime(2026, 7, 4, 14, 0, tzinfo=timezone)
    monday = datetime(2026, 7, 6, 14, 0, tzinfo=timezone)
    assert not policy.market_data_collection_allowed(saturday)
    assert policy.market_data_collection_allowed(monday)


def test_storage_settings_inherits_maintenance_root(monkeypatch):
    monkeypatch.setenv("MAINTENANCE_DATA_ROOT", "/tmp/spx-data")
    monkeypatch.delenv("MARKET_DATA_DATA_ROOT", raising=False)
    monkeypatch.delenv("MARKET_DATA_LATEST_STATE_PATH", raising=False)

    settings = StorageSettings.from_env()

    assert settings.data_root == "/tmp/spx-data"
    assert settings.latest_state_path == "/tmp/spx-data/latest/state.json"
