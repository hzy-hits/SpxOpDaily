from datetime import date, datetime, time
from zoneinfo import ZoneInfo

from spx_spark.config import (
    IbkrSettings,
    NotificationSettings,
    PolymarketSettings,
    RuntimePolicySettings,
    StorageSettings,
    SchwabSettings,
    default_spxw_expiry,
    is_time_in_window,
    next_equity_futures_month,
    parse_hhmm,
)

NY_TZ = ZoneInfo("America/New_York")


def test_notification_settings_default_review_audit_path(monkeypatch, tmp_path) -> None:
    data_root = tmp_path / "market-data"
    monkeypatch.setenv("MARKET_DATA_DATA_ROOT", str(data_root))
    monkeypatch.delenv("ALERT_NOTIFY_REVIEW_AUDIT_PATH", raising=False)

    settings = NotificationSettings.from_env()

    assert settings.review_audit_path == str(data_root / "latest" / "alert_review_audit.jsonl")


def test_next_equity_futures_month_returns_yyyymm():
    value = next_equity_futures_month()
    assert len(value) == 6
    assert value.isdigit()


def test_default_spxw_expiry_returns_yyyymmdd():
    value = default_spxw_expiry(today=date(2026, 7, 9))
    assert len(value) == 8
    assert value.isdigit()
    assert value == "20260709"


def test_default_spxw_expiry_rolls_at_1700_et():
    thursday_afternoon = datetime(2026, 7, 9, 15, 0, tzinfo=NY_TZ)
    assert default_spxw_expiry(now=thursday_afternoon) == "20260709"

    thursday_after_close = datetime(2026, 7, 9, 16, 20, tzinfo=NY_TZ)
    assert default_spxw_expiry(now=thursday_after_close) == "20260709"

    thursday_rollover = datetime(2026, 7, 9, 17, 0, tzinfo=NY_TZ)
    assert default_spxw_expiry(now=thursday_rollover) == "20260710"

    friday_after_close = datetime(2026, 7, 10, 17, 0, tzinfo=NY_TZ)
    assert default_spxw_expiry(now=friday_after_close) == "20260713"

    before_observed_holiday = datetime(2026, 7, 2, 17, 0, tzinfo=NY_TZ)
    assert default_spxw_expiry(now=before_observed_holiday) == "20260706"


def test_default_spxw_expiry_explicit_today_does_not_roll():
    thursday = date(2026, 7, 9)
    assert default_spxw_expiry(today=thursday) == "20260709"


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
    friday_us_afternoon = datetime(2026, 7, 11, 1, 30, tzinfo=timezone)
    observed_holiday = datetime(2026, 7, 3, 14, 0, tzinfo=timezone)
    assert not policy.market_data_collection_allowed(saturday)
    assert policy.market_data_collection_allowed(monday)
    assert policy.market_data_collection_allowed(friday_us_afternoon)
    assert not policy.market_data_collection_allowed(observed_holiday)

    sunday_before_reopen = datetime(2026, 7, 12, 17, 59, tzinfo=NY_TZ)
    sunday_reopen = datetime(2026, 7, 12, 18, 0, tzinfo=NY_TZ)
    labor_day_reopen = datetime(2026, 9, 6, 18, 0, tzinfo=NY_TZ)
    holiday_evening_reopen = datetime(2026, 9, 7, 18, 0, tzinfo=NY_TZ)
    assert not policy.market_data_collection_allowed(sunday_before_reopen)
    assert policy.market_data_collection_allowed(sunday_reopen)
    assert not policy.market_data_collection_allowed(labor_day_reopen)
    assert policy.market_data_collection_allowed(holiday_evening_reopen)


def test_storage_settings_inherits_maintenance_root(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("MAINTENANCE_DATA_ROOT", "/tmp/spx-data")
    monkeypatch.delenv("MARKET_DATA_DATA_ROOT", raising=False)
    monkeypatch.delenv("MARKET_DATA_LATEST_STATE_PATH", raising=False)

    settings = StorageSettings.from_env()

    assert settings.data_root == "/tmp/spx-data"
    assert settings.latest_state_path == "/tmp/spx-data/latest/state.json"


def test_ibkr_default_verifier_uses_dia_as_dow_proxy(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("IBKR_VERIFY_STOCKS", raising=False)
    monkeypatch.delenv("IBKR_VERIFY_INDEXES", raising=False)
    monkeypatch.delenv("IBKR_QUALIFY_CONTRACTS", raising=False)
    monkeypatch.delenv("IBKR_REQUEST_TIMEOUT_SECONDS", raising=False)

    settings = IbkrSettings.from_env()

    assert "DIA" in settings.verify_stocks
    assert settings.verify_indexes == [
        "SPX",
        "VIX",
        "VIX1D",
        "VIX9D",
        "VIX3M",
        "VVIX",
        "SKEW",
    ]
    assert "RSP" in settings.verify_stocks
    assert "XLU" in settings.verify_stocks
    assert settings.qualify_contracts is True
    assert settings.request_timeout_seconds == 30.0


def test_polymarket_settings_defaults_are_research_context(monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("POLYMARKET_SEARCH_TERMS", raising=False)
    monkeypatch.delenv("POLYMARKET_USER_AGENT", raising=False)

    settings = PolymarketSettings.from_env()

    assert settings.gamma_api_base_url == "https://gamma-api.polymarket.com"
    assert settings.search_terms == ["SPY", "Fed decision", "CPI", "FOMC", "Powell", "NFP"]
    assert settings.max_results_per_query == 5
    assert settings.max_markets_per_run == 80
    assert settings.min_relevance_score == 0.35
    assert settings.include_closed is False
    assert settings.user_agent


def test_schwab_cloudflare_gateway_settings(monkeypatch, tmp_path) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://schwab-auth.example.com/oauth/callback")
    monkeypatch.setenv("SCHWAB_OAUTH_BIND_PORT", "8183")
    monkeypatch.setenv("SCHWAB_GATEWAY_BIND_PORT", "8184")
    monkeypatch.setenv("SCHWAB_GATEWAY_URL", "http://127.0.0.1:8184")

    settings = SchwabSettings.from_env()

    assert settings.callback_url == "https://schwab-auth.example.com/oauth/callback"
    assert settings.oauth_bind_host == "127.0.0.1"
    assert settings.oauth_bind_port == 8183
    assert settings.gateway_bind_host == "127.0.0.1"
    assert settings.gateway_bind_port == 8184
    assert settings.gateway_url == "http://127.0.0.1:8184"
