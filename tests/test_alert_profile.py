from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from spx_spark.alert_profile import active_window, parse_at, profile


BJ_TZ = ZoneInfo("Asia/Shanghai")


def test_beijing_14_to_16_maps_to_overnight_liquidity_dip_watch() -> None:
    window = active_window(datetime(2026, 7, 6, 14, 30, tzinfo=BJ_TZ))

    assert window.name == "overnight_liquidity_dip_watch"
    assert window.spxw_sampling_mode == "off"
    assert "hyperliquid" in window.primary_sources


def test_beijing_16_to_18_maps_to_early_premarket_dip_watch() -> None:
    window = active_window(datetime(2026, 7, 6, 17, 0, tzinfo=BJ_TZ))

    assert window.name == "early_premarket_dip_watch"
    assert window.priority == "high"
    assert "equity:SPY" in window.required_instruments
    assert "future:ES" in window.optional_instruments


def test_beijing_after_0130_is_unattended_afternoon_watch() -> None:
    window = active_window(datetime(2026, 7, 7, 1, 30, tzinfo=BJ_TZ))

    assert window.name == "unattended_afternoon_watch"
    assert window.user_unattended is True
    assert window.spxw_sampling_mode == "human_alert"


def test_close_hour_is_critical_and_unattended() -> None:
    window = active_window(datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ))

    assert window.name == "close_one_hour"
    assert window.priority == "critical"
    assert window.user_unattended is True


def test_beijing_morning_is_high_priority_globex_watch() -> None:
    # Thursday 09:30 Beijing = Wednesday 21:30 ET: the reader is at his desk
    # while Globex/GTH trade; this used to fall into the low quiet window.
    window = active_window(datetime(2026, 7, 9, 9, 30, tzinfo=BJ_TZ))

    assert window.name == "beijing_morning_globex_watch"
    assert window.priority == "high"
    assert window.user_unattended is False


def test_friday_evening_et_stays_quiet_for_weekend_beijing_morning() -> None:
    # Saturday 09:30 Beijing = Friday 21:30 ET: ES closed, reader off.
    window = active_window(datetime(2026, 7, 11, 9, 30, tzinfo=BJ_TZ))

    assert window.name == "quiet_futures_context"
    assert window.priority == "high"


def test_weekend_before_futures_reopen_is_maintenance() -> None:
    window = active_window(datetime(2026, 7, 5, 12, 0, tzinfo=BJ_TZ))

    assert window.name == "weekend_maintenance"
    assert window.priority == "off"


def test_parse_at_treats_naive_timestamp_as_beijing_time() -> None:
    parsed = parse_at("2026-07-06T14:30:00")

    assert parsed.tzinfo == BJ_TZ
    assert active_window(parsed).name == "overnight_liquidity_dip_watch"


def test_profile_includes_beijing_and_et_timestamps() -> None:
    payload = profile(datetime(2026, 7, 6, 21, 45, tzinfo=BJ_TZ))

    assert payload["window"]["name"] == "open_one_hour"
    assert "now_et" in payload
    assert "now_beijing" in payload
