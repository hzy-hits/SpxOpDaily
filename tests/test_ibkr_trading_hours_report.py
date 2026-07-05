from __future__ import annotations

from datetime import datetime

from spx_spark.config import IbkrSettings, NY_TZ
from spx_spark.ibkr.trading_hours_report import (
    check_row,
    is_regular_trading_hours,
    report_payload,
    summarize_group,
)
from spx_spark.ibkr.verifier import VerifyRow


def make_settings() -> IbkrSettings:
    return IbkrSettings(
        host="127.0.0.1",
        port=4001,
        client_id=171,
        market_data_type=1,
        es_expiry="202609",
        mes_expiry="202609",
        verify_indexes=["SPX", "VIX", "VIX1D", "VIX9D", "VIX3M", "VVIX", "SKEW"],
        verify_stocks=["SPY", "QQQ"],
        verify_futures=["ES", "MES"],
        option_expiry="20260706",
        option_strike_window_points=50,
        option_strike_step=5,
        max_option_lines=40,
        quote_wait_seconds=0.1,
        stale_after_seconds=10.0,
        qualify_contracts=False,
    )


def live_index(label: str) -> VerifyRow:
    symbol = label.split(":", 1)[1]
    return VerifyRow(
        label=label,
        kind="index",
        symbol=symbol,
        exchange="CBOE",
        qualified=True,
        subscribed=True,
        market_data_type=1,
        market_price=7500.0,
        ticker_time="2026-07-06T14:00:00+00:00",
        stale=False,
    )


def test_regular_trading_hours_uses_new_york_time() -> None:
    assert is_regular_trading_hours(datetime(2026, 7, 6, 10, 0, tzinfo=NY_TZ))
    assert not is_regular_trading_hours(datetime(2026, 7, 5, 10, 0, tzinfo=NY_TZ))
    assert not is_regular_trading_hours(datetime(2026, 7, 6, 16, 0, tzinfo=NY_TZ))


def test_check_row_marks_live_index_ok() -> None:
    row = live_index("index:SPX")

    check = check_row(row)

    assert check.group == "p0_indexes"
    assert check.status == "ok"
    assert check.has_price is True


def test_check_row_marks_option_missing_greeks() -> None:
    row = VerifyRow(
        label="option:SPXW:20260706:7500:C",
        kind="option",
        symbol="SPX",
        qualified=True,
        subscribed=True,
        market_data_type=1,
        bid=10.0,
        ask=10.5,
        market_price=10.25,
        stale=False,
    )

    check = check_row(row)

    assert check.group == "spxw_options"
    assert check.status == "missing_greeks"
    assert check.has_bid_ask is True


def test_group_summary_flags_missing_required_p0_index() -> None:
    checks = [check_row(live_index("index:SPX"))]

    group = summarize_group(
        "p0_indexes",
        checks,
        required_labels=("index:SPX", "index:VIX"),
    )

    assert group.status == "failed"
    assert group.missing_required_labels == ["index:VIX"]


def test_report_status_is_not_rth_outside_trading_hours() -> None:
    rows = [
        live_index("index:SPX"),
        live_index("index:VIX"),
        live_index("index:VIX1D"),
        live_index("index:VIX9D"),
        live_index("index:VIX3M"),
        live_index("index:VVIX"),
        live_index("index:SKEW"),
        VerifyRow(
            label="stock:SPY",
            kind="stock",
            symbol="SPY",
            qualified=True,
            subscribed=True,
            market_data_type=1,
            market_price=750.0,
            stale=False,
        ),
        VerifyRow(
            label="future:ES",
            kind="future",
            symbol="ES",
            qualified=True,
            subscribed=True,
            market_data_type=1,
            market_price=7500.0,
            stale=False,
        ),
    ]

    payload = report_payload(
        settings=make_settings(),
        rows=rows,
        errors=[],
        connected=True,
        authenticated=True,
        latency_ms=1.0,
        skip_options=True,
        allow_outside_rth=False,
        generated_at=datetime(2026, 7, 5, 10, 0, tzinfo=NY_TZ),
    )

    assert payload["overall_status"] == "not_rth"


def test_report_status_available_inside_rth_when_required_groups_ok() -> None:
    rows = [
        live_index("index:SPX"),
        live_index("index:VIX"),
        live_index("index:VIX1D"),
        live_index("index:VIX9D"),
        live_index("index:VIX3M"),
        live_index("index:VVIX"),
        live_index("index:SKEW"),
        VerifyRow(
            label="stock:SPY",
            kind="stock",
            symbol="SPY",
            qualified=True,
            subscribed=True,
            market_data_type=1,
            market_price=750.0,
            stale=False,
        ),
        VerifyRow(
            label="future:ES",
            kind="future",
            symbol="ES",
            qualified=True,
            subscribed=True,
            market_data_type=1,
            market_price=7500.0,
            stale=False,
        ),
    ]

    payload = report_payload(
        settings=make_settings(),
        rows=rows,
        errors=[],
        connected=True,
        authenticated=True,
        latency_ms=1.0,
        skip_options=True,
        allow_outside_rth=False,
        generated_at=datetime(2026, 7, 6, 10, 0, tzinfo=NY_TZ),
    )

    assert payload["overall_status"] == "available"
