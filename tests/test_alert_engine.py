from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from spx_spark.alert_engine import evaluate_payload
from spx_spark.marketdata import InstrumentId, MarketDataQuality, Provider, Quote
from spx_spark.storage import LatestState


BJ_TZ = ZoneInfo("Asia/Shanghai")


def make_quote(
    instrument: InstrumentId,
    *,
    mark: float,
    close: float | None = None,
    quality: MarketDataQuality = MarketDataQuality.LIVE,
    now: datetime,
) -> Quote:
    return Quote(
        instrument=instrument,
        provider=Provider.IBKR,
        provider_symbol=instrument.canonical_id,
        received_at=now,
        quality=quality,
        mark=mark,
        close=close,
        quote_time=now,
    )


def make_state(*quotes: Quote, now: datetime) -> LatestState:
    return LatestState(
        created_at=now,
        as_of=now,
        quotes=tuple(quotes),
        best_quotes=tuple(quotes),
    )


def test_alert_engine_flags_missing_required_data_for_current_window() -> None:
    now = datetime(2026, 7, 6, 17, 0, tzinfo=BJ_TZ)
    state = make_state(now=now)

    payload = evaluate_payload(state, now=now)

    kinds = {alert["kind"] for alert in payload["alerts"]}
    assert payload["window"]["name"] == "early_premarket_dip_watch"
    assert payload["market_context"]["quality_summary"]["total_count"] >= 20
    assert "required_data_missing" in kinds
    assert payload["alert_count"] >= 4


def test_alert_engine_flags_large_move_from_close() -> None:
    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)
    spy = make_quote(
        InstrumentId.equity("SPY"),
        mark=754.0,
        close=750.0,
        now=now,
    )
    state = make_state(spy, now=now)

    payload = evaluate_payload(state, now=now)

    move_alerts = [
        alert for alert in payload["alerts"] if alert["kind"] == "price_move_from_close"
    ]
    assert payload["window"]["name"] == "close_one_hour"
    assert move_alerts
    assert move_alerts[0]["instrument_id"] == "equity:SPY"


def test_alert_engine_does_not_warn_for_optional_missing_at_critical_level() -> None:
    now = datetime(2026, 7, 7, 3, 15, tzinfo=BJ_TZ)
    state = make_state(now=now)

    payload = evaluate_payload(state, now=now)

    optional_missing = [
        alert for alert in payload["alerts"] if alert["kind"] == "optional_data_missing"
    ]
    assert optional_missing
    assert all(alert["severity"] == "low" for alert in optional_missing)
