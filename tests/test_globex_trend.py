from __future__ import annotations

from datetime import datetime, timedelta, timezone

from spx_spark.application.globex_trend.machine import (
    advance_trend_state,
    initial_state,
)
from spx_spark.application.globex_trend.service import (
    alert_from_event,
    select_live_es,
    trend_context_id,
)
from spx_spark.marketdata import InstrumentId, MarketDataQuality, Provider, Quote
from spx_spark.notifier import direct_push_alerts
from spx_spark.settings.globex_trend import GlobexTrendSettings
from spx_spark.storage import LatestState


UTC = timezone.utc


def test_trend_context_resets_at_gth_and_rth_boundaries() -> None:
    assert trend_context_id(datetime(2026, 7, 13, 23, 0, tzinfo=UTC)).endswith(
        ":globex"
    )
    assert trend_context_id(datetime(2026, 7, 14, 0, 30, tzinfo=UTC)).endswith(":gth")
    assert trend_context_id(datetime(2026, 7, 14, 13, 30, tzinfo=UTC)).endswith(":rth")


def test_new_gth_context_can_confirm_a_short_impulse_before_sixty_minutes() -> None:
    policy = GlobexTrendSettings()
    start = datetime(2026, 7, 14, 0, 15, tzinfo=UTC)
    state = initial_state("2026-07-14:gth")
    transition = None
    for minute, price in ((0, 7600.0), (15, 7590.0), (16, 7589.0)):
        observed_at = start + timedelta(minutes=minute)
        state, transition = advance_trend_state(
            state,
            session_id="2026-07-14:gth",
            at=observed_at,
            price=price,
            provider="ibkr",
            source_at=observed_at,
            policy=policy,
        )

    assert transition is not None
    assert transition["to_regime"] == "bearish"
    assert transition["reason"] == "initial_short_impulse"


def test_globex_replay_detects_down_up_down_without_churn() -> None:
    policy = GlobexTrendSettings()
    start = datetime(2026, 7, 13, 0, 0, tzinfo=UTC)
    anchors = (
        (0, 7600.0),
        (60, 7590.0),
        (120, 7580.0),
        (180, 7570.0),
        (240, 7585.0),
        (300, 7605.0),
        (360, 7605.0),
        (390, 7590.0),
    )
    state = initial_state("2026-07-13")
    transitions: list[dict[str, object]] = []
    for minute, price in _minute_path(anchors):
        observed_at = start + timedelta(minutes=minute)
        state, transition = advance_trend_state(
            state,
            session_id="2026-07-13",
            at=observed_at,
            price=price,
            provider="schwab",
            source_at=observed_at,
            policy=policy,
        )
        if transition is not None:
            transitions.append(transition)

    assert [event["to_regime"] for event in transitions] == [
        "bearish",
        "bullish",
        "bearish",
    ]
    assert [event["reason"] for event in transitions] == [
        "multi_horizon_downtrend",
        "confirmed_reversal_from_regime_low",
        "confirmed_reversal_from_regime_high",
    ]


def test_duplicate_source_timestamp_is_not_sampled_twice() -> None:
    policy = GlobexTrendSettings()
    observed_at = datetime(2026, 7, 13, 0, 0, tzinfo=UTC)
    state, _ = advance_trend_state(
        initial_state("2026-07-13"),
        session_id="2026-07-13",
        at=observed_at,
        price=7600.0,
        provider="schwab",
        source_at=observed_at,
        policy=policy,
    )
    repeated, transition = advance_trend_state(
        state,
        session_id="2026-07-13",
        at=observed_at + timedelta(minutes=1),
        price=7590.0,
        provider="schwab",
        source_at=observed_at,
        policy=policy,
    )

    assert transition is None
    assert len(repeated["samples"]) == 1


def test_live_es_selection_uses_freshest_vendor_quote_and_falls_back() -> None:
    now = datetime(2026, 7, 13, 12, 0, tzinfo=UTC)
    schwab = _es_quote(Provider.SCHWAB, 7599.0, now - timedelta(seconds=8), now)
    ibkr = _es_quote(Provider.IBKR, 7600.0, now - timedelta(seconds=1), now)
    state = LatestState(now, now, (schwab, ibkr), (schwab,))

    selected = select_live_es(state, now=now, policy=GlobexTrendSettings())
    assert selected is ibkr

    stale_ibkr = _es_quote(Provider.IBKR, 7600.0, now - timedelta(seconds=120), now)
    fallback_state = LatestState(now, now, (schwab, stale_ibkr), (schwab,))
    selected = select_live_es(fallback_state, now=now, policy=GlobexTrendSettings())
    assert selected is schwab


def test_confirmed_globex_transition_is_direct() -> None:
    alert = {
        "kind": "globex_trend_transition",
        "instrument_id": "future:ES",
        "research_only": False,
    }

    assert direct_push_alerts([alert]) == [alert]


def test_es_transition_uses_rth_semantics_during_cash_session() -> None:
    event = {
        "event_id": "globex-trend:2026-07-13:1:bullish",
        "at": "2026-07-13T14:00:00+00:00",
        "source_at": "2026-07-13T14:00:00+00:00",
        "from_regime": "bearish",
        "to_regime": "bullish",
        "price": 7577.0,
        "provider": "ibkr",
        "metrics": {},
    }

    alert = alert_from_event(event)

    assert alert.severity == "info"
    assert alert.research_only is True
    assert alert.title == "ES RTH 多头趋势确认"
    assert "ES RTH 趋势确认切换" in alert.detail
    assert "不得按夜盘薄流动性解释" in alert.detail


def test_es_transition_keeps_globex_semantics_outside_cash_session() -> None:
    event = {
        "event_id": "globex-trend:2026-07-13:1:bullish",
        "at": "2026-07-13T12:00:00+00:00",
        "source_at": "2026-07-13T12:00:00+00:00",
        "from_regime": "bearish",
        "to_regime": "bullish",
        "price": 7577.0,
        "provider": "ibkr",
        "metrics": {},
    }

    alert = alert_from_event(event)

    assert alert.title == "ES Globex 多头趋势确认"
    assert "现金盘外" in alert.detail


def _minute_path(
    anchors: tuple[tuple[int, float], ...],
) -> list[tuple[int, float]]:
    rows: list[tuple[int, float]] = []
    for (start_minute, start_price), (end_minute, end_price) in zip(anchors, anchors[1:]):
        for minute in range(start_minute, end_minute):
            fraction = (minute - start_minute) / (end_minute - start_minute)
            rows.append((minute, start_price + (end_price - start_price) * fraction))
    rows.append(anchors[-1])
    return rows


def _es_quote(
    provider: Provider,
    price: float,
    source_at: datetime,
    received_at: datetime,
) -> Quote:
    return Quote(
        instrument=InstrumentId.future("ES"),
        provider=provider,
        received_at=received_at,
        last_update_at=received_at,
        quote_time=source_at,
        quality=MarketDataQuality.LIVE,
        last=price,
    )
