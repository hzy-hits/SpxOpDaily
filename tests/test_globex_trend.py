from __future__ import annotations

from datetime import datetime, timedelta, timezone

from spx_spark.application.globex_trend.machine import (
    advance_trend_state,
    initial_state,
)
from spx_spark.application.globex_trend.service import select_live_es
from spx_spark.marketdata import InstrumentId, MarketDataQuality, Provider, Quote
from spx_spark.notifier import direct_push_alerts
from spx_spark.settings.globex_trend import GlobexTrendSettings
from spx_spark.storage import LatestState


UTC = timezone.utc


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


def test_confirmed_globex_transition_is_a_direct_market_push() -> None:
    alert = {
        "kind": "globex_trend_transition",
        "instrument_id": "future:ES",
        "research_only": False,
    }

    assert direct_push_alerts([alert]) == [alert]


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
