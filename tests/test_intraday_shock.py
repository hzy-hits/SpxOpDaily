from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from spx_spark.alert_model import Alert
from spx_spark.intraday_shock import (
    RECLAIM_KIND,
    SHOCK_KIND,
    IntradayShockSettings,
    PriceSample,
    advance_monitor_state,
    empty_monitor_state,
    event_greek_shadow_due,
    mark_alert_attempts,
    mark_event_greek_shadow_sampled,
    reconcile_acknowledged_alerts,
    synchronized_live_sample,
)
from spx_spark.marketdata import InstrumentId, MarketDataQuality, Provider, Quote
from spx_spark.storage import LatestState


UTC = timezone.utc


def settings(tmp_path) -> IntradayShockSettings:
    return IntradayShockSettings(
        state_path=str(tmp_path / "shock.json"),
        one_minute_threshold_bps=20.0,
        three_minute_threshold_bps=35.0,
        reclaim_fraction=0.60,
        es_reclaim_fraction=0.40,
        reclaim_confirm_samples=2,
    )


def sample(
    at: datetime,
    spx: float,
    es: float,
    *,
    provider: str = Provider.UNKNOWN.value,
) -> PriceSample:
    return PriceSample(
        at=at,
        spx=spx,
        es=es,
        spx_source_at=at,
        es_source_at=at,
        provider=provider,
    )


def test_trump_style_down_shock_then_v_reclaim_is_two_phases(tmp_path) -> None:
    cfg = settings(tmp_path)
    start = datetime(2026, 7, 10, 14, 32, 26, tzinfo=UTC)
    state = empty_monitor_state("2026-07-10")

    state, alerts = advance_monitor_state(state, sample(start, 7556.30, 7602.75), cfg)
    assert alerts == []

    state, alerts = advance_monitor_state(
        state,
        sample(start + timedelta(seconds=22), 7536.53, 7582.00),
        cfg,
    )
    assert [alert.kind for alert in alerts] == [SHOCK_KIND]
    shock = alerts[0]
    assert shock.event_id == "spx_shock:20260710:down:1432"
    assert shock.value is not None and shock.value <= -20.0

    state = mark_alert_attempts(state, alerts, at=start + timedelta(seconds=22), delivered=True)
    state, alerts = advance_monitor_state(
        state,
        sample(start + timedelta(seconds=45), 7511.38, 7559.75),
        cfg,
    )
    assert alerts == []

    # One sample over 60% is only a watch; a second synchronized sample confirms.
    state, alerts = advance_monitor_state(
        state,
        sample(start + timedelta(minutes=2), 7539.00, 7587.00),
        cfg,
    )
    assert alerts == []
    state, alerts = advance_monitor_state(
        state,
        sample(start + timedelta(minutes=2, seconds=6), 7537.80, 7585.00),
        cfg,
    )
    assert [alert.kind for alert in alerts] == [RECLAIM_KIND]
    assert alerts[0].event_id == shock.event_id
    assert "V 反" in alerts[0].title
    assert "不自动生成入场" in alerts[0].detail


def test_provider_switch_cannot_create_a_cross_provider_shock(tmp_path) -> None:
    cfg = settings(tmp_path)
    start = datetime(2026, 7, 10, 14, 32, 26, tzinfo=UTC)
    state = empty_monitor_state("2026-07-10")

    state, _ = advance_monitor_state(
        state,
        sample(start, 7556.30, 7602.75, provider=Provider.IBKR.value),
        cfg,
    )
    state, alerts = advance_monitor_state(
        state,
        sample(
            start + timedelta(seconds=22),
            7536.53,
            7582.00,
            provider=Provider.SCHWAB.value,
        ),
        cfg,
    )

    assert alerts == []
    assert state["active_event"] is None


def test_provider_switch_cannot_confirm_reclaim_for_an_existing_event(tmp_path) -> None:
    cfg = settings(tmp_path)
    start = datetime(2026, 7, 10, 14, 32, 26, tzinfo=UTC)
    state = empty_monitor_state("2026-07-10")
    state, _ = advance_monitor_state(
        state,
        sample(start, 7556.30, 7602.75, provider=Provider.IBKR.value),
        cfg,
    )
    state, alerts = advance_monitor_state(
        state,
        sample(
            start + timedelta(seconds=22),
            7536.53,
            7582.00,
            provider=Provider.IBKR.value,
        ),
        cfg,
    )
    assert [alert.kind for alert in alerts] == [SHOCK_KIND]
    state = mark_alert_attempts(
        state,
        alerts,
        at=start + timedelta(seconds=22),
        delivered=True,
    )

    for seconds in (30, 35):
        state, alerts = advance_monitor_state(
            state,
            sample(
                start + timedelta(seconds=seconds),
                7550.0,
                7595.0,
                provider=Provider.SCHWAB.value,
            ),
            cfg,
        )
        assert alerts == []

    assert state["active_event"]["reclaim_streak"] == 0  # type: ignore[index]

    state, alerts = advance_monitor_state(
        state,
        sample(
            start + timedelta(seconds=61),
            7520.0,
            7565.0,
            provider=Provider.SCHWAB.value,
        ),
        cfg,
    )

    assert [alert.kind for alert in alerts] == [SHOCK_KIND]
    assert alerts[0].provider == Provider.SCHWAB.value
    assert alerts[0].event_id is not None
    assert alerts[0].event_id.endswith(":schwab")


def test_up_shock_and_down_reversal_are_symmetric(tmp_path) -> None:
    cfg = settings(tmp_path)
    start = datetime(2026, 7, 10, 15, 0, tzinfo=UTC)
    state = empty_monitor_state("2026-07-10")
    state, _ = advance_monitor_state(state, sample(start, 7500.0, 7550.0), cfg)
    state, alerts = advance_monitor_state(
        state,
        sample(start + timedelta(seconds=30), 7520.0, 7572.0),
        cfg,
    )
    assert [alert.kind for alert in alerts] == [SHOCK_KIND]
    assert alerts[0].event_id == "spx_shock:20260710:up:1500"
    assert "急拉" in alerts[0].title
    state = mark_alert_attempts(state, alerts, at=start + timedelta(seconds=30), delivered=True)

    state, _ = advance_monitor_state(
        state,
        sample(start + timedelta(seconds=40), 7525.0, 7576.0),
        cfg,
    )
    state, first = advance_monitor_state(
        state,
        sample(start + timedelta(minutes=1), 7509.0, 7560.0),
        cfg,
    )
    assert first == []
    state, second = advance_monitor_state(
        state,
        sample(start + timedelta(minutes=1, seconds=6), 7508.0, 7559.0),
        cfg,
    )
    assert [alert.kind for alert in second] == [RECLAIM_KIND]
    assert "倒 V" in second[0].title


def test_reclaim_requires_two_samples_and_es_confirmation(tmp_path) -> None:
    cfg = settings(tmp_path)
    start = datetime(2026, 7, 10, 14, 0, tzinfo=UTC)
    state = empty_monitor_state("2026-07-10")
    state, _ = advance_monitor_state(state, sample(start, 7500.0, 7550.0), cfg)
    state, alerts = advance_monitor_state(
        state,
        sample(start + timedelta(seconds=30), 7480.0, 7528.0),
        cfg,
    )
    state = mark_alert_attempts(state, alerts, at=start + timedelta(seconds=30), delivered=True)
    state, _ = advance_monitor_state(
        state,
        sample(start + timedelta(seconds=40), 7475.0, 7525.0),
        cfg,
    )

    # SPX recovers enough, but ES does not recover 40% of its own shock.
    state, alerts = advance_monitor_state(
        state,
        sample(start + timedelta(minutes=1), 7491.0, 7527.0),
        cfg,
    )
    assert alerts == []
    assert state["active_event"]["reclaim_streak"] == 0  # type: ignore[index]


def test_same_source_timestamps_do_not_fake_reclaim_confirmation(tmp_path) -> None:
    cfg = settings(tmp_path)
    start = datetime(2026, 7, 10, 14, 0, tzinfo=UTC)
    state = empty_monitor_state("2026-07-10")
    first = sample(start, 7500.0, 7550.0)
    state, _ = advance_monitor_state(state, first, cfg)
    duplicate_state, alerts = advance_monitor_state(state, first, cfg)
    assert alerts == []
    assert duplicate_state == state


def test_state_roundtrip_preserves_deterministic_event_id(tmp_path) -> None:
    cfg = settings(tmp_path)
    start = datetime(2026, 7, 10, 14, 0, tzinfo=UTC)
    state = empty_monitor_state("2026-07-10")
    state, _ = advance_monitor_state(state, sample(start, 7500.0, 7550.0), cfg)
    state, alerts = advance_monitor_state(
        state,
        sample(start + timedelta(seconds=30), 7480.0, 7528.0),
        cfg,
    )
    restored = json.loads(json.dumps(state))
    assert restored["active_event"]["event_id"] == alerts[0].event_id


def test_phase_ack_recovers_delivery_without_duplicate_retry(tmp_path) -> None:
    cfg = settings(tmp_path)
    start = datetime(2026, 7, 10, 14, 0, tzinfo=UTC)
    state = empty_monitor_state("2026-07-10")
    state, _ = advance_monitor_state(state, sample(start, 7500.0, 7550.0), cfg)
    state, alerts = advance_monitor_state(
        state,
        sample(start + timedelta(seconds=30), 7480.0, 7528.0),
        cfg,
    )
    assert len(alerts) == 1
    assert alerts[0].dedup_group is not None

    state, pending = reconcile_acknowledged_alerts(
        state,
        alerts,
        acknowledged_event_ids={str(alerts[0].dedup_group)},
        at=start + timedelta(seconds=35),
    )
    assert pending == []
    assert state["active_event"]["shock_delivered"] is True  # type: ignore[index]

    state, retried = advance_monitor_state(
        state,
        sample(start + timedelta(seconds=65), 7479.0, 7527.0),
        cfg,
    )
    assert retried == []


def test_failed_delivery_retries_only_after_backoff(tmp_path) -> None:
    cfg = settings(tmp_path)
    start = datetime(2026, 7, 10, 14, 0, tzinfo=UTC)
    state = empty_monitor_state("2026-07-10")
    state, _ = advance_monitor_state(state, sample(start, 7500.0, 7550.0), cfg)
    shock_at = start + timedelta(seconds=30)
    state, alerts = advance_monitor_state(state, sample(shock_at, 7480.0, 7528.0), cfg)
    assert [alert.kind for alert in alerts] == [SHOCK_KIND]
    state = mark_alert_attempts(state, alerts, at=shock_at, delivered=False)

    state, early = advance_monitor_state(
        state,
        sample(shock_at + timedelta(seconds=29), 7479.0, 7527.0),
        cfg,
    )
    assert early == []
    state, retry = advance_monitor_state(
        state,
        sample(shock_at + timedelta(seconds=31), 7478.0, 7526.0),
        cfg,
    )
    assert [alert.kind for alert in retry] == [SHOCK_KIND]


def test_strategy_delivery_state_never_marks_reclaim_delivered() -> None:
    now = datetime(2026, 7, 10, 14, 30, tzinfo=UTC)
    state = empty_monitor_state("2026-07-10")
    state["active_event"] = {
        "event_id": "spx_shock:20260710:down:1428",
        "reclaim_delivered": False,
    }
    state["call_strategy"] = {
        "schema_version": 1,
        "last_signal": {
            "event_id": "spx_call:flip_reclaim_call:7500:143000",
            "delivered": False,
        },
    }
    alert = Alert(
        severity="high",
        kind="flip_reclaim_call",
        instrument_id="index:SPX",
        title="flip reclaim",
        detail="confirmed",
        provider=Provider.IBKR.value,
        quality=MarketDataQuality.LIVE.value,
        dedup_group="spx_call:flip_reclaim_call:7500:143000:strategy",
        event_id="spx_call:flip_reclaim_call:7500:143000",
    )

    marked = mark_alert_attempts(state, [alert], at=now, delivered=True)

    assert marked["call_strategy"]["last_signal"]["delivered"] is True  # type: ignore[index]
    assert marked["active_event"]["reclaim_delivered"] is False  # type: ignore[index]

    recovered, pending = reconcile_acknowledged_alerts(
        state,
        [alert],
        acknowledged_event_ids={str(alert.dedup_group)},
        at=now,
    )
    assert pending == []
    assert recovered["call_strategy"]["last_signal"]["delivered"] is True  # type: ignore[index]
    assert recovered["active_event"]["reclaim_delivered"] is False  # type: ignore[index]


def test_event_greek_shadow_is_sampled_once_per_phase(tmp_path) -> None:
    cfg = settings(tmp_path)
    start = datetime(2026, 7, 10, 14, 0, tzinfo=UTC)
    state = empty_monitor_state("2026-07-10")
    state, _ = advance_monitor_state(state, sample(start, 7500.0, 7550.0), cfg)
    state, alerts = advance_monitor_state(
        state,
        sample(start + timedelta(seconds=30), 7480.0, 7528.0),
        cfg,
    )
    assert event_greek_shadow_due(state, alerts[0]) is True

    marked = mark_event_greek_shadow_sampled(
        state,
        alerts,
        at=start + timedelta(seconds=30),
    )

    assert event_greek_shadow_due(marked, alerts[0]) is False
    assert marked["active_event"]["shock_greeks_sampled_at"]  # type: ignore[index]


def _quote(
    instrument: InstrumentId,
    *,
    price: float,
    at: datetime,
    quality: MarketDataQuality = MarketDataQuality.LIVE,
    provider: Provider = Provider.IBKR,
    sampling_mode: str | None = None,
) -> Quote:
    return Quote(
        instrument=instrument,
        provider=provider,
        provider_symbol=instrument.canonical_id,
        received_at=at,
        quote_time=at,
        quality=quality,
        mark=price,
        market_data_type=1 if quality == MarketDataQuality.LIVE else 3,
        sampling_mode=sampling_mode,
    )


def test_synchronized_sample_rejects_stale_or_delayed_anchor(tmp_path) -> None:
    cfg = settings(tmp_path)
    now = datetime(2026, 7, 10, 14, 0, tzinfo=UTC)
    spx = _quote(InstrumentId.index("SPX"), price=7500.0, at=now)
    delayed_es = _quote(
        InstrumentId.future("ES"),
        price=7550.0,
        at=now,
        quality=MarketDataQuality.DELAYED,
    )
    state = LatestState(
        created_at=now,
        as_of=now,
        quotes=(spx, delayed_es),
        best_quotes=(spx, delayed_es),
    )
    result, reason = synchronized_live_sample(state, cfg)
    assert result is None
    assert reason == "non_live_or_stale_anchor"

    stale_spx = replace(
        spx, received_at=now - timedelta(seconds=20), quote_time=now - timedelta(seconds=20)
    )
    live_es = _quote(InstrumentId.future("ES"), price=7550.0, at=now)
    stale_state = LatestState(
        created_at=now,
        as_of=now,
        quotes=(stale_spx, live_es),
        best_quotes=(stale_spx, live_es),
    )
    result, reason = synchronized_live_sample(stale_state, cfg)
    assert result is None
    assert reason in {"non_live_or_stale_anchor", "stale_spx_anchor"}


def test_synchronized_sample_prefers_schwab_same_provider_pair(tmp_path) -> None:
    cfg = settings(tmp_path)
    now = datetime(2026, 7, 10, 14, 0, tzinfo=UTC)
    ibkr_spx = _quote(InstrumentId.index("SPX"), price=7499.0, at=now)
    ibkr_es = _quote(InstrumentId.future("ES"), price=7549.0, at=now)
    schwab_spx = _quote(
        InstrumentId.index("SPX"),
        price=7500.0,
        at=now,
        provider=Provider.SCHWAB,
        sampling_mode="schwab_stream",
    )
    schwab_es = _quote(
        InstrumentId.future("ES"),
        price=7550.0,
        at=now,
        provider=Provider.SCHWAB,
        sampling_mode="schwab_stream",
    )
    state = LatestState(
        created_at=now,
        as_of=now,
        quotes=(ibkr_spx, ibkr_es, schwab_spx, schwab_es),
        best_quotes=(schwab_spx, schwab_es),
    )

    result, reason = synchronized_live_sample(state, cfg)

    assert reason is None
    assert result is not None
    assert result.provider == Provider.SCHWAB.value
    assert result.spx == 7500.0
    assert result.es == 7550.0


def test_synchronized_sample_falls_back_to_ibkr_when_schwab_pair_is_stale(tmp_path) -> None:
    cfg = settings(tmp_path)
    now = datetime(2026, 7, 10, 14, 0, tzinfo=UTC)
    stale_at = now - timedelta(seconds=20)
    schwab_spx = _quote(
        InstrumentId.index("SPX"),
        price=7500.0,
        at=stale_at,
        provider=Provider.SCHWAB,
    )
    schwab_es = _quote(
        InstrumentId.future("ES"),
        price=7550.0,
        at=stale_at,
        provider=Provider.SCHWAB,
    )
    ibkr_spx = _quote(InstrumentId.index("SPX"), price=7501.0, at=now)
    ibkr_es = _quote(InstrumentId.future("ES"), price=7551.0, at=now)
    state = LatestState(
        created_at=now,
        as_of=now,
        quotes=(schwab_spx, schwab_es, ibkr_spx, ibkr_es),
        best_quotes=(ibkr_spx, ibkr_es),
    )

    result, reason = synchronized_live_sample(state, cfg)

    assert reason is None
    assert result is not None
    assert result.provider == Provider.IBKR.value
    assert result.spx == 7501.0
    assert result.es == 7551.0


def test_synchronized_sample_does_not_promote_fresh_schwab_rest_into_fast_lane(
    tmp_path,
) -> None:
    cfg = settings(tmp_path)
    now = datetime(2026, 7, 10, 14, 0, tzinfo=UTC)
    schwab_spx = _quote(
        InstrumentId.index("SPX"),
        price=7500.0,
        at=now,
        provider=Provider.SCHWAB,
    )
    schwab_es = _quote(
        InstrumentId.future("ES"),
        price=7550.0,
        at=now,
        provider=Provider.SCHWAB,
    )
    ibkr_spx = _quote(InstrumentId.index("SPX"), price=7501.0, at=now)
    ibkr_es = _quote(InstrumentId.future("ES"), price=7551.0, at=now)
    state = LatestState(
        created_at=now,
        as_of=now,
        quotes=(schwab_spx, schwab_es, ibkr_spx, ibkr_es),
        best_quotes=(schwab_spx, schwab_es),
    )

    result, reason = synchronized_live_sample(state, cfg)

    assert reason is None
    assert result is not None
    assert result.provider == Provider.IBKR.value


def test_es_extreme_is_tracked_independently_from_spx(tmp_path) -> None:
    cfg = settings(tmp_path)
    start = datetime(2026, 7, 10, 14, 0, tzinfo=UTC)
    state = empty_monitor_state("2026-07-10")
    state, _ = advance_monitor_state(state, sample(start, 7500.0, 7550.0), cfg)
    state, alerts = advance_monitor_state(
        state,
        sample(start + timedelta(seconds=30), 7480.0, 7530.0),
        cfg,
    )
    state = mark_alert_attempts(state, alerts, at=start + timedelta(seconds=30), delivered=True)
    state, _ = advance_monitor_state(
        state,
        sample(start + timedelta(seconds=36), 7481.0, 7520.0),
        cfg,
    )
    event = state["active_event"]
    assert event["extreme_spx"] == 7480.0  # type: ignore[index]
    assert event["extreme_es"] == 7520.0  # type: ignore[index]


def test_reclaim_streak_requires_both_source_timestamps_to_advance(tmp_path) -> None:
    cfg = settings(tmp_path)
    start = datetime(2026, 7, 10, 14, 0, tzinfo=UTC)
    state = empty_monitor_state("2026-07-10")
    state, _ = advance_monitor_state(state, sample(start, 7500.0, 7550.0), cfg)
    state, alerts = advance_monitor_state(
        state,
        sample(start + timedelta(seconds=30), 7480.0, 7528.0),
        cfg,
    )
    state = mark_alert_attempts(state, alerts, at=start + timedelta(seconds=30), delivered=True)
    state, _ = advance_monitor_state(
        state,
        sample(start + timedelta(seconds=40), 7475.0, 7525.0),
        cfg,
    )
    first_at = start + timedelta(minutes=1)
    state, alerts = advance_monitor_state(state, sample(first_at, 7491.0, 7541.0), cfg)
    assert alerts == []
    assert state["active_event"]["reclaim_streak"] == 1  # type: ignore[index]

    # ES advances, but SPX repeats the same source tick: no second confirmation.
    repeated_spx = PriceSample(
        at=first_at + timedelta(seconds=5),
        spx=7491.0,
        es=7542.0,
        spx_source_at=first_at,
        es_source_at=first_at + timedelta(seconds=5),
    )
    state, alerts = advance_monitor_state(state, repeated_spx, cfg)
    assert alerts == []
    assert state["active_event"]["reclaim_streak"] == 1  # type: ignore[index]

    both_fresh = sample(first_at + timedelta(seconds=10), 7492.0, 7542.5)
    state, alerts = advance_monitor_state(state, both_fresh, cfg)
    assert [alert.kind for alert in alerts] == [RECLAIM_KIND]


def test_expired_trend_requires_neutral_rearm_before_another_shock(tmp_path) -> None:
    cfg = settings(tmp_path)
    start = datetime(2026, 7, 10, 14, 0, tzinfo=UTC)
    state = empty_monitor_state("2026-07-10")
    state, _ = advance_monitor_state(state, sample(start, 7500.0, 7550.0), cfg)
    state, alerts = advance_monitor_state(
        state,
        sample(start + timedelta(seconds=30), 7480.0, 7528.0),
        cfg,
    )
    assert [alert.kind for alert in alerts] == [SHOCK_KIND]
    state = mark_alert_attempts(state, alerts, at=start + timedelta(seconds=30), delivered=True)

    # Keep extending the same trend beyond event expiry. A fresh event id must
    # not be minted just because the last three minutes also crossed 20 bps.
    for minute, spx, es in (
        (3, 7460.0, 7508.0),
        (6, 7440.0, 7488.0),
        (9, 7440.0, 7488.0),
        (12, 7440.0, 7488.0),
    ):
        state, alerts = advance_monitor_state(
            state,
            sample(start + timedelta(minutes=minute), spx, es),
            cfg,
        )
        assert alerts == []

    assert state["active_event"] is None
    assert state["rearm"]["direction"] == "down"  # type: ignore[index]


@pytest.mark.parametrize(
    ("spx_now", "es_now"),
    ((7480.0, 7555.0), (7520.0, 7545.0)),
)
def test_es_must_confirm_shock_direction(tmp_path, spx_now: float, es_now: float) -> None:
    cfg = settings(tmp_path)
    start = datetime(2026, 7, 10, 14, 0, tzinfo=UTC)
    state = empty_monitor_state("2026-07-10")
    state, _ = advance_monitor_state(state, sample(start, 7500.0, 7550.0), cfg)
    state, alerts = advance_monitor_state(
        state,
        sample(start + timedelta(seconds=30), spx_now, es_now),
        cfg,
    )
    assert alerts == []
