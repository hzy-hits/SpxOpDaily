from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from spx_spark.intraday_shock import PriceSample, empty_monitor_state
from spx_spark.intraday_strategy import (
    CALL_WALL_BREAKOUT_CALL_KIND,
    FLIP_RECLAIM_CALL_KIND,
    IntradayStrategySettings,
    IntradayStructure,
    advance_intraday_strategy,
    confirmed_call_bias,
    mark_strategy_alert_attempts,
    structure_from_options_map,
)
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    OptionGreeks,
    Provider,
    Quote,
)
from spx_spark.storage import LatestState


UTC = timezone.utc


def sample(at: datetime, spx: float, es: float) -> PriceSample:
    return PriceSample(
        at=at,
        spx=spx,
        es=es,
        spx_source_at=at,
        es_source_at=at,
    )


def structure(at: datetime, **overrides) -> IntradayStructure:
    values = {
        "valid": True,
        "reason": None,
        "expiry": "20260710",
        "flip_low": 7495.0,
        "flip_high": 7500.0,
        "zero_gamma": 7498.0,
        "call_wall": 7520.0,
        "put_wall": 7460.0,
        "gamma_state": "negative_gamma",
        "net_gex": -10.0,
        "abs_gex": 100.0,
        "net_gamma_ratio": -0.1,
        "gex_quality": "open_interest_gex",
        "gex_weighting": "oi_plus_volume",
        "wall_method": "oi_gex",
        "observed_at": at,
    }
    values.update(overrides)
    return IntradayStructure(**values)


def advance(
    state,
    at,
    spx,
    es,
    *,
    struct=None,
    settings=IntradayStrategySettings(),
):
    return advance_intraday_strategy(
        state,
        sample(at, spx, es),
        struct or structure(at),
        settings,
    )


def test_flip_reclaim_requires_two_new_pairs_above_frozen_flip() -> None:
    start = datetime(2026, 7, 10, 14, 30, tzinfo=UTC)
    state = empty_monitor_state("2026-07-10")
    state, _, _ = advance(state, start, 7510.0, 5500.0)
    state, _, _ = advance(state, start + timedelta(seconds=5), 7508.0, 5498.0)
    state["active_event"] = {
        "event_id": "spx_shock:20260710:down:1430",
        "direction": "down",
        "anchor_at": start.isoformat(),
        "extreme_spx": 7485.0,
        "reclaim_confirmed_at": None,
        "spx_recovery_fraction": 0.0,
        "es_recovery_fraction": 0.0,
    }
    state, decision, _ = advance(state, start + timedelta(seconds=10), 7485.0, 5480.0)
    assert decision.status == "watch"

    event = dict(state["active_event"])
    event.update(
        {
            "reclaim_confirmed_at": (start + timedelta(seconds=15)).isoformat(),
            "spx_recovery_fraction": 0.70,
            "es_recovery_fraction": 0.50,
        }
    )
    state["active_event"] = event
    state, decision, signals = advance(state, start + timedelta(seconds=15), 7504.0, 5492.0)
    assert decision.status == "watch"
    assert not signals
    state, decision, signals = advance(state, start + timedelta(seconds=20), 7504.5, 5492.2)
    assert decision.status == "watch"
    assert not signals
    state, decision, signals = advance(state, start + timedelta(seconds=25), 7505.0, 5492.4)
    assert decision.flip_reclaim_call is True
    assert [row.kind for row in signals] == [FLIP_RECLAIM_CALL_KIND]
    assert decision.dealer_position_sign == "unknown"
    assert "not_dealer_position" in decision.signed_gex_sign_method


def test_gamma_sign_alone_never_creates_directional_bias() -> None:
    start = datetime(2026, 7, 10, 14, 30, tzinfo=UTC)
    for gamma_state in ("positive_gamma_pin", "negative_gamma", "zero_gamma_transition"):
        state = empty_monitor_state("2026-07-10")
        state, _, _ = advance(
            state,
            start,
            7510.0,
            5500.0,
            struct=structure(start, gamma_state=gamma_state),
        )
        state, decision, signals = advance(
            state,
            start + timedelta(seconds=5),
            7511.0,
            5501.0,
            struct=structure(start, gamma_state=gamma_state),
        )
        assert decision.conditional_call_bias is False
        assert not signals


def test_v_reclaim_that_never_broke_frozen_flip_does_not_become_flip_reclaim() -> None:
    start = datetime(2026, 7, 10, 14, 30, tzinfo=UTC)
    state = empty_monitor_state("2026-07-10")
    state, _, _ = advance(state, start, 7510.0, 5500.0)
    state, _, _ = advance(state, start + timedelta(seconds=5), 7508.0, 5499.0)
    state["active_event"] = {
        "event_id": "spx_shock:20260710:down:1430",
        "direction": "down",
        "anchor_at": start.isoformat(),
        "extreme_spx": 7502.0,
        "reclaim_confirmed_at": (start + timedelta(seconds=10)).isoformat(),
        "spx_recovery_fraction": 0.80,
        "es_recovery_fraction": 0.60,
    }

    for offset in (10, 15, 20):
        state, decision, signals = advance(
            state,
            start + timedelta(seconds=offset),
            7505.0,
            5501.0 + offset / 100,
        )

    assert decision.flip_reclaim_call is False
    assert not signals


def test_flip_reclaim_keeps_pre_shock_band_when_live_flip_drifts() -> None:
    start = datetime(2026, 7, 10, 14, 30, tzinfo=UTC)
    state = empty_monitor_state("2026-07-10")
    state, _, _ = advance(state, start, 7510.0, 5500.0)
    state, _, _ = advance(state, start + timedelta(seconds=5), 7508.0, 5499.0)
    state["active_event"] = {
        "event_id": "spx_shock:20260710:down:1430",
        "direction": "down",
        "anchor_at": start.isoformat(),
        "extreme_at": (start + timedelta(seconds=10)).isoformat(),
        "extreme_spx": 7485.0,
        "reclaim_confirmed_at": None,
        "spx_recovery_fraction": 0.0,
        "es_recovery_fraction": 0.0,
    }
    drifted = structure(start, flip_low=7510.0, flip_high=7515.0)
    state, decision, _ = advance(
        state,
        start + timedelta(seconds=10),
        7485.0,
        5480.0,
        struct=drifted,
    )
    assert decision.status == "watch"
    assert state["call_strategy"]["flip_watch"]["flip_high"] == 7500.0  # type: ignore[index]

    event = dict(state["active_event"])
    event.update(
        {
            "reclaim_confirmed_at": (start + timedelta(seconds=15)).isoformat(),
            "spx_recovery_fraction": 0.75,
            "es_recovery_fraction": 0.55,
        }
    )
    state["active_event"] = event
    for offset, spx, es in ((15, 7504.0, 5492.0), (20, 7504.5, 5492.2), (25, 7505.0, 5492.4)):
        state, decision, signals = advance(
            state,
            start + timedelta(seconds=offset),
            spx,
            es,
            struct=drifted,
        )

    assert decision.flip_reclaim_call is True
    assert decision.level == 7500.0
    assert signals[0].level == 7500.0


def test_call_wall_breakout_freezes_old_wall_when_live_wall_jumps() -> None:
    start = datetime(2026, 7, 10, 14, 30, tzinfo=UTC)
    state = empty_monitor_state("2026-07-10")
    state, _, _ = advance(state, start, 7518.0, 5500.0)
    state, _, _ = advance(state, start + timedelta(seconds=5), 7519.0, 5500.5)
    state, decision, _ = advance(state, start + timedelta(seconds=10), 7523.5, 5503.0)
    assert decision.status == "watch"

    jumped = structure(start, call_wall=7540.0)
    state, decision, signals = advance(
        state, start + timedelta(seconds=15), 7524.0, 5504.0, struct=jumped
    )
    assert decision.status == "watch"
    assert not signals
    state, decision, signals = advance(
        state, start + timedelta(seconds=20), 7525.0, 5505.0, struct=jumped
    )
    assert decision.call_wall_breakout_call is True
    assert decision.level == 7520.0
    assert [row.kind for row in signals] == [CALL_WALL_BREAKOUT_CALL_KIND]


def test_call_wall_cold_start_arms_provisional_pre_break_watch() -> None:
    start = datetime(2026, 7, 10, 14, 30, tzinfo=UTC)
    state = empty_monitor_state("2026-07-10")
    state, decision, _ = advance(state, start, 7519.0, 5500.0)
    assert decision.status == "watch"
    assert state["call_strategy"]["wall_watch"]["pre_cross_structure_samples"] == 1  # type: ignore[index]

    state, decision, _ = advance(state, start + timedelta(seconds=5), 7524.0, 5503.0)
    assert decision.status == "watch"
    state, decision, signals = advance(state, start + timedelta(seconds=10), 7525.0, 5504.0)
    assert decision.status == "watch"
    assert not signals
    state, decision, signals = advance(state, start + timedelta(seconds=15), 7526.0, 5505.0)
    assert decision.call_wall_breakout_call is True
    assert signals[0].kind == CALL_WALL_BREAKOUT_CALL_KIND


def test_provisional_wall_follows_pre_cross_structure_change() -> None:
    start = datetime(2026, 7, 10, 14, 30, tzinfo=UTC)
    state = empty_monitor_state("2026-07-10")
    state, _, _ = advance(state, start, 7518.0, 5500.0)
    moved = structure(start, call_wall=7540.0)
    state, decision, _ = advance(
        state,
        start + timedelta(seconds=5),
        7519.0,
        5500.5,
        struct=moved,
    )
    assert decision.status == "watch"
    assert state["call_strategy"]["wall_watch"]["level"] == 7540.0  # type: ignore[index]

    for offset, spx, es in ((10, 7524.0, 5503.0), (15, 7525.0, 5504.0), (20, 7526.0, 5505.0)):
        state, decision, signals = advance(
            state,
            start + timedelta(seconds=offset),
            spx,
            es,
            struct=moved,
        )

    assert decision.call_wall_breakout_call is False
    assert not signals


def test_same_tick_live_wall_jump_does_not_erase_old_wall_cross() -> None:
    start = datetime(2026, 7, 10, 14, 30, tzinfo=UTC)
    state = empty_monitor_state("2026-07-10")
    state, _, _ = advance(state, start, 7518.0, 5500.0)
    jumped = structure(start, call_wall=7540.0)
    state, decision, _ = advance(
        state,
        start + timedelta(seconds=5),
        7524.0,
        5503.0,
        struct=jumped,
    )
    assert decision.status == "watch"
    assert state["call_strategy"]["wall_watch"]["level"] == 7520.0  # type: ignore[index]
    assert state["call_strategy"]["wall_watch"]["crossed_at"]  # type: ignore[index]

    state, _, _ = advance(
        state,
        start + timedelta(seconds=10),
        7525.0,
        5504.0,
        struct=jumped,
    )
    state, decision, signals = advance(
        state,
        start + timedelta(seconds=15),
        7526.0,
        5505.0,
        struct=jumped,
    )
    assert decision.call_wall_breakout_call is True
    assert decision.level == 7520.0
    assert signals[0].level == 7520.0


def test_failed_wall_breakout_and_invalid_structure_do_not_confirm() -> None:
    start = datetime(2026, 7, 10, 14, 30, tzinfo=UTC)
    state = empty_monitor_state("2026-07-10")
    state, _, _ = advance(state, start, 7518.0, 5500.0)
    state, _, _ = advance(state, start + timedelta(seconds=5), 7519.0, 5500.5)
    state, _, _ = advance(state, start + timedelta(seconds=10), 7523.5, 5503.0)
    state, decision, signals = advance(state, start + timedelta(seconds=15), 7516.0, 5499.0)
    assert decision.conditional_call_bias is False
    assert not signals

    bad = structure(start, valid=False, reason="open_interest_gex_unavailable")
    state, decision, signals = advance(
        state, start + timedelta(seconds=20), 7525.0, 5505.0, struct=bad
    )
    assert decision.status == "neutral"
    assert decision.blocks == ("open_interest_gex_unavailable",)
    assert not signals


def test_delivery_retry_and_confirmed_bias_expiry() -> None:
    start = datetime(2026, 7, 10, 14, 30, tzinfo=UTC)
    cfg = IntradayStrategySettings(confirm_samples=1, retry_seconds=30, bias_ttl_seconds=60)
    state = empty_monitor_state("2026-07-10")
    state, _, _ = advance(state, start, 7518.0, 5500.0, settings=cfg)
    state, _, _ = advance(state, start + timedelta(seconds=5), 7519.0, 5500.5, settings=cfg)
    state, _, _ = advance(state, start + timedelta(seconds=10), 7523.5, 5503.0, settings=cfg)
    state, decision, signals = advance(
        state, start + timedelta(seconds=15), 7524.0, 5504.0, settings=cfg
    )
    assert decision.conditional_call_bias is True
    event_id = signals[0].event_id
    state = mark_strategy_alert_attempts(
        state, event_ids={event_id}, at=start + timedelta(seconds=15), delivered=False
    )
    state, _, signals = advance(state, start + timedelta(seconds=20), 7524.0, 5504.1, settings=cfg)
    assert not signals
    state, _, signals = advance(state, start + timedelta(seconds=46), 7524.0, 5504.2, settings=cfg)
    assert signals
    state = mark_strategy_alert_attempts(
        state, event_ids={event_id}, at=start + timedelta(seconds=46), delivered=True
    )
    state, _, signals = advance(state, start + timedelta(seconds=50), 7524.0, 5504.3, settings=cfg)
    assert not signals
    assert confirmed_call_bias(state, now=start + timedelta(seconds=59)) is not None
    assert confirmed_call_bias(state, now=start + timedelta(seconds=76)) is None
    state, decision, signals = advance(
        state,
        start + timedelta(seconds=76),
        7525.0,
        5505.0,
        settings=cfg,
    )
    assert decision.status == "neutral"
    assert not signals


def test_confirmed_bias_invalidates_below_level() -> None:
    start = datetime(2026, 7, 10, 14, 30, tzinfo=UTC)
    cfg = replace(IntradayStrategySettings(), confirm_samples=1)
    state = empty_monitor_state("2026-07-10")
    state, _, _ = advance(state, start, 7518.0, 5500.0, settings=cfg)
    state, _, _ = advance(state, start + timedelta(seconds=5), 7519.0, 5500.5, settings=cfg)
    state, _, _ = advance(state, start + timedelta(seconds=10), 7523.5, 5503.0, settings=cfg)
    state, decision, _ = advance(state, start + timedelta(seconds=15), 7524.0, 5504.0, settings=cfg)
    assert decision.conditional_call_bias
    state, decision, _ = advance(state, start + timedelta(seconds=20), 7516.0, 5499.0, settings=cfg)
    assert decision.conditional_call_bias is False


def test_transient_invalid_structure_preserves_unexpired_confirmed_bias() -> None:
    start = datetime(2026, 7, 10, 14, 30, tzinfo=UTC)
    cfg = replace(
        IntradayStrategySettings(),
        confirm_samples=1,
        structure_grace_seconds=30,
    )
    state = empty_monitor_state("2026-07-10")
    state, _, _ = advance(state, start, 7518.0, 5500.0, settings=cfg)
    state, _, _ = advance(state, start + timedelta(seconds=5), 7519.0, 5500.5, settings=cfg)
    state, _, _ = advance(state, start + timedelta(seconds=10), 7523.5, 5503.0, settings=cfg)
    state, decision, _ = advance(
        state,
        start + timedelta(seconds=15),
        7524.0,
        5504.0,
        settings=cfg,
    )
    assert decision.conditional_call_bias

    bad = structure(start, valid=False, reason="open_interest_gex_unavailable")
    state, decision, _ = advance(
        state,
        start + timedelta(seconds=20),
        7524.0,
        5504.1,
        struct=bad,
        settings=cfg,
    )
    assert decision.conditional_call_bias
    assert decision.blocks == ("open_interest_gex_unavailable",)
    state, decision, _ = advance(
        state,
        start + timedelta(seconds=55),
        7524.0,
        5504.2,
        struct=bad,
        settings=cfg,
    )
    assert decision.conditional_call_bias is False
    assert state["call_strategy"]["wall_watch"] is None  # type: ignore[index]

    state, decision, _ = advance(
        state,
        start + timedelta(seconds=60),
        7516.0,
        5499.0,
        struct=bad,
        settings=cfg,
    )
    assert decision.conditional_call_bias is False


def test_live_structure_requires_fresh_spxw_oi_gamma_at_key_strikes() -> None:
    now = datetime(2026, 7, 10, 14, 30, tzinfo=UTC)

    def option(strike: float, right: str, *, age_seconds: int = 0) -> Quote:
        at = now - timedelta(seconds=age_seconds)
        return Quote(
            instrument=InstrumentId.option(
                "SPX",
                expiry="20260710",
                strike=strike,
                right=right,
                trading_class="SPXW",
            ),
            provider=Provider.IBKR,
            received_at=at,
            quote_time=at,
            last_update_at=at,
            quality=MarketDataQuality.LIVE,
            market_data_type=1,
            bid=10.0,
            ask=10.2,
            open_interest=100.0,
            greeks=OptionGreeks(gamma=0.01),
        )

    quotes = (
        option(7495.0, "C"),
        option(7495.0, "P"),
        option(7500.0, "C"),
        option(7500.0, "P"),
        option(7520.0, "C"),
    )
    state = LatestState(
        created_at=now,
        as_of=now,
        quotes=quotes,
        best_quotes=quotes,
    )
    front = SimpleNamespace(
        expiry="20260710",
        gamma_flip_zone=(7495.0, 7500.0),
        zero_gamma=7498.0,
        call_wall=7520.0,
        put_wall=7460.0,
        gamma_state="negative_gamma",
        net_gex=-10.0,
        abs_gex=100.0,
        net_gamma_ratio=-0.1,
        gex_quality="open_interest_gex",
        gex_weighting="oi_plus_volume",
        wall_method="oi_gex",
    )
    options = SimpleNamespace(
        expiries=(front,),
        underlier=SimpleNamespace(source="index:SPX"),
        warnings=(),
    )

    live = structure_from_options_map(
        options,
        session_date="2026-07-10",
        observed_at=now,
        state=state,
    )
    assert live.valid is True
    assert live.flip_source_fresh is True
    assert live.call_wall_source_fresh is True

    stale_at = now - timedelta(seconds=121)
    stale_quotes = tuple(
        replace(
            row,
            last_update_at=stale_at,
            quote_time=stale_at,
            received_at=stale_at,
        )
        for row in quotes
    )
    stale_state = replace(state, quotes=stale_quotes, best_quotes=stale_quotes)
    stale = structure_from_options_map(
        options,
        session_date="2026-07-10",
        observed_at=now,
        state=stale_state,
    )
    assert stale.valid is False
    assert stale.reason == "key_structure_quotes_stale_or_unavailable"
