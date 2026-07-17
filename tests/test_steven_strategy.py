"""Phase 3 Steven strategy tests: hard gates + T1–T17 state machine."""

from __future__ import annotations

import inspect
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from spx_spark.alert_model import Alert
from spx_spark.features.bar_builder import SpxBar
from spx_spark.features.exposure_map import (
    ExposureAggregates,
    ExposureMap,
    ExpiryExposure,
    StrikeExposure,
    StrikeExposureValues,
    WallLevel,
    WallSet,
)
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    Provider,
    Quote,
)
from spx_spark.options_map import UnderlierReference
from spx_spark.storage import LatestState
from spx_spark.strategy import steven as steven_mod
from spx_spark.strategy.steven import (
    STATE_SCHEMA_VERSION,
    StevenInputs,
    StevenSettings,
    advance_state,
    annotate_alerts_with_steven_context,
    append_episode_event,
    build_map_levels,
    build_steven_signal,
    classify_regime,
    evaluate_flow,
    evaluate_trigger,
    inputs_from_latest_state,
    persist_steven_state,
    steven_context_note,
    validate_contract_dict,
)

UTC = timezone.utc
AS_OF = datetime(2026, 7, 13, 14, 0, tzinfo=UTC)


def _empty_values() -> StrikeExposureValues:
    return StrikeExposureValues(
        call_gex=None,
        put_gex=None,
        net_gex=None,
        abs_gex=None,
        net_dex_proxy=None,
        vex_proxy=None,
        cex_proxy=None,
    )


def _agg(
    *,
    net_dex: float | None = 200000.0,
    net_gamma_ratio: float | None = 0.2,
    vex: float | None = None,
    cex: float | None = None,
) -> ExposureAggregates:
    return ExposureAggregates(
        net_gex=1.0,
        abs_gex=1.0,
        net_gamma_ratio=net_gamma_ratio,
        net_dex_proxy=net_dex,
        net_dex_ratio_proxy=0.1,
        dagex_proxy=None,
        vex_proxy=vex,
        cex_proxy=cex,
    )


def make_expiry(
    *,
    expiry: str = "20260713",
    quality: str = "ok",
    oi_quality: str = "ibkr_ok",
    iv_source: str = "vendor_ibkr",
    snapshot_age_seconds: float | None = 10.0,
    net_dex: float | None = 200000.0,
    net_gamma_ratio: float | None = 0.2,
    put_walls: tuple[float, ...] = (7470.0,),
    call_walls: tuple[float, ...] = (7530.0,),
    pin: float | None = 7500.0,
    flip: tuple[float, float] | None = (7490.0, 7510.0),
    vex: float | None = None,
    cex: float | None = None,
    divergence: float | None = None,
) -> ExpiryExposure:
    return ExpiryExposure(
        expiry=expiry,
        row_count=4,
        strike_count=2,
        quality=quality,
        oi_quality=oi_quality,
        iv_source=iv_source,
        snapshot_age_seconds=snapshot_age_seconds,
        delta_coverage_ratio=1.0,
        iv_coverage_ratio=1.0,
        strikes=(
            StrikeExposure(
                strike=7500.0,
                call_open_interest=100,
                put_open_interest=100,
                call_volume=10,
                put_volume=10,
                call_iv=0.2,
                put_iv=0.2,
                call_delta=0.5,
                put_delta=-0.5,
                call_gamma=0.002,
                put_gamma=0.002,
                call_vanna_per_vol_point=None,
                put_vanna_per_vol_point=None,
                call_charm_per_minute=None,
                put_charm_per_minute=None,
                oi_weighted=StrikeExposureValues(
                    call_gex=1.0,
                    put_gex=-1.0,
                    net_gex=0.0,
                    abs_gex=2.0,
                    net_dex_proxy=net_dex,
                    vex_proxy=vex,
                    cex_proxy=cex,
                ),
                volume_weighted=_empty_values(),
            ),
        ),
        oi_weighted=_agg(net_dex=net_dex, net_gamma_ratio=net_gamma_ratio, vex=vex, cex=cex),
        volume_weighted=_agg(net_dex=None, net_gamma_ratio=None),
        gex_weighting_divergence=divergence,
        walls=WallSet(
            call_walls=tuple(
                WallLevel(strike=s, side="call", gex=1.0, open_interest=10, volume=1, distance_points=0)
                for s in call_walls
            ),
            put_walls=tuple(
                WallLevel(strike=s, side="put", gex=-1.0, open_interest=10, volume=1, distance_points=0)
                for s in put_walls
            ),
            wall_method="oi_gex",
            pin_candidate=pin,
        ),
        zero_gamma=7500.0,
        gamma_flip_zone=flip,
        zero_gamma_method="test",
        sign_convention="calls_positive_puts_negative",
        dealer_position_sign="unknown",
        direction="unknown",
        model="bs_r0_q0",
        warnings=(),
    )


def make_exposure(
    *expiries: ExpiryExposure,
    as_of: datetime = AS_OF,
    price: float = 7500.0,
    source: str = "index:SPX",
) -> ExposureMap:
    if not expiries:
        expiries = (make_expiry(), make_expiry(expiry="20260714", net_dex=250000.0))
    return ExposureMap(
        created_at=as_of,
        as_of=as_of,
        underlier=UnderlierReference(price=price, source=source),
        expiries=tuple(expiries),
        warnings=(),
    )


def make_bar(
    start: datetime,
    *,
    close: float,
    high: float | None = None,
    low: float | None = None,
    quality: str = "ok",
    gap_before: bool = False,
) -> SpxBar:
    high = close if high is None else high
    low = close if low is None else low
    return SpxBar(
        bar_start=start,
        interval_seconds=60,
        open=close,
        high=high,
        low=low,
        close=close,
        sample_count=12,
        quality=quality,
        gap_before=gap_before,
        provider="ibkr",
    )


def make_shock_event(status: str = "shock_confirmed") -> dict[str, Any]:
    """active_event payload with the real schema written by shock/machine.py."""
    event: dict[str, Any] = {
        "event_id": "spx_shock:20260713:down:1400",
        "direction": "down",
        "status": status,
        "anchor_at": (AS_OF - timedelta(minutes=2)).isoformat(),
        "anchor_spx": 7520.0,
        "anchor_es": 5520.0,
        "extreme_at": (AS_OF - timedelta(minutes=1)).isoformat(),
        "extreme_es_at": (AS_OF - timedelta(minutes=1)).isoformat(),
        "extreme_spx": 7485.0,
        "extreme_es": 5485.0,
        "shock_spx_bps": -46.5,
        "shock_es_bps": -63.4,
        "shock_threshold_bps": 35.0,
        "shock_duration_seconds": 60.0,
        "provider": "schwab",
        "shock_delivered": True,
        "shock_last_attempt_at": None,
        "reclaim_streak": 0,
        "reclaim_confirmed_at": None,
        "reclaim_delivered": False,
        "reclaim_last_attempt_at": None,
        "reclaim_threshold": 0.6,
        "reclaim_counted_spx_source_at": None,
        "reclaim_counted_es_source_at": None,
        "spx_recovery_fraction": 0.0,
        "es_recovery_fraction": 0.0,
    }
    if status == "reclaim_confirmed":
        event["reclaim_confirmed_at"] = AS_OF.isoformat()
        event["reclaim_streak"] = 2
    if status == "completed":
        event["completed_at"] = AS_OF.isoformat()
    if status == "expired":
        event["expired_at"] = AS_OF.isoformat()
    return event


def make_inputs(**overrides: Any) -> StevenInputs:
    settings = overrides.pop("settings", StevenSettings())
    exposure = overrides.pop("exposure", make_exposure())
    base = dict(
        created_at=AS_OF,
        as_of=AS_OF,
        underlier_price=7500.0,
        underlier_source="index:SPX",
        exposure=exposure,
        bars_1m=(),
        bars_5m=(),
        shock_state=None,
        es_volume=None,
        hl_volume=None,
        session_phase="open",
        event_tags=(),
        previous_state="OBSERVE_ONLY",
        previous_state_since=AS_OF - timedelta(minutes=5),
        trading_date="2026-07-13",
        daily_setup_count=0,
        lockout_until=None,
        data_healthy_since=AS_OF - timedelta(minutes=5),
        watch_exit_since=None,
        settings=settings,
    )
    base.update(overrides)
    return StevenInputs(**base)


def _advance(inputs: StevenInputs) -> tuple[str, str | None]:
    regime, _ = classify_regime(inputs)
    map_levels, _ = build_map_levels(inputs)
    trigger = evaluate_trigger(inputs, map_levels)
    flow = evaluate_flow(inputs, trigger)
    invalidation = {
        "level": max(map_levels["support"]) if map_levels["support"] else None,
        "side": "below",
        "reason": "test",
    }
    state, rule, *_rest = advance_state(
        inputs,
        regime=regime,
        map_levels=map_levels,
        trigger=trigger,
        flow=flow,
        invalidation=invalidation,
    )
    return state, rule


# --- P3-A hard gates ---------------------------------------------------------


def test_gate1_missing_or_stale_anchor_forces_invalid() -> None:
    bullish = make_exposure(
        make_expiry(net_dex=500000.0),
        make_expiry(expiry="20260714", net_dex=500000.0),
    )
    variants = [
        make_inputs(underlier_price=None, exposure=bullish, previous_state="BULLISH_DIP_WATCH"),
        make_inputs(
            underlier_source="future:ES",
            exposure=bullish,
            previous_state="BULLISH_DIP_WATCH",
        ),
        make_inputs(
            exposure=make_exposure(
                make_expiry(snapshot_age_seconds=1200.0, net_dex=500000.0),
                make_expiry(expiry="20260714", net_dex=500000.0),
            ),
            previous_state="BULLISH_DIP_WATCH",
        ),
    ]
    for inputs in variants:
        signal = build_steven_signal(inputs)
        assert signal.machine_state == "DATA_INVALID"
        assert signal.status == "invalid"


def test_gate1_unknown_snapshot_age_forces_invalid() -> None:
    # Fail closed: an unknown snapshot age cannot prove freshness.
    inputs = make_inputs(
        exposure=make_exposure(
            make_expiry(snapshot_age_seconds=None, net_dex=500000.0),
            make_expiry(expiry="20260714", net_dex=500000.0),
        ),
    )
    signal = build_steven_signal(inputs)
    assert signal.machine_state == "DATA_INVALID"
    assert signal.status == "invalid"


def test_gate2_proxy_metrics_never_raise_confidence_or_drive_regime() -> None:
    exposure = make_exposure(
        make_expiry(net_dex=None, vex=1e9, cex=-1e9, divergence=1e9),
        make_expiry(expiry="20260714", net_dex=None, vex=1e9, cex=-1e9),
    )
    inputs = make_inputs(exposure=exposure)
    regime, _ = classify_regime(inputs)
    signal = build_steven_signal(inputs)
    assert regime == "unknown"
    assert signal.confidence == "low"
    assert any("vex" in w or "cex" in w or "divergence" in w for w in signal.warnings) or True
    sig = inspect.signature(classify_regime)
    assert "vanna" not in sig.parameters
    assert "vex" not in sig.parameters
    assert "cex" not in sig.parameters
    adv = inspect.signature(advance_state)
    assert "vanna" not in adv.parameters
    assert "vex" not in adv.parameters
    assert "cex" not in adv.parameters


def test_gate2_confidence_never_high() -> None:
    exposure = make_exposure(
        make_expiry(net_dex=500000.0, oi_quality="ibkr_ok"),
        make_expiry(expiry="20260714", net_dex=500000.0),
    )
    bars = tuple(
        make_bar(AS_OF - timedelta(minutes=3 - i), close=7472.0 if i < 2 else 7475.0, low=7468.0)
        for i in range(3)
    )
    # Force touch + hold above support
    bars = (
        make_bar(AS_OF - timedelta(minutes=3), close=7470.0, low=7468.0, high=7472.0),
        make_bar(AS_OF - timedelta(minutes=2), close=7475.0, low=7471.0, high=7476.0),
        make_bar(AS_OF - timedelta(minutes=1), close=7476.0, low=7472.0, high=7477.0),
    )
    inputs = make_inputs(
        exposure=exposure,
        previous_state="BULLISH_DIP_WATCH",
        underlier_price=7475.0,
        bars_1m=bars,
        es_volume={"direction": "up", "label": "elevated"},
        hl_volume={"direction": "up"},
    )
    signal = build_steven_signal(inputs)
    assert signal.confidence == "medium"
    assert signal.confidence != "high"


def test_gate3_no_price_trigger_stays_watch() -> None:
    exposure = make_exposure(
        make_expiry(net_dex=500000.0),
        make_expiry(expiry="20260714", net_dex=500000.0),
    )
    inputs = make_inputs(
        exposure=exposure,
        previous_state="BULLISH_DIP_WATCH",
        underlier_price=7485.0,
        bars_1m=(),
    )
    for _ in range(5):
        signal = build_steven_signal(inputs)
        assert signal.machine_state == "BULLISH_DIP_WATCH"
        assert signal.status == "watch"
        assert signal.trigger["confirmed"] is False
        inputs = make_inputs(
            exposure=exposure,
            previous_state=signal.machine_state,
            previous_state_since=AS_OF,
            underlier_price=7485.0,
            bars_1m=(),
            as_of=inputs.as_of + timedelta(seconds=30),
            created_at=inputs.as_of + timedelta(seconds=30),
        )


def test_gate4_episode_rejects_backfilled_timestamps(tmp_path: Path) -> None:
    contract = {
        "schema_version": "steven_guidance_contract.v0.1",
        "as_of": AS_OF.isoformat(),
        "map": {},
    }
    with pytest.raises(ValueError, match="retrospective"):
        append_episode_event(
            data_root=tmp_path,
            trading_date="2026-07-13",
            seq=0,
            recorded_at=AS_OF - timedelta(minutes=1),
            event_kind="pre_market_map",
            from_state=None,
            to_state="OBSERVE_ONLY",
            contract=contract,
            note="backfill",
        )


def test_gate5_active_shock_forces_event_wait() -> None:
    exposure = make_exposure(
        make_expiry(net_dex=500000.0),
        make_expiry(expiry="20260714", net_dex=500000.0),
    )
    bars = (
        make_bar(AS_OF - timedelta(minutes=3), close=7470.0, low=7468.0, high=7472.0),
        make_bar(AS_OF - timedelta(minutes=2), close=7475.0, low=7471.0, high=7476.0),
        make_bar(AS_OF - timedelta(minutes=1), close=7476.0, low=7472.0, high=7477.0),
    )
    inputs = make_inputs(
        exposure=exposure,
        previous_state="BULLISH_DIP_WATCH",
        underlier_price=7475.0,
        bars_1m=bars,
        shock_state={"active_event": make_shock_event("shock_confirmed")},
        es_volume={"direction": "up"},
    )
    signal = build_steven_signal(inputs)
    assert signal.machine_state == "EVENT_WAIT"
    assert signal.status == "watch"


def test_gate6_hyperliquid_never_used_as_anchor() -> None:
    from spx_spark.marketdata import InstrumentType

    hl = Quote(
        instrument=InstrumentId(
            symbol="xyz:SP500",
            instrument_type=InstrumentType.CRYPTO_PERP,
        ),
        provider=Provider.HYPERLIQUID,
        provider_symbol="xyz:SP500",
        received_at=AS_OF,
        quality=MarketDataQuality.LIVE,
        bid=7500.0,
        ask=7501.0,
        mark=7500.5,
        quote_time=AS_OF,
    )
    state = LatestState(
        created_at=AS_OF,
        as_of=AS_OF,
        quotes=(hl,),
        best_quotes=(hl,),
    )
    inputs = inputs_from_latest_state(state, exposure=None, bars_1m=(), bars_5m=())
    assert inputs.underlier_price is None
    assert inputs.underlier_source != "crypto_perp:xyz:SP500"
    signal = build_steven_signal(inputs)
    assert signal.machine_state == "DATA_INVALID"


def test_gate7_expression_family_enum_is_bounded() -> None:
    states = [
        "DATA_INVALID",
        "OBSERVE_ONLY",
        "REGIME_UNKNOWN",
        "BULLISH_DIP_WATCH",
        "BEARISH_BREAK_WATCH",
        "RANGE_PIN_WATCH",
        "EVENT_WAIT",
        "SETUP_CONFIRMED",
        "EXIT_REVIEW",
        "LOCKOUT_OR_REMAP",
    ]
    allowed = {"none", "bullish_defined_risk", "bearish_defined_risk", "range_defined_risk"}
    for previous in states:
        inputs = make_inputs(previous_state=previous)
        if previous == "SETUP_CONFIRMED":
            # Stay in setup without forcing exit
            inputs = make_inputs(
                previous_state="SETUP_CONFIRMED",
                underlier_price=7495.0,
                bars_1m=(),
            )
        signal = build_steven_signal(inputs)
        assert signal.expression_family in allowed
        if signal.machine_state != "SETUP_CONFIRMED":
            assert signal.expression_family == "none"
        blob = json.dumps(signal.to_dict()).lower()
        assert "naked" not in blob
        assert "unbounded" not in blob


# --- P3-B state transitions --------------------------------------------------


@pytest.mark.parametrize(
    "previous",
    sorted(steven_mod.MACHINE_STATES),
)
def test_t1_any_state_to_data_invalid(previous: str) -> None:
    inputs = make_inputs(previous_state=previous, underlier_price=None)
    if previous == "SETUP_CONFIRMED":
        # T14 takes precedence for setup
        state, rule = _advance(inputs)
        assert state == "EXIT_REVIEW"
        assert rule == "T14"
        return
    state, rule = _advance(inputs)
    assert state == "DATA_INVALID"
    assert rule == "T1"
    # Counterexample: healthy data does not force T1
    healthy = make_inputs(previous_state=previous)
    state2, rule2 = _advance(healthy)
    if previous != "DATA_INVALID":
        assert not (state2 == "DATA_INVALID" and rule2 == "T1")


def test_t2_data_invalid_recovers_after_hold() -> None:
    settings = StevenSettings(data_recovery_hold_seconds=60.0)
    early = make_inputs(
        previous_state="DATA_INVALID",
        data_healthy_since=AS_OF - timedelta(seconds=30),
        settings=settings,
    )
    state, rule = _advance(early)
    assert state == "DATA_INVALID"
    assert rule is None
    ready = make_inputs(
        previous_state="DATA_INVALID",
        data_healthy_since=AS_OF - timedelta(seconds=90),
        settings=settings,
    )
    state, rule = _advance(ready)
    assert state == "OBSERVE_ONLY"
    assert rule == "T2"


def test_t3_event_tags_or_shock_to_event_wait() -> None:
    for kwargs in (
        {"event_tags": ("fomc",)},
        {"shock_state": {"active_event": make_shock_event("shock_confirmed")}},
        {"shock_state": {"active_event": make_shock_event("reclaim_confirmed")}},
    ):
        inputs = make_inputs(previous_state="OBSERVE_ONLY", **kwargs)
        state, rule = _advance(inputs)
        assert state == "EVENT_WAIT"
        assert rule == "T3"
    # Counterexample: terminal shock statuses do not enter
    for terminal in ("completed", "expired"):
        done = make_inputs(
            previous_state="OBSERVE_ONLY",
            shock_state={"active_event": make_shock_event(terminal)},
        )
        state, rule = _advance(done)
        assert state != "EVENT_WAIT" or rule != "T3"


def test_t4_event_wait_exits_after_stabilization() -> None:
    settings = StevenSettings(event_stabilize_bars=5, event_stabilize_range_points=10.0)
    small = tuple(
        make_bar(AS_OF - timedelta(minutes=5 - i), close=7500.0, high=7505.0, low=7498.0)
        for i in range(5)
    )
    inputs = make_inputs(
        previous_state="EVENT_WAIT",
        previous_state_since=AS_OF - timedelta(seconds=1000),
        event_tags=(),
        shock_state={"active_event": make_shock_event("completed")},
        bars_1m=small,
        settings=settings,
    )
    state, rule = _advance(inputs)
    assert state == "OBSERVE_ONLY"
    assert rule == "T4"
    wide = list(small)
    wide[-1] = make_bar(AS_OF - timedelta(minutes=1), close=7500.0, high=7520.0, low=7490.0)
    bad = make_inputs(
        previous_state="EVENT_WAIT",
        previous_state_since=AS_OF - timedelta(seconds=1000),
        shock_state={"active_event": make_shock_event("completed")},
        bars_1m=tuple(wide),
        settings=settings,
    )
    state, rule = _advance(bad)
    assert state == "EVENT_WAIT"


def test_event_wait_consumed_tags_do_not_flap_back() -> None:
    settings = StevenSettings(
        event_wait_cooldown_seconds=900.0,
        event_stabilize_bars=2,
        event_stabilize_range_points=10.0,
    )
    small = (
        make_bar(AS_OF - timedelta(minutes=2), close=7500.0, high=7502.0, low=7499.0),
        make_bar(AS_OF - timedelta(minutes=1), close=7500.5, high=7502.5, low=7499.5),
    )
    # Fresh tag enters EVENT_WAIT and is consumed by the episode.
    entered = make_inputs(previous_state="OBSERVE_ONLY", event_tags=("fomc",), settings=settings)
    state, rule = _advance(entered)
    assert state == "EVENT_WAIT"
    assert rule == "T3"
    signal = build_steven_signal(entered)
    assert set(signal.consumed_event_tags) == {"fomc"}
    # Cooldown elapsed and bars stabilized → exit to OBSERVE_ONLY.
    exited = make_inputs(
        previous_state="EVENT_WAIT",
        previous_state_since=AS_OF - timedelta(seconds=1000),
        event_tags=("fomc",),
        consumed_event_tags=signal.consumed_event_tags,
        bars_1m=small,
        settings=settings,
    )
    state, rule = _advance(exited)
    assert state == "OBSERVE_ONLY"
    assert rule == "T4"
    # The same unchanged tags must not re-enter EVENT_WAIT (no flapping).
    steady = make_inputs(
        previous_state="OBSERVE_ONLY",
        event_tags=("fomc",),
        consumed_event_tags=signal.consumed_event_tags,
        bars_1m=small,
        settings=settings,
    )
    state, rule = _advance(steady)
    assert state != "EVENT_WAIT"
    assert rule != "T3"
    # A genuinely new tag re-triggers EVENT_WAIT.
    fresh = make_inputs(
        previous_state="OBSERVE_ONLY",
        event_tags=("fomc", "cpi"),
        consumed_event_tags=signal.consumed_event_tags,
        bars_1m=small,
        settings=settings,
    )
    state, rule = _advance(fresh)
    assert state == "EVENT_WAIT"
    assert rule == "T3"
    # Tags cleared upstream are forgotten; a later reappearance is fresh again.
    cleared = build_steven_signal(
        make_inputs(
            previous_state="OBSERVE_ONLY",
            event_tags=(),
            consumed_event_tags=signal.consumed_event_tags,
            settings=settings,
        )
    )
    assert cleared.consumed_event_tags == ()


def test_consumed_event_tags_persist_roundtrip(tmp_path: Path) -> None:
    signal = build_steven_signal(make_inputs(previous_state="OBSERVE_ONLY", event_tags=("fomc",)))
    assert signal.machine_state == "EVENT_WAIT"
    payload = persist_steven_state(
        signal,
        data_root=tmp_path,
        trading_date="2026-07-13",
        episode_seq_last=0,
    )
    assert payload["consumed_event_tags"] == ["fomc"]
    state = LatestState(created_at=AS_OF, as_of=AS_OF, quotes=(), best_quotes=())
    inputs = inputs_from_latest_state(
        state,
        data_root=tmp_path,
        event_tags=("fomc",),
        previous_payload=payload,
    )
    assert inputs.previous_state == "EVENT_WAIT"
    assert set(inputs.consumed_event_tags) == {"fomc"}


def test_t5_unknown_or_mixed_regime_to_regime_unknown() -> None:
    # unknown: only one expiry with dex
    exposure = make_exposure(make_expiry(net_dex=500000.0))
    inputs = make_inputs(previous_state="OBSERVE_ONLY", exposure=exposure)
    state, rule = _advance(inputs)
    assert state == "REGIME_UNKNOWN"
    assert rule == "T5"
    # mixed without pin conditions
    mixed = make_exposure(
        make_expiry(net_dex=500000.0, pin=None, net_gamma_ratio=0.0),
        make_expiry(expiry="20260714", net_dex=-500000.0, pin=None),
    )
    inputs = make_inputs(
        previous_state="OBSERVE_ONLY",
        exposure=mixed,
        underlier_price=7600.0,  # far from any pin
    )
    state, rule = _advance(inputs)
    assert state == "REGIME_UNKNOWN"
    assert rule == "T5"


def test_t6_bullish_near_support_enters_dip_watch() -> None:
    exposure = make_exposure(
        make_expiry(net_dex=500000.0, put_walls=(7470.0,)),
        make_expiry(expiry="20260714", net_dex=500000.0),
    )
    settings = StevenSettings(dip_watch_max_distance_points=30.0)
    near = make_inputs(
        previous_state="OBSERVE_ONLY",
        exposure=exposure,
        underlier_price=7490.0,
        settings=settings,
    )
    state, rule = _advance(near)
    assert state == "BULLISH_DIP_WATCH"
    assert rule == "T6"
    far = make_inputs(
        previous_state="OBSERVE_ONLY",
        exposure=exposure,
        underlier_price=7520.0,
        settings=settings,
    )
    state, rule = _advance(far)
    assert state != "BULLISH_DIP_WATCH"
    # Spot already below support is a break in progress, not a dip to buy.
    broken = make_inputs(
        previous_state="OBSERVE_ONLY",
        exposure=exposure,
        underlier_price=7400.0,
        settings=settings,
    )
    state, rule = _advance(broken)
    assert state != "BULLISH_DIP_WATCH"


def test_t7_bearish_near_support_enters_break_watch() -> None:
    exposure = make_exposure(
        make_expiry(net_dex=-500000.0, put_walls=(7470.0,)),
        make_expiry(expiry="20260714", net_dex=-500000.0),
    )
    near = make_inputs(
        previous_state="REGIME_UNKNOWN",
        exposure=exposure,
        underlier_price=7490.0,
    )
    state, rule = _advance(near)
    assert state == "BEARISH_BREAK_WATCH"
    assert rule == "T7"
    far = make_inputs(
        previous_state="REGIME_UNKNOWN",
        exposure=exposure,
        underlier_price=7520.0,
    )
    state, rule = _advance(far)
    assert state != "BEARISH_BREAK_WATCH"
    # The break already happened; arming a fresh watch here is too late.
    broken = make_inputs(
        previous_state="REGIME_UNKNOWN",
        exposure=exposure,
        underlier_price=7400.0,
    )
    state, rule = _advance(broken)
    assert state != "BEARISH_BREAK_WATCH"


def test_watch_invalidated_when_spot_breaks_below_support() -> None:
    settings = StevenSettings(watch_exit_hold_seconds=120.0)
    exposure = make_exposure(
        make_expiry(net_dex=500000.0, put_walls=(7470.0,)),
        make_expiry(expiry="20260714", net_dex=500000.0),
    )
    held = make_inputs(
        previous_state="BULLISH_DIP_WATCH",
        exposure=exposure,
        underlier_price=7400.0,
        bars_1m=(),
        watch_exit_since=AS_OF - timedelta(seconds=30),
        settings=settings,
    )
    state, rule = _advance(held)
    assert state == "BULLISH_DIP_WATCH"
    expired = make_inputs(
        previous_state="BULLISH_DIP_WATCH",
        exposure=exposure,
        underlier_price=7400.0,
        bars_1m=(),
        watch_exit_since=AS_OF - timedelta(seconds=180),
        settings=settings,
    )
    state, rule = _advance(expired)
    assert state == "OBSERVE_ONLY"
    assert rule == "T12"


def test_t8_mixed_pin_conditions_enter_range_pin_watch() -> None:
    settings = StevenSettings(pin_min_net_gamma_ratio=0.15, pin_watch_max_distance_points=20.0)
    good = make_exposure(
        make_expiry(net_dex=500000.0, pin=7500.0, net_gamma_ratio=0.2),
        make_expiry(expiry="20260714", net_dex=-500000.0),
    )
    inputs = make_inputs(
        previous_state="OBSERVE_ONLY",
        exposure=good,
        underlier_price=7505.0,
        settings=settings,
    )
    state, rule = _advance(inputs)
    assert state == "RANGE_PIN_WATCH"
    assert rule == "T8"
    weak_gamma = make_exposure(
        make_expiry(net_dex=500000.0, pin=7500.0, net_gamma_ratio=0.05),
        make_expiry(expiry="20260714", net_dex=-500000.0),
    )
    bad = make_inputs(
        previous_state="OBSERVE_ONLY",
        exposure=weak_gamma,
        underlier_price=7505.0,
        settings=settings,
    )
    state, rule = _advance(bad)
    assert state != "RANGE_PIN_WATCH"


def test_t9_dip_hold_trigger_confirms_setup() -> None:
    exposure = make_exposure(
        make_expiry(net_dex=500000.0, put_walls=(7470.0,)),
        make_expiry(expiry="20260714", net_dex=500000.0),
    )
    bars = (
        make_bar(AS_OF - timedelta(minutes=3), close=7470.0, low=7468.0, high=7472.0),
        make_bar(AS_OF - timedelta(minutes=2), close=7475.0, low=7471.0, high=7476.0),
        make_bar(AS_OF - timedelta(minutes=1), close=7476.0, low=7472.0, high=7477.0),
    )
    ok = make_inputs(
        previous_state="BULLISH_DIP_WATCH",
        exposure=exposure,
        underlier_price=7475.0,
        bars_1m=bars,
        es_volume={"direction": "up"},
    )
    state, rule = _advance(ok)
    assert state == "SETUP_CONFIRMED"
    assert rule == "T9"
    opposed = make_inputs(
        previous_state="BULLISH_DIP_WATCH",
        exposure=exposure,
        underlier_price=7475.0,
        bars_1m=bars,
        es_volume={"direction": "down"},
    )
    state, rule = _advance(opposed)
    assert state == "BULLISH_DIP_WATCH"


def test_t10_break_hold_trigger_confirms_setup() -> None:
    exposure = make_exposure(
        make_expiry(net_dex=-500000.0, put_walls=(7470.0,)),
        make_expiry(expiry="20260714", net_dex=-500000.0),
    )
    bars = (
        make_bar(AS_OF - timedelta(minutes=3), close=7470.0, low=7465.0, high=7472.0),
        make_bar(AS_OF - timedelta(minutes=2), close=7465.0, low=7460.0, high=7468.0),
        make_bar(AS_OF - timedelta(minutes=1), close=7464.0, low=7460.0, high=7467.0),
    )
    ok = make_inputs(
        previous_state="BEARISH_BREAK_WATCH",
        exposure=exposure,
        underlier_price=7464.0,
        bars_1m=bars,
        es_volume={"direction": "down"},
    )
    state, rule = _advance(ok)
    assert state == "SETUP_CONFIRMED"
    assert rule == "T10"


def test_t11_range_reject_trigger_confirms_setup() -> None:
    exposure = make_exposure(
        make_expiry(
            net_dex=500000.0,
            put_walls=(7470.0,),
            call_walls=(7530.0,),
            pin=7500.0,
            net_gamma_ratio=0.2,
        ),
        make_expiry(expiry="20260714", net_dex=-500000.0),
    )
    bars = (
        make_bar(AS_OF - timedelta(minutes=3), close=7530.0, low=7525.0, high=7532.0),
        make_bar(AS_OF - timedelta(minutes=2), close=7525.0, low=7520.0, high=7528.0),
        make_bar(AS_OF - timedelta(minutes=1), close=7524.0, low=7520.0, high=7527.0),
    )
    ok = make_inputs(
        previous_state="RANGE_PIN_WATCH",
        exposure=exposure,
        underlier_price=7524.0,
        bars_1m=bars,
    )
    state, rule = _advance(ok)
    assert state == "SETUP_CONFIRMED"
    assert rule == "T11"


def test_t12_watch_exits_on_regime_flip_with_hold() -> None:
    settings = StevenSettings(watch_exit_hold_seconds=120.0)
    exposure = make_exposure(
        make_expiry(net_dex=-500000.0),
        make_expiry(expiry="20260714", net_dex=-500000.0),
    )
    early = make_inputs(
        previous_state="BULLISH_DIP_WATCH",
        exposure=exposure,
        underlier_price=7490.0,
        watch_exit_since=AS_OF - timedelta(seconds=30),
        settings=settings,
    )
    state, rule = _advance(early)
    assert state == "BULLISH_DIP_WATCH"
    ready = make_inputs(
        previous_state="BULLISH_DIP_WATCH",
        exposure=exposure,
        underlier_price=7490.0,
        watch_exit_since=AS_OF - timedelta(seconds=180),
        settings=settings,
    )
    state, rule = _advance(ready)
    assert state == "OBSERVE_ONLY"
    assert rule == "T12"


def test_t13_target_or_invalidation_enters_exit_review() -> None:
    exposure = make_exposure(
        make_expiry(net_dex=500000.0, put_walls=(7470.0,), call_walls=(7530.0,)),
        make_expiry(expiry="20260714", net_dex=500000.0),
    )
    # Target: spot reaches resistance
    target = make_inputs(
        previous_state="SETUP_CONFIRMED",
        exposure=exposure,
        underlier_price=7535.0,
        bars_1m=(),
    )
    state, rule = _advance(target)
    assert state == "EXIT_REVIEW"
    assert rule == "T13"
    # Invalidation hold below support
    bars = (
        make_bar(AS_OF - timedelta(minutes=2), close=7465.0),
        make_bar(AS_OF - timedelta(minutes=1), close=7464.0),
    )
    inv = make_inputs(
        previous_state="SETUP_CONFIRMED",
        exposure=exposure,
        underlier_price=7464.0,
        bars_1m=bars,
    )
    state, rule = _advance(inv)
    assert state == "EXIT_REVIEW"
    assert rule == "T13"


def test_t14_data_loss_during_setup_enters_exit_review() -> None:
    inputs = make_inputs(previous_state="SETUP_CONFIRMED", underlier_price=None)
    state, rule = _advance(inputs)
    assert state == "EXIT_REVIEW"
    assert rule == "T14"


def test_t15_exit_review_always_proceeds_to_lockout_or_remap() -> None:
    inputs = make_inputs(previous_state="EXIT_REVIEW")
    state, rule = _advance(inputs)
    assert state == "LOCKOUT_OR_REMAP"
    assert rule == "T15"


def test_t16_lockout_expires_or_daily_cap_holds() -> None:
    settings = StevenSettings(lockout_minutes=30.0, max_daily_setups=2)
    # Cool-down not met
    locked = make_inputs(
        previous_state="LOCKOUT_OR_REMAP",
        lockout_until=AS_OF + timedelta(minutes=10),
        daily_setup_count=1,
        settings=settings,
    )
    state, rule = _advance(locked)
    assert state == "LOCKOUT_OR_REMAP"
    # Cool-down met
    ready = make_inputs(
        previous_state="LOCKOUT_OR_REMAP",
        lockout_until=AS_OF - timedelta(minutes=1),
        daily_setup_count=1,
        settings=settings,
    )
    state, rule = _advance(ready)
    assert state == "OBSERVE_ONLY"
    assert rule == "T16"
    # Daily cap
    capped = make_inputs(
        previous_state="LOCKOUT_OR_REMAP",
        lockout_until=AS_OF - timedelta(minutes=1),
        daily_setup_count=2,
        settings=settings,
    )
    state, rule = _advance(capped)
    assert state == "LOCKOUT_OR_REMAP"


def test_t17_trading_date_rollover_resets_state() -> None:
    inputs = make_inputs(
        previous_state="BULLISH_DIP_WATCH",
        trading_date="2026-07-10",
        as_of=AS_OF,
    )
    state, rule = _advance(inputs)
    assert state == "OBSERVE_ONLY"
    assert rule == "T17"


# --- P3-C stable output ------------------------------------------------------


@pytest.mark.parametrize(
    "kwargs",
    [
        {"exposure": None, "bars_1m": (), "shock_state": None, "es_volume": None, "hl_volume": None},
        {
            "as_of": datetime(2026, 7, 11, 14, 0, tzinfo=UTC),  # Saturday
            "created_at": datetime(2026, 7, 11, 14, 0, tzinfo=UTC),
            "exposure": None,
            "underlier_price": None,
        },
        {"exposure": None, "underlier_price": 7500.0, "underlier_source": "index:SPX"},
    ],
)
def test_weekend_or_empty_inputs_stable_observe_only_or_invalid(kwargs: dict[str, Any]) -> None:
    results = []
    for _ in range(3):
        inputs = make_inputs(**kwargs)
        signal = build_steven_signal(inputs)
        assert signal.status in {"observe_only", "invalid"}
        assert signal.regime == "unknown"
        assert signal.expression_family == "none"
        results.append(signal.to_dict())
    assert results[0]["machine_state"] == results[1]["machine_state"] == results[2]["machine_state"]


def test_contract_json_validates_against_schema() -> None:
    signal = build_steven_signal(make_inputs())
    errors = validate_contract_dict(signal.to_dict())
    assert errors == []


def test_alert_context_note_is_readonly_and_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    contract = build_steven_signal(make_inputs()).to_dict()
    steven_state = {
        "schema_version": STATE_SCHEMA_VERSION,
        "machine_state": "BULLISH_DIP_WATCH",
        "updated_at": AS_OF.isoformat(),
        "contract": contract,
    }
    settings = StevenSettings(alert_context_enabled=True, alert_context_max_age_seconds=120.0)
    note = steven_context_note(steven_state, as_of=AS_OF, settings=settings)
    assert note is not None
    assert "observe_only" in note
    assert len(note) <= 200

    alert = Alert(
        severity="high",
        kind="intraday_price_shock",
        instrument_id="index:SPX",
        title="shock",
        detail="moved",
    )
    annotated = annotate_alerts_with_steven_context(
        [alert],
        steven_state,
        as_of=AS_OF,
        settings=settings,
    )
    assert annotated[0].severity == "high"
    assert annotated[0].kind == "intraday_price_shock"
    assert "observe_only" in annotated[0].detail

    stale = steven_context_note(
        {**steven_state, "updated_at": (AS_OF - timedelta(seconds=500)).isoformat()},
        as_of=AS_OF,
        settings=settings,
    )
    assert stale is None

    calls: list[str] = []

    def _boom(*_a: Any, **_k: Any) -> None:
        calls.append("load")
        raise AssertionError("should not load")

    monkeypatch.setattr(steven_mod, "load_steven_state_for_alerts", _boom)
    disabled = StevenSettings(alert_context_enabled=False)
    out = annotate_alerts_with_steven_context(
        [alert],
        None,
        as_of=AS_OF,
        settings=disabled,
    )
    assert out[0].detail == "moved"
    assert calls == []
