from __future__ import annotations

import json
import math
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from spx_spark.config import NotificationSettings
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    OptionGreeks,
    Provider,
    Quote,
)
from spx_spark.options_map import (
    ExpiryOptionsMap,
    OptionCoverage,
    OptionsMap,
    UnderlierReference,
)
from spx_spark.order_map import (
    already_sent,
    build_candidates,
    build_order_payload,
    chain_implied_spot,
    classify_price_direction,
    classify_spot_location,
    classify_volume_price_event,
    es_session_elapsed_minutes,
    es_volume_signal,
    update_break_watch,
    frontrun_level_for,
    mark_sent,
    material_changes,
    minutes_to_open,
    expiry_close_utc,
    option_tick,
    payload_fingerprint,
    project_option_price,
    project_option_price_bs,
    hl_volume_signal,
    render_status_template,
    render_template,
    round_to_tick,
    send_order_map,
    session_phase,
    touch_eta_minutes,
    within_refresh_window,
    within_send_window,
    within_status_window,
    _wall_rung_option_ref,
)
from spx_spark.options_map import pair_by_strike
from spx_spark.storage import LatestState


@pytest.fixture(autouse=True)
def _stub_feishu(monkeypatch):
    monkeypatch.setattr(
        "spx_spark.notifier.sinks.post_feishu",
        lambda url, payload, timeout: {"code": 0, "msg": "success"},
    )

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


def make_settings(
    state_path: str,
    *,
    missed_queue_path: str = "",
    agent_enabled: bool = False,
    bark_enabled: bool = False,
    feishu_enabled: bool = True,
) -> NotificationSettings:
    return NotificationSettings(
        enabled=True,
        min_severity="high",
        cooldown_seconds=300,
        state_path=state_path,
        openclaw_enabled=False,
        openclaw_command="openclaw",
        openclaw_channel="",
        openclaw_account="",
        openclaw_target="",
        openclaw_dry_run=True,
        openclaw_timeout_seconds=20.0,
        openclaw_agent_enabled=agent_enabled,
        openclaw_agent_deliver=False,
        openclaw_agent_name="main",
        openclaw_agent_model="gpt-5.3-codex-spark",
        openclaw_agent_session_key="spx-spark-alerts",
        openclaw_agent_thinking="high",
        openclaw_agent_timeout_seconds=180.0,
        codex_enabled=False,
        codex_deliver=True,
        codex_command="codex",
        codex_model="gpt-5.3-codex-spark",
        codex_reasoning_effort="high",
        codex_sandbox="read-only",
        codex_cwd="/tmp",
        codex_timeout_seconds=120.0,
        codex_output_max_chars=4000,
        codex_require_delivery_cue=True,
        bark_enabled=bark_enabled,
        bark_url="https://example.com/bark" if bark_enabled else "",
        bark_group="spx-spark",
        bark_level="",
        bark_timeout_seconds=10.0,
        feishu_enabled=feishu_enabled,
        feishu_webhook_url="https://open.feishu.cn/open-apis/bot/v2/hook/test" if feishu_enabled else "",
        feishu_secret="",
        feishu_timeout_seconds=10.0,
        missed_queue_path=missed_queue_path,
    )


def make_option(
    *,
    expiry: str,
    strike: float,
    right: str,
    mark: float,
    delta: float,
    gamma: float,
    now: datetime,
    quality: MarketDataQuality = MarketDataQuality.LIVE,
    open_interest: float = 1000.0,
) -> Quote:
    return Quote(
        instrument=InstrumentId.option(
            "SPX",
            expiry=expiry,
            strike=strike,
            right=right,
            trading_class="SPXW",
        ),
        provider=Provider.IBKR,
        provider_symbol=f"SPXW:{expiry}:{strike}:{right}",
        received_at=now,
        quality=quality,
        bid=mark - 0.1,
        ask=mark + 0.1,
        mark=mark,
        open_interest=open_interest,
        quote_time=now,
        greeks=OptionGreeks(
            implied_vol=0.2,
            delta=delta,
            gamma=gamma,
            theta=-1.0,
            vega=0.3,
            model="test",
        ),
    )


def make_state(*quotes: Quote, now: datetime) -> LatestState:
    return LatestState(
        created_at=now,
        as_of=now,
        quotes=tuple(quotes),
        best_quotes=tuple(quotes),
    )


def make_coverage(*, total: int = 4) -> OptionCoverage:
    return OptionCoverage(
        total=total,
        live=total,
        stale=0,
        delayed=0,
        unknown_age=0,
        max_age_ms=100.0,
        with_bid_ask=total,
        with_mid=total,
        with_iv=total,
        with_delta=total,
        with_gamma=total,
        with_theta=total,
        with_vega=total,
        with_open_interest=total,
        avg_spread_bps=50.0,
    )


def make_front_expiry(
    *,
    put_wall: float = 7500.0,
    call_wall: float = 7550.0,
    zero_gamma: float = 7533.0,
    flip_zone: tuple[float, float] = (7530.0, 7535.0),
) -> ExpiryOptionsMap:
    return ExpiryOptionsMap(
        expiry="20260707",
        option_count=8,
        strike_count=4,
        atm_strike=7530.0,
        atm_call_mid=10.0,
        atm_put_mid=11.0,
        atm_straddle_mid=21.0,
        expected_move_points=41.0,
        expected_move_pct=41.0 / 7569.0,
        atm_iv=0.2,
        put_wing_iv=0.22,
        call_wing_iv=0.21,
        put_skew_ratio=1.1,
        call_skew_ratio=1.05,
        net_gex=1000.0,
        abs_gex=5000.0,
        net_gamma_ratio=0.2,
        zero_gamma=zero_gamma,
        zero_gamma_distance_points=zero_gamma - 7569.0,
        call_wall=call_wall,
        put_wall=put_wall,
        nearest_wall=put_wall,
        nearest_wall_distance_points=put_wall - 7569.0,
        gamma_state="positive_gamma_pin",
        gex_quality="open_interest_gex",
        coverage=make_coverage(),
        top_gex_strikes=(),
        warnings=(),
        gamma_flip_zone=flip_zone,
    )


def make_options_map(front: ExpiryOptionsMap, *, price: float = 7569.0) -> OptionsMap:
    now = datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc)
    return OptionsMap(
        created_at=now,
        as_of=now,
        underlier=UnderlierReference(price=price, source="future:ES"),
        expiries=(front,),
        warnings=(),
    )


def test_option_tick_and_round_to_tick() -> None:
    assert option_tick(2.97) == 0.05
    assert round_to_tick(2.97) == pytest.approx(2.95)
    assert option_tick(3.2) == 0.10
    assert round_to_tick(3.2) == pytest.approx(3.2)
    assert round_to_tick(3.27) == pytest.approx(3.2)


def test_project_option_price_call_and_put() -> None:
    projected_call = project_option_price(
        mid=10.0,
        delta=0.35,
        gamma=0.008,
        spot=7500.0,
        target=7550.0,
    )
    expected_call = 10.0 + 0.35 * 50.0 + 0.5 * 0.008 * 50.0 * 50.0
    assert projected_call == pytest.approx(expected_call)
    assert projected_call > 0.05

    projected_put = project_option_price(
        mid=9.0,
        delta=-0.30,
        gamma=0.007,
        spot=7569.0,
        target=7500.0,
    )
    expected_put = 9.0 + (-0.30) * (-69.0) + 0.5 * 0.007 * 69.0 * 69.0
    assert projected_put == pytest.approx(expected_put)

    clamped = project_option_price(
        mid=4.2,
        delta=0.35,
        gamma=0.008,
        spot=7569.0,
        target=7500.0,
    )
    assert clamped == 0.05


def test_expiry_close_utc_is_4pm_et() -> None:
    close = expiry_close_utc("20260707")
    assert close is not None
    assert close == datetime(2026, 7, 7, 20, 0, tzinfo=timezone.utc)
    assert expiry_close_utc("garbage") is None


def test_bs_projection_accounts_for_time_decay_and_vol_shift() -> None:
    # Calibration case from the 2026-07-07 session: 14:00 BJT map, ITM 7500C,
    # spot 7523.5, put wall 7500 touched ~8h later. Taylor said 16.04, the
    # market printed 12.45 at the touch.
    tau_now = 14.0 / (365.0 * 24.0)
    bs_proj = project_option_price_bs(
        mid=30.75,
        iv=0.1346,
        strike=7500.0,
        right="C",
        spot=7523.5,
        target=7500.0,
        tau_now_years=tau_now,
        em_points=25.8,
        slope_per_point=-0.00068,
    )
    taylor_proj = project_option_price(30.75, 0.718, 0.006, 7523.5, 7500.0)
    assert bs_proj is not None
    # Time decay must pull the BS estimate well below the Taylor one, close
    # to the observed 12.45.
    assert bs_proj < taylor_proj
    assert 11.0 < bs_proj < 14.5

    # Vol shift: with the put-skew slope, a down-move projection prices the
    # option richer than a pure sticky-strike (no-slope) repricing.
    no_slope = project_option_price_bs(
        mid=30.75,
        iv=0.1346,
        strike=7500.0,
        right="C",
        spot=7523.5,
        target=7500.0,
        tau_now_years=tau_now,
        em_points=25.8,
        slope_per_point=None,
    )
    assert no_slope is not None
    assert bs_proj > no_slope

    # Missing IV -> caller must fall back to Taylor.
    assert (
        project_option_price_bs(
            mid=30.75,
            iv=None,
            strike=7500.0,
            right="C",
            spot=7523.5,
            target=7500.0,
            tau_now_years=tau_now,
            em_points=25.8,
            slope_per_point=None,
        )
        is None
    )


def test_build_candidates_produces_three_plays_with_limits() -> None:
    now = datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc)
    underlier = Quote(
        instrument=InstrumentId.future("ES"),
        provider=Provider.IBKR,
        provider_symbol="future:ES",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7569.0,
        quote_time=now,
    )
    # Option marks are put-call-parity consistent with spot ~7569 so the
    # chain-implied spot sits above the flip zone (7530) and all three plays
    # stay valid (breakdown level must be below spot).
    state = make_state(
        underlier,
        make_option(
            expiry="20260707",
            strike=7500,
            right="C",
            mark=73.2,
            delta=0.85,
            gamma=0.008,
            now=now,
        ),
        make_option(
            expiry="20260707",
            strike=7530,
            right="P",
            mark=9.1,
            delta=-0.28,
            gamma=0.007,
            now=now,
        ),
        make_option(
            expiry="20260707",
            strike=7550,
            right="P",
            mark=11.2,
            delta=-0.22,
            gamma=0.006,
            now=now,
        ),
        make_option(
            expiry="20260707",
            strike=7550,
            right="C",
            delta=0.45,
            mark=30.0,
            gamma=0.005,
            now=now,
        ),
        make_option(
            expiry="20260707",
            strike=7500,
            right="P",
            delta=-0.15,
            mark=4.2,
            gamma=0.006,
            now=now,
        ),
        now=now,
    )
    options_map = make_options_map(make_front_expiry())
    candidates = build_candidates(state, options_map)

    assert len(candidates) == 3
    plays = {candidate.play for candidate in candidates}
    assert plays == {
        "put_wall_bounce_call",
        "flip_breakdown_put",
        "call_wall_fade_put",
    }

    by_play = {candidate.play: candidate for candidate in candidates}
    assert by_play["put_wall_bounce_call"].strike == 7500
    assert by_play["put_wall_bounce_call"].right == "C"
    assert by_play["flip_breakdown_put"].strike == 7530
    assert by_play["flip_breakdown_put"].right == "P"
    assert by_play["call_wall_fade_put"].strike == 7550
    assert by_play["call_wall_fade_put"].right == "P"

    for candidate in candidates:
        assert candidate.limit_aggressive == round_to_tick(candidate.projected_mid)
        assert candidate.limit_conservative == round_to_tick(candidate.projected_mid * 0.85)
        assert math.isfinite(candidate.projected_mid)


def test_frontrun_level_shifts_toward_spot_with_caps() -> None:
    # Spot above the level: rung sits above the level (30% of a 24pt distance).
    assert frontrun_level_for(7524.0, 7500.0) == pytest.approx(7507.2)
    # Spot below the level: rung sits below the level.
    assert frontrun_level_for(7524.0, 7550.0) == pytest.approx(7542.2)
    # Distance under the minimum: no rung (level is already close enough).
    assert frontrun_level_for(7524.0, 7523.0) is None
    # Offset floor: 30% of 8pts = 2.4 >= 2.0 min.
    assert frontrun_level_for(7508.0, 7500.0) == pytest.approx(7502.4)
    # Offset cap at 8 points for far levels.
    assert frontrun_level_for(7600.0, 7500.0) == pytest.approx(7508.0)


def test_build_candidates_marks_stop_trigger_and_frontrun() -> None:
    now = datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc)
    underlier = Quote(
        instrument=InstrumentId.future("ES"),
        provider=Provider.IBKR,
        provider_symbol="future:ES",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7569.0,
        quote_time=now,
    )
    # Option marks are put-call-parity consistent with spot 7569 so the
    # chain-implied spot matches the ES reference.
    state = make_state(
        underlier,
        make_option(expiry="20260707", strike=7500, right="C", mark=73.2, delta=0.85, gamma=0.004, now=now),
        make_option(expiry="20260707", strike=7500, right="P", mark=4.2, delta=-0.15, gamma=0.004, now=now),
        make_option(expiry="20260707", strike=7530, right="P", mark=9.1, delta=-0.28, gamma=0.007, now=now),
        make_option(expiry="20260707", strike=7550, right="P", mark=11.0, delta=-0.22, gamma=0.006, now=now),
        make_option(expiry="20260707", strike=7550, right="C", mark=30.0, delta=0.45, gamma=0.006, now=now),
        now=now,
    )
    candidates = build_candidates(state, make_options_map(make_front_expiry()))
    by_play = {candidate.play: candidate for candidate in candidates}

    # Spot 7569 dropping to put wall 7500: the call gets cheaper -> resting limit.
    bounce = by_play["put_wall_bounce_call"]
    assert bounce.order_style == "resting_limit"
    # 30% of the 69pt distance exceeds the 8pt cap -> rung at wall + 8.
    assert bounce.frontrun_level == pytest.approx(7508.0)
    assert bounce.frontrun_projected_mid is not None
    assert bounce.frontrun_projected_mid > bounce.projected_mid

    # Spot 7569 breaking below flip 7530: the put gets dearer -> stop trigger.
    breakdown = by_play["flip_breakdown_put"]
    assert breakdown.order_style == "stop_trigger"


def test_build_candidates_skips_stale_breakdown_and_reanchors_walls() -> None:
    # 2026-07-07 regression: spot 7490 with flip zone 7570-7575 produced a
    # "7570 跌破买 put" while the breakdown had already happened 80 points ago.
    now = datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc)
    underlier = Quote(
        instrument=InstrumentId.future("ES"),
        provider=Provider.IBKR,
        provider_symbol="future:ES",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7490.0,
        quote_time=now,
    )
    # Parity-consistent with spot ~7490.
    state = make_state(
        underlier,
        make_option(expiry="20260707", strike=7480, right="C", mark=18.0, delta=0.55, gamma=0.008, now=now),
        make_option(expiry="20260707", strike=7480, right="P", mark=8.0, delta=-0.45, gamma=0.008, now=now),
        make_option(expiry="20260707", strike=7550, right="P", mark=62.0, delta=-0.85, gamma=0.004, now=now),
        make_option(expiry="20260707", strike=7550, right="C", mark=2.0, delta=0.10, gamma=0.004, now=now),
        now=now,
    )
    warnings: list[str] = []
    front = make_front_expiry(
        put_wall=7480.0,
        call_wall=7550.0,
        zero_gamma=7572.0,
        flip_zone=(7570.0, 7575.0),
    )
    candidates = build_candidates(state, make_options_map(front), warnings)
    plays = {candidate.play for candidate in candidates}

    assert "flip_breakdown_put" not in plays
    assert any("above_spot_breakdown_already_done" in warning for warning in warnings)
    assert "put_wall_bounce_call" in plays
    assert "call_wall_fade_put" in plays


def test_render_template_includes_wall_ladder_lines() -> None:
    payload = {
        "kind": "order_map",
        "trading_date": "2026-07-07",
        "beijing_time": "14:00",
        "expiry": "20260707",
        "underlier": {"price": 7524.0, "source": "chain_implied"},
        "expected_move_points": 24.7,
        "gamma_state": "zero_gamma_transition",
        "zero_gamma": 7516.1,
        "flip_zone": [7515.0, 7520.0],
        "candidates": [],
        "wall_ladder": {
            "put_walls": [
                {
                    "strike": 7500.0,
                    "open_interest": 3604,
                    "prob_touch": 0.47,
                    "option_right": "C",
                    "option_strike": 7500,
                    "current_mid": 31.05,
                    "projected_mid": 14.2,
                    "limit_aggressive": 14.2,
                    "limit_conservative": 12.0,
                },
                {
                    "strike": 7480.0,
                    "open_interest": 1500,
                    "prob_touch": 0.30,
                    "option_right": "C",
                    "option_strike": 7480,
                    "current_mid": 40.0,
                    "projected_mid": 9.5,
                    "limit_aggressive": 9.5,
                    "limit_conservative": 8.0,
                    "degraded": True,
                    "quote_quality": "stale",
                },
                {
                    "strike": 7450.0,
                    "open_interest": 2876,
                    "prob_touch": 0.18,
                    "option_right": "C",
                    "option_strike": 7450,
                    "current_mid": 55.0,
                    "projected_mid": 5.1,
                    "limit_aggressive": 5.1,
                    "limit_conservative": 4.3,
                },
            ],
            "call_walls": [
                {
                    "strike": 7550.0,
                    "open_interest": 6555,
                    "prob_touch": 0.51,
                    "option_right": "P",
                    "option_strike": 7550,
                    "current_mid": 28.0,
                    "projected_mid": 12.4,
                    "limit_aggressive": 12.4,
                    "limit_conservative": 10.5,
                },
                {
                    "strike": 7600.0,
                    "open_interest": 6533,
                    "prob_touch": 0.12,
                    "option_right": "P",
                    "option_strike": 7600,
                    "current_mid": 50.0,
                    "projected_mid": 6.2,
                    "limit_aggressive": 6.2,
                    "limit_conservative": 5.2,
                },
            ],
        },
        "warnings": [],
    }
    text = render_template(payload)
    assert "put 墙阶梯(下方支撑→买 call) (★=主墙):" in text
    assert "★7500 (OI 3604,触达47%) → 7500C 到位预估14.20(现31.05) 限价14.20/12.00" in text
    assert " 7480 (OI 1500,触达30%) → 7480C 到位预估9.50(现40.00) 限价9.50/8.00 [stale]" in text
    assert "call 墙阶梯(上方阻力→买 put) (★=主墙):" in text
    assert "★7550 (OI 6555,触达51%) → 7550P 到位预估12.40(现28.00) 限价12.40/10.50" in text


def test_wall_rung_option_ref_degrades_recent_stale_quote() -> None:
    now = datetime(2026, 7, 9, 15, 0, tzinfo=timezone.utc)
    live = make_option(
        expiry="20260709",
        strike=7500.0,
        right="C",
        mark=25.0,
        delta=0.55,
        gamma=0.01,
        now=now,
        quality=MarketDataQuality.LIVE,
    )
    # Cold-lane STALE but only 60s old — structure gate should still accept.
    stale = make_option(
        expiry="20260709",
        strike=7480.0,
        right="C",
        mark=40.0,
        delta=0.80,
        gamma=0.006,
        now=now,
        quality=MarketDataQuality.STALE,
    )
    stale = Quote(
        instrument=stale.instrument,
        provider=stale.provider,
        provider_symbol=stale.provider_symbol,
        received_at=now,
        quality=MarketDataQuality.STALE,
        bid=stale.bid,
        ask=stale.ask,
        mark=stale.mark,
        open_interest=stale.open_interest,
        quote_time=now - timedelta(seconds=60),
        greeks=stale.greeks,
    )
    quotes = [live, stale]
    pairs = pair_by_strike(quotes)
    live_ref = _wall_rung_option_ref(
        wall_strike=7500.0,
        right="C",
        spot=7520.0,
        expiry_quotes=quotes,
        pairs=pairs,
        strike_step=5.0,
        tau_now_years=4.0 / (365.0 * 24.0),
        em_points=20.0,
        as_of=now,
    )
    stale_ref = _wall_rung_option_ref(
        wall_strike=7480.0,
        right="C",
        spot=7520.0,
        expiry_quotes=quotes,
        pairs=pairs,
        strike_step=5.0,
        tau_now_years=4.0 / (365.0 * 24.0),
        em_points=20.0,
        as_of=now,
    )
    assert live_ref["projected_mid"] is not None
    assert live_ref["degraded"] is False
    assert stale_ref["projected_mid"] is not None
    assert stale_ref["degraded"] is True
    assert stale_ref["quote_quality"] == "stale"
    assert str(stale_ref["projection_model"]).endswith("_stale")

    # Too old (>15 min structure window) still rejected.
    ancient = Quote(
        instrument=stale.instrument,
        provider=stale.provider,
        provider_symbol=stale.provider_symbol,
        received_at=now,
        quality=MarketDataQuality.STALE,
        bid=stale.bid,
        ask=stale.ask,
        mark=stale.mark,
        open_interest=stale.open_interest,
        quote_time=now - timedelta(seconds=1200),
        greeks=stale.greeks,
    )
    ancient_ref = _wall_rung_option_ref(
        wall_strike=7480.0,
        right="C",
        spot=7520.0,
        expiry_quotes=[live, ancient],
        pairs=pair_by_strike([live, ancient]),
        strike_step=5.0,
        tau_now_years=4.0 / (365.0 * 24.0),
        em_points=20.0,
        as_of=now,
    )
    assert ancient_ref["projected_mid"] is None


def test_render_template_shows_frontrun_and_stop_trigger_notes() -> None:
    payload = {
        "kind": "order_map",
        "trading_date": "2026-07-07",
        "beijing_time": "14:00",
        "expiry": "20260707",
        "underlier": {"price": 7524.0, "source": "chain_implied"},
        "expected_move_points": 24.7,
        "gamma_state": "zero_gamma_transition",
        "zero_gamma": 7516.1,
        "flip_zone": [7515.0, 7520.0],
        "candidates": [
            {
                "play": "put_wall_bounce_call",
                "level": 7500.0,
                "level_label": "put wall 7500",
                "contract_id": "option:SPX:SPXW:20260707:7500:C",
                "strike": 7500,
                "right": "C",
                "current_mid": 31.05,
                "projected_mid": 15.73,
                "limit_aggressive": 15.7,
                "limit_conservative": 13.3,
                "prob_touch": 0.54,
                "prob_close_beyond": 0.27,
                "delta": 0.6,
                "gamma": 0.01,
                "frontrun_level": 7507.2,
                "frontrun_projected_mid": 19.85,
                "frontrun_limit": 19.8,
                "frontrun_prob_touch": 0.62,
                "order_style": "resting_limit",
            },
            {
                "play": "flip_breakdown_put",
                "level": 7515.0,
                "level_label": "flip zone 7515",
                "contract_id": "option:SPX:SPXW:20260707:7515:P",
                "strike": 7515,
                "right": "P",
                "current_mid": 8.4,
                "projected_mid": 15.92,
                "limit_aggressive": 15.9,
                "limit_conservative": 13.5,
                "prob_touch": 0.65,
                "prob_close_beyond": 0.32,
                "delta": -0.4,
                "gamma": 0.01,
                "frontrun_level": None,
                "frontrun_projected_mid": None,
                "frontrun_limit": None,
                "frontrun_prob_touch": None,
                "order_style": "stop_trigger",
            },
        ],
        "warnings": [],
    }
    text = render_template(payload)
    assert "先手挡 7507.2: 限价 19.80" in text
    assert "触达≈62%" in text
    assert "被动限价会立即成交" in text
    assert "挂单参考: 激进 15.90" not in text


def test_resolve_spx_spot_prefers_perp_when_cash_closed_and_diverged() -> None:
    from spx_spark.order_map import resolve_spx_spot

    from spx_spark.marketdata import InstrumentType

    now = datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc)  # 2:00 ET, cash closed
    hl_quote = Quote(
        instrument=InstrumentId(symbol="xyz:SP500", instrument_type=InstrumentType.CRYPTO_PERP),
        provider=Provider.HYPERLIQUID,
        provider_symbol="xyz:SP500",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7520.0,
        quote_time=now,
    )
    # Parity pair implies 7533 (C-P=8 at 7525): 17bps above the perp.
    state = make_state(
        hl_quote,
        make_option(expiry="20260707", strike=7525, right="C", mark=18.0, delta=0.55, gamma=0.008, now=now),
        make_option(expiry="20260707", strike=7525, right="P", mark=10.0, delta=-0.45, gamma=0.008, now=now),
        now=now,
    )
    options_map = make_options_map(make_front_expiry())

    warnings: list[str] = []
    spot, source = resolve_spx_spot(state, options_map, warnings=warnings, now=now)
    assert source == "hl_perp"
    assert spot == pytest.approx(7520.0)
    assert any("参考价采用 perp" in item for item in warnings)

    # Same divergence during cash hours: the chain's own view wins.
    rth = datetime(2026, 7, 7, 15, 0, tzinfo=timezone.utc)  # 11:00 ET
    warnings_rth: list[str] = []
    spot_rth, source_rth = resolve_spx_spot(state, options_map, warnings=warnings_rth, now=rth)
    assert source_rth == "chain_implied"
    assert spot_rth == pytest.approx(7533.0)
    assert any("diverges" in item for item in warnings_rth)


def test_push_context_roundtrip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from spx_spark.notifier.llm_writer import load_previous_push, record_push

    context_path = str(tmp_path / "push_context.json")
    monkeypatch.setenv("SPX_PUSH_CONTEXT_PATH", context_path)

    assert load_previous_push(context_path) is None
    record_push("order_map", "挂单参考: 测试" * 400, at="2026-07-07T06:00:00+00:00")
    loaded = load_previous_push(context_path)
    assert loaded is not None
    assert loaded["kind"] == "order_map"
    assert loaded["at"] == "2026-07-07T06:00:00+00:00"
    assert len(loaded["text"]) <= 1600

    record_push("market_status", "剧本维持", at="2026-07-07T06:30:00+00:00")
    assert load_previous_push(context_path)["kind"] == "market_status"


def test_prompts_include_previous_push() -> None:
    from spx_spark.order_map import build_order_prompt, build_status_prompt

    payload = {"expiry": "20260707", "candidates": [], "warnings": []}
    previous = {"kind": "order_map", "at": "2026-07-07T06:00:00+00:00", "text": "上一条正文"}
    order_prompt = build_order_prompt(payload, "模板行", previous)
    assert "上一条正文" in order_prompt
    assert "previous_push:" in order_prompt
    status_prompt = build_status_prompt(payload, "模板行", None)
    assert "previous_push:null" in status_prompt
    # Merged push: the status prompt must demand the verbatim limit-price
    # section that used to live in the separate refresh push.
    assert "挂单参考" in status_prompt
    assert "市场状态+挂单参考" in status_prompt


def test_chain_implied_spot_uses_put_call_parity() -> None:
    from spx_spark.marketdata import OptionRight
    from spx_spark.options_map import pair_by_strike

    now = datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc)
    # True spot 7517: C(7515)=12.0, P(7515)=10.0 -> implied 7517;
    # the wing pair has a larger |C-P| so parity should pick 7515.
    quotes = [
        make_option(expiry="20260707", strike=7515, right="C", mark=12.0, delta=0.52, gamma=0.008, now=now),
        make_option(expiry="20260707", strike=7515, right="P", mark=10.0, delta=-0.48, gamma=0.008, now=now),
        make_option(expiry="20260707", strike=7550, right="C", mark=2.0, delta=0.15, gamma=0.004, now=now),
        make_option(expiry="20260707", strike=7550, right="P", mark=34.0, delta=-0.85, gamma=0.004, now=now),
    ]
    pairs = pair_by_strike(quotes)
    assert set(pairs[7515.0]) == {OptionRight.CALL, OptionRight.PUT}
    implied = chain_implied_spot(pairs)
    assert implied == pytest.approx(7517.0)


def test_build_candidates_skips_missing_greeks_with_warning() -> None:
    now = datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc)
    quote_no_greeks = Quote(
        instrument=InstrumentId.option(
            "SPX",
            expiry="20260707",
            strike=7500,
            right="C",
            trading_class="SPXW",
        ),
        provider=Provider.IBKR,
        provider_symbol="SPXW:20260707:7500:C",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        bid=4.0,
        ask=4.4,
        mark=4.2,
        quote_time=now,
        greeks=None,
    )
    state = make_state(quote_no_greeks, now=now)
    warnings: list[str] = []
    candidates = build_candidates(state, make_options_map(make_front_expiry()), warnings)

    assert candidates == []
    assert any("missing_greeks_for_7500C" in item for item in warnings)


def test_render_template_contains_play_lines_and_limits() -> None:
    payload = {
        "kind": "order_map",
        "trading_date": "2026-07-07",
        "beijing_time": "14:00",
        "expiry": "20260707",
        "underlier": {"price": 7569.2, "source": "future:ES"},
        "expected_move_points": 41.0,
        "gamma_state": "positive_gamma_pin",
        "zero_gamma": 7533.3,
        "flip_zone": [7530.0, 7535.0],
        "candidates": [
            {
                "play": "put_wall_bounce_call",
                "level": 7500.0,
                "level_label": "put wall 7500",
                "contract_id": "option:SPX:SPXW:20260707:7500:C",
                "strike": 7500,
                "right": "C",
                "current_mid": 4.2,
                "projected_mid": 12.3,
                "limit_aggressive": 12.3,
                "limit_conservative": 10.4,
                "prob_touch": 0.24,
                "prob_close_beyond": 0.12,
                "delta": 0.35,
                "gamma": 0.008,
            },
            {
                "play": "flip_breakdown_put",
                "level": 7530.0,
                "level_label": "flip zone 7530",
                "contract_id": "option:SPX:SPXW:20260707:7530:P",
                "strike": 7530,
                "right": "P",
                "current_mid": 9.1,
                "projected_mid": 15.8,
                "limit_aggressive": 15.8,
                "limit_conservative": 13.4,
                "prob_touch": 0.41,
                "prob_close_beyond": 0.20,
                "delta": -0.28,
                "gamma": 0.007,
            },
            {
                "play": "call_wall_fade_put",
                "level": 7550.0,
                "level_label": "call wall 7550",
                "contract_id": "option:SPX:SPXW:20260707:7550:P",
                "strike": 7550,
                "right": "P",
                "current_mid": 11.2,
                "projected_mid": 6.4,
                "limit_aggressive": 6.4,
                "limit_conservative": 5.4,
                "prob_touch": 0.35,
                "prob_close_beyond": 0.17,
                "delta": -0.22,
                "gamma": 0.006,
            },
        ],
        "warnings": [],
    }
    text = render_template(payload)
    assert "【挂单地图 2026-07-07】" in text
    assert "put wall 7500 反弹买 call" in text
    assert "触达概率≈24%" in text
    assert "挂单参考: 激进 12.30 / 保守 10.40" in text
    assert "flip zone 7530 跌破买 put" in text
    assert "call wall 7550 冲墙买 put" in text


def test_within_send_window_beijing_weekday() -> None:
    beijing_1400 = datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc).astimezone(SHANGHAI_TZ)
    assert within_send_window(beijing_1400.astimezone(timezone.utc)) is True

    beijing_1200 = datetime(2026, 7, 7, 4, 0, tzinfo=timezone.utc)
    assert within_send_window(beijing_1200) is False

    saturday_1400 = datetime(2026, 7, 11, 6, 0, tzinfo=timezone.utc)
    assert within_send_window(saturday_1400) is False


def test_mark_sent_merges_kinds_without_clobbering(tmp_path: Path) -> None:
    state_path = str(tmp_path / "state.json")
    map_time = datetime(2026, 7, 8, 6, 0, tzinfo=timezone.utc)
    status_time = datetime(2026, 7, 8, 6, 30, tzinfo=timezone.utc)
    mark_sent(state_path, "2026-07-08", now=map_time, kind="map")
    mark_sent(state_path, "2026-07-08", now=status_time, kind="status")
    state = json.loads(Path(state_path).read_text(encoding="utf-8"))
    # Status push must not reset the map cadence timestamp.
    assert state["last_map_at"] == map_time.timestamp()
    assert state["last_status_at"] == status_time.timestamp()
    assert state["last_sent_at"] == status_time.timestamp()
    assert already_sent(state_path, "2026-07-08") is True


def test_status_push_does_not_mask_missing_baseline(tmp_path: Path) -> None:
    state_path = str(tmp_path / "state.json")
    # Only a status report went out today; the baseline map push failed.
    mark_sent(
        state_path,
        "2026-07-08",
        now=datetime(2026, 7, 8, 6, 30, tzinfo=timezone.utc),
        kind="status",
    )
    assert already_sent(state_path, "2026-07-08") is False


def test_already_sent_roundtrip(tmp_path: Path) -> None:
    state_path = str(tmp_path / "order_map_state.json")
    assert already_sent(state_path, "2026-07-07") is False
    mark_sent(state_path, "2026-07-07", kind="map")
    assert already_sent(state_path, "2026-07-07") is True
    assert already_sent(state_path, "2026-07-08") is False


def test_payload_fingerprint_and_material_changes() -> None:
    payload = {
        "expiry": "20260707",
        "expected_move_points": 44.0,
        "flip_zone": [7500.0, 7505.0],
        "candidates": [
            {"play": "put_wall_bounce_call", "level": 7500.0},
            {"play": "call_wall_fade_put", "level": 7550.0},
        ],
    }
    fingerprint = payload_fingerprint(payload)
    assert fingerprint["put_wall"] == 7500.0
    assert fingerprint["call_wall"] == 7550.0
    assert fingerprint["flip_low"] == 7500.0

    # Identical fingerprint: no material change.
    assert material_changes(fingerprint, dict(fingerprint)) == []
    # No baseline: nothing to compare.
    assert material_changes(None, fingerprint) == []

    moved = dict(fingerprint, put_wall=7510.0)
    changes = material_changes(fingerprint, moved)
    assert changes and "put wall" in changes[0]

    small_move = dict(fingerprint, call_wall=7552.0)
    assert material_changes(fingerprint, small_move) == []

    em_jump = dict(fingerprint, expected_move_points=60.0)
    changes = material_changes(fingerprint, em_jump)
    assert changes and "预期波幅" in changes[0]

    rolled = dict(fingerprint, expiry="20260708")
    changes = material_changes(fingerprint, rolled)
    assert changes == ["到期日切换 20260707→20260708"]


def test_within_refresh_window_beijing() -> None:
    # Refresh follows the status window: Beijing 07:30 -> next-day 01:30.
    # 14:45 Beijing: fixed cadence continues pre-open.
    beijing_1445 = datetime(2026, 7, 7, 6, 45, tzinfo=timezone.utc)
    assert within_refresh_window(beijing_1445) is True
    # 22:30 Beijing = 10:30 ET (summer): US session, inside window.
    beijing_2230 = datetime(2026, 7, 7, 14, 30, tzinfo=timezone.utc)
    assert within_refresh_window(beijing_2230) is True
    # 23:45 Beijing: late US session now covered too.
    beijing_2345 = datetime(2026, 7, 7, 15, 45, tzinfo=timezone.utc)
    assert within_refresh_window(beijing_2345) is True
    # 01:30 Beijing Wednesday: last inclusive fire of Tuesday's session.
    beijing_0130 = datetime(2026, 7, 7, 17, 30, tzinfo=timezone.utc)
    assert within_refresh_window(beijing_0130) is True
    # 01:45 Beijing: past the 01:30 cutoff.
    beijing_0145 = datetime(2026, 7, 7, 17, 45, tzinfo=timezone.utc)
    assert within_refresh_window(beijing_0145) is False
    # 09:00 Beijing: the reader's working morning is now inside the window.
    beijing_0900 = datetime(2026, 7, 7, 1, 0, tzinfo=timezone.utc)
    assert within_refresh_window(beijing_0900) is True
    # 07:30 Beijing: start of the reader's day.
    beijing_0730 = datetime(2026, 7, 6, 23, 30, tzinfo=timezone.utc)
    assert within_refresh_window(beijing_0730) is True
    # 07:00 Beijing: before the 07:30 start of the reader's day.
    beijing_0700 = datetime(2026, 7, 6, 23, 0, tzinfo=timezone.utc)
    assert within_refresh_window(beijing_0700) is False
    # 02:30 Beijing: past the cutoff.
    beijing_0230 = datetime(2026, 7, 7, 18, 30, tzinfo=timezone.utc)
    assert within_refresh_window(beijing_0230) is False


def test_session_phase_tracks_partner_clock() -> None:
    # Beijing 09:00 Thursday = ET 21:00 Wednesday: Asia/Globex overnight.
    asia = session_phase(datetime(2026, 7, 9, 1, 0, tzinfo=timezone.utc))
    assert asia["name"] == "asia_globex"
    assert asia["user_awake"] is True
    assert asia["minutes_to_us_open"] == 750
    assert asia["minutes_since_us_open"] is None
    # Beijing 14:30 = ET 02:30: Europe session, research window.
    europe = session_phase(datetime(2026, 7, 9, 6, 30, tzinfo=timezone.utc))
    assert europe["name"] == "europe_session"
    # Beijing 21:45 = ET 09:45: first hour after the open.
    open_hour = session_phase(datetime(2026, 7, 9, 13, 45, tzinfo=timezone.utc))
    assert open_hour["name"] == "us_open_hour"
    assert open_hour["minutes_since_us_open"] == 15
    # Beijing 00:45 = ET 12:45: main battle, 15 minutes to bedtime.
    late = session_phase(datetime(2026, 7, 9, 16, 45, tzinfo=timezone.utc))
    assert late["name"] == "us_morning_battle"
    assert late["minutes_to_bedtime"] == 15
    # Beijing 02:30 = ET 14:30: user asleep, unattended afternoon.
    asleep = session_phase(datetime(2026, 7, 9, 18, 30, tzinfo=timezone.utc))
    assert asleep["name"] == "us_afternoon_unattended"
    assert asleep["user_awake"] is False
    assert asleep["minutes_to_bedtime"] is None


def test_touch_eta_minutes_brownian_scaling() -> None:
    # 6 hours to expiry, EM 26 points, level 13 points away:
    # fraction = 0.6 * (13/26)^2 = 0.15 -> 0.15 * 360min = 54min.
    tau = 6.0 / (365.0 * 24.0)
    eta = touch_eta_minutes(13.0, 26.0, tau)
    assert eta is not None
    assert eta == pytest.approx(54.0, abs=1.0)
    # A level right at spot floors at 5% of remaining time.
    near = touch_eta_minutes(0.5, 26.0, tau)
    assert near == pytest.approx(18.0, abs=1.0)
    # Missing EM or tau -> None.
    assert touch_eta_minutes(13.0, None, tau) is None
    assert touch_eta_minutes(13.0, 26.0, None) is None


def test_hl_volume_signal_pace_and_rolling_caveat() -> None:
    now = datetime(2026, 7, 9, 6, 0, tzinfo=timezone.utc)
    # Three prior samples 15 minutes apart, ~200k notional per window
    # (13.3k/min baseline); the current window prints 600k in 15 minutes.
    samples = [
        {"at": (now - timedelta(minutes=45)).isoformat(), "volume": 100_000_000.0},
        {"at": (now - timedelta(minutes=30)).isoformat(), "volume": 100_200_000.0},
        {"at": (now - timedelta(minutes=15)).isoformat(), "volume": 100_400_000.0},
    ]
    signal = hl_volume_signal(101_000_000.0, samples, now=now)
    assert signal is not None
    assert signal["label"] == "elevated"
    assert signal["basis"] == "rolling_24h_notional"
    assert signal["pace_ratio"] == pytest.approx(3.0, abs=0.1)
    # Rolling 24h volume can decline; clamp reads as quiet, not negative.
    quiet = hl_volume_signal(100_300_000.0, samples, now=now)
    assert quiet is not None
    assert quiet["delta_notional"] == 0
    assert quiet["label"] == "quiet"
    # Fewer than two history windows -> no baseline.
    thin = hl_volume_signal(100_500_000.0, samples[-1:], now=now)
    assert thin is not None
    assert thin["label"] == "no_baseline"
    assert hl_volume_signal(None, samples, now=now) is None


def test_status_template_carries_session_phase() -> None:
    payload = {
        "expiry": "20260709",
        "underlier": {"price": 7523.5, "source": "chain_implied"},
        "gamma_state": "zero_gamma_transition",
        "zero_gamma": 7507.4,
        "flip_zone": [7505.0, 7510.0],
        "expected_move_points": 25.8,
        "vol_context": {},
        "candidates": [],
        "warnings": [],
    }
    # Beijing 09:00 = overnight Globex phase; header must not claim "已开盘".
    now = datetime(2026, 7, 9, 1, 0, tzinfo=timezone.utc)
    text = render_status_template(payload, [], now)
    assert "亚盘夜盘" in text
    assert "距开盘 750 分钟" in text
    assert "已开盘" not in text
    # Beijing 00:30 = US morning battle with the bedtime countdown showing.
    late = datetime(2026, 7, 9, 16, 30, tzinfo=timezone.utc)
    text_late = render_status_template(payload, [], late)
    assert "美盘上午主战场" in text_late
    assert "距收官 30 分钟" in text_late


def test_within_status_window_and_minutes_to_open() -> None:
    # 14:30 Beijing = 2:30 ET: inside status window, 420 minutes to open.
    beijing_1430 = datetime(2026, 7, 7, 6, 30, tzinfo=timezone.utc)
    assert within_status_window(beijing_1430) is True
    assert minutes_to_open(beijing_1430) == 420
    # 14:00 Beijing: now inside the window (day starts 07:30).
    beijing_1400 = datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc)
    assert within_status_window(beijing_1400) is True
    # 07:30 Beijing: start of the reader's day.
    beijing_0730 = datetime(2026, 7, 6, 23, 30, tzinfo=timezone.utc)
    assert within_status_window(beijing_0730) is True
    # 07:00 Beijing: before the reader's day starts.
    beijing_0700 = datetime(2026, 7, 6, 23, 0, tzinfo=timezone.utc)
    assert within_status_window(beijing_0700) is False
    # 21:30 Beijing = 9:30 ET: market open, status keeps running.
    beijing_2130 = datetime(2026, 7, 7, 13, 30, tzinfo=timezone.utc)
    assert within_status_window(beijing_2130) is True
    assert minutes_to_open(beijing_2130) is None
    # 01:30 Beijing Wednesday = 13:30 ET Tuesday: still Tuesday's session (inclusive).
    beijing_0130 = datetime(2026, 7, 7, 17, 30, tzinfo=timezone.utc)
    assert within_status_window(beijing_0130) is True
    # 01:45 Beijing: past the 01:30 cutoff.
    beijing_0145 = datetime(2026, 7, 7, 17, 45, tzinfo=timezone.utc)
    assert within_status_window(beijing_0145) is False
    # 02:30 Beijing: well past cutoff.
    beijing_0230 = datetime(2026, 7, 7, 18, 30, tzinfo=timezone.utc)
    assert within_status_window(beijing_0230) is False
    # Saturday 01:30 Beijing = Friday 13:30 ET: Friday's session, allowed.
    saturday_0130 = datetime(2026, 7, 10, 17, 30, tzinfo=timezone.utc)
    assert within_status_window(saturday_0130) is True
    # Monday 01:30 Beijing = Sunday ET: no session.
    monday_0130 = datetime(2026, 7, 5, 17, 30, tzinfo=timezone.utc)
    assert within_status_window(monday_0130) is False
    saturday = datetime(2026, 7, 11, 6, 30, tzinfo=timezone.utc)
    assert within_status_window(saturday) is False


def test_render_status_template_contains_levels_and_changes() -> None:
    payload = {
        "expiry": "20260707",
        "underlier": {"price": 7523.5, "source": "chain_implied"},
        "es_last": 7530.2,
        "hl_sp500_perp": 7522.8,
        "gamma_state": "zero_gamma_transition",
        "zero_gamma": 7507.4,
        "flip_zone": [7505.0, 7510.0],
        "expected_move_points": 25.8,
        "vol_context": {"vix": 15.9, "vix1d": 7.1, "vvix": 95.0, "skew": 150.0},
        "candidates": [
            {
                "play": "put_wall_bounce_call",
                "level": 7500.0,
                "level_label": "put wall 7500",
                "prob_touch": 0.57,
            },
        ],
        "warnings": [],
    }
    now = datetime(2026, 7, 7, 6, 30, tzinfo=timezone.utc)
    text = render_status_template(payload, ["put wall 7495→7500"], now)
    assert "【市场状态 14:30】" in text
    assert "距开盘 420 分钟" in text
    assert "put wall 7500 触达≈57%" in text
    assert "VIX 15.9" in text
    assert "较上次推送变化: put wall 7495→7500" in text

    text_no_change = render_status_template(payload, [], now)
    assert "关键位无实质变化" in text_no_change


def test_send_order_map_queues_on_feishu_failure(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("SPX_PUSH_LLM_ENABLED", "false")
    monkeypatch.setattr(
        "spx_spark.notifier.sinks.post_feishu",
        lambda url, payload, timeout: {"code": 19001, "msg": "fail"},
    )
    payload = build_order_payload(
        make_state(
            Quote(
                instrument=InstrumentId.future("ES"),
                provider=Provider.IBKR,
                provider_symbol="future:ES",
                received_at=datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc),
                quality=MarketDataQuality.LIVE,
                mark=7569.0,
                quote_time=datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc),
            ),
            now=datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc),
        )
    )
    template = render_template(payload)
    missed_path = str(tmp_path / "missed.jsonl")
    settings = make_settings(str(tmp_path / "notify-state.json"), missed_queue_path=missed_path)

    result = send_order_map(payload, settings)
    assert result["im_ok"] is False
    assert result["text"] == template
    lines = Path(missed_path).read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
    entry = json.loads(lines[0])
    assert entry["kind"] == "order_map"
    assert entry["message"] == template


def test_build_order_payload_shape() -> None:
    now = datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc)
    state = make_state(
        Quote(
            instrument=InstrumentId.future("ES"),
            provider=Provider.IBKR,
            provider_symbol="future:ES",
            received_at=now,
            quality=MarketDataQuality.LIVE,
            mark=7569.0,
            quote_time=now,
        ),
        now=now,
    )
    payload = build_order_payload(state, now=now)
    assert payload["kind"] == "order_map"
    assert "underlier" in payload
    assert "candidates" in payload
    assert "warnings" in payload


def _volume_sample(at: datetime, volume: float) -> dict[str, object]:
    return {"at": at.isoformat(), "volume": volume}


def test_es_volume_signal_needs_history_for_a_label() -> None:
    now = datetime(2026, 7, 8, 15, 0, tzinfo=timezone.utc)
    signal = es_volume_signal(1_000_000.0, [], now=now)
    assert signal is not None
    assert signal["label"] == "no_baseline"
    assert signal["delta"] is None
    assert es_volume_signal(None, [], now=now) is None


def test_es_volume_signal_session_average_fallback_flags_elevated() -> None:
    # 15:00 UTC = 11:00 ET -> 17h since the 18:00 ET open, avg ~980/min.
    now = datetime(2026, 7, 8, 15, 0, tzinfo=timezone.utc)
    samples = [_volume_sample(now - timedelta(minutes=30), 940_000.0)]
    signal = es_volume_signal(1_000_000.0, samples, now=now)
    assert signal is not None
    assert signal["baseline"] == "session_average"
    assert signal["delta"] == 60_000
    assert signal["pace_ratio"] >= 1.5
    assert signal["label"] == "elevated"


def test_es_volume_signal_median_window_baseline_flags_quiet() -> None:
    now = datetime(2026, 7, 8, 15, 0, tzinfo=timezone.utc)
    # Three prior windows at ~2000/min; the latest window collapses to 200/min.
    samples = [
        _volume_sample(now - timedelta(minutes=120), 700_000.0),
        _volume_sample(now - timedelta(minutes=90), 760_000.0),
        _volume_sample(now - timedelta(minutes=60), 820_000.0),
        _volume_sample(now - timedelta(minutes=30), 880_000.0),
    ]
    signal = es_volume_signal(886_000.0, samples, now=now)
    assert signal is not None
    assert signal["baseline"] == "recent_windows"
    assert signal["pace_ratio"] <= 0.5
    assert signal["label"] == "quiet"


def test_es_volume_signal_detects_session_reset_and_bad_windows() -> None:
    now = datetime(2026, 7, 8, 23, 30, tzinfo=timezone.utc)
    reset = es_volume_signal(
        5_000.0, [_volume_sample(now - timedelta(minutes=30), 900_000.0)], now=now
    )
    assert reset is not None
    assert reset["label"] == "session_reset"
    too_short = es_volume_signal(
        1_000_000.0, [_volume_sample(now - timedelta(seconds=60), 999_000.0)], now=now
    )
    assert too_short is not None
    assert too_short["label"] == "no_baseline"


def test_es_session_elapsed_minutes_wraps_at_globex_open() -> None:
    # 22:05 UTC = 18:05 ET -> 5 minutes into the new session.
    just_after_open = datetime(2026, 7, 8, 22, 5, tzinfo=timezone.utc)
    elapsed = es_session_elapsed_minutes(just_after_open)
    assert elapsed is not None and 4.0 <= elapsed <= 6.0
    # 21:00 UTC = 17:00 ET -> 23 hours into the prior session.
    before_open = datetime(2026, 7, 8, 21, 0, tzinfo=timezone.utc)
    elapsed_prior = es_session_elapsed_minutes(before_open)
    assert elapsed_prior is not None and elapsed_prior > 22 * 60


def test_templates_render_es_volume_line() -> None:
    payload = {
        "kind": "order_map",
        "trading_date": "2026-07-08",
        "beijing_time": "22:00",
        "expiry": "20260708",
        "underlier": {"price": 7470.0, "source": "chain_implied"},
        "expected_move_points": 20.0,
        "gamma_state": "zero_gamma_transition",
        "zero_gamma": 7455.0,
        "flip_zone": [7455.0, 7460.0],
        "candidates": [],
        "warnings": [],
        "es_volume": {
            "cumulative": 1_000_000,
            "delta": 60_000,
            "window_minutes": 30.0,
            "recent_pace_per_min": 2000.0,
            "baseline_pace_per_min": 1000.0,
            "baseline": "recent_windows",
            "pace_ratio": 2.0,
            "label": "elevated",
            "direction": "down",
            "price_delta": -12.0,
            "location": "at_put_wall",
            "event_id": "elevated_sell_into_support",
            "break_outcome": None,
        },
    }
    map_text = render_template(payload)
    assert "ES 量价: 最近30分钟 60,000 手, 节奏为近几窗的 2.0 倍(放量)" in map_text
    assert "价-12.0(下跌)" in map_text
    assert "贴put墙" in map_text
    assert "放量砸支撑" in map_text
    status_text = render_status_template(payload, [], datetime(2026, 7, 8, 14, 0, tzinfo=timezone.utc))
    assert "ES 量价" in status_text
    # Signals without a computable window stay silent instead of rendering "-".
    payload["es_volume"] = {"cumulative": 1_000_000, "label": "no_baseline", "delta": None,
                            "window_minutes": None, "pace_ratio": None}
    assert "ES 量价" not in render_template(payload)


def test_classify_spot_location_and_volume_price_events() -> None:
    loc = classify_spot_location(
        7452.0,
        put_wall=7450.0,
        call_wall=7500.0,
        flip_zone=[7455.0, 7460.0],
    )
    assert loc["location"] == "at_put_wall"

    mid = classify_spot_location(
        7475.0,
        put_wall=7450.0,
        call_wall=7500.0,
        flip_zone=[7455.0, 7460.0],
    )
    assert mid["location"] == "mid_range"

    assert classify_price_direction(-12.0) == "down"
    assert classify_price_direction(1.0) == "flat"

    event = classify_volume_price_event(
        pace="elevated",
        direction="down",
        location="at_put_wall",
    )
    assert event["event_id"] == "elevated_sell_into_support"
    assert event["play_hints"]

    held = classify_volume_price_event(
        pace="quiet",
        direction="down",
        location="below_put_wall",
        break_outcome="holds",
    )
    assert held["event_id"] == "quiet_breakdown_holds"

    reclaimed = classify_volume_price_event(
        pace="elevated",
        direction="up",
        location="mid_range",
        break_outcome="reclaimed",
    )
    assert reclaimed["event_id"] == "break_reclaimed"


def test_update_break_watch_hold_and_reclaim() -> None:
    now = datetime(2026, 7, 8, 15, 0, tzinfo=timezone.utc)
    watch, outcome = update_break_watch(
        None,
        spot=7440.0,
        put_wall=7450.0,
        call_wall=7500.0,
        flip_zone=[7455.0, 7460.0],
        pace="elevated",
        now=now,
    )
    assert watch is not None
    assert watch["kind"] == "put_wall"
    assert outcome == "pending"

    # Too soon: still pending.
    soon = now + timedelta(minutes=5)
    watch2, outcome2 = update_break_watch(
        watch,
        spot=7435.0,
        put_wall=7450.0,
        call_wall=7500.0,
        flip_zone=[7455.0, 7460.0],
        pace="quiet",
        now=soon,
    )
    assert outcome2 == "pending"
    assert watch2 is not None

    # After min window, still below -> holds.
    later = now + timedelta(minutes=15)
    watch3, outcome3 = update_break_watch(
        watch2,
        spot=7430.0,
        put_wall=7450.0,
        call_wall=7500.0,
        flip_zone=[7455.0, 7460.0],
        pace="quiet",
        now=later,
    )
    assert outcome3 == "holds"

    # Reclaim back above the wall.
    reclaim_at = now + timedelta(minutes=20)
    watch4, outcome4 = update_break_watch(
        watch3,
        spot=7460.0,
        put_wall=7450.0,
        call_wall=7500.0,
        flip_zone=[7455.0, 7460.0],
        pace="quiet",
        now=reclaim_at,
    )
    assert outcome4 == "reclaimed"


def test_es_volume_signal_binds_direction_location_event() -> None:
    now = datetime(2026, 7, 8, 15, 0, tzinfo=timezone.utc)
    samples = [
        {"at": (now - timedelta(minutes=30)).isoformat(), "volume": 940_000.0, "price": 7480.0},
    ]
    signal = es_volume_signal(
        1_000_000.0,
        samples,
        now=now,
        spot=7452.0,
        put_wall=7450.0,
        call_wall=7500.0,
        flip_zone=[7455.0, 7460.0],
    )
    assert signal is not None
    assert signal["label"] == "elevated"
    assert signal["direction"] == "down"
    assert signal["price_delta"] == -28.0
    assert signal["location"] == "at_put_wall"
    assert signal["event_id"] == "elevated_sell_into_support"
