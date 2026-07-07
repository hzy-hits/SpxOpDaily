from __future__ import annotations

import json
import math
import subprocess
from datetime import datetime, timezone
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
    frontrun_level_for,
    mark_sent,
    material_changes,
    minutes_to_open,
    option_tick,
    payload_fingerprint,
    project_option_price,
    render_status_template,
    render_template,
    round_to_tick,
    send_order_map,
    within_refresh_window,
    within_send_window,
    within_status_window,
)
from spx_spark.storage import LatestState

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")


def make_settings(
    state_path: str,
    *,
    missed_queue_path: str = "",
    agent_enabled: bool = False,
    bark_enabled: bool = False,
) -> NotificationSettings:
    return NotificationSettings(
        enabled=True,
        min_severity="high",
        cooldown_seconds=300,
        state_path=state_path,
        openclaw_enabled=True,
        openclaw_command="openclaw",
        openclaw_channel="openclaw-weixin",
        openclaw_account="account-im-bot",
        openclaw_target="user@im.wechat",
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
    state = make_state(
        underlier,
        make_option(
            expiry="20260707",
            strike=7500,
            right="C",
            mark=4.2,
            delta=0.35,
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
            delta=0.20,
            mark=5.0,
            gamma=0.005,
            now=now,
        ),
        make_option(
            expiry="20260707",
            strike=7500,
            right="P",
            delta=-0.25,
            mark=8.0,
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


def test_already_sent_roundtrip(tmp_path: Path) -> None:
    state_path = str(tmp_path / "order_map_state.json")
    assert already_sent(state_path, "2026-07-07") is False
    mark_sent(state_path, "2026-07-07")
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
    # 22:30 Beijing = 10:30 ET (summer): after open, inside window.
    beijing_2230 = datetime(2026, 7, 7, 14, 30, tzinfo=timezone.utc)
    assert within_refresh_window(beijing_2230) is True
    beijing_2345 = datetime(2026, 7, 7, 15, 45, tzinfo=timezone.utc)
    assert within_refresh_window(beijing_2345) is False
    # 13:00 Beijing = pre-open: refresh handed over to the status report.
    beijing_1300 = datetime(2026, 7, 7, 5, 0, tzinfo=timezone.utc)
    assert within_refresh_window(beijing_1300) is False
    # 21:00 Beijing = 9:00 ET: still pre-open, refresh stays quiet.
    beijing_2100 = datetime(2026, 7, 7, 13, 0, tzinfo=timezone.utc)
    assert within_refresh_window(beijing_2100) is False


def test_within_status_window_and_minutes_to_open() -> None:
    # 14:30 Beijing = 2:30 ET: inside status window, 420 minutes to open.
    beijing_1430 = datetime(2026, 7, 7, 6, 30, tzinfo=timezone.utc)
    assert within_status_window(beijing_1430) is True
    assert minutes_to_open(beijing_1430) == 420
    # 14:00 Beijing: before the 14:15 start (baseline order map slot).
    beijing_1400 = datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc)
    assert within_status_window(beijing_1400) is False
    # 21:30 Beijing = 9:30 ET: market open, status stops.
    beijing_2130 = datetime(2026, 7, 7, 13, 30, tzinfo=timezone.utc)
    assert within_status_window(beijing_2130) is False
    assert minutes_to_open(beijing_2130) is None
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


def test_send_order_map_queues_on_weixin_failure(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("SPX_PUSH_LLM_ENABLED", "false")
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

    def runner(command: list[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 1, stdout="", stderr="send failed")

    result = send_order_map(payload, settings, runner=runner)
    assert result["weixin_ok"] is False
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
