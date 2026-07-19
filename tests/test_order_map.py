from __future__ import annotations

import json
import math
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from spx_spark.application.order_map.pricing import (
    build_option_price_bs_projection,
    parity_forward,
)
from spx_spark.application.order_map.pricing_audit import (
    append_pricing_audit,
    build_pricing_audit_record,
)
from spx_spark.application.order_map.frozen_structure import attach_frozen_option_structure
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
    _quote_mid,
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
        feishu_webhook_url="https://open.feishu.cn/open-apis/bot/v2/hook/test"
        if feishu_enabled
        else "",
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


def test_frozen_option_structure_renders_walls_but_never_bs_or_limits() -> None:
    payload = {
        "research_only": True,
        "research_reference": {"price": 7550.0, "source": "future:ES"},
        "pricing_reference": {"gate_state": "missing"},
        "beijing_time": "09:30",
        "expiry": "20260716",
        "session_phase": {"name_cn": "GTH"},
        "gamma_state": "unknown",
        "wall_ladder": {"put_walls": [], "call_walls": []},
    }
    attach_frozen_option_structure(
        payload,
        {
            "front_expiry": "20260716",
            "structure": {
                "frozen": True,
                "source": "frozen_last_usable_option_frame",
                "frozen_as_of": "2026-07-16T01:00:00+00:00",
                "gamma_state": "zero_gamma_transition",
                "put_walls": [{"strike": 7525.0, "open_interest": 3622.0, "gex": -1.3e9}],
                "call_walls": [{"strike": 7625.0, "open_interest": 1562.0, "gex": 9.3e8}],
            },
        },
    )

    text = render_template(payload)

    assert "7525" in text
    assert "7625" in text
    assert "冻结" in text or "最后有效" in text
    assert "BS触位" not in text
    assert "限价" in text
    assert payload["wall_ladder"]["put_walls"][0]["projected_mid"] is None


def test_feishu_keeps_priced_wall_layout_when_level_state_is_far() -> None:
    from spx_spark.application.order_map.prompts import render_feishu_delivery_text

    payload = {
        "research_only": False,
        "expiry": "20260716",
        "session_phase": {"name_cn": "美盘数据前小时"},
        "underlier": {"price": 7552.0, "source": "chain_implied"},
        "gamma_state": "zero_gamma_transition",
        "level_decision": {"phase": "far"},
        "plan_candidates": [],
        "wall_ladder": {
            "put_walls": [
                {
                    "strike": 7525.0,
                    "option_strike": 7525,
                    "option_right": "C",
                    "current_mid": 32.0,
                    "projected_mid": 14.0,
                    "projection_range_low": 13.0,
                    "projection_range_high": 15.0,
                    "limit_aggressive": 14.0,
                    "limit_conservative": 12.0,
                }
            ],
            "call_walls": [],
        },
        "warnings": [],
    }

    text = render_feishu_delivery_text(
        payload,
        [],
        datetime(2026, 7, 16, 13, 20, tzinfo=timezone.utc),
        "决策摘要",
    )

    assert "决策摘要" in text
    assert "## 当前布局参考" in text
    assert "| 7525 | 主 Put Wall | 7525C | 32.00 | 13.00–15.00 | 12.00–14.00 |" in text


def test_feishu_delivery_includes_eight_strike_exposure_map() -> None:
    from spx_spark.application.order_map.prompts import render_feishu_delivery_text

    key_strikes = [
        {
            "strike": 7530.0 + index * 5,
            "distance_points": -17.0 + index * 5,
            "roles": ["ATM"] if index == 3 else [f"暴露{index + 1}"],
            "call_delta": 0.65 - index * 0.05,
            "put_delta": -0.35 - index * 0.05,
            "call_gamma": 0.004 + index * 0.0001,
            "put_gamma": 0.0041 + index * 0.0001,
            "oi_weighted": {
                "net_gex": 20_000_000.0 - index * 1_000_000,
                "abs_gex": 80_000_000.0,
                "net_dex_proxy": 500_000.0,
                "abs_dex_proxy": 2_000_000.0,
            },
            "volume_weighted": {
                "net_gex": -5_000_000.0,
                "abs_gex": 25_000_000.0,
                "net_dex_proxy": -100_000.0,
                "abs_dex_proxy": 800_000.0,
            },
        }
        for index in range(8)
    ]
    payload = {
        "research_only": False,
        "expiry": "20260717",
        "session_phase": {"name_cn": "美盘上午主战场"},
        "underlier": {"price": 7547.0, "source": "index:SPX"},
        "gamma_state": "zero_gamma_transition",
        "level_decision": {"phase": "far"},
        "plan_candidates": [],
        "wall_ladder": {"put_walls": [], "call_walls": []},
        "option_structure_frame": {
            "exposure": {
                "oi_weighted": {
                    "net_gex": 120_000_000.0,
                    "abs_gex": 640_000_000.0,
                    "net_gamma_ratio": 0.1875,
                    "net_dex_proxy": 4_000_000.0,
                    "abs_dex_proxy": 16_000_000.0,
                    "net_dex_ratio_proxy": 0.25,
                },
                "volume_weighted": {
                    "net_gex": -40_000_000.0,
                    "abs_gex": 200_000_000.0,
                    "net_gamma_ratio": -0.2,
                    "net_dex_proxy": -800_000.0,
                    "abs_dex_proxy": 6_400_000.0,
                    "net_dex_ratio_proxy": -0.125,
                },
                "key_strikes": key_strikes,
            }
        },
        "warnings": [],
    }

    text = render_feishu_delivery_text(
        payload,
        [],
        datetime(2026, 7, 17, 14, 0, tzinfo=timezone.utc),
        "决策摘要",
    )

    assert "## 0DTE 暴露地图" in text
    assert "OI代理　GEX净/绝 +120.0M/+640.0M（+19%）" in text
    assert "| 7545 | ATM　-2.0点 |" in text
    assert sum(line.startswith("| 75") for line in text.splitlines()) == 8
    assert "均不是 dealer 实仓" in text


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


def make_candidate_retry_state(
    *,
    state_now: datetime,
    candidate_quote_at: datetime,
    vix: float,
    extra_quotes: tuple[Quote, ...] = (),
) -> LatestState:
    return make_state(
        Quote(
            instrument=InstrumentId.index("SPX"),
            provider=Provider.IBKR,
            provider_symbol="index:SPX",
            received_at=state_now,
            quality=MarketDataQuality.LIVE,
            mark=7569.0,
            close=7570.0,
            quote_time=state_now,
        ),
        Quote(
            instrument=InstrumentId.index("VIX"),
            provider=Provider.IBKR,
            provider_symbol="index:VIX",
            received_at=state_now,
            quality=MarketDataQuality.LIVE,
            mark=vix,
            quote_time=state_now,
        ),
        make_option(
            expiry="20260707",
            strike=7500,
            right="C",
            mark=73.2,
            delta=0.85,
            gamma=0.008,
            now=state_now,
        ),
        make_option(
            expiry="20260707",
            strike=7500,
            right="P",
            mark=4.2,
            delta=-0.15,
            gamma=0.006,
            now=state_now,
        ),
        make_option(
            expiry="20260707",
            strike=7530,
            right="P",
            mark=9.1,
            delta=-0.28,
            gamma=0.007,
            now=state_now,
        ),
        make_option(
            expiry="20260707",
            strike=7550,
            right="C",
            mark=30.0,
            delta=0.45,
            gamma=0.005,
            now=state_now,
        ),
        make_option(
            expiry="20260707",
            strike=7550,
            right="P",
            mark=11.2,
            delta=-0.22,
            gamma=0.006,
            now=candidate_quote_at,
        ),
        *extra_quotes,
        now=state_now,
    )


def run_candidate_retry(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    states: list[LatestState],
    *,
    now: datetime,
    attempts: int = 3,
    delay_seconds: float = 10.0,
) -> tuple[dict, list[datetime], list[float]]:
    import spx_spark.application.order_map.service as order_map_module

    loaded_at: list[datetime] = []
    sleeps: list[float] = []
    elapsed = [0.0]

    class SequenceStore:
        def __init__(self, settings) -> None:
            pass

        def load(self, *, now: datetime) -> LatestState:
            index = len(loaded_at)
            loaded_at.append(now)
            return states[index]

    monkeypatch.setattr(order_map_module, "LatestStateStore", SequenceStore)
    monkeypatch.setattr(
        order_map_module,
        "build_options_map",
        lambda state: make_options_map(make_front_expiry()),
    )
    monkeypatch.setattr(order_map_module.time_module, "monotonic", lambda: elapsed[0])

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        elapsed[0] += seconds

    monkeypatch.setattr(order_map_module.time_module, "sleep", sleep)
    monkeypatch.setattr(order_map_module, "attach_es_volume_signal", lambda *a, **k: None)
    monkeypatch.setattr(order_map_module, "attach_hl_volume_signal", lambda *a, **k: None)

    payload = order_map_module.build_order_payload_with_retry(
        SimpleNamespace(data_root=str(tmp_path)),
        now=now,
        attempts=attempts,
        delay_seconds=delay_seconds,
    )
    return payload, loaded_at, sleeps


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


def test_bs_projection_exposes_replay_inputs() -> None:
    projection = build_option_price_bs_projection(
        mid=45.85,
        iv=0.1668,
        strike=7500.0,
        right="C",
        spot=7539.2,
        target=7500.0,
        tau_now_years=14.75 / (365.0 * 24.0),
        em_points=32.0,
        slope_per_point=-0.00062,
    )
    assert projection is not None
    assert projection.projected_mid > 0
    assert projection.iv_at_touch > projection.iv_now
    assert projection.touch_time_fraction == pytest.approx(0.9)
    assert projection.tau_at_touch_minutes == pytest.approx(88.5)


def test_pricing_audit_persists_model_payload_separately_from_prose(tmp_path: Path) -> None:
    now = datetime(2026, 7, 13, 5, 0, tzinfo=timezone.utc)
    payload = {
        "as_of": now.isoformat(),
        "trading_date": "2026-07-13",
        "expiry": "20260713",
        "underlier": {"price": 7533.2, "source": "chain_implied"},
        "pricing_reference": {"pricing_allowed": True},
        "expected_move_points": 32.0,
        "candidates": [
            {
                "contract_id": "option:SPX:SPXW:20260713:7500:C",
                "projection_model": "bs_repricing",
                "projected_mid": 9.12,
                "projection_touch_time_fraction": 0.6,
            }
        ],
        "warnings": [],
    }
    record = build_pricing_audit_record(
        payload,
        generated_at=now,
        report_kind="status",
        template="raw 9.12",
        delivered_text="writer 9.12",
        writer="deepseek",
        delivered_ok=True,
    )
    path = append_pricing_audit(str(tmp_path), record)
    loaded = json.loads(path.read_text(encoding="utf-8"))
    assert loaded["candidates"][0]["projection_model"] == "bs_repricing"
    assert loaded["template"] == "raw 9.12"
    assert loaded["delivered_text"] == "writer 9.12"


def test_build_candidates_produces_three_plays_with_limits() -> None:
    now = datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc)
    underlier = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        provider_symbol="index:SPX",
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
            right="C",
            mark=43.0,
            delta=0.72,
            gamma=0.007,
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
    options_map = replace(
        make_options_map(make_front_expiry()),
        underlier=UnderlierReference(price=7569.0, source="index:SPX"),
    )
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

    flip_bias = {
        "status": "confirmed",
        "play": "flip_reclaim_call",
        "expiry": "20260707",
        "level": 7530.0,
        "invalidation_level": 7527.0,
    }
    with_flip = build_candidates(
        state,
        options_map,
        conditional_call_bias=flip_bias,
    )
    assert with_flip[0].play == "flip_reclaim_call"
    assert [row.play for row in with_flip].count("flip_reclaim_call") == 1
    assert "flip_breakdown_put" not in {row.play for row in with_flip}
    assert next(row for row in with_flip if row.play == "flip_reclaim_call").right == "C"

    wall_bias = {
        "status": "confirmed",
        "play": "call_wall_breakout_call",
        "expiry": "20260707",
        "level": 7550.0,
        "invalidation_level": 7547.0,
    }
    with_breakout = build_candidates(
        state,
        options_map,
        conditional_call_bias=wall_bias,
    )
    assert with_breakout[0].play == "call_wall_breakout_call"
    assert [row.play for row in with_breakout].count("call_wall_breakout_call") == 1
    assert "call_wall_fade_put" not in {row.play for row in with_breakout}
    assert next(row for row in with_breakout if row.play == "call_wall_breakout_call").right == "C"

    formal_up = {
        "status": "confirmed",
        "formal_signal": True,
        "actionable": True,
        "play": "level_breakout_call",
        "direction": "up",
        "level_kind": "call_wall",
        "expiry": "20260707",
        "level": 7550.0,
        "invalidation_level": 7547.0,
    }
    with_formal_up = build_candidates(
        state,
        options_map,
        conditional_call_bias=formal_up,
    )
    assert with_formal_up[0].play == "level_breakout_call"
    assert with_formal_up[0].right == "C"

    formal_down = {
        **formal_up,
        "play": "level_breakout_put",
        "direction": "down",
        "level_kind": "flip_low",
        "invalidation_level": 7573.0,
    }
    with_formal_down = build_candidates(
        state,
        options_map,
        conditional_call_bias=formal_down,
    )
    assert with_formal_down[0].play == "level_breakout_put"
    assert with_formal_down[0].right == "P"

    invalidated = build_candidates(
        state,
        options_map,
        conditional_call_bias={**wall_bias, "invalidation_level": 7600.0},
    )
    assert "call_wall_breakout_call" not in {row.play for row in invalidated}

    wrong_expiry = build_candidates(
        state,
        options_map,
        conditional_call_bias={**wall_bias, "expiry": "20260708"},
    )
    assert "call_wall_breakout_call" not in {row.play for row in wrong_expiry}

    unanchored = build_candidates(
        state,
        replace(
            options_map,
            underlier=UnderlierReference(price=7569.0, source="future:ES"),
        ),
        conditional_call_bias=wall_bias,
    )
    assert "call_wall_breakout_call" not in {row.play for row in unanchored}


def test_build_candidates_bias_gate_uses_research_expiry_during_gth() -> None:
    # 2026-07-07 21:00 ET (01:00 UTC next day): inside GTH the research expiry
    # has rolled to the next trading day while the New York calendar date has
    # not. The bias gate must follow the research expiry like the level
    # decision machine does.
    now = datetime(2026, 7, 8, 1, 0, tzinfo=timezone.utc)
    underlier = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        provider_symbol="index:SPX",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7569.0,
        quote_time=now,
    )

    def _state_for(expiry: str) -> LatestState:
        return make_state(
            underlier,
            make_option(
                expiry=expiry, strike=7530, right="C", mark=43.0, delta=0.72, gamma=0.007, now=now
            ),
            make_option(
                expiry=expiry, strike=7530, right="P", mark=9.1, delta=-0.28, gamma=0.007, now=now
            ),
            now=now,
        )

    def _map_for(expiry: str) -> OptionsMap:
        return replace(
            make_options_map(replace(make_front_expiry(), expiry=expiry)),
            underlier=UnderlierReference(price=7569.0, source="index:SPX"),
        )

    flip_bias = {
        "status": "confirmed",
        "play": "flip_reclaim_call",
        "expiry": "20260708",
        "level": 7530.0,
        "invalidation_level": 7527.0,
    }
    rolled = build_candidates(
        _state_for("20260708"),
        _map_for("20260708"),
        conditional_call_bias=flip_bias,
    )
    assert [row.play for row in rolled].count("flip_reclaim_call") == 1

    # A front expiry still pinned to the calendar day is stale at this hour and
    # must keep the gate closed.
    stale = build_candidates(
        _state_for("20260707"),
        _map_for("20260707"),
        conditional_call_bias={**flip_bias, "expiry": "20260707"},
    )
    assert "flip_reclaim_call" not in {row.play for row in stale}


def test_order_payload_retry_rebuilds_after_stale_candidate_refresh(
    monkeypatch,
    tmp_path: Path,
) -> None:
    now = datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc)
    states = [
        make_candidate_retry_state(
            state_now=now,
            candidate_quote_at=now - timedelta(seconds=46),
            vix=15.0,
        ),
        make_candidate_retry_state(
            state_now=now + timedelta(seconds=10),
            candidate_quote_at=now + timedelta(seconds=10),
            vix=16.0,
        ),
    ]
    payload, loaded_at, sleeps = run_candidate_retry(
        monkeypatch,
        tmp_path,
        states,
        now=now,
    )

    assert loaded_at == [now, now + timedelta(seconds=10)]
    assert sleeps == [10.0]
    assert {item["play"] for item in payload["candidates"]} == {
        "put_wall_bounce_call",
        "flip_breakdown_put",
        "call_wall_fade_put",
    }
    assert not any("bad_quality_for_7550P" in item for item in payload["warnings"])
    assert payload["vol_context"]["vix"] == 16.0


def test_order_payload_retry_is_bounded_when_candidate_stays_stale(
    monkeypatch,
    tmp_path: Path,
) -> None:
    now = datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc)
    states = [
        make_candidate_retry_state(
            state_now=now + timedelta(seconds=offset),
            candidate_quote_at=now - timedelta(seconds=46),
            vix=15.0 + offset,
        )
        for offset in (0, 10, 20)
    ]
    payload, loaded_at, sleeps = run_candidate_retry(
        monkeypatch,
        tmp_path,
        states,
        now=now,
    )

    assert loaded_at == [
        now,
        now + timedelta(seconds=10),
        now + timedelta(seconds=20),
    ]
    assert sleeps == [10.0, 10.0]
    assert len(payload["candidates"]) == 2
    assert any(
        item == "bad_quality_for_7550P:transport_stale_after_45s" for item in payload["warnings"]
    )


def test_stale_non_candidate_does_not_trigger_order_payload_retry(
    monkeypatch,
    tmp_path: Path,
) -> None:
    now = datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc)
    stale_non_candidate = make_option(
        expiry="20260707",
        strike=7700,
        right="C",
        mark=1.0,
        delta=0.05,
        gamma=0.001,
        now=now - timedelta(seconds=30),
    )
    state = make_candidate_retry_state(
        state_now=now,
        candidate_quote_at=now,
        vix=15.0,
        extra_quotes=(stale_non_candidate,),
    )
    payload, loaded_at, sleeps = run_candidate_retry(
        monkeypatch,
        tmp_path,
        [state],
        now=now,
    )

    assert loaded_at == [now]
    assert sleeps == []
    assert len(payload["candidates"]) == 3


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


def test_build_candidates_require_underlier_trigger_and_frontrun() -> None:
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
        make_option(
            expiry="20260707", strike=7500, right="C", mark=73.2, delta=0.85, gamma=0.004, now=now
        ),
        make_option(
            expiry="20260707", strike=7500, right="P", mark=4.2, delta=-0.15, gamma=0.004, now=now
        ),
        make_option(
            expiry="20260707", strike=7530, right="P", mark=9.1, delta=-0.28, gamma=0.007, now=now
        ),
        make_option(
            expiry="20260707", strike=7550, right="P", mark=11.0, delta=-0.22, gamma=0.006, now=now
        ),
        make_option(
            expiry="20260707", strike=7550, right="C", mark=30.0, delta=0.45, gamma=0.006, now=now
        ),
        now=now,
    )
    candidates = build_candidates(state, make_options_map(make_front_expiry()))
    by_play = {candidate.play: candidate for candidate in candidates}

    # A future target-level price cannot be a naked resting option limit.
    bounce = by_play["put_wall_bounce_call"]
    assert bounce.order_style == "underlier_triggered_limit"
    # 30% of the 69pt distance exceeds the 8pt cap -> rung at wall + 8.
    assert bounce.frontrun_level == pytest.approx(7508.0)
    assert bounce.frontrun_projected_mid is not None
    assert bounce.frontrun_projected_mid > bounce.projected_mid

    # Dearer-at-touch options use the same underlier-triggered contract.
    breakdown = by_play["flip_breakdown_put"]
    assert breakdown.order_style == "underlier_triggered_limit"


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
        make_option(
            expiry="20260707", strike=7480, right="C", mark=18.0, delta=0.55, gamma=0.008, now=now
        ),
        make_option(
            expiry="20260707", strike=7480, right="P", mark=8.0, delta=-0.45, gamma=0.008, now=now
        ),
        make_option(
            expiry="20260707", strike=7550, right="P", mark=62.0, delta=-0.85, gamma=0.004, now=now
        ),
        make_option(
            expiry="20260707", strike=7550, right="C", mark=2.0, delta=0.10, gamma=0.004, now=now
        ),
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
    assert "★7500 (OI 3604,触达47%) → 7500C BS触位区间14.20(现31.05) 触发后参考14.20/12.00" in text
    assert (
        " 7480 (OI 1500,触达30%) → 7480C BS触位区间9.50(现40.00) 触发后参考9.50/8.00 [stale]"
        in text
    )
    assert "call 墙阶梯(上方阻力→买 put) (★=主墙):" in text
    assert "★7550 (OI 6555,触达51%) → 7550P BS触位区间12.40(现28.00) 触发后参考12.40/10.50" in text


def test_feishu_wall_layout_matches_compact_trading_table() -> None:
    from spx_spark.application.order_map.prompts import _detail_ladder_lines

    def rung(
        strike: int,
        right: str,
        current: float,
        projected: float,
        *,
        range_high: float | None = None,
        timing_capped: bool = False,
    ) -> dict[str, object]:
        return {
            "strike": strike,
            "option_strike": strike,
            "option_right": right,
            "current_mid": current,
            "projected_mid": projected,
            "projection_range_low": projected,
            "projection_range_high": range_high if range_high is not None else projected,
            "projection_timing_capped": timing_capped,
            "limit_aggressive": projected,
            "limit_conservative": round(projected * 0.85, 2),
            "distance_points": strike - 7561,
        }

    lines = _detail_ladder_lines(
        {
            "underlier": {"price": 7561.0},
            "wall_ladder": {
                "put_walls": [
                    rung(7550, "C", 23.75, 18.22),
                    rung(7535, "C", 34.65, 15.92),
                    {"strike": 7500, "distance_points": -61},
                    rung(7525, "C", 42.65, 7.67),
                ],
                "call_walls": [
                    rung(7600, "P", 41.95, 3.71, range_high=9.80, timing_capped=True),
                    rung(7610, "P", 50.85, 3.82),
                    rung(7570, "P", 21.35, 14.79),
                    rung(7580, "P", 27.00, 11.56),
                ],
            },
        }
    )

    assert lines[2:8] == [
        "| 7550 | 主 Put Wall | 7550C | 23.75 | 18.22 | 15.49–18.22 |",
        "| 7535 | 次级支撑 | 7535C | 34.65 | 15.92 | 13.53–15.92 |",
        "| 7525 | 外侧支撑 | 7525C | 42.65 | 7.67 | 6.52–7.67 |",
        "| 7570 | 近端 Call GEX | 7570P | 21.35 | 14.79 | 12.57–14.79 |",
        "| 7600 | 主 Call Wall | 7600P | 41.95 | 早触≈9.80 / 晚触重算 | 触位重算 |",
        "| 7610 | 次级 Call GEX | 7610P | 50.85 | 3.82 | 3.25–3.82 |",
    ]
    assert lines[8].startswith("> 触位情景是标的到墙时的早/基准/晚到达估值")


def test_feishu_wall_layout_suppresses_limits_when_quote_is_range_only() -> None:
    from spx_spark.application.order_map.prompts import _detail_ladder_lines

    lines = _detail_ladder_lines(
        {
            "underlier": {"price": 7561.0},
            "wall_ladder": {
                "put_walls": [
                    {
                        "strike": 7550,
                        "option_strike": 7550,
                        "option_right": "C",
                        "current_mid": 23.75,
                        "projected_mid": 18.22,
                        "projection_range_low": 17.80,
                        "projection_range_high": 18.60,
                        "limit_aggressive": None,
                        "limit_conservative": None,
                        "execution_quote_status": "range_only",
                        "execution_quote_reasons": ["provider_mid_divergence_exceeded"],
                    }
                ],
                "call_walls": [],
            },
        }
    )

    assert "| 7550 | 主 Put Wall | 7550C | 23.75（源分歧） | 暂不估值 | 触位重算 |" in lines


def test_feishu_wall_layout_labels_the_stable_structure_as_primary() -> None:
    from spx_spark.application.order_map.prompts import _detail_ladder_lines

    def rung(strike: int, right: str) -> dict[str, object]:
        return {
            "strike": strike,
            "option_strike": strike,
            "option_right": right,
            "current_mid": 10.0,
            "projected_mid": 8.0,
            "projection_range_low": 7.0,
            "projection_range_high": 9.0,
            "limit_aggressive": 8.0,
            "limit_conservative": 7.0,
            "execution_quote_status": "executable",
        }

    lines = _detail_ladder_lines(
        {
            "underlier": {"price": 7561.0},
            "level_decision": {"levels": {"put_wall": 7550.0, "call_wall": 7600.0}},
            "wall_ladder": {
                "put_walls": [rung(7525, "C"), rung(7550, "C"), rung(7500, "C")],
                "call_walls": [rung(7575, "P"), rung(7555, "P")],
            },
        }
    )

    assert any("| 7550 | 主 Put Wall |" in line for line in lines)
    assert any("| 7525 | 次级支撑 |" in line for line in lines)
    assert any("| 7575 | Call GEX 主峰候选 |" in line for line in lines)


def test_candidate_and_wall_ladder_share_the_same_bs_projection() -> None:
    from spx_spark.application.order_map.candidates import _build_candidate

    now = datetime(2026, 7, 16, 15, 30, tzinfo=timezone.utc)
    quotes = [
        make_option(
            expiry="20260716",
            strike=strike,
            right=right,
            mark=mid,
            delta=delta,
            gamma=0.01,
            now=now,
        )
        for strike, right, mid, delta in (
            (7545, "C", 18.0, 0.70),
            (7545, "P", 8.0, -0.30),
            (7550, "C", 15.0, 0.62),
            (7550, "P", 10.0, -0.38),
            (7555, "C", 12.0, 0.54),
            (7555, "P", 12.0, -0.46),
        )
    ]
    pairs = pair_by_strike(quotes)
    tau_now_years = 4.5 / (365.0 * 24.0)
    candidate = _build_candidate(
        play="put_wall_bounce_call",
        level=7550.0,
        level_label="put wall 7550",
        target_strike=7550,
        right="C",
        spot=7561.0,
        expiry_quotes=quotes,
        all_quotes=tuple(quotes),
        strike_step=5.0,
        pairs=pairs,
        warnings=[],
        as_of=now,
        tau_now_years=tau_now_years,
        em_points=23.0,
    )
    wall = _wall_rung_option_ref(
        wall_strike=7550.0,
        right="C",
        spot=7561.0,
        expiry_quotes=quotes,
        all_quotes=quotes,
        pairs=pairs,
        strike_step=5.0,
        tau_now_years=tau_now_years,
        em_points=23.0,
        as_of=now,
    )

    assert candidate is not None
    assert wall["projected_mid"] == pytest.approx(candidate.projected_mid)
    assert wall["projection_range_low"] == pytest.approx(candidate.projection_range_low)
    assert wall["projection_range_high"] == pytest.approx(candidate.projection_range_high)


def test_actionable_pricing_rejects_stale_and_frozen_quotes() -> None:
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
    # Cold-lane STALE may remain research-visible but cannot produce limits.
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
    assert stale_ref["projected_mid"] is None
    assert stale_ref["limit_aggressive"] is None

    divergent_schwab = replace(
        live,
        provider=Provider.SCHWAB,
        provider_symbol="SPXW:SCHWAB:7500:C",
        bid=34.9,
        ask=35.1,
        mark=35.0,
    )
    range_only_ref = _wall_rung_option_ref(
        wall_strike=7500.0,
        right="C",
        spot=7520.0,
        expiry_quotes=[live],
        all_quotes=[live, divergent_schwab],
        pairs=pair_by_strike([live]),
        strike_step=5.0,
        tau_now_years=4.0 / (365.0 * 24.0),
        em_points=20.0,
        as_of=now,
    )
    assert range_only_ref["execution_quote_status"] == "range_only"
    assert "provider_mid_divergence_exceeded" in range_only_ref["execution_quote_reasons"]
    assert range_only_ref["projected_mid"] is not None
    assert range_only_ref["limit_aggressive"] is None
    assert range_only_ref["limit_conservative"] is None

    far_put = make_option(
        expiry="20260709",
        strike=7600.0,
        right="P",
        mark=40.0,
        delta=-0.85,
        gamma=0.006,
        now=now,
    )
    far_ref = _wall_rung_option_ref(
        wall_strike=7600.0,
        right="P",
        spot=7564.0,
        expiry_quotes=[far_put],
        pairs=pair_by_strike([far_put]),
        strike_step=5.0,
        tau_now_years=15.0 / (365.0 * 24.0),
        em_points=28.0,
        as_of=now,
    )
    assert far_ref["projection_timing_capped"] is True
    assert far_ref["projection_touch_time_fraction"] == pytest.approx(0.9)
    assert far_ref["projection_tau_at_touch_minutes"] == pytest.approx(90.0)
    assert far_ref["projection_range_high"] > far_ref["projected_mid"]

    frozen = Quote(
        instrument=live.instrument,
        provider=live.provider,
        provider_symbol=live.provider_symbol,
        received_at=now,
        quality=MarketDataQuality.FROZEN,
        bid=live.bid,
        ask=live.ask,
        mark=live.mark,
        quote_time=now,
        last_update_at=now,
        greeks=live.greeks,
    )
    assert _quote_mid(frozen, as_of=now) is None

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


def test_render_template_shows_underlier_trigger_and_frontrun_notes() -> None:
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
                "order_style": "underlier_triggered_limit",
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
                "order_style": "underlier_triggered_limit",
            },
        ],
        "spxw_0dte_greeks_reference": {
            "status": "ok",
            "aggregate": {
                "gross_gamma_abs": 1234.0,
                "gross_charm_5m_abs": 56.0,
                "gross_vanna_1vol_abs": 7.0,
            },
            "coverage": {
                "usable_contract_count": 8,
                "exact_expiry_contract_count": 10,
            },
        },
        "warnings": [],
    }
    text = render_template(payload)
    assert "先手挡 7507.2: 触发后参考 19.80" in text
    assert "触达≈62%" in text
    assert "SPX 触及 7515 后再提交" in text
    assert text.count("当前不可预挂") == 2


def test_resolve_spx_spot_keeps_hl_research_separate_from_chain_pricing() -> None:
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
    es_anchor = Quote(
        instrument=InstrumentId.future("ES"),
        provider=Provider.IBKR,
        provider_symbol="future:ES",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7520.0,
        quote_time=now,
    )
    # Parity pair implies 7533 (C-P=8 at 7525): a small, accepted divergence.
    state = make_state(
        hl_quote,
        es_anchor,
        make_option(
            expiry="20260707", strike=7525, right="C", mark=18.0, delta=0.55, gamma=0.008, now=now
        ),
        make_option(
            expiry="20260707", strike=7525, right="P", mark=10.0, delta=-0.45, gamma=0.008, now=now
        ),
        now=now,
    )
    options_map = make_options_map(make_front_expiry())

    warnings: list[str] = []
    resolution = resolve_spx_spot(state, options_map, warnings=warnings, now=now)
    assert resolution.research_source == "hl_perp"
    assert resolution.research_price == pytest.approx(7520.0)
    assert resolution.pricing_source == "chain_implied"
    assert resolution.pricing_price == pytest.approx(7533.0)
    assert resolution.pricing_allowed is True
    assert resolution.gate_state == "basis_ok"

    # During cash hours the live SPX index is the level and pricing coordinate.
    rth = datetime(2026, 7, 7, 15, 0, tzinfo=timezone.utc)  # 11:00 ET
    rth_state = make_state(
        Quote(
            instrument=hl_quote.instrument,
            provider=Provider.HYPERLIQUID,
            provider_symbol="xyz:SP500",
            received_at=rth,
            quality=MarketDataQuality.LIVE,
            mark=7520.0,
            quote_time=rth,
        ),
        Quote(
            instrument=es_anchor.instrument,
            provider=Provider.IBKR,
            provider_symbol="future:ES",
            received_at=rth,
            quality=MarketDataQuality.LIVE,
            mark=7520.0,
            quote_time=rth,
        ),
        Quote(
            instrument=InstrumentId.index("SPX"),
            provider=Provider.IBKR,
            provider_symbol="index:SPX",
            received_at=rth,
            quality=MarketDataQuality.LIVE,
            mark=7532.0,
            quote_time=rth,
        ),
        make_option(
            expiry="20260707",
            strike=7525,
            right="C",
            mark=18.0,
            delta=0.55,
            gamma=0.008,
            now=rth,
        ),
        make_option(
            expiry="20260707",
            strike=7525,
            right="P",
            mark=10.0,
            delta=-0.45,
            gamma=0.008,
            now=rth,
        ),
        now=rth,
    )
    warnings_rth: list[str] = []
    rth_resolution = resolve_spx_spot(
        rth_state,
        options_map,
        warnings=warnings_rth,
        now=rth,
    )
    assert rth_resolution.research_source == "index:SPX"
    assert rth_resolution.research_price == pytest.approx(7532.0)
    assert rth_resolution.pricing_source == "index:SPX"
    assert rth_resolution.pricing_price == pytest.approx(7532.0)


def test_resolve_spx_spot_blocks_model_pricing_when_hl_basis_warns() -> None:
    from spx_spark.marketdata import InstrumentType
    from spx_spark.order_map import resolve_spx_spot

    now = datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc)
    hl_quote = Quote(
        instrument=InstrumentId(
            symbol="xyz:SP500",
            instrument_type=InstrumentType.CRYPTO_PERP,
        ),
        provider=Provider.HYPERLIQUID,
        provider_symbol="xyz:SP500",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7488.0,
        quote_time=now,
    )
    spx_anchor = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        provider_symbol="index:SPX",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7533.0,
        quote_time=now,
    )
    state = make_state(
        hl_quote,
        spx_anchor,
        make_option(
            expiry="20260707",
            strike=7525,
            right="C",
            mark=18.0,
            delta=0.55,
            gamma=0.008,
            now=now,
        ),
        make_option(
            expiry="20260707",
            strike=7525,
            right="P",
            mark=10.0,
            delta=-0.45,
            gamma=0.008,
            now=now,
        ),
        now=now,
    )

    resolution = resolve_spx_spot(
        state,
        make_options_map(make_front_expiry()),
        now=now,
    )

    assert resolution.research_source == "hl_perp"
    assert resolution.pricing_allowed is False
    assert resolution.pricing_price is None
    assert resolution.gate_state == "basis_warn"


def test_market_context_anchor_gate_overrides_chain_hl_agreement() -> None:
    from spx_spark.marketdata import InstrumentType
    from spx_spark.order_map import resolve_spx_spot

    now = datetime(2026, 7, 7, 15, 0, tzinfo=timezone.utc)
    hl_quote = Quote(
        instrument=InstrumentId(
            symbol="xyz:SP500",
            instrument_type=InstrumentType.CRYPTO_PERP,
        ),
        provider=Provider.HYPERLIQUID,
        provider_symbol="xyz:SP500",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7500.0,
        quote_time=now,
    )
    spx_anchor = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        provider_symbol="index:SPX",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7400.0,
        quote_time=now,
    )
    state = make_state(
        hl_quote,
        spx_anchor,
        make_option(
            expiry="20260707",
            strike=7500,
            right="C",
            mark=10.0,
            delta=0.5,
            gamma=0.008,
            now=now,
        ),
        make_option(
            expiry="20260707",
            strike=7500,
            right="P",
            mark=10.0,
            delta=-0.5,
            gamma=0.008,
            now=now,
        ),
        now=now,
    )

    resolution = resolve_spx_spot(
        state,
        make_options_map(make_front_expiry()),
        now=now,
    )

    assert resolution.gate_state == "basis_blocked"
    assert resolution.pricing_allowed is False
    assert resolution.pricing_price is None


def test_spy_is_not_a_standalone_executable_pricing_anchor() -> None:
    from spx_spark.marketdata import InstrumentType
    from spx_spark.order_map import resolve_spx_spot

    now = datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc)
    state = make_state(
        Quote(
            instrument=InstrumentId(
                symbol="xyz:SP500",
                instrument_type=InstrumentType.CRYPTO_PERP,
            ),
            provider=Provider.HYPERLIQUID,
            provider_symbol="xyz:SP500",
            received_at=now,
            quality=MarketDataQuality.LIVE,
            mark=7500.0,
            quote_time=now,
        ),
        Quote(
            instrument=InstrumentId.equity("SPY"),
            provider=Provider.IBKR,
            provider_symbol="stock:SPY",
            received_at=now,
            quality=MarketDataQuality.LIVE,
            mark=750.0,
            quote_time=now,
        ),
        now=now,
    )

    resolution = resolve_spx_spot(
        state,
        make_options_map(make_front_expiry()),
        now=now,
    )

    assert resolution.gate_state == "unanchored"
    assert resolution.pricing_allowed is False
    assert resolution.pricing_price is None


def test_hl_only_order_payload_is_valid_research_without_executable_aliases(
    monkeypatch,
) -> None:
    import spx_spark.application.order_map.service as order_map_module
    from spx_spark.marketdata import InstrumentType

    now = datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc)
    hl_quote = Quote(
        instrument=InstrumentId(
            symbol="xyz:SP500",
            instrument_type=InstrumentType.CRYPTO_PERP,
        ),
        provider=Provider.HYPERLIQUID,
        provider_symbol="xyz:SP500",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7520.0,
        quote_time=now,
    )
    options_map = make_options_map(make_front_expiry())
    monkeypatch.setattr(order_map_module, "build_options_map", lambda state: options_map)

    payload = build_order_payload(make_state(hl_quote, now=now), now=now)

    assert payload["research_only"] is True
    assert payload["pricing_allowed"] is False
    assert payload["research_reference"] == {"price": 7520.0, "source": "hl_perp"}
    assert payload["underlier"] == {"price": None, "source": None}
    assert payload["candidates"] == []
    assert payload["wall_ladder"] == {"call_walls": [], "put_walls": []}
    assert len(payload["research_candidates"]) == 3
    assert all(
        "play" not in item and "right" not in item for item in payload["research_candidates"]
    )
    assert all(
        {option["right"] for option in item["observed_options"]} == {"C", "P"}
        for item in payload["research_candidates"]
    )
    assert payload["rn_density"] is None
    rendered = render_template(payload)
    assert "不可执行定价" in rendered
    assert "挂单参考:" not in rendered
    assert "触达≈" not in rendered


def test_globex_payload_promotes_level_machine_spx_proxy_to_context_reference(
    monkeypatch,
    tmp_path,
) -> None:
    import spx_spark.application.order_map.service as order_map_module
    from spx_spark.marketdata import InstrumentType

    now = datetime(2026, 7, 13, 4, 30, tzinfo=timezone.utc)
    hl_quote = Quote(
        instrument=InstrumentId(
            symbol="xyz:SP500",
            instrument_type=InstrumentType.CRYPTO_PERP,
        ),
        provider=Provider.HYPERLIQUID,
        provider_symbol="xyz:SP500",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7530.0,
        quote_time=now,
    )
    monkeypatch.setattr(
        order_map_module,
        "load_level_decision_shadow",
        lambda settings: {
            "spot": 7533.5,
            "spot_source": "es_basis_adjusted:46.2150",
            "es": 7579.75,
            "phase": "approaching",
        },
    )

    payload, _, _ = run_candidate_retry(
        monkeypatch,
        tmp_path,
        [make_state(hl_quote, now=now)],
        now=now,
        attempts=1,
    )

    assert payload["analysis_mode"] == "globex_context"
    assert payload["context_reference"] == {
        "price": 7533.5,
        "source": "es_basis_adjusted:46.2150",
        "executable": False,
    }
    assert payload["pricing_allowed"] is False
    assert payload["underlier"] == {"price": None, "source": None}


def test_missing_all_references_still_fails_closed_as_research_only() -> None:
    now = datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc)

    payload = build_order_payload(make_state(now=now), now=now)

    assert payload["pricing_allowed"] is False
    assert payload["research_only"] is True
    assert payload["research_reference"] == {"price": None, "source": None}
    assert payload["underlier"] == {"price": None, "source": None}
    assert payload["candidates"] == []
    assert payload["expiry"] == "20260707"
    assert "不可执行定价" in render_template(payload)


def test_research_status_includes_basis_spx_and_frozen_key_level_context() -> None:
    payload = {
        "research_only": True,
        "beijing_time": "12:30",
        "expiry": "20260713",
        "research_reference": {"price": 7582.25, "source": "future:ES"},
        "pricing_reference": {"gate_state": "missing", "reason": "cash SPX unavailable"},
        "level_decision": {
            "phase": "approaching",
            "formal_signal": False,
            "level_kind": "flip_low",
            "level": 7545.0,
            "spot": 7536.03,
            "es": 7582.25,
            "spot_source": "es_basis_adjusted:46.2150",
            "levels": {
                "put_wall": 7550.0,
                "flip_low": 7545.0,
                "flip_high": 7550.0,
                "call_wall": 7575.0,
            },
        },
        "warnings": [],
    }

    rendered = render_template(payload)

    assert "SPX 代理: 7536(es_basis_adjusted:46.2150)" in rendered
    assert "Put Wall 7550 | Flip 7545–7550 | Call Wall 7575" in rendered
    assert "距触发位 -9.0 点" in rendered
    assert "正在接近，尚未完成关键位测试" in rendered
    assert "低于Put Wall 14.0点" in rendered


def test_us_data_hour_still_labels_es_path_as_globex_before_cash_open() -> None:
    payload = {
        "research_only": True,
        "beijing_time": "21:00",
        "expiry": "20260715",
        "research_reference": {"price": 7566.0, "source": "future:ES"},
        "pricing_reference": {"gate_state": "missing"},
        "session_phase": {
            "name": "us_data_hour",
            "name_cn": "美盘数据前小时",
            "minutes_since_us_open": None,
        },
        "globex_trend": {
            "regime": "bullish",
            "metrics": {
                "price": 7611.5,
                "return_15m_points": 1.0,
                "return_60m_points": 11.0,
                "return_180m_points": 7.5,
            },
        },
    }

    rendered = render_template(payload)

    assert "ES Globex路径" in rendered
    assert "ES RTH路径" not in rendered


def test_research_observed_quotes_apply_freshness_and_label_stale_rows(
    monkeypatch,
) -> None:
    import spx_spark.application.order_map.service as order_map_module
    from spx_spark.marketdata import InstrumentType

    now = datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc)
    hl_quote = Quote(
        instrument=InstrumentId(
            symbol="xyz:SP500",
            instrument_type=InstrumentType.CRYPTO_PERP,
        ),
        provider=Provider.HYPERLIQUID,
        provider_symbol="xyz:SP500",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7520.0,
        quote_time=now,
    )
    stale_call = make_option(
        expiry="20260707",
        strike=7500,
        right="C",
        mark=10.0,
        delta=0.5,
        gamma=0.008,
        now=now - timedelta(minutes=10),
        quality=MarketDataQuality.STALE,
    )
    monkeypatch.setattr(
        order_map_module,
        "build_options_map",
        lambda state: make_options_map(make_front_expiry()),
    )

    payload = build_order_payload(make_state(hl_quote, stale_call, now=now), now=now)
    put_wall = next(
        item for item in payload["research_candidates"] if item["level_kind"] == "put_wall"
    )
    observed_call = next(item for item in put_wall["observed_options"] if item["right"] == "C")

    assert observed_call["observed_bid"] is None
    assert observed_call["observed_ask"] is None
    assert observed_call["quote_freshness"] == "stale"
    assert "[stale/stale]" in render_template(payload)


def test_globex_status_uses_writer_and_trade_delivery(monkeypatch, tmp_path) -> None:
    import spx_spark.application.order_map.service as order_map_module

    payload = {
        "research_only": True,
        "research_reference": {"price": 7520.0, "source": "hl_perp"},
    }
    captured: dict[str, object] = {}
    monkeypatch.setattr(order_map_module, "load_order_map_state", lambda path: {})
    monkeypatch.setattr(
        order_map_module,
        "build_order_payload_with_retry",
        lambda *args, **kwargs: payload,
    )
    monkeypatch.setattr(order_map_module, "payload_fingerprint", lambda value: {})
    monkeypatch.setattr(order_map_module, "material_changes", lambda *args: [])
    monkeypatch.setattr(
        order_map_module,
        "render_status_template",
        lambda *args: "deterministic research status",
    )
    monkeypatch.setattr(
        order_map_module,
        "render_feishu_delivery_text",
        lambda *args: "detailed feishu status",
    )
    monkeypatch.setattr(
        order_map_module,
        "generate_push_text",
        lambda *args, **kwargs: ("written globex context", "deepseek"),
    )
    monkeypatch.setattr(
        order_map_module.NotificationSettings,
        "from_env",
        classmethod(lambda cls: object()),
    )

    def deliver(settings, envelope, *, title, text, friend, feishu_text, runner, **kwargs):
        captured.update(
            title=title,
            text=text,
            kind=envelope.kind,
            lane=envelope.lane,
            friend=friend,
            feishu_text=feishu_text,
        )
        return SimpleNamespace(
            sinks=(
                SimpleNamespace(sink="bark", ok=True, attempted=True),
                SimpleNamespace(sink="feishu", ok=True, attempted=True),
            ),
            delivered=True,
        )

    monkeypatch.setattr(order_map_module, "dispatch_notification", deliver)
    monkeypatch.setattr(order_map_module, "mark_sent", lambda *args, **kwargs: None)
    monkeypatch.setattr(order_map_module, "record_push", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        order_map_module,
        "persist_order_map_pricing_audit",
        lambda *args, **kwargs: None,
    )

    result = order_map_module.run_status(
        SimpleNamespace(force=True, dry_run=False),
        now=datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc),
        state_path=str(tmp_path / "state.json"),
        trading_date="2026-07-07",
    )

    assert result == 0
    assert captured == {
        "title": "SPX 15分钟市场状态",
        "text": "written globex context",
        "kind": "status",
        "lane": "scheduled_report",
        "friend": True,
        "feishu_text": "detailed feishu status",
    }


def test_force_cannot_bypass_research_only_direct_map_gate(
    monkeypatch,
    tmp_path,
    capsys,
) -> None:
    import spx_spark.application.order_map.service as order_map_module

    payload = {
        "research_only": True,
        "research_reference": {"price": 7520.0, "source": "hl_perp"},
    }
    monkeypatch.setenv("SPX_ORDER_MAP_STATE_PATH", str(tmp_path / "state.json"))
    monkeypatch.setattr(
        order_map_module.StorageSettings,
        "from_env",
        classmethod(lambda cls: object()),
    )
    monkeypatch.setattr(
        order_map_module,
        "build_order_payload_with_retry",
        lambda *args, **kwargs: payload,
    )
    monkeypatch.setattr(order_map_module, "render_template", lambda value: "research")
    monkeypatch.setattr(order_map_module, "load_order_map_state", lambda path: {})
    monkeypatch.setattr(
        order_map_module,
        "send_order_map",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("direct map sent")),
    )

    now = datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc)
    assert order_map_module.run(["--force"], now=now) == 0
    assert order_map_module.run(["--refresh", "--force"], now=now) == 0

    outputs = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert outputs == [
        {"skipped": True, "reason": "research_only_no_direct_map"},
        {"skipped": True, "reason": "research_only_no_direct_map"},
    ]


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

    payload = {
        "expiry": "20260707",
        "candidates": [],
        "warnings": [],
        "signed_gex_proxy": {
            "net_gex": -2_500_000_000.0,
            "abs_gex": 12_000_000_000.0,
            "net_gamma_ratio": -0.2083,
            "gamma_state": "zero_gamma_transition",
            "weighting": "open_interest",
            "sign_method": "calls_positive_puts_negative",
            "dealer_position_sign": "unknown",
        },
        "option_structure_frame": {
            "as_of": "2026-07-07T06:00:00+00:00",
            "exposure": {
                "quality": "ok",
                "snapshot_age_seconds": 4.2,
                "delta_coverage_ratio": 0.8,
                "iv_coverage_ratio": 0.9,
                "oi_quality": "ibkr_ok",
                "dealer_position_sign": "unknown",
                "sign_convention": "calls_positive_puts_negative",
                "gex_weighting_divergence": 0.31,
                "oi_weighted": {
                    "net_gex": -2_500_000_000.0,
                    "abs_gex": 12_000_000_000.0,
                    "net_gamma_ratio": -0.2083,
                    "net_dex_proxy": -900_000.0,
                    "abs_dex_proxy": 4_500_000.0,
                    "net_dex_ratio_proxy": -0.2,
                },
                "volume_weighted": {
                    "net_gex": 300_000_000.0,
                    "abs_gex": 3_000_000_000.0,
                    "net_gamma_ratio": 0.1,
                    "net_dex_proxy": 700_000.0,
                    "abs_dex_proxy": 2_800_000.0,
                    "net_dex_ratio_proxy": 0.25,
                },
                "key_strikes": [
                    {
                        "strike": 7550.0,
                        "roles": ["ATM", "Flip下"],
                        "call_delta": 0.52,
                        "put_delta": -0.48,
                    }
                ],
                "warnings": ["oi_volume_gex_divergent"],
            },
        },
        "_spxw_0dte_greeks_audit": {"secret_scenario_price": 123.45},
    }
    previous = {"kind": "order_map", "at": "2026-07-07T06:00:00+00:00", "text": "上一条正文"}
    order_prompt = build_order_prompt(payload, "模板行", previous)
    assert "上一条正文" in order_prompt
    assert "previous_push:" in order_prompt
    status_prompt = build_status_prompt(payload, "模板行", None)
    assert "previous_push:null" in status_prompt
    assert "SPX 决策摘要" in status_prompt
    assert "第一行逐字保留模板标题" in status_prompt
    assert "最多两个情景" in status_prompt
    assert "observation_candidates 必须称为观察情景" in status_prompt
    assert "负 gamma 不等于下跌" in order_prompt
    assert "负 gamma 不等于下跌" in status_prompt
    assert "TradeReady" in order_prompt
    assert "自动下单仍关闭" in order_prompt
    assert "TradeReady" in status_prompt
    assert "自动下单仍关闭" in status_prompt
    assert "breakout_filter.verdict=blocked" in order_prompt
    assert "supported 且 actionable=true" in status_prompt
    assert "SPXW_0DTE_options_not_ES_options" in status_prompt
    assert '"net_dex_proxy":-900000.0' in status_prompt
    assert '"abs_dex_proxy":4500000.0' in status_prompt
    assert '"key_strikes":[{"strike":7550.0' in status_prompt
    assert "两者背离时优先提示假突破风险" in status_prompt
    assert "不是 ES 期货自身的 GEX/DEX" in order_prompt
    assert "secret_scenario_price" not in order_prompt
    assert "secret_scenario_price" not in status_prompt


def test_globex_prompts_require_proxy_analysis_without_executable_pricing() -> None:
    from spx_spark.application.order_map.prompts import globex_writer_output_valid
    from spx_spark.order_map import build_order_prompt, build_status_prompt

    payload = {
        "research_only": True,
        "analysis_mode": "globex_context",
        "context_reference": {
            "price": 7533.5,
            "source": "es_basis_adjusted:46.2150",
            "executable": False,
        },
        "level_decision": {
            "phase": "testing",
            "levels": {"put_wall": 7550, "flip_low": 7545, "call_wall": 7575},
        },
    }
    previous = {"kind": "market_status", "text": "上一条夜盘上下文"}

    status_prompt = build_status_prompt(payload, "模板", previous)
    order_prompt = build_order_prompt(payload, "模板", previous)

    assert "ES-basis SPX 代理" in status_prompt
    assert "主情景" in status_prompt
    assert "确认条件和证伪条件" in status_prompt
    assert "不许写『等开盘再说』" in status_prompt
    assert "不可执行定价" in order_prompt
    assert "上一条夜盘上下文" in status_prompt
    assert "上一条夜盘上下文" in order_prompt
    template = "SPX 代理 7531.0；Flip 7545.0；ES 等效 7591.2"
    assert globex_writer_output_valid(
        "SPX 代理 7531，站回 7545 时看 ES 7591.2",
        template,
    )
    assert not globex_writer_output_valid(
        "跌破 ES 7560 才确认",
        template,
    )
    assert not globex_writer_output_valid(
        "当前是无引力气垫区",
        template,
    )


def test_actionable_writer_requires_exact_numbers_contracts_and_no_prehang() -> None:
    from spx_spark.application.order_map.prompts import actionable_writer_output_valid

    template = "\n".join(
        (
            "1) [地图候选] put wall 7550 → SPXW 7550C",
            "BS触位情景价 13.39，现价 18.60",
            "2) [地图候选] flip 7550 → SPXW 7550P",
            "BS触位情景价 13.47，现价 9.35",
            "当前不可预挂",
        )
    )
    assert actionable_writer_output_valid(
        "条件执行参考：7550C 13.39；7550P 13.47；当前不可预挂",
        template,
    )
    assert not actionable_writer_output_valid(
        "条件执行参考：7550C 14.00；7550P 13.47；当前不可预挂",
        template,
    )
    assert not actionable_writer_output_valid(
        "条件执行参考：7550C 13.39；当前不可预挂",
        template,
    )
    assert not actionable_writer_output_valid(
        "条件执行参考：7550C 13.39；7550P 13.47",
        template,
    )


def test_actionable_writer_preserves_compact_status_layout() -> None:
    from spx_spark.application.order_map.prompts import actionable_writer_output_valid

    template = "\n".join(
        (
            "【SPX 15m · 22:30 · 0DTE 07-13 · 开盘首小时】",
            "价格  SPX 7550　ES 7595",
            "",
            "【条件计划】标的触发后执行",
            "计划1 · 支撑反弹  SPX 7545触发 → SPXW 7545C　触达 60%　参考 9–11",
            "执行  触位后按实时 mid/IV 重算；当前不可预挂",
        )
    )
    assert actionable_writer_output_valid(template, template)
    extended = template + "\n" + "\n".join("补充  数据正常" for _ in range(20))
    assert actionable_writer_output_valid(extended, template)
    assert not actionable_writer_output_valid(template.replace("\n\n", "\n"), template)


def test_persistence_uses_private_audit_reference_not_writer_payload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import spx_spark.application.order_map.service as order_map_module

    captured: dict[str, object] = {}

    def write(reference, *, data_root):
        captured["reference"] = reference
        captured["data_root"] = data_root
        return None

    monkeypatch.setattr(order_map_module, "write_zero_dte_greeks_snapshot", write)
    payload = {
        "spxw_0dte_greeks_reference": {"contracts": []},
        "_spxw_0dte_greeks_audit": {"contracts": [{"scenario": "clock_plus_15m"}]},
    }
    settings = SimpleNamespace(data_root=str(tmp_path))

    order_map_module.persist_zero_dte_greeks_reference(payload, settings)

    assert captured["reference"] == payload["_spxw_0dte_greeks_audit"]
    assert captured["data_root"] == str(tmp_path)


def test_chain_implied_spot_uses_put_call_parity() -> None:
    from spx_spark.marketdata import OptionRight
    from spx_spark.options_map import pair_by_strike

    now = datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc)
    # True spot ~7517: the 7515 pair implies 7517, the looser 7550 wing pair
    # implies 7518; with fewer than five pairs both enter the sample and the
    # median of two is their average.
    quotes = [
        make_option(
            expiry="20260707", strike=7515, right="C", mark=12.0, delta=0.52, gamma=0.008, now=now
        ),
        make_option(
            expiry="20260707", strike=7515, right="P", mark=10.0, delta=-0.48, gamma=0.008, now=now
        ),
        make_option(
            expiry="20260707", strike=7550, right="C", mark=2.0, delta=0.15, gamma=0.004, now=now
        ),
        make_option(
            expiry="20260707", strike=7550, right="P", mark=34.0, delta=-0.85, gamma=0.004, now=now
        ),
    ]
    pairs = pair_by_strike(quotes)
    assert set(pairs[7515.0]) == {OptionRight.CALL, OptionRight.PUT}
    implied = chain_implied_spot(pairs)
    assert implied == pytest.approx(7517.5)


def test_chain_implied_spot_matches_parity_forward_median() -> None:
    # Same robust convention as parity_forward with discount_factor=1.0:
    # five tightest |C - P| pairs, median of K + C - P.
    now = datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc)
    quotes = [
        make_option(
            expiry="20260707", strike=strike, right=right, mark=mark, delta=delta, gamma=0.004,
            now=now,
        )
        for strike, right, mark, delta in (
            (7490, "C", 14.0, 0.9),
            (7490, "P", 2.0, -0.1),
            (7495, "C", 9.0, 0.75),
            (7495, "P", 2.1, -0.25),
            (7498, "C", 4.0, 0.6),
            (7498, "P", 3.9, -0.4),
            (7500, "C", 5.4, 0.52),
            (7500, "P", 3.3, -0.48),
            (7505, "C", 2.2, 0.35),
            (7505, "P", 5.3, -0.65),
            (7510, "C", 1.1, 0.2),
            (7510, "P", 9.2, -0.8),
        )
    ]
    pairs = pair_by_strike(quotes)

    implied = chain_implied_spot(pairs)

    assert implied == pytest.approx(7501.9)
    assert implied == pytest.approx(parity_forward(pairs, discount_factor=1.0))


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
    underlier = Quote(
        instrument=InstrumentId.index("SPX"),
        provider=Provider.IBKR,
        provider_symbol="index:SPX",
        received_at=now,
        quality=MarketDataQuality.LIVE,
        mark=7569.0,
        quote_time=now,
    )
    state = make_state(underlier, quote_no_greeks, now=now)
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
    assert "【条件交易地图 2026-07-07】" in text
    assert "put wall 7500 反弹买 call" in text
    assert "触达概率≈24%" in text
    assert "触发后限价参考 12.30 / 10.40; 当前不可预挂" in text
    assert "flip zone 7530 跌破买 put" in text
    assert "call wall 7550 冲墙买 put" in text


def test_within_send_window_beijing_weekday() -> None:
    beijing_1400 = datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc).astimezone(SHANGHAI_TZ)
    assert within_send_window(beijing_1400.astimezone(timezone.utc)) is True

    beijing_1200 = datetime(2026, 7, 7, 4, 0, tzinfo=timezone.utc)
    assert within_send_window(beijing_1200) is False

    saturday_1400 = datetime(2026, 7, 11, 6, 0, tzinfo=timezone.utc)
    assert within_send_window(saturday_1400) is False

    holiday_1400 = datetime(2026, 7, 3, 6, 0, tzinfo=timezone.utc)
    assert within_send_window(holiday_1400) is False


def test_mark_sent_merges_kinds_without_clobbering(tmp_path: Path) -> None:
    state_path = str(tmp_path / "state.json")
    map_time = datetime(2026, 7, 8, 6, 0, tzinfo=timezone.utc)
    status_time = datetime(2026, 7, 8, 6, 30, tzinfo=timezone.utc)
    mark_sent(
        state_path,
        "2026-07-08",
        fingerprint={"put_wall": 7500},
        now=map_time,
        kind="map",
    )
    mark_sent(
        state_path,
        "2026-07-08",
        fingerprint={"status_phase": "rth_open"},
        now=status_time,
        kind="status",
    )
    state = json.loads(Path(state_path).read_text(encoding="utf-8"))
    # Status push must not reset the map cadence timestamp.
    assert state["last_map_at"] == map_time.timestamp()
    assert state["last_status_at"] == status_time.timestamp()
    assert state["map_fingerprint"] == {"put_wall": 7500}
    assert state["status_fingerprint"] == {"status_phase": "rth_open"}
    assert state["fingerprint"] == {"put_wall": 7500}
    assert state["last_sent_at"] == status_time.timestamp()
    assert already_sent(state_path, "2026-07-08") is True

    next_map_time = datetime(2026, 7, 8, 7, 0, tzinfo=timezone.utc)
    mark_sent(
        state_path,
        "2026-07-08",
        fingerprint={"put_wall": 7510},
        now=next_map_time,
        kind="map",
    )
    state = json.loads(Path(state_path).read_text(encoding="utf-8"))
    assert state["map_fingerprint"] == {"put_wall": 7510}
    assert state["status_fingerprint"] == {"status_phase": "rth_open"}
    assert state["fingerprint"] == {"put_wall": 7500}


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


def test_candidate_presentation_requires_one_directionally_supported_plan() -> None:
    from spx_spark.application.order_map.service import _apply_candidate_presentation

    payload = {
        "expiry": "20260714",
        "underlier": {"price": 7495.0, "source": "chain_implied"},
        "es_last": 7540.0,
        "vol_context": {},
        "trade_intent": {
            "schema_version": 3,
            "policy_version": "rth_trade_intent.v3+sha256:test",
            "valid_until": "2026-07-14T04:16:30+00:00",
            "coordinate": {
                "kind": "chain_implied_spx",
                "instrument_id": "synthetic:SPXW_PARITY",
                "observed_value": 7495.0,
                "target_value": 7500.0,
                "spx_observed_value": 7495.0,
                "basis_points": 45.0,
                "as_of": "2026-07-14T04:15:00+00:00",
            },
            "block_reasons": [],
            "status": "trade_ready",
            "intent_id": "intent-1",
            "context_id": "context-1",
            "contract_id": "option:SPX:SPXW:20260714:7500:P",
            "play": "level_breakout_put",
            "event_id": "event-1",
            "thesis": "breakout",
            "direction": "down",
            "decision_bid": 10.0,
            "decision_ask": 10.4,
            "decision_mid": 10.2,
            "entry_limit": 10.1,
            "invalidation_spx": 7503.0,
            "target_spx": 7475.0,
            "expires_at": "2026-07-14T04:16:30+00:00",
        },
        "level_decision": {
            "phase": "confirmed",
            "formal_signal": True,
            "actionable": False,
            "play": "level_breakout_put",
            "event_id": "event-1",
            "thesis": "breakout",
            "direction": "down",
        },
        "decision_context": {
            "trade_intent": {"status": "trade_ready"},
            "breakout_filter": {
                "event_id": "event-1",
                "direction": "down",
                "verdict": "supported",
                "actionable": True,
            },
            "regime_decision": {"mode": "trending", "direction": "down"},
        },
        "level_trigger_repricing": {
            "event_id": "event-1",
            "candidates": [
                {
                    "play": "level_breakout_put",
                    "level": 7500.0,
                    "strike": 7500,
                    "right": "P",
                    "contract_id": "option:SPX:SPXW:20260714:7500:P",
                    "execution_quote_status": "executable",
                }
            ],
        },
        "candidates": [
            {
                "play": "level_breakout_put",
                "level": 7500.0,
                "strike": 7500,
                "right": "P",
                "execution_quote_status": "executable",
            },
            {
                "play": "level_breakout_put",
                "level": 7495.0,
                "strike": 7495,
                "right": "P",
                "contract_id": "option:SPX:SPXW:20260714:7495:P",
                "execution_quote_status": "executable",
            },
            {
                "play": "put_wall_bounce_call",
                "level": 7500.0,
                "strike": 7500,
                "right": "C",
                "execution_quote_status": "executable",
            },
        ],
    }

    evaluation_now = datetime(2026, 7, 14, 4, 15, tzinfo=timezone.utc)
    _apply_candidate_presentation(payload, now=evaluation_now)

    assert [item["play"] for item in payload["plan_candidates"]] == ["level_breakout_put"]
    assert payload["plan_candidates"][0]["decision_executable"] is True
    assert payload["observation_candidates"] == []
    assert payload["opposing_invalidation"]["play"] == "put_wall_bounce_call"
    presented = payload["plan_candidates"] + payload["observation_candidates"]
    assert (
        sum(item.get("contract_id") == payload["trade_intent"]["contract_id"] for item in presented)
        == 1
    )
    rendered = render_status_template(
        payload,
        [],
        datetime(2026, 7, 14, 4, 15, tzinfo=timezone.utc),
    )
    assert "【条件计划】决策门控已通过" in rendered
    assert "SPXW 7500P" in rendered
    assert "实时 10/10.4" in rendered
    assert "入场≤10.1" in rendered
    assert "SPXW 7500C" not in rendered
    from spx_spark.application.order_map.prompts import actionable_writer_output_valid

    assert actionable_writer_output_valid(rendered, rendered)
    assert not actionable_writer_output_valid(
        rendered.replace("入场≤10.1", "参考10.1"),
        rendered,
    )

    payload["trade_intent"]["status"] = "blocked"
    _apply_candidate_presentation(payload, now=evaluation_now)
    assert payload["plan_candidates"] == []
    assert len(payload["observation_candidates"]) == 1
    assert payload["observation_candidates"][0]["right"] == "P"
    assert payload["candidate_presentation"]["reason"] == "trade_intent_blocked"

    payload["trade_intent"]["status"] = "trade_ready"
    payload["trade_intent"]["expires_at"] = evaluation_now.isoformat()
    payload["trade_intent"]["valid_until"] = evaluation_now.isoformat()
    _apply_candidate_presentation(payload, now=evaluation_now)
    assert payload["plan_candidates"] == []
    assert payload["candidate_presentation"]["reason"] == "trade_intent_expired"

    payload["trade_intent"]["expires_at"] = "2026-07-14T04:16:30+00:00"
    payload["trade_intent"]["valid_until"] = "2026-07-14T04:16:30+00:00"
    payload["level_trigger_repricing"]["event_id"] = "event-2"
    _apply_candidate_presentation(payload, now=evaluation_now)
    assert payload["plan_candidates"] == []
    assert len(payload["observation_candidates"]) == 1
    assert payload["candidate_presentation"]["reason"] == "unique_trade_ready_candidate_unavailable"

    payload["level_trigger_repricing"]["event_id"] = "event-1"
    payload["level_trigger_repricing"]["candidates"][0]["contract_id"] = (
        "option:SPX:SPXW:20260714:7495:P"
    )
    _apply_candidate_presentation(payload, now=evaluation_now)
    assert payload["plan_candidates"] == []
    assert payload["candidate_presentation"]["reason"] == "unique_trade_ready_candidate_unavailable"


def test_candidate_primary_direction_ignores_expired_or_legacy_gth_signal() -> None:
    from spx_spark.application.order_map.candidate_presentation import _primary_direction

    now = datetime(2026, 7, 14, 4, 15, tzinfo=timezone.utc)
    signal = {
        "schema_version": 3,
        "policy_version": "gth_dip_reclaim.v3+sha256:test",
        "valid_until": (now + timedelta(seconds=30)).isoformat(),
        "coordinate": {
            "kind": "raw_es",
            "instrument_id": "future:ES",
            "observed_value": 7550.0,
            "target_value": 7548.0,
            "basis_points": 0.0,
            "as_of": now.isoformat(),
        },
        "block_reasons": [],
        "kind": "gth_dip_reclaim_call",
    }

    assert _primary_direction({"gth_dip_reclaim_signal": signal}, now=now) == (
        "up",
        "gth_dip_reclaim",
    )
    assert _primary_direction(
        {"gth_dip_reclaim_signal": {**signal, "valid_until": now.isoformat()}},
        now=now,
    ) == ("", "nearest_level_no_direction_guess")
    assert _primary_direction(
        {"gth_dip_reclaim_signal": {"kind": "gth_dip_reclaim_call"}},
        now=now,
    ) == ("", "nearest_level_no_direction_guess")

def test_status_fingerprint_tracks_trade_intent_identity() -> None:
    from spx_spark.application.order_map.service import (
        _status_fingerprint,
        _status_material_changes,
    )

    payload = {
        "expiry": "20260714",
        "expected_move_points": 40.0,
        "flip_zone": [7500.0, 7505.0],
        "candidates": [
            {"play": "put_wall_bounce_call", "level": 7490.0},
            {"play": "call_wall_fade_put", "level": 7520.0},
        ],
        "plan_candidates": [
            {
                "intent_id": "intent-1",
                "play": "level_breakout_put",
                "level": 7500.0,
                "strike": 7500,
                "right": "P",
            }
        ],
        "session_phase": {"name": "europe_session"},
    }
    baseline = _status_fingerprint(payload)
    assert _status_material_changes(baseline, dict(baseline)) == []

    payload["plan_candidates"][0].update(intent_id="intent-2", strike=7495)
    changed = _status_fingerprint(payload)
    assert changed["decision_thesis"] == baseline["decision_thesis"]
    assert _status_material_changes(baseline, changed) == ["执行意图更新 7500P→7495P"]


def test_status_candidate_presentation_labels_unapproved_sides_as_observation() -> None:
    payload = {
        "expiry": "20260714",
        "underlier": {"price": 7510.0, "source": "chain_implied"},
        "es_last": 7554.0,
        "vol_context": {},
        "plan_candidates": [],
        "observation_candidates": [
            {
                "play": "put_wall_bounce_call",
                "level": 7500.0,
                "strike": 7500,
                "right": "C",
                "prob_touch": 0.5,
                "projection_range_low": 10.0,
                "projection_range_high": 12.0,
                "execution_quote_status": "executable",
            },
            {
                "play": "flip_breakdown_put",
                "level": 7515.0,
                "strike": 7515,
                "right": "P",
                "prob_touch": 0.6,
                "projection_range_low": 11.0,
                "projection_range_high": 13.0,
                "execution_quote_status": "executable",
            },
        ],
        "candidate_presentation": {
            "mode": "observation_only",
            "reason": "decision_not_actionable",
        },
        "warnings": [],
    }

    text = render_status_template(
        payload,
        [],
        datetime(2026, 7, 14, 2, 0, tzinfo=timezone.utc),
    )
    assert "【观察情景】尚未通过决策门控" in text
    assert "观察1" in text and "观察2" in text
    assert "【条件计划】" not in text
    assert "当前不可预挂" not in text

    detail = render_template(payload)
    assert "[观察情景]" in detail
    assert "当前未通过决策门控，不生成下单计划" in detail
    assert "条件执行:" not in detail


def test_status_delivery_gate_suppresses_unchanged_scheduled_report(
    monkeypatch, tmp_path, capsys
) -> None:
    import spx_spark.application.order_map.service as order_map_module

    now = datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc)
    payload = {
        "research_only": True,
        "research_reference": {"price": 7520.0, "source": "hl_perp"},
        "session_phase": {"name": "europe_session", "name_cn": "欧盘时段"},
        "regime_decision": {"mode": "trending", "direction": "down"},
        "plan_candidates": [],
        "observation_candidates": [],
        "candidates": [],
    }
    fingerprint = order_map_module._status_fingerprint(payload)
    monkeypatch.setattr(
        order_map_module,
        "load_order_map_state",
        lambda path: {
            "last_status_date": "2026-07-07",
            "last_status_at": now.timestamp(),
            "fingerprint": fingerprint,
        },
    )
    monkeypatch.setattr(
        order_map_module,
        "build_order_payload_with_retry",
        lambda *args, **kwargs: payload,
    )
    monkeypatch.setattr(order_map_module, "_has_open_position_risk", lambda settings: False)
    monkeypatch.setattr(
        order_map_module,
        "generate_push_text",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("writer called")),
    )

    result = order_map_module.run_status(
        SimpleNamespace(force=False, dry_run=False),
        now=now,
        state_path=str(tmp_path / "state.json"),
        trading_date="2026-07-07",
    )

    assert result == 0
    assert json.loads(capsys.readouterr().out)["reason"] == "no_material_changes"


def test_map_refresh_suppresses_unchanged_fingerprint(monkeypatch, tmp_path, capsys) -> None:
    import spx_spark.application.order_map.service as order_map_module

    now = datetime(2026, 7, 14, 6, 0, tzinfo=timezone.utc)
    payload = {
        "research_only": False,
        "expiry": "20260714",
        "expected_move_points": 40.0,
        "flip_zone": [7500.0, 7505.0],
        "underlier": {"price": 7510.0, "source": "index:SPX"},
        "candidates": [
            {"play": "put_wall_bounce_call", "level": 7490.0},
            {"play": "call_wall_fade_put", "level": 7520.0},
        ],
        "plan_candidates": [],
        "observation_candidates": [],
        "session_phase": {"name": "europe_session"},
    }
    fingerprint = order_map_module._status_fingerprint(payload)
    monkeypatch.setattr(
        order_map_module,
        "load_order_map_state",
        lambda path: {
            "last_map_date": "2026-07-14",
            "last_map_at": now.timestamp() - 2_000,
            "map_fingerprint": fingerprint,
            "status_fingerprint": {"status_phase": "different"},
        },
    )
    monkeypatch.setattr(
        order_map_module,
        "build_order_payload_with_retry",
        lambda *args, **kwargs: payload,
    )
    monkeypatch.setattr(
        order_map_module,
        "send_order_map",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("map sent")),
    )

    result = order_map_module.run_refresh(
        SimpleNamespace(force=False, dry_run=False),
        now=now,
        state_path=str(tmp_path / "state.json"),
        trading_date="2026-07-14",
    )

    assert result == 0
    assert json.loads(capsys.readouterr().out)["reason"] == "no_material_changes"


def test_status_delivery_gate_allows_material_and_one_shot_key_windows() -> None:
    from spx_spark.application.order_map.service import _status_delivery_reason

    previous = {
        "last_status_date": "2026-07-14",
        "fingerprint": {
            "status_phase": "europe_session",
            "trade_intent_id": "intent-1",
        },
    }
    current = {"status_phase": "europe_session", "trade_intent_id": "intent-1"}
    assert (
        _status_delivery_reason(
            previous,
            current,
            ["决策剧本 bullish→bearish"],
            now=datetime(2026, 7, 14, 6, 0, tzinfo=timezone.utc),
            trading_date="2026-07-14",
            position_risk=False,
        )
        == "material_changes"
    )
    current["status_phase"] = "us_open_hour"
    assert (
        _status_delivery_reason(
            previous,
            current,
            [],
            now=datetime(2026, 7, 14, 6, 0, tzinfo=timezone.utc),
            trading_date="2026-07-14",
            position_risk=False,
        )
        == "key_window:us_open_hour"
    )
    previous["fingerprint"]["status_phase"] = "us_open_hour"
    assert (
        _status_delivery_reason(
            previous,
            current,
            [],
            now=datetime(2026, 7, 14, 6, 0, tzinfo=timezone.utc),
            trading_date="2026-07-14",
            position_risk=False,
        )
        is None
    )


def test_status_delivery_gate_sends_quarter_hour_gth_heartbeat_without_trade_intent() -> None:
    from spx_spark.application.order_map.service import _status_delivery_reason

    now = datetime(2026, 7, 15, 4, 14, tzinfo=timezone.utc)
    fingerprint = {"status_phase": "asia_globex", "trade_intent_id": ""}
    recent = {
        "last_status_date": "2026-07-15",
        "last_status_at": now.timestamp() - 13 * 60,
        "status_fingerprint": fingerprint,
    }
    due = {**recent, "last_status_at": now.timestamp() - 15 * 60}

    assert (
        _status_delivery_reason(
            recent,
            fingerprint,
            ["决策剧本 过渡偏多→过渡偏空"],
            now=now,
            trading_date="2026-07-15",
            position_risk=False,
        )
        is None
    )
    assert (
        _status_delivery_reason(
            due,
            fingerprint,
            [],
            now=now,
            trading_date="2026-07-15",
            position_risk=False,
        )
        == "gth_quarter_hour_heartbeat:asia_globex"
    )


def test_within_refresh_window_beijing() -> None:
    # Refresh follows the status window: Beijing 08:15 -> next-day 01:30.
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
    # 07:30 Beijing: before SPX GTH starts.
    beijing_0730 = datetime(2026, 7, 6, 23, 30, tzinfo=timezone.utc)
    assert within_refresh_window(beijing_0730) is False
    # 08:15 Beijing: SPX GTH start.
    beijing_0815 = datetime(2026, 7, 7, 0, 15, tzinfo=timezone.utc)
    assert within_refresh_window(beijing_0815) is True
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
    # Beijing 00:45 = ET 12:45: midday confirmation, 15 minutes to bedtime.
    late = session_phase(datetime(2026, 7, 9, 16, 45, tzinfo=timezone.utc))
    assert late["name"] == "us_midday_confirmation"
    assert late["minutes_to_bedtime"] == 15
    # Beijing 02:30 = ET 14:30: user asleep, unattended afternoon.
    asleep = session_phase(datetime(2026, 7, 9, 18, 30, tzinfo=timezone.utc))
    assert asleep["name"] == "us_afternoon_unattended"
    assert asleep["user_awake"] is False
    assert asleep["minutes_to_bedtime"] is None


def test_session_phase_marks_weekday_holiday_cash_hours_closed() -> None:
    et = ZoneInfo("America/New_York")
    holiday_cash_hours = session_phase(datetime(2026, 7, 3, 10, 0, tzinfo=et))
    assert holiday_cash_hours["name"] == "market_closed"
    assert holiday_cash_hours["name_cn"] == "休市"
    assert holiday_cash_hours["minutes_since_us_open"] is None
    assert holiday_cash_hours["minutes_to_us_close"] is None

    # A holiday outside normal cash hours retains the ordinary phase clock.
    holiday_off_hours = session_phase(datetime(2026, 7, 3, 7, 0, tzinfo=et))
    assert holiday_off_hours["name"] == "europe_session"

    weekend_cash_hours = session_phase(datetime(2026, 7, 11, 10, 0, tzinfo=et))
    assert weekend_cash_hours["name"] == "market_closed"

    # Early-close sessions retain their existing post-close transition.
    early_close_afternoon = session_phase(datetime(2026, 11, 27, 14, 0, tzinfo=et))
    assert early_close_afternoon["name"] == "post_close"


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
        "max_pain": {
            "settlement_strike": 7510.0,
            "call_oi_peak_strike": 7550.0,
            "call_oi_peak": 6555,
            "put_oi_peak_strike": 7500.0,
            "put_oi_peak": 3604,
            "oi_strike_count": 61,
            "quality": "ok",
        },
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
    # Beijing 00:30 = ET 12:30 midday confirmation window.
    late = datetime(2026, 7, 9, 16, 30, tzinfo=timezone.utc)
    text_late = render_status_template(payload, [], late)
    assert "午盘趋势确认窗" in text_late
    assert "距收官 30 分钟" in text_late


def test_within_status_window_and_minutes_to_open() -> None:
    # 14:30 Beijing = 2:30 ET: inside status window, 420 minutes to open.
    beijing_1430 = datetime(2026, 7, 7, 6, 30, tzinfo=timezone.utc)
    assert within_status_window(beijing_1430) is True
    assert minutes_to_open(beijing_1430) == 420
    # 14:00 Beijing: inside the GTH-to-RTH window.
    beijing_1400 = datetime(2026, 7, 7, 6, 0, tzinfo=timezone.utc)
    assert within_status_window(beijing_1400) is True
    # 07:30 Beijing: before SPX GTH starts.
    beijing_0730 = datetime(2026, 7, 6, 23, 30, tzinfo=timezone.utc)
    assert within_status_window(beijing_0730) is False
    # 08:15 Beijing: SPX GTH starts.
    beijing_0815 = datetime(2026, 7, 7, 0, 15, tzinfo=timezone.utc)
    assert within_status_window(beijing_0815) is True
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
    holiday_morning = datetime(2026, 7, 2, 23, 30, tzinfo=timezone.utc)
    assert within_status_window(holiday_morning) is False


def test_minutes_to_open_after_close_targets_next_trading_session() -> None:
    et = ZoneInfo("America/New_York")
    assert minutes_to_open(datetime(2026, 7, 10, 8, 30, tzinfo=et)) == 60
    assert minutes_to_open(datetime(2026, 7, 10, 10, 0, tzinfo=et)) is None
    # Friday 16:30 ET to Monday 09:30 ET is 65 hours.
    assert minutes_to_open(datetime(2026, 7, 10, 16, 30, tzinfo=et)) == 65 * 60


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
        "max_pain": {
            "settlement_strike": 7510.0,
            "call_oi_peak_strike": 7550.0,
            "call_oi_peak": 6555,
            "put_oi_peak_strike": 7500.0,
            "put_oi_peak": 3604,
            "oi_strike_count": 61,
            "quality": "ok",
        },
        "vol_context": {"vix": 15.9, "vix1d": 7.1, "vvix": 95.0, "skew": 150.0},
        "candidates": [
            {
                "play": "put_wall_bounce_call",
                "level": 7500.0,
                "level_label": "put wall 7500",
                "prob_touch": 0.57,
            },
        ],
        "spxw_0dte_greeks_reference": {
            "status": "ok",
            "aggregate": {
                "gross_gamma_abs": 1234.0,
                "gross_charm_5m_abs": 56.0,
                "gross_vanna_1vol_abs": 7.0,
            },
            "coverage": {
                "usable_contract_count": 8,
                "exact_expiry_contract_count": 10,
            },
        },
        "warnings": [],
    }
    now = datetime(2026, 7, 7, 6, 30, tzinfo=timezone.utc)
    text = render_status_template(payload, ["put wall 7495→7500"], now)
    assert "【SPX 15m · 14:30 · 0DTE 07-07 · 欧盘时段】" in text
    assert "距开盘 420 分钟" in text
    assert "put wall 7500 触达≈57%" in text
    assert "VIX1D/VIX 0.45" in text
    assert "Max Pain 7510" in text
    assert "Call峰 7550（6,555）" in text
    assert "Put峰 7500（3,604）" in text
    assert "0DTE Greeks" not in text
    assert "墙阶梯" not in text
    assert "收盘分布" not in text
    assert "变化  put wall 7495→7500" in text
    assert "｜" not in text

    text_no_change = render_status_template(payload, [], now)
    assert "关键位无实质变化" in text_no_change


def test_render_status_keeps_stable_and_candidate_structures_explicit() -> None:
    payload = {
        "expiry": "20260714",
        "underlier": {"price": 7521.8, "source": "chain_implied"},
        "es_last": 7565.0,
        "gamma_state": "zero_gamma_transition",
        "flip_zone": [7520.0, 7525.0],
        "vol_context": {},
        "candidates": [
            {"play": "put_wall_bounce_call", "level": 7500.0},
            {"play": "call_wall_fade_put", "level": 7550.0},
        ],
        "level_decision": {
            "phase": "invalidated",
            "level_kind": "flip_high",
            "level": 7515.0,
            "spot": 7521.8,
            "quality_ok": False,
            "quality_reason": "structure_change_pending",
            "structure_change_pending": True,
            "structure_candidate": {
                "confirmation_count": 2,
                "required_confirmations": 3,
                "levels": {
                    "put_wall": 7500.0,
                    "flip_low": 7520.0,
                    "flip_high": 7525.0,
                    "call_wall": 7550.0,
                },
            },
            "levels": {
                "put_wall": 7500.0,
                "flip_low": 7510.0,
                "flip_high": 7515.0,
                "call_wall": 7550.0,
            },
        },
        "warnings": [],
    }

    text = render_status_template(
        payload,
        ["flip zone 下界 7510→7520", "flip zone 上界 7515→7525"],
        datetime(2026, 7, 14, 5, 20, tzinfo=timezone.utc),
    )

    assert "结构  ZeroGamma过渡　Put 7500　Flip 7510–7515　Call 7550" in text
    assert "结构更新  新链 Put 7500　Flip 7520–7525　Call 7550　稳定确认 2/3，旧结构暂停" in text
    assert "动作  暂停新开仓：当前 OI/GEX 结构正在切换确认" in text
    assert "状态  INVALIDATED（已失效）　事件位 Flip上沿 7515" in text


def test_render_status_template_limits_candidates_to_nearest_two() -> None:
    payload = {
        "expiry": "20260713",
        "underlier": {"price": 7562.6, "source": "chain_implied"},
        "es_last": 7607.5,
        "gamma_state": "zero_gamma_transition",
        "flip_zone": [7550.0, 7555.0],
        "vol_context": {},
        "candidates": [
            {
                "play": "put_wall_bounce_call",
                "level": 7550.0,
                "strike": 7550.0,
                "right": "C",
                "prob_touch": 0.71,
                "projection_range_low": 9.8,
                "projection_range_high": 11.5,
                "execution_quote_status": "executable",
            },
            {
                "play": "call_wall_fade_put",
                "level": 7575.0,
                "strike": 7575.0,
                "right": "P",
                "prob_touch": 0.61,
                "projection_range_low": 7.4,
                "projection_range_high": 8.7,
                "execution_quote_status": "executable",
            },
            {
                "play": "flip_reclaim_call",
                "level": 7525.0,
                "strike": 7525.0,
                "right": "C",
                "prob_touch": 0.31,
                "projected_mid": 5.7,
                "execution_quote_status": "executable",
            },
        ],
        "warnings": [],
    }

    text = render_status_template(
        payload,
        [],
        datetime(2026, 7, 13, 14, 0, tzinfo=timezone.utc),
    )

    assert text.count("计划") == 3  # section header plus two candidates
    assert "SPXW 7550C" in text
    assert "SPXW 7575P" in text
    assert "SPXW 7525C" not in text
    assert text.count("当前不可预挂") == 1
    assert "【条件计划】标的触发后执行" in text
    assert "计划1 · 支撑反弹" in text
    assert "计划2 · 冲墙回落" in text

    from spx_spark.application.order_map.prompts import (
        render_feishu_status_detail_template,
    )

    detail = render_feishu_status_detail_template(
        payload,
        [],
        datetime(2026, 7, 13, 14, 0, tzinfo=timezone.utc),
    )
    assert "## 市场概览" in detail
    assert "## ES 与跨资产确认" not in detail  # unavailable inputs are not invented
    assert "## 条件计划与 BS 审计" in detail
    assert "### 计划 1" in detail
    assert "**条件执行**" in detail


def test_market_feature_report_uses_consistent_quality_and_volume_source() -> None:
    from spx_spark.application.order_map.render import _market_feature_lines

    payload = {
        "minute_market_frame": {
            "es": {},
            "volume": {
                "volume_delta_5m": 9676,
                "price_volume_alignment_5m": "unavailable",
                "recent_volume_provider": "ibkr",
            },
            "cross_asset": {
                "es_spy_direction_confirmation_15m": "confirmed",
            },
        },
        "option_structure_frame": {
            "structure": {},
            "volatility": {},
            "l1": {
                "quality": "unavailable",
                "contract_count": 0,
                "metrics": {"liquidity_score": None},
            },
        },
    }

    lines = _market_feature_lines(payload)

    assert "窗口不足" in lines[1]
    assert "源 ibkr" in lines[1]
    assert "L1流动性 不可用" in lines[2]


def test_compact_option_line_identifies_selected_quote_provider() -> None:
    from spx_spark.application.order_map.prompts import _compact_option_line

    line = _compact_option_line(
        {
            "vol_context": {"vix1d": 14.0, "vix": 16.0, "skew": 145.7},
            "option_structure_frame": {
                "l1": {
                    "metrics": {"liquidity_score": 70.6},
                    "diagnostics": {"selected_provider_counts": {"ibkr": 52}},
                }
            },
        }
    )

    assert line is not None
    assert "L1流动性 70.6（IBKR 52）" in line


def test_recent_market_frame_es_fills_short_quote_rotation_gap() -> None:
    from spx_spark.application.order_map.service import _recent_market_frame_es

    now = datetime(2026, 7, 13, 15, 6, tzinfo=timezone.utc)
    frame = {
        "as_of": (now - timedelta(seconds=61)).isoformat(),
        "quality": "ready",
        "es": {"price": 7594.25, "provider": "ibkr"},
    }

    assert _recent_market_frame_es(frame, now=now, max_age_seconds=120) == (
        7594.25,
        "ibkr",
    )
    assert _recent_market_frame_es(frame, now=now + timedelta(minutes=3), max_age_seconds=120) == (
        None,
        None,
    )


def test_gth_em_usage_uses_session_open_instead_of_prior_close() -> None:
    from spx_spark.application.order_map.service import _apply_gth_em_usage

    payload = {
        "trading_date": "2026-07-14",
        "es_last": 7555.0,
        "expected_move_points": 50.0,
        "day_move": {
            "prior_close": 7575.0,
            "points": -63.0,
            "em_used_fraction": None,
        },
    }
    frame = {
        "session_id": "2026-07-14",
        "es": {"price": 7554.0, "gth_open_price": 7550.0},
    }

    _apply_gth_em_usage(payload, frame)

    assert payload["day_move"]["points"] == -63.0
    assert payload["day_move"]["em_move_points"] == 5.0
    assert payload["day_move"]["em_used_fraction"] == 0.10
    assert payload["day_move"]["em_session_id"] == "2026-07-14"

    payload["trading_date"] = "2026-07-15"
    payload["day_move"]["em_used_fraction"] = None
    _apply_gth_em_usage(payload, frame)
    assert payload["day_move"]["em_used_fraction"] is None


def test_send_order_map_queues_on_feishu_failure(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("SPX_PUSH_LLM_ENABLED", "false")
    monkeypatch.setattr(
        "spx_spark.notifier.sinks.post_feishu",
        lambda url, payload, timeout: {"code": 19001, "msg": "fail"},
    )
    payload = build_order_payload(
        make_state(
            Quote(
                instrument=InstrumentId.index("SPX"),
                provider=Provider.IBKR,
                provider_symbol="index:SPX",
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
    assert payload["spxw_0dte_greeks_reference"]["mode"] == "reference_only"
    assert payload["spxw_0dte_greeks_reference"]["contracts"] == []
    assert "warnings" in payload


def test_order_payload_trading_date_uses_research_rollover() -> None:
    monday_evening = datetime(2026, 7, 6, 23, 30, tzinfo=timezone.utc)
    friday_rth = datetime(2026, 7, 10, 17, 30, tzinfo=timezone.utc)

    evening_payload = build_order_payload(make_state(now=monday_evening), now=monday_evening)
    friday_payload = build_order_payload(make_state(now=friday_rth), now=friday_rth)

    assert evening_payload["trading_date"] == "2026-07-07"
    assert friday_payload["trading_date"] == "2026-07-10"


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
    status_text = render_status_template(
        payload, [], datetime(2026, 7, 8, 14, 0, tzinfo=timezone.utc)
    )
    assert "ES 量价" in status_text
    # Signals without a computable window stay silent instead of rendering "-".
    payload["es_volume"] = {
        "cumulative": 1_000_000,
        "label": "no_baseline",
        "delta": None,
        "window_minutes": None,
        "pace_ratio": None,
    }
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


def test_compact_price_line_labels_chain_implied_source() -> None:
    from spx_spark.application.order_map.prompts import _compact_price_line

    line = _compact_price_line(
        {
            "underlier": {"price": 7474.0, "source": "chain_implied"},
            "es_last": 7513.25,
            "day_move": {"points": -59.8, "em_used_fraction": 0.99},
        }
    )

    assert "SPX 7474(期权隐含)" in line
    assert "ES 7513.2" in line


def test_compact_price_line_leaves_cash_index_untagged() -> None:
    from spx_spark.application.order_map.prompts import _compact_price_line

    line = _compact_price_line(
        {
            "underlier": {"price": 7550.0, "source": "index:SPX"},
            "es_last": 7595.0,
            "day_move": {"points": 12.5, "em_used_fraction": 0.4},
        }
    )

    assert "SPX 7550　" in line
    assert "期权隐含" not in line
