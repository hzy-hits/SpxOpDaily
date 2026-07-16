from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from spx_spark.application.market_features.models import (
    FrameQuality,
    L1MicrostructureFrame,
    MinuteMarketFrame,
    OptionStructureFrame,
)
from spx_spark.application.market_features.session_episode import advance_session_episode
from spx_spark.settings.market_features import MarketFeatureSettings


UTC = timezone.utc
START = datetime(2026, 7, 15, 13, 30, tzinfo=UTC)


def test_session_episode_retains_break_extreme_reclaim_and_recovery_path() -> None:
    policy = MarketFeatureSettings(session_reclaim_hold_seconds=60.0)
    state = advance_session_episode(
        None,
        session_id="2026-07-15",
        now=START,
        spot=7572.0,
        market=_market(START),
        options=_options(START, gamma_ratio=-0.1),
        policy=policy,
    )
    state = advance_session_episode(
        state,
        session_id="2026-07-15",
        now=START + timedelta(minutes=30),
        spot=7548.0,
        market=_market(START + timedelta(minutes=30), one=-2, five=-8, fifteen=-15),
        options=_options(START + timedelta(minutes=30), gamma_ratio=-0.3),
        policy=policy,
    )
    assert state["phase"] == "structure_break"
    assert state["break_direction"] == "down"
    assert state["break_level_kind"] == "flip_low"
    assert state["break_level"] == 7555.0

    state = advance_session_episode(
        state,
        session_id="2026-07-15",
        now=START + timedelta(minutes=45),
        spot=7538.0,
        market=_market(START + timedelta(minutes=45), one=-1, five=-5, fifteen=-12),
        options=_options(START + timedelta(minutes=45), gamma_ratio=-0.4),
        policy=policy,
    )
    assert state["phase"] == "extreme"
    assert state["extreme_spot"] == 7538.0

    reclaim_at = START + timedelta(minutes=60)
    state = advance_session_episode(
        state,
        session_id="2026-07-15",
        now=reclaim_at,
        spot=7562.0,
        market=_market(reclaim_at, one=2, five=6, fifteen=10),
        options=_options(reclaim_at, gamma_ratio=0.05),
        policy=policy,
    )
    assert state["phase"] == "reclaim_pending"

    confirmed_at = reclaim_at + timedelta(seconds=61)
    state = advance_session_episode(
        state,
        session_id="2026-07-15",
        now=confirmed_at,
        spot=7565.0,
        market=_market(confirmed_at, one=1, five=6, fifteen=8),
        options=_options(confirmed_at, gamma_ratio=0.19),
        policy=policy,
    )
    assert state["phase"] == "v_reversal_confirmed"
    assert state["reversal_direction"] == "up"
    assert state["reversal_evidence"]["net_gamma_ratio_proxy"] == 0.19

    state = advance_session_episode(
        state,
        session_id="2026-07-15",
        now=confirmed_at + timedelta(minutes=1),
        spot=7570.0,
        market=_market(confirmed_at + timedelta(minutes=1), one=1, five=5, fifteen=9),
        options=_options(confirmed_at + timedelta(minutes=1), gamma_ratio=0.2),
        policy=policy,
    )
    assert state["phase"] == "recovery"
    assert state["recovery_ratio"] == pytest.approx(32.0 / 34.0)
    assert [row["phase"] for row in state["transition_history"]] == [
        "structure_break",
        "extreme",
        "reclaim_pending",
        "v_reversal_confirmed",
        "recovery",
    ]


def test_session_episode_resets_on_new_research_session() -> None:
    policy = MarketFeatureSettings()
    prior = advance_session_episode(
        None,
        session_id="2026-07-15",
        now=START,
        spot=7572.0,
        market=_market(START),
        options=_options(START, gamma_ratio=0.0),
        policy=policy,
    )
    reset = advance_session_episode(
        prior,
        session_id="2026-07-16",
        now=START + timedelta(days=1),
        spot=7580.0,
        market=_market(START + timedelta(days=1)),
        options=_options(START + timedelta(days=1), gamma_ratio=0.0),
        policy=policy,
    )

    assert reset["session_id"] == "2026-07-16"
    assert reset["phase"] == "observing"
    assert reset["session_open"] == 7580.0
    assert reset["transition_history"] == []


def _market(
    at: datetime,
    *,
    one: float = 0.0,
    five: float = 0.0,
    fifteen: float = 0.0,
) -> MinuteMarketFrame:
    return MinuteMarketFrame(
        schema_version=1,
        frame_id=f"market:{at.isoformat()}",
        session_id="2026-07-15",
        as_of=at,
        quality=FrameQuality.READY,
        es={
            "return_1m_points": one,
            "return_5m_points": five,
            "return_15m_points": fifteen,
        },
        session_ranges={},
        volume={},
        cross_asset={"es_spy_direction_confirmation_15m": "confirmed"},
        volatility={},
        diagnostics={},
    )


def _options(at: datetime, *, gamma_ratio: float) -> OptionStructureFrame:
    return OptionStructureFrame(
        schema_version=1,
        frame_id=f"options:{at.isoformat()}",
        as_of=at,
        quality=FrameQuality.READY,
        front_expiry="20260715",
        next_expiry="20260716",
        structure={
            "put_wall": 7550.0,
            "flip_zone": [7555.0, 7560.0],
            "call_wall": 7600.0,
            "net_gamma_ratio": gamma_ratio,
        },
        volatility={
            "expected_move_points_0dte": 20.0,
            "atm_iv_change_5m": -0.01,
            "atm_iv_change_15m": -0.02,
        },
        concentration={},
        density={},
        l1=L1MicrostructureFrame(
            quality=FrameQuality.READY,
            expiry="20260715",
            contract_count=20,
            metrics={},
            diagnostics={},
        ),
        diagnostics={},
        exposure={
            "volume_weighted": {"net_gamma_ratio": gamma_ratio},
            "sign_convention": "calls_positive_puts_negative",
            "dealer_position_sign": "unknown",
        },
    )
