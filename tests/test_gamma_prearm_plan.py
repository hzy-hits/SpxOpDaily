from __future__ import annotations

import inspect
from datetime import datetime, timedelta, timezone

import spx_spark.application.market_features.service as market_feature_service
from spx_spark.application.market_features.gamma_prearm_plan import (
    _notification_intent,
    evaluate_gamma_prearm_plan,
)
from spx_spark.application.order_map.level_trigger_repricing import REPRICING_PHASES


NOW = datetime(2026, 7, 30, 12, 0, tzinfo=timezone.utc)


def test_market_feature_runtime_processes_gamma_prearm_before_ready_candidate() -> None:
    source = inspect.getsource(market_feature_service.run)

    assert source.index("process_gamma_prearm_plan(") < source.index(
        "process_gth_level_manual_candidate("
    )
    assert source.count("spring_gamma=spring_gamma_snapshot") == 2


def test_approaching_gamma_level_builds_two_sided_prearm_plan() -> None:
    plan = evaluate_gamma_prearm_plan(
        _repricing(),
        _level_decision(),
        now=NOW,
        spring_gamma=_spring_gamma(),
        prior_session=_prior_session(),
        gth_position_fraction=0.05,
    )

    assert "approaching" in REPRICING_PHASES
    assert plan["status"] == "prearm_ready"
    assert plan["execution_eligible"] is False
    assert plan["automatic_ordering"] is False
    assert plan["level_kind"] == "flip_low"
    assert plan["level"] == 7375.0
    assert plan["distance_points"] == 15.0
    assert [item["side"] for item in plan["paths"]] == ["CALL", "PUT"]
    assert plan["spring_gamma"]["preferred_side"] == "CALL"
    assert plan["paths"][1]["prior_session_chase_risk"] == "high"

    card = _notification_intent(plan, event_id="gamma-plan:ready", now=NOW)
    assert "🎯 GAMMA 伏击计划 · 先准备，未触发不下单" in card["text"]
    assert "Flip Low 7375.00" in card["text"]
    assert "下沿拒绝并收复：CALL" in card["text"]
    assert "向下接受并保持：PUT" in card["text"]
    assert "Spring Gamma 偏多（0.67）" in card["text"]
    assert "CALL 路径优先" in card["text"]
    assert "只作排序，不作门禁" in card["text"]
    assert "前日  -1.52%" in card["text"]
    assert "PUT 同向极值追单需等待墙位接受" in card["text"]
    assert "现在不追" in card["text"]
    assert "预埋计划不是方向信号" in card["text"]


def test_prearm_plan_identity_is_semantic_across_rearmed_events() -> None:
    first = evaluate_gamma_prearm_plan(_repricing(), _level_decision(), now=NOW)
    rearmed = evaluate_gamma_prearm_plan(
        _repricing(event_id="level:rearmed"),
        _level_decision(event_id="level:rearmed"),
        now=NOW,
    )

    assert first["plan_id"] == rearmed["plan_id"]
    assert first["source_event_id"] != rearmed["source_event_id"]


def test_prearm_plan_requires_fresh_approaching_repricing() -> None:
    stale = evaluate_gamma_prearm_plan(
        _repricing(as_of=NOW - timedelta(minutes=2)),
        _level_decision(),
        now=NOW,
    )
    testing = evaluate_gamma_prearm_plan(
        {**_repricing(), "phase": "testing"},
        {**_level_decision(), "phase": "testing"},
        now=NOW,
    )

    assert stale["status"] == "blocked"
    assert stale["block_reasons"] == ["approach_repricing_stale"]
    assert testing["status"] == "inactive"
    assert testing["block_reasons"] == ["level_not_approaching"]


def test_break_pending_emits_one_sided_human_conditional_card() -> None:
    level = {
        **_level_decision(),
        "phase": "break_pending",
        "thesis": "breakout",
        "direction": "down",
    }
    repricing = {
        **_repricing(),
        "phase": "break_pending",
        "path_geometries": {
            "level_breakout_put": {
                "target_spx": 7365.0,
                "feasible": True,
            }
        },
    }
    repricing["candidates"][0]["execution_bid"] = 12.1
    repricing["candidates"][0]["execution_ask"] = 12.3

    plan = evaluate_gamma_prearm_plan(repricing, level, now=NOW)
    card = _notification_intent(
        plan,
        event_id=f"{plan['plan_id']}:break_pending",
        now=NOW,
    )

    assert plan["notification_stage"] == "break_pending"
    assert [item["side"] for item in plan["paths"]] == ["PUT"]
    assert "🟡 条件准备卡 · 已发生突破/拒绝，等确认" in card["text"]
    assert "现价 12.10/12.30" in card["text"]
    assert "状态机 CONFIRMED 后才入场" in card["text"]
    assert "失效 SPX 收回 7378.00" in card["text"]
    assert "下一有效结构目标 7365.00" in card["text"]


def _level_decision(event_id: str = "level:flip-low-approach") -> dict[str, object]:
    return {
        "phase": "approaching",
        "event_id": event_id,
        "level_kind": "flip_low",
        "level": 7375.0,
    }


def _repricing(
    *,
    event_id: str = "level:flip-low-approach",
    as_of: datetime = NOW,
) -> dict[str, object]:
    return {
        "status": "repriced",
        "phase": "approaching",
        "event_id": event_id,
        "as_of": as_of.isoformat(),
        "expiry": "20260730",
        "level_kind": "flip_low",
        "spx_level": 7375.0,
        "pricing_spot": 7390.0,
        "trigger_coordinate": {
            "kind": "chain_implied_spx",
            "observed_value": 7390.0,
            "target_value": 7375.0,
        },
        "touch_time_estimate": {"base_minutes": 12.0},
        "candidates": [
            {
                "play": "level_breakout_put",
                "right": "P",
                "contract_id": "option:SPX:SPXW:20260730:7375:P",
                "execution_quote_status": "executable",
                "projection_range_low": 12.2,
                "projected_mid": 12.5,
                "projection_range_high": 12.8,
                "limit_conservative": 12.2,
                "limit_aggressive": 12.8,
                "frontrun_level": 7373.0,
                "frontrun_limit": 12.0,
                "touch_eta_minutes": 12.0,
                "execution_quote_provider": "ibkr",
            },
            {
                "play": "level_fade_call",
                "right": "C",
                "contract_id": "option:SPX:SPXW:20260730:7375:C",
                "execution_quote_status": "executable",
                "projection_range_low": 13.1,
                "projected_mid": 13.4,
                "projection_range_high": 13.7,
                "limit_conservative": 13.1,
                "limit_aggressive": 13.7,
                "frontrun_level": 7377.0,
                "frontrun_limit": 13.0,
                "touch_eta_minutes": 12.0,
                "execution_quote_provider": "ibkr",
            },
        ],
    }


def _spring_gamma() -> dict[str, object]:
    return {
        "status": "ready",
        "as_of": NOW.isoformat(),
        "expiry": "20260730",
        "actionable": False,
        "automatic_ordering": False,
        "action_authority": "none",
        "direction": {
            "decision": "up",
            "composite_score": 0.67,
        },
    }


def _prior_session() -> dict[str, object]:
    return {
        "status": "ready",
        "session_date": "2026-07-29",
        "return_fraction": -0.0152,
        "return_points": -112.63,
        "close_location_fraction": 0.02,
        "tail_return_fraction": -0.004,
        "shock_direction": "down",
        "close_zone": "lower",
        "path_class": "shock_down_close_low",
    }
