from __future__ import annotations

import inspect
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import spx_spark.application.market_features.service as market_feature_service
from spx_spark.application.market_features.gamma_prearm_plan import (
    _human_notification_eligible,
    _notification_intent,
    _notification_event_id,
    evaluate_gamma_prearm_plan,
    process_gamma_prearm_plan,
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
    assert card["title"] == "SPX 结构观察 · 等价格选边"
    assert "🟡 结构观察 · NO TRADE · 等价格选边" in card["text"]
    assert "Flip Low 7375.00" in card["text"]
    assert "Gamma职责  代理正 Gamma" in card["text"]
    assert "dealer sign unknown" in card["text"]
    assert "LONG条件  下沿拒绝并收复" in card["text"]
    assert "SHORT条件  向下接受并保持" in card["text"]
    assert "SPXW 7375" not in card["text"]
    assert "当前报价" not in card["text"]
    assert "Spring v3（ES）偏多（0.67）" in card["text"]
    assert "CALL 路径优先" in card["text"]
    assert "只作排序，不作门禁" in card["text"]
    assert "前日  -1.52%" in card["text"]
    assert "PUT 同向极值追单需等待墙位接受" in card["text"]
    assert "只观察结构，不展示合约" in card["text"]
    assert "结构观察不是交易信号" in card["text"]


def test_approaching_and_gth_plans_are_not_human_notifiable() -> None:
    rth = datetime(2026, 7, 30, 15, 30, tzinfo=timezone.utc)
    gth = datetime(2026, 8, 13, 3, 15, tzinfo=timezone.utc)
    approaching = evaluate_gamma_prearm_plan(_repricing(), _level_decision(), now=NOW)
    repricing, level = _break_pending_inputs(as_of=rth)
    pending = evaluate_gamma_prearm_plan(repricing, level, now=rth)

    assert approaching["notification_stage"] == "approaching"
    assert _human_notification_eligible(approaching, now=rth) is False
    assert _human_notification_eligible(approaching, now=gth) is False
    assert pending["status"] == "prearm_ready"
    assert pending["notification_stage"] == "break_pending"
    assert _human_notification_eligible(pending, now=rth) is True
    assert _human_notification_eligible(pending, now=gth) is False


def test_process_does_not_enqueue_approaching_observation_cards(
    tmp_path,
    monkeypatch,
) -> None:
    flushed: list[str | None] = []

    def fake_flush(*_args, **kwargs):
        flushed.append(kwargs.get("only_event_id"))
        return {"attempted": True, "accepted": True, "outcome": "delivered"}

    monkeypatch.setattr(
        "spx_spark.application.market_features.gamma_prearm_plan.flush_pending_notifications",
        fake_flush,
    )
    plan = evaluate_gamma_prearm_plan(_repricing(), _level_decision(), now=NOW)
    leftover_id = _notification_event_id(plan)
    (tmp_path / "latest").mkdir()
    (tmp_path / "latest" / "gamma_prearm_plan_state.json").write_text(
        json.dumps(
            {
                "pending_notifications": [
                    {"event_id": leftover_id, "lane": "gamma_prearm_plan"}
                ]
            }
        )
    )

    result = process_gamma_prearm_plan(
        SimpleNamespace(data_root=str(tmp_path)),
        _repricing(),
        _level_decision(),
        now=NOW,
        notification=SimpleNamespace(),
    )
    state = json.loads((tmp_path / "latest" / "gamma_prearm_plan_state.json").read_text())

    assert leftover_id
    assert leftover_id.endswith(":approaching")
    assert result["status"] == "prearm_ready"
    assert result["notification_attempted"] is False
    assert flushed == []
    assert state["pending_notifications"] == []


def test_process_enqueues_rth_break_pending_card(tmp_path, monkeypatch) -> None:
    flushed: list[str | None] = []

    def fake_flush(*_args, **kwargs):
        flushed.append(kwargs.get("only_event_id"))
        return {"attempted": True, "accepted": True, "outcome": "delivered"}

    monkeypatch.setattr(
        "spx_spark.application.market_features.gamma_prearm_plan.flush_pending_notifications",
        fake_flush,
    )
    rth = datetime(2026, 7, 30, 15, 30, tzinfo=timezone.utc)
    repricing, level = _break_pending_inputs(as_of=rth)
    result = process_gamma_prearm_plan(
        SimpleNamespace(data_root=str(tmp_path)),
        repricing,
        level,
        now=rth,
        notification=SimpleNamespace(),
    )

    assert result["status"] == "prearm_ready"
    assert result["notification_stage"] == "break_pending"
    assert result["notification_attempted"] is True
    assert flushed == [_notification_event_id(result)]


def test_prearm_plan_identity_is_semantic_across_rearmed_events() -> None:
    first = evaluate_gamma_prearm_plan(_repricing(), _level_decision(), now=NOW)
    rearmed = evaluate_gamma_prearm_plan(
        _repricing(event_id="level:rearmed"),
        _level_decision(event_id="level:rearmed"),
        now=NOW,
    )

    assert first["plan_id"] == rearmed["plan_id"]
    assert first["source_event_id"] != rearmed["source_event_id"]
    assert _notification_event_id(first) != _notification_event_id(rearmed)

    one_sided_repricing = _repricing()
    one_sided_repricing["candidates"] = one_sided_repricing["candidates"][:1]
    one_sided = evaluate_gamma_prearm_plan(
        one_sided_repricing,
        _level_decision(),
        now=NOW,
    )
    assert first["plan_id"] == one_sided["plan_id"]
    assert _notification_event_id(first) == _notification_event_id(one_sided)


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
    assert card["title"] == "SPX SHORT / PUT 候选 · 等最终确认"
    assert "🟠 SHORT / PUT 候选 · 价格条件已出现，尚未确认" in card["text"]
    assert "方向来源  SHORT / PUT 来自价格向下接受并保持" in card["text"]
    assert "当前报价 12.10/12.30" in card["text"]
    assert "状态机 CONFIRMED 后才入场" in card["text"]
    assert "失效 SPX 收回 7378.00" in card["text"]
    assert "下一有效结构目标 7365.00" in card["text"]


def test_pending_card_explicitly_blocks_a_quote_above_touch_reference() -> None:
    level = {
        **_level_decision(),
        "phase": "break_pending",
        "thesis": "breakout",
        "direction": "down",
    }
    repricing = {**_repricing(), "phase": "break_pending"}
    repricing["candidates"][0]["execution_bid"] = 13.8
    repricing["candidates"][0]["execution_ask"] = 14.0

    plan = evaluate_gamma_prearm_plan(repricing, level, now=NOW)
    card = _notification_intent(plan, event_id="gamma-plan:over-reference", now=NOW)

    assert "当前 ask 高于触位参考上限" in card["text"]
    assert "不得按现价追入" in card["text"]


def _break_pending_inputs(*, as_of: datetime) -> tuple[dict[str, object], dict[str, object]]:
    level = {
        **_level_decision(),
        "phase": "break_pending",
        "thesis": "breakout",
        "direction": "down",
    }
    repricing = {
        **_repricing(as_of=as_of),
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
    return repricing, level


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
        "gamma_context": {
            "state": "positive_gamma_pin",
            "zero_gamma": 7360.0,
            "net_gamma_ratio": 0.61,
            "weighting": "oi",
            "sign_method": "call_positive_put_negative_oi_proxy_not_dealer_position",
            "dealer_position_sign": "unknown",
            "direction": "unknown",
        },
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
