from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone

from spx_spark.application.order_map.prompts import (
    _status_writer_payload,
    render_status_template,
)
from spx_spark.application.order_map.put_candidate_presentation import (
    build_put_candidate_report,
    presentable_plan_candidates,
    put_candidate_report_lines,
)
from spx_spark.application.order_map.writer_validation import (
    actionable_writer_output_valid,
)


NOW = datetime(2026, 7, 24, 14, 15, tzinfo=timezone.utc)


def _payload() -> dict[str, object]:
    return {
        "expiry": "20260724",
        "session_phase": {"name": "us_open_hour", "name_cn": "美盘开盘首小时"},
        "underlier": {"price": 7560.0, "source": "index:SPX"},
        "es_last": 7605.0,
        "gamma_state": "zero_gamma_transition",
        "level_decision": {
            "event_id": "level:flip-low",
            "phase": "confirmed",
            "thesis": "breakout",
            "direction": "down",
            "level_kind": "flip_low",
            "formal_signal": True,
            "levels": {
                "put_wall": 7550.0,
                "flip_low": 7560.0,
                "flip_high": 7565.0,
                "call_wall": 7600.0,
            },
        },
        "trade_intent": {
            "event_id": "level:flip-low",
            "status": "blocked",
            "direction": "down",
            "play": "level_breakout_put",
            "block_reasons": ["pilot_scope_upside_breakout_only"],
        },
        "plan_candidates": [],
        "observation_candidates": [],
        "warnings": [],
        "spring_gamma_v3_shadow": {
            "rth_market_state": {
                "state": "TREND_DOWN",
                "D": -8,
                "Q": {"quality": "high"},
            }
        },
    }


def _candidate(report: dict[str, object], setup: str) -> dict[str, object]:
    return next(
        row for row in report["candidates"] if isinstance(row, dict) and row.get("setup") == setup
    )


def test_put_report_keeps_supported_hypotheses_separate_and_put_wall_disabled() -> None:
    payload = _payload()

    report = build_put_candidate_report(payload)
    lines = put_candidate_report_lines(payload)
    flip = _candidate(report, "flip_low_breakdown")
    rejection = _candidate(report, "call_wall_or_flip_high_rejection")
    put_wall = _candidate(report, "put_wall_breakdown")

    assert flip["wall_signal"]["status"] == "CONFIRMED"
    assert flip["execution_eligible"] == {
        "eligible": False,
        "status": "ineligible",
        "reason": "pilot_scope_upside_breakout_only",
        "source": "persisted_trade_intent_only",
    }
    assert flip["priority"]["status"] == "HIGH"
    assert rejection["wall_signal"]["status"] == "WATCH"
    assert rejection["level_kinds"] == ["call_wall", "flip_high"]
    assert put_wall["wall_signal"]["status"] == "WATCH"
    assert put_wall["execution_eligible"]["eligible"] is False
    assert put_wall["priority"]["status"] == "UNSUPPORTED"
    assert len(lines) == 3
    assert all("WALL_SIGNAL=" in line for line in lines)
    assert all("EXECUTION_ELIGIBLE=" in line for line in lines)
    assert all("PRIORITY=" in line for line in lines)
    assert "Put候选[flip_low_breakdown]" in lines[0]
    assert "Put候选[call_wall_or_flip_high_rejection]" in lines[1]
    assert "Put候选[put_wall_breakdown]" in lines[2]
    assert "WALL_SIGNAL=WATCH" in lines[2]
    assert "PRIORITY=UNSUPPORTED" in lines[2]


def test_extended_down_is_reported_as_no_chase_without_hiding_wall_signal() -> None:
    payload = _payload()
    payload["trade_intent"]["moving_average_context"] = {
        "regime_state": "TREND_EXTENDED",
        "regime_direction": "down",
    }

    flip = _candidate(build_put_candidate_report(payload), "flip_low_breakdown")

    assert flip["wall_signal"]["status"] == "CONFIRMED"
    assert flip["priority"] == {
        "status": "NO_CHASE",
        "reason": "ma_trend_extended_down",
        "authority": "soft_report_priority_only",
    }


def test_upper_rejection_can_reflect_shadow_ready_without_promoting_put_wall() -> None:
    payload = _payload()
    payload["level_decision"].update(
        {
            "event_id": "level:call-wall",
            "thesis": "fade",
            "level_kind": "call_wall",
        }
    )
    payload["trade_intent"] = {
        "event_id": "level:call-wall",
        "status": "shadow_ready",
        "direction": "down",
        "play": "level_fade_put",
        "execution_eligible": False,
        "quote_observation_eligible": True,
        "shadow_mode": True,
    }
    payload["regime_decision"] = {"mode": "mean_reverting", "direction": "none"}

    report = build_put_candidate_report(payload)
    rejection = _candidate(report, "call_wall_or_flip_high_rejection")
    put_wall = _candidate(report, "put_wall_breakdown")

    assert rejection["wall_signal"]["status"] == "CONFIRMED"
    assert rejection["execution_eligible"]["eligible"] is False
    assert rejection["execution_eligible"]["reason"] == "trade_intent_shadow_ready"
    assert rejection["priority"]["status"] == "HIGH"
    assert put_wall["execution_eligible"]["eligible"] is False
    assert put_wall["priority"]["status"] == "UNSUPPORTED"


def test_flip_low_manual_ready_is_visible_without_promoting_put_wall() -> None:
    payload = _payload()
    payload["trade_intent"] = {
        "event_id": "level:flip-low",
        "status": "trade_ready",
        "strategy_lane": "long_0dte_rth_flip_low_breakdown_put_manual",
        "direction": "down",
        "play": "level_breakout_put",
        "contract_id": "option:SPX:SPXW:20260724:7560:P",
        "execution_eligible": True,
        "quote_observation_eligible": False,
        "shadow_mode": False,
        "automatic_ordering": False,
    }
    payload["plan_candidates"] = [
        {
            "play": "level_breakout_put",
            "level_kind": "flip_low",
            "strike": 7560.0,
            "right": "P",
        }
    ]

    report = build_put_candidate_report(payload)
    flip = _candidate(report, "flip_low_breakdown")
    put_wall = _candidate(report, "put_wall_breakdown")

    assert flip["execution_eligible"] == {
        "eligible": True,
        "status": "eligible",
        "reason": "manual_ready_exact_quote",
        "source": "persisted_trade_intent_only",
    }
    assert presentable_plan_candidates(payload) == payload["plan_candidates"]
    assert put_wall["execution_eligible"]["eligible"] is False
    assert put_wall["priority"]["status"] == "UNSUPPORTED"


def test_status_formatter_suppresses_unsupported_put_wall_plan() -> None:
    payload = _payload()
    payload["level_decision"].update(
        {
            "event_id": "level:put-wall",
            "level_kind": "put_wall",
        }
    )
    payload["trade_intent"] = {
        "event_id": "level:put-wall",
        "status": "trade_ready",
        "direction": "down",
        "play": "level_breakout_put",
    }
    payload["plan_candidates"] = [
        {
            "play": "level_breakout_put",
            "level": 7550.0,
            "strike": 7550.0,
            "right": "P",
            "order_style": "live_nbbo_limit",
            "decision_bid": 10.0,
            "decision_ask": 10.2,
            "limit_aggressive": 10.1,
        }
    ]

    rendered = render_status_template(payload, [], NOW)
    writer = _status_writer_payload(payload)

    assert "Put候选[put_wall_breakdown]" in rendered
    assert "WALL_SIGNAL=CONFIRMED" in rendered
    assert "EXECUTION_ELIGIBLE=NO(disabled_unsupported)" in rendered
    assert "【条件计划】" not in rendered
    assert "SPXW 7550P" not in rendered
    assert "plan_candidates" not in writer
    assert writer["put_candidate_report"]["schema_version"] == "put_candidate_report.v1"
    put_wall = _candidate(writer["put_candidate_report"], "put_wall_breakdown")
    assert put_wall["priority"]["status"] == "UNSUPPORTED"


def test_status_formatter_suppresses_stale_put_plan_during_flip_low_signal() -> None:
    payload = _payload()
    payload["trade_intent"] = {
        "event_id": "level:flip-low",
        "status": "trade_ready",
        "strategy_lane": "long_0dte_rth_flip_low_breakdown_put_manual",
        "direction": "down",
        "play": "level_breakout_put",
        "contract_id": "option:SPX:SPXW:20260724:7560:P",
        "execution_eligible": True,
        "quote_observation_eligible": False,
        "shadow_mode": False,
        "automatic_ordering": False,
    }
    payload["plan_candidates"] = [
        {
            "play": "level_breakout_put",
            "level_kind": "put_wall",
            "level_label": "put_wall 7550",
            "level": 7550.0,
            "strike": 7550.0,
            "right": "P",
            "order_style": "live_nbbo_limit",
            "decision_bid": 10.0,
            "decision_ask": 10.2,
            "limit_aggressive": 10.1,
        }
    ]

    rendered = render_status_template(payload, [], NOW)
    writer = _status_writer_payload(payload)

    assert "Put候选[flip_low_breakdown]" in rendered
    assert "EXECUTION_ELIGIBLE=YES(manual_ready_exact_quote)" in rendered
    assert "结论  NO TRADE" in rendered
    assert "【条件计划】" not in rendered
    assert "SPXW 7550P" not in rendered
    assert writer.get("plan_candidates") in (None, [])
    assert writer["decision_guidance"]["action"] != "trade_ready"


def test_writer_validation_requires_all_deterministic_put_status_lines() -> None:
    template = render_status_template(_payload(), [], NOW)
    missing = "\n".join(
        line
        for line in template.splitlines()
        if not line.startswith("Put候选[call_wall_or_flip_high_rejection]")
    )
    changed = template.replace("PRIORITY=HIGH(dqv_clean_downtrend)", "PRIORITY=LOW(changed)")

    assert actionable_writer_output_valid(template, template)
    assert not actionable_writer_output_valid(missing, template)
    assert not actionable_writer_output_valid(changed, template)


def test_writer_validation_rejects_put_report_contradictions() -> None:
    template = render_status_template(_payload(), [], NOW)

    for contradiction in (
        "当前没有 Put 策略",
        "当前不存在Put候选",
        "当前存在可执行 Put 订单",
        "Put 订单已挂单",
        "已经买入 Put",
        "当前无可用的 Put 策略",
        "当前并无任何 Put 交易策略",
        "Put 是可执行订单",
        "Put 可以执行",
        "Put 现已挂出订单",
        "准备挂单买 Put",
        "Put 可以下单",
        "Put 可挂单",
        "Put 准备下单",
        "Put 已买入",
        "目前没有能用的 Put 候选",
        "当前未提供 Put 策略",
        "Put 具备执行资格",
        "Put 可供执行",
        "可操作 Put 订单",
    ):
        assert not actionable_writer_output_valid(
            f"{template}\n{contradiction}",
            template,
        )


def test_research_status_always_contains_all_put_rows_without_levels() -> None:
    payload = deepcopy(_payload())
    payload.update(
        {
            "research_only": True,
            "beijing_time": "19:00",
            "research_reference": {"price": 7560.0, "source": "future:ES"},
            "pricing_reference": {"gate_state": "missing"},
        }
    )
    payload["level_decision"] = {"phase": "far", "levels": {}}

    rendered = render_status_template(payload, [], NOW)

    assert rendered.count("Put候选[") == 3
    assert "WALL_SIGNAL=UNAVAILABLE(flip_low -;structure_level_unavailable)" in rendered
    assert "Put候选[put_wall_breakdown]" in rendered
    assert "PRIORITY=UNSUPPORTED" in rendered


def test_full_order_map_always_contains_all_put_status_rows() -> None:
    payload = _payload()
    payload.update(
        {
            "trading_date": "2026-07-24",
            "beijing_time": "22:15",
            "underlier": {"price": 7560.0, "source": "index:SPX"},
            "expected_move_points": 35.0,
            "zero_gamma": 7565.0,
            "flip_zone": [7560.0, 7565.0],
            "candidates": [],
        }
    )

    from spx_spark.application.order_map.render import render_template

    rendered = render_template(payload)

    assert rendered.count("Put候选[") == 3
    assert "WALL_SIGNAL=CONFIRMED" in rendered
    assert "EXECUTION_ELIGIBLE=NO" in rendered
    assert "PRIORITY=UNSUPPORTED" in rendered
