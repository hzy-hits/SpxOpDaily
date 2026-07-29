from __future__ import annotations

from datetime import datetime, timezone

from spx_spark.application.order_map.guidance import (
    GuidanceAction,
    build_decision_guidance,
)
from spx_spark.application.order_map.prompts import (
    render_feishu_delivery_text,
    render_operator_status_brief,
    render_status_template,
)


NOW = datetime(2026, 7, 15, 14, 0, tzinfo=timezone.utc)


def _payload() -> dict[str, object]:
    return {
        "expiry": "20260715",
        "underlier": {"price": 7558.0, "source": "index:SPX"},
        "es_last": 7603.0,
        "flip_zone": [7560.0, 7565.0],
        "regime_decision": {
            "mode": "trending",
            "direction": "down",
            "trend_score": 70.0,
            "mean_reversion_score": 45.0,
        },
        "level_decision": {
            "phase": "far",
            "quality_ok": True,
            "snapshot_consistent": True,
            "levels": {
                "put_wall": 7550.0,
                "flip_low": 7560.0,
                "flip_high": 7565.0,
                "call_wall": 7600.0,
            },
        },
        "trade_intent": {"status": "observing"},
        "plan_candidates": [],
        "candidates": [
            {"play": "put_wall_bounce_call", "level": 7550.0},
            {"play": "call_wall_fade_put", "level": 7600.0},
        ],
        "warnings": [],
    }


def test_guidance_turns_regime_into_directional_wait_conditions() -> None:
    guidance = build_decision_guidance(_payload())

    assert guidance.action is GuidanceAction.WAIT_FOR_TRIGGER
    assert guidance.bias == "趋势偏空"
    assert guidance.action_text == "当前不进场；等待价格进入关键位测试"
    assert "SPX 7560 下方保持" in guidance.trigger_text
    assert "SPX 收回 7565" in guidance.invalidation_text


def test_guidance_translates_joined_quality_failures() -> None:
    payload = _payload()
    payload["level_decision"] = {
        "phase": "far",
        "quality_ok": False,
        "snapshot_consistent": True,
        "quality_reason": "es_not_live;spx_price_unavailable;key_levels_unavailable",
    }

    guidance = build_decision_guidance(payload)

    assert guidance.action is GuidanceAction.PAUSED
    assert "ES 行情不满足实时门槛" in guidance.action_text
    assert "SPX 触发坐标不可用" in guidance.action_text
    assert "Put Wall、Flip 或 Call Wall 不完整" in guidance.action_text


def test_guidance_emits_one_trade_ready_plan() -> None:
    payload = _payload()
    payload["trade_intent"] = {"status": "trade_ready"}
    payload["plan_candidates"] = [
        {
            "strike": 7550.0,
            "right": "P",
            "level": 7560.0,
            "invalidation_spx": 7565.0,
            "target_spx": 7545.0,
        }
    ]

    guidance = build_decision_guidance(payload)

    assert guidance.action is GuidanceAction.TRADE_READY
    assert "SPXW 7550P" in guidance.action_text
    assert guidance.trigger_text == "SPX 7560 已确认触发"
    assert guidance.invalidation_text == "SPX 7565 失效；目标 7545"


def test_guidance_exposes_trend_and_local_path_conflict() -> None:
    payload = _payload()
    payload["level_decision"] = {
        "phase": "reject_pending",
        "thesis": "fade",
        "level_kind": "put_wall",
        "level": 7550.0,
        "quality_ok": True,
        "snapshot_consistent": True,
        "level_bands": {"put_wall": {"low": 7545.0, "high": 7555.0}},
    }

    guidance = build_decision_guidance(payload)

    assert guidance.action_text == "当前不进场；趋势偏空与局部反弹路径冲突"
    assert guidance.trigger_text == (
        "Put Wall 7550 需完成 REJECTED→RETEST→CONFIRMED；之后才评估 Call"
    )
    assert guidance.invalidation_text == "SPX 跌破 7545 则当前反弹路径失效"

    payload["level_decision"]["phase"] = "expired"  # type: ignore[index]
    expired = build_decision_guidance(payload)
    assert expired.action_text == "当前不进场；等待新事件"
    assert expired.trigger_text == "SPX 7560 下方保持且状态机 CONFIRMED 后才评估 Put"


def test_status_first_screen_is_guidance_and_far_delivery_stays_compact() -> None:
    payload = _payload()
    rendered = render_status_template(payload, [], NOW)

    assert "结论  NO TRADE · 未通过执行门控" in rendered
    assert "动作  当前不进场；等待价格进入关键位测试" in rendered
    assert "观察  趋势偏空（仅结构背景，不是入场方向）" in rendered
    assert "确认  SPX 7560 下方保持且状态机 CONFIRMED 后才评估 Put" in rendered
    assert "证伪  SPX 收回 7565 且 ES 量价不再同向时，偏空判断取消" in rendered

    delivered = render_feishu_delivery_text(payload, [], NOW, rendered)
    assert delivered == rendered
    assert "## Greeks 与波动" not in delivered


def test_operator_status_brief_keeps_decision_facts_and_drops_research_density() -> None:
    payload = _payload()
    payload.update(
        {
            "gamma_state": "zero_gamma_transition",
            "put_wall": 7550.0,
            "call_wall": 7600.0,
            "expected_move_points": 28.1,
            "minute_market_frame": {
                "es": {
                    "return_15m_points": -2.0,
                    "return_60m_points": -7.0,
                    "vwap_distance_points": -4.0,
                },
                "volume": {"price_volume_alignment_5m": "price_volume_aligned"},
                "cross_asset": {"es_spy_direction_confirmation_15m": "confirmed"},
            },
            "spring_gamma_v3_shadow": {"status": "abstain"},
            "convexity_idea_radar": {
                "status": "ready",
                "opportunity_board": {
                    "path_percentiles": {
                        "sample_count": 12,
                        "target_sessions": 20,
                        "confidence": "medium",
                        "dip": {
                            "raw_percentile": 0.75,
                            "shrunk_percentile": 0.65,
                        },
                        "rally": {
                            "raw_percentile": 0.25,
                            "shrunk_percentile": 0.35,
                        },
                    },
                    "lanes": {
                        "call": {
                            "priority": "MEDIUM",
                            "priority_score": 4,
                            "wall_signal": "WATCH:lower_rejection_call",
                            "edge_status": "unknown",
                            "structure_rank": ["long_call_watch"],
                            "execution": {
                                "eligible": False,
                                "block_reasons": [
                                    "dense_shadow_no_execution_authority"
                                ],
                            },
                        },
                        "put": {
                            "priority": "WATCH",
                            "priority_score": 2,
                            "wall_signal": "WATCH:upper_rejection_put",
                            "edge_status": "unknown",
                            "structure_rank": ["long_put_watch"],
                            "execution": {
                                "eligible": False,
                                "block_reasons": [
                                    "dense_shadow_no_execution_authority"
                                ],
                            },
                        },
                        "vol_range": {
                            "priority": "WATCH",
                            "priority_score": 1,
                            "volatility_signal": "MIXED_OR_UNCALIBRATED",
                            "edge_status": "requires_remaining_vol_and_tail_pricing_edge",
                            "structure_rank": ["no_structure_until_remaining_vol_edge"],
                            "execution": {
                                "eligible": False,
                                "block_reasons": [
                                    "remaining_vol_edge_not_calibrated"
                                ],
                            },
                        },
                    },
                },
            },
            "warnings": ["rth_heartbeat_degraded_snapshot"],
        }
    )

    rendered = render_operator_status_brief(payload, [], NOW)

    assert rendered.startswith("【SPX 状态 ·")
    assert "🔴 只观察" in rendered
    assert "方向  趋势偏空（仅结构背景）" in rendered
    assert "等待  SPX 7560 下方保持" in rendered
    assert "证伪  SPX 收回 7565" in rendered
    assert "合约  当前没有可执行合约" in rendered
    assert "Spring Gamma" not in rendered
    assert "凸性雷达" not in rendered
    assert "30m路径分位" in rendered
    assert "机会[Call]" in rendered
    assert "机会[Put]" in rendered
    assert "机会[Vol/Range]" in rendered
    assert rendered.count("EXECUTION_ELIGIBLE=NO") == 3
    assert "当前布局参考" not in rendered
    assert "Skew Spread Shadow" not in rendered
    assert "数据 rth_heartbeat_degraded_snapshot" in rendered
    assert len(rendered.splitlines()) <= 17


def test_operator_status_brief_never_duplicates_a_live_execution_ticket() -> None:
    payload = _payload()
    payload["trade_intent"] = {"status": "trade_ready"}
    payload["plan_candidates"] = [
        {
            "contract_id": "option:SPX:SPXW:20260715:7575:C",
            "strike": 7575.0,
            "right": "C",
            "level": 7565.0,
            "decision_bid": 9.8,
            "decision_ask": 10.2,
            "limit_aggressive": 10.0,
            "invalidation_spx": 7560.0,
            "target_spx": 7600.0,
            "intent_expires_at": "2026-07-15T14:00:18+00:00",
            "time_stop_at": "2026-07-15T14:15:00+00:00",
            "order_style": "live_nbbo_limit",
        }
    ]

    rendered = render_operator_status_brief(payload, [], NOW)

    assert "🔴 状态快照 · 本卡不执行" in rendered
    assert "独立 MANUAL READY 卡为准" in rendered
    assert "本状态卡不承载合约、报价或下单权限" in rendered
    assert "状态心跳不绕过实时重验" in rendered
    assert "🟢 MANUAL READY" not in rendered
    assert "买入  " not in rendered
    assert "限价  " not in rendered
