from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from spx_spark.application.order_map.guidance import (
    GuidanceAction,
    build_decision_guidance,
)
from spx_spark.application.order_map.prompts import (
    render_feishu_delivery_text,
    render_operator_status_brief,
    render_status_template,
)
from spx_spark.application.order_map.operator_status import (
    build_desk_map_projection,
    build_desk_message_sections,
)
from spx_spark.application.order_map.status_explanation import (
    status_explanation_output_valid,
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
            "event_id": "level:test-current",
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
        "minute_market_frame": {"quality": "ready"},
        "option_structure_frame": {
            "as_of": NOW.isoformat(),
            "quality": "ready",
            "l1": {"quality": "ready"},
            "diagnostics": {"max_quote_age_seconds": 90.0},
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


def test_unknown_level_phase_is_visible_as_degraded_data_quality() -> None:
    payload = _payload()
    payload["level_decision"] = {
        **payload["level_decision"],  # type: ignore[dict-item]
        "phase": "typo_phase",
    }

    projection = build_desk_map_projection(payload)
    rendered = render_operator_status_brief(payload, [], NOW)

    assert projection.data_quality == "DEGRADED"
    assert "unknown_level_phase" in projection.quality_reasons
    assert "主要影响：状态机阶段非法" in rendered
    assert "共 1 项" in rendered
    assert "unknown_level_phase" not in rendered


def test_terminal_level_phase_renders_as_current_standby_not_a_repeated_old_path() -> None:
    payload = _payload()
    payload["level_decision"] = {
        **payload["level_decision"],  # type: ignore[dict-item]
        "phase": "expired",
        "thesis": "breakout",
        "direction": "up",
        "level_kind": "call_wall",
        "level": 7600.0,
    }

    projection = build_desk_map_projection(payload)
    rendered = render_operator_status_brief(payload, [], NOW)

    assert projection.stage.value == "OBSERVING"
    assert projection.phase.value == "expired"
    assert "NO TRADE · STANDBY · 当前没有有效机会" in rendered
    assert "原因  旧事件已经结束，当前没有活跃交易路径" in rendered
    assert "Execution  WAIT · 当前没有可执行机会" in rendered
    assert "已过期（已过期）" not in rendered
    assert "当前结构阶段继续有效" not in rendered


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
                "quality": "ready",
                "es": {
                    "return_15m_points": -2.0,
                    "return_60m_points": -7.0,
                    "vwap": 7607.0,
                    "vwap_distance_points": -4.0,
                },
                "volume": {"price_volume_alignment_5m": "price_volume_aligned"},
                "cross_asset": {"es_spy_direction_confirmation_15m": "confirmed"},
                "volatility": {"vix1d": 17.2, "vix": 18.5},
                "diagnostics": {
                    "rth_market_state": {
                        "input_lineage": {
                            "values": {"opening_range_state": "above_orh_confirmed"},
                            "diagnostics": {
                                "opening_range": {
                                    "status": "ready",
                                    "orh": 7595.0,
                                    "orl": 7578.0,
                                },
                                "same_time_range": {"current_range_points": 18.0},
                            },
                        }
                    }
                },
            },
            "option_structure_frame": {
                "as_of": NOW.isoformat(),
                "quality": "ready",
                "volatility": {
                    "atm_iv_0dte": 0.182,
                    "atm_iv_change_5m": 0.004,
                    "atm_iv_change_15m": 0.008,
                    "atm_iv_change_60m": 0.012,
                },
                "structure": {"gex_quality": "open_interest_gex"},
                "density": {"clipped_mass_fraction": 0.01},
                "exposure": {"oi_quality": "ibkr_ok"},
                "l1": {"quality": "ready"},
                "diagnostics": {"max_quote_age_seconds": 90.0},
            },
            "day_move": {"em_used_fraction": 0.64},
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
                                "block_reasons": ["dense_shadow_no_execution_authority"],
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
                                "block_reasons": ["dense_shadow_no_execution_authority"],
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
                                "block_reasons": ["remaining_vol_edge_not_calibrated"],
                            },
                        },
                    },
                },
            },
            "warnings": ["rth_heartbeat_degraded_snapshot"],
        }
    )

    rendered = render_operator_status_brief(payload, [], NOW)

    assert rendered.startswith("【SPX Desk Map ·")
    assert "Desk View  NO TRADE · 趋势偏空仅作背景，尚无价格触发 · 状态：观察中（尚未触发）" in rendered
    assert "Location  SPX 7558 · ES 7603" in rendered
    assert "Gamma位置 Flip 下方 2.0pt · ZG unavailable" in rendered
    assert "ES VWAP 7607（偏离 -4pt）" in rendered
    assert "OR 上沿上方确认（ORL 7578 / ORH 7595）" in rendered
    assert "EM ±28.1pt" in rendered
    assert "GTH 已用" not in rendered
    assert "Primary  Evidence · 方向来源  尚无价格接受/拒绝确认；趋势偏空仅为 ES/量价背景" in rendered
    assert "下一触发  等待当前 Flip 7560–7565 的接受或拒绝；确认前 NO TRADE" in rendered
    assert "流确认  ES 15m -2pt / 60m -7pt" in rendered
    assert "量价 同向确认 · ES/SPY 同向确认" in rendered
    assert "Alternative  Evidence · 尚无单边方向；当前不存在交易失效位" in rendered
    assert "Structure  Put/Flip/Call 7550 / 7560–7565 / 7600 · event=live" in rendered
    assert "Gamma职责  Gamma 过渡" in rendered
    assert "dealer sign unknown" in rendered
    assert "价格选边前 NO TRADE" in rendered
    assert "Targets  当前无交易目标 · 实时结构 Put 7550 / Call 7600" in rendered
    assert "Execution  WAIT · 尚无确定性结构入场" in rendered
    assert "Data Quality  DEGRADED · 主要影响：rth heartbeat degraded snapshot" in rendered
    assert "共 1 项" in rendered
    assert "rth_heartbeat_degraded_snapshot" not in rendered
    assert "ATM IV 0DTE" not in rendered
    assert "IVΔ 5/15/60m" not in rendered
    assert "VIX1D/VIX" not in rendered
    assert "Frames market=" not in rendered
    assert "原因  当前未触发关键位，趋势信号继续独立评估" in rendered
    assert "Spring Gamma" not in rendered
    assert "凸性雷达" not in rendered
    assert "路径分位" not in rendered
    assert "机会[" not in rendered
    assert "EXECUTION_ELIGIBLE" not in rendered
    assert "CONFIRMED" not in rendered
    assert "REJECTED" not in rendered
    assert "当前布局参考" not in rendered
    assert "Skew Spread Shadow" not in rendered
    assert "Gamma 不给" not in rendered  # transition has no inferred direction at all


def test_desk_map_primary_conclusion_comes_from_strategy_decision_blockers() -> None:
    payload = _payload()
    payload["level_decision"] = {
        **payload["level_decision"],  # type: ignore[dict-item]
        "phase": "confirmed",
        "direction": "up",
        "thesis": "breakout",
        "level_kind": "flip_high",
        "level": 7565.0,
    }
    payload["trade_intent"] = {
        "status": "trade_ready",
        "event_id": "level:test-current",
        "intent_id": "intent:legacy-ready",
        "contract_id": "option:SPX:SPXW:20260715:7565:C",
    }
    payload["plan_candidates"] = [
        {
            "intent_id": "intent:legacy-ready",
            "contract_id": "option:SPX:SPXW:20260715:7565:C",
            "target_spx": 7600.0,
        }
    ]
    payload["strategy_decision"] = {
        "decision_type": "NO_TRADE",
        "candidate": None,
        "action_authority": "none",
        "execution": {"action": "WAIT"},
        "why_not": {
            "reasons": ["quote_refresh_required", "max_debit_fraction_exceeded"],
            "nearest_candidate": {
                "strategy_type": "CALL_DEBIT_VERTICAL",
                "long": {"strike": 7565.0},
                "short": {"strike": 7575.0},
                "failed_gates": [
                    {
                        "gate": "max_debit_fraction_exceeded",
                        "actual": 0.52,
                        "threshold": 0.45,
                    }
                ],
            },
            "reauthorize_on": "刷新 SPXW 两腿双边报价后重新计算",
        },
    }

    sections = build_desk_message_sections(payload, NOW)
    rendered = render_operator_status_brief(payload, [], NOW)

    assert "结论  不做" in sections.desk_view
    assert "主因  精确双边报价需要刷新" in sections.desk_view
    assert "最近候选  Call 价差 7565/7575（卡在：权利金相对翼宽偏贵）" in sections.desk_view
    assert "下一步  刷新 SPXW 两腿双边报价后重新计算" in sections.desk_view
    assert "quote_refresh_required" not in sections.desk_view
    assert "max_debit_fraction_exceeded" not in sections.desk_view
    assert sections.execution.startswith("等待 · 不做 · 精确双边报价需要刷新")
    assert "7600" not in sections.targets
    assert "结构与执行门控已通过" not in rendered
    assert "原因  精确双边报价需要刷新" in rendered


def test_desk_sections_make_unavailable_market_facts_explicit() -> None:
    sections = build_desk_message_sections(_payload(), NOW)

    assert "ES VWAP unavailable" in sections.location
    assert "OR unavailable" in sections.location
    assert "EM unavailable" in sections.location
    assert "流确认  ES 15m unavailable / 60m unavailable" in sections.primary_path
    assert "量价 unavailable · ES/SPY unavailable" in sections.primary_path
    assert sections.data_quality == "READY · 决策坐标与结构快照可用"


def test_missing_live_spx_labels_latched_decision_spot_as_non_actionable_reference() -> None:
    payload = _payload()
    payload["underlier"] = {"price": None, "source": None}
    payload["level_decision"] = {
        **payload["level_decision"],  # type: ignore[dict-item]
        "spot": 7754.825,
        "trigger_coordinate_kind": "es_equivalent",
        "trigger_instrument_id": "future:ES",
    }

    sections = build_desk_message_sections(payload, NOW)

    assert "SPX unavailable" in sections.location
    assert "reference 7754.82（ES-equivalent proxy；latched/proxy，not actionable）" in (
        sections.location
    )
    assert "SPX 7754.82 ·" not in sections.location
    assert "距离" not in sections.location
    assert "Gamma位置 Flip 位置 unavailable" in sections.location


def test_gth_location_uses_chain_implied_or_es_not_cash_spx_missing() -> None:
    gth_now = datetime(2026, 8, 11, 1, 0, tzinfo=timezone.utc)
    payload = _payload()
    payload["session_phase"] = {"name": "asia_globex", "name_cn": "亚盘夜盘"}
    payload["underlier"] = {
        "price": 7758.7,
        "source": "chain_implied",
        "kind": "chain_implied_spx",
        "observed_value": 7758.7,
        "spx_observed_value": 7758.7,
    }
    payload["trigger_coordinate"] = {
        "kind": "chain_implied_spx",
        "observed_value": 7758.7,
        "spx_observed_value": 7758.7,
        "source": "chain_implied",
    }
    payload["es_last"] = 7782.0
    payload["option_structure_frame"] = {
        "as_of": gth_now.isoformat(),
        "quality": "ready",
        "l1": {"quality": "ready"},
        "diagnostics": {"max_quote_age_seconds": 90.0},
        "structure": {
            "put_wall": 7700.0,
            "flip_zone": [7740.0, 7745.0],
            "call_wall": 7775.0,
            "gex_quality": "open_interest_gex",
        },
    }
    payload["flip_zone"] = [7740.0, 7745.0]

    sections = build_desk_message_sections(payload, gth_now)

    assert "夜盘观察坐标 7758.7（期权隐含）" in sections.location
    assert "可用 SPX 坐标缺失" not in sections.desk_view
    assert "SPX unavailable" not in sections.location
    assert "Put/Flip/Call 7700 / 7740–7745 / 7775 · event=live" in sections.structure
    assert "event=live" in sections.structure
    assert "frozen/reference" not in sections.structure


def test_gth_missing_coordinate_does_not_demand_cash_spx() -> None:
    gth_now = datetime(2026, 8, 11, 1, 0, tzinfo=timezone.utc)
    payload = _payload()
    payload["session_phase"] = {"name": "asia_globex", "name_cn": "亚盘夜盘"}
    payload["underlier"] = {"price": None, "source": None}
    payload["es_last"] = 7782.0
    payload["level_decision"] = {
        **payload["level_decision"],  # type: ignore[dict-item]
        "session_mode": "gth",
        "spot": 7756.65,
        "trigger_coordinate_kind": "es_equivalent",
    }
    payload["option_structure_frame"] = {
        "as_of": gth_now.isoformat(),
        "quality": "unavailable",
        "l1": {"quality": "unavailable"},
        "diagnostics": {"max_quote_age_seconds": 90.0},
        "structure": {},
    }

    sections = build_desk_message_sections(payload, gth_now)

    assert "夜盘观察坐标" in sections.location
    assert "现金 SPX 不适用" in sections.location
    assert "SPX unavailable" not in sections.location
    assert "GTH期权帧未就绪" in sections.structure
    assert "frozen/reference" not in sections.structure


def test_gth_desk_map_does_not_mix_rth_cash_confirmation_or_analytical_only_noise() -> None:
    gth_now = datetime(2026, 8, 11, 1, 0, tzinfo=timezone.utc)
    payload = _payload()
    payload["session_phase"] = {"name": "asia_globex", "name_cn": "亚盘夜盘"}
    payload["underlier"] = {
        "price": 7758.7,
        "source": "chain_implied",
        "kind": "chain_implied_spx",
        "observed_value": 7758.7,
        "spx_observed_value": 7758.7,
    }
    payload["minute_market_frame"] = {
        "quality": "ready",
        "es": {
            "price": 7782.0,
            "return_15m_points": 0.5,
            "return_60m_points": 2.0,
        },
        "volume": {"price_volume_alignment_5m": "price_volume_aligned"},
        "cross_asset": {"es_spy_direction_confirmation_15m": "unavailable"},
        "diagnostics": {
            "warnings": ["cash_index_cash_session_closed"],
        },
    }
    payload["option_structure_frame"] = {
        "as_of": gth_now.isoformat(),
        "quality": "ready",
        "l1": {"quality": "ready"},
        "diagnostics": {"max_quote_age_seconds": 90.0},
        "exposure": {
            "oi_quality": "ibkr_ok",
            "warnings": ["analytical_leg_rejected:analytical_only_non_executable:3"],
        },
        "structure": {
            "put_wall": 7700.0,
            "flip_zone": [7740.0, 7745.0],
            "call_wall": 7775.0,
            "gex_quality": "open_interest_gex",
        },
    }

    projection = build_desk_map_projection(payload)
    sections = build_desk_message_sections(payload, gth_now)

    assert projection.data_quality == "READY"
    assert projection.quality_reasons == ()
    assert "ES/SPY" not in sections.primary_path
    assert "ES 15m +0.5pt / 60m +2pt" in sections.primary_path
    assert "量价 同向确认" in sections.primary_path
    assert sections.data_quality == "READY · 决策坐标与结构快照可用"
    assert "analytical" not in sections.data_quality.lower()
    assert "cash_index" not in sections.data_quality


def test_rth_desk_map_still_flags_analytical_only_legs_as_degraded() -> None:
    payload = _payload()
    frame = dict(payload["option_structure_frame"])  # type: ignore[arg-type]
    frame["exposure"] = {
        "oi_quality": "ibkr_ok",
        "warnings": ["analytical_leg_rejected:analytical_only_non_executable:3"],
    }
    payload["option_structure_frame"] = frame

    projection = build_desk_map_projection(payload)
    sections = build_desk_message_sections(payload, NOW)

    assert projection.data_quality == "DEGRADED"
    assert projection.quality_reasons == (
        "analytical_leg_rejected:analytical_only_non_executable:3",
    )
    assert "ES/SPY" in sections.primary_path
    assert "结构腿仅分析用、不可当作执行报价" in sections.data_quality


def test_gth_no_trade_does_not_park_a_near_miss_put_vertical() -> None:
    gth_now = datetime(2026, 8, 11, 1, 0, tzinfo=timezone.utc)
    payload = _payload()
    payload["session_phase"] = {"name": "asia_globex", "name_cn": "亚盘夜盘"}
    payload["strategy_decision"] = {
        "decision_type": "NO_TRADE",
        "candidate": None,
        "action_authority": "none",
        "execution": {"action": "WAIT"},
        "iron_condor_map": {
            "status": "ready",
            "short_abs_delta": 0.20,
            "wing_width": 10.0,
            "strikes": [7680.0, 7690.0, 7810.0, 7820.0],
            "quote": {"credit": 9.0},
            "economics": {"max_gain_points": 9.0, "max_loss_points": 1.0, "width_points": 10.0},
        },
        "rejection_funnel": {"candidate_enumerated": 30, "hard_gate_pass": 0},
        "why_not": {
            "reasons": ["confirmed_price_trigger_unavailable"],
            "nearest_candidate": {
                "strategy_type": "PUT_DEBIT_VERTICAL",
                "long": {"strike": 7730.0},
                "short": {"strike": 7725.0},
                "failed_gates": [
                    {"gate": "confirmed_price_trigger_unavailable"},
                ],
            },
        },
    }

    sections = build_desk_message_sections(payload, gth_now)

    assert "心跳 · 健康检查" not in sections.desk_view
    assert "7730/7725" not in sections.desk_view
    assert "Put 价差" not in sections.desk_view
    assert "待评估" not in sections.desk_view
    assert "可看 ·" not in sections.desk_view
    assert "卖20Δ 10宽 7680/7690/7810/7820 贷记 9 最大亏损 1" in sections.desk_view


def test_gth_event_settlement_put_vertical_is_not_watchable() -> None:
    gth_now = datetime(2026, 8, 13, 4, 22, tzinfo=timezone.utc)
    payload = _payload()
    payload["session_phase"] = {"name": "asia_globex", "name_cn": "亚盘夜盘"}
    payload["strategy_decision"] = {
        "decision_type": "PUT_DEBIT_VERTICAL",
        "action_authority": "manual",
        "candidate": {
            "strategy_type": "PUT_DEBIT_VERTICAL",
            "setup_kind": "EVENT_SETTLEMENT_THRESHOLD",
            "source": "prior_close_event_view",
            "long": {"strike": 7750.0},
            "short": {"strike": 7745.0},
            "opportunity_id": "strategy-opportunity:d95b5599b6bb81771e",
        },
        "execution": {"action": "MANUAL_LIMIT"},
        "iron_condor_map": {
            "status": "unavailable",
            "reason": "iron_condor_delta_quotes_unavailable",
            "strikes": [],
        },
        "rejection_funnel": {"candidate_enumerated": 12, "hard_gate_pass": 0},
        "why_not": {"reasons": []},
    }

    sections = build_desk_message_sections(payload, gth_now)

    assert "心跳 · 健康检查" not in sections.desk_view
    assert "最近候选  无" not in sections.desk_view
    assert "7750/7745" not in sections.desk_view
    assert "可看 ·" not in sections.desk_view
    assert "可看 ·" not in sections.execution
    assert "扫描中 · 铁鹰已标位" in sections.execution


@pytest.mark.parametrize(
    "frame_update",
    (
        {"quality": "unavailable"},
        {"as_of": (NOW.replace(second=0) - timedelta(minutes=3)).isoformat()},
        {"structure": {"frozen": True}},
    ),
)
def test_non_live_option_structure_is_labeled_frozen_reference(
    frame_update: dict[str, object],
) -> None:
    payload = _payload()
    frame = dict(payload["option_structure_frame"])  # type: ignore[arg-type]
    frame.update(frame_update)
    payload["option_structure_frame"] = frame

    sections = build_desk_message_sections(payload, NOW)

    assert "event=frozen/reference" in sections.structure
    assert "event=live" not in sections.structure


def test_desk_structure_explains_promoted_wall_migration_when_live_unavailable() -> None:
    payload = _payload()
    payload["level_decision"] = {
        **payload["level_decision"],  # type: ignore[dict-item]
        "levels": {
            "put_wall": 7750.0,
            "flip_low": 7740.0,
            "flip_high": 7745.0,
            "call_wall": 7800.0,
        },
        "previous_structure_levels": {
            "put_wall": 7730.0,
            "flip_low": 7725.0,
            "flip_high": 7730.0,
            "call_wall": 7775.0,
        },
    }
    payload["flip_zone"] = [7740.0, 7745.0]
    payload["candidates"] = [
        {"play": "put_wall_bounce_call", "level": 7750.0},
        {"play": "call_wall_fade_put", "level": 7800.0},
    ]
    payload["option_structure_frame"] = {
        "as_of": NOW.isoformat(),
        "quality": "unavailable",
        "l1": {"quality": "unavailable"},
        "diagnostics": {"max_quote_age_seconds": 90.0},
        "structure": {
            "put_wall": 7750.0,
            "flip_zone": [7740.0, 7745.0],
            "call_wall": 7800.0,
        },
    }

    sections = build_desk_message_sections(payload, NOW)

    assert "Put/Flip/Call 7750 / 7740–7745 / 7800 · event=frozen/reference" in (
        sections.structure
    )
    assert (
        "Structure change: Call Wall 7775 → 7800 · Put Wall 7730 → 7750 · "
        "Flip 7725–7730 → 7740–7745 · source=frozen/reference · "
        "live confirmation unavailable"
    ) in sections.structure


def test_desk_structure_marks_pending_candidate_migration() -> None:
    payload = _payload()
    payload["level_decision"] = {
        **payload["level_decision"],  # type: ignore[dict-item]
        "structure_change_pending": True,
        "structure_candidate": {
            "levels": {
                "put_wall": 7750.0,
                "flip_low": 7740.0,
                "flip_high": 7745.0,
                "call_wall": 7800.0,
            }
        },
        "levels": {
            "put_wall": 7730.0,
            "flip_low": 7725.0,
            "flip_high": 7730.0,
            "call_wall": 7775.0,
        },
    }

    sections = build_desk_message_sections(payload, NOW)

    assert "Structure change pending: Call Wall 7775 → 7800" in sections.structure
    assert "source=live · confirming" in sections.structure


def test_gth_omits_rth_only_market_state_failures_but_keeps_gth_failures() -> None:
    payload = _payload()
    payload["session_phase"] = {"name": "asia_globex", "name_cn": "亚盘夜盘"}
    payload["level_decision"] = {
        **payload["level_decision"],  # type: ignore[dict-item]
        "session_mode": "globex",
    }
    payload["minute_market_frame"] = {
        "quality": "ready",
        "diagnostics": {
            "rth_market_state": {
                "status": "uncertain",
                "reasons": [
                    "outside_rth_session",
                    "price_vs_vwap_missing",
                    "classification_gate_failed",
                ],
            }
        },
    }
    payload["warnings"] = ["rth_heartbeat_degraded_snapshot", "ibkr_feed_unavailable"]

    projection = build_desk_map_projection(payload)

    assert projection.quality_reasons == ("ibkr_feed_unavailable",)


def test_rth_desk_stays_ready_when_ibkr_is_down_and_schwab_frames_are_live() -> None:
    payload = _payload()
    payload["session_phase"] = {"name": "us_open_hour", "name_cn": "开盘首小时"}
    payload["option_structure_frame"] = {
        **payload["option_structure_frame"],  # type: ignore[dict-item]
        "quality": "ready",
        "structure": {"gex_quality": "open_interest_gex"},
        "exposure": {"oi_quality": "schwab_unverified", "warnings": ["schwab_oi_unverified"]},
        "l1": {"quality": "ready"},
        "diagnostics": {
            "warnings": [
                "IBKR feed unavailable; stale SPXW option quotes suppressed",
                "open interest wall scope:schwab_rth_lane",
            ]
        },
    }
    payload["warnings"] = [
        "ibkr_feed_unavailable",
        "IBKR feed unavailable; stale SPXW option quotes suppressed",
    ]

    projection = build_desk_map_projection(payload)
    sections = build_desk_message_sections(payload, NOW)

    assert projection.data_quality == "READY"
    assert projection.quality_reasons == ()
    assert sections.data_quality == "READY · 决策坐标与结构快照可用"


def test_current_rth_phase_overrides_latched_globex_decision_for_quality() -> None:
    payload = _payload()
    payload["session_phase"] = {"name": "us_open_hour", "name_cn": "开盘首小时"}
    payload["level_decision"] = {
        **payload["level_decision"],  # type: ignore[dict-item]
        "session_mode": "globex",
    }
    payload["minute_market_frame"] = {
        "quality": "degraded",
        "diagnostics": {
            "rth_market_state": {
                "status": "uncertain",
                "reasons": ["price_vs_vwap_missing", "classification_gate_failed"],
            }
        },
    }
    payload["warnings"] = ["rth_heartbeat_degraded_snapshot"]

    projection = build_desk_map_projection(payload)

    assert "market_state:price_vs_vwap_missing" in projection.quality_reasons
    assert "market_state:classification_gate_failed" in projection.quality_reasons
    assert "rth_heartbeat_degraded_snapshot" in projection.quality_reasons


def test_expected_move_usage_requires_matching_horizon_contract() -> None:
    payload = _payload()
    payload["expected_move_points"] = 8.16
    payload["day_move"] = {
        "em_used_fraction": 17.05,
        "em_baseline_source": "es_gth_open",
    }

    mismatched = build_desk_message_sections(payload, NOW)
    assert "EM ±8.2pt" in mismatched.location
    assert "1705%" not in mismatched.location

    payload["day_move"] = {
        "em_used_fraction": 0.64,
        "em_numerator_horizon_id": "gth-session-2026-07-15",
        "em_denominator_horizon_id": "gth-session-2026-07-15",
        "em_usage_label": "GTH",
    }
    aligned = build_desk_message_sections(payload, NOW)
    assert "EM ±8.2pt · GTH 已用 64%" in aligned.location


def test_market_bias_never_becomes_a_typed_direction_without_a_price_path() -> None:
    payload = _payload()
    payload["regime_decision"] = {"mode": "trending", "direction": "up"}

    projection = build_desk_map_projection(payload)
    sections = build_desk_message_sections(payload, NOW)

    assert projection.direction == "none"
    assert sections.desk_view.startswith("NO TRADE")
    assert "尚无价格接受/拒绝确认" in sections.primary_path


@pytest.mark.parametrize(
    ("gamma_state", "expected"),
    [
        ("positive_gamma_pin", "反馈偏压制/回归"),
        ("negative_gamma_acceleration", "反馈偏放大"),
        ("zero_gamma_transition", "价格选边前 NO TRADE"),
    ],
)
def test_gamma_is_a_conditional_feedback_mechanism_not_direction(
    gamma_state: str,
    expected: str,
) -> None:
    payload = _payload()
    payload["option_structure_frame"] = {
        "quality": "ready",
        "structure": {
            "gamma_state": gamma_state,
            "gex_quality": "open_interest_gex",
            "net_gamma_ratio": 0.61,
            "zero_gamma": 7495.0,
            "flip_zone": [7490.0, 7510.0],
            "put_wall": 7450.0,
            "call_wall": 7550.0,
        },
        "exposure": {"oi_quality": "ibkr_ok"},
        "l1": {"quality": "ready"},
    }

    sections = build_desk_message_sections(payload, NOW)

    assert expected in sections.structure
    assert "dealer sign unknown" in sections.structure
    assert "做市商买" not in sections.structure
    assert "做市商卖" not in sections.structure


def test_confirmed_direction_names_price_path_as_authority() -> None:
    payload = _payload()
    payload["level_decision"] = {
        **payload["level_decision"],  # type: ignore[dict-item]
        "phase": "confirmed",
        "thesis": "fade",
        "direction": "up",
        "level_kind": "put_wall",
        "level": 7550.0,
    }
    payload["option_structure_frame"] = {
        "quality": "ready",
        "structure": {
            "gamma_state": "positive_gamma_pin",
            "gex_quality": "open_interest_gex",
            "put_wall": 7550.0,
            "call_wall": 7600.0,
            "flip_zone": [7560.0, 7565.0],
        },
        "exposure": {"oi_quality": "ibkr_ok"},
        "l1": {"quality": "ready"},
    }

    projection = build_desk_map_projection(payload)
    sections = build_desk_message_sections(payload, NOW)

    assert projection.direction == "up"
    assert "LONG / CALL FADE" in sections.desk_view
    assert "LONG 来自 Put Wall 7550 的价格拒绝回归" in sections.primary_path
    assert "Gamma 不提供第一步方向" in sections.primary_path


def test_observing_report_does_not_reuse_old_event_targets() -> None:
    payload = _payload()
    payload["underlier"] = {"price": 7608.0, "source": "index:SPX"}
    payload["flip_zone"] = [7510.0, 7515.0]
    payload["level_decision"] = {
        **payload["level_decision"],  # type: ignore[dict-item]
        "phase": "approaching",
        "level_kind": "put_wall",
        "level": 7600.0,
    }
    payload["candidates"] = [
        {"play": "put_wall_bounce_call", "level": 7600.0},
        {"play": "call_wall_fade_put", "level": 7610.0},
    ]
    payload["plan_candidates"] = [{"target_spx": 7999.0}]

    sections = build_desk_message_sections(payload, NOW)

    assert "7510" not in sections.primary_path
    assert "观察当前 Put Wall 7600 的接受或拒绝" in sections.primary_path
    assert "当前不存在交易失效位" in sections.alternative_path
    assert "当前无交易目标 · 实时结构 Put 7600 / Call 7610" == sections.targets
    assert "7550" not in sections.targets
    assert "7999" not in sections.targets


def test_stale_ready_card_cannot_override_current_path_or_quality() -> None:
    payload = _payload()
    payload["level_decision"] = {
        **payload["level_decision"],  # type: ignore[dict-item]
        "phase": "approaching",
        "snapshot_consistent": False,
        "thesis": "none",
        "direction": "none",
        "level_kind": "put_wall",
        "level": 7550.0,
    }
    payload["trade_intent"] = {"status": "trade_ready"}
    payload["plan_candidates"] = [
        {
            "play": "level_breakout_call",
            "right": "C",
            "target_spx": 7999.0,
        }
    ]

    projection = build_desk_map_projection(payload)
    sections = build_desk_message_sections(payload, NOW)

    assert projection.stage.value == "PAUSED"
    assert projection.direction == "none"
    assert "ready_without_current_confirmed_path" in projection.quality_reasons
    assert sections.desk_view.startswith("NO TRADE")
    assert "禁止使用旧机会或旧目标" in sections.execution
    assert "7999" not in sections.targets


def test_new_confirmed_event_rejects_old_ready_opportunity_and_target() -> None:
    payload = _payload()
    payload["level_decision"] = {
        **payload["level_decision"],  # type: ignore[dict-item]
        "event_id": "level:current",
        "phase": "confirmed",
        "direction": "down",
        "thesis": "breakout",
        "level_kind": "put_wall",
        "level": 7550.0,
    }
    payload["trade_intent"] = {
        "status": "trade_ready",
        "event_id": "level:old",
        "intent_id": "intent:old",
        "contract_id": "option:SPX:SPXW:20260715:7500:P",
    }
    payload["plan_candidates"] = [
        {
            "intent_id": "intent:old",
            "contract_id": "option:SPX:SPXW:20260715:7500:P",
            "right": "P",
            "target_spx": 7999.0,
        }
    ]

    projection = build_desk_map_projection(payload)
    sections = build_desk_message_sections(payload, NOW)

    assert projection.stage.value == "PAUSED"
    assert "ready_opportunity_mismatch" in projection.quality_reasons
    assert sections.desk_view.startswith("NO TRADE")
    assert "READY 不属于当前价格事件" in sections.execution
    assert "7999" not in sections.targets
    assert "intent:old" not in sections.execution


def test_rth_ready_requires_one_current_plan() -> None:
    payload = _payload()
    payload["level_decision"] = {
        **payload["level_decision"],  # type: ignore[dict-item]
        "phase": "confirmed",
        "direction": "up",
        "thesis": "breakout",
        "level_kind": "flip_high",
        "level": 7565.0,
    }
    payload["trade_intent"] = {
        "status": "trade_ready",
        "event_id": "level:test-current",
        "intent_id": "intent:test-current",
        "contract_id": "option:SPX:SPXW:20260715:7575:C",
    }

    projection = build_desk_map_projection(payload)
    sections = build_desk_message_sections(payload, NOW)

    assert projection.stage.value == "PAUSED"
    assert "ready_opportunity_mismatch" in projection.quality_reasons
    assert sections.desk_view.startswith("NO TRADE")
    assert "禁止使用旧机会或旧目标" in sections.execution


def test_gth_ready_requires_current_source_signal() -> None:
    payload = _payload()
    payload["level_decision"] = {
        **payload["level_decision"],  # type: ignore[dict-item]
        "event_id": "level:current-gth",
        "phase": "confirmed",
        "direction": "down",
        "thesis": "breakout",
        "level_kind": "put_wall",
        "level": 7550.0,
    }
    payload["gth_level_manual_candidate"] = {
        "status": "manual_ready",
        "source_signal_id": "level:old-gth",
        "candidate_id": "gth:old",
        "direction": "down",
        "thesis": "breakout",
        "target_spx": 7500.0,
    }

    projection = build_desk_map_projection(payload)
    sections = build_desk_message_sections(payload, NOW)

    assert projection.stage.value == "PAUSED"
    assert "ready_opportunity_mismatch" in projection.quality_reasons
    assert sections.desk_view.startswith("NO TRADE")
    assert "gth:old" not in sections.execution
    assert "7500" not in sections.targets


def test_confirmed_path_holds_for_execution_gate_and_does_not_reuse_trigger_as_target() -> None:
    payload = _payload()
    payload["underlier"] = {"price": 7637.0, "source": "index:SPX"}
    payload["level_decision"] = {
        **payload["level_decision"],  # type: ignore[dict-item]
        "phase": "confirmed",
        "direction": "up",
        "thesis": "breakout",
        "level_kind": "call_wall",
        "level": 7630.0,
        "levels": {
            "put_wall": 7600.0,
            "flip_low": 7610.0,
            "flip_high": 7615.0,
            "call_wall": 7630.0,
        },
    }

    sections = build_desk_message_sections(payload, NOW)

    assert sections.desk_view.startswith("HOLD · LONG / CALL BREAKOUT")
    assert "尚不可入场" in sections.desk_view
    assert "当前路径已确认；等待实时合约、报价与盈亏比" in sections.primary_path
    assert "需完成" not in sections.primary_path
    assert sections.targets == "当前无可执行目标 · 等待当前机会生成并校验有效目标位"


def test_missing_required_frames_cannot_report_ready_data_quality() -> None:
    payload = _payload()
    payload.pop("minute_market_frame")
    payload.pop("option_structure_frame")

    projection = build_desk_map_projection(payload)
    sections = build_desk_message_sections(payload, NOW)

    assert projection.data_quality == "DEGRADED"
    assert projection.quality_reasons[:3] == (
        "market_frame:unavailable",
        "option_frame:unavailable",
        "option_l1:unavailable",
    )
    assert sections.data_quality.startswith("DEGRADED")
    assert "Frames market=" not in sections.data_quality


def test_current_ready_is_paused_when_required_frames_are_missing() -> None:
    payload = _payload()
    payload.pop("minute_market_frame")
    payload.pop("option_structure_frame")
    payload["level_decision"] = {
        **payload["level_decision"],  # type: ignore[dict-item]
        "phase": "confirmed",
        "direction": "up",
        "thesis": "breakout",
        "level_kind": "flip_high",
        "level": 7565.0,
    }
    payload["trade_intent"] = {
        "status": "trade_ready",
        "event_id": "level:test-current",
        "intent_id": "intent:test-current",
        "contract_id": "option:SPX:SPXW:20260715:7575:C",
    }
    payload["plan_candidates"] = [
        {
            "intent_id": "intent:test-current",
            "contract_id": "option:SPX:SPXW:20260715:7575:C",
        }
    ]

    projection = build_desk_map_projection(payload)
    sections = build_desk_message_sections(payload, NOW)

    assert projection.stage.value == "PAUSED"
    assert "ready_required_frame_unavailable" in projection.quality_reasons
    assert sections.desk_view.startswith("NO TRADE")
    assert "ready_required_frame_unavailable" not in sections.data_quality
    assert "共 4 项" in sections.data_quality
    assert "禁止执行 READY" in sections.execution


def test_desk_sections_do_not_invent_opening_range_levels_from_state() -> None:
    payload = _payload()
    payload["minute_market_frame"] = {
        "quality": "ready",
        "diagnostics": {
            "rth_market_state": {
                "input_lineage": {
                    "values": {"opening_range_state": "inside"},
                }
            }
        },
    }

    sections = build_desk_message_sections(payload, NOW)

    assert "OR 区间内（区间值 unavailable）" in sections.location
    assert "ORL" not in sections.location
    assert "ORH" not in sections.location


def test_desk_data_quality_keeps_raw_reasons_in_projection_but_summarizes_human_text() -> None:
    payload = _payload()
    payload.update(
        {
            "minute_market_frame": {"quality": "degraded"},
            "option_structure_frame": {
                "quality": "degraded",
                "diagnostics": {"warnings": ["schwab_unverified"]},
                "structure": {
                    "gex_quality": "no_open_interest_gex",
                    "warnings": ["wall_source_frozen"],
                },
                "density": {"clipped_mass_fraction": 0.284},
                "exposure": {
                    "oi_quality": "missing",
                    "warnings": ["exposure_coverage_low"],
                },
                "l1": {
                    "quality": "degraded",
                    "diagnostics": {"warnings": ["nbbo_sparse"]},
                },
            },
            "warnings": [f"payload_warning_{index}" for index in range(1, 7)],
        }
    )

    projection = build_desk_map_projection(payload)
    sections = build_desk_message_sections(payload, NOW)

    expected = {
        "market_frame:degraded",
        "option_frame:degraded",
        "option_l1:degraded",
        "oi:missing",
        "gex:no_open_interest_gex",
        "density_clipped:28%",
        "wall_source_frozen",
        "exposure_coverage_low",
        "nbbo_sparse",
        *(f"payload_warning_{index}" for index in range(1, 7)),
    }
    assert expected.issubset(set(projection.quality_reasons))
    assert "schwab_unverified" not in projection.quality_reasons
    assert "主要影响：市场帧降级，ES 流确认需谨慎" in sections.data_quality
    assert "次要影响：期权结构帧降级" in sections.data_quality
    assert f"共 {len(projection.quality_reasons)} 项" in sections.data_quality
    assert "payload_warning_6" not in sections.data_quality
    assert "审计码" not in sections.data_quality


def test_operator_status_brief_never_duplicates_a_live_execution_ticket() -> None:
    payload = _payload()
    payload["level_decision"] = {
        **payload["level_decision"],  # type: ignore[dict-item]
        "phase": "confirmed",
    }
    payload["trade_intent"] = {
        "status": "trade_ready",
        "event_id": "level:test-current",
        "intent_id": "intent:test-current",
        "contract_id": "option:SPX:SPXW:20260715:7575:C",
    }
    payload["plan_candidates"] = [
        {
            "play": "level_breakout_call",
            "intent_id": "intent:test-current",
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

    assert "Desk View  READY · LONG / CALL BREAKOUT · 状态：执行候选已就绪（已确认）" in rendered
    assert "Execution  READY · 独立 MANUAL READY 卡承载实时合约与报价" in rendered
    assert "🟢 MANUAL READY" not in rendered
    assert "买入  " not in rendered
    assert "限价  " not in rendered


def test_operator_status_brief_points_to_separate_gth_manual_ready_card() -> None:
    payload = _payload()
    payload["level_decision"] = {
        **payload["level_decision"],  # type: ignore[dict-item]
        "phase": "confirmed",
    }
    payload["gth_level_manual_candidate"] = {
        "status": "manual_ready",
        "source_signal_id": "level:test-current",
        "manual_action_eligible": True,
        "operator_notification_eligible": True,
        "edge_authority": "validated_first_touch_time_stop_net_pnl",
        "position_type": "put_debit_spread",
        "direction": "down",
        "thesis": "breakout",
    }

    rendered = render_operator_status_brief(payload, [], NOW)

    assert "Desk View  READY · SHORT / PUT BREAKOUT · 状态：执行候选已就绪" in rendered
    assert "Execution  READY · 独立 MANUAL READY 卡承载实时合约与报价" in rendered
    assert "买入  " not in rendered
    assert "限价  " not in rendered


@pytest.mark.parametrize(
    "authority_fields",
    (
        {},
        {
            "manual_action_eligible": False,
            "operator_notification_eligible": True,
            "edge_authority": "validated_first_touch_time_stop_net_pnl",
        },
        {
            "manual_action_eligible": True,
            "operator_notification_eligible": False,
            "edge_authority": "validated_first_touch_time_stop_net_pnl",
        },
        {
            "manual_action_eligible": True,
            "operator_notification_eligible": True,
        },
        {
            "manual_action_eligible": True,
            "operator_notification_eligible": True,
            "edge_authority": "unvalidated_expiry_payoff_geometry",
        },
    ),
    ids=(
        "legacy",
        "manual-action-false",
        "notification-false",
        "authority-missing",
        "authority-malformed",
    ),
)
def test_operator_status_rejects_unauthorized_manual_ready_projection(
    authority_fields: dict[str, object],
) -> None:
    payload = _payload()
    payload["level_decision"] = {
        **payload["level_decision"],  # type: ignore[dict-item]
        "phase": "confirmed",
        "direction": "up",
        "thesis": "breakout",
    }
    payload["gth_level_manual_candidate"] = {
        "status": "manual_ready",
        "source_signal_id": "level:test-current",
        "candidate_id": "gth:unauthorized",
        "direction": "up",
        "thesis": "breakout",
        **authority_fields,
    }

    projection = build_desk_map_projection(payload)
    rendered = render_operator_status_brief(payload, [], NOW)

    assert projection.stage.value == "PAUSED"
    assert "ready_opportunity_mismatch" in projection.quality_reasons
    assert "Execution  READY" not in rendered
    assert "状态：执行候选已就绪" not in rendered
    assert "结构与执行门控已通过" not in rendered
    assert "GTH 手工候选执行权限契约不完整，已失效关闭" in rendered


def test_operator_status_keeps_fully_quoted_structure_watch_out_of_ready() -> None:
    payload = _payload()
    payload["level_decision"] = {
        **payload["level_decision"],  # type: ignore[dict-item]
        "phase": "confirmed",
        "thesis": "breakout",
        "direction": "up",
        "level_kind": "call_wall",
        "level": 7730.0,
    }
    payload["gth_level_manual_candidate"] = {
        "status": "structure_watch",
        "source_signal_id": "level:test-current",
        "manual_action_eligible": False,
        "operator_notification_eligible": False,
        "edge_authority": "none",
        "edge_authority_reason": (
            "first_touch_time_stop_net_pnl_authority_unavailable"
        ),
        "long_contract_id": "option:SPX:SPXW:20260715:7730:C",
        "short_contract_id": "option:SPX:SPXW:20260715:7770:C",
        "decision_bid": 15.20,
        "decision_ask": 15.60,
        "entry_limit": 15.60,
        "target_spx": 7770.0,
    }

    projection = build_desk_map_projection(payload)
    rendered = render_operator_status_brief(payload, [], NOW)

    assert projection.stage.value == "CONFIRMED"
    assert "Desk View  HOLD · LONG / CALL BREAKOUT" in rendered
    assert "Execution  HOLD · 方向已确认，执行门控未完成" in rendered
    assert "缺少经验证的首次触及/时间退出净收益权限" in rendered
    assert "Execution  READY" not in rendered
    assert "状态：执行候选已就绪" not in rendered
    assert "MANUAL READY" not in rendered
    assert "买入  SPXW" not in rendered
    assert "限价  净借记" not in rendered


def test_operator_status_brief_labels_frozen_and_live_levels_separately() -> None:
    payload = _payload()
    payload["flip_zone"] = [7570.0, 7575.0]
    payload["candidates"] = [
        {"play": "put_wall_bounce_call", "level": 7540.0},
        {"play": "call_wall_fade_put", "level": 7610.0},
    ]

    rendered = render_operator_status_brief(payload, [], NOW)

    assert ("Structure  event 7550 / 7560–7565 / 7600 · live 7540 / 7570–7575 / 7610") in rendered


def test_status_llm_reason_validation_rejects_authority_or_multiline_changes() -> None:
    assert status_explanation_output_valid("原因  新结构仍在确认")
    assert not status_explanation_output_valid("原因  新结构仍在确认\n买入 Put")
    assert not status_explanation_output_valid("原因  EXECUTION_ELIGIBLE=YES")
    assert not status_explanation_output_valid("原因  建议限价开仓")
