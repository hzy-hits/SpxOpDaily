from __future__ import annotations

from datetime import datetime, timedelta

from spx_spark.application.order_map.convexity_idea_radar import (
    build_convexity_idea_radar,
)
from spx_spark.application.order_map.convexity_idea_presentation import (
    compact_convexity_idea_radar,
    render_convexity_idea_radar_lines,
)
from spx_spark.application.order_map.pricing_audit import build_pricing_audit_record
from spx_spark.application.order_map.prompts import (
    _status_writer_payload,
    render_operator_status_brief,
)
from spx_spark.market_calendar import ET


def _payload() -> dict[str, object]:
    return {
        "as_of": datetime(2026, 7, 24, 10, 0, tzinfo=ET).isoformat(),
        "trading_date": "2026-07-24",
        "expiry": "20260724",
        "underlier": {"price": 7409.9, "source": "index:SPX"},
        "rn_density": {
            "quality": "ok",
            "p10": 7368.123,
            "p25": 7388.456,
            "median": 7412.789,
            "p75": 7438.123,
            "p90": 7461.987,
            "prob_below_put_wall": 0.11,
            "prob_above_call_wall": 0.14,
        },
        "expected_move_points": 52.345,
        "day_move": {"em_used_fraction": 0.74},
        "level_decision": {
            "event_id": "flip-high:test",
            "phase": "TESTING",
            "level_kind": "flip_high",
            "level": 7410.0,
            "levels": {
                "put_wall": 7345.0,
                "flip_low": 7395.0,
                "flip_high": 7410.0,
                "call_wall": 7465.0,
            },
            "quality_ok": True,
        },
        "spring_gamma_v3_shadow": {
            "status": "ready",
            "as_of": datetime(2026, 7, 24, 10, 0, tzinfo=ET).isoformat(),
            "session_id": "2026-07-24",
            "expiry": "20260724",
            "calibration_status": "warming",
            "direction": {
                "decision": "abstain",
                "diagnostic_es_direction": "down",
                "p_up": 0.73,
                "p_down": 0.27,
                "composite_score": 0.31,
            },
            "rth_market_state": {
                "state": "UNCERTAIN",
                "status": "ready",
                "D": 3,
                "Q": {"efficiency_ratio": 0.77, "vwap_cross_count": 0},
                "V": {"same_time_range_ratio": None},
                "input_availability": {"available_count": 8, "required_count": 8},
                "direction_components": {
                    "price_vs_vwap": 2,
                    "market_structure": -2,
                },
                "input_lineage": {
                    "diagnostics": {
                        "rolling_path_percentiles": {
                            "status": "provisional",
                            "slot_et": "10:00",
                            "window_minutes": 30,
                            "sample_count": 12,
                            "latest_bar_end": datetime(
                                2026,
                                7,
                                24,
                                10,
                                0,
                                tzinfo=ET,
                            ).isoformat(),
                            "minimum_sessions": 5,
                            "target_sessions": 20,
                            "confidence": "medium",
                            "dip": {
                                "value_atr": 0.55,
                                "raw_percentile": 0.75,
                                "shrunk_percentile": 0.65,
                                "sample_count": 12,
                            },
                            "rally": {
                                "value_atr": 0.25,
                                "raw_percentile": 0.25,
                                "shrunk_percentile": 0.35,
                                "sample_count": 12,
                            },
                            "signed_path_bias": -0.30,
                            "shrinkage": "linear_to_50pct_until_20_prior_sessions",
                            "probability_semantics": (
                                "historical_rank_not_forward_probability"
                            ),
                            "action_authority": "none",
                        },
                        "moving_averages": {
                            "status": "ready",
                            "price": 7454.9,
                            "sma20": 7452.0,
                            "sma50": 7448.0,
                            "sma200": 7455.2,
                            "atr_5m": 8.0,
                            "distance_to_sma20_points": 2.9,
                            "distance_to_sma50_points": 6.9,
                            "distance_to_sma200_points": -0.3,
                            "distance_to_sma50_atr": 0.8625,
                            "distance_to_sma200_atr": -0.0375,
                            "ma50_slope_3_atr": 0.22,
                            "ma50_slope_6_atr": 0.35,
                            "ma200_slope_3_atr": -0.01,
                            "ma200_slope_6_atr": -0.04,
                            "ma50_ma200_spread_points": -7.2,
                            "ma50_ma200_spread_atr": -0.9,
                            "spread_change_3_atr": 0.23,
                            "cross_direction": "death",
                            "bars_since_cross": 108,
                            "cross_persistent_2_bars": True,
                            "cross_fresh": False,
                            "regime_state": "REGIME_TRANSITION",
                            "regime_direction": "up",
                            "same_direction_convexity": (
                                "wait_for_wall_confirmation"
                            ),
                            "relation": "price_above_sma50_below_sma200",
                            "spx_equivalent_sma20": 7407.0,
                            "spx_equivalent_sma50": 7403.0,
                            "spx_equivalent_sma200": 7410.2,
                            "basis_contract_identity_matches_sma": True,
                            "action_authority": "none",
                        }
                    }
                },
            },
            "option_overlay": {"status": "ready", "reasons": []},
            "wall_probability": {
                "probability_status": "ready",
                "probability_semantics": "risk_neutral_not_physical",
                "touch_probability_semantics": "reflection_heuristic_not_calibrated",
                "wall_probabilities": {
                    "15m": {
                        "flip_high": {
                            "status": "available",
                            "level": 7410.0,
                            "prob_close_beyond": 0.12,
                            "prob_touch": 0.24,
                            "source_iv": 0.19,
                            "source_quote_age_seconds": 1.25,
                            "planned_exit_at": datetime(
                                2026, 7, 24, 10, 15, tzinfo=ET
                            ).isoformat(),
                        }
                    },
                    "30m": {
                        "call_wall": {
                            "status": "available",
                            "level": 7465.0,
                            "prob_close_beyond": 0.04,
                            "prob_touch": 0.08,
                            "source_iv": 0.20,
                            "source_quote_age_seconds": 1.75,
                            "planned_exit_at": datetime(
                                2026, 7, 24, 10, 30, tzinfo=ET
                            ).isoformat(),
                        }
                    },
                },
            },
        },
        "strike_price_coverage": {
            "complete_pair_count": 81,
            "target_pair_count": 61,
            "pair_quote_age_p90_seconds": 2.34,
            "nbbo_interpolation": False,
        },
        "pricing_reference": {
            "pricing_allowed": True,
            "gate_state": "live",
        },
        "call_skew_spread_shadow": {
            "status": "candidate",
            "candidate": {
                "strategy": "call_debit_spread",
                "long": {
                    "contract_id": "SPXW:20260724:C:7410",
                    "strike": 7410.0,
                    "right": "C",
                    "bid": 16.8,
                    "ask": 17.2,
                    "iv": 0.19,
                },
                "short": {
                    "contract_id": "SPXW:20260724:C:7420",
                    "strike": 7420.0,
                    "right": "C",
                    "bid": 11.2,
                    "ask": 11.6,
                    "iv": 0.205,
                },
                "executable_debit": 6.0,
                "fair_debit": 6.55,
                "edge_points": 0.55,
                "iv_fit": {},
                "defined_risk": True,
            },
        },
        "put_skew_spread_shadow": {
            "status": "no_candidate",
            "reason": "no_local_skew_edge",
            "candidate": None,
        },
        "candidates": [
            {
                "contract_id": "SPXW:20260724:C:7410",
                "play": "flip_reclaim",
                "level": 7410.0,
                "strike": 7410.0,
                "right": "C",
                "current_mid": 17.0,
                "prob_touch": 0.44,
                "execution_quote_status": "live",
            },
            {
                "contract_id": "SPXW:20260724:P:7465",
                "play": "call_wall_rejection",
                "level": 7465.0,
                "strike": 7465.0,
                "right": "P",
                "current_mid": 55.0,
                "prob_touch": 0.18,
                "execution_quote_status": "live",
            },
        ],
    }


def test_radar_keeps_both_boundaries_and_both_option_sides_before_exit() -> None:
    radar = build_convexity_idea_radar(
        _payload(),
        now=datetime(2026, 7, 24, 10, 0, tzinfo=ET),
    )

    assert radar["status"] == "ready"
    assert radar["mandate"]["instrument"] == "SPXW_0DTE_long_convexity"
    assert radar["mandate"]["analysis_start_et"] == "09:45"
    assert radar["mandate"]["minutes_to_hard_exit"] == 180
    assert radar["mandate"]["new_idea_generation_allowed"] is True
    assert radar["gth_prior"]["status"] == "unavailable"
    assert radar["boundary_tests"]["lower"]["name"] == "flip_low"
    assert radar["boundary_tests"]["upper"]["name"] == "flip_high"
    assert {
        row["scenario"] for row in radar["hypotheses"]
    } == {
        "lower_rejection_call",
        "lower_acceptance_put",
        "upper_rejection_put",
        "upper_acceptance_call",
    }
    assert {row["option_right"] for row in radar["hypotheses"]} == {"C", "P"}
    assert radar["action_authority"] == "none"
    assert radar["automatic_ordering"] is False
    moving = radar["market_state"]["moving_averages"]
    assert moving["regime_state"] == "REGIME_TRANSITION"
    assert moving["same_direction_convexity"] == "wait_for_wall_confirmation"
    confluence = moving["ma200_structure_confluence"]
    assert confluence["status"] == "ready"
    assert confluence["nearest_kind"] == "flip_high"
    assert confluence["nearest_level"] == 7410.0
    assert confluence["distance_points"] == 0.2
    assert confluence["distance_atr"] == 0.02
    assert confluence["decision_zone"] is True
    assert confluence["entry_trigger"] is False
    probability = radar["boundary_tests"]["risk_neutral_wall_probabilities"]
    assert probability["horizons"]["15m"]["flip_high"]["prob_touch"] == 0.24
    assert probability["horizons"]["30m"]["call_wall"]["prob_touch"] == 0.08
    assert probability["to_1300"]["status"] == "not_calculated"


def test_radar_always_emits_three_non_executable_opportunity_lanes() -> None:
    radar = build_convexity_idea_radar(
        _payload(),
        now=datetime(2026, 7, 24, 10, 0, tzinfo=ET),
    )

    board = radar["opportunity_board"]
    assert board["status"] == "observing"
    assert set(board["lanes"]) == {"call", "put", "vol_range"}
    assert set(board["rank_order"]) == {"call", "put", "vol_range"}
    assert board["action_authority"] == "none"
    assert board["actionable"] is False
    assert board["automatic_ordering"] is False
    for name, lane in board["lanes"].items():
        assert lane["lane"] == name
        assert lane["execution"]["eligible"] is False
        assert "dense_shadow_no_execution_authority" in (
            lane["execution"]["block_reasons"]
        )
        assert lane["action_authority"] == "none"
        assert lane["actionable"] is False
        assert lane["automatic_ordering"] is False


def test_radar_renders_small_sample_path_rank_and_all_three_lanes() -> None:
    radar = build_convexity_idea_radar(
        _payload(),
        now=datetime(2026, 7, 24, 10, 0, tzinfo=ET),
    )

    board = radar["opportunity_board"]
    assert board["sample_policy"] == {
        "minimum_prior_sessions": 5,
        "target_prior_sessions": 20,
        "below_target_behavior": "emit_shrunk_low_confidence_shadow",
        "execution_gate_affected": False,
    }
    assert board["path_percentiles"]["status"] == "provisional"
    assert board["path_percentiles"]["confidence"] == "medium"
    assert board["path_percentiles"]["sample_count"] == 12
    assert board["path_percentiles"]["dip"]["raw_percentile"] == 0.75
    assert board["path_percentiles"]["dip"]["shrunk_percentile"] == 0.65
    assert board["path_percentiles"]["rally"]["raw_percentile"] == 0.25
    assert board["path_percentiles"]["rally"]["shrunk_percentile"] == 0.35

    lines = render_convexity_idea_radar_lines({"convexity_idea_radar": radar})
    path_lines = [line for line in lines if line.startswith("30m路径分位")]
    opportunity_lines = [line for line in lines if line.startswith("机会[")]

    assert len(path_lines) == 1
    assert "75.00%→65.00%" in path_lines[0]
    assert "25.00%→35.00%" in path_lines[0]
    assert "n=12/20" in path_lines[0]
    assert "medium" in path_lines[0]
    assert "历史排名非预测概率" in path_lines[0]
    assert len(opportunity_lines) == 3
    assert {line.split("]", 1)[0] + "]" for line in opportunity_lines} == {
        "机会[Call]",
        "机会[Put]",
        "机会[Vol/Range]",
    }
    assert all("EXECUTION_ELIGIBLE=NO" in line for line in opportunity_lines)


def test_radar_accepts_only_identity_matched_causal_path_fallback() -> None:
    payload = _payload()
    current = payload["spring_gamma_v3_shadow"]["rth_market_state"]["input_lineage"][
        "diagnostics"
    ]["rolling_path_percentiles"]
    current.clear()
    current.update(
        {
            "status": "warming",
            "sample_count": 0,
            "confidence": "unavailable",
            "action_authority": "none",
        }
    )
    source_as_of = datetime(2026, 7, 24, 9, 56, tzinfo=ET)
    latest_bar_end = datetime(2026, 7, 24, 9, 55, tzinfo=ET)
    payload["spring_gamma_v3_path_fallback"] = {
        "schema_version": "spring_gamma_v3_path_fallback.v1",
        "session_id": "2026-07-24",
        "expiry": "20260724",
        "source_as_of": source_as_of.isoformat(),
        "source_latest_bar_end": latest_bar_end.isoformat(),
        "maximum_age_seconds": 900.0,
        "rolling_path_percentiles": {
            "status": "provisional",
            "slot_et": "09:55",
            "sample_count": 13,
            "target_sessions": 20,
            "confidence": "low",
            "input_quality": "stale_fallback",
            "latest_bar_end": latest_bar_end.isoformat(),
            "source_latest_bar_end": latest_bar_end.isoformat(),
            "source_lag_seconds": 300.0,
            "dip": {"shrunk_percentile": 0.76},
            "rally": {"shrunk_percentile": 0.29},
            "signed_path_bias": -0.47,
            "action_authority": "none",
        },
        "action_authority": "none",
        "actionable": False,
        "automatic_ordering": False,
    }

    radar = build_convexity_idea_radar(
        payload,
        now=datetime(2026, 7, 24, 10, 0, tzinfo=ET),
    )

    path = radar["opportunity_board"]["path_percentiles"]
    assert path["input_quality"] == "stale_fallback"
    assert path["confidence"] == "low"
    assert path["signed_path_bias"] == -0.47
    assert path["source_lag_seconds"] == 300.0


def test_radar_rejects_stale_current_path_and_future_fallback_modifier() -> None:
    payload = _payload()
    current = payload["spring_gamma_v3_shadow"]["rth_market_state"]["input_lineage"][
        "diagnostics"
    ]["rolling_path_percentiles"]
    current["latest_bar_end"] = datetime(
        2026,
        7,
        24,
        9,
        39,
        tzinfo=ET,
    ).isoformat()
    payload["spring_gamma_v3_path_fallback"] = {
        "schema_version": "spring_gamma_v3_path_fallback.v1",
        "session_id": "2026-07-24",
        "expiry": "20260724",
        "source_as_of": datetime(
            2026,
            7,
            24,
            10,
            0,
            1,
            tzinfo=ET,
        ).isoformat(),
        "source_latest_bar_end": datetime(
            2026,
            7,
            24,
            10,
            0,
            tzinfo=ET,
        ).isoformat(),
        "rolling_path_percentiles": {
            **current,
            "confidence": "low",
            "input_quality": "stale_fallback",
            "source_latest_bar_end": datetime(
                2026,
                7,
                24,
                10,
                0,
                tzinfo=ET,
            ).isoformat(),
        },
        "action_authority": "none",
        "actionable": False,
        "automatic_ordering": False,
    }

    radar = build_convexity_idea_radar(
        payload,
        now=datetime(2026, 7, 24, 10, 0, tzinfo=ET),
    )

    path = radar["opportunity_board"]["path_percentiles"]
    assert path["status"] == "unavailable"
    assert path["reason"] == "rolling_path_freshness_or_identity_invalid"
    for lane in ("call", "put"):
        assert all(
            row["feature"] != "rolling_path_bias"
            for row in radar["opportunity_board"]["lanes"][lane][
                "score_contributions"
            ]
        )


def test_radar_separates_risk_neutral_destination_from_physical_probability() -> None:
    radar = build_convexity_idea_radar(
        _payload(),
        now=datetime(2026, 7, 24, 10, 0, tzinfo=ET),
    )
    destination = radar["destination_map"]

    assert destination["median"] == 7412.79
    assert destination["terminal_time_et"] == "16:00"
    assert destination["mark_1300_proxy"]["status"] == "unavailable"
    assert destination["probability_semantics"] == "risk_neutral_terminal_not_physical"
    assert (
        radar["semantics"]["destination_distribution"]
        == "option_implied_risk_neutral_terminal_distribution_not_physical_forecast"
    )
    lines = render_convexity_idea_radar_lines({"convexity_idea_radar": radar})
    assert "0DTE 16:00期权隐含" in "\n".join(lines)
    assert "7368.12/7412.79/7461.99" in "\n".join(lines)
    assert "风险中性而非真实胜率" in "\n".join(lines)
    assert "上 15/30/60m 24.00%/-/-" in "\n".join(lines)
    rendered = "\n".join(lines)
    assert "MA50/200背景  REGIME_TRANSITION/up" in rendered
    assert "必须等待wall/flip接受或拒绝确认" in rendered
    assert (
        "MA200×结构位  SPX基差投影邻近 Flip High 7410.00 · "
        "距离 0.20点/0.02ATR · 决策区 是"
    ) in rendered
    assert "不生成方向/入场（非SPX自身MA200）" in rendered


def test_radar_ma200_wall_confluence_is_explicitly_unavailable_without_atr() -> None:
    payload = _payload()
    moving = payload["spring_gamma_v3_shadow"]["rth_market_state"]["input_lineage"][
        "diagnostics"
    ]["moving_averages"]
    moving["atr_5m"] = None

    radar = build_convexity_idea_radar(
        payload,
        now=datetime(2026, 7, 24, 10, 0, tzinfo=ET),
    )
    confluence = radar["market_state"]["moving_averages"][
        "ma200_structure_confluence"
    ]

    assert confluence["status"] == "unavailable"
    assert confluence["reason"] == "atr_5m_unavailable"
    assert confluence["decision_zone"] is None
    rendered = "\n".join(
        render_convexity_idea_radar_lines({"convexity_idea_radar": radar})
    )
    assert "MA200×结构位  unavailable（atr_5m_unavailable）" in rendered
    assert "不得补算共振或方向" in rendered


def test_radar_only_names_observed_local_skew_edge_as_evidence() -> None:
    radar = build_convexity_idea_radar(
        _payload(),
        now=datetime(2026, 7, 24, 10, 0, tzinfo=ET),
    )

    assert radar["option_evidence"]["call"]["edge_status"] == "observed_local_skew_edge"
    assert radar["option_evidence"]["call"]["claim_allowed"] == "observed_local_skew_edge_only"
    assert radar["option_evidence"]["put"]["edge_status"] == "not_observed"
    assert radar["option_evidence"]["put"]["claim_allowed"] == "no_mispricing_claim"
    rendered = "\n".join(render_convexity_idea_radar_lines({"convexity_idea_radar": radar}))
    assert "局部skew边际 0.55点（借记 6.00）" in rendered
    assert "Put 未发现可执行局部skew边际" in rendered


def test_radar_gives_llm_iv_skew_and_realized_range_without_direction_authority() -> None:
    payload = _payload()
    payload["option_structure_frame"] = {
        "front_expiry": "20260724",
        "volatility": {
            "atm_iv_0dte": 0.1823,
            "atm_iv_1dte": 0.1945,
            "put_skew_25d_0dte": 0.0212,
            "call_skew_25d_0dte": -0.0084,
            "term_gap": -0.0122,
        },
    }
    payload["spring_gamma_v3_shadow"]["rth_market_state"]["V"][
        "same_time_range_ratio"
    ] = 1.34

    radar = build_convexity_idea_radar(
        payload,
        now=datetime(2026, 7, 24, 10, 0, tzinfo=ET),
    )

    volatility = radar["volatility_context"]
    assert volatility["atm_iv_0dte"] == 0.1823
    assert volatility["put_skew_25d_0dte"] == 0.0212
    assert volatility["same_time_range_ratio"] == 1.34
    assert volatility["remaining_implied_move_to_1300"]["status"] == "unavailable"
    assert volatility["action_authority"] == "none"
    rendered = "\n".join(render_convexity_idea_radar_lines({"convexity_idea_radar": radar}))
    assert "ATM IV 0/1DTE 18.23%/19.45%" in rendered
    assert "25Δ P/C skew +2.12%/-0.84%" in rendered
    assert "同刻区间比 1.34x" in rendered


def test_radar_exposes_model_conflicts_as_human_questions() -> None:
    radar = build_convexity_idea_radar(
        _payload(),
        now=datetime(2026, 7, 24, 12, 59, tzinfo=ET),
    )

    assert radar["mandate"]["minutes_to_hard_exit"] == 1
    assert {row["kind"] for row in radar["tensions"]} == {
        "direction_model_conflict",
        "market_structure_vs_vwap_conflict",
        "clean_path_but_classification_uncertain",
        "gth_expected_move_largely_consumed",
    }


def test_radar_closes_new_ideas_at_hard_exit() -> None:
    radar = build_convexity_idea_radar(
        _payload(),
        now=datetime(2026, 7, 24, 13, 0, tzinfo=ET),
    )

    assert radar["status"] == "closed"
    assert radar["mandate"]["phase"] == "hard_exit_reached"
    assert radar["mandate"]["new_idea_generation_allowed"] is False
    assert radar["mandate"]["position_must_be_flat"] is True
    assert {row["status"] for row in radar["hypotheses"]} == {"closed"}
    assert radar["opportunity_board"]["status"] == "closed"
    assert set(radar["opportunity_board"]["lanes"]) == {
        "call",
        "put",
        "vol_range",
    }
    lines = render_convexity_idea_radar_lines({"convexity_idea_radar": radar})
    assert len([line for line in lines if line.startswith("机会[")]) == 3


def test_radar_waits_until_0945_after_open() -> None:
    radar = build_convexity_idea_radar(
        _payload(),
        now=datetime(2026, 7, 24, 9, 40, tzinfo=ET),
    )

    assert radar["status"] == "warming"
    assert radar["mandate"]["phase"] == "rth_warmup"
    assert radar["mandate"]["new_idea_generation_allowed"] is False
    assert {row["status"] for row in radar["hypotheses"]} == {"closed"}


def test_radar_cannot_be_ready_with_warming_or_incomplete_market_state() -> None:
    payload = _payload()
    state = payload["spring_gamma_v3_shadow"]["rth_market_state"]
    state["status"] = "warming"
    state["input_availability"] = {"available_count": 7, "required_count": 8}

    radar = build_convexity_idea_radar(
        payload,
        now=datetime(2026, 7, 24, 10, 0, tzinfo=ET),
    )

    assert radar["status"] == "partial"
    assert "rth_market_state_not_ready" in radar["data_quality"]["reasons"]
    assert "rth_market_state_inputs_incomplete" in radar["data_quality"]["reasons"]


def test_radar_selects_true_nearest_levels_on_each_side_of_spot() -> None:
    payload = _payload()
    payload["underlier"] = {"price": 7470.0, "source": "index:SPX"}

    radar = build_convexity_idea_radar(
        payload,
        now=datetime(2026, 7, 24, 10, 0, tzinfo=ET),
    )

    assert radar["boundary_tests"]["lower"]["name"] == "call_wall"
    assert radar["boundary_tests"]["lower"]["level"] == 7465.0
    assert radar["boundary_tests"]["upper"]["status"] == "unavailable"
    assert (
        radar["boundary_tests"]["upper"]["reason"]
        == "no_structure_level_on_upper_side_of_spot"
    )


def test_weekend_is_inactive_not_an_artificial_multi_day_gth_window() -> None:
    payload = _payload()
    weekend = datetime(2026, 7, 25, 9, 0, tzinfo=ET)
    payload["as_of"] = weekend.isoformat()
    payload["expiry"] = "20260727"

    radar = build_convexity_idea_radar(payload, now=weekend)

    assert radar["status"] == "inactive"
    assert radar["mandate"]["phase"] == "outside_strategy_window"
    assert radar["mandate"]["minutes_to_hard_exit"] is None
    assert radar["mandate"]["new_idea_generation_allowed"] is False
    assert {row["status"] for row in radar["hypotheses"]} == {"closed"}
    assert radar["opportunity_board"]["status"] == "inactive"
    assert set(radar["opportunity_board"]["lanes"]) == {
        "call",
        "put",
        "vol_range",
    }
    lines = render_convexity_idea_radar_lines({"convexity_idea_radar": radar})
    assert len([line for line in lines if line.startswith("机会[")]) == 3


def test_actual_sunday_evening_gth_prepares_monday_session() -> None:
    payload = _payload()
    gth = datetime(2026, 7, 26, 20, 30, tzinfo=ET)
    payload["as_of"] = gth.isoformat()
    payload["expiry"] = "20260727"
    shadow = payload["spring_gamma_v3_shadow"]
    shadow["as_of"] = gth.isoformat()
    shadow["expiry"] = "20260727"
    wall = shadow["wall_probability"]["wall_probabilities"]
    wall["15m"]["flip_high"]["planned_exit_at"] = (
        gth.replace(minute=45).isoformat()
    )
    wall["30m"]["call_wall"]["planned_exit_at"] = (
        gth.replace(hour=21, minute=0).isoformat()
    )

    radar = build_convexity_idea_radar(payload, now=gth)

    assert radar["status"] == "preparation"
    assert radar["mandate"]["phase"] == "gth_preparation"
    assert radar["mandate"]["trading_date"] == "2026-07-27"
    assert radar["mandate"]["new_idea_generation_allowed"] is True
    assert radar["gth_prior"]["status"] == "live_context_not_frozen"
    assert radar["gth_prior"]["frozen_at"] is None


def test_gth_radar_keeps_two_sided_live_es_observations_when_options_degrade() -> None:
    payload = _payload()
    gth = datetime(2026, 7, 26, 20, 30, tzinfo=ET)
    payload["as_of"] = gth.isoformat()
    payload["expiry"] = "20260727"
    payload["globex_trend"] = {
        "session_id": "2026-07-27:gth",
        "regime": "bullish",
        "updated_at": (gth - timedelta(seconds=10)).isoformat(),
        "metrics": {
            "return_15m_points": 7.0,
            "return_60m_points": 19.0,
            "return_180m_points": None,
        },
        "samples": [{"provider": "schwab"}],
    }
    payload["gth_dip_reclaim_signal"] = {
        "kind": "gth_dip_reclaim_call",
        "event_id": "gth-dip:live",
        "session_date": "2026-07-27",
        "confirmed_at": (gth - timedelta(minutes=1)).isoformat(),
        "valid_until": (gth + timedelta(minutes=9)).isoformat(),
        "entry_quality": {
            "mode": "decision_grade",
            "verdict": "pass",
            "block_reasons": [],
        },
    }
    payload["gth_manual_candidate"] = {
        "status": "blocked",
        "candidate_id": "gth-manual:live",
        "source_signal_id": "gth-dip:live",
        "evaluated_at": (gth - timedelta(seconds=5)).isoformat(),
        "block_reasons": ["long_leg_quote_unavailable"],
        "manual_action_eligible": False,
        "automatic_ordering": False,
        "broker_submission_allowed": False,
    }
    payload["spring_gamma_v3_shadow"]["as_of"] = gth.isoformat()
    payload["spring_gamma_v3_shadow"]["expiry"] = "20260727"
    payload["spring_gamma_v3_shadow"]["option_overlay"] = {
        "status": "unavailable",
        "reasons": ["gth_spxw_nbbo_unavailable"],
    }

    radar = build_convexity_idea_radar(payload, now=gth)

    board = radar["opportunity_board"]
    assert board["gth_observation"]["status"] == "ready"
    assert board["lanes"]["call"]["status"] == "observed"
    assert board["lanes"]["put"]["status"] == "observed"
    assert board["lanes"]["call"]["priority"] == "HIGH"
    assert "DIP_RECLAIM_ALIGNED" in board["lanes"]["call"]["gth_signal"]
    assert board["lanes"]["put"]["gth_signal"] == "COUNTER_TREND"
    for side in ("call", "put"):
        assert board["lanes"][side]["execution"]["eligible"] is False
        assert board["lanes"][side]["automatic_ordering"] is False

    lines = render_convexity_idea_radar_lines({"convexity_idea_radar": radar})
    assert any(line.startswith("GTH方向观察") for line in lines)
    opportunity_lines = [line for line in lines if line.startswith("机会[")]
    assert len(opportunity_lines) == 3
    assert any("GTH_SIGNAL=DIP_RECLAIM_ALIGNED" in line for line in opportunity_lines)
    payload["convexity_idea_radar"] = radar
    operator_brief = render_operator_status_brief(payload, [], gth)
    assert "GTH方向观察" in operator_brief
    assert "15/60/180m 7.00/19.00/-" in operator_brief
    assert "schwab" in operator_brief


def test_wall_probability_requires_fresh_shadow_and_unexpired_horizon() -> None:
    payload = _payload()
    now = datetime(2026, 7, 24, 10, 3, tzinfo=ET)
    payload["as_of"] = now.isoformat()

    stale = build_convexity_idea_radar(payload, now=now)
    stale_row = stale["boundary_tests"]["risk_neutral_wall_probabilities"][
        "horizons"
    ]["15m"]["flip_high"]

    assert stale["status"] == "partial"
    assert stale_row["strategy_usable"] is False
    assert stale_row["prob_touch"] is None
    assert "spring_shadow_stale_or_future" in stale_row["strategy_gate_reasons"]

    payload = _payload()
    now = datetime(2026, 7, 24, 10, 20, tzinfo=ET)
    payload["as_of"] = now.isoformat()
    payload["spring_gamma_v3_shadow"]["as_of"] = now.isoformat()
    expired = build_convexity_idea_radar(payload, now=now)
    expired_row = expired["boundary_tests"]["risk_neutral_wall_probabilities"][
        "horizons"
    ]["15m"]["flip_high"]

    assert expired_row["strategy_usable"] is False
    assert expired_row["prob_touch"] is None
    assert "probability_horizon_elapsed" in expired_row["strategy_gate_reasons"]


def test_stale_or_wrong_expiry_density_cannot_masquerade_as_current_0dte() -> None:
    payload = _payload()
    payload["expiry"] = "20260727"
    payload["as_of"] = datetime(2026, 7, 24, 10, 0, tzinfo=ET).isoformat()
    now = datetime(2026, 7, 27, 10, 0, tzinfo=ET)

    radar = build_convexity_idea_radar(payload, now=now)

    destination = radar["destination_map"]
    assert destination["status"] == "unavailable"
    assert destination["p10"] is None
    assert destination["median"] is None
    assert "density_stale_or_future" in destination["gate_reasons"]


def test_destination_uses_calendar_terminal_time_on_early_close() -> None:
    payload = _payload()
    now = datetime(2026, 11, 27, 10, 0, tzinfo=ET)
    payload["as_of"] = now.isoformat()
    payload["expiry"] = "20261127"

    radar = build_convexity_idea_radar(payload, now=now)

    assert radar["mandate"]["terminal_time_et"] == "13:00"
    assert radar["destination_map"]["terminal_time_et"] == "13:00"


def test_wall_probability_horizon_cannot_cross_1300_hard_exit() -> None:
    payload = _payload()
    now = datetime(2026, 7, 24, 12, 30, tzinfo=ET)
    payload["as_of"] = now.isoformat()
    payload["spring_gamma_v3_shadow"]["as_of"] = now.isoformat()
    wall = payload["spring_gamma_v3_shadow"]["wall_probability"]
    wall["wall_probabilities"]["60m"] = {
        "call_wall": {
            "status": "available",
            "level": 7465.0,
            "prob_close_beyond": 0.10,
            "prob_touch": 0.20,
            "source_quote_age_seconds": 1.0,
            "planned_exit_at": datetime(2026, 7, 24, 13, 30, tzinfo=ET).isoformat(),
        }
    }

    radar = build_convexity_idea_radar(payload, now=now)

    row = radar["boundary_tests"]["risk_neutral_wall_probabilities"]["horizons"]["60m"][
        "call_wall"
    ]
    assert row["status"] == "outside_1300_hard_exit"
    assert row["prob_touch"] is None
    compact = compact_convexity_idea_radar(radar)
    assert compact is not None
    assert (
        compact["boundary_tests"]["risk_neutral_wall_probabilities"]["horizons"]["60m"][
            "call_wall"
        ]["status"]
        == "outside_1300_hard_exit"
    )


def test_radar_quality_uses_dependencies_not_unrelated_global_warnings() -> None:
    payload = _payload()
    payload["warnings"] = ["benign lifecycle note"]

    ready = build_convexity_idea_radar(
        payload,
        now=datetime(2026, 7, 24, 10, 0, tzinfo=ET),
    )
    assert ready["status"] == "ready"

    payload["strike_price_coverage"] = {}
    missing_coverage = build_convexity_idea_radar(
        payload,
        now=datetime(2026, 7, 24, 10, 0, tzinfo=ET),
    )
    assert missing_coverage["status"] == "partial"
    assert "complete_cp_pairs_unavailable" in missing_coverage["data_quality"]["reasons"]
    assert "target_cp_pair_count_invalid" in missing_coverage["data_quality"]["reasons"]
    assert "nbbo_interpolation_not_explicitly_false" in (
        missing_coverage["data_quality"]["reasons"]
    )

    payload = _payload()
    payload["strike_price_coverage"]["complete_pair_count"] = 1
    payload["strike_price_coverage"].pop("target_pair_count")
    thin_coverage = build_convexity_idea_radar(
        payload,
        now=datetime(2026, 7, 24, 10, 0, tzinfo=ET),
    )
    assert thin_coverage["status"] == "partial"
    assert "target_cp_pair_count_invalid" in thin_coverage["data_quality"]["reasons"]
    assert "minimum_complete_cp_pairs_not_met" in (
        thin_coverage["data_quality"]["reasons"]
    )

    payload = _payload()
    payload["strike_price_coverage"]["nbbo_interpolation"] = True
    interpolated_nbbo = build_convexity_idea_radar(
        payload,
        now=datetime(2026, 7, 24, 10, 0, tzinfo=ET),
    )
    assert interpolated_nbbo["status"] == "partial"
    assert "nbbo_interpolation_not_explicitly_false" in (
        interpolated_nbbo["data_quality"]["reasons"]
    )

    payload = _payload()
    payload["level_decision"]["quality_ok"] = False
    bad_structure = build_convexity_idea_radar(
        payload,
        now=datetime(2026, 7, 24, 10, 0, tzinfo=ET),
    )
    assert bad_structure["status"] == "partial"
    assert "level_decision_quality_failed" in bad_structure["data_quality"]["reasons"]


def test_compact_writer_packet_preserves_two_sided_context() -> None:
    payload = _payload()
    radar = build_convexity_idea_radar(
        payload,
        now=datetime(2026, 7, 24, 10, 0, tzinfo=ET),
    )
    payload["convexity_idea_radar"] = radar

    compact = compact_convexity_idea_radar(radar)
    writer = _status_writer_payload(payload)["convexity_idea_radar"]

    assert compact is not None
    assert compact["opportunity_board"] == radar["opportunity_board"]
    assert compact["boundary_tests"]["lower"]["level"] == 7395.0
    assert compact["boundary_tests"]["upper"]["level"] == 7410.0
    moving = compact["market_state"]["moving_averages"]
    assert moving["regime_state"] == "REGIME_TRANSITION"
    assert moving["same_direction_convexity"] == "wait_for_wall_confirmation"
    assert moving["ma200_structure_confluence"]["decision_zone"] is True
    assert writer["option_evidence"]["call"]["edge_status"] == "observed_local_skew_edge"
    assert writer["option_evidence"]["put"]["edge_status"] == "not_observed"
    assert writer["opportunity_board"] == radar["opportunity_board"]


def test_pricing_audit_persists_the_exact_radar_for_forward_validation() -> None:
    now = datetime(2026, 7, 24, 10, 0, tzinfo=ET)
    payload = _payload()
    payload["trading_date"] = "2026-07-24"
    payload["convexity_idea_radar"] = build_convexity_idea_radar(payload, now=now)

    audit = build_pricing_audit_record(
        payload,
        generated_at=now,
        report_kind="status",
        template="template",
        delivered_text="delivered",
        writer="template",
        delivered_ok=True,
    )

    assert audit["convexity_idea_radar"] == payload["convexity_idea_radar"]
    assert audit["level_decision"] == payload["level_decision"]
    assert audit["day_move"] == payload["day_move"]
