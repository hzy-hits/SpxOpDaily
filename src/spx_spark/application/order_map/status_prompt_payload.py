"""Bounded deterministic payloads for the status-report LLM writer."""

from __future__ import annotations

from typing import Any

from spx_spark.application.order_map import guidance as guidance_module
from spx_spark.application.order_map.convexity_idea_presentation import (
    compact_convexity_idea_radar,
)
from spx_spark.application.order_map.exposure_presentation import (
    compact_exposure_context,
)
from spx_spark.application.order_map.put_candidate_presentation import (
    build_put_candidate_report,
    presentable_plan_candidates,
    put_wall_breakdown_report_disabled,
)
from spx_spark.application.order_map.spring_gamma_presentation import (
    spring_gamma_v3_writer_summary,
)


def build_status_writer_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Keep status prompts bounded so the CLI behaves like a one-shot LLM API."""

    keys = (
        "beijing_time",
        "expiry",
        "session_phase",
        "research_only",
        "analysis_mode",
        "pricing_reference",
        "level_decision",
        "regime_decision",
        "breakout_filter",
        "session_episode",
        "trade_candidate",
        "confirmed_gate",
        "call_skew_spread_shadow",
        "put_skew_spread_shadow",
        "spring_gamma_v3_shadow",
        "spring_gamma_v3_state_window",
        "convexity_idea_radar",
        "convexity_idea_critic",
        "warnings",
    )
    compact = {key: payload.get(key) for key in keys if key in payload}
    compact["put_candidate_report"] = build_put_candidate_report(payload)
    for shadow_key in ("call_skew_spread_shadow", "put_skew_spread_shadow"):
        shadow = compact.get(shadow_key)
        if not isinstance(shadow, dict):
            continue
        candidate = shadow.get("candidate")
        compact[shadow_key] = {
            key: shadow.get(key)
            for key in ("status", "reason", "automatic_ordering", "operator_action")
        }
        if isinstance(candidate, dict):
            compact[shadow_key]["candidate"] = {
                key: candidate.get(key)
                for key in (
                    "strategy",
                    "long",
                    "short",
                    "executable_debit",
                    "fair_debit",
                    "edge_points",
                    "iv_fit",
                    "defined_risk",
                    "execution",
                )
            }
    spring_gamma_shadow = compact.get("spring_gamma_v3_shadow")
    spring_gamma_summary = spring_gamma_v3_writer_summary(spring_gamma_shadow)
    if spring_gamma_summary is not None:
        compact["spring_gamma_v3_shadow"] = spring_gamma_summary
    radar_summary = compact_convexity_idea_radar(compact.get("convexity_idea_radar"))
    if radar_summary is not None:
        compact["convexity_idea_radar"] = radar_summary
    compact["decision_guidance"] = status_decision_guidance(payload)
    signed_gex = payload.get("signed_gex_proxy")
    if isinstance(signed_gex, dict):
        compact["signed_gex_proxy"] = {
            key: signed_gex.get(key)
            for key in (
                "net_gex",
                "abs_gex",
                "net_gamma_ratio",
                "gamma_state",
                "weighting",
                "sign_method",
                "dealer_position_sign",
            )
        }
    strike_coverage = payload.get("strike_price_coverage")
    if isinstance(strike_coverage, dict):
        compact["strike_price_coverage"] = {
            key: strike_coverage.get(key)
            for key in (
                "expiry",
                "reference_price",
                "center_strike",
                "strike_step_points",
                "radius_strikes",
                "target_pair_count",
                "complete_pair_count",
                "core_complete_pair_count",
                "rotation_assisted_pair_count",
                "missing_call_count",
                "missing_put_count",
                "coverage_ratio",
                "coverage_confidence_95_low",
                "coverage_confidence_95_high",
                "pair_quote_age_p50_seconds",
                "pair_quote_age_p90_seconds",
                "pair_quote_age_max_seconds",
                "complete_min_strike",
                "complete_max_strike",
                "radius_points",
                "point_target_pair_count",
                "point_complete_pair_count",
                "point_coverage_ratio",
                "price_contract",
                "nbbo_interpolation",
                "smoothing_scope",
            )
        }
    exposure_context = compact_exposure_context(payload)
    if exposure_context:
        compact["exposure_context"] = exposure_context
    classified = "plan_candidates" in payload
    plans = presentable_plan_candidates(payload)
    observations = payload.get("observation_candidates")
    candidates = (
        plans
        if isinstance(plans, list) and plans
        else observations
        if classified and isinstance(observations, list)
        else payload.get("candidates")
    )
    if isinstance(candidates, list):
        candidate_keys = (
            "intent_id",
            "contract_id",
            "play",
            "level_label",
            "level",
            "strike",
            "right",
            "prob_touch",
            "projection_range_low",
            "projection_range_high",
            "execution_quote_status",
            "order_style",
            "decision_bid",
            "decision_ask",
            "limit_aggressive",
            "invalidation_spx",
            "target_spx",
            "intent_expires_at",
            "automatic_ordering",
        )
        key = (
            "plan_candidates"
            if isinstance(plans, list) and plans
            else ("observation_candidates" if classified else "candidates")
        )
        compact[key] = [
            {key: item.get(key) for key in candidate_keys if key in item}
            for item in candidates[:2]
            if isinstance(item, dict)
        ]
    if classified:
        compact["candidate_presentation"] = payload.get("candidate_presentation")
    return compact


def status_decision_guidance(payload: dict[str, Any]) -> dict[str, object]:
    guidance = guidance_module.build_decision_guidance(_guidance_payload(payload)).to_dict()
    if not put_wall_breakdown_report_disabled(payload):
        return guidance
    return {
        **guidance,
        "action": "paused",
        "action_text": "Put Wall跌破 disabled/unsupported；不生成 Put 执行计划",
        "trigger_text": "仅保留 Flip Low跌破或 Call Wall/Flip High拒绝为独立 Put 候选",
        "invalidation_text": "Put Wall仍是结构定位，不与两类支持的 Put 假设混算",
        "gate_reason": "put_wall_breakdown_disabled_unsupported",
    }


def status_guidance_lines(payload: dict[str, Any]) -> list[str]:
    if not put_wall_breakdown_report_disabled(payload):
        return guidance_module.compact_guidance_lines(_guidance_payload(payload))
    guidance = status_decision_guidance(payload)
    return [
        "结论  NO TRADE · Put Wall跌破策略未支持",
        f"动作  {guidance['action_text']}",
        "观察  Put Wall仅作结构定位，不是可执行 Put 策略",
        f"确认  {guidance['trigger_text']}",
        f"证伪  {guidance['invalidation_text']}",
    ]


def _guidance_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        **payload,
        "plan_candidates": presentable_plan_candidates(payload),
    }
