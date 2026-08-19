from __future__ import annotations

from datetime import datetime
import json
import os
from pathlib import Path
from typing import Any


REPO_ROOT = next(
    (path for path in (Path.cwd(), *Path.cwd().parents) if (path / "src/spx_spark").is_dir()),
    None,
)
if REPO_ROOT is None:
    raise RuntimeError("Run from the spx-spark repository")

RESEARCH_OUTPUT_ROOT = Path(
    os.environ.get(
        "SPX_SPARK_RESEARCH_OUTPUT_ROOT",
        str(REPO_ROOT / "docs/research"),
    )
).expanduser()
RESEARCH_OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
SNAPSHOT_PATH = RESEARCH_OUTPUT_ROOT / "raw-tick-edge-discovery-2026-08-19.snapshot.json"
OUTPUT_PATH = RESEARCH_OUTPUT_ROOT / "raw-tick-edge-discovery-2026-08-19.artifact.json"
snapshot = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))
generated_at = datetime.now().astimezone().isoformat()


def mode_label(mode: str) -> str:
    return mode.upper()


direction_rows: list[dict[str, Any]] = []
direction_session_rows: list[dict[str, Any]] = []
delay_rows: list[dict[str, Any]] = []
option_rows: list[dict[str, Any]] = []
option_session_rows: list[dict[str, Any]] = []
factor_share_rows: list[dict[str, Any]] = []
factor_loading_rows: list[dict[str, Any]] = []
day_label_rows: list[dict[str, Any]] = []
launch_effect_rows: list[dict[str, Any]] = []
event_rows: list[dict[str, Any]] = []
event_option_rows: list[dict[str, Any]] = []
qualifier_rows: list[dict[str, Any]] = []
forward_rows: list[dict[str, Any]] = []
denoising_development_rows: list[dict[str, Any]] = []
denoising_cohort_rows: list[dict[str, Any]] = []
denoising_option_rows: list[dict[str, Any]] = []
denoising_forward_rows: list[dict[str, Any]] = []
gth_hurdle_rows: list[dict[str, Any]] = []
gth_hurdle_stage_rows: list[dict[str, Any]] = []
gex_gate_rows: list[dict[str, Any]] = []
cusum_rows: list[dict[str, Any]] = []
bocpd_rows: list[dict[str, Any]] = []

for mode in ("rth", "gth"):
    direction = snapshot["sealed_test"][mode]
    metrics = direction["metrics"]
    direction_rows.append(
        {
            "mode": mode_label(mode),
            "target": snapshot["data_quality"][mode]["target"],
            "holding_seconds": direction["holding_seconds"],
            "model": direction["model"],
            "feature_block": direction["feature_block"],
            "signals": metrics["signals"],
            "sessions": metrics["sessions"],
            "mean_signed_bps": metrics["mean_signed_bps"],
            "hit_rate": metrics["hit_rate"],
            "ci_low_bps": direction["session_bootstrap_95_bps"][0],
            "ci_high_bps": direction["session_bootstrap_95_bps"][1],
            "holm_p": direction["holm_adjusted_p_across_modes"],
            "validated": direction["validated_directional_edge"],
        }
    )
    for session_date, value in metrics["session_means_bps"].items():
        direction_session_rows.append(
            {"mode": mode_label(mode), "session_date": session_date, "mean_signed_bps": value}
        )
    delay_rows.append(
        {
            "mode": mode_label(mode),
            "delay_seconds": 0,
            "mean_signed_bps": metrics["mean_signed_bps"],
        }
    )
    for delay, delay_metrics in direction["execution_delay_robustness"].items():
        delay_rows.append(
            {
                "mode": mode_label(mode),
                "delay_seconds": int(delay.removesuffix("s")),
                "mean_signed_bps": delay_metrics["mean_signed_bps"],
            }
        )

    joint = snapshot["joint_direction_option_search"][mode]
    option = joint["sealed_test"]
    option_metrics = option["metrics"]
    validation = joint["validation_champion"]
    option_rows.append(
        {
            "mode": mode_label(mode),
            "direction_candidate": option["direction_candidate"],
            "option_structure": option["candidate"].split("::", 1)[1],
            "maximum_relative_spread": joint["validation_champion"][
                "maximum_selection_relative_spread"
            ],
            "validation_lcb_return": validation["selection_score"],
            "trades": option_metrics["trades"],
            "sessions": option_metrics["sessions"],
            "coverage": option_metrics["executable_coverage"],
            "mean_net_dollars": option_metrics["mean_net_dollars"],
            "mean_net_return": option_metrics["mean_net_return"],
            "hit_rate": option_metrics["hit_rate"],
            "ci_low_return": option["session_bootstrap_95_return"][0],
            "ci_high_return": option["session_bootstrap_95_return"][1],
            "holm_p": option["holm_adjusted_p_across_modes"],
            "validated": joint["validated_executable_option_edge"],
        }
    )
    for session_date, value in option_metrics["session_mean_returns"].items():
        option_session_rows.append(
            {"mode": mode_label(mode), "session_date": session_date, "mean_net_return": value}
        )

    decomposition = snapshot["factor_decomposition"][mode]
    for family, share in decomposition["absolute_loading_share_by_family"].items():
        factor_share_rows.append(
            {"mode": mode_label(mode), "factor_family": family, "absolute_loading_share": share}
        )
    for rank, row in enumerate(decomposition["top_standardized_loadings"][:10], start=1):
        factor_loading_rows.append(
            {
                "mode": mode_label(mode),
                "rank": rank,
                "feature": row["feature"],
                "standardized_loading": row["standardized_loading"],
            }
        )

atlas = snapshot["regime_event_discovery"]
for axis, counts in atlas["day_label_contract"]["counts"].items():
    for label, sessions in counts.items():
        day_label_rows.append(
            {"mode": "RTH", "axis": axis, "label": label, "sessions": sessions}
        )

gth_atlas = atlas["gth_event_discovery"]
for axis, counts in gth_atlas["day_label_contract"]["counts"].items():
    for label, sessions in counts.items():
        day_label_rows.append(
            {"mode": "GTH", "axis": axis, "label": label, "sessions": sessions}
        )

for rank, row in enumerate(
    atlas["trend_launches"]["prelaunch_feature_changes_vs_clock_controls"][:10],
    start=1,
):
    launch_effect_rows.append({"mode": "RTH", "rank": rank, **row})

for rank, row in enumerate(
    gth_atlas["trend_launches"]["prelaunch_feature_changes_vs_clock_controls"][:10],
    start=1,
):
    launch_effect_rows.append({"mode": "GTH", "rank": rank, **row})

for event_type, cohorts in atlas["event_summary"].items():
    for cohort in ("retrospective_validation", "previously_seen_tail"):
        metrics = cohorts[cohort]
        event_rows.append(
            {
                "mode": "RTH",
                "event_type": event_type,
                "cohort": cohort,
                "events": metrics["events"],
                "sessions": metrics["sessions"],
                "target_first_rate": metrics["target_first_rate"],
                "mean_return_r": metrics["mean_return_r"],
                "ci_low_r": metrics["session_bootstrap_95_mean_r"][0],
                "ci_high_r": metrics["session_bootstrap_95_mean_r"][1],
                "p_value": metrics["exact_one_sided_session_sign_flip_p"],
            }
        )

for event_type, cohorts in gth_atlas["event_summary"].items():
    for cohort in ("retrospective_validation", "previously_seen_tail"):
        metrics = cohorts[cohort]
        event_rows.append(
            {
                "mode": "GTH",
                "event_type": event_type,
                "cohort": cohort,
                "events": metrics["events"],
                "sessions": metrics["sessions"],
                "target_first_rate": metrics["target_first_rate"],
                "mean_return_r": metrics["mean_return_r"],
                "ci_low_r": metrics["session_bootstrap_95_mean_r"][0],
                "ci_high_r": metrics["session_bootstrap_95_mean_r"][1],
                "p_value": metrics["exact_one_sided_session_sign_flip_p"],
            }
        )

def append_event_option_scope(
    *,
    mode: str,
    event_type: str,
    option_scope: str,
    result: dict[str, Any],
) -> None:
    champion = result.get("development_champion") or {}
    for cohort in ("retrospective_validation", "previously_seen_tail"):
        cohort_result = result.get(cohort) or {}
        metrics = cohort_result.get("metrics") or {}
        interval = cohort_result.get("session_bootstrap_95_return") or [None, None]
        event_option_rows.append(
            {
                "mode": mode,
                "option_scope": option_scope,
                "event_type": event_type,
                "cohort": cohort,
                "candidate": champion.get("candidate"),
                "maximum_relative_spread": champion.get(
                    "maximum_selection_relative_spread"
                ),
                "signals": metrics.get("signals"),
                "trades": metrics.get("trades"),
                "sessions": metrics.get("sessions"),
                "coverage": metrics.get("executable_coverage"),
                "mean_net_dollars": metrics.get("mean_net_dollars"),
                "mean_net_return": metrics.get("mean_net_return"),
                "positive_session_rate": metrics.get("positive_session_rate"),
                "ci_low_return": interval[0],
                "ci_high_return": interval[1],
                "p_value": cohort_result.get("exact_one_sided_session_sign_flip_p"),
            }
        )


for event_type, result in atlas["exact_bbo_option_validation"].items():
    append_event_option_scope(
        mode="RTH",
        event_type=event_type,
        option_scope="unrestricted_research",
        result=result,
    )
    append_event_option_scope(
        mode="RTH",
        event_type=event_type,
        option_scope="production_vertical",
        result=result.get("production_compatible_vertical") or {},
    )

for event_type, result in gth_atlas["exact_bbo_option_validation"].items():
    append_event_option_scope(
        mode="GTH",
        event_type=event_type,
        option_scope="unrestricted_research",
        result=result,
    )
    append_event_option_scope(
        mode="GTH",
        event_type=event_type,
        option_scope="production_vertical",
        result=result.get("production_compatible_vertical") or {},
    )

qualifier_research = atlas["exact_bbo_option_validation"][
    "right_pullback_resume"
].get("causal_qualifier_research") or {}
qualifier_champion = qualifier_research.get("development_champion") or {}
selected_qualifier = qualifier_champion.get("qualifier")
for row in qualifier_research.get("development_search") or ():
    metrics = row.get("metrics") or {}
    qualifier_rows.append(
        {
            "cohort": "development_selection",
            "qualifier": row.get("qualifier"),
            "definition": row.get("definition"),
            "selected": row.get("qualifier") == selected_qualifier,
            "eligible": row.get("eligible"),
            "signals": row.get("signals"),
            "trades": metrics.get("trades"),
            "sessions": metrics.get("sessions"),
            "coverage": row.get("coverage"),
            "mean_net_dollars": metrics.get("mean_net_dollars"),
            "mean_net_return": metrics.get("mean_net_return"),
            "selection_lcb90": row.get("selection_score"),
            "ci_low_return": None,
            "ci_high_return": None,
            "p_value": None,
        }
    )
for cohort in ("retrospective_validation", "previously_seen_tail"):
    result = qualifier_research.get(cohort) or {}
    metrics = result.get("metrics") or {}
    interval = result.get("session_bootstrap_95_return") or [None, None]
    if selected_qualifier is not None:
        qualifier_rows.append(
            {
                "cohort": cohort,
                "qualifier": selected_qualifier,
                "definition": qualifier_champion.get("definition"),
                "selected": True,
                "eligible": None,
                "signals": metrics.get("signals"),
                "trades": metrics.get("trades"),
                "sessions": metrics.get("sessions"),
                "coverage": metrics.get("executable_coverage"),
                "mean_net_dollars": metrics.get("mean_net_dollars"),
                "mean_net_return": metrics.get("mean_net_return"),
                "selection_lcb90": None,
                "ci_low_return": interval[0],
                "ci_high_return": interval[1],
                "p_value": result.get("exact_one_sided_session_sign_flip_p"),
            }
        )

forward = atlas["strict_forward_evaluation"]
for event_type, result in forward["results"].items():
    metrics = result["metrics"]
    interval = result["session_bootstrap_95_return"]
    forward_rows.append(
        {
            "event_type": event_type,
            "contract_hash": forward["contract_hash"],
            "complete_forward_sessions": forward["complete_forward_session_count"],
            "signals": metrics["signals"],
            "trades": metrics["trades"],
            "event_sessions": metrics["sessions"],
            "coverage": metrics["executable_coverage"],
            "mean_net_dollars": metrics["mean_net_dollars"],
            "ci_low_return": interval[0],
            "ci_high_return": interval[1],
            "holm_p": result["holm_adjusted_p_across_forward_hypotheses"],
            "promotion_eligible": result["promotion_eligible"],
        }
    )

denoising = snapshot["denoising_state_benchmark"]
for mode in ("rth", "gth"):
    result = denoising[mode]
    champion = (result.get("development_champion") or {}).get("pipeline")
    for row in result.get("development_search") or ():
        metrics = row["metrics"]
        denoising_development_rows.append(
            {
                "mode": mode_label(mode),
                "pipeline": row["pipeline"],
                "selected": row["pipeline"] == champion,
                "eligible": row["eligible"],
                "events": metrics["events"],
                "resolved": metrics["resolved"],
                "sessions": metrics["sessions"],
                "target_first_rate": metrics["target_first_rate"],
                "mean_return_r": metrics["mean_return_r"],
                "ci_low_r": metrics["session_bootstrap_95_mean_r"][0],
                "ci_high_r": metrics["session_bootstrap_95_mean_r"][1],
                "one_sided_p": metrics["exact_one_sided_session_sign_flip_p"],
                "familywise_max_p": (
                    result.get("familywise_sign_flip_max_p")
                    if row["pipeline"] == champion
                    else None
                ),
            }
        )
    for cohort, metrics in (
        result.get("underlying_champion_cohorts") or {}
    ).items():
        denoising_cohort_rows.append(
            {
                "mode": mode_label(mode),
                "pipeline": champion,
                "cohort": cohort,
                "events": metrics["events"],
                "resolved": metrics["resolved"],
                "sessions": metrics["sessions"],
                "target_first_rate": metrics["target_first_rate"],
                "mean_return_r": metrics["mean_return_r"],
                "ci_low_r": metrics["session_bootstrap_95_mean_r"][0],
                "ci_high_r": metrics["session_bootstrap_95_mean_r"][1],
                "one_sided_p": metrics["exact_one_sided_session_sign_flip_p"],
            }
        )
    for pipeline, comparison in (
        result.get("fixed_option_comparison") or {}
    ).items():
        for cohort, cohort_result in comparison["cohorts"].items():
            metrics = cohort_result["metrics"]
            denoising_option_rows.append(
                {
                    "mode": mode_label(mode),
                    "role": (
                        "development_champion"
                        if pipeline == champion
                        else "raw_baseline"
                    ),
                    "pipeline": pipeline,
                    "cohort": cohort,
                    "candidate": comparison["candidate"],
                    "maximum_relative_spread": comparison[
                        "maximum_selection_relative_spread"
                    ],
                    "signals": metrics["signals"],
                    "trades": metrics["trades"],
                    "sessions": metrics["sessions"],
                    "coverage": metrics["executable_coverage"],
                    "mean_net_dollars": metrics["mean_net_dollars"],
                    "mean_net_return": metrics["mean_net_return"],
                    "positive_session_rate": metrics["positive_session_rate"],
                    "ci_low_return": cohort_result[
                        "session_bootstrap_95_return"
                    ][0],
                    "ci_high_return": cohort_result[
                        "session_bootstrap_95_return"
                    ][1],
                    "one_sided_p": cohort_result[
                        "exact_one_sided_session_sign_flip_p"
                    ],
                }
            )

followup = snapshot["layered_followup_research"]
forward_dual = followup["rth_denoising_forward_evaluation"]
for lane, result in forward_dual["results"].items():
    metrics = result["metrics"]
    interval = result["session_bootstrap_95_return"]
    denoising_forward_rows.append(
        {
            "row_type": "lane",
            "lane": lane,
            "contract_hash": forward_dual["contract_hash"],
            "complete_forward_sessions": forward_dual[
                "complete_forward_session_count"
            ],
            "signals": metrics["signals"],
            "trades": metrics["trades"],
            "event_sessions": metrics["sessions"],
            "coverage": metrics["executable_coverage"],
            "mean_net_dollars": metrics["mean_net_dollars"],
            "mean_net_return_or_improvement": metrics["mean_net_return"],
            "ci_low": interval[0],
            "ci_high": interval[1],
            "p_value": result["exact_one_sided_session_sign_flip_p"],
            "promotion_eligible": forward_dual["promotion_eligible"],
        }
    )
paired_forward = forward_dual["paired_preaverage_minus_raw"]
denoising_forward_rows.append(
    {
        "row_type": "paired_increment",
        "lane": "preaverage_minus_raw",
        "contract_hash": forward_dual["contract_hash"],
        "complete_forward_sessions": forward_dual[
            "complete_forward_session_count"
        ],
        "signals": None,
        "trades": None,
        "event_sessions": paired_forward["paired_sessions"],
        "coverage": None,
        "mean_net_dollars": None,
        "mean_net_return_or_improvement": paired_forward[
            "mean_return_improvement"
        ],
        "ci_low": paired_forward["session_bootstrap_95_improvement"][0],
        "ci_high": paired_forward["session_bootstrap_95_improvement"][1],
        "p_value": paired_forward["exact_one_sided_session_sign_flip_p"],
        "promotion_eligible": forward_dual["promotion_eligible"],
    }
)

hurdle = followup["gth_execution_hurdle"]
for cohort in (
    "development_oof",
    "retrospective_validation",
    "previously_seen_tail",
):
    result = hurdle[cohort]
    observed = result["observed_exact_bbo"]
    selected = result["selected_metrics"]
    interval = result["session_bootstrap_95_return"]
    gth_hurdle_rows.append(
        {
            "cohort": cohort,
            "direction_signals": observed["signals"],
            "fresh_exact_bbo_trades": observed["trades"],
            "exact_bbo_coverage": observed["executable_coverage"],
            "selected_trades": selected["trades"],
            "selected_sessions": selected["sessions"],
            "conditional_selection_rate": selected[
                "conditional_selection_rate"
            ],
            "mean_net_dollars": selected["mean_net_dollars"],
            "mean_net_return": selected["mean_net_return"],
            "ci_low_return": interval[0],
            "ci_high_return": interval[1],
            "p_value": result["exact_one_sided_session_sign_flip_p"],
        }
    )
    for stage, count in (
        ("direction_signal", observed["signals"]),
        ("fresh_exact_bbo", observed["trades"]),
        ("conditional_ev_gt_0", selected["trades"]),
    ):
        gth_hurdle_stage_rows.append(
            {"cohort": cohort, "stage": stage, "count": count}
        )

gex_gate = followup["rth_gex_location_gate"]
for cohort, result in gex_gate["cohorts"].items():
    for policy, metrics in (
        ("preaverage_baseline", result["baseline_metrics"]),
        ("gex_location_gate", result["gated_metrics"]),
    ):
        gex_gate_rows.append(
            {
                "cohort": cohort,
                "policy": policy,
                "exact_bbo_trades": result["exact_bbo_trades"],
                "surface_located_trades": result["surface_located_trades"],
                "surface_coverage": result["surface_coverage_of_exact_bbo"],
                "gated_trades": result["gated_trades"],
                "gate_rate": result["gate_rate_of_exact_bbo"],
                "mean_net_dollars": metrics["mean_net_dollars"],
                "mean_net_return": metrics["mean_net_return"],
                "return_increment": (
                    result["mean_net_return_increment"]
                    if policy == "gex_location_gate"
                    else None
                ),
                "ci_low_return": (
                    result["session_bootstrap_95_return"][0]
                    if policy == "gex_location_gate"
                    else None
                ),
                "ci_high_return": (
                    result["session_bootstrap_95_return"][1]
                    if policy == "gex_location_gate"
                    else None
                ),
                "p_value": (
                    result["exact_one_sided_session_sign_flip_p"]
                    if policy == "gex_location_gate"
                    else None
                ),
            }
        )

cusum = followup["rth_cusum_state_transition"]["fixed_option_result"]
for cohort, result in cusum["cohorts"].items():
    metrics = result["metrics"]
    interval = result["session_bootstrap_95_return"]
    cusum_rows.append(
        {
            "cohort": cohort,
            "signals": metrics["signals"],
            "trades": metrics["trades"],
            "sessions": metrics["sessions"],
            "coverage": metrics["executable_coverage"],
            "mean_net_dollars": metrics["mean_net_dollars"],
            "mean_net_return": metrics["mean_net_return"],
            "positive_session_rate": metrics["positive_session_rate"],
            "ci_low_return": interval[0],
            "ci_high_return": interval[1],
            "p_value": result["exact_one_sided_session_sign_flip_p"],
        }
    )

for mode, result in followup["bocpd_state_diagnostics"].items():
    quantiles = result["all_observation_probability_quantiles"]
    bocpd_rows.append(
        {
            "mode": mode_label(mode),
            "trend_launches": result["trend_launches"],
            "prelaunch_max_probability": result[
                "mean_prelaunch_max_probability"
            ],
            "matched_control_max_probability": result[
                "mean_matched_clock_control_max_probability"
            ],
            "standardized_difference": result["standardized_difference"],
            "prelaunch_hit_rate_at_0_20": result[
                "prelaunch_hit_rate_at_fixed_0.20"
            ],
            "all_probability_p90": quantiles.get("p90"),
            "all_probability_p99": quantiles.get("p99"),
        }
    )


headline = [
    {
        "rth_sessions": snapshot["data_quality"]["rth"]["complete_sessions"],
        "gth_sessions": snapshot["data_quality"]["gth"]["complete_sessions"],
        "rth_direction_bps": direction_rows[0]["mean_signed_bps"],
        "gth_direction_bps": direction_rows[1]["mean_signed_bps"],
        "rth_option_dollars": option_rows[0]["mean_net_dollars"],
        "gth_option_dollars": option_rows[1]["mean_net_dollars"],
    }
]

quality_rows = []
for mode in ("rth", "gth"):
    quality = snapshot["data_quality"][mode]
    quality_rows.append(
        {
            "mode": mode_label(mode),
            "target": quality["target"],
            "sessions": quality["complete_sessions"],
            "quote_updates": quality["causal_quote_updates"],
            "decision_samples": quality["decision_samples"],
            "features": quality["features"],
            "coverage_min": quality["target_fresh_coverage_min"],
            "coverage_median": quality["target_fresh_coverage_median"],
            "train_sessions": len(quality["split"]["train"]),
            "validation_sessions": len(quality["split"]["validation"]),
            "test_sessions": len(quality["split"]["test"]),
        }
    )

search_rows = []
for mode in ("rth", "gth"):
    joint = snapshot["joint_direction_option_search"][mode]
    for row in joint["direction_candidates"]:
        search_rows.append(
            {
                "mode": mode_label(mode),
                "holding_seconds": row["holding_seconds"],
                "model": row["model"],
                "confidence_quantile": row["confidence_quantile"],
                "direction_validation_lcb_bps": row["selection_score"],
            }
        )

decision_rows = [
    {
        "layer": "Raw directional predictability",
        "RTH": "1.44 bp mean, but only 7 signals / 4 sessions; Holm p=0.125",
        "GTH": "1.95 bp mean on ES proxy; session CI crosses zero",
        "decision": "Suggestive, not validated",
    },
    {
        "layer": "Latency robustness",
        "RTH": "Falls to 0.15 bp at +5s and -0.14 bp at +10s",
        "GTH": "1.53 bp at +15s, but still session-unstable",
        "decision": "RTH resembles short-lived cross-market repricing",
    },
    {
        "layer": "Validation exact-BBO",
        "RTH": "Best confidence+spread-gated LCB = -0.94%",
        "GTH": "Best confidence+spread-gated LCB = -0.84%",
        "decision": "No candidate clears promotion before test",
    },
    {
        "layer": "Held-out exact-BBO",
        "RTH": "-$33.00/trade; 0/4 positive sessions",
        "GTH": "+$41.81/trade; CI crosses zero; Holm p=0.1875",
        "decision": "NO EXECUTABLE OPTION EDGE",
    },
    {
        "layer": "GEX wall / zero-gamma left-side fade",
        "RTH": "Tail exact-BBO mean -$84.00 / -$79.43; both session CIs are below zero",
        "GTH": "Not tested: cash SPX/GEX structure is not a live GTH equivalent",
        "decision": "REJECT",
    },
    {
        "layer": "Right-side 30m breakout",
        "RTH": "Production vertical validation +$8.15/trade but mean return -0.14%; seen tail -$23.66",
        "GTH": "Production vertical validation -$63.33/trade; seen tail -$61.29",
        "decision": "REJECT BOTH MODES",
    },
    {
        "layer": "Right-side impulse pullback/resume",
        "RTH": "Production 15-point vertical validation +$16.58; seen tail +$27.21, but all CIs cross zero",
        "GTH": "Production vertical validation -$58.76/trade; seen tail -$67.67",
        "decision": "RTH WEAK FORWARD HYPOTHESIS; GTH REJECT",
    },
]


def money_text(value: object) -> str:
    return "n/a" if not isinstance(value, int | float) else f"${float(value):+.2f}"


if selected_qualifier is None:
    qualifier_decision_text = "No causal qualifier met minimum development coverage."
    qualifier_decision = "NO NEW HYPOTHESIS"
else:
    validation = qualifier_research.get("retrospective_validation") or {}
    tail = qualifier_research.get("previously_seen_tail") or {}
    validation_metrics = validation.get("metrics") or {}
    tail_metrics = tail.get("metrics") or {}
    qualifier_decision_text = (
        f"Selected `{selected_qualifier}` on development only; validation "
        f"{money_text(validation_metrics.get('mean_net_dollars'))}/trade and seen tail "
        f"{money_text(tail_metrics.get('mean_net_dollars'))}/trade."
    )
    qualifier_decision = (
        "RETROSPECTIVE PASS; SEPARATE FORWARD CONTRACT STILL REQUIRED"
        if qualifier_research.get("retrospective_strict_pass")
        else "NO NEW FORWARD HYPOTHESIS"
    )
decision_rows.append(
    {
        "layer": "Causal pullback qualifier scan",
        "RTH": qualifier_decision_text,
        "GTH": "Not searched; GTH base pullback economics were already negative.",
        "decision": qualifier_decision,
    }
)

rth_denoising = denoising["rth"]
gth_denoising = denoising["gth"]
rth_denoising_champion = rth_denoising["development_champion"]["pipeline"]
gth_denoising_champion = gth_denoising["development_champion"]["pipeline"]
rth_denoising_option = rth_denoising["fixed_option_comparison"][
    rth_denoising_champion
]["cohorts"]
gth_denoising_option = gth_denoising["fixed_option_comparison"][
    gth_denoising_champion
]["cohorts"]
decision_rows.append(
    {
        "layer": "Causal denoising + state-change benchmark",
        "RTH": (
            f"{rth_denoising_champion}; family-wise p="
            f"{rth_denoising['familywise_sign_flip_max_p']:.3f}; validation/tail "
            f"{money_text(rth_denoising_option['retrospective_validation']['metrics']['mean_net_dollars'])} / "
            f"{money_text(rth_denoising_option['previously_seen_tail']['metrics']['mean_net_dollars'])}"
        ),
        "GTH": (
            f"{gth_denoising_champion}; family-wise p="
            f"{gth_denoising['familywise_sign_flip_max_p']:.3f}; validation/tail "
            f"{money_text(gth_denoising_option['retrospective_validation']['metrics']['mean_net_dollars'])} / "
            f"{money_text(gth_denoising_option['previously_seen_tail']['metrics']['mean_net_dollars'])}"
        ),
        "decision": "RTH PROMISING BUT UNCONFIRMED; GTH REJECT",
    }
)
decision_rows.extend(
    [
        {
            "layer": "RTH raw/pre-average paired forward contract",
            "RTH": (
                f"Starts 2026-08-20; {forward_dual['complete_forward_session_count']} "
                f"new sessions; contract {forward_dual['contract_hash']}"
            ),
            "GTH": "Out of scope; RTH cash/Schwab contract only.",
            "decision": "FROZEN RESEARCH CONTRACT; AWAIT FORWARD DATA",
        },
        {
            "layer": "GTH observed-BBO + conditional-EV hurdle",
            "RTH": "Out of scope.",
            "GTH": (
                "Development OOF selected 1 losing trade; validation and seen tail "
                "selected 0 trades; upstream direction gate also failed."
            ),
            "decision": "REJECT AS EDGE; FAIL CLOSED",
        },
        {
            "layer": "RTH GEX location gate",
            "RTH": (
                "Development gated mean +$27.21; validation/seen tail "
                "-$7.15/-$6.00 despite positive ungated baselines."
            ),
            "GTH": "Not copied into GTH.",
            "decision": "REJECT GATE",
        },
        {
            "layer": "CUSUM / BOCPD state change",
            "RTH": (
                "CUSUM development -$38.58, later means positive but CIs cross zero; "
                "BOCPD prelaunch standardized difference -0.36."
            ),
            "GTH": "BOCPD prelaunch standardized difference +0.20; no trigger support.",
            "decision": "STATE OBSERVATION ONLY; NO TRADE AUTHORITY",
        },
    ]
)

source_sql = snapshot["source"]["sql"]
option_sql_template = """
-- Representative exact-BBO event-time join. Provider, expiry and event table are bound per session.
WITH option_quotes AS (
  SELECT instrument_id, strike, right, received_at, source_at, quote_time, bid, ask, delta
  FROM read_parquet($partition, hive_partitioning=true, union_by_name=true)
  WHERE quality = 'live'
    AND lower(coalesce(market_data_type, 'live')) IN ('live', '1')
    AND bid >= 0 AND ask >= bid AND ask > 0
    AND source_at BETWEEN received_at - INTERVAL 30 SECOND AND received_at + INTERVAL 5 SECOND
    AND (quote_time IS NULL OR quote_time <= received_at + INTERVAL 5 SECOND)
)
SELECT event.*, quote.bid, quote.ask, quote.received_at
FROM option_events AS event
ASOF LEFT JOIN option_quotes AS quote
  ON event.instrument_id = quote.instrument_id
 AND event.event_at >= quote.received_at;
""".strip()

sources = [
    {
        "id": "raw_quotes",
        "label": "Causal raw quote-update lake",
        "path": "quote_lake/schema=v1/provider=schwab",
        "query": {
            "engine": "DuckDB over Parquet (read-only)",
            "id": "raw-quote-edge-discovery-20260819",
            "sql": source_sql,
            "description": "Builds five-second causal RTH/GTH quote-update buckets.",
            "executed_at": snapshot["generated_at"],
            "language": "sql+python",
            "filters": snapshot["source"]["knowledge_guard"],
            "metric_definitions": [
                "RTH target is future SPX log return in basis points.",
                "GTH target is future ES-proxy log return in basis points.",
                "Decision points are fifteen seconds apart; signal outcomes use a horizon-length cooldown.",
            ],
            "tables_used": ["raw quote Parquet partitions"],
        },
    },
    {
        "id": "exact_bbo",
        "label": "Same-day SPXW exact BBO",
        "path": "quote_lake/schema=v1/provider={schwab|ibkr}",
        "query": {
            "engine": "DuckDB ASOF joins over Parquet (read-only)",
            "id": "exact-bbo-option-falsification-20260819",
            "sql": option_sql_template,
            "description": "Selects contracts at decision time and applies conservative leg-side BBO fills.",
            "executed_at": snapshot["generated_at"],
            "language": "sql+python",
            "filters": [
                "same-day SPXW",
                "RTH Schwab BBO age <= 5 seconds",
                "GTH IBKR BBO age <= 15 seconds",
                "entry at decision + 5 seconds",
                "buy at ask and sell at bid",
            ],
            "metric_definitions": [
                "Outright P&L = exit bid - entry ask - fees.",
                "Vertical P&L uses long ask/short bid entry and long bid/short ask exit.",
                "$1.50 per contract leg-side is charged in addition to displayed spread crossing.",
            ],
            "tables_used": ["raw SPXW quote Parquet partitions"],
        },
    },
    {
        "id": "notebook",
        "label": "Executed companion notebook",
        "path": "docs/notebooks/raw-tick-edge-discovery-2026-08-19.ipynb",
        "query": {
            "engine": "Python notebook",
            "id": "raw-tick-edge-notebook-20260819",
            "sql": source_sql,
            "description": "Reproduces data quality, validation, held-out tests and exact-BBO falsification.",
            "executed_at": generated_at,
            "language": "python+sql",
            "filters": ["read-only research", "no production strategy input", "no order creation"],
            "metric_definitions": ["Post-hoc use of a seen test segment cannot independently confirm an edge."],
            "tables_used": ["derived causal five-second grids"],
        },
    },
]

cards = [
    {
        "id": "rth_sessions",
        "description": "Complete RTH sessions.",
        "dataset": "headline",
        "sourceId": "raw_quotes",
        "metrics": [{"label": "RTH sessions", "field": "rth_sessions", "format": "number"}],
    },
    {
        "id": "gth_sessions",
        "description": "Complete GTH sessions.",
        "dataset": "headline",
        "sourceId": "raw_quotes",
        "metrics": [{"label": "GTH sessions", "field": "gth_sessions", "format": "number"}],
    },
    {
        "id": "rth_direction",
        "description": "Held-out-test RTH signed SPX return.",
        "dataset": "headline",
        "sourceId": "raw_quotes",
        "metrics": [{"label": "RTH direction", "field": "rth_direction_bps", "format": "decimal", "suffix": " bp"}],
    },
    {
        "id": "gth_direction",
        "description": "Held-out-test GTH signed ES-proxy return.",
        "dataset": "headline",
        "sourceId": "raw_quotes",
        "metrics": [{"label": "GTH direction", "field": "gth_direction_bps", "format": "decimal", "suffix": " bp"}],
    },
    {
        "id": "rth_option",
        "description": "Held-out-test exact-BBO net P&L per RTH trade.",
        "dataset": "headline",
        "sourceId": "exact_bbo",
        "metrics": [{"label": "RTH option", "field": "rth_option_dollars", "format": "currency"}],
    },
    {
        "id": "gth_option",
        "description": "Held-out-test exact-BBO net P&L per GTH trade.",
        "dataset": "headline",
        "sourceId": "exact_bbo",
        "metrics": [{"label": "GTH option", "field": "gth_option_dollars", "format": "currency"}],
    },
]


def bar_chart(
    chart_id: str,
    title: str,
    subtitle: str,
    dataset: str,
    source_id: str,
    x_field: str,
    x_label: str,
    y_field: str,
    y_label: str,
    y_format: str,
    color_field: str | None = None,
    color_label: str = "Mode",
) -> dict[str, Any]:
    encodings: dict[str, Any] = {
        "x": {"field": x_field, "type": "nominal", "label": x_label},
        "y": {"field": y_field, "type": "quantitative", "format": y_format, "label": y_label},
    }
    if color_field:
        encodings["color"] = {
            "field": color_field,
            "type": "nominal",
            "label": color_label,
        }
    return {
        "id": chart_id,
        "title": title,
        "subtitle": subtitle,
        "intent": "comparison",
        "question": title,
        "rationale": subtitle,
        "type": "bar",
        "dataset": dataset,
        "sourceId": source_id,
        "encodings": encodings,
        "layout": "full",
        "labels": {"values": "none"},
        "settings": {"categoryLabelPolicy": "wrap", "groupMode": "grouped", "sort": "none"},
    }


charts = [
    bar_chart(
        "direction_sessions",
        "Directional candidates are suggestive but under-supported",
        "RTH has only four represented sessions; GTH session outcomes are unstable.",
        "direction_session_rows",
        "raw_quotes",
        "session_date",
        "Test session",
        "mean_signed_bps",
        "Mean signed future return (bp)",
        "decimal",
        "mode",
    ),
    {
        "id": "delay_decay",
        "title": "RTH edge decays almost completely within ten seconds",
        "subtitle": "The GTH 15-minute signal is less latency-sensitive; the two horizons are not pooled.",
        "intent": "trend",
        "question": "How much directional return remains after realistic entry delay?",
        "rationale": "A statistically positive mark is not executable if it disappears before an order can fill.",
        "type": "line",
        "dataset": "delay_rows",
        "sourceId": "raw_quotes",
        "encodings": {
            "x": {"field": "delay_seconds", "type": "quantitative", "format": "number", "label": "Entry delay (seconds)"},
            "y": {"field": "mean_signed_bps", "type": "quantitative", "format": "decimal", "label": "Remaining signed return (bp)"},
            "color": {"field": "mode", "type": "nominal", "label": "Mode"},
        },
        "layout": "full",
        "settings": {"showPoints": True, "sort": "none"},
    },
    bar_chart(
        "option_sessions",
        "RTH option P&L loses; GTH remains inconclusive",
        "GTH mean is positive after spread gating, but its session interval crosses zero.",
        "option_session_rows",
        "exact_bbo",
        "session_date",
        "Test session",
        "mean_net_return",
        "Mean net option return",
        "percent",
        "mode",
    ),
    bar_chart(
        "denoising_development_lcb",
        "Light pre-averaging improves RTH; no GTH pipeline clears zero",
        "Bars are development session-bootstrap 95% lower bounds across six fixed pipelines.",
        "denoising_development_rows",
        "raw_quotes",
        "pipeline",
        "Fixed causal pipeline",
        "ci_low_r",
        "Development lower bound (R)",
        "decimal",
        "mode",
    ),
    bar_chart(
        "gth_hurdle_stage_counts",
        "GTH hurdle stage counts by cohort",
        "Direction signals, fresh fixed-vertical BBOs, and conditional-EV selections are shown separately.",
        "gth_hurdle_stage_rows",
        "exact_bbo",
        "cohort",
        "Cohort",
        "count",
        "Observations",
        "number",
        "stage",
        "Stage",
    ),
    bar_chart(
        "gex_gate_option_pnl",
        "RTH pre-average and GEX-gated mean option P&L",
        "Fixed 60-delta/15-point vertical, shown by development, validation, and seen-tail cohort.",
        "gex_gate_rows",
        "exact_bbo",
        "cohort",
        "Cohort",
        "mean_net_dollars",
        "Mean net P&L per trade ($)",
        "currency",
        "policy",
        "Policy",
    ),
    bar_chart(
        "factor_share",
        "Cross-market information dominates RTH; price path dominates GTH",
        "Shares use absolute standardized Ridge loadings and are descriptive, not causal.",
        "factor_share_rows",
        "raw_quotes",
        "factor_family",
        "Factor family",
        "absolute_loading_share",
        "Absolute loading share",
        "percent",
        "mode",
    ),
]


def table(
    table_id: str,
    title: str,
    subtitle: str,
    dataset: str,
    source_id: str,
    columns: list[tuple[str, str, str]],
) -> dict[str, Any]:
    rendered = []
    for field, label, value_type in columns:
        column: dict[str, Any] = {"field": field, "label": label}
        if value_type == "text":
            column["type"] = "text"
        else:
            column["format"] = value_type
        rendered.append(column)
    return {
        "id": table_id,
        "title": title,
        "subtitle": subtitle,
        "dataset": dataset,
        "sourceId": source_id,
        "layout": "full",
        "columns": rendered,
    }


tables = [
    table(
        "decision_table",
        "What passed and what failed",
        "Direction and executable option EV are separate claims.",
        "decision_rows",
        "notebook",
        [("layer", "Layer", "text"), ("RTH", "RTH", "text"), ("GTH", "GTH", "text"), ("decision", "Decision", "text")],
    ),
    table(
        "quality_table",
        "Point-in-time dataset and split",
        "Target freshness is measured after the source-time knowledge guard.",
        "quality_rows",
        "raw_quotes",
        [("mode", "Mode", "text"), ("target", "Target", "text"), ("sessions", "Sessions", "number"), ("quote_updates", "Quote updates", "number"), ("decision_samples", "Decision points", "number"), ("features", "Features", "number"), ("coverage_min", "Min fresh coverage", "percent"), ("train_sessions", "Train", "number"), ("validation_sessions", "Validation", "number"), ("test_sessions", "Test", "number")],
    ),
    table(
        "direction_table",
        "Held-out directional candidates",
        "The test segment is now seen and cannot support another confirmatory tuning round.",
        "direction_rows",
        "raw_quotes",
        [("mode", "Mode", "text"), ("target", "Target", "text"), ("holding_seconds", "Horizon s", "number"), ("model", "Model", "text"), ("signals", "Signals", "number"), ("mean_signed_bps", "Mean bp", "decimal"), ("ci_low_bps", "CI low", "decimal"), ("ci_high_bps", "CI high", "decimal"), ("hit_rate", "Hit rate", "percent"), ("holm_p", "Holm p", "decimal")],
    ),
    table(
        "option_table",
        "Joint direction + option validation",
        "The option structure and direction horizon are both selected on validation only.",
        "option_rows",
        "exact_bbo",
        [("mode", "Mode", "text"), ("direction_candidate", "Direction candidate", "text"), ("option_structure", "Option", "text"), ("maximum_relative_spread", "Max rel spread", "percent"), ("validation_lcb_return", "Validation LCB", "percent"), ("trades", "Test trades", "number"), ("coverage", "BBO coverage", "percent"), ("mean_net_dollars", "Mean net $", "currency"), ("mean_net_return", "Mean return", "percent"), ("ci_low_return", "CI low", "percent"), ("ci_high_return", "CI high", "percent")],
    ),
    table(
        "search_table",
        "Validation-supported horizons entering joint option search",
        "Only horizon champions with a positive directional validation lower bound enter this layer.",
        "search_rows",
        "raw_quotes",
        [("mode", "Mode", "text"), ("holding_seconds", "Horizon s", "number"), ("model", "Model", "text"), ("confidence_quantile", "Confidence q", "decimal"), ("direction_validation_lcb_bps", "Direction LCB bp", "decimal")],
    ),
    table(
        "denoising_development_table",
        "Six fixed causal denoising/state pipelines",
        "Selection uses development only; family-wise max-p penalizes choosing the best of six.",
        "denoising_development_rows",
        "raw_quotes",
        [("mode", "Mode", "text"), ("pipeline", "Pipeline", "text"), ("selected", "Selected", "text"), ("events", "Events", "number"), ("resolved", "Resolved", "number"), ("sessions", "Sessions", "number"), ("target_first_rate", "Target first", "percent"), ("mean_return_r", "Mean R", "decimal"), ("ci_low_r", "CI low R", "decimal"), ("ci_high_r", "CI high R", "decimal"), ("one_sided_p", "One-sided p", "decimal"), ("familywise_max_p", "Family-wise p", "decimal")],
    ),
    table(
        "denoising_cohort_table",
        "Development champion on later underlying paths",
        "Validation and the previously seen tail can falsify but cannot create new forward evidence.",
        "denoising_cohort_rows",
        "raw_quotes",
        [("mode", "Mode", "text"), ("pipeline", "Pipeline", "text"), ("cohort", "Cohort", "text"), ("events", "Events", "number"), ("resolved", "Resolved", "number"), ("sessions", "Sessions", "number"), ("target_first_rate", "Target first", "percent"), ("mean_return_r", "Mean R", "decimal"), ("ci_low_r", "CI low R", "decimal"), ("ci_high_r", "CI high R", "decimal"), ("one_sided_p", "One-sided p", "decimal")],
    ),
    table(
        "denoising_option_table",
        "Raw baseline vs selected denoiser at fixed exact-BBO economics",
        "Both use 60-delta/15-point verticals; no delta, width or spread threshold is reselected here.",
        "denoising_option_rows",
        "exact_bbo",
        [("mode", "Mode", "text"), ("role", "Role", "text"), ("pipeline", "Pipeline", "text"), ("cohort", "Cohort", "text"), ("trades", "Trades", "number"), ("sessions", "Sessions", "number"), ("coverage", "Coverage", "percent"), ("mean_net_dollars", "Mean net $", "currency"), ("mean_net_return", "Mean return", "percent"), ("positive_session_rate", "Positive sessions", "percent"), ("ci_low_return", "CI low", "percent"), ("ci_high_return", "CI high", "percent"), ("one_sided_p", "One-sided p", "decimal")],
    ),
    table(
        "denoising_forward_table",
        "RTH raw/pre-average strict-forward ledger",
        "Only complete sessions on or after August 20, 2026 enter the paired contract.",
        "denoising_forward_rows",
        "notebook",
        [("row_type", "Row type", "text"), ("lane", "Lane", "text"), ("complete_forward_sessions", "New sessions", "number"), ("signals", "Signals", "number"), ("trades", "Trades", "number"), ("event_sessions", "Event/paired sessions", "number"), ("coverage", "Coverage", "percent"), ("mean_net_dollars", "Mean net $", "currency"), ("mean_net_return_or_improvement", "Return / paired lift", "percent"), ("ci_low", "CI low", "percent"), ("ci_high", "CI high", "percent"), ("p_value", "One-sided p", "decimal"), ("promotion_eligible", "Promotable", "text")],
    ),
    table(
        "gth_hurdle_table",
        "GTH observed-BBO and conditional-EV hurdle",
        "Development uses leave-one-session-out predictions; later cohorts use the frozen development fit.",
        "gth_hurdle_rows",
        "exact_bbo",
        [("cohort", "Cohort", "text"), ("direction_signals", "Direction signals", "number"), ("fresh_exact_bbo_trades", "Fresh BBO", "number"), ("exact_bbo_coverage", "BBO coverage", "percent"), ("selected_trades", "EV>0 selections", "number"), ("selected_sessions", "Selected sessions", "number"), ("conditional_selection_rate", "Selection rate", "percent"), ("mean_net_dollars", "Selected mean $", "currency"), ("mean_net_return", "Selected return", "percent"), ("ci_low_return", "CI low", "percent"), ("ci_high_return", "CI high", "percent"), ("p_value", "One-sided p", "decimal")],
    ),
    table(
        "gex_gate_table",
        "RTH GEX location gate against the pre-average baseline",
        "The gate requires a causal surface within ten minutes and one event-scale of room to the next wall.",
        "gex_gate_rows",
        "exact_bbo",
        [("cohort", "Cohort", "text"), ("policy", "Policy", "text"), ("exact_bbo_trades", "Exact-BBO trades", "number"), ("surface_located_trades", "Located", "number"), ("surface_coverage", "Surface coverage", "percent"), ("gated_trades", "Gated trades", "number"), ("gate_rate", "Gate rate", "percent"), ("mean_net_dollars", "Mean net $", "currency"), ("mean_net_return", "Mean return", "percent"), ("return_increment", "Return lift", "percent"), ("ci_low_return", "Gate CI low", "percent"), ("ci_high_return", "Gate CI high", "percent"), ("p_value", "One-sided p", "decimal")],
    ),
    table(
        "cusum_table",
        "RTH CUSUM state-transition exact-BBO results",
        "The same fixed 60-delta/15-point vertical and five-percent relative-spread gate are used in every cohort.",
        "cusum_rows",
        "exact_bbo",
        [("cohort", "Cohort", "text"), ("signals", "Signals", "number"), ("trades", "Trades", "number"), ("sessions", "Sessions", "number"), ("coverage", "Coverage", "percent"), ("mean_net_dollars", "Mean net $", "currency"), ("mean_net_return", "Mean return", "percent"), ("positive_session_rate", "Positive sessions", "percent"), ("ci_low_return", "CI low", "percent"), ("ci_high_return", "CI high", "percent"), ("p_value", "One-sided p", "decimal")],
    ),
    table(
        "bocpd_table",
        "BOCPD change probability before hindsight trend launches",
        "Maximum probability in the prior fifteen minutes is compared with matched-clock non-trend controls.",
        "bocpd_rows",
        "raw_quotes",
        [("mode", "Mode", "text"), ("trend_launches", "Launches", "number"), ("prelaunch_max_probability", "Prelaunch max p", "percent"), ("matched_control_max_probability", "Control max p", "percent"), ("standardized_difference", "Std difference", "decimal"), ("prelaunch_hit_rate_at_0_20", "Hit rate at 0.20", "percent"), ("all_probability_p90", "All p90", "percent"), ("all_probability_p99", "All p99", "percent")],
    ),
    table(
        "loading_table",
        "Largest standardized factor loadings",
        "Correlated loadings explain model composition, not causality.",
        "factor_loading_rows",
        "raw_quotes",
        [("mode", "Mode", "text"), ("rank", "Rank", "number"), ("feature", "Feature", "text"), ("standardized_loading", "Loading", "decimal")],
    ),
    table(
        "day_label_table",
        "Hindsight day labels use two independent axes",
        "Amplitude does not define trend; mixed days are retained instead of forced into a binary class.",
        "day_label_rows",
        "raw_quotes",
        [("mode", "Mode", "text"), ("axis", "Axis", "text"), ("label", "Label", "text"), ("sessions", "Sessions", "number")],
    ),
    table(
        "launch_effect_table",
        "What changed before hindsight trend launches",
        "Matched-clock standardized differences are descriptive; only seven trend sessions exist.",
        "launch_effect_rows",
        "raw_quotes",
        [("mode", "Mode", "text"), ("rank", "Rank", "number"), ("feature", "Feature", "text"), ("launches", "Launches", "number"), ("launch_mean_change_15m", "Launch Δ15m", "decimal"), ("matched_clock_control_mean_change_15m", "Control Δ15m", "decimal"), ("standardized_difference", "Std difference", "decimal")],
    ),
    table(
        "event_table",
        "Causal event labels on the SPX path",
        "Outcomes use volatility-scaled symmetric first passage to close, never a fixed 20-minute return.",
        "event_rows",
        "raw_quotes",
        [("mode", "Mode", "text"), ("event_type", "Event", "text"), ("cohort", "Cohort", "text"), ("events", "Events", "number"), ("sessions", "Sessions", "number"), ("target_first_rate", "Target first", "percent"), ("mean_return_r", "Mean R", "decimal"), ("ci_low_r", "CI low R", "decimal"), ("ci_high_r", "CI high R", "decimal"), ("p_value", "One-sided p", "decimal")],
    ),
    table(
        "event_option_table",
        "Event hypotheses after exact-BBO costs",
        "Option mapping is selected only on each mode's earliest development sessions and held fixed through later cohorts.",
        "event_option_rows",
        "exact_bbo",
        [("mode", "Mode", "text"), ("option_scope", "Option scope", "text"), ("event_type", "Event", "text"), ("cohort", "Cohort", "text"), ("candidate", "Selected option", "text"), ("trades", "Trades", "number"), ("sessions", "Sessions", "number"), ("coverage", "Coverage", "percent"), ("mean_net_dollars", "Mean net $", "currency"), ("mean_net_return", "Mean return", "percent"), ("ci_low_return", "CI low", "percent"), ("ci_high_return", "CI high", "percent")],
    ),
    table(
        "qualifier_table",
        "Causal conditions around RTH pullback/resume",
        "Twelve predefined single-variable qualifiers are ranked on development only; only the selected row is evaluated later.",
        "qualifier_rows",
        "exact_bbo",
        [("cohort", "Cohort", "text"), ("qualifier", "Qualifier", "text"), ("selected", "Selected", "text"), ("eligible", "Eligible", "text"), ("signals", "Signals", "number"), ("trades", "Trades", "number"), ("sessions", "Sessions", "number"), ("coverage", "Coverage", "percent"), ("mean_net_dollars", "Mean net $", "currency"), ("mean_net_return", "Mean return", "percent"), ("selection_lcb90", "Dev LCB90", "percent"), ("ci_low_return", "Later CI low", "percent"), ("ci_high_return", "Later CI high", "percent")],
    ),
    table(
        "forward_table",
        "Strict forward promotion ledger",
        "The contract starts with the 2026-08-19 RTH session; no pre-freeze session can enter this ledger.",
        "forward_rows",
        "notebook",
        [("event_type", "Event", "text"), ("complete_forward_sessions", "New sessions", "number"), ("signals", "Signals", "number"), ("trades", "Trades", "number"), ("event_sessions", "Event sessions", "number"), ("coverage", "Coverage", "percent"), ("mean_net_dollars", "Mean net $", "currency"), ("ci_low_return", "CI low", "percent"), ("holm_p", "Holm p", "decimal"), ("promotion_eligible", "Promotable", "text")],
    ),
]

blocks = [
    {
        "id": "title",
        "type": "markdown",
        "layout": "full",
        "body": "# SPX 0DTE 原始行情 Edge Discovery\n\n**结论：原始 quote updates 中出现了方向候选，但严格统计门和 exact-BBO 执行门都没有找到已验证的 RTH/GTH 0DTE 期权 edge。**",
    },
    {
        "id": "technical_summary",
        "type": "markdown",
        "layout": "full",
        "body": "## Technical Summary\n\n研究完全没有读取生产策略的 decision、setup、candidate、threshold、management 或 strategy_version。25 个完整 RTH session 和 26 个完整 GTH session 被因果聚合成 5 秒 quote-update 桶，每 15 秒观察一次；自由搜索 15/30/60/120/240/480/900/1800/3600 秒九个窗口，没有固定 20 分钟标签。最后六个 session 作为测试段。\n\n扩大到 97.5%/99% 极高置信后，方向结果更尖锐但也更稀疏：RTH 15 秒候选均值 1.44 bp，却只有 7 个信号、4 个 session，Holm p=0.125；GTH 15 分钟候选均值 1.95 bp，但 session bootstrap 区间 [-0.27, 6.55] bp。二者都不满足严格确认门。RTH 在延迟 5 秒后只剩 0.15 bp，延迟 10 秒转为 -0.14 bp。\n\n执行层仍失败：validation 同时比较方向窗口、30/40/50/60 delta、单腿/5/10/15 点 vertical，以及 1%–20% 相对 spread 门。最佳 RTH/GTH validation session-LCB 仍为 -0.94%/-0.84%。测试段 RTH 为 -$33.00/笔；GTH 虽为 +$41.81/笔，但只有 27 笔且 95% session 区间 [-0.22%, 7.57%]、Holm p=0.1875，不能确认。\n\n新增去噪/状态 benchmark 只比较六条预定义 pipeline。RTH 的 15 秒 causal pre-average pullback 在开发段为 29 个 resolved event/11 个 session、均值 +0.405R、95% 下界 +0.170R；跨六模型 max sign-flip p=0.015。它在 validation/已见尾部固定 60-delta、15 点 vertical 后为 +$37.94/+42.33 每笔，但两段 95% session 区间都跨零，严格门仍失败。GTH 的开发冠军 Kalman pullback family-wise p=0.800，底层方向没有优势；固定 vertical 在 validation/尾部为 -$46.48/-56.00，明确拒绝。\n\n本轮进一步把建议的四条路径逐一做了样本外审计。RTH raw 与 15 秒 pre-average 已冻结为从 2026-08-20 起、按完整 session 配对的双轨前向合同，目前零笔是正确状态。GTH 两阶段 hurdle 在 development 的 32 笔 fresh-BBO 交易中只选出 1 笔，实际 -15.06%，validation/tail 均选择零笔；它学会的是不交易，而不是稳定挑出赢家。GEX location gate 从 development 的 +3.20% 收益增量翻为 validation -7.24%、tail -6.35%；CUSUM 三段区间均跨零。BOCPD 在 RTH/GTH 趋势启动前的 change probability 与匹配对照几乎相同，0.20 阈值命中率均为零。\n\n事件层同样没有可推送 edge。RTH GEX 墙和 zero-gamma 左侧反转均亏损；production-compatible breakout vertical 在 validation 均值约 +$8.15 但收益率为负、尾段 -$23.66，因此拒绝。RTH 回撤续行的 15 点 vertical 在 validation/尾段为 +$16.58/+27.21，但 development LCB 为 -0.11%，两段区间也跨零，只能冻结为弱前向假设。GTH 使用独立 ES 路径和 IBKR SPXW BBO 后，两类事件 validation/tail 全部为负，均拒绝。action authority 仍为 none。",
    },
    {"id": "headline", "type": "metric-strip", "layout": "full", "cardIds": [card["id"] for card in cards]},
    {"id": "findings", "type": "markdown", "layout": "full", "body": "## Key Findings"},
    {"id": "direction_chart", "type": "chart", "layout": "full", "chartId": "direction_sessions"},
    {"id": "direction_note", "type": "markdown", "layout": "full", "sourceId": "raw_quotes", "body": "方向结果按 session 聚类，而不是把行级观察当独立样本。极端置信门把 RTH test 压缩到 7 个信号/4 个 session，把 GTH 压缩到 13 个信号/5 个 session；均值看起来更大，但有效样本反而不足，不能称为已验证 edge。"},
    {"id": "delay_chart", "type": "chart", "layout": "full", "chartId": "delay_decay"},
    {"id": "delay_note", "type": "markdown", "layout": "full", "sourceId": "raw_quotes", "body": "RTH 的主要增量来自 cross block：SPY/ES 相对 SPX 的短时变化与 ES 微价格。候选在 +5 秒只剩 0.15 bp、+10 秒变成负值，不能把未延迟 SPX mark 当作可成交利润。GTH 的 15 分钟候选对延迟不敏感，但收益集中于少数 session。"},
    {"id": "option_chart", "type": "chart", "layout": "full", "chartId": "option_sessions"},
    {"id": "option_note", "type": "markdown", "layout": "full", "sourceId": "exact_bbo", "body": "期权层没有使用 mid：决策后五秒买在 ask，退出卖在 bid；vertical 逐腿使用 long ask/short bid 入场和 long bid/short ask 退出，并另收每 leg-side $1.50。即使只交易相对 spread 最窄的 1%–5% 门，最佳 validation 下界仍为负；GTH test 的正均值属于未通过统计门的候选。"},
    {"id": "denoising_header", "type": "markdown", "layout": "full", "sourceId": "raw_quotes", "body": "## Causal Denoising and State-Change Benchmark\n\n复杂因子被改写成四层：Observation 只处理报价离群点/平滑，State 只估水平、斜率和结构变化，Trigger 只产生 first-passage 方向事件，Execution 只负责 exact-BBO 净收益。首轮固定六条 pipeline：raw pullback、25 秒 causal Hampel、15 秒 causal pre-average、local-linear Kalman、Kalman+breadth，以及 Kalman+CUSUM+breadth；没有搜索神经网络结构或任意因子组合。"},
    {"id": "denoising_chart", "type": "chart", "layout": "full", "chartId": "denoising_development_lcb"},
    {"id": "denoising_development_block", "type": "table", "layout": "full", "tableId": "denoising_development_table"},
    {"id": "denoising_cohort_block", "type": "table", "layout": "full", "tableId": "denoising_cohort_table"},
    {"id": "denoising_option_block", "type": "table", "layout": "full", "tableId": "denoising_option_table"},
    {"id": "denoising_note", "type": "markdown", "layout": "full", "sourceId": "exact_bbo", "body": "RTH 的结果说明轻度 pre-average 有增量，但不是已确认 edge：它把 development event LCB 从 raw 的 +0.045R 提高到 +0.170R，validation 的固定 vertical 均值也从 +$8.53 提高到 +$37.94；然而 validation CI 为 [-8.94%, +14.38%]、已见尾部为 [-18.25%, +28.52%]，p=0.309/0.406。GTH 六条 pipeline 的开发 LCB 全部为负，Kalman 只能把固定 vertical 的亏损从约 -$58.76 缩到 -$46.48，不能把负 edge 变正。结论：RTH 的触发噪声是次要瓶颈且可改善；GTH 的核心瓶颈是方向信息与 SPXW payoff/执行，不是滤波器不够复杂。"},
    {"id": "followup_header", "type": "markdown", "layout": "full", "body": "## Layered Follow-up Audit\n\n四条路径分别承担不同职责：RTH 双轨只回答去噪是否有增量；GTH hurdle 把方向存在与期权值得买拆开；GEX 只做位置门控；CUSUM/BOCPD 只检测状态变化。它们不共享最终分数，也没有任何通知或策略权限。"},
    {"id": "denoising_forward_block", "type": "table", "layout": "full", "tableId": "denoising_forward_table"},
    {"id": "denoising_forward_note", "type": "markdown", "layout": "full", "sourceId": "notebook", "body": "双轨合同冻结时间为 2026-08-19 09:41:19 UTC，首个可计入交易日为 2026-08-20。raw 与 pre-average 必须在同一完整 session 上配对；除各自通过 20 session、30 笔、8 个 event session、覆盖率和统计门外，还要求 pre-average 减 raw 的 session 配对提升下界大于零且 p≤0.05。现有历史不能进入这份新合同，所以当前零 session、零笔和不可晋级都是防止回看污染的正确结果。"},
    {"id": "gth_hurdle_chart_block", "type": "chart", "layout": "full", "chartId": "gth_hurdle_stage_counts"},
    {"id": "gth_hurdle_chart_note", "type": "markdown", "layout": "full", "sourceId": "exact_bbo", "body": "GTH 第一层沿用已失败的 ES/Kalman 方向事件，第二层只在 fresh two-sided SPXW exact BBO 存在时用固定 Ridge 估计条件净收益。开发段 leave-one-session-out 共 32 笔只选中 1 笔，而该笔实际亏损 $116、收益率 -15.06%；validation 与 tail 均选择零笔。模型的有效行为是 fail closed，不是发现可交易 edge。"},
    {"id": "gth_hurdle_block", "type": "table", "layout": "full", "tableId": "gth_hurdle_table"},
    {"id": "gex_gate_chart_block", "type": "chart", "layout": "full", "chartId": "gex_gate_option_pnl"},
    {"id": "gex_gate_chart_note", "type": "markdown", "layout": "full", "sourceId": "exact_bbo", "body": "GEX gate 只使用决策时之前十分钟内的 RTH surface，要求现货位于 put/call wall 之间且顺势方向至少留有一个事件尺度空间。它在 development 把均值从 $13.26 提到 $27.21，但 validation/tail 的 gated 均值都变为负值，收益增量分别为 -7.24%/-6.35%。surface 覆盖率为 100%，所以失败来自关系不稳定，不是数据缺失。"},
    {"id": "gex_gate_block", "type": "table", "layout": "full", "tableId": "gex_gate_table"},
    {"id": "cusum_block", "type": "table", "layout": "full", "tableId": "cusum_table"},
    {"id": "bocpd_block", "type": "table", "layout": "full", "tableId": "bocpd_table"},
    {"id": "state_change_note", "type": "markdown", "layout": "full", "sourceId": "raw_quotes", "body": "CUSUM 触发在 development 为负，validation/tail 虽转正但 session 区间均跨零。BOCPD 用 60 秒 Kalman 标准化变化、1/60 hazard，并比较趋势启动前 15 分钟与匹配时钟对照；RTH 标准化差异 -0.355，GTH +0.195，0.20 阈值命中率均为零。两者最多保留为可视化状态诊断，不产生方向或交易。"},
    {"id": "algorithm_evidence", "type": "markdown", "layout": "full", "body": "### Why these algorithms\n\n- [Pre-averaging spot-volatility estimation](https://arxiv.org/abs/2004.01865) 和 [realized kernels](https://faculty.washington.edu/~ezivot/econ589/realizedKernelsInPractice.pdf) 提供了在微观结构噪声下先稳健估计局部路径/波动的依据。\n- [The Micro-Price](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2970694) 与 [order-flow imbalance](https://arxiv.org/abs/1011.6402) 支持把 microprice/OFI 作为短时观测，不把成交量方向当作真实持仓。\n- [Bayesian online changepoint detection](https://arxiv.org/abs/0710.3742) 支持把 run-length/change probability 独立成 State 层；本轮已与参数更少的 CUSUM 一并审计，两者都没有获得交易权限。\n- [White's Reality Check](https://onlinelibrary.wiley.com/doi/abs/10.1111/1468-0262.00152) 与 [backtest-overfitting analysis](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2308659) 是限制 pipeline 数量并报告 family-wise max-p 的原因。"},
    {"id": "day_type_header", "type": "markdown", "layout": "full", "sourceId": "raw_quotes", "body": "## Hindsight Regime and Event Atlas\n\n按你的建议把后视镜拆成两个轴：振幅 low ≤50 点、mid 50–80 点、high ≥80 点；路径独立标成 trend/range/mixed。RTH 25 个 session 的振幅为 12/9/4，路径为 7/7/11；GTH 26 个 session 的 ES 振幅为 21/4/1，路径为 6/7/13。全天标签只用于问‘后来是哪种日’，不会进入当时的事件特征。"},
    {"id": "day_label_block", "type": "table", "layout": "full", "tableId": "day_label_table"},
    {"id": "launch_effect_block", "type": "table", "layout": "full", "tableId": "launch_effect_table"},
    {"id": "launch_note", "type": "markdown", "layout": "full", "sourceId": "raw_quotes", "body": "RTH 趋势启动前较明显的是 SPX 5/10 分钟路径效率提升（标准化差异 1.50/1.73）；GTH 恰好相反，ES 5/10 分钟效率变化为 -2.06/-1.77。两个时段没有可共用的启动签名，而且各自只有 6–7 个有效启动样本；启动时点又由全天路径回看定义，所以这里只能生成分时段假设。"},
    {"id": "event_block", "type": "table", "layout": "full", "tableId": "event_table"},
    {"id": "event_option_block", "type": "table", "layout": "full", "tableId": "event_option_table"},
    {"id": "event_decision", "type": "markdown", "layout": "full", "sourceId": "exact_bbo", "body": "RTH GEX 墙位与 zero-gamma 左侧 fade 被明确否定。更重要的是，outright 结果不能为生产 Debit Vertical 背书：production-compatible breakout vertical 在 validation 为 +$8.15/笔但平均收益率 -0.14%，尾段 -$23.66，故拒绝；pullback 15 点 vertical 在 validation/尾段为 +$16.58/+27.21，但 development session-LCB -0.11%，95% 区间均跨零，仅保留为弱前向假设。GTH 两类事件 validation 与尾段全部为负，不冻结。"},
    {"id": "qualifier_block", "type": "table", "layout": "full", "tableId": "qualifier_table"},
    {"id": "qualifier_note", "type": "markdown", "layout": "full", "sourceId": "exact_bbo", "body": f"固定扫描只包含时段、路径效率、短长波动比、跨资产 breadth、VIX 反向确认、ES OFI 和冲动尺度这些决策时已知的单变量条件，不使用全天 trend/range 标签，也不组合条件。{qualifier_decision_text} 判定：{qualifier_decision}。无论结果如何，现有 v2 前向合约保持不变，扫描结果没有通知权限。"},
    {"id": "forward_block", "type": "table", "layout": "full", "tableId": "forward_table"},
    {"id": "forward_note", "type": "markdown", "layout": "full", "sourceId": "notebook", "body": "v2 冻结合约从 2026-08-19 开始，只接收全新完整 RTH session。唯一假设是冲动后回撤续行：60-delta long leg、15 点 Debit Vertical、两腿 selection relative spread ≤5%、+5 秒按 long ask-short bid 入场、first-passage 时按 long bid-short ask 退出。至少 20 个新 session、30 笔/8 个 event session、95% session bootstrap 下界 >0、单侧 session sign-flip p≤0.05 后，才允许讨论接入 strategy_decision；新增 RIGHT_PULLBACK_RESUME 人读 setup 仍需显式批准。"},
    {"id": "decision_block", "type": "table", "layout": "full", "tableId": "decision_table"},
    {"id": "scope", "type": "markdown", "layout": "full", "body": "## Scope, Data and Metrics\n\nRTH 预测 fresh SPX；GTH 因现货 SPX 非 live，明确预测 ES proxy，两个 mode 从不 pooled。输入是 sampled near-tick quote updates，不是每个交易所 packet。knowledge guard 要求 live、source_at 不早于 received_at 30 秒且不晚于 5 秒、quote_time 不得来自未来；五秒桶只在 bucket_end 后可用。"},
    {"id": "quality_block", "type": "table", "layout": "full", "tableId": "quality_table"},
    {"id": "methodology", "type": "markdown", "layout": "full", "body": "## Methodology and Model Validation\n\n1. 从 raw quote updates 构建 5 秒 causal grid；每 15 秒产生一个研究观察。\n2. 特征按 price、L1 microstructure、cross-market、state 四块递增；没有 VWAP 墙位、现有 setup 或管理变量。\n3. 方向目标是未来 log return bps，持有期自由覆盖 15 秒到 60 分钟；置信门覆盖 50%–99%。\n4. 最早 11/12 个 session 训练，中间 8 个选择模型、窗口和预测置信分位，最后 6 个作为测试段。\n5. 搜索 Ridge 与低容量 HistGradientBoosting；validation 以非重叠信号的 session-cluster 90% 下界排序，至少 25 个信号、6 个 session。\n6. 每个 mode 只落一个方向冠军；跨 RTH/GTH 对精确 session sign-flip p-value 做 Holm 校正。\n7. 期权联合层只接纳方向 validation 下界为正的窗口冠军，再比较 16 种结构和 1%/2%/5%/10%/20% 相对 spread 门；合约按决策时已知 delta 选择，五秒后才成交。\n8. 去噪 benchmark 固定六条 pipeline，在 development 只选一次；模型选择后的 p-value 采用跨 pipeline 的 exact session sign-flip maximum statistic。冠军随后固定为 60-delta/15 点 vertical，RTH/GTH 相对 spread 门分别固定为 5%/2%。\n9. RTH 双轨前向合同在全部历史结果已知之后冻结；只接收 2026-08-20 起的完整新 session，并同时检验各 lane 与 session 配对增量。\n10. GTH hurdle 第一层只定义固定方向事件，第二层以 development session leave-one-out Ridge 预测 exact-BBO 净收益；validation/tail 不重估参数。\n11. GEX 只使用 decision_at 之前十分钟内的 surface，作为 pre-average 事件的 location gate；它不生成方向。\n12. BOCPD 只在 60 秒 Kalman 标准化 path 上生成 change probability，趋势启动标签仅用于盘后匹配对照，不进入交易触发。\n13. 极端置信、spread 门及所有 follow-up benchmark 均发生在原 test 已被看过之后，所以 validation/tail 只用于 retrospective falsification；它们不能成为新的 independent confirmation。"},
    {"id": "direction_table_block", "type": "table", "layout": "full", "tableId": "direction_table"},
    {"id": "search_table_block", "type": "table", "layout": "full", "tableId": "search_table"},
    {"id": "option_table_block", "type": "table", "layout": "full", "tableId": "option_table"},
    {"id": "factor_header", "type": "markdown", "layout": "full", "body": "## Factor Decomposition"},
    {"id": "factor_chart", "type": "chart", "layout": "full", "chartId": "factor_share"},
    {"id": "factor_note", "type": "markdown", "layout": "full", "body": "RTH 标准化绝对 loading 中 cross 占 55.9%、micro 占 26.9%、自身 price 占 17.1%；GTH 则 price 44.5%、state 18.0%、cross 19.0%、micro 18.5%。这实现了因子块解耦，但高度相关特征的线性 loading 不能解释为因果贡献。"},
    {"id": "loading_block", "type": "table", "layout": "full", "tableId": "loading_table"},
    {"id": "limitations", "type": "markdown", "layout": "full", "body": "## Limitations, Uncertainty and Robustness\n\n只有 25/26 个完整 session，封存检验每个 mode 只有 6 个 session；精确 sign-flip 的最小单侧 p-value 是 1/64。RTH SPX mark 的短时可预测性可能是 cross-market repricing/指数发布延迟，而不是能交易的错误定价，5–10 秒延迟和期权 BBO 已支持这一反证。GTH 用 ES proxy，不代表 SPX cash 方向可直接成交。历史期权 universe 受当时订阅 lattice 影响；本研究只在 fresh exact BBO 存在时计入，不能证明未订阅合约的结果。联合搜索与去噪 benchmark 都在人类看过第一次 sealed 方向/期权结果后扩展，因此是更完整的 falsification，不是新的 independent confirmatory trial。RTH pre-average 的正均值和 family-wise development p 值只说明它值得继续观察；validation/tail 区间跨零，不能称为稳定 edge。GTH hurdle 的 development OOF 只选中一笔，无法估计稳定条件分布；GEX gate 的开发提升在两个后续 cohort 反号；BOCPD 只有 RTH 7 个、GTH 6 个 hindsight launch，且 0.20 门没有一次命中。这些结果足以拒绝当前规格，却不足以证明所有同类算法永久无效。"},
    {"id": "next_steps", "type": "markdown", "layout": "full", "body": "## Recommended Next Steps\n\n1. 不把任何本次候选接入 Trade Ready；当前可执行结论仍是 NO EDGE / NO TRADE。\n2. 保持 v2 前向合约与哈希不变；另按新冻结的双轨合同，从 2026-08-20 起自动累计 raw 与 15 秒 pre-average 的同日完整 RTH session。\n3. 双轨合同达到 20 个新 session、各 30 笔/8 个 event session 前不读中途输赢、不调阈值；届时同时报告各 lane 和 pre-average-minus-raw 的 session 配对检验。\n4. 停止当前 GTH hurdle 规格和方向滤波器搜索：上游方向层未通过，条件 EV 层又只学会零选择。若未来研究 XSP/SPY/MES 等更低摩擦 payoff，应另立合同，不能把本结果包装为 SPXW edge。\n5. GEX 保持 RTH 描述性位置字段，不做方向与 gate；CUSUM/BOCPD 保持诊断性状态字段，不形成交易。高波震荡反转仍必须另开前向假设，不能把全天 >80 点标签放进盘中特征。"},
    {"id": "questions", "type": "markdown", "layout": "full", "body": "## Further Questions\n\n- RTH 的 SPY/ES→SPX 重定价在 option combo NBBO 本身是否存在 1–3 秒 lead，而不是只存在于 SPX mark？\n- GTH 30 分钟 edge 能否在更低 spread 的 XSP/SPY 或 MES/ES options 上存活，还是仅 SPXW 成本过高？\n- 下一批数据是否应随机记录固定 delta lattice 的 inclusion probability，以便对历史订阅缺失做加权估计？"},
    {"id": "reproducibility", "type": "markdown", "layout": "full", "sourceId": "notebook", "body": "## Reproducibility\n\n伴随 notebook 从 raw Parquet top-to-bottom 执行并生成 snapshot。全部工作为只读研究：没有读取生产策略候选，没有修改配置/服务/数据库，没有通知或订单。"},
]

artifact = {
    "surface": "report",
    "manifest": {
        "version": 1,
        "surface": "report",
        "title": "SPX 0DTE 原始行情 Edge Discovery",
        "description": "Strategy-independent raw quote-update discovery, held-out directional tests and exact-BBO option falsification for RTH and GTH.",
        "generatedAt": generated_at,
        "sources": sources,
        "cards": cards,
        "charts": charts,
        "tables": tables,
        "blocks": blocks,
    },
    "snapshot": {
        "version": 1,
        "generatedAt": generated_at,
        "status": "ready",
        "datasets": {
            "headline": headline,
            "quality_rows": quality_rows,
            "direction_rows": direction_rows,
            "direction_session_rows": direction_session_rows,
            "delay_rows": delay_rows,
            "option_rows": option_rows,
            "option_session_rows": option_session_rows,
            "factor_share_rows": factor_share_rows,
            "factor_loading_rows": factor_loading_rows,
            "search_rows": search_rows,
            "decision_rows": decision_rows,
            "day_label_rows": day_label_rows,
            "launch_effect_rows": launch_effect_rows,
            "event_rows": event_rows,
            "event_option_rows": event_option_rows,
            "qualifier_rows": qualifier_rows,
            "forward_rows": forward_rows,
            "denoising_development_rows": denoising_development_rows,
            "denoising_cohort_rows": denoising_cohort_rows,
            "denoising_option_rows": denoising_option_rows,
            "denoising_forward_rows": denoising_forward_rows,
            "gth_hurdle_rows": gth_hurdle_rows,
            "gth_hurdle_stage_rows": gth_hurdle_stage_rows,
            "gex_gate_rows": gex_gate_rows,
            "cusum_rows": cusum_rows,
            "bocpd_rows": bocpd_rows,
        },
    },
}

OUTPUT_PATH.write_text(
    json.dumps(artifact, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
    encoding="utf-8",
)
print(OUTPUT_PATH)
