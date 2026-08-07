"""Causal legacy-vs-v2 Vertical replay over existing quote artifacts."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from spx_spark.analytics.options.strategy_payoff import vertical_entry_quality
from spx_spark.application.order_map.strategy_regime import DEFAULT_STRATEGY_POLICY
from spx_spark.infrastructure.operational_db import read_strategy_decisions

SLIPPAGE_GRID = (0.0, 0.05, 0.10, 0.20)


def load_strategy_decisions_for_replay(
    database_path: str | Path,
    *,
    session_date: str | None = None,
) -> tuple[dict[str, object], ...]:
    """Load the authoritative SQL decision payloads; JSON is export-only."""

    return read_strategy_decisions(
        database_path=database_path,
        session_date=session_date,
    )


def classify_gth_vertical_record(
    record: Mapping[str, Any],
    *,
    atr_5m: float | None,
    distance_to_vwap_points: float | None = None,
    impulse_15m_points: float | None = None,
) -> dict[str, Any]:
    decision_at = _time(record.get("evaluated_at"))
    snapshot = _map(record.get("exact_spread_snapshot"))
    quote_times = [_time(_map(snapshot.get(side)).get("source_at")) for side in ("long", "short")]
    available_at = max((item for item in quote_times if item), default=decision_at)
    path_kind = str(record.get("path_kind") or "")
    reasons = ["trend_background_cannot_authorize_entry"] if path_kind.startswith("trend_transition_") else []
    values = {name: _number(record.get(name)) for name in (
        "current_parity_spx", "target_spx", "invalidation_spx", "trigger_level",
        "spread_width_points", "decision_ask",
    )}
    quality: dict[str, float] = {}
    if decision_at is None or available_at is None:
        reasons.append("historical_lineage_unavailable")
    if atr_5m is None or any(value is None for value in values.values()):
        reasons.append("historical_entry_quality_inputs_unavailable")
    else:
        width, ask = float(values["spread_width_points"]), float(values["decision_ask"])
        if width <= 0 or not 0 < ask < width:
            reasons.append("vertical_contract_geometry_invalid")
        else:
            setup = "FAILED_BREAK_RECLAIM" if any(
                token in path_kind for token in ("rejection", "reclaim", "dip")
            ) else "TREND_PULLBACK"
            quality, gate_reasons = vertical_entry_quality(
                spot=float(values["current_parity_spx"]),
                atr=float(atr_5m),
                target=float(values["target_spx"]),
                stop=float(values["invalidation_spx"]),
                trigger=float(values["trigger_level"]),
                direction=str(record.get("direction") or "").upper(),
                setup_kind=setup,
                distance_to_vwap_points=distance_to_vwap_points,
                impulse_15m_points=impulse_15m_points,
                debit_fraction=ask / width,
                thresholds=DEFAULT_STRATEGY_POLICY.entry_quality_kwargs(),
            )
            reasons.extend(gate_reasons)
    required = (
        "long_contract_id", "short_contract_id", "decision_ask", "target_spx",
        "invalidation_spx", "valid_until",
    )
    return {
        "opportunity_id": str(record.get("candidate_id") or ""),
        "session_date": str(record.get("session_date") or ""),
        "decision_at": decision_at.isoformat() if decision_at else None,
        "available_at": available_at.isoformat() if available_at else None,
        "new_action": "TRADE" if not reasons else "NO_TRADE",
        "new_reason": reasons[0] if reasons else "vertical_entry_quality_passed",
        "entry_quality": quality,
        "manual_candidate_complete": all(record.get(key) is not None for key in required),
        "automatic_ordering": record.get("automatic_ordering") is True,
    }


def build_vertical_replay_report(
    opportunities: Sequence[Mapping[str, Any]],
    decisions: Sequence[Mapping[str, Any]],
    *,
    frozen_cases: Mapping[str, bool] | None = None,
    covered_sessions: Sequence[str] | None = None,
    minimum_sessions: int = 15,
) -> dict[str, Any]:
    by_id = {str(row.get("opportunity_id") or ""): row for row in decisions}
    joined, violations = [], []
    for opportunity in opportunities:
        opportunity_id = str(opportunity.get("opportunity_id") or "")
        decision = by_id.get(opportunity_id)
        if not decision:
            continue
        decision_at, available_at = _time(decision.get("decision_at")), _time(decision.get("available_at"))
        if decision_at is None or available_at is None or available_at > decision_at:
            violations.append(opportunity_id or "missing_opportunity_id")
            continue
        latency_rows = [row for row in opportunity.get("latency_sensitivity") or () if isinstance(row, Mapping)]
        baseline = next((row for row in latency_rows if row.get("latency_seconds") == 0), {})
        costs = {
            float(row["per_leg_side_slippage_points"]): float(row["net_pnl_usd"])
            for row in _map(baseline.get("cost")).get("slippage_sensitivity") or ()
            if isinstance(row, Mapping)
            and _number(row.get("per_leg_side_slippage_points")) in SLIPPAGE_GRID
            and _number(row.get("net_pnl_usd")) is not None
        }
        if tuple(sorted(costs)) == SLIPPAGE_GRID:
            joined.append({
                "id": opportunity_id,
                "session": decision.get("session_date"),
                "action": decision.get("new_action"),
                "reason": decision.get("new_reason"),
                "complete": decision.get("manual_candidate_complete") is True,
                "pnl": costs,
            })

    comparisons = []
    for slippage in SLIPPAGE_GRID:
        legacy = [row["pnl"][slippage] for row in joined]
        new = [pnl if row["action"] == "TRADE" else 0.0 for row, pnl in zip(joined, legacy)]
        comparisons.append({
            "per_leg_side_slippage_points": slippage,
            "legacy_net_pnl_usd": round(sum(legacy), 2),
            "new_net_pnl_usd": round(sum(new), 2),
            "legacy_expected_shortfall_usd": _es(legacy),
            "new_expected_shortfall_usd": _es(new),
        })
    reference = comparisons[1]
    late_loss = sum(
        row["pnl"][0.05] for row in joined
        if row["reason"] == "direction_valid_but_entry_too_late" and row["pnl"][0.05] < 0
    )
    sessions = sorted({str(row["session"]) for row in joined if row.get("session")})
    coverage, cases = sorted(set(covered_sessions or sessions)), dict(frozen_cases or {})
    checks = {
        "no_lookahead": not violations,
        "minimum_sessions": len(coverage) >= minimum_sessions,
        "two_metric_improvement": sum((
            reference["new_net_pnl_usd"] > reference["legacy_net_pnl_usd"],
            reference["new_expected_shortfall_usd"] > reference["legacy_expected_shortfall_usd"],
            late_loss < 0,
        )) >= 2,
        "frozen_2026_08_05": cases.get("2026-08-05") is True,
        "frozen_2026_08_06": cases.get("2026-08-06") is True,
        "manual_candidate_complete": any(row["action"] == "TRADE" for row in joined)
        and all(row["action"] != "TRADE" or row["complete"] for row in joined),
        "automatic_ordering_false": all(row.get("automatic_ordering") is not True for row in decisions),
    }
    return {
        "schema_version": "strategy_decision_vertical_replay.v1",
        "slippage_grid": list(SLIPPAGE_GRID),
        "comparable_opportunities": len(joined),
        "covered_sessions": coverage,
        "candidate_sessions": sessions,
        "lookahead_violations": violations,
        "legacy_vs_new": comparisons,
        "late_chase_legacy_loss_usd": round(late_loss, 2),
        "bootstrap_gate": {"status": "pass" if all(checks.values()) else "collecting", "checks": checks},
    }


def _es(values: Sequence[float]) -> float:
    count = max(1, math.ceil(len(values) * 0.10))
    return round(sum(sorted(values)[:count]) / count, 2) if values else 0.0


def _map(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    return float(value) if isinstance(value, int | float) else None


def _time(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else None
