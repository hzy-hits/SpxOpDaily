"""Causal legacy-vs-v2 Vertical replay over existing quote artifacts."""

from __future__ import annotations

import math
import argparse
import ast
import hashlib
import json
import re
import sqlite3
import subprocess
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

from spx_spark.analytics.options.strategy_payoff import vertical_entry_quality
from spx_spark.analytics.options.strategy_payoff import ManagementPolicy, POLICY_PRICE_EPSILON, policy_mark_horizon_end
from spx_spark.data_platform.research.odte_level_quotes import QuoteStore
from spx_spark.data_platform.research.strategy_policy_backfill import _candidate_legs, _combo_quotes, _label_decision
from spx_spark.application.order_map.strategy_regime import DEFAULT_STRATEGY_POLICY
from spx_spark.infrastructure.operational_db import read_strategy_decisions

SLIPPAGE_GRID = (0.0, 0.05, 0.10, 0.20)
# Frozen sensitivity scenarios, not measured human reaction times or fill rates.
REACTION_SECONDS = (0, 15, 30, 60)


def audit_strategy_pushes(
    decisions: Sequence[Mapping[str, Any]], notifications: Sequence[Mapping[str, Any]], *,
    store: QuoteStore, repository: Path,
) -> dict[str, Any]:
    """Replay the exact decisions admitted to trade_ready, including failed delivery.

    This reconstructs the historical selection sequence; it cannot invent a
    fill or re-enumerate absent historical inputs under a different policy.
    """
    index = {(_map(row.get("candidate")).get("opportunity_id"), _time(row.get("decision_at"))): row
             for row in decisions}
    events: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for notification in notifications:
        if notification.get("lane") == "trade_ready":
            events[str(_map(notification.get("envelope")).get("event_id"))].append(notification)
    rows = []
    for event_id, receipts in sorted(events.items(), key=lambda item: str(item[1][0].get("created_at"))):
        envelope = _map(receipts[0].get("envelope"))
        occurred_at = _time(envelope.get("occurred_at"))
        opportunity = event_id.removesuffix(":ready")
        delivery_times = [_time(row.get("delivered_at")) for row in receipts]
        delivered_at = min((at for at in delivery_times if at is not None), default=None)
        base = {"event_id": event_id, "channel_records": len(receipts),
                "delivered": any(row.get("status") == "delivered" for row in receipts),
                "delivered_at": delivered_at.isoformat() if delivered_at else None,
                "fill_status": "UNKNOWN", "position_status": "UNKNOWN"}
        if len({str(_map(row.get("envelope")).get("occurred_at")) for row in receipts}) != 1:
            rows.append({**base, "label_status": "NOTIFICATION_SNAPSHOT_CONFLICT"})
            continue
        decision = index.get((opportunity, occurred_at))
        if decision is None:
            rows.append({**base, "label_status": "DECISION_UNAVAILABLE"})
            continue
        candidate = _map(decision.get("candidate"))
        quote_until = _time(candidate.get("quote_valid_until"))
        opportunity_until = _time(candidate.get("opportunity_valid_until"))
        base.update({"decision_id": decision.get("decision_id"),
                     "session_date": decision.get("session_date"),
                     "strategy_version": decision.get("policy_version"),
                     "strategy_type": decision.get("decision_type"),
                     "setup_kind": candidate.get("setup_kind"),
                     "quote_expired_at_delivery": (delivered_at >= quote_until
                         if delivered_at is not None and quote_until is not None else None),
                     "opportunity_expired_at_delivery": (delivered_at >= opportunity_until
                         if delivered_at is not None and opportunity_until is not None else None),
                     "decision_to_delivery_seconds": ((delivered_at - occurred_at).total_seconds()
                         if delivered_at is not None and occurred_at is not None else None)})
        frozen = _map(decision.get("management_contract"))
        if not frozen:
            constants = _historical_management_constants(repository, str(decision.get("runtime_git_sha") or ""))
            name = ("RTH_IRON_CONDOR_MANAGEMENT_POLICY" if candidate.get("session_mode") == "rth"
                    else "IRON_CONDOR_MANAGEMENT_POLICY") if decision.get("decision_type") == "IRON_CONDOR" else (
                        "PIN_BUTTERFLY_MANAGEMENT_POLICY" if candidate.get("setup_kind") == "STABLE_PIN"
                        else "CLOSE_CONVERGENCE_BUTTERFLY_MANAGEMENT_POLICY" if candidate.get("setup_kind") == "CLOSE_CONVERGENCE_60M"
                        else "DEFAULT_MANAGEMENT_POLICY")
            if name == "PIN_BUTTERFLY_MANAGEMENT_POLICY" and name not in constants:
                name = "DEFAULT_MANAGEMENT_POLICY"
            frozen = constants.get(name) or {}
            base["policy_source"] = f"git:{decision.get('runtime_git_sha')}:{name}"
        if not frozen or (decision.get("decision_type") == "IRON_CONDOR" and frozen.get("entry_side", "debit") != "credit"):
            rows.append({**base, "label_status": "POLICY_UNRECOVERABLE"})
            continue
        decision = {**decision, "management_contract": frozen}
        scenarios = _delivery_entry_scenarios(decision, delivered_at=delivered_at, store=store) if base["delivered"] else []
        label = _label_decision(decision, store=store, lookforward_minutes=24 * 60)
        exit_at = _time(label.get("exit_at"))
        rows.append({**base, **label, "entry_assumption": "decision_time_bbo_not_notification_execution",
                     "exit_before_delivery": exit_at < delivered_at if exit_at and delivered_at else None,
                     "delivery_entry_scenarios": scenarios})
    completed = [row for row in rows if row.get("delivered") and row.get("label_status") == "COMPLETE_EXIT"]
    values = [float(row["policy_pnl_points"]) for row in completed]
    produced_opportunities = {str(_map(row.get("candidate")).get("opportunity_id")) for row in decisions}
    sensitivity = []
    for lag in REACTION_SECONDS:
        cases = [next((case for case in row.get("delivery_entry_scenarios", []) if case["reaction_seconds"] == lag),
                      {"label_status": row["label_status"]}) for row in rows if row.get("delivered")]
        complete = [case["policy_pnl_points"] for case in cases if case["label_status"] == "COMPLETE_EXIT"]
        sensitivity.append({"reaction_seconds": lag, "delivered_events": len(cases),
                            "label_status_counts": dict(Counter(case["label_status"] for case in cases)),
                            "complete_case_n": len(complete),
                            "conditional_mean_net_pnl_points": sum(complete) / len(complete) if complete else None})
    return {
        "source_selected_decisions": len(decisions),
        "unique_selected_opportunities": len(produced_opportunities),
        "selected_opportunities_without_trade_notification": len(produced_opportunities - {event.removesuffix(":ready") for event in events}),
        "source_notification_records": len(notifications),
        "unique_trade_events": len(rows),
        "delivered_trade_events": sum(row.get("delivered", False) for row in rows),
        "independent_sessions": len({row.get("session_date") for row in rows if row.get("session_date")}),
        "label_status_counts": dict(Counter(row["label_status"] for row in rows)),
        "delivered_label_status_counts": dict(Counter(row["label_status"] for row in rows if row.get("delivered"))),
        "quote_expired_at_delivery": sum(row.get("quote_expired_at_delivery") is True for row in rows),
        "opportunity_expired_at_delivery": sum(row.get("opportunity_expired_at_delivery") is True for row in rows),
        "decision_time_exit_before_delivery": sum(row.get("exit_before_delivery") is True for row in rows),
        "delivery_entry_sensitivity": sensitivity,
        "complete_case_diagnostics": {
            "n": len(values), "fees_included": True,
            "mean_net_pnl_points": sum(values) / len(values) if values else None,
            "worst_net_pnl_points": min(values) if values else None,
            "slippage_grid_mean_points": {str(slip): sum(values) / len(values) - slip if values else None for slip in SLIPPAGE_GRID},
        },
        "evidence_limits": ["fills_unknown", "incomplete_exits_excluded_from_pnl_only_not_denominator",
                            "decision_time_pnl_does_not_measure_execution_after_delivery",
                            "complete_cases_are_not_policy_expected_return",
                            "historical_policy_not_current_policy_performance",
                            "quote_source_and_received_time_checked_ingestion_manifest_unavailable",
                            "no_independent_alpha_or_feature_ablation_claim"],
        "rows": rows,
    }


def _delivery_entry_scenarios(
    decision: Mapping[str, Any], *, delivered_at: datetime | None, store: QuoteStore,
) -> list[dict[str, Any]]:
    """Original card limit at the first valid BBO after receipt plus reaction delay.

    A crossing BBO is only an explicit fill assumption. Later cheaper quotes
    are not searched when the first fresh book cannot meet the card limit.
    Entry uses the common 15s/2s research BBO requirement; a position's looser
    management quote limits do not become permission to enter on stale legs.
    """
    candidate = _map(decision.get("candidate"))
    policy = ManagementPolicy(**decision["management_contract"])
    start, expiry = _time(decision.get("decision_at")), _time(candidate.get("opportunity_valid_until"))
    legs = _candidate_legs(candidate)
    limit = _number(_map(candidate.get("quote")).get("bid" if policy.entry_side == "credit" else "ask"))
    if delivered_at is None or start is None or expiry is None or not legs or limit is None or limit <= 0:
        return [{"reaction_seconds": lag, "label_status": "ENTRY_EVIDENCE_UNAVAILABLE"} for lag in REACTION_SECONDS]
    session_date = datetime.fromisoformat(str(decision["session_date"])).date()
    controls = dict(legs=legs, provider=str(legs[0].get("provider") or "schwab"),
                    max_quote_age_seconds=float(_map(candidate.get("management_plan")).get("management_quote_max_age_seconds") or 15),
                    max_source_skew_seconds=float(_map(candidate.get("management_plan")).get("management_quote_max_skew_seconds") or 2))
    # Capture once with enough coverage for a time stop measured from delayed entry.
    latest_entry = min(expiry, delivered_at + timedelta(seconds=max(REACTION_SECONDS) + 60))
    end = policy_mark_horizon_end(latest_entry, policy, session_date=session_date) + timedelta(seconds=60)
    _combo_quotes(store, start=start, end=max(end, expiry), **controls)
    result = []
    for lag in REACTION_SECONDS:
        action = delivered_at + timedelta(seconds=lag)
        row = {"reaction_seconds": lag, "action_at": action.isoformat(), "entry_limit_points": limit,
               "entry_quote_max_age_seconds": 15, "entry_quote_max_source_skew_seconds": 2,
               "fill_status": "UNKNOWN", "entry_assumption": "first_fresh_bbo_crosses_original_limit"}
        if action < start or action >= policy_mark_horizon_end(action, policy, session_date=session_date):
            result.append({**row, "label_status": "SESSION_ERROR"})
            continue
        if action >= expiry:
            result.append({**row, "label_status": "OPPORTUNITY_EXPIRED"})
            continue
        quotes = _combo_quotes(store, start=action, end=min(expiry, action + timedelta(seconds=60)),
                               legs=legs, provider=controls["provider"])
        first = next((quote for quote in quotes if quote[0] < expiry), None)
        if first is None:
            result.append({**row, "label_status": "ENTRY_QUOTE_UNAVAILABLE"})
            continue
        entry_at, purchase, _liquidation = first
        price = -purchase if policy.entry_side == "credit" else purchase
        crossed = price >= limit - POLICY_PRICE_EPSILON if policy.entry_side == "credit" else price <= limit + POLICY_PRICE_EPSILON
        row.update({"entry_quote_at": entry_at.isoformat(), "entry_bbo_points": price})
        if price <= 0 or not crossed:
            result.append({**row, "label_status": "ENTRY_LIMIT_UNAVAILABLE"})
            continue
        label = _label_decision(decision, store=store, lookforward_minutes=24 * 60,
                                entry_at=entry_at, entry_price=limit)
        result.append({**label, **row})
    return result


@lru_cache(maxsize=64)
def _historical_management_constants(repository: Path, revision: str) -> dict[str, dict[str, Any]]:
    """Read literal policy constants from the recorded revision without executing old code."""
    if not re.fullmatch(r"[0-9a-f]{7,40}", revision):
        return {}
    source = subprocess.run(
        ["git", "show", f"{revision}:src/spx_spark/analytics/options/strategy_payoff.py"],
        cwd=repository, text=True, capture_output=True, check=False,
    )
    if source.returncode:
        return {}
    try:
        tree = ast.parse(source.stdout)
        cls = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "ManagementPolicy")
        defaults = {node.target.id: ast.literal_eval(node.value) for node in cls.body
                    if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.value is not None}
        result = {}
        for node in tree.body:
            if (isinstance(node, ast.Assign) and isinstance(node.value, ast.Call)
                    and isinstance(node.value.func, ast.Name) and node.value.func.id == "ManagementPolicy"):
                try:
                    keywords = {kw.arg: ast.literal_eval(kw.value) for kw in node.value.keywords}
                except ValueError:
                    # An unsupported policy expression does not erase the
                    # independently readable debit and butterfly contracts.
                    continue
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        result[target.id] = asdict(ManagementPolicy(**{**defaults, **keywords}))
        return result
    except (SyntaxError, ValueError, TypeError, StopIteration):
        return {}


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
    europe_transition = bool(
        path_kind.startswith("trend_transition_")
        and str(record.get("source_kind") or "") == "gth_es_trend_transition"
        and str(record.get("source_segment") or "") == "europe"
    )
    reasons = (
        ["trend_background_cannot_authorize_entry"]
        if path_kind.startswith("trend_transition_") and not europe_transition
        else []
    )
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
            setup = (
                "EUROPE_TREND_TRANSITION"
                if europe_transition
                else "FAILED_BREAK_RECLAIM"
                if any(token in path_kind for token in ("rejection", "reclaim", "dip"))
                else "TREND_PULLBACK"
            )
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


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Audit actual strategy pushes against frozen decisions and real quote paths.")
    parser.add_argument("--database", type=Path)
    parser.add_argument("--data-root", type=Path)
    parser.add_argument("--snapshot-root", type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    args = parser.parse_args(argv)
    if args.output_root.exists() and any(args.output_root.iterdir()):
        parser.error("output-root must be empty; keep prior research evidence immutable")
    args.output_root.mkdir(parents=True, exist_ok=True)
    if args.snapshot_root is not None:
        prior_manifest = json.loads((args.snapshot_root / "manifest.json").read_text())
        for name, expected in {**{f"{name}.json": digest for name, digest in prior_manifest["inputs"].items()},
                               "quote-series.jsonl": prior_manifest["quote_snapshot_sha256"]}.items():
            with (args.snapshot_root / name).open("rb") as handle:
                if hashlib.file_digest(handle, "sha256").hexdigest() != expected:
                    parser.error(f"snapshot checksum mismatch: {name}")
        selected = json.loads((args.snapshot_root / "selected-decisions.json").read_text())
        notifications = json.loads((args.snapshot_root / "notifications.json").read_text())
        decision_counts = prior_manifest["decision_counts"]
    else:
        if args.database is None or args.data_root is None:
            parser.error("supply --snapshot-root or both --database and --data-root")
        with sqlite3.connect(f"file:{args.database.resolve()}?mode=ro", uri=True) as connection:
            connection.execute("PRAGMA query_only=ON")
            connection.execute("BEGIN")
            notifications = []
            for row in connection.execute(
                "SELECT n.id,n.logical_event_id,n.kind,n.lane,n.status,n.created_at,"
                "json_extract(n.payload_json,'$.envelope'),"
                "(SELECT MIN(a.finished_at) FROM notification_attempts a WHERE a.event_id=n.id AND a.ok=1) "
                "FROM notification_events n WHERE n.source='strategy_decision' ORDER BY n.created_at,n.id"
            ):
                item = dict(zip(("id", "logical_event_id", "kind", "lane", "status", "created_at", "envelope", "delivered_at"), row, strict=True))
                item["envelope"] = json.loads(item["envelope"] or "{}")
                # SQLite stores UTC without offsets; the research clock is explicit.
                if item["delivered_at"]:
                    item["delivered_at"] = datetime.fromisoformat(item["delivered_at"]).replace(tzinfo=timezone.utc).isoformat()
                notifications.append(item)
            selected = []
            for ident, event_key, raw in connection.execute(
                "SELECT decision_id,event_key,attributes_json FROM decisions "
                "WHERE strategy_name='strategy_signal_engine_v2' AND status='selected' ORDER BY decision_at,decision_id"
            ):
                selected.append({**json.loads(raw), "decision_id": ident, "event_key": event_key})
            decision_counts = dict(connection.execute(
                "SELECT status,COUNT(*) FROM decisions WHERE strategy_name='strategy_signal_engine_v2' GROUP BY status"
            ))
            connection.rollback()
    manifest = {"decision_counts": decision_counts, "inputs": {}, "sources": {}}
    for name, rows in (("selected-decisions", selected), ("notifications", notifications)):
        encoded = json.dumps(rows, ensure_ascii=False, sort_keys=True).encode()
        (args.output_root / f"{name}.json").write_bytes(encoded)
        manifest["inputs"][name] = hashlib.sha256(encoded).hexdigest()
    repository = Path(__file__).resolve().parents[4]
    for relative in ("uv.lock", "src/spx_spark/analytics/options/strategy_payoff.py",
                     "src/spx_spark/data_platform/research/odte_level_quotes.py",
                     "src/spx_spark/data_platform/research/strategy_policy_backfill.py",
                     "src/spx_spark/data_platform/research/strategy_decision_replay.py"):
        manifest["sources"][relative] = hashlib.sha256((repository / relative).read_bytes()).hexdigest()
    store = QuoteStore(args.data_root or args.snapshot_root)
    try:
        if args.snapshot_root is not None:
            store.load_snapshot(args.snapshot_root / "quote-series.jsonl")
        report = audit_strategy_pushes(selected, notifications, store=store, repository=repository)
        manifest["quote_snapshot_sha256"] = store.write_snapshot(args.output_root / "quote-series.jsonl")
    finally:
        store.close()
    (args.output_root / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True))
    (args.output_root / "push-audit.json").write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    print(json.dumps({key: value for key, value in report.items() if key != "rows"}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
