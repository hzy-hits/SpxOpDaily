"""Frozen walk-forward exploration over pre-labeled ATM 10-wide debit rows."""

from collections import defaultdict
from collections.abc import Mapping, Sequence
import json
import math
from pathlib import Path
import statistics
from typing import Any

import numpy as np


TRAIN = ("2026-07-06", "2026-07-31")
HOLDOUT = ("2026-08-03", "2026-08-17")
MODES = ("bar_5m", "session_first", "session_mean")
Q_FACTORS = {
    "debit": ("debit_fraction_of_width", False),
    "straddle": ("atm_straddle_mid", False),
    "iv_skew": ("iv_skew", False),
    "spread": ("quote_spread_fraction", False),
    "abs_ret_5m": ("spot_ret_5m", True),
    "minutes": ("minutes_to_close", False),
    "hour": ("hour_et", False),
}


def _specs() -> list[dict[str, Any]]:
    specs = [
        ("all", "unconditional", [], "每个可用 bar 同时买 call 与 put"),
        ("rth", "unconditional", [("eq", "session_mode", "rth")], "仅 RTH"),
        ("gth", "unconditional", [("eq", "session_mode", "gth")], "仅 GTH"),
        ("call", "unconditional", [("eq", "direction", "call")], "仅 call"),
        ("put", "unconditional", [("eq", "direction", "put")], "仅 put"),
    ]
    for name, label in (("cheaper", "较便宜"), ("expensive", "较贵")):
        specs.append((name, "one_side", [("side", name)], f"每个 bar 只买{label}的一边；同价跳过"))
    for horizon in (5, 15, 60):
        for relation, label in (("same", "同向"), ("reverse", "反向")):
            name = f"ret_{horizon}m_{relation}"
            specs.append((name, "one_side", [("side", name)], f"只买与 {horizon}m 收益{label}的一边"))
    single = (
        ("debit_q1", "debit", 1), ("straddle_q1", "straddle", 1),
        ("iv_skew_q5", "iv_skew", 5), ("spread_q1", "spread", 1),
        ("abs_ret_5m_q5", "abs_ret_5m", 5), ("minutes_q1", "minutes", 1),
        ("hour_q5", "hour", 5),
    )
    for name, factor, bucket in single:
        specs.append((name, "single_factor", [("q", factor, bucket)], f"train {factor} 的 q{bucket}"))
    specs.extend(
        [
            ("ret_5m_same_rth", "two_factor", [("side", "ret_5m_same"), ("eq", "session_mode", "rth")], "RTH 且买 5m 同向一边"),
            ("cheaper_rth", "two_factor", [("side", "cheaper"), ("eq", "session_mode", "rth")], "RTH 且买较便宜一边"),
            ("debit_q1_ret_5m_same", "two_factor", [("side", "ret_5m_same"), ("q", "debit", 1)], "买 5m 同向一边且 debit 位于 train q1"),
            ("debit_q2_ret_5m_same", "two_factor", [("side", "ret_5m_same"), ("q", "debit", 2)], "买 5m 同向一边且 debit 位于 train q2"),
            ("straddle_q1_rth", "two_factor", [("q", "straddle", 1), ("eq", "session_mode", "rth")], "RTH 且 straddle 位于 train q1"),
            ("iv_skew_q5_put", "two_factor", [("q", "iv_skew", 5), ("eq", "direction", "put")], "put 且 iv_skew 位于 train q5"),
            ("iv_skew_q1_call", "two_factor", [("q", "iv_skew", 1), ("eq", "direction", "call")], "call 且 iv_skew 位于 train q1"),
        ]
    )
    specs.extend((f"rth_hour_{hour}", "hour", [("eq", "session_mode", "rth"), ("eq", "hour_et", hour)], f"RTH 且 ET 小时={hour}") for hour in range(10, 15))
    specs.extend((f"gth_hour_{hour}", "hour", [("eq", "session_mode", "gth"), ("eq", "hour_et", hour)], f"GTH 且 ET 小时={hour}") for hour in range(3, 9))
    return [{"name": name, "family": family, "criteria": criteria, "definition": definition} for name, family, criteria, definition in specs]


RULE_SPECS = _specs()


def fit_thresholds(rows: Sequence[Mapping[str, Any]]) -> dict[str, list[float]]:
    result = {}
    for name, (field, absolute) in Q_FACTORS.items():
        values = [_value(row.get(field), absolute=absolute) for row in rows]
        usable = [value for value in values if value is not None]
        result[name] = [float(value) for value in np.quantile(usable, (0.2, 0.4, 0.8))]
    return result


def _value(raw: object, *, absolute: bool = False) -> float | None:
    if isinstance(raw, bool) or not isinstance(raw, int | float) or not math.isfinite(raw):
        return None
    return abs(float(raw)) if absolute else float(raw)


def _side(rows: Sequence[Mapping[str, Any]], name: str) -> list[Mapping[str, Any]]:
    grouped: defaultdict[tuple[object, object], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(row.get("session_date"), row.get("decision_at"))].append(row)
    chosen = []
    for group in grouped.values():
        by_direction = {row.get("direction"): row for row in group}
        if name in {"cheaper", "expensive"}:
            if set(by_direction) != {"call", "put"}:
                continue
            call, put = by_direction["call"], by_direction["put"]
            call_debit, put_debit = _value(call.get("debit_fraction_of_width")), _value(put.get("debit_fraction_of_width"))
            if call_debit is None or put_debit is None or call_debit == put_debit:
                continue
            low = call if call_debit < put_debit else put
            chosen.append(low if name == "cheaper" else (put if low is call else call))
            continue
        horizon, relation = name.removeprefix("ret_").split("m_")
        returns = next((_value(row.get(f"spot_ret_{horizon}m")) for row in group if _value(row.get(f"spot_ret_{horizon}m")) is not None), None)
        if not returns:
            continue
        direction = "call" if returns > 0 else "put"
        if relation == "reverse":
            direction = "put" if direction == "call" else "call"
        if direction in by_direction:
            chosen.append(by_direction[direction])
    return chosen


def select_rows(rows: Sequence[Mapping[str, Any]], spec: Mapping[str, Any], thresholds: Mapping[str, Sequence[float]]) -> list[Mapping[str, Any]]:
    selected = list(rows)
    for criterion in spec["criteria"]:
        if criterion[0] == "side":
            selected = _side(selected, str(criterion[1]))
    for criterion in spec["criteria"]:
        if criterion[0] == "eq":
            selected = [row for row in selected if row.get(criterion[1]) == criterion[2]]
        elif criterion[0] == "q":
            factor, bucket = str(criterion[1]), int(criterion[2])
            field, absolute = Q_FACTORS[factor]
            q20, q40, q80 = thresholds[factor]
            def inside(row: Mapping[str, Any]) -> bool:
                value = _value(row.get(field), absolute=absolute)
                return value is not None and ((value <= q20 if bucket == 1 else q20 < value <= q40) if bucket < 5 else value >= q80)
            selected = [row for row in selected if inside(row)]
    return selected


def _aggregate(rows: Sequence[Mapping[str, Any]], mode: str) -> list[Mapping[str, Any]]:
    if mode == "bar_5m":
        return list(rows)
    grouped: defaultdict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["session_date"])].append(row)
    if mode == "session_first":
        return [min(group, key=lambda row: str(row["decision_at"])) for group in grouped.values()]
    return [{"session_date": day, "pnl_hold_to_1545": statistics.fmean(float(row["pnl_hold_to_1545"]) for row in group)} for day, group in grouped.items()]


def metrics(rows: Sequence[Mapping[str, Any]], mode: str) -> dict[str, Any]:
    observations = _aggregate(rows, mode)
    values = [float(row["pnl_hold_to_1545"]) for row in observations]
    days = {str(row["session_date"]) for row in observations}
    mean = statistics.fmean(values) if values else None
    se = statistics.stdev(values) / math.sqrt(len(values)) if len(values) > 1 else None
    day_sums = defaultdict(float)
    for row in observations:
        day_sums[str(row["session_date"])] += float(row["pnl_hold_to_1545"])
    total = sum(day_sums.values())
    return {
        "n": len(values), "sessions": len(days), "mean": mean,
        "median": statistics.median(values) if values else None,
        "hit_rate": sum(value > 0 for value in values) / len(values) if values else None,
        "se": se, "lcb_90": mean - 1.64 * se if mean is not None and se is not None else None,
        "max_single_day_contribution_fraction": max(day_sums.values()) / total if total > 0 else None,
    }


def _passes(values: Mapping[str, Any], mode: str, *, train: bool = False) -> bool:
    minimum_n, minimum_days = ((40, 6) if train else (20, 5)) if mode == "bar_5m" else (5, 5)
    return bool(values["mean"] is not None and values["mean"] > 0 and values["lcb_90"] is not None and values["lcb_90"] > 0 and values["n"] >= minimum_n and values["sessions"] >= minimum_days)


def _failures(values: Mapping[str, Any], mode: str, *, train: bool = False) -> list[str]:
    minimum_n, minimum_days = ((40, 6) if train else (20, 5)) if mode == "bar_5m" else (5, 5)
    checks = (("mean_nonpositive", values["mean"] is None or values["mean"] <= 0), ("lcb_nonpositive", values["lcb_90"] is None or values["lcb_90"] <= 0), ("n_below_minimum", values["n"] < minimum_n), ("trading_days_below_minimum", values["sessions"] < minimum_days))
    return [name for name, failed in checks if failed]


def explore(rows: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    train = [row for row in rows if TRAIN[0] <= str(row.get("session_date")) <= TRAIN[1]]
    holdout = [row for row in rows if HOLDOUT[0] <= str(row.get("session_date")) <= HOLDOUT[1]]
    thresholds = fit_thresholds(train)
    records, by_rule = [], {}
    for spec in RULE_SPECS:
        train_selected, holdout_selected = select_rows(train, spec, thresholds), select_rows(holdout, spec, thresholds)
        train_metrics = {mode: metrics(train_selected, mode) for mode in MODES}
        holdout_metrics = {mode: metrics(holdout_selected, mode) for mode in MODES}
        qualified = _passes(train_metrics["bar_5m"], "bar_5m", train=True)
        passes = {mode: qualified and _passes(holdout_metrics[mode], mode) for mode in MODES}
        robust = all(passes.values())
        by_rule[spec["name"]] = {"train": train_metrics, "holdout": holdout_metrics, "passes": passes, "robust": robust}
        for mode in MODES:
            records.append({"rule_name": spec["name"], "family": spec["family"], "definition": spec["definition"], "evaluation_mode": mode, "is_session_level": mode != "bar_5m", "train_thresholds": thresholds, "train": train_metrics[mode], "holdout": holdout_metrics[mode], "train_qualified": qualified, "holdout_pass": passes[mode], "robust_holdout_pass": robust})
    qualified_names = [name for name, value in by_rule.items() if _passes(value["train"]["bar_5m"], "bar_5m", train=True)]
    def rank(name: str) -> tuple[Any, ...]:
        value = by_rule[name]
        lcbs = [value["holdout"][mode]["lcb_90"] for mode in MODES]
        return (sum(value["passes"].values()), min(x if x is not None else -math.inf for x in lcbs), value["holdout"]["bar_5m"]["mean"] or -math.inf)
    closest = sorted(qualified_names, key=rank, reverse=True)
    train_rejected = [spec["name"] for spec in RULE_SPECS if spec["name"] not in qualified_names]
    train_rejected.sort(key=lambda name: (sum(_passes(by_rule[name]["holdout"][mode], mode) for mode in MODES), by_rule[name]["holdout"]["bar_5m"]["lcb_90"] or -math.inf, by_rule[name]["train"]["bar_5m"]["lcb_90"] or -math.inf), reverse=True)
    closest = (closest + train_rejected)[:5]
    closest_details = {}
    for name in closest:
        value = by_rule[name]
        failures = {"train_bar_5m": _failures(value["train"]["bar_5m"], "bar_5m", train=True)}
        failures.update({f"holdout_{mode}": _failures(value["holdout"][mode], mode) for mode in MODES})
        failures["selection"] = [] if name in qualified_names else ["train_not_qualified"]
        closest_details[name] = {**value, "failure_reasons": failures}
    summary = {
        "train_window": list(TRAIN), "holdout_window": list(HOLDOUT),
        "base_rules_tested": len(RULE_SPECS), "hypotheses_tested": len(records),
        "train_rows": len(train), "holdout_rows": len(holdout), "train_thresholds": thresholds,
        "train_qualified_base_rules": len(qualified_names), "train_qualified_rule_names": qualified_names,
        "holdout_evaluated_base_rules": len(qualified_names),
        "holdout_failed_base_rules": sum(not by_rule[name]["robust"] for name in qualified_names),
        "holdout_survivors": {mode: sum(value["passes"][mode] for value in by_rule.values()) for mode in MODES},
        "robust_holdout_survivors": [name for name, value in by_rule.items() if value["robust"]],
        "closest_rule_names": closest,
        "closest_rules": closest_details,
        "multiple_comparison": {"robust_survivors_over_base_rules": f"{sum(value['robust'] for value in by_rule.values())}/{len(RULE_SPECS)}", "bar_survivors_over_base_rules": f"{sum(value['passes']['bar_5m'] for value in by_rule.values())}/{len(RULE_SPECS)}"},
        "gate_contract": {"train_bar_5m": "mean>0, lcb_90>0, n>=40, sessions>=6", "holdout_bar_5m": "mean>0, lcb_90>0, n>=20, sessions>=5", "holdout_session_modes": "mean>0, lcb_90>0, n>=5 daily observations, sessions>=5", "robust": "all three holdout modes pass after train qualification"},
        "honesty": {"entry": "existing conservative combo ask labels", "exit": "last live combo bid by 15:45 ET; no stop or trail", "threshold_source": "train only", "holdout_tuning": False, "live_path_written": False, "production_candidate": False},
    }
    return summary, records


def run(rows_path: Path, output_dir: Path) -> dict[str, Any]:
    rows = [json.loads(line) for line in rows_path.read_text().splitlines() if line.strip()]
    report, records = explore(rows)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "odte_ev_explore.report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    (output_dir / "odte_ev_explore.rules.jsonl").write_text("".join(json.dumps(record, sort_keys=True) + "\n" for record in records))
    return report
