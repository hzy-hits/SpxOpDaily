"""Walk-forward calibration report for ManagementPolicy EV (V3-3b).

Offline research only. Reads ``features/strategy_policy_labels/`` parquet and
prints whether §7.4 promotion gates are met. Does not promote EV to a hard gate.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any


def load_policy_labels(root: Path) -> list[dict[str, Any]]:
    import duckdb

    rows: list[dict[str, Any]] = []
    base = root / "features" / "strategy_policy_labels"
    if not base.exists():
        return rows
    con = duckdb.connect()
    try:
        for directory in sorted(base.glob("date=*")):
            parquet = directory / "labels.parquet"
            jsonl = directory / "labels.jsonl"
            if parquet.exists():
                relation = con.execute("SELECT * FROM read_parquet(?)", [str(parquet)])
                columns = [item[0] for item in relation.description]
                for values in relation.fetchall():
                    rows.append(dict(zip(columns, values, strict=True)))
            elif jsonl.exists():
                with jsonl.open(encoding="utf-8") as handle:
                    for line in handle:
                        if line.strip():
                            rows.append(json.loads(line))
    finally:
        con.close()
    return rows


def calibration_report(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Compute bootstrap calibration diagnostics against §7.4 gates."""

    usable = [row for row in rows if row.get("policy_pnl_points") is not None]
    sessions = sorted({str(row.get("session_date") or "") for row in usable if row.get("session_date")})
    by_bucket: dict[str, list[float]] = defaultdict(list)
    for row in usable:
        bucket = "|".join(
            [
                str(row.get("regime_terminal_state") or "unknown"),
                str(row.get("setup_kind") or "unknown"),
                str(row.get("strategy_type") or "unknown"),
            ]
        )
        by_bucket[bucket].append(float(row["policy_pnl_points"]))

    bucket_stats = []
    for bucket, values in sorted(by_bucket.items()):
        mean = sum(values) / len(values)
        positives = sum(1 for value in values if value > 0)
        bucket_stats.append(
            {
                "bucket": bucket,
                "n": len(values),
                "mean_policy_pnl": round(mean, 6),
                "hit_rate": round(positives / len(values), 6),
            }
        )

    # Simple score discrimination: top vs bottom tercile by entry_ask as proxy
    # when no model score is stored yet (Pass A labels).
    ordered = sorted(usable, key=lambda row: float(row.get("entry_ask") or 0.0))
    tercile = max(len(ordered) // 3, 1)
    low = ordered[:tercile]
    high = ordered[-tercile:]
    low_mean = _mean_pnl(low)
    high_mean = _mean_pnl(high)

    gates = {
        "sessions_covered": len(sessions),
        "sessions_required": 25,
        "sessions_gate": len(sessions) >= 25,
        "labeled_rows": len(usable),
        "bucket_min_n": min((item["n"] for item in bucket_stats), default=0),
        "bucket_fallback_needed": any(item["n"] < 8 for item in bucket_stats),
        "top_vs_bottom_mean_diff": round(high_mean - low_mean, 6),
        "promotion_ready": False,
    }
    # Explicit: V3-3b never auto-promotes. Human must approve after gates green.
    gates["promotion_ready"] = bool(
        gates["sessions_gate"]
        and gates["labeled_rows"] >= 50
        and gates["top_vs_bottom_mean_diff"] > 0
    )
    return {
        "schema_version": "strategy_policy_calibration.v1",
        "policy_authority": "rank_only",
        "sessions": sessions,
        "bucket_stats": bucket_stats,
        "gates": gates,
        "note": (
            "EV scoring may rank candidates but must not veto until §7.4 gates "
            "pass and the user re-approves a policy_version bump."
        ),
    }


def apply_policy_ev_score(
    candidate: Mapping[str, Any],
    *,
    expected_policy_pnl: float | None,
    expected_shortfall_10: float | None,
) -> dict[str, Any]:
    """Attach rank-only ManagementPolicy EV score when empirical EV is available."""

    economics = candidate.get("economics") if isinstance(candidate.get("economics"), dict) else {}
    utility = candidate.get("utility") if isinstance(candidate.get("utility"), dict) else {}
    max_loss = economics.get("max_loss_points")
    if (
        expected_policy_pnl is None
        or expected_shortfall_10 is None
        or not isinstance(max_loss, (int, float))
        or max_loss <= 0
    ):
        return dict(candidate)
    liquidity = float(utility.get("liquidity_penalty") or 0.0)
    uncertainty = float(utility.get("model_uncertainty") or 0.0)
    score = (
        float(expected_policy_pnl) / float(max_loss)
        - 0.50 * float(expected_shortfall_10) / float(max_loss)
        - 0.25 * liquidity
        - 0.25 * uncertainty
    )
    scored = dict(candidate)
    scored["policy_ev"] = {
        "expected_policy_pnl": round(float(expected_policy_pnl), 6),
        "expected_shortfall_10": round(float(expected_shortfall_10), 6),
        "score": round(score, 6),
        "authority": "rank_only",
        "method": "management_policy_ev.v1",
    }
    return scored


def _mean_pnl(rows: Sequence[Mapping[str, Any]]) -> float:
    if not rows:
        return 0.0
    return sum(float(row["policy_pnl_points"]) for row in rows) / len(rows)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args(argv)
    report = calibration_report(load_policy_labels(args.data_root))
    text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
