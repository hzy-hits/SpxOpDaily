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

    usable = [row for row in rows if row.get("policy_pnl_points") is not None
              and row.get("exit_reason") in {"profit_take", "stop_loss", "premium_stop", "trail", "time_stop", "hard_close"}
              and row.get("policy_version")]
    sessions = sorted({str(row.get("session_date") or "") for row in usable if row.get("session_date")})
    by_bucket: dict[str, list[float]] = defaultdict(list)
    for row in usable:
        bucket = "|".join(
            [
                str(row.get("regime_terminal_state") or "unknown"),
                str(row.get("setup_kind") or "unknown"),
                str(row.get("strategy_type") or "unknown"),
                str(row["policy_version"]),
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

    gates = {
        "sessions_covered": len(sessions),
        "sessions_required": 25,
        "sessions_gate": len(sessions) >= 25,
        "labeled_rows": len(usable),
        "bucket_min_n": min((item["n"] for item in bucket_stats), default=0),
        "bucket_fallback_needed": any(item["n"] < 8 for item in bucket_stats),
        "excluded_rows": len(rows) - len(usable),
        "promotion_ready": False,
    }
    # Entry premium is not a model score; these descriptive buckets cannot prove promotion.
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
