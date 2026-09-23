"""Recheck published condor cash results against raw broker Parquet; no card inputs."""
import importlib.util
import json
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path
from statistics import mean

import duckdb


def run(output: Path) -> None:
    root = Path("/srv/data/spx-spark/research/condor-delta-width-volatility-2026-09-05")
    spec = importlib.util.spec_from_file_location("frozen_cash_reader", root / "audit-research-source.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    setups = ["ic_broker_recorded_d20_w10", "ic_broker_recorded_d20_w20", "ic_bbo_implied_d20_w20"]
    rows = [json.loads(line) for line in (root / "rows.jsonl").read_text().splitlines()]
    rows = [row for row in rows if row["provider"] == "schwab" and row["setup"] in setups]
    summaries = []
    for setup in setups:
        cohort = [row for row in rows if row["setup"] == setup]
        resolved = [row for row in cohort if row["status"] == "COMPLETE_EXIT"]
        pnl = [row["pnl_usd"] for row in resolved]
        summaries.append({"setup": setup, "attempts": len(cohort),
            "statuses": dict(Counter(row["status"] for row in cohort)),
            "wins": sum(value > 0 for value in pnl), "complete": len(pnl),
            "win_rate": sum(value > 0 for value in pnl) / len(pnl),
            "mean_usd": mean(pnl), "total_usd": sum(pnl), "worst_usd": min(pnl),
            "mean_win_usd": mean(value for value in pnl if value > 0),
            "mean_loss_usd": mean(value for value in pnl if value <= 0),
            "exits": dict(Counter(row["exit_reason"] for row in resolved))})
    groups = defaultdict(list)
    for row in rows:
        if row["status"] == "COMPLETE_EXIT":
            groups[row["session_date"]].append(row)
    con = duckdb.connect(config={"threads": 2, "memory_limit": "768MB"})
    con.execute("SET TimeZone='UTC'")
    checked = 0
    for day, group in sorted(groups.items()):
        times = sorted({datetime.fromisoformat(row[k]) for row in group for k in ("entry_at", "exit_at")})
        files = module._files(Path("/srv/data/spx-spark/data/lake/quotes/schema=v1"), "schwab", min(times), max(times))
        books = module._snapshots(con, files, date.fromisoformat(day), times, "schwab", 15)
        for row in group:
            cash = []
            for kind in ("entry_at", "exit_at"):
                at = datetime.fromisoformat(row[kind])
                legs = [books[at].get((leg["strike"], leg["right"])) for leg in row["legs"]]
                quote = module._cash_quote(legs, row["quantities"], at, 15, 2)
                assert quote and module._depth(legs, row["quantities"], liquidate=kind == "exit_at")
                assert all(leg["quote_time"] <= leg["received_at"] <= at for leg in legs)
                cash.append(-quote[0] if kind == "entry_at" else quote[1])
            net = (sum(cash) - sum(abs(q) for q in row["quantities"]) * 2 * 1.32 / 100) * 100
            assert abs(net - row["pnl_usd"]) < 0.000001, (day, row["setup"], net, row["pnl_usd"])
            checked += 1
        print(day, "verified trades", checked, flush=True)
    con.close()
    report = {"prior_replay_period": ["2026-07-06", "2026-09-04"],
        "rechecked_complete_trade_rows": checked, "raw_cash_agrees": True,
        "scope": "cash recheck of existing fixed-10:00 replay; not a fresh production-policy backtest",
        "summaries": summaries}
    output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2), flush=True)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    run(args.output)
