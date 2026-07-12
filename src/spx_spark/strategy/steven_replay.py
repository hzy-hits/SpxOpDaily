"""Deterministic offline replay for Steven forward metrics and baselines.

Usage:
  uv run python -m spx_spark.strategy.steven_replay --date YYYY-MM-DD
  uv run spx-spark-steven-replay --date YYYY-MM-DD --data-root /path/to/data
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import date
from pathlib import Path

from spx_spark.settings import settings_value
from spx_spark.steven_validation import build_replay_payload


def _default_data_root() -> str:
    return (
        os.getenv("MARKET_DATA_DATA_ROOT")
        or os.getenv("MAINTENANCE_DATA_ROOT")
        or str(settings_value("maintenance.data_root"))
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Recompute Steven forward metrics and baselines from lake data."
    )
    parser.add_argument("--date", required=True, help="Trading date YYYY-MM-DD (ET).")
    parser.add_argument(
        "--data-root",
        default=None,
        help="Data root containing lake/steven/{episodes,bars}.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional output path; defaults to stdout.",
    )
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    trading_date = date.fromisoformat(args.date).isoformat()
    data_root = Path(args.data_root or _default_data_root())
    payload = build_replay_payload(trading_date=trading_date, data_root=data_root)
    text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text, encoding="utf-8")
    else:
        print(text, end="")
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
