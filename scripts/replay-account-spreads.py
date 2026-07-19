#!/usr/bin/env python3
"""Replay the account's real SPXW spread rounds against exit-rule counterfactuals."""

from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta
from pathlib import Path

from spx_spark.data_platform.research.odte_level_replay import (
    MAX_ENTRY_QUOTE_AGE,
    MAX_LEG_SKEW,
    MAX_MARK_QUOTE_AGE,
    run,
)


def _nonnegative_seconds(raw: str) -> float:
    value = float(raw)
    if value < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return value


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--statement",
        required=True,
        help="path to a local IBKR statement CSV (the statement is never committed)",
    )
    parser.add_argument(
        "--data-root",
        default="/srv/data/spx-spark/data",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
    )
    parser.add_argument(
        "--max-entry-quote-age-seconds",
        type=_nonnegative_seconds,
        default=MAX_ENTRY_QUOTE_AGE.total_seconds(),
        help="maximum age of either leg at actual entry (default: 30)",
    )
    parser.add_argument(
        "--max-mark-quote-age-seconds",
        type=_nonnegative_seconds,
        default=MAX_MARK_QUOTE_AGE.total_seconds(),
        help="maximum age of either leg at a path mark (default: 30)",
    )
    parser.add_argument(
        "--max-leg-skew-seconds",
        type=_nonnegative_seconds,
        default=MAX_LEG_SKEW.total_seconds(),
        help="maximum timestamp skew between spread legs (default: 5)",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    output_dir = args.output_dir or (
        f"/srv/data/spx-spark/data/reports/odte_level_backtest/as_of={date.today().isoformat()}"
    )
    target = run(
        Path(args.statement),
        Path(args.data_root),
        Path(output_dir),
        max_entry_quote_age=timedelta(seconds=args.max_entry_quote_age_seconds),
        max_mark_quote_age=timedelta(seconds=args.max_mark_quote_age_seconds),
        max_leg_skew=timedelta(seconds=args.max_leg_skew_seconds),
    )
    print(target)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
