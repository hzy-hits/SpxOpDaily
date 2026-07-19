#!/usr/bin/env python3
"""Run the 0DTE level-alert backtest over the JSONL feature stores and quote lake."""

from __future__ import annotations

import argparse
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Sequence

from spx_spark.data_platform.research.odte_level_backtest import run


def _parse_as_of(value: str) -> date | datetime:
    """Parse an ISO date or datetime for the backtest's complete-session cutoff."""
    try:
        if "T" in value or " " in value:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "--as-of must be an ISO date or datetime (for example 2026-07-17)"
        ) from exc


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--features-root",
        default="/srv/data/spx-spark/data/features",
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
        "--as-of",
        type=_parse_as_of,
        default=None,
        help=(
            "reproducible UTC data cutoff; a date includes that complete date, "
            "while a datetime admits only sessions completed before it "
            "(default: latest completed UTC date)"
        ),
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    output_dir = args.output_dir or (
        f"/srv/data/spx-spark/data/reports/odte_level_backtest/as_of={date.today().isoformat()}"
    )
    target = run(
        Path(args.features_root),
        Path(args.data_root),
        Path(output_dir),
        as_of=args.as_of,
    )
    print(target)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
