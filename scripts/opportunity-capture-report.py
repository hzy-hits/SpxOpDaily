#!/usr/bin/env python3
"""Print the phase-1 opportunity capture summary for one session."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from spx_spark.infrastructure.opportunity_ledger import (
    capture_summary,
    capture_week_summary,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", default="/srv/data/spx-spark")
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--week-ending", default=None)
    args = parser.parse_args(argv)
    if args.week_ending:
        summary = capture_week_summary(Path(args.data_root), args.week_ending)
    else:
        summary = capture_summary(Path(args.data_root), args.date)
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
