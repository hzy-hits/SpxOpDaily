#!/usr/bin/env python3
"""Print the phase-1 opportunity capture summary for one session."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from spx_spark.app_settings import get_settings
from spx_spark.config import StorageSettings
from spx_spark.infrastructure.opportunity_ledger import (
    capture_summary,
    capture_week_summary,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-root", default=None)
    parser.add_argument("--database", default=None)
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--week-ending", default=None)
    args = parser.parse_args(argv)
    data_root = Path(args.data_root or StorageSettings.from_env().data_root)
    database = Path(args.database) if args.database else get_settings().data_root / "spx.sqlite"
    if args.week_ending:
        summary = capture_week_summary(data_root, args.week_ending, database_path=database)
    else:
        summary = capture_summary(data_root, args.date, database_path=database)
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
