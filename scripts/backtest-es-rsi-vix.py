#!/usr/bin/env python3
"""Run the ES RSI/VIX pilot backtest from durable alert snapshots."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from spx_spark.data_platform.research.es_rsi_vix_backtest import run


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--outbox",
        default="/srv/data/spx-spark/data/ledger/domain_event_outbox.sqlite",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
    )
    args = parser.parse_args()
    output_dir = args.output_dir or (
        f"/srv/data/spx-spark/data/reports/es_rsi_vix_backtest/as_of={date.today().isoformat()}"
    )
    target = run(Path(args.outbox), Path(output_dir))
    print(target)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
