#!/usr/bin/env python3
"""Run registered shadow strategies against a saved facts JSON file."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from spx_spark.application.order_map.strategy_plugins import run_shadow_strategies


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--facts", required=True)
    parser.add_argument("--data-root", default=".")
    args = parser.parse_args(argv)
    facts = json.loads(Path(args.facts).read_text(encoding="utf-8"))
    candidates = run_shadow_strategies(
        Path(args.data_root),
        facts,
        now=datetime.now(tz=timezone.utc),
        persist=False,
    )
    print(json.dumps({"authority": "none", "candidates": candidates}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
