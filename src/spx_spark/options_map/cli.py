"""CLI entrypoint for the SPXW options map."""

from __future__ import annotations

import argparse
import json

from spx_spark.config import StorageSettings
from spx_spark.options_map.orchestration import build_options_map
from spx_spark.options_map.render import print_options_map
from spx_spark.storage import LatestMarketProjectionStore


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the current SPXW options map.")
    parser.add_argument("--json", action="store_true", help="Print JSON.")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = StorageSettings.from_env()
    state = LatestMarketProjectionStore(settings).load()
    options_map = build_options_map(state, storage_settings=settings)
    if args.json:
        print(json.dumps(options_map.to_dict(), indent=2, sort_keys=True))
    else:
        print_options_map(options_map)
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
