"""CLI entrypoint for the SPXW options map."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from spx_spark.config import StorageSettings
from spx_spark.options_map.orchestration import build_options_map
from spx_spark.options_map.render import (
    print_options_map,
    render_open_interest_mirror_svg,
    render_strategy_risk_svg,
)
from spx_spark.storage import LatestMarketProjectionStore


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the current SPXW options map.")
    parser.add_argument("--json", action="store_true", help="Print JSON.")
    output = parser.add_mutually_exclusive_group()
    output.add_argument(
        "--oi-mirror-svg",
        metavar="PATH",
        help="Write a mobile put/call open-interest mirror chart as SVG.",
    )
    output.add_argument(
        "--oi-mirror-png",
        metavar="PATH",
        help="Write a mobile put/call open-interest mirror chart as PNG.",
    )
    output.add_argument(
        "--strategy-risk-svg",
        metavar="PATH",
        help="Write the latest decision-owned strategy risk sheet as SVG.",
    )
    output.add_argument(
        "--strategy-risk-png",
        metavar="PATH",
        help="Write the latest decision-owned strategy risk sheet as PNG.",
    )
    parser.add_argument(
        "--oi-window-points",
        type=float,
        default=100.0,
        help="Points on either side of ATM included in the OI mirror (default: 100).",
    )
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = StorageSettings.from_env()
    if args.strategy_risk_svg or args.strategy_risk_png:
        decision_path = Path(settings.data_root).expanduser() / "latest" / "strategy_decision.json"
        decision = json.loads(decision_path.read_text(encoding="utf-8"))
        if args.strategy_risk_png:
            from spx_spark.options_map.render import write_strategy_risk_png

            output = write_strategy_risk_png(decision, args.strategy_risk_png)
            print(output)
            return 0
        svg = render_strategy_risk_svg(decision)
        output = Path(args.strategy_risk_svg).expanduser()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(svg, encoding="utf-8")
        print(output)
        return 0
    if args.oi_mirror_svg or args.oi_mirror_png:
        exposure_path = Path(settings.data_root).expanduser() / "latest" / "exposure_map.json"
        exposure = json.loads(exposure_path.read_text(encoding="utf-8"))
        if args.oi_mirror_png:
            from spx_spark.options_map.render import write_open_interest_mirror_png

            output = write_open_interest_mirror_png(
                exposure,
                args.oi_mirror_png,
                window_points=args.oi_window_points,
            )
            print(output)
            return 0
        svg = render_open_interest_mirror_svg(
            exposure,
            window_points=args.oi_window_points,
        )
        output = Path(args.oi_mirror_svg).expanduser()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(svg, encoding="utf-8")
        print(output)
        return 0
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
