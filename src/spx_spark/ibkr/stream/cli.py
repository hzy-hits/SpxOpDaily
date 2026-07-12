"""IBKR stream CLI composition root."""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict

from spx_spark.config import (
    IbkrBrokerSettings,
    IbkrSettings,
    IbkrStreamSettings,
    RuntimePolicySettings,
    SamplingSettings,
    StorageSettings,
)
from spx_spark.ibkr.stream.collector import StreamCollector
from spx_spark.ibkr.stream.supervisor import StreamRuntime
from spx_spark.ibkr.verifier import prepare_ib_client


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the persistent streaming IBKR market-data collector."
    )
    parser.add_argument("--print-config", action="store_true", help="Print settings and exit.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore the runtime mode gate and stream anyway.",
    )
    parser.add_argument(
        "--skip-options",
        action="store_true",
        help="Stream indexes/stocks/futures/CFDs only.",
    )
    parser.add_argument(
        "--duration-seconds",
        type=float,
        default=None,
        help="Exit after this many seconds (smoke tests). Default: run forever.",
    )
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    ibkr_settings = IbkrSettings.from_env()
    stream_settings = IbkrStreamSettings.from_env()
    broker_settings = IbkrBrokerSettings.from_env()
    sampling_settings = SamplingSettings.from_env()
    storage_settings = StorageSettings.from_env()
    runtime_policy = RuntimePolicySettings.from_env()

    if args.print_config:
        print(
            json.dumps(
                {
                    "ibkr": asdict(ibkr_settings),
                    "stream": asdict(stream_settings),
                    "broker": asdict(broker_settings),
                    "sampling": asdict(sampling_settings),
                    "storage": asdict(storage_settings),
                },
                indent=2,
                sort_keys=True,
                default=str,
            )
        )
        return 0

    try:
        from ib_async import IB
    except ImportError as exc:
        raise SystemExit("Missing dependency: ib_async. Run `uv sync` first.") from exc

    collector = StreamCollector(
        IB(),
        ibkr_settings=ibkr_settings,
        stream_settings=stream_settings,
        sampling_settings=sampling_settings,
        storage_settings=storage_settings,
        runtime_policy=runtime_policy,
        broker_settings=broker_settings,
        force=args.force,
        skip_options=args.skip_options,
    )
    prepare_ib_client(collector.ib, request_timeout_seconds=ibkr_settings.request_timeout_seconds)
    runtime = StreamRuntime(
        collector=collector,
        stream_settings=stream_settings,
        storage_settings=storage_settings,
        runtime_policy=runtime_policy,
    )
    if args.duration_seconds is not None:
        runtime.deadline = time.monotonic() + args.duration_seconds
    try:
        return runtime.run()
    finally:
        collector.teardown()


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()

