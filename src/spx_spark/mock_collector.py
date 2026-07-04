from __future__ import annotations

import argparse
import json
import math
import time
from dataclasses import asdict
from datetime import datetime, timedelta, timezone

from spx_spark.config import SamplingSettings, StorageSettings, default_spxw_expiry
from spx_spark.marketdata import (
    InstrumentId,
    MarketDataQuality,
    OptionGreeks,
    Provider,
    Quote,
)
from spx_spark.sampling import (
    VALID_MODES,
    OptionContractSpec,
    build_sampling_plan,
    plan_summary,
)
from spx_spark.storage import JsonlQuoteWriter, LatestStateStore


def make_two_sided_quote(
    instrument: InstrumentId,
    *,
    provider_symbol: str,
    mark: float,
    spread: float,
    received_at: datetime,
    greeks: OptionGreeks | None = None,
    sampling_mode: str | None = None,
    sampling_group: int | None = None,
) -> Quote:
    half_spread = max(spread / 2.0, 0.005)
    bid = max(mark - half_spread, 0.01)
    ask = max(mark + half_spread, bid)
    return Quote(
        instrument=instrument,
        provider=Provider.MOCK,
        provider_symbol=provider_symbol,
        received_at=received_at,
        quality=MarketDataQuality.LIVE,
        bid=bid,
        ask=ask,
        last=mark,
        mark=mark,
        bid_size=10,
        ask_size=12,
        volume=100,
        quote_time=received_at,
        trade_time=received_at,
        source_latency_ms=0.0,
        greeks=greeks,
        sampling_mode=sampling_mode,
        sampling_group=sampling_group,
    )


def make_context_quotes(underlier: float, *, received_at: datetime, cycle_index: int) -> list[Quote]:
    drift = cycle_index * 0.75
    spx = underlier + drift
    return [
        make_two_sided_quote(
            InstrumentId.index("SPX", provider_symbol="mock:SPX"),
            provider_symbol="mock:SPX",
            mark=spx,
            spread=0.8,
            received_at=received_at,
        ),
        make_two_sided_quote(
            InstrumentId.future("ES", provider_symbol="mock:ES", exchange="CME"),
            provider_symbol="mock:ES",
            mark=spx + 4.25,
            spread=0.5,
            received_at=received_at,
        ),
        make_two_sided_quote(
            InstrumentId.equity("SPY", provider_symbol="mock:SPY"),
            provider_symbol="mock:SPY",
            mark=spx / 10.0,
            spread=0.02,
            received_at=received_at,
        ),
        make_two_sided_quote(
            InstrumentId.index("VIX", provider_symbol="mock:VIX"),
            provider_symbol="mock:VIX",
            mark=16.5 + math.sin(cycle_index / 3.0) * 0.4,
            spread=0.03,
            received_at=received_at,
        ),
        make_two_sided_quote(
            InstrumentId.index("VIX1D", provider_symbol="mock:VIX1D"),
            provider_symbol="mock:VIX1D",
            mark=14.8 + math.cos(cycle_index / 3.0) * 0.6,
            spread=0.05,
            received_at=received_at,
        ),
        make_two_sided_quote(
            InstrumentId.index("VIX9D", provider_symbol="mock:VIX9D"),
            provider_symbol="mock:VIX9D",
            mark=15.6,
            spread=0.04,
            received_at=received_at,
        ),
        make_two_sided_quote(
            InstrumentId.index("VVIX", provider_symbol="mock:VVIX"),
            provider_symbol="mock:VVIX",
            mark=88.0,
            spread=0.2,
            received_at=received_at,
        ),
        make_two_sided_quote(
            InstrumentId.index("SKEW", provider_symbol="mock:SKEW"),
            provider_symbol="mock:SKEW",
            mark=142.0,
            spread=0.2,
            received_at=received_at,
        ),
    ]


def make_option_quote(
    spec: OptionContractSpec,
    *,
    underlier: float,
    received_at: datetime,
    mode: str,
) -> Quote:
    is_call = spec.right == "C"
    intrinsic = max(underlier - spec.strike, 0.0) if is_call else max(spec.strike - underlier, 0.0)
    distance = abs(underlier - spec.strike)
    time_value = max(0.35, 19.0 * math.exp(-distance / 115.0))
    mark = intrinsic + time_value
    spread = max(0.05, min(1.5, mark * 0.035 + distance * 0.0008))
    moneyness = max(min((underlier - spec.strike) / 120.0, 1.0), -1.0)
    call_delta = 0.5 + 0.45 * moneyness
    delta = call_delta if is_call else call_delta - 1.0
    greeks = OptionGreeks(
        implied_vol=max(0.10, 0.19 + distance / 7500.0),
        delta=delta,
        gamma=max(0.0004, 0.0045 * math.exp(-distance / 90.0)),
        theta=-(0.45 + time_value / 12.0),
        vega=max(0.02, 0.45 * math.exp(-distance / 150.0)),
        underlier_price=underlier,
        model="mock",
    )
    instrument = InstrumentId.option(
        "SPX",
        expiry=spec.expiry,
        strike=spec.strike,
        right=spec.right,
        trading_class="SPXW",
        provider_symbol=f"mock:SPXW:{spec.expiry}:{spec.strike}:{spec.right}",
    )
    return make_two_sided_quote(
        instrument,
        provider_symbol=instrument.provider_symbol or instrument.canonical_id,
        mark=mark,
        spread=spread,
        received_at=received_at,
        greeks=greeks,
        sampling_mode=mode,
        sampling_group=spec.group_index,
    )


def build_mock_quotes(
    *,
    underlier: float,
    expiry: str,
    next_expiry: str | None,
    mode: str,
    sampling_settings: SamplingSettings,
    rolling_group_index: int,
    received_at: datetime,
    cycle_index: int = 0,
) -> tuple[list[Quote], dict[str, object]]:
    plan = build_sampling_plan(
        underlier_price=underlier + cycle_index * 0.75,
        expiry=expiry,
        next_expiry=next_expiry,
        mode=mode,
        settings=sampling_settings,
    )
    quotes = make_context_quotes(underlier, received_at=received_at, cycle_index=cycle_index)

    selected_specs: list[OptionContractSpec] = list(plan.hot_lane)
    if plan.rolling_groups:
        group = plan.rolling_groups[rolling_group_index % len(plan.rolling_groups)]
        selected_specs.extend(group.contracts)

    seen: set[str] = set()
    for spec in selected_specs:
        instrument_id = f"option:SPX:SPXW:{spec.expiry}:{spec.strike}:{spec.right}"
        if instrument_id in seen:
            continue
        seen.add(instrument_id)
        quotes.append(
            make_option_quote(
                spec,
                underlier=underlier + cycle_index * 0.75,
                received_at=received_at,
                mode=mode,
            )
        )

    return quotes, plan_summary(plan)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one or more mock normalized quote cycles.")
    parser.add_argument("--underlier", type=float, default=7500.0)
    parser.add_argument("--expiry", default=default_spxw_expiry())
    parser.add_argument("--next-expiry")
    parser.add_argument("--mode", choices=sorted(VALID_MODES))
    parser.add_argument("--cycles", type=int, default=1)
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    parser.add_argument("--rolling-group-index", type=int, default=0)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.cycles <= 0:
        raise ValueError("--cycles must be positive")

    sampling_settings = SamplingSettings.from_env()
    storage_settings = StorageSettings.from_env()
    mode = args.mode or sampling_settings.default_mode
    writer = JsonlQuoteWriter(storage_settings)
    latest_store = LatestStateStore(storage_settings)

    raw_path_counts: dict[str, int] = {}
    latest_result = None
    total_quotes = 0
    plan = {}

    for cycle_index in range(args.cycles):
        received_at = datetime.now(tz=timezone.utc) + timedelta(milliseconds=cycle_index)
        quotes, plan = build_mock_quotes(
            underlier=args.underlier,
            expiry=args.expiry,
            next_expiry=args.next_expiry,
            mode=mode,
            sampling_settings=sampling_settings,
            rolling_group_index=args.rolling_group_index + cycle_index,
            received_at=received_at,
            cycle_index=cycle_index,
        )
        raw_result = writer.write_quotes(quotes)
        latest_result = latest_store.update(quotes, now=received_at)
        total_quotes += len(quotes)
        for path, count in raw_result.path_counts.items():
            raw_path_counts[path] = raw_path_counts.get(path, 0) + count
        if cycle_index < args.cycles - 1 and args.sleep_seconds > 0:
            time.sleep(args.sleep_seconds)

    summary = {
        "mode": mode,
        "cycles": args.cycles,
        "quotes_written": total_quotes,
        "raw_paths": raw_path_counts,
        "latest_state": asdict(latest_result) if latest_result else None,
        "plan": plan,
    }
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        print(f"Mode: {mode}")
        print(f"Cycles: {args.cycles}")
        print(f"Quotes written: {total_quotes}")
        print("Raw files:")
        for path, count in sorted(raw_path_counts.items()):
            print(f"- {path}: {count}")
        if latest_result is not None:
            print(f"Latest state: {latest_result.path}")
            print(f"Best quotes: {latest_result.best_quote_count}")
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
