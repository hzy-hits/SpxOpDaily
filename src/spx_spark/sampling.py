from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

from spx_spark.config import SamplingSettings, default_spxw_expiry


VALID_GROUP_STRATEGIES = {"contiguous", "interleaved"}
VALID_MODES = {"human_alert", "execution_monitor", "degraded"}


@dataclass(frozen=True)
class OptionContractSpec:
    expiry: str
    strike: int
    right: str
    lane: str
    group_index: int | None = None


@dataclass(frozen=True)
class SamplingGroup:
    index: int
    cadence_seconds: int
    strikes: list[int]
    contracts: list[OptionContractSpec]


@dataclass(frozen=True)
class SamplingPlan:
    created_at: str
    mode: str
    underlier_price: float
    atm_strike: int
    expiries: list[str]
    strike_step: int
    window_points: int
    hot_window_points: int
    hot_cadence_seconds: int
    group_strategy: str
    hot_lane: list[OptionContractSpec]
    rolling_groups: list[SamplingGroup]

    @property
    def hot_contract_count(self) -> int:
        return len(self.hot_lane)

    @property
    def rolling_contract_count(self) -> int:
        return sum(len(group.contracts) for group in self.rolling_groups)

    @property
    def full_scan_seconds(self) -> int:
        return sum(group.cadence_seconds for group in self.rolling_groups)


def round_to_step(value: float, step: int) -> int:
    return int(round(value / step) * step)


def build_strikes(center: int, window_points: int, step: int) -> list[int]:
    start = center - window_points
    stop = center + window_points
    return [strike for strike in range(start, stop + step, step) if strike > 0]


def split_groups(strikes: list[int], group_count: int, strategy: str = "interleaved") -> list[list[int]]:
    if group_count <= 0:
        raise ValueError("group_count must be positive")
    if strategy not in VALID_GROUP_STRATEGIES:
        raise ValueError(f"Unsupported group strategy: {strategy!r}")
    groups: list[list[int]] = [[] for _ in range(group_count)]
    for index, strike in enumerate(strikes):
        if strategy == "interleaved":
            group_index = index % group_count
        else:
            group_index = min((index * group_count) // len(strikes), group_count - 1)
        groups[group_index].append(strike)
    return [group for group in groups if group]


def build_contracts(
    expiries: list[str],
    strikes: list[int],
    *,
    lane: str,
    group_index: int | None = None,
) -> list[OptionContractSpec]:
    contracts: list[OptionContractSpec] = []
    for expiry in expiries:
        for strike in strikes:
            for right in ("C", "P"):
                contracts.append(
                    OptionContractSpec(
                        expiry=expiry,
                        strike=strike,
                        right=right,
                        lane=lane,
                        group_index=group_index,
                    )
                )
    return contracts


def next_weekday_expiry(expiry: str) -> str:
    parsed = datetime.strptime(expiry, "%Y%m%d").date()
    parsed = parsed.fromordinal(parsed.toordinal() + 1)
    while parsed.weekday() >= 5:
        parsed = parsed.fromordinal(parsed.toordinal() + 1)
    return parsed.strftime("%Y%m%d")


def resolve_expiries(
    expiry: str,
    next_expiry: str | None,
    include_next_expiry: bool,
) -> list[str]:
    expiries = [expiry]
    if include_next_expiry:
        expiries.append(next_expiry or next_weekday_expiry(expiry))
    return expiries


def build_sampling_plan(
    *,
    underlier_price: float,
    expiry: str,
    next_expiry: str | None,
    mode: str,
    settings: SamplingSettings,
) -> SamplingPlan:
    if mode not in VALID_MODES:
        raise ValueError(f"Unsupported sampling mode: {mode!r}")

    atm = round_to_step(underlier_price, settings.strike_step)
    expiries = resolve_expiries(expiry, next_expiry, settings.include_next_expiry)
    hot_strikes = build_strikes(atm, settings.hot_window_points, settings.strike_step)
    rolling_strikes = build_strikes(atm, settings.window_points, settings.strike_step)

    if mode == "degraded":
        group_count = settings.degraded_group_count
        group_interval = settings.degraded_group_interval_seconds
    else:
        group_count = settings.group_count
        group_interval = settings.group_interval_seconds

    if mode == "execution_monitor":
        hot_cadence = settings.hot_execution_cadence_seconds
    else:
        hot_cadence = settings.hot_human_cadence_seconds

    hot_lane = build_contracts(expiries, hot_strikes, lane="hot")
    groups: list[SamplingGroup] = []
    for index, group_strikes in enumerate(
        split_groups(rolling_strikes, group_count, settings.group_strategy)
    ):
        groups.append(
            SamplingGroup(
                index=index,
                cadence_seconds=group_interval,
                strikes=group_strikes,
                contracts=build_contracts(
                    expiries,
                    group_strikes,
                    lane="rolling",
                    group_index=index,
                ),
            )
        )

    return SamplingPlan(
        created_at=datetime.now(tz=timezone.utc).isoformat(),
        mode=mode,
        underlier_price=underlier_price,
        atm_strike=atm,
        expiries=expiries,
        strike_step=settings.strike_step,
        window_points=settings.window_points,
        hot_window_points=settings.hot_window_points,
        hot_cadence_seconds=hot_cadence,
        group_strategy=settings.group_strategy,
        hot_lane=hot_lane,
        rolling_groups=groups,
    )


def plan_summary(plan: SamplingPlan) -> dict[str, object]:
    return {
        "mode": plan.mode,
        "underlier_price": plan.underlier_price,
        "atm_strike": plan.atm_strike,
        "expiries": plan.expiries,
        "hot_contract_count": plan.hot_contract_count,
        "rolling_contract_count": plan.rolling_contract_count,
        "group_strategy": plan.group_strategy,
        "group_count": len(plan.rolling_groups),
        "full_scan_seconds": plan.full_scan_seconds,
        "groups": [
            {
                "index": group.index,
                "cadence_seconds": group.cadence_seconds,
                "min_strike": min(group.strikes),
                "max_strike": max(group.strikes),
                "strike_count": len(group.strikes),
                "sample_strikes": group.strikes[:8],
                "contract_count": len(group.contracts),
            }
            for group in plan.rolling_groups
        ],
    }


def print_summary(plan: SamplingPlan) -> None:
    summary = plan_summary(plan)
    print(f"Mode: {summary['mode']}")
    print(f"Underlier: {summary['underlier_price']}")
    print(f"ATM strike: {summary['atm_strike']}")
    print(f"Expiries: {', '.join(plan.expiries)}")
    print(
        "Hot lane: "
        f"{plan.hot_contract_count} contracts, cadence {plan.hot_cadence_seconds}s"
    )
    print(
        "Rolling scan: "
        f"{len(plan.rolling_groups)} {plan.group_strategy} groups, "
        f"{plan.rolling_contract_count} contracts, "
        f"full scan {plan.full_scan_seconds}s"
    )
    print("\nGroups:")
    for group in plan.rolling_groups:
        print(
            f"- group {group.index}: {min(group.strikes)}-{max(group.strikes)} "
            f"strikes={len(group.strikes)} contracts={len(group.contracts)} "
            f"cadence={group.cadence_seconds}s sample={format_strike_sample(group.strikes)}"
        )


def format_strike_sample(strikes: list[int]) -> str:
    sample = ",".join(str(strike) for strike in strikes[:8])
    return f"{sample},..." if len(strikes) > 8 else sample


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build an SPXW sampling plan.")
    parser.add_argument("--underlier", type=float, required=True, help="Current SPX reference price.")
    parser.add_argument("--expiry", default=default_spxw_expiry())
    parser.add_argument("--next-expiry")
    parser.add_argument("--mode", choices=sorted(VALID_MODES))
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--summary-json", action="store_true")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = SamplingSettings.from_env()
    mode = args.mode or settings.default_mode
    plan = build_sampling_plan(
        underlier_price=args.underlier,
        expiry=args.expiry,
        next_expiry=args.next_expiry,
        mode=mode,
        settings=settings,
    )
    if args.json:
        print(json.dumps(asdict(plan), indent=2, sort_keys=True))
    elif args.summary_json:
        print(json.dumps(plan_summary(plan), indent=2, sort_keys=True))
    else:
        print_summary(plan)
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
