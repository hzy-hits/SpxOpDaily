"""Contract factories, qualification keys, and subscription plan builders."""

from __future__ import annotations

from typing import Any, TypeVar

from spx_spark.config import SamplingSettings
from spx_spark.sampling import OptionContractSpec, build_sampling_plan

from spx_spark.ibkr.stream.models import OptionSubscriptionPlan

T = TypeVar("T")


def contract_pairs_by_atm_distance(
    specs: list[OptionContractSpec],
    atm_strike: int,
) -> list[OptionContractSpec]:
    """Order specs nearest-ATM first, keeping C/P pairs adjacent."""
    pairs: dict[tuple[str, int], list[OptionContractSpec]] = {}
    for spec in specs:
        pairs.setdefault((spec.expiry, spec.strike), []).append(spec)

    ordered: list[OptionContractSpec] = []
    for key in sorted(
        pairs,
        key=lambda item: (abs(item[1] - atm_strike), item[0], item[1]),
    ):
        ordered.extend(sorted(pairs[key], key=lambda spec: spec.right))
    return ordered


def build_option_subscription_plan(
    *,
    atm_reference: float,
    expiry: str,
    next_expiry: str | None,
    mode: str,
    sampling_settings: SamplingSettings,
    max_option_lines: int,
    hot_lane_share: float,
) -> OptionSubscriptionPlan:
    plan = build_sampling_plan(
        underlier_price=atm_reference,
        expiry=expiry,
        next_expiry=next_expiry,
        mode=mode,
        settings=sampling_settings,
    )
    total_budget = max(int(max_option_lines), 0)
    hot_budget = min(max(2, int(total_budget * hot_lane_share)), total_budget)
    hot_budget -= hot_budget % 2  # keep whole C/P pairs
    rotation_budget = max(total_budget - hot_budget, 0)
    rotation_budget -= rotation_budget % 2

    hot = tuple(contract_pairs_by_atm_distance(plan.hot_lane, plan.atm_strike)[:hot_budget])
    hot_keys = {(spec.expiry, spec.strike, spec.right) for spec in hot}

    rotations: list[tuple[OptionContractSpec, ...]] = []
    if rotation_budget >= 2:
        for group in plan.rolling_groups:
            remaining = [
                spec
                for spec in contract_pairs_by_atm_distance(group.contracts, plan.atm_strike)
                if (spec.expiry, spec.strike, spec.right) not in hot_keys
            ]
            for start in range(0, len(remaining), rotation_budget):
                chunk = tuple(remaining[start : start + rotation_budget])
                if chunk:
                    rotations.append(chunk)

    return OptionSubscriptionPlan(
        atm_strike=plan.atm_strike,
        expiry=expiry,
        hot=hot,
        rotations=tuple(rotations),
    )


def should_replan(
    plan: OptionSubscriptionPlan | None,
    atm_reference: float | None,
    *,
    replan_drift_points: float,
    today_expiry: str,
) -> bool:
    if atm_reference is None:
        return False
    if plan is None:
        return True
    if plan.expiry != today_expiry:
        return True
    return abs(atm_reference - plan.atm_strike) >= replan_drift_points


def option_spec_label(spec: OptionContractSpec) -> str:
    return f"option:SPXW:{spec.expiry}:{spec.strike}:{spec.right}"


def option_label_distance(label: str, atm_strike: int) -> float:
    try:
        strike = float(label.rsplit(":", 2)[-2])
    except (IndexError, ValueError):
        return float("inf")
    return abs(strike - atm_strike)


def option_contracts_from_specs(specs: tuple[OptionContractSpec, ...]) -> list[tuple[str, str, Any]]:
    from ib_async import Option

    contracts: list[tuple[str, str, Any]] = []
    for spec in specs:
        contracts.append(
            (
                option_spec_label(spec),
                "option",
                Option(
                    "SPX",
                    spec.expiry,
                    float(spec.strike),
                    spec.right,
                    "SMART",
                    multiplier="100",
                    currency="USD",
                    tradingClass="SPXW",
                ),
            )
        )
    return contracts


def build_spy_option_strikes(spy_price: float, *, lines: int, step: int) -> list[int]:
    n_strikes = max(1, lines // 2)
    atm = round(spy_price / step) * step
    return [atm + step * i for i in range(-(n_strikes // 2), n_strikes - n_strikes // 2)]


def spy_option_spec_label(expiry: str, strike: int, right: str) -> str:
    return f"option:SPY:{expiry}:{strike}:{right}"


def spy_option_contracts(expiry: str, strikes: list[int]) -> list[tuple[str, str, Any]]:
    from ib_async import Option

    contracts: list[tuple[str, str, Any]] = []
    for strike in strikes:
        for right in ("C", "P"):
            contracts.append(
                (
                    spy_option_spec_label(expiry, strike, right),
                    "option",
                    Option(
                        "SPY",
                        expiry,
                        float(strike),
                        right,
                        "SMART",
                        multiplier="100",
                        currency="USD",
                        tradingClass="SPY",
                    ),
                )
            )
    return contracts


def split_base_contracts(
    contracts: list[tuple[str, str, Any]],
    slow_poll_labels: tuple[str, ...],
) -> tuple[list[tuple[str, str, Any]], list[tuple[str, str, Any]]]:
    """Split contracts into persistent vs slow-poll lanes by label."""
    slow_set = set(slow_poll_labels)
    persistent: list[tuple[str, str, Any]] = []
    slow: list[tuple[str, str, Any]] = []
    for contract in contracts:
        label = contract[0]
        if label in slow_set:
            slow.append(contract)
        else:
            persistent.append(contract)
    return persistent, slow


def chunked(items: list[T], size: int) -> list[list[T]]:
    chunk_size = size if size > 0 else 1
    return [items[index : index + chunk_size] for index in range(0, len(items), chunk_size)]


def contract_qualification_key(contract: Any) -> tuple[object, ...]:
    return tuple(
        getattr(contract, field, None)
        for field in (
            "secType",
            "symbol",
            "lastTradeDateOrContractMonth",
            "strike",
            "right",
        )
    )

