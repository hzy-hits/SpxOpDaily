"""Schwab option-chain request construction and adaptive width."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from statistics import median

from spx_spark.marketdata import OptionRight, Quote


@dataclass(frozen=True)
class ChainWidthPolicy:
    candidates: tuple[int, ...] = (80, 100, 120)
    min_usable_strikes: int = 40
    min_two_sided_ratio: float = 0.80
    expected_move_multiple: float = 2.5
    min_width_points: float = 150.0
    max_gap_multiple: float = 2.0

    def __post_init__(self) -> None:
        if not self.candidates or tuple(sorted(set(self.candidates))) != self.candidates:
            raise ValueError("strike-count candidates must be unique and ascending")


@dataclass(frozen=True)
class ChainCoverageObservation:
    distinct_strikes: int
    usable_strikes: int
    two_sided_strikes: int
    lower_width_points: float | None
    upper_width_points: float | None
    expected_move_points: float | None
    median_step: float | None
    max_gap: float | None

    @property
    def two_sided_ratio(self) -> float:
        return self.two_sided_strikes / self.usable_strikes if self.usable_strikes else 0.0


def chain_params(*, symbol: str, expiry: date, strike_count: int) -> dict[str, str | int]:
    return {
        "symbol": symbol,
        "contractType": "ALL",
        "strategy": "SINGLE",
        "strikeCount": strike_count,
        "includeUnderlyingQuote": "true",
        "fromDate": expiry.isoformat(),
        "toDate": expiry.isoformat(),
    }


def measure_chain_coverage(quotes: tuple[Quote, ...], *, spot: float | None) -> ChainCoverageObservation:
    pairs: dict[float, dict[OptionRight, Quote]] = {}
    for quote in quotes:
        strike = quote.instrument.strike
        right = quote.instrument.right
        if strike is None or right is None:
            continue
        pairs.setdefault(float(strike), {})[right] = quote
    usable = {
        strike: sides
        for strike, sides in pairs.items()
        if any(quote.mid is not None for quote in sides.values())
    }
    two_sided = sum(
        1
        for sides in usable.values()
        if sides.get(OptionRight.CALL) is not None
        and sides[OptionRight.CALL].mid is not None
        and sides.get(OptionRight.PUT) is not None
        and sides[OptionRight.PUT].mid is not None
    )
    strikes = sorted(usable)
    steps = [right - left for left, right in zip(strikes, strikes[1:], strict=False)]
    lower = spot - strikes[0] if spot is not None and strikes else None
    upper = strikes[-1] - spot if spot is not None and strikes else None
    expected_move = None
    if spot is not None and strikes:
        atm = min(strikes, key=lambda strike: abs(strike - spot))
        call = usable[atm].get(OptionRight.CALL)
        put = usable[atm].get(OptionRight.PUT)
        if call is not None and put is not None and call.mid is not None and put.mid is not None:
            expected_move = 0.85 * (call.mid + put.mid)
    return ChainCoverageObservation(
        distinct_strikes=len(pairs),
        usable_strikes=len(usable),
        two_sided_strikes=two_sided,
        lower_width_points=lower,
        upper_width_points=upper,
        expected_move_points=expected_move,
        median_step=median(steps) if steps else None,
        max_gap=max(steps) if steps else None,
    )


def coverage_sufficient(observation: ChainCoverageObservation, policy: ChainWidthPolicy) -> bool:
    required_width = max(
        policy.min_width_points,
        policy.expected_move_multiple * (observation.expected_move_points or 0.0),
    )
    return bool(
        observation.usable_strikes >= policy.min_usable_strikes
        and observation.two_sided_ratio >= policy.min_two_sided_ratio
        and observation.lower_width_points is not None
        and observation.lower_width_points >= required_width
        and observation.upper_width_points is not None
        and observation.upper_width_points >= required_width
        and observation.median_step is not None
        and observation.max_gap is not None
        and observation.max_gap <= policy.max_gap_multiple * observation.median_step
    )


def next_strike_count(current: int, observation: ChainCoverageObservation, policy: ChainWidthPolicy) -> int:
    if coverage_sufficient(observation, policy):
        return current
    for candidate in policy.candidates:
        if candidate > current:
            return candidate
    return policy.candidates[-1]
