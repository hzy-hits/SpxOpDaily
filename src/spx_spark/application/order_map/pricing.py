"""Pure order-map pricing helpers: ticks, Taylor/BS projection, ETA."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone

from spx_spark.analytics.greeks.black_scholes import black76_price
from spx_spark.analytics.options.pricing import finite_float, option_mid
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import OptionRight, Quote
from spx_spark.settings.order_map import DEFAULT_ORDER_MAP_POLICY, OrderMapPolicy

YEAR_SECONDS = 365.0 * 24.0 * 3600.0
# Fraction of remaining time expected to elapse before the touch, as a
# multiple of the Brownian-scaling estimate (distance/EM)^2. Calibrated on
# 2026-07-07: SPX covered 23.5pts (0.91 EM) in ~59% of the remaining session;
# first passages concentrate earlier than the full scaling suggests.
TOUCH_TIME_FRACTION_COEF = 0.6
TOUCH_TIME_FRACTION_MAX = 0.90
# "Sticky strike plus": on a move down, fixed-strike IV rises by roughly this
# multiple of the local smile slope (and falls on the way up).
VOL_SLOPE_BETA = 1.2
MIN_TAU_AT_TOUCH_HOURS = 0.25


@dataclass(frozen=True)
class BSProjection:
    """Auditable inputs and outputs for one target-level BS scenario."""

    projected_mid: float
    model_anchor_price: float
    model_target_price: float
    iv_now: float
    iv_at_touch: float
    tau_now_minutes: float
    tau_at_touch_minutes: float
    touch_time_fraction: float
    early_projected_mid: float
    late_projected_mid: float
    price_range_low: float
    price_range_high: float
    forward_now: float
    forward_at_touch: float
    discount_factor_now: float
    pricing_kernel: str


@dataclass(frozen=True)
class IVSurfaceFit:
    center: float
    intercept: float
    linear: float
    quadratic: float
    point_count: int

    def value_at(self, strike: float) -> float:
        x = strike - self.center
        return self.intercept + self.linear * x + self.quadratic * x * x


def option_tick(premium: float) -> float:
    """SPX option tick: 0.05 below 3.00, 0.10 at/above."""
    return 0.05 if premium < 3.0 else 0.10


def round_to_tick(premium: float) -> float:
    """Round DOWN to tick (limit buy: 挂低一格比挂高一格好)."""
    tick = option_tick(premium)
    return math.floor(premium / tick + 1e-12) * tick


def project_option_price(
    mid: float, delta: float, gamma: float, spot: float, target: float
) -> float:
    """Second-order Taylor projection, clamped to >= 0.05.

    Fallback only: ignores theta and vol dynamics, which for 0DTE makes buy
    limits fill hours early on pure time decay (2026-07-07: projected 16.04
    for 7500C at the wall, actual at touch 12.45). Prefer
    project_option_price_bs when IV is available.
    """
    move = target - spot
    projected = mid + delta * move + 0.5 * gamma * move * move
    return max(0.05, projected)


def expiry_close_utc(expiry: str) -> datetime | None:
    """SPXW PM-settled close, including calendar early-close sessions."""
    try:
        day = datetime.strptime(expiry, "%Y%m%d").date()
    except ValueError:
        return None
    session = DEFAULT_MARKET_CALENDAR.session(day)
    return session.close_at.astimezone(timezone.utc) if session is not None else None


def smile_slope_per_point(
    pairs: dict[float, dict[OptionRight, Quote]],
    right: str,
    strike: float,
    strike_step: float,
) -> float | None:
    """Local dIV/dK (per index point) via least squares on nearby strikes."""
    right_enum = OptionRight.CALL if right == "C" else OptionRight.PUT
    points: list[tuple[float, float]] = []
    for strike_k in sorted(pairs):
        if abs(strike_k - strike) > 3.0 * strike_step:
            continue
        quote = (pairs.get(strike_k) or {}).get(right_enum)
        if quote is None or quote.greeks is None:
            continue
        iv = finite_float(quote.greeks.implied_vol)
        if iv is not None and iv > 0:
            points.append((strike_k, iv))
    if len(points) < 2:
        return None
    n = float(len(points))
    sx = sum(k for k, _ in points)
    sy = sum(v for _, v in points)
    sxx = sum(k * k for k, _ in points)
    sxy = sum(k * v for k, v in points)
    denom = n * sxx - sx * sx
    if abs(denom) < 1e-9:
        return None
    return (n * sxy - sx * sy) / denom


def parity_forward(
    pairs: dict[float, dict[OptionRight, Quote]],
    *,
    discount_factor: float = 1.0,
) -> float | None:
    """Robust near-ATM forward from put-call parity across usable pairs."""

    if not 0 < discount_factor <= 1:
        return None
    values: list[tuple[float, float]] = []
    for strike, sides in pairs.items():
        call_mid = option_mid(sides.get(OptionRight.CALL))
        put_mid = option_mid(sides.get(OptionRight.PUT))
        if call_mid is None or put_mid is None:
            continue
        values.append((abs(call_mid - put_mid), strike + (call_mid - put_mid) / discount_factor))
    if not values:
        return None
    values.sort(key=lambda item: item[0])
    sample = sorted(value for _, value in values[: min(5, len(values))])
    middle = len(sample) // 2
    return sample[middle] if len(sample) % 2 else (sample[middle - 1] + sample[middle]) / 2


def fit_iv_surface(
    pairs: dict[float, dict[OptionRight, Quote]],
    right: str,
    strike: float,
    strike_step: float,
) -> IVSurfaceFit | None:
    """Weighted local quadratic IV surface with a linear fallback."""

    right_enum = OptionRight.CALL if right == "C" else OptionRight.PUT
    points: list[tuple[float, float, float]] = []
    radius = max(4.0 * strike_step, 1.0)
    for strike_k, sides in pairs.items():
        if abs(strike_k - strike) > radius:
            continue
        quote = sides.get(right_enum)
        iv = finite_float(quote.greeks.implied_vol) if quote and quote.greeks else None
        if iv is None or iv <= 0:
            continue
        x = strike_k - strike
        weight = 1.0 / (1.0 + (x / max(strike_step, 1.0)) ** 2)
        points.append((x, iv, weight))
    if len(points) < 3:
        return None
    matrix = [[0.0] * 3 for _ in range(3)]
    vector = [0.0] * 3
    for x, iv, weight in points:
        row = (1.0, x, x * x)
        for i in range(3):
            vector[i] += weight * row[i] * iv
            for j in range(3):
                matrix[i][j] += weight * row[i] * row[j]
    coefficients = _solve_3x3(matrix, vector)
    if coefficients is None:
        return None
    return IVSurfaceFit(strike, *coefficients, point_count=len(points))


def _solve_3x3(matrix: list[list[float]], vector: list[float]) -> tuple[float, float, float] | None:
    augmented = [row[:] + [vector[index]] for index, row in enumerate(matrix)]
    for column in range(3):
        pivot = max(range(column, 3), key=lambda row: abs(augmented[row][column]))
        if abs(augmented[pivot][column]) < 1e-12:
            return None
        augmented[column], augmented[pivot] = augmented[pivot], augmented[column]
        scale = augmented[column][column]
        augmented[column] = [value / scale for value in augmented[column]]
        for row in range(3):
            if row == column:
                continue
            factor = augmented[row][column]
            augmented[row] = [
                value - factor * pivot_value
                for value, pivot_value in zip(augmented[row], augmented[column], strict=True)
            ]
    return tuple(augmented[index][3] for index in range(3))  # type: ignore[return-value]


def project_option_price_bs(
    *,
    mid: float,
    iv: float | None,
    strike: float,
    right: str,
    spot: float,
    target: float,
    tau_now_years: float | None,
    em_points: float | None,
    slope_per_point: float | None,
    policy: OrderMapPolicy = DEFAULT_ORDER_MAP_POLICY,
) -> float | None:
    """Reprice the option at the target level with Black-Scholes.

    Unlike the Taylor projection this accounts for:
    - time decay before the touch (touch time estimated by Brownian scaling
      of distance vs remaining expected move);
    - fixed-strike IV drift along the smile (down moves lift IV, up moves
      compress it).
    The result is ratio-anchored to the current market mid so provider IV or
    model mismatch does not shift the base level.
    """
    projection = build_option_price_bs_projection(
        mid=mid,
        iv=iv,
        strike=strike,
        right=right,
        spot=spot,
        target=target,
        tau_now_years=tau_now_years,
        em_points=em_points,
        slope_per_point=slope_per_point,
        policy=policy,
    )
    return projection.projected_mid if projection is not None else None


def build_option_price_bs_projection(
    *,
    mid: float,
    iv: float | None,
    strike: float,
    right: str,
    spot: float,
    target: float,
    tau_now_years: float | None,
    em_points: float | None,
    slope_per_point: float | None,
    forward_now: float | None = None,
    surface_iv_at_touch: float | None = None,
    empirical_touch_fractions: tuple[float, float, float] | None = None,
    policy: OrderMapPolicy = DEFAULT_ORDER_MAP_POLICY,
) -> BSProjection | None:
    """Build the full target-level scenario used by ``project_option_price_bs``."""

    if iv is None or iv <= 0 or mid <= 0:
        return None
    if tau_now_years is None or tau_now_years <= 0:
        return None
    discount_now = math.exp(-policy.risk_free_rate * tau_now_years)
    forward = forward_now if forward_now is not None and forward_now > 0 else spot / discount_now
    anchor = black76_price(forward, strike, iv, tau_now_years, right, discount_factor=discount_now)
    if anchor <= 0.01:
        return None

    distance = abs(target - spot)
    fraction = 0.05
    if em_points is not None and em_points > 0:
        fraction = min(
            max(policy.touch_time_fraction_coefficient * (distance / em_points) ** 2, 0.05),
            policy.touch_time_fraction_maximum,
        )
    if empirical_touch_fractions is not None:
        fraction = min(
            max(empirical_touch_fractions[1], 0.01),
            policy.touch_time_fraction_maximum,
        )
    iv_touch = surface_iv_at_touch if surface_iv_at_touch and surface_iv_at_touch > 0 else iv
    if surface_iv_at_touch is None and slope_per_point is not None:
        # slope is negative across the put skew; a down move (spot > target)
        # then raises fixed-strike IV, an up move compresses it.
        iv_touch = iv - policy.vol_slope_beta * slope_per_point * (spot - target)
        iv_touch = min(max(iv_touch, 0.5 * iv), 2.5 * iv)

    minimum_tau = policy.minimum_tau_at_touch_hours * 3600.0 / YEAR_SECONDS
    forward_target = forward + (target - spot)

    def price_at_fraction(touch_fraction: float) -> tuple[float, float, float]:
        tau = max(tau_now_years * (1.0 - touch_fraction), minimum_tau)
        discount = math.exp(-policy.risk_free_rate * tau)
        model = black76_price(
            forward_target, strike, iv_touch, tau, right, discount_factor=discount
        )
        return max(0.05, mid * model / anchor), model, tau

    if empirical_touch_fractions is not None:
        early_fraction = min(max(empirical_touch_fractions[0], 0.01), fraction)
        late_fraction = max(
            fraction,
            min(empirical_touch_fractions[2], policy.touch_time_fraction_maximum),
        )
    else:
        early_fraction = max(0.01, fraction * policy.early_touch_fraction_multiplier)
        late_fraction = min(
            policy.touch_time_fraction_maximum,
            fraction * policy.late_touch_fraction_multiplier,
        )
    early_price, _, _ = price_at_fraction(early_fraction)
    projected_mid, target_price, tau_touch = price_at_fraction(fraction)
    late_price, _, _ = price_at_fraction(late_fraction)
    return BSProjection(
        projected_mid=projected_mid,
        model_anchor_price=anchor,
        model_target_price=target_price,
        iv_now=iv,
        iv_at_touch=iv_touch,
        tau_now_minutes=tau_now_years * YEAR_SECONDS / 60.0,
        tau_at_touch_minutes=tau_touch * YEAR_SECONDS / 60.0,
        touch_time_fraction=fraction,
        early_projected_mid=early_price,
        late_projected_mid=late_price,
        price_range_low=min(early_price, projected_mid, late_price),
        price_range_high=max(early_price, projected_mid, late_price),
        forward_now=forward,
        forward_at_touch=forward_target,
        discount_factor_now=discount_now,
        pricing_kernel="black76_parity_forward",
    )


def touch_eta_minutes(
    distance: float,
    em_points: float | None,
    tau_now_years: float | None,
    *,
    policy: OrderMapPolicy = DEFAULT_ORDER_MAP_POLICY,
) -> float | None:
    """Expected minutes until first touch, by the same Brownian scaling the BS
    repricing uses. Discipline rule: if the level has not traded after ~2x
    this estimate, the odds of the play have decayed and the order should be
    pulled (theta has eaten the edge even if the level eventually prints)."""
    if tau_now_years is None or tau_now_years <= 0:
        return None
    if em_points is None or em_points <= 0:
        return None
    fraction = min(
        max(policy.touch_time_fraction_coefficient * (distance / em_points) ** 2, 0.05),
        policy.touch_time_fraction_maximum,
    )
    return fraction * tau_now_years * YEAR_SECONDS / 60.0
