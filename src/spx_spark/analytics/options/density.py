"""Risk-neutral density from synthetic call curves."""

from __future__ import annotations

from spx_spark.analytics.options.constants import (
    RN_DENSITY_MIN_STRIKES,
    RN_DENSITY_NOISY_CLIP_FRACTION,
)
from spx_spark.analytics.options.models import DensityDiagnostics, DensityQuality, RnDensity
from spx_spark.analytics.options.pricing import option_mid
from spx_spark.marketdata import OptionRight, Quote


def _synthetic_call_curve(
    pairs: dict[float, dict[OptionRight, Quote]],
    underlier: float,
) -> list[tuple[float, float]]:
    """Call mid per strike, synthesized from the OTM side via put-call parity.

    Below spot the OTM put is the liquid quote: C = P + S - K (r≈0). Above
    spot the call itself is OTM. Deep ITM mids are wide/stale and would poison
    the second derivative.
    """
    points: list[tuple[float, float]] = []
    for strike in sorted(pairs):
        sides = pairs[strike]
        call_mid = option_mid(sides.get(OptionRight.CALL))
        put_mid = option_mid(sides.get(OptionRight.PUT))
        if strike < underlier:
            synth = put_mid + underlier - strike if put_mid is not None else call_mid
        else:
            synth = (
                call_mid
                if call_mid is not None
                else (put_mid + underlier - strike if put_mid is not None else None)
            )
        if synth is not None and synth > 0:
            points.append((strike, synth))
    return points


def build_rn_density(
    pairs: dict[float, dict[OptionRight, Quote]],
    *,
    underlier: float,
    put_wall: float | None = None,
    call_wall: float | None = None,
    expected_move_points: float | None = None,
) -> RnDensity:
    """Breeden-Litzenberger: f(K) = d²C/dK² via non-uniform second differences."""
    points = _synthetic_call_curve(pairs, underlier)
    if len(points) < RN_DENSITY_MIN_STRIKES:
        return RnDensity(
            quality=DensityQuality.INSUFFICIENT_STRIKES,
            diagnostics=DensityDiagnostics(usable_strikes=len(points)),
        )

    strikes = [strike for strike, _mid in points]
    mids = [mid for _strike, mid in points]

    # Second derivative at interior strikes (non-uniform grid).
    raw: list[tuple[float, float]] = []
    for index in range(1, len(points) - 1):
        k0, k1, k2 = strikes[index - 1], strikes[index], strikes[index + 1]
        c0, c1, c2 = mids[index - 1], mids[index], mids[index + 1]
        h01, h12, h02 = k1 - k0, k2 - k1, k2 - k0
        if h01 <= 0 or h12 <= 0:
            continue
        density = 2.0 * (c0 / (h01 * h02) - c1 / (h01 * h12) + c2 / (h12 * h02))
        raw.append((k1, density))
    if len(raw) < 3:
        return RnDensity(
            quality=DensityQuality.INSUFFICIENT_STRIKES,
            diagnostics=DensityDiagnostics(usable_strikes=len(points)),
        )

    # Noisy mids produce negative lobes; clip and track how much mass we cut.
    positive_mass = 0.0
    clipped_mass = 0.0
    cells: list[tuple[float, float, float, float]] = []  # (low, high, strike, mass)
    for index, (strike, density) in enumerate(raw):
        low = (
            (raw[index - 1][0] + strike) / 2.0
            if index > 0
            else strike - (raw[index + 1][0] - strike) / 2.0
        )
        high = (
            (strike + raw[index + 1][0]) / 2.0
            if index < len(raw) - 1
            else strike + (strike - raw[index - 1][0]) / 2.0
        )
        width = max(high - low, 0.0)
        mass = density * width
        if mass >= 0:
            positive_mass += mass
            cells.append((low, high, strike, mass))
        else:
            clipped_mass += -mass
    if positive_mass <= 0:
        return RnDensity(
            quality=DensityQuality.INSUFFICIENT_STRIKES,
            diagnostics=DensityDiagnostics(usable_strikes=len(points)),
        )
    clipped_fraction = clipped_mass / (positive_mass + clipped_mass)

    cells = [(low, high, strike, mass / positive_mass) for low, high, strike, mass in cells]

    def cdf(level: float) -> float:
        total = 0.0
        for low, high, _strike, mass in cells:
            if level >= high:
                total += mass
            elif level > low:
                total += mass * (level - low) / (high - low)
        return min(1.0, max(0.0, total))

    def percentile(target: float) -> float | None:
        cumulative = 0.0
        for low, high, _strike, mass in cells:
            if mass <= 0:
                continue
            if cumulative + mass >= target:
                return low + (high - low) * (target - cumulative) / mass
            cumulative += mass
        return None

    strike_lo, strike_hi = strikes[0], strikes[-1]
    quality = DensityQuality.OK
    if clipped_fraction > RN_DENSITY_NOISY_CLIP_FRACTION:
        quality = DensityQuality.NOISY_QUOTES
    elif expected_move_points and expected_move_points > 0:
        if (
            strike_lo > underlier - expected_move_points
            or strike_hi < underlier + expected_move_points
        ):
            quality = DensityQuality.NARROW_RANGE

    round1 = lambda value: round(value, 1) if value is not None else None  # noqa: E731
    diagnostics = DensityDiagnostics(
        usable_strikes=len(points),
        clipped_mass_fraction=round(clipped_fraction, 3),
        lower_width_points=round(underlier - strike_lo, 1),
        upper_width_points=round(strike_hi - underlier, 1),
        negative_mass_fraction=round(clipped_fraction, 3),
        normalized_mass=1.0,
    )
    return RnDensity(
        quality=quality,
        median=round1(percentile(0.5)),
        p10=round1(percentile(0.1)),
        p25=round1(percentile(0.25)),
        p75=round1(percentile(0.75)),
        p90=round1(percentile(0.9)),
        prob_below_put_wall=round(cdf(put_wall), 3) if put_wall is not None else None,
        prob_above_call_wall=round(1.0 - cdf(call_wall), 3) if call_wall is not None else None,
        clipped_mass_fraction=round(clipped_fraction, 3),
        strike_range=(strike_lo, strike_hi),
        diagnostics=diagnostics,
    )
