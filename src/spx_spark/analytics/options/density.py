"""Risk-neutral density and local operators from one synthetic Call curve."""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import replace
from datetime import datetime, timezone

from spx_spark.analytics.options.constants import (
    RN_DENSITY_MIN_STRIKES,
    RN_DENSITY_NOISY_CLIP_FRACTION,
)
from spx_spark.analytics.options.models import (
    DensityDiagnostics,
    DensityQuality,
    RnDensity,
    SyntheticCallPoint,
)
from spx_spark.analytics.options.pricing import option_mid
from spx_spark.marketdata import OptionRight, Quote


STRIKE_DIFFERENTIAL_SCALES_POINTS = (5.0, 10.0, 15.0, 20.0)
STRIKE_DIFFERENTIAL_FEATURE_VERSION = "strike_differential_context.v1"
_STATIC_ARBITRAGE_TOLERANCE = 1e-9
_SNR_EPSILON = 1e-12
_REFERENCE_LABEL_PRIORITY = (
    "atm",
    "q_mode",
    "zero_gamma",
    "flip_midpoint",
    "put_wall",
    "call_wall",
)
_VIRTUAL_PORTFOLIO_UNITS: dict[str, int | list[int]] = {
    "d2_gross": 4,
    "d2_raw_coefficients": [1, -2, 1],
    "d3_gross": 8,
    "d3_raw_coefficients": [-1, 2, 0, -2, 1],
    "d4_gross": 16,
    "d4_raw_coefficients": [1, -4, 6, -4, 1],
    "mexican_hat_gross": 16,
    "mexican_hat_raw_coefficients": [-1, 4, -6, 4, -1],
    "richardson_gross": 64,
    "richardson_raw_coefficients": [-1, 16, -30, 16, -1],
    "simpson_netted_gross": 12,
    "simpson_raw_coefficients": [1, 2, -6, 2, 1],
}


def synthetic_call_curve(
    pairs: dict[float, dict[OptionRight, Quote]],
    underlier: float,
) -> tuple[SyntheticCallPoint, ...]:
    """Build the shared OTM/parity Call curve used by density and local operators."""

    points: list[SyntheticCallPoint] = []
    for strike in sorted(pairs):
        sides = pairs[strike]
        call, put = sides.get(OptionRight.CALL), sides.get(OptionRight.PUT)
        call_mid, put_mid = option_mid(call), option_mid(put)
        if strike < underlier:
            quote, mid, shift = (
                (put, put_mid, underlier - strike)
                if put_mid is not None
                else (call, call_mid, 0.0)
            )
        else:
            quote, mid, shift = (
                (call, call_mid, 0.0)
                if call_mid is not None
                else (put, put_mid, underlier - strike)
            )
        if quote is None or mid is None:
            continue
        synthetic_mid = mid + shift
        if synthetic_mid <= 0:
            continue
        bid, ask = _synthetic_bbo(quote, shift=shift)
        points.append(
            SyntheticCallPoint(
                strike=float(strike),
                mid=synthetic_mid,
                bid=bid,
                ask=ask,
                source_right=quote.instrument.right.value if quote.instrument.right else "",
                source_at=quote.quote_time or quote.trade_time or quote.received_at,
            )
        )
    return tuple(points)


def build_rn_density(
    pairs: dict[float, dict[OptionRight, Quote]],
    *,
    underlier: float,
    put_wall: float | None = None,
    call_wall: float | None = None,
    expected_move_points: float | None = None,
    expiry: str | None = None,
    as_of: datetime | None = None,
    reference_levels: Mapping[str, float | None] | None = None,
) -> RnDensity:
    """Build global RN density and, when requested, independent local context."""

    curve = synthetic_call_curve(pairs, underlier)
    density, cdf = _build_global_density(
        curve,
        underlier=underlier,
        put_wall=put_wall,
        call_wall=call_wall,
        expected_move_points=expected_move_points,
    )
    if expiry is None or as_of is None or reference_levels is None:
        return density

    curve_is_causal = all(
        point.source_at is not None and _utc(point.source_at) <= _utc(as_of)
        for point in curve
    )
    references = dict(reference_levels)
    references["q_mode"] = density.mode if curve_is_causal else None
    context = build_strike_differential_context(
        curve,
        expiry=expiry,
        as_of=as_of,
        reference_levels=references,
        density_interval_mass=(
            (lambda lower, upper: cdf(upper) - cdf(lower))
            if cdf is not None and curve_is_causal
            else None
        ),
    )
    publishable = any(
        observation.get("strike_d2") is not None
        or observation.get("quality") != "unavailable_missing_strikes"
        for reference in context["references"]
        for observation in reference["observations"]
    )
    return replace(
        density,
        strike_differential_context=context if publishable else None,
    )


def build_strike_differential_context(
    curve: Sequence[SyntheticCallPoint],
    *,
    expiry: str,
    as_of: datetime,
    reference_levels: Mapping[str, float | None],
    scales: Sequence[float] = STRIKE_DIFFERENTIAL_SCALES_POINTS,
    density_interval_mass: Callable[[float, float], float] | None = None,
) -> dict[str, object]:
    """Build compact, exact-strike D2/D3/D4 context without trading authority."""

    point_by_strike = {point.strike: point for point in sorted(curve, key=lambda row: row.strike)}
    scale_values = tuple(
        dict.fromkeys(float(scale) for scale in scales if math.isfinite(scale) and scale > 0)
    )[:4]
    references = _select_references(reference_levels, available_strikes=set(point_by_strike))
    missing_strikes: set[float] = set()
    observations: list[dict[str, object]] = []
    monotonic_violations = 0
    convexity_violations = 0
    reference_rows: list[dict[str, object]] = []

    for center, labels in references:
        center_observations: list[dict[str, object]] = []
        for scale in scale_values:
            observation, missing, monotonic, convexity = _build_observation(
                point_by_strike,
                center=center,
                scale=scale,
                scales=scale_values,
                as_of=as_of,
                density_interval_mass=density_interval_mass,
            )
            center_observations.append(observation)
            observations.append(observation)
            missing_strikes.update(missing)
            monotonic_violations += int(monotonic)
            convexity_violations += int(convexity)
        reference_rows.append(
            {
                "center": center,
                "labels": list(labels),
                "observations": center_observations,
            }
        )

    ready_count = sum(row["quality"] == "ready" for row in observations)
    degraded_count = sum(str(row["quality"]).startswith("degraded_") for row in observations)
    unavailable_count = len(observations) - ready_count - degraded_count
    usable_count = sum(
        row.get("strike_d2") is not None
        and not str(row["quality"]).startswith("blocked_")
        and row["quality"] != "unavailable_future_quote"
        for row in observations
    )
    if usable_count == 0:
        status = "unavailable"
    elif ready_count == len(observations):
        status = "ready"
    else:
        status = "partial"

    return {
        "feature_version": STRIKE_DIFFERENTIAL_FEATURE_VERSION,
        "authority": "context_only",
        "semantics": "risk_neutral_strike_shape",
        "expiry": expiry,
        "as_of": _utc(as_of).isoformat(),
        "status": status,
        "source_curve": "otm_synthetic_call_bbo",
        "scales_points": list(scale_values),
        "references": reference_rows,
        "diagnostics": {
            "reference_count": len(reference_rows),
            "observation_count": len(observations),
            "ready_count": ready_count,
            "degraded_count": degraded_count,
            "unavailable_count": unavailable_count,
            "missing_strikes": sorted(missing_strikes),
            "local_monotonic_violations": monotonic_violations,
            "local_convexity_violations": convexity_violations,
        },
    }


def _build_observation(
    point_by_strike: Mapping[float, SyntheticCallPoint],
    *,
    center: float,
    scale: float,
    scales: Sequence[float],
    as_of: datetime,
    density_interval_mass: Callable[[float, float], float] | None,
) -> tuple[dict[str, object], set[float], bool, bool]:
    required = tuple(center + offset * scale for offset in (-2, -1, 0, 1, 2))
    points = tuple(point_by_strike.get(strike) for strike in required)
    missing = {strike for strike, point in zip(required, points, strict=True) if point is None}
    base: dict[str, object] = {
        "scale_points": scale,
        "quality": "unavailable_missing_strikes",
        "fly_mid_points": None,
        "strike_d2": None,
        "adjacent_fly_spread_points": None,
        "strike_d3": None,
        "fly_curvature_points": None,
        "strike_d4": None,
        "mexican_hat_points": None,
        "peak_vs_shoulders": None,
        "dependency_group": "d4_equivalent",
        "d2_noise_bound": None,
        "d3_noise_bound": None,
        "d4_noise_bound": None,
        "d2_snr": None,
        "d3_snr": None,
        "d4_snr": None,
        "richardson": None,
        "simpson_local_mass": None,
        "virtual_portfolio_units": dict(_VIRTUAL_PORTFOLIO_UNITS),
        "required_strikes": list(required),
        "reasons": [],
    }
    present = [point for point in points if point is not None]
    if any(point.source_at is None or _utc(point.source_at) > _utc(as_of) for point in present):
        base["quality"] = "unavailable_future_quote"
        base["reasons"] = ["source_quote_after_as_of_or_missing"]
        return base, missing, False, False

    inner = points[1:4]
    if any(point is None for point in inner):
        base["reasons"] = ["missing_exact_strikes"]
        return base, missing, False, False

    cm1, c0, cp1 = (point.mid for point in inner if point is not None)
    fly_mid = cm1 - 2.0 * c0 + cp1
    strike_d2 = fly_mid / scale**2
    base["fly_mid_points"] = fly_mid
    base["strike_d2"] = strike_d2
    d2_noise = _noise_bound(inner, (1.0, -2.0, 1.0), divisor=scale**2)
    base["d2_noise_bound"] = d2_noise
    base["d2_snr"] = _snr(strike_d2, d2_noise)

    ordered = [point for point in points if point is not None]
    monotonic = any(
        right.mid > left.mid + _STATIC_ARBITRAGE_TOLERANCE
        for left, right in zip(ordered, ordered[1:])
    )
    convexity = fly_mid < -_STATIC_ARBITRAGE_TOLERANCE
    if len(points) == 5 and not missing:
        cm2, cm1_point, c0_point, cp1_point, cp2 = points
        assert all(point is not None for point in points)
        convexity = convexity or any(
            value < -_STATIC_ARBITRAGE_TOLERANCE
            for value in (
                cm2.mid - 2.0 * cm1_point.mid + c0_point.mid,
                c0_point.mid - 2.0 * cp1_point.mid + cp2.mid,
            )
        )
    if monotonic or convexity:
        base["quality"] = (
            "blocked_monotonicity_violation"
            if monotonic
            else "blocked_convexity_violation"
        )
        base["reasons"] = [
            reason
            for violated, reason in (
                (monotonic, "local_call_monotonicity_violation"),
                (convexity, "local_call_convexity_violation"),
            )
            if violated
        ]
        return base, missing, monotonic, convexity

    if missing:
        base["reasons"] = ["missing_exact_strikes"]
        if 2.0 * scale in scales:
            base["richardson"] = _unavailable_operator(
                quality="unavailable_missing_strikes",
                reasons=["missing_exact_strikes"],
            )
        base["simpson_local_mass"] = _unavailable_operator(
            quality="unavailable_missing_strikes",
            reasons=["missing_exact_strikes"],
        )
        return base, missing, False, False

    cm2, cm1_point, c0_point, cp1_point, cp2 = points
    assert all(point is not None for point in points)
    values = tuple(point.mid for point in points)
    c_m2, c_m1, c_0, c_p1, c_p2 = values
    adjacent_fly = -c_m2 + 2.0 * c_m1 - 2.0 * c_p1 + c_p2
    fly_curvature = c_m2 - 4.0 * c_m1 + 6.0 * c_0 - 4.0 * c_p1 + c_p2
    mexican_hat = -fly_curvature
    strike_d3 = adjacent_fly / (2.0 * scale**3)
    strike_d4 = fly_curvature / scale**4
    peak_vs_shoulders = mexican_hat / (2.0 * scale**2)
    d3_noise = _noise_bound(points, (-1.0, 2.0, 0.0, -2.0, 1.0), divisor=2.0 * scale**3)
    d4_noise = _noise_bound(points, (1.0, -4.0, 6.0, -4.0, 1.0), divisor=scale**4)
    d3_snr, d4_snr = _snr(strike_d3, d3_noise), _snr(strike_d4, d4_noise)
    base.update(
        adjacent_fly_spread_points=adjacent_fly,
        strike_d3=strike_d3,
        fly_curvature_points=fly_curvature,
        strike_d4=strike_d4,
        mexican_hat_points=mexican_hat,
        peak_vs_shoulders=peak_vs_shoulders,
        d3_noise_bound=d3_noise,
        d4_noise_bound=d4_noise,
        d3_snr=d3_snr,
        d4_snr=d4_snr,
    )

    richardson = (
        _richardson(points, center=center, scale=scale, strike_d2=strike_d2)
        if 2.0 * scale in scales
        else None
    )
    simpson = _simpson_local_mass(
        points,
        center=center,
        scale=scale,
        density_interval_mass=density_interval_mass,
    )
    base["richardson"] = richardson
    base["simpson_local_mass"] = simpson

    missing_bbo = any(bound is None for bound in (d2_noise, d3_noise, d4_noise))
    low_snr = [
        name
        for name, value in (("d2", base["d2_snr"]), ("d3", d3_snr), ("d4", d4_snr))
        if isinstance(value, int | float) and value < 1.0
    ]
    if missing_bbo:
        base["quality"] = "degraded_missing_bbo"
        base["reasons"] = ["operator_bbo_incomplete"]
    elif low_snr:
        base["quality"] = "degraded_low_snr"
        base["reasons"] = [f"low_snr:{name}" for name in low_snr]
    else:
        base["quality"] = "ready"
    return base, missing, False, False


def _richardson(
    points: Sequence[SyntheticCallPoint | None],
    *,
    center: float,
    scale: float,
    strike_d2: float,
) -> dict[str, object]:
    concrete = tuple(point for point in points if point is not None)
    if len(concrete) != 5:
        return _unavailable_operator(
            quality="unavailable_missing_strikes",
            reasons=["missing_exact_strikes"],
        )
    c_m2, _c_m1, c_0, _c_p1, c_p2 = (point.mid for point in concrete)
    paired_d2 = (c_m2 - 2.0 * c_0 + c_p2) / (2.0 * scale) ** 2
    value = (4.0 * strike_d2 - paired_d2) / 3.0
    noise = _noise_bound(
        concrete,
        (-1.0, 16.0, -30.0, 16.0, -1.0),
        divisor=12.0 * scale**2,
    )
    snr = _snr(value, noise)
    reasons: list[str] = []
    if value * strike_d2 < 0:
        reasons.append("scale_sign_conflict")
    if noise is None:
        quality = "degraded_missing_bbo"
        reasons.append("operator_bbo_incomplete")
    elif snr is not None and snr < 1.0:
        quality = "degraded_low_snr"
        reasons.append("low_snr")
    else:
        quality = "ready"
    return {
        "base_scale_points": scale,
        "paired_scale_points": 2.0 * scale,
        "strike_d2": value,
        "truncation_disagreement": abs(paired_d2 - strike_d2) / 3.0,
        "noise_bound": noise,
        "snr": snr,
        "quality": quality,
        "reasons": reasons,
    }


def _simpson_local_mass(
    points: Sequence[SyntheticCallPoint | None],
    *,
    center: float,
    scale: float,
    density_interval_mass: Callable[[float, float], float] | None,
) -> dict[str, object]:
    concrete = tuple(point for point in points if point is not None)
    if len(concrete) != 5:
        return _unavailable_operator(
            quality="unavailable_missing_strikes",
            reasons=["missing_exact_strikes"],
        )
    value = sum(
        coefficient * point.mid
        for coefficient, point in zip((1.0, 2.0, -6.0, 2.0, 1.0), concrete, strict=True)
    ) / (3.0 * scale)
    noise = _noise_bound(
        concrete,
        (1.0, 2.0, -6.0, 2.0, 1.0),
        divisor=3.0 * scale,
    )
    snr = _snr(value, noise)
    global_mass = density_interval_mass(center - scale, center + scale) if density_interval_mass else None
    reasons: list[str] = []
    if noise is None:
        quality = "degraded_missing_bbo"
        reasons.append("operator_bbo_incomplete")
    elif snr is not None and snr < 1.0:
        quality = "degraded_low_snr"
        reasons.append("low_snr")
    else:
        quality = "ready"
    return {
        "lower": center - scale,
        "upper": center + scale,
        "state_price_mass_proxy": value,
        "noise_bound": noise,
        "snr": snr,
        "rn_density_interval_mass": global_mass,
        "quadrature_mass_gap": value - global_mass if global_mass is not None else None,
        "quality": quality,
        "reasons": reasons,
    }


def _unavailable_operator(*, quality: str, reasons: list[str]) -> dict[str, object]:
    return {"quality": quality, "reasons": reasons}


def _noise_bound(
    points: Sequence[SyntheticCallPoint | None],
    coefficients: Sequence[float],
    *,
    divisor: float,
) -> float | None:
    if len(points) != len(coefficients) or any(
        point is None or point.bid is None or point.ask is None for point in points
    ):
        return None
    return sum(
        abs(coefficient) * (point.ask - point.bid) / 2.0
        for coefficient, point in zip(coefficients, points, strict=True)
        if point is not None and point.ask is not None and point.bid is not None
    ) / divisor


def _snr(value: float, noise_bound: float | None) -> float | None:
    return abs(value) / max(noise_bound, _SNR_EPSILON) if noise_bound is not None else None


def _select_references(
    reference_levels: Mapping[str, float | None],
    *,
    available_strikes: set[float],
) -> tuple[tuple[float, tuple[str, ...]], ...]:
    labels = [
        *(_REFERENCE_LABEL_PRIORITY),
        *(label for label in reference_levels if label not in _REFERENCE_LABEL_PRIORITY),
    ]
    grouped: dict[float, list[str]] = {}
    for label in dict.fromkeys(labels):
        level = reference_levels.get(label)
        if level is None or not math.isfinite(level):
            continue
        center = _round_to_five(float(level))
        if center not in available_strikes:
            continue
        grouped.setdefault(center, []).append(label)
    return tuple((center, tuple(labels)) for center, labels in list(grouped.items())[:6])


def _round_to_five(value: float) -> float:
    return float(math.floor(value / 5.0 + 0.5) * 5.0)


def _synthetic_bbo(quote: Quote, *, shift: float) -> tuple[float | None, float | None]:
    if (
        quote.bid is None
        or quote.ask is None
        or quote.bid < 0
        or quote.ask <= 0
        or quote.ask < quote.bid
    ):
        return None, None
    return quote.bid + shift, quote.ask + shift


def _utc(value: datetime) -> datetime:
    return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _build_global_density(
    points: Sequence[SyntheticCallPoint],
    *,
    underlier: float,
    put_wall: float | None,
    call_wall: float | None,
    expected_move_points: float | None,
) -> tuple[RnDensity, Callable[[float], float] | None]:
    """Preserve the historical global-density calculation byte-for-byte in semantics."""

    if len(points) < RN_DENSITY_MIN_STRIKES:
        return (
            RnDensity(
                quality=DensityQuality.INSUFFICIENT_STRIKES,
                diagnostics=DensityDiagnostics(usable_strikes=len(points)),
            ),
            None,
        )

    strikes = [point.strike for point in points]
    mids = [point.mid for point in points]
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
        return (
            RnDensity(
                quality=DensityQuality.INSUFFICIENT_STRIKES,
                diagnostics=DensityDiagnostics(usable_strikes=len(points)),
            ),
            None,
        )

    positive_mass = 0.0
    clipped_mass = 0.0
    cells: list[tuple[float, float, float, float]] = []
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
        return (
            RnDensity(
                quality=DensityQuality.INSUFFICIENT_STRIKES,
                diagnostics=DensityDiagnostics(usable_strikes=len(points)),
            ),
            None,
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
    return (
        RnDensity(
            quality=quality,
            median=round1(percentile(0.5)),
            p10=round1(percentile(0.1)),
            p25=round1(percentile(0.25)),
            p75=round1(percentile(0.75)),
            p90=round1(percentile(0.9)),
            mode=round1(max(raw, key=lambda item: item[1])[0]),
            local_mass_5pt={
                f"{center:g}": round(cdf(center + 2.5) - cdf(center - 2.5), 4)
                for center in range(
                    round(underlier / 5) * 5 - 30,
                    round(underlier / 5) * 5 + 31,
                    5,
                )
            },
            prob_below_put_wall=round(cdf(put_wall), 3) if put_wall is not None else None,
            prob_above_call_wall=(
                round(1.0 - cdf(call_wall), 3) if call_wall is not None else None
            ),
            clipped_mass_fraction=round(clipped_fraction, 3),
            strike_range=(strike_lo, strike_hi),
            diagnostics=diagnostics,
        ),
        cdf,
    )
