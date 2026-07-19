"""Bounded SPXW spot-by-time exposure surfaces.

The surface is deliberately an open-interest/volume proxy.  Calls are signed
positive and puts negative; no dealer-position sign is inferred.  Each time
slice revalues the existing r=0, q=0 Black-Scholes kernels across a bounded
spot grid and keeps the two weightings separate.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping

from spx_spark.analytics.greeks.black_scholes import bs_gamma
from spx_spark.analytics.greeks.higher_order import (
    bs_charm_per_minute,
    bs_vanna_per_vol_point,
)
from spx_spark.features.exposure_surface_models import (
    DEALER_POSITION_SIGN,
    EXPIRY_TIMEZONE,
    GREEK_KERNELS_PER_CONTRACT_CELL,
    METRIC_UNITS,
    MODEL,
    SCHEMA_VERSION,
    SIGN_CONVENTION,
    STRIKE_LADDER_BASIS,
    WEIGHTINGS,
    WEIGHTING_SEMANTICS,
    YEAR_SECONDS,
    ExposureSurface,
    SurfaceContract,
    SurfaceCoverage,
    SurfaceExtremum,
    SurfaceGridConfig,
    SurfaceMetricPoint,
    SurfaceMetrics,
    SurfaceStrikeLeg,
    SurfaceStrikeRow,
    SurfaceStrikeWeighting,
    SurfaceTimeSlice,
    SurfaceWeightingSlice,
    _PreparedContract,
)
from spx_spark.features.exposure_surface_vectorized import (
    surface_metrics_vectorized as _surface_metrics_vectorized_impl,
)


SCALAR_CALCULATION_ENGINE = "python_math_fsum.v1"
VECTORIZED_CALCULATION_ENGINE = "numpy_vectorized_bs_stable_sum.v1"


def _aware(value: datetime) -> bool:
    return value.tzinfo is not None and value.utcoffset() is not None


def _finite_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _normalized_right(value: Any) -> str | None:
    right = str(value or "").strip().upper()
    if right in {"C", "CALL"}:
        return "C"
    if right in {"P", "PUT"}:
        return "P"
    return None


def _resolve_spot_grid(
    reference_spot: float,
    requested: Iterable[float] | None,
    config: SurfaceGridConfig,
) -> tuple[float, ...]:
    if requested is None:
        values = tuple(
            reference_spot + offset * config.spot_step_points
            for offset in range(-config.spot_steps_each_side, config.spot_steps_each_side + 1)
            if reference_spot + offset * config.spot_step_points > 0
        )
    else:
        values = tuple(requested)
    if not values:
        raise ValueError("spot grid must not be empty")
    if len(values) > config.max_spot_points:
        raise ValueError(
            f"spot grid exceeds max_spot_points={config.max_spot_points}: {len(values)}"
        )
    normalized: list[float] = []
    for raw in values:
        value = _finite_number(raw)
        if value is None or value <= 0:
            raise ValueError("spot grid values must be positive and finite")
        normalized.append(value)
    if any(right <= left for left, right in zip(normalized, normalized[1:])):
        raise ValueError("spot grid must be strictly increasing")
    return tuple(normalized)


def _resolve_time_grid(
    requested: Iterable[float] | None,
    config: SurfaceGridConfig,
) -> tuple[float, ...]:
    values = tuple(config.default_time_offsets_minutes if requested is None else requested)
    if not values:
        raise ValueError("time grid must not be empty")
    if len(values) > config.max_time_points:
        raise ValueError(
            f"time grid exceeds max_time_points={config.max_time_points}: {len(values)}"
        )
    normalized: list[float] = []
    for raw in values:
        value = _finite_number(raw)
        if value is None or value < 0:
            raise ValueError("time offsets must be non-negative and finite")
        normalized.append(value)
    if any(right <= left for left, right in zip(normalized, normalized[1:])):
        raise ValueError("time offsets must be strictly increasing")
    return tuple(normalized)


def _coverage_quality(
    coverage: SurfaceCoverage,
    *,
    config: SurfaceGridConfig,
) -> tuple[str, tuple[str, ...]]:
    if coverage.usable_contracts == 0:
        return "unavailable", ("no_usable_contracts",)
    warnings: list[str] = []
    if coverage.usable_contracts < config.min_usable_contracts:
        warnings.append("too_few_usable_contracts")
    if coverage.ratio < config.min_coverage_ratio:
        warnings.append("low_contract_coverage")
    return ("degraded" if warnings else "ok"), tuple(warnings)


def _normalized_weight(value: Any, *, config: SurfaceGridConfig) -> float | None:
    weight = _finite_number(value)
    if weight is None or not 0.0 <= weight <= config.max_weight:
        return None
    return weight


def _prepared_contracts(
    contracts: tuple[SurfaceContract, ...],
    *,
    expiry: str,
    config: SurfaceGridConfig,
) -> tuple[
    tuple[_PreparedContract, ...],
    Mapping[str, SurfaceCoverage],
    Mapping[str, str],
    Mapping[str, tuple[str, ...]],
]:
    prepared: list[_PreparedContract] = []
    usable_counts = {weighting: 0 for weighting in WEIGHTINGS}
    for contract in contracts:
        strike = _finite_number(contract.strike)
        iv = _finite_number(contract.iv)
        right = _normalized_right(contract.right)
        if (
            str(contract.expiry).strip() != expiry
            or strike is None
            or strike <= 0
            or iv is None
            or not config.min_iv <= iv <= config.max_iv
            or right is None
        ):
            continue

        oi_weight = _normalized_weight(contract.open_interest, config=config)
        volume_weight = _normalized_weight(contract.volume, config=config)
        if oi_weight is not None:
            usable_counts["oi_weighted"] += 1
        if volume_weight is not None:
            usable_counts["volume_weighted"] += 1
        if oi_weight is None and volume_weight is None:
            continue
        prepared.append(
            _PreparedContract(
                strike=strike,
                right=right,
                iv=iv,
                oi_weight=oi_weight,
                volume_weight=volume_weight,
            )
        )

    total = len(contracts)
    coverages: dict[str, SurfaceCoverage] = {}
    qualities: dict[str, str] = {}
    all_warnings: dict[str, tuple[str, ...]] = {}
    for weighting in WEIGHTINGS:
        usable = usable_counts[weighting]
        coverage = SurfaceCoverage(
            total_contracts=total,
            usable_contracts=usable,
            ratio=usable / total if total else 0.0,
        )
        quality, warnings = _coverage_quality(coverage, config=config)
        coverages[weighting] = coverage
        qualities[weighting] = quality
        all_warnings[weighting] = warnings
    return tuple(prepared), coverages, qualities, all_warnings


def _observed_strike_legs(
    contracts: tuple[SurfaceContract, ...],
    *,
    expiry: str,
    config: SurfaceGridConfig,
) -> Mapping[float, Mapping[str, SurfaceStrikeLeg]]:
    observed: dict[float, dict[str, SurfaceStrikeLeg]] = {}
    for contract in contracts:
        strike = _finite_number(contract.strike)
        right = _normalized_right(contract.right)
        if str(contract.expiry).strip() != expiry or strike is None or strike <= 0 or right is None:
            continue
        iv = _finite_number(contract.iv)
        if iv is None or not config.min_iv <= iv <= config.max_iv:
            iv = None
        leg = SurfaceStrikeLeg(
            iv=iv,
            open_interest=_normalized_weight(contract.open_interest, config=config),
            volume=_normalized_weight(contract.volume, config=config),
        )
        side = "call" if right == "C" else "put"
        existing = observed.setdefault(strike, {}).get(side)
        if existing is None:
            observed[strike][side] = leg
            continue
        existing_score = sum(
            value is not None for value in (existing.iv, existing.open_interest, existing.volume)
        )
        candidate_score = sum(
            value is not None for value in (leg.iv, leg.open_interest, leg.volume)
        )
        if candidate_score > existing_score:
            observed[strike][side] = leg
    return observed


def _finite_sum(values: Iterable[float]) -> float | None:
    rows = tuple(values)
    if not rows or any(not math.isfinite(value) for value in rows):
        return None
    try:
        total = math.fsum(rows)
    except (OverflowError, ValueError):
        return None
    return total if math.isfinite(total) else None


def _contract_exposure_bases(
    contract: _PreparedContract,
    *,
    spot: float,
    tau_years: float,
) -> tuple[float, float, float] | None:
    try:
        gamma_value = bs_gamma(spot, contract.strike, contract.iv, tau_years)
        charm_value = bs_charm_per_minute(
            spot,
            contract.strike,
            contract.iv,
            tau_years,
        )
        vanna_value = bs_vanna_per_vol_point(
            spot,
            contract.strike,
            contract.iv,
            tau_years,
        )
        greek_values = (gamma_value, charm_value, vanna_value)
        if any(value is None or not math.isfinite(value) for value in greek_values):
            return None
        gamma_base = gamma_value * 100.0 * spot * spot * 0.01
        charm_base = charm_value * 100.0 * spot * 0.01
        vanna_base = vanna_value * 100.0 * spot * 0.01
    except (ArithmeticError, OverflowError, ValueError):
        return None
    if not all(math.isfinite(value) for value in (gamma_base, charm_base, vanna_base)):
        return None
    return gamma_base, charm_base, vanna_base


def _surface_metrics_scalar(
    contracts: tuple[_PreparedContract, ...],
    *,
    spots: tuple[float, ...],
    tau_seconds: float,
    coverages: Mapping[str, SurfaceCoverage],
    reference_spot: float,
    reference_tau_seconds: float,
    reference_cache: dict[_PreparedContract, tuple[float, float, float] | None],
) -> tuple[Mapping[str, SurfaceMetrics], Mapping[str, bool]]:
    tau_years = tau_seconds / YEAR_SECONDS
    rows: dict[str, dict[str, list[float | None]]] = {
        weighting: {
            "signed_gamma": [],
            "gross_gamma": [],
            "charm": [],
            "vanna": [],
        }
        for weighting in WEIGHTINGS
    }
    calculation_failed = {weighting: False for weighting in WEIGHTINGS}

    for spot in spots:
        terms: dict[str, dict[str, list[float]]] = {
            weighting: {
                "signed_gamma": [],
                "gross_gamma": [],
                "charm": [],
                "vanna": [],
            }
            for weighting in WEIGHTINGS
        }
        failed_weightings: set[str] = set()
        for contract in contracts:
            active_weights = {
                weighting: weight
                for weighting in WEIGHTINGS
                if (weight := contract.weight(weighting)) is not None and weight > 0.0
            }
            if not active_weights:
                continue

            sign = 1.0 if contract.right == "C" else -1.0
            is_reference_cell = spot == reference_spot and tau_seconds == reference_tau_seconds
            if is_reference_cell and contract in reference_cache:
                bases = reference_cache[contract]
            else:
                bases = _contract_exposure_bases(
                    contract,
                    spot=spot,
                    tau_years=tau_years,
                )
                if is_reference_cell:
                    reference_cache[contract] = bases
            if bases is None:
                failed_weightings.update(active_weights)
                continue
            gamma_base, charm_base, vanna_base = bases

            for weighting, weight in active_weights.items():
                weighted_values = {
                    "signed_gamma": sign * gamma_base * weight,
                    "gross_gamma": abs(gamma_base * weight),
                    "charm": sign * charm_base * weight,
                    "vanna": sign * vanna_base * weight,
                }
                if any(not math.isfinite(value) for value in weighted_values.values()):
                    failed_weightings.add(weighting)
                    continue
                for metric, value in weighted_values.items():
                    terms[weighting][metric].append(value)

        for weighting in WEIGHTINGS:
            target = rows[weighting]
            if coverages[weighting].usable_contracts == 0:
                for metric in target:
                    target[metric].append(None)
                continue
            if weighting in failed_weightings:
                calculation_failed[weighting] = True
                for metric in target:
                    target[metric].append(None)
                continue

            cell_values = {
                metric: (_finite_sum(values) if values else 0.0)
                for metric, values in terms[weighting].items()
            }
            if any(value is None for value in cell_values.values()):
                calculation_failed[weighting] = True
                for metric in target:
                    target[metric].append(None)
                continue
            for metric, value in cell_values.items():
                target[metric].append(value)

    metrics = {
        weighting: SurfaceMetrics(
            signed_gamma=tuple(values["signed_gamma"]),
            gross_gamma=tuple(values["gross_gamma"]),
            charm=tuple(values["charm"]),
            vanna=tuple(values["vanna"]),
        )
        for weighting, values in rows.items()
    }
    return metrics, calculation_failed


def _surface_metrics_vectorized(
    contracts: tuple[_PreparedContract, ...],
    *,
    spots: tuple[float, ...],
    tau_seconds: float,
    coverages: Mapping[str, SurfaceCoverage],
    reference_spot: float,
    reference_tau_seconds: float,
    reference_cache: dict[_PreparedContract, tuple[float, float, float] | None],
) -> tuple[Mapping[str, SurfaceMetrics], Mapping[str, bool]]:
    """Dispatch the replay-only optimized kernel after the expiry guard."""

    if tau_seconds <= 0.0:
        return _surface_metrics_scalar(
            contracts,
            spots=spots,
            tau_seconds=tau_seconds,
            coverages=coverages,
            reference_spot=reference_spot,
            reference_tau_seconds=reference_tau_seconds,
            reference_cache=reference_cache,
        )
    return _surface_metrics_vectorized_impl(
        contracts,
        spots=spots,
        tau_seconds=tau_seconds,
        coverages=coverages,
        reference_spot=reference_spot,
        reference_tau_seconds=reference_tau_seconds,
        reference_cache=reference_cache,
    )


def _nearest_zero_ridge(
    spots: tuple[float, ...],
    values: tuple[float | None, ...],
    *,
    reference_spot: float,
) -> float | None:
    present = [abs(value) for value in values if value is not None]
    if not present or max(present) <= 1e-12:
        return None
    roots: list[float] = []
    for left_spot, right_spot, left_value, right_value in zip(
        spots,
        spots[1:],
        values,
        values[1:],
    ):
        if left_value is None or right_value is None:
            continue
        if abs(left_value) <= 1e-12:
            roots.append(left_spot)
        elif left_value * right_value < 0:
            fraction = -left_value / (right_value - left_value)
            roots.append(left_spot + fraction * (right_spot - left_spot))
    if values[-1] is not None and abs(values[-1]) <= 1e-12:
        roots.append(spots[-1])
    if not roots:
        return None
    return min(roots, key=lambda value: abs(value - reference_spot))


def _global_extremum(
    spots: tuple[float, ...],
    values: tuple[float | None, ...],
    *,
    positive: bool,
) -> SurfaceExtremum | None:
    rows = [
        (spot, value)
        for spot, value in zip(spots, values)
        if value is not None and (value > 0 if positive else value < 0)
    ]
    if not rows:
        return None
    spot, value = (max(rows, key=lambda row: row[1]) if positive else min(rows, key=lambda row: row[1]))
    return SurfaceExtremum(spot=spot, value=value)


def _empty_weighting_slice(
    *,
    size: int,
    total_contracts: int,
    warning: str,
) -> SurfaceWeightingSlice:
    return SurfaceWeightingSlice(
        metrics=SurfaceMetrics.empty(size),
        zero_ridge_spot=None,
        positive_peak=None,
        negative_trough=None,
        coverage=SurfaceCoverage(
            total_contracts=total_contracts,
            usable_contracts=0,
            ratio=0.0,
        ),
        quality="unavailable",
        warnings=(warning,),
    )


def _combined_quality(qualities: Iterable[str]) -> str:
    values = tuple(qualities)
    if not values or all(value == "unavailable" for value in values):
        return "unavailable"
    if all(value == "ok" for value in values):
        return "ok"
    return "degraded"


def _deduplicated_warnings(*groups: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    warnings: list[str] = []
    for group in groups:
        for warning in group:
            if warning not in seen:
                seen.add(warning)
                warnings.append(warning)
    return tuple(warnings)


def _expiry_validation_warning(expiry: str, expiry_close: datetime) -> str | None:
    if len(expiry) != 8 or not expiry.isdigit():
        return "invalid_expiry"
    try:
        expiry_date = datetime.strptime(expiry, "%Y%m%d").date()
    except ValueError:
        return "invalid_expiry"
    if expiry_close.astimezone(EXPIRY_TIMEZONE).date() != expiry_date:
        return "expiry_close_mismatch"
    return None


def _quality_after_calculation(
    base_quality: str,
    metrics: SurfaceMetrics,
    *,
    calculation_failed: bool,
) -> str:
    if not calculation_failed:
        return base_quality
    has_value = any(
        value is not None
        for values in metrics.to_dict().values()
        for value in values
    )
    return "degraded" if has_value else "unavailable"


def _strike_weighting(
    contracts: tuple[_PreparedContract, ...],
    *,
    observed_sides: set[str],
    weighting: str,
    spot: float,
    tau_seconds: float,
    reference_cache: dict[_PreparedContract, tuple[float, float, float] | None],
) -> SurfaceStrikeWeighting:
    weighted = tuple(contract for contract in contracts if contract.weight(weighting) is not None)
    usable_sides = {contract.right for contract in weighted}
    warnings: list[str] = []
    if len(observed_sides) < 2:
        warnings.append("unpaired_strike")
    if usable_sides != observed_sides:
        warnings.append("incomplete_strike_coverage")
    if not weighted:
        warnings.append("no_usable_contracts")
        return SurfaceStrikeWeighting(
            metrics=SurfaceMetricPoint.empty(),
            quality="unavailable",
            warnings=_deduplicated_warnings(warnings),
        )

    terms = {
        "signed_gamma": [],
        "gross_gamma": [],
        "charm": [],
        "vanna": [],
    }
    tau_years = tau_seconds / YEAR_SECONDS
    for contract in weighted:
        weight = contract.weight(weighting)
        if weight is None or weight == 0.0:
            continue
        if contract in reference_cache:
            bases = reference_cache[contract]
        else:
            bases = _contract_exposure_bases(
                contract,
                spot=spot,
                tau_years=tau_years,
            )
            reference_cache[contract] = bases
        if bases is None:
            warnings.append("calculation_failed")
            return SurfaceStrikeWeighting(
                metrics=SurfaceMetricPoint.empty(),
                quality="unavailable",
                warnings=_deduplicated_warnings(warnings),
            )
        gamma_base, charm_base, vanna_base = bases
        sign = 1.0 if contract.right == "C" else -1.0
        weighted_values = {
            "signed_gamma": sign * gamma_base * weight,
            "gross_gamma": abs(gamma_base * weight),
            "charm": sign * charm_base * weight,
            "vanna": sign * vanna_base * weight,
        }
        if any(not math.isfinite(value) for value in weighted_values.values()):
            warnings.append("calculation_failed")
            return SurfaceStrikeWeighting(
                metrics=SurfaceMetricPoint.empty(),
                quality="unavailable",
                warnings=_deduplicated_warnings(warnings),
            )
        for metric, value in weighted_values.items():
            terms[metric].append(value)

    values = {
        metric: (_finite_sum(items) if items else 0.0)
        for metric, items in terms.items()
    }
    if any(value is None for value in values.values()):
        warnings.append("calculation_failed")
        return SurfaceStrikeWeighting(
            metrics=SurfaceMetricPoint.empty(),
            quality="unavailable",
            warnings=_deduplicated_warnings(warnings),
        )
    return SurfaceStrikeWeighting(
        metrics=SurfaceMetricPoint(
            signed_gamma=values["signed_gamma"],
            gross_gamma=values["gross_gamma"],
            charm=values["charm"],
            vanna=values["vanna"],
        ),
        quality="degraded" if warnings else "ok",
        warnings=_deduplicated_warnings(warnings),
    )


def _build_strike_ladder(
    observed: Mapping[float, Mapping[str, SurfaceStrikeLeg]],
    contracts: tuple[_PreparedContract, ...],
    *,
    spot: float,
    tau_seconds: float,
    reference_cache: dict[_PreparedContract, tuple[float, float, float] | None],
    unavailable_warning: str | None = None,
) -> tuple[SurfaceStrikeRow, ...]:
    by_strike: dict[float, list[_PreparedContract]] = {}
    for contract in contracts:
        by_strike.setdefault(contract.strike, []).append(contract)

    rows: list[SurfaceStrikeRow] = []
    for strike, legs in sorted(observed.items()):
        if unavailable_warning is not None:
            weightings = {
                weighting: SurfaceStrikeWeighting(
                    metrics=SurfaceMetricPoint.empty(),
                    quality="unavailable",
                    warnings=(unavailable_warning,),
                )
                for weighting in WEIGHTINGS
            }
        else:
            observed_sides = {"C" if side == "call" else "P" for side in legs}
            strike_contracts = tuple(by_strike.get(strike, ()))
            weightings = {
                weighting: _strike_weighting(
                    strike_contracts,
                    observed_sides=observed_sides,
                    weighting=weighting,
                    spot=spot,
                    tau_seconds=tau_seconds,
                    reference_cache=reference_cache,
                )
                for weighting in WEIGHTINGS
            }
        quality = _combined_quality(row.quality for row in weightings.values())
        warnings = _deduplicated_warnings(*(row.warnings for row in weightings.values()))
        rows.append(
            SurfaceStrikeRow(
                strike=strike,
                call=legs.get("call"),
                put=legs.get("put"),
                weightings=weightings,
                quality=quality,
                warnings=warnings,
            )
        )
    return tuple(rows)


def _unavailable_time_slice(
    *,
    minutes_forward: float,
    tau_seconds: float,
    spot_count: int,
    contract_count: int,
    warning: str,
) -> SurfaceTimeSlice:
    weightings = {
        weighting: _empty_weighting_slice(
            size=spot_count,
            total_contracts=contract_count,
            warning=warning,
        )
        for weighting in WEIGHTINGS
    }
    return SurfaceTimeSlice(
        minutes_forward=minutes_forward,
        tau_seconds=max(tau_seconds, 0.0),
        weightings=weightings,
        quality="unavailable",
        warnings=(warning,),
    )


def build_exposure_surface(
    contracts: Iterable[SurfaceContract],
    *,
    spot: float,
    as_of: datetime,
    expiry_close: datetime,
    spot_points: Iterable[float] | None = None,
    time_offsets_minutes: Iterable[float] | None = None,
    config: SurfaceGridConfig | None = None,
    _calculation_engine: str = SCALAR_CALCULATION_ENGINE,
) -> ExposureSurface:
    """Build a bounded time-by-spot surface for one exact SPXW expiry.

    ``signed_gamma`` uses the project's GEX scale (contract gamma x weight x
    100 multiplier x spot squared x 1%). ``charm`` and ``vanna`` use the same
    delta-dollar proxy scale (Greek x weight x 100 x spot x 1%).
    """

    if _calculation_engine not in {
        SCALAR_CALCULATION_ENGINE,
        VECTORIZED_CALCULATION_ENGINE,
    }:
        raise ValueError("unsupported exposure-surface calculation engine")
    metrics_builder = (
        _surface_metrics_vectorized
        if _calculation_engine == VECTORIZED_CALCULATION_ENGINE
        else _surface_metrics_scalar
    )
    resolved_config = config or SurfaceGridConfig()
    if not _aware(as_of) or not _aware(expiry_close):
        raise ValueError("as_of and expiry_close must be timezone-aware")
    reference_spot = _finite_number(spot)
    if reference_spot is None or reference_spot <= 0:
        raise ValueError("spot must be positive and finite")

    rows = tuple(contracts)
    if len(rows) > resolved_config.max_contracts:
        raise ValueError(
            f"contract count exceeds max_contracts={resolved_config.max_contracts}: {len(rows)}"
        )
    spot_grid = _resolve_spot_grid(reference_spot, spot_points, resolved_config)
    time_grid = _resolve_time_grid(time_offsets_minutes, resolved_config)
    cell_count = len(spot_grid) * len(time_grid)
    if cell_count > resolved_config.max_cells:
        raise ValueError(
            f"surface grid exceeds max_cells={resolved_config.max_cells}: {cell_count}"
        )
    reference_cell_in_surface = 0.0 in time_grid and reference_spot in spot_grid
    evaluated_cells = cell_count + (0 if reference_cell_in_surface else 1)
    evaluations = len(rows) * evaluated_cells * GREEK_KERNELS_PER_CONTRACT_CELL
    if evaluations > resolved_config.max_contract_cell_evaluations:
        raise ValueError(
            "surface Greek evaluation count exceeds "
            f"max_contract_cell_evaluations={resolved_config.max_contract_cell_evaluations}: "
            f"{evaluations}"
        )

    expiries = {str(row.expiry).strip() for row in rows if str(row.expiry).strip()}
    expiry = next(iter(expiries)) if len(expiries) == 1 else ""
    top_warnings: list[str] = []
    blocked_warning: str | None = None
    if not rows:
        blocked_warning = "no_contracts"
    elif len(expiries) != 1:
        blocked_warning = "mixed_expiry_contracts"
    else:
        blocked_warning = _expiry_validation_warning(expiry, expiry_close)
    if blocked_warning is not None:
        top_warnings.append(blocked_warning)

    prepared: tuple[_PreparedContract, ...] = ()
    coverages: Mapping[str, SurfaceCoverage] = {}
    weighting_qualities: Mapping[str, str] = {}
    weighting_warnings: Mapping[str, tuple[str, ...]] = {}
    observed: Mapping[float, Mapping[str, SurfaceStrikeLeg]] = {}
    if expiry:
        observed = _observed_strike_legs(
            rows,
            expiry=expiry,
            config=resolved_config,
        )
    if blocked_warning is None:
        prepared, coverages, weighting_qualities, weighting_warnings = _prepared_contracts(
            rows,
            expiry=expiry,
            config=resolved_config,
        )

    reference_tau_seconds = (expiry_close - as_of).total_seconds()
    reference_cache: dict[_PreparedContract, tuple[float, float, float] | None] = {}
    time_slices: list[SurfaceTimeSlice] = []
    for minutes_forward in time_grid:
        scenario_at = as_of + timedelta(minutes=minutes_forward)
        tau_seconds = (expiry_close - scenario_at).total_seconds()
        if blocked_warning is not None:
            time_slices.append(
                _unavailable_time_slice(
                    minutes_forward=minutes_forward,
                    tau_seconds=tau_seconds,
                    spot_count=len(spot_grid),
                    contract_count=len(rows),
                    warning=blocked_warning,
                )
            )
            continue
        if tau_seconds <= resolved_config.min_tau_seconds:
            warning = "near_expiry_under_min_tau"
            if minutes_forward == 0.0 and warning not in top_warnings:
                top_warnings.append(warning)
            time_slices.append(
                _unavailable_time_slice(
                    minutes_forward=minutes_forward,
                    tau_seconds=tau_seconds,
                    spot_count=len(spot_grid),
                    contract_count=len(rows),
                    warning=warning,
                )
            )
            continue

        metrics_by_weighting, calculation_failed = metrics_builder(
            prepared,
            spots=spot_grid,
            tau_seconds=tau_seconds,
            coverages=coverages,
            reference_spot=reference_spot,
            reference_tau_seconds=reference_tau_seconds,
            reference_cache=reference_cache,
        )
        weighting_slices: dict[str, SurfaceWeightingSlice] = {}
        for weighting in WEIGHTINGS:
            metrics = metrics_by_weighting[weighting]
            failed = calculation_failed[weighting]
            warnings = _deduplicated_warnings(
                weighting_warnings[weighting],
                ("calculation_failed",) if failed else (),
            )
            quality = _quality_after_calculation(
                weighting_qualities[weighting],
                metrics,
                calculation_failed=failed,
            )
            weighting_slices[weighting] = SurfaceWeightingSlice(
                metrics=metrics,
                zero_ridge_spot=_nearest_zero_ridge(
                    spot_grid,
                    metrics.signed_gamma,
                    reference_spot=reference_spot,
                ),
                positive_peak=_global_extremum(
                    spot_grid,
                    metrics.signed_gamma,
                    positive=True,
                ),
                negative_trough=_global_extremum(
                    spot_grid,
                    metrics.signed_gamma,
                    positive=False,
                ),
                coverage=coverages[weighting],
                quality=quality,
                warnings=warnings,
            )
        slice_quality = _combined_quality(row.quality for row in weighting_slices.values())
        slice_warnings = _deduplicated_warnings(
            *(row.warnings for row in weighting_slices.values())
        )
        time_slices.append(
            SurfaceTimeSlice(
                minutes_forward=minutes_forward,
                tau_seconds=tau_seconds,
                weightings=weighting_slices,
                quality=slice_quality,
                warnings=slice_warnings,
            )
        )

    strike_ladder_warning = blocked_warning
    if strike_ladder_warning is None and reference_tau_seconds <= resolved_config.min_tau_seconds:
        strike_ladder_warning = "near_expiry_under_min_tau"
    strike_ladder = _build_strike_ladder(
        observed,
        prepared,
        spot=reference_spot,
        tau_seconds=reference_tau_seconds,
        reference_cache=reference_cache,
        unavailable_warning=strike_ladder_warning,
    )

    surface_quality = _combined_quality(row.quality for row in time_slices)
    top_warnings = list(
        _deduplicated_warnings(
            top_warnings,
            *(row.warnings for row in time_slices),
            *(row.warnings for row in strike_ladder),
        )
    )
    return ExposureSurface(
        created_at=datetime.now(tz=timezone.utc),
        as_of=as_of,
        expiry=expiry,
        expiry_close=expiry_close,
        reference_spot=reference_spot,
        spot_grid=spot_grid,
        time_offsets_minutes=time_grid,
        contract_count=len(rows),
        time_slices=tuple(time_slices),
        strike_ladder=strike_ladder,
        quality=surface_quality,
        warnings=tuple(top_warnings),
    )


__all__ = (
    "DEALER_POSITION_SIGN",
    "ExposureSurface",
    "METRIC_UNITS",
    "MODEL",
    "SCHEMA_VERSION",
    "SCALAR_CALCULATION_ENGINE",
    "SIGN_CONVENTION",
    "STRIKE_LADDER_BASIS",
    "SurfaceContract",
    "SurfaceCoverage",
    "SurfaceExtremum",
    "SurfaceGridConfig",
    "SurfaceMetricPoint",
    "SurfaceMetrics",
    "SurfaceStrikeLeg",
    "SurfaceStrikeRow",
    "SurfaceStrikeWeighting",
    "SurfaceTimeSlice",
    "SurfaceWeightingSlice",
    "VECTORIZED_CALCULATION_ENGINE",
    "WEIGHTING_SEMANTICS",
    "build_exposure_surface",
)
