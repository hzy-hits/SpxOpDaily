"""Vectorized Greek cells for bounded exposure surfaces."""

from __future__ import annotations

import math
from collections.abc import Mapping

import numpy as np

from spx_spark.features.exposure_surface_models import (
    WEIGHTINGS,
    YEAR_SECONDS,
    SurfaceCoverage,
    SurfaceMetrics,
    _PreparedContract,
)


def surface_metrics_vectorized(
    contracts: tuple[_PreparedContract, ...],
    *,
    spots: tuple[float, ...],
    tau_seconds: float,
    coverages: Mapping[str, SurfaceCoverage],
    reference_spot: float,
    reference_tau_seconds: float,
    reference_cache: dict[_PreparedContract, tuple[float, float, float] | None],
) -> tuple[Mapping[str, SurfaceMetrics], Mapping[str, bool]]:
    """Evaluate one positive-tau time slice in bounded vector batches."""

    spot_values = np.asarray(spots, dtype=np.float64)[:, np.newaxis]
    strike_values = np.asarray([row.strike for row in contracts], dtype=np.float64)[np.newaxis, :]
    iv_values = np.asarray([row.iv for row in contracts], dtype=np.float64)[np.newaxis, :]
    sign_values = np.asarray(
        [1.0 if row.right == "C" else -1.0 for row in contracts],
        dtype=np.float64,
    )[np.newaxis, :]
    tau_years = tau_seconds / YEAR_SECONDS
    sqrt_tau = math.sqrt(tau_years)

    with np.errstate(divide="ignore", invalid="ignore", over="ignore", under="ignore"):
        d1_values = (
            np.log(spot_values / strike_values) + 0.5 * iv_values * iv_values * tau_years
        ) / (iv_values * sqrt_tau)
        normal_pdf_values = np.exp(-0.5 * d1_values * d1_values) / math.sqrt(2.0 * math.pi)
        d2_values = d1_values - iv_values * sqrt_tau
        gamma_values = normal_pdf_values / (spot_values * iv_values * sqrt_tau)
        charm_values = (normal_pdf_values * d2_values / (2.0 * tau_years)) / 525_600.0
        vanna_values = (-normal_pdf_values * d2_values / iv_values) * 0.01
        gamma_bases = gamma_values * 100.0 * spot_values * spot_values * 0.01
        charm_bases = charm_values * 100.0 * spot_values * 0.01
        vanna_bases = vanna_values * 100.0 * spot_values * 0.01

    finite_bases = np.isfinite(gamma_bases) & np.isfinite(charm_bases) & np.isfinite(vanna_bases)
    safe_gamma = np.where(finite_bases, gamma_bases, 0.0)
    safe_charm = np.where(finite_bases, charm_bases, 0.0)
    safe_vanna = np.where(finite_bases, vanna_bases, 0.0)

    # Preserve the reference-cell memo used by the scalar strike ladder.
    if tau_seconds == reference_tau_seconds and reference_spot in spots:
        reference_index = spots.index(reference_spot)
        for contract_index, contract in enumerate(contracts):
            if not any(
                (weight := contract.weight(weighting)) is not None and weight > 0.0
                for weighting in WEIGHTINGS
            ):
                continue
            if not bool(finite_bases[reference_index, contract_index]):
                reference_cache[contract] = None
                continue
            reference_cache[contract] = (
                float(gamma_bases[reference_index, contract_index]),
                float(charm_bases[reference_index, contract_index]),
                float(vanna_bases[reference_index, contract_index]),
            )

    metrics_by_weighting: dict[str, SurfaceMetrics] = {}
    calculation_failed: dict[str, bool] = {}
    for weighting in WEIGHTINGS:
        coverage = coverages[weighting]
        if coverage.usable_contracts == 0:
            metrics_by_weighting[weighting] = SurfaceMetrics.empty(len(spots))
            calculation_failed[weighting] = False
            continue

        raw_weights = tuple(contract.weight(weighting) for contract in contracts)
        active = np.asarray(
            [weight is not None and weight > 0.0 for weight in raw_weights],
            dtype=np.bool_,
        )
        weights = np.asarray(
            [
                float(weight) if is_active and weight is not None else 0.0
                for weight, is_active in zip(raw_weights, active, strict=True)
            ],
            dtype=np.float64,
        )[np.newaxis, :]
        failed_rows = (
            np.any(~finite_bases[:, active], axis=1)
            if np.any(active)
            else np.zeros(len(spots), dtype=np.bool_)
        )

        with np.errstate(invalid="ignore", over="ignore"):
            terms = {
                "signed_gamma": safe_gamma * sign_values * weights,
                "gross_gamma": np.abs(safe_gamma * weights),
                "charm": safe_charm * sign_values * weights,
                "vanna": safe_vanna * sign_values * weights,
            }
        for values in terms.values():
            if np.any(active):
                failed_rows |= np.any(~np.isfinite(values[:, active]), axis=1)
        finite_terms = {
            metric: np.where(np.isfinite(values), values, 0.0) for metric, values in terms.items()
        }
        sums = {
            "signed_gamma": _stable_rows(finite_terms["signed_gamma"]),
            "gross_gamma": np.sum(finite_terms["gross_gamma"], axis=1),
            "charm": _stable_rows(finite_terms["charm"]),
            "vanna": _stable_rows(finite_terms["vanna"]),
        }
        for values in sums.values():
            failed_rows |= ~np.isfinite(values)

        def output(metric: str) -> tuple[float | None, ...]:
            return tuple(
                None if bool(failed) else float(value)
                for value, failed in zip(sums[metric], failed_rows, strict=True)
            )

        metrics_by_weighting[weighting] = SurfaceMetrics(
            signed_gamma=output("signed_gamma"),
            gross_gamma=output("gross_gamma"),
            charm=output("charm"),
            vanna=output("vanna"),
        )
        calculation_failed[weighting] = bool(np.any(failed_rows))

    return metrics_by_weighting, calculation_failed


def _stable_rows(values: np.ndarray) -> np.ndarray:
    """Sum signed rows without manufacturing residual exposure near zero."""

    if np.finfo(np.longdouble).nmant > np.finfo(np.float64).nmant:
        with np.errstate(invalid="ignore", over="ignore"):
            extended = np.sum(values, axis=1, dtype=np.longdouble)
            absolute = np.sum(np.abs(values), axis=1, dtype=np.longdouble)
        term_count = values.shape[1]
        unit_roundoff = np.longdouble(np.finfo(np.longdouble).eps / 2.0)
        accumulated_roundoff = np.longdouble(term_count) * unit_roundoff
        error_factor = accumulated_roundoff / (1.0 - accumulated_roundoff)
        ambiguous = np.abs(extended) <= error_factor * absolute
        for index in np.flatnonzero(ambiguous):
            try:
                extended[index] = math.fsum(values[index])
            except (OverflowError, ValueError):
                extended[index] = math.nan
        invalid = ~np.isfinite(extended) | (
            np.abs(extended) > np.longdouble(np.finfo(np.float64).max)
        )
        with np.errstate(invalid="ignore", over="ignore"):
            output = np.asarray(extended, dtype=np.float64)
        output[invalid] = math.nan
        return output

    output: list[float] = []
    for row in values:
        try:
            output.append(math.fsum(row))
        except (OverflowError, ValueError):
            output.append(math.nan)
    return np.asarray(output, dtype=np.float64)


__all__ = ("surface_metrics_vectorized",)
