"""Shared statistical primitives used by live analytics and reports."""

from __future__ import annotations

from statistics import NormalDist

import numpy as np


def percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    return float(np.quantile(np.asarray(values, dtype=float), quantile, method="linear"))


def wilson_score_interval(
    successes: int,
    total: int,
    *,
    confidence: float = 0.95,
) -> tuple[float, float]:
    if total <= 0:
        return 0.0, 0.0
    z = NormalDist().inv_cdf((1.0 + confidence) / 2.0)
    proportion = successes / total
    denominator = 1.0 + z * z / total
    center = (proportion + z * z / (2.0 * total)) / denominator
    margin = (
        z
        * ((proportion * (1.0 - proportion) / total + z * z / (4.0 * total * total)) ** 0.5)
        / denominator
    )
    return max(0.0, center - margin), min(1.0, center + margin)
