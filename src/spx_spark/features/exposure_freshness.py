"""Freshness and source-quality summaries for exposure-map inputs."""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any

import numpy as np

from spx_spark.analytics.options.quote_policy import ANALYTICAL_CORE_LANE
from spx_spark.features.exposure_schema import ExposureInputRow
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET

def tau_is_floored(expiry: str, as_of: datetime) -> bool:
    """Compatibility name: true only when no positive expiry time remains."""

    expiry_date = datetime.strptime(expiry, "%Y%m%d").date()
    session = DEFAULT_MARKET_CALENDAR.session(expiry_date)
    if session is None:
        return True
    delta_seconds = (session.close_at - as_of.astimezone(session.close_at.tzinfo)).total_seconds()
    if delta_seconds <= 0:
        return True
    return False


def determine_oi_quality(rows: tuple[ExposureInputRow, ...]) -> str:
    if not rows:
        return "missing"
    positive = [row for row in rows if row.open_interest > 0]
    if not positive:
        return "stale_or_zero"
    providers = {str(row.open_interest_provider or "").lower() for row in positive}
    if providers == {"ibkr"}:
        return "ibkr_ok"
    if "schwab" in providers:
        return "schwab_unverified"
    return "unverified_provider"


def determine_iv_source(rows: tuple[ExposureInputRow, ...]) -> str:
    if not rows:
        return "missing"
    accepted = [row for row in rows if row.analytical_allowed]
    with_iv = [row for row in accepted if row.iv is not None]
    if not accepted or len(with_iv) / len(accepted) < 0.5:
        return "missing"
    providers = Counter((row.greeks_provider or row.provider) for row in with_iv)
    if len(providers) > 1:
        return "mixed"
    dominant = providers.most_common(1)[0][0]
    if dominant == "schwab":
        return "vendor_schwab"
    return "vendor_ibkr"


def snapshot_age_seconds(rows: tuple[ExposureInputRow, ...]) -> float | None:
    analytical = [
        row for row in rows if row.analytical_allowed and row.structure_age_seconds is not None
    ]
    core_ages = [
        row.structure_age_seconds
        for row in analytical
        if row.greeks_lane == ANALYTICAL_CORE_LANE and row.structure_age_seconds is not None
    ]
    ages = core_ages or [
        row.structure_age_seconds for row in analytical if row.structure_age_seconds is not None
    ]
    if not ages:
        return None
    return max(ages)


def freshness_summary(rows: tuple[ExposureInputRow, ...]) -> dict[str, Any]:
    usable = [row for row in rows if row.analytical_allowed]
    core = [row for row in usable if row.greeks_lane == ANALYTICAL_CORE_LANE]
    rotation = [row for row in usable if row.greeks_lane != ANALYTICAL_CORE_LANE]

    def ages_for(
        selected: list[ExposureInputRow],
        *,
        field: str = "greeks",
    ) -> list[float]:
        return [
            float(age)
            for row in selected
            if (
                age := (
                    row.open_interest_observation_age_seconds
                    if field == "open_interest"
                    else row.structure_age_seconds
                )
            )
            is not None
        ]

    def age_stats(
        selected: list[ExposureInputRow],
        *,
        field: str = "greeks",
    ) -> dict[str, float | None]:
        ages = ages_for(selected, field=field)
        return {
            "p50_seconds": float(np.percentile(ages, 50)) if ages else None,
            "p90_seconds": float(np.percentile(ages, 90)) if ages else None,
            "max_seconds": max(ages) if ages else None,
        }

    rejections = Counter(
        row.analytical_reason or "analytical_input_unavailable"
        for row in rows
        if not row.analytical_allowed
    )
    with_oi = [row for row in rows if row.open_interest > 0]
    oi_core = [row for row in with_oi if row.open_interest_lane == ANALYTICAL_CORE_LANE]
    oi_rotation = [row for row in with_oi if row.open_interest_lane != ANALYTICAL_CORE_LANE]
    return {
        "contract_count": len(rows),
        "analytical_contract_count": len(usable),
        "core_contract_count": len(core),
        "rotation_contract_count": len(rotation),
        "pricing_provider_counts": dict(
            sorted(Counter(row.pricing_provider for row in usable).items())
        ),
        "greeks_provider_counts": dict(
            sorted(Counter(row.greeks_provider or "unknown" for row in usable).items())
        ),
        "open_interest_provider_counts": dict(
            sorted(Counter(row.open_interest_provider for row in with_oi).items())
        ),
        "all": age_stats(usable),
        "core": age_stats(core),
        "rotation": age_stats(rotation),
        "open_interest": {
            "leg_count": len(with_oi),
            "core_leg_count": len(oi_core),
            "rotation_leg_count": len(oi_rotation),
            "all": age_stats(with_oi, field="open_interest"),
            "core": age_stats(oi_core, field="open_interest"),
            "rotation": age_stats(oi_rotation, field="open_interest"),
        },
        "rejection_counts": dict(sorted(rejections.items())),
        "nbbo_interpolated": False,
        "clock_contract": "as_of_ge_observed_at_field_specific",
    }


def early_session(as_of: datetime) -> bool:
    session = DEFAULT_MARKET_CALENDAR.session(as_of.astimezone(ET).date())
    if session is None:
        return False
    elapsed = (as_of.astimezone(ET) - session.open_at).total_seconds()
    return 0 <= elapsed <= 30 * 60
