from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping, Sequence
from typing import Any


def build_greek_reference_payload(
    *,
    schema_version: str,
    model_name: str,
    as_of: Any,
    expiry: str,
    spot: float,
    spot_source: str | None,
    spot_warnings: tuple[str, ...],
    exact_quote_count: int,
    inputs_by_contract: Mapping[str, Any],
    references: Sequence[Any],
    aggregate: Any,
    front: Any,
    blocked_counts: Mapping[str, int],
    focus_contract_ids: Iterable[str],
    max_serialized_contracts: int,
    serialized_scenario_names: Iterable[str],
) -> dict[str, Any]:
    displayed = _displayed_references(
        references,
        spot=spot,
        focus_contract_ids=focus_contract_ids,
        limit=max_serialized_contracts,
    )
    quality_counts, reason_counts = _quality_counts(references)
    universe_ids = sorted(inputs_by_contract)
    fingerprint = _universe_fingerprint(universe_ids, inputs_by_contract)
    degraded = (
        aggregate.quality != "ok"
        or quality_counts["ok"] / len(references) < 0.60
        or bool(spot_warnings)
    )
    return {
        "schema_version": schema_version,
        "kind": "snapshot",
        "mode": "reference_only",
        "status": "degraded" if degraded else "ok",
        "as_of": as_of.isoformat(),
        "expiry": expiry,
        "scope": {"underlier": "SPX", "trading_class": "SPXW", "expiry": expiry, "dte": 0},
        "model": _model_payload(model_name, spot, spot_source, inputs_by_contract),
        "direction": "unknown",
        "position_sign": "unknown",
        "signed_gex_proxy": _signed_gex_proxy(front),
        "weighting": {
            "aggregate": "open_interest_only",
            "intraday_volume": "context_only_not_used",
        },
        "units": _UNITS,
        "aggregate_scope": "currently_actionable_exact_expiry_contracts_oi_only",
        "aggregate_universe": {
            "fingerprint": fingerprint,
            "contract_count": len(universe_ids),
        },
        "aggregate": aggregate.to_dict(),
        "coverage": {
            "exact_expiry_contract_count": exact_quote_count,
            "usable_contract_count": len(references),
            "usable_ratio": len(references) / exact_quote_count,
            "oi_ratio": aggregate.oi_coverage_ratio,
        },
        "quality_counts": quality_counts,
        "quality_reason_counts": reason_counts,
        "usable_contract_count": len(references),
        "serialized_contract_count": len(displayed),
        "blocked_counts": dict(blocked_counts),
        "warnings": list(spot_warnings),
        "contracts": [
            row.to_dict(scenario_names=tuple(serialized_scenario_names)) for row in displayed
        ],
    }


def _displayed_references(
    references: Sequence[Any],
    *,
    spot: float,
    focus_contract_ids: Iterable[str],
    limit: int,
) -> list[Any]:
    focus_rank = {contract_id: index for index, contract_id in enumerate(focus_contract_ids)}
    ordered = sorted(
        references,
        key=lambda row: (
            0 if row.contract_id in focus_rank else 1,
            focus_rank.get(row.contract_id, 0),
            abs(row.strike - spot),
            row.right,
        ),
    )
    return ordered[: max(limit, 0)]


def _quality_counts(references: Sequence[Any]) -> tuple[dict[str, int], dict[str, int]]:
    status_counts = {
        status: sum(1 for row in references if row.quality.status == status)
        for status in ("ok", "degraded")
    }
    reason_counts: dict[str, int] = {}
    for row in references:
        for reason in row.quality.reasons:
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
    return status_counts, reason_counts


def _universe_fingerprint(universe_ids: list[str], inputs: Mapping[str, Any]) -> str:
    tokens = [
        f"{contract_id}:{inputs[contract_id].open_interest or 0.0:.6f}"
        for contract_id in universe_ids
    ]
    return hashlib.sha256("\n".join(tokens).encode()).hexdigest()[:16]


def _model_payload(
    model_name: str,
    spot: float,
    spot_source: str | None,
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    first = inputs[next(iter(inputs))]
    return {
        "name": model_name,
        "spot": spot,
        "spot_source": spot_source,
        "minutes_to_expiry": round(first.tau_seconds / 60.0, 2),
        "time_derivative_convention": "calendar_time_forward",
        "vol_point_decimal": 0.01,
    }


def _signed_gex_proxy(front: Any) -> dict[str, Any]:
    weighting = getattr(front, "gex_weighting", "unknown")
    sign_method = (
        "call_positive_put_negative_oi_plus_volume_proxy_not_dealer_position"
        if weighting == "oi_plus_volume"
        else "call_positive_put_negative_oi_proxy_not_dealer_position"
    )
    return {
        "net_gex": getattr(front, "net_gex", None),
        "abs_gex": getattr(front, "abs_gex", None),
        "net_gamma_ratio": getattr(front, "net_gamma_ratio", None),
        "gamma_state": getattr(front, "gamma_state", "unknown"),
        "weighting": weighting,
        "sign_method": sign_method,
        "dealer_position_sign": "unknown",
        "direction": "unknown",
    }


_UNITS = {
    "delta": "delta_per_option",
    "gamma": "delta_change_per_spx_point",
    "theta": "option_points_per_calendar_minute",
    "vega": "option_points_per_1_vol_point",
    "charm": "delta_change_per_calendar_minute",
    "color": "gamma_change_per_calendar_minute",
    "speed": "gamma_change_per_spx_point",
    "vanna": "delta_change_per_1_vol_point",
    "vomma": "option_points_per_1_vol_point_squared",
    "zomma": "gamma_change_per_1_vol_point",
    "gross_multiplier": "open_interest_x_100_contract_multiplier",
}
