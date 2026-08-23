"""Entry-frozen SPXW surface loadings for one defined-risk structure."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from spx_spark.analytics.greeks.black_scholes import (
    bs_delta,
    bs_gamma,
    bs_price,
    bs_vega,
)
from spx_spark.analytics.greeks.higher_order import bs_vanna_per_vol_point
from spx_spark.analytics.options.pricing import time_to_expiry_years

SURFACE_ATTRIBUTION_VERSION = "entry_frozen_surface_attribution.v1"
_BASIS_NAMES = ("atm", "put_skew", "call_skew", "put_fly", "call_fly")


def attribute_candidate_surface(
    candidate: Mapping[str, Any],
    facts: Mapping[str, Any],
    *,
    now: datetime,
    bump_vol_points: float,
    modifier_cap: float,
) -> dict[str, Any]:
    """Return current-entry Greeks, surface loadings and a bounded risk penalty.

    The five shocks are applied to strikes frozen at candidate creation.  The
    penalty is deliberately non-positive: surface state may choose how to
    express an existing thesis, but cannot create or reverse that thesis.
    """

    spot = _number(_map(facts.get("spot")).get("spx"))
    legs = _structure_legs(candidate)
    expiry = str(candidate.get("expiry") or "") or _expiry_from_legs(legs)
    if spot is None or spot <= 0:
        return _unavailable("spx_price_unavailable")
    if not expiry:
        return _unavailable("surface_expiry_unavailable")
    if not legs:
        return _unavailable("surface_legs_unavailable")
    if bump_vol_points <= 0 or modifier_cap < 0:
        return _unavailable("surface_policy_invalid")
    try:
        tau_years = time_to_expiry_years(expiry, as_of=now)
    except ValueError:
        return _unavailable("surface_expiry_invalid")
    if tau_years <= 0:
        return _unavailable("surface_expired")

    priced_legs = _priced_legs(legs)
    if priced_legs is None:
        return _unavailable("surface_leg_iv_unavailable")
    scale = _surface_scale(candidate, facts, spot=spot, legs=priced_legs)
    bump = bump_vol_points / 100.0
    base_value = _portfolio_value(
        priced_legs,
        spot=spot,
        tau_years=tau_years,
    )
    betas = {
        name: _basis_beta(
            priced_legs,
            basis=name,
            bump=bump,
            spot=spot,
            scale=scale,
            tau_years=tau_years,
            base_value=base_value,
        )
        for name in _BASIS_NAMES
    }
    greeks = _portfolio_greeks(
        priced_legs,
        spot=spot,
        tau_years=tau_years,
        bump=bump,
    )
    risk_points = max(abs(value) for value in betas.values())
    max_loss = _number(_map(candidate.get("economics")).get("max_loss_points"))
    modifier = (
        -min(modifier_cap, risk_points / max_loss)
        if max_loss is not None and max_loss > 0
        else 0.0
    )
    volatility = _map(facts.get("volatility"))
    return {
        "status": "ready",
        "version": SURFACE_ATTRIBUTION_VERSION,
        "authority": "structure_risk_only",
        "automatic_ordering": False,
        "coordinate": "entry_frozen_strike",
        "surface_context": {
            "spot": round(spot, 6),
            "expiry": expiry,
            "tau_years": round(tau_years, 10),
            "scale_points": round(scale, 6),
            "bump_vol_points": round(bump_vol_points, 4),
            "atm_iv_0dte": _rounded(volatility.get("atm_iv_0dte")),
            "put_skew_25d_0dte": _rounded(volatility.get("put_skew_25d_0dte")),
            "call_skew_25d_0dte": _rounded(volatility.get("call_skew_25d_0dte")),
            "atm_iv_minus_es_realized_vol": _rounded(
                volatility.get("atm_iv_minus_es_realized_vol")
            ),
            "strikes": [round(float(leg["strike"]), 4) for leg in priced_legs],
            "log_moneyness": [
                round(math.log(float(leg["strike"]) / spot), 8) for leg in priced_legs
            ],
        },
        "surface_exposure": {
            **{f"{name}_beta_points": round(value, 6) for name, value in betas.items()},
            **greeks,
        },
        "surface_risk_points": round(risk_points, 6),
        "max_loss_points": _rounded(max_loss),
        "decision_modifier": round(modifier, 6),
        "reason_codes": (
            [] if max_loss is not None and max_loss > 0 else ["surface_max_loss_unavailable"]
        ),
    }


def _portfolio_greeks(
    legs: Sequence[Mapping[str, Any]],
    *,
    spot: float,
    tau_years: float,
    bump: float,
) -> dict[str, float]:
    totals = {"delta": 0.0, "gamma": 0.0, "vega": 0.0, "vanna": 0.0, "volga": 0.0}
    for leg in legs:
        strike = float(leg["strike"])
        iv = float(leg["implied_vol"])
        right = str(leg["right"])
        quantity = float(leg["quantity"])
        totals["delta"] += quantity * bs_delta(spot, strike, iv, tau_years, right)
        totals["gamma"] += quantity * bs_gamma(spot, strike, iv, tau_years)
        totals["vega"] += quantity * bs_vega(spot, strike, iv, tau_years) * 0.01
        totals["vanna"] += quantity * (
            bs_vanna_per_vol_point(spot, strike, iv, tau_years) or 0.0
        )
        down_iv = max(iv - bump, 1e-6)
        totals["volga"] += quantity * (
            bs_price(spot, strike, iv + bump, tau_years, right)
            - 2.0 * bs_price(spot, strike, iv, tau_years, right)
            + bs_price(spot, strike, down_iv, tau_years, right)
        )
    return {
        "delta": round(totals["delta"], 6),
        "gamma": round(totals["gamma"], 8),
        "vega_points_per_vol_point": round(totals["vega"], 6),
        "vanna_delta_per_vol_point": round(totals["vanna"], 8),
        "volga_points_for_symmetric_bump": round(totals["volga"], 8),
    }


def _basis_beta(
    legs: Sequence[Mapping[str, Any]],
    *,
    basis: str,
    bump: float,
    spot: float,
    scale: float,
    tau_years: float,
    base_value: float,
) -> float:
    shocked = 0.0
    for leg in legs:
        strike = float(leg["strike"])
        right = str(leg["right"])
        iv = float(leg["implied_vol"])
        weight = _basis_weight(basis, strike=strike, spot=spot, scale=scale)
        shocked += float(leg["quantity"]) * bs_price(
            spot,
            strike,
            max(iv + bump * weight, 1e-6),
            tau_years,
            right,
        )
    return shocked - base_value


def _basis_weight(basis: str, *, strike: float, spot: float, scale: float) -> float:
    coordinate = max(-1.0, min(1.0, (strike - spot) / scale))
    put_distance = max(-coordinate, 0.0)
    call_distance = max(coordinate, 0.0)
    return {
        "atm": 1.0,
        "put_skew": put_distance,
        "call_skew": call_distance,
        "put_fly": put_distance * put_distance,
        "call_fly": call_distance * call_distance,
    }[basis]


def _portfolio_value(
    legs: Sequence[Mapping[str, Any]], *, spot: float, tau_years: float
) -> float:
    return sum(
        float(leg["quantity"])
        * bs_price(
            spot,
            float(leg["strike"]),
            float(leg["implied_vol"]),
            tau_years,
            str(leg["right"]),
        )
        for leg in legs
    )


def _priced_legs(legs: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Any], ...] | None:
    priced: list[dict[str, Any]] = []
    total = len(legs)
    for index, leg in enumerate(legs):
        strike = _number(leg.get("strike"))
        iv = _number(leg.get("implied_vol"))
        right = str(leg.get("right") or "").upper()
        if strike is None or strike <= 0 or iv is None or iv <= 0 or right not in {"C", "P"}:
            return None
        quantity = leg.get("quantity")
        if not isinstance(quantity, int) or isinstance(quantity, bool):
            quantity = _default_quantity(index=index, total=total)
        priced.append(
            {"strike": strike, "implied_vol": iv, "right": right, "quantity": quantity}
        )
    return tuple(priced)


def _structure_legs(candidate: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = candidate.get("legs")
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)) and raw:
        rows = [dict(_map(item)) for item in raw]
        if all(rows):
            return rows
    long_leg, short_leg = _map(candidate.get("long")), _map(candidate.get("short"))
    return [dict(long_leg), dict(short_leg)] if long_leg and short_leg else []


def _surface_scale(
    candidate: Mapping[str, Any],
    facts: Mapping[str, Any],
    *,
    spot: float,
    legs: Sequence[Mapping[str, Any]],
) -> float:
    expected_move = _number(_map(facts.get("volatility")).get("expected_move_points"))
    width = _number(_map(candidate.get("economics")).get("width_points"))
    farthest = max(abs(float(leg["strike"]) - spot) for leg in legs)
    return max(expected_move or 0.0, width or 0.0, farthest, 5.0)


def _expiry_from_legs(legs: Sequence[Mapping[str, Any]]) -> str:
    for leg in legs:
        parts = str(leg.get("contract_id") or "").split(":")
        if len(parts) >= 6:
            return parts[-3]
    return ""


def _default_quantity(*, index: int, total: int) -> int:
    if total == 2:
        return (1, -1)[index]
    if total == 3:
        return (1, -2, 1)[index]
    if total == 4:
        return (1, -1, -1, 1)[index]
    return 1


def _unavailable(reason: str) -> dict[str, Any]:
    return {
        "status": "unavailable",
        "version": SURFACE_ATTRIBUTION_VERSION,
        "authority": "structure_risk_only",
        "automatic_ordering": False,
        "surface_context": {},
        "surface_exposure": {},
        "surface_risk_points": None,
        "decision_modifier": 0.0,
        "reason_codes": [reason],
    }


def _rounded(value: object) -> float | None:
    parsed = _number(value)
    return None if parsed is None else round(parsed, 6)


def _map(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    parsed = float(value)
    return parsed if math.isfinite(parsed) else None


__all__ = ["SURFACE_ATTRIBUTION_VERSION", "attribute_candidate_surface"]
