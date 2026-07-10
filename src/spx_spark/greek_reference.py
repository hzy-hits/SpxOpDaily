from __future__ import annotations

import fcntl
import hashlib
import json
import math
import os
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from statistics import median
from typing import TYPE_CHECKING, Any

from spx_spark.config import StorageSettings
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET
from spx_spark.marketdata import InstrumentType, Quote
from spx_spark.options_map import actionable_chain_implied_spot
from spx_spark.storage import configured_quote_use_decision

if TYPE_CHECKING:
    from spx_spark.options_map import OptionsMap
    from spx_spark.storage import LatestState


YEAR_SECONDS = 365.0 * 24.0 * 3600.0
MIN_TAU_SECONDS = 300.0
MAX_SPREAD_BPS = 250.0
MAX_SERIALIZED_CONTRACTS = 0
DEFAULT_SERIALIZED_SCENARIOS = (
    "spot_down_0_25pct",
    "spot_up_0_25pct",
    "clock_plus_15m",
    "iv_down_1vol",
    "iv_up_1vol",
)
ANCHOR_WARN_BPS = 20.0
ANCHOR_BLOCK_BPS = 50.0
MODEL_NAME = "bs_r0_q0"
SCHEMA_VERSION = "spxw_0dte_greeks_reference.v1"
AGGREGATE_METRICS = (
    "gross_delta_abs",
    "gross_gamma_abs",
    "gross_theta_5m_abs",
    "gross_vega_1vol_abs",
    "gross_charm_5m_abs",
    "gross_color_5m_abs",
    "gross_speed_5pt_abs",
    "gross_vanna_1vol_abs",
    "gross_vomma_per_vol_point2_abs",
    "gross_zomma_1vol_abs",
)


@dataclass(frozen=True)
class GreekInputs:
    contract_id: str
    as_of: datetime
    expiry: str
    spot: float
    strike: float
    right: str
    iv: float
    tau_seconds: float
    mid: float | None = None
    spread_bps: float | None = None
    open_interest: float | None = None
    vendor_delta: float | None = None
    vendor_gamma: float | None = None
    vendor_theta: float | None = None
    vendor_vega: float | None = None
    vendor_underlier: float | None = None


@dataclass(frozen=True)
class DifferenceSteps:
    spot_points: float
    vol_decimal: float
    time_seconds: float


@dataclass(frozen=True)
class GreekQuality:
    status: str
    reasons: tuple[str, ...]
    model: str = MODEL_NAME
    vendor_delta_error: float | None = None
    vendor_gamma_rel_error: float | None = None
    vendor_theta_rel_error: float | None = None
    vendor_vega_rel_error: float | None = None
    step_stability_max_rel_error: float | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["reasons"] = list(self.reasons)
        return payload


@dataclass(frozen=True)
class RepricingScenario:
    name: str
    dimension: str
    shock: float
    spot: float
    iv: float
    tau_seconds: float
    model_price: float
    reference_price: float
    bounded: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ContractGreekReference:
    contract_id: str
    as_of: datetime
    expiry: str
    strike: float
    right: str
    delta: float
    gamma_per_point: float
    theta_per_minute: float
    vega_per_vol_point: float
    charm_delta_per_minute: float
    color_gamma_per_minute: float
    speed_gamma_per_point: float
    vanna_delta_per_vol_point: float
    vomma_price_per_vol_point2: float
    zomma_gamma_per_vol_point: float
    scenarios: tuple[RepricingScenario, ...]
    quality: GreekQuality

    def to_dict(
        self,
        *,
        scenario_names: Iterable[str] | None = None,
    ) -> dict[str, Any]:
        selected_names = set(scenario_names) if scenario_names is not None else None
        scenarios = (
            self.scenarios
            if selected_names is None
            else tuple(row for row in self.scenarios if row.name in selected_names)
        )
        return {
            "contract_id": self.contract_id,
            "as_of": self.as_of.isoformat(),
            "expiry": self.expiry,
            "strike": self.strike,
            "right": self.right,
            "delta": self.delta,
            "gamma_per_point": self.gamma_per_point,
            "theta_per_minute": self.theta_per_minute,
            "vega_per_vol_point": self.vega_per_vol_point,
            "charm_delta_per_minute": self.charm_delta_per_minute,
            "color_gamma_per_minute": self.color_gamma_per_minute,
            "speed_gamma_per_point": self.speed_gamma_per_point,
            "vanna_delta_per_vol_point": self.vanna_delta_per_vol_point,
            "vomma_price_per_vol_point2": self.vomma_price_per_vol_point2,
            "zomma_gamma_per_vol_point": self.zomma_gamma_per_vol_point,
            "scenarios": [scenario.to_dict() for scenario in scenarios],
            "quality": self.quality.to_dict(),
        }


@dataclass(frozen=True)
class ExpiryGreekReference:
    expiry: str
    as_of: datetime
    contract_count: int
    usable_count: int
    iv_coverage_ratio: float
    oi_coverage_ratio: float
    gross_gamma_abs: float | None
    gross_delta_abs: float | None
    gross_theta_5m_abs: float | None
    gross_vega_1vol_abs: float | None
    gross_charm_5m_abs: float | None
    gross_color_5m_abs: float | None
    gross_speed_5pt_abs: float | None
    gross_vanna_1vol_abs: float | None
    gross_vomma_per_vol_point2_abs: float | None
    gross_zomma_1vol_abs: float | None
    direction: str = "unknown"
    position_sign: str = "unknown"
    quality: str = "insufficient"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["as_of"] = self.as_of.isoformat()
        return payload


def _normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))


def _normal_pdf(value: float) -> float:
    return math.exp(-0.5 * value * value) / math.sqrt(2.0 * math.pi)


def _intrinsic(spot: float, strike: float, right: str) -> float:
    if right == "C":
        return max(0.0, spot - strike)
    return max(0.0, strike - spot)


def _d1(spot: float, strike: float, iv: float, tau_years: float) -> float:
    root_t = math.sqrt(tau_years)
    return (math.log(spot / strike) + 0.5 * iv * iv * tau_years) / (iv * root_t)


def bs_price(spot: float, strike: float, iv: float, tau_years: float, right: str) -> float:
    """Black-Scholes price with r=q=0, matching the existing order-map kernel."""

    intrinsic = _intrinsic(spot, strike, right)
    if spot <= 0 or strike <= 0 or tau_years <= 0 or iv <= 0:
        return intrinsic
    d1 = _d1(spot, strike, iv, tau_years)
    d2 = d1 - iv * math.sqrt(tau_years)
    if right == "C":
        return max(intrinsic, spot * _normal_cdf(d1) - strike * _normal_cdf(d2))
    return max(intrinsic, strike * _normal_cdf(-d2) - spot * _normal_cdf(-d1))


def bs_delta(spot: float, strike: float, iv: float, tau_years: float, right: str) -> float:
    if tau_years <= 0 or iv <= 0:
        if spot > strike:
            return 1.0 if right == "C" else 0.0
        if spot < strike:
            return 0.0 if right == "C" else -1.0
        return 0.5 if right == "C" else -0.5
    call_delta = _normal_cdf(_d1(spot, strike, iv, tau_years))
    return call_delta if right == "C" else call_delta - 1.0


def bs_gamma(spot: float, strike: float, iv: float, tau_years: float) -> float:
    if spot <= 0 or strike <= 0 or iv <= 0 or tau_years <= 0:
        return 0.0
    return _normal_pdf(_d1(spot, strike, iv, tau_years)) / (spot * iv * math.sqrt(tau_years))


def bs_vega(spot: float, strike: float, iv: float, tau_years: float) -> float:
    """Return price change per 1.00 absolute volatility, before unit scaling."""

    if spot <= 0 or strike <= 0 or iv <= 0 or tau_years <= 0:
        return 0.0
    return spot * _normal_pdf(_d1(spot, strike, iv, tau_years)) * math.sqrt(tau_years)


def difference_steps(inputs: GreekInputs) -> DifferenceSteps:
    return DifferenceSteps(
        spot_points=min(2.0, max(0.5, inputs.spot * 0.0001)),
        vol_decimal=min(0.01, max(0.0025, inputs.iv * 0.025)),
        time_seconds=min(
            60.0,
            max(15.0, inputs.tau_seconds * 0.05),
            inputs.tau_seconds * 0.25,
        ),
    )


def _is_spxw_option(quote: Quote) -> bool:
    instrument = quote.instrument
    if instrument.instrument_type != InstrumentType.OPTION:
        return False
    if (instrument.underlier or instrument.symbol).upper() != "SPX":
        return False
    trading_class = (instrument.trading_class or "").upper()
    return trading_class.startswith("SPXW")


def _expiry_date(expiry: str) -> datetime | None:
    try:
        return datetime.strptime(expiry, "%Y%m%d")
    except ValueError:
        return None


def is_spxw_zero_dte(quote: Quote, *, as_of: datetime) -> bool:
    """Require the literal New York calendar date, never a research-expiry fallback."""

    if as_of.tzinfo is None or not _is_spxw_option(quote):
        return False
    expiry = quote.instrument.expiry or ""
    parsed = _expiry_date(expiry)
    if parsed is None:
        return False
    current_et = as_of.astimezone(ET)
    if parsed.date() != current_et.date():
        return False
    session = DEFAULT_MARKET_CALENDAR.session(parsed.date())
    return session is not None and session.open_at <= current_et < session.close_at


def _blocked(*reasons: str) -> GreekQuality:
    return GreekQuality(status="blocked", reasons=tuple(dict.fromkeys(reasons)))


def inputs_from_quote(
    quote: Quote,
    *,
    as_of: datetime,
    spot: float | None = None,
    storage_settings: StorageSettings | None = None,
) -> tuple[GreekInputs | None, GreekQuality]:
    if not is_spxw_zero_dte(quote, as_of=as_of):
        return None, _blocked("not_exact_same_day_spxw")

    decision = configured_quote_use_decision(
        quote,
        as_of=as_of,
        settings=storage_settings,
    )
    if not decision.pricing_allowed:
        return None, _blocked(f"quote_not_pricing_allowed:{decision.reason}")

    instrument = quote.instrument
    strike = instrument.strike
    right = instrument.right.value if instrument.right is not None else ""
    greeks = quote.greeks
    iv = greeks.implied_vol if greeks is not None else None
    vendor_underlier = greeks.underlier_price if greeks is not None else None
    chosen_spot = spot if spot is not None else vendor_underlier
    if strike is None or not math.isfinite(strike) or strike <= 0:
        return None, _blocked("invalid_strike")
    if right not in {"C", "P"}:
        return None, _blocked("invalid_option_right")
    if iv is None or not math.isfinite(iv) or iv <= 0:
        return None, _blocked("missing_or_invalid_iv")
    if chosen_spot is None or not math.isfinite(chosen_spot) or chosen_spot <= 0:
        return None, _blocked("missing_or_invalid_underlier")

    parsed = _expiry_date(instrument.expiry or "")
    session = DEFAULT_MARKET_CALENDAR.session(parsed.date()) if parsed is not None else None
    if session is None:
        return None, _blocked("missing_expiry_session")
    tau_seconds = (session.close_at - as_of.astimezone(ET)).total_seconds()
    if tau_seconds <= MIN_TAU_SECONDS:
        return None, _blocked("near_expiry_under_5m")

    inputs = GreekInputs(
        contract_id=instrument.canonical_id,
        as_of=as_of,
        expiry=instrument.expiry or "",
        spot=float(chosen_spot),
        strike=float(strike),
        right=right,
        iv=float(iv),
        tau_seconds=tau_seconds,
        mid=quote.mid or quote.effective_price,
        spread_bps=quote.spread_bps,
        open_interest=quote.open_interest,
        vendor_delta=greeks.delta if greeks is not None else None,
        vendor_gamma=greeks.gamma if greeks is not None else None,
        vendor_theta=greeks.theta if greeks is not None else None,
        vendor_vega=greeks.vega if greeks is not None else None,
        vendor_underlier=vendor_underlier,
    )
    steps = difference_steps(inputs)
    if inputs.iv - steps.vol_decimal <= 0:
        return None, _blocked("iv_below_finite_difference_step")
    return inputs, GreekQuality(status="ok", reasons=())


def _higher_order_values(
    inputs: GreekInputs,
    steps: DifferenceSteps,
) -> dict[str, float]:
    tau = inputs.tau_seconds / YEAR_SECONDS
    tau_minus = (inputs.tau_seconds - steps.time_seconds) / YEAR_SECONDS
    tau_plus = (inputs.tau_seconds + steps.time_seconds) / YEAR_SECONDS
    time_minutes = steps.time_seconds / 60.0

    delta_future = bs_delta(inputs.spot, inputs.strike, inputs.iv, tau_minus, inputs.right)
    delta_past = bs_delta(inputs.spot, inputs.strike, inputs.iv, tau_plus, inputs.right)
    gamma_future = bs_gamma(inputs.spot, inputs.strike, inputs.iv, tau_minus)
    gamma_past = bs_gamma(inputs.spot, inputs.strike, inputs.iv, tau_plus)
    price_future = bs_price(inputs.spot, inputs.strike, inputs.iv, tau_minus, inputs.right)
    price_past = bs_price(inputs.spot, inputs.strike, inputs.iv, tau_plus, inputs.right)

    spot_up = inputs.spot + steps.spot_points
    spot_down = inputs.spot - steps.spot_points
    vol_up = inputs.iv + steps.vol_decimal
    vol_down = inputs.iv - steps.vol_decimal

    speed = (
        bs_gamma(spot_up, inputs.strike, inputs.iv, tau)
        - bs_gamma(spot_down, inputs.strike, inputs.iv, tau)
    ) / (2.0 * steps.spot_points)
    vanna_raw = (
        bs_delta(inputs.spot, inputs.strike, vol_up, tau, inputs.right)
        - bs_delta(inputs.spot, inputs.strike, vol_down, tau, inputs.right)
    ) / (2.0 * steps.vol_decimal)
    vomma_raw = (
        bs_vega(inputs.spot, inputs.strike, vol_up, tau)
        - bs_vega(inputs.spot, inputs.strike, vol_down, tau)
    ) / (2.0 * steps.vol_decimal)
    zomma_raw = (
        bs_gamma(inputs.spot, inputs.strike, vol_up, tau)
        - bs_gamma(inputs.spot, inputs.strike, vol_down, tau)
    ) / (2.0 * steps.vol_decimal)

    return {
        "theta_per_minute": (price_future - price_past) / (2.0 * time_minutes),
        "charm_delta_per_minute": (delta_future - delta_past) / (2.0 * time_minutes),
        "color_gamma_per_minute": (gamma_future - gamma_past) / (2.0 * time_minutes),
        "speed_gamma_per_point": speed,
        "vanna_delta_per_vol_point": vanna_raw * 0.01,
        "vomma_price_per_vol_point2": vomma_raw * 0.01 * 0.01,
        "zomma_gamma_per_vol_point": zomma_raw * 0.01,
    }


def _relative_step_error(left: float, right: float) -> float:
    if max(abs(left), abs(right)) <= 1e-8:
        return 0.0
    return abs(left - right) / max(abs(left), abs(right), 1e-12)


def _scenario_reference_price(
    inputs: GreekInputs,
    *,
    spot: float,
    iv: float,
    tau_seconds: float,
) -> tuple[float, float, bool]:
    intrinsic = _intrinsic(spot, inputs.strike, inputs.right)
    if tau_seconds <= 0:
        return intrinsic, intrinsic, True
    base_model = bs_price(
        inputs.spot,
        inputs.strike,
        inputs.iv,
        inputs.tau_seconds / YEAR_SECONDS,
        inputs.right,
    )
    scenario_model = bs_price(spot, inputs.strike, iv, tau_seconds / YEAR_SECONDS, inputs.right)
    anchor = inputs.mid if inputs.mid is not None and inputs.mid > 0 else base_model
    if base_model <= 1e-12:
        reference = scenario_model
    else:
        reference = anchor * scenario_model / base_model
    upper_bound = spot if inputs.right == "C" else inputs.strike
    bounded_reference = min(max(intrinsic, reference), upper_bound)
    was_bounded = not math.isclose(reference, bounded_reference, rel_tol=1e-12, abs_tol=1e-12)
    return scenario_model, bounded_reference, was_bounded


def _build_scenarios(inputs: GreekInputs) -> tuple[RepricingScenario, ...]:
    scenarios: list[RepricingScenario] = []

    for pct in (-0.005, -0.0025, 0.0025, 0.005):
        target_spot = max(inputs.spot * (1.0 + pct), 0.01)
        model_price, reference_price, price_was_bounded = _scenario_reference_price(
            inputs,
            spot=target_spot,
            iv=inputs.iv,
            tau_seconds=inputs.tau_seconds,
        )
        direction = "down" if pct < 0 else "up"
        magnitude = "0_50" if abs(pct) == 0.005 else "0_25"
        scenarios.append(
            RepricingScenario(
                name=f"spot_{direction}_{magnitude}pct",
                dimension="spot_pct",
                shock=pct,
                spot=target_spot,
                iv=inputs.iv,
                tau_seconds=inputs.tau_seconds,
                model_price=model_price,
                reference_price=reference_price,
                bounded=price_was_bounded,
            )
        )

    for minutes in (5, 15, 30):
        raw_tau = inputs.tau_seconds - minutes * 60.0
        target_tau = max(raw_tau, 0.0)
        model_price, reference_price, price_was_bounded = _scenario_reference_price(
            inputs,
            spot=inputs.spot,
            iv=inputs.iv,
            tau_seconds=target_tau,
        )
        scenarios.append(
            RepricingScenario(
                name=f"clock_plus_{minutes}m",
                dimension="clock_minutes",
                shock=float(minutes),
                spot=inputs.spot,
                iv=inputs.iv,
                tau_seconds=target_tau,
                model_price=model_price,
                reference_price=reference_price,
                bounded=raw_tau < 0 or price_was_bounded,
            )
        )

    for vol_points in (-3, -1, 1, 3):
        raw_iv = inputs.iv + vol_points * 0.01
        target_iv = min(max(raw_iv, 0.0001), 10.0)
        model_price, reference_price, price_was_bounded = _scenario_reference_price(
            inputs,
            spot=inputs.spot,
            iv=target_iv,
            tau_seconds=inputs.tau_seconds,
        )
        direction = "down" if vol_points < 0 else "up"
        scenarios.append(
            RepricingScenario(
                name=f"iv_{direction}_{abs(vol_points)}vol",
                dimension="iv_vol_points",
                shock=float(vol_points),
                spot=inputs.spot,
                iv=target_iv,
                tau_seconds=inputs.tau_seconds,
                model_price=model_price,
                reference_price=reference_price,
                bounded=target_iv != raw_iv or price_was_bounded,
            )
        )
    return tuple(scenarios)


def calculate_contract_reference(
    inputs: GreekInputs,
    *,
    steps: DifferenceSteps | None = None,
) -> ContractGreekReference:
    if inputs.spot <= 0 or inputs.strike <= 0 or inputs.iv <= 0:
        raise ValueError("spot, strike, and iv must be positive")
    if inputs.tau_seconds <= MIN_TAU_SECONDS:
        raise ValueError("tau_seconds must be greater than five minutes")
    if inputs.right not in {"C", "P"}:
        raise ValueError("right must be C or P")

    resolved_steps = steps or difference_steps(inputs)
    if inputs.iv - resolved_steps.vol_decimal <= 0:
        raise ValueError("volatility finite-difference step crosses zero")
    values = _higher_order_values(inputs, resolved_steps)
    half_steps = DifferenceSteps(
        spot_points=resolved_steps.spot_points / 2.0,
        vol_decimal=resolved_steps.vol_decimal / 2.0,
        time_seconds=resolved_steps.time_seconds / 2.0,
    )
    half_values = _higher_order_values(inputs, half_steps)
    stability = max(
        _relative_step_error(values[name], half_values[name])
        for name in (
            "charm_delta_per_minute",
            "color_gamma_per_minute",
            "speed_gamma_per_point",
            "vanna_delta_per_vol_point",
            "vomma_price_per_vol_point2",
            "zomma_gamma_per_vol_point",
        )
    )

    tau_years = inputs.tau_seconds / YEAR_SECONDS
    delta = bs_delta(inputs.spot, inputs.strike, inputs.iv, tau_years, inputs.right)
    gamma = bs_gamma(inputs.spot, inputs.strike, inputs.iv, tau_years)
    vega_per_vol_point = bs_vega(inputs.spot, inputs.strike, inputs.iv, tau_years) * 0.01

    reasons: list[str] = []
    if inputs.spread_bps is not None and inputs.spread_bps > MAX_SPREAD_BPS:
        reasons.append("wide_quote_over_250bps")
    if abs(delta) < 0.05 or abs(delta) > 0.95:
        reasons.append("deep_wing_delta")
    base_model = bs_price(
        inputs.spot,
        inputs.strike,
        inputs.iv,
        tau_years,
        inputs.right,
    )
    if inputs.mid is not None and inputs.mid > 0:
        intrinsic = _intrinsic(inputs.spot, inputs.strike, inputs.right)
        upper_bound = inputs.spot if inputs.right == "C" else inputs.strike
        if inputs.mid < intrinsic - 0.05 or inputs.mid > upper_bound + 0.05:
            reasons.append("market_mid_no_arbitrage_violation")
        if base_model >= 0.05:
            anchor_ratio = inputs.mid / base_model
            if anchor_ratio < 0.25 or anchor_ratio > 4.0:
                reasons.append("market_mid_model_ratio_extreme")
    if inputs.vendor_underlier is not None:
        mismatch = abs(inputs.vendor_underlier / inputs.spot - 1.0)
        if mismatch > 0.002:
            reasons.append("vendor_underlier_mismatch_over_20bps")

    vendor_delta_error = None
    if inputs.vendor_delta is not None:
        vendor_delta_error = abs(inputs.vendor_delta - delta)
        if vendor_delta_error > 0.05:
            reasons.append("vendor_delta_mismatch_over_0_05")

    vendor_gamma_rel_error = None
    if inputs.vendor_gamma is not None:
        gamma_error = abs(inputs.vendor_gamma - gamma)
        vendor_gamma_rel_error = gamma_error / max(abs(inputs.vendor_gamma), 1e-6)
        if (
            abs(inputs.vendor_gamma) <= 1e-6
            and gamma_error > 1e-6
            or abs(inputs.vendor_gamma) > 1e-6
            and vendor_gamma_rel_error > 0.25
        ):
            reasons.append("vendor_gamma_mismatch")

    vendor_theta_rel_error = None
    if inputs.vendor_theta is not None:
        model_theta_per_day = values["theta_per_minute"] * 1440.0
        theta_error = abs(inputs.vendor_theta - model_theta_per_day)
        vendor_theta_rel_error = theta_error / max(abs(inputs.vendor_theta), 1e-6)
        if theta_error > 0.25 and vendor_theta_rel_error > 0.50:
            reasons.append("vendor_theta_mismatch")

    vendor_vega_rel_error = None
    if inputs.vendor_vega is not None:
        vega_error = abs(inputs.vendor_vega - vega_per_vol_point)
        vendor_vega_rel_error = vega_error / max(abs(inputs.vendor_vega), 1e-6)
        if vega_error > 0.05 and vendor_vega_rel_error > 0.50:
            reasons.append("vendor_vega_mismatch")
    if stability > 0.20:
        reasons.append("finite_difference_unstable")

    quality = GreekQuality(
        status="degraded" if reasons else "ok",
        reasons=tuple(dict.fromkeys(reasons)),
        vendor_delta_error=vendor_delta_error,
        vendor_gamma_rel_error=vendor_gamma_rel_error,
        vendor_theta_rel_error=vendor_theta_rel_error,
        vendor_vega_rel_error=vendor_vega_rel_error,
        step_stability_max_rel_error=stability,
    )
    return ContractGreekReference(
        contract_id=inputs.contract_id,
        as_of=inputs.as_of,
        expiry=inputs.expiry,
        strike=inputs.strike,
        right=inputs.right,
        delta=delta,
        gamma_per_point=gamma,
        theta_per_minute=values["theta_per_minute"],
        vega_per_vol_point=vega_per_vol_point,
        charm_delta_per_minute=values["charm_delta_per_minute"],
        color_gamma_per_minute=values["color_gamma_per_minute"],
        speed_gamma_per_point=values["speed_gamma_per_point"],
        vanna_delta_per_vol_point=values["vanna_delta_per_vol_point"],
        vomma_price_per_vol_point2=values["vomma_price_per_vol_point2"],
        zomma_gamma_per_vol_point=values["zomma_gamma_per_vol_point"],
        scenarios=_build_scenarios(inputs),
        quality=quality,
    )


def calculate_zero_dte_references(
    quotes: Iterable[Quote],
    *,
    as_of: datetime,
    spot: float | None = None,
    storage_settings: StorageSettings | None = None,
) -> tuple[ContractGreekReference, ...]:
    references: list[ContractGreekReference] = []
    for quote in quotes:
        inputs, _quality = inputs_from_quote(
            quote,
            as_of=as_of,
            spot=spot,
            storage_settings=storage_settings,
        )
        if inputs is not None:
            references.append(calculate_contract_reference(inputs))
    return tuple(references)


def aggregate_expiry_reference(
    references: Iterable[ContractGreekReference],
    *,
    inputs_by_contract: Mapping[str, GreekInputs],
    total_contract_count: int | None = None,
) -> ExpiryGreekReference:
    rows = tuple(references)
    inputs = tuple(inputs_by_contract.values())
    as_of = rows[0].as_of if rows else (inputs[0].as_of if inputs else datetime.now(tz=ET))
    expiry = rows[0].expiry if rows else (inputs[0].expiry if inputs else "")
    contract_count = max(total_contract_count or len(inputs), len(inputs))
    usable_count = len(rows)
    iv_coverage = usable_count / contract_count if contract_count else 0.0
    oi_coverage = (
        sum(1 for item in inputs if item.open_interest is not None and item.open_interest > 0)
        / contract_count
        if contract_count
        else 0.0
    )
    sides_by_strike: dict[float, set[str]] = {}
    for row in rows:
        sides_by_strike.setdefault(row.strike, set()).add(row.right)
    paired_strikes = sum(1 for sides in sides_by_strike.values() if sides == {"C", "P"})
    quality = (
        "ok"
        if usable_count >= 6 and paired_strikes >= 3 and iv_coverage >= 0.60 and oi_coverage >= 0.60
        else "insufficient"
    )

    weighted: list[tuple[ContractGreekReference, float]] = []
    for row in rows:
        source = inputs_by_contract.get(row.contract_id)
        if source is not None and source.open_interest is not None and source.open_interest > 0:
            weighted.append((row, source.open_interest))
    allow_exposure = oi_coverage >= 0.60 and bool(weighted)

    def gross(value: Any) -> float | None:
        if not allow_exposure:
            return None
        return sum(abs(float(value(row))) * oi * 100.0 for row, oi in weighted)

    return ExpiryGreekReference(
        expiry=expiry,
        as_of=as_of,
        contract_count=contract_count,
        usable_count=usable_count,
        iv_coverage_ratio=iv_coverage,
        oi_coverage_ratio=oi_coverage,
        gross_gamma_abs=gross(lambda row: row.gamma_per_point),
        gross_delta_abs=gross(lambda row: row.delta),
        gross_theta_5m_abs=gross(lambda row: row.theta_per_minute * 5.0),
        gross_vega_1vol_abs=gross(lambda row: row.vega_per_vol_point),
        gross_charm_5m_abs=gross(lambda row: row.charm_delta_per_minute * 5.0),
        gross_color_5m_abs=gross(lambda row: row.color_gamma_per_minute * 5.0),
        gross_speed_5pt_abs=gross(lambda row: row.speed_gamma_per_point * 5.0),
        gross_vanna_1vol_abs=gross(lambda row: row.vanna_delta_per_vol_point),
        gross_vomma_per_vol_point2_abs=gross(lambda row: row.vomma_price_per_vol_point2),
        gross_zomma_1vol_abs=gross(lambda row: row.zomma_gamma_per_vol_point),
        quality=quality,
    )


def _unavailable_payload(as_of: datetime, expiry: str, reason: str) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "snapshot",
        "mode": "reference_only",
        "status": "unavailable",
        "as_of": as_of.isoformat(),
        "expiry": expiry,
        "direction": "unknown",
        "position_sign": "unknown",
        "reason": reason,
        "aggregate": None,
        "contracts": [],
    }


def _reference_spot(
    state: LatestState,
    *,
    quotes: Iterable[Quote],
    storage_settings: StorageSettings,
) -> tuple[float | None, str | None, tuple[str, ...]]:
    """Resolve an SPX-scale spot without silently substituting ES/SPY basis."""

    quote_rows = tuple(quotes)
    if not quote_rows:
        return None, None, ("exact_same_day_quotes_unavailable",)

    warnings: list[str] = []
    vendor_spots: list[float] = []
    for quote in quote_rows:
        decision = configured_quote_use_decision(
            quote,
            as_of=state.as_of,
            settings=storage_settings,
        )
        value = quote.greeks.underlier_price if quote.greeks is not None else None
        if decision.pricing_allowed and value is not None and math.isfinite(value) and value > 0:
            vendor_spots.append(float(value))
    vendor_spot = float(median(vendor_spots)) if vendor_spots else None
    if vendor_spot is not None:
        dispersion_bps = (max(vendor_spots) - min(vendor_spots)) / vendor_spot * 10_000.0
        if dispersion_bps > ANCHOR_WARN_BPS:
            warnings.append("vendor_underlier_dispersion_over_20bps")

    expiry = quote_rows[0].instrument.expiry or ""
    chain_spot = actionable_chain_implied_spot(
        state,
        expiry=expiry,
        as_of=state.as_of,
        max_leg_skew_seconds=2.0,
    )

    direct_spot: float | None = None
    spx_quote = state.best_quote("index:SPX")
    if spx_quote is not None:
        decision = configured_quote_use_decision(
            spx_quote,
            as_of=state.as_of,
            settings=storage_settings,
        )
        price = spx_quote.effective_price
        if decision.pricing_allowed and price is not None and math.isfinite(price) and price > 0:
            direct_spot = float(price)

    if direct_spot is not None:
        comparisons = [value for value in (chain_spot, vendor_spot) if value is not None]
        max_divergence_bps = max(
            (abs(value / direct_spot - 1.0) * 10_000.0 for value in comparisons),
            default=0.0,
        )
        if max_divergence_bps >= ANCHOR_BLOCK_BPS:
            return None, None, ("spx_anchor_divergence_over_50bps",)
        if max_divergence_bps >= ANCHOR_WARN_BPS:
            warnings.append("spx_anchor_divergence_over_20bps")
        return direct_spot, "index:SPX", tuple(dict.fromkeys(warnings))

    if chain_spot is not None:
        divergence_bps = (
            abs(vendor_spot / chain_spot - 1.0) * 10_000.0 if vendor_spot is not None else 0.0
        )
        if divergence_bps >= ANCHOR_BLOCK_BPS:
            return None, None, ("spx_anchor_divergence_over_50bps",)
        if divergence_bps >= ANCHOR_WARN_BPS:
            warnings.append("spx_anchor_divergence_over_20bps")
        return chain_spot, "spxw_put_call_parity", tuple(dict.fromkeys(warnings))

    if vendor_spot is not None:
        return vendor_spot, "spxw_model_underlier_median", tuple(dict.fromkeys(warnings))
    return None, None, ("live_spx_underlier_unavailable",)


def build_zero_dte_greeks_reference(
    state: LatestState,
    *,
    options_map: OptionsMap,
    focus_contract_ids: Iterable[str] = (),
    max_serialized_contracts: int = MAX_SERIALIZED_CONTRACTS,
    serialized_scenario_names: Iterable[str] = DEFAULT_SERIALIZED_SCENARIOS,
) -> dict[str, Any]:
    """Build a bounded, reference-only payload without expiry or dealer-sign fallback."""

    as_of = state.as_of
    exact_expiry = as_of.astimezone(ET).strftime("%Y%m%d")
    available_expiries = {
        str(getattr(expiry, "expiry", "")) for expiry in getattr(options_map, "expiries", ())
    }
    if exact_expiry not in available_expiries:
        return _unavailable_payload(as_of, exact_expiry, "exact_same_day_expiry_unavailable")

    exact_quotes = [
        quote
        for quote in state.best_quotes
        if is_spxw_zero_dte(quote, as_of=as_of) and (quote.instrument.expiry or "") == exact_expiry
    ]
    if not exact_quotes:
        return _unavailable_payload(as_of, exact_expiry, "exact_same_day_quotes_unavailable")

    storage_settings = StorageSettings.from_env()
    spot, spot_source, spot_warnings = _reference_spot(
        state,
        quotes=exact_quotes,
        storage_settings=storage_settings,
    )
    if spot is None:
        payload = _unavailable_payload(
            as_of,
            exact_expiry,
            "exact_expiry_spx_underlier_unavailable",
        )
        payload["warnings"] = list(spot_warnings)
        return payload

    inputs_by_contract: dict[str, GreekInputs] = {}
    blocked_counts: dict[str, int] = {}
    references: list[ContractGreekReference] = []
    for quote in exact_quotes:
        inputs, quality = inputs_from_quote(
            quote,
            as_of=as_of,
            spot=spot,
            storage_settings=storage_settings,
        )
        if inputs is None:
            for reason in quality.reasons:
                blocked_counts[reason] = blocked_counts.get(reason, 0) + 1
            continue
        inputs_by_contract[inputs.contract_id] = inputs
        references.append(calculate_contract_reference(inputs))
    if not references:
        payload = _unavailable_payload(as_of, exact_expiry, "no_usable_exact_expiry_references")
        payload["blocked_counts"] = blocked_counts
        return payload

    aggregate = aggregate_expiry_reference(
        references,
        inputs_by_contract=inputs_by_contract,
        total_contract_count=len(exact_quotes),
    )
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
    displayed = ordered[: max(max_serialized_contracts, 0)]
    selected_scenarios = tuple(serialized_scenario_names)
    quality_counts = {
        status: sum(1 for row in references if row.quality.status == status)
        for status in ("ok", "degraded")
    }
    quality_reason_counts: dict[str, int] = {}
    for row in references:
        for reason in row.quality.reasons:
            quality_reason_counts[reason] = quality_reason_counts.get(reason, 0) + 1
    universe_ids = sorted(inputs_by_contract)
    universe_tokens = [
        f"{contract_id}:{inputs_by_contract[contract_id].open_interest or 0.0:.6f}"
        for contract_id in universe_ids
    ]
    universe_fingerprint = hashlib.sha256("\n".join(universe_tokens).encode()).hexdigest()[:16]
    ok_ratio = quality_counts["ok"] / len(references)
    degraded = aggregate.quality != "ok" or ok_ratio < 0.60 or bool(spot_warnings)
    front = next(
        (row for row in options_map.expiries if str(getattr(row, "expiry", "")) == exact_expiry),
        None,
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "snapshot",
        "mode": "reference_only",
        "status": "degraded" if degraded else "ok",
        "as_of": as_of.isoformat(),
        "expiry": exact_expiry,
        "scope": {
            "underlier": "SPX",
            "trading_class": "SPXW",
            "expiry": exact_expiry,
            "dte": 0,
        },
        "model": {
            "name": MODEL_NAME,
            "spot": spot,
            "spot_source": spot_source,
            "minutes_to_expiry": round(
                inputs_by_contract[next(iter(inputs_by_contract))].tau_seconds / 60.0, 2
            ),
            "time_derivative_convention": "calendar_time_forward",
            "vol_point_decimal": 0.01,
        },
        "direction": "unknown",
        "position_sign": "unknown",
        "signed_gex_proxy": {
            "net_gex": getattr(front, "net_gex", None),
            "abs_gex": getattr(front, "abs_gex", None),
            "net_gamma_ratio": getattr(front, "net_gamma_ratio", None),
            "gamma_state": getattr(front, "gamma_state", "unknown"),
            "weighting": getattr(front, "gex_weighting", "unknown"),
            "sign_method": (
                "call_positive_put_negative_oi_plus_volume_proxy_not_dealer_position"
                if getattr(front, "gex_weighting", None) == "oi_plus_volume"
                else "call_positive_put_negative_oi_proxy_not_dealer_position"
            ),
            "dealer_position_sign": "unknown",
            "direction": "unknown",
        },
        "weighting": {
            "aggregate": "open_interest_only",
            "intraday_volume": "context_only_not_used",
        },
        "units": {
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
        },
        "aggregate_scope": "currently_actionable_exact_expiry_contracts_oi_only",
        "aggregate_universe": {
            "fingerprint": universe_fingerprint,
            "contract_count": len(universe_ids),
        },
        "aggregate": aggregate.to_dict(),
        "coverage": {
            "exact_expiry_contract_count": len(exact_quotes),
            "usable_contract_count": len(references),
            "usable_ratio": len(references) / len(exact_quotes),
            "oi_ratio": aggregate.oi_coverage_ratio,
        },
        "quality_counts": quality_counts,
        "quality_reason_counts": quality_reason_counts,
        "usable_contract_count": len(references),
        "serialized_contract_count": len(displayed),
        "blocked_counts": blocked_counts,
        "warnings": list(spot_warnings),
        "contracts": [row.to_dict(scenario_names=selected_scenarios) for row in displayed],
    }


def write_zero_dte_greeks_snapshot(
    payload: Mapping[str, Any],
    *,
    data_root: str | Path,
) -> dict[str, str] | None:
    """Persist a versioned snapshot under a cross-process lock."""

    if payload.get("schema_version") != SCHEMA_VERSION:
        return None
    if payload.get("status") not in {"ok", "degraded", "unavailable"}:
        return None
    expiry = str(payload.get("expiry") or "")
    if _expiry_date(expiry) is None:
        return None

    root = Path(data_root)
    raw_path = (
        root
        / "features"
        / "spxw_0dte_greeks_reference"
        / f"date={expiry[:4]}-{expiry[4:6]}-{expiry[6:8]}"
        / "snapshots.jsonl"
    )
    latest_path = root / "latest" / "spxw_0dte_greeks_reference.json"
    lock_path = root / "latest" / "spxw_0dte_greeks_reference.lock"
    serialized = json.dumps(dict(payload), sort_keys=True, separators=(",", ":"))
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            with raw_path.open("a", encoding="utf-8") as handle:
                handle.write(serialized)
                handle.write("\n")

            current_as_of = ""
            try:
                current = json.loads(latest_path.read_text(encoding="utf-8"))
                if isinstance(current, dict) and isinstance(current.get("as_of"), str):
                    current_as_of = str(current["as_of"])
            except (OSError, json.JSONDecodeError):
                pass
            incoming_as_of = str(payload.get("as_of") or "")
            if not current_as_of or incoming_as_of >= current_as_of:
                temporary = latest_path.with_name(f".{latest_path.name}.{os.getpid()}.tmp")
                temporary.write_text(serialized, encoding="utf-8")
                temporary.replace(latest_path)
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
    return {"raw_path": str(raw_path), "latest_path": str(latest_path)}


def load_zero_dte_greeks_snapshots(
    *,
    data_root: str | Path,
    trading_date: str,
) -> tuple[dict[str, Any], ...]:
    expiry = trading_date.replace("-", "")
    path = (
        Path(data_root)
        / "features"
        / "spxw_0dte_greeks_reference"
        / f"date={trading_date}"
        / "snapshots.jsonl"
    )
    if not path.exists():
        return ()
    by_as_of: dict[str, dict[str, Any]] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return ()
    for line in lines:
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(row, dict):
            continue
        if row.get("schema_version") != SCHEMA_VERSION or row.get("expiry") != expiry:
            continue
        as_of = row.get("as_of")
        if isinstance(as_of, str) and as_of:
            by_as_of[as_of] = row
    return tuple(by_as_of[key] for key in sorted(by_as_of))


def summarize_zero_dte_greeks_session(
    snapshots: Iterable[Mapping[str, Any]],
    *,
    expiry: str,
) -> dict[str, Any]:
    rows = sorted(
        (
            dict(row)
            for row in snapshots
            if row.get("schema_version") == SCHEMA_VERSION
            and row.get("expiry") == expiry
            and row.get("status") in {"ok", "degraded", "unavailable"}
            and isinstance(row.get("as_of"), str)
        ),
        key=lambda row: str(row["as_of"]),
    )
    if not rows:
        return {
            "schema_version": SCHEMA_VERSION,
            "kind": "session_summary",
            "mode": "reference_only",
            "status": "unavailable",
            "expiry": expiry,
            "direction": "unknown",
            "position_sign": "unknown",
            "snapshot_count": 0,
            "metrics": {},
            "warnings": [],
        }

    usable_rows = [row for row in rows if row.get("status") in {"ok", "degraded"}]
    by_universe: dict[str, list[dict[str, Any]]] = {}
    for row in usable_rows:
        universe = row.get("aggregate_universe")
        fingerprint = (
            str(universe.get("fingerprint"))
            if isinstance(universe, Mapping) and universe.get("fingerprint")
            else f"missing:{row['as_of']}"
        )
        by_universe.setdefault(fingerprint, []).append(row)
    comparison_fingerprint = None
    comparison_rows: list[dict[str, Any]] = []
    if by_universe:
        comparison_fingerprint, comparison_rows = max(
            by_universe.items(),
            key=lambda item: (len(item[1]), str(item[1][-1]["as_of"])),
        )

    metric_rows: dict[str, dict[str, float]] = {}
    if len(comparison_rows) >= 2:
        for name in AGGREGATE_METRICS:
            values = [
                float(aggregate[name])
                for row in comparison_rows
                if isinstance(aggregate := row.get("aggregate"), Mapping)
                and isinstance(aggregate.get(name), int | float)
            ]
            if values:
                metric_rows[name] = {
                    "first": values[0],
                    "last": values[-1],
                    "peak": max(values),
                }
    quality_counts = {
        status: sum(1 for row in rows if row.get("status") == status)
        for status in ("ok", "degraded", "unavailable")
    }
    usable_ratios = [
        float(coverage["usable_ratio"])
        for row in usable_rows
        if isinstance(coverage := row.get("coverage"), Mapping)
        and isinstance(coverage.get("usable_ratio"), int | float)
    ]
    oi_ratios = [
        float(coverage["oi_ratio"])
        for row in usable_rows
        if isinstance(coverage := row.get("coverage"), Mapping)
        and isinstance(coverage.get("oi_ratio"), int | float)
    ]

    def coverage_change(values: list[float]) -> dict[str, float] | None:
        if not values:
            return None
        return {"first": values[0], "last": values[-1], "min": min(values)}

    blocked_counts: dict[str, int] = {}
    quality_reason_counts: dict[str, int] = {}
    for row in usable_rows:
        for target, source_name in (
            (blocked_counts, "blocked_counts"),
            (quality_reason_counts, "quality_reason_counts"),
        ):
            source = row.get(source_name)
            if not isinstance(source, Mapping):
                continue
            for reason, count in source.items():
                if isinstance(count, int | float):
                    target[str(reason)] = target.get(str(reason), 0) + int(count)
    summary_degraded = (
        quality_counts["degraded"] > 0
        or quality_counts["unavailable"] > 0
        or len(comparison_rows) < 2
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "session_summary",
        "mode": "reference_only",
        "status": "degraded" if summary_degraded else "ok",
        "expiry": expiry,
        "direction": "unknown",
        "position_sign": "unknown",
        "snapshot_count": len(rows),
        "usable_snapshot_count": len(usable_rows),
        "comparison_snapshot_count": len(comparison_rows),
        "comparison_universe_fingerprint": comparison_fingerprint,
        "aggregate_universe_count": len(by_universe),
        "first_as_of": rows[0]["as_of"],
        "last_as_of": rows[-1]["as_of"],
        "quality_counts": quality_counts,
        "coverage": {
            "usable_ratio": coverage_change(usable_ratios),
            "oi_ratio": coverage_change(oi_ratios),
        },
        "blocked_counts": blocked_counts,
        "quality_reason_counts": quality_reason_counts,
        "metrics": metric_rows,
        "warnings": sorted(
            {str(warning) for row in rows for warning in (row.get("warnings") or ())}
        ),
    }
