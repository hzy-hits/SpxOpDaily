from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
from statistics import median
from typing import TYPE_CHECKING, Any

from spx_spark.analytics.greeks.black_scholes import (
    bs_delta,
    bs_gamma,
    bs_price,
    bs_vega,
    intrinsic_value as _intrinsic,
)
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
    """Require the active SPX trading date during RTH or its preceding GTH."""

    if as_of.tzinfo is None or not _is_spxw_option(quote):
        return False
    expiry = quote.instrument.expiry or ""
    parsed = _expiry_date(expiry)
    if parsed is None:
        return False
    current_et = as_of.astimezone(ET)
    session_open = DEFAULT_MARKET_CALENDAR.is_rth_open(current_et)
    gth_open = DEFAULT_MARKET_CALENDAR.is_spx_gth_open(current_et)
    if not session_open and not gth_open:
        return False
    active_expiry = (
        DEFAULT_MARKET_CALENDAR.research_expiry(current_et)
        if gth_open
        else current_et.date()
    )
    return parsed.date() == active_expiry


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


def _validate_calculation_inputs(inputs: GreekInputs, steps: DifferenceSteps) -> None:
    if inputs.spot <= 0 or inputs.strike <= 0 or inputs.iv <= 0:
        raise ValueError("spot, strike, and iv must be positive")
    if inputs.tau_seconds <= MIN_TAU_SECONDS:
        raise ValueError("tau_seconds must be greater than five minutes")
    if inputs.right not in {"C", "P"}:
        raise ValueError("right must be C or P")
    if inputs.iv - steps.vol_decimal <= 0:
        raise ValueError("volatility finite-difference step crosses zero")


def _step_stability(
    inputs: GreekInputs,
    steps: DifferenceSteps,
    values: Mapping[str, float],
) -> float:
    half_steps = DifferenceSteps(
        spot_points=steps.spot_points / 2.0,
        vol_decimal=steps.vol_decimal / 2.0,
        time_seconds=steps.time_seconds / 2.0,
    )
    half_values = _higher_order_values(inputs, half_steps)
    names = (
        "charm_delta_per_minute",
        "color_gamma_per_minute",
        "speed_gamma_per_point",
        "vanna_delta_per_vol_point",
        "vomma_price_per_vol_point2",
        "zomma_gamma_per_vol_point",
    )
    return max(_relative_step_error(values[name], half_values[name]) for name in names)


def _relative_vendor_error(vendor: float | None, model: float) -> tuple[float | None, float]:
    if vendor is None:
        return None, 0.0
    absolute = abs(vendor - model)
    return absolute / max(abs(vendor), 1e-6), absolute


def _contract_quality(
    inputs: GreekInputs,
    *,
    delta: float,
    gamma: float,
    theta_per_minute: float,
    vega_per_vol_point: float,
    stability: float,
) -> GreekQuality:
    reasons: list[str] = []
    if inputs.spread_bps is not None and inputs.spread_bps > MAX_SPREAD_BPS:
        reasons.append("wide_quote_over_250bps")
    if abs(delta) < 0.05 or abs(delta) > 0.95:
        reasons.append("deep_wing_delta")

    tau_years = inputs.tau_seconds / YEAR_SECONDS
    base_model = bs_price(inputs.spot, inputs.strike, inputs.iv, tau_years, inputs.right)
    if inputs.mid is not None and inputs.mid > 0:
        intrinsic = _intrinsic(inputs.spot, inputs.strike, inputs.right)
        upper_bound = inputs.spot if inputs.right == "C" else inputs.strike
        if inputs.mid < intrinsic - 0.05 or inputs.mid > upper_bound + 0.05:
            reasons.append("market_mid_no_arbitrage_violation")
        if base_model >= 0.05 and not 0.25 <= inputs.mid / base_model <= 4.0:
            reasons.append("market_mid_model_ratio_extreme")
    if (
        inputs.vendor_underlier is not None
        and abs(inputs.vendor_underlier / inputs.spot - 1.0) > 0.002
    ):
        reasons.append("vendor_underlier_mismatch_over_20bps")

    delta_error = abs(inputs.vendor_delta - delta) if inputs.vendor_delta is not None else None
    if delta_error is not None and delta_error > 0.05:
        reasons.append("vendor_delta_mismatch_over_0_05")
    gamma_relative, gamma_absolute = _relative_vendor_error(inputs.vendor_gamma, gamma)
    if inputs.vendor_gamma is not None and (
        (abs(inputs.vendor_gamma) <= 1e-6 and gamma_absolute > 1e-6)
        or (
            abs(inputs.vendor_gamma) > 1e-6 and gamma_relative is not None and gamma_relative > 0.25
        )
    ):
        reasons.append("vendor_gamma_mismatch")
    theta_relative, theta_absolute = _relative_vendor_error(
        inputs.vendor_theta,
        theta_per_minute * 1440.0,
    )
    if theta_relative is not None and theta_absolute > 0.25 and theta_relative > 0.50:
        reasons.append("vendor_theta_mismatch")
    vega_relative, vega_absolute = _relative_vendor_error(inputs.vendor_vega, vega_per_vol_point)
    if vega_relative is not None and vega_absolute > 0.05 and vega_relative > 0.50:
        reasons.append("vendor_vega_mismatch")
    if stability > 0.20:
        reasons.append("finite_difference_unstable")
    return GreekQuality(
        status="degraded" if reasons else "ok",
        reasons=tuple(dict.fromkeys(reasons)),
        vendor_delta_error=delta_error,
        vendor_gamma_rel_error=gamma_relative,
        vendor_theta_rel_error=theta_relative,
        vendor_vega_rel_error=vega_relative,
        step_stability_max_rel_error=stability,
    )


def calculate_contract_reference(
    inputs: GreekInputs,
    *,
    steps: DifferenceSteps | None = None,
) -> ContractGreekReference:
    resolved_steps = steps or difference_steps(inputs)
    _validate_calculation_inputs(inputs, resolved_steps)
    values = _higher_order_values(inputs, resolved_steps)
    stability = _step_stability(inputs, resolved_steps, values)
    tau_years = inputs.tau_seconds / YEAR_SECONDS
    delta = bs_delta(inputs.spot, inputs.strike, inputs.iv, tau_years, inputs.right)
    gamma = bs_gamma(inputs.spot, inputs.strike, inputs.iv, tau_years)
    vega_per_vol_point = bs_vega(inputs.spot, inputs.strike, inputs.iv, tau_years) * 0.01
    quality = _contract_quality(
        inputs,
        delta=delta,
        gamma=gamma,
        theta_per_minute=values["theta_per_minute"],
        vega_per_vol_point=vega_per_vol_point,
        stability=stability,
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


def _calculate_reference_universe(
    quotes: Iterable[Quote],
    *,
    as_of: datetime,
    spot: float,
    storage_settings: StorageSettings,
) -> tuple[dict[str, GreekInputs], list[ContractGreekReference], dict[str, int]]:
    inputs_by_contract: dict[str, GreekInputs] = {}
    references: list[ContractGreekReference] = []
    blocked_counts: dict[str, int] = {}
    for quote in quotes:
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
    return inputs_by_contract, references, blocked_counts


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
    exact_expiry = (
        DEFAULT_MARKET_CALENDAR.research_expiry(as_of)
        if DEFAULT_MARKET_CALENDAR.is_spx_gth_open(as_of)
        else as_of.astimezone(ET).date()
    ).strftime("%Y%m%d")
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

    inputs_by_contract, references, blocked_counts = _calculate_reference_universe(
        exact_quotes,
        as_of=as_of,
        spot=spot,
        storage_settings=storage_settings,
    )
    if not references:
        payload = _unavailable_payload(as_of, exact_expiry, "no_usable_exact_expiry_references")
        payload["blocked_counts"] = blocked_counts
        return payload

    aggregate = aggregate_expiry_reference(
        references,
        inputs_by_contract=inputs_by_contract,
        total_contract_count=len(exact_quotes),
    )
    front = next(
        (row for row in options_map.expiries if str(getattr(row, "expiry", "")) == exact_expiry),
        None,
    )
    from spx_spark.greek_reference_payload import build_greek_reference_payload

    return build_greek_reference_payload(
        schema_version=SCHEMA_VERSION,
        model_name=MODEL_NAME,
        as_of=as_of,
        expiry=exact_expiry,
        spot=spot,
        spot_source=spot_source,
        spot_warnings=spot_warnings,
        exact_quote_count=len(exact_quotes),
        inputs_by_contract=inputs_by_contract,
        references=references,
        aggregate=aggregate,
        front=front,
        blocked_counts=blocked_counts,
        focus_contract_ids=focus_contract_ids,
        max_serialized_contracts=max_serialized_contracts,
        serialized_scenario_names=serialized_scenario_names,
    )


from spx_spark.greek_reference_io import (  # noqa: E402
    load_zero_dte_greeks_snapshots,
    summarize_zero_dte_greeks_session,
    write_zero_dte_greeks_snapshot,
)

__all__ = [
    "load_zero_dte_greeks_snapshots",
    "summarize_zero_dte_greeks_session",
    "write_zero_dte_greeks_snapshot",
]
