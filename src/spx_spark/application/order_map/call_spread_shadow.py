"""Read-only Call/Put skew vertical selectors for the 15-minute report."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import median
from typing import Any

from spx_spark.analytics.greeks import bs_charm_per_minute
from spx_spark.analytics.greeks.black_scholes import bs_price
from spx_spark.analytics.options.pricing import finite_float, time_to_expiry_years
from spx_spark.application.order_map.execution_quote import (
    ExecutionQuoteGate,
    evaluate_execution_quote,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.marketdata import OptionRight, Quote
from spx_spark.options_map import is_spxw_option, median_strike_step
from spx_spark.settings.order_map import DEFAULT_ORDER_MAP_POLICY, OrderMapPolicy
from spx_spark.storage import LatestState


SCHEMA_VERSION = 1
MIN_ANCHOR_COUNT = 3
MAX_ANCHOR_COUNT = 5
MIN_LONG_DELTA = 0.25
MAX_LONG_DELTA = 0.65
MIN_SHORT_DELTA = 0.05
MAX_SHORT_DELTA = 0.40
MIN_IV_RICHNESS = 0.005
MIN_ADJACENT_IV_RICHNESS = 0.0025
MIN_SHORT_BID_RICHNESS_POINTS = 0.05
MIN_EXECUTABLE_EDGE_POINTS = 0.10
MAX_WIDTH_STEPS = 6.0
MAX_ANCHOR_DISTANCE_STEPS = 6.0
MAX_FIT_EXTRAPOLATION_STEPS = 2.0
MAX_LEG_TIME_SKEW_SECONDS = 5.0
MIN_EXECUTABLE_SIZE = 1.0


@dataclass(frozen=True)
class _Leg:
    quote: Quote
    gate: ExecutionQuoteGate
    strike: float
    iv: float
    delta: float
    gamma: float


@dataclass(frozen=True)
class _LocalIvFit:
    predicted_iv: float
    slope_per_point: float
    mad: float
    anchors: tuple[_Leg, ...]


def build_skew_spread_shadows(
    state: LatestState,
    *,
    expiry: str | None,
    spot: float | None,
    now: datetime,
    policy: OrderMapPolicy = DEFAULT_ORDER_MAP_POLICY,
) -> dict[str, dict[str, Any]]:
    return {
        "call_skew_spread_shadow": build_call_skew_spread_shadow(
            state,
            expiry=expiry,
            spot=spot,
            now=now,
            policy=policy,
        ),
        "put_skew_spread_shadow": build_put_skew_spread_shadow(
            state,
            expiry=expiry,
            spot=spot,
            now=now,
            policy=policy,
        ),
    }


def build_call_skew_spread_shadow(
    state: LatestState,
    *,
    expiry: str | None,
    spot: float | None,
    now: datetime,
    policy: OrderMapPolicy = DEFAULT_ORDER_MAP_POLICY,
) -> dict[str, Any]:
    return _build_skew_spread_shadow(
        state,
        expiry=expiry,
        spot=spot,
        now=now,
        right=OptionRight.CALL,
        policy=policy,
    )


def build_put_skew_spread_shadow(
    state: LatestState,
    *,
    expiry: str | None,
    spot: float | None,
    now: datetime,
    policy: OrderMapPolicy = DEFAULT_ORDER_MAP_POLICY,
) -> dict[str, Any]:
    return _build_skew_spread_shadow(
        state,
        expiry=expiry,
        spot=spot,
        now=now,
        right=OptionRight.PUT,
        policy=policy,
    )


def _build_skew_spread_shadow(
    state: LatestState,
    *,
    expiry: str | None,
    spot: float | None,
    now: datetime,
    right: OptionRight,
    policy: OrderMapPolicy,
) -> dict[str, Any]:
    """Select one conservative 1x/-1x vertical without creating an order."""

    side = "call" if right == OptionRight.CALL else "put"
    direction = 1.0 if right == OptionRight.CALL else -1.0

    base: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "kind": f"spxw_{side}_skew_spread_shadow",
        "right": right.value,
        "mode": "shadow",
        "automatic_ordering": False,
        "operator_action": "observe_only",
        "as_of": _utc(now).isoformat(),
        "expiry": expiry,
        "spot": _rounded(spot, 2),
        "status": "unavailable",
        "reason": None,
        "candidate": None,
        "diagnostics": {},
        "warnings": [
            "shadow_only_not_an_order",
            "combo_net_debit_limit_only_no_legging",
        ],
    }
    if not DEFAULT_MARKET_CALENDAR.is_rth_open(now):
        return _finish(base, reason="rth_only_live_spxw_required")
    if spot is None or spot <= 0:
        return _finish(base, reason="pricing_reference_unavailable")
    if not expiry:
        return _finish(base, reason="front_expiry_unavailable")
    expected_expiry = DEFAULT_MARKET_CALENDAR.research_expiry(now).strftime("%Y%m%d")
    if expiry != expected_expiry:
        return _finish(base, reason="front_expiry_not_current_0dte")

    all_options = [
        quote
        for quote in state.best_quotes
        if is_spxw_option(quote)
        and quote.instrument.right == right
        and quote.instrument.expiry == expiry
    ]
    comparable_quotes = tuple(
        quote
        for quote in state.quotes
        if is_spxw_option(quote) and quote.instrument.expiry == expiry
    )
    strike_step = median_strike_step(
        sorted(
            {
                strike
                for quote in all_options
                if (strike := finite_float(quote.instrument.strike)) is not None
            }
        )
    )
    rejects: Counter[str] = Counter()
    eligible: list[_Leg] = []
    for quote in all_options:
        leg, reasons = _eligible_leg(
            quote,
            all_quotes=comparable_quotes,
            spot=spot,
            now=now,
            policy=policy,
        )
        if leg is None:
            rejects.update(reasons)
        else:
            eligible.append(leg)
    eligible.sort(key=lambda item: item.strike)

    diagnostics: dict[str, Any] = {
        "option_quotes_seen": len(all_options),
        "executable_option_quotes": len(eligible),
        "strike_step": _rounded(strike_step, 2),
        "pairs_evaluated": 0,
        "reject_counts": dict(sorted(rejects.items())),
    }
    base["diagnostics"] = diagnostics
    if len(eligible) < MIN_ANCHOR_COUNT + 2:
        return _finish(
            base,
            status="no_candidate",
            reason="insufficient_executable_option_quotes",
        )

    tau_years = time_to_expiry_years(expiry, as_of=now)
    candidates: list[tuple[tuple[float, float, float], dict[str, Any]]] = []
    pair_rejects: Counter[str] = Counter()
    for short in eligible:
        if (short.strike - spot) * direction <= 0 or not (
            MIN_SHORT_DELTA <= abs(short.delta) <= MAX_SHORT_DELTA
        ):
            continue
        fit = _fit_local_iv(
            eligible,
            target_strike=short.strike,
            strike_step=strike_step,
            direction=direction,
        )
        if fit is None:
            pair_rejects["local_iv_fit_unavailable"] += 1
            continue
        required_richness = max(MIN_IV_RICHNESS, 3.0 * fit.mad)
        iv_richness = short.iv - fit.predicted_iv
        if iv_richness < required_richness:
            pair_rejects["short_iv_not_rich_enough"] += 1
            continue

        adjacent = _adjacent_confirmation(
            eligible,
            short=short,
            fit=fit,
            strike_step=strike_step,
            required_richness=required_richness,
            direction=direction,
        )
        if adjacent is None:
            pair_rejects["adjacent_wing_confirmation_unavailable"] += 1
            continue

        fair_short_mid = _fair_short_mid(
            short,
            fair_iv=fit.predicted_iv,
            spot=spot,
            tau_years=tau_years,
            right=right,
        )
        if fair_short_mid is None:
            pair_rejects["fair_short_price_unavailable"] += 1
            continue
        short_bid = finite_float(short.quote.bid)
        if short_bid is None or short_bid - fair_short_mid < MIN_SHORT_BID_RICHNESS_POINTS:
            pair_rejects["short_bid_not_rich_enough"] += 1
            continue

        for long in fit.anchors:
            width = (short.strike - long.strike) * direction
            if not (
                strike_step <= width <= strike_step * MAX_WIDTH_STEPS
                and MIN_LONG_DELTA <= abs(long.delta) <= MAX_LONG_DELTA
            ):
                continue
            diagnostics["pairs_evaluated"] += 1
            if long.quote.provider != short.quote.provider:
                pair_rejects["spread_leg_provider_mismatch"] += 1
                continue
            if not _legs_are_synchronized(long.quote, short.quote):
                pair_rejects["spread_leg_time_skew_exceeded"] += 1
                continue
            long_ask = finite_float(long.quote.ask)
            long_mid = finite_float(long.quote.mid)
            if long_ask is None or long_mid is None:
                pair_rejects["spread_leg_nbbo_invalid"] += 1
                continue
            executable_debit = long_ask - short_bid
            fair_debit = long_mid - fair_short_mid
            edge = fair_debit - executable_debit
            if executable_debit <= 0:
                pair_rejects["non_positive_debit_anomaly"] += 1
                continue
            if executable_debit >= width:
                pair_rejects["debit_exceeds_vertical_width"] += 1
                continue
            if edge < MIN_EXECUTABLE_EDGE_POINTS:
                pair_rejects["executable_edge_below_minimum"] += 1
                continue

            candidate = _candidate_payload(
                long=long,
                short=short,
                adjacent=adjacent,
                fit=fit,
                spot=spot,
                tau_years=tau_years,
                fair_short_mid=fair_short_mid,
                executable_debit=executable_debit,
                fair_debit=fair_debit,
                edge=edge,
                required_richness=required_richness,
                right=right,
                direction=direction,
            )
            score = (edge / executable_debit, edge, -executable_debit)
            candidates.append((score, candidate))

    diagnostics["reject_counts"] = dict(sorted((rejects + pair_rejects).items()))
    if not candidates:
        return _finish(
            base,
            status="no_candidate",
            reason="no_positive_executable_skew_edge",
        )

    _score, selected = max(candidates, key=lambda item: item[0])
    base.update(
        {
            "status": "candidate",
            "reason": "positive_executable_skew_edge",
            "candidate": selected,
        }
    )
    return base


def compact_skew_spread_shadow_line(payload: dict[str, Any]) -> str | None:
    parts = [
        line
        for label, key in (
            ("Call", "call_skew_spread_shadow"),
            ("Put", "put_skew_spread_shadow"),
        )
        if isinstance((shadow := payload.get(key)), dict)
        and (line := _compact_side_shadow(shadow, label))
    ]
    return "；".join(parts) if parts else None


def compact_call_spread_shadow_line(payload: dict[str, Any]) -> str | None:
    """Compatibility alias for the original Call-only renderer."""

    return compact_skew_spread_shadow_line(payload)


def _compact_side_shadow(shadow: dict[str, Any], label: str) -> str:
    status = str(shadow.get("status") or "unavailable")
    candidate = shadow.get("candidate")
    if status == "candidate" and isinstance(candidate, dict):
        long = candidate.get("long") if isinstance(candidate.get("long"), dict) else {}
        short = candidate.get("short") if isinstance(candidate.get("short"), dict) else {}
        right = str(long.get("right") or label[0])
        return (
            f"{label} Spread Shadow  "
            f"{_strike_text(long.get('strike'))}{right}/"
            f"{_strike_text(short.get('strike'))}{right}　"
            f"净借记 {_display(candidate.get('executable_debit'))}　"
            f"边际 {_display(candidate.get('edge_points'))} 点　只读"
        )
    reason = str(shadow.get("reason") or "unavailable")
    status_label = "无候选" if status == "no_candidate" else "不可用"
    return f"{label} Spread Shadow  {status_label}（{_reason_label(reason)}）　只读"


def skew_spread_shadow_detail_lines(payload: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    for label, key in (
        ("Call", "call_skew_spread_shadow"),
        ("Put", "put_skew_spread_shadow"),
    ):
        shadow = payload.get(key)
        if not isinstance(shadow, dict):
            continue
        lines.extend((f"### {label}", *_side_shadow_detail_lines(shadow, label)))
    return lines


def call_spread_shadow_detail_lines(payload: dict[str, Any]) -> list[str]:
    """Compatibility alias for the original Call-only renderer."""

    return skew_spread_shadow_detail_lines(payload)


def _side_shadow_detail_lines(shadow: dict[str, Any], side_label: str) -> list[str]:
    status = str(shadow.get("status") or "unavailable")
    diagnostics = shadow.get("diagnostics") if isinstance(shadow.get("diagnostics"), dict) else {}
    candidate = shadow.get("candidate")
    if status != "candidate" or not isinstance(candidate, dict):
        label = "无候选" if status == "no_candidate" else "不可用"
        return [
            f"**状态**　{label}（{_reason_label(str(shadow.get('reason') or 'unavailable'))}）",
            (
                "**覆盖**　"
                f"看到 {side_label} {diagnostics.get('option_quotes_seen') or 0} 张；"
                "通过双边/时效/价差/尺寸门控 "
                f"{diagnostics.get('executable_option_quotes') or 0} 张"
            ),
            "**边界**　仅 RTH 实时 SPXW；Shadow 只读，不自动下单",
        ]

    long = candidate.get("long") if isinstance(candidate.get("long"), dict) else {}
    short = candidate.get("short") if isinstance(candidate.get("short"), dict) else {}
    fit = candidate.get("iv_fit") if isinstance(candidate.get("iv_fit"), dict) else {}
    greeks = candidate.get("net_greeks") if isinstance(candidate.get("net_greeks"), dict) else {}
    risk = candidate.get("defined_risk") if isinstance(candidate.get("defined_risk"), dict) else {}
    right = str(long.get("right") or side_label[0])
    return [
        (
            "**Shadow 候选**　"
            f"买 {_strike_text(long.get('strike'))}{right} / "
            f"卖 {_strike_text(short.get('strike'))}{right}；"
            "只读，不自动下单"
        ),
        (
            "**可执行组合**　"
            f"买腿 ask {_display(long.get('ask'))}，卖腿 bid {_display(short.get('bid'))}；"
            f"净借记 {_display(candidate.get('executable_debit'))}，"
            f"公允借记 {_display(candidate.get('fair_debit'))}，"
            f"边际 {_display(candidate.get('edge_points'))} 点"
        ),
        (
            "**Skew 审计**　"
            f"卖腿 IV {_percent(short.get('iv'))}，局部拟合 {_percent(fit.get('fair_short_iv'))}，"
            f"偏离 +{_display(fit.get('short_iv_richness_vol_points'))} vol；"
            f"相邻 {_strike_text(fit.get('adjacent_confirmation_strike'))}{right} 确认"
        ),
        (
            "**定义风险**　"
            f"最大亏损 ${_money(risk.get('max_loss_usd'))}，"
            f"最大收益 ${_money(risk.get('max_profit_usd'))}，"
            f"盈亏平衡 {_display(risk.get('breakeven_spx'))}"
        ),
        (
            "**净 Greeks**　"
            f"Delta {_signed(greeks.get('delta'), 3)}　"
            f"Gamma {_signed(greeks.get('gamma'), 5)}　"
            f"Charm/分钟 {_signed(greeks.get('charm_per_minute'), 6)}"
        ),
        "**执行边界**　仅组合净借记限价；禁止拆腿；两腿需持续满足同源、同步、双边和尺寸门控",
    ]


def _eligible_leg(
    quote: Quote,
    *,
    all_quotes: tuple[Quote, ...],
    spot: float,
    now: datetime,
    policy: OrderMapPolicy,
) -> tuple[_Leg | None, tuple[str, ...]]:
    strike = finite_float(quote.instrument.strike)
    iv = finite_float(quote.greeks.implied_vol) if quote.greeks is not None else None
    delta = finite_float(quote.greeks.delta) if quote.greeks is not None else None
    gamma = finite_float(quote.greeks.gamma) if quote.greeks is not None else None
    reasons: list[str] = []
    if strike is None or strike <= 0:
        reasons.append("strike_unavailable")
    if iv is None or iv <= 0:
        reasons.append("iv_unavailable")
    if delta is None or not 0.03 <= abs(delta) <= 0.80:
        reasons.append("delta_outside_selector_range")
    if gamma is None or gamma < 0:
        reasons.append("gamma_unavailable")
    if finite_float(quote.ask_size) is None or float(quote.ask_size or 0) < MIN_EXECUTABLE_SIZE:
        reasons.append("ask_size_unavailable")
    if finite_float(quote.bid_size) is None or float(quote.bid_size or 0) < MIN_EXECUTABLE_SIZE:
        reasons.append("bid_size_unavailable")
    model_underlier = (
        finite_float(quote.greeks.underlier_price) if quote.greeks is not None else None
    )
    if (
        model_underlier is not None
        and abs(model_underlier - spot) > policy.execution_max_provider_underlier_divergence_points
    ):
        reasons.append("model_underlier_divergence")
    source_at = quote.quote_time or quote.trade_time
    transport_at = quote.last_update_at or quote.received_at
    if source_at is not None and (_utc(now) - _utc(source_at)).total_seconds() < -1.0:
        reasons.append("source_quote_in_future")
    if (_utc(now) - _utc(transport_at)).total_seconds() < -1.0:
        reasons.append("transport_quote_in_future")
    gate = evaluate_execution_quote(quote, all_quotes, as_of=now, policy=policy)
    reasons.extend(gate.reasons)
    if reasons or None in (strike, iv, delta, gamma):
        return None, tuple(dict.fromkeys(reasons))
    return (
        _Leg(
            quote=quote,
            gate=gate,
            strike=float(strike),
            iv=float(iv),
            delta=float(delta),
            gamma=float(gamma),
        ),
        (),
    )


def _fit_local_iv(
    eligible: list[_Leg],
    *,
    target_strike: float,
    strike_step: float,
    direction: float,
) -> _LocalIvFit | None:
    anchors = sorted(
        (
            leg
            for leg in eligible
            if (target_strike - leg.strike) * direction > 0
            and abs(target_strike - leg.strike) <= strike_step * MAX_ANCHOR_DISTANCE_STEPS
        ),
        key=lambda leg: abs(target_strike - leg.strike),
    )[:MAX_ANCHOR_COUNT]
    anchors.sort(key=lambda leg: leg.strike)
    if len(anchors) < MIN_ANCHOR_COUNT:
        return None
    if min(abs(target_strike - leg.strike) for leg in anchors) > (
        strike_step * MAX_FIT_EXTRAPOLATION_STEPS
    ):
        return None
    slopes = [
        (right.iv - left.iv) / (right.strike - left.strike)
        for index, left in enumerate(anchors)
        for right in anchors[index + 1 :]
        if right.strike != left.strike
    ]
    if not slopes:
        return None
    slope = median(slopes)
    center = median([leg.strike for leg in anchors])
    intercept = median([leg.iv - slope * (leg.strike - center) for leg in anchors])
    predicted = intercept + slope * (target_strike - center)
    if not 0.01 <= predicted <= 5.0:
        return None
    residuals = [abs(leg.iv - (intercept + slope * (leg.strike - center))) for leg in anchors]
    return _LocalIvFit(
        predicted_iv=predicted,
        slope_per_point=slope,
        mad=median(residuals),
        anchors=tuple(anchors),
    )


def _adjacent_confirmation(
    eligible: list[_Leg],
    *,
    short: _Leg,
    fit: _LocalIvFit,
    strike_step: float,
    required_richness: float,
    direction: float,
) -> _Leg | None:
    center = median([leg.strike for leg in fit.anchors])
    intercept = median(
        [leg.iv - fit.slope_per_point * (leg.strike - center) for leg in fit.anchors]
    )
    for leg in eligible:
        wing_distance = (leg.strike - short.strike) * direction
        if not 0 < wing_distance <= 2.0 * strike_step:
            continue
        predicted = intercept + fit.slope_per_point * (leg.strike - center)
        if leg.iv - predicted >= max(MIN_ADJACENT_IV_RICHNESS, required_richness * 0.5):
            return leg
    return None


def _fair_short_mid(
    short: _Leg,
    *,
    fair_iv: float,
    spot: float,
    tau_years: float,
    right: OptionRight,
) -> float | None:
    market_model = bs_price(spot, short.strike, short.iv, tau_years, right.value)
    fair_model = bs_price(spot, short.strike, fair_iv, tau_years, right.value)
    market_mid = finite_float(short.quote.mid)
    if market_model <= 0 or market_mid is None or market_mid <= 0:
        return None
    fair_mid = market_mid * fair_model / market_model
    return fair_mid if fair_mid >= 0 else None


def _candidate_payload(
    *,
    long: _Leg,
    short: _Leg,
    adjacent: _Leg,
    fit: _LocalIvFit,
    spot: float,
    tau_years: float,
    fair_short_mid: float,
    executable_debit: float,
    fair_debit: float,
    edge: float,
    required_richness: float,
    right: OptionRight,
    direction: float,
) -> dict[str, Any]:
    width = (short.strike - long.strike) * direction
    displayed_debit = round(executable_debit, 2)
    displayed_edge = round(edge, 2)
    long_charm = bs_charm_per_minute(spot, long.strike, long.iv, tau_years)
    short_charm = bs_charm_per_minute(spot, short.strike, short.iv, tau_years)
    long_theta = finite_float(long.quote.greeks.theta) if long.quote.greeks else None
    short_theta = finite_float(short.quote.greeks.theta) if short.quote.greeks else None
    long_vega = finite_float(long.quote.greeks.vega) if long.quote.greeks else None
    short_vega = finite_float(short.quote.greeks.vega) if short.quote.greeks else None
    return {
        "strategy": "long_call_vertical" if right == OptionRight.CALL else "long_put_vertical",
        "quantity": "1x/-1x",
        "long": _leg_payload(long),
        "short": _leg_payload(short),
        "width_points": _rounded(width, 2),
        "executable_debit": displayed_debit,
        "fair_debit": _rounded(fair_debit, 2),
        "edge_points": displayed_edge,
        "edge_usd": _rounded(displayed_edge * 100.0, 2),
        "iv_fit": {
            "method": (
                "local_theil_sen_lower_strike_calls"
                if right == OptionRight.CALL
                else "local_theil_sen_higher_strike_puts"
            ),
            "anchor_contract_ids": [leg.quote.instrument.canonical_id for leg in fit.anchors],
            "anchor_strikes": [_rounded(leg.strike, 2) for leg in fit.anchors],
            "anchor_count": len(fit.anchors),
            "slope_vol_points_per_spx_point": _rounded(fit.slope_per_point * 100.0, 5),
            "fit_mad_vol_points": _rounded(fit.mad * 100.0, 3),
            "fair_short_iv": _rounded(fit.predicted_iv, 6),
            "observed_short_iv": _rounded(short.iv, 6),
            "short_iv_richness_vol_points": _rounded(
                (short.iv - fit.predicted_iv) * 100.0,
                3,
            ),
            "required_richness_vol_points": _rounded(required_richness * 100.0, 3),
            "fair_short_mid": _rounded(fair_short_mid, 2),
            "short_bid_richness_points": _rounded(
                float(short.quote.bid or 0.0) - fair_short_mid,
                2,
            ),
            "adjacent_confirmation_contract_id": adjacent.quote.instrument.canonical_id,
            "adjacent_confirmation_strike": _rounded(adjacent.strike, 2),
        },
        "liquidity_relation": _liquidity_relation(long, short),
        "net_greeks": {
            "delta": _rounded(long.delta - short.delta, 6),
            "gamma": _rounded(long.gamma - short.gamma, 8),
            "theta": _difference(long_theta, short_theta, 6),
            "vega": _difference(long_vega, short_vega, 6),
            "charm_per_minute": _difference(long_charm, short_charm, 9),
        },
        "defined_risk": {
            "max_loss_usd": _rounded(displayed_debit * 100.0, 2),
            "max_profit_usd": _rounded((width - displayed_debit) * 100.0, 2),
            "breakeven_spx": _rounded(long.strike + direction * displayed_debit, 2),
        },
        "execution": {
            "order_style": "combo_net_debit_limit_shadow",
            "net_debit_reference": displayed_debit,
            "leg_orders_prohibited": True,
            "max_leg_time_skew_seconds": MAX_LEG_TIME_SKEW_SECONDS,
            "automatic_ordering": False,
        },
    }


def _leg_payload(leg: _Leg) -> dict[str, Any]:
    source_at = leg.quote.quote_time or leg.quote.trade_time
    transport_at = leg.quote.last_update_at or leg.quote.received_at
    return {
        "contract_id": leg.quote.instrument.canonical_id,
        "strike": _rounded(leg.strike, 2),
        "right": leg.quote.instrument.right.value if leg.quote.instrument.right else None,
        "provider": leg.quote.provider.value,
        "bid": _rounded(leg.quote.bid, 2),
        "ask": _rounded(leg.quote.ask, 2),
        "mid": _rounded(leg.quote.mid, 2),
        "bid_size": _rounded(leg.quote.bid_size, 0),
        "ask_size": _rounded(leg.quote.ask_size, 0),
        "spread_points": _rounded(leg.gate.spread_points, 2),
        "spread_bps": _rounded(leg.gate.spread_bps, 1),
        "iv": _rounded(leg.iv, 6),
        "delta": _rounded(leg.delta, 6),
        "gamma": _rounded(leg.gamma, 8),
        "source_at": source_at.isoformat() if source_at else None,
        "transport_at": transport_at.isoformat(),
    }


def _legs_are_synchronized(long: Quote, short: Quote) -> bool:
    long_source = long.quote_time or long.trade_time
    short_source = short.quote_time or short.trade_time
    if long_source is None or short_source is None:
        return False
    long_transport = long.last_update_at or long.received_at
    short_transport = short.last_update_at or short.received_at
    return (
        abs((_utc(long_source) - _utc(short_source)).total_seconds()) <= MAX_LEG_TIME_SKEW_SECONDS
        and abs((_utc(long_transport) - _utc(short_transport)).total_seconds())
        <= MAX_LEG_TIME_SKEW_SECONDS
    )


def _liquidity_relation(long: _Leg, short: _Leg) -> str:
    long_bps = long.gate.spread_bps or 0.0
    short_bps = short.gate.spread_bps or 0.0
    long_size = min(float(long.quote.bid_size or 0.0), float(long.quote.ask_size or 0.0))
    short_size = min(float(short.quote.bid_size or 0.0), float(short.quote.ask_size or 0.0))
    if short_bps >= long_bps * 1.25 or short_size < long_size:
        return "short_thinner_but_executable"
    return "both_legs_executable"


def _finish(
    payload: dict[str, Any],
    *,
    reason: str,
    status: str = "unavailable",
) -> dict[str, Any]:
    payload["status"] = status
    payload["reason"] = reason
    return payload


def _difference(left: float | None, right: float | None, digits: int) -> float | None:
    if left is None or right is None:
        return None
    return _rounded(left - right, digits)


def _rounded(value: object, digits: int) -> float | None:
    number = finite_float(value)
    return round(number, digits) if number is not None else None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _display(value: object) -> str:
    number = finite_float(value)
    return f"{number:.2f}" if number is not None else "-"


def _strike_text(value: object) -> str:
    number = finite_float(value)
    if number is None:
        return "-"
    return f"{number:.1f}".removesuffix(".0")


def _percent(value: object) -> str:
    number = finite_float(value)
    return f"{number:.2%}" if number is not None else "-"


def _money(value: object) -> str:
    number = finite_float(value)
    return f"{number:,.0f}" if number is not None else "-"


def _signed(value: object, digits: int) -> str:
    number = finite_float(value)
    return f"{number:+.{digits}f}" if number is not None else "-"


def _reason_label(reason: str) -> str:
    return {
        "rth_only_live_spxw_required": "仅 RTH 实时 SPXW 双边链",
        "pricing_reference_unavailable": "SPX 定价参考不可用",
        "front_expiry_unavailable": "前月到期日不可用",
        "front_expiry_not_current_0dte": "不是当前 0DTE 到期日",
        "insufficient_executable_option_quotes": "可执行期权覆盖不足",
        "no_positive_executable_skew_edge": "扣除双腿价差后没有正边际",
        "positive_executable_skew_edge": "存在可执行正边际",
    }.get(reason, reason)


def skew_spread_shadow_identity(payload: dict[str, Any]) -> str:
    identities: list[str] = []
    for side, key in (
        ("C", "call_skew_spread_shadow"),
        ("P", "put_skew_spread_shadow"),
    ):
        shadow = payload.get(key)
        if not isinstance(shadow, dict):
            continue
        status = str(shadow.get("status") or "unavailable")
        candidate = shadow.get("candidate")
        if status != "candidate" or not isinstance(candidate, dict):
            identities.append(f"{side}:{status}:{shadow.get('reason') or '-'}")
            continue
        long = candidate.get("long") if isinstance(candidate.get("long"), dict) else {}
        short = candidate.get("short") if isinstance(candidate.get("short"), dict) else {}
        identities.append(
            f"{side}:candidate:{long.get('contract_id') or long.get('strike') or '-'}:"
            f"{short.get('contract_id') or short.get('strike') or '-'}"
        )
    return "|".join(identities)


def skew_spread_shadow_material_change(previous: str, current: str) -> str | None:
    if previous == current or not (previous or current):
        return None
    prior_candidates = {part[0]: part for part in previous.split("|") if ":candidate:" in part}
    current_candidates = {part[0]: part for part in current.split("|") if ":candidate:" in part}
    labels = {"C": "Call", "P": "Put"}
    updated = {
        side
        for side in prior_candidates.keys() & current_candidates.keys()
        if prior_candidates[side] != current_candidates[side]
    }
    established = current_candidates.keys() - prior_candidates.keys()
    expired = prior_candidates.keys() - current_candidates.keys()
    parts = [f"{labels[side]} 候选更新" for side in sorted(updated)]
    parts.extend(f"{labels[side]} 候选建立" for side in sorted(established))
    parts.extend(f"{labels[side]} 候选失效" for side in sorted(expired))
    return "Skew Spread Shadow " + "、".join(parts) if parts else None
