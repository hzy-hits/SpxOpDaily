"""Research-only wall ladder and option reference helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from spx_spark.analytics.options.pricing import finite_float
from spx_spark.application.order_map.candidates import (
    _find_option_quote,
    _front_expiry_quotes,
    _quote_greeks_ok,
    _quote_mid_structural,
)
from spx_spark.application.order_map.pricing import (
    YEAR_SECONDS,
    build_option_price_bs_projection,
    expiry_close_utc,
    project_option_price,
    round_to_tick,
    smile_slope_per_point,
)
from spx_spark.marketdata import OptionRight, Quote
from spx_spark.options_map import (
    BAD_QUALITIES,
    OptionsMap,
    median_strike_step,
    pair_by_strike,
    probability_for_level,
)
from spx_spark.sampling import round_to_step
from spx_spark.storage import LatestState, configured_quote_use_decision
from spx_spark.settings.order_map import DEFAULT_ORDER_MAP_POLICY, OrderMapPolicy


def _observed_option_reference(
    quotes: list[Quote],
    *,
    target_strike: int,
    right: str,
    strike_step: float,
    as_of: datetime,
) -> dict[str, object]:
    quote = _find_option_quote(
        quotes,
        target_strike=target_strike,
        right=right,
        strike_step=strike_step,
    )
    if quote is None:
        return {
            "contract_id": None,
            "observed_bid": None,
            "observed_ask": None,
            "quote_quality": None,
            "quote_freshness": None,
            "quote_reason": "quote_missing",
        }
    decision = configured_quote_use_decision(quote, as_of=as_of)
    research_usable = decision.research_usable
    return {
        "contract_id": quote.instrument.canonical_id,
        "observed_bid": finite_float(quote.bid) if research_usable else None,
        "observed_ask": finite_float(quote.ask) if research_usable else None,
        "quote_quality": (
            quote.quality.value if hasattr(quote.quality, "value") else str(quote.quality)
        ),
        "quote_freshness": decision.freshness.value,
        "quote_reason": decision.reason,
    }


def _research_candidates(
    state: LatestState,
    options_map: OptionsMap,
    *,
    research_price: float | None,
    as_of: datetime,
) -> list[dict[str, object]]:
    """Scenario locations with observed quotes, never executable math."""

    if not options_map.expiries:
        return []
    front = options_map.expiries[0]
    quotes = _front_expiry_quotes(state, front.expiry)
    pairs = pair_by_strike(quotes)
    strike_step = median_strike_step(sorted(pairs)) if pairs else 5.0
    strike_step_int = max(1, int(round(strike_step)))
    flip_level = front.gamma_flip_zone[0] if front.gamma_flip_zone is not None else front.zero_gamma
    scenarios = (
        (front.put_wall, "put_wall"),
        (flip_level, "flip"),
        (front.call_wall, "call_wall"),
    )
    payload: list[dict[str, object]] = []
    for level, level_kind in scenarios:
        if level is None:
            continue
        target_strike = round_to_step(level, strike_step_int)
        payload.append(
            {
                "level": level,
                "level_kind": level_kind,
                "distance_points": (
                    round(level - research_price, 1) if research_price is not None else None
                ),
                "strike": target_strike,
                "observed_options": [
                    {
                        "right": right,
                        **_observed_option_reference(
                            quotes,
                            target_strike=target_strike,
                            right=right,
                            strike_step=strike_step,
                            as_of=as_of,
                        ),
                    }
                    for right in ("C", "P")
                ],
            }
        )
    return payload


def _research_wall_ladder(
    state: LatestState,
    options_map: OptionsMap,
    *,
    research_price: float | None,
    as_of: datetime,
) -> dict[str, list[dict[str, object]]]:
    """Wall locations and observed markets with all model outputs omitted."""

    ladder: dict[str, list[dict[str, object]]] = {"call_walls": [], "put_walls": []}
    if not options_map.expiries:
        return ladder
    front = options_map.expiries[0]
    quotes = _front_expiry_quotes(state, front.expiry)
    pairs = pair_by_strike(quotes)
    strike_step = median_strike_step(sorted(pairs)) if pairs else 5.0
    strike_step_int = max(1, int(round(strike_step)))
    wall_groups: tuple[tuple[str, tuple[object, ...], float | None], ...] = (
        ("call_walls", tuple(front.call_walls), front.call_wall),
        ("put_walls", tuple(front.put_walls), front.put_wall),
    )
    for key, configured_walls, primary in wall_groups:
        walls = list(configured_walls)
        if not walls and primary is not None:
            walls = [None]
        for wall in walls:
            strike = finite_float(getattr(wall, "strike", primary))
            if strike is None:
                continue
            target_strike = round_to_step(strike, strike_step_int)
            ladder[key].append(
                {
                    "strike": strike,
                    "gex": finite_float(getattr(wall, "gex", None)),
                    "open_interest": finite_float(getattr(wall, "open_interest", None)),
                    "volume": finite_float(getattr(wall, "volume", None)),
                    "distance_points": (
                        round(strike - research_price, 1) if research_price is not None else None
                    ),
                    "option_strike": target_strike,
                    "observed_options": [
                        {
                            "right": right,
                            **_observed_option_reference(
                                quotes,
                                target_strike=target_strike,
                                right=right,
                                strike_step=strike_step,
                                as_of=as_of,
                            ),
                        }
                        for right in ("C", "P")
                    ],
                }
            )
    return ladder


def _wall_rung_option_ref(
    *,
    wall_strike: float,
    right: str,
    spot: float,
    expiry_quotes: list[Quote],
    pairs: dict[float, dict[OptionRight, Quote]],
    strike_step: float,
    tau_now_years: float | None,
    em_points: float | None,
    as_of: datetime | None = None,
    policy: OrderMapPolicy = DEFAULT_ORDER_MAP_POLICY,
) -> dict[str, Any]:
    """BS (or Taylor fallback) reference premium for the option at a wall strike.

    Put walls → Call (bounce); call walls → Put (fade). Same projection model
    as the primary plays so ladder rungs and conditional references agree.

    Only quotes allowed by the central pricing policy can produce projected
    premiums or executable limits. Research-only rows stay empty here.
    """
    strike_step_int = max(1, int(round(strike_step)))
    target_strike = round_to_step(wall_strike, strike_step_int)
    quote = _find_option_quote(
        expiry_quotes,
        target_strike=target_strike,
        right=right,
        strike_step=strike_step,
    )
    empty = {
        "right": right,
        "strike": target_strike,
        "current_mid": None,
        "projected_mid": None,
        "limit_aggressive": None,
        "limit_conservative": None,
        "projection_model": None,
        "projection_range_low": None,
        "projection_range_high": None,
        "projection_tau_now_minutes": None,
        "projection_tau_at_touch_minutes": None,
        "projection_touch_time_fraction": None,
        "projection_timing_capped": False,
        "quote_quality": None,
        "degraded": False,
    }
    if (
        quote is None
        or not configured_quote_use_decision(
            quote,
            as_of=as_of or datetime.now(tz=timezone.utc),
        ).pricing_allowed
        or not _quote_greeks_ok(quote)
    ):
        return empty
    mid = _quote_mid_structural(quote, as_of=as_of)
    delta = finite_float(quote.greeks.delta) if quote.greeks is not None else None  # type: ignore[union-attr]
    gamma = finite_float(quote.greeks.gamma) if quote.greeks is not None else None  # type: ignore[union-attr]
    if mid is None or delta is None or gamma is None:
        return empty
    strike_float = finite_float(quote.instrument.strike) or float(target_strike)
    iv = finite_float(quote.greeks.implied_vol) if quote.greeks is not None else None
    slope = smile_slope_per_point(pairs, right, strike_float, strike_step)
    bs_projection = build_option_price_bs_projection(
        mid=mid,
        iv=iv,
        strike=strike_float,
        right=right,
        spot=spot,
        target=wall_strike,
        tau_now_years=tau_now_years,
        em_points=em_points,
        slope_per_point=slope,
        policy=policy,
    )
    if bs_projection is not None:
        projected, model = bs_projection.projected_mid, "bs_repricing"
    else:
        projected, model = (
            project_option_price(mid, delta, gamma, spot, wall_strike),
            "taylor_fallback",
        )
    quality = quote.quality.value if hasattr(quote.quality, "value") else str(quote.quality)
    degraded = quote.quality in BAD_QUALITIES
    if degraded:
        model = f"{model}_stale"
    return {
        "right": right,
        "strike": int(round(strike_float)),
        "current_mid": mid,
        "projected_mid": projected,
        "limit_aggressive": round_to_tick(projected),
        "limit_conservative": round_to_tick(
            projected * policy.conservative_limit_multiplier
        ),
        "projection_model": model,
        "projection_range_low": (
            bs_projection.price_range_low if bs_projection is not None else projected
        ),
        "projection_range_high": (
            bs_projection.price_range_high if bs_projection is not None else projected
        ),
        "projection_tau_now_minutes": (
            bs_projection.tau_now_minutes if bs_projection is not None else None
        ),
        "projection_tau_at_touch_minutes": (
            bs_projection.tau_at_touch_minutes if bs_projection is not None else None
        ),
        "projection_touch_time_fraction": (
            bs_projection.touch_time_fraction if bs_projection is not None else None
        ),
        "projection_timing_capped": bool(
            bs_projection is not None
            and bs_projection.touch_time_fraction
            >= policy.touch_time_fraction_maximum - 1e-9
        ),
        "quote_quality": quality,
        "degraded": degraded,
    }


def _wall_ladder_payload(
    state: LatestState,
    options_map: OptionsMap,
    spot: float | None,
    *,
    now: datetime | None = None,
    policy: OrderMapPolicy = DEFAULT_ORDER_MAP_POLICY,
) -> dict[str, list[dict[str, Any]]]:
    """Top-4 call/put walls with touch probs + BS option reference prices.

    Put-wall rungs carry the matching Call premium (bounce); call-wall rungs
    carry the matching Put premium (fade). A single wall per side loses the
    structure: on 2026-07-07 the put side was a near-flat band (7460-7500) and
    price ground to 7479, ten points past the "the" put wall.
    """
    ladder: dict[str, list[dict[str, Any]]] = {"call_walls": [], "put_walls": []}
    if not options_map.expiries:
        return ladder
    front = options_map.expiries[0]
    expiry_quotes = _front_expiry_quotes(state, front.expiry)
    pairs = pair_by_strike(expiry_quotes)
    strike_step = median_strike_step(sorted(pairs)) if pairs else 5.0
    now_utc = now or datetime.now(tz=timezone.utc)
    tau_now_years: float | None = None
    close_utc = expiry_close_utc(front.expiry)
    if close_utc is not None:
        seconds_left = (close_utc - now_utc).total_seconds()
        if seconds_left > 0:
            tau_now_years = seconds_left / YEAR_SECONDS
    em_points = finite_float(front.expected_move_points)

    for key, walls, right in (
        ("call_walls", front.call_walls, "P"),
        ("put_walls", front.put_walls, "C"),
    ):
        for wall in walls:
            prob_touch = None
            option_ref: dict[str, Any] = {
                "right": right,
                "strike": None,
                "current_mid": None,
                "projected_mid": None,
                "limit_aggressive": None,
                "limit_conservative": None,
                "projection_model": None,
                "projection_range_low": None,
                "projection_range_high": None,
                "projection_tau_now_minutes": None,
                "projection_tau_at_touch_minutes": None,
                "projection_touch_time_fraction": None,
                "projection_timing_capped": False,
                "quote_quality": None,
                "degraded": False,
            }
            if spot is not None:
                _, prob_touch, _, _ = probability_for_level(
                    wall.strike,
                    underlier=spot,
                    pairs=pairs,
                    strike_step=strike_step,
                )
                option_ref = _wall_rung_option_ref(
                    wall_strike=wall.strike,
                    right=right,
                    spot=spot,
                    expiry_quotes=expiry_quotes,
                    pairs=pairs,
                    strike_step=strike_step,
                    tau_now_years=tau_now_years,
                    em_points=em_points,
                    as_of=now_utc,
                    policy=policy,
                )
            ladder[key].append(
                {
                    "strike": wall.strike,
                    "gex": wall.gex,
                    "open_interest": wall.open_interest,
                    "volume": wall.volume,
                    "distance_points": round(wall.strike - spot, 1) if spot is not None else None,
                    "prob_touch": prob_touch,
                    "option_right": option_ref.get("right"),
                    "option_strike": option_ref.get("strike"),
                    "current_mid": option_ref.get("current_mid"),
                    "projected_mid": option_ref.get("projected_mid"),
                    "limit_aggressive": option_ref.get("limit_aggressive"),
                    "limit_conservative": option_ref.get("limit_conservative"),
                    "projection_model": option_ref.get("projection_model"),
                    "projection_range_low": option_ref.get("projection_range_low"),
                    "projection_range_high": option_ref.get("projection_range_high"),
                    "projection_tau_now_minutes": option_ref.get(
                        "projection_tau_now_minutes"
                    ),
                    "projection_tau_at_touch_minutes": option_ref.get(
                        "projection_tau_at_touch_minutes"
                    ),
                    "projection_touch_time_fraction": option_ref.get(
                        "projection_touch_time_fraction"
                    ),
                    "projection_timing_capped": bool(
                        option_ref.get("projection_timing_capped")
                    ),
                    "quote_quality": option_ref.get("quote_quality"),
                    "degraded": bool(option_ref.get("degraded")),
                }
            )
    return ladder


def _index_value(state: LatestState, canonical_id: str) -> float | None:
    quote = state.best_quote(canonical_id)
    if quote is None or quote.quality in BAD_QUALITIES:
        return None
    return finite_float(quote.effective_price)
