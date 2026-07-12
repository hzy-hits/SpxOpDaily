from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from spx_spark.analytics.options.pricing import finite_float
from spx_spark.application.order_map.models import (
    FRONTRUN_FRACTION,
    FRONTRUN_MAX_POINTS,
    FRONTRUN_MIN_POINTS,
    PLAY_ORDER,
    OrderCandidate,
    SpotResolution,
)
from spx_spark.application.order_map.pricing import (
    YEAR_SECONDS,
    expiry_close_utc,
    project_option_price,
    project_option_price_bs,
    round_to_tick,
    smile_slope_per_point,
    touch_eta_minutes,
)
from spx_spark.application.order_map.spot import resolve_spx_spot
from spx_spark.config import NY_TZ
from spx_spark.intraday_strategy import (
    CALL_WALL_BREAKOUT_CALL_KIND,
    FLIP_RECLAIM_CALL_KIND,
)
from spx_spark.marketdata import OptionRight, Quote
from spx_spark.options_map import (
    OptionsMap,
    is_spxw_option,
    median_strike_step,
    option_mid,
    pair_by_strike,
    probability_for_level,
)
from spx_spark.sampling import round_to_step
from spx_spark.storage import LatestState, configured_quote_use_decision


def frontrun_level_for(spot: float, level: float) -> float | None:
    """Level shifted from the target back toward spot by a capped fraction."""
    distance = abs(spot - level)
    if distance <= FRONTRUN_MIN_POINTS:
        return None
    offset = min(max(FRONTRUN_FRACTION * distance, FRONTRUN_MIN_POINTS), FRONTRUN_MAX_POINTS)
    direction = 1.0 if spot > level else -1.0
    return round(level + direction * offset, 1)


def _dash(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.1f}".removesuffix(".0")
    return str(value)


def _fmt_premium(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.2f}"


def _fmt_prob(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.0%}"


def _fmt_eta_minutes(minutes: float) -> str:
    if minutes >= 90.0:
        return f"{minutes / 60.0:.1f} 小时".replace(".0 ", " ")
    return f"{minutes:.0f} 分钟"


def _quote_greeks_ok(quote: Quote) -> bool:
    if quote.greeks is None:
        return False
    delta = finite_float(quote.greeks.delta)
    gamma = finite_float(quote.greeks.gamma)
    return delta is not None and gamma is not None


def _quote_mid(quote: Quote, *, as_of: datetime) -> float | None:
    if not configured_quote_use_decision(quote, as_of=as_of).pricing_allowed:
        return None
    return option_mid(quote) or quote.effective_price


def _quote_mid_structural(quote: Quote, *, as_of: datetime | None = None) -> float | None:
    """Actionable mid for ladder repricing; research-only quotes stay unpriced."""
    decision = configured_quote_use_decision(
        quote,
        as_of=as_of or datetime.now(tz=timezone.utc),
    )
    if not decision.pricing_allowed:
        return None
    return option_mid(quote) or quote.effective_price


def _front_expiry_quotes(state: LatestState, expiry: str) -> list[Quote]:
    return [
        quote
        for quote in state.best_quotes
        if is_spxw_option(quote) and (quote.instrument.expiry or "unknown") == expiry
    ]


def _find_option_quote(
    quotes: list[Quote],
    *,
    target_strike: int,
    right: str,
    strike_step: float,
) -> Quote | None:
    right_enum = OptionRight.CALL if right == "C" else OptionRight.PUT
    candidates: list[tuple[float, Quote]] = []
    max_distance = strike_step
    for quote in quotes:
        if quote.instrument.right != right_enum:
            continue
        strike = finite_float(quote.instrument.strike)
        if strike is None:
            continue
        distance = abs(strike - target_strike)
        if distance <= max_distance:
            candidates.append((distance, quote))
    if not candidates:
        return None
    return min(candidates, key=lambda item: item[0])[1]


def _build_candidate(
    *,
    play: str,
    level: float,
    level_label: str,
    target_strike: int,
    right: str,
    spot: float,
    expiry_quotes: list[Quote],
    strike_step: float,
    pairs: dict[float, dict[OptionRight, Quote]],
    warnings: list[str],
    as_of: datetime,
    tau_now_years: float | None = None,
    em_points: float | None = None,
) -> OrderCandidate | None:
    quote = _find_option_quote(
        expiry_quotes,
        target_strike=target_strike,
        right=right,
        strike_step=strike_step,
    )
    if quote is None:
        warnings.append(f"no_quote_for_{target_strike}{right}")
        return None
    decision = configured_quote_use_decision(quote, as_of=as_of)
    if not decision.pricing_allowed:
        warnings.append(f"bad_quality_for_{target_strike}{right}:{decision.reason}")
        return None
    if not _quote_greeks_ok(quote):
        warnings.append(f"missing_greeks_for_{target_strike}{right}")
        return None

    mid = _quote_mid(quote, as_of=as_of)
    if mid is None:
        warnings.append(f"no_mid_for_{target_strike}{right}")
        return None

    delta = finite_float(quote.greeks.delta)  # type: ignore[union-attr]
    gamma = finite_float(quote.greeks.gamma)  # type: ignore[union-attr]
    if delta is None or gamma is None:
        warnings.append(f"missing_greeks_for_{target_strike}{right}")
        return None

    strike_float = finite_float(quote.instrument.strike) or float(target_strike)
    iv = finite_float(quote.greeks.implied_vol) if quote.greeks is not None else None
    slope = smile_slope_per_point(pairs, right, strike_float, strike_step)

    def _project(target: float) -> tuple[float, str]:
        bs_projected = project_option_price_bs(
            mid=mid,
            iv=iv,
            strike=strike_float,
            right=right,
            spot=spot,
            target=target,
            tau_now_years=tau_now_years,
            em_points=em_points,
            slope_per_point=slope,
        )
        if bs_projected is not None:
            return bs_projected, "bs_repricing"
        return project_option_price(mid, delta, gamma, spot, target), "taylor_fallback"

    projected, projection_model = _project(level)
    if projection_model == "taylor_fallback":
        warnings.append(f"taylor_fallback_for_{target_strike}{right}")
    prob_close, prob_touch, _source_strike, _source_delta = probability_for_level(
        level,
        underlier=spot,
        pairs=pairs,
        strike_step=strike_step,
    )

    frontrun_level = frontrun_level_for(spot, level)
    frontrun_projected = None
    frontrun_limit = None
    frontrun_prob_touch = None
    if frontrun_level is not None:
        frontrun_projected, _ = _project(frontrun_level)
        frontrun_limit = round_to_tick(frontrun_projected)
        _, frontrun_prob_touch, _, _ = probability_for_level(
            frontrun_level,
            underlier=spot,
            pairs=pairs,
            strike_step=strike_step,
        )

    order_style = "stop_trigger" if projected > mid else "resting_limit"
    eta_minutes = touch_eta_minutes(abs(level - spot), em_points, tau_now_years)

    strike_value = int(round(finite_float(quote.instrument.strike) or target_strike))
    return OrderCandidate(
        play=play,
        level=level,
        level_label=level_label,
        contract_id=quote.instrument.canonical_id,
        strike=strike_value,
        right=right,
        current_mid=mid,
        projected_mid=projected,
        limit_aggressive=round_to_tick(projected),
        limit_conservative=round_to_tick(projected * 0.85),
        prob_touch=prob_touch,
        prob_close_beyond=prob_close,
        delta=delta,
        gamma=gamma,
        frontrun_level=frontrun_level,
        frontrun_projected_mid=frontrun_projected,
        frontrun_limit=frontrun_limit,
        frontrun_prob_touch=frontrun_prob_touch,
        order_style=order_style,
        projection_model=projection_model,
        touch_eta_minutes=round(eta_minutes, 1) if eta_minutes is not None else None,
    )


def build_candidates(
    state: LatestState,
    options_map: OptionsMap,
    warnings: list[str] | None = None,
    *,
    now: datetime | None = None,
    resolution: SpotResolution | None = None,
    conditional_call_bias: dict[str, object] | None = None,
) -> list[OrderCandidate]:
    local_warnings = warnings if warnings is not None else []
    if not options_map.expiries:
        local_warnings.append("missing expiries")
        return []

    front = options_map.expiries[0]
    expiry_quotes = _front_expiry_quotes(state, front.expiry)
    pairs = pair_by_strike(expiry_quotes)
    strikes = sorted(pairs)
    strike_step = median_strike_step(strikes)
    strike_step_int = max(1, int(round(strike_step)))

    now_utc = now or state.as_of
    resolution = resolution or resolve_spx_spot(
        state, options_map, warnings=local_warnings, now=now_utc
    )
    spot = resolution.pricing_price if resolution.pricing_allowed else None
    if spot is None:
        local_warnings.append("missing underlier price")
        return []

    tau_now_years: float | None = None
    close_utc = expiry_close_utc(front.expiry)
    if close_utc is not None:
        seconds_left = (close_utc - now_utc).total_seconds()
        if seconds_left > 0:
            tau_now_years = seconds_left / YEAR_SECONDS
    em_points = finite_float(front.expected_move_points)

    candidates: list[OrderCandidate] = []

    # Side guards: options_map constrains walls against its own underlier, but
    # the projection spot here can differ (chain parity vs perp). A "bounce"
    # level above spot or a "breakdown" level above spot is a stale/nonsense
    # play — the 2026-07-07 session pushed a 7570 breakdown put with spot at
    # 7490 exactly this way.
    put_wall_level = front.put_wall
    if put_wall_level is not None and put_wall_level > spot + strike_step:
        local_warnings.append(f"put_wall_{_dash(put_wall_level)}_above_spot_skipped")
        put_wall_level = next(
            (wall.strike for wall in front.put_walls if wall.strike <= spot + strike_step),
            None,
        )

    if put_wall_level is not None:
        target_strike = round_to_step(put_wall_level, strike_step_int)
        candidate = _build_candidate(
            play="put_wall_bounce_call",
            level=put_wall_level,
            level_label=f"put wall {_dash(put_wall_level)}",
            target_strike=target_strike,
            right="C",
            spot=spot,
            expiry_quotes=expiry_quotes,
            strike_step=strike_step,
            pairs=pairs,
            warnings=local_warnings,
            as_of=now_utc,
            tau_now_years=tau_now_years,
            em_points=em_points,
        )
        if candidate is not None:
            candidates.append(candidate)

    bias_play = str((conditional_call_bias or {}).get("play") or "")
    bias_level = finite_float((conditional_call_bias or {}).get("level"))
    bias_expiry = str((conditional_call_bias or {}).get("expiry") or "")
    bias_invalidation = finite_float((conditional_call_bias or {}).get("invalidation_level"))
    bias_valid = bool(
        (conditional_call_bias or {}).get("status") == "confirmed"
        and bias_play in {FLIP_RECLAIM_CALL_KIND, CALL_WALL_BREAKOUT_CALL_KIND}
        and bias_level is not None
        and bias_invalidation is not None
        and spot >= bias_invalidation
        and bias_expiry == front.expiry
        and front.expiry == now_utc.astimezone(NY_TZ).strftime("%Y%m%d")
        and front.gex_quality == "open_interest_gex"
        and options_map.underlier.source == "index:SPX"
        and (
            bias_play != CALL_WALL_BREAKOUT_CALL_KIND
            or front.wall_method == "oi_gex"
        )
    )
    if bias_valid and bias_play == FLIP_RECLAIM_CALL_KIND and bias_level is not None:
        candidate = _build_candidate(
            play=FLIP_RECLAIM_CALL_KIND,
            level=bias_level,
            level_label=f"frozen flip {_dash(bias_level)}",
            target_strike=round_to_step(bias_level, strike_step_int),
            right="C",
            spot=spot,
            expiry_quotes=expiry_quotes,
            strike_step=strike_step,
            pairs=pairs,
            warnings=local_warnings,
            as_of=now_utc,
            tau_now_years=tau_now_years,
            em_points=em_points,
        )
        if candidate is not None:
            candidates.append(candidate)

    flip_level = None
    flip_label = None
    if front.gamma_flip_zone is not None:
        flip_level = front.gamma_flip_zone[0]
        flip_label = f"flip zone {_dash(flip_level)}"
    elif front.zero_gamma is not None:
        flip_level = front.zero_gamma
        flip_label = f"zero gamma {_dash(flip_level)}"

    if flip_level is not None and flip_level > spot + strike_step:
        # Spot already below the flip zone: the breakdown has happened; a
        # "跌破买 put" limit anchored above spot would be a stale play.
        local_warnings.append(f"flip_{_dash(flip_level)}_above_spot_breakdown_already_done")
        flip_level = None
        flip_label = None

    if (
        flip_level is not None
        and flip_label is not None
        and not (bias_valid and bias_play == FLIP_RECLAIM_CALL_KIND)
    ):
        target_strike = round_to_step(flip_level, strike_step_int)
        candidate = _build_candidate(
            play="flip_breakdown_put",
            level=flip_level,
            level_label=flip_label,
            target_strike=target_strike,
            right="P",
            spot=spot,
            expiry_quotes=expiry_quotes,
            strike_step=strike_step,
            pairs=pairs,
            warnings=local_warnings,
            as_of=now_utc,
            tau_now_years=tau_now_years,
            em_points=em_points,
        )
        if candidate is not None:
            candidates.append(candidate)

    if bias_valid and bias_play == CALL_WALL_BREAKOUT_CALL_KIND and bias_level is not None:
        candidate = _build_candidate(
            play=CALL_WALL_BREAKOUT_CALL_KIND,
            level=bias_level,
            level_label=f"frozen call wall {_dash(bias_level)}",
            target_strike=round_to_step(bias_level, strike_step_int),
            right="C",
            spot=spot,
            expiry_quotes=expiry_quotes,
            strike_step=strike_step,
            pairs=pairs,
            warnings=local_warnings,
            as_of=now_utc,
            tau_now_years=tau_now_years,
            em_points=em_points,
        )
        if candidate is not None:
            candidates.append(candidate)

    call_wall_level = front.call_wall
    if call_wall_level is not None and call_wall_level < spot - strike_step:
        local_warnings.append(f"call_wall_{_dash(call_wall_level)}_below_spot_skipped")
        call_wall_level = next(
            (wall.strike for wall in front.call_walls if wall.strike >= spot - strike_step),
            None,
        )

    if (
        call_wall_level is not None
        and not (bias_valid and bias_play == CALL_WALL_BREAKOUT_CALL_KIND)
    ):
        target_strike = round_to_step(call_wall_level, strike_step_int)
        candidate = _build_candidate(
            play="call_wall_fade_put",
            level=call_wall_level,
            level_label=f"call wall {_dash(call_wall_level)}",
            target_strike=target_strike,
            right="P",
            spot=spot,
            expiry_quotes=expiry_quotes,
            strike_step=strike_step,
            pairs=pairs,
            warnings=local_warnings,
            as_of=now_utc,
            tau_now_years=tau_now_years,
            em_points=em_points,
        )
        if candidate is not None:
            candidates.append(candidate)

    if bias_valid:
        canonical_rank = {play: index for index, play in enumerate(PLAY_ORDER)}
        candidates.sort(
            key=lambda row: (
                0 if row.play == bias_play else 1,
                canonical_rank.get(row.play, len(canonical_rank)),
            )
        )
    return candidates
