from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from spx_spark.analytics.options.pricing import finite_float
from spx_spark.application.order_map.models import (
    LEVEL_DECISION_PLAYS,
    PLAY_ORDER,
    OrderCandidate,
    SpotResolution,
)
from spx_spark.application.order_map.execution_quote import evaluate_execution_quote
from spx_spark.application.order_map.pricing import (
    BSProjection,
    YEAR_SECONDS,
    build_chain_option_price_bs_projection,
    expiry_close_utc,
    project_option_price,
    round_to_tick,
    touch_eta_minutes,
)
from spx_spark.application.order_map.spot import resolve_spx_spot
from spx_spark.intraday_strategy import (
    CALL_WALL_BREAKOUT_CALL_KIND,
    FLIP_RECLAIM_CALL_KIND,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
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
from spx_spark.settings.order_map import DEFAULT_ORDER_MAP_POLICY, OrderMapPolicy
from spx_spark.storage import LatestState, configured_quote_use_decision


def frontrun_level_for(
    spot: float,
    level: float,
    *,
    policy: OrderMapPolicy = DEFAULT_ORDER_MAP_POLICY,
) -> float | None:
    """Level shifted from the target back toward spot by a capped fraction."""
    distance = abs(spot - level)
    if distance <= policy.frontrun_min_points:
        return None
    offset = min(
        max(policy.frontrun_fraction * distance, policy.frontrun_min_points),
        policy.frontrun_max_points,
    )
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
    all_quotes: tuple[Quote, ...],
    strike_step: float,
    pairs: dict[float, dict[OptionRight, Quote]],
    warnings: list[str],
    as_of: datetime,
    tau_now_years: float | None = None,
    em_points: float | None = None,
    policy: OrderMapPolicy = DEFAULT_ORDER_MAP_POLICY,
    empirical_touch_fractions: tuple[float, float, float] | None = None,
    touch_time_model_source: str = "brownian_heuristic",
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
    vendor_iv = finite_float(quote.greeks.implied_vol) if quote.greeks is not None else None
    quote_gate = evaluate_execution_quote(quote, all_quotes, as_of=as_of, policy=policy)

    def _project(target: float) -> tuple[float, str, BSProjection | None]:
        bs_projection = build_chain_option_price_bs_projection(
            mid=mid,
            vendor_iv=vendor_iv,
            strike=strike_float,
            right=right,
            spot=spot,
            target=target,
            tau_now_years=tau_now_years,
            em_points=em_points,
            pairs=pairs,
            strike_step=strike_step,
            empirical_touch_fractions=empirical_touch_fractions,
            policy=policy,
        )
        if bs_projection is not None:
            return bs_projection.projected_mid, "bs_repricing", bs_projection
        return (
            project_option_price(mid, delta, gamma, spot, target),
            "taylor_fallback",
            None,
        )

    model_projected, projection_model, bs_projection = _project(level)
    if projection_model == "taylor_fallback":
        warnings.append(f"taylor_fallback_for_{target_strike}{right}")
    prob_close, prob_touch, _source_strike, _source_delta = probability_for_level(
        level,
        underlier=spot,
        pairs=pairs,
        strike_step=strike_step,
        tau_years=tau_now_years,
    )

    frontrun_level = frontrun_level_for(spot, level, policy=policy)
    frontrun_projected = None
    frontrun_limit = None
    frontrun_prob_touch = None
    if frontrun_level is not None:
        frontrun_projected, _, _ = _project(frontrun_level)
        frontrun_limit = round_to_tick(frontrun_projected)
        _, frontrun_prob_touch, _, _ = probability_for_level(
            frontrun_level,
            underlier=spot,
            pairs=pairs,
            strike_step=strike_step,
            tau_years=tau_now_years,
        )

    # Every price here is conditional on the underlier reaching ``level`` at
    # an estimated future time. A naked option limit can fill on theta or IV
    # before that happens, even when the projected premium is below spot-mid.
    order_style = "underlier_triggered_limit" if quote_gate.executable else "range_only"
    eta_minutes = touch_eta_minutes(abs(level - spot), em_points, tau_now_years, policy=policy)

    strike_value = int(round(finite_float(quote.instrument.strike) or target_strike))
    return OrderCandidate(
        play=play,
        level=level,
        level_label=level_label,
        contract_id=quote.instrument.canonical_id,
        strike=strike_value,
        right=right,
        current_mid=mid,
        projected_mid=model_projected if quote_gate.executable else None,
        limit_aggressive=round_to_tick(model_projected) if quote_gate.executable else None,
        limit_conservative=(
            round_to_tick(model_projected * policy.conservative_limit_multiplier)
            if quote_gate.executable
            else None
        ),
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
        projection_iv_now=(bs_projection.iv_now if bs_projection is not None else None),
        projection_iv_at_touch=(bs_projection.iv_at_touch if bs_projection is not None else None),
        projection_tau_now_minutes=(
            bs_projection.tau_now_minutes if bs_projection is not None else None
        ),
        projection_tau_at_touch_minutes=(
            bs_projection.tau_at_touch_minutes if bs_projection is not None else None
        ),
        projection_touch_time_fraction=(
            bs_projection.touch_time_fraction if bs_projection is not None else None
        ),
        projection_model_anchor_price=(
            bs_projection.model_anchor_price if bs_projection is not None else None
        ),
        projection_model_target_price=(
            bs_projection.model_target_price if bs_projection is not None else None
        ),
        projection_early_mid=(
            bs_projection.early_projected_mid
            if bs_projection is not None and quote_gate.executable
            else None
        ),
        projection_late_mid=(
            bs_projection.late_projected_mid
            if bs_projection is not None and quote_gate.executable
            else None
        ),
        projection_range_low=(
            bs_projection.price_range_low if bs_projection is not None else model_projected
        ),
        projection_range_high=(
            bs_projection.price_range_high if bs_projection is not None else model_projected
        ),
        projection_forward_now=(bs_projection.forward_now if bs_projection is not None else None),
        projection_forward_at_touch=(
            bs_projection.forward_at_touch if bs_projection is not None else None
        ),
        projection_pricing_kernel=(
            bs_projection.pricing_kernel if bs_projection is not None else projection_model
        ),
        execution_quote_status=quote_gate.status.value,
        execution_quote_reasons=quote_gate.reasons,
        execution_quote_spread_bps=quote_gate.spread_bps,
        execution_quote_spread_percentile=quote_gate.spread_percentile,
        execution_quote_source_age_seconds=quote_gate.source_age_seconds,
        execution_quote_provider_divergence_bps=quote_gate.provider_mid_divergence_bps,
        execution_quote_excluded_providers=quote_gate.excluded_providers,
        touch_time_model_source=touch_time_model_source,
    )


def build_candidates(
    state: LatestState,
    options_map: OptionsMap,
    warnings: list[str] | None = None,
    *,
    now: datetime | None = None,
    resolution: SpotResolution | None = None,
    conditional_call_bias: dict[str, object] | None = None,
    policy: OrderMapPolicy = DEFAULT_ORDER_MAP_POLICY,
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
            all_quotes=state.quotes,
            strike_step=strike_step,
            pairs=pairs,
            warnings=local_warnings,
            as_of=now_utc,
            tau_now_years=tau_now_years,
            em_points=em_points,
            policy=policy,
        )
        if candidate is not None:
            candidates.append(candidate)

    bias_play = str((conditional_call_bias or {}).get("play") or "")
    bias_level = finite_float((conditional_call_bias or {}).get("level"))
    bias_expiry = str((conditional_call_bias or {}).get("expiry") or "")
    bias_invalidation = finite_float((conditional_call_bias or {}).get("invalidation_level"))
    bias_direction = str((conditional_call_bias or {}).get("direction") or "")
    level_bias = bias_play in LEVEL_DECISION_PLAYS
    invalidation_holds = (
        spot >= bias_invalidation
        if bias_direction == "up" and bias_invalidation is not None
        else spot <= bias_invalidation
        if bias_direction == "down" and bias_invalidation is not None
        else spot >= bias_invalidation
        if bias_invalidation is not None
        else False
    )
    bias_valid = bool(
        (conditional_call_bias or {}).get("status") == "confirmed"
        and bias_play
        in {
            FLIP_RECLAIM_CALL_KIND,
            CALL_WALL_BREAKOUT_CALL_KIND,
            *LEVEL_DECISION_PLAYS,
        }
        and bias_level is not None
        and bias_invalidation is not None
        and invalidation_holds
        and bias_expiry == front.expiry
        # Same research-expiry semantics as the level decision machine: during
        # GTH the front expiry has already rolled to the next trading day.
        and front.expiry == DEFAULT_MARKET_CALENDAR.research_expiry(now_utc).strftime("%Y%m%d")
        and front.gex_quality == "open_interest_gex"
        and options_map.underlier.source == "index:SPX"
        and (bias_play != CALL_WALL_BREAKOUT_CALL_KIND or front.wall_method == "oi_gex")
    )
    if bias_valid and level_bias and bias_level is not None:
        right = "C" if bias_direction == "up" else "P"
        candidate = _build_candidate(
            play=bias_play,
            level=bias_level,
            level_label=(
                f"frozen {(conditional_call_bias or {}).get('level_kind') or 'level'} "
                f"{_dash(bias_level)}"
            ),
            target_strike=round_to_step(bias_level, strike_step_int),
            right=right,
            spot=spot,
            expiry_quotes=expiry_quotes,
            all_quotes=state.quotes,
            strike_step=strike_step,
            pairs=pairs,
            warnings=local_warnings,
            as_of=now_utc,
            tau_now_years=tau_now_years,
            em_points=em_points,
            policy=policy,
        )
        if candidate is not None:
            candidates.append(candidate)
    if bias_valid and bias_play == FLIP_RECLAIM_CALL_KIND and bias_level is not None:
        candidate = _build_candidate(
            play=FLIP_RECLAIM_CALL_KIND,
            level=bias_level,
            level_label=f"frozen flip {_dash(bias_level)}",
            target_strike=round_to_step(bias_level, strike_step_int),
            right="C",
            spot=spot,
            expiry_quotes=expiry_quotes,
            all_quotes=state.quotes,
            strike_step=strike_step,
            pairs=pairs,
            warnings=local_warnings,
            as_of=now_utc,
            tau_now_years=tau_now_years,
            em_points=em_points,
            policy=policy,
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
            all_quotes=state.quotes,
            strike_step=strike_step,
            pairs=pairs,
            warnings=local_warnings,
            as_of=now_utc,
            tau_now_years=tau_now_years,
            em_points=em_points,
            policy=policy,
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
            all_quotes=state.quotes,
            strike_step=strike_step,
            pairs=pairs,
            warnings=local_warnings,
            as_of=now_utc,
            tau_now_years=tau_now_years,
            em_points=em_points,
            policy=policy,
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

    if call_wall_level is not None and not (
        bias_valid and bias_play == CALL_WALL_BREAKOUT_CALL_KIND
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
            all_quotes=state.quotes,
            strike_step=strike_step,
            pairs=pairs,
            warnings=local_warnings,
            as_of=now_utc,
            tau_now_years=tau_now_years,
            em_points=em_points,
            policy=policy,
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


def build_level_trigger_candidates(
    state: LatestState,
    options_map: OptionsMap,
    *,
    level: float,
    level_kind: str,
    phase: str,
    thesis: str,
    direction: str | None,
    now: datetime,
    policy: OrderMapPolicy = DEFAULT_ORDER_MAP_POLICY,
    empirical_touch_fractions: tuple[float, float, float] | None = None,
    touch_time_model_source: str = "brownian_heuristic",
) -> tuple[list[OrderCandidate], list[str]]:
    """Reprice the one or two paths that remain possible for an active level event."""

    warnings: list[str] = []
    if not options_map.expiries:
        return [], ["missing_expiries"]
    resolution = resolve_spx_spot(state, options_map, warnings=warnings, now=now)
    spot = resolution.pricing_price if resolution.pricing_allowed else None
    if spot is None:
        return [], [*warnings, "missing_pricing_spot"]
    front = options_map.expiries[0]
    expiry_quotes = _front_expiry_quotes(state, front.expiry)
    pairs = pair_by_strike(expiry_quotes)
    strike_step = median_strike_step(sorted(pairs))
    strike_step_int = max(1, int(round(strike_step)))
    close_utc = expiry_close_utc(front.expiry)
    tau_now_years = (
        max((close_utc - now).total_seconds(), 0.0) / YEAR_SECONDS
        if close_utc is not None
        else None
    )
    outside = -1 if level_kind in {"put_wall", "flip_low"} else 1
    path_directions: list[tuple[str, int]]
    if direction in {"up", "down"}:
        path_directions = [(thesis, 1 if direction == "up" else -1)]
    elif thesis == "breakout":
        path_directions = [("breakout", outside)]
    elif thesis == "fade":
        path_directions = [("fade", -outside)]
    elif phase == "testing":
        path_directions = [("breakout", outside), ("fade", -outside)]
    else:
        return [], [*warnings, "phase_has_no_pricing_path"]

    results: list[OrderCandidate] = []
    for path_thesis, path_direction in path_directions:
        right = "C" if path_direction > 0 else "P"
        play = {
            ("breakout", 1): "level_breakout_call",
            ("breakout", -1): "level_breakout_put",
            ("fade", 1): "level_fade_call",
            ("fade", -1): "level_fade_put",
        }[(path_thesis, path_direction)]
        candidate = _build_candidate(
            play=play,
            level=level,
            level_label=f"{level_kind} {_dash(level)}",
            target_strike=round_to_step(level, strike_step_int),
            right=right,
            spot=spot,
            expiry_quotes=expiry_quotes,
            all_quotes=state.quotes,
            strike_step=strike_step,
            pairs=pairs,
            warnings=warnings,
            as_of=now,
            tau_now_years=tau_now_years,
            em_points=finite_float(front.expected_move_points),
            policy=policy,
            empirical_touch_fractions=empirical_touch_fractions,
            touch_time_model_source=touch_time_model_source,
        )
        if candidate is not None:
            results.append(candidate)
    return results, list(dict.fromkeys(warnings))
