from __future__ import annotations

import argparse
import json
import math
import os
import time as time_module
from dataclasses import asdict, dataclass
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from spx_spark.config import NY_TZ, NotificationSettings, StorageSettings
from spx_spark.marketdata import OptionRight, Quote
from spx_spark.notifier.llm_writer import (
    generate_push_text,
    load_previous_push,
    previous_push_json,
    record_push,
)
from spx_spark.notifier.missed_queue import append_missed
from spx_spark.notifier.model import CommandRunner, default_runner
from spx_spark.notifier.sinks import (
    any_delivery_ok,
    deliver_trade_push,
    im_delivery_ok,
)
from spx_spark.options_map import (
    BAD_QUALITIES,
    OptionsMap,
    build_options_map,
    chain_implied_spot,
    finite_float,
    is_spxw_option,
    median_strike_step,
    option_mid,
    pair_by_strike,
    probability_for_level,
)
from spx_spark.sampling import round_to_step
from spx_spark.storage import LatestState, LatestStateStore

SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
BJ_WINDOW_START = time(13, 30)
BJ_WINDOW_END = time(21, 25)

PLAY_ORDER = (
    "put_wall_bounce_call",
    "flip_breakdown_put",
    "call_wall_fade_put",
)

PLAY_TEMPLATE_LINES = {
    "put_wall_bounce_call": "{level_label} 反弹买 call → SPXW {strike}{right}",
    "flip_breakdown_put": "{level_label} 跌破买 put → SPXW {strike}{right}",
    "call_wall_fade_put": "{level_label} 冲墙买 put → SPXW {strike}{right}",
}


def option_tick(premium: float) -> float:
    """SPX option tick: 0.05 below 3.00, 0.10 at/above."""
    return 0.05 if premium < 3.0 else 0.10


def round_to_tick(premium: float) -> float:
    """Round DOWN to tick (limit buy: 挂低一格比挂高一格好)."""
    tick = option_tick(premium)
    return math.floor(premium / tick + 1e-12) * tick


def project_option_price(
    mid: float, delta: float, gamma: float, spot: float, target: float
) -> float:
    """Second-order Taylor projection, clamped to >= 0.05.

    Fallback only: ignores theta and vol dynamics, which for 0DTE makes buy
    limits fill hours early on pure time decay (2026-07-07: projected 16.04
    for 7500C at the wall, actual at touch 12.45). Prefer
    project_option_price_bs when IV is available.
    """
    move = target - spot
    projected = mid + delta * move + 0.5 * gamma * move * move
    return max(0.05, projected)


# --- BS repricing at a target level, with time decay and vol-shift ---

SESSION_CLOSE_ET = time(16, 0)
YEAR_SECONDS = 365.0 * 24.0 * 3600.0
# Fraction of remaining time expected to elapse before the touch, as a
# multiple of the Brownian-scaling estimate (distance/EM)^2. Calibrated on
# 2026-07-07: SPX covered 23.5pts (0.91 EM) in ~59% of the remaining session;
# first passages concentrate earlier than the full scaling suggests.
TOUCH_TIME_FRACTION_COEF = 0.6
TOUCH_TIME_FRACTION_MAX = 0.90
# "Sticky strike plus": on a move down, fixed-strike IV rises by roughly this
# multiple of the local smile slope (and falls on the way up).
VOL_SLOPE_BETA = 1.2
MIN_TAU_AT_TOUCH_HOURS = 0.25


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _bs_price(spot: float, strike: float, iv: float, tau_years: float, right: str) -> float:
    intrinsic = max(0.0, spot - strike) if right == "C" else max(0.0, strike - spot)
    if tau_years <= 0 or iv <= 0:
        return intrinsic
    st = iv * math.sqrt(tau_years)
    d1 = (math.log(spot / strike) + 0.5 * st * st) / st
    d2 = d1 - st
    if right == "C":
        return spot * _norm_cdf(d1) - strike * _norm_cdf(d2)
    return strike * _norm_cdf(-d2) - spot * _norm_cdf(-d1)


def expiry_close_utc(expiry: str) -> datetime | None:
    """SPXW dailies are PM-settled: last trade 16:00 ET on the expiry date."""
    try:
        day = datetime.strptime(expiry, "%Y%m%d").date()
    except ValueError:
        return None
    return datetime.combine(day, SESSION_CLOSE_ET, tzinfo=NY_TZ).astimezone(timezone.utc)


def smile_slope_per_point(
    pairs: dict[float, dict[OptionRight, Quote]],
    right: str,
    strike: float,
    strike_step: float,
) -> float | None:
    """Local dIV/dK (per index point) via least squares on nearby strikes."""
    right_enum = OptionRight.CALL if right == "C" else OptionRight.PUT
    points: list[tuple[float, float]] = []
    for k in sorted(pairs):
        if abs(k - strike) > 3.0 * strike_step:
            continue
        quote = (pairs.get(k) or {}).get(right_enum)
        if quote is None or quote.greeks is None:
            continue
        iv = finite_float(quote.greeks.implied_vol)
        if iv is not None and iv > 0:
            points.append((k, iv))
    if len(points) < 2:
        return None
    n = float(len(points))
    sx = sum(k for k, _ in points)
    sy = sum(v for _, v in points)
    sxx = sum(k * k for k, _ in points)
    sxy = sum(k * v for k, v in points)
    denom = n * sxx - sx * sx
    if abs(denom) < 1e-9:
        return None
    return (n * sxy - sx * sy) / denom


def project_option_price_bs(
    *,
    mid: float,
    iv: float | None,
    strike: float,
    right: str,
    spot: float,
    target: float,
    tau_now_years: float | None,
    em_points: float | None,
    slope_per_point: float | None,
) -> float | None:
    """Reprice the option at the target level with Black-Scholes.

    Unlike the Taylor projection this accounts for:
    - time decay before the touch (touch time estimated by Brownian scaling
      of distance vs remaining expected move);
    - fixed-strike IV drift along the smile (down moves lift IV, up moves
      compress it).
    The result is ratio-anchored to the current market mid so provider IV or
    model mismatch does not shift the base level.
    """
    if iv is None or iv <= 0 or mid <= 0:
        return None
    if tau_now_years is None or tau_now_years <= 0:
        return None
    anchor = _bs_price(spot, strike, iv, tau_now_years, right)
    if anchor <= 0.01:
        return None

    distance = abs(target - spot)
    fraction = 0.05
    if em_points is not None and em_points > 0:
        fraction = min(
            max(TOUCH_TIME_FRACTION_COEF * (distance / em_points) ** 2, 0.05),
            TOUCH_TIME_FRACTION_MAX,
        )
    tau_touch = max(
        tau_now_years * (1.0 - fraction),
        MIN_TAU_AT_TOUCH_HOURS * 3600.0 / YEAR_SECONDS,
    )

    iv_touch = iv
    if slope_per_point is not None:
        # slope is negative across the put skew; a down move (spot > target)
        # then raises fixed-strike IV, an up move compresses it.
        iv_touch = iv - VOL_SLOPE_BETA * slope_per_point * (spot - target)
        iv_touch = min(max(iv_touch, 0.5 * iv), 2.5 * iv)

    projected = _bs_price(target, strike, iv_touch, tau_touch, right)
    return max(0.05, mid * projected / anchor)


def touch_eta_minutes(
    distance: float,
    em_points: float | None,
    tau_now_years: float | None,
) -> float | None:
    """Expected minutes until first touch, by the same Brownian scaling the BS
    repricing uses. Discipline rule: if the level has not traded after ~2x
    this estimate, the odds of the play have decayed and the order should be
    pulled (theta has eaten the edge even if the level eventually prints)."""
    if tau_now_years is None or tau_now_years <= 0:
        return None
    if em_points is None or em_points <= 0:
        return None
    fraction = min(
        max(TOUCH_TIME_FRACTION_COEF * (distance / em_points) ** 2, 0.05),
        TOUCH_TIME_FRACTION_MAX,
    )
    return fraction * tau_now_years * YEAR_SECONDS / 60.0


@dataclass(frozen=True)
class OrderCandidate:
    play: str
    level: float
    level_label: str
    contract_id: str
    strike: int
    right: str
    current_mid: float
    projected_mid: float
    limit_aggressive: float
    limit_conservative: float
    prob_touch: float | None
    prob_close_beyond: float | None
    delta: float
    gamma: float
    # Front-run rung: dealers defend walls before the exact strike, so price
    # often reverses a few points short. This rung prices the option at a level
    # shifted toward spot for a much higher fill probability.
    frontrun_level: float | None = None
    frontrun_projected_mid: float | None = None
    frontrun_limit: float | None = None
    frontrun_prob_touch: float | None = None
    # "resting_limit": projected <= current, a passive buy limit rests until
    # the move happens. "stop_trigger": projected > current, a plain buy limit
    # would fill immediately at market -> needs a stop-limit trigger instead.
    order_style: str = "resting_limit"
    # "bs_repricing" (theta + vol-shift aware) or "taylor_fallback".
    projection_model: str = "taylor_fallback"
    # Brownian-scaling estimate of minutes to first touch; ~2x without a touch
    # means the odds have decayed and the resting order should be pulled.
    touch_eta_minutes: float | None = None


FRONTRUN_FRACTION = 0.30
FRONTRUN_MIN_POINTS = 2.0
FRONTRUN_MAX_POINTS = 8.0


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


def _quote_mid(quote: Quote) -> float | None:
    if quote.quality in BAD_QUALITIES:
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
    if quote.quality in BAD_QUALITIES:
        warnings.append(f"bad_quality_for_{target_strike}{right}")
        return None
    if not _quote_greeks_ok(quote):
        warnings.append(f"missing_greeks_for_{target_strike}{right}")
        return None

    mid = _quote_mid(quote)
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


HL_SP500_PROXY_ID = "crypto_perp:xyz:SP500"
# Chain-implied vs Hyperliquid perp divergence beyond this suggests wide or
# stale GTH option quotes; surface it instead of silently trusting either.
HL_DIVERGENCE_WARN_BPS = 15.0

SPX_CASH_OPEN_ET = time(9, 30)
SPX_CASH_CLOSE_ET = time(16, 0)


def spx_cash_session_open(now_utc: datetime) -> bool:
    ny = now_utc.astimezone(NY_TZ)
    if ny.weekday() >= 5:
        return False
    return SPX_CASH_OPEN_ET <= ny.time() < SPX_CASH_CLOSE_ET


def hyperliquid_sp500_price(state: LatestState) -> float | None:
    quote = state.best_quote(HL_SP500_PROXY_ID)
    if quote is None or quote.quality in BAD_QUALITIES:
        return None
    return finite_float(quote.mid or quote.mark or quote.effective_price)


def resolve_spx_spot(
    state: LatestState,
    options_map: OptionsMap,
    *,
    warnings: list[str] | None = None,
    now: datetime | None = None,
) -> tuple[float | None, str]:
    """Return (spot, source_label) for projections.

    During SPX cash hours the chain-implied parity spot is the option
    market's own view and wins. Outside cash hours SPXW GTH quotes go wide
    and the parity spot drifts, so when it diverges from the Hyperliquid
    SP500 perp (24/7 liquid) beyond the threshold, the perp wins.
    """
    now = now or datetime.now(tz=timezone.utc)
    hl_price = hyperliquid_sp500_price(state)
    if options_map.expiries:
        front = options_map.expiries[0]
        pairs = pair_by_strike(_front_expiry_quotes(state, front.expiry))
        implied = chain_implied_spot(pairs)
        if implied is not None:
            if hl_price is not None:
                divergence_bps = abs(implied / hl_price - 1.0) * 10_000.0
                if divergence_bps > HL_DIVERGENCE_WARN_BPS:
                    if not spx_cash_session_open(now):
                        if warnings is not None:
                            warnings.append(
                                f"SPX 现货闭市: 链隐含 {implied:.1f} 与 HL perp "
                                f"{hl_price:.1f} 分歧 {divergence_bps:.0f} bps,"
                                "GTH 报价偏宽,参考价采用 perp"
                            )
                        return hl_price, "hl_perp"
                    if warnings is not None:
                        warnings.append(
                            f"chain-implied spot {implied:.1f} diverges from "
                            f"hyperliquid SP500 perp {hl_price:.1f} "
                            f"({divergence_bps:.0f} bps); GTH quotes may be wide"
                        )
            return implied, "chain_implied"
    if hl_price is not None:
        if warnings is not None:
            warnings.append("spot from hyperliquid SP500 perp (chain parity unavailable)")
        return hl_price, "hl_perp"
    price = options_map.underlier.price
    source = options_map.underlier.source or "-"
    if price is not None and source.startswith("future:") and warnings is not None:
        warnings.append("spot from futures reference; basis not adjusted")
    return price, source


def build_candidates(
    state: LatestState,
    options_map: OptionsMap,
    warnings: list[str] | None = None,
    *,
    now: datetime | None = None,
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

    spot, _spot_source = resolve_spx_spot(state, options_map, warnings=local_warnings, now=now)
    if spot is None:
        local_warnings.append("missing underlier price")
        return []

    now_utc = now or datetime.now(tz=timezone.utc)
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

    if flip_level is not None and flip_label is not None:
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

    if call_wall_level is not None:
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
            tau_now_years=tau_now_years,
            em_points=em_points,
        )
        if candidate is not None:
            candidates.append(candidate)

    return candidates


def _wall_ladder_payload(
    state: LatestState,
    options_map: OptionsMap,
    spot: float | None,
) -> dict[str, list[dict[str, Any]]]:
    """Top-4 call/put walls with touch probabilities for the payload.

    A single wall per side loses the structure: on 2026-07-07 the put side was
    a near-flat band (7460-7500) and price ground to 7479, ten points past the
    "the" put wall. The ladder lets the writer talk about bands and second
    entries instead of one line in the sand.
    """
    ladder: dict[str, list[dict[str, Any]]] = {"call_walls": [], "put_walls": []}
    if not options_map.expiries:
        return ladder
    front = options_map.expiries[0]
    pairs = pair_by_strike(_front_expiry_quotes(state, front.expiry))
    strike_step = median_strike_step(sorted(pairs))
    for key, walls in (("call_walls", front.call_walls), ("put_walls", front.put_walls)):
        for wall in walls:
            prob_touch = None
            if spot is not None:
                _, prob_touch, _, _ = probability_for_level(
                    wall.strike,
                    underlier=spot,
                    pairs=pairs,
                    strike_step=strike_step,
                )
            ladder[key].append(
                {
                    "strike": wall.strike,
                    "gex": wall.gex,
                    "open_interest": wall.open_interest,
                    "volume": wall.volume,
                    "distance_points": round(wall.strike - spot, 1) if spot is not None else None,
                    "prob_touch": prob_touch,
                }
            )
    return ladder


def _index_value(state: LatestState, canonical_id: str) -> float | None:
    quote = state.best_quote(canonical_id)
    if quote is None or quote.quality in BAD_QUALITIES:
        return None
    return finite_float(quote.effective_price)


# --- ES volume pace + volume-price events ---------------------------------
# SPX prints no volume; ES cumulative day volume is the proxy. Each order-map
# run diffs volume AND the spot reference vs the previous sample, then classifies
# a volume-price event from four axes:
#   1) pace (elevated / normal / quiet vs recent-window baseline)
#   2) direction (up / down / flat from spot delta over the same window)
#   3) location (at put wall / call wall / flip / mid / broken side)
#   4) sequence (wall test → reclaim, break → hold / reclaim, vacuum drift)
# The event_id is what prompts consume; pace alone is never a truth filter.

ES_VOLUME_SESSION_OPEN_ET = time(18, 0)
ES_VOLUME_MIN_WINDOW_MINUTES = 3.0
ES_VOLUME_MAX_WINDOW_MINUTES = 120.0
ES_VOLUME_ELEVATED_RATIO = 1.5
ES_VOLUME_QUIET_RATIO = 0.5
ES_VOLUME_MAX_SAMPLES = 16
ES_VOLUME_MAX_QUOTE_AGE_SECONDS = 900.0
# Direction flat band: moves smaller than this are noise for a 15-30m window.
ES_VOLUME_FLAT_POINTS = 3.0
# "Near a level" band for location classification.
ES_VOLUME_LEVEL_BAND_POINTS = 8.0
# Break watch: after a key level is crossed, wait at least this long before
# calling hold vs reclaim (avoids labeling a one-tick pierce).
ES_VOLUME_RECLAIM_MIN_MINUTES = 10.0
ES_VOLUME_RECLAIM_MAX_MINUTES = 90.0


def default_es_volume_sample_path(settings: StorageSettings) -> str:
    return os.getenv("SPX_ES_VOLUME_SAMPLE_PATH") or str(
        Path(settings.data_root) / "latest" / "es_volume_samples.json"
    )


def load_es_volume_samples(path: str) -> list[dict[str, Any]]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    samples = payload.get("samples") if isinstance(payload, dict) else None
    return [item for item in samples or [] if isinstance(item, dict)]


def load_es_volume_break_watch(path: str) -> dict[str, Any] | None:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    watch = payload.get("break_watch") if isinstance(payload, dict) else None
    return watch if isinstance(watch, dict) else None


def save_es_volume_state(
    path: str,
    samples: list[dict[str, Any]],
    *,
    break_watch: dict[str, Any] | None = None,
) -> None:
    file_path = Path(path)
    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        payload: dict[str, Any] = {"samples": samples[-ES_VOLUME_MAX_SAMPLES:]}
        if break_watch is not None:
            payload["break_watch"] = break_watch
        file_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except OSError:
        pass


def save_es_volume_samples(path: str, samples: list[dict[str, Any]]) -> None:
    """Backward-compatible wrapper used by older tests."""
    save_es_volume_state(path, samples, break_watch=load_es_volume_break_watch(path))


def es_session_elapsed_minutes(now: datetime) -> float | None:
    """Minutes since the current Globex session opened (18:00 ET)."""
    local = now.astimezone(NY_TZ)
    session_open = local.replace(hour=18, minute=0, second=0, microsecond=0)
    if local.time() < ES_VOLUME_SESSION_OPEN_ET:
        session_open -= timedelta(days=1)
    elapsed = (local - session_open).total_seconds() / 60.0
    return elapsed if elapsed > 1.0 else None


def _parse_sample(sample: dict[str, Any]) -> tuple[datetime, float, float | None] | None:
    volume = finite_float(sample.get("volume"))
    at_raw = sample.get("at")
    if volume is None or volume <= 0 or not isinstance(at_raw, str):
        return None
    try:
        at = datetime.fromisoformat(at_raw)
    except ValueError:
        return None
    if at.tzinfo is None:
        at = at.replace(tzinfo=timezone.utc)
    price = finite_float(sample.get("price"))
    return at, volume, price


def _window_paces(points: list[tuple[datetime, float, float | None]]) -> list[float]:
    """Contracts/minute for each valid consecutive sample pair."""
    paces: list[float] = []
    for (prev_at, prev_volume, _), (cur_at, cur_volume, _) in zip(points, points[1:]):
        minutes = (cur_at - prev_at).total_seconds() / 60.0
        if not (ES_VOLUME_MIN_WINDOW_MINUTES <= minutes <= ES_VOLUME_MAX_WINDOW_MINUTES):
            continue
        if cur_volume < prev_volume:  # session rollover inside the pair
            continue
        paces.append((cur_volume - prev_volume) / minutes)
    return paces


def classify_price_direction(price_delta: float | None, *, flat_points: float = ES_VOLUME_FLAT_POINTS) -> str | None:
    if price_delta is None:
        return None
    if price_delta >= flat_points:
        return "up"
    if price_delta <= -flat_points:
        return "down"
    return "flat"


def _primary_wall_strike(ladder: dict[str, Any] | None, side: str) -> float | None:
    if not isinstance(ladder, dict):
        return None
    walls = ladder.get("put_walls" if side == "put" else "call_walls")
    if not isinstance(walls, list) or not walls:
        return None
    first = walls[0]
    if isinstance(first, dict):
        return finite_float(first.get("strike"))
    return finite_float(first)


def classify_spot_location(
    spot: float | None,
    *,
    put_wall: float | None,
    call_wall: float | None,
    flip_zone: list[float] | None,
    band: float = ES_VOLUME_LEVEL_BAND_POINTS,
) -> dict[str, Any]:
    """Where spot sits relative to the structural map."""
    result: dict[str, Any] = {
        "location": "unknown",
        "nearest_level": None,
        "distance_to_put_wall": None,
        "distance_to_call_wall": None,
        "distance_to_flip": None,
    }
    if spot is None:
        return result

    candidates: list[tuple[str, float, float]] = []  # kind, strike, signed distance
    if put_wall is not None:
        dist = spot - put_wall
        result["distance_to_put_wall"] = round(dist, 1)
        candidates.append(("put_wall", put_wall, dist))
    if call_wall is not None:
        dist = spot - call_wall
        result["distance_to_call_wall"] = round(dist, 1)
        candidates.append(("call_wall", call_wall, dist))

    flip_mid = None
    if isinstance(flip_zone, list) and len(flip_zone) >= 2:
        lo, hi = finite_float(flip_zone[0]), finite_float(flip_zone[1])
        if lo is not None and hi is not None:
            if lo > hi:
                lo, hi = hi, lo
            flip_mid = (lo + hi) / 2.0
            dist_flip = spot - flip_mid
            result["distance_to_flip"] = round(dist_flip, 1)
            # Strict inside the flip zone wins immediately; the band only
            # participates later via nearest-level selection so a put wall
            # sitting just under the flip is not swallowed by flip.
            if lo <= spot <= hi:
                result["location"] = "in_flip"
                result["nearest_level"] = {
                    "kind": "flip",
                    "strike": round(flip_mid, 1),
                    "distance": round(dist_flip, 1),
                }
                return result
            candidates.append(("flip", flip_mid, dist_flip))

    if put_wall is not None and spot < put_wall - band:
        result["location"] = "below_put_wall"
        result["nearest_level"] = {"kind": "put_wall", "strike": put_wall, "distance": round(spot - put_wall, 1)}
        return result
    if call_wall is not None and spot > call_wall + band:
        result["location"] = "above_call_wall"
        result["nearest_level"] = {"kind": "call_wall", "strike": call_wall, "distance": round(spot - call_wall, 1)}
        return result

    # Prefer the closest level within band.
    near = [
        (kind, strike, dist)
        for kind, strike, dist in candidates
        if abs(dist) <= band
    ]
    if near:
        kind, strike, dist = min(near, key=lambda item: abs(item[2]))
        if kind == "put_wall":
            result["location"] = "at_put_wall"
        elif kind == "call_wall":
            result["location"] = "at_call_wall"
        else:
            result["location"] = "in_flip"
        result["nearest_level"] = {"kind": kind if kind != "flip" else "flip", "strike": strike, "distance": round(dist, 1)}
        return result

    if put_wall is not None and call_wall is not None and put_wall < spot < call_wall:
        result["location"] = "mid_range"
        if candidates:
            kind, strike, dist = min(candidates, key=lambda item: abs(item[2]))
            result["nearest_level"] = {"kind": kind if kind != "flip" else "flip", "strike": strike, "distance": round(dist, 1)}
        return result

    result["location"] = "mid_range"
    return result


def classify_volume_price_event(
    *,
    pace: str,
    direction: str | None,
    location: str,
    break_outcome: str | None = None,
) -> dict[str, Any]:
    """Map the four axes onto a single event_id + play hints."""
    event_id = "unclassified"
    sequence: str | None = None
    hints: list[str] = []

    if break_outcome == "reclaimed":
        event_id = "break_reclaimed"
        sequence = "break_reclaim"
        hints.append("破位后已收回：假破概率高，破位追单剧本降权，等站稳再论")
    elif break_outcome == "holds":
        if pace == "elevated":
            event_id = "elevated_break_holds"
            sequence = "break_hold"
            hints.append("放量破位后仍在破位侧：加速/弃守更可信，条件单可按剧本执行")
        elif pace == "quiet":
            event_id = "quiet_breakdown_holds"
            sequence = "break_hold"
            hints.append("缩量破位后仍在破位侧：共识一边倒，走得干净但回抽浅")
        else:
            event_id = "break_holds"
            sequence = "break_hold"

    elif pace == "elevated" and direction == "down" and location in {"at_put_wall", "in_flip"}:
        event_id = "elevated_sell_into_support"
        sequence = "wall_test"
        hints.append("放量砸向支撑/flip：墙在接还是弃守未定；反弹单等缩量收回，破位单等站不稳确认")
    elif pace == "elevated" and direction == "up" and location in {"at_call_wall", "in_flip"}:
        event_id = "elevated_buy_into_resistance"
        sequence = "wall_test"
        hints.append("放量撞阻力/flip：常先假突再回抽；fade 等滞涨，突破单等站稳")
    elif pace == "quiet" and direction == "down" and location in {"at_put_wall", "in_flip", "below_put_wall"}:
        event_id = "quiet_sell_near_support"
        sequence = "vacuum_or_abandon"
        hints.append("缩量靠近/跌破支撑：可能是弃守阴跌，也可能是真空漂移；站不稳才当破位")
    elif pace == "quiet" and direction == "up" and location in {"at_call_wall", "in_flip", "above_call_wall"}:
        event_id = "quiet_buy_near_resistance"
        sequence = "vacuum_or_abandon"
        hints.append("缩量靠近/越过阻力：可能是共识上移，也可能是真空；站稳才升级突破")
    elif pace == "quiet" and location == "mid_range":
        event_id = "quiet_mid_range"
        sequence = "vacuum_drift"
        hints.append("中间地带缩量：流动性真空漂移，不是突破信号，不追单")
    elif pace == "elevated" and location == "mid_range":
        event_id = "elevated_mid_range"
        sequence = "dispute"
        hints.append("中间地带放量：分歧对打，半路不追，等墙/flip")
    elif pace == "elevated":
        event_id = "elevated_move"
    elif pace == "quiet":
        event_id = "quiet_move"
    elif pace == "normal":
        event_id = "normal_pace"

    # Reclaim-after-test is layered on by the caller when previous sequence was wall_test.
    return {
        "event_id": event_id,
        "sequence": sequence,
        "play_hints": hints,
    }


def update_break_watch(
    previous: dict[str, Any] | None,
    *,
    spot: float | None,
    put_wall: float | None,
    call_wall: float | None,
    flip_zone: list[float] | None,
    pace: str,
    now: datetime,
) -> tuple[dict[str, Any] | None, str | None]:
    """Track whether a freshly broken key level holds or gets reclaimed.

    Returns (new_watch_or_none, outcome) where outcome is holds/reclaimed/pending/None.
    """
    if spot is None:
        return previous, None

    def flip_bounds() -> tuple[float, float] | None:
        if not isinstance(flip_zone, list) or len(flip_zone) < 2:
            return None
        lo, hi = finite_float(flip_zone[0]), finite_float(flip_zone[1])
        if lo is None or hi is None:
            return None
        return (lo, hi) if lo <= hi else (hi, lo)

    outcome: str | None = None
    watch = dict(previous) if isinstance(previous, dict) else None

    if watch is not None:
        level = finite_float(watch.get("level"))
        side = str(watch.get("broken_side") or "")
        broken_at_raw = watch.get("broken_at")
        if level is not None and isinstance(broken_at_raw, str):
            try:
                broken_at = datetime.fromisoformat(broken_at_raw)
                if broken_at.tzinfo is None:
                    broken_at = broken_at.replace(tzinfo=timezone.utc)
                age_min = (now - broken_at).total_seconds() / 60.0
            except ValueError:
                age_min = None
            if age_min is not None and age_min > ES_VOLUME_RECLAIM_MAX_MINUTES:
                watch = None
            elif age_min is not None and age_min >= ES_VOLUME_RECLAIM_MIN_MINUTES:
                if side == "below":
                    if spot >= level + ES_VOLUME_FLAT_POINTS:
                        outcome = "reclaimed"
                        watch = None
                    elif spot <= level - ES_VOLUME_FLAT_POINTS:
                        outcome = "holds"
                        # Keep watch so later windows can still say holds, but
                        # refresh timestamp so we don't spam forever.
                        watch["confirmed_at"] = now.isoformat()
                        watch["outcome"] = "holds"
                    else:
                        outcome = "pending"
                elif side == "above":
                    if spot <= level - ES_VOLUME_FLAT_POINTS:
                        outcome = "reclaimed"
                        watch = None
                    elif spot >= level + ES_VOLUME_FLAT_POINTS:
                        outcome = "holds"
                        watch["confirmed_at"] = now.isoformat()
                        watch["outcome"] = "holds"
                    else:
                        outcome = "pending"
            else:
                outcome = "pending"

    # Arm a new watch only when none is active / just cleared by reclaim.
    if watch is None or outcome == "reclaimed":
        bounds = flip_bounds()
        armed = None
        if put_wall is not None and spot < put_wall - ES_VOLUME_FLAT_POINTS:
            armed = {"level": put_wall, "kind": "put_wall", "broken_side": "below"}
        elif call_wall is not None and spot > call_wall + ES_VOLUME_FLAT_POINTS:
            armed = {"level": call_wall, "kind": "call_wall", "broken_side": "above"}
        elif bounds is not None and spot < bounds[0] - ES_VOLUME_FLAT_POINTS:
            armed = {"level": bounds[0], "kind": "flip_low", "broken_side": "below"}
        elif bounds is not None and spot > bounds[1] + ES_VOLUME_FLAT_POINTS:
            armed = {"level": bounds[1], "kind": "flip_high", "broken_side": "above"}
        if armed is not None:
            armed.update(
                {
                    "broken_at": now.isoformat(),
                    "pace_at_break": pace,
                    "spot_at_break": spot,
                }
            )
            watch = armed
            if outcome is None:
                outcome = "pending"

    return watch, outcome


def es_volume_signal(
    cumulative: float | None,
    samples: list[dict[str, Any]],
    *,
    now: datetime,
    spot: float | None = None,
    put_wall: float | None = None,
    call_wall: float | None = None,
    flip_zone: list[float] | None = None,
    break_watch: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if cumulative is None or cumulative <= 0:
        return None
    signal: dict[str, Any] = {
        "cumulative": cumulative,
        "delta": None,
        "window_minutes": None,
        "recent_pace_per_min": None,
        "baseline_pace_per_min": None,
        "baseline": None,
        "pace_ratio": None,
        "label": "no_baseline",
        "price": spot,
        "price_delta": None,
        "direction": None,
        "location": "unknown",
        "nearest_level": None,
        "break_outcome": None,
        "break_watch": break_watch,
        "event_id": None,
        "sequence": None,
        "play_hints": [],
    }
    points = [parsed for sample in samples if (parsed := _parse_sample(sample)) is not None]
    points.sort(key=lambda item: item[0])
    if not points:
        loc = classify_spot_location(spot, put_wall=put_wall, call_wall=call_wall, flip_zone=flip_zone)
        signal.update(loc)
        return signal
    last_at, last_volume, last_price = points[-1]
    if cumulative < last_volume:
        signal["label"] = "session_reset"
        return signal
    window_minutes = (now - last_at).total_seconds() / 60.0
    if not (ES_VOLUME_MIN_WINDOW_MINUTES <= window_minutes <= ES_VOLUME_MAX_WINDOW_MINUTES):
        loc = classify_spot_location(spot, put_wall=put_wall, call_wall=call_wall, flip_zone=flip_zone)
        signal.update(loc)
        return signal
    delta = cumulative - last_volume
    recent_pace = delta / window_minutes

    history_paces = _window_paces(points)
    if len(history_paces) >= 2:
        ordered = sorted(history_paces)
        mid = len(ordered) // 2
        baseline = (
            ordered[mid] if len(ordered) % 2 == 1 else (ordered[mid - 1] + ordered[mid]) / 2.0
        )
        baseline_name = "recent_windows"
    else:
        elapsed = es_session_elapsed_minutes(now)
        if elapsed is None:
            return signal
        baseline = cumulative / elapsed
        baseline_name = "session_average"
    if baseline <= 0:
        return signal

    ratio = recent_pace / baseline
    if ratio >= ES_VOLUME_ELEVATED_RATIO:
        label = "elevated"
    elif ratio <= ES_VOLUME_QUIET_RATIO:
        label = "quiet"
    else:
        label = "normal"

    price_delta = None
    if spot is not None and last_price is not None:
        price_delta = round(spot - last_price, 1)
    direction = classify_price_direction(price_delta)
    loc = classify_spot_location(spot, put_wall=put_wall, call_wall=call_wall, flip_zone=flip_zone)
    new_watch, break_outcome = update_break_watch(
        break_watch,
        spot=spot,
        put_wall=put_wall,
        call_wall=call_wall,
        flip_zone=flip_zone,
        pace=label,
        now=now,
    )
    event = classify_volume_price_event(
        pace=label,
        direction=direction,
        location=str(loc.get("location") or "unknown"),
        break_outcome=break_outcome,
    )
    # Sequence upgrade: quiet reclaim after an elevated wall test.
    if (
        label == "quiet"
        and direction == "up"
        and loc.get("location") in {"mid_range", "at_put_wall", "in_flip"}
        and len(points) >= 2
    ):
        # Look at previous window direction via last two priced samples if present.
        prev_priced = [p for p in points[-3:] if p[2] is not None]
        if len(prev_priced) >= 2 and spot is not None:
            prev_delta = prev_priced[-1][2] - prev_priced[-2][2]  # type: ignore[operator]
            if prev_delta is not None and prev_delta <= -ES_VOLUME_FLAT_POINTS:
                event = {
                    "event_id": "quiet_reclaim_after_sell_test",
                    "sequence": "reclaim",
                    "play_hints": [
                        "前窗下跌测试后本窗缩量收回：反弹剧本升温，破位追空降权"
                    ],
                }

    signal.update(
        {
            "delta": round(delta),
            "window_minutes": round(window_minutes, 1),
            "recent_pace_per_min": round(recent_pace, 1),
            "baseline_pace_per_min": round(baseline, 1),
            "baseline": baseline_name,
            "pace_ratio": round(ratio, 2),
            "label": label,
            "price": spot,
            "price_delta": price_delta,
            "direction": direction,
            "break_outcome": break_outcome,
            "break_watch": new_watch,
            "event_id": event["event_id"],
            "sequence": event["sequence"],
            "play_hints": event["play_hints"],
        }
    )
    signal.update(loc)
    return signal


def attach_es_volume_signal(
    payload: dict[str, Any],
    state: LatestState,
    *,
    sample_path: str,
    now: datetime,
    persist: bool = True,
) -> None:
    """Compute the ES volume-price event and append the new sample.

    Side-effectful on purpose (appends to the sample file), so it runs once per
    push at the call site instead of inside the pure payload builder that the
    thin-snapshot retry loop may invoke several times.
    """
    quote = state.best_quote("future:ES")
    cumulative = finite_float(quote.volume) if quote is not None else None
    age_ms = quote.quote_age_ms(now) if quote is not None else None
    if age_ms is not None and age_ms > ES_VOLUME_MAX_QUOTE_AGE_SECONDS * 1000.0:
        # Frozen feed: a zero delta would read as "quiet" when it is just no data.
        cumulative = None

    underlier = payload.get("underlier") if isinstance(payload.get("underlier"), dict) else {}
    spot = finite_float(underlier.get("price"))
    if spot is None:
        spot = finite_float(payload.get("es_last"))

    ladder = payload.get("wall_ladder") if isinstance(payload.get("wall_ladder"), dict) else {}
    put_wall = _primary_wall_strike(ladder, "put")
    call_wall = _primary_wall_strike(ladder, "call")
    # Prefer candidate levels when ladder missing.
    by_play = _candidate_by_play(payload)
    if put_wall is None and "put_wall_bounce_call" in by_play:
        put_wall = finite_float(by_play["put_wall_bounce_call"].get("level"))
    if call_wall is None and "call_wall_fade_put" in by_play:
        call_wall = finite_float(by_play["call_wall_fade_put"].get("level"))
    flip_zone = payload.get("flip_zone") if isinstance(payload.get("flip_zone"), list) else None

    samples = load_es_volume_samples(sample_path)
    previous_watch = load_es_volume_break_watch(sample_path)
    signal = es_volume_signal(
        cumulative,
        samples,
        now=now,
        spot=spot,
        put_wall=put_wall,
        call_wall=call_wall,
        flip_zone=flip_zone,
        break_watch=previous_watch,
    )
    payload["es_volume"] = signal
    if persist and cumulative is not None:
        sample: dict[str, Any] = {"at": now.isoformat(), "volume": cumulative}
        if spot is not None:
            sample["price"] = spot
        samples.append(sample)
        new_watch = signal.get("break_watch") if isinstance(signal, dict) else previous_watch
        save_es_volume_state(
            sample_path,
            samples,
            break_watch=new_watch if isinstance(new_watch, dict) else None,
        )


# --- Hyperliquid SP500 perp volume-price lane -------------------------------
# The perp trades 24/7 (including weekends when ES is dark), and its trade
# tape carries aggressor sides that ES cannot give us. Caveats: dayNtlVlm is a
# ROLLING 24h notional, so short diffs mix fresh volume with the 24h-ago tail
# dropping off -- bursts (elevated) are trustworthy, quiet readings are only
# indicative. The venue is thin, so this is a secondary confirm for the ES
# lane and the sole volume source on weekends, never a standalone break filter.

HL_VOLUME_MAX_QUOTE_AGE_SECONDS = 900.0


def default_hl_volume_sample_path(settings: StorageSettings) -> str:
    return os.getenv("SPX_HL_VOLUME_SAMPLE_PATH") or str(
        Path(settings.data_root) / "latest" / "hl_volume_samples.json"
    )


def _latest_hl_context(settings: StorageSettings, now: datetime) -> dict[str, Any] | None:
    """Last Hyperliquid asset-context record; carries the aggressor buy/sell
    split and book imbalance that the latest-state quote drops."""
    base = (
        Path(settings.data_root)
        / "context"
        / "provider=hyperliquid"
        / "dex=xyz"
        / "coin=xyz:SP500"
    )
    for offset_hours in (0, 1):
        stamp = (now - timedelta(hours=offset_hours)).astimezone(timezone.utc)
        path = (
            base
            / f"date={stamp.strftime('%Y-%m-%d')}"
            / f"hour={stamp.strftime('%H')}"
            / "asset-context.jsonl"
        )
        try:
            last = ""
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if line.strip():
                        last = line
            if last:
                record = json.loads(last)
                if isinstance(record, dict):
                    return record
        except (OSError, json.JSONDecodeError):
            continue
    return None


def hl_volume_signal(
    cumulative: float | None,
    samples: list[dict[str, Any]],
    *,
    now: datetime,
) -> dict[str, Any] | None:
    """Pace signal from the HL SP500 perp rolling-24h notional volume."""
    if cumulative is None or cumulative <= 0:
        return None
    signal: dict[str, Any] = {
        "cumulative_notional": round(cumulative),
        "delta_notional": None,
        "window_minutes": None,
        "recent_pace_per_min": None,
        "baseline_pace_per_min": None,
        "pace_ratio": None,
        "label": "no_baseline",
        "basis": "rolling_24h_notional",
    }
    points = [parsed for sample in samples if (parsed := _parse_sample(sample)) is not None]
    points.sort(key=lambda item: item[0])
    if not points:
        return signal
    last_at, last_volume, _last_price = points[-1]
    window_minutes = (now - last_at).total_seconds() / 60.0
    if not (ES_VOLUME_MIN_WINDOW_MINUTES <= window_minutes <= ES_VOLUME_MAX_WINDOW_MINUTES):
        return signal
    # Rolling window: a decline means the 24h-ago tail outweighs fresh prints;
    # clamp to zero, which honestly reads as "quiet now".
    delta = max(0.0, cumulative - last_volume)
    recent_pace = delta / window_minutes

    history: list[float] = []
    for (prev_at, prev_volume, _), (cur_at, cur_volume, _) in zip(points, points[1:]):
        minutes = (cur_at - prev_at).total_seconds() / 60.0
        if not (ES_VOLUME_MIN_WINDOW_MINUTES <= minutes <= ES_VOLUME_MAX_WINDOW_MINUTES):
            continue
        history.append(max(0.0, cur_volume - prev_volume) / minutes)
    if len(history) < 2:
        signal.update(
            {"delta_notional": round(delta), "window_minutes": round(window_minutes, 1)}
        )
        return signal
    ordered = sorted(history)
    mid = len(ordered) // 2
    baseline = (
        ordered[mid] if len(ordered) % 2 == 1 else (ordered[mid - 1] + ordered[mid]) / 2.0
    )
    if baseline <= 0:
        signal.update(
            {"delta_notional": round(delta), "window_minutes": round(window_minutes, 1)}
        )
        return signal
    ratio = recent_pace / baseline
    if ratio >= ES_VOLUME_ELEVATED_RATIO:
        label = "elevated"
    elif ratio <= ES_VOLUME_QUIET_RATIO:
        label = "quiet"
    else:
        label = "normal"
    signal.update(
        {
            "delta_notional": round(delta),
            "window_minutes": round(window_minutes, 1),
            "recent_pace_per_min": round(recent_pace),
            "baseline_pace_per_min": round(baseline),
            "pace_ratio": round(ratio, 2),
            "label": label,
        }
    )
    return signal


def attach_hl_volume_signal(
    payload: dict[str, Any],
    state: LatestState,
    *,
    storage_settings: StorageSettings,
    sample_path: str,
    now: datetime,
    persist: bool = True,
) -> None:
    quote = state.best_quote(HL_SP500_PROXY_ID)
    cumulative = finite_float(quote.volume) if quote is not None else None
    age_ms = quote.quote_age_ms(now) if quote is not None else None
    if age_ms is not None and age_ms > HL_VOLUME_MAX_QUOTE_AGE_SECONDS * 1000.0:
        cumulative = None

    samples = load_es_volume_samples(sample_path)  # same {at, volume, price} schema
    signal = hl_volume_signal(cumulative, samples, now=now)
    if signal is not None:
        context = _latest_hl_context(storage_settings, now)
        if context:
            trade_stats = (
                context.get("trade_stats") if isinstance(context.get("trade_stats"), dict) else {}
            )
            buy = finite_float(trade_stats.get("buy_notional")) or 0.0
            sell = finite_float(trade_stats.get("sell_notional")) or 0.0
            if buy + sell > 0:
                signal["aggressor_buy_ratio"] = round(buy / (buy + sell), 2)
            book_imbalance = finite_float(context.get("book_imbalance"))
            if book_imbalance is not None:
                signal["book_imbalance"] = round(book_imbalance, 2)
    payload["hl_volume"] = signal

    if persist and cumulative is not None:
        sample: dict[str, Any] = {"at": now.isoformat(), "volume": cumulative}
        price = hyperliquid_sp500_price(state)
        if price is not None:
            sample["price"] = price
        samples.append(sample)
        save_es_volume_state(sample_path, samples)


def build_order_payload(state: LatestState, *, now: datetime | None = None) -> dict[str, Any]:
    now = now or state.as_of
    options_map = build_options_map(state)
    warnings = list(options_map.warnings)

    front = options_map.expiries[0] if options_map.expiries else None
    expiry = front.expiry if front is not None else None
    expected_move_points = front.expected_move_points if front is not None else None
    gamma_state = front.gamma_state if front is not None else "unknown"
    zero_gamma = front.zero_gamma if front is not None else None
    flip_zone = list(front.gamma_flip_zone) if front is not None and front.gamma_flip_zone else None

    if options_map.underlier.price is None:
        warnings.append("missing underlier reference")
    if not options_map.expiries:
        warnings.append("missing expiries")
    if front is not None and front.gex_quality == "no_open_interest_gex":
        warnings.append("no open interest; walls unavailable")

    spot, spot_source = resolve_spx_spot(state, options_map, now=now)
    candidates = build_candidates(state, options_map, warnings, now=now)
    beijing = now.astimezone(SHANGHAI_TZ)

    # Day move vs expected move: the writer's anti-FOMO anchor. "The drop has
    # already consumed 120% of today's EM" is the number that talks a reader
    # out of shorting the bottom of a slide or panic-selling into a wall band.
    spx_quote = state.best_quote("index:SPX")
    prior_close = finite_float(spx_quote.close) if spx_quote is not None else None
    day_move_points = (
        round(spot - prior_close, 1) if spot is not None and prior_close else None
    )
    em_used_fraction = None
    if day_move_points is not None and expected_move_points and expected_move_points > 0:
        em_used_fraction = round(abs(day_move_points) / expected_move_points, 2)

    return {
        "kind": "order_map",
        "as_of": state.as_of.isoformat(),
        "beijing_time": beijing.strftime("%H:%M"),
        "trading_date": now.astimezone(NY_TZ).date().isoformat(),
        "underlier": {
            "price": spot if spot is not None else options_map.underlier.price,
            "source": spot_source,
        },
        "expiry": expiry,
        "expected_move_points": expected_move_points,
        "candidates": [asdict(candidate) for candidate in candidates],
        "gamma_state": gamma_state,
        "zero_gamma": zero_gamma,
        "flip_zone": flip_zone,
        "wall_ladder": _wall_ladder_payload(state, options_map, spot),
        "wall_method": front.wall_method if front is not None else None,
        "day_move": {
            "prior_close": prior_close,
            "points": day_move_points,
            "em_used_fraction": em_used_fraction,
        },
        "rn_density": (
            front.rn_density.to_dict() if front is not None and front.rn_density else None
        ),
        "vol_context": {
            "vix": _index_value(state, "index:VIX"),
            "vix1d": _index_value(state, "index:VIX1D"),
            "vvix": _index_value(state, "index:VVIX"),
            "skew": _index_value(state, "index:SKEW"),
        },
        "hl_sp500_perp": hyperliquid_sp500_price(state),
        "es_last": _index_value(state, "future:ES"),
        "session_phase": session_phase(now),
        "warnings": list(dict.fromkeys(warnings)),
    }


def _payload_is_thin(payload: dict[str, Any]) -> bool:
    """True when the snapshot caught a mid-rotation flush (missing spot/OI/plays)."""
    underlier = payload.get("underlier") if isinstance(payload.get("underlier"), dict) else {}
    if underlier.get("price") is None:
        return True
    if not payload.get("candidates"):
        return True
    warnings = payload.get("warnings")
    if isinstance(warnings, list) and any("no open interest" in str(item) for item in warnings):
        return True
    fingerprint = payload_fingerprint(payload)
    if (
        fingerprint.get("put_wall") is None
        and fingerprint.get("call_wall") is None
        and fingerprint.get("flip_low") is None
    ):
        return True
    return False


def build_order_payload_with_retry(
    storage_settings: StorageSettings,
    *,
    now: datetime,
    attempts: int = 6,
    delay_seconds: float = 10.0,
) -> dict[str, Any]:
    """Reload latest state a few times if the first snapshot looks thin.

    Thin snapshots happen during slow-poll windows (the stream blocks ~30-50s
    without flushing) and option line rotation gaps; the retry budget must
    outlast those.
    """
    payload: dict[str, Any] = {}
    state: LatestState | None = None
    for attempt in range(attempts):
        state = LatestStateStore(storage_settings).load()
        payload = build_order_payload(state, now=now)
        if not _payload_is_thin(payload):
            break
        if attempt < attempts - 1:
            time_module.sleep(delay_seconds)
    if state is not None:
        attach_es_volume_signal(
            payload,
            state,
            sample_path=default_es_volume_sample_path(storage_settings),
            now=now,
        )
        attach_hl_volume_signal(
            payload,
            state,
            storage_settings=storage_settings,
            sample_path=default_hl_volume_sample_path(storage_settings),
            now=now,
        )
    return payload


def _day_move_line(payload: dict[str, Any]) -> str | None:
    day_move = payload.get("day_move") if isinstance(payload.get("day_move"), dict) else {}
    points = day_move.get("points")
    if points is None:
        return None
    em_used = day_move.get("em_used_fraction")
    em_text = f",已用当日预期波幅的 {em_used:.0%}" if isinstance(em_used, (int, float)) else ""
    return f"较昨收: {points:+.1f} 点{em_text}"


ES_VOLUME_LABEL_TEXT = {
    "elevated": "放量",
    "quiet": "缩量",
    "normal": "正常",
}

ES_VOLUME_DIRECTION_TEXT = {
    "up": "上涨",
    "down": "下跌",
    "flat": "横盘",
}

ES_VOLUME_LOCATION_TEXT = {
    "at_put_wall": "贴put墙",
    "at_call_wall": "贴call墙",
    "in_flip": "在flip区",
    "below_put_wall": "破put墙下方",
    "above_call_wall": "破call墙上方",
    "mid_range": "中间地带",
    "unknown": "位置未知",
}

ES_VOLUME_EVENT_TEXT = {
    "elevated_sell_into_support": "放量砸支撑",
    "elevated_buy_into_resistance": "放量撞阻力",
    "quiet_sell_near_support": "缩量阴跌近支撑",
    "quiet_buy_near_resistance": "缩量摸高近阻力",
    "quiet_reclaim_after_sell_test": "缩量收回(测支撑后)",
    "quiet_mid_range": "中间缩量漂移",
    "elevated_mid_range": "中间放量对打",
    "elevated_break_holds": "放量破位站稳",
    "quiet_breakdown_holds": "缩量破位站稳",
    "break_holds": "破位站稳",
    "break_reclaimed": "破位后收回",
    "elevated_move": "放量移动",
    "quiet_move": "缩量移动",
    "normal_pace": "节奏正常",
    "unclassified": "未分类",
}


def _es_volume_line(payload: dict[str, Any]) -> str | None:
    signal = payload.get("es_volume") if isinstance(payload.get("es_volume"), dict) else None
    if not signal:
        return None
    ratio = signal.get("pace_ratio")
    delta = signal.get("delta")
    window = signal.get("window_minutes")
    if ratio is None or delta is None or window is None:
        return None
    label = ES_VOLUME_LABEL_TEXT.get(str(signal.get("label")), "正常")
    baseline_text = "近几窗" if signal.get("baseline") == "recent_windows" else "当日均值"
    parts = [
        f"ES 量价: 最近{window:.0f}分钟 {int(delta):,} 手, "
        f"节奏为{baseline_text}的 {ratio:.1f} 倍({label})"
    ]
    direction = signal.get("direction")
    price_delta = signal.get("price_delta")
    if direction is not None:
        dir_text = ES_VOLUME_DIRECTION_TEXT.get(str(direction), str(direction))
        if isinstance(price_delta, (int, float)):
            parts.append(f"价{price_delta:+.1f}({dir_text})")
        else:
            parts.append(dir_text)
    location = signal.get("location")
    if location and location != "unknown":
        parts.append(ES_VOLUME_LOCATION_TEXT.get(str(location), str(location)))
    event_id = signal.get("event_id")
    if event_id:
        parts.append(ES_VOLUME_EVENT_TEXT.get(str(event_id), str(event_id)))
    break_outcome = signal.get("break_outcome")
    if break_outcome in {"holds", "reclaimed", "pending"}:
        outcome_text = {"holds": "破位确认中/站稳", "reclaimed": "已收回", "pending": "破位观察中"}[
            str(break_outcome)
        ]
        parts.append(outcome_text)
    return " · ".join(parts)


def _hl_volume_line(payload: dict[str, Any]) -> str | None:
    signal = payload.get("hl_volume") if isinstance(payload.get("hl_volume"), dict) else None
    if not signal:
        return None
    ratio = signal.get("pace_ratio")
    window = signal.get("window_minutes")
    delta = signal.get("delta_notional")
    if ratio is None or window is None or delta is None:
        return None
    label = ES_VOLUME_LABEL_TEXT.get(str(signal.get("label")), "正常")
    parts = [
        f"HL永续量价(24/7薄代理): 最近{window:.0f}分钟名义 ${delta / 1e4:,.0f}万, "
        f"节奏 {ratio:.1f} 倍({label})"
    ]
    aggressor = signal.get("aggressor_buy_ratio")
    if isinstance(aggressor, (int, float)):
        parts.append(f"主动买占比 {aggressor:.0%}")
    imbalance = signal.get("book_imbalance")
    if isinstance(imbalance, (int, float)):
        parts.append(f"盘口失衡 {imbalance:+.2f}")
    return " · ".join(parts)


def _rn_density_line(payload: dict[str, Any]) -> str | None:
    density = payload.get("rn_density") if isinstance(payload.get("rn_density"), dict) else None
    if not density or density.get("median") is None:
        return None
    quality = density.get("quality") or "-"
    parts = [f"中位 {_dash(density.get('median'))}"]
    p10, p90 = density.get("p10"), density.get("p90")
    if p10 is not None and p90 is not None:
        parts.append(f"80%区间 {_dash(p10)}-{_dash(p90)}")
    below = density.get("prob_below_put_wall")
    if below is not None:
        parts.append(f"收破put墙 {_fmt_prob(below)}")
    above = density.get("prob_above_call_wall")
    if above is not None:
        parts.append(f"越call墙 {_fmt_prob(above)}")
    suffix = f" [{quality}]" if quality != "ok" else ""
    return "收盘分布(B-L市场定价): " + ", ".join(parts) + suffix


def _wall_ladder_lines(payload: dict[str, Any]) -> list[str]:
    ladder = payload.get("wall_ladder") if isinstance(payload.get("wall_ladder"), dict) else {}
    lines: list[str] = []
    for key, label in (("put_walls", "put 墙阶梯(下方支撑)"), ("call_walls", "call 墙阶梯(上方阻力)")):
        rungs = [rung for rung in (ladder.get(key) or []) if isinstance(rung, dict)]
        if not rungs:
            continue
        # Payload order is GEX rank; rungs[0] is the primary wall. Display in
        # spatial order (nearest first) so it reads as an actual ladder.
        primary_strike = rungs[0].get("strike")
        spatial = sorted(
            rungs,
            key=lambda rung: -(rung.get("strike") or 0.0),
            reverse=(key == "call_walls"),
        )
        parts = []
        for rung in spatial:
            strike = _dash(rung.get("strike"))
            if rung.get("strike") == primary_strike:
                strike = f"★{strike}"
            oi = rung.get("open_interest")
            oi_text = f"OI {int(oi)}" if isinstance(oi, (int, float)) and oi > 0 else "OI -"
            prob = rung.get("prob_touch")
            prob_text = f",触达{_fmt_prob(prob)}" if prob is not None else ""
            parts.append(f"{strike}({oi_text}{prob_text})")
        lines.append(f"{label}: " + " > ".join(parts) + " (★=主墙)")
    return lines


def _candidate_by_play(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    raw_candidates = payload.get("candidates")
    if not isinstance(raw_candidates, list):
        return {}
    mapped: dict[str, dict[str, Any]] = {}
    for item in raw_candidates:
        if isinstance(item, dict) and isinstance(item.get("play"), str):
            mapped[item["play"]] = item
    return mapped


def render_template(payload: dict[str, Any]) -> str:
    trading_date = payload.get("trading_date") or "-"
    beijing_time = payload.get("beijing_time") or "14:00"
    expiry = payload.get("expiry") or "-"

    underlier = payload.get("underlier") if isinstance(payload.get("underlier"), dict) else {}
    underlier_price = underlier.get("price")
    underlier_source = underlier.get("source") or "-"

    expected_move = payload.get("expected_move_points")
    gamma_state = payload.get("gamma_state") or "-"
    zero_gamma = payload.get("zero_gamma")
    flip_zone = payload.get("flip_zone") if isinstance(payload.get("flip_zone"), list) else None

    flip_lo = _dash(flip_zone[0]) if flip_zone and len(flip_zone) >= 2 else "-"
    flip_hi = _dash(flip_zone[1]) if flip_zone and len(flip_zone) >= 2 else "-"

    lines = [
        f"【挂单地图 {trading_date}】(北京 {beijing_time},0DTE={expiry})",
        *(
            [f"时段: {phase.get('name_cn')} — {phase.get('traits')}"]
            if isinstance(phase := payload.get("session_phase"), dict) and phase.get("name_cn")
            else []
        ),
        (
            f"参考价: {_dash(underlier_price)}({underlier_source}), "
            f"预期波幅 ±{_dash(expected_move)} 点"
        ),
        (
            f"gamma: {gamma_state}, zero gamma {_dash(zero_gamma)}, "
            f"flip zone {flip_lo}-{flip_hi}"
        ),
    ]
    day_move_line = _day_move_line(payload)
    if day_move_line:
        lines.append(day_move_line)
    es_volume_line = _es_volume_line(payload)
    if es_volume_line:
        lines.append(es_volume_line)
    hl_volume_line = _hl_volume_line(payload)
    if hl_volume_line:
        lines.append(hl_volume_line)
    ladder_lines = _wall_ladder_lines(payload)
    lines.extend(ladder_lines)
    density_line = _rn_density_line(payload)
    if density_line:
        lines.append(density_line)

    by_play = _candidate_by_play(payload)
    for index, play in enumerate(PLAY_ORDER, start=1):
        candidate = by_play.get(play)
        if candidate is None:
            lines.append(f"{index}) -")
            continue
        level_label = candidate.get("level_label") or "-"
        strike = candidate.get("strike")
        right = candidate.get("right") or ""
        headline = PLAY_TEMPLATE_LINES[play].format(
            level_label=level_label,
            strike=strike,
            right=right,
        )
        lines.append(f"{index}) {headline}")
        lines.append(
            "   触达概率≈"
            f"{_fmt_prob(candidate.get('prob_touch'))}, "
            f"到位时预估价≈{_fmt_premium(candidate.get('projected_mid'))}"
            f"(现价 {_fmt_premium(candidate.get('current_mid'))})"
        )
        if candidate.get("order_style") == "stop_trigger":
            lines.append(
                "   注意: 预估价高于现价,被动限价会立即成交;"
                "此单需破位确认后下条件单/市价,不适合提前挂"
            )
        else:
            lines.append(
                "   挂单参考: 激进 "
                f"{_fmt_premium(candidate.get('limit_aggressive'))} / 保守 "
                f"{_fmt_premium(candidate.get('limit_conservative'))}"
            )
            eta = candidate.get("touch_eta_minutes")
            if isinstance(eta, (int, float)) and eta > 0:
                lines.append(
                    f"   时效: 预计 ≈{_fmt_eta_minutes(float(eta))} 到位; "
                    "超约 2 倍时间未到, 赔率已变质, 先撤"
                )
            frontrun_level = candidate.get("frontrun_level")
            if frontrun_level is not None:
                lines.append(
                    f"   先手挡 {_dash(frontrun_level)}: 限价 "
                    f"{_fmt_premium(candidate.get('frontrun_limit'))}, "
                    f"触达≈{_fmt_prob(candidate.get('frontrun_prob_touch'))}"
                    "(墙前反转也能吃到)"
                )

    lines.append(
        "注: 墙位是 OI 真实聚集处(多在整数位),但价格常在墙前几点反转;"
        "先手挡=向现价方向让 30% 距离,成交率高、价格稍差。"
        "预估价按 BS 重定价,已计触达前的时间衰减与 vol 斜率(跌到位 IV 上抬/涨到位 IV 回落);"
        "保守价≈预估×0.85。"
        "提醒: 0DTE 权利金随时间单边衰减,纯权利金限价单可能在指数未到位时就被时间衰减打成;"
        "要严格按点位入场,用指数条件单(SPX 到 XX 触发限价)更精确。仅供参考,不是订单指令。"
    )

    warnings = payload.get("warnings")
    if isinstance(warnings, list) and warnings:
        lines.append(f"数据警告: {'; '.join(str(item) for item in warnings)}")

    return "\n".join(lines)


def build_order_prompt(
    payload: dict[str, Any],
    template: str,
    previous_push: dict[str, Any] | None = None,
) -> str:
    return "\n".join(
        (
            "这条是当天第一张『挂单地图』。搭档下午刚坐到屏幕前，要拿这张图定今天的埋伏方案：挂什么单、挂什么价、赌的是什么。",
            "动笔前先在心里过一遍(不写出来)：今天的 OI 是怎么摆的——put 侧是密集防线还是孤零零一档？dealer 在现价附近是"
            "正 gamma 压波动还是负 gamma 放大波动？今天的 play 里哪张是真机会、哪张只是模板凑数？想清楚再落笔，观点要有取舍，"
            "三张单同等推荐等于没推荐。",
            "",
            "输出中文，最多 18 行。第一行以『挂单参考:』开头，复述模板第一行的日期与时间。",
            "接着给地形定调：pin 还是 transition，为什么(gamma 状态+价格相对 flip 的位置)，今天哪类 play 优先。",
            "墙位讲阶梯不讲孤点(数据在 wall_ladder，OI 定位)：相邻 put 墙 OI 接近(差三成以内)就说成一条支撑带并给出"
            "破了之后的二、三档；第一档独大才说单点硬墙。call 侧同理。",
            "rn_density(B-L 风险中性分布)可用时引用：市场把收盘定价在哪个中位、80% 区间在哪；给垂直价差选腿时"
            "买腿放赌的方向内、卖腿放 80% 区间外沿附近最划算；quality 非 ok 时注明并降权。",
            "",
            "然后逐条 play(最多 3 条，每条 2-3 行)，每条都要把账算给他看：",
            "- 墙位价 vs 先手挡价的取舍：墙位价便宜但常在墙前几点反转吃不到，先手挡成交率高；预估价已含触达前的"
            "时间衰减与 vol 斜率(BS 重定价)，比现价低不是便宜，是时间价值正常流失；",
            "- 赔率账：触达概率、到位预估价、现价放一起，这笔单赌的是一次多大概率的什么事，赔付幅度配不配得上这个概率；",
            "- resting_limit 提醒：0DTE 纯权利金限价可能因时间衰减在指数未到位时提前成交，严格按点位入场改用指数条件单(SPX 触及 XX 时下限价)；",
            "- order_style=stop_trigger 必须提醒：预估价高于现价，预挂被动限价会立即成交，等破位确认后用条件单。",
            "",
            "最后 2-3 行 if/then：开盘前参考价/ES 走到哪些具体位置，哪张单赔率变差该撤或改价，哪个剧本作废——这就是这张图的证伪条件。",
            "es_volume 可用且 label 非 no_baseline/session_reset 时，读量价事件(event_id)而不是只读放量/缩量："
            "字段含 direction(涨跌)、location(贴墙/flip/中间/破位侧)、sequence、break_outcome(holds/reclaimed/pending)、play_hints。"
            "用法按 play 对号入座——put wall 反弹想看 elevated_sell_into_support 后出现 quiet_reclaim_after_sell_test；"
            "flip 破位 put 想看 elevated_break_holds / quiet_breakdown_holds，最怕 break_reclaimed；"
            "call wall fade 想看 elevated_buy_into_resistance 后滞涨，最怕 quiet 站稳在墙上方。"
            "quiet_mid_range / elevated_mid_range 都是半路，不追单。",
            "hl_volume(HL SP500 永续，24/7 薄代理)只当次级证据：与 ES 同向加一分确认，分歧提示 crypto 侧先动或噪声；"
            "aggressor_buy_ratio/book_imbalance 是 ES 没有的方向色彩；ES 停盘/周末时它是唯一量价源，但绝不单独确认破位。",
            "每张单的 touch_eta_minutes 是按布朗缩放估的到位耗时：给出时效纪律——约 2 倍该时间价格还没来，"
            "赔率已被 theta 吃掉，写明大约几点(北京)前不来就撤单。",
            "session_phase 是搭档的时钟：这张图会跨欧盘、美盘数据小时和开盘使用，建议要写清哪些单是欧盘就能成交的埋伏、"
            "哪些要等美盘数据落地校准后才算数；不许把『等开盘』当默认建议。",
            "day_move.em_used_fraction ≥ 0.7 时点明：日内已走完预期波幅的多少，顺方向追单赔率差；挂单纪律是等价格来找你，不去半路追它。",
            "previous_push 是上一条推送正文；关键位相对它有实质变化就在定调处说『剧本有变』并指出哪张单要改，没变化不必提。",
            "previous_push:" + previous_push_json(previous_push),
            "JSON:" + json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            "模板:" + template,
        )
    )




def send_order_map(
    payload: dict[str, Any],
    settings: NotificationSettings,
    *,
    runner: CommandRunner = default_runner,
    now: datetime | None = None,
    extra_header: str | None = None,
    previous_push: dict[str, Any] | None = None,
) -> dict[str, Any]:
    now = now or datetime.now(tz=timezone.utc)
    template = render_template(payload)
    if extra_header:
        template = f"{extra_header}\n{template}"
    text, writer = generate_push_text(
        template,
        build_order_prompt(payload, template, previous_push),
        settings,
        runner=runner,
    )

    delivery_sinks = deliver_trade_push(
        settings,
        title="挂单地图",
        text=text,
        kind="order_map",
        lane="trade",
        friend=True,
        runner=runner,
    )
    delivered_ok = any_delivery_ok(delivery_sinks)
    if not im_delivery_ok(delivery_sinks):
        append_missed(settings.missed_queue_path, text, kind="order_map", at=now)

    return {
        "text": text,
        "writer": writer,
        "used_agent": writer != "template",
        "weixin_ok": any(s.sink == "openclaw_message" and s.ok for s in delivery_sinks),
        "bark_ok": any(s.sink == "bark" and s.ok for s in delivery_sinks),
        "feishu_ok": any(s.sink == "feishu" and s.ok for s in delivery_sinks),
        "delivered_ok": delivered_ok,
    }


def default_state_path(settings: StorageSettings) -> str:
    return os.getenv("SPX_ORDER_MAP_STATE_PATH") or str(
        Path(settings.data_root) / "latest" / "order_map_state.json"
    )


# --- fixed-cadence refresh: re-push the order map every 30 minutes (interleaved
# with the status report), annotating material level changes in the header ---

REFRESH_COOLDOWN_SECONDS_DEFAULT = 1500.0
MATERIAL_LEVEL_MOVE_POINTS = 5.0
MATERIAL_EM_REL_CHANGE = 0.20

# --- status report: fixed cadence across the partner's working day (Beijing
# 08:30 -> next-day 02:00; hourly through the morning, every 30 minutes from
# 14:00 -- density is set by the systemd timer, this window only bounds it) ---

STATUS_WINDOW_START = time(8, 30)
STATUS_WINDOW_END_EARLY = time(2, 0)
US_OPEN_ET = time(9, 30)


def payload_fingerprint(payload: dict[str, Any]) -> dict[str, Any]:
    """Key levels that define the order map; used to detect material changes."""
    by_play = _candidate_by_play(payload)

    def level_of(play: str) -> float | None:
        candidate = by_play.get(play)
        return finite_float(candidate.get("level")) if candidate else None

    flip_zone = payload.get("flip_zone") if isinstance(payload.get("flip_zone"), list) else None
    return {
        "expiry": payload.get("expiry"),
        "put_wall": level_of("put_wall_bounce_call"),
        "flip_low": finite_float(flip_zone[0]) if flip_zone and len(flip_zone) >= 2 else None,
        "flip_high": finite_float(flip_zone[1]) if flip_zone and len(flip_zone) >= 2 else None,
        "call_wall": level_of("call_wall_fade_put"),
        "expected_move_points": finite_float(payload.get("expected_move_points")),
    }


def material_changes(
    previous: dict[str, Any] | None,
    current: dict[str, Any],
) -> list[str]:
    """Human-readable list of material level changes since the last push."""
    if not isinstance(previous, dict):
        return []
    changes: list[str] = []
    if previous.get("expiry") != current.get("expiry"):
        changes.append(f"到期日切换 {previous.get('expiry')}→{current.get('expiry')}")
        return changes

    labels = {
        "put_wall": "put wall",
        "flip_low": "flip zone 下界",
        "flip_high": "flip zone 上界",
        "call_wall": "call wall",
    }
    for key, label in labels.items():
        prev_value = finite_float(previous.get(key))
        cur_value = finite_float(current.get(key))
        if prev_value is None and cur_value is None:
            continue
        if prev_value is None or cur_value is None:
            changes.append(f"{label} {_dash(prev_value)}→{_dash(cur_value)}")
            continue
        if abs(cur_value - prev_value) >= MATERIAL_LEVEL_MOVE_POINTS:
            changes.append(f"{label} {_dash(prev_value)}→{_dash(cur_value)}")

    prev_em = finite_float(previous.get("expected_move_points"))
    cur_em = finite_float(current.get("expected_move_points"))
    if prev_em and cur_em and prev_em > 0:
        if abs(cur_em / prev_em - 1.0) >= MATERIAL_EM_REL_CHANGE:
            changes.append(f"预期波幅 ±{_dash(prev_em)}→±{_dash(cur_em)} 点")
    return changes


def within_refresh_window(now_utc: datetime) -> bool:
    """Same window as the status report: the map refresh runs on a fixed
    30-minute cadence (offset by 15 minutes from status) instead of only
    firing on material changes."""
    return within_status_window(now_utc)


def within_status_window(now_utc: datetime) -> bool:
    """Beijing 08:30 through next-day 02:00: the partner's full working day.

    The timer controls density (hourly through the Beijing morning, every 30
    minutes from 14:00); this gate only bounds the day. The after-midnight leg
    belongs to the previous day's session, so it runs on Tue-Sat local
    mornings (Sat 00:xx = Friday's US session).
    """
    local = now_utc.astimezone(SHANGHAI_TZ)
    if local.time() >= STATUS_WINDOW_START:
        return local.weekday() < 5
    if local.time() < STATUS_WINDOW_END_EARLY:
        return local.weekday() in (1, 2, 3, 4, 5)
    return False


def minutes_to_open(now_utc: datetime) -> int | None:
    ny = now_utc.astimezone(NY_TZ)
    open_dt = ny.replace(hour=US_OPEN_ET.hour, minute=US_OPEN_ET.minute, second=0, microsecond=0)
    if ny >= open_dt:
        return None
    return int((open_dt - ny).total_seconds() // 60)


# --- session phase: the partner's clock, not the exchange's -----------------
# The reader works Beijing 08:30 -> next-day 01:00 (ET ~20:30 -> ~13:00 in
# summer). He sleeps through the entire US afternoon and close, and for most
# of his waking day the market IS live (Globex futures + SPX GTH options).
# Every push must speak to the phase of HIS day instead of defaulting to
# "wait for the open".

USER_DAY_START_BJ = time(8, 30)
USER_DAY_END_BJ = time(1, 0)  # bedtime: positions after this are unattended
US_CLOSE_ET = time(16, 0)

SESSION_PHASES_ET: tuple[tuple[time, time, str, str, str], ...] = (
    (
        time(18, 0),
        time(2, 0),
        "asia_globex",
        "亚盘夜盘",
        "Globex+GTH 流动性薄、期权点差宽; 复盘昨日、搭今天骨架, 只挂远端埋伏单",
    ),
    (
        time(2, 0),
        time(8, 30),
        "europe_session",
        "欧盘时段",
        "欧洲接力后 ES 开始有真方向尝试; 研究和布挂单的黄金窗",
    ),
    (
        time(8, 30),
        time(9, 30),
        "us_data_hour",
        "美盘数据前小时",
        "ET 8:30 宏观数据落地、EM/IV 重定价; 挂单最后校准窗",
    ),
    (
        time(9, 30),
        time(10, 30),
        "us_open_hour",
        "开盘首小时",
        "假突破多、OI 刷新后墙才作数; 等回踩确认再动手",
    ),
    (
        time(10, 30),
        time(13, 0),
        "us_morning_battle",
        "美盘上午主战场",
        "趋势/区间定型, 搭档唯一在场的执行窗; 睡前必须处理完仓位",
    ),
    (
        time(13, 0),
        time(16, 0),
        "us_afternoon_unattended",
        "无人值守下午",
        "搭档已睡; theta 磨损+尾盘对冲解锁, 留下的仓位必须已带 bracket",
    ),
    (
        time(16, 0),
        time(18, 0),
        "post_close",
        "盘后过渡",
        "现金已收、期货重定价; 复盘与次日准备",
    ),
)


def _phase_contains(start: time, stop: time, now_t: time) -> bool:
    if start <= stop:
        return start <= now_t < stop
    return now_t >= start or now_t < stop


def session_phase(now_utc: datetime) -> dict[str, Any]:
    ny = now_utc.astimezone(NY_TZ)
    bj = now_utc.astimezone(SHANGHAI_TZ)
    name, name_cn, traits = "asia_globex", "亚盘夜盘", ""
    for start, stop, phase_name, phase_cn, phase_traits in SESSION_PHASES_ET:
        if _phase_contains(start, stop, ny.time()):
            name, name_cn, traits = phase_name, phase_cn, phase_traits
            break

    open_dt = ny.replace(hour=US_OPEN_ET.hour, minute=US_OPEN_ET.minute, second=0, microsecond=0)
    close_dt = ny.replace(hour=US_CLOSE_ET.hour, minute=US_CLOSE_ET.minute, second=0, microsecond=0)
    if ny >= close_dt:
        # Evening: count down to the NEXT session's open (skip weekends).
        open_dt += timedelta(days=1)
        while open_dt.weekday() >= 5:
            open_dt += timedelta(days=1)
    minutes_to_us_open = int((open_dt - ny).total_seconds() // 60) if ny < open_dt else None
    minutes_since_us_open = (
        int((ny - open_dt).total_seconds() // 60) if open_dt <= ny < close_dt else None
    )
    minutes_to_us_close = int((close_dt - ny).total_seconds() // 60) if ny < close_dt else None

    # Bedtime countdown: next Beijing 01:00. No countdown while asleep
    # (Beijing 01:00-08:30).
    user_awake = not (USER_DAY_END_BJ <= bj.time() < USER_DAY_START_BJ)
    minutes_to_bedtime = None
    if user_awake:
        bedtime = bj.replace(
            hour=USER_DAY_END_BJ.hour, minute=USER_DAY_END_BJ.minute, second=0, microsecond=0
        )
        if bj.time() >= USER_DAY_END_BJ:
            bedtime += timedelta(days=1)
        minutes_to_bedtime = int((bedtime - bj).total_seconds() // 60)

    return {
        "name": name,
        "name_cn": name_cn,
        "traits": traits,
        "beijing_now": bj.strftime("%H:%M"),
        "minutes_to_us_open": minutes_to_us_open,
        "minutes_since_us_open": minutes_since_us_open,
        "minutes_to_us_close": minutes_to_us_close,
        "minutes_to_bedtime": minutes_to_bedtime,
        "user_awake": user_awake,
    }


def _phase_clock_text(phase: dict[str, Any]) -> str:
    parts = [str(phase.get("name_cn") or "-")]
    since_open = phase.get("minutes_since_us_open")
    to_open = phase.get("minutes_to_us_open")
    if since_open is not None:
        parts.append(f"开盘后 {since_open} 分钟")
    elif to_open is not None:
        parts.append(f"距开盘 {to_open} 分钟")
    to_bed = phase.get("minutes_to_bedtime")
    if isinstance(to_bed, int) and to_bed <= 180:
        parts.append(f"距收官 {to_bed} 分钟")
    return ", ".join(parts)


def _session_phase_of(payload: dict[str, Any], now_utc: datetime) -> dict[str, Any]:
    phase = payload.get("session_phase")
    if isinstance(phase, dict) and phase.get("name_cn"):
        return phase
    return session_phase(now_utc)


def load_order_map_state(state_path: str) -> dict[str, Any]:
    try:
        payload = json.loads(Path(state_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def within_send_window(now_utc: datetime) -> bool:
    local = now_utc.astimezone(SHANGHAI_TZ)
    if local.weekday() >= 5:
        return False
    current = local.time()
    return BJ_WINDOW_START <= current < BJ_WINDOW_END


def already_sent(state_path: str, trading_date: str) -> bool:
    path = Path(state_path)
    if not path.exists():
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if not isinstance(payload, dict):
        return False
    # Baseline idempotency tracks map pushes specifically; status reports also
    # write last_sent_date and must not mask a failed baseline.
    return payload.get("last_map_date") == trading_date


def mark_sent(
    state_path: str,
    trading_date: str,
    *,
    fingerprint: dict[str, Any] | None = None,
    now: datetime | None = None,
    kind: str | None = None,
) -> None:
    path = Path(state_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Merge into existing state: map pushes and status reports interleave on
    # separate cadences, so one must not wipe the other's timestamp.
    payload = load_order_map_state(state_path)
    payload["last_sent_date"] = trading_date
    if kind:
        payload[f"last_{kind}_date"] = trading_date
    if fingerprint is not None:
        payload["fingerprint"] = fingerprint
    if now is not None:
        payload["last_sent_at"] = now.timestamp()
        if kind:
            payload[f"last_{kind}_at"] = now.timestamp()
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _level_probs_line(payload: dict[str, Any]) -> str:
    by_play = _candidate_by_play(payload)
    parts: list[str] = []
    for play in PLAY_ORDER:
        candidate = by_play.get(play)
        if candidate is None:
            continue
        parts.append(
            f"{candidate.get('level_label') or '-'} 触达≈{_fmt_prob(candidate.get('prob_touch'))}"
        )
    return "; ".join(parts) if parts else "-"


def render_status_template(
    payload: dict[str, Any],
    changes: list[str],
    now_utc: datetime,
) -> str:
    beijing = now_utc.astimezone(SHANGHAI_TZ)
    phase = _session_phase_of(payload, now_utc)
    open_text = _phase_clock_text(phase)

    underlier = payload.get("underlier") if isinstance(payload.get("underlier"), dict) else {}
    vol = payload.get("vol_context") if isinstance(payload.get("vol_context"), dict) else {}
    flip_zone = payload.get("flip_zone") if isinstance(payload.get("flip_zone"), list) else None
    flip_lo = _dash(flip_zone[0]) if flip_zone and len(flip_zone) >= 2 else "-"
    flip_hi = _dash(flip_zone[1]) if flip_zone and len(flip_zone) >= 2 else "-"

    lines = [
        f"【市场状态 {beijing.strftime('%H:%M')}】(0DTE={payload.get('expiry') or '-'}, {open_text})",
        f"时段: {phase.get('name_cn')} — {phase.get('traits')}",
        (
            f"参考价: {_dash(underlier.get('price'))}({underlier.get('source') or '-'}); "
            f"ES {_dash(payload.get('es_last'))}; HL perp {_dash(payload.get('hl_sp500_perp'))}"
        ),
        (
            f"gamma: {payload.get('gamma_state') or '-'}, "
            f"zero gamma {_dash(payload.get('zero_gamma'))}, flip zone {flip_lo}-{flip_hi}, "
            f"预期波幅 ±{_dash(payload.get('expected_move_points'))} 点"
        ),
        f"关键位: {_level_probs_line(payload)}",
        *( [line] if (line := _day_move_line(payload)) else [] ),
        *( [line] if (line := _es_volume_line(payload)) else [] ),
        *( [line] if (line := _hl_volume_line(payload)) else [] ),
        *_wall_ladder_lines(payload),
        *( [line] if (line := _rn_density_line(payload)) else [] ),
        (
            f"vol: VIX {_dash(vol.get('vix'))}, VIX1D {_dash(vol.get('vix1d'))}, "
            f"VVIX {_dash(vol.get('vvix'))}, SKEW {_dash(vol.get('skew'))}"
        ),
    ]
    if changes:
        lines.append(f"较上次推送变化: {'; '.join(changes)}")
    else:
        lines.append("较上次推送: 关键位无实质变化")

    warnings = payload.get("warnings")
    if isinstance(warnings, list) and warnings:
        lines.append(f"数据警告: {'; '.join(str(item) for item in warnings)}")
    return "\n".join(lines)


def build_status_prompt(
    payload: dict[str, Any],
    template: str,
    previous_push: dict[str, Any] | None = None,
) -> str:
    return "\n".join(
        (
            "这条是『市场状态+挂单参考』二合一便签，每 30 分钟一条。搭档已经按上一张地图挂好单了，"
            "他扫一眼要能决定：单动不动、价来了接不接。这不是行情播报，是接班交接——上一班的判断在 previous_push 里，"
            "你要么确认它还成立，要么指出哪里被市场证伪了。",
            "",
            "动笔前先在心里过一遍(不要写出来)：现在价格站的这个位置，是谁的地盘？下方 put 墙的 OI 是真金白银的防守还是昨天的尸体？"
            "dealer 在这个价位是被迫买还是被迫卖(gamma 正负)？时间衰减在帮谁？想清楚了再写结论。",
            "",
            "输出中文，14-20 行。第一行以『市场状态:』开头，保留模板第一行的时间与时段信息，紧跟一句定调：",
            "『剧本维持』或『剧本有变: 变在哪』——判断基准是 previous_push 正文和模板『较上次推送』行。",
            "",
            "session_phase 是搭档的时钟：便签必须落在当前时段的语境里(traits 字段是提示)。亚盘/欧盘时段市场在交易",
            "(Globex+GTH)，不许写『等开盘再说』——该说的是这个时段的地形有多可信、埋伏单摆哪里；开盘首小时提防假突破；",
            "主战场时段(北京 22:30-1:00)直接谈执行。minutes_to_bedtime ≤ 60 时这条是【睡前收官】便签：",
            "正文以收官为主——逐张说未成交挂单撤/留、持仓带什么 bracket(止盈止损给具体价)、哪些单绝不能裸奔进无人值守的",
            "美盘下午；证伪条件写给醒着的最后一小时，而不是写给睡着的他。",
            "",
            "正文必须覆盖(顺序自己组织，写成连贯的段落而不是清单)：",
            "- 位置：参考价在 flip zone/zero gamma/两侧墙位阶梯里站在哪，距各关键位几点，这个位置意味着 pin 还是易加速；"
            "相邻 put 墙 OI 接近(差三成以内)就说成支撑带并报出二、三档，别只报一个点；",
            "- 赔率：三张挂单的触达概率各多少、相对上一条谁在改善谁在恶化(引用具体百分比变化)，此刻哪张性价比最高、为什么；",
            "- 市场定价对照(rn_density quality=ok 时)：市场把收盘定价在哪个中位、80% 区间在哪，当前价格相对它偏回归还是已到尾部；",
            "- vol：VIX1D/VIX 说明今天的 vol 卖得贵还是便宜，SKEW 异常时说明谁在抢保护；",
            "- 量价事件(es_volume 可用时)：不要只说放量/缩量。读 event_id + direction + location + break_outcome："
            "放量砸支撑(elevated_sell_into_support)是墙测试不是自动破位；缩量收回(quiet_reclaim_after_sell_test)才升温反弹；"
            "破位站稳(holds)才给破位单开灯，破位收回(reclaimed)则假破降权；中间地带(quiet/elevated_mid_range)半路不追。"
            "play_hints 里有现成句子可直接引用。label=no_baseline/session_reset 时不引用；",
            "- hl_volume 是 Hyperliquid SP500 永续的量价(24/7 薄流动性代理)，只当次级证据：与 ES 量价同向可加一分确认，"
            "分歧时提示 crypto 侧资金先动或只是噪声；aggressor_buy_ratio(主动买占比)和 book_imbalance(盘口失衡)是"
            "ES 给不了的方向色彩；周末与 ES 停盘时它是唯一量价源。绝不允许单独用它确认破位；",
            "- 每张挂单的 touch_eta_minutes 是按布朗缩放估的到位耗时：写挂单参考时给出时效纪律——超过约 2 倍该时间"
            "价格还没来，这单的赔率已被 theta 吃掉，写明大约几点(北京时间)前不来就撤；",
            "- 双向 if/then：上行到哪个具体位置、下行到哪个具体位置，分别哪张单该撤/改价、哪个剧本激活——这也是你这个判断的证伪条件；",
            "- 情绪拦截(违反即失职)：价格在中间地带就写明『此处不追单，计划位在 XX/XX』；day_move.em_used_fraction ≥ 0.7 就写明"
            "『日内已走完预期波幅的 X%，顺方向追单赔率差』；价格进 put 墙支撑带就写明『计划中的接多区不是恐慌区，防守只在跌破 XX 后执行』，"
            "进 call 墙带对称处理。哪条情况成立写哪条，都不成立就不硬写。",
            "",
            "倒数第二段固定是『挂单参考』段，3-6 行：从模板的挂单地图部分逐字引用每张单的合约、墙位限价、先手挡价、触达概率，"
            "数字照抄不改写；stop_trigger 的 play 保留『勿预挂限价、用指数条件单』提醒；限价相对上一条有变化就点出方向。",
            "最后 1 行：到下条推送之间最值得盯的一个量，以及它变到什么程度你会改判断；"
            "这个量必须是本系统数据里有的(参考价/触达概率/gamma 状态/墙位 OI/VIX/VIX1D/ES 量能节奏)，"
            "不要让搭档去盯我们不推送的量(如内盘外盘)。",
            "",
            "剧本维持时照样给完整读数，别因为『没变』缩成三行；也别硬编不存在的变化，数字平稳就说平稳。",
            "previous_push:" + previous_push_json(previous_push),
            "JSON:" + json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            "模板:" + template,
        )
    )


def run_status(
    args: argparse.Namespace,
    *,
    now: datetime,
    state_path: str,
    trading_date: str,
    runner: CommandRunner = default_runner,
) -> int:
    if not args.force and not within_status_window(now):
        print(json.dumps({"skipped": True, "reason": "outside_status_window"}))
        return 0

    previous = load_order_map_state(state_path)
    payload = build_order_payload_with_retry(StorageSettings.from_env(), now=now)
    if _payload_is_thin(payload) and not args.force:
        # Normal sampling gap (slow poll / line rotation), not an outage:
        # skip quietly, the next 30-minute run will have full data.
        print(json.dumps({"skipped": True, "reason": "thin_snapshot_sampling_gap"}))
        return 0
    fingerprint = payload_fingerprint(payload)
    changes = material_changes(previous.get("fingerprint"), fingerprint)
    # Combined push: status narrative + the order-map limit table used to be
    # two interleaved 30-minute pushes; the map template rides along so the
    # writer (and the raw fallback) always carries concrete limit prices.
    template = "\n".join(
        (render_status_template(payload, changes, now), render_template(payload))
    )

    if args.dry_run:
        print(template)
        print(json.dumps({"dry_run": True, "changes": changes}, ensure_ascii=False))
        return 0

    settings = NotificationSettings.from_env()
    text, writer = generate_push_text(
        template,
        build_status_prompt(payload, template, load_previous_push()),
        settings,
        runner=runner,
    )
    delivery_sinks = deliver_trade_push(
        settings,
        title="市场状态",
        text=text,
        kind="status",
        lane="trade",
        friend=True,
        runner=runner,
    )
    delivered_ok = any_delivery_ok(delivery_sinks)
    if not im_delivery_ok(delivery_sinks):
        append_missed(settings.missed_queue_path, text, kind="order_map_status", at=now)
    weixin_ok = any(s.sink == "openclaw_message" and s.ok for s in delivery_sinks)
    bark_ok = any(s.sink == "bark" and s.ok for s in delivery_sinks)
    feishu_ok = any(s.sink == "feishu" and s.ok for s in delivery_sinks)

    if delivered_ok:
        mark_sent(state_path, trading_date, fingerprint=fingerprint, now=now, kind="status")
        record_push("market_status", text, at=now.isoformat())
    result = {
        "text": text,
        "writer": writer,
        "weixin_ok": weixin_ok,
        "bark_ok": bark_ok,
        "feishu_ok": feishu_ok,
        "delivered_ok": delivered_ok,
        "changes": changes,
    }
    print(json.dumps(result, ensure_ascii=False))
    if not delivered_ok:
        return 1
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send SPX Spark order map push.")
    parser.add_argument("--dry-run", action="store_true", help="Print template only.")
    parser.add_argument("--force", action="store_true", help="Skip time window and idempotency gate.")
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Re-push only when key levels moved materially since the last push.",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Push a market status report (fixed cadence, Beijing 14:15 -> US open).",
    )
    return parser.parse_args(argv)


def run_refresh(args: argparse.Namespace, *, now: datetime, state_path: str, trading_date: str) -> int:
    if not args.force and not within_refresh_window(now):
        print(json.dumps({"skipped": True, "reason": "outside_refresh_window"}))
        return 0

    previous = load_order_map_state(state_path)
    if not args.force and previous.get("last_map_date") != trading_date:
        print(json.dumps({"skipped": True, "reason": "no_baseline_push_today"}))
        return 0
    # Cooldown is keyed on map pushes only (baseline + refreshes); the
    # interleaved status reports must not reset it.
    last_map_at = finite_float(previous.get("last_map_at"))
    cooldown = float(
        os.getenv("SPX_ORDER_MAP_REFRESH_COOLDOWN_SECONDS", "")
        or REFRESH_COOLDOWN_SECONDS_DEFAULT
    )
    if not args.force and last_map_at is not None and now.timestamp() - last_map_at < cooldown:
        print(json.dumps({"skipped": True, "reason": "refresh_cooldown"}))
        return 0

    payload = build_order_payload_with_retry(StorageSettings.from_env(), now=now)
    if _payload_is_thin(payload) and not args.force:
        print(json.dumps({"skipped": True, "reason": "thin_snapshot_sampling_gap"}))
        return 0
    fingerprint = payload_fingerprint(payload)
    changes = material_changes(previous.get("fingerprint"), fingerprint)

    if changes:
        header = f"【挂单地图·更新】变化: {'; '.join(changes)}"
    else:
        header = "【挂单地图·更新】关键位无实质变化，限价随最新报价刷新"
    if args.dry_run:
        print(header)
        print(render_template(payload))
        print(json.dumps({"dry_run": True, "changes": changes}, ensure_ascii=False))
        return 0

    settings = NotificationSettings.from_env()
    result = send_order_map(
        payload, settings, now=now, extra_header=header, previous_push=load_previous_push()
    )
    if result.get("delivered_ok") or result["weixin_ok"] or result["bark_ok"] or result.get("feishu_ok"):
        mark_sent(state_path, trading_date, fingerprint=fingerprint, now=now, kind="map")
        record_push("order_map_refresh", result["text"], at=now.isoformat())
    result["changes"] = changes
    print(json.dumps(result, ensure_ascii=False))
    if not (result.get("delivered_ok") or result["weixin_ok"] or result["bark_ok"] or result.get("feishu_ok")):
        return 1
    return 0


def run(argv: list[str] | None = None, *, now: datetime | None = None) -> int:
    args = parse_args(argv)
    now = now or datetime.now(tz=timezone.utc)
    storage_settings = StorageSettings.from_env()
    state_path = default_state_path(storage_settings)
    trading_date = now.astimezone(NY_TZ).date().isoformat()

    if args.status:
        return run_status(args, now=now, state_path=state_path, trading_date=trading_date)

    if args.refresh:
        return run_refresh(args, now=now, state_path=state_path, trading_date=trading_date)

    if not args.force and not args.dry_run:
        if not within_send_window(now):
            print(json.dumps({"skipped": True, "reason": "outside_send_window"}))
            return 0
        if already_sent(state_path, trading_date):
            print(json.dumps({"skipped": True, "reason": "already_sent"}))
            return 0

    payload = build_order_payload_with_retry(storage_settings, now=now)
    template = render_template(payload)

    if args.dry_run:
        print(template)
        print(json.dumps({"dry_run": True}))
        return 0

    settings = NotificationSettings.from_env()
    result = send_order_map(payload, settings, now=now, previous_push=load_previous_push())
    if result.get("delivered_ok") or result["weixin_ok"] or result["bark_ok"] or result.get("feishu_ok"):
        mark_sent(
            state_path,
            trading_date,
            fingerprint=payload_fingerprint(payload),
            now=now,
            kind="map",
        )
        record_push("order_map", result["text"], at=now.isoformat())
    print(json.dumps(result, ensure_ascii=False))
    if not (result.get("delivered_ok") or result["weixin_ok"] or result["bark_ok"] or result.get("feishu_ok")):
        return 1
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
