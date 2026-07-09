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
    send_bark_friend_message,
    send_bark_message,
    send_openclaw_message,
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


# --- ES volume pace: cumulative day volume differenced between order-map runs.
# SPX itself never prints volume (it is an index), so ES is the volume proxy.
# The signal compares the latest window's contracts/minute against the median
# pace of the preceding sampled windows (adapts to overnight-vs-RTH regimes);
# with too little history it falls back to the whole-session average pace. ---

ES_VOLUME_SESSION_OPEN_ET = time(18, 0)
ES_VOLUME_MIN_WINDOW_MINUTES = 3.0
ES_VOLUME_MAX_WINDOW_MINUTES = 120.0
ES_VOLUME_ELEVATED_RATIO = 1.5
ES_VOLUME_QUIET_RATIO = 0.5
ES_VOLUME_MAX_SAMPLES = 16
ES_VOLUME_MAX_QUOTE_AGE_SECONDS = 900.0


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


def save_es_volume_samples(path: str, samples: list[dict[str, Any]]) -> None:
    file_path = Path(path)
    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(
            json.dumps({"samples": samples[-ES_VOLUME_MAX_SAMPLES:]}, ensure_ascii=False),
            encoding="utf-8",
        )
    except OSError:
        pass


def es_session_elapsed_minutes(now: datetime) -> float | None:
    """Minutes since the current Globex session opened (18:00 ET)."""
    local = now.astimezone(NY_TZ)
    session_open = local.replace(hour=18, minute=0, second=0, microsecond=0)
    if local.time() < ES_VOLUME_SESSION_OPEN_ET:
        session_open -= timedelta(days=1)
    elapsed = (local - session_open).total_seconds() / 60.0
    return elapsed if elapsed > 1.0 else None


def _parse_sample(sample: dict[str, Any]) -> tuple[datetime, float] | None:
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
    return at, volume


def _window_paces(points: list[tuple[datetime, float]]) -> list[float]:
    """Contracts/minute for each valid consecutive sample pair."""
    paces: list[float] = []
    for (prev_at, prev_volume), (cur_at, cur_volume) in zip(points, points[1:]):
        minutes = (cur_at - prev_at).total_seconds() / 60.0
        if not (ES_VOLUME_MIN_WINDOW_MINUTES <= minutes <= ES_VOLUME_MAX_WINDOW_MINUTES):
            continue
        if cur_volume < prev_volume:  # session rollover inside the pair
            continue
        paces.append((cur_volume - prev_volume) / minutes)
    return paces


def es_volume_signal(
    cumulative: float | None,
    samples: list[dict[str, Any]],
    *,
    now: datetime,
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
    }
    points = [parsed for sample in samples if (parsed := _parse_sample(sample)) is not None]
    points.sort(key=lambda item: item[0])
    if not points:
        return signal
    last_at, last_volume = points[-1]
    if cumulative < last_volume:
        signal["label"] = "session_reset"
        return signal
    window_minutes = (now - last_at).total_seconds() / 60.0
    if not (ES_VOLUME_MIN_WINDOW_MINUTES <= window_minutes <= ES_VOLUME_MAX_WINDOW_MINUTES):
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
    signal.update(
        {
            "delta": round(delta),
            "window_minutes": round(window_minutes, 1),
            "recent_pace_per_min": round(recent_pace, 1),
            "baseline_pace_per_min": round(baseline, 1),
            "baseline": baseline_name,
            "pace_ratio": round(ratio, 2),
            "label": label,
        }
    )
    return signal


def attach_es_volume_signal(
    payload: dict[str, Any],
    state: LatestState,
    *,
    sample_path: str,
    now: datetime,
    persist: bool = True,
) -> None:
    """Compute the ES volume pace signal and append the new sample.

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
    samples = load_es_volume_samples(sample_path)
    payload["es_volume"] = es_volume_signal(cumulative, samples, now=now)
    if persist and cumulative is not None:
        samples.append({"at": now.isoformat(), "volume": cumulative})
        save_es_volume_samples(sample_path, samples)


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
    return (
        f"ES 量能: 最近{window:.0f}分钟 {int(delta):,} 手, "
        f"节奏为{baseline_text}的 {ratio:.1f} 倍({label})"
    )


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
            "es_volume 可用且 label 非 no_baseline/session_reset 时，量能是破位的『性格』不是『真假』开关："
            "放量(≥1.5 倍)破位=分歧大、双方在对打，容易假突破后回抽再走；缩量(≤0.5 倍)破位=一方弃守、共识一边倒，"
            "往往走得更干净但回抽浅。两种都能是真突破，别用『没放量就假』这种一刀切；"
            "真正要警惕的是缩量磨到关键位附近却站不稳——那是流动性真空漂移，不是突破。",
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

    weixin_result = send_openclaw_message(settings, text, runner=runner)
    if not weixin_result.ok:
        append_missed(settings.missed_queue_path, text, kind="order_map", at=now)

    bark_ok = True
    if settings.bark_enabled:
        bark_result = send_bark_message(settings, "挂单地图", text)
        bark_ok = bark_result.ok
    if settings.bark_friend_enabled:
        send_bark_friend_message(settings, "挂单地图", text)

    return {
        "text": text,
        "writer": writer,
        "used_agent": writer != "template",
        "weixin_ok": weixin_result.ok,
        "bark_ok": bark_ok,
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

# --- status report: fixed 30-minute cadence (Beijing 14:15 -> next-day 02:00,
# i.e. through pre-open research and the first ~4.5 hours of the US session) ---

STATUS_WINDOW_START = time(14, 15)
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
    """Beijing 14:15 through next-day 02:00 (covers pre-open + early US session).

    The after-midnight leg belongs to the previous day's session, so it runs
    on Tue-Sat local mornings (Sat 00:xx = Friday's US session).
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
    to_open = minutes_to_open(now_utc)
    open_text = f"距开盘 {to_open} 分钟" if to_open is not None else "已开盘"

    underlier = payload.get("underlier") if isinstance(payload.get("underlier"), dict) else {}
    vol = payload.get("vol_context") if isinstance(payload.get("vol_context"), dict) else {}
    flip_zone = payload.get("flip_zone") if isinstance(payload.get("flip_zone"), list) else None
    flip_lo = _dash(flip_zone[0]) if flip_zone and len(flip_zone) >= 2 else "-"
    flip_hi = _dash(flip_zone[1]) if flip_zone and len(flip_zone) >= 2 else "-"

    lines = [
        f"【市场状态 {beijing.strftime('%H:%M')}】(0DTE={payload.get('expiry') or '-'}, {open_text})",
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
            "输出中文，14-20 行。第一行以『市场状态:』开头，保留模板第一行的时间与距开盘信息，紧跟一句定调：",
            "『剧本维持』或『剧本有变: 变在哪』——判断基准是 previous_push 正文和模板『较上次推送』行。",
            "",
            "正文必须覆盖(顺序自己组织，写成连贯的段落而不是清单)：",
            "- 位置：参考价在 flip zone/zero gamma/两侧墙位阶梯里站在哪，距各关键位几点，这个位置意味着 pin 还是易加速；"
            "相邻 put 墙 OI 接近(差三成以内)就说成支撑带并报出二、三档，别只报一个点；",
            "- 赔率：三张挂单的触达概率各多少、相对上一条谁在改善谁在恶化(引用具体百分比变化)，此刻哪张性价比最高、为什么；",
            "- 市场定价对照(rn_density quality=ok 时)：市场把收盘定价在哪个中位、80% 区间在哪，当前价格相对它偏回归还是已到尾部；",
            "- vol：VIX1D/VIX 说明今天的 vol 卖得贵还是便宜，SKEW 异常时说明谁在抢保护；",
            "- 量能性格(es_volume 可用时)：ES 节奏是破位的性格不是真假开关——"
            "放量(≥1.5 倍)=分歧大、双方对打，假突破后回抽再走的概率高；缩量(≤0.5 倍)=一方弃守、共识一边倒，"
            "走得更干净但回抽浅。两种都能是真突破；真正要警惕的是缩量磨到关键位附近却站不稳(流动性真空漂移)。"
            "label=no_baseline/session_reset 时不引用；",
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
    weixin_result = send_openclaw_message(settings, text, runner=runner)
    if not weixin_result.ok:
        append_missed(settings.missed_queue_path, text, kind="order_map_status", at=now)
    bark_ok = True
    if settings.bark_enabled:
        bark_result = send_bark_message(settings, "市场状态", text)
        bark_ok = bark_result.ok
    if settings.bark_friend_enabled:
        send_bark_friend_message(settings, "市场状态", text)

    if weixin_result.ok or bark_ok:
        mark_sent(state_path, trading_date, fingerprint=fingerprint, now=now, kind="status")
        record_push("market_status", text, at=now.isoformat())
    result = {
        "text": text,
        "writer": writer,
        "weixin_ok": weixin_result.ok,
        "bark_ok": bark_ok,
        "changes": changes,
    }
    print(json.dumps(result, ensure_ascii=False))
    if not weixin_result.ok and not bark_ok:
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
    if result["weixin_ok"] or result["bark_ok"]:
        mark_sent(state_path, trading_date, fingerprint=fingerprint, now=now, kind="map")
        record_push("order_map_refresh", result["text"], at=now.isoformat())
    result["changes"] = changes
    print(json.dumps(result, ensure_ascii=False))
    if not result["weixin_ok"] and not result["bark_ok"]:
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
    if result["weixin_ok"] or result["bark_ok"]:
        mark_sent(
            state_path,
            trading_date,
            fingerprint=payload_fingerprint(payload),
            now=now,
            kind="map",
        )
        record_push("order_map", result["text"], at=now.isoformat())
    print(json.dumps(result, ensure_ascii=False))
    if not result["weixin_ok"] and not result["bark_ok"]:
        return 1
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
