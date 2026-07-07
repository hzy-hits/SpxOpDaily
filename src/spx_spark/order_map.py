from __future__ import annotations

import argparse
import json
import math
import os
import time as time_module
from dataclasses import asdict, dataclass
from datetime import datetime, time, timezone
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
from spx_spark.notifier.sinks import send_bark_message, send_openclaw_message
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
    """Second-order Taylor projection, clamped to >= 0.05."""
    move = target - spot
    projected = mid + delta * move + 0.5 * gamma * move * move
    return max(0.05, projected)


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

    projected = project_option_price(mid, delta, gamma, spot, level)
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
        frontrun_projected = project_option_price(mid, delta, gamma, spot, frontrun_level)
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

    candidates: list[OrderCandidate] = []

    if front.put_wall is not None:
        target_strike = round_to_step(front.put_wall, strike_step_int)
        candidate = _build_candidate(
            play="put_wall_bounce_call",
            level=front.put_wall,
            level_label=f"put wall {_dash(front.put_wall)}",
            target_strike=target_strike,
            right="C",
            spot=spot,
            expiry_quotes=expiry_quotes,
            strike_step=strike_step,
            pairs=pairs,
            warnings=local_warnings,
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
        )
        if candidate is not None:
            candidates.append(candidate)

    if front.call_wall is not None:
        target_strike = round_to_step(front.call_wall, strike_step_int)
        candidate = _build_candidate(
            play="call_wall_fade_put",
            level=front.call_wall,
            level_label=f"call wall {_dash(front.call_wall)}",
            target_strike=target_strike,
            right="P",
            spot=spot,
            expiry_quotes=expiry_quotes,
            strike_step=strike_step,
            pairs=pairs,
            warnings=local_warnings,
        )
        if candidate is not None:
            candidates.append(candidate)

    return candidates


def _index_value(state: LatestState, canonical_id: str) -> float | None:
    quote = state.best_quote(canonical_id)
    if quote is None or quote.quality in BAD_QUALITIES:
        return None
    return finite_float(quote.effective_price)


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
    return False


def build_order_payload_with_retry(
    storage_settings: StorageSettings,
    *,
    now: datetime,
    attempts: int = 3,
    delay_seconds: float = 6.0,
) -> dict[str, Any]:
    """Reload latest state a few times if the first snapshot looks thin."""
    payload: dict[str, Any] = {}
    for attempt in range(attempts):
        state = LatestStateStore(storage_settings).load()
        payload = build_order_payload(state, now=now)
        if not _payload_is_thin(payload):
            return payload
        if attempt < attempts - 1:
            time_module.sleep(delay_seconds)
    return payload


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
        "预估价按 delta/gamma 外推,未计时间衰减;保守价≈预估×0.85。仅供参考,不是订单指令。"
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
            "本条推送是『挂单地图』，要回答读者的一个问题：现在挂什么单、挂什么价。",
            "输出中文，最多 18 行。第一行必须以『挂单参考:』开头，并复述模板第一行（日期与时间）。",
            "接着 1-2 行地形结论：今天是 pin 还是 transition，哪类 play 优先级最高，为什么。",
            "然后逐条 play（最多 3 条，每条 2-3 行）：",
            "- 给墙位价与先手挡价（数字取自模板），并说取舍：墙位价更便宜但常在墙前几点反转吃不到，先手挡成交率更高；",
            "- 给赔率判断：把触达概率、到位预估价、现价放在一起，说这笔单在赌一次多大概率的什么事件、期权价从现价到预估价的变化幅度是否配得上这个概率；",
            "- order_style=stop_trigger 的必须提醒：预估价高于现价，提前挂被动限价会立即按市价成交，应等破位确认后用条件单。",
            "最后 2-3 行 if/then 剧本：开盘前参考价/ES 走到哪些具体位置时，哪张挂单的赔率变差该撤或改价，哪个剧本作废。",
            "previous_push 是上一条推送的正文；若关键位相对上一条有实质变化，须在地形结论处明确说『剧本有变』并指出哪张单要改；无实质变化则不必提。",
            "数据 degraded 时如实说明，不给方向判断。",
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


# --- event-driven refresh: re-push when key levels move materially ---

REFRESH_WINDOW_END = time(23, 30)
REFRESH_COOLDOWN_SECONDS_DEFAULT = 3600.0
MATERIAL_LEVEL_MOVE_POINTS = 5.0
MATERIAL_EM_REL_CHANGE = 0.20

# --- pre-open status report: fixed 30-minute cadence (Beijing 14:15 -> US open) ---

STATUS_WINDOW_START = time(14, 15)
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
    """Event-driven refresh only after US open; pre-open is covered by the 30m status report."""
    local = now_utc.astimezone(SHANGHAI_TZ)
    if local.weekday() >= 5:
        return False
    if now_utc.astimezone(NY_TZ).time() < US_OPEN_ET:
        return False
    return local.time() < REFRESH_WINDOW_END


def within_status_window(now_utc: datetime) -> bool:
    """Beijing 14:15 until US market open (9:30 ET)."""
    local = now_utc.astimezone(SHANGHAI_TZ)
    if local.weekday() >= 5:
        return False
    if local.time() < STATUS_WINDOW_START:
        return False
    return now_utc.astimezone(NY_TZ).time() < US_OPEN_ET


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
    return payload.get("last_sent_date") == trading_date


def mark_sent(
    state_path: str,
    trading_date: str,
    *,
    fingerprint: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> None:
    path = Path(state_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any] = {"last_sent_date": trading_date}
    if fingerprint is not None:
        payload["fingerprint"] = fingerprint
    if now is not None:
        payload["last_sent_at"] = now.timestamp()
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
            "本条推送是『市场状态』。读者已按之前的挂单地图挂好单，每 30 分钟想知道：剧本变了没有、挂的单要不要动、走进开盘的这段时间该注意什么。",
            "输出中文，10-14 行。第一行必须以『市场状态:』开头，保留模板第一行的时间与距开盘信息，并紧跟结论：『剧本维持』或『剧本有变: 一句话说变化』。",
            "判断『变没变』的基准是 previous_push 字段里的上一条推送正文，以及模板里的『较上次推送』一行。",
            "第 2-4 行位置读数：参考价此刻在 flip zone/zero gamma/两侧墙位构成的地形里的具体位置(距各关键位多少点)，以及这个位置对开盘意味着什么(偏 pin 还是易加速)。",
            "第 5-6 行赔率读数：三张挂单此刻的触达概率各是多少、和上一条相比谁在改善谁在恶化(引用百分比变化)，现在哪张单性价比最高。",
            "1 行 vol 面：VIX1D/VIX 水平说明隔夜与今日 vol 定价贵还是便宜，SKEW 有无异常。",
            "然后 2-3 行 if/then，必须覆盖上下两个方向：开盘前参考价若上行到哪个位置、下行到哪个位置，分别哪张挂单赔率变差该撤/改价、哪个剧本被激活。",
            "最后 1 行：到下一条推送(30 分钟后)之间最值得盯的一个量。",
            "剧本维持时照样给完整读数，不要因为『没变』就缩成两三行；但不要硬编不存在的变化，没变就说数字平稳。",
            "数据 degraded 时如实说明，不给方向判断。",
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
    fingerprint = payload_fingerprint(payload)
    changes = material_changes(previous.get("fingerprint"), fingerprint)
    template = render_status_template(payload, changes, now)

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

    if weixin_result.ok or bark_ok:
        mark_sent(state_path, trading_date, fingerprint=fingerprint, now=now)
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
    if previous.get("last_sent_date") != trading_date:
        print(json.dumps({"skipped": True, "reason": "no_baseline_push_today"}))
        return 0
    last_sent_at = finite_float(previous.get("last_sent_at"))
    cooldown = float(
        os.getenv("SPX_ORDER_MAP_REFRESH_COOLDOWN_SECONDS", "")
        or REFRESH_COOLDOWN_SECONDS_DEFAULT
    )
    if not args.force and last_sent_at is not None and now.timestamp() - last_sent_at < cooldown:
        print(json.dumps({"skipped": True, "reason": "refresh_cooldown"}))
        return 0

    payload = build_order_payload_with_retry(StorageSettings.from_env(), now=now)
    fingerprint = payload_fingerprint(payload)
    changes = material_changes(previous.get("fingerprint"), fingerprint)
    if not changes and not args.force:
        print(json.dumps({"skipped": True, "reason": "no_material_change"}))
        return 0

    header = f"【挂单地图·更新】原因: {'; '.join(changes) if changes else 'forced'}"
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
        mark_sent(state_path, trading_date, fingerprint=fingerprint, now=now)
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
        mark_sent(state_path, trading_date, fingerprint=payload_fingerprint(payload), now=now)
        record_push("order_map", result["text"], at=now.isoformat())
    print(json.dumps(result, ensure_ascii=False))
    if not result["weixin_ok"] and not result["bark_ok"]:
        return 1
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
