from __future__ import annotations

import argparse
import json
import math
import os
from dataclasses import asdict, dataclass
from datetime import datetime, time, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from spx_spark.config import NY_TZ, NotificationSettings, StorageSettings
from spx_spark.marketdata import OptionRight, Quote
from spx_spark.notifier.missed_queue import append_missed
from spx_spark.notifier.model import CommandRunner, default_runner
from spx_spark.notifier.sinks import run_openclaw_agent, send_bark_message, send_openclaw_message
from spx_spark.options_map import (
    BAD_QUALITIES,
    OptionsMap,
    build_options_map,
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


def chain_implied_spot(pairs: dict[float, dict[OptionRight, Quote]]) -> float | None:
    """SPX spot implied by put-call parity at the synthetic ATM strike.

    The stream's underlier reference can be ES/MES, which carries a large
    basis vs SPX (~100 pts on a far contract). Options are priced off the
    real SPX forward, so projections must use a chain-consistent spot:
    S ~= K + C(K) - P(K) at the strike where |C - P| is smallest (r~=0 for
    0DTE/1DTE).
    """
    best: tuple[float, float, float, float] | None = None
    for strike, sides in pairs.items():
        call_mid = option_mid(sides.get(OptionRight.CALL))
        put_mid = option_mid(sides.get(OptionRight.PUT))
        if call_mid is None or put_mid is None:
            continue
        diff = abs(call_mid - put_mid)
        if best is None or diff < best[0]:
            best = (diff, strike, call_mid, put_mid)
    if best is None:
        return None
    _, strike, call_mid, put_mid = best
    return strike + call_mid - put_mid


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
    )


HL_SP500_PROXY_ID = "crypto_perp:xyz:SP500"
# Chain-implied vs Hyperliquid perp divergence beyond this suggests wide or
# stale GTH option quotes; surface it instead of silently trusting either.
HL_DIVERGENCE_WARN_BPS = 15.0


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
) -> tuple[float | None, str]:
    """Return (spot, source_label) for projections.

    Priority: chain-implied put-call parity (the option market's own view)
    with the Hyperliquid SP500 perp as a cross-check, then the perp itself,
    then the stream underlier (ES carries basis vs SPX -> warned).
    """
    hl_price = hyperliquid_sp500_price(state)
    if options_map.expiries:
        front = options_map.expiries[0]
        pairs = pair_by_strike(_front_expiry_quotes(state, front.expiry))
        implied = chain_implied_spot(pairs)
        if implied is not None:
            if hl_price is not None and warnings is not None:
                divergence_bps = abs(implied / hl_price - 1.0) * 10_000.0
                if divergence_bps > HL_DIVERGENCE_WARN_BPS:
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

    spot, _spot_source = resolve_spx_spot(state, options_map, warnings=local_warnings)
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

    spot, spot_source = resolve_spx_spot(state, options_map)
    candidates = build_candidates(state, options_map, warnings)
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
        "warnings": list(dict.fromkeys(warnings)),
    }


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
        lines.append(
            "   挂单参考: 激进 "
            f"{_fmt_premium(candidate.get('limit_aggressive'))} / 保守 "
            f"{_fmt_premium(candidate.get('limit_conservative'))}"
        )

    lines.append(
        "注: 预估价按当前 delta/gamma 外推,未计时间衰减(0DTE 下午触发会更便宜);"
        "保守价≈预估×0.85。仅供挂单参考,不是订单指令。"
    )

    warnings = payload.get("warnings")
    if isinstance(warnings, list) and warnings:
        lines.append(f"数据警告: {'; '.join(str(item) for item in warnings)}")

    return "\n".join(lines)


def build_order_prompt(payload: dict[str, Any], template: str) -> str:
    return "\n".join(
        (
            "你是 SPX Spark 的挂单地图写手，为一个只交易 SPX/SPXW 0DTE/1DTE 期权(买 call/put 或垂直价差)的人写盘前挂单参考。",
            "只依据下面 JSON 与模板事实，不得编造数字、新闻或仓位；不给下单指令。",
            "输出中文，最多 14 行，第一行必须是『挂单参考:』开头，并复述模板第一行。",
            "必须覆盖：参考价与预期波幅、gamma 地形、三类 play 的触达概率与激进/保守限价；数字必须与模板一致。",
            "在数字之外，用 2-3 句交易员口吻解读：今天更适合反弹买 call、跌破买 put 还是冲墙 fade，以及保守限价为何留安全边际。",
            "数据 degraded 时如实说明，不给方向判断。",
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
) -> dict[str, Any]:
    now = now or datetime.now(tz=timezone.utc)
    template = render_template(payload)
    if extra_header:
        template = f"{extra_header}\n{template}"
    used_agent = False
    text = template

    if settings.openclaw_agent_enabled:
        sink, reply = run_openclaw_agent(settings, build_order_prompt(payload, template), runner=runner)
        if reply and sink.ok:
            text = reply
            used_agent = True

    weixin_result = send_openclaw_message(settings, text, runner=runner)
    if not weixin_result.ok:
        append_missed(settings.missed_queue_path, text, kind="order_map", at=now)

    bark_ok = True
    if settings.bark_enabled:
        bark_result = send_bark_message(settings, "挂单地图", text)
        bark_ok = bark_result.ok

    return {
        "text": text,
        "used_agent": used_agent,
        "weixin_ok": weixin_result.ok,
        "bark_ok": bark_ok,
    }


def default_state_path(settings: StorageSettings) -> str:
    return os.getenv("SPX_ORDER_MAP_STATE_PATH") or str(
        Path(settings.data_root) / "latest" / "order_map_state.json"
    )


# --- event-driven refresh: re-push when key levels move materially ---

REFRESH_WINDOW_START = time(13, 30)
REFRESH_WINDOW_END = time(23, 30)
REFRESH_COOLDOWN_SECONDS_DEFAULT = 3600.0
MATERIAL_LEVEL_MOVE_POINTS = 5.0
MATERIAL_EM_REL_CHANGE = 0.20


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
    local = now_utc.astimezone(SHANGHAI_TZ)
    if local.weekday() >= 5:
        return False
    current = local.time()
    return REFRESH_WINDOW_START <= current < REFRESH_WINDOW_END


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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send SPX Spark order map push.")
    parser.add_argument("--dry-run", action="store_true", help="Print template only.")
    parser.add_argument("--force", action="store_true", help="Skip time window and idempotency gate.")
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Re-push only when key levels moved materially since the last push.",
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

    state = LatestStateStore(StorageSettings.from_env()).load()
    payload = build_order_payload(state, now=now)
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
    result = send_order_map(payload, settings, now=now, extra_header=header)
    if result["weixin_ok"] or result["bark_ok"]:
        mark_sent(state_path, trading_date, fingerprint=fingerprint, now=now)
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

    if args.refresh:
        return run_refresh(args, now=now, state_path=state_path, trading_date=trading_date)

    if not args.force and not args.dry_run:
        if not within_send_window(now):
            print(json.dumps({"skipped": True, "reason": "outside_send_window"}))
            return 0
        if already_sent(state_path, trading_date):
            print(json.dumps({"skipped": True, "reason": "already_sent"}))
            return 0

    state = LatestStateStore(storage_settings).load()
    payload = build_order_payload(state, now=now)
    template = render_template(payload)

    if args.dry_run:
        print(template)
        print(json.dumps({"dry_run": True}))
        return 0

    settings = NotificationSettings.from_env()
    result = send_order_map(payload, settings, now=now)
    if result["weixin_ok"] or result["bark_ok"]:
        mark_sent(state_path, trading_date, fingerprint=payload_fingerprint(payload), now=now)
    print(json.dumps(result, ensure_ascii=False))
    if not result["weixin_ok"] and not result["bark_ok"]:
        return 1
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
