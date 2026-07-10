from __future__ import annotations

import argparse
import json
import os
import time as time_module
from dataclasses import replace
from datetime import datetime, time, timezone
from pathlib import Path
from typing import Any

from spx_spark.config import NY_TZ, NotificationSettings, StorageSettings
from spx_spark.human_focus import build_human_focus_context
from spx_spark.iv_surface import IvSurfaceSettings, load_latest_snapshot
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
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
from spx_spark.options_map import build_options_map
from spx_spark.storage import LatestState, LatestStateStore

ET_WINDOW_START = time(8, 30)
ET_WINDOW_END = time(9, 30)


def load_current_iv_surface(
    settings: IvSurfaceSettings | None = None,
    *,
    now: datetime | None = None,
):
    settings = settings or IvSurfaceSettings.from_env()
    try:
        surface = load_latest_snapshot(settings.latest_surface_path)
    except (OSError, ValueError, json.JSONDecodeError, KeyError):
        return None
    if surface is None:
        return None
    current = now or datetime.now(tz=timezone.utc)
    age_seconds = (current - surface.as_of).total_seconds()
    max_age_seconds = float(os.getenv("ALERT_MAX_IV_SURFACE_AGE_SECONDS", "420"))
    active_expiry = DEFAULT_MARKET_CALENDAR.research_expiry(current).strftime("%Y%m%d")
    if age_seconds < -5.0 or age_seconds > max_age_seconds:
        return None
    if surface.front_expiry != active_expiry:
        return None
    return surface


def overnight_gap(state: LatestState) -> dict[str, Any]:
    es_quote = state.best_quote("future:ES")
    spx_quote = state.best_quote("index:SPX")
    es_last = es_quote.effective_price if es_quote else None
    es_prev_close = es_quote.close if es_quote else None
    spx_prev_close = spx_quote.close if spx_quote else None
    gap_points = None
    gap_pct = None
    if es_last is not None and es_prev_close is not None:
        gap_points = es_last - es_prev_close
        if es_last > 0 and es_prev_close > 0:
            gap_pct = gap_points / es_prev_close
    return {
        "es_last": es_last,
        "es_prev_close": es_prev_close,
        "spx_prev_close": spx_prev_close,
        "gap_points": gap_points,
        "gap_pct": gap_pct,
    }


def build_morning_payload(state: LatestState, *, now: datetime | None = None) -> dict[str, Any]:
    evaluation_time = now or state.as_of
    evaluation_state = replace(state, as_of=evaluation_time)
    options_map = build_options_map(evaluation_state)
    iv_surface = load_current_iv_surface(now=evaluation_time)
    focus = build_human_focus_context(
        evaluation_state,
        options_map=options_map,
        iv_surface=iv_surface,
        iv_surface_history_1h=None,
        window={"name": "premarket_map", "priority": "info"},
    )
    return {
        "kind": "morning_map",
        "as_of": state.as_of.isoformat(),
        "trading_date": DEFAULT_MARKET_CALENDAR.research_expiry(evaluation_time).isoformat(),
        "overnight": overnight_gap(state),
        "human_focus_context": focus,
    }


def _morning_payload_is_thin(payload: dict[str, Any]) -> bool:
    """True when the snapshot caught a slow-poll/rotation gap (no walls at all)."""
    focus = payload.get("human_focus_context")
    if not isinstance(focus, dict):
        return True
    spxw = focus.get("spxw_options") if isinstance(focus.get("spxw_options"), dict) else {}
    expiries = spxw.get("expiries") if isinstance(spxw.get("expiries"), list) else []
    front = expiries[0] if expiries and isinstance(expiries[0], dict) else {}
    return front.get("put_wall") is None and front.get("call_wall") is None


def build_morning_payload_with_retry(
    storage_settings: StorageSettings,
    *,
    now: datetime | None = None,
    attempts: int = 6,
    delay_seconds: float = 10.0,
) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for attempt in range(attempts):
        state = LatestStateStore(storage_settings).load(now=now)
        payload = build_morning_payload(state, now=now)
        if not _morning_payload_is_thin(payload):
            return payload
        if attempt < attempts - 1:
            time_module.sleep(delay_seconds)
    return payload


def _dash(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.1f}".removesuffix(".0")
    return str(value)


def _fmt_gap_points(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:+.0f}"


def _fmt_gap_pct(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:+.2%}"


def _fmt_prob(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.0%}"


def _fmt_oi(value: float | None) -> str:
    if value is None:
        return ""
    return f"(OI {value:.0f})"


def _greeks_reference_line(reference: object) -> str | None:
    if not isinstance(reference, dict) or reference.get("status") not in {"ok", "degraded"}:
        return None
    aggregate = reference.get("aggregate")
    coverage = reference.get("coverage")
    if not isinstance(aggregate, dict) or not isinstance(coverage, dict):
        return None

    def metric(name: str) -> str:
        value = aggregate.get(name)
        return f"{float(value):.2e}" if isinstance(value, int | float) else "-"

    return (
        "0DTE Greeks(只读/仓位符号未知, OI×100): "
        f"Gamma {metric('gross_gamma_abs')}, "
        f"Charm5m {metric('gross_charm_5m_abs')}, "
        f"Vanna1vol {metric('gross_vanna_1vol_abs')}; "
        f"覆盖 {coverage.get('usable_contract_count')}/"
        f"{coverage.get('exact_expiry_contract_count')} [{reference.get('status')}]"
    )


def _confluence_label(value: bool | None) -> str:
    if value is True:
        return "共振"
    if value is False:
        return "不共振"
    return "-"


def _strike_oi(
    top_strikes: list[dict[str, Any]] | None, strike: float | None, kind: str
) -> float | None:
    if strike is None or not top_strikes:
        return None
    key = "call_oi" if kind == "call" else "put_oi"
    for row in top_strikes:
        if isinstance(row, dict) and row.get("strike") == strike:
            oi = row.get(key)
            return float(oi) if oi is not None else None
    return None


def render_template(payload: dict[str, Any]) -> str:
    as_of_raw = payload.get("as_of")
    payload_trading_date = payload.get("trading_date")
    trading_date = (
        payload_trading_date
        if isinstance(payload_trading_date, str) and payload_trading_date
        else "-"
    )
    if trading_date == "-" and isinstance(as_of_raw, str) and as_of_raw:
        try:
            as_of = datetime.fromisoformat(as_of_raw.replace("Z", "+00:00"))
            if as_of.tzinfo is None:
                as_of = as_of.replace(tzinfo=timezone.utc)
            trading_date = as_of.astimezone(NY_TZ).date().isoformat()
        except ValueError:
            pass

    overnight = payload.get("overnight") if isinstance(payload.get("overnight"), dict) else {}
    es_last = overnight.get("es_last")
    gap_points = overnight.get("gap_points")
    gap_pct = overnight.get("gap_pct")
    spx_prev_close = overnight.get("spx_prev_close")

    focus = (
        payload.get("human_focus_context")
        if isinstance(payload.get("human_focus_context"), dict)
        else {}
    )
    spxw = focus.get("spxw_options") if isinstance(focus.get("spxw_options"), dict) else {}
    expiries = spxw.get("expiries") if isinstance(spxw.get("expiries"), list) else []
    front = expiries[0] if expiries and isinstance(expiries[0], dict) else {}

    call_wall = front.get("call_wall")
    put_wall = front.get("put_wall")
    gamma_profile = (
        front.get("gamma_profile") if isinstance(front.get("gamma_profile"), dict) else {}
    )
    zero_gamma = gamma_profile.get("zero_gamma")
    flip_zone = gamma_profile.get("flip_zone")
    top_strikes = (
        gamma_profile.get("top_strikes")
        if isinstance(gamma_profile.get("top_strikes"), list)
        else []
    )

    flip_lo = "-"
    flip_hi = "-"
    if isinstance(flip_zone, list) and len(flip_zone) >= 2:
        flip_lo = _dash(flip_zone[0])
        flip_hi = _dash(flip_zone[1])

    call_oi_suffix = _fmt_oi(_strike_oi(top_strikes, call_wall, "call"))
    put_oi_suffix = _fmt_oi(_strike_oi(top_strikes, put_wall, "put"))

    level_probs = (
        front.get("level_probabilities")
        if isinstance(front.get("level_probabilities"), list)
        else []
    )
    prob_parts: list[str] = []
    seen_levels: set[str] = set()
    for item in level_probs:
        if not isinstance(item, dict):
            continue
        level = item.get("level")
        level_key = _dash(level)
        if level_key in seen_levels:
            continue
        seen_levels.add(level_key)
        prob_touch = item.get("prob_touch")
        prob_close = item.get("prob_close_beyond")
        prob_parts.append(f"触及 {level_key}≈{_fmt_prob(prob_touch)}/收破≈{_fmt_prob(prob_close)}")
    prob_line = "; ".join(prob_parts) if prob_parts else "-"

    wall_confluence = (
        spxw.get("wall_confluence") if isinstance(spxw.get("wall_confluence"), dict) else None
    )
    if wall_confluence:
        spy_put = wall_confluence.get("spy_put_wall_spx")
        spy_call = wall_confluence.get("spy_call_wall_spx")
        spy_line = (
            f"put 墙折算 {_dash(spy_put)}({_confluence_label(wall_confluence.get('put_wall_confluent'))}), "
            f"call 墙折算 {_dash(spy_call)}({_confluence_label(wall_confluence.get('call_wall_confluent'))})"
        )
    else:
        spy_line = "无 SPY 数据"

    micopedia = focus.get("micopedia") if isinstance(focus.get("micopedia"), dict) else {}
    regime = _dash(micopedia.get("regime"))
    vix_ratio = micopedia.get("vix_ratio")
    vix_ratio_text = f"{vix_ratio:.2f}" if isinstance(vix_ratio, int | float) else "-"
    dip_context = _dash(micopedia.get("dip_context"))

    event_tags = micopedia.get("event_tags")
    if isinstance(event_tags, list) and event_tags:
        events = ", ".join(str(tag) for tag in event_tags)
    else:
        events = "无"

    watchlist = micopedia.get("trigger_watchlist")
    if isinstance(watchlist, list) and watchlist:
        watch_text = "; ".join(str(item) for item in watchlist[:3])
    else:
        watch_text = "-"

    greeks_line = _greeks_reference_line(spxw.get("greeks_reference_0dte"))

    lines = [
        f"【盘前地图 {trading_date}】",
        (
            f"隔夜: ES {_dash(es_last)}({_fmt_gap_points(gap_points)} 点/{_fmt_gap_pct(gap_pct)} vs 昨结), "
            f"SPX 昨收 {_dash(spx_prev_close)}"
        ),
        (
            f"gamma 地形: call wall {_dash(call_wall)}{call_oi_suffix}, "
            f"put wall {_dash(put_wall)}{put_oi_suffix}, "
            f"zero gamma {_dash(zero_gamma)}, flip zone {flip_lo}-{flip_hi}"
        ),
        f"概率锥: {prob_line}",
        *([greeks_line] if greeks_line else []),
        f"SPY 对照: {spy_line}",
        f"regime: {regime}, VIX1D/VIX={vix_ratio_text}, dip_context={dip_context}",
        f"事件: {events}",
        f"开盘前 2 小时关注: {watch_text}",
    ]
    return "\n".join(lines)


def build_map_prompt(
    payload: dict[str, Any],
    template: str,
    previous_push: dict[str, Any] | None = None,
) -> str:
    return "\n".join(
        (
            "这条是『盘前地图』，开盘铃前最后一份便签。搭档挂好的单马上要接受开盘检验，他要的是：开盘头一小时的剧本，"
            "以及第一根急拉/急跌出现时他该做什么、不该做什么。",
            "动笔前先想清楚(不写出来)：隔夜 gap 是谁推的、开盘后大概率被回补还是被延续？做市商今天开在正 gamma 还是负 gamma，"
            "开盘的波动会被吸收还是被放大？昨天的墙隔夜有没有被 OI 变化掏空？",
            "",
            "输出中文，最多 14 行，第一行必须是模板的第一行。",
            "开头定调：相对 previous_push(下午以来最近一条)剧本有变还是延续——墙位/flip 移位、gap 改变优先 play 才算有变，"
            "有变就点名哪张单要改。",
            "隔夜 gap 给结论不给流水账：方向、幅度相对预期波幅算大还是小、对挂单意味着什么。",
            "地形一句话：pin/transition/negative，墙位与 flip zone 在哪，开盘价落在地形的哪个位置。",
            "开盘剧本写成双向 if/then(3-4 行)：开盘后 30-60 分钟，站上/跌破哪些具体点位(引用触及/收破概率)分别激活什么剧本、"
            "盯哪张单；急跌时结合 dip_context 说清是回调买点还是加速风险——这是搭档最容易在开盘慌手的地方，话要说死：到什么位置之前不动作。",
            "1 行 vol：VIX1D/VIX 比值与 SKEW，今天 vol 卖得贵还是便宜、有无事件定价。",
            "human_focus_context.spxw_options.greeks_reference_0dte 只覆盖严格 SPXW 当日到期，是价格/时间/IV 情景参考；"
            "position_sign/direction=unknown 时负 gamma 不等于下跌，不得改变原候选方向、排序或限价。",
            "1 行 SPY 墙位对照：共振增强可信度，不共振就明说墙位参考价值打折。",
            "previous_push:" + previous_push_json(previous_push),
            "JSON:" + json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            "模板:" + template,
        )
    )


def send_morning_map(
    payload: dict[str, Any],
    settings: NotificationSettings,
    *,
    runner: CommandRunner = default_runner,
    now: datetime | None = None,
    previous_push: dict[str, Any] | None = None,
) -> dict[str, Any]:
    now = now or datetime.now(tz=timezone.utc)
    template = render_template(payload)
    text, writer = generate_push_text(
        template,
        build_map_prompt(payload, template, previous_push),
        settings,
        runner=runner,
    )

    delivery_sinks = deliver_trade_push(
        settings,
        title="盘前地图",
        text=text,
        kind="morning_map",
        lane="trade",
        friend=True,
        runner=runner,
    )
    delivered_ok = any_delivery_ok(delivery_sinks)
    if not im_delivery_ok(delivery_sinks):
        append_missed(settings.missed_queue_path, text, kind="morning_map", at=now)

    return {
        "text": text,
        "writer": writer,
        "used_agent": writer != "template",
        "im_ok": any(s.sink == "feishu" and s.ok for s in delivery_sinks),
        "bark_ok": any(s.sink == "bark" and s.ok for s in delivery_sinks),
        "feishu_ok": any(s.sink == "feishu" and s.ok for s in delivery_sinks),
        "delivered_ok": delivered_ok,
    }


def default_state_path(settings: StorageSettings) -> str:
    return os.getenv("SPX_MORNING_MAP_STATE_PATH") or str(
        Path(settings.data_root) / "latest" / "morning_map_state.json"
    )


def within_send_window(now_utc: datetime) -> bool:
    local = now_utc.astimezone(NY_TZ)
    if not DEFAULT_MARKET_CALENDAR.is_trading_day(local.date()):
        return False
    current = local.time()
    return ET_WINDOW_START <= current < ET_WINDOW_END


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


def mark_sent(state_path: str, trading_date: str) -> None:
    path = Path(state_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"last_sent_date": trading_date}, ensure_ascii=False),
        encoding="utf-8",
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send SPX Spark pre-market map push.")
    parser.add_argument("--dry-run", action="store_true", help="Print template/agent text only.")
    parser.add_argument(
        "--force", action="store_true", help="Skip time window and idempotency gate."
    )
    return parser.parse_args(argv)


def run(argv: list[str] | None = None, *, now: datetime | None = None) -> int:
    args = parse_args(argv)
    now = now or datetime.now(tz=timezone.utc)
    storage_settings = StorageSettings.from_env()
    state_path = default_state_path(storage_settings)
    trading_date = DEFAULT_MARKET_CALENDAR.research_expiry(now).isoformat()

    if not args.force and not args.dry_run:
        if not within_send_window(now):
            print(json.dumps({"skipped": True, "reason": "outside_send_window"}))
            return 0
        if already_sent(state_path, trading_date):
            print(json.dumps({"skipped": True, "reason": "already_sent"}))
            return 0

    payload = build_morning_payload_with_retry(storage_settings, now=now)
    if _morning_payload_is_thin(payload) and not args.force and not args.dry_run:
        print(json.dumps({"skipped": True, "reason": "thin_snapshot_sampling_gap"}))
        return 0
    template = render_template(payload)

    if args.dry_run:
        print(template)
        settings = NotificationSettings.from_env()
        text, writer = generate_push_text(template, build_map_prompt(payload, template), settings)
        if writer != "template":
            print(f"\n--- {writer} ---\n")
            print(text)
        print(json.dumps({"dry_run": True}))
        return 0

    settings = NotificationSettings.from_env()
    result = send_morning_map(payload, settings, now=now, previous_push=load_previous_push())
    if (
        result.get("delivered_ok")
        or result["im_ok"]
        or result["bark_ok"]
        or result.get("feishu_ok")
    ):
        mark_sent(state_path, trading_date)
        record_push("morning_map", result["text"], at=now.isoformat())
    print(json.dumps(result, ensure_ascii=False))
    if not (
        result.get("delivered_ok")
        or result["im_ok"]
        or result["bark_ok"]
        or result.get("feishu_ok")
    ):
        return 1
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
