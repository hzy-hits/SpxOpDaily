from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, time, timezone
from pathlib import Path
from typing import Any

from spx_spark.config import NY_TZ, NotificationSettings, StorageSettings
from spx_spark.human_focus import build_human_focus_context
from spx_spark.iv_surface import IvSurfaceSettings, load_latest_snapshot
from spx_spark.notifier.llm_writer import generate_push_text
from spx_spark.notifier.missed_queue import append_missed
from spx_spark.notifier.model import CommandRunner, default_runner
from spx_spark.notifier.sinks import send_bark_message, send_openclaw_message
from spx_spark.options_map import build_options_map
from spx_spark.storage import LatestState, LatestStateStore

ET_WINDOW_START = time(8, 30)
ET_WINDOW_END = time(9, 30)


def load_current_iv_surface(settings: IvSurfaceSettings | None = None):
    settings = settings or IvSurfaceSettings.from_env()
    try:
        return load_latest_snapshot(settings.latest_surface_path)
    except (OSError, ValueError, json.JSONDecodeError, KeyError):
        return None


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
    del now
    options_map = build_options_map(state)
    iv_surface = load_current_iv_surface()
    focus = build_human_focus_context(
        state,
        options_map=options_map,
        iv_surface=iv_surface,
        iv_surface_history_1h=None,
        window={"name": "premarket_map", "priority": "info"},
    )
    return {
        "kind": "morning_map",
        "as_of": state.as_of.isoformat(),
        "overnight": overnight_gap(state),
        "human_focus_context": focus,
    }


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


def _confluence_label(value: bool | None) -> str:
    if value is True:
        return "共振"
    if value is False:
        return "不共振"
    return "-"


def _strike_oi(top_strikes: list[dict[str, Any]] | None, strike: float | None, kind: str) -> float | None:
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
    trading_date = "-"
    if isinstance(as_of_raw, str) and as_of_raw:
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

    focus = payload.get("human_focus_context") if isinstance(payload.get("human_focus_context"), dict) else {}
    spxw = focus.get("spxw_options") if isinstance(focus.get("spxw_options"), dict) else {}
    expiries = spxw.get("expiries") if isinstance(spxw.get("expiries"), list) else []
    front = expiries[0] if expiries and isinstance(expiries[0], dict) else {}

    call_wall = front.get("call_wall")
    put_wall = front.get("put_wall")
    gamma_profile = front.get("gamma_profile") if isinstance(front.get("gamma_profile"), dict) else {}
    zero_gamma = gamma_profile.get("zero_gamma")
    flip_zone = gamma_profile.get("flip_zone")
    top_strikes = gamma_profile.get("top_strikes") if isinstance(gamma_profile.get("top_strikes"), list) else []

    flip_lo = "-"
    flip_hi = "-"
    if isinstance(flip_zone, list) and len(flip_zone) >= 2:
        flip_lo = _dash(flip_zone[0])
        flip_hi = _dash(flip_zone[1])

    call_oi_suffix = _fmt_oi(_strike_oi(top_strikes, call_wall, "call"))
    put_oi_suffix = _fmt_oi(_strike_oi(top_strikes, put_wall, "put"))

    level_probs = front.get("level_probabilities") if isinstance(front.get("level_probabilities"), list) else []
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
        prob_parts.append(
            f"触及 {level_key}≈{_fmt_prob(prob_touch)}/收破≈{_fmt_prob(prob_close)}"
        )
    prob_line = "; ".join(prob_parts) if prob_parts else "-"

    wall_confluence = spxw.get("wall_confluence") if isinstance(spxw.get("wall_confluence"), dict) else None
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
        f"SPY 对照: {spy_line}",
        f"regime: {regime}, VIX1D/VIX={vix_ratio_text}, dip_context={dip_context}",
        f"事件: {events}",
        f"开盘前 2 小时关注: {watch_text}",
    ]
    return "\n".join(lines)


def build_map_prompt(payload: dict[str, Any], template: str) -> str:
    return "\n".join(
        (
            "你是 SPX Spark 的盘前地图写手，为一个只交易 SPX/SPXW 0DTE/1DTE 期权(买 call/put 或垂直价差)的人写开盘前简报。",
            "只依据下面 JSON 与模板事实，不得编造数字、新闻或仓位；不给下单指令。",
            "输出中文，最多 14 行，第一行必须是模板的第一行。",
            "必须覆盖：隔夜 gap、gamma 地形(墙位+OI+zero gamma+flip zone)、各关键位触及/收破概率、SPY 墙位对照、regime 与 dip_context、事件标签。",
            "在数字之外，用 2-3 句交易员口吻解读：今天的地形偏 pin 还是易加速、急跌该当回调买点还是风险、开盘后两小时最该盯什么。",
            "数据 degraded 时如实说明，不给方向判断。",
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
) -> dict[str, Any]:
    now = now or datetime.now(tz=timezone.utc)
    template = render_template(payload)
    text, writer = generate_push_text(
        template,
        build_map_prompt(payload, template),
        settings,
        runner=runner,
    )

    weixin_result = send_openclaw_message(settings, text, runner=runner)
    if not weixin_result.ok:
        append_missed(settings.missed_queue_path, text, kind="morning_map", at=now)

    bark_ok = True
    if settings.bark_enabled:
        bark_result = send_bark_message(settings, "盘前地图", text)
        bark_ok = bark_result.ok

    return {
        "text": text,
        "writer": writer,
        "used_agent": writer != "template",
        "weixin_ok": weixin_result.ok,
        "bark_ok": bark_ok,
    }


def default_state_path(settings: StorageSettings) -> str:
    return os.getenv("SPX_MORNING_MAP_STATE_PATH") or str(
        Path(settings.data_root) / "latest" / "morning_map_state.json"
    )


def within_send_window(now_utc: datetime) -> bool:
    local = now_utc.astimezone(NY_TZ)
    if local.weekday() >= 5:
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
    parser.add_argument("--force", action="store_true", help="Skip time window and idempotency gate.")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None, *, now: datetime | None = None) -> int:
    args = parse_args(argv)
    now = now or datetime.now(tz=timezone.utc)
    storage_settings = StorageSettings.from_env()
    state_path = default_state_path(storage_settings)
    trading_date = now.astimezone(NY_TZ).date().isoformat()

    if not args.force and not args.dry_run:
        if not within_send_window(now):
            print(json.dumps({"skipped": True, "reason": "outside_send_window"}))
            return 0
        if already_sent(state_path, trading_date):
            print(json.dumps({"skipped": True, "reason": "already_sent"}))
            return 0

    state = LatestStateStore(storage_settings).load()
    payload = build_morning_payload(state, now=now)
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
    result = send_morning_map(payload, settings, now=now)
    mark_sent(state_path, trading_date)
    print(json.dumps(result, ensure_ascii=False))
    if not result["weixin_ok"] and not result["bark_ok"]:
        return 1
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
