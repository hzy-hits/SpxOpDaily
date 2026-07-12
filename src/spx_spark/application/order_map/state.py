"""Order-map persistence, send windows, and session-phase helpers."""

from __future__ import annotations

import json
import os
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any

from spx_spark.application.order_map.models import (
    BJ_WINDOW_END,
    BJ_WINDOW_START,
    SHANGHAI_TZ,
)
from spx_spark.analytics.options.pricing import finite_float
from spx_spark.config import NY_TZ, StorageSettings
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR
from spx_spark.application.order_map.render import _candidate_by_play, _dash


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
# 07:30 -> next-day 01:30 every 15 minutes -- density is set by the systemd
# timer, this window only bounds it) ---

STATUS_WINDOW_START = time(7, 30)
STATUS_WINDOW_END_EARLY = time(1, 30)  # inclusive last fire


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
    """Beijing 07:30 through next-day 01:30: the partner's full working day.

    The timer fires every 15 minutes; this gate only bounds the day.
    The after-midnight leg belongs to the previous day's session, so it runs
    on Tue-Sat local mornings (Sat 00:xx = Friday's US session). Last fire at
    01:30 is inclusive.
    """
    local = now_utc.astimezone(SHANGHAI_TZ)
    if not exchange_session_relevant(now_utc):
        return False
    if local.time() >= STATUS_WINDOW_START:
        return local.weekday() < 5
    if local.time() <= STATUS_WINDOW_END_EARLY:
        return local.weekday() in (1, 2, 3, 4, 5)
    return False


def exchange_session_relevant(now_utc: datetime) -> bool:
    local = now_utc.astimezone(SHANGHAI_TZ)
    associated_date = local.date()
    if local.time() < STATUS_WINDOW_START:
        associated_date -= timedelta(days=1)
    return DEFAULT_MARKET_CALENDAR.is_trading_day(associated_date)


def minutes_to_open(now_utc: datetime) -> int | None:
    ny = now_utc.astimezone(NY_TZ)
    current_session = DEFAULT_MARKET_CALENDAR.session(ny.date())
    if current_session is not None:
        if current_session.open_at <= ny < current_session.close_at:
            return None
        if ny < current_session.open_at:
            open_dt = current_session.open_at
        else:
            next_day = DEFAULT_MARKET_CALENDAR.next_trading_day(ny.date())
            next_session = DEFAULT_MARKET_CALENDAR.session(next_day)
            assert next_session is not None
            open_dt = next_session.open_at
    else:
        next_day = DEFAULT_MARKET_CALENDAR.next_trading_day(ny.date())
        next_session = DEFAULT_MARKET_CALENDAR.session(next_day)
        assert next_session is not None
        open_dt = next_session.open_at
    if ny >= open_dt:
        return None
    return int((open_dt - ny).total_seconds() // 60)


# --- session phase: the partner's clock, not the exchange's -----------------
# The reader works Beijing 07:30 -> next-day 01:00 (ET ~19:30 -> ~13:00 in
# summer). He sleeps through the entire US afternoon and close, and for most
# of his waking day the market IS live (Globex futures + SPX GTH options).
# Every push must speak to the phase of HIS day instead of defaulting to
# "wait for the open".

USER_DAY_START_BJ = time(7, 30)
USER_DAY_END_BJ = time(1, 0)  # bedtime: positions after this are unattended

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

    current_session = DEFAULT_MARKET_CALENDAR.session(ny.date())
    if current_session is None and time(9, 30) <= ny.time() < time(16, 0):
        name, name_cn, traits = (
            "market_closed",
            "休市",
            "美股现金市场休市; 仅保留研究上下文",
        )
    if current_session is not None and ny < current_session.close_at:
        open_dt = current_session.open_at
    else:
        next_day = DEFAULT_MARKET_CALENDAR.next_trading_day(ny.date())
        next_session = DEFAULT_MARKET_CALENDAR.session(next_day)
        assert next_session is not None
        open_dt = next_session.open_at
        if (
            current_session is not None
            and current_session.early_close
            and ny >= current_session.close_at
            and ny.time() < time(18, 0)
        ):
            name, name_cn, traits = (
                "post_close",
                "盘后过渡",
                "现金已收、期货重定价; 复盘与次日准备",
            )
    minutes_to_us_open = int((open_dt - ny).total_seconds() // 60) if ny < open_dt else None
    minutes_since_us_open = (
        int((ny - current_session.open_at).total_seconds() // 60)
        if current_session is not None and current_session.open_at <= ny < current_session.close_at
        else None
    )
    minutes_to_us_close = (
        int((current_session.close_at - ny).total_seconds() // 60)
        if current_session is not None and ny < current_session.close_at
        else None
    )

    # Bedtime countdown: next Beijing 01:00. No countdown while asleep
    # (Beijing 01:00-07:30).
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
    if local.weekday() >= 5 or not exchange_session_relevant(now_utc):
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
