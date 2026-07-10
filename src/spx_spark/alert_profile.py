from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass, replace
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo

from spx_spark.config import NY_TZ
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR


BJ_TZ = ZoneInfo("Asia/Shanghai")


@dataclass(frozen=True)
class AlertWindow:
    name: str
    start_et: time
    stop_et: time
    priority: str
    user_unattended: bool
    cadence_seconds: int
    summary_cadence_seconds: int
    spxw_sampling_mode: str
    primary_sources: tuple[str, ...]
    required_instruments: tuple[str, ...]
    optional_instruments: tuple[str, ...]
    focus: tuple[str, ...]

    def contains(self, now_et: time) -> bool:
        if self.start_et <= self.stop_et:
            return self.start_et <= now_et < self.stop_et
        return now_et >= self.start_et or now_et < self.stop_et

    def to_dict(self, *, now: datetime | None = None) -> dict[str, object]:
        payload = asdict(self)
        payload["start_et"] = self.start_et.strftime("%H:%M")
        payload["stop_et"] = self.stop_et.strftime("%H:%M")
        if now is not None:
            now_et = now.astimezone(NY_TZ)
            start_dt = now_et.replace(
                hour=self.start_et.hour,
                minute=self.start_et.minute,
                second=0,
                microsecond=0,
            )
            stop_dt = now_et.replace(
                hour=self.stop_et.hour,
                minute=self.stop_et.minute,
                second=0,
                microsecond=0,
            )
            if self.stop_et <= self.start_et:
                stop_dt += timedelta(days=1)
            payload["start_beijing"] = start_dt.astimezone(BJ_TZ).strftime("%H:%M")
            payload["stop_beijing"] = stop_dt.astimezone(BJ_TZ).strftime("%H:%M")
        return payload


WINDOWS: tuple[AlertWindow, ...] = (
    # ET 02:00-04:00 = Beijing 14:00-16:00: the reader's research window opens.
    # Off-exchange hours for SPX cash, but prime attention hours for the user,
    # so priority matches RTH ("high"), not a sleepy overnight tier.
    AlertWindow(
        name="overnight_liquidity_dip_watch",
        start_et=time(2, 0),
        stop_et=time(4, 0),
        priority="high",
        user_unattended=False,
        cadence_seconds=30,
        summary_cadence_seconds=900,
        spxw_sampling_mode="off",
        primary_sources=("hyperliquid", "polymarket", "ibkr_futures"),
        required_instruments=("crypto_perp:xyz:SP500",),
        optional_instruments=("future:ES", "future:MES"),
        focus=("thin-liquidity SPX proxy dip", "global futures lead"),
    ),
    AlertWindow(
        name="early_premarket_dip_watch",
        start_et=time(4, 0),
        stop_et=time(8, 30),
        priority="high",
        user_unattended=False,
        cadence_seconds=15,
        summary_cadence_seconds=600,
        spxw_sampling_mode="off",
        primary_sources=("ibkr_etf", "ibkr_futures", "hyperliquid", "polymarket"),
        required_instruments=("equity:SPY", "equity:QQQ", "equity:IWM", "equity:DIA"),
        optional_instruments=("future:ES", "future:MES", "crypto_perp:xyz:SP500"),
        focus=("premarket ETF dip", "ES/SPY divergence", "risk proxy confirmation"),
    ),
    AlertWindow(
        name="premarket_one_hour",
        start_et=time(8, 30),
        stop_et=time(9, 30),
        priority="critical",
        user_unattended=False,
        cadence_seconds=10,
        summary_cadence_seconds=300,
        spxw_sampling_mode="degraded",
        primary_sources=("ibkr_etf", "ibkr_futures", "hyperliquid", "polymarket"),
        required_instruments=("equity:SPY", "equity:QQQ", "equity:IWM", "equity:DIA"),
        optional_instruments=("index:SPX", "index:VIX", "future:ES", "future:MES"),
        focus=("opening gap risk", "0DTE setup precheck", "macro headline reaction"),
    ),
    AlertWindow(
        name="open_one_hour",
        start_et=time(9, 30),
        stop_et=time(10, 30),
        priority="critical",
        user_unattended=False,
        cadence_seconds=5,
        summary_cadence_seconds=180,
        spxw_sampling_mode="human_alert",
        primary_sources=("ibkr_indexes", "ibkr_spxw_options", "ibkr_etf", "hyperliquid"),
        required_instruments=("index:SPX", "index:VIX"),
        optional_instruments=("index:VVIX", "index:SKEW", "equity:SPY", "equity:QQQ", "equity:IWM", "equity:DIA"),
        focus=("opening drive or fade", "SPXW greek availability", "vol regime shift"),
    ),
    # ET 10:30-13:30 = Beijing 22:30-01:30: the reader is still awake and often
    # holding 0DTE positions opened at the US open. "normal" priority stamped
    # medium severity and got filtered by the notify gate; keep it "high".
    AlertWindow(
        name="rth_midday_watch",
        start_et=time(10, 30),
        stop_et=time(13, 30),
        priority="high",
        user_unattended=False,
        cadence_seconds=30,
        summary_cadence_seconds=900,
        spxw_sampling_mode="degraded",
        primary_sources=("ibkr_indexes", "ibkr_spxw_options", "ibkr_etf", "hyperliquid"),
        required_instruments=("index:SPX", "index:VIX"),
        optional_instruments=("index:VVIX", "index:SKEW", "equity:SPY", "future:ES"),
        focus=("range expansion", "vol compression", "headline break"),
    ),
    AlertWindow(
        name="unattended_afternoon_watch",
        start_et=time(13, 30),
        stop_et=time(15, 0),
        priority="high",
        user_unattended=True,
        cadence_seconds=15,
        summary_cadence_seconds=300,
        spxw_sampling_mode="human_alert",
        primary_sources=("ibkr_indexes", "ibkr_spxw_options", "ibkr_etf", "hyperliquid"),
        required_instruments=("index:SPX", "index:VIX", "equity:SPY", "equity:QQQ"),
        optional_instruments=("index:VVIX", "index:SKEW", "future:ES", "future:MES"),
        focus=("unattended RTH alert", "failed breakout", "dip into close setup"),
    ),
    AlertWindow(
        name="close_one_hour",
        start_et=time(15, 0),
        stop_et=time(16, 0),
        priority="critical",
        user_unattended=True,
        cadence_seconds=5,
        summary_cadence_seconds=180,
        spxw_sampling_mode="human_alert",
        primary_sources=("ibkr_indexes", "ibkr_spxw_options", "ibkr_etf", "hyperliquid"),
        required_instruments=("index:SPX", "index:VIX", "equity:SPY", "equity:QQQ"),
        optional_instruments=("index:VVIX", "index:SKEW", "future:ES", "future:MES"),
        focus=("closing imbalance risk", "0DTE decay acceleration", "late trend reversal"),
    ),
    AlertWindow(
        name="afterhours_one_hour",
        start_et=time(16, 0),
        stop_et=time(17, 0),
        priority="high",
        user_unattended=True,
        cadence_seconds=15,
        summary_cadence_seconds=300,
        spxw_sampling_mode="off",
        primary_sources=("ibkr_etf", "ibkr_futures", "hyperliquid", "polymarket"),
        required_instruments=("equity:SPY", "equity:QQQ"),
        optional_instruments=("future:ES", "future:MES", "crypto_perp:xyz:SP500"),
        focus=("post-close repricing", "earnings or headline shock", "futures follow-through"),
    ),
    AlertWindow(
        name="afterhours_second_hour",
        start_et=time(17, 0),
        stop_et=time(18, 0),
        # Same notify weight as RTH/high off-hours: overnight handoff still
        # needs parity with the regular session for price/vol pushes.
        priority="high",
        user_unattended=True,
        cadence_seconds=30,
        summary_cadence_seconds=900,
        spxw_sampling_mode="off",
        primary_sources=("ibkr_futures", "hyperliquid", "polymarket"),
        required_instruments=("crypto_perp:xyz:SP500",),
        optional_instruments=("future:ES", "future:MES", "equity:SPY", "equity:QQQ"),
        focus=("post-close drift", "global risk handoff"),
    ),
    # ET 20:30-02:00 = Beijing 08:30-14:00: the reader's working morning.
    # Globex + SPX GTH are live and he is at the desk building the day's
    # skeleton, so off-hours moves here deserve the same attention as RTH.
    AlertWindow(
        name="beijing_morning_globex_watch",
        start_et=time(20, 30),
        stop_et=time(2, 0),
        priority="high",
        user_unattended=False,
        cadence_seconds=30,
        summary_cadence_seconds=900,
        spxw_sampling_mode="off",
        primary_sources=("hyperliquid", "ibkr_futures", "polymarket"),
        required_instruments=("crypto_perp:xyz:SP500",),
        optional_instruments=("future:ES", "future:MES"),
        focus=("overnight Globex dip or squeeze", "Asia-session risk lead", "gap risk building"),
    ),
)

QUIET_WINDOW = AlertWindow(
    name="quiet_futures_context",
    start_et=time(18, 0),
    stop_et=time(20, 30),
    # ET 18:00-20:30 = Beijing ~06:00-08:30. Name stays "quiet" for the thin
    # Globex tape, but notify priority matches RTH so off-hours moves are not
    # filtered below the delivery floor.
    priority="high",
    user_unattended=True,
    cadence_seconds=30,
    summary_cadence_seconds=900,
    spxw_sampling_mode="off",
    primary_sources=("hyperliquid", "polymarket", "ibkr_futures"),
    required_instruments=("crypto_perp:xyz:SP500",),
    optional_instruments=("future:ES", "future:MES"),
    focus=("background regime watch",),
)

WEEKEND_WINDOW = AlertWindow(
    name="weekend_maintenance",
    start_et=time(0, 0),
    stop_et=time(23, 59),
    priority="off",
    user_unattended=True,
    cadence_seconds=300,
    summary_cadence_seconds=3600,
    spxw_sampling_mode="off",
    primary_sources=("hyperliquid", "polymarket"),
    required_instruments=(),
    optional_instruments=("crypto_perp:xyz:SP500",),
    focus=("maintenance", "disk cleanup", "provider health audit"),
)

SUNDAY_FUTURES_REOPEN = AlertWindow(
    name="sunday_futures_reopen",
    start_et=time(18, 0),
    stop_et=time(23, 59),
    priority="elevated",
    user_unattended=True,
    cadence_seconds=30,
    summary_cadence_seconds=900,
    spxw_sampling_mode="off",
    primary_sources=("ibkr_futures", "hyperliquid", "polymarket"),
    required_instruments=("crypto_perp:xyz:SP500",),
    optional_instruments=("future:ES", "future:MES"),
    focus=("futures reopen gap", "weekend headline repricing"),
)


def active_window(now: datetime | None = None) -> AlertWindow:
    now = now or datetime.now(tz=NY_TZ)
    now_et = now.astimezone(NY_TZ)
    weekday = now_et.weekday()
    current_time = now_et.time()

    if weekday == 5:
        return WEEKEND_WINDOW
    if weekday == 6 and current_time < time(18, 0):
        return WEEKEND_WINDOW
    if weekday == 6:
        return SUNDAY_FUTURES_REOPEN
    # Friday evening ET is Saturday morning Beijing: ES is closed and the
    # reader is off; keep it quiet instead of the Beijing-morning watch.
    if weekday == 4 and current_time >= time(18, 0):
        return QUIET_WINDOW
    session = DEFAULT_MARKET_CALENDAR.session(now_et.date())
    if session is None:
        return QUIET_WINDOW
    if session.early_close and session.close_at - timedelta(hours=1) <= now_et < session.close_at:
        close_window = next(window for window in WINDOWS if window.name == "close_one_hour")
        return replace(
            close_window,
            start_et=(session.close_at - timedelta(hours=1)).time(),
            stop_et=session.close_at.time(),
        )
    if session.early_close and session.close_at <= now_et < now_et.replace(
        hour=16,
        minute=0,
        second=0,
        microsecond=0,
    ):
        return QUIET_WINDOW

    for window in WINDOWS:
        if window.contains(current_time):
            return window
    return QUIET_WINDOW


def profile(now: datetime | None = None) -> dict[str, object]:
    now = now or datetime.now(tz=NY_TZ)
    window = active_window(now)
    now_et = now.astimezone(NY_TZ)
    now_bj = now.astimezone(BJ_TZ)
    return {
        "created_at": datetime.now(tz=NY_TZ).isoformat(),
        "now_et": now_et.isoformat(),
        "now_beijing": now_bj.isoformat(),
        "window": window.to_dict(now=now_et),
    }


def parse_at(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=BJ_TZ)
    return parsed


def print_profile(payload: dict[str, object]) -> None:
    window = payload["window"]
    assert isinstance(window, dict)
    print(f"Alert window: {window['name']}")
    print(f"ET: {payload['now_et']}")
    print(f"Beijing: {payload['now_beijing']}")
    print(f"Priority: {window['priority']} unattended={str(window['user_unattended']).lower()}")
    print(
        "Cadence: "
        f"alerts {window['cadence_seconds']}s, summary {window['summary_cadence_seconds']}s"
    )
    print(f"SPXW sampling: {window['spxw_sampling_mode']}")
    print(f"Sources: {', '.join(window['primary_sources'])}")
    if window["required_instruments"]:
        print(f"Required: {', '.join(window['required_instruments'])}")
    if window["optional_instruments"]:
        print(f"Optional: {', '.join(window['optional_instruments'])}")
    print(f"Focus: {', '.join(window['focus'])}")


def print_schedule(now: datetime | None = None) -> None:
    now = now or datetime.now(tz=NY_TZ)
    print("ET       | Beijing | priority  | window")
    print("---------+---------+-----------+----------------------------")
    for window in WINDOWS + (QUIET_WINDOW, SUNDAY_FUTURES_REOPEN, WEEKEND_WINDOW):
        item = window.to_dict(now=now)
        print(
            f"{item['start_et']}-{item['stop_et']} | "
            f"{item['start_beijing']}-{item['stop_beijing']} | "
            f"{window.priority.ljust(9)} | {window.name}"
        )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resolve the current SPX alert monitoring profile.")
    parser.add_argument("--at", help="ISO timestamp. Naive timestamps are treated as Asia/Shanghai.")
    parser.add_argument("--json", action="store_true", help="Print JSON.")
    parser.add_argument("--schedule", action="store_true", help="Print all monitoring windows.")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    now = parse_at(args.at) if args.at else datetime.now(tz=NY_TZ)
    if args.schedule:
        print_schedule(now)
        return 0

    payload = profile(now)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print_profile(payload)
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
