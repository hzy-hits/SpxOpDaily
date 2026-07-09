from __future__ import annotations

import argparse
import json
import os
import urllib.error
import urllib.request
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

from spx_spark.config import NY_TZ, NotificationSettings, StorageSettings, env_bool, load_dotenv
from spx_spark.notifier.llm_writer import DEFAULT_SYSTEM_PROMPT
from spx_spark.notifier.missed_queue import append_missed
from spx_spark.notifier.model import CommandRunner, default_runner
from spx_spark.notifier.sinks import (
    any_delivery_ok,
    deliver_trade_push,
    im_delivery_ok,
    run_openclaw_agent,
)
from spx_spark.iv_surface import (
    IvSurfaceSnapshot,
    IvSurfaceExpiry,
    raw_snapshot_paths_for_window,
    snapshot_from_dict,
)
from spx_spark.marketdata import InstrumentType, Quote, as_utc, quote_from_dict


SESSION_START = time(9, 30)
SESSION_END = time(16, 0)
REVIEW_READY = time(18, 0)


@dataclass(frozen=True)
class ReviewPaths:
    report_dir: Path
    markdown_path: Path
    json_path: Path
    latest_markdown_path: Path
    latest_json_path: Path
    hermes_markdown_path: Path | None
    hermes_latest_markdown_path: Path | None


@dataclass(frozen=True)
class ReviewLlmSettings:
    enabled: bool
    provider: str
    model: str
    url: str
    env_file: str
    timeout_seconds: float
    max_tokens: int

    @classmethod
    def from_env(cls) -> "ReviewLlmSettings":
        load_dotenv()
        return cls(
            enabled=env_bool("SPX_REVIEW_LLM_ENABLED", False),
            provider=os.getenv("SPX_REVIEW_LLM_PROVIDER", "deepseek").strip(),
            model=os.getenv("SPX_REVIEW_LLM_MODEL", "deepseek-v4-pro").strip(),
            url=os.getenv(
                "SPX_REVIEW_LLM_URL",
                "https://api.deepseek.com/v1/chat/completions",
            ).strip(),
            env_file=os.getenv("SPX_REVIEW_LLM_ENV_FILE", "/home/ubuntu/.hermes/.env").strip(),
            timeout_seconds=float(os.getenv("SPX_REVIEW_LLM_TIMEOUT_SECONDS", "120")),
            max_tokens=int(os.getenv("SPX_REVIEW_LLM_MAX_TOKENS", "2200")),
        )


def read_env_file_value(path: str, key: str) -> str:
    env_path = Path(path).expanduser()
    try:
        lines = env_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return ""
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        name, value = line.split("=", 1)
        if name.strip() == key:
            return value.strip().strip('"').strip("'")
    return ""


def deepseek_api_key(settings: ReviewLlmSettings) -> str:
    return os.getenv("DEEPSEEK_API_KEY", "").strip() or read_env_file_value(
        settings.env_file,
        "DEEPSEEK_API_KEY",
    )


def observed_fixed_holiday(year: int, month: int, day: int) -> date:
    holiday = date(year, month, day)
    if holiday.weekday() == 5:
        return holiday - timedelta(days=1)
    if holiday.weekday() == 6:
        return holiday + timedelta(days=1)
    return holiday


def nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    current = date(year, month, 1)
    offset = (weekday - current.weekday()) % 7
    return current + timedelta(days=offset + 7 * (n - 1))


def last_weekday(year: int, month: int, weekday: int) -> date:
    current = date(year, month + 1, 1) - timedelta(days=1) if month < 12 else date(year, 12, 31)
    offset = (current.weekday() - weekday) % 7
    return current - timedelta(days=offset)


def easter_date(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    length = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * length) // 451
    month = (h + length - 7 * m + 114) // 31
    day = ((h + length - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def us_market_holidays(year: int) -> set[date]:
    return {
        observed_fixed_holiday(year, 1, 1),
        nth_weekday(year, 1, 0, 3),
        nth_weekday(year, 2, 0, 3),
        easter_date(year) - timedelta(days=2),
        last_weekday(year, 5, 0),
        observed_fixed_holiday(year, 6, 19),
        observed_fixed_holiday(year, 7, 4),
        nth_weekday(year, 9, 0, 1),
        nth_weekday(year, 11, 3, 4),
        observed_fixed_holiday(year, 12, 25),
    }


def is_trading_day(value: date) -> bool:
    return value.weekday() < 5 and value not in us_market_holidays(value.year)


def previous_trading_day(value: date) -> date:
    current = value - timedelta(days=1)
    while not is_trading_day(current):
        current -= timedelta(days=1)
    return current


def resolve_trading_date(raw: str | None, *, now: datetime | None = None) -> date:
    if raw and raw.lower() != "auto":
        return date.fromisoformat(raw)

    local_now = (now or datetime.now(tz=timezone.utc)).astimezone(NY_TZ)
    candidate = local_now.date()
    if local_now.time() < REVIEW_READY:
        candidate = previous_trading_day(candidate)
    while not is_trading_day(candidate):
        candidate = previous_trading_day(candidate + timedelta(days=1))
    return candidate


def session_window(trading_date: date) -> tuple[datetime, datetime, datetime]:
    start = datetime.combine(trading_date, SESSION_START, tzinfo=NY_TZ)
    end = datetime.combine(trading_date, SESSION_END, tzinfo=NY_TZ)
    ready = datetime.combine(trading_date, REVIEW_READY, tzinfo=NY_TZ)
    return start, end, ready


def raw_quote_paths_for_window(
    settings: StorageSettings,
    *,
    start: datetime,
    end: datetime,
) -> list[Path]:
    root = Path(settings.data_root)
    start_utc = start.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    end_utc = end.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    paths: list[Path] = []
    current = start_utc
    while current <= end_utc:
        paths.extend(
            sorted(
                (root / "raw").glob(
                    "provider=*/"
                    f"date={current.strftime('%Y-%m-%d')}/"
                    f"hour={current.strftime('%H')}/"
                    f"{settings.raw_file_name}"
                )
            )
        )
        current += timedelta(hours=1)
    return sorted(dict.fromkeys(paths))


def is_spx_focus_quote(quote: Quote) -> bool:
    instrument = quote.instrument
    canonical = instrument.canonical_id
    if canonical == "index:SPX":
        return True
    if canonical.startswith("future:ES"):
        return True
    if canonical.startswith("future:MES"):
        return True
    if instrument.instrument_type == InstrumentType.OPTION:
        trading_class = (instrument.trading_class or "").upper()
        return (instrument.underlier or instrument.symbol).upper() == "SPX" and trading_class.startswith("SPXW")
    return canonical.startswith("option:SPX:SPXW:")


def load_raw_quotes(
    settings: StorageSettings,
    *,
    start: datetime,
    end: datetime,
) -> tuple[Quote, ...]:
    start_utc = start.astimezone(timezone.utc)
    end_utc = end.astimezone(timezone.utc)
    quotes: list[Quote] = []
    for path in raw_quote_paths_for_window(settings, start=start, end=end):
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line in lines:
            if not line.strip():
                continue
            try:
                quote = quote_from_dict(json.loads(line))
            except (json.JSONDecodeError, TypeError, ValueError):
                continue
            received = as_utc(quote.received_at)
            if start_utc <= received <= end_utc and is_spx_focus_quote(quote):
                quotes.append(quote)
    return tuple(sorted(quotes, key=lambda item: item.received_at))


def load_surface_snapshots(
    settings: StorageSettings,
    *,
    start: datetime,
    end: datetime,
) -> tuple[IvSurfaceSnapshot, ...]:
    class SurfaceSettings:
        data_root = settings.data_root
        raw_file_name = os.getenv("IV_SURFACE_RAW_FILE_NAME", "snapshots.jsonl")

    snapshots: list[IvSurfaceSnapshot] = []
    start_utc = start.astimezone(timezone.utc)
    end_utc = end.astimezone(timezone.utc)
    for path in raw_snapshot_paths_for_window(SurfaceSettings(), start=start, end=end):
        if not path.exists():
            continue
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line in lines:
            if not line.strip():
                continue
            try:
                snapshot = snapshot_from_dict(json.loads(line))
            except (json.JSONDecodeError, KeyError, TypeError, ValueError):
                continue
            snapshot_time = snapshot.as_of.astimezone(timezone.utc)
            if start_utc <= snapshot_time <= end_utc:
                snapshots.append(snapshot)
    return tuple(sorted(snapshots, key=lambda item: item.as_of))


def finite_price(quote: Quote) -> float | None:
    price = quote.effective_price
    return price if price is not None and price > 0 else None


def series_stats(quotes: list[Quote]) -> dict[str, Any]:
    rows = [(quote.received_at, finite_price(quote), quote) for quote in quotes]
    rows = [(ts, price, quote) for ts, price, quote in rows if price is not None]
    if not rows:
        return {
            "count": len(quotes),
            "price_count": 0,
            "first": None,
            "last": None,
            "high": None,
            "low": None,
            "change_points": None,
            "change_bps": None,
        }

    first_ts, first_price, _first_quote = rows[0]
    last_ts, last_price, _last_quote = rows[-1]
    high_ts, high_price, _ = max(rows, key=lambda item: item[1])
    low_ts, low_price, _ = min(rows, key=lambda item: item[1])
    quality_counts = Counter(quote.quality.value for quote in quotes)
    provider_counts = Counter(quote.provider.value for quote in quotes)
    return {
        "count": len(quotes),
        "price_count": len(rows),
        "first": first_price,
        "first_at": first_ts.isoformat(),
        "last": last_price,
        "last_at": last_ts.isoformat(),
        "high": high_price,
        "high_at": high_ts.isoformat(),
        "low": low_price,
        "low_at": low_ts.isoformat(),
        "range_points": high_price - low_price,
        "change_points": last_price - first_price,
        "change_bps": (last_price / first_price - 1.0) * 10_000.0 if first_price else None,
        "qualities": dict(sorted(quality_counts.items())),
        "providers": dict(sorted(provider_counts.items())),
    }


def option_quote_summary(quotes: list[Quote]) -> dict[str, Any]:
    option_quotes = [quote for quote in quotes if quote.instrument.instrument_type == InstrumentType.OPTION]
    expiries = sorted({quote.instrument.expiry or "unknown" for quote in option_quotes})
    contracts = {quote.instrument.canonical_id for quote in option_quotes}
    spreads = [quote.spread_bps for quote in option_quotes if quote.spread_bps is not None]
    strikes = [quote.instrument.strike for quote in option_quotes if quote.instrument.strike is not None]
    return {
        "rows": len(option_quotes),
        "unique_contracts": len(contracts),
        "expiries": expiries,
        "min_strike": min(strikes) if strikes else None,
        "max_strike": max(strikes) if strikes else None,
        "with_iv": sum(1 for quote in option_quotes if quote.greeks and quote.greeks.implied_vol is not None),
        "with_gamma": sum(1 for quote in option_quotes if quote.greeks and quote.greeks.gamma is not None),
        "with_open_interest": sum(1 for quote in option_quotes if quote.open_interest is not None),
        "avg_spread_bps": sum(spreads) / len(spreads) if spreads else None,
        "quality_counts": dict(sorted(Counter(quote.quality.value for quote in option_quotes).items())),
    }


def metric_change(first: float | None, last: float | None) -> dict[str, float | None]:
    return {
        "first": first,
        "last": last,
        "change": last - first if first is not None and last is not None else None,
    }


def expiry_surface_summary(expiry: str, rows: list[IvSurfaceExpiry]) -> dict[str, Any]:
    if not rows:
        return {}
    first = rows[0]
    last = rows[-1]
    return {
        "expiry": expiry,
        "snapshot_count": len(rows),
        "first_as_of": None,
        "last_as_of": None,
        "atm_iv": metric_change(first.atm_iv, last.atm_iv),
        "expected_move_points": metric_change(first.expected_move_points, last.expected_move_points),
        "put_skew_ratio": metric_change(first.put_skew_ratio, last.put_skew_ratio),
        "call_skew_ratio": metric_change(first.call_skew_ratio, last.call_skew_ratio),
        "iv_surface_level": metric_change(first.iv_surface_level, last.iv_surface_level),
        "smile_curvature": metric_change(first.smile_curvature, last.smile_curvature),
        "gamma_state_first": first.gamma_state,
        "gamma_state_last": last.gamma_state,
        "zero_gamma_first": first.zero_gamma,
        "zero_gamma_last": last.zero_gamma,
        "put_wall_first": first.put_wall,
        "put_wall_last": last.put_wall,
        "call_wall_first": first.call_wall,
        "call_wall_last": last.call_wall,
        "quality_counts": dict(sorted(Counter(item.surface_fit_quality for item in rows).items())),
        "avg_spread_bps_last": last.avg_spread_bps,
        "iv_coverage_ratio_last": last.iv_coverage_ratio,
        "gamma_coverage_ratio_last": last.gamma_coverage_ratio,
    }


def surface_summary(snapshots: tuple[IvSurfaceSnapshot, ...]) -> dict[str, Any]:
    if not snapshots:
        return {"snapshot_count": 0, "expiries": []}
    by_expiry: dict[str, list[IvSurfaceExpiry]] = {}
    first_seen: dict[str, datetime] = {}
    last_seen: dict[str, datetime] = {}
    for snapshot in snapshots:
        for expiry in snapshot.expiries:
            by_expiry.setdefault(expiry.expiry, []).append(expiry)
            first_seen.setdefault(expiry.expiry, snapshot.as_of)
            last_seen[expiry.expiry] = snapshot.as_of

    last_expiry_order = [expiry.expiry for expiry in snapshots[-1].expiries[:2]]
    ordered_expiries = last_expiry_order + [
        expiry for expiry in sorted(by_expiry) if expiry not in last_expiry_order
    ]
    expiry_rows = []
    for expiry in ordered_expiries[:4]:
        row = expiry_surface_summary(expiry, by_expiry[expiry])
        row["first_as_of"] = first_seen[expiry].isoformat()
        row["last_as_of"] = last_seen[expiry].isoformat()
        expiry_rows.append(row)

    return {
        "snapshot_count": len(snapshots),
        "first_as_of": snapshots[0].as_of.isoformat(),
        "last_as_of": snapshots[-1].as_of.isoformat(),
        "underlier_first": snapshots[0].underlier_price,
        "underlier_last": snapshots[-1].underlier_price,
        "front_vs_next_atm_iv_gap": metric_change(
            snapshots[0].front_vs_next_atm_iv_gap,
            snapshots[-1].front_vs_next_atm_iv_gap,
        ),
        "expiries": expiry_rows,
        "warnings": sorted({warning for snapshot in snapshots for warning in snapshot.warnings}),
    }


def build_review_payload(
    *,
    trading_date: date,
    settings: StorageSettings,
    now: datetime | None = None,
) -> dict[str, Any]:
    start, end, ready = session_window(trading_date)
    quotes = load_raw_quotes(settings, start=start, end=end)
    snapshots = load_surface_snapshots(settings, start=start, end=end)
    spx_quotes = [quote for quote in quotes if quote.instrument.canonical_id == "index:SPX"]
    es_quotes = [quote for quote in quotes if quote.instrument.canonical_id.startswith("future:ES")]
    mes_quotes = [quote for quote in quotes if quote.instrument.canonical_id.startswith("future:MES")]
    payload = {
        "created_at": (now or datetime.now(tz=timezone.utc)).isoformat(),
        "trading_date": trading_date.isoformat(),
        "session": {
            "timezone": str(NY_TZ),
            "start": start.isoformat(),
            "end": end.isoformat(),
            "review_ready_after": ready.isoformat(),
        },
        "coverage": {
            "raw_quote_rows": len(quotes),
            "iv_surface_snapshots": len(snapshots),
            "spx_rows": len(spx_quotes),
            "es_rows": len(es_quotes),
            "mes_rows": len(mes_quotes),
        },
        "spx": series_stats(spx_quotes),
        "es": series_stats(es_quotes),
        "mes": series_stats(mes_quotes),
        "spxw_options": option_quote_summary(list(quotes)),
        "iv_surface": surface_summary(snapshots),
    }
    payload["verdict"] = review_verdict(payload)
    return payload


def review_verdict(payload: dict[str, Any]) -> dict[str, Any]:
    warnings: list[str] = []
    if payload["coverage"]["spx_rows"] == 0:
        warnings.append("missing SPX raw quote rows")
    if payload["coverage"]["es_rows"] == 0:
        warnings.append("missing ES raw quote rows")
    if payload["coverage"]["iv_surface_snapshots"] == 0:
        warnings.append("missing SPXW IV surface snapshots")
    if payload["spxw_options"]["unique_contracts"] == 0:
        warnings.append("missing SPXW option quote rows")
    degraded = bool(warnings)
    return {
        "status": "degraded" if degraded else "complete",
        "warnings": warnings,
    }


def fmt(value: Any, digits: int = 2) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def fmt_bps(value: Any) -> str:
    return "-" if value is None else f"{float(value):+.1f} bps"


def change_cell(metric: dict[str, Any], digits: int = 4) -> str:
    return f"{fmt(metric.get('first'), digits)} -> {fmt(metric.get('last'), digits)} ({fmt(metric.get('change'), digits)})"


def render_markdown(payload: dict[str, Any]) -> str:
    lines = [
        f"# SPX/SPXW Post-Close Review - {payload['trading_date']}",
        "",
        "Scope: SPX, SPXW option structure, and ES confirmation only. This is a post-session review, not an order recommendation.",
        "",
        "## Summary",
        "",
        f"- Status: `{payload['verdict']['status']}`",
        f"- Raw quote rows: {payload['coverage']['raw_quote_rows']}; IV surface snapshots: {payload['coverage']['iv_surface_snapshots']}",
        f"- SPX rows: {payload['coverage']['spx_rows']}; ES rows: {payload['coverage']['es_rows']}; SPXW contracts: {payload['spxw_options']['unique_contracts']}",
        f"- SPX change: {fmt(payload['spx'].get('change_points'))} pts / {fmt_bps(payload['spx'].get('change_bps'))}; range: {fmt(payload['spx'].get('range_points'))} pts",
        "",
    ]
    warnings = payload["verdict"].get("warnings") or []
    if warnings:
        lines.extend(["## Data Warnings", ""])
        lines.extend(f"- {warning}" for warning in warnings)
        lines.append("")

    lines.extend(
        [
            "## Price Path",
            "",
            "| Instrument | First | Last | Change | High | Low | Rows |",
            "|---|---:|---:|---:|---:|---:|---:|",
            price_row("SPX", payload["spx"]),
            price_row("ES", payload["es"]),
            price_row("MES", payload["mes"]),
            "",
            "## SPXW Quote Coverage",
            "",
            f"- Rows: {payload['spxw_options']['rows']}; unique contracts: {payload['spxw_options']['unique_contracts']}",
            f"- Expiries: {', '.join(payload['spxw_options']['expiries']) if payload['spxw_options']['expiries'] else '-'}",
            f"- Strike window: {fmt(payload['spxw_options']['min_strike'], 0)} - {fmt(payload['spxw_options']['max_strike'], 0)}",
            f"- With IV: {payload['spxw_options']['with_iv']}; with gamma: {payload['spxw_options']['with_gamma']}; with OI: {payload['spxw_options']['with_open_interest']}",
            f"- Average spread: {fmt(payload['spxw_options']['avg_spread_bps'], 1)} bps",
            "",
            "## IV Surface And Walls",
            "",
        ]
    )
    surface = payload["iv_surface"]
    if surface["snapshot_count"] == 0:
        lines.extend(["- No SPXW IV surface snapshots were available.", ""])
    else:
        lines.extend(
            [
                f"- Snapshots: {surface['snapshot_count']} ({surface['first_as_of']} -> {surface['last_as_of']})",
                f"- Underlier: {fmt(surface['underlier_first'])} -> {fmt(surface['underlier_last'])}",
                f"- 0DTE vs next ATM IV gap: {change_cell(surface['front_vs_next_atm_iv_gap'], 4)}",
                "",
                "| Expiry | ATM IV | Exp move | Put skew | Call skew | Gamma | Put wall | Call wall | Quality |",
                "|---|---:|---:|---:|---:|---|---:|---:|---|",
            ]
        )
        for expiry in surface["expiries"][:4]:
            lines.append(surface_row(expiry))
        lines.append("")
    lines.extend(
        [
            "## Hermes Attachment Note",
            "",
            "This section is designed to be appended to the local Hermes daily report after the post-close delay. Hidden cross-market inputs remain outside the human-facing SPX review.",
            "",
        ]
    )
    return "\n".join(lines)


def build_llm_writer_prompt(payload: dict[str, Any], deterministic_markdown: str) -> str:
    compact = {
        "trading_date": payload.get("trading_date"),
        "coverage": payload.get("coverage"),
        "verdict": payload.get("verdict"),
        "spx": payload.get("spx"),
        "es": payload.get("es"),
        "spxw_options": payload.get("spxw_options"),
        "iv_surface": payload.get("iv_surface"),
    }
    return "\n".join(
        (
            "你是做了十几年 SPX 期权的自营交易员，收盘后给搭档写当日复盘。搭档只做 SPX/SPXW 0DTE/1DTE 买方"
            "(call/put/垂直价差)，他要的不是当日行情回放，而是对账：盘前那张地图说的墙位/gamma/预期波幅，"
            "今天市场兑现了多少、哪里打脸了、明天的剧本要改哪里。",
            "写之前先想清楚(不写出来)：今天价格是被墙拦住的还是根本没碰到墙？pin 是 gamma 压出来的还是碰巧？"
            "预期波幅是高估还是低估了，错在 vol 定价还是错在事件？模型今天哪里说对了、哪里说错了，都要点名。",
            "只允许使用给定 JSON 和模板报告里的事实；不编造价格、新闻、仓位。",
            "搭档只交易 SPX/SPXW；正文只提 SPX、SPXW、ES、IV surface、期权墙、gamma 和数据质量。",
            "输出中文 Markdown。第一行必须是：",
            f"# SPX/SPXW Post-Close Review - {payload.get('trading_date')}",
            "结构紧凑：摘要(第一句话就给结论：今天价格路径相对墙位/预期波幅的表现)、价格路径、SPXW 报价覆盖、"
            "IV 曲面与期权墙、下一交易日检查点。",
            "摘要必须有量化对照：实际波动占预期波幅的比例、收盘相对墙位/zero gamma 的位置(引用具体数字)，"
            "并且明说今天的地形判断是兑现还是被证伪。",
            "下一交易日检查点写成双向 if/then：明早价格在哪些位置之上/之下，分别先看什么、动哪张单。",
            "数据 degraded 时只说明覆盖质量，不给方向判断。",
            "",
            "JSON:",
            json.dumps(compact, ensure_ascii=False, sort_keys=True),
            "",
            "模板报告:",
            deterministic_markdown,
        )
    )


def call_deepseek_writer(
    payload: dict[str, Any],
    deterministic_markdown: str,
    settings: ReviewLlmSettings,
) -> tuple[str | None, str | None]:
    api_key = deepseek_api_key(settings)
    if not api_key:
        return None, "missing DEEPSEEK_API_KEY"
    body = {
        "model": settings.model,
        "messages": [
            {
                "role": "system",
                # Same master-to-apprentice doctrine as the intraday writers so the
                # review carries the identical trading philosophy and voice.
                "content": DEFAULT_SYSTEM_PROMPT,
            },
            {"role": "user", "content": build_llm_writer_prompt(payload, deterministic_markdown)},
        ],
        "temperature": 0.2,
        "max_tokens": settings.max_tokens,
        "stream": False,
    }
    request = urllib.request.Request(
        settings.url,
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=settings.timeout_seconds) as response:
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:300]
        return None, f"http={exc.code}: {detail}"
    except OSError as exc:
        return None, str(exc)
    try:
        content = json.loads(raw)["choices"][0]["message"]["content"].strip()
    except (KeyError, IndexError, TypeError, json.JSONDecodeError) as exc:
        return None, f"bad response shape: {exc}"
    if not content:
        return None, "empty response"
    expected_title = f"# SPX/SPXW Post-Close Review - {payload.get('trading_date')}"
    if not content.lstrip().startswith(expected_title):
        content = expected_title + "\n\n" + content.lstrip("# \n")
    return content, None


def maybe_write_llm_review(
    payload: dict[str, Any],
    deterministic_markdown: str,
    settings: ReviewLlmSettings | None = None,
) -> str:
    settings = settings or ReviewLlmSettings.from_env()
    payload["llm_writer"] = {
        "enabled": settings.enabled,
        "provider": settings.provider,
        "model": settings.model,
        "status": "disabled",
    }
    if not settings.enabled:
        return deterministic_markdown
    if settings.provider.lower() != "deepseek":
        payload["llm_writer"]["status"] = "fallback_template"
        payload["llm_writer"]["error"] = f"unsupported provider: {settings.provider}"
        return deterministic_markdown
    markdown, error = call_deepseek_writer(payload, deterministic_markdown, settings)
    if error or not markdown:
        payload["llm_writer"]["status"] = "fallback_template"
        payload["llm_writer"]["error"] = error or "empty response"
        return deterministic_markdown
    payload["llm_writer"]["status"] = "ok"
    return markdown


def price_row(label: str, stats: dict[str, Any]) -> str:
    return (
        f"| {label} | {fmt(stats.get('first'))} | {fmt(stats.get('last'))} | "
        f"{fmt(stats.get('change_points'))} / {fmt_bps(stats.get('change_bps'))} | "
        f"{fmt(stats.get('high'))} | {fmt(stats.get('low'))} | {stats.get('count', 0)} |"
    )


def surface_row(expiry: dict[str, Any]) -> str:
    qualities = expiry.get("quality_counts") or {}
    quality_text = ", ".join(f"{key}:{value}" for key, value in qualities.items()) or "-"
    return (
        f"| {expiry['expiry']} | {change_cell(expiry['atm_iv'], 4)} | "
        f"{change_cell(expiry['expected_move_points'], 2)} | "
        f"{change_cell(expiry['put_skew_ratio'], 3)} | "
        f"{change_cell(expiry['call_skew_ratio'], 3)} | "
        f"{expiry.get('gamma_state_first')} -> {expiry.get('gamma_state_last')} | "
        f"{fmt(expiry.get('put_wall_first'), 0)} -> {fmt(expiry.get('put_wall_last'), 0)} | "
        f"{fmt(expiry.get('call_wall_first'), 0)} -> {fmt(expiry.get('call_wall_last'), 0)} | "
        f"{quality_text} |"
    )


def default_output_dir(settings: StorageSettings) -> Path:
    return Path(os.getenv("SPX_REVIEW_OUTPUT_DIR") or Path(settings.data_root) / "reports" / "spx_options_review")


def default_latest_markdown_path(settings: StorageSettings) -> Path:
    return Path(os.getenv("SPX_REVIEW_LATEST_MARKDOWN_PATH") or Path(settings.data_root) / "latest" / "spx_options_review.md")


def default_hermes_export_dir() -> Path:
    return Path(os.getenv("SPX_REVIEW_HERMES_EXPORT_DIR") or "/home/ubuntu/research/finance/daily/spx-options-review")


def review_paths(
    *,
    trading_date: date,
    settings: StorageSettings,
    output_dir: Path | None = None,
    latest_markdown_path: Path | None = None,
    hermes_export_dir: Path | None = None,
) -> ReviewPaths:
    root = output_dir or default_output_dir(settings)
    report_dir = root / f"date={trading_date.isoformat()}"
    latest_md = latest_markdown_path or default_latest_markdown_path(settings)
    hermes_path = None
    hermes_latest = None
    if hermes_export_dir is not None:
        hermes_path = hermes_export_dir / f"{trading_date.isoformat()}-spx-options-review.md"
        hermes_latest = hermes_export_dir / "latest-spx-options-review.md"
    return ReviewPaths(
        report_dir=report_dir,
        markdown_path=report_dir / "review.md",
        json_path=report_dir / "review.json",
        latest_markdown_path=latest_md,
        latest_json_path=latest_md.with_suffix(".json"),
        hermes_markdown_path=hermes_path,
        hermes_latest_markdown_path=hermes_latest,
    )


def write_outputs(payload: dict[str, Any], markdown: str, paths: ReviewPaths) -> dict[str, str]:
    paths.report_dir.mkdir(parents=True, exist_ok=True)
    paths.markdown_path.write_text(markdown, encoding="utf-8")
    paths.json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    paths.latest_markdown_path.parent.mkdir(parents=True, exist_ok=True)
    paths.latest_markdown_path.write_text(markdown, encoding="utf-8")
    paths.latest_json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    result = {
        "markdown_path": str(paths.markdown_path),
        "json_path": str(paths.json_path),
        "latest_markdown_path": str(paths.latest_markdown_path),
        "latest_json_path": str(paths.latest_json_path),
    }
    if paths.hermes_markdown_path is not None and paths.hermes_latest_markdown_path is not None:
        paths.hermes_markdown_path.parent.mkdir(parents=True, exist_ok=True)
        paths.hermes_markdown_path.write_text(markdown, encoding="utf-8")
        paths.hermes_latest_markdown_path.write_text(markdown, encoding="utf-8")
        result["hermes_markdown_path"] = str(paths.hermes_markdown_path)
        result["hermes_latest_markdown_path"] = str(paths.hermes_latest_markdown_path)
    return result


def build_push_summary(payload: dict[str, Any], *, latest_markdown_path: str) -> str:
    trading_date = payload.get("trading_date", "-")
    spx = payload.get("spx") if isinstance(payload.get("spx"), dict) else {}
    first = spx.get("first")
    last = spx.get("last")
    change_points = spx.get("change_points")
    change_bps = spx.get("change_bps")
    range_points = spx.get("range_points")
    low = spx.get("low")
    high = spx.get("high")

    iv_surface = payload.get("iv_surface") if isinstance(payload.get("iv_surface"), dict) else {}
    expiries = iv_surface.get("expiries") if isinstance(iv_surface.get("expiries"), list) else []
    front = expiries[0] if expiries and isinstance(expiries[0], dict) else {}

    put_wall_last = front.get("put_wall_last")
    call_wall_last = front.get("call_wall_last")
    zero_gamma_last = front.get("zero_gamma_last")
    gamma_state_last = front.get("gamma_state_last")

    atm_iv = front.get("atm_iv") if isinstance(front.get("atm_iv"), dict) else {}
    put_skew = front.get("put_skew_ratio") if isinstance(front.get("put_skew_ratio"), dict) else {}
    atm_iv_text = f"{fmt(atm_iv.get('first'), 4)}→{fmt(atm_iv.get('last'), 4)}"
    put_skew_text = f"{fmt(put_skew.get('first'), 3)}→{fmt(put_skew.get('last'), 3)}"

    verdict = payload.get("verdict") if isinstance(payload.get("verdict"), dict) else {}
    status = verdict.get("status", "-")
    warnings = verdict.get("warnings") if isinstance(verdict.get("warnings"), list) else []
    warning_text = f" ({', '.join(str(item) for item in warnings)})" if warnings else ""

    change_points_text = "-" if change_points is None else f"{float(change_points):+.1f}"
    change_bps_text = "-" if change_bps is None else f"{float(change_bps):+.0f}"
    range_text = "-" if range_points is None else f"{float(range_points):.1f}"

    lines = [
        f"【盘后复盘 {trading_date}】",
        (
            f"SPX: {fmt(first)}→{fmt(last)}({change_points_text} 点/{change_bps_text}bp), "
            f"区间 {range_text} 点(低 {fmt(low)} 高 {fmt(high)})"
        ),
        (
            f"0DTE 收盘墙位: put {fmt(put_wall_last, 0)} call {fmt(call_wall_last, 0)}, "
            f"zero gamma {fmt(zero_gamma_last, 0)}, gamma {fmt(gamma_state_last)}"
        ),
        f"ATM IV: {atm_iv_text}, put skew: {put_skew_text}",
        f"数据: {status}{warning_text}",
        f"完整报告: {latest_markdown_path}",
    ]
    return "\n".join(lines)


def build_review_push_prompt(payload: dict[str, Any], summary: str) -> str:
    return "\n".join(
        (
            "收盘了，给刚睡醒或还没睡的搭档发一条当日收盘便签。他只做 SPX/SPXW 0DTE 买方，凌晨挂的单已经了结或作废，"
            "他现在想知道的是：今天的地形判断靠不靠谱、明天开盘前要先看什么。",
            "只依据 JSON 与摘要事实。输出中文最多 12 行，第一行必须是摘要第一行。",
            "必须覆盖：当日价格路径一句话(相对预期波幅走了多少)、墙位/zero gamma/gamma state 的收盘位、IV 与 skew 当日变化；",
            "然后 2-3 句结构点评，要下判断不要罗列：pin 是 gamma 压出来的还是碰巧、墙被打穿过没有、IV 是 crush 还是抬升、"
            "今天地图哪里说对了哪里说错了；",
            "最后 2-3 条『下一交易日开盘前检查项』，写成看什么、到什么位置意味着什么。数据 degraded 时如实说明。",
            "JSON:" + json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            "摘要:" + summary,
        )
    )


def push_review(
    payload: dict[str, Any],
    *,
    latest_markdown_path: str,
    runner: CommandRunner = default_runner,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = now or datetime.now(tz=timezone.utc)
    if not env_bool("SPX_REVIEW_PUSH_ENABLED", True):
        return {"skipped": True, "reason": "push_disabled"}

    settings = NotificationSettings.from_env()
    summary = build_push_summary(payload, latest_markdown_path=latest_markdown_path)
    used_agent = False
    text = summary

    if settings.openclaw_agent_enabled:
        sink, reply = run_openclaw_agent(
            settings,
            build_review_push_prompt(payload, summary),
            runner=runner,
        )
        if reply and sink.ok:
            text = reply
            used_agent = True

    delivery_sinks = deliver_trade_push(
        settings,
        title="盘后复盘",
        text=text,
        kind="post_close_review",
        lane="trade",
        friend=True,
        runner=runner,
    )
    delivered_ok = any_delivery_ok(delivery_sinks)
    if not im_delivery_ok(delivery_sinks):
        append_missed(settings.missed_queue_path, text, kind="post_close_review", at=now)

    return {
        "text": text,
        "used_agent": used_agent,
        "weixin_ok": any(s.sink == "openclaw_message" and s.ok for s in delivery_sinks),
        "bark_ok": any(s.sink == "bark" and s.ok for s in delivery_sinks),
        "feishu_ok": any(s.sink == "feishu" and s.ok for s in delivery_sinks),
        "delivered_ok": delivered_ok,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build an SPX/SPXW post-close daily review.")
    parser.add_argument("--date", default="auto", help="NY trading date, YYYY-MM-DD, or auto.")
    parser.add_argument("--json", action="store_true", help="Print JSON payload.")
    parser.add_argument("--markdown", action="store_true", help="Print Markdown report.")
    parser.add_argument("--no-write", action="store_true", help="Do not write report artifacts.")
    parser.add_argument("--output-dir", help="Report output directory.")
    parser.add_argument("--latest-markdown-path", help="Latest Markdown path.")
    parser.add_argument("--hermes-export-dir", help="Hermes daily attachment export directory.")
    parser.add_argument("--no-hermes-export", action="store_true", help="Do not write Hermes export files.")
    parser.add_argument("--llm", action="store_true", help="Force-enable the configured LLM writer.")
    parser.add_argument("--no-llm", action="store_true", help="Disable the LLM writer for this run.")
    parser.add_argument("--quiet-if-empty", action="store_true", help="Suppress stdout when there are no raw rows or snapshots.")
    parser.add_argument("--no-push", action="store_true", help="Do not push review summary to WeChat/Bark.")
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = StorageSettings.from_env()
    trading_date = resolve_trading_date(args.date)
    payload = build_review_payload(trading_date=trading_date, settings=settings)
    markdown = render_markdown(payload)
    llm_settings = ReviewLlmSettings.from_env()
    if args.llm:
        llm_settings = ReviewLlmSettings(
            enabled=True,
            provider=llm_settings.provider,
            model=llm_settings.model,
            url=llm_settings.url,
            env_file=llm_settings.env_file,
            timeout_seconds=llm_settings.timeout_seconds,
            max_tokens=llm_settings.max_tokens,
        )
    if args.no_llm:
        llm_settings = ReviewLlmSettings(
            enabled=False,
            provider=llm_settings.provider,
            model=llm_settings.model,
            url=llm_settings.url,
            env_file=llm_settings.env_file,
            timeout_seconds=llm_settings.timeout_seconds,
            max_tokens=llm_settings.max_tokens,
        )
    markdown = maybe_write_llm_review(payload, markdown, llm_settings)
    paths_payload = None
    if not args.no_write:
        hermes_export_dir = None if args.no_hermes_export else (
            Path(args.hermes_export_dir) if args.hermes_export_dir else default_hermes_export_dir()
        )
        paths = review_paths(
            trading_date=trading_date,
            settings=settings,
            output_dir=Path(args.output_dir) if args.output_dir else None,
            latest_markdown_path=Path(args.latest_markdown_path) if args.latest_markdown_path else None,
            hermes_export_dir=hermes_export_dir,
        )
        paths_payload = write_outputs(payload, markdown, paths)
        payload["paths"] = paths_payload

    latest_markdown_path = str(
        paths_payload["latest_markdown_path"]
        if paths_payload
        else default_latest_markdown_path(settings)
    )
    if not args.no_push:
        coverage = payload["coverage"]
        if not (coverage["raw_quote_rows"] == 0 and coverage["iv_surface_snapshots"] == 0):
            payload["push"] = push_review(payload, latest_markdown_path=latest_markdown_path)

    if (
        args.quiet_if_empty
        and payload["coverage"]["raw_quote_rows"] == 0
        and payload["coverage"]["iv_surface_snapshots"] == 0
    ):
        return 0
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(markdown)
        if paths_payload:
            print(f"\nWrote review: {paths_payload['markdown_path']}")
    return 0


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
