"""CLI, LLM writer, output, and notification runtime for post-close review."""

from __future__ import annotations

import argparse
import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from spx_spark.config import NotificationSettings, StorageSettings, env_bool, load_dotenv
from spx_spark.notifier.llm_writer import DEFAULT_SYSTEM_PROMPT
from spx_spark.notifier.missed_queue import append_missed
from spx_spark.notifier.model import CommandRunner, default_runner
from spx_spark.notifier.sinks import (
    any_delivery_ok,
    deliver_trade_push,
    im_delivery_ok,
    run_openclaw_agent,
)
from spx_spark.market_calendar import DEFAULT_MARKET_CALENDAR, ET, MarketCalendar
from spx_spark.post_close_render import fmt, render_markdown
from spx_spark.settings import settings_value
from spx_spark.post_close_review import (
    build_review_payload,
)


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
            enabled=env_bool("SPX_REVIEW_LLM_ENABLED", bool(settings_value("review.llm_enabled"))),
            provider=os.getenv(
                "SPX_REVIEW_LLM_PROVIDER", str(settings_value("review.llm_provider"))
            ).strip(),
            model=os.getenv(
                "SPX_REVIEW_LLM_MODEL", str(settings_value("review.llm_model"))
            ).strip(),
            url=os.getenv(
                "SPX_REVIEW_LLM_URL",
                str(settings_value("review.llm_url")),
            ).strip(),
            env_file=os.getenv(
                "SPX_REVIEW_LLM_ENV_FILE",
                str(settings_value("review.llm_env_file")),
            ).strip(),
            timeout_seconds=float(
                os.getenv(
                    "SPX_REVIEW_LLM_TIMEOUT_SECONDS",
                    str(settings_value("review.llm_timeout_seconds")),
                )
            ),
            max_tokens=int(
                os.getenv("SPX_REVIEW_LLM_MAX_TOKENS", str(settings_value("review.llm_max_tokens")))
            ),
        )


def resolve_trading_date(
    raw: str | None,
    *,
    now: datetime | None = None,
    calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR,
) -> date:
    if raw and raw.lower() != "auto":
        return date.fromisoformat(raw)
    return calendar.completed_review_date(now or datetime.now(tz=timezone.utc))


def ready_auto_review_date(
    *,
    now: datetime,
    calendar: MarketCalendar = DEFAULT_MARKET_CALENDAR,
) -> date | None:
    local_now = now.astimezone(ET)
    session = calendar.session(local_now.date())
    if session is None or local_now < session.review_ready_at:
        return None
    selected = calendar.completed_review_date(local_now)
    return selected if selected == local_now.date() else None


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


def build_llm_writer_prompt(payload: dict[str, Any], deterministic_markdown: str) -> str:
    compact = {
        "trading_date": payload.get("trading_date"),
        "coverage": payload.get("coverage"),
        "verdict": payload.get("verdict"),
        "spx": payload.get("spx"),
        "es": payload.get("es"),
        "spxw_options": payload.get("spxw_options"),
        "iv_surface": payload.get("iv_surface"),
        "spxw_0dte_greeks_reference": payload.get("spxw_0dte_greeks_reference"),
        "completeness": payload.get("completeness"),
    }
    return "\n".join(
        (
            "你是做了十几年 SPX 期权的自营交易员，收盘后给搭档写当日复盘。搭档只做 SPX/SPXW 0DTE/1DTE 买方"
            "(call/put/垂直价差)，他要的不是当日行情回放，而是对账：盘前那张地图说的墙位/gamma/预期波幅，"
            "今天市场兑现了多少、哪里打脸了、明天的剧本要改哪里。",
            "写之前先想清楚(不写出来)：今天价格是被墙拦住的还是根本没碰到墙？pin 是 gamma 压出来的还是碰巧？"
            "预期波幅是高估还是低估了，错在 vol 定价还是错在事件？模型今天哪里说对了、哪里说错了，都要点名。",
            "只允许使用给定 JSON 和模板报告里的事实；不编造价格、新闻、仓位。",
            "框架口径：复盘对照 Micopedia/Steven observe_only 剧本（regime→map→trigger→exit）；"
            "Steven episode 若存在只作审计附注；*_proxy 曝露不是 vendor DEX；不下单授权。",
            "搭档只交易 SPX/SPXW；正文只提 SPX、SPXW、ES、IV surface、期权墙、gamma 和数据质量。",
            "输出中文 Markdown。第一行必须是：",
            f"# SPX/SPXW Post-Close Review - {payload.get('trading_date')}",
            "结构紧凑：摘要(第一句话就给结论：今天价格路径相对墙位/预期波幅的表现)、价格路径、SPXW 报价覆盖、"
            "IV 曲面与期权墙、下一交易日检查点。",
            "摘要必须有量化对照：实际波动占预期波幅的比例、收盘相对墙位/zero gamma 的位置(引用具体数字)，"
            "并且明说今天的地形判断是兑现还是被证伪。",
            "下一交易日检查点写成双向 if/then：明早价格在哪些位置之上/之下，分别先看什么、动哪张单。",
            "数据 degraded 时只说明覆盖质量，不给方向判断。",
            "spxw_0dte_greeks_reference 是同日到期的 reference-only 影子层；position_sign/direction 永远 unknown。"
            "负 gamma 不等于看跌，绝不能据此改写 Call/Put 方向；next expiry 只能用于已有 ATM IV gap 对照。",
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
    # Resolve through the compatibility facade so existing integrations can
    # replace the writer without importing this runtime module directly.
    from spx_spark import post_close_review as facade

    markdown, error = facade.call_deepseek_writer(payload, deterministic_markdown, settings)
    if error or not markdown:
        payload["llm_writer"]["status"] = "fallback_template"
        payload["llm_writer"]["error"] = error or "empty response"
        return deterministic_markdown
    payload["llm_writer"]["status"] = "ok"
    expected_title = f"# SPX/SPXW Post-Close Review - {payload.get('trading_date')}"
    narrative = markdown.strip()
    if narrative.startswith(expected_title):
        narrative = narrative[len(expected_title) :].lstrip()
    return deterministic_markdown.rstrip() + "\n\n## LLM Commentary\n\n" + narrative + "\n"


def default_output_dir(settings: StorageSettings) -> Path:
    return Path(
        os.getenv("SPX_REVIEW_OUTPUT_DIR")
        or Path(settings.data_root) / str(settings_value("review.output_dir_name"))
    )


def default_latest_markdown_path(settings: StorageSettings) -> Path:
    return Path(
        os.getenv("SPX_REVIEW_LATEST_MARKDOWN_PATH")
        or Path(settings.data_root) / "latest" / "spx_options_review.md"
    )


def default_hermes_export_dir() -> Path:
    return Path(
        os.getenv("SPX_REVIEW_HERMES_EXPORT_DIR") or str(settings_value("review.hermes_export_dir"))
    )


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
    paths.latest_json_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
    )
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
    expected_front_expiry = str(trading_date).replace("-", "")
    front = next(
        (
            item
            for item in expiries
            if isinstance(item, dict) and item.get("expiry") == expected_front_expiry
        ),
        {},
    )

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
    if not env_bool("SPX_REVIEW_PUSH_ENABLED", bool(settings_value("review.push_enabled"))):
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
        "im_ok": any(s.sink == "feishu" and s.ok for s in delivery_sinks),
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
    parser.add_argument(
        "--no-hermes-export", action="store_true", help="Do not write Hermes export files."
    )
    parser.add_argument(
        "--llm", action="store_true", help="Force-enable the configured LLM writer."
    )
    parser.add_argument(
        "--no-llm", action="store_true", help="Disable the LLM writer for this run."
    )
    parser.add_argument(
        "--quiet-if-empty",
        action="store_true",
        help="Suppress stdout when there are no raw rows or snapshots.",
    )
    parser.add_argument(
        "--no-push", action="store_true", help="Do not push review summary to Feishu/Bark."
    )
    return parser.parse_args(argv)


def run(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    settings = StorageSettings.from_env()
    run_now = datetime.now(tz=timezone.utc)
    if args.date.lower() == "auto":
        selected_date = ready_auto_review_date(now=run_now)
        if selected_date is None:
            return 0
        trading_date = selected_date
    else:
        trading_date = resolve_trading_date(args.date, now=run_now)
    payload = build_review_payload(trading_date=trading_date, settings=settings, now=run_now)
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
        hermes_export_dir = (
            None
            if args.no_hermes_export
            else (
                Path(args.hermes_export_dir)
                if args.hermes_export_dir
                else default_hermes_export_dir()
            )
        )
        paths = review_paths(
            trading_date=trading_date,
            settings=settings,
            output_dir=Path(args.output_dir) if args.output_dir else None,
            latest_markdown_path=Path(args.latest_markdown_path)
            if args.latest_markdown_path
            else None,
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
